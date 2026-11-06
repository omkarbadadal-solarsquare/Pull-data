
import csv
import hashlib
import io
import json
import os
import sys
import time
from datetime import date, timedelta
from typing import Any

import gspread
import requests
from gspread.exceptions import WorksheetNotFound
from requests.exceptions import RequestException


# ============================================================
# 1. CONFIGURATION
# ============================================================

METABASE_URL = os.getenv(
    "METABASE_URL",
    "https://metabase-lighthouse.solarsquare.in"
).rstrip("/")

QUESTION_ID = os.getenv("QUESTION_ID", "6541")

SHEET_NAME = os.getenv("SHEET_NAME", "Sheet1")

STATE_SHEET_NAME = os.getenv(
    "STATE_SHEET_NAME",
    "_loader_state_6541"
)

COHORT_START = os.getenv("COHORT_START", "2026-09-01")

COHORT_END = os.getenv("COHORT_END", "2026-10-01")

CHUNK_DAYS = int(os.getenv("CHUNK_DAYS", "1"))

REQUEST_RETRIES = int(os.getenv("REQUEST_RETRIES", "2"))

RESET_IMPORT = (
    os.getenv("RESET_IMPORT", "false").strip().lower() == "true"
)

REQUEST_CONNECT_TIMEOUT = 20
REQUEST_READ_TIMEOUT = 180


# ============================================================
# 2. OUTPUT COLUMNS
# ============================================================

OUTPUT_COLUMNS = [
    "lead_id",
    "lead_created_timestamp_ist",
    "cohort_month",
    "city",
    "channel",
    "lrm",
    "heuristic_score",
    "current_stage",
    "current_status",
    "disposition_bucket",
    "first_ms_timestamp_ist",
    "first_md_timestamp_ist",
]

STATE_HEADERS = [
    "job_signature",
    "next_chunk_index",
    "next_output_row",
    "total_chunks",
    "status",
]

GOOGLE_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

# Cache template tags after the first successful lookup.
TEMPLATE_TAG_CACHE = None


# ============================================================
# 3. VALIDATE SECRETS
# ============================================================

def required_env(name: str) -> str:

    value = os.getenv(name, "").strip()

    if not value:
        raise RuntimeError(
            f"Missing required GitHub secret: {name}"
        )

    return value


# ============================================================
# 4. BUILD DATE WINDOWS
# ============================================================

def build_windows() -> list[tuple[str, str]]:

    start = date.fromisoformat(COHORT_START)
    end = date.fromisoformat(COHORT_END)

    if end <= start:
        raise ValueError(
            "COHORT_END must be greater than COHORT_START."
        )

    if CHUNK_DAYS < 1:
        raise ValueError(
            "CHUNK_DAYS must be at least 1."
        )

    windows = []

    current = start

    while current < end:

        next_date = min(
            current + timedelta(days=CHUNK_DAYS),
            end
        )

        windows.append((
            current.isoformat(),
            next_date.isoformat()
        ))

        current = next_date

    # Process the latest dates first.
    # This ensures the newest duplicate Lead ID is retained.

    windows.reverse()

    return windows


# ============================================================
# 5. NORMALIZE CSV HEADERS
# ============================================================

def normalize_header(value: Any) -> str:

    return "".join(
        character
        for character in str(value)
        .replace("\ufeff", "")
        .strip()
        .lower()
        if character.isalnum()
    )


# ============================================================
# 6. CREATE METABASE SESSION
# ============================================================

def get_metabase_session() -> requests.Session:

    api_key = required_env("METABASE_API_KEY")

    session = requests.Session()

    session.headers.update({
        "X-API-Key": api_key,
        "Accept": "application/json, text/csv, */*"
    })

    return session


# ============================================================
# 7. GET ACTUAL METABASE TEMPLATE TAG IDS
# ============================================================

def get_template_tags(
    session: requests.Session
) -> dict:

    global TEMPLATE_TAG_CACHE

    if TEMPLATE_TAG_CACHE is not None:
        return TEMPLATE_TAG_CACHE

    url = (
        f"{METABASE_URL}/api/card/"
        f"{QUESTION_ID}?legacy-mbql=true"
    )

    print(
        f"Reading metadata for Metabase Question {QUESTION_ID}..."
    )

    try:

        response = session.get(
            url,
            timeout=(
                REQUEST_CONNECT_TIMEOUT,
                60
            )
        )

    except RequestException as exc:

        raise RuntimeError(
            f"Could not retrieve Metabase question metadata: {exc}"
        ) from exc


    if response.status_code != 200:

        raise RuntimeError(
            "Metabase metadata request failed.\n"
            f"HTTP: {response.status_code}\n"
            f"Response: {response.text[:1500]}"
        )


    card = response.json()

    dataset_query = card.get("dataset_query") or {}

    native = dataset_query.get("native")

    if not isinstance(native, dict):
        native = {}


    # First source: native template-tags
    raw_tags = (
        native.get("template-tags")
        or native.get("template_tags")
        or {}
    )


    # Support both dictionary and list formats.
    if isinstance(raw_tags, list):

        raw_tags = {
            tag["name"]: tag
            for tag in raw_tags
            if isinstance(tag, dict) and tag.get("name")
        }


    if not isinstance(raw_tags, dict):
        raw_tags = {}


    # Second source: card.parameters
    # Use this as a fallback if an ID is missing in template-tags.

    parameters_by_name = {}

    for parameter in card.get("parameters") or []:

        if not isinstance(parameter, dict):
            continue

        target = parameter.get("target") or []

        parameter_name = None

        if (
            isinstance(target, list)
            and len(target) >= 2
            and target[0] == "variable"
            and isinstance(target[1], list)
            and len(target[1]) >= 2
            and target[1][0] == "template-tag"
        ):

            parameter_name = str(target[1][1])


        if not parameter_name:

            parameter_name = (
                parameter.get("name")
                or parameter.get("slug")
            )


        if parameter_name:

            parameters_by_name[parameter_name] = parameter


    # Merge metadata and parameter information.

    result = {}

    for name in ("chunk_start", "chunk_end"):

        tag = raw_tags.get(name)

        if not isinstance(tag, dict):
            tag = {}

        tag = dict(tag)

        matching_parameter = parameters_by_name.get(
            name,
            {}
        )


        # Use tag ID first; otherwise parameter ID.
        tag_id = (
            tag.get("id")
            or matching_parameter.get("id")
        )

        tag_type = (
            tag.get("type")
            or matching_parameter.get("type")
            or "text"
        )


        if not tag_id:

            raise RuntimeError(
                f"Metabase variable {{{{{name}}}}} was found "
                "but its ID is missing.\n"
                f"Available template tags: "
                f"{list(raw_tags.keys())}\n"
                f"Available parameters: "
                f"{list(parameters_by_name.keys())}\n"
                "Check the saved SQL question and its "
                "parameter configuration."
            )


        result[name] = {
            "id": str(tag_id),
            "type": str(tag_type).lower()
        }


    TEMPLATE_TAG_CACHE = result

    print(
        "\nMetabase template tags successfully retrieved:"
    )

    for name, tag in result.items():

        print(
            f"{name}: ID={tag['id']}, "
            f"Type={tag['type']}"
        )

    return TEMPLATE_TAG_CACHE


# ============================================================
# 8. MAP METABASE PARAMETER TYPES
# ============================================================

def get_parameter_type(tag: dict) -> str:

    tag_type = str(
        tag.get("type", "text")
    ).lower()

    if tag_type == "text":
        return "category"

    if tag_type == "date":
        return "date/single"

    if tag_type == "number":
        return "number/="

    raise RuntimeError(
        f"Unsupported Metabase template variable type: "
        f"{tag_type}"
    )


# ============================================================
# 9. BUILD PARAMETERS WITH ACTUAL IDS
# ============================================================

def build_parameters(
    tags: dict,
    chunk_start: str,
    chunk_end: str
) -> list[dict]:

    values = {
        "chunk_start": chunk_start,
        "chunk_end": chunk_end
    }

    parameters = []

    for name in ("chunk_start", "chunk_end"):

        tag = tags[name]

        tag_id = str(
            tag.get("id", "")
        ).strip()

        if not tag_id:

            raise RuntimeError(
                f"Missing required parameter ID: {name}"
            )


        parameter = {

            # Critical: Metabase requires this ID.
            "id": tag_id,

            "type": get_parameter_type(tag),

            "target": [
                "variable",
                ["template-tag", name]
            ],

            "value": values[name]

        }

        parameters.append(parameter)


    return parameters


# ============================================================
# 10. FETCH CSV FROM METABASE
# ============================================================

def fetch_chunk_csv(
    session: requests.Session,
    tags: dict,
    chunk_start: str,
    chunk_end: str
) -> list[list[str]]:

    endpoint = (
        f"{METABASE_URL}/api/card/"
        f"{QUESTION_ID}/query/csv"
    )


    parameters = build_parameters(
        tags,
        chunk_start,
        chunk_end
    )


    print(
        f"\nFetching chunk: {chunk_start} to {chunk_end}"
    )


    # Show parameter metadata, never show the API key.
    for parameter in parameters:

        print(
            f"Parameter target: {parameter['target']}, "
            f"ID: {parameter['id']}"
        )


    last_error = ""


    for attempt in range(1, REQUEST_RETRIES + 2):

        try:

            response = session.post(

                endpoint,

                data={
                    "parameters": json.dumps(parameters)
                },

                timeout=(
                    REQUEST_CONNECT_TIMEOUT,
                    REQUEST_READ_TIMEOUT
                )

            )


        except RequestException as exc:

            last_error = str(exc)

            if attempt <= REQUEST_RETRIES:

                wait_seconds = min(
                    5 * (2 ** (attempt - 1)),
                    30
                )

                print(
                    f"Network error. Retrying in "
                    f"{wait_seconds} seconds."
                )

                time.sleep(wait_seconds)

                continue

            raise RuntimeError(
                f"Network request failed for "
                f"{chunk_start} to {chunk_end}: "
                f"{last_error}"
            ) from exc


        code = response.status_code

        print(
            f"Metabase HTTP {code} | Attempt {attempt}"
        )


        if code == 200:

            content = response.text.lstrip("\ufeff")

            if content.lstrip().lower().startswith(
                ("<html", "<!doctype html")
            ):

                raise RuntimeError(
                    "Metabase returned HTML instead of CSV. "
                    "Check authentication and endpoint access."
                )


            try:

                return list(
                    csv.reader(
                        io.StringIO(content)
                    )
                )

            except csv.Error as exc:

                raise RuntimeError(
                    f"Unable to parse Metabase CSV: {exc}"
                ) from exc


        last_error = (
            f"HTTP {code}: {response.text[:2000]}"
        )


        # These errors will not be fixed by retries.

        if code in (400, 401, 403, 404):
            break


        if attempt <= REQUEST_RETRIES:

            wait_seconds = min(
                5 * (2 ** (attempt - 1)),
                30
            )

            print(
                f"Retrying after {wait_seconds} seconds..."
            )

            time.sleep(wait_seconds)


    extra_message = ""

    if "524" in last_error:

        extra_message = (
            "\nMetabase proxy timeout detected. "
            "The query needs further optimization, even "
            "with one-day chunks."
        )


    raise RuntimeError(
        f"Metabase extraction failed for "
        f"{chunk_start} to {chunk_end}.\n"
        f"{last_error}{extra_message}"
    )


# ============================================================
# 11. MAP CSV TO REQUIRED 12 COLUMNS
# ============================================================

def map_csv_rows(
    csv_rows: list[list[str]]
) -> list[list[str]]:

    if not csv_rows:
        return []


    headers = csv_rows[0]

    normalized_headers = [
        normalize_header(header)
        for header in headers
    ]


    indexes = []

    for column in OUTPUT_COLUMNS:

        key = normalize_header(column)

        if key not in normalized_headers:

            raise RuntimeError(
                f'Missing required column: "{column}".\n'
                f"Columns received: {headers}"
            )

        indexes.append(
            normalized_headers.index(key)
        )


    output_rows = []

    for source_row in csv_rows[1:]:

        if not source_row:
            continue


        row = [

            source_row[index]
            if index < len(source_row)
            else ""

            for index in indexes

        ]


        if all(
            str(value).strip() == ""
            for value in row
        ):
            continue


        output_rows.append(row)


    return output_rows


# ============================================================
# 12. CONNECT TO GOOGLE SHEETS
# ============================================================

def setup_sheets():

    service_account_json = required_env(
        "GOOGLE_SERVICE_ACCOUNT_JSON"
    )

    spreadsheet_id = required_env(
        "GOOGLE_SPREADSHEET_ID"
    )


    try:

        service_account_info = json.loads(
            service_account_json
        )

    except json.JSONDecodeError as exc:

        raise RuntimeError(
            "GOOGLE_SERVICE_ACCOUNT_JSON is not valid JSON."
        ) from exc


    client = gspread.service_account_from_dict(
        service_account_info,
        scopes=GOOGLE_SCOPES
    )


    spreadsheet = client.open_by_key(
        spreadsheet_id
    )


    target_ws = open_or_create_worksheet(
        spreadsheet,
        SHEET_NAME,
        rows=1000,
        cols=len(OUTPUT_COLUMNS)
    )


    state_ws = open_or_create_worksheet(
        spreadsheet,
        STATE_SHEET_NAME,
        rows=10,
        cols=len(STATE_HEADERS)
    )


    try:
        state_ws.hide()

    except Exception as exc:

        print(
            f"Checkpoint tab could not be hidden: {exc}"
        )


    return spreadsheet, target_ws, state_ws


# ============================================================
# 13. OPEN OR CREATE WORKSHEET
# ============================================================

def open_or_create_worksheet(
    spreadsheet,
    title: str,
    rows: int,
    cols: int
):

    try:

        worksheet = spreadsheet.worksheet(title)

    except WorksheetNotFound:

        worksheet = spreadsheet.add_worksheet(
            title=title,
            rows=max(rows, 10),
            cols=max(cols, 5)
        )


    if worksheet.col_count < cols:

        worksheet.resize(cols=cols)


    return worksheet


# ============================================================
# 14. READ SAVED CHECKPOINT
# ============================================================

def get_state(
    state_ws
) -> dict | None:

    values = state_ws.row_values(2)

    if not values:
        return None


    values += [""] * (
        len(STATE_HEADERS) - len(values)
    )


    if not any(str(value).strip() for value in values):
        return None


    try:

        return {

            "signature": values[0],

            "next_chunk": int(values[1]),

            "next_row": int(values[2]),

            "total_chunks": int(values[3]),

            "status": values[4]

        }

    except (ValueError, TypeError) as exc:

        raise RuntimeError(
            "Invalid checkpoint data. "
            "Use reset_existing_data=true to restart."
        ) from exc


# ============================================================
# 15. SAVE CHECKPOINT
# ============================================================

def save_state(
    state_ws,
    signature: str,
    next_chunk: int,
    next_row: int,
    total_chunks: int,
    status: str
):

    state_ws.update(
        [STATE_HEADERS],
        "A1",
        raw=True
    )


    state_ws.update(
        [[
            signature,
            next_chunk,
            next_row,
            total_chunks,
            status
        ]],
        "A2",
        raw=True
    )


# ============================================================
# 16. INITIALIZE OR RESUME
# ============================================================

def initialize_or_resume(
    target_ws,
    state_ws,
    signature: str,
    total_chunks: int
):

    saved = get_state(state_ws)


    # --------------------------------------------------------
    # RESET
    # --------------------------------------------------------

    if RESET_IMPORT:

        print(
            "\nRESET_IMPORT=true. "
            "Starting a completely fresh extraction."
        )

        target_ws.clear()

        state_ws.clear()


        target_ws.update(
            [OUTPUT_COLUMNS],
            "A1",
            raw=True
        )

        target_ws.freeze(rows=1)


        save_state(
            state_ws,
            signature,
            0,
            2,
            total_chunks,
            "running"
        )


        return 0, 2, "running"


    # --------------------------------------------------------
    # FIRST RUN
    # --------------------------------------------------------

    if saved is None:

        existing_values = target_ws.col_values(1)

        existing_data = existing_values[1:]


        if any(
            str(value).strip()
            for value in existing_data
        ):

            raise RuntimeError(
                f"{SHEET_NAME} already contains data, "
                "but no valid extraction checkpoint exists. "
                "Run with reset_existing_data=true "
                "to start fresh."
            )


        target_ws.clear()

        target_ws.update(
            [OUTPUT_COLUMNS],
            "A1",
            raw=True
        )

        target_ws.freeze(rows=1)


        save_state(
            state_ws,
            signature,
            0,
            2,
            total_chunks,
            "running"
        )


        print(
            f"Starting fresh. Total chunks: {total_chunks}"
        )


        return 0, 2, "running"


    # --------------------------------------------------------
    # VALIDATE SIGNATURE
    # --------------------------------------------------------

    if (
        saved["signature"] != signature
        or saved["total_chunks"] != total_chunks
    ):

        raise RuntimeError(
            "Checkpoint configuration does not match "
            "this extraction. Run with "
            "reset_existing_data=true to restart."
        )


    # --------------------------------------------------------
    # ALREADY COMPLETED
    # --------------------------------------------------------

    if saved["status"] == "complete":

        print(
            "Extraction already completed. "
            f"Rows imported: {max(saved['next_row'] - 2, 0)}. "
            "Set reset_existing_data=true for a fresh run."
        )

        return (
            saved["next_chunk"],
            saved["next_row"],
            "complete"
        )


    # --------------------------------------------------------
    # RESUME
    # --------------------------------------------------------

    existing_ids = target_ws.col_values(1)


    # Recover the actual next output row from the sheet.
    actual_next_row = max(
        len(existing_ids) + 1,
        2
    )


    next_row = max(
        saved["next_row"],
        actual_next_row
    )


    target_ws.update(
        [OUTPUT_COLUMNS],
        "A1",
        raw=True
    )

    target_ws.freeze(rows=1)


    print(
        f"Resuming extraction at chunk "
        f"{saved['next_chunk'] + 1}/{total_chunks}."
    )

    print(
        f"Next Google Sheet row: {next_row}"
    )


    return (
        saved["next_chunk"],
        next_row,
        "running"
    )


# ============================================================
# 17. SORT FINAL DATA
# ============================================================

def sort_final_data(
    spreadsheet,
    target_ws,
    last_data_row: int
):

    if last_data_row < 2:
        return


    request_body = {

        "requests": [

            {

                "sortRange": {

                    "range": {

                        "sheetId": target_ws.id,

                        "startRowIndex": 1,

                        "endRowIndex": last_data_row,

                        "startColumnIndex": 0,

                        "endColumnIndex": len(OUTPUT_COLUMNS)

                    },

                    "sortSpecs": [

                        {

                            "dimensionIndex": 1,

                            "sortOrder": "ASCENDING"

                        }

                    ]

                }

            }

        ]

    }


    spreadsheet.batch_update(request_body)

    print(
        "Final Google Sheet sorted by lead creation timestamp."
    )


# ============================================================
# 18. MAIN EXTRACTION
# ============================================================

def main():

    print("========================================")
    print("METABASE TO GOOGLE SHEETS")
    print(f"Question ID: {QUESTION_ID}")
    print(f"Cohort: {COHORT_START} to {COHORT_END}")
    print(f"Chunk days: {CHUNK_DAYS}")
    print("========================================")


    required_env("METABASE_API_KEY")

    required_env("GOOGLE_SERVICE_ACCOUNT_JSON")

    required_env("GOOGLE_SPREADSHEET_ID")


    windows = build_windows()


    if not windows:

        raise RuntimeError(
            "No date windows generated."
        )


    print(
        f"Total chunks to process: {len(windows)}"
    )


    # --------------------------------------------------------
    # BUILD JOB SIGNATURE
    # --------------------------------------------------------

    signature_payload = {

        "question_id": QUESTION_ID,

        "cohort_start": COHORT_START,

        "cohort_end": COHORT_END,

        "chunk_days": CHUNK_DAYS,

        "columns": OUTPUT_COLUMNS

    }


    signature = hashlib.sha256(

        json.dumps(
            signature_payload,
            sort_keys=True
        ).encode("utf-8")

    ).hexdigest()


    # --------------------------------------------------------
    # GOOGLE SHEETS CONNECTION
    # --------------------------------------------------------

    spreadsheet, target_ws, state_ws = setup_sheets()


    # --------------------------------------------------------
    # INITIALIZE OR RESUME
    # --------------------------------------------------------

    next_chunk, next_row, status = initialize_or_resume(

        target_ws,

        state_ws,

        signature,

        len(windows)

    )


    if status == "complete":
        return


    # --------------------------------------------------------
    # LOAD EXISTING LEAD IDS
    # --------------------------------------------------------

    existing_ids = target_ws.col_values(1)


    seen_ids = {

        str(value).strip()

        for value in existing_ids[1:]

        if str(value).strip()

    }


    print(
        f"Previously imported unique Lead IDs: "
        f"{len(seen_ids)}"
    )


    # --------------------------------------------------------
    # METABASE SESSION
    # --------------------------------------------------------

    session = get_metabase_session()


    # IMPORTANT:
    # Retrieve actual template-tag IDs ONCE.
    # These IDs are included in every date request.

    tags = get_template_tags(session)


    # --------------------------------------------------------
    # PROCESS DATE CHUNKS
    # --------------------------------------------------------

    while next_chunk < len(windows):

        chunk_start, chunk_end = windows[next_chunk]


        print(
            f"\n========================================"
        )

        print(
            f"Processing chunk "
            f"{next_chunk + 1}/{len(windows)}"
        )

        print(
            f"Date window: {chunk_start} to {chunk_end}"
        )

        print(
            f"========================================"
        )


        # STEP 1: FETCH CSV

        csv_rows = fetch_chunk_csv(

            session,

            tags,

            chunk_start,

            chunk_end

        )


        # STEP 2: MAP 12 COLUMNS

        mapped_rows = map_csv_rows(csv_rows)


        # STEP 3: REMOVE DUPLICATE LEAD IDS

        fresh_rows = []


        for row in mapped_rows:

            lead_id = str(row[0]).strip()

            if not lead_id:
                continue


            if lead_id in seen_ids:
                continue


            seen_ids.add(lead_id)

            fresh_rows.append(row)


        # STEP 4: WRITE TO GOOGLE SHEETS

        if fresh_rows:

            required_last_row = (
                next_row + len(fresh_rows) - 1
            )


            if target_ws.row_count < required_last_row:

                target_ws.add_rows(
                    required_last_row - target_ws.row_count
                )


            target_ws.update(

                fresh_rows,

                f"A{next_row}",

                raw=True

            )


            next_row += len(fresh_rows)


        # STEP 5: SAVE CHECKPOINT

        next_chunk += 1


        save_state(

            state_ws,

            signature,

            next_chunk,

            next_row,

            len(windows),

            "running"

        )


        print(
            f"Chunk completed: {chunk_start} to {chunk_end}"
        )

        print(
            f"CSV rows received: {len(mapped_rows)}"
        )

        print(
            f"New unique rows imported: {len(fresh_rows)}"
        )

        print(
            f"Total unique Lead IDs: {len(seen_ids)}"
        )


    # --------------------------------------------------------
    # SORT OUTPUT
    # --------------------------------------------------------

    last_data_row = next_row - 1


    sort_final_data(

        spreadsheet,

        target_ws,

        last_data_row

    )


    # --------------------------------------------------------
    # MARK COMPLETE
    # --------------------------------------------------------

    save_state(

        state_ws,

        signature,

        next_chunk,

        next_row,

        len(windows),

        "complete"

    )


    print(
        "\n========================================"
    )

    print(
        "EXTRACTION COMPLETED SUCCESSFULLY"
    )

    print(
        f"Question ID: {QUESTION_ID}"
    )

    print(
        f"Total chunks: {len(windows)}"
    )

    print(
        f"Total unique Lead IDs: {next_row - 2}"
    )

    print(
        f"Google Sheet tab: {SHEET_NAME}"
    )

    print(
        "========================================"
    )


# ============================================================
# 19. EXECUTION
# ============================================================

if __name__ == "__main__":

    try:

        main()

    except Exception as exc:

        print(
            f"\nEXTRACTION FAILED: {exc}",
            file=sys.stderr
        )

        raise

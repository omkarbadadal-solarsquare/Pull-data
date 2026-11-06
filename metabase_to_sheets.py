
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
# CONFIGURATION
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


# ============================================================
# REQUIRED OUTPUT COLUMNS
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


# ============================================================
# VALIDATE REQUIRED SECRETS
# ============================================================

def required_env(name: str) -> str:
    value = os.getenv(name, "").strip()

    if not value:
        raise RuntimeError(
            f"Missing required GitHub secret/environment variable: {name}"
        )

    return value


# ============================================================
# CREATE DATE WINDOWS
# ============================================================

def build_windows() -> list[tuple[str, str]]:

    start = date.fromisoformat(COHORT_START)
    end = date.fromisoformat(COHORT_END)

    if end <= start:
        raise ValueError(
            "COHORT_END must be later than COHORT_START."
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

    # Newest dates first.
    # This retains the latest occurrence when one business
    # lead_id appears in multiple date windows.

    windows.reverse()

    return windows


# ============================================================
# NORMALIZE CSV HEADERS
# ============================================================

def normalize_header(value: Any) -> str:

    return "".join(
        ch
        for ch in str(value)
        .replace("\ufeff", "")
        .strip()
        .lower()
        if ch.isalnum()
    )


# ============================================================
# METABASE SESSION
# ============================================================

def get_metabase_session() -> requests.Session:

    api_key = required_env("METABASE_API_KEY")

    session = requests.Session()

    session.headers.update({
        "X-API-Key": api_key,
        "Accept": "text/csv,*/*",
    })

    return session


# ============================================================
# FETCH METABASE CSV
# ============================================================

def fetch_chunk_csv(
    session: requests.Session,
    chunk_start: str,
    chunk_end: str
) -> list[list[str]]:

    endpoint = (
        f"{METABASE_URL}/api/card/{QUESTION_ID}/query/csv"
    )

    # The SQL question uses two basic Text variables.
    # Pass them directly by template-tag name.
    # No card metadata lookup or template-tag ID lookup needed.

    parameters = [
        {
            "type": "category",
            "target": [
                "variable",
                ["template-tag", "chunk_start"]
            ],
            "value": chunk_start
        },
        {
            "type": "category",
            "target": [
                "variable",
                ["template-tag", "chunk_end"]
            ],
            "value": chunk_end
        }
    ]

    last_error = ""

    for attempt in range(1, REQUEST_RETRIES + 2):

        try:

            response = session.post(
                endpoint,
                data={
                    "parameters": json.dumps(parameters)
                },
                timeout=(20, 180)
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
                f"Metabase request failed for "
                f"{chunk_start} to {chunk_end}: {last_error}"
            ) from exc


        code = response.status_code

        if code == 200:

            body = response.text.lstrip("\ufeff")

            if body.lstrip().lower().startswith(
                ("<html", "<!doctype html")
            ):

                raise RuntimeError(
                    "Metabase returned HTML instead of CSV. "
                    "Check API authentication, endpoint access, "
                    "and company VPN/IP restrictions."
                )

            print(
                f"Successfully fetched "
                f"{chunk_start} to {chunk_end}"
            )

            return list(
                csv.reader(io.StringIO(body))
            )


        last_error = (
            f"HTTP {code}: {response.text[:1500]}"
        )

        print(
            f"Metabase HTTP {code} for "
            f"{chunk_start} to {chunk_end}"
        )

        if code in (400, 401, 403, 404):
            break

        if attempt <= REQUEST_RETRIES:

            wait_seconds = min(
                5 * (2 ** (attempt - 1)),
                30
            )

            print(
                f"Retrying in {wait_seconds} seconds."
            )

            time.sleep(wait_seconds)


    extra = ""

    if "524" in last_error:

        extra = (
            "\nMetabase proxy timed out. "
            "CHUNK_DAYS is currently "
            f"{CHUNK_DAYS}. If a one-day query still "
            "returns 524, the SQL/database query must be "
            "optimized. GitHub cannot bypass a Metabase-side "
            "proxy timeout."
        )

    raise RuntimeError(
        f"Metabase extraction failed for "
        f"{chunk_start} to {chunk_end}.\n"
        f"{last_error}{extra}"
    )


# ============================================================
# MAP CSV TO 12 OUTPUT COLUMNS
# ============================================================

def map_csv_rows(
    csv_rows: list[list[str]]
) -> list[list[str]]:

    if not csv_rows:
        return []

    header = csv_rows[0]

    normalized = [
        normalize_header(col)
        for col in header
    ]

    indexes = []

    for expected in OUTPUT_COLUMNS:

        key = normalize_header(expected)

        try:

            index = normalized.index(key)

        except ValueError as exc:

            raise RuntimeError(
                f'Metabase CSV is missing column "{expected}". '
                f"Columns returned: {header}"
            ) from exc

        indexes.append(index)


    output = []

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

        output.append(row)

    return output


# ============================================================
# OPEN OR CREATE GOOGLE SHEET TAB
# ============================================================

def open_or_create_worksheet(
    spreadsheet: gspread.Spreadsheet,
    title: str,
    rows: int,
    cols: int
) -> gspread.Worksheet:

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
# READ CHECKPOINT
# ============================================================

def get_state(
    state_ws: gspread.Worksheet
) -> dict[str, Any] | None:

    values = state_ws.row_values(2)

    if not values or not any(
        str(value).strip()
        for value in values
    ):
        return None

    values += [""] * (
        len(STATE_HEADERS) - len(values)
    )

    try:

        return {
            "signature": values[0],
            "next_chunk": int(values[1]),
            "next_row": int(values[2]),
            "total_chunks": int(values[3]),
            "status": values[4]
        }

    except (TypeError, ValueError) as exc:

        raise RuntimeError(
            f"The checkpoint tab {STATE_SHEET_NAME} "
            "has invalid values. Run with "
            "reset_existing_data=true to restart."
        ) from exc


# ============================================================
# SAVE CHECKPOINT
# ============================================================

def save_state(
    state_ws: gspread.Worksheet,
    signature: str,
    next_chunk: int,
    next_row: int,
    total_chunks: int,
    status: str
) -> None:

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
# CONNECT TO GOOGLE SHEETS
# ============================================================

def setup_sheets() -> tuple[
    gspread.Spreadsheet,
    gspread.Worksheet,
    gspread.Worksheet
]:

    service_account_json = required_env(
        "GOOGLE_SERVICE_ACCOUNT_JSON"
    )

    service_account_info = json.loads(
        service_account_json
    )

    client = gspread.service_account_from_dict(
        service_account_info,
        scopes=GOOGLE_SCOPES
    )

    spreadsheet = client.open_by_key(
        required_env("GOOGLE_SPREADSHEET_ID")
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

    # Keep checkpoint tab hidden.
    try:
        state_ws.hide()
    except Exception as exc:
        print(
            f"Could not hide checkpoint tab: {exc}"
        )

    return spreadsheet, target_ws, state_ws


# ============================================================
# INITIALIZE OR RESUME EXTRACTION
# ============================================================

def initialize_or_resume(
    target_ws: gspread.Worksheet,
    state_ws: gspread.Worksheet,
    signature: str,
    total_chunks: int
) -> tuple[int, int, str]:

    saved = get_state(state_ws)


    # FULL RESET

    if RESET_IMPORT:

        print(
            "RESET_IMPORT=true. Clearing destination "
            "tab and checkpoint."
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


    # FIRST RUN WITHOUT CHECKPOINT

    if saved is None:

        existing_column_a = target_ws.col_values(1)

        if existing_column_a:

            raise RuntimeError(
                f"{SHEET_NAME} already contains data but "
                "no matching checkpoint exists. Run the "
                "workflow with reset_existing_data=true "
                "to start a fresh extraction."
            )

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


    # VALIDATE EXISTING CHECKPOINT

    if (
        saved["signature"] != signature
        or saved["total_chunks"] != total_chunks
    ):

        raise RuntimeError(
            "Saved checkpoint belongs to a different "
            "extraction configuration. Run with "
            "reset_existing_data=true to restart."
        )


    # ALREADY COMPLETED

    if saved["status"] == "complete":

        print(
            f"Question {QUESTION_ID} is already complete. "
            f"Rows imported: {max(saved['next_row'] - 2, 0)}. "
            "Use reset_existing_data=true for a fresh import."
        )

        return (
            saved["next_chunk"],
            saved["next_row"],
            "complete"
        )


    # RECOVER WRITTEN ROWS IF A PREVIOUS RUN STOPPED

    existing_ids = target_ws.col_values(1)

    actual_next_row = (
        len(existing_ids) + 1
        if existing_ids
        else 2
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
        f"Resuming at chunk "
        f"{saved['next_chunk'] + 1}/{total_chunks}. "
        f"Next output row: {next_row}."
    )

    return (
        saved["next_chunk"],
        next_row,
        "running"
    )


# ============================================================
# MAIN EXTRACTION
# ============================================================

def main() -> None:

    required_env("METABASE_API_KEY")

    required_env("GOOGLE_SERVICE_ACCOUNT_JSON")

    required_env("GOOGLE_SPREADSHEET_ID")


    windows = build_windows()

    if not windows:

        raise RuntimeError(
            "No date windows were generated."
        )


    # Create a signature for this extraction configuration.

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


    # Connect Google Sheets

    _, target_ws, state_ws = setup_sheets()


    # Initialize or resume

    next_chunk, next_row, status = initialize_or_resume(
        target_ws,
        state_ws,
        signature,
        len(windows)
    )


    if status == "complete":
        return


    # Load existing Lead IDs to prevent duplicates.

    existing_ids = target_ws.col_values(1)

    seen_ids = {
        str(value).strip()
        for value in existing_ids[1:]
        if str(value).strip()
    }


    session = get_metabase_session()


    # ========================================================
    # PROCESS DATE CHUNKS
    # ========================================================

    while next_chunk < len(windows):

        chunk_start, chunk_end = windows[next_chunk]

        print(
            f"\nFetching chunk "
            f"{next_chunk + 1}/{len(windows)}: "
            f"{chunk_start} to {chunk_end} "
            "(end exclusive)"
        )


        # STEP 1: FETCH CSV

        csv_rows = fetch_chunk_csv(
            session,
            chunk_start,
            chunk_end
        )


        # STEP 2: MAP REQUIRED COLUMNS

        mapped_rows = map_csv_rows(csv_rows)


        # STEP 3: REMOVE DUPLICATE LEAD IDs

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
            f"Chunk completed.\n"
            f"CSV rows: {len(mapped_rows)}\n"
            f"New unique rows: {len(fresh_rows)}\n"
            f"Total unique Lead IDs: {len(seen_ids)}"
        )


    # ========================================================
    # SORT FINAL OUTPUT BY LEAD CREATION TIMESTAMP
    # ========================================================

    last_data_row = next_row - 1

    if last_data_row >= 2:

        target_ws.sort(
            (2, "asc"),
            range=f"A2:L{last_data_row}"
        )


    # ========================================================
    # MARK EXTRACTION COMPLETE
    # ========================================================

    save_state(
        state_ws,
        signature,
        next_chunk,
        next_row,
        len(windows),
        "complete"
    )


    print(
        "\n======================================"
        "\nEXTRACTION COMPLETED SUCCESSFULLY"
        f"\nQuestion ID: {QUESTION_ID}"
        f"\nTotal chunks: {len(windows)}"
        f"\nUnique leads imported: {next_row - 2}"
        f"\nDestination: {SHEET_NAME}"
        "\n======================================"
    )


# ============================================================
# EXECUTION ENTRY POINT
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

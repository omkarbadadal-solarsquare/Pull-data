import csv
import io
import json
import os
import sys
import time
from datetime import date, timedelta

import gspread
import requests
from google.oauth2.service_account import Credentials
from gspread.exceptions import APIError

# ---------- CONFIG (from GitHub secrets / env) ----------
METABASE_URL = os.environ.get("METABASE_URL", "https://metabase-lighthouse.solarsquare.in").rstrip("/")
QUESTION_ID = os.environ.get("QUESTION_ID", "6358")
METABASE_API_KEY = os.environ["METABASE_API_KEY"]

SPREADSHEET_ID = os.environ["SPREADSHEET_ID"]
SHEET_NAME = os.environ.get("SHEET_NAME", "SIQ_Sept")
GCP_SA_JSON = os.environ["GCP_SA_JSON"]

START_DATE = os.environ.get("START_DATE", "2026-09-01")
END_DATE = os.environ.get("END_DATE", "2026-10-01")   # exclusive
CHUNK_DAYS = int(os.environ.get("CHUNK_DAYS", "3"))

BATCH_ROWS = int(os.environ.get("BATCH_ROWS", "5000"))
MAX_CELL_CHARS = 49000  # Google Sheets limit is 50,000 characters per cell
VALUE_INPUT_OPTION = os.environ.get("VALUE_INPUT_OPTION", "USER_ENTERED")
MAX_FETCH_ATTEMPTS = 3

HEADERS = {"X-API-Key": METABASE_API_KEY}


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


# ---------- Metabase ----------
def find_template_tags(obj):
    """Finds the saved question's variables as {name: tag}. Handles dict or list formats."""
    if isinstance(obj, dict):
        direct = obj.get("template-tags") or obj.get("template_tags")
        if direct:
            by_name = {}
            if isinstance(direct, list):
                for t in direct:
                    if isinstance(t, dict) and t.get("name"):
                        by_name[t["name"]] = t
            elif isinstance(direct, dict):
                for k, t in direct.items():
                    by_name[(t or {}).get("name", k)] = t
            if by_name:
                return by_name
        for v in obj.values():
            found = find_template_tags(v)
            if found:
                return found
    elif isinstance(obj, list):
        for v in obj:
            found = find_template_tags(v)
            if found:
                return found
    return None


def get_param_ids():
    resp = requests.get(f"{METABASE_URL}/api/card/{QUESTION_ID}", headers=HEADERS, timeout=60)
    if resp.status_code != 200:
        raise RuntimeError(f"Could not read question {QUESTION_ID}: {resp.status_code} {resp.text[:500]}")

    card = resp.json()
    tags = find_template_tags(card.get("dataset_query") or card) or {}
    if "start_date" not in tags or "end_date" not in tags:
        raise RuntimeError(
            f"Question {QUESTION_ID} must have variables named exactly start_date and end_date. "
            f"Variables found: {', '.join(tags) or 'none'}. Save the question after adding them."
        )
    return tags["start_date"].get("id", "start_date"), tags["end_date"].get("id", "end_date")


def build_chunks():
    chunks = []
    cursor = date.fromisoformat(START_DATE)
    final = date.fromisoformat(END_DATE)
    while cursor < final:
        end = min(cursor + timedelta(days=CHUNK_DAYS), final)
        chunks.append((cursor.isoformat(), end.isoformat()))
        cursor = end
    return chunks


def fetch_chunk(start, end, start_id, end_id):
    parameters = [
        {"id": start_id, "type": "date/single",
         "target": ["variable", ["template-tag", "start_date"]], "value": start},
        {"id": end_id, "type": "date/single",
         "target": ["variable", ["template-tag", "end_date"]], "value": end},
    ]
    url = f"{METABASE_URL}/api/card/{QUESTION_ID}/query/csv"
    last_error = ""

    for attempt in range(1, MAX_FETCH_ATTEMPTS + 1):
        try:
            resp = requests.post(
                url,
                headers=HEADERS,
                data={"parameters": json.dumps(parameters)},
                timeout=(30, 900),
            )
        except requests.RequestException as exc:
            last_error = str(exc)
            log(f"Chunk {start}: request failed ({last_error}), attempt {attempt}")
            time.sleep(20 * attempt)
            continue

        if resp.status_code == 200:
            resp.encoding = "utf-8"
            rows = list(csv.reader(io.StringIO(resp.text)))
            log(f"Chunk {start} to {end}: {max(len(rows) - 1, 0)} rows, {len(resp.content):,} bytes")
            return rows

        last_error = f"{resp.status_code}: {resp.text[:500]}"
        log(f"Chunk {start}: Metabase returned {last_error} (attempt {attempt})")
        if 400 <= resp.status_code < 500:
            break  # a request problem, retrying won't help
        time.sleep(20 * attempt)

    raise RuntimeError(f"Chunk {start} to {end} failed: {last_error}")


# ---------- Google Sheets ----------
def with_retry(fn, *args, **kwargs):
    for attempt in range(1, 6):
        try:
            return fn(*args, **kwargs)
        except APIError as exc:
            status = getattr(exc.response, "status_code", 0)
            if status in (429, 500, 502, 503) and attempt < 5:
                wait = 15 * attempt
                log(f"Sheets API {status}, retrying in {wait}s...")
                time.sleep(wait)
                continue
            raise


def write_to_sheet(rows):
    num_cols = len(rows[0])
    num_rows = len(rows)
    cells = num_rows * num_cols
    log(f"Writing {num_rows - 1} rows x {num_cols} columns = {cells:,} cells")
    if cells > 8_000_000:
        log("WARNING: close to the 10 million cell limit of a Google spreadsheet")

    creds = Credentials.from_service_account_info(
        json.loads(GCP_SA_JSON),
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    client = gspread.authorize(creds)
    spreadsheet = client.open_by_key(SPREADSHEET_ID)

    try:
        ws = spreadsheet.worksheet(SHEET_NAME)
    except gspread.WorksheetNotFound:
        ws = spreadsheet.add_worksheet(title=SHEET_NAME, rows=num_rows, cols=num_cols)

    with_retry(ws.clear)
    with_retry(ws.resize, rows=num_rows, cols=num_cols)

    def clean(row):
        return [c if len(c) <= MAX_CELL_CHARS else c[:MAX_CELL_CHARS] for c in row]

    for start in range(0, num_rows, BATCH_ROWS):
        batch = [clean(r) for r in rows[start:start + BATCH_ROWS]]
        top_left = gspread.utils.rowcol_to_a1(start + 1, 1)
        bottom_right = gspread.utils.rowcol_to_a1(start + len(batch), num_cols)
        with_retry(
            ws.update,
            range_name=f"{top_left}:{bottom_right}",
            values=batch,
            value_input_option=VALUE_INPUT_OPTION,
        )
        log(f"Written rows {start + 1} to {start + len(batch)}")


def main():
    start_id, end_id = get_param_ids()
    chunks = build_chunks()
    log(f"{len(chunks)} chunks from {START_DATE} to {END_DATE} ({CHUNK_DAYS} days each)")

    header = None
    all_rows = []

    for start, end in chunks:
        rows = fetch_chunk(start, end, start_id, end_id)
        if len(rows) <= 1:
            continue
        if header is None:
            header = rows[0]
        elif len(rows[0]) != len(header):
            raise RuntimeError(f"Chunk {start} has {len(rows[0])} columns, expected {len(header)}")
        all_rows.extend(rows[1:])

    if header is None or not all_rows:
        raise RuntimeError("No data rows in any chunk. Leaving the sheet unchanged.")

    write_to_sheet([header] + all_rows)
    log(f"Done. {len(all_rows)} rows imported.")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        log(f"ERROR: {exc}")
        sys.exit(1)

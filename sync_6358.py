import csv
import json
import os
import sys
import time

import gspread
import requests
from google.oauth2.service_account import Credentials
from gspread.exceptions import APIError

# ---------- CONFIG (from GitHub secrets / env) ----------
METABASE_URL = os.environ.get("METABASE_URL", "https://metabase-lighthouse.solarsquare.in").rstrip("/")
QUESTION_ID = os.environ.get("QUESTION_ID", "6359")
METABASE_API_KEY = os.environ["METABASE_API_KEY"]

SPREADSHEET_ID = os.environ["SPREADSHEET_ID"]
SHEET_NAME = os.environ.get("SHEET_NAME", "SIQ_Sept")
GCP_SA_JSON = os.environ["GCP_SA_JSON"]

BATCH_ROWS = int(os.environ.get("BATCH_ROWS", "5000"))
MAX_CELL_CHARS = 49000  # Google Sheets limit is 50,000 characters per cell
# USER_ENTERED lets Sheets parse numbers and dates; RAW keeps everything as text
VALUE_INPUT_OPTION = os.environ.get("VALUE_INPUT_OPTION", "USER_ENTERED")
MAX_FETCH_ATTEMPTS = 3


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def fetch_csv_rows():
    """Calls the saved question and returns all CSV rows (header first)."""
    url = f"{METABASE_URL}/api/card/{QUESTION_ID}/query/csv"
    last_error = ""

    for attempt in range(1, MAX_FETCH_ATTEMPTS + 1):
        log(f"Fetching question {QUESTION_ID} (attempt {attempt})...")
        try:
            resp = requests.post(
                url,
                headers={"X-API-Key": METABASE_API_KEY},
                stream=True,
                timeout=(30, 900),
            )
        except requests.RequestException as exc:
            last_error = str(exc)
            log(f"Request failed: {last_error}")
            time.sleep(20 * attempt)
            continue

        if resp.status_code == 200:
            resp.encoding = "utf-8"
            rows = list(csv.reader(resp.iter_lines(decode_unicode=True)))
            log(f"Fetched {max(len(rows) - 1, 0)} data rows")
            return rows

        last_error = f"{resp.status_code}: {resp.text[:600]}"
        log(f"Metabase returned {last_error}")

        # 4xx means the request itself is wrong, so retrying will not help
        if 400 <= resp.status_code < 500:
            break
        time.sleep(20 * attempt)

    raise RuntimeError(f"Metabase fetch failed: {last_error}")


def with_retry(fn, *args, **kwargs):
    """Retries Google Sheets calls on rate limits and transient errors."""
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
    header, data = rows[0], rows[1:]
    num_cols = len(header)
    num_rows = len(rows)

    cells = num_rows * num_cols
    log(f"Writing {len(data)} rows x {num_cols} columns = {cells:,} cells")
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

    # Trim any cell that is over the Sheets character limit
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
    rows = fetch_csv_rows()
    if len(rows) <= 1:
        raise RuntimeError("Query returned a header only, no data rows. Leaving the sheet unchanged.")
    write_to_sheet(rows)
    log("Done.")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        log(f"ERROR: {exc}")
        sys.exit(1)

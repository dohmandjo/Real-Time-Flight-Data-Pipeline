"""
===============================================================================
Project: Real-Time Flight Data Pipeline
Component: Google Sheets Sink (Incremental Exporter)

Purpose / What it does
----------------------
- Reads *new or changed* rows from the warehouse view `vw_flights_for_sheet_fact`
  using a high-watermark (`gsheet_sync_state.last_sync_time`).
- Appends those rows to a specific Google Sheet tab, keeping headers intact.
- Ensures two delay columns (dep_delay_min, arr_delay_min) are *numeric* in Sheets,
  so they aggregate properly and flow cleanly into Tableau.
- Applies basic sheet formatting: header row, freeze top row, autosize, number format.

Why
---
- Google Sheets is the data bridge to Tableau Public for this project.
- Incremental write avoids re-sending the whole dataset and keeps API usage small.
- Explicit numeric formatting prevents text-number pitfalls in analytics tools.

How (high level)
----------------
1) Bootstrap Google Sheets client (service account), ensure the tab exists.
2) Ensure header row is present and formatted; set numeric format on delay columns.
3) Loop:
   - Fetch a batch of rows newer than the last sync time.
   - Convert Python values → Sheets cells (keep delays as float, timestamps ISO8601).
   - Append below header (A2).
   - Advance high-watermark to the newest `last_updated` sent.
   - Sleep briefly, repeat.

Operational Notes
-----------------
- Requires a Postgres table `gsheet_sync_state` with a single row id=1, storing
  `last_sync_time` (TIMESTAMPTZ). The view `vw_flights_for_sheet_fact` must include
  the exact column names listed in COLUMNS (last one is `last_updated`).
- Uses `valueInputOption="RAW"` so numeric floats remain numbers in Sheets.
- Idempotence: The high-watermark guarantees each row is exported once.
===============================================================================
"""

import os, sys, time, shutil
from datetime import datetime, timezone
from decimal import Decimal  # numeric safety for delay columns

import psycopg
from google.oauth2 import service_account
from googleapiclient.discovery import build

# ---------- Config ----------
# Postgres DSN pulled from environment (Compose passes these in).
# `connect_timeout` can be added on demand via environment if needed.
PG_DSN = (
    f"host={os.getenv('PGHOST','flight_postgres')} "
    f"port={os.getenv('PGPORT','5432')} "
    f"dbname={os.getenv('PGDATABASE','flight_pipeline')} "
    f"user={os.getenv('PGUSER','flight_user')} "
    f"password={os.getenv('PGPASSWORD','Password123')}"
)

# Google API credentials (Service Account JSON) and Sheet info
CRED_SRC  = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "/secrets/gsa-keys.json")
SHEET_ID  = os.getenv("GSHEET_ID")
TAB       = (os.getenv("GSHEET_TAB", "flights_curated") or "flights_curated").strip()

# Batching & loop interval: small batches reduce API payloads and help with quotas
BATCH_SIZE = int(os.getenv("GSHEET_BATCH_SIZE", "300"))
INTERVAL   = int(os.getenv("GSHEET_INTERVAL_SEC", "30"))
SCOPES     = ["https://www.googleapis.com/auth/spreadsheets"]

# Column order expected by the view; also becomes the Sheet header row.
# NOTE: The last column must be `last_updated` (used to advance the watermark).
COLUMNS = [
    "flight_key","flight_date","status","airline_iata","airline_name",
    "dep_scheduled","dep_estimated","dep_actual","dep_delay_min",
    "arr_scheduled","arr_estimated","arr_actual","arr_delay_min",
    "dep_airport","dep_iata","dep_icao","arr_airport","arr_iata","arr_icao",
    "last_updated"
]

# Indices of numeric columns to preserve as numbers in Sheets
NUMERIC_COLS_IDX = [
    COLUMNS.index("dep_delay_min"),   # 0-based index for dep_delay_min
    COLUMNS.index("arr_delay_min"),   # 0-based index for arr_delay_min
]

# Query pulls only rows newer than the last exported timestamp (watermark).
SELECT_SQL = f"""
WITH last AS (
  SELECT COALESCE(last_sync_time, 'epoch'::timestamptz) AS t
  FROM gsheet_sync_state WHERE id=1
)
SELECT {", ".join(COLUMNS)}
FROM vw_flights_for_sheet_fact, last
WHERE last_updated > last.t
ORDER BY last_updated
LIMIT %s;
"""

# Bump the high-watermark to the max(last_updated) we just pushed.
UPDATE_STATE_SQL = """
UPDATE gsheet_sync_state
SET last_sync_time = GREATEST(last_sync_time, %s)
WHERE id=1;
"""

# ---------- Helpers ----------
def stage_creds(src_path: str, retries: int = 6, backoff: float = 0.2) -> str:
    """
    Copy the SA JSON to a temp location inside the container (helps with some host
    volume oddities), and set safe permissions.
    """
    staged = "/tmp/gsa-keys.json"
    last_err = None
    for _ in range(retries):
        try:
            shutil.copyfile(src_path, staged)
            os.chmod(staged, 0o600)
            # sanity read to catch transient mount issues
            with open(staged, "rb") as f:
                _ = f.read(64)
            return staged
        except OSError as e:
            last_err = e
            time.sleep(backoff)
            backoff = min(backoff * 2, 2.0)
    raise last_err

def a1_quote_tab(title: str) -> str:
    """Quote a sheet/tab title for A1 ranges (escape single quotes)."""
    return "'" + title.replace("'", "''") + "'"

def get_sheet_id(svc, spreadsheet_id: str, tab_title: str) -> int:
    """
    Look up the tab by title; if missing, create it and return its sheetId.
    """
    meta = svc.spreadsheets().get(
        spreadsheetId=spreadsheet_id,
        fields="sheets.properties(sheetId,title)"
    ).execute()
    for s in meta.get("sheets", []):
        if s["properties"]["title"] == tab_title:
            return s["properties"]["sheetId"]
    resp = svc.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body={"requests": [{"addSheet": {"properties": {"title": tab_title}}}]}
    ).execute()
    sheet_id = resp["replies"][0]["addSheet"]["properties"]["sheetId"]
    print(f"[Sheets] Created missing tab: {tab_title}", flush=True)
    return sheet_id

def ensure_header_and_format(svc, sheet_id: int):
    """
    - If header is missing/mismatched, write the header row to A1.
    - Freeze the first row, bold the header, auto-size columns.
    """
    rng_tab = a1_quote_tab(TAB)

    # Check A1 to detect whether header is already present
    res = svc.spreadsheets().values().get(
        spreadsheetId=SHEET_ID, range=f"{rng_tab}!A1"
    ).execute()
    a1 = (res.get("values") or [[]])
    a1_val = a1[0][0] if a1 and a1[0] else ""

    # If A1 != first column name, lay down the header row
    if a1_val != COLUMNS[0]:
        svc.spreadsheets().values().update(
            spreadsheetId=SHEET_ID,
            range=f"{rng_tab}!A1",
            valueInputOption="RAW",
            body={"values":[COLUMNS]}
        ).execute()
        print("[Sheets] Wrote header row.", flush=True)

    # Formatting: freeze row 1, bold header, auto-size columns
    svc.spreadsheets().batchUpdate(
        spreadsheetId=SHEET_ID,
        body={"requests":[
            {"updateSheetProperties":{
                "properties":{"sheetId": sheet_id, "gridProperties":{"frozenRowCount":1}},
                "fields":"gridProperties.frozenRowCount"
            }},
            {"repeatCell":{
                "range":{"sheetId": sheet_id, "startRowIndex":0, "endRowIndex":1},
                "cell":{"userEnteredFormat":{"textFormat":{"bold":True}}},
                "fields":"userEnteredFormat.textFormat.bold"
            }},
            {"autoResizeDimensions":{
                "dimensions":{"sheetId": sheet_id, "dimension":"COLUMNS"}
            }}
        ]}
    ).execute()

def set_numeric_format_for_delay_cols(svc, sheet_id: int):
    """
    Apply numeric formatting to the delay columns for all data rows (row >= 2).
    This ensures Sheets stores and displays them as numbers (e.g., 0.##).
    """
    requests = []
    for col_idx in NUMERIC_COLS_IDX:
        requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": 1,           # skip header
                    "startColumnIndex": col_idx,  # 0-based inclusive
                    "endColumnIndex": col_idx + 1 # 0-based exclusive
                },
                "cell": {
                    "userEnteredFormat": {
                        "numberFormat": {"type": "NUMBER", "pattern": "0.##"},
                        "horizontalAlignment": "RIGHT"
                    }
                },
                "fields": "userEnteredFormat.numberFormat,userEnteredFormat.horizontalAlignment"
            }
        })
    if requests:
        svc.spreadsheets().batchUpdate(
            spreadsheetId=SHEET_ID,
            body={"requests": requests}
        ).execute()

def get_sheets():
    """
    Initialize Sheets API client with service account credentials.
    Guardrails: verify env vars and that the credentials file exists.
    """
    if not SHEET_ID:
        print("[Sheets] GSHEET_ID is not set. Exiting.", flush=True)
        sys.exit(1)
    if not TAB:
        print("[Sheets] GSHEET_TAB is empty after stripping. Set GSHEET_TAB.", flush=True)
        sys.exit(1)
    if not os.path.isfile(CRED_SRC):
        print(f"[Sheets] Credential file not found at {CRED_SRC}. /secrets listing:", flush=True)
        os.system("ls -l /secrets || true")
        sys.exit(1)

    cred_path = stage_creds(CRED_SRC)
    print(f"[Sheets] Using staged credentials: {cred_path}", flush=True)
    creds = service_account.Credentials.from_service_account_file(cred_path, scopes=SCOPES)
    return build("sheets", "v4", credentials=creds, cache_discovery=False)

def fetch_batch(conn, limit: int):
    """
    Pull at most `limit` rows newer than the last sync time from the view.
    """
    with conn.cursor() as cur:
        cur.execute(SELECT_SQL, (limit,))
        return cur.fetchall()

def append_to_sheet(svc, values):
    """
    Append rows *below* the header (starting at A2). Using RAW keeps floats numeric.
    """
    rng = f"{a1_quote_tab(TAB)}!A2"
    body = {"values": values}
    svc.spreadsheets().values().append(
        spreadsheetId=SHEET_ID,
        range=rng,
        valueInputOption="RAW",          # keep numbers as numbers (no stringification)
        insertDataOption="INSERT_ROWS",
        body=body
    ).execute()

def to_values(rows):
    """
    Convert DB rows → Sheets rows, preserving:
    - Numeric types for delay columns.
    - ISO8601 UTC strings for datetimes (so Tableau/Sheets parse consistently).
    Everything else becomes string (or empty string for None).
    """
    out = []
    for r in rows:
        vals = []
        for i, v in enumerate(r):
            if v is None:
                vals.append("")
            elif i in NUMERIC_COLS_IDX:
                # keep delays numeric; coerce Decimals/strings safely
                if isinstance(v, (int, float)):
                    vals.append(float(v))
                elif isinstance(v, Decimal):
                    vals.append(float(v))
                else:
                    try:
                        vals.append(float(v))
                    except Exception:
                        vals.append("")
            elif isinstance(v, datetime):
                vals.append(v.astimezone(timezone.utc).isoformat())
            else:
                vals.append(str(v))
        out.append(vals)
    return out

# ---------- Main ----------
def main():
    print(f"[Sheets] GSHEET_ID={SHEET_ID}, TAB={TAB}, BATCH_SIZE={BATCH_SIZE}, INTERVAL={INTERVAL}", flush=True)

    # 1) Sheets client and basic sheet setup
    svc = get_sheets()
    sheet_id = get_sheet_id(svc, SHEET_ID, TAB)
    ensure_header_and_format(svc, sheet_id)
    set_numeric_format_for_delay_cols(svc, sheet_id)  # enforce numeric display

    # 2) Postgres connection; loop forever pulling and appending new rows
    with psycopg.connect(PG_DSN, autocommit=False) as conn:
        print("[Sheets] Connected to Postgres and state bootstrapped.", flush=True)
        while True:
            rows = fetch_batch(conn, BATCH_SIZE)
            if not rows:
                time.sleep(INTERVAL)
                continue

            values = to_values(rows)

            # Try append; if the Sheets API momentarily fails, wait and retry later
            try:
                append_to_sheet(svc, values)
            except Exception as e:
                print(f"[Sheets] Append failed: {e}", flush=True)
                time.sleep(5)
                continue

            # Advance the watermark using the newest last_updated we appended
            max_ts = max(r[-1] for r in rows)  # last column is last_updated
            with conn.cursor() as cur:
                cur.execute(UPDATE_STATE_SQL, (max_ts,))
            conn.commit()

            # Small pause between batches to avoid hammering APIs
            time.sleep(1)

if __name__ == "__main__":
    main()

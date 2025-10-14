import os, sys, time, shutil
from datetime import datetime, timezone
from decimal import Decimal  # <-- add

import psycopg
from google.oauth2 import service_account
from googleapiclient.discovery import build

# ---------- Config ----------
PG_DSN = (
    f"host={os.getenv('PGHOST','flight_postgres')} "
    f"port={os.getenv('PGPORT','5432')} "
    f"dbname={os.getenv('PGDATABASE','flight_pipeline')} "
    f"user={os.getenv('PGUSER','flight_user')} "
    f"password={os.getenv('PGPASSWORD','Password123')}"
)

CRED_SRC  = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "/secrets/gsa-keys.json")
SHEET_ID  = os.getenv("GSHEET_ID")
TAB       = (os.getenv("GSHEET_TAB", "flights_curated") or "flights_curated").strip()
BATCH_SIZE = int(os.getenv("GSHEET_BATCH_SIZE", "300"))
INTERVAL   = int(os.getenv("GSHEET_INTERVAL_SEC", "30"))
SCOPES     = ["https://www.googleapis.com/auth/spreadsheets"]

COLUMNS = [
    "flight_key","flight_date","status","airline_iata","airline_name",
    "dep_scheduled","dep_estimated","dep_actual","dep_delay_min",
    "arr_scheduled","arr_estimated","arr_actual","arr_delay_min",
    "dep_airport","dep_iata","dep_icao","arr_airport","arr_iata","arr_icao",
    "last_updated"
]

# indices of numeric columns in the result set
NUMERIC_COLS_IDX = [
    COLUMNS.index("dep_delay_min"),   # column 8  (I)
    COLUMNS.index("arr_delay_min"),   # column 12 (M)
]

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

UPDATE_STATE_SQL = """
UPDATE gsheet_sync_state
SET last_sync_time = GREATEST(last_sync_time, %s)
WHERE id=1;
"""

# ---------- Helpers ----------
def stage_creds(src_path: str, retries: int = 6, backoff: float = 0.2) -> str:
    """Copy SA JSON off the bind mount to avoid macOS/docker EDEADLK."""
    staged = "/tmp/gsa-keys.json"
    last_err = None
    for _ in range(retries):
        try:
            shutil.copyfile(src_path, staged)
            os.chmod(staged, 0o600)
            with open(staged, "rb") as f:
                _ = f.read(64)
            return staged
        except OSError as e:
            last_err = e
            time.sleep(backoff)
            backoff = min(backoff * 2, 2.0)
    raise last_err

def a1_quote_tab(title: str) -> str:
    """Quote a sheet/tab title for A1 ranges."""
    return "'" + title.replace("'", "''") + "'"

def get_sheet_id(svc, spreadsheet_id: str, tab_title: str) -> int:
    meta = svc.spreadsheets().get(
        spreadsheetId=spreadsheet_id,
        fields="sheets.properties(sheetId,title)"
    ).execute()
    for s in meta.get("sheets", []):
        if s["properties"]["title"] == tab_title:
            return s["properties"]["sheetId"]
    # create if missing
    resp = svc.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body={"requests": [{"addSheet": {"properties": {"title": tab_title}}}]}
    ).execute()
    sheet_id = resp["replies"][0]["addSheet"]["properties"]["sheetId"]
    print(f"[Sheets] Created missing tab: {tab_title}", flush=True)
    return sheet_id

def ensure_header_and_format(svc, sheet_id: int):
    """Ensure header row exists, freeze/bold it, and auto-size columns."""
    rng_tab = a1_quote_tab(TAB)
    # Check A1 contents
    res = svc.spreadsheets().values().get(
        spreadsheetId=SHEET_ID, range=f"{rng_tab}!A1"
    ).execute()
    a1 = (res.get("values") or [[]])
    a1_val = a1[0][0] if a1 and a1[0] else ""

    if a1_val != COLUMNS[0]:
        # Write header row
        svc.spreadsheets().values().update(
            spreadsheetId=SHEET_ID,
            range=f"{rng_tab}!A1",
            valueInputOption="RAW",
            body={"values":[COLUMNS]}
        ).execute()
        print("[Sheets] Wrote header row.", flush=True)

    # Freeze first row, bold header, auto-size columns
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
    """Force number format (0.##) on dep_delay_min and arr_delay_min columns."""
    requests = []
    for col_idx in NUMERIC_COLS_IDX:
        requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": 1,           # skip header
                    "startColumnIndex": col_idx,  # 0-based
                    "endColumnIndex": col_idx + 1
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
    with conn.cursor() as cur:
        cur.execute(SELECT_SQL, (limit,))
        return cur.fetchall()

def append_to_sheet(svc, values):
    # Append *below* header
    rng = f"{a1_quote_tab(TAB)}!A2"
    body = {"values": values}
    svc.spreadsheets().values().append(
        spreadsheetId=SHEET_ID,
        range=rng,
        valueInputOption="RAW",          # keep numbers as numbers
        insertDataOption="INSERT_ROWS",
        body=body
    ).execute()

def to_values(rows):
    """Build a rows-of-values list with numeric delay columns kept as floats."""
    out = []
    for r in rows:
        vals = []
        for i, v in enumerate(r):
            if v is None:
                vals.append("")
            elif i in NUMERIC_COLS_IDX:
                # keep numbers numeric
                if isinstance(v, (int, float)):
                    vals.append(float(v))
                elif isinstance(v, Decimal):
                    vals.append(float(v))
                else:
                    # try coercion (in case value arrives as string)
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
    svc = get_sheets()
    sheet_id = get_sheet_id(svc, SHEET_ID, TAB)
    ensure_header_and_format(svc, sheet_id)
    set_numeric_format_for_delay_cols(svc, sheet_id)  # <-- ensure numeric formatting

    with psycopg.connect(PG_DSN, autocommit=False) as conn:
        print("[Sheets] Connected to Postgres and state bootstrapped.", flush=True)
        while True:
            rows = fetch_batch(conn, BATCH_SIZE)
            if not rows:
                time.sleep(INTERVAL)
                continue

            values = to_values(rows)
            try:
                append_to_sheet(svc, values)
            except Exception as e:
                print(f"[Sheets] Append failed: {e}", flush=True)
                time.sleep(5)
                continue

            max_ts = max(r[-1] for r in rows)  # last column is last_updated
            with conn.cursor() as cur:
                cur.execute(UPDATE_STATE_SQL, (max_ts,))
            conn.commit()

            time.sleep(1)

if __name__ == "__main__":
    main()

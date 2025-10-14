# apps/loader/load_warehouse.py
import os, time, argparse, psycopg
from datetime import datetime

PG_DSN = (
    f"host={os.getenv('PGHOST','flight_postgres')} "
    f"port={os.getenv('PGPORT','5432')} "
    f"dbname={os.getenv('PGDATABASE','flight_pipeline')} "
    f"user={os.getenv('PGUSER','flight_user')} "
    f"password={os.getenv('PGPASSWORD','Password123')} "
    f"connect_timeout={os.getenv('PGCONNECT_TIMEOUT','5')}"
)

# ---------------- AIRLINE (IATA first) ----------------
STMT_AIRLINE_IATA = """
WITH src AS (
  SELECT DISTINCT ON (airline_iata)
         airline_iata AS iata,
         airline_icao AS icao,
         airline_name,
         ingest_time
  FROM fact_flight_status_staging
  WHERE ingest_time <= %s AND airline_iata IS NOT NULL
  ORDER BY airline_iata, ingest_time DESC
)
INSERT INTO dim_airline (iata, icao, airline_name)
SELECT iata, icao, airline_name
FROM src
ON CONFLICT (iata) DO UPDATE
SET icao         = COALESCE(EXCLUDED.icao, dim_airline.icao),
    airline_name = COALESCE(EXCLUDED.airline_name, dim_airline.airline_name);
"""

# ---------------- AIRLINE (ICAO-only path; no ON CONFLICT) ----------------
STMT_AIRLINE_ICAO_ONLY = """
WITH src AS (
  SELECT DISTINCT ON (airline_icao)
         airline_icao AS icao,
         airline_name,
         ingest_time
  FROM fact_flight_status_staging
  WHERE ingest_time <= %s AND airline_iata IS NULL AND airline_icao IS NOT NULL
  ORDER BY airline_icao, ingest_time DESC
),
upd AS (
  UPDATE dim_airline d
  SET airline_name = COALESCE(src.airline_name, d.airline_name)
  FROM src
  WHERE d.icao = src.icao
  RETURNING d.icao
)
INSERT INTO dim_airline (iata, icao, airline_name)
SELECT NULL, s.icao, s.airline_name
FROM src s
WHERE NOT EXISTS (
  SELECT 1 FROM dim_airline d WHERE d.icao = s.icao
);
"""

# ---------------- AIRPORTS (IATA first) ----------------
STMT_AIRPORT_DEP_IATA = """
WITH src AS (
  SELECT DISTINCT ON (dep_airport_iata)
         dep_airport_iata AS iata,
         dep_airport_icao AS icao,
         dep_airport      AS name,
         ingest_time
  FROM fact_flight_status_staging
  WHERE ingest_time <= %s AND dep_airport_iata IS NOT NULL
  ORDER BY dep_airport_iata, ingest_time DESC
)
INSERT INTO dim_airport (iata, icao, airport_name)
SELECT iata, icao, name
FROM src
ON CONFLICT (iata) DO UPDATE
SET icao         = COALESCE(EXCLUDED.icao, dim_airport.icao),
    airport_name = COALESCE(EXCLUDED.airport_name, dim_airport.airport_name);
"""

STMT_AIRPORT_ARR_IATA = """
WITH src AS (
  SELECT DISTINCT ON (arr_airport_iata)
         arr_airport_iata AS iata,
         arr_airport_icao AS icao,
         arr_airport      AS name,
         ingest_time
  FROM fact_flight_status_staging
  WHERE ingest_time <= %s AND arr_airport_iata IS NOT NULL
  ORDER BY arr_airport_iata, ingest_time DESC
)
INSERT INTO dim_airport (iata, icao, airport_name)
SELECT iata, icao, name
FROM src
ON CONFLICT (iata) DO UPDATE
SET icao         = COALESCE(EXCLUDED.icao, dim_airport.icao),
    airport_name = COALESCE(EXCLUDED.airport_name, dim_airport.airport_name);
"""

# ---------------- AIRPORTS (ICAO fallback; no ON CONFLICT) ----------------
STMT_AIRPORT_DEP_ICAO = """
WITH src AS (
  SELECT DISTINCT ON (dep_airport_icao)
         dep_airport_icao AS icao,
         dep_airport_iata AS iata,
         dep_airport      AS name,
         ingest_time
  FROM fact_flight_status_staging
  WHERE ingest_time <= %s
    AND dep_airport_icao IS NOT NULL
    AND dep_airport_iata IS NULL
  ORDER BY dep_airport_icao, ingest_time DESC
),
upd AS (
  UPDATE dim_airport d
  SET airport_name = COALESCE(src.name, d.airport_name),
      iata         = COALESCE(d.iata, src.iata)
  FROM src
  WHERE d.icao = src.icao
  RETURNING d.icao
)
INSERT INTO dim_airport (iata, icao, airport_name)
SELECT s.iata, s.icao, s.name
FROM src s
WHERE NOT EXISTS (
  SELECT 1 FROM dim_airport d WHERE d.icao = s.icao
);
"""

STMT_AIRPORT_ARR_ICAO = """
WITH src AS (
  SELECT DISTINCT ON (arr_airport_icao)
         arr_airport_icao AS icao,
         arr_airport_iata AS iata,
         arr_airport      AS name,
         ingest_time
  FROM fact_flight_status_staging
  WHERE ingest_time <= %s
    AND arr_airport_icao IS NOT NULL
    AND arr_airport_iata IS NULL
  ORDER BY arr_airport_icao, ingest_time DESC
),
upd AS (
  UPDATE dim_airport d
  SET airport_name = COALESCE(src.name, d.airport_name),
      iata         = COALESCE(d.iata, src.iata)
  FROM src
  WHERE d.icao = src.icao
  RETURNING d.icao
)
INSERT INTO dim_airport (iata, icao, airport_name)
SELECT s.iata, s.icao, s.name
FROM src s
WHERE NOT EXISTS (
  SELECT 1 FROM dim_airport d WHERE d.icao = s.icao
);
"""

# ---------------- ROUTES ----------------
STMT_ROUTES = """
WITH pairs AS (
  SELECT DISTINCT
         COALESCE(d_dep.airport_id, di_dep.airport_id) AS dep_id,
         COALESCE(d_arr.airport_id, di_arr.airport_id) AS arr_id
  FROM fact_flight_status_staging s
  LEFT JOIN dim_airport d_dep  ON d_dep.iata = s.dep_airport_iata
  LEFT JOIN dim_airport d_arr  ON d_arr.iata = s.arr_airport_iata
  LEFT JOIN dim_airport di_dep ON di_dep.icao = s.dep_airport_icao
  LEFT JOIN dim_airport di_arr ON di_arr.icao = s.arr_airport_icao
  WHERE s.ingest_time <= %s
    AND COALESCE(d_dep.airport_id, di_dep.airport_id) IS NOT NULL
    AND COALESCE(d_arr.airport_id, di_arr.airport_id) IS NOT NULL
)
INSERT INTO dim_route (dep_airport_id, arr_airport_id)
SELECT dep_id, arr_id
FROM pairs
ON CONFLICT (dep_airport_id, arr_airport_id) DO NOTHING;
"""

# ---------------- FACTS (latest per flight_key) ----------------
STMT_FACT = """
WITH latest AS (
  SELECT DISTINCT ON (flight_key) *
  FROM fact_flight_status_staging
  WHERE ingest_time <= %s
  ORDER BY flight_key, ingest_time DESC
),
aid AS (
  SELECT l.flight_key, a.airline_id
  FROM latest l
  LEFT JOIN dim_airline a
    ON (a.iata = l.airline_iata)
    OR (l.airline_iata IS NULL AND a.icao = l.airline_icao)
),
dep AS (
  SELECT l.flight_key, ap.airport_id AS dep_id
  FROM latest l
  LEFT JOIN dim_airport ap
    ON (l.dep_airport_iata IS NOT NULL AND ap.iata = l.dep_airport_iata)
    OR (l.dep_airport_iata IS NULL AND l.dep_airport_icao IS NOT NULL AND ap.icao = l.dep_airport_icao)
),
arr AS (
  SELECT l.flight_key, ap.airport_id AS arr_id
  FROM latest l
  LEFT JOIN dim_airport ap
    ON (l.arr_airport_iata IS NOT NULL AND ap.iata = l.arr_airport_iata)
    OR (l.arr_airport_iata IS NULL AND l.arr_airport_icao IS NOT NULL AND ap.icao = l.arr_airport_icao)
),
rid AS (
  SELECT d.flight_key, r.route_id
  FROM dep d
  JOIN arr a USING (flight_key)
  LEFT JOIN dim_route r
    ON r.dep_airport_id = d.dep_id
   AND r.arr_airport_id = a.arr_id
)
INSERT INTO fact_flight_status (
  flight_key, flight_date, status, ingest_time,
  airline_id, route_id,
  dep_scheduled, dep_estimated, dep_actual, dep_delay_min,
  arr_scheduled, arr_estimated, arr_actual, arr_delay_min,
  last_updated
)
SELECT l.flight_key,
       l.flight_date::date,
       l.status,
       COALESCE(l.ingest_time, NOW()),
       a.airline_id,
       r.route_id,
       l.dep_scheduled, l.dep_estimated, l.dep_actual, l.dep_delay_min,
       l.arr_scheduled, l.arr_estimated, l.arr_actual, l.arr_delay_min,
       NOW()
FROM latest l
LEFT JOIN aid a USING (flight_key)
LEFT JOIN rid r USING (flight_key)
ON CONFLICT (flight_key) DO UPDATE
SET flight_date   = EXCLUDED.flight_date,
    status        = EXCLUDED.status,
    ingest_time   = GREATEST(fact_flight_status.ingest_time, EXCLUDED.ingest_time),
    airline_id    = COALESCE(EXCLUDED.airline_id, fact_flight_status.airline_id),
    route_id      = COALESCE(EXCLUDED.route_id,   fact_flight_status.route_id),
    dep_scheduled = EXCLUDED.dep_scheduled,
    dep_estimated = EXCLUDED.dep_estimated,
    dep_actual    = EXCLUDED.dep_actual,
    dep_delay_min = EXCLUDED.dep_delay_min,
    arr_scheduled = EXCLUDED.arr_scheduled,
    arr_estimated = EXCLUDED.arr_estimated,
    arr_actual    = EXCLUDED.arr_actual,
    arr_delay_min = EXCLUDED.arr_delay_min,
    last_updated  = NOW();
"""

STMT_DELETE_STAGING = "DELETE FROM fact_flight_status_staging WHERE ingest_time <= %s;"

def connect_with_retry(max_attempts: int = 40, base_delay: float = 1.0):
    """Try to connect to Postgres with exponential backoff."""
    attempt = 0
    while True:
        attempt += 1
        try:
            conn = psycopg.connect(PG_DSN, autocommit=False)
            # sanity check that the server is ready
            with conn.cursor() as cur:
                cur.execute("SELECT 1;")
                cur.fetchone()
            if attempt > 1:
                print(f"[loader] Connected to Postgres after {attempt} attempts.", flush=True)
            return conn
        except psycopg.OperationalError as e:
            if attempt >= max_attempts:
                print(f"[loader] Postgres still unavailable after {attempt} attempts: {e}", flush=True)
                raise
            sleep_for = min(base_delay * (2 ** (attempt - 1)), 15.0)
            print(f"[loader] Postgres unavailable (attempt {attempt}): {e}. Retrying in {sleep_for:.1f}s...", flush=True)
            time.sleep(sleep_for)

def run_once():
    with connect_with_retry() as conn:
        with conn.cursor() as cur:
            # single server-side cutoff
            cur.execute("SELECT now();")
            (cutoff,) = cur.fetchone()

            for stmt in (
                STMT_AIRLINE_IATA, STMT_AIRLINE_ICAO_ONLY,
                STMT_AIRPORT_DEP_IATA, STMT_AIRPORT_ARR_IATA,
                STMT_AIRPORT_DEP_ICAO, STMT_AIRPORT_ARR_ICAO,
                STMT_ROUTES, STMT_FACT, STMT_DELETE_STAGING
            ):
                cur.execute(stmt, (cutoff,))
        conn.commit()

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--interval-seconds", type=int, default=60)
    interval = ap.parse_args().interval_seconds

    if interval <= 0:
        run_once()
    else:
        while True:
            try:
                run_once()
            except Exception as e:
                # don't kill the container; log and try again next tick
                print(f"[loader] run_once failed: {e}", flush=True)
            time.sleep(interval)
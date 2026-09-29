"""Integration tests against a real Postgres (loader SQL, views, and the Sheets watermark).

Set TEST_PG_DSN to a *disposable* database, e.g.

    TEST_PG_DSN="host=localhost port=5432 dbname=postgres user=postgres password=postgres" pytest

The schema files in db/ are (re)applied at the start of the module, which DROPs the warehouse tables.
Without TEST_PG_DSN these tests are skipped (CI provides a Postgres service container).
"""
import os
from pathlib import Path

import pytest

psycopg = pytest.importorskip("psycopg")

import load_warehouse  # noqa: E402
import sheets_sink  # noqa: E402

DSN = os.getenv("TEST_PG_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="TEST_PG_DSN not set (needs a disposable Postgres)")
DB_DIR = Path(__file__).resolve().parents[1] / "db"


@pytest.fixture(scope="module")
def conn():
    with psycopg.connect(DSN, autocommit=True) as c:
        c.execute("DROP VIEW IF EXISTS vw_flights_for_sheet_fact; DROP TABLE IF EXISTS gsheet_sync_state;")
        for sql_file in ("00_warehouse.sql", "01_views.sql"):
            c.execute((DB_DIR / sql_file).read_text())
        yield c


@pytest.fixture(autouse=True)
def clean(conn):
    conn.execute(
        "TRUNCATE fact_flight_status_staging, fact_flight_status, dim_route, dim_airport, dim_airline "
        "RESTART IDENTITY CASCADE"
    )
    conn.execute("UPDATE gsheet_sync_state SET last_sync_time = NULL, last_flight_key = '' WHERE id = 1")


def stage(conn, flight_key="NH849_x", status="active", ingest="now()", **cols):
    """Insert one staging row; defaults describe a JFK->LAX flight operated by Delta."""
    row = {
        "flight_key": flight_key, "status": status, "flight_date": "2025-08-21",
        "airline_iata": "DL", "airline_icao": "DAL", "airline_name": "Delta",
        "dep_airport": "John F Kennedy", "dep_airport_iata": "JFK", "dep_airport_icao": "KJFK",
        "arr_airport": "Los Angeles Intl", "arr_airport_iata": "LAX", "arr_airport_icao": "KLAX",
        "dep_scheduled": "2025-08-21T10:00:00+00:00", "arr_scheduled": "2025-08-21T13:00:00+00:00",
        "dep_delay_min": 5, "arr_delay_min": 0,
    }
    row.update(cols)
    names = ", ".join(row) + ", ingest_time"
    placeholders = ", ".join(["%s"] * len(row)) + f", {ingest}"
    conn.execute(f"INSERT INTO fact_flight_status_staging ({names}) VALUES ({placeholders})", list(row.values()))


def scalar(conn, sql, *params):
    return conn.execute(sql, params).fetchone()[0]


# ---------------------------------------------------------------- loader
def test_loader_builds_dimensions_routes_and_fact_then_clears_staging(conn):
    stage(conn, "DL1_a")
    stage(conn, "DL2_b", airline_iata="AA", airline_icao="AAL", airline_name="American")
    load_warehouse.run_once(DSN)

    assert scalar(conn, "SELECT count(*) FROM dim_airline") == 2
    assert scalar(conn, "SELECT count(*) FROM dim_airport") == 2
    assert scalar(conn, "SELECT count(*) FROM dim_route") == 1
    assert scalar(conn, "SELECT count(*) FROM fact_flight_status") == 2
    assert scalar(conn, "SELECT count(*) FROM fact_flight_status WHERE airline_id IS NULL OR route_id IS NULL") == 0
    assert scalar(conn, "SELECT count(*) FROM fact_flight_status_staging") == 0


def test_loader_is_idempotent_and_keeps_latest_snapshot(conn):
    stage(conn, "DL1_a", status="active")
    load_warehouse.run_once(DSN)
    stage(conn, "DL1_a", status="landed", arr_delay_min=12)
    load_warehouse.run_once(DSN)
    load_warehouse.run_once(DSN)  # nothing staged: must be a no-op

    assert scalar(conn, "SELECT count(*) FROM fact_flight_status") == 1
    assert scalar(conn, "SELECT status FROM fact_flight_status WHERE flight_key = 'DL1_a'") == "landed"
    assert scalar(conn, "SELECT arr_delay_min FROM fact_flight_status WHERE flight_key = 'DL1_a'") == 12
    assert scalar(conn, "SELECT count(*) FROM dim_airport") == 2


def test_loader_leaves_rows_that_arrive_after_its_cutoff(conn):
    stage(conn, "DL1_a")
    stage(conn, "DL2_future", ingest="now() + interval '1 hour'")  # lands "after" the cutoff
    load_warehouse.run_once(DSN)

    assert scalar(conn, "SELECT count(*) FROM fact_flight_status") == 1
    assert scalar(conn, "SELECT count(*) FROM fact_flight_status_staging WHERE flight_key = 'DL2_future'") == 1


def test_loader_resolves_airports_that_only_have_an_icao_code(conn):
    stage(conn, "DL1_a", dep_airport_iata=None, arr_airport_iata=None)
    load_warehouse.run_once(DSN)

    assert scalar(conn, "SELECT count(*) FROM dim_airport WHERE iata IS NULL AND icao IN ('KJFK', 'KLAX')") == 2
    assert scalar(conn, "SELECT route_id IS NOT NULL FROM fact_flight_status WHERE flight_key = 'DL1_a'") is True


def test_loader_without_iata_codes_is_the_symptom_of_the_old_producer_typo(conn):
    """When the producer dropped IATA *and* ICAO codes, flights load but never get a route."""
    stage(conn, "DL1_a", dep_airport_iata=None, arr_airport_iata=None, dep_airport_icao=None, arr_airport_icao=None)
    load_warehouse.run_once(DSN)
    assert scalar(conn, "SELECT count(*) FROM fact_flight_status WHERE route_id IS NULL") == 1


# ---------------------------------------------------------------- view + Sheets sync
def test_view_exposes_every_column_the_sheets_sink_selects(conn):
    conn.execute(f"SELECT {', '.join(sheets_sink.COLUMNS)} FROM vw_flights_for_sheet_fact LIMIT 0")


class FakeSheets:
    """Minimal stand-in for the Google Sheets client; records every appended row."""

    def __init__(self, fail_times=0):
        self.rows, self.fail_times, self.calls = [], fail_times, 0

    def spreadsheets(self):
        return self

    def values(self):
        return self

    def append(self, spreadsheetId, range, valueInputOption, insertDataOption, body):  # noqa: N803, A002
        outer = self

        class _Req:
            def execute(self_inner):  # noqa: N805
                outer.calls += 1
                if outer.fail_times:
                    outer.fail_times -= 1
                    raise RuntimeError("Sheets API unavailable")
                outer.rows.extend(body["values"])

        return _Req()


def seed_fact(conn, n):
    """n fact rows written in ONE statement => identical last_updated, as one loader cycle produces."""
    conn.execute(
        "INSERT INTO fact_flight_status (flight_key, status) SELECT 'K' || lpad(i::text, 3, '0'), 'active' "
        "FROM generate_series(1, %s) AS i", (n,)
    )
    assert scalar(conn, "SELECT count(DISTINCT last_updated) FROM fact_flight_status") == 1


def test_sync_exports_every_row_when_a_cycle_is_larger_than_one_batch(conn, monkeypatch):
    """Regression: a watermark on last_updated alone skipped rows 3..5 once batch 1 (2 rows) advanced it."""
    monkeypatch.setattr(sheets_sink, "SHEET_ID", "test-sheet")
    seed_fact(conn, 5)
    svc = FakeSheets()

    assert sheets_sink.sync_once(svc, conn, batch_size=2) == 5
    assert [r[0] for r in svc.rows] == ["K001", "K002", "K003", "K004", "K005"]
    assert sheets_sink.sync_once(svc, conn, batch_size=2) == 0  # nothing new, nothing duplicated
    assert len(svc.rows) == 5


def test_sync_appends_a_row_again_only_after_the_loader_updates_it(conn, monkeypatch):
    monkeypatch.setattr(sheets_sink, "SHEET_ID", "test-sheet")
    seed_fact(conn, 3)
    svc = FakeSheets()
    sheets_sink.sync_once(svc, conn, batch_size=10)

    conn.execute("UPDATE fact_flight_status SET status = 'landed', last_updated = now() + interval '1 second' "
                 "WHERE flight_key = 'K002'")
    assert sheets_sink.sync_once(svc, conn, batch_size=10) == 1
    assert svc.rows[-1][0] == "K002" and svc.rows[-1][2] == "landed"


def test_sync_does_not_advance_the_watermark_when_the_append_fails(conn, monkeypatch):
    monkeypatch.setattr(sheets_sink, "SHEET_ID", "test-sheet")
    seed_fact(conn, 3)
    svc = FakeSheets(fail_times=1)

    with pytest.raises(RuntimeError):
        sheets_sink.sync_once(svc, conn, batch_size=10)
    conn.rollback()
    assert scalar(conn, "SELECT last_sync_time FROM gsheet_sync_state WHERE id = 1") is None

    assert sheets_sink.sync_once(svc, conn, batch_size=10) == 3  # retried in full on the next run
    assert len(svc.rows) == 3


def test_sync_respects_max_batches_and_resumes(conn, monkeypatch):
    monkeypatch.setattr(sheets_sink, "SHEET_ID", "test-sheet")
    seed_fact(conn, 5)
    svc = FakeSheets()
    assert sheets_sink.sync_once(svc, conn, batch_size=2, max_batches=1) == 2
    assert sheets_sink.sync_once(svc, conn, batch_size=2) == 3
    assert len(svc.rows) == 5

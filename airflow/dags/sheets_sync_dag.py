"""Push new fact rows to Google Sheets (which Tableau Public reads).

ensure_sheet  -> create the tab / header / number formats if needed (idempotent)
append_new_rows -> append everything after the (last_updated, flight_key) watermark, batch by batch

The watermark only moves after a batch was appended, so a failed run is simply retried by Airflow.
"""
from __future__ import annotations

from datetime import datetime, timezone

from _common import DEFAULT_ARGS
from airflow.operators.python import PythonOperator

from airflow import DAG


def _ensure_sheet() -> None:
    from sheets_sink import get_sheets, setup_sheet

    setup_sheet(get_sheets())


def _append_new_rows() -> int:
    from sheets_sink import sync_once

    pushed = sync_once()
    print(f"Appended {pushed} rows to Google Sheets")
    return pushed


with DAG(
    dag_id="sheets_sync",
    description="Incrementally export the curated flights view to Google Sheets every 2 minutes.",
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    schedule="*/2 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["flights", "sheets"],
):
    ensure_sheet = PythonOperator(task_id="ensure_sheet", python_callable=_ensure_sheet)
    append_new_rows = PythonOperator(task_id="append_new_rows", python_callable=_append_new_rows)
    ensure_sheet >> append_new_rows

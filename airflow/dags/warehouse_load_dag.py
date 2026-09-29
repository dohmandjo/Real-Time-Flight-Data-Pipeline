"""Load staged flight events into the star schema.

staging (written by Spark) --> dim_airline / dim_airport / dim_route --> fact_flight_status

The task runs the same idempotent ``run_once()`` as the standalone loader container: one consistent
cutoff timestamp per run, upserts by natural key, and deletion of only the rows it processed.
"""
from __future__ import annotations

from datetime import datetime, timezone

from _common import DEFAULT_ARGS
from airflow.operators.python import PythonOperator

from airflow import DAG


def _run_loader() -> None:
    # Imported at run time so parsing this DAG needs no database credentials.
    from load_warehouse import run_once

    run_once()


with DAG(
    dag_id="warehouse_load",
    description="Upsert dimensions + fact from staging every 2 minutes (idempotent).",
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    schedule="*/2 * * * *",
    catchup=False,
    max_active_runs=1,          # never two loads at once
    default_args=DEFAULT_ARGS,
    tags=["flights", "warehouse"],
):
    PythonOperator(task_id="upsert_from_staging", python_callable=_run_loader)

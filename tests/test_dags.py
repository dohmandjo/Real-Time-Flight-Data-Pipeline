"""DAG-level checks: they import cleanly and every task retries with backoff.

Skipped unless apache-airflow is installed (CI has a dedicated job for it).
"""
from pathlib import Path

import pytest

pytest.importorskip("airflow.models")  # "airflow" alone would match the repo's airflow/ folder

from airflow.models import DagBag  # noqa: E402

DAG_FOLDER = Path(__file__).resolve().parents[1] / "airflow" / "dags"


@pytest.fixture(scope="module")
def dagbag():
    return DagBag(dag_folder=str(DAG_FOLDER), include_examples=False)


def test_dags_import_without_errors(dagbag):
    assert dagbag.import_errors == {}


def test_expected_dags_exist(dagbag):
    assert {"warehouse_load", "sheets_sync"} <= set(dagbag.dag_ids)


def test_every_task_retries_with_exponential_backoff(dagbag):
    """The resume-worthy claim: retry handling is configured, not just hoped for."""
    for dag in dagbag.dags.values():
        for task in dag.tasks:
            assert task.retries >= 3, f"{dag.dag_id}.{task.task_id} has retries={task.retries}"
            assert task.retry_exponential_backoff, f"{dag.dag_id}.{task.task_id} has no backoff"
            assert task.execution_timeout is not None, f"{dag.dag_id}.{task.task_id} has no timeout"


def test_runs_never_overlap_and_do_not_backfill(dagbag):
    for dag in dagbag.dags.values():
        assert dag.max_active_runs == 1, dag.dag_id
        assert dag.catchup is False, dag.dag_id


def test_sheets_sync_orders_setup_before_append(dagbag):
    dag = dagbag.dags["sheets_sync"]
    assert dag.get_task("append_new_rows").upstream_task_ids == {"ensure_sheet"}

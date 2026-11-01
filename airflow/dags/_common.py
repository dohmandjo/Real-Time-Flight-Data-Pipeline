"""Settings shared by every DAG in this project."""
from datetime import timedelta

# Every task is retried with exponential backoff (30s, 60s, 120s ...) before the run is marked failed.
# Transient problems (Postgres restarting, Sheets API quota errors) heal themselves; real bugs still surface.
DEFAULT_ARGS = {
    "owner": "data-eng",
    "retries": 3,
    "retry_delay": timedelta(seconds=30),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=5),
}

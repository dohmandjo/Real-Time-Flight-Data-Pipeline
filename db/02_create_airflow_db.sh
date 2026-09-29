#!/bin/bash
# Creates a separate database + role for Airflow's metadata, next to the flight warehouse.
# Runs once, when the Postgres data volume is first initialised (docker-entrypoint-initdb.d).
set -euo pipefail

: "${AIRFLOW_DB_PASSWORD:?AIRFLOW_DB_PASSWORD must be set (see .env.example)}"

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
     -v airflow_password="$AIRFLOW_DB_PASSWORD" <<'EOSQL'
CREATE ROLE airflow LOGIN PASSWORD :'airflow_password';
CREATE DATABASE airflow OWNER airflow;
EOSQL

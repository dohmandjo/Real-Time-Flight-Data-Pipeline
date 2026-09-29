FROM apache/airflow:2.9.3-python3.11

# Extra libraries used by the DAG callables (the loader and Sheets sink code is bind-mounted, not copied).
# The constraints file keeps Airflow's own dependencies pinned to the tested set.
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.3/constraints-3.11.txt"

# ========= Base image =========
# Python 3.12 slim is fine here; small + up to date
FROM python:3.12-slim

# ========= Workdir =========
WORKDIR /apps

# ========= App code =========
COPY run_producer.py .

# ========= Python deps =========
# Installing directly (keeps image small). If you later add more,
# consider a requirements.txt for better layer caching.
RUN pip install --no-cache-dir requests kafka-python python-dateutil

# ========= Entrypoint =========
# Keep it simple: invoke your script with an interval (can be overridden by Docker/Compose)
CMD ["python", "run_producer.py", "--interval-seconds", "60"]
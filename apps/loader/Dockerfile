FROM python:3.12-slim

WORKDIR /app

# Dependencies first: this layer is cached until requirements.txt changes.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY load_warehouse.py .

# Default: loop forever (Compose overrides the interval). Use `--interval-seconds 0` for a single run.
CMD ["python", "load_warehouse.py"]

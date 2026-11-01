FROM python:3.12-slim

WORKDIR /apps

# Install dependencies first so this layer is cached until requirements.txt changes.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY run_producer.py .

# Poll interval (seconds) can be overridden: `command: ["python", "run_producer.py", "--interval-seconds", "120"]`
CMD ["python", "run_producer.py", "--interval-seconds", "60"]

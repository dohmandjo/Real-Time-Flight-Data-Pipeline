FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY sheets_sink.py .

# Reads DB settings, GSHEET_ID / GSHEET_TAB and the service-account key path from the environment.
CMD ["python", "sheets_sink.py"]

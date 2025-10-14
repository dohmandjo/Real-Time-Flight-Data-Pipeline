# apps/sheets_sink/Dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY sheets_sink.py .
RUN pip install --no-cache-dir \
    google-api-python-client google-auth google-auth-httplib2 google-auth-oauthlib \
    psycopg[binary]
CMD ["python", "sheets_sink.py"]

FROM python:3.11-slim
WORKDIR /app
COPY load_warehouse.py .
RUN pip install --no-cache-dir psycopg[binary]
CMD ["python", "load_warehouse.py"]

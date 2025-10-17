# Use the official slim Python 3.11 base image (small footprint, Debian-based)
FROM python:3.12-slim

# Set working directory inside the container (all next paths are relative to /app)
WORKDIR /app

# Copy the loader script into the image
COPY load_warehouse.py .

# Install psycopg (binary distribution) without caching wheels to keep image small
RUN pip install --no-cache-dir psycopg[binary]

# Container start command: run the loader
CMD ["python", "load_warehouse.py"]

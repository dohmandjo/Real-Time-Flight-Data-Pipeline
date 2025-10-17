# Use a small, secure Python base image (Debian slim) with Python 3.11.
# This keeps the container lightweight while still compatible with Google APIs.
FROM python:3.12-slim

# Create and set the working directory inside the container.
# All subsequent commands run from /app, and the script will execute here.
WORKDIR /app

# Copy the sink script into the container image.
# Keep the context clean so only what's needed goes in the image.
COPY sheets_sink.py .

# Install runtime dependencies:
# - google-api-python-client and auth libs for Google Sheets API calls
# - psycopg[binary] for a fast, self-contained Postgres client (no OS deps)
# --no-cache-dir avoids caching wheels to keep the final image small.
RUN pip install --no-cache-dir \
    google-api-python-client google-auth google-auth-httplib2 google-auth-oauthlib \
    psycopg[binary]

# Default command: run the sink.
# The script reads env vars (DB, Sheet ID, credentials path) provided by docker-compose.
CMD ["python", "sheets_sink.py"]
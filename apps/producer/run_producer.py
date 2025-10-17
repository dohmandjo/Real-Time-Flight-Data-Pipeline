"""
Producer: pulls recent flight data from Aviationstack and publishes JSON records to Kafka.

Requirements
------------
- Environment variables:
    AVIATIONSTACK_KEY  : your Aviationstack API key
    KAFKA_BOOTSTRAP    : Kafka bootstrap servers (default "kafka:9094" in Compose network)
    KAFKA_TOPIC        : Kafka topic to produce to (default "flights_live")

- Network assumptions:
    This script runs inside Docker Compose where the Kafka broker is reachable as "kafka:9094".

What it does
------------
1) Calls Aviationstack /v1/flights (paginated via "limit") to fetch a small batch.
2) Normalizes a subset of fields into a stable JSON schema expected by the Spark consumer.
3) Sends each record to Kafka as a JSON message (UTF-8).

Notes
-----
- Timestamps from the API are normalized to ISO 8601 in UTC using python-dateutil.
- The `flight_key` is derived from (flight number variant + departure scheduled time)
  to act as a stable dedup key downstream.
"""

import os, time, json, argparse, requests
from datetime import datetime, timezone
from dateutil import parser as dtp
from kafka import KafkaProducer

# --- Configuration via env vars (with safe defaults for local dev/Compose) ---
API_KEY = os.getenv("AVIATIONSTACK_KEY")                            # Required for API access
BROKER  = os.getenv("KAFKA_BOOTSTRAP", "kafka:9094")                # Kafka broker(s) inside Compose
TOPIC   = os.getenv("KAFKA_TOPIC", "flights_live")                  # Target Kafka topic

def build_record(rec):
    """
    Transform a single Aviationstack flight object into our downstream schema.

    The input "rec" is one element of the "data" array returned by Aviationstack.
    We guard with `.get(... ) or {}` so missing sections don’t crash processing.
    """
    dep     = rec.get("departure") or {}
    arr     = rec.get("arrival") or {}
    airline = rec.get("airline") or {}
    flight  = rec.get("flight") or {}

    # Prefer "scheduled" but fall back to older/alternate field names if present.
    dep_sched = dep.get("scheduled") or dep.get("scheduled_time")

    # Best-effort flight number: prefer IATA, then ICAO, then bare "number".
    # If everything is missing, use "UNKNOWN".
    flight_num = (
        flight.get("iata")
        or flight.get("icao")
        or flight.get("number")
        or "UNKNOWN"
    )

    # Create a stable key for dedup/merge downstream (Spark/warehouse).
    # Example: "NH849_2025-08-21T00:05:00+00:00"
    flight_key = f"{flight_num}_{dep_sched}"

    def iso(ts):
        """
        Convert arbitrary timestamp string to ISO 8601 in UTC if possible.
        - Returns None for falsy inputs.
        - If parsing fails, return the original string (let Spark handle later).
        """
        if not ts:
            return None
        try:
            # Parse with dateutil (handles many formats) and normalize to UTC
            return dtp.isoparse(ts).astimezone(timezone.utc).isoformat()
        except Exception:
            # Keep original string; downstream Spark normalization will attempt to fix it
            return ts

    # Build the normalized document; nested objects match Spark schema fields.
    return {
        "flight_key":   flight_key,
        "flight_date":  rec.get("flight_date"),       # YYYY-MM-DD as provided by API
        "status":       rec.get("flight_status"),     # e.g., "active", "landed", ...

        "airline": {
            "iata": airline.get("iata"),
            "icao": airline.get("icao"),
            "name": airline.get("name"),
        },
        "flight": {
            "number": flight.get("number"),           # numeric string e.g., "849"
            "iata":   flight.get("iata"),             # e.g., "NH849"
            "icao":   flight.get("icao"),             # e.g., "ANA849"
        },
        "departure": {
            "airport":   dep.get("airport"),
            "iata":      dep.get("iata"),
            "icao":      dep.get("icao"),
            "gate":      dep.get("gate"),
            "terminal":  dep.get("terminal"),
            "schedule":  iso(dep_sched),              # normalized UTC ISO string (or original)
            "estimated": iso(dep.get("estimated")),
            "actual":    iso(dep.get("actual")),
            "delay_min": dep.get("delay")             # numeric minutes (can be None)
        },
        "arrival": {
            "airport":   arr.get("airport"),
            "iata":      arr.get("iata"),
            "icao":      arr.get("icao"),
            "gate":      arr.get("gate"),
            "terminal":  arr.get("terminal"),
            "schedule":  iso(arr.get("scheduled")),
            "estimated": iso(arr.get("estimated")),
            "actual":    iso(arr.get("actual")),
            "delay_min": arr.get("delay")
        },
        # Record the ingestion wall-clock time (UTC) for downstream watermarking.
        "ingest_time": datetime.now(timezone.utc).isoformat(),
        "source": "aviationstack"
    }

def fetch_batch():
    """
    Pull a single page of flights from the Aviationstack API.
    - Keep 'limit' modest to respect free/low-tier API quotas.
    - Any HTTP error raises; caller may retry on next loop.
    """
    url = "http://api.aviationstack.com/v1/flights"
    params = {"access_key": API_KEY, "limit": 100}
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    # API returns: {"pagination": {...}, "data": [ ... ] }
    return r.json().get("data", [])

def main(interval):
    """
    Main loop:
      - Create a Kafka producer.
      - In an endless loop:
          fetch a batch → transform each record → send to Kafka → flush → sleep.
    """
    producer = KafkaProducer(
        bootstrap_servers=BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),  # JSON encode each message
        linger_ms=100                                              # Small batching window
    )

    while True:
        for rec in fetch_batch():
            # Transform the raw API record into the normalized schema and send it
            producer.send(TOPIC, build_record(rec))

        # Force any buffered messages to be sent now (flush the linger)
        producer.flush()

        # Pause before next API call to control rate and respect API quotas
        time.sleep(interval)

if __name__ == "__main__":
    # Simple CLI to control polling interval without changing code
    ap = argparse.ArgumentParser()
    ap.add_argument("--interval-seconds", type=int, default=60,
                    help="Sleep between API polls to control rate/quotas (default: 60s)")
    main(ap.parse_args().interval_seconds)

import os, time, json, argparse, requests
from datetime import datetime, timezone
from dateutil import parser as dtp
from kafka import KafkaProducer

API_KEY = os.getenv("AVIATIONSTACK_KEY")
BROKER = os.getenv("KAFKA_BOOTSTRAP", "kafka:9094")
TOPIC  = os.getenv("KAFKA_TOPIC", "flights_live")

def build_record(rec):
    dep = rec.get("departure") or {}
    arr = rec.get("arrival") or {}
    airline = rec.get("airline") or {}
    flight  = rec.get("flight") or {}

    dep_sched = dep.get("scheduled") or dep.get("scheduled_time")
    flight_num = flight.get("iata") or flight.get("icao") or flight.get("number") or "UNKNOWN"
    flight_key = f"{flight_num}_{dep_sched}"

    def iso(ts):
        if not ts: return None
        try:
            return dtp.isoparse(ts).astimezone(timezone.utc).isoformat()
        except Exception:
            return ts

    return {
        "flight_key": flight_key,
        "flight_date": rec.get("flight_date"),
        "status": rec.get("flight_status"),

        "airline": {
            "iata": airline.get("iata"),
            "icao": airline.get("icao"),
            "name": airline.get("name"),
        },
        "flight": {
            "number": flight.get("number"),
            "iata": flight.get("iata"),
            "icao": flight.get("icao"),
        },
        "departure": {
            "airport": dep.get("airport"),
            "iata": dep.get("iata"),
            "icao": dep.get("icao"),
            "gate": dep.get("gate"),
            "terminal": dep.get("terminal"),
            "schedule": iso(dep_sched),
            "estimated": iso(dep.get("estimated")),
            "actual": iso(dep.get("actual")),
            "delay_min": dep.get("delay")
        },
        "arrival": {
            "airport": arr.get("airport"),
            "iata": arr.get("iata"),
            "icao": arr.get("icao"),
            "gate": arr.get("gate"),
            "terminal": arr.get("terminal"),
            "schedule": iso(arr.get("scheduled")),
            "estimated": iso(arr.get("estimated")),
            "actual": iso(arr.get("actual")),
            "delay_min": arr.get("delay")
        },
        "ingest_time": datetime.now(timezone.utc).isoformat(),
        "source": "aviationstack"
    }

def fetch_batch():
    url = "http://api.aviationstack.com/v1/flights"
    params = {"access_key": API_KEY, "limit": 100}
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    return r.json().get("data", [])

def main(interval):
    producer = KafkaProducer(
        bootstrap_servers=BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        linger_ms=100
    )
    while True:
        for rec in fetch_batch():
            producer.send(TOPIC, build_record(rec))
        producer.flush()
        time.sleep(interval)

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--interval-seconds", type=int, default=60)
    main(ap.parse_args().interval_seconds)

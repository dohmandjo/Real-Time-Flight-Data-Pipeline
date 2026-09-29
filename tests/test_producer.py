"""Unit tests for apps/producer/run_producer.py (no Kafka or network needed)."""
import pytest

import run_producer as producer

API_RECORD = {
    "flight_date": "2025-08-21",
    "flight_status": "active",
    "departure": {
        "airport": "Haneda Airport", "iata": "HND", "icao": "RJTT", "terminal": "2", "gate": "73",
        "delay": 48, "scheduled": "2025-08-21T00:05:00+00:00", "estimated": "2025-08-21T00:05:00+00:00",
        "actual": "2025-08-21T00:53:00+00:00",
    },
    "arrival": {
        "airport": "Suvarnabhumi International", "iata": "BKK", "icao": "VTBS", "terminal": "MAIN", "gate": None,
        "delay": None, "scheduled": "2025-08-21T04:35:00+00:00", "estimated": "2025-08-21T04:20:00+00:00",
        "actual": None,
    },
    "airline": {"name": "ANA", "iata": "NH", "icao": "ANA"},
    "flight": {"number": "849", "iata": "NH849", "icao": "ANA849", "codeshared": None},
}


def test_build_record_maps_airport_codes():
    """Regression: a typo ('aita') once left every airport IATA code NULL, which emptied dim_route."""
    rec = producer.build_record(API_RECORD)
    assert rec["departure"]["iata"] == "HND"
    assert rec["arrival"]["iata"] == "BKK"
    assert rec["departure"]["icao"] == "RJTT"
    assert rec["arrival"]["icao"] == "VTBS"


def test_build_record_key_uses_iata_flight_number_and_departure_time():
    rec = producer.build_record(API_RECORD)
    assert rec["flight_key"] == "NH849_2025-08-21T00:05:00+00:00"
    assert rec["status"] == "active"
    assert rec["source"] == "aviationstack"


def test_build_record_key_falls_back_to_icao_then_number_then_unknown():
    no_iata = {**API_RECORD, "flight": {"number": "849", "iata": None, "icao": "ANA849"}}
    assert producer.build_record(no_iata)["flight_key"].startswith("ANA849_")
    number_only = {**API_RECORD, "flight": {"number": "849"}}
    assert producer.build_record(number_only)["flight_key"].startswith("849_")
    assert producer.build_record({})["flight_key"].startswith("UNKNOWN_")


def test_build_record_normalizes_timestamps_to_utc():
    rec = producer.build_record(
        {**API_RECORD, "departure": {**API_RECORD["departure"], "scheduled": "2025-08-21T09:05:00+09:00"}}
    )
    assert rec["departure"]["schedule"] == "2025-08-21T00:05:00+00:00"


def test_build_record_tolerates_missing_sections_and_bad_timestamps():
    rec = producer.build_record({"flight_status": "active", "departure": {"scheduled": "garbage"}})
    assert rec["departure"]["schedule"] == "garbage"   # left as-is for Spark to null out
    assert rec["arrival"]["iata"] is None
    assert rec["ingest_time"]


class _Resp:
    def __init__(self, payload, status=200):
        self._payload, self.status_code = payload, status

    def raise_for_status(self):
        if self.status_code >= 400:
            import requests
            raise requests.HTTPError(f"{self.status_code}")

    def json(self):
        return self._payload


def test_fetch_batch_returns_data(monkeypatch):
    monkeypatch.setattr(producer.requests, "get", lambda *a, **k: _Resp({"data": [API_RECORD]}))
    assert producer.fetch_batch() == [API_RECORD]


def test_fetch_batch_raises_on_in_band_api_error(monkeypatch):
    """Aviationstack reports bad keys / quota exhaustion as HTTP 200 + {'error': ...}."""
    err = {"error": {"code": "usage_limit_reached", "message": "Monthly quota exceeded"}}
    monkeypatch.setattr(producer.requests, "get", lambda *a, **k: _Resp(err))
    with pytest.raises(RuntimeError, match="usage_limit_reached"):
        producer.fetch_batch()


def test_backoff_grows_exponentially_and_is_capped():
    assert [producer.backoff_seconds(n, base=60) for n in (1, 2, 3)] == [60, 120, 240]
    assert producer.backoff_seconds(10, base=60) == producer.MAX_BACKOFF_SECONDS

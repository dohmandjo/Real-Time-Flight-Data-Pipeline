"""Unit tests for apps/spark_app/transforms.py (runs against a local Spark session)."""
import json

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import functions as F  # noqa: E402

from transforms import TS_FMT, clean_ts, filter_events, parse_events  # noqa: E402


def _now():
    """Fixed "current time" so the 3-day window is deterministic (needs an active SparkContext)."""
    return F.to_timestamp(F.lit("2025-08-21T12:00:00+00:00"))


# Spark formats timestamps in the *session* time zone (UTC, see conftest). Collecting them as Python
# datetimes would use the test machine's local zone, so tests compare formatted UTC strings instead.
FMT = "yyyy-MM-dd'T'HH:mm:ss"


def _parse_ts(spark, values):
    df = spark.createDataFrame([(v,) for v in values], "raw string")
    out = df.select(F.date_format(F.to_timestamp(clean_ts("raw"), TS_FMT), FMT).alias("ts")).collect()
    return [r.ts for r in out]


@pytest.mark.parametrize(
    "raw",
    [
        "2025-08-21T00:05:00+00:00",   # already canonical
        "2025-08-21T00:05:00Z",        # 'Z' suffix
        "2025-08-21T00:05:00+0000",    # offset without colon
        "2025-08-21T00:05:00",         # no timezone -> assume UTC
        "2025-08-21T00:05+00:00",      # missing seconds
    ],
)
def test_clean_ts_normalizes_common_shapes(spark, raw):
    assert _parse_ts(spark, [raw]) == ["2025-08-21T00:05:00"]


def test_clean_ts_handles_fractional_seconds(spark):
    """Producer 'ingest_time' has microseconds; it must parse rather than silently become NULL."""
    assert _parse_ts(spark, ["2025-08-21T00:05:07.123456+00:00"]) == ["2025-08-21T00:05:07"]


def test_clean_ts_pads_single_digit_seconds(spark):
    assert _parse_ts(spark, ["2025-08-21T00:05:7+00:00"]) == ["2025-08-21T00:05:07"]


def test_clean_ts_garbage_becomes_null(spark):
    assert _parse_ts(spark, ["not-a-timestamp", None]) == [None, None]


def _event(**overrides):
    base = {
        "flight_key": "NH849_2025-08-21T00:05:00+00:00",
        "flight_date": "2025-08-21",
        "status": "active",
        "airline": {"iata": "NH", "icao": "ANA", "name": "ANA"},
        "flight": {"number": "849", "iata": "NH849", "icao": "ANA849"},
        "departure": {"airport": "Haneda", "iata": "HND", "icao": "RJTT", "gate": "73", "terminal": "2",
                      "schedule": "2025-08-21T00:05:00+00:00", "estimated": None,
                      "actual": "2025-08-21T00:53:00+00:00", "delay_min": 48},
        "arrival": {"airport": "Suvarnabhumi", "iata": "BKK", "icao": "VTBS", "gate": None, "terminal": "MAIN",
                    "schedule": "2025-08-21T04:35:00+00:00", "estimated": "2025-08-21T04:20:00+00:00",
                    "actual": None, "delay_min": None},
        "ingest_time": "2025-08-21T11:59:00.123456+00:00",
        "source": "aviationstack",
    }
    base.update(overrides)
    return json.dumps(base)


def _run(spark, events, **kwargs):
    df = spark.createDataFrame([(e,) for e in events], "json string")
    return filter_events(parse_events(df), now=_now(), **kwargs).collect()


def test_parse_events_flattens_nested_fields(spark):
    df = spark.createDataFrame([(_event(),)], "json string")
    row = parse_events(df).withColumn("dep_sched_str", F.date_format("dep_sched_ts", FMT)).collect()[0]
    assert row.airline_iata == "NH"
    assert row.dep_airport_iata == "HND"
    assert row.arr_airport_iata == "BKK"
    assert row.dep_delay_min == 48.0
    assert row.arr_delay_min is None
    assert row.dep_sched_str == "2025-08-21T00:05:00"


def test_parse_events_ingest_time_is_not_lost(spark):
    """ingest_time carries microseconds; if it parses to NULL the loader silently uses processing time."""
    df = spark.createDataFrame([(_event(),)], "json string")
    row = parse_events(df).collect()[0]
    assert row.ingest_ts is not None


def test_filter_keeps_active_recent_flight(spark):
    assert len(_run(spark, [_event()])) == 1


@pytest.mark.parametrize("status", ["ACTIVE", "Landed", "arrived", "en-route", "enroute"])
def test_filter_status_is_case_insensitive(spark, status):
    assert len(_run(spark, [_event(status=status)])) == 1


@pytest.mark.parametrize("status", ["scheduled", "cancelled", "diverted", None])
def test_filter_drops_other_statuses(spark, status):
    assert _run(spark, [_event(status=status)]) == []


def test_filter_drops_flights_older_than_three_days(spark):
    old = _event(
        departure={"schedule": "2025-08-10T00:05:00+00:00", "actual": "2025-08-10T00:53:00+00:00"},
        arrival={"schedule": "2025-08-10T04:35:00+00:00"},
    )
    assert _run(spark, [old]) == []


def test_filter_drops_rows_without_key_or_timestamps(spark):
    no_key = _event(flight_key=None)
    no_times = _event(departure={"airport": "X"}, arrival={"airport": "Y"})
    assert _run(spark, [no_key, no_times]) == []


def test_filter_days_is_configurable(spark):
    two_days_old = _event(
        departure={"schedule": "2025-08-19T00:05:00+00:00"}, arrival={"schedule": "2025-08-19T04:35:00+00:00"}
    )
    assert len(_run(spark, [two_days_old], days=3)) == 1
    assert _run(spark, [two_days_old], days=1) == []

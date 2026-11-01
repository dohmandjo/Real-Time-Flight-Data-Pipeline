"""Unit tests for the pure helpers in apps/sheets_sink/sheets_sink.py."""
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import sheets_sink as sink

COLS = sink.COLUMNS


def _row(**overrides):
    base = dict.fromkeys(COLS)
    base.update(overrides)
    return tuple(base[c] for c in COLS)


def test_to_values_keeps_delays_numeric_and_blanks_none():
    (vals,) = sink.to_values([_row(flight_key="K1", dep_delay_min=Decimal("12.5"), arr_delay_min=None)])
    assert vals[COLS.index("dep_delay_min")] == 12.5
    assert isinstance(vals[COLS.index("dep_delay_min")], float)
    assert vals[COLS.index("arr_delay_min")] == ""
    assert vals[COLS.index("flight_key")] == "K1"


def test_to_values_coerces_ints_and_bad_numbers():
    (vals,) = sink.to_values([_row(dep_delay_min=7, arr_delay_min="oops")])
    assert vals[COLS.index("dep_delay_min")] == 7.0
    assert vals[COLS.index("arr_delay_min")] == ""


def test_to_values_renders_datetimes_as_utc_iso():
    tz = timezone(timedelta(hours=9))
    (vals,) = sink.to_values([_row(dep_scheduled=datetime(2025, 8, 21, 9, 5, tzinfo=tz))])
    assert vals[COLS.index("dep_scheduled")] == "2025-08-21T00:05:00+00:00"


def test_a1_quote_tab_escapes_single_quotes():
    assert sink.a1_quote_tab("flights") == "'flights'"
    assert sink.a1_quote_tab("bob's tab") == "'bob''s tab'"


def test_watermark_columns_are_selected_in_the_expected_positions():
    """sync_once() reads flight_key from index 0 and last_updated from the last index."""
    assert COLS[0] == "flight_key"
    assert COLS[-1] == "last_updated"


def test_dsn_requires_a_password(monkeypatch):
    import pytest
    monkeypatch.delenv("PGPASSWORD", raising=False)
    with pytest.raises(RuntimeError, match="PGPASSWORD"):
        sink.pg_dsn()

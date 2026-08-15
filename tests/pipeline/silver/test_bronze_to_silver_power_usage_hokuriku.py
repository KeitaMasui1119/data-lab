"""Unit tests for the Hokuriku power_usage bronze-to-silver transformation.

Modeled on tests/test_bronze_to_silver_occto_unit_generation.py: the
transform SQL is exercised against a locally registered relation instead of
iceberg_scan, so these tests need neither RustFS nor an Iceberg catalog.
Correctness against real production data (2,082 real historical snapshots,
including the comma-padded-separator and legitimately-all-empty edge cases)
was additionally verified manually during development; see conversation
history / docs/architecture/data_model.md 3.1.
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import date

import duckdb
import polars as pl
import pytest
from pyiceberg.expressions import And, GreaterThanOrEqual, LessThanOrEqual

from pipeline.silver.bronze_to_silver_power_usage_hokuriku import (
    HOURLY_METRICS,
    HOURLY_ROW_COUNT,
    INTERVAL5_METRICS,
    INTERVAL5_ROW_COUNT,
    _hourly_column,
    _interval5_column,
    build_target_date_window,
    extract_daily_summary_frame,
    extract_hourly_frame,
    extract_interval5_frame,
    resolve_staged_target_date_range,
    target_date_bound,
)

SOURCE_RELATION = "bronze_source"

DAILY_SUMMARY_COLUMNS = [
    "target_date",
    "file_updated_at",
    "today_peak_supply_capacity",
    "today_peak_supply_time_range",
    "today_peak_supply_updated_date",
    "today_peak_supply_updated_time",
    "today_peak_supply_reserve_margin_pct",
    "today_peak_supply_usage_rate_pct",
]


@pytest.fixture
def conn() -> Iterator[duckdb.DuckDBPyConnection]:
    connection = duckdb.connect()
    yield connection
    connection.close()


def _register(conn: duckdb.DuckDBPyConnection, rows: list[dict[str, object]]) -> None:
    frame = pl.DataFrame(rows)
    conn.register(SOURCE_RELATION, frame)


# ---------------------------------------------------------------------------
# Column-name generators — position defines the slot, must be lockstep with
# the bronze schema CSV column order.
# ---------------------------------------------------------------------------


def test_hourly_column_covers_24_hours_no_duplicates():
    columns = [_hourly_column(h, "actual_demand") for h in range(HOURLY_ROW_COUNT)]
    assert len(columns) == 24
    assert len(set(columns)) == 24
    assert columns[0] == "hourly_00_00_actual_demand"
    assert columns[-1] == "hourly_23_00_actual_demand"


def test_interval5_column_covers_288_slots_5_minutes_apart():
    columns = [
        _interval5_column(s, "actual_demand") for s in range(INTERVAL5_ROW_COUNT)
    ]
    assert len(columns) == 288
    assert len(set(columns)) == 288
    assert columns[0] == "interval5_00_00_actual_demand"
    assert columns[1] == "interval5_00_05_actual_demand"
    assert columns[-1] == "interval5_23_55_actual_demand"


# ---------------------------------------------------------------------------
# extract_daily_summary_frame() — cast + dedup, no unpivot
# ---------------------------------------------------------------------------


def _daily_summary_row(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "target_date": "2026-08-07",
        "file_updated_at": "2026/08/08 00:10 UPDATE",
        "today_peak_supply_capacity": "123",
        "today_peak_supply_time_range": "19:00～20:00",
        "today_peak_supply_updated_date": "08月07日",
        "today_peak_supply_updated_time": "20:08",
        "today_peak_supply_reserve_margin_pct": "41",
        "today_peak_supply_usage_rate_pct": "70",
        "ingestion_time": None,
    }
    row.update(overrides)
    return row


def test_extract_daily_summary_frame_casts_numeric_fields(conn) -> None:
    _register(conn, [_daily_summary_row()])

    frame = extract_daily_summary_frame(
        conn, source_relation=SOURCE_RELATION, silver_columns=DAILY_SUMMARY_COLUMNS
    )

    assert frame["target_date"][0] == date(2026, 8, 7)
    assert frame["today_peak_supply_capacity"][0] == 123
    assert frame["today_peak_supply_reserve_margin_pct"][0] == 41.0
    assert frame["today_peak_supply_time_range"][0] == "19:00～20:00"


def test_extract_daily_summary_frame_keeps_latest_revision_per_date(conn) -> None:
    _register(
        conn,
        [
            _daily_summary_row(
                file_updated_at="2026/08/08 00:10 UPDATE",
                today_peak_supply_capacity="100",
            ),
            _daily_summary_row(
                file_updated_at="2026/08/08 06:00 UPDATE",
                today_peak_supply_capacity="200",
            ),
        ],
    )

    frame = extract_daily_summary_frame(
        conn, source_relation=SOURCE_RELATION, silver_columns=DAILY_SUMMARY_COLUMNS
    )

    assert frame.height == 1
    assert frame["today_peak_supply_capacity"][0] == 200


def test_extract_daily_summary_frame_treats_empty_string_as_null(conn) -> None:
    _register(conn, [_daily_summary_row(today_peak_supply_capacity="")])

    frame = extract_daily_summary_frame(
        conn, source_relation=SOURCE_RELATION, silver_columns=DAILY_SUMMARY_COLUMNS
    )

    assert frame["today_peak_supply_capacity"][0] is None


# ---------------------------------------------------------------------------
# extract_hourly_frame() / extract_interval5_frame() — unpivot + rejoin
# ---------------------------------------------------------------------------


def _full_width_row(
    target_date: str, file_updated_at: str, default: str = "0"
) -> dict[str, object]:
    row: dict[str, object] = {
        "target_date": target_date,
        "file_updated_at": file_updated_at,
        "ingestion_time": None,
    }
    for metric in HOURLY_METRICS:
        for h in range(HOURLY_ROW_COUNT):
            row[_hourly_column(h, metric)] = default
    for metric in INTERVAL5_METRICS:
        for s in range(INTERVAL5_ROW_COUNT):
            row[_interval5_column(s, metric)] = default
    return row


def test_extract_hourly_frame_unpivots_24_hours_and_rejoins_metrics(conn) -> None:
    row = _full_width_row("2026-08-07", "2026/08/08 00:10 UPDATE")
    row["hourly_00_00_actual_demand"] = "68"
    row["hourly_00_00_forecasted_demand"] = "64"
    row["hourly_00_00_usage_rate_pct"] = "61"
    row["hourly_00_00_supply_capacity"] = "111"
    _register(conn, [row])

    frame = extract_hourly_frame(conn, source_relation=SOURCE_RELATION)

    assert frame.height == 24
    hour0 = frame.filter(pl.col("hour_of_day") == 0)
    assert hour0["actual_demand"][0] == 68
    assert hour0["forecasted_demand"][0] == 64
    assert hour0["usage_rate_pct"][0] == 61.0
    assert hour0["supply_capacity"][0] == 111


def test_extract_hourly_frame_delivery_datetime_is_jst_converted_to_utc(conn) -> None:
    row = _full_width_row("2026-08-07", "2026/08/08 00:10 UPDATE")
    _register(conn, [row])

    frame = extract_hourly_frame(conn, source_relation=SOURCE_RELATION)

    hour0 = frame.filter(pl.col("hour_of_day") == 0)
    # 2026-08-07 00:00 JST == 2026-08-06 15:00 UTC
    assert str(hour0["delivery_datetime"][0]) == "2026-08-06 15:00:00+00:00"


def test_extract_interval5_frame_unpivots_288_slots_and_rejoins_metrics(conn) -> None:
    row = _full_width_row("2026-08-07", "2026/08/08 00:10 UPDATE")
    row["interval5_00_00_actual_demand"] = "71"
    row["interval5_00_00_solar_generation_actual"] = "0"
    row["interval5_23_55_actual_demand"] = "42"
    _register(conn, [row])

    frame = extract_interval5_frame(conn, source_relation=SOURCE_RELATION)

    assert frame.height == 288
    slot0 = frame.filter(pl.col("slot_index") == 0)
    assert slot0["actual_demand"][0] == 71
    slot287 = frame.filter(pl.col("slot_index") == 287)
    assert slot287["actual_demand"][0] == 42


def test_extract_hourly_frame_missing_values_become_null(conn) -> None:
    """A mid-day snapshot's not-yet-elapsed hours are legitimately empty
    strings in bronze (see 2025-12-12 in real data); they must not become 0."""
    row = _full_width_row("2026-08-07", "2026/08/08 00:10 UPDATE", default="")
    _register(conn, [row])

    frame = extract_hourly_frame(conn, source_relation=SOURCE_RELATION)

    assert frame["actual_demand"].null_count() == 24


# ---------------------------------------------------------------------------
# window predicate builders — same logic as OCCTO's, re-tested per module
# ---------------------------------------------------------------------------


def test_resolve_staged_target_date_range_returns_min_and_max():
    frame = pl.DataFrame({"target_date": [date(2026, 8, 1), date(2026, 8, 7)]})

    assert resolve_staged_target_date_range(frame) == (
        date(2026, 8, 1),
        date(2026, 8, 7),
    )


def test_resolve_staged_target_date_range_returns_none_for_empty_frame():
    frame = pl.DataFrame({"target_date": pl.Series([], dtype=pl.Date)})

    assert resolve_staged_target_date_range(frame) is None


def test_build_target_date_window_prefers_explicit_from_date():
    window = build_target_date_window(
        from_date=date(2026, 8, 1),
        to_date=date(2026, 8, 7),
        staged_range=(date(2020, 1, 1), date(2020, 1, 1)),
    )

    assert window == And(
        left=target_date_bound(GreaterThanOrEqual, date(2026, 8, 1)),
        right=target_date_bound(LessThanOrEqual, date(2026, 8, 7)),
    )


def test_build_target_date_window_falls_back_to_staged_range():
    window = build_target_date_window(
        from_date=None, to_date=None, staged_range=(date(2020, 4, 1), date(2020, 4, 3))
    )

    assert window == And(
        left=target_date_bound(GreaterThanOrEqual, date(2020, 4, 1)),
        right=target_date_bound(LessThanOrEqual, date(2020, 4, 3)),
    )


def test_build_target_date_window_returns_none_when_nothing_staged():
    assert (
        build_target_date_window(from_date=None, to_date=None, staged_range=None)
        is None
    )

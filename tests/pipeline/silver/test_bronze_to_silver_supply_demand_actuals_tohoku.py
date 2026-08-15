"""Unit tests for the Tohoku supply_demand_actuals bronze-to-silver transformation.

Modeled on tests/test_bronze_to_silver_power_usage_hokuriku.py: the
transform SQL is exercised against a locally registered relation instead
of iceberg_scan, so these tests need neither RustFS nor an Iceberg
catalog. Correctness against real live data (2026-08-14) was additionally
verified manually during development.
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import date, datetime

import duckdb
import polars as pl
import pytest
from pyiceberg.expressions import And, GreaterThanOrEqual, LessThanOrEqual

from pipeline.silver.bronze_to_silver_supply_demand_actuals_tohoku import (
    EXTRA_COLUMNS,
    build_target_date_window,
    extract_supply_demand_actuals_frame,
    resolve_staged_target_date_range,
    target_date_bound,
)

SOURCE_RELATION = "bronze_source"


@pytest.fixture
def conn() -> Iterator[duckdb.DuckDBPyConnection]:
    connection = duckdb.connect()
    yield connection
    connection.close()


def _bronze_row(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "target_date": "2026-08-14",
        "target_time": "00:00",
        "actual_demand_10k_kw": "676",
        "ingestion_time": datetime(2026, 8, 15, 3, 0, 0),
    }
    row.update(overrides)
    return row


def _register(conn: duckdb.DuckDBPyConnection, rows: list[dict[str, object]]) -> None:
    conn.register(SOURCE_RELATION, pl.DataFrame(rows))


def test_extract_frame_casts_and_derives_hour_of_day(conn) -> None:
    _register(conn, [_bronze_row(target_time="05:00")])

    frame = extract_supply_demand_actuals_frame(conn, source_relation=SOURCE_RELATION)

    assert frame["target_date"][0] == date(2026, 8, 14)
    assert frame["hour_of_day"][0] == 5
    assert frame["actual_demand_10k_kw"][0] == 676


def test_extract_frame_delivery_datetime_is_jst_converted_to_utc(conn) -> None:
    _register(conn, [_bronze_row(target_time="00:00")])

    frame = extract_supply_demand_actuals_frame(conn, source_relation=SOURCE_RELATION)

    # 2026-08-14 00:00 JST == 2026-08-13 15:00 UTC
    assert str(frame["delivery_datetime"][0]) == "2026-08-13 15:00:00+00:00"


def test_extra_columns_is_empty_for_tohoku():
    assert EXTRA_COLUMNS == ()


def test_extract_frame_keeps_latest_revision_per_hour(conn) -> None:
    _register(
        conn,
        [
            _bronze_row(
                actual_demand_10k_kw="100",
                ingestion_time=datetime(2026, 8, 15, 3, 0, 0),
            ),
            _bronze_row(
                actual_demand_10k_kw="200",
                ingestion_time=datetime(2026, 8, 15, 6, 0, 0),
            ),
        ],
    )

    frame = extract_supply_demand_actuals_frame(conn, source_relation=SOURCE_RELATION)

    assert frame.height == 1
    assert frame["actual_demand_10k_kw"][0] == 200


def test_extract_frame_treats_empty_string_as_null(conn) -> None:
    _register(conn, [_bronze_row(actual_demand_10k_kw="")])

    frame = extract_supply_demand_actuals_frame(conn, source_relation=SOURCE_RELATION)

    assert frame["actual_demand_10k_kw"][0] is None


def test_extract_frame_respects_target_date_filter(conn) -> None:
    _register(
        conn,
        [
            _bronze_row(target_date="2026-08-13"),
            _bronze_row(target_date="2026-08-14"),
        ],
    )

    frame = extract_supply_demand_actuals_frame(
        conn,
        source_relation=SOURCE_RELATION,
        from_date=date(2026, 8, 14),
        to_date=date(2026, 8, 14),
    )

    assert frame.height == 1
    assert frame["target_date"][0] == date(2026, 8, 14)


def test_resolve_staged_target_date_range_returns_min_and_max():
    frame = pl.DataFrame({"target_date": [date(2026, 8, 1), date(2026, 8, 7)]})

    assert resolve_staged_target_date_range(frame) == (
        date(2026, 8, 1),
        date(2026, 8, 7),
    )


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


def test_build_target_date_window_returns_none_when_nothing_staged():
    assert (
        build_target_date_window(from_date=None, to_date=None, staged_range=None)
        is None
    )

"""Unit tests for the Shikoku supply_demand_actuals bronze-to-silver transformation.

See test_bronze_to_silver_supply_demand_actuals_tohoku.py for the fuller
suite this mirrors; Shikoku's transform logic is otherwise identical, so
coverage here focuses on its one unique behavior: carrying the extra
supply_capacity_forecast_10k_kw column that Tohoku/Chugoku don't have.
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import date, datetime

import duckdb
import polars as pl
import pytest
from pyiceberg.expressions import And, GreaterThanOrEqual, LessThanOrEqual

from pipeline.silver.bronze_to_silver_supply_demand_actuals_shikoku import (
    EXTRA_COLUMNS,
    build_target_date_window,
    extract_supply_demand_actuals_frame,
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
        "actual_demand_10k_kw": "215",
        "supply_capacity_forecast_10k_kw": "295",
        "ingestion_time": datetime(2026, 8, 15, 3, 0, 0),
    }
    row.update(overrides)
    return row


def _register(conn: duckdb.DuckDBPyConnection, rows: list[dict[str, object]]) -> None:
    conn.register(SOURCE_RELATION, pl.DataFrame(rows))


def test_extra_columns_includes_supply_capacity_for_shikoku():
    assert EXTRA_COLUMNS == ("supply_capacity_forecast_10k_kw",)


def test_extract_frame_includes_supply_capacity_column(conn) -> None:
    _register(conn, [_bronze_row()])

    frame = extract_supply_demand_actuals_frame(conn, source_relation=SOURCE_RELATION)

    assert "supply_capacity_forecast_10k_kw" in frame.columns
    assert frame["supply_capacity_forecast_10k_kw"][0] == 295


def test_extract_frame_casts_and_derives_hour_of_day(conn) -> None:
    _register(conn, [_bronze_row(target_time="05:00")])

    frame = extract_supply_demand_actuals_frame(conn, source_relation=SOURCE_RELATION)

    assert frame["target_date"][0] == date(2026, 8, 14)
    assert frame["hour_of_day"][0] == 5
    assert frame["actual_demand_10k_kw"][0] == 215


def test_extract_frame_delivery_datetime_is_jst_converted_to_utc(conn) -> None:
    _register(conn, [_bronze_row(target_time="00:00")])

    frame = extract_supply_demand_actuals_frame(conn, source_relation=SOURCE_RELATION)

    # 2026-08-14 00:00 JST == 2026-08-13 15:00 UTC
    assert str(frame["delivery_datetime"][0]) == "2026-08-13 15:00:00+00:00"


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

"""Unit tests for the JEPX silver-to-gold daily aggregation.

The aggregation SQL runs against locally registered relations instead of
``iceberg_scan``, so these tests need neither RustFS nor an Iceberg catalog.
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, date, datetime
from decimal import Decimal

import duckdb
import polars as pl
import pytest

from pipeline.gold.silver_to_gold_jepx_spot_price import (
    EXPECTED_TIME_CODES_PER_DAY,
    build_daily_relation,
    count_delivery_dates,
    count_incomplete_days,
    count_staged_rows,
    extract_daily_frame,
    resolve_staged_delivery_range,
    summarize_incomplete_days,
)

AREA_RELATION = "silver_area"
BASE_RELATION = "silver_base"

AREAS = ("tokyo", "kyushu")


def _rows(
    *,
    delivery_date: date = date(2026, 4, 1),
    time_codes: int = EXPECTED_TIME_CODES_PER_DAY,
    area_price: float | dict[str, float] = 10.0,
    system_price: float = 10.0,
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    """Build matching area/base rows for one delivery date.

    ``area_price`` may be a single value applied to every area, or a mapping
    of area name to price.
    """
    prices = (
        area_price if isinstance(area_price, dict) else dict.fromkeys(AREAS, area_price)
    )
    area_rows: list[dict[str, object]] = []
    base_rows: list[dict[str, object]] = []
    for time_code in range(1, time_codes + 1):
        base_rows.append(
            {
                "delivery_date": delivery_date,
                "time_code": time_code,
                "system_price": Decimal(str(system_price)),
            }
        )
        for area_name, price in prices.items():
            area_rows.append(
                {
                    "delivery_date": delivery_date,
                    "time_code": time_code,
                    "area_name": area_name,
                    "area_price": Decimal(str(price)),
                }
            )
    return area_rows, base_rows


def _register(
    conn: duckdb.DuckDBPyConnection,
    area_rows: list[dict[str, object]],
    base_rows: list[dict[str, object]],
) -> None:
    conn.register(AREA_RELATION, pl.DataFrame(area_rows))
    conn.register(BASE_RELATION, pl.DataFrame(base_rows))


def _build(conn: duckdb.DuckDBPyConnection, **kwargs: object) -> None:
    build_daily_relation(
        conn,
        area_relation=AREA_RELATION,
        base_relation=BASE_RELATION,
        **kwargs,  # pyright: ignore[reportArgumentType]
    )


@pytest.fixture
def conn() -> Iterator[duckdb.DuckDBPyConnection]:
    connection = duckdb.connect()
    yield connection
    connection.close()


def test_one_row_per_delivery_date_and_area(conn) -> None:
    """The 48 time codes of a day collapse to one row per area."""
    # Arrange
    _register(conn, *_rows())

    # Act
    _build(conn)
    frame = extract_daily_frame(conn)

    # Assert
    assert frame.height == len(AREAS)
    assert sorted(frame["area_name"].to_list()) == sorted(AREAS)
    assert set(frame["time_code_count"].to_list()) == {EXPECTED_TIME_CODES_PER_DAY}


def test_price_statistics_describe_the_day(conn) -> None:
    """min/max/range/stddev come from the day's own time codes."""
    # Arrange
    area_rows, base_rows = _rows(time_codes=2)
    area_rows[0]["area_price"] = Decimal("8.00")  # tokyo, time_code 1
    area_rows[2]["area_price"] = Decimal("12.00")  # tokyo, time_code 2
    _register(conn, area_rows, base_rows)

    # Act
    _build(conn)
    tokyo = extract_daily_frame(conn).filter(pl.col("area_name") == "tokyo")

    # Assert
    assert tokyo["min_price"][0] == Decimal("8.000")
    assert tokyo["max_price"][0] == Decimal("12.000")
    assert tokyo["avg_price"][0] == Decimal("10.000")
    assert tokyo["intraday_range"][0] == Decimal("4.000")
    assert tokyo["stddev_price"][0] == Decimal("2.000")  # population, not sample


def test_stddev_is_zero_rather_than_null_for_a_single_time_code(conn) -> None:
    """stddev_samp would return NULL here and lose the row's meaning."""
    # Arrange
    _register(conn, *_rows(time_codes=1))

    # Act
    _build(conn)
    frame = extract_daily_frame(conn)

    # Assert
    assert set(frame["stddev_price"].to_list()) == {Decimal("0.000")}


def test_system_price_is_denormalized_onto_every_area_row(conn) -> None:
    """Averaging an intensive quantity across areas still returns it."""
    # Arrange
    _register(conn, *_rows(system_price=11.0))

    # Act
    _build(conn)
    frame = extract_daily_frame(conn)

    # Assert
    assert set(frame["avg_system_price"].to_list()) == {Decimal("11.000")}


def test_spread_is_measured_against_the_system_price(conn) -> None:
    """Area minus system, signed for the average and absolute for the max."""
    # Arrange
    _register(
        conn,
        *_rows(area_price={"tokyo": 12.0, "kyushu": 7.0}, system_price=10.0),
    )

    # Act
    _build(conn)
    frame = extract_daily_frame(conn)
    tokyo = frame.filter(pl.col("area_name") == "tokyo")
    kyushu = frame.filter(pl.col("area_name") == "kyushu")

    # Assert
    assert tokyo["avg_spread"][0] == Decimal("2.000")
    assert kyushu["avg_spread"][0] == Decimal("-3.000")
    assert kyushu["max_abs_spread"][0] == Decimal("3.000")


def test_split_counts_time_codes_that_differ_from_the_system_price(conn) -> None:
    """Market splitting is any non-zero gap, down to one price tick."""
    # Arrange
    area_rows, base_rows = _rows(time_codes=2, area_price=10.0, system_price=10.0)
    area_rows[0]["area_price"] = Decimal("10.01")  # tokyo, time_code 1: one tick
    _register(conn, area_rows, base_rows)

    # Act
    _build(conn)
    frame = extract_daily_frame(conn)

    # Assert
    tokyo = frame.filter(pl.col("area_name") == "tokyo")
    kyushu = frame.filter(pl.col("area_name") == "kyushu")
    assert tokyo["split_time_code_count"][0] == 1
    assert kyushu["split_time_code_count"][0] == 0


def test_spike_and_floor_counts_use_fixed_thresholds(conn) -> None:
    """A 50 yen spike and a 0.01 yen floor are counted per time code."""
    # Arrange
    area_rows, base_rows = _rows(time_codes=3)
    area_rows[0]["area_price"] = Decimal("60.00")  # tokyo, time_code 1
    area_rows[2]["area_price"] = Decimal("0.01")  # tokyo, time_code 2
    _register(conn, area_rows, base_rows)

    # Act
    _build(conn)
    tokyo = extract_daily_frame(conn).filter(pl.col("area_name") == "tokyo")

    # Assert
    assert tokyo["spike_time_code_count"][0] == 1
    assert tokyo["floor_time_code_count"][0] == 1


def test_fiscal_year_narrows_the_aggregated_range(conn) -> None:
    """Fiscal year is the only difference between daily and full runs."""
    # Arrange
    area_rows: list[dict[str, object]] = []
    base_rows: list[dict[str, object]] = []
    for day in (date(2025, 3, 31), date(2025, 4, 1), date(2026, 3, 31)):
        day_area, day_base = _rows(delivery_date=day, time_codes=1)
        area_rows.extend(day_area)
        base_rows.extend(day_base)
    _register(conn, area_rows, base_rows)

    # Act
    _build(conn, fiscal_year=2025)
    frame = extract_daily_frame(conn)

    # Assert
    assert sorted(set(frame["delivery_date"].to_list())) == [
        date(2025, 4, 1),
        date(2026, 3, 31),
    ]


def test_incomplete_days_are_written_but_reported(conn) -> None:
    """A short day stays in the table; the count is what makes it visible."""
    # Arrange
    _register(conn, *_rows(time_codes=47))

    # Act
    _build(conn)
    frame = extract_daily_frame(conn)

    # Assert
    assert frame.height == len(AREAS)  # not dropped
    assert count_incomplete_days(conn) == len(AREAS)
    assert summarize_incomplete_days(conn)[0] == (date(2026, 4, 1), "kyushu", 47)


def test_complete_days_report_no_gaps(conn) -> None:
    # Arrange
    _register(conn, *_rows())

    # Act
    _build(conn)

    # Assert
    assert count_incomplete_days(conn) == 0


def test_area_rows_without_a_matching_base_row_are_not_aggregated(conn) -> None:
    """The join is inner, and the missing slot surfaces as a short count."""
    # Arrange
    area_rows, base_rows = _rows(time_codes=2)
    base_rows.pop()  # drop time_code 2 from the base table only
    _register(conn, area_rows, base_rows)

    # Act
    _build(conn)
    frame = extract_daily_frame(conn)

    # Assert
    assert set(frame["time_code_count"].to_list()) == {1}
    assert count_incomplete_days(conn) == len(AREAS)


def test_staged_counts_describe_the_run(conn) -> None:
    """Row count and date count feed the run's result object."""
    # Arrange
    area_rows: list[dict[str, object]] = []
    base_rows: list[dict[str, object]] = []
    for day in (date(2026, 4, 1), date(2026, 4, 2)):
        day_area, day_base = _rows(delivery_date=day, time_codes=1)
        area_rows.extend(day_area)
        base_rows.extend(day_base)
    _register(conn, area_rows, base_rows)

    # Act
    _build(conn)

    # Assert
    assert count_staged_rows(conn) == 2 * len(AREAS)
    assert count_delivery_dates(conn) == 2


def test_staged_delivery_range_spans_the_aggregated_days(conn) -> None:
    # Arrange
    area_rows: list[dict[str, object]] = []
    base_rows: list[dict[str, object]] = []
    for day in (date(2026, 4, 1), date(2026, 4, 9)):
        day_area, day_base = _rows(delivery_date=day, time_codes=1)
        area_rows.extend(day_area)
        base_rows.extend(day_base)
    _register(conn, area_rows, base_rows)

    # Act
    _build(conn)

    # Assert
    assert resolve_staged_delivery_range(conn) == (date(2026, 4, 1), date(2026, 4, 9))


def test_staged_delivery_range_is_none_when_nothing_matched(conn) -> None:
    """An out-of-scope fiscal year leaves no window to replace."""
    # Arrange
    _register(conn, *_rows(time_codes=1))

    # Act
    _build(conn, fiscal_year=1999)

    # Assert
    assert resolve_staged_delivery_range(conn) is None
    assert count_staged_rows(conn) == 0


def test_gold_rows_carry_their_silver_lineage(conn) -> None:
    """source_data names the silver tables rather than an object key."""
    # Arrange
    _register(conn, *_rows(time_codes=1))

    # Act
    _build(conn)
    frame = extract_daily_frame(conn)

    # Assert
    assert set(frame["source_data"].to_list()) == {
        "silver.jepx_spot_price_area+silver.jepx_spot_price_base"
    }


def test_delivery_date_is_kept_as_a_date_not_a_timestamp(conn) -> None:
    """The gold grain is the JST business day, not an instant."""
    # Arrange
    _register(conn, *_rows(time_codes=1))

    # Act
    _build(conn)
    frame = extract_daily_frame(conn)

    # Assert
    assert frame["delivery_date"][0] == date(2026, 4, 1)
    assert not isinstance(frame["delivery_date"][0], datetime)
    assert datetime(2026, 4, 1, tzinfo=UTC).date() == frame["delivery_date"][0]

"""Unit tests for the JEPX silver-to-gold aggregations.

The aggregation SQL runs against locally registered relations instead of
``iceberg_scan``, so these tests need neither RustFS nor an Iceberg catalog.
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, date, datetime, time, timedelta, timezone
from decimal import Decimal

import duckdb
import polars as pl
import pytest
from pyiceberg.expressions import And, GreaterThanOrEqual, LessThan, LessThanOrEqual

from common.silver_write import column_bound
from pipeline.gold.silver_to_gold_jepx_spot_price import (
    AREA_SPREAD_STAGING_RELATION,
    DAILY_STAGING_RELATION,
    EXPECTED_TIME_CODES_PER_DAY,
    HOLIDAY_RELATION,
    build_area_spread_relation,
    build_daily_relation,
    build_period_profile_relation,
    build_profile_month_window,
    count_delivery_dates,
    count_incomplete_days,
    count_staged_rows,
    extract_area_spread_frame,
    extract_daily_frame,
    extract_period_profile_frame,
    register_holidays,
    resolve_scanned_date_range,
    resolve_staged_delivery_range,
    summarize_incomplete_days,
)

AREA_RELATION = "silver_area"
BASE_RELATION = "silver_base"

AREAS = ("tokyo", "kyushu")

# Time code 1 starts at 00:00 JST, which is 15:00 UTC the previous day. The
# silver area table carries this alongside the business date and time code.
JST = timezone(timedelta(hours=9))


def _slot_start(delivery_date: date, time_code: int) -> datetime:
    """Build the UTC instant a time code starts at, as silver stores it."""
    start = datetime.combine(delivery_date, time.min, tzinfo=JST)
    return (start + timedelta(minutes=30 * (time_code - 1))).astimezone(UTC)


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
                    "delivery_datetime": _slot_start(delivery_date, time_code),
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
    assert count_staged_rows(conn, DAILY_STAGING_RELATION) == 2 * len(AREAS)
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
    assert count_staged_rows(conn, DAILY_STAGING_RELATION) == 0


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


# --- Period profile ----------------------------------------------------------
#
# The profile keeps the time code and collapses the dates, which is the shape
# that shows the intraday curve. Month is the finest time axis the plan calls
# for: a caller can roll months into seasons or eras, but cannot recover months
# from a table that only stored seasons.


def _build_profile(conn: duckdb.DuckDBPyConnection, **kwargs: object) -> None:
    build_period_profile_relation(
        conn,
        area_relation=AREA_RELATION,
        base_relation=BASE_RELATION,
        holiday_relation=HOLIDAY_RELATION,
        **kwargs,  # pyright: ignore[reportArgumentType]
    )


def _register_days(
    conn: duckdb.DuckDBPyConnection,
    days: list[date],
    *,
    time_codes: int = 1,
    area_price: float | dict[str, float] = 10.0,
) -> None:
    """Register one row per area per time code for each of the given days."""
    area_rows: list[dict[str, object]] = []
    base_rows: list[dict[str, object]] = []
    for day in days:
        day_area, day_base = _rows(
            delivery_date=day, time_codes=time_codes, area_price=area_price
        )
        area_rows.extend(day_area)
        base_rows.extend(day_base)
    _register(conn, area_rows, base_rows)
    register_holidays(conn, from_date=min(days), to_date=max(days))


def test_profile_keeps_the_time_code_and_collapses_the_dates(conn) -> None:
    """Two dates in the same month become one row per slot, area and day type."""
    # Arrange: 2026-04-01 and 2026-04-02 are both weekdays (Wed, Thu)
    _register_days(conn, [date(2026, 4, 1), date(2026, 4, 2)], time_codes=2)

    # Act
    _build_profile(conn)
    frame = extract_period_profile_frame(conn)

    # Assert
    assert frame.height == 2 * len(AREAS)  # 2 time codes x 2 areas, one month
    assert set(frame["profile_month"].to_list()) == {date(2026, 4, 1)}
    assert set(frame["observation_count"].to_list()) == {2}


def test_profile_month_is_the_first_of_the_month(conn) -> None:
    """The month is stored as a date so the window replace is a range check."""
    # Arrange
    _register_days(conn, [date(2026, 4, 17)])

    # Act
    _build_profile(conn)
    frame = extract_period_profile_frame(conn)

    # Assert
    assert set(frame["profile_month"].to_list()) == {date(2026, 4, 1)}


def test_profile_separates_months(conn) -> None:
    # Arrange
    _register_days(conn, [date(2026, 4, 30), date(2026, 5, 1)])

    # Act
    _build_profile(conn)
    frame = extract_period_profile_frame(conn)

    # Assert
    assert sorted(set(frame["profile_month"].to_list())) == [
        date(2026, 4, 1),
        date(2026, 5, 1),
    ]


@pytest.mark.parametrize(
    ("day", "expected"),
    [
        pytest.param(date(2026, 4, 17), "weekday", id="friday"),
        pytest.param(date(2026, 4, 18), "holiday", id="saturday"),
        pytest.param(date(2026, 4, 19), "holiday", id="sunday"),
        pytest.param(date(2026, 4, 29), "holiday", id="showa-day-on-a-wednesday"),
    ],
)
def test_day_type_covers_weekends_and_national_holidays(conn, day, expected) -> None:
    """A national holiday on a weekday must not land in the weekday profile."""
    # Arrange
    _register_days(conn, [day])

    # Act
    _build_profile(conn)
    frame = extract_period_profile_frame(conn)

    # Assert
    assert set(frame["day_type"].to_list()) == {expected}


def test_day_type_splits_the_same_month_in_two(conn) -> None:
    """Weekday and holiday curves are aggregated separately, not blended."""
    # Arrange: Friday and Saturday of the same week
    _register_days(conn, [date(2026, 4, 17), date(2026, 4, 18)])

    # Act
    _build_profile(conn)
    frame = extract_period_profile_frame(conn)

    # Assert
    assert sorted(set(frame["day_type"].to_list())) == ["holiday", "weekday"]
    assert set(frame["observation_count"].to_list()) == {1}


def test_profile_counts_are_stored_rather_than_rates(conn) -> None:
    """Counts roll up across months; a stored percentage would not."""
    # Arrange: two days, one of which sits at the floor
    _register_days(conn, [date(2026, 4, 1)], area_price=0.01)
    frame_floor = None

    # Act
    _build_profile(conn)
    frame_floor = extract_period_profile_frame(conn)

    # Assert
    assert set(frame_floor["floor_observation_count"].to_list()) == {1}
    assert set(frame_floor["observation_count"].to_list()) == {1}
    assert "floor_rate_pct" not in frame_floor.columns


def test_profile_averages_prices_across_the_dates(conn) -> None:
    """The curve is the mean of each slot over the month's matching days."""
    # Arrange
    area_rows: list[dict[str, object]] = []
    base_rows: list[dict[str, object]] = []
    for day, price in ((date(2026, 4, 1), 8.0), (date(2026, 4, 2), 12.0)):
        day_area, day_base = _rows(delivery_date=day, time_codes=1, area_price=price)
        area_rows.extend(day_area)
        base_rows.extend(day_base)
    _register(conn, area_rows, base_rows)
    register_holidays(conn, from_date=date(2026, 4, 1), to_date=date(2026, 4, 2))

    # Act
    _build_profile(conn)
    frame = extract_period_profile_frame(conn)

    # Assert
    assert set(frame["avg_price"].to_list()) == {Decimal("10.000")}
    assert set(frame["observation_count"].to_list()) == {2}


def test_profile_fiscal_year_narrows_the_months(conn) -> None:
    # Arrange
    _register_days(conn, [date(2025, 3, 31), date(2025, 4, 1), date(2026, 3, 31)])

    # Act
    _build_profile(conn, fiscal_year=2025)
    frame = extract_period_profile_frame(conn)

    # Assert
    assert sorted(set(frame["profile_month"].to_list())) == [
        date(2025, 4, 1),
        date(2026, 3, 1),
    ]


def test_profile_month_window_covers_the_fiscal_year(conn) -> None:
    """April through the following March, as a range on profile_month."""
    # Arrange / Act
    window = build_profile_month_window(fiscal_year=2026, staged_range=None)

    # Assert
    assert window == And(
        left=column_bound("profile_month", GreaterThanOrEqual, date(2026, 4, 1)),
        right=column_bound("profile_month", LessThan, date(2027, 4, 1)),
    )


def test_profile_month_window_falls_back_to_the_staged_months() -> None:
    # Arrange / Act
    window = build_profile_month_window(
        fiscal_year=None, staged_range=(date(2005, 4, 1), date(2026, 8, 1))
    )

    # Assert
    assert window == And(
        left=column_bound("profile_month", GreaterThanOrEqual, date(2005, 4, 1)),
        right=column_bound("profile_month", LessThanOrEqual, date(2026, 8, 1)),
    )


def test_profile_month_window_is_none_when_nothing_was_staged() -> None:
    assert build_profile_month_window(fiscal_year=None, staged_range=None) is None


def test_register_holidays_keeps_only_dates_inside_the_range(conn) -> None:
    """The relation covers the scanned span, not whole calendar years."""
    # Arrange / Act
    register_holidays(conn, from_date=date(2026, 4, 28), to_date=date(2026, 4, 30))
    rows = conn.execute(f"SELECT holiday_date FROM {HOLIDAY_RELATION}").fetchall()

    # Assert
    assert [row[0] for row in rows] == [date(2026, 4, 29)]  # Showa Day only


def test_scanned_date_range_reports_what_a_run_will_read(conn) -> None:
    """The range drives which years the holiday calendar has to cover."""
    # Arrange
    _register_days(conn, [date(2026, 4, 1), date(2026, 5, 9)])

    # Act / Assert
    assert resolve_scanned_date_range(conn, base_relation=BASE_RELATION) == (
        date(2026, 4, 1),
        date(2026, 5, 9),
    )


def test_scanned_date_range_is_none_for_an_empty_scope(conn) -> None:
    # Arrange
    _register_days(conn, [date(2026, 4, 1)])

    # Act / Assert
    assert (
        resolve_scanned_date_range(conn, base_relation=BASE_RELATION, fiscal_year=1999)
        is None
    )


# --- Area spread -------------------------------------------------------------
#
# The only table here that does not aggregate. It is the pre-joined interval
# fact the dashboard reads, so the heatmap does not have to join the silver
# area and base tables on every query.


def _build_spread(conn: duckdb.DuckDBPyConnection, **kwargs: object) -> None:
    build_area_spread_relation(
        conn,
        area_relation=AREA_RELATION,
        base_relation=BASE_RELATION,
        **kwargs,  # pyright: ignore[reportArgumentType]
    )


def test_area_spread_keeps_one_row_per_slot_and_area(conn) -> None:
    """No aggregation: the grain matches the silver area table exactly."""
    # Arrange
    _register(conn, *_rows(time_codes=3))

    # Act
    _build_spread(conn)
    frame = extract_area_spread_frame(conn)

    # Assert
    assert frame.height == 3 * len(AREAS)
    assert sorted(frame["time_code"].unique().to_list()) == [1, 2, 3]


def test_area_spread_carries_both_prices_and_their_gap(conn) -> None:
    """The prices ride along so a chart never has to join back to silver."""
    # Arrange
    _register(
        conn,
        *_rows(
            time_codes=1, area_price={"tokyo": 12.0, "kyushu": 7.0}, system_price=10.0
        ),
    )

    # Act
    _build_spread(conn)
    frame = extract_area_spread_frame(conn)
    tokyo = frame.filter(pl.col("area_name") == "tokyo")
    kyushu = frame.filter(pl.col("area_name") == "kyushu")

    # Assert
    assert tokyo["area_price"][0] == Decimal("12.000")
    assert tokyo["system_price"][0] == Decimal("10.000")
    assert tokyo["spread"][0] == Decimal("2.000")
    assert kyushu["spread"][0] == Decimal("-3.000")


def test_area_spread_flags_a_one_tick_difference_as_split(conn) -> None:
    """The same threshold the aggregates count with, applied in one place."""
    # Arrange
    area_rows, base_rows = _rows(time_codes=2, area_price=10.0, system_price=10.0)
    area_rows[0]["area_price"] = Decimal("10.01")  # tokyo, time_code 1
    _register(conn, area_rows, base_rows)

    # Act
    _build_spread(conn)
    frame = extract_area_spread_frame(conn)

    # Assert
    split = frame.filter(pl.col("is_split"))
    assert split.height == 1
    assert split["area_name"][0] == "tokyo"
    assert split["time_code"][0] == 1


def test_area_spread_is_not_split_when_the_prices_match(conn) -> None:
    # Arrange
    _register(conn, *_rows(time_codes=2, area_price=10.0, system_price=10.0))

    # Act
    _build_spread(conn)
    frame = extract_area_spread_frame(conn)

    # Assert
    assert set(frame["is_split"].to_list()) == {False}


def test_area_spread_fiscal_year_narrows_the_rows(conn) -> None:
    # Arrange
    area_rows: list[dict[str, object]] = []
    base_rows: list[dict[str, object]] = []
    for day in (date(2025, 3, 31), date(2025, 4, 1)):
        day_area, day_base = _rows(delivery_date=day, time_codes=1)
        area_rows.extend(day_area)
        base_rows.extend(day_base)
    _register(conn, area_rows, base_rows)

    # Act
    _build_spread(conn, fiscal_year=2025)
    frame = extract_area_spread_frame(conn)

    # Assert
    assert set(frame["delivery_date"].to_list()) == {date(2025, 4, 1)}


def test_area_spread_row_count_matches_the_daily_time_code_counts(conn) -> None:
    """The two tables are two views of the same joined rows."""
    # Arrange
    _register(conn, *_rows(time_codes=5))

    # Act
    _build(conn)
    _build_spread(conn)

    # Assert
    daily_total = sum(extract_daily_frame(conn)["time_code_count"].to_list())
    assert count_staged_rows(conn, AREA_SPREAD_STAGING_RELATION) == daily_total

"""Unit tests for the dashboard's gold-layer reads.

Relations are registered locally instead of scanning Iceberg, so these need
neither RustFS nor a catalog. The app layer is not covered here -- what is
worth pinning down is the SQL, especially the two places where a naive
aggregate would be quietly wrong.
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import date

import duckdb
import polars as pl
import pytest

from dashboard.queries import (
    fetch_areas,
    fetch_fiscal_years,
    fetch_intraday_profile,
    fetch_price_heatmap,
    fetch_split_rate_matrix,
    fiscal_year_of,
    iceberg,
    slot_label,
)

DAILY_RELATION = "gold_daily"
PROFILE_RELATION = "gold_profile"
AREA_SPREAD_RELATION = "gold_area_spread"


@pytest.fixture
def conn() -> Iterator[duckdb.DuckDBPyConnection]:
    connection = duckdb.connect()
    yield connection
    connection.close()


def _register_daily(conn: duckdb.DuckDBPyConnection, rows: list[dict]) -> None:
    conn.register(DAILY_RELATION, pl.DataFrame(rows))


def _daily_row(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "delivery_date": date(2026, 4, 1),
        "area_name": "tokyo",
        "split_time_code_count": 0,
        "time_code_count": 48,
    }
    row.update(overrides)
    return row


def test_iceberg_builds_a_scan_expression() -> None:
    assert iceberg("s3://bucket/gold/t") == "iceberg_scan('s3://bucket/gold/t')"


@pytest.mark.parametrize(
    ("time_code", "expected"),
    [
        pytest.param(1, "00:00", id="first-slot-starts-at-midnight"),
        pytest.param(2, "00:30", id="second-slot"),
        pytest.param(21, "10:00", id="mid-morning"),
        pytest.param(48, "23:30", id="last-slot"),
    ],
)
def test_slot_label_reads_as_the_jst_clock_time(time_code: int, expected: str) -> None:
    assert slot_label(time_code) == expected


@pytest.mark.parametrize(
    ("delivery_date", "expected"),
    [
        pytest.param(date(2026, 3, 31), 2025, id="march-belongs-to-the-previous-year"),
        pytest.param(date(2026, 4, 1), 2026, id="april-opens-the-year"),
        pytest.param(date(2026, 12, 31), 2026, id="december"),
    ],
)
def test_fiscal_year_expression_splits_on_april(
    conn, delivery_date: date, expected: int
) -> None:
    """The dashboard must agree with the pipeline about where a year starts."""
    # Arrange
    conn.register("d", pl.DataFrame({"delivery_date": [delivery_date]}))

    # Act
    result = conn.execute(f"SELECT {fiscal_year_of('delivery_date')} FROM d").fetchone()

    # Assert
    assert result is not None
    assert int(result[0]) == expected


def test_fetch_areas_lists_areas_once(conn) -> None:
    # Arrange
    _register_daily(
        conn,
        [
            _daily_row(area_name="tokyo"),
            _daily_row(area_name="tokyo", delivery_date=date(2026, 4, 2)),
            _daily_row(area_name="kyushu"),
        ],
    )

    # Act / Assert
    assert fetch_areas(conn, daily_relation=DAILY_RELATION) == ["kyushu", "tokyo"]


def test_fetch_fiscal_years_is_newest_first(conn) -> None:
    # Arrange
    _register_daily(
        conn,
        [
            _daily_row(delivery_date=date(2024, 5, 1)),
            _daily_row(delivery_date=date(2026, 5, 1)),
            _daily_row(delivery_date=date(2025, 5, 1)),
        ],
    )

    # Act / Assert
    assert fetch_fiscal_years(conn, daily_relation=DAILY_RELATION) == [2026, 2025, 2024]


def test_split_rate_weights_by_slot_count_not_by_day(conn) -> None:
    """A short day must not count as much as a full one.

    Averaging the per-day percentages would give (100 + 0) / 2 = 50%. The
    honest answer weights by slots: 1 split slot out of 49.
    """
    # Arrange
    _register_daily(
        conn,
        [
            _daily_row(split_time_code_count=1, time_code_count=1),
            _daily_row(
                delivery_date=date(2026, 4, 2),
                split_time_code_count=0,
                time_code_count=48,
            ),
        ],
    )

    # Act
    frame = fetch_split_rate_matrix(conn, daily_relation=DAILY_RELATION)

    # Assert
    assert frame.height == 1
    assert frame["split_count"][0] == 1
    assert frame["slot_count"][0] == 49
    assert frame["split_pct"][0] == pytest.approx(100 * 1 / 49)


def test_split_rate_groups_by_fiscal_year_and_area(conn) -> None:
    # Arrange
    _register_daily(
        conn,
        [
            _daily_row(area_name="tokyo", split_time_code_count=48),
            _daily_row(area_name="kyushu", split_time_code_count=0),
            _daily_row(delivery_date=date(2026, 3, 31), area_name="tokyo"),
        ],
    )

    # Act
    frame = fetch_split_rate_matrix(conn, daily_relation=DAILY_RELATION)

    # Assert
    assert sorted(frame["fiscal_year"].unique().to_list()) == [2025, 2026]
    tokyo_2026 = frame.filter(
        (pl.col("fiscal_year") == 2026) & (pl.col("area_name") == "tokyo")
    )
    assert tokyo_2026["split_pct"][0] == pytest.approx(100.0)


def test_price_heatmap_narrows_to_one_year_and_area(conn) -> None:
    # Arrange
    conn.register(
        AREA_SPREAD_RELATION,
        pl.DataFrame(
            [
                {
                    "delivery_date": date(2026, 4, 1),
                    "time_code": 1,
                    "area_name": "tokyo",
                    "area_price": 10.0,
                    "spread": 0.0,
                    "is_split": False,
                },
                {
                    "delivery_date": date(2026, 4, 1),
                    "time_code": 1,
                    "area_name": "kyushu",
                    "area_price": 8.0,
                    "spread": -2.0,
                    "is_split": True,
                },
                {
                    "delivery_date": date(2025, 4, 1),
                    "time_code": 1,
                    "area_name": "tokyo",
                    "area_price": 9.0,
                    "spread": 0.0,
                    "is_split": False,
                },
            ]
        ),
    )

    # Act
    frame = fetch_price_heatmap(
        conn,
        area_spread_relation=AREA_SPREAD_RELATION,
        fiscal_year=2026,
        area_name="tokyo",
    )

    # Assert
    assert frame.height == 1
    assert frame["area_price"][0] == 10.0


def _profile_row(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "profile_month": date(2026, 4, 1),
        "area_name": "kyushu",
        "day_type": "weekday",
        "time_code": 1,
        "avg_price": 10.0,
        "observation_count": 20,
    }
    row.update(overrides)
    return row


def test_intraday_profile_weights_months_by_observation_count(conn) -> None:
    """Collapsing months to a year is a weighted mean, not a mean of means.

    A plain mean of 10.0 and 20.0 is 15.0. Weighted by 20 and 5 observations
    it is 12.0, which is what the underlying days actually average.
    """
    # Arrange
    conn.register(
        PROFILE_RELATION,
        pl.DataFrame(
            [
                _profile_row(avg_price=10.0, observation_count=20),
                _profile_row(
                    profile_month=date(2026, 5, 1),
                    avg_price=20.0,
                    observation_count=5,
                ),
            ]
        ),
    )

    # Act
    frame = fetch_intraday_profile(
        conn,
        profile_relation=PROFILE_RELATION,
        area_name="kyushu",
        day_type="weekday",
        fiscal_years=[2026],
    )

    # Assert
    assert frame.height == 1
    assert frame["avg_price"][0] == pytest.approx(12.0)
    assert frame["observation_count"][0] == 25


def test_intraday_profile_filters_area_day_type_and_year(conn) -> None:
    # Arrange
    conn.register(
        PROFILE_RELATION,
        pl.DataFrame(
            [
                _profile_row(),
                _profile_row(area_name="tokyo"),
                _profile_row(day_type="holiday"),
                _profile_row(profile_month=date(2025, 4, 1)),
            ]
        ),
    )

    # Act
    frame = fetch_intraday_profile(
        conn,
        profile_relation=PROFILE_RELATION,
        area_name="kyushu",
        day_type="weekday",
        fiscal_years=[2026],
    )

    # Assert
    assert frame.height == 1
    assert frame["fiscal_year"][0] == 2026


def test_intraday_profile_returns_an_empty_frame_without_years(conn) -> None:
    """Selecting no year must not build an `IN ()` clause."""
    # Arrange
    conn.register(PROFILE_RELATION, pl.DataFrame([_profile_row()]))

    # Act
    frame = fetch_intraday_profile(
        conn,
        profile_relation=PROFILE_RELATION,
        area_name="kyushu",
        day_type="weekday",
        fiscal_years=[],
    )

    # Assert
    assert frame.is_empty()
    assert "avg_price" in frame.columns

"""Unit tests for the OCCTO bronze-to-silver transformation.

Modeled on tests/test_bronze_to_silver_jepx_spot_price.py: the transform
SQL is exercised against a locally registered relation instead of
iceberg_scan, so these tests need neither RustFS nor an Iceberg catalog.
"""

from __future__ import annotations

import csv
from collections.abc import Iterator
from datetime import UTC, date, datetime
from pathlib import Path

import duckdb
import polars as pl
import pytest
from pyiceberg.expressions import And, GreaterThanOrEqual, LessThanOrEqual

from pipeline.silver.bronze_to_silver_occto_unit_generation import (
    STAGING_RELATION,
    TIMESLOT_COLUMNS,
    build_staging_relation,
    build_target_date_window,
    count_dropped_rows,
    extract_unit_generation_frame,
    resolve_staged_target_date_range,
    summarize_daily_amount_mismatches,
    summarize_violations,
    target_date_bound,
)

ROOT = Path(__file__).resolve().parents[1]
BRONZE_SCHEMA = (
    ROOT / "configuration/iceberg/schema/bronze/occto_unit_generation_actuals.csv"
)
SOURCE_RELATION = "bronze_source"


def _bronze_row(**overrides: object) -> dict[str, object]:
    """Build one bronze-shaped row. All source columns are strings."""
    row: dict[str, object] = {
        "power_plant_code": "10001",
        "area": "01",
        "power_plant_name": "テスト発電所",
        "unit_name": "1号機",
        "power_generation_method_and_fuel_type": "火力・LNG",
        "target_date": "2026/08/07",
        "daily_amount": "0",
        "updated_datetime": "2026/08/07 15:30:00",
        "source_data": "raw/occto/unit_generation/target_date=2026-08-07/file.csv",
        "status": "new",
        "ingestion_time": datetime(2026, 8, 7, 16, 0, 0, tzinfo=UTC),
        "execution_id": "exec-1",
    }
    for column in TIMESLOT_COLUMNS:
        row[column] = "0"
    row.update(overrides)
    return row


STRING_COLUMNS = (
    "power_plant_code",
    "area",
    "power_plant_name",
    "unit_name",
    "power_generation_method_and_fuel_type",
    "target_date",
    "daily_amount",
    "updated_datetime",
    "source_data",
    "status",
    "execution_id",
    *TIMESLOT_COLUMNS,
)


def _register_bronze(
    conn: duckdb.DuckDBPyConnection, rows: list[dict[str, object]]
) -> None:
    """Register rows as a relation that stands in for the bronze table.

    Every bronze column is cast to Utf8 explicitly: a test row with one of
    them set to None and no other row to infer a type from would otherwise
    make polars type the whole column as Null (or, worse, Int64), which the
    real bronze Iceberg table -- every column declared string in its schema
    CSV -- never does.
    """
    frame = pl.DataFrame(rows).with_columns(
        [pl.col(column).cast(pl.Utf8) for column in STRING_COLUMNS]
    )
    conn.register(SOURCE_RELATION, frame)


def _staged(conn: duckdb.DuckDBPyConnection) -> pl.DataFrame:
    return conn.execute(f"SELECT * FROM {STAGING_RELATION}").pl()


@pytest.fixture
def conn() -> Iterator[duckdb.DuckDBPyConnection]:
    """In-memory DuckDB connection (icu is statically linked, no install needed)."""
    connection = duckdb.connect()
    yield connection
    connection.close()


def _bronze_timeslot_column_order() -> list[str]:
    with BRONZE_SCHEMA.open(encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    return [row["name"] for row in rows if row["name"].startswith("timeslot_")]


def test_timeslot_columns_has_48_entries():
    assert len(TIMESLOT_COLUMNS) == 48


def test_timeslot_columns_starts_at_00_30_and_ends_at_24_00():
    assert TIMESLOT_COLUMNS[0] == "timeslot_00_30"
    assert TIMESLOT_COLUMNS[-1] == "timeslot_24_00"


def test_timeslot_columns_has_no_duplicates():
    assert len(set(TIMESLOT_COLUMNS)) == 48


def test_timeslot_columns_matches_bronze_schema_csv_order():
    """This is the load-bearing check: time_code is defined as the column's
    *position*, not parsed from its name, so silence here would mean every
    slot silently shifts if the bronze CSV's column order ever changes."""
    assert list(TIMESLOT_COLUMNS) == _bronze_timeslot_column_order()


# ---------------------------------------------------------------------------
# build_staging_relation() — casting and deduplication (Step 4-3)
# ---------------------------------------------------------------------------


def test_target_date_parses_slash_separated_format(conn) -> None:
    """target_date_d stays a TIMESTAMP through staging, same as JEPX's
    delivery_date_d; casting to DATE happens only in the final output
    projection added in a later step."""
    _register_bronze(conn, [_bronze_row(target_date="2026/08/07")])

    build_staging_relation(conn, source_relation=SOURCE_RELATION)
    frame = _staged(conn)

    assert frame["target_date_d"][0] == datetime(2026, 8, 7)


def test_target_date_parses_hyphenated_format(conn) -> None:
    _register_bronze(conn, [_bronze_row(target_date="2026-08-07")])

    build_staging_relation(conn, source_relation=SOURCE_RELATION)
    frame = _staged(conn)

    assert frame["target_date_d"][0] == datetime(2026, 8, 7)


def test_updated_datetime_parses_slash_separated_datetime(conn) -> None:
    _register_bronze(conn, [_bronze_row(updated_datetime="2026/08/07 15:30:28")])

    build_staging_relation(conn, source_relation=SOURCE_RELATION)
    frame = _staged(conn)

    assert frame["updated_datetime_ts"][0] == datetime(2026, 8, 7, 15, 30, 28)


def test_unit_name_empty_string_is_not_null(conn) -> None:
    """A blank unit_name is a real, distinct identifier value (Phase 0
    confirmed this occurs for real plants), not a missing value."""
    _register_bronze(conn, [_bronze_row(unit_name="")])

    build_staging_relation(conn, source_relation=SOURCE_RELATION)
    frame = _staged(conn)

    assert frame["unit_name"][0] == ""
    assert frame["unit_name"].null_count() == 0


def test_unit_name_null_is_normalized_to_empty_string(conn) -> None:
    _register_bronze(conn, [_bronze_row(unit_name=None)])

    build_staging_relation(conn, source_relation=SOURCE_RELATION)
    frame = _staged(conn)

    assert frame["unit_name"][0] == ""


def test_timeslot_values_parsed_with_thousand_separators(conn) -> None:
    _register_bronze(conn, [_bronze_row(**{"timeslot_00_30": "1,234,567"})])

    build_staging_relation(conn, source_relation=SOURCE_RELATION)
    frame = _staged(conn)

    assert frame["timeslot_00_30"][0] == 1_234_567


def test_daily_amount_parsed_with_thousand_separators(conn) -> None:
    _register_bronze(conn, [_bronze_row(daily_amount="12,345,678")])

    build_staging_relation(conn, source_relation=SOURCE_RELATION)
    frame = _staged(conn)

    assert frame["daily_amount"][0] == 12_345_678


def test_keeps_latest_row_per_natural_key(conn) -> None:
    """The newer updated_datetime wins for the same
    (power_plant_code, unit_name, target_date) key."""
    older = _bronze_row(
        **{"timeslot_00_30": "100"}, updated_datetime="2026/08/07 09:00:00"
    )
    newer = _bronze_row(
        **{"timeslot_00_30": "200"}, updated_datetime="2026/08/07 15:30:00"
    )
    _register_bronze(conn, [older, newer])

    build_staging_relation(conn, source_relation=SOURCE_RELATION)
    frame = _staged(conn)

    assert frame.height == 1
    assert frame["timeslot_00_30"][0] == 200


def test_dedup_is_scoped_per_unit_not_just_per_plant(conn) -> None:
    """Two distinct units at the same plant/date must both survive dedup;
    this is the Phase 3-4 bug this natural key exists to prevent."""
    unit_1 = _bronze_row(unit_name="1号機")
    unit_2 = _bronze_row(unit_name="2号機")
    _register_bronze(conn, [unit_1, unit_2])

    build_staging_relation(conn, source_relation=SOURCE_RELATION)
    frame = _staged(conn)

    assert frame.height == 2
    assert set(frame["unit_name"].to_list()) == {"1号機", "2号機"}


def test_dedup_falls_back_to_ingestion_time_when_updated_datetime_tied(conn) -> None:
    older = _bronze_row(
        **{"timeslot_00_30": "100"},
        ingestion_time=datetime(2026, 8, 7, 16, 0, 0, tzinfo=UTC),
    )
    newer = _bronze_row(
        **{"timeslot_00_30": "200"},
        ingestion_time=datetime(2026, 8, 7, 17, 0, 0, tzinfo=UTC),
    )
    _register_bronze(conn, [older, newer])

    build_staging_relation(conn, source_relation=SOURCE_RELATION)
    frame = _staged(conn)

    assert frame.height == 1
    assert frame["timeslot_00_30"][0] == 200


# ---------------------------------------------------------------------------
# validated / violations (Step 4-4)
# ---------------------------------------------------------------------------


def test_violations_empty_for_a_valid_row(conn) -> None:
    _register_bronze(conn, [_bronze_row()])

    build_staging_relation(conn, source_relation=SOURCE_RELATION)
    frame = _staged(conn)

    assert frame["violations"][0].to_list() == []


def test_violations_flags_null_target_date(conn) -> None:
    _register_bronze(conn, [_bronze_row(target_date=None)])

    build_staging_relation(conn, source_relation=SOURCE_RELATION)
    frame = _staged(conn)

    assert "target_date_null" in frame["violations"][0].to_list()


def test_violations_flags_null_power_plant_code(conn) -> None:
    _register_bronze(conn, [_bronze_row(power_plant_code=None)])

    build_staging_relation(conn, source_relation=SOURCE_RELATION)
    frame = _staged(conn)

    assert "power_plant_code_null" in frame["violations"][0].to_list()


def test_violations_flags_negative_generation_value(conn) -> None:
    _register_bronze(conn, [_bronze_row(**{"timeslot_00_30": "-100"})])

    build_staging_relation(conn, source_relation=SOURCE_RELATION)
    frame = _staged(conn)

    assert "generation_negative" in frame["violations"][0].to_list()


def test_violations_flags_all_timeslots_null(conn) -> None:
    all_null = {column: None for column in TIMESLOT_COLUMNS}
    _register_bronze(conn, [_bronze_row(**all_null)])

    build_staging_relation(conn, source_relation=SOURCE_RELATION)
    frame = _staged(conn)

    assert "all_timeslots_null" in frame["violations"][0].to_list()


def test_violations_does_not_flag_all_timeslots_null_when_one_is_present(conn) -> None:
    _register_bronze(conn, [_bronze_row(**{"timeslot_00_30": "0"})])

    build_staging_relation(conn, source_relation=SOURCE_RELATION)
    frame = _staged(conn)

    assert "all_timeslots_null" not in frame["violations"][0].to_list()


def test_count_dropped_rows_counts_rows_with_violations(conn) -> None:
    _register_bronze(
        conn,
        [
            _bronze_row(),
            _bronze_row(power_plant_code=None),
            _bronze_row(unit_name="別ユニット", target_date=None),
        ],
    )

    build_staging_relation(conn, source_relation=SOURCE_RELATION)

    assert count_dropped_rows(conn) == 2


def test_summarize_violations_counts_each_reason(conn) -> None:
    _register_bronze(
        conn,
        [
            _bronze_row(power_plant_code=None),
            _bronze_row(unit_name="別ユニット", power_plant_code=None),
            _bronze_row(unit_name="別ユニット2", target_date=None),
        ],
    )

    build_staging_relation(conn, source_relation=SOURCE_RELATION)
    summary = summarize_violations(conn)

    assert summary["power_plant_code_null"] == 2
    assert summary["target_date_null"] == 1


# ---------------------------------------------------------------------------
# extract_unit_generation_frame() — unpivot + delivery_datetime (Step 4-5)
# ---------------------------------------------------------------------------


def test_timeslot_00_30_maps_to_time_code_1_and_midnight_jst(conn) -> None:
    """timeslot_00_30 covers 00:00-00:30 JST -> time_code 1 -> delivery_datetime
    00:00 JST, which is 15:00 UTC on the previous day."""
    _register_bronze(
        conn, [_bronze_row(target_date="2026/08/07", **{"timeslot_00_30": "500"})]
    )

    build_staging_relation(conn, source_relation=SOURCE_RELATION)
    frame = extract_unit_generation_frame(conn)

    row = frame.filter(pl.col("time_code") == 1)
    assert row.height == 1
    assert row["delivery_datetime"][0] == datetime(2026, 8, 6, 15, 0, tzinfo=UTC)
    assert row["generation_kwh"][0] == 500


def test_timeslot_24_00_maps_to_time_code_48_and_23_30_jst(conn) -> None:
    """timeslot_24_00 covers 23:30-24:00 JST -> time_code 48 -> delivery_datetime
    23:30 JST, which is 14:30 UTC the same day."""
    _register_bronze(
        conn, [_bronze_row(target_date="2026/08/07", **{"timeslot_24_00": "700"})]
    )

    build_staging_relation(conn, source_relation=SOURCE_RELATION)
    frame = extract_unit_generation_frame(conn)

    row = frame.filter(pl.col("time_code") == 48)
    assert row.height == 1
    assert row["delivery_datetime"][0] == datetime(2026, 8, 7, 14, 30, tzinfo=UTC)
    assert row["generation_kwh"][0] == 700


def test_unpivot_produces_48_rows_per_valid_source_row(conn) -> None:
    _register_bronze(conn, [_bronze_row()])

    build_staging_relation(conn, source_relation=SOURCE_RELATION)
    frame = extract_unit_generation_frame(conn)

    assert frame.height == 48
    assert sorted(frame["time_code"].to_list()) == list(range(1, 49))


def test_unpivot_excludes_rows_with_violations(conn) -> None:
    _register_bronze(
        conn,
        [
            _bronze_row(),
            _bronze_row(unit_name="無効ユニット", power_plant_code=None),
        ],
    )

    build_staging_relation(conn, source_relation=SOURCE_RELATION)
    frame = extract_unit_generation_frame(conn)

    assert frame.height == 48
    assert set(frame["power_plant_code"].to_list()) == {"10001"}


def test_unpivot_carries_attributes_and_target_date_through(conn) -> None:
    _register_bronze(conn, [_bronze_row(target_date="2026/08/07")])

    build_staging_relation(conn, source_relation=SOURCE_RELATION)
    frame = extract_unit_generation_frame(conn)

    row = frame.filter(pl.col("time_code") == 1)
    assert row["power_plant_code"][0] == "10001"
    assert row["unit_name"][0] == "1号機"
    assert row["target_date"][0] == datetime(2026, 8, 7).date()
    assert row["area"][0] == "01"
    assert row["power_plant_name"][0] == "テスト発電所"
    assert row["power_generation_method_and_fuel_type"][0] == "火力・LNG"
    assert row["source_data"][0] == (
        "raw/occto/unit_generation/target_date=2026-08-07/file.csv"
    )
    assert row["updated_datetime"][0] == datetime(2026, 8, 7, 15, 30, 0)


# ---------------------------------------------------------------------------
# daily_amount quality check (Step 4-6) — warn only, never drops rows
# ---------------------------------------------------------------------------


def test_daily_amount_mismatch_is_counted_with_its_deviation(conn) -> None:
    _register_bronze(
        conn,
        [
            _bronze_row(
                **{"timeslot_00_30": "100", "timeslot_01_00": "200"},
                daily_amount="250",
            )
        ],
    )

    build_staging_relation(conn, source_relation=SOURCE_RELATION)
    summary = summarize_daily_amount_mismatches(conn)

    assert summary["mismatch_count"] == 1
    assert summary["max_deviation"] == 50


def test_daily_amount_match_reports_no_mismatches(conn) -> None:
    _register_bronze(
        conn,
        [
            _bronze_row(
                **{"timeslot_00_30": "100", "timeslot_01_00": "200"},
                daily_amount="300",
            )
        ],
    )

    build_staging_relation(conn, source_relation=SOURCE_RELATION)
    summary = summarize_daily_amount_mismatches(conn)

    assert summary["mismatch_count"] == 0
    assert summary["max_deviation"] == 0


def test_daily_amount_mismatch_does_not_drop_the_row(conn) -> None:
    """A daily_amount mismatch is a quality signal only; the row must still
    reach the long output (docs/tasks/plan_occto_pipeline.md 4-5)."""
    _register_bronze(
        conn,
        [
            _bronze_row(
                **{"timeslot_00_30": "100", "timeslot_01_00": "200"},
                daily_amount="999999",
            )
        ],
    )

    build_staging_relation(conn, source_relation=SOURCE_RELATION)
    frame = extract_unit_generation_frame(conn)

    assert frame.height == 48


# ---------------------------------------------------------------------------
# window resolution for the write path (Step 4-8)
# ---------------------------------------------------------------------------


def test_resolve_staged_target_date_range_returns_min_and_max(conn) -> None:
    _register_bronze(
        conn,
        [
            _bronze_row(target_date="2026/08/05"),
            _bronze_row(unit_name="2号機", target_date="2026/08/07"),
        ],
    )

    build_staging_relation(conn, source_relation=SOURCE_RELATION)
    frame = extract_unit_generation_frame(conn)

    staged_range = resolve_staged_target_date_range(frame)

    assert staged_range == (date(2026, 8, 5), date(2026, 8, 7))


def test_resolve_staged_target_date_range_is_none_for_empty_frame() -> None:
    empty = pl.DataFrame({"target_date": pl.Series([], dtype=pl.Date)})

    assert resolve_staged_target_date_range(empty) is None


def test_build_target_date_window_uses_explicit_range() -> None:
    window = build_target_date_window(
        from_date=date(2026, 8, 1),
        to_date=date(2026, 8, 7),
        staged_range=None,
    )

    assert window == And(
        left=target_date_bound(GreaterThanOrEqual, date(2026, 8, 1)),
        right=target_date_bound(LessThanOrEqual, date(2026, 8, 7)),
    )


def test_build_target_date_window_treats_missing_to_date_as_single_day() -> None:
    window = build_target_date_window(
        from_date=date(2026, 8, 7), to_date=None, staged_range=None
    )

    assert window == And(
        left=target_date_bound(GreaterThanOrEqual, date(2026, 8, 7)),
        right=target_date_bound(LessThanOrEqual, date(2026, 8, 7)),
    )


def test_build_target_date_window_falls_back_to_staged_range() -> None:
    staged_range = (date(2026, 8, 1), date(2026, 8, 3))

    window = build_target_date_window(
        from_date=None, to_date=None, staged_range=staged_range
    )

    assert window == And(
        left=target_date_bound(GreaterThanOrEqual, date(2026, 8, 1)),
        right=target_date_bound(LessThanOrEqual, date(2026, 8, 3)),
    )


def test_build_target_date_window_is_none_without_dates_or_staged_range() -> None:
    window = build_target_date_window(from_date=None, to_date=None, staged_range=None)

    assert window is None

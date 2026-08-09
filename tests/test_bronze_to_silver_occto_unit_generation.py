"""Unit tests for the OCCTO bronze-to-silver transformation.

Modeled on tests/test_bronze_to_silver_jepx_spot_price.py: the transform
SQL is exercised against a locally registered relation instead of
iceberg_scan, so these tests need neither RustFS nor an Iceberg catalog.
"""

from __future__ import annotations

import csv
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import polars as pl
import pytest

from pipeline.silver.bronze_to_silver_occto_unit_generation import (
    STAGING_RELATION,
    TIMESLOT_COLUMNS,
    build_staging_relation,
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


def _register_bronze(
    conn: duckdb.DuckDBPyConnection, rows: list[dict[str, object]]
) -> None:
    """Register rows as a relation that stands in for the bronze table.

    unit_name is cast to Utf8 explicitly: a row with unit_name=None and no
    other row to infer a type from would otherwise make polars type the
    whole column as Null, which the real bronze Iceberg table (declared
    string in its schema CSV) never does.
    """
    frame = pl.DataFrame(rows).with_columns(pl.col("unit_name").cast(pl.Utf8))
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

"""Unit tests for the JEPX bronze-to-silver transformation.

The transformation SQL is exercised against a locally registered relation
instead of ``iceberg_scan``, so these tests need neither RustFS nor an
Iceberg catalog. Only the ``integration`` marked tests touch real storage.
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime
from decimal import Decimal

import duckdb
import polars as pl
import pytest

from pipeline.silver.bronze_to_silver_jepx_spot_price import (
    build_staging_relation,
    count_dropped_rows,
    extract_area_frame,
    extract_base_frame,
    extract_block_frame,
)

AREA_SUFFIXES = (
    "hokkaido",
    "tohoku",
    "tokyo",
    "chubu",
    "hokuriku",
    "kansai",
    "chugoku",
    "shikoku",
    "kyushu",
)

SOURCE_RELATION = "bronze_source"


def _bronze_row(**overrides: object) -> dict[str, object]:
    """Build one bronze-shaped row. All source columns are strings."""
    row: dict[str, object] = {
        "delivery_date": "2024/04/01",
        "time_code": "1",
        "selling_bid_volume": "27474550",
        "purchase_bid_volume": "22189150",
        "contracted_volume": "17741050",
        "system_price": "17.75",
        "block_selling_bid_volume": "8938850",
        "block_selling_contracted_volume": "3146000",
        "block_purchase_bid_volume": "1198550",
        "block_purchase_contracted_volume": "823850",
        "source_data": "raw/jepx/spot_price/spot.csv.gz",
        "status": "new",
        "ingestion_time": datetime(2026, 7, 30, 14, 58, 31, tzinfo=UTC),
        "execution_id": "exec-1",
    }
    for index, suffix in enumerate(AREA_SUFFIXES):
        row[f"area_price_{suffix}"] = f"{20 + index}.25"
    row.update(overrides)
    return row


def _register_bronze(
    conn: duckdb.DuckDBPyConnection, rows: list[dict[str, object]]
) -> None:
    """Register rows as a relation that stands in for the bronze table."""
    frame = pl.DataFrame(rows)
    conn.register(SOURCE_RELATION, frame)


@pytest.fixture
def conn() -> Iterator[duckdb.DuckDBPyConnection]:
    """In-memory DuckDB connection.

    ``icu`` is statically linked into DuckDB and auto-loads, so
    ``AT TIME ZONE`` works without installing anything over the network.
    """
    connection = duckdb.connect()
    yield connection
    connection.close()


def test_delivery_datetime_interprets_time_code_as_jst(conn) -> None:
    """Time code 1 is 00:00 JST, which is 15:00 UTC on the previous day."""
    # Arrange
    _register_bronze(conn, [_bronze_row(delivery_date="2024/04/01", time_code="1")])

    # Act
    build_staging_relation(conn, source_relation=SOURCE_RELATION)
    frame = extract_base_frame(conn)

    # Assert
    assert frame["delivery_datetime"][0] == datetime(2024, 3, 31, 15, 0, tzinfo=UTC)


def test_time_code_48_maps_to_last_slot(conn) -> None:
    """Time code 48 starts at 23:30 JST, which is 14:30 UTC the same day."""
    # Arrange
    _register_bronze(conn, [_bronze_row(delivery_date="2024/04/01", time_code="48")])

    # Act
    build_staging_relation(conn, source_relation=SOURCE_RELATION)
    frame = extract_base_frame(conn)

    # Assert
    assert frame["delivery_datetime"][0] == datetime(2024, 4, 1, 14, 30, tzinfo=UTC)


def test_parses_hyphenated_delivery_date(conn) -> None:
    """Both slash and hyphen separated delivery dates are accepted."""
    # Arrange
    _register_bronze(conn, [_bronze_row(delivery_date="2024-04-01", time_code="1")])

    # Act
    build_staging_relation(conn, source_relation=SOURCE_RELATION)
    frame = extract_base_frame(conn)

    # Assert
    assert frame["delivery_datetime"][0] == datetime(2024, 3, 31, 15, 0, tzinfo=UTC)


def test_keeps_latest_row_per_delivery_key(conn) -> None:
    """Revised rows win over earlier ones for the same delivery key."""
    # Arrange
    older = _bronze_row(
        system_price="10.00",
        ingestion_time=datetime(2026, 7, 1, 0, 0, tzinfo=UTC),
    )
    newer = _bronze_row(
        system_price="20.00",
        ingestion_time=datetime(2026, 7, 2, 0, 0, tzinfo=UTC),
    )
    _register_bronze(conn, [older, newer])

    # Act
    build_staging_relation(conn, source_relation=SOURCE_RELATION)
    frame = extract_base_frame(conn)

    # Assert
    assert frame.height == 1
    assert frame["system_price"][0] == Decimal("20.000")


def test_reports_dropped_row_count_for_invalid_rows(conn) -> None:
    """Rows failing validation are excluded and counted."""
    # Arrange
    _register_bronze(
        conn,
        [
            _bronze_row(time_code="1"),
            _bronze_row(time_code="0", delivery_date="2024/04/02"),
            _bronze_row(delivery_date=None, time_code="3"),
        ],
    )

    # Act
    build_staging_relation(conn, source_relation=SOURCE_RELATION)
    frame = extract_base_frame(conn)

    # Assert
    assert count_dropped_rows(conn) == 2
    assert frame.height == 1


def test_prices_keep_decimal_precision(conn) -> None:
    """System price keeps its fractional part instead of being rounded."""
    # Arrange
    _register_bronze(conn, [_bronze_row(system_price="17.75")])

    # Act
    build_staging_relation(conn, source_relation=SOURCE_RELATION)
    frame = extract_base_frame(conn)

    # Assert
    assert frame["system_price"][0] == Decimal("17.750")


def test_area_prices_keep_decimal_precision(conn) -> None:
    """Area prices keep their fractional part as well."""
    # Arrange
    _register_bronze(conn, [_bronze_row(area_price_tokyo="8.07")])

    # Act
    build_staging_relation(conn, source_relation=SOURCE_RELATION)
    frame = extract_area_frame(conn)

    # Assert
    tokyo = frame.filter(pl.col("area_name") == "tokyo")
    assert tokyo["area_price"][0] == Decimal("8.070")


def test_volumes_are_parsed_with_thousand_separators(conn) -> None:
    """Thousand separators in volume columns are stripped before casting."""
    # Arrange
    _register_bronze(conn, [_bronze_row(contracted_volume="17,741,050")])

    # Act
    build_staging_relation(conn, source_relation=SOURCE_RELATION)
    frame = extract_base_frame(conn)

    # Assert
    assert frame["contracted_volume"][0] == 17741050


def test_area_frame_unpivots_nine_areas(conn) -> None:
    """One source row expands into one row per area."""
    # Arrange
    _register_bronze(conn, [_bronze_row()])

    # Act
    build_staging_relation(conn, source_relation=SOURCE_RELATION)
    frame = extract_area_frame(conn)

    # Assert
    assert frame.height == 9
    assert sorted(frame["area_name"].to_list()) == sorted(AREA_SUFFIXES)


def test_area_frame_excludes_block_columns(conn) -> None:
    """UNPIVOT must not carry block columns through into the area frame."""
    # Arrange
    _register_bronze(conn, [_bronze_row()])

    # Act
    build_staging_relation(conn, source_relation=SOURCE_RELATION)
    frame = extract_area_frame(conn)

    # Assert
    assert not [column for column in frame.columns if column.startswith("block_")]


def test_silver_frames_retain_delivery_date_and_time_code(conn) -> None:
    """Every silver frame keeps the business keys the timestamp derives from."""
    # Arrange
    _register_bronze(conn, [_bronze_row(delivery_date="2024/04/01", time_code="1")])

    # Act
    build_staging_relation(conn, source_relation=SOURCE_RELATION)
    frames = (
        extract_base_frame(conn),
        extract_area_frame(conn),
        extract_block_frame(conn),
    )

    # Assert
    for frame in frames:
        assert "delivery_date" in frame.columns
        assert "time_code" in frame.columns
        assert frame["delivery_date"][0] == datetime(2024, 4, 1).date()
        assert frame["time_code"][0] == 1


def test_block_frame_contains_block_volumes(conn) -> None:
    """The block frame carries the block volume columns."""
    # Arrange
    _register_bronze(conn, [_bronze_row(block_selling_bid_volume="8938850")])

    # Act
    build_staging_relation(conn, source_relation=SOURCE_RELATION)
    frame = extract_block_frame(conn)

    # Assert
    assert frame["block_selling_bid_volume"][0] == 8938850


def test_fiscal_year_narrows_the_selected_range(conn) -> None:
    """Fiscal year is the only difference between daily and full runs."""
    # Arrange
    _register_bronze(
        conn,
        [
            _bronze_row(delivery_date="2024/03/31"),
            _bronze_row(delivery_date="2024/04/01"),
            _bronze_row(delivery_date="2025/03/31"),
            _bronze_row(delivery_date="2025/04/01"),
        ],
    )

    # Act
    build_staging_relation(conn, source_relation=SOURCE_RELATION, fiscal_year=2024)
    frame = extract_base_frame(conn)

    # Assert
    assert sorted(frame["delivery_date"].to_list()) == [
        datetime(2024, 4, 1).date(),
        datetime(2025, 3, 31).date(),
    ]


def test_source_data_is_carried_into_silver(conn) -> None:
    """Silver rows keep the bronze source reference for auditing."""
    # Arrange
    _register_bronze(conn, [_bronze_row(source_data="raw/jepx/spot.csv.gz")])

    # Act
    build_staging_relation(conn, source_relation=SOURCE_RELATION)
    frame = extract_base_frame(conn)

    # Assert
    assert frame["source_data"][0] == "raw/jepx/spot.csv.gz"

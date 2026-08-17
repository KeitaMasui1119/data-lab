"""Unit tests for the JEPX bronze-to-silver transformation.

The transformation SQL is exercised against a locally registered relation
instead of ``iceberg_scan``, so these tests need neither RustFS nor an
Iceberg catalog. Only the ``integration`` marked tests touch real storage.
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, date, datetime
from decimal import Decimal
from types import SimpleNamespace
from typing import cast

import duckdb
import polars as pl
import pyarrow as pa
import pytest
from pyiceberg.catalog import Catalog
from pyiceberg.expressions import And, GreaterThanOrEqual, LessThan, LessThanOrEqual

from common.pipeline_utilities import add_metadata
from common.silver_write import (
    SILVER_STATUS_LOADED,
    SilverWriteResult,
    ensure_unique_keys,
    write_silver_table,
)
from pipeline.silver import bronze_to_silver_jepx_spot_price as jepx_silver
from pipeline.silver.bronze_to_silver_jepx_spot_price import (
    build_delivery_window,
    build_staging_relation,
    count_dropped_rows,
    count_staged_rows,
    delivery_date_bound,
    extract_area_frame,
    extract_base_frame,
    extract_block_frame,
    resolve_staged_delivery_range,
    run_bronze_to_silver_jepx_spot_price,
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


# --- Silver write path -------------------------------------------------------
#
# The write path replaces a delivery window wholesale instead of upserting row
# by row. PyIceberg's upsert() builds a match predicate over every join key in
# the source frame, which crashed the process once the area table reached a few
# million rows. The staging relation already holds the complete, deduplicated
# set of rows for the target window, so replacing that window is equivalent and
# does not scale with table size.


class _FakeTable:
    """Iceberg table stand-in that records how it was written to."""

    def __init__(self, schema: pa.Schema) -> None:
        self._schema = schema
        self.overwrite_calls: list[tuple[pa.Table, object]] = []

    def schema(self) -> SimpleNamespace:
        return SimpleNamespace(as_arrow=lambda: self._schema)

    def overwrite(self, df: pa.Table, overwrite_filter: object = None, **_: object):
        self.overwrite_calls.append((df, overwrite_filter))

    def upsert(self, *_: object, **__: object):
        raise AssertionError("upsert must not be used by the silver write path")


class _FakeCatalog:
    def __init__(self, table: _FakeTable) -> None:
        self._table = table

    def load_table(self, _identifier: str) -> _FakeTable:
        return self._table


def _silver_frame(rows: int = 2) -> pl.DataFrame:
    """Build a silver-shaped frame the way the extract_* helpers would."""
    return pl.DataFrame(
        {
            "delivery_date": [date(2026, 4, 1)] * rows,
            "time_code": list(range(1, rows + 1)),
            "system_price": [Decimal("10.000")] * rows,
            "source_data": ["raw/jepx/spot.csv.gz"] * rows,
        }
    )


def _target_schema_for(frame: pl.DataFrame) -> pa.Schema:
    """Derive the schema the silver table would expose for this frame."""
    stamped = add_metadata(
        frame.with_columns(pl.lit(SILVER_STATUS_LOADED).alias("status")),
        execution_id="exec-1",
    )
    return stamped.to_arrow().schema


@pytest.fixture(autouse=True)
def _stub_provision_table(monkeypatch) -> None:
    """provision_table talks to a real catalog, which unit tests must not do."""
    monkeypatch.setattr(
        "common.silver_write.provision_table",
        lambda *_, **__: None,
    )


def test_build_delivery_window_covers_the_whole_fiscal_year() -> None:
    """A fiscal year window runs April 1 through the following March 31."""
    # Arrange / Act
    window = build_delivery_window(fiscal_year=2026, staged_range=None)

    # Assert
    assert window == And(
        left=delivery_date_bound(GreaterThanOrEqual, date(2026, 4, 1)),
        right=delivery_date_bound(LessThan, date(2027, 4, 1)),
    )


def test_build_delivery_window_falls_back_to_the_staged_range() -> None:
    """Without a fiscal year the window covers exactly what was staged."""
    # Arrange
    staged_range = (date(2005, 4, 2), date(2026, 8, 9))

    # Act
    window = build_delivery_window(fiscal_year=None, staged_range=staged_range)

    # Assert
    assert window == And(
        left=delivery_date_bound(GreaterThanOrEqual, date(2005, 4, 2)),
        right=delivery_date_bound(LessThanOrEqual, date(2026, 8, 9)),
    )


def test_build_delivery_window_is_none_when_nothing_was_staged() -> None:
    """No staged rows means there is no window to replace."""
    # Arrange / Act
    window = build_delivery_window(fiscal_year=None, staged_range=None)

    # Assert
    assert window is None


def test_resolve_staged_delivery_range_returns_valid_row_bounds(conn) -> None:
    """The staged range spans only rows that passed validation."""
    # Arrange
    _register_bronze(
        conn,
        [
            _bronze_row(delivery_date="2024/04/01"),
            _bronze_row(delivery_date="2024/04/05"),
            _bronze_row(delivery_date="2024/04/09", time_code="0"),
        ],
    )

    # Act
    build_staging_relation(conn, source_relation=SOURCE_RELATION)
    staged_range = resolve_staged_delivery_range(conn)

    # Assert
    assert staged_range == (date(2024, 4, 1), date(2024, 4, 5))


def test_resolve_staged_delivery_range_is_none_without_valid_rows(conn) -> None:
    """An all-invalid staging relation yields no range."""
    # Arrange
    _register_bronze(conn, [_bronze_row(time_code="0")])

    # Act
    build_staging_relation(conn, source_relation=SOURCE_RELATION)

    # Assert
    assert resolve_staged_delivery_range(conn) is None


def test_write_silver_table_replaces_the_target_window() -> None:
    """The write path overwrites the delivery window rather than upserting."""
    # Arrange
    frame = _silver_frame()
    table = _FakeTable(_target_schema_for(frame))
    window = build_delivery_window(fiscal_year=2026, staged_range=None)

    # Act
    result = write_silver_table(
        cast(Catalog, _FakeCatalog(table)),
        table_identifier="silver.jepx_spot_price_base",
        schema_path="/unused.csv",
        frame=frame,
        key_cols=("delivery_date", "time_code"),
        overwrite_filter=window,
        execution_id="exec-1",
    )

    # Assert
    assert len(table.overwrite_calls) == 1
    written, overwrite_filter = table.overwrite_calls[0]
    assert overwrite_filter == window
    assert written.num_rows == 2
    assert result.rows_written == 2


def test_write_silver_table_stamps_status_and_execution_id() -> None:
    """Rows carry the loaded status and the run's execution id."""
    # Arrange
    frame = _silver_frame()
    table = _FakeTable(_target_schema_for(frame))

    # Act
    write_silver_table(
        cast(Catalog, _FakeCatalog(table)),
        table_identifier="silver.jepx_spot_price_base",
        schema_path="/unused.csv",
        frame=frame,
        key_cols=("delivery_date", "time_code"),
        overwrite_filter=build_delivery_window(fiscal_year=2026, staged_range=None),
        execution_id="exec-42",
    )

    # Assert
    written = table.overwrite_calls[0][0].to_pydict()
    assert set(written["status"]) == {SILVER_STATUS_LOADED}
    assert set(written["execution_id"]) == {"exec-42"}


def test_write_silver_table_skips_empty_frame_without_deleting() -> None:
    """An empty frame must not wipe the window it would have replaced."""
    # Arrange
    frame = _silver_frame(rows=0)
    table = _FakeTable(_target_schema_for(_silver_frame()))

    # Act
    result = write_silver_table(
        cast(Catalog, _FakeCatalog(table)),
        table_identifier="silver.jepx_spot_price_base",
        schema_path="/unused.csv",
        frame=frame,
        key_cols=("delivery_date", "time_code"),
        overwrite_filter=build_delivery_window(fiscal_year=2026, staged_range=None),
        execution_id="exec-1",
    )

    # Assert
    assert table.overwrite_calls == []
    assert result.rows_written == 0


def test_write_silver_table_rejects_duplicate_business_keys() -> None:
    """upsert() used to reject duplicate keys; overwrite must keep that guard."""
    # Arrange
    frame = pl.DataFrame(
        {
            "delivery_date": [date(2026, 4, 1), date(2026, 4, 1)],
            "time_code": [1, 1],
            "system_price": [Decimal("10.000"), Decimal("11.000")],
            "source_data": ["raw/a.csv.gz", "raw/a.csv.gz"],
        }
    )
    table = _FakeTable(_target_schema_for(frame))

    # Act / Assert
    with pytest.raises(ValueError, match="duplicate"):
        write_silver_table(
            cast(Catalog, _FakeCatalog(table)),
            table_identifier="silver.jepx_spot_price_base",
            schema_path="/unused.csv",
            frame=frame,
            key_cols=("delivery_date", "time_code"),
            overwrite_filter=build_delivery_window(fiscal_year=2026, staged_range=None),
            execution_id="exec-1",
        )
    assert table.overwrite_calls == []


def test_ensure_unique_keys_accepts_distinct_keys() -> None:
    """Distinct business keys pass the guard untouched."""
    # Arrange
    frame = _silver_frame()

    # Act / Assert
    ensure_unique_keys(
        frame, key_cols=("delivery_date", "time_code"), table_identifier="silver.t"
    )


def test_area_rows_are_unique_per_area(conn) -> None:
    """The area frame's business key includes area_name, so rows stay unique."""
    # Arrange
    _register_bronze(conn, [_bronze_row()])

    # Act
    build_staging_relation(conn, source_relation=SOURCE_RELATION)
    frame = extract_area_frame(conn)

    # Assert
    ensure_unique_keys(
        frame,
        key_cols=("delivery_date", "time_code", "area_name"),
        table_identifier="silver.jepx_spot_price_area",
    )


# --- Whole-run guards --------------------------------------------------------
#
# docs/tasks/tasks.md sections 8.2 and 8.4: the checks that only hold across a
# whole run -- every frame is validated before any table is written, and the
# run counts what it staged so a silent no-op cannot pass for success.


def _duplicate_key_area_frame() -> pl.DataFrame:
    """An area frame holding one business key twice.

    The staging SQL cannot produce this on its own -- it deduplicates by
    (delivery_date, time_code) and UNPIVOT emits each area once -- so the
    frame is injected to stand in for a future extract_area_frame() bug.
    """
    return pl.DataFrame(
        {
            "delivery_date": [date(2024, 4, 1)] * 2,
            "time_code": [1, 1],
            "delivery_datetime": [datetime(2024, 3, 31, 15, 0, tzinfo=UTC)] * 2,
            "area_name": ["tokyo", "tokyo"],
            "area_price": [Decimal("10.000"), Decimal("11.000")],
            "source_data": ["raw/jepx/spot.csv.gz"] * 2,
        }
    )


def _stub_run_dependencies(monkeypatch, conn, rows: list[dict[str, object]]):
    """Run the transform against a registered relation, recording every write.

    A real run scans ``iceberg_scan(<bronze location>)`` and loads a catalog;
    the staging SQL and the ordering of the guards are what this exercises.
    """
    _register_bronze(conn, rows)
    real_build_staging_relation = build_staging_relation

    def build_from_registered_relation(
        connection, *, source_relation: str, fiscal_year: int | None = None
    ) -> None:
        real_build_staging_relation(
            connection, source_relation=SOURCE_RELATION, fiscal_year=fiscal_year
        )

    written_tables: list[str] = []

    def record_write(_catalog, *, table_identifier: str, frame, **_kwargs):
        written_tables.append(table_identifier)
        return SilverWriteResult(table_identifier, frame.height)

    monkeypatch.setattr(jepx_silver, "create_duckdb_connection", lambda: conn)
    monkeypatch.setattr(jepx_silver, "get_catalog", lambda _name: object())
    monkeypatch.setattr(
        jepx_silver, "build_staging_relation", build_from_registered_relation
    )
    monkeypatch.setattr(jepx_silver, "write_silver_table", record_write)
    return written_tables


def test_run_writes_every_target_table(monkeypatch, conn) -> None:
    """The happy path writes all three silver tables."""
    # Arrange
    written_tables = _stub_run_dependencies(monkeypatch, conn, [_bronze_row()])

    # Act
    result = run_bronze_to_silver_jepx_spot_price()

    # Assert
    assert written_tables == [
        "silver.jepx_spot_price_base",
        "silver.jepx_spot_price_block",
        "silver.jepx_spot_price_area",
    ]
    assert result.staged_row_count == 1
    assert result.valid_row_count == 1


def test_run_validates_every_frame_before_writing_any_table(monkeypatch, conn) -> None:
    """A duplicate key in the last target must not leave the first two rebuilt.

    Each table is replaced in its own transaction, so validating lazily would
    rebuild base and block against the new snapshot and then abort, leaving
    the three tables disagreeing about the same delivery window.
    """
    # Arrange
    written_tables = _stub_run_dependencies(monkeypatch, conn, [_bronze_row()])
    monkeypatch.setattr(
        jepx_silver, "extract_area_frame", lambda _conn: _duplicate_key_area_frame()
    )

    # Act / Assert
    with pytest.raises(ValueError, match="duplicate"):
        run_bronze_to_silver_jepx_spot_price()

    assert written_tables == []


def test_count_staged_rows_counts_valid_and_invalid_rows(conn) -> None:
    """The staged count spans the whole relation, not just the usable rows."""
    # Arrange
    _register_bronze(
        conn,
        [
            _bronze_row(delivery_date="2024/04/01"),
            _bronze_row(delivery_date="2024/04/02", time_code="0"),
        ],
    )

    # Act
    build_staging_relation(conn, source_relation=SOURCE_RELATION)

    # Assert
    assert count_staged_rows(conn) == 2
    assert count_dropped_rows(conn) == 1


def test_unparseable_delivery_dates_stage_nothing_under_a_fiscal_year(conn) -> None:
    """The silent no-op that section 8.4's status check exists to catch.

    Neither delivery_date format matches, so the cast yields NULL and the
    fiscal year filter discards the row before validation can flag it. The
    run then sees dropped=0 and written=0, which is indistinguishable from a
    scope that legitimately held no rows -- hence the staged count.
    """
    # Arrange
    _register_bronze(conn, [_bronze_row(delivery_date="01.04.2024")])

    # Act
    build_staging_relation(conn, source_relation=SOURCE_RELATION, fiscal_year=2024)

    # Assert
    assert count_staged_rows(conn) == 0
    assert count_dropped_rows(conn) == 0

"""Unit tests for the shared silver-layer window-replace write helpers.

Extracted from bronze_to_silver_jepx_spot_price.py (Phase 4 Step 4-7 of
docs/tasks/plan_occto_pipeline.md): write_silver_table(), ensure_unique_keys()
and the overwrite-filter column_bound() helper have nothing JEPX-specific in
them, so OCCTO's bronze-to-silver reuses them instead of duplicating the
window-replace logic.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from types import SimpleNamespace
from typing import cast

import polars as pl
import pyarrow as pa
import pytest
from pyiceberg.catalog import Catalog
from pyiceberg.expressions import GreaterThanOrEqual, LessThan

from common.pipeline_utilities import add_metadata
from common.silver_write import (
    SILVER_STATUS_LOADED,
    column_bound,
    ensure_unique_keys,
    write_silver_table,
)


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


def _generic_frame(rows: int = 2) -> pl.DataFrame:
    """Build a silver-shaped frame the way an extract_* helper would."""
    return pl.DataFrame(
        {
            "business_date": [date(2026, 4, 1)] * rows,
            "slot": list(range(1, rows + 1)),
            "value": [Decimal("10.000")] * rows,
            "source_data": ["raw/example.csv"] * rows,
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


def test_column_bound_builds_the_same_predicate_as_calling_it_directly() -> None:
    # Direct construction hits the same pyright/pydantic false positive that
    # column_bound() exists to suppress in one place; see its docstring.
    boundary = date(2026, 8, 7)

    assert column_bound("business_date", GreaterThanOrEqual, boundary) == (
        GreaterThanOrEqual("business_date", boundary)  # pyright: ignore[reportCallIssue]
    )
    assert column_bound("business_date", LessThan, boundary) == (
        LessThan("business_date", boundary)  # pyright: ignore[reportCallIssue]
    )


def test_write_silver_table_replaces_the_target_window() -> None:
    """The write path overwrites the target window rather than upserting."""
    frame = _generic_frame()
    table = _FakeTable(_target_schema_for(frame))
    window = column_bound("business_date", GreaterThanOrEqual, date(2026, 4, 1))

    result = write_silver_table(
        cast(Catalog, _FakeCatalog(table)),
        table_identifier="silver.example",
        schema_path="/unused.csv",
        frame=frame,
        key_cols=("business_date", "slot"),
        overwrite_filter=window,
        execution_id="exec-1",
    )

    assert len(table.overwrite_calls) == 1
    written, overwrite_filter = table.overwrite_calls[0]
    assert overwrite_filter == window
    assert written.num_rows == 2
    assert result.rows_written == 2


def test_write_silver_table_stamps_status_and_execution_id() -> None:
    frame = _generic_frame()
    table = _FakeTable(_target_schema_for(frame))
    window = column_bound("business_date", GreaterThanOrEqual, date(2026, 4, 1))

    write_silver_table(
        cast(Catalog, _FakeCatalog(table)),
        table_identifier="silver.example",
        schema_path="/unused.csv",
        frame=frame,
        key_cols=("business_date", "slot"),
        overwrite_filter=window,
        execution_id="exec-42",
    )

    written = table.overwrite_calls[0][0].to_pydict()
    assert set(written["status"]) == {SILVER_STATUS_LOADED}
    assert set(written["execution_id"]) == {"exec-42"}


def test_write_silver_table_skips_empty_frame_without_deleting() -> None:
    """An empty frame must not wipe the window it would have replaced."""
    frame = _generic_frame(rows=0)
    table = _FakeTable(_target_schema_for(_generic_frame()))
    window = column_bound("business_date", GreaterThanOrEqual, date(2026, 4, 1))

    result = write_silver_table(
        cast(Catalog, _FakeCatalog(table)),
        table_identifier="silver.example",
        schema_path="/unused.csv",
        frame=frame,
        key_cols=("business_date", "slot"),
        overwrite_filter=window,
        execution_id="exec-1",
    )

    assert table.overwrite_calls == []
    assert result.rows_written == 0


def test_write_silver_table_raises_without_a_window_for_a_nonempty_frame() -> None:
    frame = _generic_frame()
    table = _FakeTable(_target_schema_for(frame))

    with pytest.raises(ValueError, match="without a delivery window"):
        write_silver_table(
            cast(Catalog, _FakeCatalog(table)),
            table_identifier="silver.example",
            schema_path="/unused.csv",
            frame=frame,
            key_cols=("business_date", "slot"),
            overwrite_filter=None,
            execution_id="exec-1",
        )


def test_write_silver_table_rejects_duplicate_business_keys() -> None:
    """upsert() used to reject duplicate keys; overwrite must keep that guard."""
    frame = pl.DataFrame(
        {
            "business_date": [date(2026, 4, 1), date(2026, 4, 1)],
            "slot": [1, 1],
            "value": [Decimal("10.000"), Decimal("11.000")],
            "source_data": ["raw/a.csv", "raw/a.csv"],
        }
    )
    table = _FakeTable(_target_schema_for(frame))
    window = column_bound("business_date", GreaterThanOrEqual, date(2026, 4, 1))

    with pytest.raises(ValueError, match="duplicate"):
        write_silver_table(
            cast(Catalog, _FakeCatalog(table)),
            table_identifier="silver.example",
            schema_path="/unused.csv",
            frame=frame,
            key_cols=("business_date", "slot"),
            overwrite_filter=window,
            execution_id="exec-1",
        )
    assert table.overwrite_calls == []


def test_ensure_unique_keys_accepts_distinct_keys() -> None:
    frame = _generic_frame()

    ensure_unique_keys(
        frame, key_cols=("business_date", "slot"), table_identifier="silver.t"
    )


def test_ensure_unique_keys_rejects_duplicate_keys() -> None:
    frame = pl.DataFrame(
        {
            "business_date": [date(2026, 4, 1), date(2026, 4, 1)],
            "slot": [1, 1],
        }
    )

    with pytest.raises(ValueError, match="duplicate"):
        ensure_unique_keys(
            frame, key_cols=("business_date", "slot"), table_identifier="silver.t"
        )

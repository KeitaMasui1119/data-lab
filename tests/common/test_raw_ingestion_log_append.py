"""Unit tests for the shared raw ingestion log append path.

Each of the six raw scrapers used to carry its own copy of
``_build_empty_ingestion_log`` and ``_update_ingestion_log``: identical
schemas, and update bodies that differed only in which column scopes
"latest". The copies drifted -- JEPX's scoped its is_latest flip by
fiscal_year alone, so a JEPX snapshot cleared the latest flag on every
other dataset's row for the same fiscal year. One implementation, tested
here, replaces all six.
"""

from __future__ import annotations

import io
from datetime import UTC, datetime

import polars as pl
import pytest

from common.raw_ingestion_log import (
    DEFAULT_INGESTION_LOG_KEY,
    INGESTION_LOG_SCHEMA,
    append_ingestion_log_entry,
    build_empty_ingestion_log,
)

INGESTED_AT = datetime(2026, 8, 22, 3, 0, 0, tzinfo=UTC)


class FakeStorage:
    """In-memory stand-in for RustFSClient's get/put of the log parquet."""

    def __init__(self, initial: pl.DataFrame | None = None) -> None:
        self.objects: dict[str, bytes] = {}
        if initial is not None:
            self.objects[DEFAULT_INGESTION_LOG_KEY] = _to_parquet(initial)

    def get_object_or_none(self, bucket_name: str, object_name: str) -> bytes | None:
        return self.objects.get(object_name)

    def upload_bytes(
        self, *, bucket_name: str, object_name: str, body: bytes, content_type: str
    ) -> None:
        self.objects[object_name] = body

    def read_log(self) -> pl.DataFrame:
        return pl.read_parquet(io.BytesIO(self.objects[DEFAULT_INGESTION_LOG_KEY]))


def _to_parquet(frame: pl.DataFrame) -> bytes:
    buffer = io.BytesIO()
    frame.write_parquet(buffer)
    return buffer.getvalue()


def _append(storage: FakeStorage, **overrides: object) -> None:
    """Append one entry for a fiscal-year-keyed dataset, with overrides."""
    kwargs: dict[str, object] = {
        "dataset": "jepx.spot_price",
        "ingested_at": INGESTED_AT,
        "file_hash": "hash-1",
        "file_path": "raw/jepx/spot_price/year=2026/f.csv",
        "content_length": 1024,
        "execution_id": "run-1",
        "fiscal_year": 2026,
    }
    kwargs.update(overrides)
    append_ingestion_log_entry(storage, "jp-power-grid-dev", **kwargs)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------


def test_empty_log_declares_every_column_with_an_explicit_dtype() -> None:
    """An inferred dtype on an empty frame breaks the later vertical concat."""
    # Act
    frame = build_empty_ingestion_log()

    # Assert
    assert frame.height == 0
    assert dict(frame.schema) == INGESTION_LOG_SCHEMA


def test_log_carries_an_execution_id() -> None:
    """Without it the log records which file was fetched but not by which run."""
    # Act / Assert
    assert "execution_id" in INGESTION_LOG_SCHEMA


# ---------------------------------------------------------------------------
# Appending onto an empty log
# ---------------------------------------------------------------------------


def test_first_entry_creates_the_log() -> None:
    # Arrange
    storage = FakeStorage()

    # Act
    _append(storage)

    # Assert
    log = storage.read_log()
    assert log.height == 1
    assert log["dataset"].to_list() == ["jepx.spot_price"]
    assert log["file_path"].to_list() == ["raw/jepx/spot_price/year=2026/f.csv"]


def test_new_entry_is_latest_and_pending_for_bronze() -> None:
    """A freshly saved snapshot has not reached bronze yet."""
    # Arrange
    storage = FakeStorage()

    # Act
    _append(storage)

    # Assert
    log = storage.read_log()
    assert log["is_latest"].to_list() == [True]
    assert log["bronze_status"].to_list() == ["pending"]
    assert log["bronze_processed_at"].to_list() == [None]


def test_entry_records_the_run_that_fetched_it() -> None:
    # Arrange
    storage = FakeStorage()

    # Act
    _append(storage, execution_id="run-abc")

    # Assert
    assert storage.read_log()["execution_id"].to_list() == ["run-abc"]


def test_ingested_at_is_stored_as_an_iso_string() -> None:
    """The log is read back with polars and compared as text, not parsed."""
    # Arrange
    storage = FakeStorage()

    # Act
    _append(storage)

    # Assert
    assert storage.read_log()["ingested_at"].to_list() == [INGESTED_AT.isoformat()]


# ---------------------------------------------------------------------------
# is_latest scoping — the drift that made consolidation worth doing
# ---------------------------------------------------------------------------


def test_appending_clears_the_previous_latest_for_the_same_scope() -> None:
    # Arrange
    storage = FakeStorage()
    _append(storage, file_hash="hash-1", file_path="first.csv")

    # Act
    _append(storage, file_hash="hash-2", file_path="second.csv")

    # Assert
    log = storage.read_log().sort("file_path")
    assert log["file_path"].to_list() == ["first.csv", "second.csv"]
    assert log["is_latest"].to_list() == [False, True]


def test_appending_leaves_another_datasets_latest_alone() -> None:
    """JEPX's copy scoped the flip by fiscal_year alone and cleared these."""
    # Arrange
    storage = FakeStorage()
    _append(storage, dataset="supply_demand_actuals_tohoku", file_path="tohoku.csv")

    # Act
    _append(storage, dataset="jepx.spot_price", file_path="jepx.csv")

    # Assert
    log = storage.read_log().sort("file_path")
    assert log["file_path"].to_list() == ["jepx.csv", "tohoku.csv"]
    assert log["is_latest"].to_list() == [True, True]


def test_appending_leaves_another_fiscal_years_latest_alone() -> None:
    # Arrange
    storage = FakeStorage()
    _append(storage, fiscal_year=2025, file_path="fy2025.csv")

    # Act
    _append(storage, fiscal_year=2026, file_path="fy2026.csv")

    # Assert
    log = storage.read_log().sort("file_path")
    assert log["is_latest"].to_list() == [True, True]


def test_a_snapshot_date_keyed_dataset_scopes_by_date() -> None:
    """OCCTO and the denki-yohou sources are keyed by day, not fiscal year."""
    # Arrange
    storage = FakeStorage()
    _append(
        storage,
        dataset="occto_unit_generation",
        fiscal_year=None,
        snapshot_date="2026-08-20",
        file_path="d20.csv",
    )

    # Act
    _append(
        storage,
        dataset="occto_unit_generation",
        fiscal_year=None,
        snapshot_date="2026-08-21",
        file_path="d21.csv",
    )

    # Assert
    log = storage.read_log().sort("file_path")
    assert log["is_latest"].to_list() == [True, True]


def test_a_snapshot_date_keyed_dataset_clears_its_own_previous_day() -> None:
    # Arrange
    storage = FakeStorage()
    _append(
        storage,
        dataset="occto_unit_generation",
        fiscal_year=None,
        snapshot_date="2026-08-20",
        file_path="first.csv",
    )

    # Act
    _append(
        storage,
        dataset="occto_unit_generation",
        fiscal_year=None,
        snapshot_date="2026-08-20",
        file_path="second.csv",
    )

    # Assert
    log = storage.read_log().sort("file_path")
    assert log["is_latest"].to_list() == [False, True]


# ---------------------------------------------------------------------------
# Guard rails
# ---------------------------------------------------------------------------


def test_an_entry_scoped_by_neither_key_is_refused() -> None:
    """Without a scope the append cannot know which rows it supersedes."""
    # Arrange
    storage = FakeStorage()

    # Act / Assert
    with pytest.raises(ValueError, match="fiscal_year or snapshot_date"):
        _append(storage, fiscal_year=None, snapshot_date=None)


def test_appending_onto_a_log_written_before_execution_id_existed() -> None:
    """The log is a parquet blob with no migration step, so old rows lack it."""
    # Arrange
    legacy = build_empty_ingestion_log().drop("execution_id")
    legacy = pl.concat(
        [
            legacy,
            pl.DataFrame(
                {
                    "dataset": ["jepx.spot_price"],
                    "fiscal_year": [2025],
                    "snapshot_date": [None],
                    "ingested_at": ["2026-01-01T00:00:00+00:00"],
                    "file_hash": ["old"],
                    "file_path": ["old.csv"],
                    "content_length": [1],
                    "etag": [None],
                    "last_modified": [None],
                    "is_latest": [True],
                    "bronze_status": ["processed"],
                    "bronze_processed_at": [None],
                },
                schema={
                    k: v for k, v in INGESTION_LOG_SCHEMA.items() if k != "execution_id"
                },
            ),
        ],
        how="vertical_relaxed",
    )
    storage = FakeStorage(legacy)

    # Act
    _append(storage, fiscal_year=2026, file_path="new.csv")

    # Assert
    log = storage.read_log().sort("file_path")
    assert log["file_path"].to_list() == ["new.csv", "old.csv"]
    assert log["execution_id"].to_list() == ["run-1", None]

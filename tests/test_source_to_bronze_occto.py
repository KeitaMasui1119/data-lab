"""Unit tests for the OCCTO raw-to-bronze ingestion helpers.

Mirrors tests/test_ingest_jepx_metadata_catalog.py's approach: the
ingestion-log resolution/marking helpers are pure enough to unit test
without a real Iceberg catalog, so that's where coverage is focused (the
Iceberg append path itself has no equivalent coverage for JEPX either).
"""

from __future__ import annotations

import io
from datetime import UTC, datetime
from unittest.mock import MagicMock

import polars as pl
import pytest

from pipeline.bronze.source_to_bronze_occto_unit_generation_actuals import (
    INGESTION_LOG_KEY,
    _normalize_unit_name,
    _resolve_effective_source_file_name,
    mark_ingestion_log_processed,
    resolve_raw_object_from_ingestion_log,
)


def _to_parquet_bytes(df: pl.DataFrame) -> bytes:
    buffer = io.BytesIO()
    df.write_parquet(buffer)
    return buffer.getvalue()


def _log_df(rows: list[dict]) -> pl.DataFrame:
    columns = {
        "dataset": pl.Utf8,
        "fiscal_year": pl.Int64,
        "snapshot_date": pl.Utf8,
        "ingested_at": pl.Utf8,
        "file_hash": pl.Utf8,
        "file_path": pl.Utf8,
        "content_length": pl.Int64,
        "etag": pl.Utf8,
        "last_modified": pl.Utf8,
        "is_latest": pl.Boolean,
        "bronze_status": pl.Utf8,
        "bronze_processed_at": pl.Utf8,
    }
    return pl.DataFrame(
        {
            name: pl.Series([row.get(name) for row in rows], dtype=dtype)
            for name, dtype in columns.items()
        }
    )


# ---------------------------------------------------------------------------
# source_data defaulting — the Phase 3-4 bug fix
# ---------------------------------------------------------------------------


def test_source_file_name_defaults_to_full_object_key_when_not_provided():
    """A revised snapshot (different ingested_at) must not collide with the
    previous one on source_data, or it would be silently skipped as a
    duplicate. The full snapshot object key is unique per revision; a bare
    file name is not (it only varies by target_date)."""
    object_key = (
        "raw/occto/unit_generation/target_date=2026-08-07/"
        "ingested_at=20260807T093000/ユニット別発電実績_2026-08-07.csv"
    )

    assert _resolve_effective_source_file_name(object_key, None) == object_key


def test_source_file_name_uses_explicit_override_when_given():
    object_key = "raw/occto/unit_generation/target_date=2026-08-07/.../file.csv"

    assert _resolve_effective_source_file_name(object_key, "custom-label") == (
        "custom-label"
    )


# ---------------------------------------------------------------------------
# unit_name normalization — single-unit plants omit it in the real CSV (e.g.
# power_plant_code=52271, 電源開発 手取川第一), but bronze's unit_name is
# is_identifier=TRUE, required=TRUE, so a raw null must become '' before the
# frame is appended or PyArrow's schema cast rejects it.
# ---------------------------------------------------------------------------


def test_normalize_unit_name_fills_null_with_empty_string():
    df = pl.DataFrame({"unit_name": [None, "１号機"]})

    result = _normalize_unit_name(df)

    assert result["unit_name"].to_list() == ["", "１号機"]


def test_normalize_unit_name_leaves_non_null_values_untouched():
    df = pl.DataFrame({"unit_name": ["１号機", "２号機"]})

    result = _normalize_unit_name(df)

    assert result["unit_name"].to_list() == ["１号機", "２号機"]


# ---------------------------------------------------------------------------
# ingestion log resolution, keyed by snapshot_date (not fiscal_year)
# ---------------------------------------------------------------------------


def test_resolve_raw_object_from_ingestion_log_returns_latest_unprocessed():
    log_df = _log_df(
        [
            {
                "dataset": "occto_unit_generation",
                "fiscal_year": None,
                "snapshot_date": "2026-08-07",
                "ingested_at": "2026-08-07T09:00:00+00:00",
                "file_hash": "hash-1",
                "file_path": (
                    "raw/occto/unit_generation/target_date=2026-08-07/"
                    "ingested_at=20260807T090000/file.csv"
                ),
                "content_length": 10,
                "is_latest": False,
                "bronze_status": "processed",
                "bronze_processed_at": "2026-08-07T09:30:00+00:00",
            },
            {
                "dataset": "occto_unit_generation",
                "fiscal_year": None,
                "snapshot_date": "2026-08-07",
                "ingested_at": "2026-08-07T15:30:00+00:00",
                "file_hash": "hash-2",
                "file_path": (
                    "raw/occto/unit_generation/target_date=2026-08-07/"
                    "ingested_at=20260807T153000/file.csv"
                ),
                "content_length": 11,
                "is_latest": True,
                "bronze_status": "pending",
                "bronze_processed_at": None,
            },
        ]
    )
    client = MagicMock()
    client.get_object_or_none.return_value = _to_parquet_bytes(log_df)

    object_key, source_file_name = resolve_raw_object_from_ingestion_log(
        client=client,
        bucket_name="test-bucket",
        target_date=datetime(2026, 8, 7, tzinfo=UTC).date(),
        require_unprocessed=True,
    )

    assert object_key == (
        "raw/occto/unit_generation/target_date=2026-08-07/"
        "ingested_at=20260807T153000/file.csv"
    )
    assert source_file_name == object_key


def test_resolve_raw_object_from_ingestion_log_raises_when_no_target():
    log_df = _log_df(
        [
            {
                "dataset": "occto_unit_generation",
                "fiscal_year": None,
                "snapshot_date": "2026-08-07",
                "ingested_at": "2026-08-07T09:00:00+00:00",
                "file_hash": "hash-1",
                "file_path": (
                    "raw/occto/unit_generation/target_date=2026-08-07/"
                    "ingested_at=20260807T090000/file.csv"
                ),
                "content_length": 10,
                "is_latest": True,
                "bronze_status": "processed",
                "bronze_processed_at": "2026-08-07T09:30:00+00:00",
            },
        ]
    )
    client = MagicMock()
    client.get_object_or_none.return_value = _to_parquet_bytes(log_df)

    with pytest.raises(ValueError, match="No unprocessed latest snapshot"):
        resolve_raw_object_from_ingestion_log(
            client=client,
            bucket_name="test-bucket",
            target_date=datetime(2026, 8, 7, tzinfo=UTC).date(),
            require_unprocessed=True,
        )


def test_mark_ingestion_log_processed_updates_target_row():
    log_df = _log_df(
        [
            {
                "dataset": "occto_unit_generation",
                "fiscal_year": None,
                "snapshot_date": "2026-08-07",
                "ingested_at": "2026-08-07T15:30:00+00:00",
                "file_hash": "hash-2",
                "file_path": (
                    "raw/occto/unit_generation/target_date=2026-08-07/"
                    "ingested_at=20260807T153000/file.csv"
                ),
                "content_length": 11,
                "is_latest": True,
                "bronze_status": "pending",
                "bronze_processed_at": None,
            },
        ]
    )
    client = MagicMock()
    client.get_object_or_none.return_value = _to_parquet_bytes(log_df)

    target_object = (
        "raw/occto/unit_generation/target_date=2026-08-07/"
        "ingested_at=20260807T153000/file.csv"
    )
    updated = mark_ingestion_log_processed(
        client=client,
        bucket_name="test-bucket",
        object_key=target_object,
        processed_at=datetime(2026, 8, 7, 16, 0, 0, tzinfo=UTC),
    )

    assert updated is True
    client.upload_bytes.assert_called_once()

    upload_call = client.upload_bytes.call_args
    assert upload_call.kwargs["object_name"] == INGESTION_LOG_KEY

    updated_df = pl.read_parquet(io.BytesIO(upload_call.kwargs["body"]))
    target = updated_df.filter(pl.col("file_path") == target_object)
    assert target.item(0, "bronze_status") == "processed"
    assert target.item(0, "bronze_processed_at") == "2026-08-07T16:00:00+00:00"

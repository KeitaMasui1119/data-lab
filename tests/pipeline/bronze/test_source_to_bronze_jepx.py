from __future__ import annotations

import io
from datetime import UTC, datetime
from unittest.mock import MagicMock

import polars as pl
import pytest

from pipeline.bronze.source_to_bronze_jepx_spot_price import (
    INGESTION_LOG_KEY,
    mark_ingestion_log_processed,
    resolve_raw_object_from_ingestion_log,
)


def _to_parquet_bytes(df: pl.DataFrame) -> bytes:
    buffer = io.BytesIO()
    df.write_parquet(buffer)
    return buffer.getvalue()


def test_resolve_raw_object_from_ingestion_log_returns_latest_unprocessed() -> None:
    log_df = pl.DataFrame(
        {
            "dataset": ["jepx_spot_price", "jepx_spot_price"],
            "fiscal_year": [2026, 2026],
            "snapshot_date": ["2026-05-23", "2026-05-24"],
            "ingested_at": ["2026-05-23T00:00:00+00:00", "2026-05-24T00:00:00+00:00"],
            "file_hash": ["hash-1", "hash-2"],
            "file_path": [
                "raw/jepx/spot_price/year=2026/ingested_at=20260523T000000/spot.csv.gz",
                "raw/jepx/spot_price/year=2026/ingested_at=20260524T000000/spot.csv.gz",
            ],
            "content_length": [10, 11],
            "etag": [None, None],
            "last_modified": [None, None],
            "is_latest": [False, True],
            "bronze_status": ["processed", "pending"],
            "bronze_processed_at": ["2026-05-23T01:00:00+00:00", None],
        }
    )
    client = MagicMock()
    client.get_object_or_none.return_value = _to_parquet_bytes(log_df)

    object_key, source_file_name = resolve_raw_object_from_ingestion_log(
        client=client,
        bucket_name="test-bucket",
        fiscal_year=2026,
        require_unprocessed=True,
    )

    assert (
        object_key
        == "raw/jepx/spot_price/year=2026/ingested_at=20260524T000000/spot.csv.gz"
    )
    assert source_file_name == object_key


def test_resolve_raw_object_from_ingestion_log_raises_when_no_target() -> None:
    log_df = pl.DataFrame(
        {
            "dataset": ["jepx_spot_price"],
            "fiscal_year": [2026],
            "snapshot_date": ["2026-05-24"],
            "ingested_at": ["2026-05-24T00:00:00+00:00"],
            "file_hash": ["hash-2"],
            "file_path": [
                "raw/jepx/spot_price/year=2026/ingested_at=20260524T000000/spot.csv.gz"
            ],
            "content_length": [11],
            "etag": [None],
            "last_modified": [None],
            "is_latest": [True],
            "bronze_status": ["processed"],
            "bronze_processed_at": ["2026-05-24T01:00:00+00:00"],
        }
    )
    client = MagicMock()
    client.get_object_or_none.return_value = _to_parquet_bytes(log_df)

    with pytest.raises(ValueError, match="No unprocessed latest snapshot"):
        resolve_raw_object_from_ingestion_log(
            client=client,
            bucket_name="test-bucket",
            fiscal_year=2026,
            require_unprocessed=True,
        )


def test_mark_ingestion_log_processed_updates_target_row() -> None:
    log_df = pl.DataFrame(
        {
            "dataset": ["jepx_spot_price", "jepx_spot_price"],
            "fiscal_year": [2026, 2026],
            "snapshot_date": ["2026-05-23", "2026-05-24"],
            "ingested_at": ["2026-05-23T00:00:00+00:00", "2026-05-24T00:00:00+00:00"],
            "file_hash": ["hash-1", "hash-2"],
            "file_path": [
                "raw/jepx/spot_price/year=2026/ingested_at=20260523T000000/spot.csv.gz",
                "raw/jepx/spot_price/year=2026/ingested_at=20260524T000000/spot.csv.gz",
            ],
            "content_length": [10, 11],
            "etag": [None, None],
            "last_modified": [None, None],
            "is_latest": [False, True],
            "bronze_status": ["processed", "pending"],
            "bronze_processed_at": ["2026-05-23T01:00:00+00:00", None],
        }
    )

    client = MagicMock()
    client.get_object_or_none.return_value = _to_parquet_bytes(log_df)

    target_object = (
        "raw/jepx/spot_price/year=2026/ingested_at=20260524T000000/spot.csv.gz"
    )
    updated = mark_ingestion_log_processed(
        client=client,
        bucket_name="test-bucket",
        object_key=target_object,
        processed_at=datetime(2026, 5, 24, 12, 0, 0, tzinfo=UTC),
    )

    assert updated is True
    client.upload_bytes.assert_called_once()

    upload_call = client.upload_bytes.call_args
    assert upload_call.kwargs["object_name"] == INGESTION_LOG_KEY

    updated_df = pl.read_parquet(io.BytesIO(upload_call.kwargs["body"]))
    target = updated_df.filter(pl.col("file_path") == target_object)
    assert target.item(0, "bronze_status") == "processed"
    assert target.item(0, "bronze_processed_at") == "2026-05-24T12:00:00+00:00"

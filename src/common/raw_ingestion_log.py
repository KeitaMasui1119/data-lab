"""Reusable helpers for raw ingestion log metadata operations."""

from __future__ import annotations

import io
from datetime import UTC, datetime

import polars as pl

from common.storage_client import RustFSClient

DEFAULT_INGESTION_LOG_KEY = "metadata/raw_ingestion_log.parquet"


def load_ingestion_log(
    client: RustFSClient,
    bucket_name: str,
    log_key: str = DEFAULT_INGESTION_LOG_KEY,
) -> pl.DataFrame:
    """Load ingestion log parquet from object storage.

    Returns an empty dataframe when the object does not exist.
    """
    payload = client.get_object_or_none(bucket_name, log_key)
    if payload is None:
        return pl.DataFrame()

    return pl.read_parquet(io.BytesIO(payload))


def resolve_latest_raw_object(
    client: RustFSClient,
    bucket_name: str,
    dataset: str,
    fiscal_year: int,
    require_unprocessed: bool = True,
    log_key: str = DEFAULT_INGESTION_LOG_KEY,
) -> str:
    """Resolve latest raw object key for a dataset/year from ingestion log."""
    ingestion_log = load_ingestion_log(client, bucket_name, log_key=log_key)
    if ingestion_log.is_empty():
        raise ValueError(
            f"Ingestion log not found or empty: s3://{bucket_name}/{log_key}"
        )

    filtered = ingestion_log.filter(
        (pl.col("dataset") == dataset)
        & (pl.col("fiscal_year") == fiscal_year)
        & pl.col("is_latest")
    )

    if require_unprocessed:
        filtered = filtered.filter(pl.col("bronze_status") != "processed")

    if filtered.is_empty():
        state_label = "unprocessed latest" if require_unprocessed else "latest"
        raise ValueError(
            f"No {state_label} snapshot found in ingestion log "
            f"for fiscal_year={fiscal_year}"
        )

    latest = filtered.sort("ingested_at", descending=True).head(1)
    object_key = latest.item(0, "file_path")
    if not isinstance(object_key, str) or not object_key:
        raise ValueError("Invalid file_path in ingestion log")

    return object_key


def mark_raw_object_processed(
    client: RustFSClient,
    bucket_name: str,
    object_key: str,
    processed_at: datetime | None = None,
    log_key: str = DEFAULT_INGESTION_LOG_KEY,
) -> bool:
    """Mark one raw object row as processed in ingestion log."""
    ingestion_log = load_ingestion_log(client, bucket_name, log_key=log_key)
    if ingestion_log.is_empty() or "file_path" not in ingestion_log.columns:
        return False

    target_rows = ingestion_log.filter(pl.col("file_path") == object_key)
    if target_rows.is_empty():
        return False

    processed_at = processed_at or datetime.now(UTC)
    processed_at_iso = processed_at.isoformat()

    updated_log = ingestion_log.with_columns(
        pl.when(pl.col("file_path") == object_key)
        .then(pl.lit("processed"))
        .otherwise(pl.col("bronze_status"))
        .alias("bronze_status"),
        pl.when(pl.col("file_path") == object_key)
        .then(pl.lit(processed_at_iso))
        .otherwise(pl.col("bronze_processed_at"))
        .alias("bronze_processed_at"),
    )

    buffer = io.BytesIO()
    updated_log.write_parquet(buffer)
    client.upload_bytes(
        bucket_name=bucket_name,
        object_name=log_key,
        body=buffer.getvalue(),
        content_type="application/x-parquet",
    )
    return True

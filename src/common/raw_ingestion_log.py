"""Reusable helpers for raw ingestion log metadata operations.

The log is one parquet object listing every raw snapshot ever saved: which
file, from which source, with what hash, and whether bronze has consumed it.
``execution_id`` names the run that fetched it, which is the same id that run
records in ``metadata.pipeline_run_log`` and stamps on the bronze and silver
rows it goes on to write.

Each scraper used to carry its own copy of the empty-frame builder and the
append, and the copies drifted -- JEPX's scoped its is_latest flip by
fiscal_year alone, clearing the flag on every other dataset's row for that
year. There is one implementation now.
"""

from __future__ import annotations

import io
from datetime import UTC, datetime

import polars as pl

from common.storage_client import RustFSClient

DEFAULT_INGESTION_LOG_KEY = "metadata/raw_ingestion_log.parquet"

# Explicit dtypes throughout: an empty frame with inferred columns comes back
# as Null-typed and will not concat with a populated row.
INGESTION_LOG_SCHEMA: dict[str, pl.DataType] = {
    "dataset": pl.Utf8(),
    "fiscal_year": pl.Int64(),
    "snapshot_date": pl.Utf8(),
    "ingested_at": pl.Utf8(),
    "file_hash": pl.Utf8(),
    "file_path": pl.Utf8(),
    "content_length": pl.Int64(),
    "etag": pl.Utf8(),
    "last_modified": pl.Utf8(),
    "is_latest": pl.Boolean(),
    "bronze_status": pl.Utf8(),
    "bronze_processed_at": pl.Utf8(),
    "execution_id": pl.Utf8(),
}


def build_empty_ingestion_log() -> pl.DataFrame:
    """Return an empty log frame with every column's dtype pinned."""
    return pl.DataFrame(schema=INGESTION_LOG_SCHEMA)


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


def append_ingestion_log_entry(
    client: RustFSClient,
    bucket_name: str,
    *,
    dataset: str,
    ingested_at: datetime,
    file_hash: str,
    file_path: str,
    content_length: int,
    execution_id: str,
    fiscal_year: int | None = None,
    snapshot_date: str | None = None,
    etag: str | None = None,
    last_modified: str | None = None,
    log_key: str = DEFAULT_INGESTION_LOG_KEY,
) -> None:
    """Record one saved snapshot and demote the entry it supersedes.

    "Supersedes" is scoped by the column that keys the dataset: fiscal year
    for the sources published as one file per year (JEPX, supply_demand), and
    snapshot date for the ones published per day (OCCTO, power_usage). JEPX
    sets both -- its snapshot_date is informational -- so a fiscal year wins
    the scope whenever one is given. The scope is always narrowed by dataset
    as well, so one source's snapshot never clears another's latest flag.
    """
    if fiscal_year is None and snapshot_date is None:
        raise ValueError(
            f"An ingestion log entry for {dataset!r} needs a fiscal_year or "
            "snapshot_date to scope which entry it supersedes"
        )

    existing_log = load_ingestion_log(client, bucket_name, log_key=log_key)
    if existing_log.is_empty():
        existing_log = build_empty_ingestion_log()
    else:
        if fiscal_year is not None:
            in_scope = pl.col("fiscal_year") == fiscal_year
        else:
            in_scope = pl.col("snapshot_date") == snapshot_date
        existing_log = existing_log.with_columns(
            pl.when((pl.col("dataset") == dataset) & in_scope)
            .then(pl.lit(False))
            .otherwise(pl.col("is_latest"))
            .alias("is_latest")
        )

    new_row = pl.DataFrame(
        {
            "dataset": [dataset],
            "fiscal_year": [fiscal_year],
            "snapshot_date": [snapshot_date],
            "ingested_at": [ingested_at.isoformat()],
            "file_hash": [file_hash],
            "file_path": [file_path],
            "content_length": [content_length],
            "etag": [etag],
            "last_modified": [last_modified],
            "is_latest": [True],
            "bronze_status": ["pending"],
            "bronze_processed_at": [None],
            "execution_id": [execution_id],
        },
        schema=INGESTION_LOG_SCHEMA,
    )

    # vertical_relaxed, not vertical: a log written before execution_id existed
    # has no such column, and this is the only migration the parquet blob gets.
    updated_log = pl.concat([existing_log, new_row], how="diagonal_relaxed")
    buffer = io.BytesIO()
    updated_log.write_parquet(buffer)
    client.upload_bytes(
        bucket_name=bucket_name,
        object_name=log_key,
        body=buffer.getvalue(),
        content_type="application/x-parquet",
    )


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


def resolve_latest_raw_object_by_snapshot_date(
    client: RustFSClient,
    bucket_name: str,
    dataset: str,
    snapshot_date: str,
    require_unprocessed: bool = True,
    log_key: str = DEFAULT_INGESTION_LOG_KEY,
) -> str:
    """Resolve latest raw object key for a dataset/snapshot_date from ingestion log.

    Counterpart to resolve_latest_raw_object() for datasets keyed by a
    calendar date (e.g. OCCTO unit generation) rather than a fiscal year
    (e.g. JEPX spot price).
    """
    ingestion_log = load_ingestion_log(client, bucket_name, log_key=log_key)
    if ingestion_log.is_empty():
        raise ValueError(
            f"Ingestion log not found or empty: s3://{bucket_name}/{log_key}"
        )

    filtered = ingestion_log.filter(
        (pl.col("dataset") == dataset)
        & (pl.col("snapshot_date") == snapshot_date)
        & pl.col("is_latest")
    )

    if require_unprocessed:
        filtered = filtered.filter(pl.col("bronze_status") != "processed")

    if filtered.is_empty():
        state_label = "unprocessed latest" if require_unprocessed else "latest"
        raise ValueError(
            f"No {state_label} snapshot found in ingestion log "
            f"for snapshot_date={snapshot_date}"
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

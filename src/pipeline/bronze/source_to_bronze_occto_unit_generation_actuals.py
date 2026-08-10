import io
import logging
import sys
from datetime import date
from pathlib import Path

import polars as pl

sys.path.append(str(Path(__file__).resolve().parent.parent.parent))

from common.iceberg import get_catalog
from common.pipeline_utilities import add_metadata, build_schema_exprs
from common.raw_ingestion_log import (
    DEFAULT_INGESTION_LOG_KEY,
    mark_raw_object_processed,
    resolve_latest_raw_object_by_snapshot_date,
)
from common.raw_object_io import read_object_text
from common.storage_client import RustFSClient

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)

SCHEMA_PATH = (
    "/workspace/configuration/iceberg/schema/bronze/occto_unit_generation_actuals.csv"
)
DEFAULT_TABLE = "bronze.occto_unit_generation_actuals"
DATASET_NAME = "occto_unit_generation"
INGESTION_LOG_KEY = DEFAULT_INGESTION_LOG_KEY


def source_data_exists(table, source_file_name: str) -> bool:
    row_filter = f"source_data == '{source_file_name}'"
    existing = table.scan(row_filter=row_filter).to_arrow()
    return existing.num_rows > 0


def _resolve_effective_source_file_name(
    object_key: str, source_file_name: str | None
) -> str:
    """Resolve the value to store in source_data for dedup/versioning.

    Defaults to the full snapshot object key (including its ingested_at=
    component), not just the trailing file name segment. OCCTO republishes
    revised actuals under the same target_date, and the file name alone
    (e.g. "ユニット別発電実績_2026-08-07.csv") is identical across revisions
    of the same day, so using it as source_data would make a revision look
    like an already-ingested duplicate and silently skip it.
    """
    return source_file_name or object_key


def resolve_raw_object_from_ingestion_log(
    client: RustFSClient,
    bucket_name: str,
    target_date: date,
    require_unprocessed: bool = True,
) -> tuple[str, str]:
    """Resolve latest raw snapshot object key from ingestion log metadata."""
    object_key = resolve_latest_raw_object_by_snapshot_date(
        client=client,
        bucket_name=bucket_name,
        dataset=DATASET_NAME,
        snapshot_date=target_date.isoformat(),
        require_unprocessed=require_unprocessed,
        log_key=INGESTION_LOG_KEY,
    )
    return object_key, object_key


def _normalize_unit_name(df: pl.DataFrame) -> pl.DataFrame:
    """Fill null unit_name with ''.

    OCCTO omits ユニット名 for single-unit plants (observed for
    power_plant_code=52271, 電源開発 手取川第一), which polars parses as
    null. Bronze's unit_name is part of the natural key and is
    is_identifier=TRUE, required=TRUE, so a null value would fail PyArrow's
    schema cast on append; '' is a valid, distinct key value instead.
    """
    return df.with_columns(pl.col("unit_name").fill_null(""))


def mark_ingestion_log_processed(
    client: RustFSClient,
    bucket_name: str,
    object_key: str,
    processed_at=None,
) -> bool:
    """Mark one raw snapshot as processed in the ingestion log."""
    updated = mark_raw_object_processed(
        client=client,
        bucket_name=bucket_name,
        object_key=object_key,
        processed_at=processed_at,
        log_key=INGESTION_LOG_KEY,
    )
    if updated:
        logger.info("Updated ingestion log status to processed for %s", object_key)
    else:
        logger.warning(
            "Skipping ingestion-log update because target row was not found: %s",
            object_key,
        )
    return updated


def ingest_occto_unit_generation(
    client: RustFSClient,
    bucket_name: str,
    object_key: str | None,
    source_file_name: str | None,
    catalog_name: str = "dlh_dev",
    table_identifier: str = DEFAULT_TABLE,
    schema_path: str = SCHEMA_PATH,
    skip_if_exists: bool = True,
    target_date: date | None = None,
    use_ingestion_log: bool = False,
    require_unprocessed: bool = True,
    update_ingestion_log_status: bool = True,
) -> int:
    if use_ingestion_log and object_key is None:
        if target_date is None:
            raise ValueError(
                "target_date is required when use_ingestion_log is enabled"
            )
        object_key, source_file_name = resolve_raw_object_from_ingestion_log(
            client=client,
            bucket_name=bucket_name,
            target_date=target_date,
            require_unprocessed=require_unprocessed,
        )

    if object_key is None:
        raise ValueError("object_key is required")

    source_file_name = _resolve_effective_source_file_name(object_key, source_file_name)

    logger.info("Starting raw-to-bronze ingestion: s3://%s/%s", bucket_name, object_key)

    catalog = get_catalog(catalog_name)
    table = catalog.load_table(table_identifier)

    if skip_if_exists and source_data_exists(table, source_file_name):
        logger.info(
            "Skipped ingestion because source_data already exists: %s",
            source_file_name,
        )
        if use_ingestion_log and update_ingestion_log_status:
            mark_ingestion_log_processed(
                client=client,
                bucket_name=bucket_name,
                object_key=object_key,
            )
        return 0

    try:
        decoded = read_object_text(
            client=client,
            bucket_name=bucket_name,
            object_key=object_key,
            encoding="utf-8-sig",
        )
    except UnicodeDecodeError as error:
        raise ValueError(
            f"Failed to decode object with utf-8-sig: s3://{bucket_name}/{object_key}"
        ) from error

    csv_string_io = io.StringIO(decoded)
    raw_df = pl.read_csv(csv_string_io, infer_schema_length=0)

    select_exprs = build_schema_exprs(schema_path)
    cast_df = raw_df.select(select_exprs)
    cast_df = _normalize_unit_name(cast_df)

    cast_df = cast_df.with_columns(
        pl.lit(source_file_name).alias("source_data"),
        pl.lit("new").alias("status"),
    )
    df_with_metadata = add_metadata(cast_df)

    target_schema = table.schema().as_arrow()
    arrow_table = df_with_metadata.to_arrow()
    casted_arrow_table = arrow_table.cast(target_schema)
    table.append(casted_arrow_table)

    row_count = len(df_with_metadata)
    logger.info(
        "Ingested %s rows into %s from source_data=%s",
        row_count,
        table_identifier,
        source_file_name,
    )

    if use_ingestion_log and update_ingestion_log_status:
        mark_ingestion_log_processed(
            client=client,
            bucket_name=bucket_name,
            object_key=object_key,
        )

    return row_count

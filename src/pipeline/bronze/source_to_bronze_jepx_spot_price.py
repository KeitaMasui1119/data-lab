import io
import logging
import sys
from datetime import datetime
from pathlib import Path

import polars as pl

sys.path.append(str(Path(__file__).resolve().parent.parent.parent))

from common.iceberg.catalog import get_catalog
from common.pipeline_utilities import add_metadata, build_schema_exprs
from common.raw_ingestion_log import (
    DEFAULT_INGESTION_LOG_KEY,
    mark_raw_object_processed,
    resolve_latest_raw_object,
)
from common.raw_object_io import read_object_text
from common.storage_client import RustFSClient
from pipeline.jepx_common import (
    resolve_spot_summary_file_name,
    resolve_spot_summary_object_key,
)

# ログ設定
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)

SCHEMA_PATH = (
    "/workspace/configuration/iceberg/schema/bronze/jepx_spot_price/jepx_spot_price.csv"
)
INGESTION_LOG_KEY = DEFAULT_INGESTION_LOG_KEY


def resolve_default_raw_object(target_at: datetime) -> tuple[str, str]:
    return (
        resolve_spot_summary_object_key(target_at),
        resolve_spot_summary_file_name(target_at),
    )


def source_data_exists(table, source_file_name: str) -> bool:
    row_filter = f"source_data == '{source_file_name}'"
    existing = table.scan(row_filter=row_filter).to_arrow()
    return existing.num_rows > 0


def resolve_raw_object_from_ingestion_log(
    client: RustFSClient,
    bucket_name: str,
    fiscal_year: int,
    require_unprocessed: bool = True,
) -> tuple[str, str]:
    """Resolve latest raw snapshot object key from ingestion log metadata."""
    object_key = resolve_latest_raw_object(
        client=client,
        bucket_name=bucket_name,
        dataset="jepx.spot_price",
        fiscal_year=fiscal_year,
        require_unprocessed=require_unprocessed,
        log_key=INGESTION_LOG_KEY,
    )

    source_file_name = object_key

    return object_key, source_file_name


def mark_ingestion_log_processed(
    client: RustFSClient,
    bucket_name: str,
    object_key: str,
    processed_at: datetime | None = None,
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


def run_source_to_bronze_jepx_spot_price(
    client: RustFSClient,
    bucket_name: str,
    object_key: str | None,
    source_file_name: str | None,
    catalog_name: str = "dlh_dev",
    table_identifier: str = "bronze.jepx_spot_price",
    schema_path: str = SCHEMA_PATH,
    skip_if_exists: bool = True,
    fiscal_year: int | None = None,
    use_ingestion_log: bool = False,
    require_unprocessed: bool = True,
    update_ingestion_log_status: bool = True,
) -> int:
    if use_ingestion_log and object_key is None:
        if fiscal_year is None:
            raise ValueError(
                "fiscal_year is required when use_ingestion_log is enabled"
            )
        object_key, source_file_name = resolve_raw_object_from_ingestion_log(
            client=client,
            bucket_name=bucket_name,
            fiscal_year=fiscal_year,
            require_unprocessed=require_unprocessed,
        )

    if object_key is None:
        raise ValueError("object_key is required")

    if source_file_name is None:
        source_file_name = Path(object_key).name

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
            encoding="cp932",
        )
    except UnicodeDecodeError as error:
        raise ValueError(
            f"Failed to decode object with cp932: s3://{bucket_name}/{object_key}"
        ) from error

    csv_string_io = io.StringIO(decoded)
    raw_df = pl.read_csv(csv_string_io, infer_schema_length=0)

    select_exprs = build_schema_exprs(schema_path)
    cast_df = raw_df.select(select_exprs)

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

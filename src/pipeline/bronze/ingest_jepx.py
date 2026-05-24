import argparse
import gzip
import io
import logging
import sys
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

sys.path.append(str(Path(__file__).resolve().parent.parent.parent))

from common.iceberg import get_catalog
from common.jepx_common import (
    resolve_spot_summary_file_name,
    resolve_spot_summary_object_key,
    resolve_target_at,
)
from common.pipeline_utilities import add_metadata, build_schema_exprs
from common.storage_client import RustFSClient

# ログ設定
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)

SCHEMA_PATH = "/workspace/configuration/iceberg/schema/bronze/jepx_spot_price.csv"
INGESTION_LOG_KEY = "metadata/raw_ingestion_log.parquet"


def resolve_default_raw_object(target_at: datetime) -> tuple[str, str]:
    file_name = resolve_spot_summary_file_name(target_at)
    object_key = resolve_spot_summary_object_key(target_at)
    return object_key, file_name


def source_data_exists(table, source_file_name: str) -> bool:
    row_filter = f"source_data == '{source_file_name}'"
    existing = table.scan(row_filter=row_filter).to_arrow()
    return existing.num_rows > 0


def _load_ingestion_log(client: RustFSClient, bucket_name: str) -> pl.DataFrame:
    payload = client.get_object_or_none(bucket_name, INGESTION_LOG_KEY)
    if payload is None:
        return pl.DataFrame()

    return pl.read_parquet(io.BytesIO(payload))


def resolve_raw_object_from_ingestion_log(
    client: RustFSClient,
    bucket_name: str,
    fiscal_year: int,
    require_unprocessed: bool = True,
) -> tuple[str, str]:
    """Resolve latest raw snapshot object key from ingestion log metadata."""
    ingestion_log = _load_ingestion_log(client, bucket_name)
    if ingestion_log.is_empty():
        raise ValueError(
            f"Ingestion log not found or empty: s3://{bucket_name}/{INGESTION_LOG_KEY}"
        )

    filtered = ingestion_log.filter(
        (pl.col("dataset") == "jepx.spot_price")
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

    source_file_name = object_key

    return object_key, source_file_name


def mark_ingestion_log_processed(
    client: RustFSClient,
    bucket_name: str,
    object_key: str,
    processed_at: datetime | None = None,
) -> bool:
    """Mark one raw snapshot as processed in the ingestion log."""
    ingestion_log = _load_ingestion_log(client, bucket_name)
    if ingestion_log.is_empty():
        logger.warning(
            "Skipping ingestion-log update because log is missing: s3://%s/%s",
            bucket_name,
            INGESTION_LOG_KEY,
        )
        return False

    if "file_path" not in ingestion_log.columns:
        logger.warning(
            "Skipping ingestion-log update because file_path column is missing"
        )
        return False

    target_rows = ingestion_log.filter(pl.col("file_path") == object_key)
    if target_rows.is_empty():
        logger.warning(
            "Skipping ingestion-log update because object_key was not found: %s",
            object_key,
        )
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
        object_name=INGESTION_LOG_KEY,
        body=buffer.getvalue(),
        content_type="application/x-parquet",
    )
    logger.info("Updated ingestion log status to processed for %s", object_key)
    return True


def ingest_jepx_spot_summary(
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

    response = client.get_object(bucket_name=bucket_name, object_name=object_key)
    raw_bytes = gzip.decompress(response) if object_key.endswith(".gz") else response

    try:
        decoded = raw_bytes.decode("cp932")
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


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Ingest JEPX spot summary raw CSV into bronze Iceberg table"
    )
    parser.add_argument(
        "--bucket",
        default="jp-power-grid-dev",
        help="Source bucket name (default: jp-power-grid-dev)",
    )
    parser.add_argument(
        "--object-key",
        help="Source object key in raw layer (default resolved from timestamp)",
    )
    parser.add_argument(
        "--source-file-name",
        help="Source file name stored in source_data (default from object key)",
    )
    parser.add_argument(
        "--timestamp-ms",
        type=int,
        help="Optional UNIX timestamp (ms) to resolve default source file",
    )
    parser.add_argument(
        "--catalog",
        default="dlh_dev",
        help="Iceberg catalog name (default: dlh_dev)",
    )
    parser.add_argument(
        "--table",
        default="bronze.jepx_spot_price",
        help="Target Iceberg table identifier",
    )
    parser.add_argument(
        "--schema-path",
        default=SCHEMA_PATH,
        help="Schema CSV path",
    )
    parser.add_argument(
        "--allow-duplicate-source",
        action="store_true",
        help="Allow append even if source_data already exists",
    )
    parser.add_argument(
        "--use-ingestion-log",
        action="store_true",
        help="Resolve latest raw snapshot from metadata ingestion log",
    )
    parser.add_argument(
        "--require-unprocessed",
        action="store_true",
        help="When using ingestion log, select only unprocessed latest snapshot",
    )
    args = parser.parse_args()

    target_at = resolve_target_at(args.timestamp_ms)
    fiscal_year = target_at.year if target_at.month >= 4 else target_at.year - 1

    default_object_key, default_file_name = resolve_default_raw_object(target_at)
    object_key = args.object_key or default_object_key
    source_file_name = args.source_file_name or Path(object_key).name

    if args.use_ingestion_log and args.object_key is None:
        object_key = None
        source_file_name = None

    client = RustFSClient()
    ingest_jepx_spot_summary(
        client=client,
        bucket_name=args.bucket,
        object_key=object_key,
        source_file_name=source_file_name,
        catalog_name=args.catalog,
        table_identifier=args.table,
        schema_path=args.schema_path,
        skip_if_exists=not args.allow_duplicate_source,
        fiscal_year=fiscal_year,
        use_ingestion_log=args.use_ingestion_log,
        require_unprocessed=args.require_unprocessed,
        update_ingestion_log_status=args.use_ingestion_log,
    )


if __name__ == "__main__":
    main()

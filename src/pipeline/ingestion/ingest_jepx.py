import argparse
import io
import logging
import sys
from datetime import datetime
from pathlib import Path

import polars as pl

sys.path.append(str(Path(__file__).resolve().parent.parent.parent))

from catalog.manage_iceberg import get_catalog
from core.storage_client import RustFSClient
from pipeline.jepx.common import (
    resolve_spot_summary_file_name,
    resolve_spot_summary_object_key,
    resolve_target_at,
)
from utility.pipeline_utilities import add_metadata, build_schema_exprs

# ログ設定
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)

SCHEMA_PATH = "/workspace/data/schema/bronze/jepx_spot_price.csv"


def resolve_default_raw_object(target_at: datetime) -> tuple[str, str]:
    file_name = resolve_spot_summary_file_name(target_at)
    object_key = resolve_spot_summary_object_key(target_at)
    return object_key, file_name


def source_data_exists(table, source_file_name: str) -> bool:
    row_filter = f"source_data == '{source_file_name}'"
    existing = table.scan(row_filter=row_filter).to_arrow()
    return existing.num_rows > 0


def ingest_jepx_spot_summary(
    client: RustFSClient,
    bucket_name: str,
    object_key: str,
    source_file_name: str,
    catalog_name: str = "dlh_dev",
    table_identifier: str = "bronze.jepx_spot_price",
    schema_path: str = SCHEMA_PATH,
    skip_if_exists: bool = True,
) -> int:
    logger.info("Starting raw-to-bronze ingestion: s3://%s/%s", bucket_name, object_key)

    catalog = get_catalog(catalog_name)
    table = catalog.load_table(table_identifier)
    if skip_if_exists and source_data_exists(table, source_file_name):
        logger.info(
            "Skipped ingestion because source_data already exists: %s",
            source_file_name,
        )
        return 0

    response = client.get_object(bucket_name=bucket_name, object_name=object_key)

    try:
        decoded = response.decode("cp932")
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
    args = parser.parse_args()

    target_at = resolve_target_at(args.timestamp_ms)

    default_object_key, default_file_name = resolve_default_raw_object(target_at)
    object_key = args.object_key or default_object_key
    source_file_name = args.source_file_name or Path(object_key).name

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
    )


if __name__ == "__main__":
    main()

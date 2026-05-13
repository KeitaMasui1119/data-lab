import io
import logging
import sys
from pathlib import Path

import polars as pl

sys.path.append(str(Path(__file__).resolve().parent.parent.parent))

from catalog.manage_iceberg import get_catalog
from common.pipeline_utilities import add_metadata, build_schema_exprs
from common.storage_client import RustFSClient

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)

SCHEMA_PATH = (
    "/workspace/configuration/iceberg/schema/bronze/occto_unit_generation_actuals.csv"
)
DEFAULT_TABLE = "bronze.occto_unit_generation_actuals"


def source_data_exists(table, source_file_name: str) -> bool:
    row_filter = f"source_data == '{source_file_name}'"
    existing = table.scan(row_filter=row_filter).to_arrow()
    return existing.num_rows > 0


def ingest_occto_unit_generation(
    client: RustFSClient,
    bucket_name: str,
    object_key: str,
    source_file_name: str,
    catalog_name: str = "dlh_dev",
    table_identifier: str = DEFAULT_TABLE,
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
        decoded = response.decode("utf-8-sig")
    except UnicodeDecodeError as error:
        raise ValueError(
            f"Failed to decode object with utf-8-sig: s3://{bucket_name}/{object_key}"
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

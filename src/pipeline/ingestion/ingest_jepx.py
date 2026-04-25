import io
import logging
import sys
from pathlib import Path

import polars as pl

sys.path.append(str(Path(__file__).resolve().parent.parent.parent))

from catalog.manage_iceberg import get_catalog
from core.storage_client import RustFSClient
from utility.pipeline_utilities import add_metadata, build_schema_exprs

# ログ設定
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)

SCHEMA_PATH = "/workspace/data/schema/bronze/jepx_spot_price.csv"


def ingest_jepx_spot_summary(
    client: RustFSClient,
    bucket_name: str,
    object_key: str,
    source_file_name: str,
    catalog_name: str = "dlh_dev",
    table_identifier: str = "bronze.jepx_spot_price",
    schema_path: str = SCHEMA_PATH,
) -> int:
    logger.info(f"Fetching s3://{bucket_name}/{object_key}...")
    response = client.get_object(bucket_name=bucket_name, object_name=object_key)

    csv_string_io = io.StringIO(response.decode("cp932"))
    raw_df = pl.read_csv(csv_string_io, infer_schema_length=0)

    select_exprs = build_schema_exprs(schema_path)
    cast_df = raw_df.select(select_exprs)

    cast_df = cast_df.with_columns(
        pl.lit(source_file_name).alias("source_data"),
        pl.lit("new").alias("status"),
    )
    df_with_metadata = add_metadata(cast_df)

    catalog = get_catalog(catalog_name)
    table = catalog.load_table(table_identifier)
    target_schema = table.schema().as_arrow()

    arrow_table = df_with_metadata.to_arrow()
    casted_arrow_table = arrow_table.cast(target_schema)
    table.append(casted_arrow_table)

    row_count = len(df_with_metadata)
    logger.info(f"Successfully ingested {row_count} rows into {table_identifier}.")
    return row_count


def main() -> None:
    client = RustFSClient()
    ingest_jepx_spot_summary(
        client=client,
        bucket_name="jp-power-grid-dev",
        object_key="raw/jepx/spot_summary/spot_summary_2026.csv",
        source_file_name="spot_summary_2026.csv",
    )


if __name__ == "__main__":
    main()

import io
import logging

import polars as pl

from catalog.manage_iceberg import get_catalog
from core.storage_client import RustFSClient
from utility.pipeline_utilities import add_metadata, build_schema_exprs

# ログ設定
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)

container = RustFSClient()
logger.info("RustFSClient initialized successfully.")

bucket_name = "jp-power-grid-dev"
object_key = "raw/jepx/spot_summary/spot_summary_2026.csv"

SCHEMA_PATH = r"/workspace/data/schema/bronze/jepx_spot_price.csv"

try:
    print(f"Fetching s3://{bucket_name}/{object_key}...")
    response = container.get_object(bucket_name=bucket_name, object_name=object_key)

    csv_string_io = io.StringIO(response.decode("cp932"))
    raw_df = pl.read_csv(csv_string_io, infer_schema_length=0)

    print(f"Successfully loaded {len(raw_df)} rows as raw strings.")

    select_exprs = build_schema_exprs(SCHEMA_PATH)
    cast_df = raw_df.select(select_exprs)

    cast_df = cast_df.with_columns(
        pl.lit("spot_summary_2026.csv").alias("source_data"),
        pl.lit("new").alias("status"),
    )

    df_with_metadata = add_metadata(cast_df)

    print(df_with_metadata.head())

    catalog = get_catalog("dlh_dev")
    table = catalog.load_table("bronze.jepx_spot_price")

    target_schema = table.schema().as_arrow()

    arrow_table = df_with_metadata.to_arrow()
    casted_arrow_table = arrow_table.cast(target_schema)

    table.append(casted_arrow_table)

    print(f"Successfully ingested {len(df_with_metadata)} rows into the iceberg table.")

except Exception as e:
    logger.error(f"Error fetching object: {e}")
    raise

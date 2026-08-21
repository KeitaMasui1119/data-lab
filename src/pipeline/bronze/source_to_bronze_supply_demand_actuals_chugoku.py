"""Chugoku supply_demand_actuals raw-to-bronze ingestion.

The source is a flat DATE,TIME,実績(万kW) CSV -- build_schema_exprs() can
cast it directly (unlike power_usage_hokuriku's multi-section report,
which needs a custom parser).

The raw object is a whole calendar year, growing by one day daily, so
ingestion filters to exactly one target_date's rows (default: yesterday)
before appending. Because every run's raw snapshot has a fresh, unique
object key, dedup can't be "does this source_data already exist" the way
power_usage_hokuriku/OCCTO do it -- instead it checks whether bronze
already holds rows for this target_date.
"""

from __future__ import annotations

import io
import logging
from datetime import date

import polars as pl

from common.iceberg.catalog import get_catalog
from common.pipeline_utilities import add_metadata, build_schema_exprs
from common.raw_object_io import read_object_text
from common.storage_client import RustFSClient

logger = logging.getLogger(__name__)

TABLE_IDENTIFIER = "bronze.supply_demand_actuals_chugoku"
SCHEMA_PATH = (
    "/workspace/configuration/iceberg/schema/bronze/supply_demand_actuals/"
    "supply_demand_actuals_chugoku.csv"
)
HEADER_SKIP_ROWS = 2  # "<UPDATE line>\n\nDATE,TIME,..."


def parse_year_csv(text: str, header_skip_rows: int = HEADER_SKIP_ROWS) -> pl.DataFrame:
    """Skip the leading UPDATE/blank line(s) and read the rest as CSV."""
    lines = text.splitlines()
    body = "\n".join(lines[header_skip_rows:])
    return pl.read_csv(io.StringIO(body), infer_schema_length=0)


def extract_target_date_rows(df: pl.DataFrame, target_date: date) -> pl.DataFrame:
    """Filter to one target_date's rows and normalize DATE/TIME formatting.

    Source DATE is '2026/4/1' (unpadded); TIME is unpadded ('0:00'..
    '23:00'). Both are normalized (ISO date, zero-padded HH:MM) so
    bronze's target_date/target_time match power_usage_hokuriku's
    convention.
    """
    parsed = df.with_columns(
        pl.col("DATE").str.to_date(format="%Y/%m/%d", strict=True).alias("_date_parsed")
    )
    filtered = parsed.filter(pl.col("_date_parsed") == target_date)
    return filtered.with_columns(
        pl.col("_date_parsed").dt.strftime("%Y-%m-%d").alias("DATE"),
        (
            pl.col("TIME")
            .str.split(":")
            .list.get(0)
            .cast(pl.Int64)
            .cast(pl.Utf8)
            .str.zfill(2)
            + ":"
            + pl.col("TIME").str.split(":").list.get(1)
        ).alias("TIME"),
    ).drop("_date_parsed")


def target_date_exists(table, target_date: date) -> bool:
    row_filter = f"target_date == '{target_date.isoformat()}'"
    existing = table.scan(row_filter=row_filter).to_arrow()
    return existing.num_rows > 0


def run_source_to_bronze_supply_demand_actuals_chugoku(
    client: RustFSClient,
    bucket_name: str,
    object_key: str,
    target_date: date,
    source_file_name: str | None = None,
    catalog_name: str = "dlh_dev",
    skip_if_exists: bool = True,
) -> int:
    """Ingest one target_date's rows from Chugoku's year-CSV raw snapshot."""
    source_file_name = source_file_name or object_key

    logger.info(
        "Starting raw-to-bronze ingestion: company=chugoku, target_date=%s, s3://%s/%s",
        target_date,
        bucket_name,
        object_key,
    )

    catalog = get_catalog(catalog_name)
    table = catalog.load_table(TABLE_IDENTIFIER)

    if skip_if_exists and target_date_exists(table, target_date):
        logger.info(
            "Skipped ingestion because target_date already exists: "
            "company=chugoku, target_date=%s",
            target_date,
        )
        return 0

    text = read_object_text(
        client=client, bucket_name=bucket_name, object_key=object_key, encoding="cp932"
    )
    year_df = parse_year_csv(text)
    day_df = extract_target_date_rows(year_df, target_date)

    if day_df.is_empty():
        logger.warning(
            "No rows found for target_date=%s in company=chugoku snapshot; "
            "nothing to ingest",
            target_date,
        )
        return 0

    select_exprs = build_schema_exprs(SCHEMA_PATH)
    cast_df = day_df.select(select_exprs)
    cast_df = cast_df.with_columns(
        pl.lit(source_file_name).alias("source_data"),
        pl.lit("new").alias("status"),
    )
    df_with_metadata = add_metadata(cast_df)

    target_schema = table.schema().as_arrow()
    arrow_table = df_with_metadata.to_arrow().cast(target_schema)
    table.append(arrow_table)

    row_count = len(df_with_metadata)
    logger.info(
        "Ingested %s rows into %s from source_data=%s",
        row_count,
        TABLE_IDENTIFIER,
        source_file_name,
    )
    return row_count

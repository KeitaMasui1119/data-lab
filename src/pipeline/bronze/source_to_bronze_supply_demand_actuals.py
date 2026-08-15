"""supply_demand_actuals raw-to-bronze ingestion.

Shared across companies whose source is a flat DATE,TIME,実績(万kW)[,供給力
想定値(万kW)] CSV -- build_schema_exprs() can cast it directly (unlike
power_usage_hokuriku's multi-section report, which needs a custom parser).

The raw object is a whole calendar year, growing by one day daily, so
ingestion filters to exactly one target_date's rows (default: yesterday)
before appending. Because every run's raw snapshot has a fresh, unique
object key, dedup can't be "does this source_data already exist" the way
power_usage_hokuriku/OCCTO do it -- instead it checks whether Bronze
already holds rows for this (company, target_date).
"""

from __future__ import annotations

import io
import logging
from dataclasses import dataclass
from datetime import date

import polars as pl

from common.iceberg.catalog import get_catalog
from common.pipeline_utilities import add_metadata, build_schema_exprs
from common.raw_object_io import read_object_text
from common.storage_client import RustFSClient

logger = logging.getLogger(__name__)

SCHEMA_DIR = "/workspace/configuration/iceberg/schema/bronze"


@dataclass(frozen=True)
class SupplyDemandActualsBronzeConfig:
    """Per-company bronze ingestion parameters."""

    company_code: str
    table_identifier: str
    schema_path: str
    header_skip_rows: int  # lines to skip before the CSV header line


BRONZE_CONFIGS: dict[str, SupplyDemandActualsBronzeConfig] = {
    "tohoku": SupplyDemandActualsBronzeConfig(
        company_code="tohoku",
        table_identifier="bronze.supply_demand_actuals_tohoku",
        schema_path=f"{SCHEMA_DIR}/supply_demand_actuals_tohoku.csv",
        header_skip_rows=1,  # "<UPDATE line>\nDATE,TIME,..." -- no blank line
    ),
    "chugoku": SupplyDemandActualsBronzeConfig(
        company_code="chugoku",
        table_identifier="bronze.supply_demand_actuals_chugoku",
        schema_path=f"{SCHEMA_DIR}/supply_demand_actuals_chugoku.csv",
        header_skip_rows=2,  # "<UPDATE line>\n\nDATE,TIME,..."
    ),
    "shikoku": SupplyDemandActualsBronzeConfig(
        company_code="shikoku",
        table_identifier="bronze.supply_demand_actuals_shikoku",
        schema_path=f"{SCHEMA_DIR}/supply_demand_actuals_shikoku.csv",
        header_skip_rows=2,  # "<UPDATE line>\n\nDATE,TIME,...,供給力想定値(万kW)"
    ),
}


def parse_year_csv(text: str, header_skip_rows: int) -> pl.DataFrame:
    """Skip the leading UPDATE/blank line(s) and read the rest as CSV."""
    lines = text.splitlines()
    body = "\n".join(lines[header_skip_rows:])
    return pl.read_csv(io.StringIO(body), infer_schema_length=0)


def extract_target_date_rows(df: pl.DataFrame, target_date: date) -> pl.DataFrame:
    """Filter to one target_date's rows and normalize DATE/TIME formatting.

    Source DATE is '2026/1/1' or '2026/01/01' depending on company; TIME is
    unpadded ('0:00'..'23:00'). Both are normalized (ISO date, zero-padded
    HH:MM) so bronze's target_date/target_time are consistent across
    companies, matching power_usage_hokuriku's convention.
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


def ingest_supply_demand_actuals(
    client: RustFSClient,
    bucket_name: str,
    company: str,
    object_key: str,
    target_date: date,
    source_file_name: str | None = None,
    catalog_name: str = "dlh_dev",
    skip_if_exists: bool = True,
) -> int:
    """Ingest one target_date's rows from a company's year-CSV raw snapshot."""
    config = BRONZE_CONFIGS[company]
    source_file_name = source_file_name or object_key

    logger.info(
        "Starting raw-to-bronze ingestion: company=%s, target_date=%s, s3://%s/%s",
        company,
        target_date,
        bucket_name,
        object_key,
    )

    catalog = get_catalog(catalog_name)
    table = catalog.load_table(config.table_identifier)

    if skip_if_exists and target_date_exists(table, target_date):
        logger.info(
            "Skipped ingestion because target_date already exists: "
            "company=%s, target_date=%s",
            company,
            target_date,
        )
        return 0

    text = read_object_text(
        client=client, bucket_name=bucket_name, object_key=object_key, encoding="cp932"
    )
    year_df = parse_year_csv(text, config.header_skip_rows)
    day_df = extract_target_date_rows(year_df, target_date)

    if day_df.is_empty():
        logger.warning(
            "No rows found for target_date=%s in company=%s snapshot; "
            "nothing to ingest",
            target_date,
            company,
        )
        return 0

    select_exprs = build_schema_exprs(config.schema_path)
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
        config.table_identifier,
        source_file_name,
    )
    return row_count

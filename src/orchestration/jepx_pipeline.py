"""JEPX pipeline orchestrator for end-to-end workflow execution.

This module provides an ADF-like orchestration layer that runs JEPX
processing steps in dependency order:
1. Source -> Raw
2. Raw -> Bronze
3. Bronze -> Silver (dbt staging and silver)
4. Silver -> Gold (optional dbt gold)
"""

from __future__ import annotations

import argparse
import logging
import subprocess
from dataclasses import dataclass
from pathlib import Path

import duckdb
import polars as pl

from catalog.manage_iceberg import get_catalog, provision_table
from common.storage_client import RustFSClient
from jepx.common import resolve_target_at
from pipeline.bronze.ingest_jepx import ingest_jepx_spot_summary
from pipeline.raw.jepx_to_rustfs import scrape_jepx_to_rustfs
from scraper.module.jepx import JEPXSpotSummaryScraper

logger = logging.getLogger(__name__)

DEFAULT_DBT_PROJECT_DIR = Path("/workspace/src/dbt/jepx_power")
DEFAULT_DBT_DUCKDB_PATH = Path("/workspace/src/dbt/jepx_power/jepx_power.duckdb")
DEFAULT_SCHEMA_PATH = (
    "/workspace/configuration/iceberg/schema/bronze/jepx_spot_price.csv"
)
DEFAULT_SILVER_EXPORT_MAPPINGS = (
    (
        "main_silver.silver_jepx_spot_price_base",
        "silver.jepx_spot_price_base",
        "/workspace/configuration/iceberg/schema/silver/jepx_spot_price_base.csv",
    ),
    (
        "main_silver.silver_jepx_spot_price_block",
        "silver.jepx_spot_price_block",
        "/workspace/configuration/iceberg/schema/silver/jepx_spot_price_block.csv",
    ),
    (
        "main_silver.silver_jepx_spot_price_area",
        "silver.jepx_spot_price_area",
        "/workspace/configuration/iceberg/schema/silver/jepx_spot_price_area.csv",
    ),
)


@dataclass(frozen=True)
class PipelineStepResult:
    """Represents the status of a single orchestrated pipeline step."""

    name: str
    status: str
    detail: str


def run_dbt_step(
    *,
    step_name: str,
    select_expr: str,
    project_dir: Path,
    profiles_dir: Path,
    full_refresh: bool,
) -> PipelineStepResult:
    """Run one dbt step and return its execution result."""
    dbt_command = [
        "uv",
        "run",
        "dbt",
        "run",
        "--project-dir",
        str(project_dir),
        "--profiles-dir",
        str(profiles_dir),
        "--select",
        select_expr,
    ]
    if full_refresh:
        dbt_command.append("--full-refresh")

    logger.info("Executing %s: %s", step_name, " ".join(dbt_command))
    subprocess.run(dbt_command, check=True)
    return PipelineStepResult(
        name=step_name,
        status="success",
        detail=f"dbt select={select_expr}",
    )


def export_jepx_silver_to_iceberg(
    *,
    catalog_name: str,
    duckdb_path: Path,
) -> PipelineStepResult:
    """Export dbt silver tables from DuckDB into PyIceberg silver tables."""
    if not duckdb_path.exists():
        raise FileNotFoundError(f"dbt DuckDB file does not exist: {duckdb_path}")

    catalog = get_catalog(catalog_name)
    exported_rows = 0

    conn = duckdb.connect(str(duckdb_path), read_only=True)
    try:
        for (
            source_table,
            target_identifier,
            schema_path,
        ) in DEFAULT_SILVER_EXPORT_MAPPINGS:
            provision_table(catalog, target_identifier, schema_path)

            source_df = conn.execute(f"SELECT * FROM {source_table};").pl()
            if source_df.is_empty():
                logger.info(
                    "Skipped silver export for %s because source table is empty",
                    source_table,
                )
                continue

            target_table = catalog.load_table(target_identifier)
            target_schema = target_table.schema().as_arrow()
            target_field_names = [field.name for field in target_schema]

            aligned_df = source_df
            for target_field_name in target_field_names:
                if target_field_name not in aligned_df.columns:
                    aligned_df = aligned_df.with_columns(
                        pl.lit(None).alias(target_field_name)
                    )

            aligned_df = aligned_df.select(target_field_names)

            arrow_table = aligned_df.to_arrow()
            casted_arrow_table = arrow_table.cast(target_schema)
            target_table.append(casted_arrow_table)

            exported_rows += len(aligned_df)
            logger.info(
                "Exported %s rows from %s to %s",
                len(aligned_df),
                source_table,
                target_identifier,
            )
    finally:
        conn.close()

    return PipelineStepResult(
        name="silver_to_iceberg",
        status="success",
        detail=f"duckdb={duckdb_path}, exported_rows={exported_rows}",
    )


def run_jepx_orchestrated_pipeline(
    *,
    bucket_name: str,
    timestamp_ms: int | None,
    catalog_name: str,
    bronze_table_identifier: str,
    bronze_schema_path: str,
    allow_duplicate_source: bool,
    dbt_project_dir: Path,
    dbt_profiles_dir: Path,
    staging_select: str,
    silver_select: str,
    run_gold_step: bool,
    gold_select: str,
    dbt_full_refresh: bool,
    export_silver_to_iceberg: bool,
    dbt_duckdb_path: Path,
) -> list[PipelineStepResult]:
    """Run the JEPX workflow in dependency order."""
    results: list[PipelineStepResult] = []
    target_at = resolve_target_at(timestamp_ms)

    rustfs = RustFSClient()

    scraper = JEPXSpotSummaryScraper()
    try:
        scraped = scrape_jepx_to_rustfs(
            storage_client=rustfs,
            scraper=scraper,
            bucket_name=bucket_name,
            target_at=target_at,
        )
    finally:
        scraper.close()

    results.append(
        PipelineStepResult(
            name="source_to_raw",
            status="success",
            detail=(
                f"s3://{scraped.bucket_name}/{scraped.object_key} "
                f"({scraped.size_bytes} bytes)"
            ),
        )
    )

    row_count = ingest_jepx_spot_summary(
        client=rustfs,
        bucket_name=bucket_name,
        object_key=scraped.object_key,
        source_file_name=scraped.file_name,
        catalog_name=catalog_name,
        table_identifier=bronze_table_identifier,
        schema_path=bronze_schema_path,
        skip_if_exists=not allow_duplicate_source,
    )
    results.append(
        PipelineStepResult(
            name="raw_to_bronze",
            status="success",
            detail=f"table={bronze_table_identifier}, rows={row_count}",
        )
    )

    results.append(
        run_dbt_step(
            step_name="bronze_to_silver_staging",
            select_expr=staging_select,
            project_dir=dbt_project_dir,
            profiles_dir=dbt_profiles_dir,
            full_refresh=dbt_full_refresh,
        )
    )
    results.append(
        run_dbt_step(
            step_name="silver_build",
            select_expr=silver_select,
            project_dir=dbt_project_dir,
            profiles_dir=dbt_profiles_dir,
            full_refresh=dbt_full_refresh,
        )
    )

    if export_silver_to_iceberg:
        results.append(
            export_jepx_silver_to_iceberg(
                catalog_name=catalog_name,
                duckdb_path=dbt_duckdb_path,
            )
        )
    else:
        results.append(
            PipelineStepResult(
                name="silver_to_iceberg",
                status="skipped",
                detail="Silver export is disabled. Use --export-silver-to-iceberg.",
            )
        )

    if run_gold_step:
        results.append(
            run_dbt_step(
                step_name="silver_to_gold",
                select_expr=gold_select,
                project_dir=dbt_project_dir,
                profiles_dir=dbt_profiles_dir,
                full_refresh=dbt_full_refresh,
            )
        )
    else:
        results.append(
            PipelineStepResult(
                name="silver_to_gold",
                status="skipped",
                detail="Gold step is disabled. Use --run-gold-step to enable.",
            )
        )

    return results


def build_parser() -> argparse.ArgumentParser:
    """Build argument parser for the JEPX orchestrator CLI."""
    parser = argparse.ArgumentParser(
        description="Run ADF-like orchestration for the JEPX pipeline"
    )
    parser.add_argument(
        "--bucket",
        default="jp-power-grid-dev",
        help="Source/target bucket name (default: jp-power-grid-dev)",
    )
    parser.add_argument(
        "--timestamp-ms",
        type=int,
        help="Optional UNIX timestamp in milliseconds for the JEPX run",
    )
    parser.add_argument(
        "--catalog",
        default="dlh_dev",
        help="Iceberg catalog name (default: dlh_dev)",
    )
    parser.add_argument(
        "--bronze-table",
        default="bronze.jepx_spot_price",
        help="Target bronze Iceberg table identifier",
    )
    parser.add_argument(
        "--bronze-schema-path",
        default=DEFAULT_SCHEMA_PATH,
        help="Bronze schema CSV path",
    )
    parser.add_argument(
        "--allow-duplicate-source",
        action="store_true",
        help="Allow append even if source_data already exists",
    )
    parser.add_argument(
        "--dbt-project-dir",
        default=str(DEFAULT_DBT_PROJECT_DIR),
        help="dbt project directory",
    )
    parser.add_argument(
        "--dbt-profiles-dir",
        help="dbt profiles directory (default: same as dbt project dir)",
    )
    parser.add_argument(
        "--staging-select",
        default="tag:staging",
        help="dbt select expression for staging step",
    )
    parser.add_argument(
        "--silver-select",
        default="tag:silver",
        help="dbt select expression for silver step",
    )
    parser.add_argument(
        "--run-gold-step",
        action="store_true",
        help="Enable gold step execution",
    )
    parser.add_argument(
        "--gold-select",
        default="tag:gold",
        help="dbt select expression for gold step",
    )
    parser.add_argument(
        "--dbt-full-refresh",
        action="store_true",
        help="Run dbt steps with --full-refresh",
    )
    parser.add_argument(
        "--export-silver-to-iceberg",
        action="store_true",
        help="Export dbt silver tables into PyIceberg silver tables",
    )
    parser.add_argument(
        "--dbt-duckdb-path",
        default=str(DEFAULT_DBT_DUCKDB_PATH),
        help="Path to dbt DuckDB file used as silver export source",
    )
    return parser


def main() -> None:
    """CLI entrypoint for JEPX pipeline orchestration."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )

    parser = build_parser()
    args = parser.parse_args()

    dbt_project_dir = Path(args.dbt_project_dir)
    if not dbt_project_dir.exists():
        parser.error(f"dbt project directory does not exist: {dbt_project_dir}")

    dbt_profiles_dir = (
        Path(args.dbt_profiles_dir) if args.dbt_profiles_dir else dbt_project_dir
    )
    if not dbt_profiles_dir.exists():
        parser.error(f"dbt profiles directory does not exist: {dbt_profiles_dir}")

    dbt_duckdb_path = Path(args.dbt_duckdb_path)
    if args.export_silver_to_iceberg and not dbt_duckdb_path.exists():
        parser.error(f"dbt DuckDB file does not exist: {dbt_duckdb_path}")

    results = run_jepx_orchestrated_pipeline(
        bucket_name=args.bucket,
        timestamp_ms=args.timestamp_ms,
        catalog_name=args.catalog,
        bronze_table_identifier=args.bronze_table,
        bronze_schema_path=args.bronze_schema_path,
        allow_duplicate_source=args.allow_duplicate_source,
        dbt_project_dir=dbt_project_dir,
        dbt_profiles_dir=dbt_profiles_dir,
        staging_select=args.staging_select,
        silver_select=args.silver_select,
        run_gold_step=args.run_gold_step,
        gold_select=args.gold_select,
        dbt_full_refresh=args.dbt_full_refresh,
        export_silver_to_iceberg=args.export_silver_to_iceberg,
        dbt_duckdb_path=dbt_duckdb_path,
    )

    logger.info("JEPX orchestrated pipeline summary:")
    for result in results:
        logger.info(
            " - step=%s, status=%s, detail=%s",
            result.name,
            result.status,
            result.detail,
        )


if __name__ == "__main__":
    main()

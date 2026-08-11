"""JEPX pipeline orchestrator for end-to-end workflow execution.

This module provides an ADF-like orchestration layer that runs JEPX
processing steps in dependency order:
1. Source -> Raw
2. Raw -> Bronze
3. Bronze -> Silver (DuckDB transform + PyIceberg window replace)
4. Silver -> Gold (optional dbt gold)
"""

from __future__ import annotations

import argparse
import logging
import subprocess
from pathlib import Path

from common.storage_client import RustFSClient
from orchestration.pipeline_result import PipelineStepResult
from pipeline.bronze.source_to_bronze_jepx_spot_price import ingest_jepx_spot_summary
from pipeline.jepx_common import resolve_target_at
from pipeline.raw.source_to_raw_jepx_spot_price import (
    JEPXSpotSummaryScraper,
    scrape_jepx_spot_price_raw,
)
from pipeline.silver.bronze_to_silver_jepx_spot_price import (
    DEFAULT_BRONZE_LOCATION,
    DEFAULT_SILVER_SCHEMA_DIR,
    run_bronze_to_silver_jepx_spot_price,
)

logger = logging.getLogger(__name__)

DEFAULT_DBT_PROJECT_DIR = Path("/workspace/src/dbt/jepx_power")
DEFAULT_SCHEMA_PATH = (
    "/workspace/configuration/iceberg/schema/bronze/jepx_spot_price.csv"
)


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


def resolve_silver_fiscal_year(
    *,
    silver_fiscal_year: int | None,
    silver_all_fiscal_years: bool,
    snapshot_fiscal_year: int,
) -> int | None:
    """Decide which fiscal year the silver step rebuilds.

    A run defaults to the fiscal year it just ingested. Rebuilding every year
    on every run scales with how much history the tables hold rather than with
    the new data, so a full refresh has to be asked for explicitly.
    """
    if silver_all_fiscal_years:
        return None
    if silver_fiscal_year is not None:
        return silver_fiscal_year
    return snapshot_fiscal_year


def run_bronze_to_silver_step(
    *,
    catalog_name: str,
    bronze_location: str,
    silver_schema_dir: str,
    fiscal_year: int | None,
) -> PipelineStepResult:
    """Transform bronze rows into the silver Iceberg tables."""
    result = run_bronze_to_silver_jepx_spot_price(
        catalog_name=catalog_name,
        bronze_location=bronze_location,
        schema_dir=silver_schema_dir,
        fiscal_year=fiscal_year,
    )

    written = sum(write.rows_written for write in result.writes)
    scope = "all fiscal years" if fiscal_year is None else f"fiscal_year={fiscal_year}"
    return PipelineStepResult(
        name="bronze_to_silver",
        status="success",
        detail=(
            f"execution_id={result.execution_id}, {scope}, written={written}, "
            f"dropped={result.dropped_row_count}"
        ),
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
    bronze_location: str,
    silver_schema_dir: str,
    silver_fiscal_year: int | None,
    silver_all_fiscal_years: bool = False,
    run_gold_step: bool,
    gold_select: str,
    dbt_full_refresh: bool,
) -> list[PipelineStepResult]:
    """Run the JEPX workflow in dependency order."""
    results: list[PipelineStepResult] = []
    target_at = resolve_target_at(timestamp_ms)

    rustfs = RustFSClient()

    scraper = JEPXSpotSummaryScraper()
    try:
        snapshot_result = scrape_jepx_spot_price_raw(
            storage_client=rustfs,
            scraper=scraper,
            bucket_name=bucket_name,
            target_at=target_at,
        )
    finally:
        scraper.close()

    if snapshot_result.skipped:
        results.append(
            PipelineStepResult(
                name="source_to_raw",
                status="skipped",
                detail=(
                    "No snapshot change detected. "
                    f"year={snapshot_result.year}, sha256={snapshot_result.sha256[:8]}"
                ),
            )
        )
    else:
        results.append(
            PipelineStepResult(
                name="source_to_raw",
                status="success",
                detail=(
                    "Saved snapshot and updated metadata catalog. "
                    f"year={snapshot_result.year}, "
                    f"prefix={snapshot_result.snapshot_prefix}"
                ),
            )
        )

    try:
        row_count = ingest_jepx_spot_summary(
            client=rustfs,
            bucket_name=bucket_name,
            object_key=None,
            source_file_name=None,
            catalog_name=catalog_name,
            table_identifier=bronze_table_identifier,
            schema_path=bronze_schema_path,
            skip_if_exists=not allow_duplicate_source,
            fiscal_year=snapshot_result.year,
            use_ingestion_log=True,
            require_unprocessed=True,
            update_ingestion_log_status=True,
        )
        results.append(
            PipelineStepResult(
                name="raw_to_bronze",
                status="success",
                detail=f"table={bronze_table_identifier}, rows={row_count}",
            )
        )
    except ValueError as error:
        results.append(
            PipelineStepResult(
                name="raw_to_bronze",
                status="skipped",
                detail=str(error),
            )
        )

    results.append(
        run_bronze_to_silver_step(
            catalog_name=catalog_name,
            bronze_location=bronze_location,
            silver_schema_dir=silver_schema_dir,
            fiscal_year=resolve_silver_fiscal_year(
                silver_fiscal_year=silver_fiscal_year,
                silver_all_fiscal_years=silver_all_fiscal_years,
                snapshot_fiscal_year=snapshot_result.year,
            ),
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
        "--bronze-location",
        default=DEFAULT_BRONZE_LOCATION,
        help="Bronze table location scanned by the silver transform",
    )
    parser.add_argument(
        "--silver-schema-dir",
        default=DEFAULT_SILVER_SCHEMA_DIR,
        help="Directory containing the silver schema CSV files",
    )
    parser.add_argument(
        "--silver-fiscal-year",
        type=int,
        help=(
            "Fiscal year for the silver step "
            "(default: the fiscal year that was just ingested)"
        ),
    )
    parser.add_argument(
        "--silver-all-fiscal-years",
        action="store_true",
        help="Rebuild every fiscal year in the silver step instead of just one",
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
    return parser


def main() -> None:
    """CLI entrypoint for JEPX pipeline orchestration."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )

    parser = build_parser()
    args = parser.parse_args()

    if args.silver_all_fiscal_years and args.silver_fiscal_year is not None:
        parser.error(
            "--silver-all-fiscal-years rebuilds every year and would discard "
            "the year named by --silver-fiscal-year; pass only one"
        )

    dbt_project_dir = Path(args.dbt_project_dir)
    if not dbt_project_dir.exists():
        parser.error(f"dbt project directory does not exist: {dbt_project_dir}")

    dbt_profiles_dir = (
        Path(args.dbt_profiles_dir) if args.dbt_profiles_dir else dbt_project_dir
    )
    if not dbt_profiles_dir.exists():
        parser.error(f"dbt profiles directory does not exist: {dbt_profiles_dir}")

    results = run_jepx_orchestrated_pipeline(
        bucket_name=args.bucket,
        timestamp_ms=args.timestamp_ms,
        catalog_name=args.catalog,
        bronze_table_identifier=args.bronze_table,
        bronze_schema_path=args.bronze_schema_path,
        allow_duplicate_source=args.allow_duplicate_source,
        dbt_project_dir=dbt_project_dir,
        dbt_profiles_dir=dbt_profiles_dir,
        bronze_location=args.bronze_location,
        silver_schema_dir=args.silver_schema_dir,
        silver_fiscal_year=args.silver_fiscal_year,
        silver_all_fiscal_years=args.silver_all_fiscal_years,
        run_gold_step=args.run_gold_step,
        gold_select=args.gold_select,
        dbt_full_refresh=args.dbt_full_refresh,
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

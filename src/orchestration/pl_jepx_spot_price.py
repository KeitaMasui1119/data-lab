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
import time
from datetime import datetime
from pathlib import Path

from common.storage_client import RustFSClient
from common.utilities import resolve_target_at
from orchestration.pipeline_result import (
    STATUS_FAILED,
    STATUS_SKIPPED,
    STATUS_SUCCESS,
    PipelineStepResult,
    has_failed_step,
    verify_silver_row_counts,
)
from pipeline.bronze.source_to_bronze_jepx_spot_price import ingest_jepx_spot_summary
from pipeline.jepx_common import resolve_fiscal_year_start
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
    "/workspace/configuration/iceberg/schema/bronze/jepx_spot_price/jepx_spot_price.csv"
)

# The silver table that receives one row per validated delivery key.
BASE_TABLE_IDENTIFIER = "silver.jepx_spot_price_base"

# Pause between a backfill's per-year requests. Nothing else paces them: the
# scraper holds no delay or retry of its own, and a full replay is one request
# per fiscal year back to back.
DEFAULT_REQUEST_DELAY_SECONDS = 3.0


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
        status=STATUS_SUCCESS,
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

    # The base table takes exactly the rows that passed validation, one per
    # delivery key; block does too, and area multiplies them by the area
    # count. Checking base alone keeps the expectation independent of how many
    # areas a row unpivots into -- its UNPIVOT drops null area prices, so the
    # area table's own count is not a fixed multiple of the staged rows.
    actual_row_count = result.rows_written_to(BASE_TABLE_IDENTIFIER)
    status, reason = verify_silver_row_counts(
        staged_row_count=result.staged_row_count,
        expected_row_count=result.valid_row_count,
        actual_row_count=actual_row_count,
        target_description=BASE_TABLE_IDENTIFIER,
    )

    written = sum(write.rows_written for write in result.writes)
    scope = "all fiscal years" if fiscal_year is None else f"fiscal_year={fiscal_year}"
    detail = (
        f"execution_id={result.execution_id}, {scope}, written={written}, "
        f"dropped={result.dropped_row_count}, staged={result.staged_row_count}"
    )
    if reason:
        detail = f"{detail}; {reason}"
        logger.error("bronze_to_silver step failed verification: %s", reason)

    return PipelineStepResult(
        name="bronze_to_silver",
        status=status,
        detail=detail,
        expected_row_count=result.valid_row_count,
        actual_row_count=actual_row_count,
    )


def run_source_to_raw_step(
    *,
    storage_client: RustFSClient,
    scraper: JEPXSpotSummaryScraper,
    bucket_name: str,
    target_at: datetime,
) -> tuple[PipelineStepResult, int]:
    """Save one fiscal year's snapshot, returning the step and the year it covers.

    The scraper is passed in rather than created here so a caller looping
    over fiscal years can hold one session open for the whole range.
    """
    snapshot_result = scrape_jepx_spot_price_raw(
        storage_client=storage_client,
        scraper=scraper,
        bucket_name=bucket_name,
        target_at=target_at,
    )

    if snapshot_result.skipped:
        return (
            PipelineStepResult(
                name="source_to_raw",
                status=STATUS_SKIPPED,
                detail=(
                    "No snapshot change detected. "
                    f"year={snapshot_result.year}, sha256={snapshot_result.sha256[:8]}"
                ),
            ),
            snapshot_result.year,
        )
    return (
        PipelineStepResult(
            name="source_to_raw",
            status=STATUS_SUCCESS,
            detail=(
                "Saved snapshot and updated metadata catalog. "
                f"year={snapshot_result.year}, "
                f"prefix={snapshot_result.snapshot_prefix}"
            ),
        ),
        snapshot_result.year,
    )


def run_raw_to_bronze_step(
    *,
    storage_client: RustFSClient,
    bucket_name: str,
    catalog_name: str,
    bronze_table_identifier: str,
    bronze_schema_path: str,
    allow_duplicate_source: bool,
    fiscal_year: int,
    require_unprocessed: bool = True,
) -> PipelineStepResult:
    """Ingest one fiscal year's latest raw snapshot into bronze.

    ``require_unprocessed`` selects only a snapshot the ingestion log has not
    already fed to bronze, which is what a forward-moving run wants. A replay
    reruns snapshots that are already marked processed, so it has to turn the
    filter off or every year resolves to nothing at all.
    """
    try:
        row_count = ingest_jepx_spot_summary(
            client=storage_client,
            bucket_name=bucket_name,
            object_key=None,
            source_file_name=None,
            catalog_name=catalog_name,
            table_identifier=bronze_table_identifier,
            schema_path=bronze_schema_path,
            skip_if_exists=not allow_duplicate_source,
            fiscal_year=fiscal_year,
            use_ingestion_log=True,
            require_unprocessed=require_unprocessed,
            update_ingestion_log_status=True,
        )
    except ValueError as error:
        return PipelineStepResult(
            name="raw_to_bronze",
            status=STATUS_SKIPPED,
            detail=f"fiscal_year={fiscal_year}: {error}",
        )
    return PipelineStepResult(
        name="raw_to_bronze",
        status=STATUS_SUCCESS,
        detail=(
            f"table={bronze_table_identifier}, fiscal_year={fiscal_year}, "
            f"rows={row_count}"
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
        raw_result, snapshot_fiscal_year = run_source_to_raw_step(
            storage_client=rustfs,
            scraper=scraper,
            bucket_name=bucket_name,
            target_at=target_at,
        )
    finally:
        scraper.close()
    results.append(raw_result)

    results.append(
        run_raw_to_bronze_step(
            storage_client=rustfs,
            bucket_name=bucket_name,
            catalog_name=catalog_name,
            bronze_table_identifier=bronze_table_identifier,
            bronze_schema_path=bronze_schema_path,
            allow_duplicate_source=allow_duplicate_source,
            fiscal_year=snapshot_fiscal_year,
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
                snapshot_fiscal_year=snapshot_fiscal_year,
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
                status=STATUS_SKIPPED,
                detail="Gold step is disabled. Use --run-gold-step to enable.",
            )
        )

    return results


def _run_backfill_year(
    *,
    storage_client: RustFSClient,
    scraper: JEPXSpotSummaryScraper | None,
    fiscal_year: int,
    bucket_name: str,
    catalog_name: str,
    bronze_table_identifier: str,
    bronze_schema_path: str,
    allow_duplicate_source: bool,
) -> list[PipelineStepResult]:
    """Take one fiscal year from the source (or from raw) through to bronze."""
    results: list[PipelineStepResult] = []

    if scraper is not None:
        raw_result, _ = run_source_to_raw_step(
            storage_client=storage_client,
            scraper=scraper,
            bucket_name=bucket_name,
            target_at=resolve_fiscal_year_start(fiscal_year),
        )
        results.append(raw_result)

    results.append(
        run_raw_to_bronze_step(
            storage_client=storage_client,
            bucket_name=bucket_name,
            catalog_name=catalog_name,
            bronze_table_identifier=bronze_table_identifier,
            bronze_schema_path=bronze_schema_path,
            allow_duplicate_source=allow_duplicate_source,
            fiscal_year=fiscal_year,
            # A replay reruns snapshots the ingestion log already marks
            # processed; leaving the filter on would resolve nothing for
            # every year and silently do no work at all.
            require_unprocessed=scraper is not None,
        )
    )
    return results


def run_jepx_backfill_pipeline(
    *,
    bucket_name: str,
    from_fiscal_year: int,
    to_fiscal_year: int,
    catalog_name: str,
    bronze_table_identifier: str,
    bronze_schema_path: str,
    allow_duplicate_source: bool,
    bronze_location: str,
    silver_schema_dir: str,
    from_raw: bool = False,
    request_delay_seconds: float = DEFAULT_REQUEST_DELAY_SECONDS,
) -> list[PipelineStepResult]:
    """Rebuild a range of fiscal years, one raw+bronze pass per year.

    This is the executable form of the rebuild procedure in
    docs/architecture/replay_strategy.md, which the FY2005-FY2026 backfill ran
    as a throwaway shell loop instead.

    Silver is rebuilt once at the end rather than per year. A scoped silver run
    re-scans the whole bronze table -- the fiscal year filter applies to a cast
    column, so it cannot be pushed into the Iceberg scan -- and one unscoped
    pass covers every year for the cost of a single scan.

    ``from_raw`` replays from the raw snapshots already in storage instead of
    fetching them again, which is what replay_strategy.md means by raw being
    the source of truth. Bronze ingestion still skips a snapshot whose
    ``source_data`` is present, so replaying into an intact bronze table is a
    per-year no-op that leaves only the silver rebuild to do; clearing the
    affected bronze rows first is what makes it re-ingest them.

    A year that fails does not stop the range: its failure is recorded and the
    remaining years still run, because a replay that names the years needing
    attention is worth more than one that stops at the first of them.
    """
    if to_fiscal_year < from_fiscal_year:
        raise ValueError(
            f"to_fiscal_year ({to_fiscal_year}) is before from_fiscal_year "
            f"({from_fiscal_year}); the range would cover no years"
        )

    results: list[PipelineStepResult] = []
    failed_fiscal_years: list[int] = []
    fiscal_years = list(range(from_fiscal_year, to_fiscal_year + 1))

    rustfs = RustFSClient()
    scraper = None if from_raw else JEPXSpotSummaryScraper()
    try:
        for index, fiscal_year in enumerate(fiscal_years):
            try:
                results.extend(
                    _run_backfill_year(
                        storage_client=rustfs,
                        scraper=scraper,
                        fiscal_year=fiscal_year,
                        bucket_name=bucket_name,
                        catalog_name=catalog_name,
                        bronze_table_identifier=bronze_table_identifier,
                        bronze_schema_path=bronze_schema_path,
                        allow_duplicate_source=allow_duplicate_source,
                    )
                )
            except Exception as error:
                logger.exception("Backfill failed for fiscal_year=%s", fiscal_year)
                failed_fiscal_years.append(fiscal_year)
                results.append(
                    PipelineStepResult(
                        name="backfill_year",
                        status=STATUS_FAILED,
                        detail=f"fiscal_year={fiscal_year}: {error}",
                    )
                )

            is_last_year = index == len(fiscal_years) - 1
            if scraper is not None and not is_last_year:
                # Nothing else paces these requests: the scraper has no delay
                # or retry of its own, and the range is one request per year.
                time.sleep(request_delay_seconds)
    finally:
        if scraper is not None:
            scraper.close()

    results.append(
        run_bronze_to_silver_step(
            catalog_name=catalog_name,
            bronze_location=bronze_location,
            silver_schema_dir=silver_schema_dir,
            fiscal_year=None,
        )
    )

    logger.info(
        "JEPX backfill covered FY%s-FY%s (%s years, %s failed)",
        from_fiscal_year,
        to_fiscal_year,
        len(fiscal_years),
        len(failed_fiscal_years),
    )
    if failed_fiscal_years:
        logger.error("Fiscal years needing attention: %s", failed_fiscal_years)

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

    # A failed step that only reaches the log still exits 0, which is how both
    # backfill incidents passed for clean runs.
    if has_failed_step(results):
        raise SystemExit(1)


if __name__ == "__main__":
    main()

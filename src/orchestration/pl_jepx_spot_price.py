"""JEPX pipeline orchestrator for end-to-end workflow execution.

This module provides an ADF-like orchestration layer that runs JEPX
processing steps in dependency order:
1. Source -> Raw
2. Raw -> Bronze
3. Bronze -> Silver (DuckDB transform + PyIceberg window replace)
4. Silver -> Gold (optional dbt gold)
"""

from __future__ import annotations

import logging
import subprocess
import time
from datetime import datetime
from pathlib import Path

from common.storage_client import RustFSClient
from common.utilities import gen_uuid, get_now_utc, resolve_target_at
from orchestration.pipeline_result import (
    STATUS_FAILED,
    STATUS_SKIPPED,
    STATUS_SUCCESS,
    PipelineStepResult,
    stamp_step_timing,
    verify_silver_row_counts,
)
from orchestration.pipeline_run_log import record_pipeline_run
from pipeline.bronze.source_to_bronze_jepx_spot_price import (
    run_source_to_bronze_jepx_spot_price,
)
from pipeline.jepx_common import resolve_fiscal_year_start
from pipeline.raw.source_to_raw_jepx_spot_price import (
    JEPXSpotSummaryScraper,
    run_source_to_raw_jepx_spot_price,
)
from pipeline.silver.bronze_to_silver_jepx_spot_price import (
    run_bronze_to_silver_jepx_spot_price,
)

logger = logging.getLogger(__name__)

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
    # S603 flags every subprocess call for review rather than detecting a fault.
    # This one passes a list with a literal executable and no shell=True, so the
    # arguments reach dbt as argv entries with no shell parsing in between --
    # select_expr cannot break out of its slot however it is spelled.
    subprocess.run(dbt_command, check=True)  # noqa: S603
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


def _describe_silver_scope(fiscal_year: int | None) -> str:
    """Name the fiscal years a silver run covers, for logs and the run log."""
    if fiscal_year is None:
        return "all fiscal years"
    return f"fiscal_year={fiscal_year}"


def run_bronze_to_silver_step(
    *,
    catalog_name: str,
    bronze_location: str,
    silver_schema_dir: str,
    fiscal_year: int | None,
    execution_id: str | None = None,
) -> PipelineStepResult:
    """Transform bronze rows into the silver Iceberg tables.

    ``execution_id`` is the orchestrator's run id. Passing it down stamps the
    silver rows with the same value the run log records, so a run in
    metadata.pipeline_run_log can be joined to the rows it wrote.
    """
    result = run_bronze_to_silver_jepx_spot_price(
        catalog_name=catalog_name,
        bronze_location=bronze_location,
        schema_dir=silver_schema_dir,
        fiscal_year=fiscal_year,
        execution_id=execution_id,
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
    scope = _describe_silver_scope(fiscal_year)
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
    snapshot_result = run_source_to_raw_jepx_spot_price(
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
        row_count = run_source_to_bronze_jepx_spot_price(
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
    run_id = gen_uuid()

    rustfs = RustFSClient()

    started_at = get_now_utc()
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
    results.append(stamp_step_timing(raw_result, started_at=started_at))

    started_at = get_now_utc()
    results.append(
        stamp_step_timing(
            run_raw_to_bronze_step(
                storage_client=rustfs,
                bucket_name=bucket_name,
                catalog_name=catalog_name,
                bronze_table_identifier=bronze_table_identifier,
                bronze_schema_path=bronze_schema_path,
                allow_duplicate_source=allow_duplicate_source,
                fiscal_year=snapshot_fiscal_year,
            ),
            started_at=started_at,
        )
    )

    silver_fiscal_year = resolve_silver_fiscal_year(
        silver_fiscal_year=silver_fiscal_year,
        silver_all_fiscal_years=silver_all_fiscal_years,
        snapshot_fiscal_year=snapshot_fiscal_year,
    )
    started_at = get_now_utc()
    results.append(
        stamp_step_timing(
            run_bronze_to_silver_step(
                catalog_name=catalog_name,
                bronze_location=bronze_location,
                silver_schema_dir=silver_schema_dir,
                fiscal_year=silver_fiscal_year,
                execution_id=run_id,
            ),
            started_at=started_at,
        )
    )

    started_at = get_now_utc()
    if run_gold_step:
        gold_result = run_dbt_step(
            step_name="silver_to_gold",
            select_expr=gold_select,
            project_dir=dbt_project_dir,
            profiles_dir=dbt_profiles_dir,
            full_refresh=dbt_full_refresh,
        )
    else:
        gold_result = PipelineStepResult(
            name="silver_to_gold",
            status=STATUS_SKIPPED,
            detail="Gold step is disabled. Use --run-gold-step to enable.",
        )
    results.append(stamp_step_timing(gold_result, started_at=started_at))

    record_pipeline_run(
        run_id=run_id,
        pipeline_name="jepx_spot_price",
        target_scope=_describe_silver_scope(silver_fiscal_year),
        results=results,
        catalog_name=catalog_name,
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
    run_id = gen_uuid()

    rustfs = RustFSClient()
    scraper = None if from_raw else JEPXSpotSummaryScraper()
    try:
        for index, fiscal_year in enumerate(fiscal_years):
            started_at = get_now_utc()
            try:
                year_results = _run_backfill_year(
                    storage_client=rustfs,
                    scraper=scraper,
                    fiscal_year=fiscal_year,
                    bucket_name=bucket_name,
                    catalog_name=catalog_name,
                    bronze_table_identifier=bronze_table_identifier,
                    bronze_schema_path=bronze_schema_path,
                    allow_duplicate_source=allow_duplicate_source,
                )
            except Exception as error:
                logger.exception("Backfill failed for fiscal_year=%s", fiscal_year)
                failed_fiscal_years.append(fiscal_year)
                year_results = [
                    PipelineStepResult(
                        name="backfill_year",
                        status=STATUS_FAILED,
                        detail=f"fiscal_year={fiscal_year}: {error}",
                    )
                ]

            # One window covers the year's whole raw+bronze pass. Timing each
            # of its steps separately would need the timing inside
            # _run_backfill_year, and the year is the unit a replay is judged
            # by anyway.
            ended_at = get_now_utc()
            results.extend(
                stamp_step_timing(result, started_at=started_at, ended_at=ended_at)
                for result in year_results
            )

            is_last_year = index == len(fiscal_years) - 1
            if scraper is not None and not is_last_year:
                # Nothing else paces these requests: the scraper has no delay
                # or retry of its own, and the range is one request per year.
                time.sleep(request_delay_seconds)
    finally:
        if scraper is not None:
            scraper.close()

    started_at = get_now_utc()
    results.append(
        stamp_step_timing(
            run_bronze_to_silver_step(
                catalog_name=catalog_name,
                bronze_location=bronze_location,
                silver_schema_dir=silver_schema_dir,
                fiscal_year=None,
                execution_id=run_id,
            ),
            started_at=started_at,
        )
    )

    record_pipeline_run(
        run_id=run_id,
        pipeline_name="jepx_spot_price_backfill",
        target_scope=f"fiscal_year={from_fiscal_year}..{to_fiscal_year}",
        results=results,
        catalog_name=catalog_name,
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

"""OCCTO pipeline orchestrator for end-to-end workflow execution.

This module provides an ADF-like orchestration layer that runs OCCTO
processing steps in dependency order:
1. Source -> Raw
2. Raw -> Bronze
3. Bronze -> Silver (DuckDB transform + PyIceberg window replace)

Unlike JEPX, there is no dbt gold step here: OCCTO's dbt models were
removed once silver became pure Python/DuckDB/PyIceberg (see
docs/tasks/plan_occto_pipeline.md Phase 5).
"""

from __future__ import annotations

import argparse
import logging
from datetime import UTC, date, datetime

from common.storage_client import RustFSClient
from orchestration.pipeline_result import PipelineStepResult
from pipeline.bronze.source_to_bronze_occto_unit_generation_actuals import (
    ingest_occto_unit_generation,
)
from pipeline.raw.source_to_raw_occto_unit_generation_actuals import (
    OCCTOUnitGenerationScraper,
    resolve_default_target_date,
    scrape_occto_unit_generation_raw,
)
from pipeline.silver.bronze_to_silver_occto_unit_generation_actuals import (
    DEFAULT_BRONZE_LOCATION,
    DEFAULT_SILVER_SCHEMA_DIR,
    run_bronze_to_silver_occto_unit_generation,
)

logger = logging.getLogger(__name__)

DEFAULT_SCHEMA_PATH = (
    "/workspace/configuration/iceberg/schema/bronze/occto_unit_generation_actuals.csv"
)


def resolve_silver_target_date_window(
    *,
    silver_from_date: date | None,
    silver_to_date: date | None,
    silver_all_dates: bool,
    snapshot_from_date: date,
    snapshot_to_date: date,
) -> tuple[date | None, date | None]:
    """Decide which target_date range the silver step rebuilds.

    A run defaults to the range it just ingested. Rebuilding every date on
    every run scales with how much history the table holds rather than
    with the new data (the same problem JEPX's fiscal-year default
    avoids), so a full refresh has to be asked for explicitly.
    """
    if silver_all_dates:
        return None, None
    if silver_from_date is not None:
        return silver_from_date, silver_to_date or silver_from_date
    return snapshot_from_date, snapshot_to_date


def run_bronze_to_silver_step(
    *,
    catalog_name: str,
    bronze_location: str,
    silver_schema_dir: str,
    from_date: date | None,
    to_date: date | None,
) -> PipelineStepResult:
    """Transform bronze rows into the silver Iceberg table."""
    result = run_bronze_to_silver_occto_unit_generation(
        catalog_name=catalog_name,
        bronze_location=bronze_location,
        schema_dir=silver_schema_dir,
        from_date=from_date,
        to_date=to_date,
    )

    scope = "all dates" if from_date is None else f"target_date={from_date}..{to_date}"
    return PipelineStepResult(
        name="bronze_to_silver",
        status="success",
        detail=(
            f"execution_id={result.execution_id}, {scope}, "
            f"written={result.write.rows_written}, "
            f"dropped={result.dropped_row_count}"
        ),
    )


def run_occto_orchestrated_pipeline(
    *,
    bucket_name: str,
    from_date: date | None,
    to_date: date | None,
    catalog_name: str,
    bronze_table_identifier: str,
    bronze_schema_path: str,
    allow_duplicate_source: bool,
    bronze_location: str,
    silver_schema_dir: str,
    silver_from_date: date | None,
    silver_to_date: date | None,
    silver_all_dates: bool = False,
) -> list[PipelineStepResult]:
    """Run the OCCTO workflow in dependency order."""
    results: list[PipelineStepResult] = []

    resolved_from_date = from_date or resolve_default_target_date(datetime.now(UTC))
    resolved_to_date = to_date or resolved_from_date

    rustfs = RustFSClient()

    scraper = OCCTOUnitGenerationScraper()
    try:
        snapshot_result = scrape_occto_unit_generation_raw(
            storage_client=rustfs,
            scraper=scraper,
            bucket_name=bucket_name,
            from_date=resolved_from_date,
            to_date=resolved_to_date,
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
                    f"from_date={snapshot_result.from_date}, "
                    f"to_date={snapshot_result.to_date}, "
                    f"sha256={snapshot_result.sha256[:8]}"
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
                    f"from_date={snapshot_result.from_date}, "
                    f"to_date={snapshot_result.to_date}, "
                    f"prefix={snapshot_result.snapshot_prefix}"
                ),
            )
        )

    try:
        row_count = ingest_occto_unit_generation(
            client=rustfs,
            bucket_name=bucket_name,
            object_key=None,
            source_file_name=None,
            catalog_name=catalog_name,
            table_identifier=bronze_table_identifier,
            schema_path=bronze_schema_path,
            skip_if_exists=not allow_duplicate_source,
            target_date=snapshot_result.from_date,
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

    silver_window = resolve_silver_target_date_window(
        silver_from_date=silver_from_date,
        silver_to_date=silver_to_date,
        silver_all_dates=silver_all_dates,
        snapshot_from_date=snapshot_result.from_date,
        snapshot_to_date=snapshot_result.to_date,
    )
    results.append(
        run_bronze_to_silver_step(
            catalog_name=catalog_name,
            bronze_location=bronze_location,
            silver_schema_dir=silver_schema_dir,
            from_date=silver_window[0],
            to_date=silver_window[1],
        )
    )

    return results


def build_parser() -> argparse.ArgumentParser:
    """Build argument parser for the OCCTO orchestrator CLI."""
    parser = argparse.ArgumentParser(
        description="Run ADF-like orchestration for the OCCTO pipeline"
    )
    parser.add_argument(
        "--bucket",
        default="jp-power-grid-dev",
        help="Source/target bucket name (default: jp-power-grid-dev)",
    )
    parser.add_argument(
        "--target-date",
        help="Target date in YYYY-MM-DD (default: previous day in Asia/Tokyo)",
    )
    parser.add_argument(
        "--to-date",
        help=(
            "End of target date range in YYYY-MM-DD for a multi-day fetch "
            "(default: same as --target-date, i.e. a single day)"
        ),
    )
    parser.add_argument(
        "--catalog",
        default="dlh_dev",
        help="Iceberg catalog name (default: dlh_dev)",
    )
    parser.add_argument(
        "--bronze-table",
        default="bronze.occto_unit_generation_actuals",
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
        "--silver-from-date",
        help=(
            "Start of the silver step's target_date range in YYYY-MM-DD "
            "(default: the range that was just ingested)"
        ),
    )
    parser.add_argument(
        "--silver-to-date",
        help="End of the silver step's target_date range in YYYY-MM-DD"
        " (default: same as --silver-from-date)",
    )
    parser.add_argument(
        "--silver-all-dates",
        action="store_true",
        help="Rebuild every target_date in the silver step"
        " instead of just the range ingested",
    )
    return parser


def main() -> None:
    """CLI entrypoint for OCCTO pipeline orchestration."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )

    parser = build_parser()
    args = parser.parse_args()

    if args.silver_all_dates and (args.silver_from_date or args.silver_to_date):
        parser.error(
            "--silver-all-dates rebuilds every date and would discard the "
            "range named by --silver-from-date/--silver-to-date; pass only one"
        )

    from_date = date.fromisoformat(args.target_date) if args.target_date else None
    to_date = date.fromisoformat(args.to_date) if args.to_date else None
    silver_from_date = (
        date.fromisoformat(args.silver_from_date) if args.silver_from_date else None
    )
    silver_to_date = (
        date.fromisoformat(args.silver_to_date) if args.silver_to_date else None
    )

    results = run_occto_orchestrated_pipeline(
        bucket_name=args.bucket,
        from_date=from_date,
        to_date=to_date,
        catalog_name=args.catalog,
        bronze_table_identifier=args.bronze_table,
        bronze_schema_path=args.bronze_schema_path,
        allow_duplicate_source=args.allow_duplicate_source,
        bronze_location=args.bronze_location,
        silver_schema_dir=args.silver_schema_dir,
        silver_from_date=silver_from_date,
        silver_to_date=silver_to_date,
        silver_all_dates=args.silver_all_dates,
    )

    logger.info("OCCTO orchestrated pipeline summary:")
    for result in results:
        logger.info(
            " - step=%s, status=%s, detail=%s",
            result.name,
            result.status,
            result.detail,
        )


if __name__ == "__main__":
    main()

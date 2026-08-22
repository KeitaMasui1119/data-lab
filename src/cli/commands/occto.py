"""OCCTO unit generation actuals CLI commands: scrape, raw-to-bronze,
bronze-to-silver, and the end-to-end orchestrator.
"""

from __future__ import annotations

import argparse
import logging
from datetime import UTC, date, datetime

from cli.args import (
    add_catalog_arg,
    add_date_range_args,
    add_target_date_and_to_date_args,
)
from cli.dates import parse_iso_date, parse_optional_date_range
from cli.defaults import DEFAULT_BUCKET, bronze_schema_path
from cli.registry import CommandSpec
from cli.scraping import run_daily_scrape_loop
from common.storage_client import RustFSClient
from common.utils import resolve_default_target_date
from orchestration.pipeline_result import has_failed_step
from orchestration.pl_occto_unit_generation_actuals import (
    run_occto_orchestrated_pipeline,
)
from pipeline.bronze.source_to_bronze_occto_unit_generation_actuals import (
    run_source_to_bronze_occto_unit_generation_actuals,
)
from pipeline.raw.source_to_raw_occto_unit_generation_actuals import (
    OCCTOUnitGenerationScraper,
    run_source_to_raw_occto_unit_generation_actuals,
)
from pipeline.silver.bronze_to_silver_occto_unit_generation_actuals import (
    DEFAULT_BRONZE_LOCATION,
    DEFAULT_SILVER_SCHEMA_DIR,
    run_bronze_to_silver_occto_unit_generation_actuals,
)

logger = logging.getLogger(__name__)

DEFAULT_BRONZE_SCHEMA_PATH = bronze_schema_path(
    "occto_unit_generation_actuals", "occto_unit_generation_actuals.csv"
)


def _configure_scrape(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--bucket",
        default=DEFAULT_BUCKET,
        help=f"Target bucket name (default: {DEFAULT_BUCKET})",
    )
    add_target_date_and_to_date_args(parser)


def _handle_scrape(args: argparse.Namespace, parser: argparse.ArgumentParser) -> None:
    from_date = (
        parse_iso_date(parser, args.target_date, "--target-date")
        if args.target_date
        else resolve_default_target_date(datetime.now(UTC))
    )
    to_date = (
        parse_iso_date(parser, args.to_date, "--to-date") if args.to_date else None
    )

    rustfs = RustFSClient()
    scraper = OCCTOUnitGenerationScraper()
    run_daily_scrape_loop(
        from_date=from_date,
        to_date=to_date,
        scrape_one_day=lambda current: run_source_to_raw_occto_unit_generation_actuals(
            storage_client=rustfs,
            scraper=scraper,
            bucket_name=args.bucket,
            from_date=current,
            to_date=current,
        ),
        date_of=lambda result: result.from_date,
        close=scraper.close,
        label="OCCTO",
    )


def _configure_bronze(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--bucket",
        default=DEFAULT_BUCKET,
        help=f"Source bucket name (default: {DEFAULT_BUCKET})",
    )
    parser.add_argument(
        "--object-key",
        help="Source object key in raw layer"
        " (e.g. raw/occto/unit_generation/target_date=.../ingested_at=.../<file>.csv)."
        " Required unless --use-ingestion-log is set",
    )
    parser.add_argument(
        "--source-file-name",
        help="Source file name stored in source_data (default: object-key in full)",
    )
    parser.add_argument(
        "--target-date",
        help="Target date in YYYY-MM-DD. Required when --use-ingestion-log is"
        " set and --object-key is omitted",
    )
    add_catalog_arg(parser)
    parser.add_argument(
        "--table",
        default="bronze.occto_unit_generation_actuals",
        help="Target Iceberg table identifier"
        " (default: bronze.occto_unit_generation_actuals)",
    )
    parser.add_argument(
        "--schema-path",
        default=DEFAULT_BRONZE_SCHEMA_PATH,
        help="Schema CSV path",
    )
    parser.add_argument(
        "--allow-duplicate-source",
        action="store_true",
        help="Allow append even if source_data already exists",
    )
    parser.add_argument(
        "--use-ingestion-log",
        action="store_true",
        help="Resolve latest raw snapshot from metadata ingestion log",
    )
    parser.add_argument(
        "--require-unprocessed",
        action="store_true",
        help="When using ingestion log, select only unprocessed latest snapshot",
    )


def _handle_bronze(args: argparse.Namespace, parser: argparse.ArgumentParser) -> None:
    target_date = None
    if args.target_date:
        target_date = parse_iso_date(parser, args.target_date, "--target-date")
    elif args.use_ingestion_log and args.object_key is None:
        parser.error(
            "--target-date is required when --use-ingestion-log is set "
            "without --object-key"
        )

    rustfs = RustFSClient()
    row_count = run_source_to_bronze_occto_unit_generation_actuals(
        client=rustfs,
        bucket_name=args.bucket,
        object_key=args.object_key,
        source_file_name=args.source_file_name,
        catalog_name=args.catalog,
        table_identifier=args.table,
        schema_path=args.schema_path,
        skip_if_exists=not args.allow_duplicate_source,
        target_date=target_date,
        use_ingestion_log=args.use_ingestion_log,
        require_unprocessed=args.require_unprocessed,
        update_ingestion_log_status=args.use_ingestion_log,
    )
    logger.info(
        "Ingestion completed: table=%s, rows=%s",
        args.table,
        row_count,
    )


def _configure_silver(parser: argparse.ArgumentParser) -> None:
    add_catalog_arg(parser)
    parser.add_argument(
        "--bronze-location",
        default=DEFAULT_BRONZE_LOCATION,
        help="Bronze table location scanned by DuckDB",
    )
    parser.add_argument(
        "--schema-dir",
        default=DEFAULT_SILVER_SCHEMA_DIR,
        help="Directory containing the silver schema CSV files",
    )
    add_date_range_args(parser)


def _handle_silver(args: argparse.Namespace, parser: argparse.ArgumentParser) -> None:
    from_date, to_date = parse_optional_date_range(parser, args)
    result = run_bronze_to_silver_occto_unit_generation_actuals(
        catalog_name=args.catalog,
        bronze_location=args.bronze_location,
        schema_dir=args.schema_dir,
        from_date=from_date,
        to_date=to_date,
    )
    logger.info(
        "OCCTO bronze-to-silver completed: execution_id=%s, staged=%s, valid=%s, "
        "dropped=%s, daily_amount_mismatch=%s",
        result.execution_id,
        result.staged_row_count,
        result.valid_row_count,
        result.dropped_row_count,
        result.daily_amount_mismatch,
    )
    logger.info(
        " - table=%s, written=%s",
        result.write.table_identifier,
        result.write.rows_written,
    )


def _configure_orchestrator(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--bucket",
        default=DEFAULT_BUCKET,
        help=f"Source/target bucket name (default: {DEFAULT_BUCKET})",
    )
    add_target_date_and_to_date_args(parser)
    add_catalog_arg(parser)
    parser.add_argument(
        "--bronze-table",
        default="bronze.occto_unit_generation_actuals",
        help="Target bronze Iceberg table identifier",
    )
    parser.add_argument(
        "--bronze-schema-path",
        default=DEFAULT_BRONZE_SCHEMA_PATH,
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


def _handle_orchestrator(
    args: argparse.Namespace, parser: argparse.ArgumentParser
) -> None:
    if args.silver_all_dates and (args.silver_from_date or args.silver_to_date):
        parser.error(
            "--silver-all-dates rebuilds every date and would discard the "
            "range named by --silver-from-date/--silver-to-date; pass only one"
        )

    # NOTE: unlike every other date-parsing command in this codebase, these
    # were never wrapped in a parser.error()-style guard in the original
    # implementation -- an invalid date here raises an uncaught ValueError
    # instead of a clean usage message. Preserved as pre-existing behavior;
    # see docs/tasks/refactaring_20260817.md section 3 (Phase 2/3 note).
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
    for result in results:
        logger.info(
            "Orchestrator step result: step=%s, status=%s, detail=%s",
            result.name,
            result.status,
            result.detail,
        )

    # A failed step that only reaches the log still exits 0, which is how both
    # JEPX backfill incidents passed for clean runs.
    if has_failed_step(results):
        raise SystemExit(1)


COMMANDS = [
    CommandSpec(
        name="scrape-occto",
        help="Download OCCTO unit generation CSV and upload to RustFS raw layer",
        configure=_configure_scrape,
        handler=_handle_scrape,
    ),
    CommandSpec(
        name="ingest-occto-raw-to-bronze",
        help="Ingest OCCTO unit generation raw CSV into bronze Iceberg table",
        configure=_configure_bronze,
        handler=_handle_bronze,
    ),
    CommandSpec(
        name="ingest-occto-bronze-to-silver",
        help="Transform OCCTO bronze unit generation actuals into silver",
        configure=_configure_silver,
        handler=_handle_silver,
    ),
    CommandSpec(
        name="run-occto-orchestrator",
        help="Run ADF-like OCCTO end-to-end orchestrator",
        configure=_configure_orchestrator,
        handler=_handle_orchestrator,
    ),
]

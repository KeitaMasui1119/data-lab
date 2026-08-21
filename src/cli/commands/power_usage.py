"""Hokuriku power_usage CLI commands: scrape, raw-to-bronze, bronze-to-silver."""

from __future__ import annotations

import argparse
import logging
from datetime import UTC, datetime

from cli.args import (
    add_catalog_arg,
    add_date_range_args,
    add_target_date_and_to_date_args,
)
from cli.dates import parse_iso_date, parse_optional_date_range
from cli.defaults import DEFAULT_BUCKET
from cli.registry import CommandSpec
from cli.scraping import run_daily_scrape_loop
from common.storage_client import RustFSClient
from common.utilities import resolve_default_target_date
from pipeline.bronze.source_to_bronze_power_usage_hokuriku import (
    run_source_to_bronze_power_usage_hokuriku,
)
from pipeline.raw.source_to_raw_power_usage_hokuriku import (
    HokurikuPowerUsageScraper,
    run_source_to_raw_power_usage_hokuriku,
)
from pipeline.silver.bronze_to_silver_power_usage_hokuriku import (
    DEFAULT_SILVER_SCHEMA_DIR,
    run_bronze_to_silver_power_usage_hokuriku,
)

logger = logging.getLogger(__name__)


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
    scraper = HokurikuPowerUsageScraper()
    run_daily_scrape_loop(
        from_date=from_date,
        to_date=to_date,
        scrape_one_day=lambda current: run_source_to_raw_power_usage_hokuriku(
            storage_client=rustfs,
            scraper=scraper,
            bucket_name=args.bucket,
            target_date=current,
        ),
        date_of=lambda result: result.target_date,
        close=scraper.close,
        label="Hokuriku power_usage",
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
        " (e.g. raw/power_usage/hokuriku/target_date=.../ingested_at=.../<file>.csv)."
        " Required unless --use-ingestion-log is set",
    )
    parser.add_argument(
        "--source-file-name",
        help="Source file name stored in source_data (default: object-key in full)",
    )
    parser.add_argument(
        "--target-date",
        required=True,
        help="Target date in YYYY-MM-DD (the day this snapshot's data covers)",
    )
    add_catalog_arg(parser)
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
    target_date = parse_iso_date(parser, args.target_date, "--target-date")

    rustfs = RustFSClient()
    row_counts = run_source_to_bronze_power_usage_hokuriku(
        client=rustfs,
        bucket_name=args.bucket,
        object_key=args.object_key,
        source_file_name=args.source_file_name,
        catalog_name=args.catalog,
        skip_if_exists=not args.allow_duplicate_source,
        target_date=target_date,
        use_ingestion_log=args.use_ingestion_log,
        require_unprocessed=args.require_unprocessed,
        update_ingestion_log_status=args.use_ingestion_log,
    )
    logger.info("Ingestion completed: rows=%s", row_counts)


def _configure_silver(parser: argparse.ArgumentParser) -> None:
    add_catalog_arg(parser)
    parser.add_argument(
        "--schema-dir",
        default=DEFAULT_SILVER_SCHEMA_DIR,
        help="Directory containing the silver schema CSV files",
    )
    add_date_range_args(parser)


def _handle_silver(args: argparse.Namespace, parser: argparse.ArgumentParser) -> None:
    from_date, to_date = parse_optional_date_range(parser, args)
    result = run_bronze_to_silver_power_usage_hokuriku(
        catalog_name=args.catalog,
        schema_dir=args.schema_dir,
        from_date=from_date,
        to_date=to_date,
    )
    logger.info(
        "Hokuriku power_usage bronze-to-silver completed: execution_id=%s",
        result.execution_id,
    )
    for silver_write in result.writes.values():
        logger.info(
            " - table=%s, written=%s",
            silver_write.table_identifier,
            silver_write.rows_written,
        )


COMMANDS = [
    CommandSpec(
        name="scrape-power-usage-hokuriku",
        help="Scrape Hokuriku power_usage snapshot CSV and upload to RustFS raw layer",
        configure=_configure_scrape,
        handler=_handle_scrape,
    ),
    CommandSpec(
        name="ingest-power-usage-hokuriku-raw-to-bronze",
        help=(
            "Ingest Hokuriku power_usage raw snapshot CSV into its 3 bronze"
            " Iceberg tables (daily_summary, hourly, interval5)"
        ),
        configure=_configure_bronze,
        handler=_handle_bronze,
    ),
    CommandSpec(
        name="ingest-power-usage-hokuriku-bronze-to-silver",
        help="Transform Hokuriku power_usage bronze tables into silver",
        configure=_configure_silver,
        handler=_handle_silver,
    ),
]

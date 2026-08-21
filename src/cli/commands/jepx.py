"""JEPX spot price CLI commands: scrape, raw-to-bronze, raw-pipeline,
end-to-end orchestrator, and bronze-to-silver.
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

from cli.args import add_catalog_arg
from cli.defaults import DEFAULT_BUCKET, DEFAULT_DBT_PROJECT_DIR, bronze_schema_path
from cli.registry import CommandSpec
from common.storage_client import RustFSClient
from common.utilities import resolve_target_at
from orchestration.pipeline_result import has_failed_step
from orchestration.pl_jepx_spot_price import (
    DEFAULT_REQUEST_DELAY_SECONDS,
    run_jepx_backfill_pipeline,
    run_jepx_orchestrated_pipeline,
)
from pipeline.bronze.source_to_bronze_jepx_spot_price import (
    resolve_default_raw_object,
    run_source_to_bronze_jepx_spot_price,
)
from pipeline.gold.silver_to_gold_jepx_spot_price import (
    DEFAULT_AREA_LOCATION,
    DEFAULT_BASE_LOCATION,
    DEFAULT_GOLD_SCHEMA_DIR,
    run_silver_to_gold_jepx_spot_price,
)
from pipeline.jepx_common import resolve_fiscal_year, resolve_fiscal_year_start
from pipeline.raw.source_to_raw_jepx_spot_price import (
    JEPXSpotSummaryScraper,
    run_source_to_raw_jepx_spot_price,
    scrape_jepx_to_rustfs,
)
from pipeline.silver.bronze_to_silver_jepx_spot_price import (
    DEFAULT_BRONZE_LOCATION,
    DEFAULT_SILVER_SCHEMA_DIR,
    run_bronze_to_silver_jepx_spot_price,
)

logger = logging.getLogger(__name__)

DEFAULT_BRONZE_SCHEMA_PATH = bronze_schema_path(
    "jepx_spot_price", "jepx_spot_price.csv"
)


def _configure_scrape(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--bucket",
        default=DEFAULT_BUCKET,
        help=f"Target bucket name (default: {DEFAULT_BUCKET})",
    )
    parser.add_argument(
        "--timestamp-ms",
        type=int,
        help="Optional UNIX timestamp in milliseconds for the JEPX request",
    )
    parser.add_argument(
        "--fiscal-year",
        type=int,
        help="Fiscal year to scrape (e.g. 2024). Overrides --timestamp-ms."
        " Use for backfilling past years.",
    )


def _handle_scrape(args: argparse.Namespace, parser: argparse.ArgumentParser) -> None:
    rustfs = RustFSClient()
    scraper = JEPXSpotSummaryScraper()
    if args.fiscal_year is not None:
        target_at = resolve_fiscal_year_start(args.fiscal_year)
    else:
        target_at = resolve_target_at(args.timestamp_ms)

    try:
        result = run_source_to_raw_jepx_spot_price(
            storage_client=rustfs,
            scraper=scraper,
            bucket_name=args.bucket,
            target_at=target_at,
        )
        if result.skipped:
            logger.info(
                "JEPX scrape skipped (no change): year=%s, sha256=%.8s",
                result.year,
                result.sha256,
            )
        else:
            logger.info(
                "JEPX snapshot saved: year=%s, sha256=%.8s, prefix=%s",
                result.year,
                result.sha256,
                result.snapshot_prefix,
            )
    finally:
        scraper.close()


def _configure_bronze(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--bucket",
        default=DEFAULT_BUCKET,
        help=f"Source bucket name (default: {DEFAULT_BUCKET})",
    )
    parser.add_argument(
        "--object-key",
        help="Source object key in raw layer (default resolved from timestamp)",
    )
    parser.add_argument(
        "--source-file-name",
        help="Source file name stored in source_data (default from object key)",
    )
    parser.add_argument(
        "--timestamp-ms",
        type=int,
        help="Optional UNIX timestamp (ms) to resolve default source file",
    )
    add_catalog_arg(parser)
    parser.add_argument(
        "--table",
        default="bronze.jepx_spot_price",
        help="Target Iceberg table identifier",
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
    target_at = resolve_target_at(args.timestamp_ms)
    fiscal_year = resolve_fiscal_year(target_at)

    default_object_key, _ = resolve_default_raw_object(target_at)
    object_key = args.object_key or default_object_key
    source_file_name = args.source_file_name or object_key.rsplit("/", maxsplit=1)[-1]

    if args.use_ingestion_log and args.object_key is None:
        object_key = None
        source_file_name = None

    rustfs = RustFSClient()
    row_count = run_source_to_bronze_jepx_spot_price(
        client=rustfs,
        bucket_name=args.bucket,
        object_key=object_key,
        source_file_name=source_file_name,
        catalog_name=args.catalog,
        table_identifier=args.table,
        schema_path=args.schema_path,
        skip_if_exists=not args.allow_duplicate_source,
        fiscal_year=fiscal_year,
        use_ingestion_log=args.use_ingestion_log,
        require_unprocessed=args.require_unprocessed,
        update_ingestion_log_status=args.use_ingestion_log,
    )
    logger.info(
        "Ingestion completed: table=%s, source=%s, rows=%s",
        args.table,
        source_file_name,
        row_count,
    )


def _configure_raw_pipeline(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--bucket",
        default=DEFAULT_BUCKET,
        help=f"Source/target bucket name (default: {DEFAULT_BUCKET})",
    )
    parser.add_argument(
        "--timestamp-ms",
        type=int,
        help="Optional UNIX timestamp (ms) to resolve target source file",
    )
    add_catalog_arg(parser)
    parser.add_argument(
        "--table",
        default="bronze.jepx_spot_price",
        help="Target Iceberg table identifier",
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


def _handle_raw_pipeline(
    args: argparse.Namespace, parser: argparse.ArgumentParser
) -> None:
    target_at = resolve_target_at(args.timestamp_ms)

    rustfs = RustFSClient()
    scraper = JEPXSpotSummaryScraper()
    try:
        scrape_result = scrape_jepx_to_rustfs(
            storage_client=rustfs,
            scraper=scraper,
            bucket_name=args.bucket,
            target_at=target_at,
        )
    finally:
        scraper.close()

    row_count = run_source_to_bronze_jepx_spot_price(
        client=rustfs,
        bucket_name=args.bucket,
        object_key=scrape_result.object_key,
        source_file_name=scrape_result.file_name,
        catalog_name=args.catalog,
        table_identifier=args.table,
        schema_path=args.schema_path,
        skip_if_exists=not args.allow_duplicate_source,
    )
    logger.info(
        "JEPX raw pipeline completed: source=s3://%s/%s, table=%s, rows=%s",
        scrape_result.bucket_name,
        scrape_result.object_key,
        args.table,
        row_count,
    )


def _configure_orchestrator(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--bucket",
        default=DEFAULT_BUCKET,
        help=f"Source/target bucket name (default: {DEFAULT_BUCKET})",
    )
    parser.add_argument(
        "--timestamp-ms",
        type=int,
        help="Optional UNIX timestamp in milliseconds for the JEPX run",
    )
    add_catalog_arg(parser)
    parser.add_argument(
        "--bronze-table",
        default="bronze.jepx_spot_price",
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
        "--dbt-project-dir",
        default=DEFAULT_DBT_PROJECT_DIR,
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


def _handle_orchestrator(
    args: argparse.Namespace, parser: argparse.ArgumentParser
) -> None:
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
    for result in results:
        logger.info(
            "Orchestrator step result: step=%s, status=%s, detail=%s",
            result.name,
            result.status,
            result.detail,
        )

    # A failed step that only reaches the log still exits 0, which is how both
    # backfill incidents passed for clean runs.
    if has_failed_step(results):
        raise SystemExit(1)


def _configure_backfill(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--from-fiscal-year",
        type=int,
        required=True,
        help="First fiscal year to rebuild (e.g. 2005)",
    )
    parser.add_argument(
        "--to-fiscal-year",
        type=int,
        help="Last fiscal year to rebuild (default: same as --from-fiscal-year)",
    )
    parser.add_argument(
        "--from-raw",
        action="store_true",
        help=(
            "Rebuild from the raw snapshots already in storage instead of "
            "fetching them again (replay_strategy.md's rebuild procedure)"
        ),
    )
    parser.add_argument(
        "--request-delay-seconds",
        type=float,
        default=DEFAULT_REQUEST_DELAY_SECONDS,
        help=(
            "Pause between each year's source request "
            f"(default: {DEFAULT_REQUEST_DELAY_SECONDS}; ignored with --from-raw)"
        ),
    )
    parser.add_argument(
        "--bucket",
        default=DEFAULT_BUCKET,
        help=f"Source/target bucket name (default: {DEFAULT_BUCKET})",
    )
    add_catalog_arg(parser)
    parser.add_argument(
        "--bronze-table",
        default="bronze.jepx_spot_price",
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


def _handle_backfill(args: argparse.Namespace, parser: argparse.ArgumentParser) -> None:
    to_fiscal_year = (
        args.to_fiscal_year
        if args.to_fiscal_year is not None
        else args.from_fiscal_year
    )
    if to_fiscal_year < args.from_fiscal_year:
        parser.error(
            f"--to-fiscal-year ({to_fiscal_year}) is before --from-fiscal-year "
            f"({args.from_fiscal_year}); the range would cover no years"
        )

    results = run_jepx_backfill_pipeline(
        bucket_name=args.bucket,
        from_fiscal_year=args.from_fiscal_year,
        to_fiscal_year=to_fiscal_year,
        catalog_name=args.catalog,
        bronze_table_identifier=args.bronze_table,
        bronze_schema_path=args.bronze_schema_path,
        allow_duplicate_source=args.allow_duplicate_source,
        bronze_location=args.bronze_location,
        silver_schema_dir=args.silver_schema_dir,
        from_raw=args.from_raw,
        request_delay_seconds=args.request_delay_seconds,
    )
    for result in results:
        logger.info(
            "Backfill step result: step=%s, status=%s, detail=%s",
            result.name,
            result.status,
            result.detail,
        )

    # A failed step that only reaches the log still exits 0, which is how both
    # backfill incidents passed for clean runs.
    if has_failed_step(results):
        raise SystemExit(1)


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
    parser.add_argument(
        "--fiscal-year",
        type=int,
        help="Limit the run to one fiscal year (default: rebuild every year)",
    )


def _handle_silver(args: argparse.Namespace, parser: argparse.ArgumentParser) -> None:
    result = run_bronze_to_silver_jepx_spot_price(
        catalog_name=args.catalog,
        bronze_location=args.bronze_location,
        schema_dir=args.schema_dir,
        fiscal_year=args.fiscal_year,
    )
    logger.info(
        "JEPX bronze-to-silver completed: execution_id=%s, staged=%s, valid=%s, "
        "dropped=%s",
        result.execution_id,
        result.staged_row_count,
        result.valid_row_count,
        result.dropped_row_count,
    )
    for write in result.writes:
        logger.info(
            " - table=%s, written=%s",
            write.table_identifier,
            write.rows_written,
        )


def _configure_gold(parser: argparse.ArgumentParser) -> None:
    add_catalog_arg(parser)
    parser.add_argument(
        "--area-location",
        default=DEFAULT_AREA_LOCATION,
        help="Silver area table location scanned by DuckDB",
    )
    parser.add_argument(
        "--base-location",
        default=DEFAULT_BASE_LOCATION,
        help="Silver base table location scanned by DuckDB",
    )
    parser.add_argument(
        "--schema-dir",
        default=DEFAULT_GOLD_SCHEMA_DIR,
        help="Directory containing the gold schema CSV files",
    )
    parser.add_argument(
        "--fiscal-year",
        type=int,
        help="Limit the run to one fiscal year (default: rebuild every year)",
    )


def _handle_gold(args: argparse.Namespace, parser: argparse.ArgumentParser) -> None:
    result = run_silver_to_gold_jepx_spot_price(
        catalog_name=args.catalog,
        area_location=args.area_location,
        base_location=args.base_location,
        schema_dir=args.schema_dir,
        fiscal_year=args.fiscal_year,
    )
    logger.info(
        "JEPX silver-to-gold completed: execution_id=%s, dates=%s",
        result.execution_id,
        result.delivery_date_count,
    )
    for table in result.tables:
        logger.info(
            " - table=%s, staged=%s, written=%s",
            table.table_identifier,
            table.staged_row_count,
            table.rows_written,
        )


COMMANDS = [
    CommandSpec(
        name="scrape-jepx",
        help="Scrape JEPX spot summary and upload to RustFS raw layer",
        configure=_configure_scrape,
        handler=_handle_scrape,
    ),
    CommandSpec(
        name="ingest-jepx-raw-to-bronze",
        help="Ingest JEPX raw CSV from RustFS into bronze Iceberg table",
        configure=_configure_bronze,
        handler=_handle_bronze,
    ),
    CommandSpec(
        name="run-jepx-raw-pipeline",
        help="Scrape JEPX raw CSV and ingest it into bronze Iceberg table",
        configure=_configure_raw_pipeline,
        handler=_handle_raw_pipeline,
    ),
    CommandSpec(
        name="run-jepx-orchestrator",
        help="Run ADF-like JEPX end-to-end orchestrator",
        configure=_configure_orchestrator,
        handler=_handle_orchestrator,
    ),
    CommandSpec(
        name="ingest-jepx-bronze-to-silver",
        help="Transform JEPX bronze spot prices into silver Iceberg tables",
        configure=_configure_silver,
        handler=_handle_silver,
    ),
    CommandSpec(
        name="ingest-jepx-silver-to-gold",
        help="Aggregate JEPX silver spot prices into the gold daily table",
        configure=_configure_gold,
        handler=_handle_gold,
    ),
    CommandSpec(
        name="backfill-jepx",
        help="Rebuild a range of JEPX fiscal years (raw/bronze per year, silver once)",
        configure=_configure_backfill,
        handler=_handle_backfill,
    ),
]

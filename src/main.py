import argparse
import logging
import subprocess
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from common.iceberg import get_catalog, provision_table
from common.jepx_common import resolve_target_at
from common.storage_client import RustFSClient
from orchestration.jepx_pipeline import run_jepx_orchestrated_pipeline
from pipeline.bronze.migrate_occto_data import migrate_occto_data
from pipeline.bronze.source_to_bronze_jepx_spot_price import (
    ingest_jepx_spot_summary,
    resolve_default_raw_object,
)
from pipeline.bronze.source_to_bronze_occto import ingest_occto_unit_generation
from pipeline.raw.source_to_raw_jepx_spot_price import (
    JEPXSpotSummaryScraper,
    scrape_jepx_spot_price_raw,
    scrape_jepx_to_rustfs,
)
from pipeline.raw.source_to_raw_occto import (
    OCCTOUnitGenerationScraper,
    scrape_occto_unit_generation_raw,
)
from pipeline.silver.bronze_to_silver_jepx_spot_price import (
    DEFAULT_BRONZE_LOCATION,
    DEFAULT_SILVER_SCHEMA_DIR,
    run_bronze_to_silver_jepx_spot_price,
)
from pipeline.silver.bronze_to_silver_occto_unit_generation import (
    DEFAULT_BRONZE_LOCATION as OCCTO_DEFAULT_BRONZE_LOCATION,
)
from pipeline.silver.bronze_to_silver_occto_unit_generation import (
    DEFAULT_SILVER_SCHEMA_DIR as OCCTO_DEFAULT_SILVER_SCHEMA_DIR,
)
from pipeline.silver.bronze_to_silver_occto_unit_generation import (
    run_bronze_to_silver_occto_unit_generation,
)
from setup.rustfs_bucket_setup import BucketPlan, apply_bucket_plans

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)

DEFAULT_PREFIXES = ("raw", "bronze", "silver", "gold", "sandbox")
DEFAULT_BUCKET_PLANS = [
    BucketPlan(
        name="jp-power-grid-dev",
        prefixes=DEFAULT_PREFIXES,
        retention_days=None,
    ),
    BucketPlan(
        name="jp-power-grid-prd",
        prefixes=DEFAULT_PREFIXES,
        retention_days=7,
        retention_mode="COMPLIANCE",
    ),
]


def main():
    parser = argparse.ArgumentParser(description="Data platform orchestrator")
    subparsers = parser.add_subparsers(dest="command")

    bootstrap_parser = subparsers.add_parser(
        "bootstrap-storage",
        help="Create and configure RustFS buckets",
    )
    bootstrap_parser.add_argument(
        "--bucket",
        action="append",
        dest="buckets",
        help="Bucket name to initialize. Default is dev/prd plans.",
    )

    jepx_parser = subparsers.add_parser(
        "scrape-jepx",
        help="Scrape JEPX spot summary and upload to RustFS raw layer",
    )
    jepx_parser.add_argument(
        "--bucket",
        default="jp-power-grid-dev",
        help="Target bucket name (default: jp-power-grid-dev)",
    )
    jepx_parser.add_argument(
        "--timestamp-ms",
        type=int,
        help="Optional UNIX timestamp in milliseconds for the JEPX request",
    )
    jepx_parser.add_argument(
        "--fiscal-year",
        type=int,
        help="Fiscal year to scrape (e.g. 2024). Overrides --timestamp-ms."
        " Use for backfilling past years.",
    )

    bronze_parser = subparsers.add_parser(
        "ingest-jepx-raw-to-bronze",
        help="Ingest JEPX raw CSV from RustFS into bronze Iceberg table",
    )
    bronze_parser.add_argument(
        "--bucket",
        default="jp-power-grid-dev",
        help="Source bucket name (default: jp-power-grid-dev)",
    )
    bronze_parser.add_argument(
        "--object-key",
        help="Source object key in raw layer (default resolved from timestamp)",
    )
    bronze_parser.add_argument(
        "--source-file-name",
        help="Source file name stored in source_data (default from object key)",
    )
    bronze_parser.add_argument(
        "--timestamp-ms",
        type=int,
        help="Optional UNIX timestamp (ms) to resolve default source file",
    )
    bronze_parser.add_argument(
        "--catalog",
        default="dlh_dev",
        help="Iceberg catalog name (default: dlh_dev)",
    )
    bronze_parser.add_argument(
        "--table",
        default="bronze.jepx_spot_price",
        help="Target Iceberg table identifier",
    )
    bronze_parser.add_argument(
        "--schema-path",
        default="/workspace/configuration/iceberg/schema/bronze/jepx_spot_price.csv",
        help="Schema CSV path",
    )
    bronze_parser.add_argument(
        "--allow-duplicate-source",
        action="store_true",
        help="Allow append even if source_data already exists",
    )
    bronze_parser.add_argument(
        "--use-ingestion-log",
        action="store_true",
        help="Resolve latest raw snapshot from metadata ingestion log",
    )
    bronze_parser.add_argument(
        "--require-unprocessed",
        action="store_true",
        help="When using ingestion log, select only unprocessed latest snapshot",
    )

    jepx_pipeline_parser = subparsers.add_parser(
        "run-jepx-raw-pipeline",
        help="Scrape JEPX raw CSV and ingest it into bronze Iceberg table",
    )
    jepx_pipeline_parser.add_argument(
        "--bucket",
        default="jp-power-grid-dev",
        help="Source/target bucket name (default: jp-power-grid-dev)",
    )
    jepx_pipeline_parser.add_argument(
        "--timestamp-ms",
        type=int,
        help="Optional UNIX timestamp (ms) to resolve target source file",
    )
    jepx_pipeline_parser.add_argument(
        "--catalog",
        default="dlh_dev",
        help="Iceberg catalog name (default: dlh_dev)",
    )
    jepx_pipeline_parser.add_argument(
        "--table",
        default="bronze.jepx_spot_price",
        help="Target Iceberg table identifier",
    )
    jepx_pipeline_parser.add_argument(
        "--schema-path",
        default="/workspace/configuration/iceberg/schema/bronze/jepx_spot_price.csv",
        help="Schema CSV path",
    )
    jepx_pipeline_parser.add_argument(
        "--allow-duplicate-source",
        action="store_true",
        help="Allow append even if source_data already exists",
    )

    jepx_orchestrator_parser = subparsers.add_parser(
        "run-jepx-orchestrator",
        help="Run ADF-like JEPX end-to-end orchestrator",
    )
    jepx_orchestrator_parser.add_argument(
        "--bucket",
        default="jp-power-grid-dev",
        help="Source/target bucket name (default: jp-power-grid-dev)",
    )
    jepx_orchestrator_parser.add_argument(
        "--timestamp-ms",
        type=int,
        help="Optional UNIX timestamp in milliseconds for the JEPX run",
    )
    jepx_orchestrator_parser.add_argument(
        "--catalog",
        default="dlh_dev",
        help="Iceberg catalog name (default: dlh_dev)",
    )
    jepx_orchestrator_parser.add_argument(
        "--bronze-table",
        default="bronze.jepx_spot_price",
        help="Target bronze Iceberg table identifier",
    )
    jepx_orchestrator_parser.add_argument(
        "--bronze-schema-path",
        default="/workspace/configuration/iceberg/schema/bronze/jepx_spot_price.csv",
        help="Bronze schema CSV path",
    )
    jepx_orchestrator_parser.add_argument(
        "--allow-duplicate-source",
        action="store_true",
        help="Allow append even if source_data already exists",
    )
    jepx_orchestrator_parser.add_argument(
        "--dbt-project-dir",
        default="/workspace/src/dbt/jepx_power",
        help="dbt project directory",
    )
    jepx_orchestrator_parser.add_argument(
        "--dbt-profiles-dir",
        help="dbt profiles directory (default: same as dbt project dir)",
    )
    jepx_orchestrator_parser.add_argument(
        "--bronze-location",
        default=DEFAULT_BRONZE_LOCATION,
        help="Bronze table location scanned by the silver transform",
    )
    jepx_orchestrator_parser.add_argument(
        "--silver-schema-dir",
        default=DEFAULT_SILVER_SCHEMA_DIR,
        help="Directory containing the silver schema CSV files",
    )
    jepx_orchestrator_parser.add_argument(
        "--silver-fiscal-year",
        type=int,
        help=(
            "Fiscal year for the silver step "
            "(default: the fiscal year that was just ingested)"
        ),
    )
    jepx_orchestrator_parser.add_argument(
        "--silver-all-fiscal-years",
        action="store_true",
        help="Rebuild every fiscal year in the silver step instead of just one",
    )
    jepx_orchestrator_parser.add_argument(
        "--run-gold-step",
        action="store_true",
        help="Enable gold step execution",
    )
    jepx_orchestrator_parser.add_argument(
        "--gold-select",
        default="tag:gold",
        help="dbt select expression for gold step",
    )
    jepx_orchestrator_parser.add_argument(
        "--dbt-full-refresh",
        action="store_true",
        help="Run dbt steps with --full-refresh",
    )

    occto_bronze_parser = subparsers.add_parser(
        "ingest-occto-raw-to-bronze",
        help="Ingest OCCTO unit generation raw CSV into bronze Iceberg table",
    )
    occto_bronze_parser.add_argument(
        "--bucket",
        default="jp-power-grid-dev",
        help="Source bucket name (default: jp-power-grid-dev)",
    )
    occto_bronze_parser.add_argument(
        "--object-key",
        help="Source object key in raw layer"
        " (e.g. raw/occto/unit_generation/target_date=.../ingested_at=.../<file>.csv)."
        " Required unless --use-ingestion-log is set",
    )
    occto_bronze_parser.add_argument(
        "--source-file-name",
        help="Source file name stored in source_data (default: object-key in full)",
    )
    occto_bronze_parser.add_argument(
        "--target-date",
        help="Target date in YYYY-MM-DD. Required when --use-ingestion-log is"
        " set and --object-key is omitted",
    )
    occto_bronze_parser.add_argument(
        "--catalog",
        default="dlh_dev",
        help="Iceberg catalog name (default: dlh_dev)",
    )
    occto_bronze_parser.add_argument(
        "--table",
        default="bronze.occto_unit_generation_actuals",
        help="Target Iceberg table identifier"
        " (default: bronze.occto_unit_generation_actuals)",
    )
    occto_bronze_parser.add_argument(
        "--schema-path",
        default="/workspace/configuration/iceberg/schema/bronze/occto_unit_generation_actuals.csv",
        help="Schema CSV path",
    )
    occto_bronze_parser.add_argument(
        "--allow-duplicate-source",
        action="store_true",
        help="Allow append even if source_data already exists",
    )
    occto_bronze_parser.add_argument(
        "--use-ingestion-log",
        action="store_true",
        help="Resolve latest raw snapshot from metadata ingestion log",
    )
    occto_bronze_parser.add_argument(
        "--require-unprocessed",
        action="store_true",
        help="When using ingestion log, select only unprocessed latest snapshot",
    )

    occto_silver_parser = subparsers.add_parser(
        "ingest-occto-bronze-to-silver",
        help="Transform OCCTO bronze unit generation actuals into silver",
    )
    occto_silver_parser.add_argument(
        "--catalog",
        default="dlh_dev",
        help="Iceberg catalog name (default: dlh_dev)",
    )
    occto_silver_parser.add_argument(
        "--bronze-location",
        default=OCCTO_DEFAULT_BRONZE_LOCATION,
        help="Bronze table location scanned by DuckDB",
    )
    occto_silver_parser.add_argument(
        "--schema-dir",
        default=OCCTO_DEFAULT_SILVER_SCHEMA_DIR,
        help="Directory containing the silver schema CSV files",
    )
    occto_silver_parser.add_argument(
        "--target-date",
        help="Limit the run to one target_date in YYYY-MM-DD"
        " (default: rebuild the full range staged from bronze)",
    )
    occto_silver_parser.add_argument(
        "--from-date",
        help="Start of a target_date range in YYYY-MM-DD (overrides --target-date)",
    )
    occto_silver_parser.add_argument(
        "--to-date",
        help="End of a target_date range in YYYY-MM-DD"
        " (default: same as --from-date/--target-date)",
    )

    occto_migrate_parser = subparsers.add_parser(
        "migrate-occto-to-rustfs",
        help="Migrate OCCTO source CSV files from local data to RustFS raw/occto",
    )
    occto_migrate_parser.add_argument(
        "--local-dir",
        default="/workspace/data/occto",
        help="Local directory containing OCCTO CSV files",
    )
    occto_migrate_parser.add_argument(
        "--bucket",
        default="jp-power-grid-dev",
        help="Destination bucket name (default: jp-power-grid-dev)",
    )
    occto_migrate_parser.add_argument(
        "--s3-prefix",
        default="raw/occto",
        help="Destination S3 prefix (default: raw/occto)",
    )
    occto_migrate_parser.add_argument(
        "--keep-local",
        action="store_true",
        help="Keep the local directory after upload",
    )

    occto_scrape_parser = subparsers.add_parser(
        "scrape-occto",
        help="Download OCCTO unit generation CSV and upload to RustFS raw layer",
    )
    occto_scrape_parser.add_argument(
        "--bucket",
        default="jp-power-grid-dev",
        help="Target bucket name (default: jp-power-grid-dev)",
    )
    occto_scrape_parser.add_argument(
        "--target-date",
        help="Target date in YYYY-MM-DD (default: previous day in Asia/Tokyo)",
    )
    occto_scrape_parser.add_argument(
        "--to-date",
        help=(
            "End of target date range in YYYY-MM-DD for a multi-day fetch "
            "(default: same as --target-date, i.e. a single day)"
        ),
    )

    silver_parser = subparsers.add_parser(
        "provision-silver-tables",
        help="Provision silver Iceberg tables from schema CSV files",
    )
    silver_parser.add_argument(
        "--catalog",
        default="dlh_dev",
        help="Iceberg catalog name (default: dlh_dev)",
    )
    silver_parser.add_argument(
        "--schema-dir",
        default="/workspace/configuration/iceberg/schema/silver",
        help="Directory containing silver schema CSV files",
    )

    jepx_silver_parser = subparsers.add_parser(
        "ingest-jepx-bronze-to-silver",
        help="Transform JEPX bronze spot prices into silver Iceberg tables",
    )
    jepx_silver_parser.add_argument(
        "--catalog",
        default="dlh_dev",
        help="Iceberg catalog name (default: dlh_dev)",
    )
    jepx_silver_parser.add_argument(
        "--bronze-location",
        default=DEFAULT_BRONZE_LOCATION,
        help="Bronze table location scanned by DuckDB",
    )
    jepx_silver_parser.add_argument(
        "--schema-dir",
        default=DEFAULT_SILVER_SCHEMA_DIR,
        help="Directory containing the silver schema CSV files",
    )
    jepx_silver_parser.add_argument(
        "--fiscal-year",
        type=int,
        help="Limit the run to one fiscal year (default: rebuild every year)",
    )

    occto_dbt_parser = subparsers.add_parser(
        "run-occto-silver-dbt",
        help="Run dbt staging and silver models for OCCTO using DuckDB",
    )
    occto_dbt_parser.add_argument(
        "--select",
        default="tag:occto",
        help="dbt select expression (default: tag:occto)",
    )
    occto_dbt_parser.add_argument(
        "--full-refresh",
        action="store_true",
        help="Run dbt with --full-refresh",
    )

    args = parser.parse_args()

    if args.command in {None, "bootstrap-storage"}:
        rustfs = RustFSClient()
        if args.command is None:
            logger.info(
                "No command was provided. Running bootstrap-storage for compatibility."
            )

        buckets = getattr(args, "buckets", None)
        if buckets:
            target_set = set(buckets)
            known_names = {plan.name for plan in DEFAULT_BUCKET_PLANS}
            unknown_names = sorted(target_set - known_names)
            if unknown_names:
                parser.error(f"Unknown bucket names: {', '.join(unknown_names)}")

            selected_plans = [
                plan for plan in DEFAULT_BUCKET_PLANS if plan.name in target_set
            ]
        else:
            selected_plans = DEFAULT_BUCKET_PLANS

        created, existing = apply_bucket_plans(rustfs, selected_plans)

        if created:
            logger.info(f"Created buckets: {created}")
        if existing:
            logger.info(f"Existing buckets: {existing}")
        return

    if args.command == "scrape-jepx":
        rustfs = RustFSClient()
        scraper = JEPXSpotSummaryScraper()
        if args.fiscal_year is not None:
            target_at = datetime(args.fiscal_year, 4, 1, tzinfo=ZoneInfo("UTC"))
        else:
            target_at = resolve_target_at(args.timestamp_ms)

        try:
            result = scrape_jepx_spot_price_raw(
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

    if args.command == "ingest-jepx-raw-to-bronze":
        target_at = resolve_target_at(args.timestamp_ms)
        fiscal_year = target_at.year if target_at.month >= 4 else target_at.year - 1

        default_object_key, _ = resolve_default_raw_object(target_at)
        object_key = args.object_key or default_object_key
        source_file_name = (
            args.source_file_name or object_key.rsplit("/", maxsplit=1)[-1]
        )

        if args.use_ingestion_log and args.object_key is None:
            object_key = None
            source_file_name = None

        rustfs = RustFSClient()
        row_count = ingest_jepx_spot_summary(
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

    if args.command == "run-jepx-raw-pipeline":
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

        row_count = ingest_jepx_spot_summary(
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

    if args.command == "run-jepx-orchestrator":
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

    if args.command == "ingest-occto-raw-to-bronze":
        target_date = None
        if args.target_date:
            try:
                target_date = date.fromisoformat(args.target_date)
            except ValueError as exc:
                parser.error(f"Invalid --target-date value: {args.target_date} ({exc})")
        elif args.use_ingestion_log and args.object_key is None:
            parser.error(
                "--target-date is required when --use-ingestion-log is set "
                "without --object-key"
            )

        rustfs = RustFSClient()
        row_count = ingest_occto_unit_generation(
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

    if args.command == "migrate-occto-to-rustfs":
        migrated_count = migrate_occto_data(
            local_dir=args.local_dir,
            bucket_name=args.bucket,
            s3_prefix=args.s3_prefix,
            delete_after=not args.keep_local,
        )
        logger.info(
            "OCCTO migration completed: bucket=%s, prefix=%s, files=%s",
            args.bucket,
            args.s3_prefix,
            migrated_count,
        )

    if args.command == "scrape-occto":
        if args.target_date:
            try:
                from_date = date.fromisoformat(args.target_date)
            except ValueError as exc:
                parser.error(f"Invalid --target-date value: {args.target_date} ({exc})")
        else:
            jst_now = datetime.now(ZoneInfo("Asia/Tokyo"))
            from_date = (jst_now - timedelta(days=1)).date()

        to_date = None
        if args.to_date:
            try:
                to_date = date.fromisoformat(args.to_date)
            except ValueError as exc:
                parser.error(f"Invalid --to-date value: {args.to_date} ({exc})")

        rustfs = RustFSClient()
        scraper = OCCTOUnitGenerationScraper()
        try:
            result = scrape_occto_unit_generation_raw(
                storage_client=rustfs,
                scraper=scraper,
                bucket_name=args.bucket,
                from_date=from_date,
                to_date=to_date,
            )
            if result.skipped:
                logger.info(
                    "OCCTO scrape skipped (no change): target_date=%s, sha256=%.8s",
                    result.from_date,
                    result.sha256,
                )
            else:
                logger.info(
                    "OCCTO snapshot saved: target_date=%s, sha256=%.8s, prefix=%s",
                    result.from_date,
                    result.sha256,
                    result.snapshot_prefix,
                )
        finally:
            scraper.close()

    if args.command == "provision-silver-tables":
        schema_dir = Path(args.schema_dir)
        if not schema_dir.exists():
            parser.error(f"Schema directory does not exist: {schema_dir}")

        schema_files = sorted(schema_dir.glob("*.csv"))
        if not schema_files:
            parser.error(f"No schema CSV files found in: {schema_dir}")

        catalog = get_catalog(args.catalog)
        provisioned = 0
        for schema_file in schema_files:
            table_identifier = f"silver.{schema_file.stem}"
            provision_table(catalog, table_identifier, str(schema_file))
            provisioned += 1

        logger.info("Provisioned silver tables: %s", provisioned)

    if args.command == "ingest-jepx-bronze-to-silver":
        result = run_bronze_to_silver_jepx_spot_price(
            catalog_name=args.catalog,
            bronze_location=args.bronze_location,
            schema_dir=args.schema_dir,
            fiscal_year=args.fiscal_year,
        )
        logger.info(
            "JEPX bronze-to-silver completed: execution_id=%s, dropped=%s",
            result.execution_id,
            result.dropped_row_count,
        )
        for write in result.writes:
            logger.info(
                " - table=%s, written=%s",
                write.table_identifier,
                write.rows_written,
            )

    if args.command == "ingest-occto-bronze-to-silver":
        from_date = None
        to_date = None
        if args.from_date:
            try:
                from_date = date.fromisoformat(args.from_date)
            except ValueError as exc:
                parser.error(f"Invalid --from-date value: {args.from_date} ({exc})")
            if args.to_date:
                try:
                    to_date = date.fromisoformat(args.to_date)
                except ValueError as exc:
                    parser.error(f"Invalid --to-date value: {args.to_date} ({exc})")
            else:
                to_date = from_date
        elif args.target_date:
            try:
                from_date = date.fromisoformat(args.target_date)
            except ValueError as exc:
                parser.error(f"Invalid --target-date value: {args.target_date} ({exc})")
            to_date = from_date

        result = run_bronze_to_silver_occto_unit_generation(
            catalog_name=args.catalog,
            bronze_location=args.bronze_location,
            schema_dir=args.schema_dir,
            from_date=from_date,
            to_date=to_date,
        )
        logger.info(
            "OCCTO bronze-to-silver completed: execution_id=%s, dropped=%s, "
            "daily_amount_mismatch=%s",
            result.execution_id,
            result.dropped_row_count,
            result.daily_amount_mismatch,
        )
        logger.info(
            " - table=%s, written=%s",
            result.write.table_identifier,
            result.write.rows_written,
        )

    if args.command == "run-occto-silver-dbt":
        project_dir = Path("/workspace/src/dbt/jepx_power")
        profiles_dir = project_dir

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
            args.select,
        ]
        if args.full_refresh:
            dbt_command.append("--full-refresh")

        logger.info("Executing dbt OCCTO command: %s", " ".join(dbt_command))
        subprocess.run(dbt_command, check=True)


if __name__ == "__main__":
    main()

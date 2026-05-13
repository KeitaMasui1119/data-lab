import argparse
import logging
import os
import subprocess
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from catalog.manage_iceberg import get_catalog, provision_table
from core.storage_client import RustFSClient
from pipeline.bootstrap.rustfs_bootstrap import BucketPlan, apply_bucket_plans
from pipeline.ingestion.ingest_jepx import (
    ingest_jepx_spot_summary,
    resolve_default_raw_object,
)
from pipeline.ingestion.ingest_occto import ingest_occto_unit_generation
from pipeline.ingestion.migrate_occto_data import migrate_occto_data
from pipeline.jepx.common import resolve_target_at
from pipeline.orchestrator.jepx_pipeline import run_jepx_orchestrated_pipeline
from pipeline.raw.jepx_to_rustfs import scrape_jepx_to_rustfs
from pipeline.scraper.module.jepx import JEPXSpotSummaryScraper
from pipeline.scraper.module.occto import (
    OCCTOUnitGenerationConfig,
    OCCTOUnitGenerationScraper,
)
from pipeline.scraper.occto_to_rustfs import scrape_occto_to_rustfs

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
        default="/workspace/data/schema/bronze/jepx_spot_price.csv",
        help="Schema CSV path",
    )
    bronze_parser.add_argument(
        "--allow-duplicate-source",
        action="store_true",
        help="Allow append even if source_data already exists",
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
        default="/workspace/data/schema/bronze/jepx_spot_price.csv",
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
        default="/workspace/data/schema/bronze/jepx_spot_price.csv",
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
        "--staging-select",
        default="tag:staging",
        help="dbt select expression for staging step",
    )
    jepx_orchestrator_parser.add_argument(
        "--silver-select",
        default="tag:silver",
        help="dbt select expression for silver step",
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
    jepx_orchestrator_parser.add_argument(
        "--export-silver-to-iceberg",
        action="store_true",
        help="Export dbt silver tables into PyIceberg silver tables",
    )
    jepx_orchestrator_parser.add_argument(
        "--dbt-duckdb-path",
        default="/workspace/src/dbt/jepx_power/jepx_power.duckdb",
        help="Path to dbt DuckDB file used as silver export source",
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
        required=True,
        help="Source object key in raw layer"
        " (e.g. raw/occto/unit_generation/ユニット別発電実績_xxx.csv)",
    )
    occto_bronze_parser.add_argument(
        "--source-file-name",
        help="Source file name stored in source_data"
        " (default: last segment of object-key)",
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
        default="/workspace/data/schema/bronze/occto_unit_generation_actuals.csv",
        help="Schema CSV path",
    )
    occto_bronze_parser.add_argument(
        "--allow-duplicate-source",
        action="store_true",
        help="Allow append even if source_data already exists",
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
        "--download-url",
        help=(
            "OCCTO CSV download endpoint URL "
            "(default: OCCTO_DOWNLOAD_CSV_URL environment variable)"
        ),
    )
    occto_scrape_parser.add_argument(
        "--referer",
        help="Optional Referer header value",
    )
    occto_scrape_parser.add_argument(
        "--date-param-name",
        default="targetDate",
        help="Query parameter name used for target date (default: targetDate)",
    )
    occto_scrape_parser.add_argument(
        "--date-format",
        default="%Y-%m-%d",
        help="Date format for query parameter and file name (default: %%Y-%%m-%%d)",
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
        default="/workspace/data/schema/silver",
        help="Directory containing silver schema CSV files",
    )

    dbt_staging_parser = subparsers.add_parser(
        "run-jepx-staging-dbt",
        help="Run dbt staging models for JEPX using DuckDB",
    )
    dbt_staging_parser.add_argument(
        "--select",
        default="tag:staging",
        help="dbt select expression (default: tag:staging)",
    )
    dbt_staging_parser.add_argument(
        "--full-refresh",
        action="store_true",
        help="Run dbt with --full-refresh",
    )

    dbt_silver_parser = subparsers.add_parser(
        "run-jepx-silver-dbt",
        help="Run dbt silver models for JEPX using DuckDB",
    )
    dbt_silver_parser.add_argument(
        "--select",
        default="tag:silver",
        help="dbt select expression (default: tag:silver)",
    )
    dbt_silver_parser.add_argument(
        "--full-refresh",
        action="store_true",
        help="Run dbt with --full-refresh",
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
        target_at = resolve_target_at(args.timestamp_ms)

        try:
            result = scrape_jepx_to_rustfs(
                storage_client=rustfs,
                scraper=scraper,
                bucket_name=args.bucket,
                target_at=target_at,
            )
            logger.info(
                "Uploaded JEPX raw file to s3://%s/%s (%s bytes)",
                result.bucket_name,
                result.object_key,
                result.size_bytes,
            )
        finally:
            scraper.close()

    if args.command == "ingest-jepx-raw-to-bronze":
        target_at = resolve_target_at(args.timestamp_ms)

        default_object_key, _ = resolve_default_raw_object(target_at)
        object_key = args.object_key or default_object_key
        source_file_name = (
            args.source_file_name or object_key.rsplit("/", maxsplit=1)[-1]
        )

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
        for result in results:
            logger.info(
                "Orchestrator step result: step=%s, status=%s, detail=%s",
                result.name,
                result.status,
                result.detail,
            )

    if args.command == "ingest-occto-raw-to-bronze":
        object_key = args.object_key
        source_file_name = (
            args.source_file_name or object_key.rsplit("/", maxsplit=1)[-1]
        )

        rustfs = RustFSClient()
        row_count = ingest_occto_unit_generation(
            client=rustfs,
            bucket_name=args.bucket,
            object_key=object_key,
            source_file_name=source_file_name,
            catalog_name=args.catalog,
            table_identifier=args.table,
            schema_path=args.schema_path,
            skip_if_exists=not args.allow_duplicate_source,
        )
        logger.info(
            "Ingestion completed: table=%s, source=%s, rows=%s",
            args.table,
            source_file_name,
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
        download_url = args.download_url or os.getenv("OCCTO_DOWNLOAD_CSV_URL")
        if not download_url:
            parser.error(
                "OCCTO download URL is required. "
                "Provide --download-url or set OCCTO_DOWNLOAD_CSV_URL."
            )

        if args.target_date:
            try:
                target_date = date.fromisoformat(args.target_date)
            except ValueError as exc:
                parser.error(f"Invalid --target-date value: {args.target_date} ({exc})")
        else:
            jst_now = datetime.now(ZoneInfo("Asia/Tokyo"))
            target_date = (jst_now - timedelta(days=1)).date()

        rustfs = RustFSClient()
        scraper_config = OCCTOUnitGenerationConfig(
            base_url=download_url,
            referer=args.referer,
            date_param_name=args.date_param_name,
            date_format=args.date_format,
        )
        scraper = OCCTOUnitGenerationScraper(config=scraper_config)
        try:
            result = scrape_occto_to_rustfs(
                storage_client=rustfs,
                scraper=scraper,
                bucket_name=args.bucket,
                target_at=target_date,
            )
            logger.info(
                "Uploaded OCCTO raw file to s3://%s/%s (%s bytes)",
                result.bucket_name,
                result.object_key,
                result.size_bytes,
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

    if args.command == "run-jepx-staging-dbt":
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

        logger.info("Executing dbt staging command: %s", " ".join(dbt_command))
        subprocess.run(dbt_command, check=True)

    if args.command == "run-jepx-silver-dbt":
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

        logger.info("Executing dbt silver command: %s", " ".join(dbt_command))
        subprocess.run(dbt_command, check=True)

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

import argparse
import logging
import subprocess
from datetime import UTC, datetime
from pathlib import Path

from catalog.manage_iceberg import get_catalog, provision_table
from core.storage_client import RustFSClient
from pipeline.bootstrap.rustfs_bootstrap import BucketPlan, apply_bucket_plans
from pipeline.ingestion.ingest_jepx import (
    ingest_jepx_spot_summary,
    resolve_default_raw_object,
)
from pipeline.ingestion.ingest_occto import ingest_occto_unit_generation
from pipeline.ingestion.migrate_occto_data import migrate_occto_data
from pipeline.scraper.jepx_to_rustfs import scrape_jepx_to_rustfs
from pipeline.scraper.module.jepx import JEPXSpotSummaryScraper

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

        if args.timestamp_ms:
            target_at = datetime.fromtimestamp(
                args.timestamp_ms / 1000,
                tz=UTC,
            )
        else:
            target_at = datetime.now(UTC)

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
        if args.timestamp_ms:
            target_at = datetime.fromtimestamp(args.timestamp_ms / 1000, tz=UTC)
        else:
            target_at = datetime.now(UTC)

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


if __name__ == "__main__":
    main()

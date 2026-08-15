import argparse
import logging
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from common.iceberg.catalog import evolve_partition_spec, get_catalog, provision_table
from common.iceberg.maintenance import (
    delete_orphan_data_files,
    expire_old_snapshots,
    find_orphan_data_files,
)
from common.storage_client import RustFSClient
from orchestration.jepx_pipeline import run_jepx_orchestrated_pipeline
from orchestration.pl_occto_unit_generation_actuals import (
    run_occto_orchestrated_pipeline,
)
from pipeline.bronze.source_to_bronze_jepx_spot_price import (
    ingest_jepx_spot_summary,
    resolve_default_raw_object,
)
from pipeline.bronze.source_to_bronze_occto_unit_generation_actuals import (
    ingest_occto_unit_generation,
)
from pipeline.bronze.source_to_bronze_power_usage_hokuriku import (
    ingest_power_usage_hokuriku,
)
from pipeline.bronze.source_to_bronze_supply_demand_actuals_chugoku import (
    ingest_supply_demand_actuals_chugoku,
)
from pipeline.bronze.source_to_bronze_supply_demand_actuals_shikoku import (
    ingest_supply_demand_actuals_shikoku,
)
from pipeline.bronze.source_to_bronze_supply_demand_actuals_tohoku import (
    ingest_supply_demand_actuals_tohoku,
)
from pipeline.jepx_common import resolve_target_at
from pipeline.raw.source_to_raw_jepx_spot_price import (
    JEPXSpotSummaryScraper,
    scrape_jepx_spot_price_raw,
    scrape_jepx_to_rustfs,
)
from pipeline.raw.source_to_raw_occto_unit_generation_actuals import (
    OCCTOUnitGenerationScraper,
    resolve_default_target_date,
    scrape_occto_unit_generation_raw,
)
from pipeline.raw.source_to_raw_power_usage_hokuriku import (
    HokurikuPowerUsageScraper,
    scrape_power_usage_hokuriku_raw,
)
from pipeline.raw.source_to_raw_power_usage_hokuriku import (
    resolve_default_target_date as resolve_default_power_usage_hokuriku_target_date,
)
from pipeline.raw.source_to_raw_supply_demand_actuals_chugoku import (
    ChugokuSupplyDemandActualsScraper,
    scrape_supply_demand_actuals_chugoku_raw,
)
from pipeline.raw.source_to_raw_supply_demand_actuals_chugoku import (
    resolve_default_target_date as resolve_default_sda_chugoku_target_date,
)
from pipeline.raw.source_to_raw_supply_demand_actuals_shikoku import (
    ShikokuSupplyDemandActualsScraper,
    scrape_supply_demand_actuals_shikoku_raw,
)
from pipeline.raw.source_to_raw_supply_demand_actuals_shikoku import (
    resolve_default_target_date as resolve_default_sda_shikoku_target_date,
)
from pipeline.raw.source_to_raw_supply_demand_actuals_tohoku import (
    TohokuSupplyDemandActualsScraper,
    scrape_supply_demand_actuals_tohoku_raw,
)
from pipeline.raw.source_to_raw_supply_demand_actuals_tohoku import (
    resolve_default_target_date as resolve_default_sda_tohoku_target_date,
)
from pipeline.silver.bronze_to_silver_jepx_spot_price import (
    DEFAULT_BRONZE_LOCATION,
    DEFAULT_SILVER_SCHEMA_DIR,
    run_bronze_to_silver_jepx_spot_price,
)
from pipeline.silver.bronze_to_silver_occto_unit_generation_actuals import (
    DEFAULT_BRONZE_LOCATION as OCCTO_DEFAULT_BRONZE_LOCATION,
)
from pipeline.silver.bronze_to_silver_occto_unit_generation_actuals import (
    DEFAULT_SILVER_SCHEMA_DIR as OCCTO_DEFAULT_SILVER_SCHEMA_DIR,
)
from pipeline.silver.bronze_to_silver_occto_unit_generation_actuals import (
    run_bronze_to_silver_occto_unit_generation,
)
from pipeline.silver.bronze_to_silver_power_usage_hokuriku import (
    run_bronze_to_silver_power_usage_hokuriku,
)
from pipeline.silver.bronze_to_silver_supply_demand_actuals import (
    SILVER_CONFIGS as SUPPLY_DEMAND_ACTUALS_SILVER_CONFIGS,
)
from pipeline.silver.bronze_to_silver_supply_demand_actuals import (
    run_bronze_to_silver_supply_demand_actuals,
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

    power_usage_hokuriku_bronze_parser = subparsers.add_parser(
        "ingest-power-usage-hokuriku-raw-to-bronze",
        help=(
            "Ingest Hokuriku power_usage raw snapshot CSV into its 3 bronze"
            " Iceberg tables (daily_summary, hourly, interval5)"
        ),
    )
    power_usage_hokuriku_bronze_parser.add_argument(
        "--bucket",
        default="jp-power-grid-dev",
        help="Source bucket name (default: jp-power-grid-dev)",
    )
    power_usage_hokuriku_bronze_parser.add_argument(
        "--object-key",
        help="Source object key in raw layer"
        " (e.g. raw/power_usage/hokuriku/target_date=.../ingested_at=.../<file>.csv)."
        " Required unless --use-ingestion-log is set",
    )
    power_usage_hokuriku_bronze_parser.add_argument(
        "--source-file-name",
        help="Source file name stored in source_data (default: object-key in full)",
    )
    power_usage_hokuriku_bronze_parser.add_argument(
        "--target-date",
        required=True,
        help="Target date in YYYY-MM-DD (the day this snapshot's data covers)",
    )
    power_usage_hokuriku_bronze_parser.add_argument(
        "--catalog",
        default="dlh_dev",
        help="Iceberg catalog name (default: dlh_dev)",
    )
    power_usage_hokuriku_bronze_parser.add_argument(
        "--allow-duplicate-source",
        action="store_true",
        help="Allow append even if source_data already exists",
    )
    power_usage_hokuriku_bronze_parser.add_argument(
        "--use-ingestion-log",
        action="store_true",
        help="Resolve latest raw snapshot from metadata ingestion log",
    )
    power_usage_hokuriku_bronze_parser.add_argument(
        "--require-unprocessed",
        action="store_true",
        help="When using ingestion log, select only unprocessed latest snapshot",
    )

    def _add_sda_bronze_arguments(parser: argparse.ArgumentParser) -> None:
        parser.add_argument(
            "--bucket",
            default="jp-power-grid-dev",
            help="Source bucket name (default: jp-power-grid-dev)",
        )
        parser.add_argument(
            "--object-key",
            required=True,
            help="Source object key in raw layer"
            " (e.g. raw/supply_demand_actuals/tohoku/year=.../"
            "ingested_at=.../<file>.csv)",
        )
        parser.add_argument(
            "--source-file-name",
            help="Source file name stored in source_data (default: object-key in full)",
        )
        parser.add_argument(
            "--target-date",
            required=True,
            help="Target date in YYYY-MM-DD (the day to extract from the year CSV)",
        )
        parser.add_argument(
            "--catalog",
            default="dlh_dev",
            help="Iceberg catalog name (default: dlh_dev)",
        )
        parser.add_argument(
            "--allow-duplicate-target-date",
            action="store_true",
            help="Allow append even if this target_date already has rows in bronze",
        )

    sda_tohoku_bronze_parser = subparsers.add_parser(
        "ingest-supply-demand-actuals-raw-to-bronze-tohoku",
        help="Ingest one target_date's rows from Tohoku's raw actuals CSV",
    )
    _add_sda_bronze_arguments(sda_tohoku_bronze_parser)

    sda_chugoku_bronze_parser = subparsers.add_parser(
        "ingest-supply-demand-actuals-raw-to-bronze-chugoku",
        help="Ingest one target_date's rows from Chugoku's raw actuals CSV",
    )
    _add_sda_bronze_arguments(sda_chugoku_bronze_parser)

    sda_shikoku_bronze_parser = subparsers.add_parser(
        "ingest-supply-demand-actuals-raw-to-bronze-shikoku",
        help="Ingest one target_date's rows from Shikoku's raw actuals CSV",
    )
    _add_sda_bronze_arguments(sda_shikoku_bronze_parser)

    supply_demand_actuals_silver_parser = subparsers.add_parser(
        "ingest-supply-demand-actuals-bronze-to-silver",
        help="Transform one company's supply_demand_actuals bronze table into silver",
    )
    supply_demand_actuals_silver_parser.add_argument(
        "--company",
        required=True,
        choices=sorted(SUPPLY_DEMAND_ACTUALS_SILVER_CONFIGS),
        help="Company code",
    )
    supply_demand_actuals_silver_parser.add_argument(
        "--catalog",
        default="dlh_dev",
        help="Iceberg catalog name (default: dlh_dev)",
    )
    supply_demand_actuals_silver_parser.add_argument(
        "--target-date",
        help="Limit the run to one target_date in YYYY-MM-DD"
        " (default: rebuild the full range staged from bronze)",
    )
    supply_demand_actuals_silver_parser.add_argument(
        "--from-date",
        help="Start of a target_date range in YYYY-MM-DD (overrides --target-date)",
    )
    supply_demand_actuals_silver_parser.add_argument(
        "--to-date",
        help="End of a target_date range in YYYY-MM-DD"
        " (default: same as --from-date/--target-date)",
    )

    power_usage_hokuriku_silver_parser = subparsers.add_parser(
        "ingest-power-usage-hokuriku-bronze-to-silver",
        help="Transform Hokuriku power_usage bronze tables into silver",
    )
    power_usage_hokuriku_silver_parser.add_argument(
        "--catalog",
        default="dlh_dev",
        help="Iceberg catalog name (default: dlh_dev)",
    )
    power_usage_hokuriku_silver_parser.add_argument(
        "--schema-dir",
        default="/workspace/configuration/iceberg/schema/silver",
        help="Directory containing the silver schema CSV files",
    )
    power_usage_hokuriku_silver_parser.add_argument(
        "--target-date",
        help="Limit the run to one target_date in YYYY-MM-DD"
        " (default: rebuild the full range staged from bronze)",
    )
    power_usage_hokuriku_silver_parser.add_argument(
        "--from-date",
        help="Start of a target_date range in YYYY-MM-DD (overrides --target-date)",
    )
    power_usage_hokuriku_silver_parser.add_argument(
        "--to-date",
        help="End of a target_date range in YYYY-MM-DD"
        " (default: same as --from-date/--target-date)",
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

    occto_orchestrator_parser = subparsers.add_parser(
        "run-occto-orchestrator",
        help="Run ADF-like OCCTO end-to-end orchestrator",
    )
    occto_orchestrator_parser.add_argument(
        "--bucket",
        default="jp-power-grid-dev",
        help="Source/target bucket name (default: jp-power-grid-dev)",
    )
    occto_orchestrator_parser.add_argument(
        "--target-date",
        help="Target date in YYYY-MM-DD (default: previous day in Asia/Tokyo)",
    )
    occto_orchestrator_parser.add_argument(
        "--to-date",
        help=(
            "End of target date range in YYYY-MM-DD for a multi-day fetch "
            "(default: same as --target-date, i.e. a single day)"
        ),
    )
    occto_orchestrator_parser.add_argument(
        "--catalog",
        default="dlh_dev",
        help="Iceberg catalog name (default: dlh_dev)",
    )
    occto_orchestrator_parser.add_argument(
        "--bronze-table",
        default="bronze.occto_unit_generation_actuals",
        help="Target bronze Iceberg table identifier",
    )
    occto_orchestrator_parser.add_argument(
        "--bronze-schema-path",
        default=(
            "/workspace/configuration/iceberg/schema/bronze/"
            "occto_unit_generation_actuals.csv"
        ),
        help="Bronze schema CSV path",
    )
    occto_orchestrator_parser.add_argument(
        "--allow-duplicate-source",
        action="store_true",
        help="Allow append even if source_data already exists",
    )
    occto_orchestrator_parser.add_argument(
        "--bronze-location",
        default=OCCTO_DEFAULT_BRONZE_LOCATION,
        help="Bronze table location scanned by the silver transform",
    )
    occto_orchestrator_parser.add_argument(
        "--silver-schema-dir",
        default=OCCTO_DEFAULT_SILVER_SCHEMA_DIR,
        help="Directory containing the silver schema CSV files",
    )
    occto_orchestrator_parser.add_argument(
        "--silver-from-date",
        help=(
            "Start of the silver step's target_date range in YYYY-MM-DD "
            "(default: the range that was just ingested)"
        ),
    )
    occto_orchestrator_parser.add_argument(
        "--silver-to-date",
        help="End of the silver step's target_date range in YYYY-MM-DD"
        " (default: same as --silver-from-date)",
    )
    occto_orchestrator_parser.add_argument(
        "--silver-all-dates",
        action="store_true",
        help="Rebuild every target_date in the silver step"
        " instead of just the range ingested",
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

    power_usage_hokuriku_scrape_parser = subparsers.add_parser(
        "scrape-power-usage-hokuriku",
        help="Scrape Hokuriku power_usage snapshot CSV and upload to RustFS raw layer",
    )
    power_usage_hokuriku_scrape_parser.add_argument(
        "--bucket",
        default="jp-power-grid-dev",
        help="Target bucket name (default: jp-power-grid-dev)",
    )
    power_usage_hokuriku_scrape_parser.add_argument(
        "--target-date",
        help="Target date in YYYY-MM-DD (default: previous day in Asia/Tokyo)",
    )
    power_usage_hokuriku_scrape_parser.add_argument(
        "--to-date",
        help=(
            "End of target date range in YYYY-MM-DD for a multi-day fetch "
            "(default: same as --target-date, i.e. a single day)"
        ),
    )

    sda_tohoku_scrape_parser = subparsers.add_parser(
        "scrape-supply-demand-actuals-tohoku",
        help="Download Tohoku's supply_demand_actuals year CSV to raw layer",
    )
    sda_tohoku_scrape_parser.add_argument(
        "--bucket",
        default="jp-power-grid-dev",
        help="Target bucket name (default: jp-power-grid-dev)",
    )
    sda_tohoku_scrape_parser.add_argument(
        "--target-date",
        help="Target date in YYYY-MM-DD, used only to resolve the year to fetch"
        " (default: previous day in Asia/Tokyo)",
    )

    sda_chugoku_scrape_parser = subparsers.add_parser(
        "scrape-supply-demand-actuals-chugoku",
        help="Download Chugoku's supply_demand_actuals year CSV to raw layer",
    )
    sda_chugoku_scrape_parser.add_argument(
        "--bucket",
        default="jp-power-grid-dev",
        help="Target bucket name (default: jp-power-grid-dev)",
    )
    sda_chugoku_scrape_parser.add_argument(
        "--target-date",
        help="Target date in YYYY-MM-DD, used only to resolve the year to fetch"
        " (default: previous day in Asia/Tokyo)",
    )

    sda_shikoku_scrape_parser = subparsers.add_parser(
        "scrape-supply-demand-actuals-shikoku",
        help="Download Shikoku's supply_demand_actuals year CSV to raw layer",
    )
    sda_shikoku_scrape_parser.add_argument(
        "--bucket",
        default="jp-power-grid-dev",
        help="Target bucket name (default: jp-power-grid-dev)",
    )
    sda_shikoku_scrape_parser.add_argument(
        "--target-date",
        help="Target date in YYYY-MM-DD, used only to resolve the year to fetch"
        " (default: previous day in Asia/Tokyo)",
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

    evolve_spec_parser = subparsers.add_parser(
        "evolve-silver-partition-spec",
        help=(
            "Add partition fields declared in schema CSVs to existing silver "
            "tables (metadata only; does not repartition existing data files, "
            "see docs/tasks/tasks.md section 8.1)"
        ),
    )
    evolve_spec_parser.add_argument(
        "--catalog",
        default="dlh_dev",
        help="Iceberg catalog name (default: dlh_dev)",
    )
    evolve_spec_parser.add_argument(
        "--schema-dir",
        default="/workspace/configuration/iceberg/schema/silver",
        help="Directory containing silver schema CSV files",
    )

    expire_snapshots_parser = subparsers.add_parser(
        "expire-silver-snapshots",
        help=(
            "Expire silver table snapshots older than a cutoff and report/"
            "delete the data files that become orphaned as a result "
            "(see docs/tasks/tasks.md section 7)"
        ),
    )
    expire_snapshots_parser.add_argument(
        "--catalog",
        default="dlh_dev",
        help="Iceberg catalog name (default: dlh_dev)",
    )
    expire_snapshots_parser.add_argument(
        "--schema-dir",
        default="/workspace/configuration/iceberg/schema/silver",
        help="Directory containing silver schema CSV files",
    )
    expire_snapshots_parser.add_argument(
        "--bucket",
        default="jp-power-grid-dev",
        help="Bucket holding the table data files (default: jp-power-grid-dev)",
    )
    expire_snapshots_parser.add_argument(
        "--older-than-days",
        type=int,
        default=7,
        help="Expire snapshots older than this many days (default: 7)",
    )
    expire_snapshots_parser.add_argument(
        "--delete",
        action="store_true",
        help=(
            "Actually delete orphaned data files. Without this flag, orphans "
            "are only found and reported (dry run); snapshot expiration "
            "itself still runs either way."
        ),
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

    if args.command == "ingest-power-usage-hokuriku-raw-to-bronze":
        try:
            target_date = date.fromisoformat(args.target_date)
        except ValueError as exc:
            parser.error(f"Invalid --target-date value: {args.target_date} ({exc})")

        rustfs = RustFSClient()
        row_counts = ingest_power_usage_hokuriku(
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

    if args.command == "scrape-occto":
        if args.target_date:
            try:
                from_date = date.fromisoformat(args.target_date)
            except ValueError as exc:
                parser.error(f"Invalid --target-date value: {args.target_date} ({exc})")
        else:
            from_date = resolve_default_target_date(datetime.now(UTC))

        to_date = None
        if args.to_date:
            try:
                to_date = date.fromisoformat(args.to_date)
            except ValueError as exc:
                parser.error(f"Invalid --to-date value: {args.to_date} ({exc})")

        effective_to = to_date or from_date
        rustfs = RustFSClient()
        scraper = OCCTOUnitGenerationScraper()
        try:
            current = from_date
            while current <= effective_to:
                result = scrape_occto_unit_generation_raw(
                    storage_client=rustfs,
                    scraper=scraper,
                    bucket_name=args.bucket,
                    from_date=current,
                    to_date=current,
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
                current += timedelta(days=1)
        finally:
            scraper.close()

    if args.command == "scrape-power-usage-hokuriku":
        if args.target_date:
            try:
                from_date = date.fromisoformat(args.target_date)
            except ValueError as exc:
                parser.error(f"Invalid --target-date value: {args.target_date} ({exc})")
        else:
            from_date = resolve_default_power_usage_hokuriku_target_date(
                datetime.now(UTC)
            )

        to_date = None
        if args.to_date:
            try:
                to_date = date.fromisoformat(args.to_date)
            except ValueError as exc:
                parser.error(f"Invalid --to-date value: {args.to_date} ({exc})")

        effective_to = to_date or from_date
        rustfs = RustFSClient()
        scraper = HokurikuPowerUsageScraper()
        try:
            current = from_date
            while current <= effective_to:
                result = scrape_power_usage_hokuriku_raw(
                    storage_client=rustfs,
                    scraper=scraper,
                    bucket_name=args.bucket,
                    target_date=current,
                )
                if result.skipped:
                    logger.info(
                        "Hokuriku power_usage scrape skipped (no change): "
                        "target_date=%s, sha256=%.8s",
                        result.target_date,
                        result.sha256,
                    )
                else:
                    logger.info(
                        "Hokuriku power_usage snapshot saved: "
                        "target_date=%s, sha256=%.8s, prefix=%s",
                        result.target_date,
                        result.sha256,
                        result.snapshot_prefix,
                    )
                current += timedelta(days=1)
        finally:
            scraper.close()

    if args.command == "scrape-supply-demand-actuals-tohoku":
        if args.target_date:
            try:
                target_date = date.fromisoformat(args.target_date)
            except ValueError as exc:
                parser.error(f"Invalid --target-date value: {args.target_date} ({exc})")
        else:
            target_date = resolve_default_sda_tohoku_target_date(datetime.now(UTC))

        rustfs = RustFSClient()
        scraper = TohokuSupplyDemandActualsScraper()
        try:
            result = scrape_supply_demand_actuals_tohoku_raw(
                storage_client=rustfs,
                scraper=scraper,
                bucket_name=args.bucket,
                year=target_date.year,
            )
            if result.skipped:
                logger.info(
                    "Tohoku supply_demand_actuals scrape skipped (no change): "
                    "year=%s, sha256=%.8s",
                    result.year,
                    result.sha256,
                )
            else:
                logger.info(
                    "Tohoku supply_demand_actuals snapshot saved: "
                    "year=%s, sha256=%.8s, prefix=%s",
                    result.year,
                    result.sha256,
                    result.snapshot_prefix,
                )
        finally:
            scraper.close()

    if args.command == "scrape-supply-demand-actuals-chugoku":
        if args.target_date:
            try:
                target_date = date.fromisoformat(args.target_date)
            except ValueError as exc:
                parser.error(f"Invalid --target-date value: {args.target_date} ({exc})")
        else:
            target_date = resolve_default_sda_chugoku_target_date(datetime.now(UTC))

        rustfs = RustFSClient()
        scraper = ChugokuSupplyDemandActualsScraper()
        try:
            result = scrape_supply_demand_actuals_chugoku_raw(
                storage_client=rustfs,
                scraper=scraper,
                bucket_name=args.bucket,
                year=target_date.year,
            )
            if result.skipped:
                logger.info(
                    "Chugoku supply_demand_actuals scrape skipped (no change): "
                    "year=%s, sha256=%.8s",
                    result.year,
                    result.sha256,
                )
            else:
                logger.info(
                    "Chugoku supply_demand_actuals snapshot saved: "
                    "year=%s, sha256=%.8s, prefix=%s",
                    result.year,
                    result.sha256,
                    result.snapshot_prefix,
                )
        finally:
            scraper.close()

    if args.command == "scrape-supply-demand-actuals-shikoku":
        if args.target_date:
            try:
                target_date = date.fromisoformat(args.target_date)
            except ValueError as exc:
                parser.error(f"Invalid --target-date value: {args.target_date} ({exc})")
        else:
            target_date = resolve_default_sda_shikoku_target_date(datetime.now(UTC))

        rustfs = RustFSClient()
        scraper = ShikokuSupplyDemandActualsScraper()
        try:
            result = scrape_supply_demand_actuals_shikoku_raw(
                storage_client=rustfs,
                scraper=scraper,
                bucket_name=args.bucket,
                year=target_date.year,
            )
            if result.skipped:
                logger.info(
                    "Shikoku supply_demand_actuals scrape skipped (no change): "
                    "year=%s, sha256=%.8s",
                    result.year,
                    result.sha256,
                )
            else:
                logger.info(
                    "Shikoku supply_demand_actuals snapshot saved: "
                    "year=%s, sha256=%.8s, prefix=%s",
                    result.year,
                    result.sha256,
                    result.snapshot_prefix,
                )
        finally:
            scraper.close()

    if args.command == "ingest-supply-demand-actuals-raw-to-bronze-tohoku":
        try:
            target_date = date.fromisoformat(args.target_date)
        except ValueError as exc:
            parser.error(f"Invalid --target-date value: {args.target_date} ({exc})")

        rustfs = RustFSClient()
        row_count = ingest_supply_demand_actuals_tohoku(
            client=rustfs,
            bucket_name=args.bucket,
            object_key=args.object_key,
            target_date=target_date,
            source_file_name=args.source_file_name,
            catalog_name=args.catalog,
            skip_if_exists=not args.allow_duplicate_target_date,
        )
        logger.info(
            "Ingestion completed: company=tohoku, target_date=%s, rows=%s",
            target_date,
            row_count,
        )

    if args.command == "ingest-supply-demand-actuals-raw-to-bronze-chugoku":
        try:
            target_date = date.fromisoformat(args.target_date)
        except ValueError as exc:
            parser.error(f"Invalid --target-date value: {args.target_date} ({exc})")

        rustfs = RustFSClient()
        row_count = ingest_supply_demand_actuals_chugoku(
            client=rustfs,
            bucket_name=args.bucket,
            object_key=args.object_key,
            target_date=target_date,
            source_file_name=args.source_file_name,
            catalog_name=args.catalog,
            skip_if_exists=not args.allow_duplicate_target_date,
        )
        logger.info(
            "Ingestion completed: company=chugoku, target_date=%s, rows=%s",
            target_date,
            row_count,
        )

    if args.command == "ingest-supply-demand-actuals-raw-to-bronze-shikoku":
        try:
            target_date = date.fromisoformat(args.target_date)
        except ValueError as exc:
            parser.error(f"Invalid --target-date value: {args.target_date} ({exc})")

        rustfs = RustFSClient()
        row_count = ingest_supply_demand_actuals_shikoku(
            client=rustfs,
            bucket_name=args.bucket,
            object_key=args.object_key,
            target_date=target_date,
            source_file_name=args.source_file_name,
            catalog_name=args.catalog,
            skip_if_exists=not args.allow_duplicate_target_date,
        )
        logger.info(
            "Ingestion completed: company=shikoku, target_date=%s, rows=%s",
            target_date,
            row_count,
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

    if args.command == "evolve-silver-partition-spec":
        schema_dir = Path(args.schema_dir)
        if not schema_dir.exists():
            parser.error(f"Schema directory does not exist: {schema_dir}")

        schema_files = sorted(schema_dir.glob("*.csv"))
        if not schema_files:
            parser.error(f"No schema CSV files found in: {schema_dir}")

        catalog = get_catalog(args.catalog)
        evolved = 0
        for schema_file in schema_files:
            table_identifier = f"silver.{schema_file.stem}"
            added = evolve_partition_spec(catalog, table_identifier, str(schema_file))
            if added:
                evolved += 1

        logger.info("Evolved partition spec for %s silver tables", evolved)

    if args.command == "expire-silver-snapshots":
        schema_dir = Path(args.schema_dir)
        if not schema_dir.exists():
            parser.error(f"Schema directory does not exist: {schema_dir}")

        schema_files = sorted(schema_dir.glob("*.csv"))
        if not schema_files:
            parser.error(f"No schema CSV files found in: {schema_dir}")

        catalog = get_catalog(args.catalog)
        rustfs = RustFSClient()
        cutoff = datetime.now(UTC) - timedelta(days=args.older_than_days)

        total_expired = 0
        total_orphans = 0
        total_deleted = 0
        for schema_file in schema_files:
            table_identifier = f"silver.{schema_file.stem}"
            expired = expire_old_snapshots(catalog, table_identifier, cutoff)
            total_expired += len(expired)

            orphans = find_orphan_data_files(
                catalog, table_identifier, rustfs, args.bucket
            )
            total_orphans += len(orphans)
            if not orphans:
                continue

            if args.delete:
                deleted = delete_orphan_data_files(rustfs, args.bucket, orphans)
                total_deleted += deleted
            else:
                logger.info(
                    "DRY RUN: %s orphaned file(s) for '%s' would be deleted "
                    "(pass --delete to actually remove them)",
                    len(orphans),
                    table_identifier,
                )

        logger.info(
            "expire-silver-snapshots done: expired=%s, orphaned=%s, deleted=%s",
            total_expired,
            total_orphans,
            total_deleted,
        )

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

    if args.command == "ingest-power-usage-hokuriku-bronze-to-silver":
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

        power_usage_hokuriku_silver_result = run_bronze_to_silver_power_usage_hokuriku(
            catalog_name=args.catalog,
            schema_dir=args.schema_dir,
            from_date=from_date,
            to_date=to_date,
        )
        logger.info(
            "Hokuriku power_usage bronze-to-silver completed: execution_id=%s",
            power_usage_hokuriku_silver_result.execution_id,
        )
        for silver_write in power_usage_hokuriku_silver_result.writes.values():
            logger.info(
                " - table=%s, written=%s",
                silver_write.table_identifier,
                silver_write.rows_written,
            )

    if args.command == "ingest-supply-demand-actuals-bronze-to-silver":
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

        supply_demand_actuals_silver_result = (
            run_bronze_to_silver_supply_demand_actuals(
                company=args.company,
                catalog_name=args.catalog,
                from_date=from_date,
                to_date=to_date,
            )
        )
        logger.info(
            "supply_demand_actuals[%s] bronze-to-silver completed: "
            "execution_id=%s, table=%s, written=%s",
            args.company,
            supply_demand_actuals_silver_result.execution_id,
            supply_demand_actuals_silver_result.write.table_identifier,
            supply_demand_actuals_silver_result.write.rows_written,
        )

    if args.command == "run-occto-orchestrator":
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
        for result in results:
            logger.info(
                "Orchestrator step result: step=%s, status=%s, detail=%s",
                result.name,
                result.status,
                result.detail,
            )


if __name__ == "__main__":
    main()

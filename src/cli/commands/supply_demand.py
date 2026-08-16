"""supply_demand_actuals CLI commands for Tohoku/Chugoku/Shikoku.

Table-driven: each company only differs in its scraper class and the raw/
bronze/silver functions it calls, so the 9 commands (3 companies x 3 layers)
are generated from one SdaCompany row each instead of copy-pasted per
company. See docs/tasks/refactaring_20260817.md section 2.5.
"""

from __future__ import annotations

import argparse
import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Any

from cli.args import add_catalog_arg, add_date_range_args
from cli.dates import parse_iso_date, parse_optional_date_range
from cli.defaults import DEFAULT_BUCKET
from cli.registry import CommandSpec
from common.storage_client import RustFSClient
from common.utilities import resolve_default_target_date
from pipeline.bronze.source_to_bronze_supply_demand_actuals_chugoku import (
    ingest_supply_demand_actuals_chugoku,
)
from pipeline.bronze.source_to_bronze_supply_demand_actuals_shikoku import (
    ingest_supply_demand_actuals_shikoku,
)
from pipeline.bronze.source_to_bronze_supply_demand_actuals_tohoku import (
    ingest_supply_demand_actuals_tohoku,
)
from pipeline.raw.source_to_raw_supply_demand_actuals_chugoku import (
    ChugokuSupplyDemandActualsScraper,
    scrape_supply_demand_actuals_chugoku_raw,
)
from pipeline.raw.source_to_raw_supply_demand_actuals_shikoku import (
    ShikokuSupplyDemandActualsScraper,
    scrape_supply_demand_actuals_shikoku_raw,
)
from pipeline.raw.source_to_raw_supply_demand_actuals_tohoku import (
    TohokuSupplyDemandActualsScraper,
    scrape_supply_demand_actuals_tohoku_raw,
)
from pipeline.silver.bronze_to_silver_supply_demand_actuals_chugoku import (
    run_bronze_to_silver_supply_demand_actuals_chugoku,
)
from pipeline.silver.bronze_to_silver_supply_demand_actuals_shikoku import (
    run_bronze_to_silver_supply_demand_actuals_shikoku,
)
from pipeline.silver.bronze_to_silver_supply_demand_actuals_tohoku import (
    run_bronze_to_silver_supply_demand_actuals_tohoku,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SdaCompany:
    key: str  # lowercase, used in command names and the company= log field
    label: str  # capitalized, used in scrape log messages
    scraper_cls: Callable[[], Any]
    scrape: Callable[..., Any]
    ingest: Callable[..., int]
    to_silver: Callable[..., Any]


SDA_COMPANIES = (
    SdaCompany(
        key="tohoku",
        label="Tohoku",
        scraper_cls=TohokuSupplyDemandActualsScraper,
        scrape=scrape_supply_demand_actuals_tohoku_raw,
        ingest=ingest_supply_demand_actuals_tohoku,
        to_silver=run_bronze_to_silver_supply_demand_actuals_tohoku,
    ),
    SdaCompany(
        key="chugoku",
        label="Chugoku",
        scraper_cls=ChugokuSupplyDemandActualsScraper,
        scrape=scrape_supply_demand_actuals_chugoku_raw,
        ingest=ingest_supply_demand_actuals_chugoku,
        to_silver=run_bronze_to_silver_supply_demand_actuals_chugoku,
    ),
    SdaCompany(
        key="shikoku",
        label="Shikoku",
        scraper_cls=ShikokuSupplyDemandActualsScraper,
        scrape=scrape_supply_demand_actuals_shikoku_raw,
        ingest=ingest_supply_demand_actuals_shikoku,
        to_silver=run_bronze_to_silver_supply_demand_actuals_shikoku,
    ),
)


def _configure_scrape(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--bucket",
        default=DEFAULT_BUCKET,
        help=f"Target bucket name (default: {DEFAULT_BUCKET})",
    )
    parser.add_argument(
        "--target-date",
        help="Target date in YYYY-MM-DD, used only to resolve the year to fetch"
        " (default: previous day in Asia/Tokyo)",
    )


def _make_scrape_handler(
    company: SdaCompany,
) -> Callable[[argparse.Namespace, argparse.ArgumentParser], None]:
    def _handle(args: argparse.Namespace, parser: argparse.ArgumentParser) -> None:
        target_date = (
            parse_iso_date(parser, args.target_date, "--target-date")
            if args.target_date
            else resolve_default_target_date(datetime.now(UTC))
        )

        rustfs = RustFSClient()
        scraper = company.scraper_cls()
        try:
            result = company.scrape(
                storage_client=rustfs,
                scraper=scraper,
                bucket_name=args.bucket,
                year=target_date.year,
            )
            if result.skipped:
                logger.info(
                    "%s supply_demand_actuals scrape skipped (no change): "
                    "year=%s, sha256=%.8s",
                    company.label,
                    result.year,
                    result.sha256,
                )
            else:
                logger.info(
                    "%s supply_demand_actuals snapshot saved: "
                    "year=%s, sha256=%.8s, prefix=%s",
                    company.label,
                    result.year,
                    result.sha256,
                    result.snapshot_prefix,
                )
        finally:
            scraper.close()

    return _handle


def _configure_bronze(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--bucket",
        default=DEFAULT_BUCKET,
        help=f"Source bucket name (default: {DEFAULT_BUCKET})",
    )
    parser.add_argument(
        "--object-key",
        required=True,
        # NOTE: "tohoku" is a fixed example inherited from the original,
        # shared _add_sda_bronze_arguments() closure -- it never varied by
        # company even for chugoku/shikoku, and is kept as-is here to match.
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
    add_catalog_arg(parser)
    parser.add_argument(
        "--allow-duplicate-target-date",
        action="store_true",
        help="Allow append even if this target_date already has rows in bronze",
    )


def _make_bronze_handler(
    company: SdaCompany,
) -> Callable[[argparse.Namespace, argparse.ArgumentParser], None]:
    def _handle(args: argparse.Namespace, parser: argparse.ArgumentParser) -> None:
        target_date: date = parse_iso_date(parser, args.target_date, "--target-date")

        rustfs = RustFSClient()
        row_count = company.ingest(
            client=rustfs,
            bucket_name=args.bucket,
            object_key=args.object_key,
            target_date=target_date,
            source_file_name=args.source_file_name,
            catalog_name=args.catalog,
            skip_if_exists=not args.allow_duplicate_target_date,
        )
        logger.info(
            "Ingestion completed: company=%s, target_date=%s, rows=%s",
            company.key,
            target_date,
            row_count,
        )

    return _handle


def _configure_silver(parser: argparse.ArgumentParser) -> None:
    add_catalog_arg(parser)
    add_date_range_args(parser)


def _make_silver_handler(
    company: SdaCompany,
) -> Callable[[argparse.Namespace, argparse.ArgumentParser], None]:
    def _handle(args: argparse.Namespace, parser: argparse.ArgumentParser) -> None:
        from_date, to_date = parse_optional_date_range(parser, args)
        result = company.to_silver(
            catalog_name=args.catalog,
            from_date=from_date,
            to_date=to_date,
        )
        logger.info(
            "supply_demand_actuals[%s] bronze-to-silver completed: "
            "execution_id=%s, table=%s, written=%s",
            company.key,
            result.execution_id,
            result.write.table_identifier,
            result.write.rows_written,
        )

    return _handle


def _build_commands() -> list[CommandSpec]:
    commands: list[CommandSpec] = []
    for company in SDA_COMPANIES:
        commands.append(
            CommandSpec(
                name=f"scrape-supply-demand-actuals-{company.key}",
                help=f"Download {company.label}'s supply_demand_actuals year CSV"
                " to raw layer",
                configure=_configure_scrape,
                handler=_make_scrape_handler(company),
            )
        )
        commands.append(
            CommandSpec(
                name=f"ingest-supply-demand-actuals-raw-to-bronze-{company.key}",
                help=f"Ingest one target_date's rows from {company.label}'s raw"
                " actuals CSV",
                configure=_configure_bronze,
                handler=_make_bronze_handler(company),
            )
        )
        commands.append(
            CommandSpec(
                name=f"ingest-supply-demand-actuals-bronze-to-silver-{company.key}",
                help=f"Transform {company.label}'s supply_demand_actuals bronze"
                " table into silver",
                configure=_configure_silver,
                handler=_make_silver_handler(company),
            )
        )
    return commands


COMMANDS = _build_commands()

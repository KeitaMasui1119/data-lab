"""Silver table admin commands: provision, evolve partition spec, expire
snapshots. All three share the "schema CSV file name == table name"
convention, centralized here in iter_silver_schema_files(). See
docs/tasks/refactaring_20260817.md section 2.6.
"""

from __future__ import annotations

import argparse
import logging
from datetime import UTC, datetime, timedelta
from pathlib import Path

from cli.args import add_catalog_arg
from cli.defaults import DEFAULT_BUCKET, SILVER_SCHEMA_ROOT
from cli.registry import CommandSpec
from common.iceberg.catalog import evolve_partition_spec, get_catalog, provision_table
from common.iceberg.maintenance import (
    delete_orphan_data_files,
    expire_old_snapshots,
    find_orphan_data_files,
)
from common.storage_client import RustFSClient

logger = logging.getLogger(__name__)


def iter_silver_schema_files(
    parser: argparse.ArgumentParser, schema_dir: str
) -> list[tuple[str, Path]]:
    """Resolve (table_identifier, schema_csv_path) pairs from a silver schema dir."""
    directory = Path(schema_dir)
    if not directory.exists():
        parser.error(f"Schema directory does not exist: {directory}")

    schema_files = sorted(directory.rglob("*.csv"))
    if not schema_files:
        parser.error(f"No schema CSV files found in: {directory}")

    return [(f"silver.{schema_file.stem}", schema_file) for schema_file in schema_files]


def _configure_schema_dir_command(parser: argparse.ArgumentParser) -> None:
    add_catalog_arg(parser)
    parser.add_argument(
        "--schema-dir",
        default=str(SILVER_SCHEMA_ROOT),
        help="Directory containing silver schema CSV files",
    )


def _handle_provision(
    args: argparse.Namespace, parser: argparse.ArgumentParser
) -> None:
    catalog = get_catalog(args.catalog)
    provisioned = 0
    for table_identifier, schema_file in iter_silver_schema_files(
        parser, args.schema_dir
    ):
        provision_table(catalog, table_identifier, str(schema_file))
        provisioned += 1

    logger.info("Provisioned silver tables: %s", provisioned)


def _handle_evolve(args: argparse.Namespace, parser: argparse.ArgumentParser) -> None:
    catalog = get_catalog(args.catalog)
    evolved = 0
    for table_identifier, schema_file in iter_silver_schema_files(
        parser, args.schema_dir
    ):
        added = evolve_partition_spec(catalog, table_identifier, str(schema_file))
        if added:
            evolved += 1

    logger.info("Evolved partition spec for %s silver tables", evolved)


def _configure_expire(parser: argparse.ArgumentParser) -> None:
    add_catalog_arg(parser)
    parser.add_argument(
        "--schema-dir",
        default=str(SILVER_SCHEMA_ROOT),
        help="Directory containing silver schema CSV files",
    )
    parser.add_argument(
        "--bucket",
        default=DEFAULT_BUCKET,
        help=f"Bucket holding the table data files (default: {DEFAULT_BUCKET})",
    )
    parser.add_argument(
        "--older-than-days",
        type=int,
        default=7,
        help="Expire snapshots older than this many days (default: 7)",
    )
    parser.add_argument(
        "--delete",
        action="store_true",
        help=(
            "Actually delete orphaned data files. Without this flag, orphans "
            "are only found and reported (dry run); snapshot expiration "
            "itself still runs either way."
        ),
    )


def _handle_expire(args: argparse.Namespace, parser: argparse.ArgumentParser) -> None:
    catalog = get_catalog(args.catalog)
    rustfs = RustFSClient()
    cutoff = datetime.now(UTC) - timedelta(days=args.older_than_days)

    total_expired = 0
    total_orphans = 0
    total_deleted = 0
    for table_identifier, _schema_file in iter_silver_schema_files(
        parser, args.schema_dir
    ):
        expired = expire_old_snapshots(catalog, table_identifier, cutoff)
        total_expired += len(expired)

        orphans = find_orphan_data_files(catalog, table_identifier, rustfs, args.bucket)
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


COMMANDS = [
    CommandSpec(
        name="provision-silver-tables",
        help="Provision silver Iceberg tables from schema CSV files",
        configure=_configure_schema_dir_command,
        handler=_handle_provision,
    ),
    CommandSpec(
        name="evolve-silver-partition-spec",
        help=(
            "Add partition fields declared in schema CSVs to existing silver "
            "tables (metadata only; does not repartition existing data files, "
            "see docs/tasks/tasks.md section 8.1)"
        ),
        configure=_configure_schema_dir_command,
        handler=_handle_evolve,
    ),
    CommandSpec(
        name="expire-silver-snapshots",
        help=(
            "Expire silver table snapshots older than a cutoff and report/"
            "delete the data files that become orphaned as a result "
            "(see docs/tasks/tasks.md section 7)"
        ),
        configure=_configure_expire,
        handler=_handle_expire,
    ),
]

"""Metadata namespace admin: provision the tables that record what ran.

`metadata.pipeline_run_log` holds one row per orchestrated pipeline step and
is what makes a run auditable after the fact. It is provisioned separately
from the data layers because it has no bronze/silver/gold lineage of its own
-- it describes the runs that build them.
"""

from __future__ import annotations

import argparse
import logging

from cli.args import add_catalog_arg
from cli.defaults import METADATA_SCHEMA_ROOT
from cli.registry import CommandSpec
from cli.schema_files import iter_schema_files
from common.iceberg.catalog import get_catalog, provision_table

logger = logging.getLogger(__name__)


def _configure(parser: argparse.ArgumentParser) -> None:
    add_catalog_arg(parser)
    parser.add_argument(
        "--schema-dir",
        default=str(METADATA_SCHEMA_ROOT),
        help="Directory containing metadata schema CSV files",
    )


def _handle_provision(
    args: argparse.Namespace, parser: argparse.ArgumentParser
) -> None:
    catalog = get_catalog(args.catalog)
    provisioned = 0
    for table_identifier, schema_file in iter_schema_files(
        parser, args.schema_dir, namespace="metadata"
    ):
        provision_table(catalog, table_identifier, str(schema_file))
        provisioned += 1

    logger.info("Provisioned metadata tables: %s", provisioned)


COMMANDS = [
    CommandSpec(
        name="provision-metadata-tables",
        help=(
            "Provision metadata Iceberg tables from schema CSV files "
            "(metadata.pipeline_run_log records every orchestrated run)"
        ),
        configure=_configure,
        handler=_handle_provision,
    ),
]

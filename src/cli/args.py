"""Shared argparse.add_argument() helpers for the CLI commands.

Consolidates add_argument() calls that repeat identical help text across
multiple commands. See docs/tasks/refactaring_20260817.md sections 2.4, 2.7.
"""

from __future__ import annotations

import argparse

from cli.defaults import DEFAULT_CATALOG


def add_catalog_arg(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--catalog",
        default=DEFAULT_CATALOG,
        help=f"Iceberg catalog name (default: {DEFAULT_CATALOG})",
    )


def add_date_range_args(parser: argparse.ArgumentParser) -> None:
    """--target-date / --from-date / --to-date trio for bronze-to-silver commands."""
    parser.add_argument(
        "--target-date",
        help="Limit the run to one target_date in YYYY-MM-DD"
        " (default: rebuild the full range staged from bronze)",
    )
    parser.add_argument(
        "--from-date",
        help="Start of a target_date range in YYYY-MM-DD (overrides --target-date)",
    )
    parser.add_argument(
        "--to-date",
        help="End of a target_date range in YYYY-MM-DD"
        " (default: same as --from-date/--target-date)",
    )


def add_target_date_and_to_date_args(parser: argparse.ArgumentParser) -> None:
    """--target-date / --to-date pair for multi-day scrape/orchestrator commands."""
    parser.add_argument(
        "--target-date",
        help="Target date in YYYY-MM-DD (default: previous day in Asia/Tokyo)",
    )
    parser.add_argument(
        "--to-date",
        help=(
            "End of target date range in YYYY-MM-DD for a multi-day fetch "
            "(default: same as --target-date, i.e. a single day)"
        ),
    )

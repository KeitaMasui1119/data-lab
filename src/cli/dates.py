"""Shared --target-date/--from-date/--to-date parsing helpers for the CLI.

Consolidates the date.fromisoformat() + parser.error() pattern that used to
be repeated 21 times, and the --from-date/--to-date/--target-date range
resolution repeated 3 times, across src/main.py. See
docs/tasks/refactaring_20260817.md section 2.4.
"""

from __future__ import annotations

import argparse
from datetime import date


def parse_iso_date(parser: argparse.ArgumentParser, value: str, flag: str) -> date:
    """Parse an ISO date string, exiting via parser.error() on failure."""
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        parser.error(f"Invalid {flag} value: {value} ({exc})")


def parse_optional_date_range(
    parser: argparse.ArgumentParser, args: argparse.Namespace
) -> tuple[date | None, date | None]:
    """Resolve a --from-date/--to-date/--target-date trio into (from, to).

    Shared by every bronze-to-silver command that accepts either a single
    --target-date or a --from-date/--to-date range (default: rebuild
    everything staged from bronze, signaled by (None, None)).
    """
    if args.from_date:
        from_date = parse_iso_date(parser, args.from_date, "--from-date")
        to_date = (
            parse_iso_date(parser, args.to_date, "--to-date")
            if args.to_date
            else from_date
        )
        return from_date, to_date
    if args.target_date:
        target_date = parse_iso_date(parser, args.target_date, "--target-date")
        return target_date, target_date
    return None, None

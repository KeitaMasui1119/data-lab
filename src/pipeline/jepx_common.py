"""Shared helpers for JEPX spot summary pipeline components.

resolve_target_at() moved to common/utilities.py: it had no JEPX-specific
logic (just "now, or from an optional ms timestamp"), unlike everything
below, which is JEPX's fiscal-year/file-naming convention specifically.
"""

from __future__ import annotations

from datetime import datetime


def resolve_fiscal_year(target_at: datetime) -> int:
    """Return fiscal year for the target datetime (FY starts in April)."""
    return target_at.year if target_at.month >= 4 else target_at.year - 1


def resolve_spot_summary_file_name(target_at: datetime) -> str:
    """Build JEPX spot summary file name from target datetime."""
    fiscal_year = resolve_fiscal_year(target_at)
    return f"spot_summary_{fiscal_year}.csv"


def resolve_spot_summary_object_key(target_at: datetime) -> str:
    """Build raw-layer object key for JEPX spot summary file."""
    file_name = resolve_spot_summary_file_name(target_at)
    return f"raw/jepx/spot_summary/{file_name}"

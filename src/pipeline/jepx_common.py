"""Shared helpers for JEPX spot summary pipeline components."""

from __future__ import annotations

from datetime import UTC, datetime


def resolve_target_at(timestamp_ms: int | None) -> datetime:
    """Resolve target datetime from optional UNIX timestamp milliseconds."""
    if timestamp_ms is None:
        return datetime.now(UTC)
    return datetime.fromtimestamp(timestamp_ms / 1000, tz=UTC)


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

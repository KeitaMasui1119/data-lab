"""Shared "loop over a date range, scrape one day at a time" helper.

Consolidates the try/finally + skipped/saved logging shape duplicated
between scrape-occto and scrape-power-usage-hokuriku. See
docs/tasks/refactaring_20260817.md section 2.5.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import date, timedelta
from typing import Any

logger = logging.getLogger(__name__)


def run_daily_scrape_loop(
    *,
    from_date: date,
    to_date: date | None,
    scrape_one_day: Callable[[date], Any],
    date_of: Callable[[Any], date],
    close: Callable[[], None],
    label: str,
) -> None:
    """Scrape each day in [from_date, to_date] (default: just from_date).

    date_of() extracts the result's own date field for logging, since the
    underlying scrape result types don't share a common attribute name
    (OCCTO's is from_date, Hokuriku's is target_date).
    """
    effective_to = to_date or from_date
    try:
        current = from_date
        while current <= effective_to:
            result = scrape_one_day(current)
            if result.skipped:
                logger.info(
                    "%s scrape skipped (no change): target_date=%s, sha256=%.8s",
                    label,
                    date_of(result),
                    result.sha256,
                )
            else:
                logger.info(
                    "%s snapshot saved: target_date=%s, sha256=%.8s, prefix=%s",
                    label,
                    date_of(result),
                    result.sha256,
                    result.snapshot_prefix,
                )
            current += timedelta(days=1)
    finally:
        close()

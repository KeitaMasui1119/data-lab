"""Cross-cutting process primitives: the clock, run ids, and logging setup.

Deliberately narrow. This is the module every layer may import, so it holds
only things with no domain of their own -- what time is it, what id does this
run get, how does logging print. Anything that knows about a dataset, a
table, a dataframe library or object storage belongs in a module named after
that thing (polars_utils.py, duckdb_utils.py, raw_ingestion_log.py, ...),
not here.
"""

from __future__ import annotations

import logging
import uuid
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

# Set timezone information.
UTC = ZoneInfo("UTC")

DEFAULT_LOG_FORMAT = "%(asctime)s | %(levelname)s | %(message)s"


def configure_logging(level: int = logging.INFO) -> None:
    """Configure root logging once with the project default format."""
    logging.basicConfig(level=level, format=DEFAULT_LOG_FORMAT)


def get_logger(name: str) -> logging.Logger:
    """Return a logger with the given module name."""
    return logging.getLogger(name)


def get_now_utc() -> datetime:
    return datetime.now(UTC)


def resolve_target_at(timestamp_ms: int | None) -> datetime:
    """Resolve target datetime from optional UNIX timestamp milliseconds."""
    if timestamp_ms is None:
        return get_now_utc()
    return datetime.fromtimestamp(timestamp_ms / 1000, tz=UTC)


def resolve_default_target_date(now: datetime) -> date:
    """Default target date: yesterday in JST.

    Shared by every denki-yohou dataset (OCCTO, power_usage_hokuriku,
    supply_demand_actuals): each source's latest published row is
    consistently yesterday's (today has not fully elapsed / been
    finalized and published yet), confirmed live per dataset.
    """
    jst_now = now.astimezone(ZoneInfo("Asia/Tokyo"))
    return (jst_now - timedelta(days=1)).date()


def gen_uuid() -> str:
    return str(uuid.uuid4())

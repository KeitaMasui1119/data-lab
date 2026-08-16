import uuid
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

# Set timezone information.
UTC = ZoneInfo("UTC")


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

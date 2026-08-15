import uuid
from datetime import datetime
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


def gen_uuid() -> str:
    return str(uuid.uuid4())

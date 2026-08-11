import uuid
from datetime import datetime
from zoneinfo import ZoneInfo

# Set timezone information.
UTC = ZoneInfo("UTC")


def get_now_utc() -> datetime:
    return datetime.now(UTC)


def gen_uuid() -> str:
    return str(uuid.uuid4())

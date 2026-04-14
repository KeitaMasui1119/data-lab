import uuid
from datetime import datetime
from zoneinfo import ZoneInfo

# Set timezone information.
UTC = ZoneInfo("UTC")
JST = ZoneInfo("Asia/Tokyo")


def get_now_utc() -> datetime:
    return datetime.now(UTC)


def get_now_jst() -> datetime:
    return datetime.now(JST)


def str_now_utc(fmt=None) -> str:
    """
    Return the current UTC datetime as a formatted string.

    If no format string is provided, the default format "%Y-%m-%dT%H:%M:%S%z"is used.
    If a format string is provided, it is passed to strftime().

    Args:
        fmt (str, optional): A strftime-compatible format string. Defaults to None.
        If None, a default ISO-like format is used.

    Raises:
        ValueError: If the provided format string is invalid.

    Returns:
        str: The current UTC datetime formatted as a string.
    """
    if fmt is None:
        fmt = "%Y-%m-%dT%H:%M:%S%z"
        return get_now_utc().strftime(fmt)
    elif fmt is not None:
        try:
            return get_now_utc().strftime(fmt)
        except Exception as e:
            raise ValueError(f"Invalid format string: {fmt}") from e
    return get_now_utc().isoformat()


def str_now_jst(fmt=None) -> str:
    """
    Return the current JST datetime as a formatted string.

    If no format string is provided, the default format "%Y-%m-%dT%H:%M:%S%z"is used.
    If a format string is provided, it is passed to strftime().

    Args:
        fmt (str, optional): A strftime-compatible format string. Defaults to None.
        If None, a default ISO-like format is used.

    Raises:
        ValueError: If the provided format string is invalid.

    Returns:
        str: The current JST datetime formatted as a string.
    """
    if fmt is None:
        fmt = "%Y-%m-%dT%H:%M:%S%z"
        return get_now_jst().strftime(fmt)
    elif fmt is not None:
        try:
            return get_now_jst().strftime(fmt)
        except Exception as e:
            raise ValueError(f"Invalid format string: {fmt}") from e
    return get_now_jst().isoformat()


def gen_uuid() -> str:
    return str(uuid.uuid4())

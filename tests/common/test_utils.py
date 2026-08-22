"""Unit tests for the cross-cutting process primitives in common/utils.py.

``resolve_default_target_date`` used to be copied into five raw scrapers, and
its test was copied alongside it -- five near-identical cases, three of them
byte-identical (docs/tasks/refactaring_20260817.md section 2.11). The
function moved to common/; its test lives here now rather than in whichever
scraper happened to be opened first.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from zoneinfo import ZoneInfo

import pytest

from common.utils import gen_uuid, resolve_default_target_date, resolve_target_at

JST = ZoneInfo("Asia/Tokyo")


# ---------------------------------------------------------------------------
# resolve_default_target_date() — yesterday in JST
#
# Every denki-yohou source (OCCTO, power_usage_hokuriku, supply_demand_actuals
# for Tohoku/Chugoku/Shikoku) defaults here. "Yesterday" rather than "today"
# because today's figures are still live: OCCTO publishes a day's actuals
# around 15:30 JST the following day, and the でんき予報 snapshots only
# finalize shortly after midnight JST.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("now", "expected"),
    [
        pytest.param(
            datetime(2026, 8, 15, 12, 0, tzinfo=JST),
            date(2026, 8, 14),
            id="midday-jst",
        ),
        pytest.param(
            datetime(2026, 8, 10, 2, 0, tzinfo=JST),
            date(2026, 8, 9),
            id="just-after-midnight-jst",
        ),
        pytest.param(
            datetime(2026, 8, 15, 23, 59, tzinfo=JST),
            date(2026, 8, 14),
            id="last-minute-of-the-jst-day",
        ),
    ],
)
def test_resolves_to_the_previous_jst_day(now: datetime, expected: date) -> None:
    # Act / Assert
    assert resolve_default_target_date(now) == expected


@pytest.mark.parametrize(
    ("now_utc", "expected"),
    [
        pytest.param(
            datetime(2026, 8, 15, 3, 0, tzinfo=UTC),  # 12:00 JST same day
            date(2026, 8, 14),
            id="utc-midday-same-jst-day",
        ),
        pytest.param(
            datetime(2026, 8, 15, 16, 0, tzinfo=UTC),  # 01:00 JST *next* day
            date(2026, 8, 15),
            id="utc-evening-is-already-tomorrow-in-jst",
        ),
        pytest.param(
            datetime(2026, 8, 15, 14, 59, tzinfo=UTC),  # 23:59 JST same day
            date(2026, 8, 14),
            id="utc-just-before-the-jst-rollover",
        ),
    ],
)
def test_converts_a_utc_clock_before_taking_yesterday(
    now_utc: datetime, expected: date
) -> None:
    """Every caller passes ``datetime.now(UTC)``, so the conversion is the
    path that actually runs -- and 15:00 UTC onward is already tomorrow in
    JST, which is where taking the date first would go wrong."""
    # Act / Assert
    assert resolve_default_target_date(now_utc) == expected


# ---------------------------------------------------------------------------
# resolve_target_at()
# ---------------------------------------------------------------------------


def test_resolve_target_at_converts_unix_milliseconds_to_utc() -> None:
    # Arrange
    timestamp_ms = 1_786_000_000_000

    # Act
    resolved = resolve_target_at(timestamp_ms)

    # Assert
    assert resolved.tzinfo is not None
    assert resolved.timestamp() == pytest.approx(timestamp_ms / 1000)


def test_resolve_target_at_falls_back_to_now_when_unset() -> None:
    """The CLI's --timestamp-ms is optional; omitting it means "now"."""
    # Act
    before = datetime.now(UTC)
    resolved = resolve_target_at(None)

    # Assert
    assert before <= resolved <= datetime.now(UTC)


# ---------------------------------------------------------------------------
# gen_uuid()
# ---------------------------------------------------------------------------


def test_gen_uuid_returns_a_distinct_string_each_call() -> None:
    """It mints execution_ids, so a collision would merge two runs' rows."""
    # Act
    ids = {gen_uuid() for _ in range(100)}

    # Assert
    assert len(ids) == 100
    assert all(isinstance(value, str) and len(value) == 36 for value in ids)

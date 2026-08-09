"""Unit tests for BaseHttpScraper, including the prepare() hook."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock

from common.http_scraper import BaseHttpScraper, RequestSpec

TARGET_AT = datetime(2026, 8, 7, 0, 0, 0, tzinfo=UTC)


class _NoopPrepareScraper(BaseHttpScraper):
    """Scraper that only implements build_request, like JEPX's."""

    def build_request(self, target_at: datetime) -> RequestSpec:
        return RequestSpec(method="GET", url="https://example.test/download")


class _RecordingPrepareScraper(BaseHttpScraper):
    """Scraper that overrides prepare() and records call order."""

    def __init__(self, calls: list[str], session=None) -> None:
        super().__init__(session=session)
        self._calls = calls

    def prepare(self, target_at: datetime) -> None:
        self._calls.append("prepare")

    def build_request(self, target_at: datetime) -> RequestSpec:
        self._calls.append("build_request")
        return RequestSpec(method="GET", url="https://example.test/download")


def _mock_session_returning(body: bytes) -> MagicMock:
    session = MagicMock()
    response = MagicMock()
    response.content = body
    response.raise_for_status.return_value = None
    session.request.return_value = response
    return session


def test_default_prepare_is_noop_and_existing_scrapers_still_work():
    """Scrapers that never override prepare() (e.g. JEPX) keep working unchanged."""
    session = _mock_session_returning(b"csv-body")
    scraper = _NoopPrepareScraper(session=session)

    body = scraper.fetch(TARGET_AT)

    assert body == b"csv-body"
    session.request.assert_called_once()


def test_fetch_response_calls_prepare_before_build_request():
    calls: list[str] = []
    session = _mock_session_returning(b"csv-body")
    scraper = _RecordingPrepareScraper(calls, session=session)

    scraper.fetch_response(TARGET_AT)

    assert calls == ["prepare", "build_request"]

"""Unit tests for the Chugoku supply_demand_actuals source-to-raw scraping workflow.

Simple GET, no session prep (like power_usage_hokuriku's), but keyed by
calendar year instead of by date, so the snapshot/manifest pattern mirrors
JEPX's year-keyed one instead.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime
from unittest.mock import MagicMock
from zoneinfo import ZoneInfo

from common.utilities import resolve_default_target_date
from pipeline.raw.source_to_raw_supply_demand_actuals_chugoku import (
    DOWNLOAD_URL_TEMPLATE,
    ChugokuScrapedRawObject,
    ChugokuSupplyDemandActualsScraper,
    _resolve_manifest_key,
    scrape_supply_demand_actuals_chugoku_raw,
)

CSV_BODY = "2026/8/15 1:52 UPDATE\r\n\r\nDATE,TIME,実績(万kW)\r\n...".encode("cp932")


def _mock_session_returning(body: bytes = CSV_BODY) -> MagicMock:
    session = MagicMock()
    response = MagicMock()
    response.content = body
    response.raise_for_status.return_value = None
    session.request.return_value = response
    return session


def test_resolve_default_target_date_is_yesterday_in_jst():
    jst_now = datetime(2026, 8, 15, 12, 0, 0, tzinfo=ZoneInfo("Asia/Tokyo"))

    assert resolve_default_target_date(jst_now).isoformat() == "2026-08-14"


def test_build_request_is_a_plain_get_to_the_year_keyed_url():
    scraper = ChugokuSupplyDemandActualsScraper()

    spec = scraper.build_request(datetime(2026, 1, 1))

    assert spec.method == "GET"
    assert spec.url == DOWNLOAD_URL_TEMPLATE.format(year=2026)


def test_scrape_returns_body_and_year_keyed_file_name():
    session = _mock_session_returning()
    scraper = ChugokuSupplyDemandActualsScraper(session=session)

    scraped = scraper.scrape(2026)

    assert isinstance(scraped, ChugokuScrapedRawObject)
    assert scraped.file_name == "juyo-2026.csv"
    assert scraped.body == CSV_BODY


def test_scrape_does_not_call_get_or_post_before_the_download_request():
    session = _mock_session_returning()
    scraper = ChugokuSupplyDemandActualsScraper(session=session)

    scraper.scrape(2026)

    session.get.assert_not_called()
    session.post.assert_not_called()
    session.request.assert_called_once()


def _make_scraper(body: bytes = CSV_BODY) -> MagicMock:
    scraper = MagicMock()
    scraper.scrape.return_value = ChugokuScrapedRawObject(
        file_name="juyo-2026.csv",
        body=body,
    )
    return scraper


def _make_storage(manifest_body: bytes | None = None) -> MagicMock:
    client = MagicMock()

    def _get_object_or_none(bucket_name: str, object_name: str) -> bytes | None:
        if object_name == _resolve_manifest_key(2026):
            return manifest_body
        return None

    client.get_object_or_none.side_effect = _get_object_or_none
    return client


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def test_first_run_saves_snapshot():
    scraper = _make_scraper()
    storage = _make_storage(manifest_body=None)

    result = scrape_supply_demand_actuals_chugoku_raw(
        storage_client=storage,
        scraper=scraper,
        bucket_name="test-bucket",
        year=2026,
    )

    assert result.skipped is False
    assert result.sha256 == _sha256(CSV_BODY)
    assert result.snapshot_prefix is not None
    assert "year=2026" in result.snapshot_prefix
    assert "ingested_at=" in result.snapshot_prefix


def test_unchanged_content_skips_upload():
    sha = _sha256(CSV_BODY)
    manifest = json.dumps({"sha256": sha}).encode()
    scraper = _make_scraper()
    storage = _make_storage(manifest_body=manifest)

    result = scrape_supply_demand_actuals_chugoku_raw(
        storage_client=storage,
        scraper=scraper,
        bucket_name="test-bucket",
        year=2026,
    )

    assert result.skipped is True
    assert result.snapshot_prefix is None
    storage.upload_bytes.assert_not_called()


def test_changed_content_saves_new_snapshot():
    old_manifest = json.dumps({"sha256": "old_hash_value"}).encode()
    scraper = _make_scraper()
    storage = _make_storage(manifest_body=old_manifest)

    result = scrape_supply_demand_actuals_chugoku_raw(
        storage_client=storage,
        scraper=scraper,
        bucket_name="test-bucket",
        year=2026,
    )

    assert result.skipped is False
    assert storage.upload_bytes.call_count == 4  # csv + metadata.json + manifest + log

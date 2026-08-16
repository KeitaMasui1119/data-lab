"""Unit tests for the Hokuriku power_usage source-to-raw scraping workflow.

Hokuriku's でんき予報 snapshot CSV is a simple static-file GET keyed by date
(no session/disclaimer flow, unlike OCCTO), so build_request() coverage is
much thinner than test_source_to_raw_occto.py's. The snapshot/ingestion-log
write path mirrors OCCTO's and is tested the same way.
"""

from __future__ import annotations

import hashlib
import json
from datetime import date, datetime
from unittest.mock import MagicMock
from zoneinfo import ZoneInfo

from common.utilities import resolve_default_target_date
from pipeline.raw.source_to_raw_power_usage_hokuriku import (
    DOWNLOAD_URL_TEMPLATE,
    HokurikuPowerUsageConfig,
    HokurikuPowerUsageScraper,
    HokurikuScrapedRawObject,
    _resolve_manifest_key,
    scrape_power_usage_hokuriku_raw,
)

TARGET_DATE = date(2026, 8, 7)
CSV_BODY = b"2026/08/08 00:10 UPDATE\r\n..."  # arbitrary bytes stand-in for a real CSV


def _mock_session_returning(body: bytes = CSV_BODY) -> MagicMock:
    session = MagicMock()
    response = MagicMock()
    response.content = body
    response.raise_for_status.return_value = None
    session.request.return_value = response
    return session


# ---------------------------------------------------------------------------
# resolve_default_target_date() — yesterday in JST, same rationale as OCCTO's:
# today's snapshot is still live/incomplete, a date's data only finalizes
# shortly after midnight JST the following day.
# ---------------------------------------------------------------------------


def test_resolve_default_target_date_is_yesterday_in_jst():
    jst_now = datetime(2026, 8, 10, 2, 0, 0, tzinfo=ZoneInfo("Asia/Tokyo"))

    assert resolve_default_target_date(jst_now) == date(2026, 8, 9)


# ---------------------------------------------------------------------------
# build_request() / scrape() — simple GET, no session prep required
# ---------------------------------------------------------------------------


def test_build_request_is_a_plain_get_to_the_date_keyed_url():
    scraper = HokurikuPowerUsageScraper()

    spec = scraper.build_request(datetime.combine(TARGET_DATE, datetime.min.time()))

    assert spec.method == "GET"
    assert spec.url == DOWNLOAD_URL_TEMPLATE.format(date_label="20260807")


def test_scrape_returns_body_and_date_keyed_file_name():
    session = _mock_session_returning()
    scraper = HokurikuPowerUsageScraper(session=session)

    scraped = scraper.scrape(TARGET_DATE)

    assert isinstance(scraped, HokurikuScrapedRawObject)
    assert scraped.file_name == "juyo_05_20260807.csv"
    assert scraped.body == CSV_BODY


def test_scrape_does_not_call_get_or_post_before_the_download_request():
    """prepare() is left as BaseHttpScraper's no-op default — unlike OCCTO,
    no disclaimer/session flow is needed."""
    session = _mock_session_returning()
    scraper = HokurikuPowerUsageScraper(session=session)

    scraper.scrape(TARGET_DATE)

    session.get.assert_not_called()
    session.post.assert_not_called()
    session.request.assert_called_once()


# ---------------------------------------------------------------------------
# scrape_power_usage_hokuriku_raw() — snapshot + ingestion log
# ---------------------------------------------------------------------------


def _make_scraper(body: bytes = CSV_BODY) -> MagicMock:
    scraper = MagicMock()
    scraper.config = HokurikuPowerUsageConfig()
    scraper.scrape.return_value = HokurikuScrapedRawObject(
        file_name="juyo_05_20260807.csv",
        body=body,
    )
    return scraper


def _make_storage(
    manifest_body: bytes | None = None,
    ingestion_log_body: bytes | None = None,
) -> MagicMock:
    client = MagicMock()

    def _get_object_or_none(bucket_name: str, object_name: str) -> bytes | None:
        if object_name == _resolve_manifest_key(TARGET_DATE):
            return manifest_body
        if object_name == "metadata/raw_ingestion_log.parquet":
            return ingestion_log_body
        return None

    client.get_object_or_none.side_effect = _get_object_or_none
    return client


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def test_first_run_saves_snapshot():
    scraper = _make_scraper()
    storage = _make_storage(manifest_body=None)

    result = scrape_power_usage_hokuriku_raw(
        storage_client=storage,
        scraper=scraper,
        bucket_name="test-bucket",
        target_date=TARGET_DATE,
    )

    assert result.skipped is False
    assert result.sha256 == _sha256(CSV_BODY)
    assert result.snapshot_prefix is not None
    assert f"target_date={TARGET_DATE.isoformat()}" in result.snapshot_prefix
    assert "ingested_at=" in result.snapshot_prefix


def test_object_key_includes_target_date_and_ingested_at():
    scraper = _make_scraper()
    storage = _make_storage(manifest_body=None)

    scrape_power_usage_hokuriku_raw(
        storage_client=storage,
        scraper=scraper,
        bucket_name="test-bucket",
        target_date=TARGET_DATE,
    )

    uploaded_keys = [
        c.kwargs["object_name"] for c in storage.upload_bytes.call_args_list
    ]
    csv_keys = [k for k in uploaded_keys if k.endswith(".csv")]
    assert len(csv_keys) == 1
    assert f"target_date={TARGET_DATE.isoformat()}" in csv_keys[0]
    assert "ingested_at=" in csv_keys[0]


def test_unchanged_content_skips_upload():
    sha = _sha256(CSV_BODY)
    manifest = json.dumps({"sha256": sha}).encode()
    scraper = _make_scraper()
    storage = _make_storage(manifest_body=manifest)

    result = scrape_power_usage_hokuriku_raw(
        storage_client=storage,
        scraper=scraper,
        bucket_name="test-bucket",
        target_date=TARGET_DATE,
    )

    assert result.skipped is True
    assert result.snapshot_prefix is None
    storage.upload_bytes.assert_not_called()


def test_changed_content_saves_new_snapshot():
    old_manifest = json.dumps({"sha256": "old_hash_value"}).encode()
    scraper = _make_scraper()
    storage = _make_storage(manifest_body=old_manifest)

    result = scrape_power_usage_hokuriku_raw(
        storage_client=storage,
        scraper=scraper,
        bucket_name="test-bucket",
        target_date=TARGET_DATE,
    )

    assert result.skipped is False
    assert result.sha256 == _sha256(CSV_BODY)
    assert storage.upload_bytes.call_count == 4  # csv + metadata.json + manifest + log


def test_ingestion_log_records_power_usage_hokuriku_dataset_row():
    import io

    import polars as pl

    scraper = _make_scraper()
    storage = _make_storage(manifest_body=None)

    scrape_power_usage_hokuriku_raw(
        storage_client=storage,
        scraper=scraper,
        bucket_name="test-bucket",
        target_date=TARGET_DATE,
    )

    log_call = next(
        c
        for c in storage.upload_bytes.call_args_list
        if c.kwargs["object_name"] == "metadata/raw_ingestion_log.parquet"
    )
    log_df = pl.read_parquet(io.BytesIO(log_call.kwargs["body"]))
    assert log_df.height == 1
    row = log_df.row(0, named=True)
    assert row["dataset"] == "power_usage_hokuriku"
    assert row["snapshot_date"] == TARGET_DATE.isoformat()
    assert row["is_latest"] is True

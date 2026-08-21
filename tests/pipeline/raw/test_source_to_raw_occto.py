"""Unit tests for the OCCTO source-to-raw scraping workflow.

Covers the real 4-step session flow (GET home -> POST disclaimer-agree/next
with agreed=0 -> POST /info/hks/search -> GET downloadCsv) documented in
docs/tasks/occto_pipeline_ingestion.md, and the snapshot/ingestion-log write
path modeled on the JEPX equivalent.
"""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, date, datetime
from unittest.mock import MagicMock

from common.utilities import resolve_default_target_date
from pipeline.raw.source_to_raw_occto_unit_generation_actuals import (
    ALL_AREA_CODES,
    ALL_METHOD_CODES,
    DISCLAIMER_AGREE_URL,
    DISCLAIMER_AGREED_VALUE,
    DOWNLOAD_CSV_URL,
    HOME_URL,
    SEARCH_URL,
    OCCTOScrapedRawObject,
    OCCTOUnitGenerationConfig,
    OCCTOUnitGenerationScraper,
    _resolve_manifest_key,
    run_source_to_raw_occto_unit_generation_actuals,
)

TARGET_DATE = date(2026, 8, 7)
CSV_BODY = b"\xef\xbb\xbf\x94\xad\x93\x64..."  # arbitrary bytes stand-in for a real CSV


def _mock_session_returning(body: bytes = CSV_BODY) -> MagicMock:
    session = MagicMock()
    response = MagicMock()
    response.content = body
    response.raise_for_status.return_value = None
    session.request.return_value = response
    return session


# ---------------------------------------------------------------------------
# resolve_default_target_date() — shared by scrape-occto CLI and the
# orchestrator so both default to the same "yesterday in JST" target date
# ---------------------------------------------------------------------------


def test_resolve_default_target_date_is_yesterday_in_jst():
    from zoneinfo import ZoneInfo

    jst_now = datetime(2026, 8, 10, 2, 0, 0, tzinfo=ZoneInfo("Asia/Tokyo"))

    assert resolve_default_target_date(jst_now) == date(2026, 8, 9)


# ---------------------------------------------------------------------------
# prepare() — session establishment via GET home + POST disclaimer agreement
# ---------------------------------------------------------------------------


def test_prepare_visits_home_then_agrees_to_disclaimer_with_agreed_0():
    session = _mock_session_returning()
    scraper = OCCTOUnitGenerationScraper(session=session)

    scraper.prepare(datetime.combine(TARGET_DATE, datetime.min.time(), tzinfo=UTC))

    session.get.assert_called_once()
    assert session.get.call_args.args[0] == HOME_URL
    session.post.assert_called_once()
    assert session.post.call_args.args[0] == DISCLAIMER_AGREE_URL
    assert session.post.call_args.kwargs["data"] == {"agreed": DISCLAIMER_AGREED_VALUE}


def test_prepare_calls_get_before_post():
    calls: list[str] = []
    session = MagicMock()
    session.get.side_effect = lambda *a, **k: calls.append("get")
    session.post.side_effect = lambda *a, **k: calls.append("post")
    scraper = OCCTOUnitGenerationScraper(session=session)

    scraper.prepare(datetime.combine(TARGET_DATE, datetime.min.time(), tzinfo=UTC))

    assert calls == ["get", "post"]


# ---------------------------------------------------------------------------
# scrape() — download request parameters
# ---------------------------------------------------------------------------


def test_scrape_sends_full_area_and_method_code_lists():
    session = _mock_session_returning()
    scraper = OCCTOUnitGenerationScraper(session=session)

    scraper.scrape(TARGET_DATE)

    request_kwargs = session.request.call_args.kwargs
    assert request_kwargs["url"] == DOWNLOAD_CSV_URL
    assert request_kwargs["params"]["areaCheckbox"] == list(ALL_AREA_CODES)
    assert request_kwargs["params"]["hatudenHosikiCheckbox"] == list(ALL_METHOD_CODES)


def test_scrape_single_date_sets_from_and_to_to_same_date():
    session = _mock_session_returning()
    scraper = OCCTOUnitGenerationScraper(session=session)

    scraper.scrape(TARGET_DATE)

    params = session.request.call_args.kwargs["params"]
    assert params["tgtDateDateFrom"] == "2026/08/07"
    assert params["tgtDateDateTo"] == "2026/08/07"


def test_scrape_date_range_sets_distinct_from_and_to():
    session = _mock_session_returning()
    scraper = OCCTOUnitGenerationScraper(session=session)

    scraper.scrape(date(2026, 8, 1), date(2026, 8, 7))

    params = session.request.call_args.kwargs["params"]
    assert params["tgtDateDateFrom"] == "2026/08/01"
    assert params["tgtDateDateTo"] == "2026/08/07"


def test_scrape_returns_file_name_for_single_date():
    session = _mock_session_returning()
    scraper = OCCTOUnitGenerationScraper(session=session)

    scraped = scraper.scrape(TARGET_DATE)

    assert isinstance(scraped, OCCTOScrapedRawObject)
    assert scraped.file_name == "ユニット別発電実績_2026-08-07.csv"
    assert scraped.body == CSV_BODY


def test_scrape_returns_range_file_name_when_to_date_differs():
    session = _mock_session_returning()
    scraper = OCCTOUnitGenerationScraper(session=session)

    scraped = scraper.scrape(date(2026, 8, 1), date(2026, 8, 7))

    assert scraped.file_name == "ユニット別発電実績_2026-08-01_2026-08-07.csv"


def test_scrape_calls_prepare_search_then_download_in_order():
    calls: list[str] = []
    session = MagicMock()
    session.get.side_effect = lambda *a, **k: calls.append("get")
    session.post.side_effect = lambda *a, **k: calls.append("post")

    def _record_request(*args, **kwargs):
        url = kwargs.get("url", args[0] if args else "")
        calls.append("search" if SEARCH_URL in url else "download")
        response = MagicMock()
        response.content = CSV_BODY
        response.raise_for_status.return_value = None
        return response

    session.request.side_effect = _record_request
    scraper = OCCTOUnitGenerationScraper(session=session)

    scraper.scrape(TARGET_DATE)

    assert calls == ["get", "post", "search", "download"]


def test_scrape_posts_search_with_date_and_filter_params():
    session = _mock_session_returning()
    scraper = OCCTOUnitGenerationScraper(session=session)

    scraper.scrape(TARGET_DATE)

    # session.request is called twice: search (POST) then download (GET).
    # call_args_list[0] is the search call.
    search_call = session.request.call_args_list[0]
    assert search_call.kwargs["method"] == "POST"
    assert search_call.kwargs["url"] == SEARCH_URL
    assert search_call.kwargs["data"]["tgtDateDateFrom"] == "2026/08/07"
    assert search_call.kwargs["data"]["tgtDateDateTo"] == "2026/08/07"
    assert search_call.kwargs["data"]["areaCheckbox"] == list(ALL_AREA_CODES)
    assert search_call.kwargs["data"]["hatudenHosikiCheckbox"] == list(ALL_METHOD_CODES)


# ---------------------------------------------------------------------------
# run_source_to_raw_occto_unit_generation_actuals() — snapshot + ingestion log
# ---------------------------------------------------------------------------


def _make_scraper(body: bytes = CSV_BODY) -> MagicMock:
    scraper = MagicMock()
    scraper.config = OCCTOUnitGenerationConfig()
    scraper.scrape.return_value = OCCTOScrapedRawObject(
        file_name="ユニット別発電実績_2026-08-07.csv",
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

    result = run_source_to_raw_occto_unit_generation_actuals(
        storage_client=storage,
        scraper=scraper,
        bucket_name="test-bucket",
        from_date=TARGET_DATE,
    )

    assert result.skipped is False
    assert result.sha256 == _sha256(CSV_BODY)
    assert result.snapshot_prefix is not None
    assert f"target_date={TARGET_DATE.isoformat()}" in result.snapshot_prefix
    assert "ingested_at=" in result.snapshot_prefix


def test_object_key_includes_target_date_and_ingested_at():
    scraper = _make_scraper()
    storage = _make_storage(manifest_body=None)

    run_source_to_raw_occto_unit_generation_actuals(
        storage_client=storage,
        scraper=scraper,
        bucket_name="test-bucket",
        from_date=TARGET_DATE,
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

    result = run_source_to_raw_occto_unit_generation_actuals(
        storage_client=storage,
        scraper=scraper,
        bucket_name="test-bucket",
        from_date=TARGET_DATE,
    )

    assert result.skipped is True
    assert result.snapshot_prefix is None
    storage.upload_bytes.assert_not_called()


def test_changed_content_saves_new_snapshot():
    old_manifest = json.dumps({"sha256": "old_hash_value"}).encode()
    scraper = _make_scraper()
    storage = _make_storage(manifest_body=old_manifest)

    result = run_source_to_raw_occto_unit_generation_actuals(
        storage_client=storage,
        scraper=scraper,
        bucket_name="test-bucket",
        from_date=TARGET_DATE,
    )

    assert result.skipped is False
    assert result.sha256 == _sha256(CSV_BODY)
    assert storage.upload_bytes.call_count == 4  # csv + metadata.json + manifest + log


def test_ingestion_log_records_occto_dataset_row():
    import io

    import polars as pl

    scraper = _make_scraper()
    storage = _make_storage(manifest_body=None)

    run_source_to_raw_occto_unit_generation_actuals(
        storage_client=storage,
        scraper=scraper,
        bucket_name="test-bucket",
        from_date=TARGET_DATE,
    )

    log_call = next(
        c
        for c in storage.upload_bytes.call_args_list
        if c.kwargs["object_name"] == "metadata/raw_ingestion_log.parquet"
    )
    log_df = pl.read_parquet(io.BytesIO(log_call.kwargs["body"]))
    assert log_df.height == 1
    row = log_df.row(0, named=True)
    assert row["dataset"] == "occto_unit_generation"
    assert row["snapshot_date"] == TARGET_DATE.isoformat()
    assert row["is_latest"] is True

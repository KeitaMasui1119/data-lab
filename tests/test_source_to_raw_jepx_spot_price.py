from __future__ import annotations

import gzip
import hashlib
import io
import json
from datetime import UTC, datetime
from unittest.mock import MagicMock

import polars as pl

from pipeline.raw.source_to_raw_jepx_spot_price import (
    JEPXScrapedRawObject,
    _resolve_ingestion_log_key,
    _resolve_manifest_key,
    _resolve_snapshot_prefix,
    scrape_jepx_spot_price_raw,
)

# 2026-05-14 は会計年度 2026（4月始まり）
TARGET_AT = datetime(2026, 5, 14, 9, 0, 0, tzinfo=UTC)
FISCAL_YEAR = 2026
CSV_BODY = b"date,price\n2026-05-14,100\n"


def _make_scraper(
    body: bytes = CSV_BODY,
    etag: str | None = '"abc123"',
    last_modified: str | None = "Thu, 14 May 2026 09:00:00 GMT",
) -> MagicMock:
    scraper = MagicMock()
    scraper.config.base_url = "https://www.jepx.jp/_download.php"
    scraper.scrape.return_value = JEPXScrapedRawObject(
        object_key=f"raw/jepx/spot_summary/spot_summary_{FISCAL_YEAR}.csv",
        file_name=f"spot_summary_{FISCAL_YEAR}.csv",
        body=body,
        http_etag=etag,
        http_last_modified=last_modified,
    )
    return scraper


def _make_storage(
    manifest_body: bytes | None = None,
    ingestion_log_body: bytes | None = None,
) -> MagicMock:
    client = MagicMock()

    def _get_object_or_none(bucket_name: str, object_name: str) -> bytes | None:
        if object_name == _resolve_manifest_key(FISCAL_YEAR):
            return manifest_body
        if object_name == _resolve_ingestion_log_key():
            return ingestion_log_body
        return None

    client.get_object_or_none.side_effect = _get_object_or_none
    return client


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _to_parquet_bytes(df: pl.DataFrame) -> bytes:
    buffer = io.BytesIO()
    df.write_parquet(buffer)
    return buffer.getvalue()


# ---------------------------------------------------------------------------
# パスヘルパー
# ---------------------------------------------------------------------------


def test_resolve_snapshot_prefix():
    ingested_at = datetime(2026, 5, 14, 9, 0, 0, tzinfo=UTC)
    prefix = _resolve_snapshot_prefix(2026, ingested_at)
    assert prefix == "raw/jepx/spot_price/year=2026/ingested_at=20260514T090000"


def test_resolve_manifest_key():
    key = _resolve_manifest_key(2026)
    assert key == "raw/jepx/spot_price/manifests/year=2026/latest.json"


# ---------------------------------------------------------------------------
# scrape_jepx_spot_price_raw — 初回実行（マニフェストなし）
# ---------------------------------------------------------------------------


def test_first_run_saves_snapshot():
    """マニフェストが存在しない場合、skipped=False でスナップショットを保存する。"""
    scraper = _make_scraper()
    storage = _make_storage(manifest_body=None)

    result = scrape_jepx_spot_price_raw(
        storage_client=storage,
        scraper=scraper,
        bucket_name="test-bucket",
        target_at=TARGET_AT,
    )

    assert result.skipped is False
    assert result.year == FISCAL_YEAR
    assert result.sha256 == _sha256(CSV_BODY)
    assert result.content_length == len(CSV_BODY)
    assert result.snapshot_prefix is not None
    assert result.snapshot_prefix.startswith(
        f"raw/jepx/spot_price/year={FISCAL_YEAR}/ingested_at="
    )
    assert result.manifest_key == _resolve_manifest_key(FISCAL_YEAR)


def test_first_run_uploads_three_objects():
    """初回実行は4オブジェクトをアップロードする。"""
    scraper = _make_scraper()
    storage = _make_storage(manifest_body=None)

    result = scrape_jepx_spot_price_raw(
        storage_client=storage,
        scraper=scraper,
        bucket_name="test-bucket",
        target_at=TARGET_AT,
    )

    assert storage.upload_bytes.call_count == 4
    uploaded_keys = [
        c.kwargs["object_name"] for c in storage.upload_bytes.call_args_list
    ]
    assert any(k.endswith("/spot.csv.gz") for k in uploaded_keys)
    assert any(k.endswith("/metadata.json") for k in uploaded_keys)
    assert _resolve_ingestion_log_key() in uploaded_keys
    assert result.manifest_key in uploaded_keys


def test_first_run_compresses_csv():
    """spot.csv.gz はアップロード前に gzip 圧縮されている。"""
    scraper = _make_scraper()
    storage = _make_storage(manifest_body=None)

    scrape_jepx_spot_price_raw(
        storage_client=storage,
        scraper=scraper,
        bucket_name="test-bucket",
        target_at=TARGET_AT,
    )

    gz_call = next(
        c
        for c in storage.upload_bytes.call_args_list
        if c.kwargs["object_name"].endswith("/spot.csv.gz")
    )
    assert gzip.decompress(gz_call.kwargs["body"]) == CSV_BODY


def test_first_run_metadata_fields():
    """metadata.json に必要なフィールドが含まれる。"""
    scraper = _make_scraper(etag='"abc"', last_modified="Thu, 14 May 2026 09:00:00 GMT")
    storage = _make_storage(manifest_body=None)

    scrape_jepx_spot_price_raw(
        storage_client=storage,
        scraper=scraper,
        bucket_name="test-bucket",
        target_at=TARGET_AT,
    )

    meta_call = next(
        c
        for c in storage.upload_bytes.call_args_list
        if c.kwargs["object_name"].endswith("/metadata.json")
    )
    meta = json.loads(meta_call.kwargs["body"])
    assert meta["year"] == FISCAL_YEAR
    assert meta["sha256"] == _sha256(CSV_BODY)
    assert meta["content_length"] == len(CSV_BODY)
    assert meta["etag"] == '"abc"'
    assert meta["last_modified"] == "Thu, 14 May 2026 09:00:00 GMT"
    assert "source_url" in meta
    assert "ingested_at" in meta


# ---------------------------------------------------------------------------
# scrape_jepx_spot_price_raw — ハッシュ変化なし（スキップ）
# ---------------------------------------------------------------------------


def test_unchanged_content_skips_upload():
    """前回と SHA256 が同じ場合はアップロードをスキップして skipped=True を返す。"""
    sha = _sha256(CSV_BODY)
    manifest = json.dumps({"sha256": sha}).encode()
    scraper = _make_scraper()
    storage = _make_storage(manifest_body=manifest)

    result = scrape_jepx_spot_price_raw(
        storage_client=storage,
        scraper=scraper,
        bucket_name="test-bucket",
        target_at=TARGET_AT,
    )

    assert result.skipped is True
    assert result.snapshot_prefix is None
    assert result.sha256 == sha
    storage.upload_bytes.assert_not_called()


# ---------------------------------------------------------------------------
# scrape_jepx_spot_price_raw — ハッシュ変化あり（新規スナップショット）
# ---------------------------------------------------------------------------


def test_changed_content_saves_new_snapshot():
    """前回と SHA256 が異なる場合は新しいスナップショットを保存する。"""
    old_manifest = json.dumps({"sha256": "old_hash_value"}).encode()
    scraper = _make_scraper()
    storage = _make_storage(manifest_body=old_manifest)

    result = scrape_jepx_spot_price_raw(
        storage_client=storage,
        scraper=scraper,
        bucket_name="test-bucket",
        target_at=TARGET_AT,
    )

    assert result.skipped is False
    assert result.sha256 == _sha256(CSV_BODY)
    assert storage.upload_bytes.call_count == 4


def test_ingestion_log_marks_old_latest_false_when_new_snapshot_saved():
    old_log = pl.DataFrame(
        {
            "dataset": ["jepx.spot_price"],
            "fiscal_year": [FISCAL_YEAR],
            "snapshot_date": ["2026-05-13"],
            "ingested_at": ["2026-05-13T09:00:00+00:00"],
            "file_hash": ["old_hash_value"],
            "file_path": [
                "raw/jepx/spot_price/year=2026/ingested_at=20260513T090000/spot.csv.gz"
            ],
            "content_length": [10],
            "etag": [None],
            "last_modified": [None],
            "is_latest": [True],
            "bronze_status": ["pending"],
            "bronze_processed_at": [None],
        }
    )
    old_manifest = json.dumps({"sha256": "old_hash_value"}).encode()
    scraper = _make_scraper()
    storage = _make_storage(
        manifest_body=old_manifest,
        ingestion_log_body=_to_parquet_bytes(old_log),
    )

    scrape_jepx_spot_price_raw(
        storage_client=storage,
        scraper=scraper,
        bucket_name="test-bucket",
        target_at=TARGET_AT,
    )

    parquet_call = next(
        c
        for c in storage.upload_bytes.call_args_list
        if c.kwargs["object_name"] == _resolve_ingestion_log_key()
    )
    updated = pl.read_parquet(io.BytesIO(parquet_call.kwargs["body"]))

    target_year = updated.filter(pl.col("fiscal_year") == FISCAL_YEAR)
    latest_count = target_year.filter(pl.col("is_latest")).height
    assert target_year.height == 2
    assert latest_count == 1

"""Chugoku supply_demand_actuals source-to-raw scraping and upload workflow.

Chugoku publishes a single DATE,TIME,実績(万kW) CSV covering one calendar
year, growing by one day's rows daily:
  GET https://www.energia.co.jp/nw/jukyuu/sys/juyo-{year}.csv

Unlike Hokuriku's per-day URL, there is no way to fetch a single day
directly: the whole current year's file must be downloaded each run (same
shape as JEPX's fiscal-year CSV), so raw snapshots are keyed by year, not
by date. The file's latest row is consistently yesterday's (today has not
fully elapsed / been finalized and published yet), confirmed live.
"""

from __future__ import annotations

import hashlib
import io
import json
import logging
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from zoneinfo import ZoneInfo

import polars as pl

from common.http_scraper import BaseHttpScraper, RequestSpec
from common.raw_ingestion_log import DEFAULT_INGESTION_LOG_KEY, load_ingestion_log
from common.storage_client import RustFSClient

logger = logging.getLogger(__name__)

DOWNLOAD_URL_TEMPLATE = "https://www.energia.co.jp/nw/jukyuu/sys/juyo-{year}.csv"
FILE_NAME_TEMPLATE = "juyo-{year}.csv"
OBJECT_PREFIX = "raw/supply_demand_actuals/chugoku"
DATASET_NAME = "supply_demand_actuals_chugoku"


def resolve_default_target_date(now: datetime) -> date:
    """Default target date: yesterday in JST.

    Same rationale as power_usage_hokuriku and OCCTO: the year file's
    latest row is consistently yesterday's (today has not fully elapsed /
    been finalized and published yet), confirmed live.
    """
    jst_now = now.astimezone(ZoneInfo("Asia/Tokyo"))
    return (jst_now - timedelta(days=1)).date()


def _resolve_snapshot_prefix(year: int, ingested_at: datetime) -> str:
    ts = ingested_at.strftime("%Y%m%dT%H%M%S")
    return f"{OBJECT_PREFIX}/year={year}/ingested_at={ts}"


def _resolve_manifest_key(year: int) -> str:
    return f"{OBJECT_PREFIX}/manifests/year={year}/latest.json"


def _build_empty_ingestion_log() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "dataset": pl.Series([], dtype=pl.Utf8),
            "fiscal_year": pl.Series([], dtype=pl.Int64),
            "snapshot_date": pl.Series([], dtype=pl.Utf8),
            "ingested_at": pl.Series([], dtype=pl.Utf8),
            "file_hash": pl.Series([], dtype=pl.Utf8),
            "file_path": pl.Series([], dtype=pl.Utf8),
            "content_length": pl.Series([], dtype=pl.Int64),
            "etag": pl.Series([], dtype=pl.Utf8),
            "last_modified": pl.Series([], dtype=pl.Utf8),
            "is_latest": pl.Series([], dtype=pl.Boolean),
            "bronze_status": pl.Series([], dtype=pl.Utf8),
            "bronze_processed_at": pl.Series([], dtype=pl.Utf8),
        }
    )


def _update_ingestion_log(
    storage_client: RustFSClient,
    bucket_name: str,
    year: int,
    ingested_at: datetime,
    file_hash: str,
    file_path: str,
    content_length: int,
) -> None:
    existing_log = load_ingestion_log(storage_client, bucket_name)
    if existing_log.is_empty():
        existing_log = _build_empty_ingestion_log()
    else:
        existing_log = existing_log.with_columns(
            pl.when(
                (pl.col("dataset") == DATASET_NAME) & (pl.col("fiscal_year") == year)
            )
            .then(pl.lit(False))
            .otherwise(pl.col("is_latest"))
            .alias("is_latest")
        )

    new_row = pl.DataFrame(
        {
            "dataset": pl.Series([DATASET_NAME], dtype=pl.Utf8),
            "fiscal_year": pl.Series([year], dtype=pl.Int64),
            "snapshot_date": pl.Series([None], dtype=pl.Utf8),
            "ingested_at": pl.Series([ingested_at.isoformat()], dtype=pl.Utf8),
            "file_hash": pl.Series([file_hash], dtype=pl.Utf8),
            "file_path": pl.Series([file_path], dtype=pl.Utf8),
            "content_length": pl.Series([content_length], dtype=pl.Int64),
            "etag": pl.Series([None], dtype=pl.Utf8),
            "last_modified": pl.Series([None], dtype=pl.Utf8),
            "is_latest": pl.Series([True], dtype=pl.Boolean),
            "bronze_status": pl.Series(["pending"], dtype=pl.Utf8),
            "bronze_processed_at": pl.Series([None], dtype=pl.Utf8),
        }
    )

    updated_log = pl.concat([existing_log, new_row], how="vertical_relaxed")
    buffer = io.BytesIO()
    updated_log.write_parquet(buffer)
    storage_client.upload_bytes(
        bucket_name=bucket_name,
        object_name=DEFAULT_INGESTION_LOG_KEY,
        body=buffer.getvalue(),
        content_type="application/x-parquet",
    )


@dataclass(frozen=True)
class ChugokuScrapedRawObject:
    """Represents a scraped Chugoku supply_demand_actuals CSV, ready for raw storage."""

    file_name: str
    body: bytes
    content_type: str = "text/csv"


class ChugokuSupplyDemandActualsScraper(BaseHttpScraper):
    """HTTP scraper for Chugoku's supply_demand_actuals year CSV.

    A simple static-file GET keyed by calendar year; no session/disclaimer
    flow is required, so prepare() is left as the base no-op.
    """

    def __init__(self, session=None, timeout_seconds: int = 30) -> None:
        super().__init__(timeout_seconds=timeout_seconds, session=session)

    def build_request(self, target_at: datetime) -> RequestSpec:
        year = target_at.year
        url = DOWNLOAD_URL_TEMPLATE.format(year=year)
        return RequestSpec(method="GET", url=url)

    def scrape(self, year: int) -> ChugokuScrapedRawObject:
        response = self.fetch_response(datetime(year, 1, 1, tzinfo=UTC))
        file_name = FILE_NAME_TEMPLATE.format(year=year)
        return ChugokuScrapedRawObject(file_name=file_name, body=response.content)


@dataclass(frozen=True)
class ChugokuSnapshotResult:
    """Result of a Chugoku supply_demand_actuals raw snapshot attempt."""

    skipped: bool
    bucket_name: str
    year: int
    sha256: str
    content_length: int
    manifest_key: str
    ingestion_log_key: str
    snapshot_prefix: str | None = None


def scrape_supply_demand_actuals_chugoku_raw(
    storage_client: RustFSClient,
    scraper: ChugokuSupplyDemandActualsScraper,
    bucket_name: str,
    year: int,
) -> ChugokuSnapshotResult:
    """Download Chugoku's supply_demand_actuals year CSV and save a raw
    snapshot only when the content has changed since the last snapshot for
    this year (SHA256 comparison via a per-year manifest) -- the file grows
    by one day's rows daily, so this naturally saves a new snapshot on
    every run until the year is complete.
    """
    manifest_key = _resolve_manifest_key(year)

    scraped = scraper.scrape(year)
    sha256 = hashlib.sha256(scraped.body).hexdigest()

    manifest_bytes = storage_client.get_object_or_none(bucket_name, manifest_key)
    if manifest_bytes is not None:
        previous_hash = json.loads(manifest_bytes).get("sha256")
        if previous_hash == sha256:
            logger.info(
                "Chugoku supply_demand_actuals snapshot unchanged "
                "(year=%s, sha256=%.8s), skipping save",
                year,
                sha256,
            )
            return ChugokuSnapshotResult(
                skipped=True,
                bucket_name=bucket_name,
                year=year,
                sha256=sha256,
                content_length=len(scraped.body),
                manifest_key=manifest_key,
                ingestion_log_key=DEFAULT_INGESTION_LOG_KEY,
                snapshot_prefix=None,
            )

    ingested_at = datetime.now(UTC)
    snapshot_prefix = _resolve_snapshot_prefix(year, ingested_at)
    object_key = f"{snapshot_prefix}/{scraped.file_name}"

    storage_client.upload_bytes(
        bucket_name=bucket_name,
        object_name=object_key,
        body=scraped.body,
        content_type=scraped.content_type,
    )

    metadata: dict[str, str | int | None] = {
        "source_url": DOWNLOAD_URL_TEMPLATE.format(year=year),
        "ingested_at": ingested_at.isoformat(),
        "year": year,
        "sha256": sha256,
        "content_length": len(scraped.body),
    }
    metadata_bytes = json.dumps(metadata, ensure_ascii=False, indent=2).encode()

    storage_client.upload_bytes(
        bucket_name=bucket_name,
        object_name=f"{snapshot_prefix}/metadata.json",
        body=metadata_bytes,
        content_type="application/json",
    )
    storage_client.upload_bytes(
        bucket_name=bucket_name,
        object_name=manifest_key,
        body=metadata_bytes,
        content_type="application/json",
    )

    _update_ingestion_log(
        storage_client=storage_client,
        bucket_name=bucket_name,
        year=year,
        ingested_at=ingested_at,
        file_hash=sha256,
        file_path=object_key,
        content_length=len(scraped.body),
    )

    logger.info(
        "Chugoku supply_demand_actuals snapshot saved: year=%s, sha256=%.8s, prefix=%s",
        year,
        sha256,
        snapshot_prefix,
    )

    return ChugokuSnapshotResult(
        skipped=False,
        bucket_name=bucket_name,
        year=year,
        sha256=sha256,
        content_length=len(scraped.body),
        manifest_key=manifest_key,
        ingestion_log_key=DEFAULT_INGESTION_LOG_KEY,
        snapshot_prefix=snapshot_prefix,
    )

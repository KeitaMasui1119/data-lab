"""Hokuriku power_usage source-to-raw scraping and upload workflow.

Hokuriku's でんき予報 daily snapshot CSV is served as a plain static file keyed
by date, with no session/disclaimer flow required (unlike OCCTO):
  GET https://www.rikuden.co.jp/nw/denki-yoho/csv/juyo_05_{YYYYMMDD}.csv
"""

from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass
from datetime import UTC, date, datetime

from common.http_scraper import BaseHttpScraper, RequestSpec
from common.raw_ingestion_log import (
    DEFAULT_INGESTION_LOG_KEY,
    append_ingestion_log_entry,
)
from common.storage_client import RustFSClient
from common.utilities import gen_uuid

logger = logging.getLogger(__name__)

DOWNLOAD_URL_TEMPLATE = (
    "https://www.rikuden.co.jp/nw/denki-yoho/csv/juyo_05_{date_label}.csv"
)
FILE_NAME_TEMPLATE = "juyo_05_{date_label}.csv"
OBJECT_PREFIX = "raw/power_usage/hokuriku"
DATASET_NAME = "power_usage_hokuriku"

# Per the scraping prototype
# (src/Jupyter/scraping_prototypes/electric_forecast_scraping.ipynb), Hokuriku
# only publishes this snapshot format from this date onward.
EARLIEST_AVAILABLE_DATE = date(2020, 4, 1)


# resolve_default_target_date() moved to common/utilities.py (shared by every
# denki-yohou dataset). Hokuriku's own rationale for "yesterday": today's
# snapshot is a live, still-updating view (observed: a mid-day fetch has
# empty values for the remaining hours of today and for tomorrow's forecast
# blocks — see docs/architecture/data_model.md 3.1). A date's file only
# becomes fully finalized shortly after midnight JST the following day
# (observed UPDATE timestamps like "2020/04/02 00:10 UPDATE" for the
# 2020-04-01 snapshot).


def _resolve_snapshot_prefix(target_date: date, ingested_at: datetime) -> str:
    ts = ingested_at.strftime("%Y%m%dT%H%M%S")
    return f"{OBJECT_PREFIX}/target_date={target_date.isoformat()}/ingested_at={ts}"


def _resolve_manifest_key(target_date: date) -> str:
    return (
        f"{OBJECT_PREFIX}/manifests/target_date={target_date.isoformat()}/latest.json"
    )


@dataclass(frozen=True)
class HokurikuPowerUsageConfig:
    """Configuration for downloading Hokuriku power_usage snapshot CSV files."""

    url_template: str = DOWNLOAD_URL_TEMPLATE
    timeout_seconds: int = 30


@dataclass(frozen=True)
class HokurikuScrapedRawObject:
    """Represents a scraped Hokuriku power_usage CSV object ready for raw storage."""

    file_name: str
    body: bytes
    content_type: str = "text/csv"


class HokurikuPowerUsageScraper(BaseHttpScraper):
    """HTTP scraper for Hokuriku power_usage (でんき予報) daily snapshot CSV files.

    A simple static-file GET keyed by date; no session/disclaimer flow is
    required (unlike OCCTO), so prepare() is left as the base no-op.
    """

    def __init__(
        self,
        config: HokurikuPowerUsageConfig | None = None,
        session=None,
    ) -> None:
        self.config = config or HokurikuPowerUsageConfig()
        super().__init__(
            timeout_seconds=self.config.timeout_seconds,
            session=session,
        )

    def build_request(self, target_at: datetime) -> RequestSpec:
        target_date = target_at.date() if isinstance(target_at, datetime) else target_at
        url = self.config.url_template.format(date_label=target_date.strftime("%Y%m%d"))
        return RequestSpec(method="GET", url=url)

    def scrape(self, target_date: date) -> HokurikuScrapedRawObject:
        response = self.fetch_response(
            datetime.combine(target_date, datetime.min.time(), tzinfo=UTC)
        )
        file_name = FILE_NAME_TEMPLATE.format(date_label=target_date.strftime("%Y%m%d"))
        return HokurikuScrapedRawObject(file_name=file_name, body=response.content)


@dataclass(frozen=True)
class HokurikuSnapshotResult:
    """Result of a Hokuriku power_usage raw snapshot attempt."""

    skipped: bool
    bucket_name: str
    target_date: date
    sha256: str
    content_length: int
    manifest_key: str
    ingestion_log_key: str
    snapshot_prefix: str | None = None


def run_source_to_raw_power_usage_hokuriku(
    storage_client: RustFSClient,
    scraper: HokurikuPowerUsageScraper,
    bucket_name: str,
    target_date: date,
    execution_id: str | None = None,
) -> HokurikuSnapshotResult:
    """Download Hokuriku power_usage CSV and save a raw snapshot only when the
    content has changed since the last snapshot for this target date
    (SHA256 comparison via a per-date manifest).
    """
    # The orchestrator passes its run id so the log row names the run that
    # fetched the file; a standalone scrape is its own one-step run.
    execution_id = execution_id or gen_uuid()
    manifest_key = _resolve_manifest_key(target_date)

    scraped = scraper.scrape(target_date)
    sha256 = hashlib.sha256(scraped.body).hexdigest()

    manifest_bytes = storage_client.get_object_or_none(bucket_name, manifest_key)
    if manifest_bytes is not None:
        previous_hash = json.loads(manifest_bytes).get("sha256")
        if previous_hash == sha256:
            logger.info(
                "Hokuriku power_usage snapshot unchanged "
                "(target_date=%s, sha256=%.8s), skipping save",
                target_date,
                sha256,
            )
            return HokurikuSnapshotResult(
                skipped=True,
                bucket_name=bucket_name,
                target_date=target_date,
                sha256=sha256,
                content_length=len(scraped.body),
                manifest_key=manifest_key,
                ingestion_log_key=DEFAULT_INGESTION_LOG_KEY,
                snapshot_prefix=None,
            )

    ingested_at = datetime.now(UTC)
    snapshot_prefix = _resolve_snapshot_prefix(target_date, ingested_at)
    object_key = f"{snapshot_prefix}/{scraped.file_name}"

    storage_client.upload_bytes(
        bucket_name=bucket_name,
        object_name=object_key,
        body=scraped.body,
        content_type=scraped.content_type,
    )

    metadata: dict[str, str | int | None] = {
        "source_url": scraper.config.url_template.format(
            date_label=target_date.strftime("%Y%m%d")
        ),
        "ingested_at": ingested_at.isoformat(),
        "target_date": target_date.isoformat(),
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

    append_ingestion_log_entry(
        storage_client,
        bucket_name,
        dataset=DATASET_NAME,
        ingested_at=ingested_at,
        file_hash=sha256,
        file_path=object_key,
        content_length=len(scraped.body),
        execution_id=execution_id,
        snapshot_date=target_date.isoformat(),
    )

    logger.info(
        "Hokuriku power_usage snapshot saved: target_date=%s, sha256=%.8s, prefix=%s",
        target_date,
        sha256,
        snapshot_prefix,
    )

    return HokurikuSnapshotResult(
        skipped=False,
        bucket_name=bucket_name,
        target_date=target_date,
        sha256=sha256,
        content_length=len(scraped.body),
        manifest_key=manifest_key,
        ingestion_log_key=DEFAULT_INGESTION_LOG_KEY,
        snapshot_prefix=snapshot_prefix,
    )

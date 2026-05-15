from __future__ import annotations

import gzip
import hashlib
import json
import logging
from dataclasses import dataclass
from datetime import UTC, datetime

from common.http_scraper import BaseHttpScraper, RequestSpec
from common.jepx_common import (
    resolve_fiscal_year,
    resolve_spot_summary_file_name,
    resolve_spot_summary_object_key,
)
from common.storage_client import RustFSClient

logger = logging.getLogger(__name__)


def _resolve_snapshot_prefix(fiscal_year: int, ingested_at: datetime) -> str:
    ts = ingested_at.strftime("%Y%m%dT%H%M%S")
    return f"raw/jepx/spot_price/year={fiscal_year}/ingested_at={ts}"


def _resolve_manifest_key(fiscal_year: int) -> str:
    return f"raw/jepx/spot_price/manifests/year={fiscal_year}/latest.json"


@dataclass(frozen=True)
class JEPXSpotSummaryConfig:
    """Configuration for downloading JEPX spot summary CSV files."""

    base_url: str = "https://www.jepx.jp/_download.php"
    referer: str = "https://www.jepx.jp/electricpower/market-data/spot/"
    timeout_seconds: int = 30


@dataclass(frozen=True)
class JEPXScrapedRawObject:
    """Represents a scraped JEPX CSV object ready for raw storage."""

    object_key: str
    file_name: str
    body: bytes
    content_type: str = "text/csv"
    http_etag: str | None = None
    http_last_modified: str | None = None


class JEPXSpotSummaryScraper(BaseHttpScraper):
    """HTTP scraper for JEPX spot summary CSV files."""

    def __init__(
        self,
        config: JEPXSpotSummaryConfig | None = None,
        session=None,
    ) -> None:
        self.config = config or JEPXSpotSummaryConfig()
        super().__init__(
            timeout_seconds=self.config.timeout_seconds,
            session=session,
        )

    def build_request(self, target_at: datetime) -> RequestSpec:
        fiscal_year = resolve_fiscal_year(target_at)
        timestamp_millis = int(target_at.timestamp() * 1000)

        return RequestSpec(
            method="POST",
            url=self.config.base_url,
            params={"timestamp": str(timestamp_millis)},
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "Referer": self.config.referer,
            },
            data={
                "dir": "spot_summary",
                "file": f"spot_summary_{fiscal_year}.csv",
            },
        )

    def scrape(self, target_at: datetime) -> JEPXScrapedRawObject:
        file_name = resolve_spot_summary_file_name(target_at)
        object_key = resolve_spot_summary_object_key(target_at)
        response = self.fetch_response(target_at)

        return JEPXScrapedRawObject(
            object_key=object_key,
            file_name=file_name,
            body=response.content,
            http_etag=response.headers.get("ETag"),
            http_last_modified=response.headers.get("Last-Modified"),
        )


@dataclass(frozen=True)
class JEPXScrapeUploadResult:
    bucket_name: str
    object_key: str
    file_name: str
    size_bytes: int


def scrape_jepx_to_rustfs(
    storage_client: RustFSClient,
    scraper: JEPXSpotSummaryScraper,
    bucket_name: str,
    target_at: datetime | None = None,
) -> JEPXScrapeUploadResult:
    target_at = target_at or datetime.now(UTC)
    scraped = scraper.scrape(target_at)

    storage_client.upload_bytes(
        bucket_name=bucket_name,
        object_name=scraped.object_key,
        body=scraped.body,
        content_type=scraped.content_type,
    )

    return JEPXScrapeUploadResult(
        bucket_name=bucket_name,
        object_key=scraped.object_key,
        file_name=scraped.file_name,
        size_bytes=len(scraped.body),
    )


@dataclass(frozen=True)
class JEPXSnapshotResult:
    """Result of a JEPX spot price raw snapshot attempt."""

    skipped: bool
    bucket_name: str
    year: int
    sha256: str
    content_length: int
    manifest_key: str
    snapshot_prefix: str | None = None


def scrape_jepx_spot_price_raw(
    storage_client: RustFSClient,
    scraper: JEPXSpotSummaryScraper,
    bucket_name: str,
    target_at: datetime | None = None,
) -> JEPXSnapshotResult:
    """Download JEPX spot price CSV and save a raw snapshot only when the content
    has changed since the last snapshot (SHA256 comparison via manifest).
    """
    target_at = target_at or datetime.now(UTC)
    fiscal_year = resolve_fiscal_year(target_at)
    manifest_key = _resolve_manifest_key(fiscal_year)

    scraped = scraper.scrape(target_at)
    sha256 = hashlib.sha256(scraped.body).hexdigest()

    manifest_bytes = storage_client.get_object_or_none(bucket_name, manifest_key)
    if manifest_bytes is not None:
        previous_hash = json.loads(manifest_bytes).get("sha256")
        if previous_hash == sha256:
            logger.info("JEPX snapshot unchanged (sha256=%.8s), skipping save", sha256)
            return JEPXSnapshotResult(
                skipped=True,
                bucket_name=bucket_name,
                year=fiscal_year,
                sha256=sha256,
                content_length=len(scraped.body),
                manifest_key=manifest_key,
                snapshot_prefix=None,
            )

    ingested_at = datetime.now(UTC)
    snapshot_prefix = _resolve_snapshot_prefix(fiscal_year, ingested_at)

    compressed = gzip.compress(scraped.body)
    storage_client.upload_bytes(
        bucket_name=bucket_name,
        object_name=f"{snapshot_prefix}/spot.csv.gz",
        body=compressed,
        content_type="application/gzip",
    )

    metadata: dict[str, str | int | None] = {
        "source_url": scraper.config.base_url,
        "ingested_at": ingested_at.isoformat(),
        "year": fiscal_year,
        "sha256": sha256,
        "etag": scraped.http_etag,
        "last_modified": scraped.http_last_modified,
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

    logger.info(
        "JEPX snapshot saved: year=%s, sha256=%.8s, prefix=%s",
        fiscal_year,
        sha256,
        snapshot_prefix,
    )

    return JEPXSnapshotResult(
        skipped=False,
        bucket_name=bucket_name,
        year=fiscal_year,
        sha256=sha256,
        content_length=len(scraped.body),
        manifest_key=manifest_key,
        snapshot_prefix=snapshot_prefix,
    )

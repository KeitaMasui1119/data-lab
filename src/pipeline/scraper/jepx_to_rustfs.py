from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

from core.storage_client import RustFSClient
from pipeline.scraper.module.jepx import JEPXSpotSummaryScraper


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

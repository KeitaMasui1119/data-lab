from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime

from common.storage_client import RustFSClient
from pipeline.scraper.module.occto import OCCTOUnitGenerationScraper


@dataclass(frozen=True)
class OCCTOScrapeUploadResult:
    """Upload result metadata for an OCCTO scraped CSV."""

    bucket_name: str
    object_key: str
    file_name: str
    size_bytes: int


def scrape_occto_to_rustfs(
    storage_client: RustFSClient,
    scraper: OCCTOUnitGenerationScraper,
    bucket_name: str,
    target_at: date | datetime,
) -> OCCTOScrapeUploadResult:
    """Scrape OCCTO CSV for a target date and upload it to RustFS raw layer."""
    scraped = scraper.scrape(target_at)

    storage_client.upload_bytes(
        bucket_name=bucket_name,
        object_name=scraped.object_key,
        body=scraped.body,
        content_type=scraped.content_type,
    )

    return OCCTOScrapeUploadResult(
        bucket_name=bucket_name,
        object_key=scraped.object_key,
        file_name=scraped.file_name,
        size_bytes=len(scraped.body),
    )

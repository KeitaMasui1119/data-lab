from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

from common.http_scraper import BaseHttpScraper, RequestSpec
from common.jepx_common import (
    resolve_fiscal_year,
    resolve_spot_summary_file_name,
    resolve_spot_summary_object_key,
)
from common.storage_client import RustFSClient


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
        body = self.fetch(target_at)

        return JEPXScrapedRawObject(
            object_key=object_key,
            file_name=file_name,
            body=body,
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

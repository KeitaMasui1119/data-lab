"""OCCTO source-to-raw scraping and upload workflow."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime

from common.http_scraper import BaseHttpScraper, RequestSpec
from common.storage_client import RustFSClient


@dataclass(frozen=True)
class OCCTOUnitGenerationConfig:
    """Configuration for downloading OCCTO unit generation CSV files."""

    base_url: str
    referer: str | None = None
    date_param_name: str = "targetDate"
    date_format: str = "%Y-%m-%d"
    file_name_template: str = "ユニット別発電実績_{date}.csv"
    object_prefix: str = "raw/occto/unit_generation"
    timeout_seconds: int = 30


@dataclass(frozen=True)
class OCCTOScrapedRawObject:
    """Represents a scraped OCCTO CSV object ready for raw storage."""

    object_key: str
    file_name: str
    body: bytes
    content_type: str = "text/csv"


class OCCTOUnitGenerationScraper(BaseHttpScraper):
    """HTTP scraper for OCCTO unit generation actuals CSV files."""

    def __init__(
        self,
        config: OCCTOUnitGenerationConfig,
        session=None,
    ) -> None:
        self.config = config
        super().__init__(
            timeout_seconds=self.config.timeout_seconds,
            session=session,
        )

    @staticmethod
    def to_target_date(target_at: date | datetime) -> date:
        """Convert date/datetime input to a date object."""
        if isinstance(target_at, datetime):
            if target_at.tzinfo is None:
                target_at = target_at.replace(tzinfo=UTC)
            return target_at.date()
        return target_at

    def build_request(self, target_at: datetime) -> RequestSpec:
        target_date = self.to_target_date(target_at)
        formatted_date = target_date.strftime(self.config.date_format)

        headers: dict[str, str] | None = None
        if self.config.referer:
            headers = {"Referer": self.config.referer}

        return RequestSpec(
            method="GET",
            url=self.config.base_url,
            params={self.config.date_param_name: formatted_date},
            headers=headers,
        )

    def scrape(self, target_at: date | datetime) -> OCCTOScrapedRawObject:
        """Download CSV bytes and construct a raw-layer object definition."""
        target_date = self.to_target_date(target_at)
        formatted_date = target_date.strftime(self.config.date_format)

        file_name = self.config.file_name_template.format(date=formatted_date)
        object_key = f"{self.config.object_prefix.rstrip('/')}/{file_name}"
        body = self.fetch(
            datetime.combine(target_date, datetime.min.time(), tzinfo=UTC)
        )

        return OCCTOScrapedRawObject(
            object_key=object_key,
            file_name=file_name,
            body=body,
        )


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

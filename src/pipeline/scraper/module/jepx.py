from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

from pipeline.scraper.module.base import BaseHttpScraper, RequestSpec


@dataclass(frozen=True)
class JEPXSpotSummaryConfig:
    base_url: str = "https://www.jepx.jp/_download.php"
    referer: str = "https://www.jepx.jp/electricpower/market-data/spot/"
    timeout_seconds: int = 30


@dataclass(frozen=True)
class ScrapedRawObject:
    object_key: str
    file_name: str
    body: bytes
    content_type: str = "text/csv"


class JEPXSpotSummaryScraper(BaseHttpScraper):
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

    @staticmethod
    def get_fiscal_year(target_at: datetime) -> int:
        if target_at.month >= 4:
            return target_at.year
        return target_at.year - 1

    @staticmethod
    def to_timestamp_millis(target_at: datetime) -> int:
        if target_at.tzinfo is None:
            target_at = target_at.replace(tzinfo=UTC)
        return int(target_at.timestamp() * 1000)

    def build_request(self, target_at: datetime) -> RequestSpec:
        fiscal_year = self.get_fiscal_year(target_at)
        timestamp_millis = self.to_timestamp_millis(target_at)

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

    def scrape(self, target_at: datetime) -> ScrapedRawObject:
        fiscal_year = self.get_fiscal_year(target_at)
        file_name = f"spot_summary_{fiscal_year}.csv"
        object_key = f"raw/jepx/spot_summary/{file_name}"
        body = self.fetch(target_at)

        return ScrapedRawObject(
            object_key=object_key,
            file_name=file_name,
            body=body,
        )

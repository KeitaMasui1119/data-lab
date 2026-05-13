from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from common.http_scraper import BaseHttpScraper, RequestSpec
from pipeline.jepx.common import (
    resolve_fiscal_year,
    resolve_spot_summary_file_name,
    resolve_spot_summary_object_key,
)


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

    def scrape(self, target_at: datetime) -> ScrapedRawObject:
        file_name = resolve_spot_summary_file_name(target_at)
        object_key = resolve_spot_summary_object_key(target_at)
        body = self.fetch(target_at)

        return ScrapedRawObject(
            object_key=object_key,
            file_name=file_name,
            body=body,
        )

"""OCCTO source-to-raw scraping and upload workflow.

OCCTO's public disclosure system requires a 4-step requests.Session flow:
  GET home → POST disclaimer-agree/next (agreed=0) → POST /info/hks/search
  → GET downloadCsv
Cookie alone is not enough; the server requires the search POST to establish
state before the CSV download will succeed (502 otherwise).
See docs/tasks/occto_pipeline_ingestion.md for the reverse-engineered details.
"""

from __future__ import annotations

import hashlib
import io
import json
import logging
from dataclasses import dataclass
from datetime import UTC, date, datetime

import polars as pl

from common.http_scraper import BaseHttpScraper, RequestSpec
from common.raw_ingestion_log import DEFAULT_INGESTION_LOG_KEY, load_ingestion_log
from common.storage_client import RustFSClient

logger = logging.getLogger(__name__)

HOME_URL = "https://hatsuden-kokai.occto.or.jp/hks-web-public/home"
DISCLAIMER_AGREE_URL = (
    "https://hatsuden-kokai.occto.or.jp/hks-web-public/disclaimer-agree/next"
)
SEARCH_URL = "https://hatsuden-kokai.occto.or.jp/hks-web-public/info/hks/search"
DOWNLOAD_CSV_URL = (
    "https://hatsuden-kokai.occto.or.jp/hks-web-public/info/hks/downloadCsv"
)

# All area/method codes must be listed explicitly; omitting any narrows the
# result set instead of erroring, so "all" is spelled out rather than assumed.
ALL_AREA_CODES: tuple[str, ...] = (
    "99",
    "01",
    "02",
    "03",
    "04",
    "05",
    "06",
    "07",
    "08",
    "09",
    "10",
)
ALL_METHOD_CODES: tuple[str, ...] = (
    "99",
    "01",
    "02",
    "03",
    "04",
    "05",
    "06",
    "07",
    "08",
    "09",
)

# assets/js/HKSHS004.js toggles the hidden "agreed" field in an inverted way:
# agreeing to the disclaimer actually sends "0", not "1" (verified live).
DISCLAIMER_AGREED_VALUE = "0"

FILE_NAME_TEMPLATE = "ユニット別発電実績_{date_label}.csv"
OBJECT_PREFIX = "raw/occto/unit_generation"
DATASET_NAME = "occto_unit_generation"


# resolve_default_target_date() moved to common/utilities.py (shared by every
# denki-yohou dataset). OCCTO's own rationale for "yesterday": it publishes
# each day's actuals around 15:30 JST the following day (see
# plan_occto_pipeline.md Phase 0), so "yesterday" is the latest date that is
# reliably already published.


def _date_label(from_date: date, to_date: date) -> str:
    if from_date == to_date:
        return from_date.isoformat()
    return f"{from_date.isoformat()}_{to_date.isoformat()}"


def _resolve_snapshot_prefix(from_date: date, ingested_at: datetime) -> str:
    ts = ingested_at.strftime("%Y%m%dT%H%M%S")
    return f"{OBJECT_PREFIX}/target_date={from_date.isoformat()}/ingested_at={ts}"


def _resolve_manifest_key(from_date: date) -> str:
    return f"{OBJECT_PREFIX}/manifests/target_date={from_date.isoformat()}/latest.json"


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
    target_date: date,
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
                (pl.col("dataset") == DATASET_NAME)
                & (pl.col("snapshot_date") == target_date.isoformat())
            )
            .then(pl.lit(False))
            .otherwise(pl.col("is_latest"))
            .alias("is_latest")
        )

    new_row = pl.DataFrame(
        {
            "dataset": pl.Series([DATASET_NAME], dtype=pl.Utf8),
            "fiscal_year": pl.Series([None], dtype=pl.Int64),
            "snapshot_date": pl.Series([target_date.isoformat()], dtype=pl.Utf8),
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
class OCCTOUnitGenerationConfig:
    """Configuration for downloading OCCTO unit generation CSV files."""

    home_url: str = HOME_URL
    disclaimer_agree_url: str = DISCLAIMER_AGREE_URL
    search_url: str = SEARCH_URL
    download_csv_url: str = DOWNLOAD_CSV_URL
    area_codes: tuple[str, ...] = ALL_AREA_CODES
    method_codes: tuple[str, ...] = ALL_METHOD_CODES
    date_format: str = "%Y/%m/%d"
    timeout_seconds: int = 30


@dataclass(frozen=True)
class OCCTOScrapedRawObject:
    """Represents a scraped OCCTO CSV object ready for raw storage."""

    file_name: str
    body: bytes
    content_type: str = "text/csv"


class OCCTOUnitGenerationScraper(BaseHttpScraper):
    """HTTP scraper for OCCTO unit generation actuals CSV files.

    Requires session state from a disclaimer agreement before the CSV
    download request will succeed, so `prepare()` performs the GET home /
    POST disclaimer-agree/next steps ahead of the download.
    """

    def __init__(
        self,
        config: OCCTOUnitGenerationConfig | None = None,
        session=None,
    ) -> None:
        self.config = config or OCCTOUnitGenerationConfig()
        super().__init__(
            timeout_seconds=self.config.timeout_seconds,
            session=session,
        )

    def prepare(self, target_at: datetime) -> None:
        self.session.get(self.config.home_url, timeout=self.timeout_seconds)
        self.session.post(
            self.config.disclaimer_agree_url,
            data={"agreed": DISCLAIMER_AGREED_VALUE},
            timeout=self.timeout_seconds,
        )

    def _build_search_request(self, from_date: date, to_date: date) -> RequestSpec:
        """POST /info/hks/search — must precede downloadCsv to set server-side state."""
        data: dict[str, object] = {
            "htdnsCd": "",
            "htdnsNm": "",
            "unitNm": "",
            "areaCheckbox": list(self.config.area_codes),
            "hatudenHosikiCheckbox": list(self.config.method_codes),
            "tgtDateDateFrom": from_date.strftime(self.config.date_format),
            "tgtDateDateTo": to_date.strftime(self.config.date_format),
        }
        return RequestSpec(method="POST", url=self.config.search_url, data=data)

    def _build_download_request(self, from_date: date, to_date: date) -> RequestSpec:
        params: dict[str, object] = {
            "htdnsCd": "",
            "htdnsNm": "",
            "unitNm": "",
            "areaCheckbox": list(self.config.area_codes),
            "hatudenHosikiCheckbox": list(self.config.method_codes),
            "tgtDateDateFrom": from_date.strftime(self.config.date_format),
            "tgtDateDateTo": to_date.strftime(self.config.date_format),
        }
        return RequestSpec(
            method="GET", url=self.config.download_csv_url, params=params
        )

    def build_request(self, target_at: datetime) -> RequestSpec:
        target_date = target_at.date() if isinstance(target_at, datetime) else target_at
        return self._build_download_request(target_date, target_date)

    def scrape(
        self, from_date: date, to_date: date | None = None
    ) -> OCCTOScrapedRawObject:
        """Run the full 4-step flow and return the downloaded CSV bytes."""
        to_date = to_date or from_date
        self.prepare(datetime.combine(from_date, datetime.min.time(), tzinfo=UTC))
        self._execute(self._build_search_request(from_date, to_date))
        spec = self._build_download_request(from_date, to_date)
        response = self._execute(spec)

        file_name = FILE_NAME_TEMPLATE.format(
            date_label=_date_label(from_date, to_date)
        )
        return OCCTOScrapedRawObject(file_name=file_name, body=response.content)


@dataclass(frozen=True)
class OCCTOSnapshotResult:
    """Result of an OCCTO unit generation raw snapshot attempt."""

    skipped: bool
    bucket_name: str
    from_date: date
    to_date: date
    sha256: str
    content_length: int
    manifest_key: str
    ingestion_log_key: str
    snapshot_prefix: str | None = None


def run_source_to_raw_occto_unit_generation_actuals(
    storage_client: RustFSClient,
    scraper: OCCTOUnitGenerationScraper,
    bucket_name: str,
    from_date: date,
    to_date: date | None = None,
) -> OCCTOSnapshotResult:
    """Download OCCTO unit generation CSV and save a raw snapshot only when
    the content has changed since the last snapshot for this target date
    (SHA256 comparison via a per-date manifest).
    """
    to_date = to_date or from_date
    manifest_key = _resolve_manifest_key(from_date)

    scraped = scraper.scrape(from_date, to_date)
    sha256 = hashlib.sha256(scraped.body).hexdigest()

    manifest_bytes = storage_client.get_object_or_none(bucket_name, manifest_key)
    if manifest_bytes is not None:
        previous_hash = json.loads(manifest_bytes).get("sha256")
        if previous_hash == sha256:
            logger.info(
                "OCCTO snapshot unchanged (target_date=%s, sha256=%.8s), skipping save",
                from_date,
                sha256,
            )
            return OCCTOSnapshotResult(
                skipped=True,
                bucket_name=bucket_name,
                from_date=from_date,
                to_date=to_date,
                sha256=sha256,
                content_length=len(scraped.body),
                manifest_key=manifest_key,
                ingestion_log_key=DEFAULT_INGESTION_LOG_KEY,
                snapshot_prefix=None,
            )

    ingested_at = datetime.now(UTC)
    snapshot_prefix = _resolve_snapshot_prefix(from_date, ingested_at)
    object_key = f"{snapshot_prefix}/{scraped.file_name}"

    storage_client.upload_bytes(
        bucket_name=bucket_name,
        object_name=object_key,
        body=scraped.body,
        content_type=scraped.content_type,
    )

    metadata: dict[str, str | int | None] = {
        "source_url": scraper.config.download_csv_url,
        "ingested_at": ingested_at.isoformat(),
        "target_date": from_date.isoformat(),
        "to_date": to_date.isoformat(),
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
        target_date=from_date,
        ingested_at=ingested_at,
        file_hash=sha256,
        file_path=object_key,
        content_length=len(scraped.body),
    )

    logger.info(
        "OCCTO snapshot saved: target_date=%s, sha256=%.8s, prefix=%s",
        from_date,
        sha256,
        snapshot_prefix,
    )

    return OCCTOSnapshotResult(
        skipped=False,
        bucket_name=bucket_name,
        from_date=from_date,
        to_date=to_date,
        sha256=sha256,
        content_length=len(scraped.body),
        manifest_key=manifest_key,
        ingestion_log_key=DEFAULT_INGESTION_LOG_KEY,
        snapshot_prefix=snapshot_prefix,
    )

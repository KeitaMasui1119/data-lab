import argparse
import logging

from common.storage_client import RustFSClient
from pipeline.jepx.common import resolve_target_at
from pipeline.raw.jepx_to_rustfs import scrape_jepx_to_rustfs
from pipeline.scraper.module.jepx import JEPXSpotSummaryScraper

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Scrape JEPX spot summary and upload to RustFS"
    )
    parser.add_argument(
        "--bucket",
        default="jp-power-grid-dev",
        help="Target bucket name (default: jp-power-grid-dev)",
    )
    parser.add_argument(
        "--timestamp-ms",
        type=int,
        help="Optional UNIX timestamp in milliseconds for the JEPX request",
    )
    args = parser.parse_args()

    target_at = resolve_target_at(args.timestamp_ms)

    rustfs = RustFSClient()
    scraper = JEPXSpotSummaryScraper()
    try:
        result = scrape_jepx_to_rustfs(
            storage_client=rustfs,
            scraper=scraper,
            bucket_name=args.bucket,
            target_at=target_at,
        )
        logger.info(
            "Uploaded JEPX raw file to s3://%s/%s (%s bytes)",
            result.bucket_name,
            result.object_key,
            result.size_bytes,
        )
    finally:
        scraper.close()


if __name__ == "__main__":
    main()

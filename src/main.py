import argparse
import logging
from datetime import UTC, datetime

from core.storage_client import RustFSClient
from pipeline.bootstrap.rustfs_bootstrap import BucketPlan, apply_bucket_plans
from pipeline.scraper.jepx_to_rustfs import scrape_jepx_to_rustfs
from pipeline.scraper.module.jepx import JEPXSpotSummaryScraper

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)

DEFAULT_PREFIXES = ("raw", "bronze", "silver", "gold", "sandbox")
DEFAULT_BUCKET_PLANS = [
    BucketPlan(
        name="jp-power-grid-dev",
        prefixes=DEFAULT_PREFIXES,
        retention_days=None,
    ),
    BucketPlan(
        name="jp-power-grid-prd",
        prefixes=DEFAULT_PREFIXES,
        retention_days=7,
        retention_mode="COMPLIANCE",
    ),
]


def main():
    parser = argparse.ArgumentParser(description="Data platform orchestrator")
    subparsers = parser.add_subparsers(dest="command")

    bootstrap_parser = subparsers.add_parser(
        "bootstrap-storage",
        help="Create and configure RustFS buckets",
    )
    bootstrap_parser.add_argument(
        "--bucket",
        action="append",
        dest="buckets",
        help="Bucket name to initialize. Default is dev/prd plans.",
    )

    jepx_parser = subparsers.add_parser(
        "scrape-jepx",
        help="Scrape JEPX spot summary and upload to RustFS raw layer",
    )
    jepx_parser.add_argument(
        "--bucket",
        default="jp-power-grid-dev",
        help="Target bucket name (default: jp-power-grid-dev)",
    )
    jepx_parser.add_argument(
        "--timestamp-ms",
        type=int,
        help="Optional UNIX timestamp in milliseconds for the JEPX request",
    )

    args = parser.parse_args()

    if args.command in {None, "bootstrap-storage"}:
        rustfs = RustFSClient()
        if args.command is None:
            logger.info(
                "No command was provided. Running bootstrap-storage for compatibility."
            )

        buckets = getattr(args, "buckets", None)
        if buckets:
            target_set = set(buckets)
            known_names = {plan.name for plan in DEFAULT_BUCKET_PLANS}
            unknown_names = sorted(target_set - known_names)
            if unknown_names:
                parser.error(f"Unknown bucket names: {', '.join(unknown_names)}")

            selected_plans = [
                plan for plan in DEFAULT_BUCKET_PLANS if plan.name in target_set
            ]
        else:
            selected_plans = DEFAULT_BUCKET_PLANS

        created, existing = apply_bucket_plans(rustfs, selected_plans)

        if created:
            logger.info(f"Created buckets: {created}")
        if existing:
            logger.info(f"Existing buckets: {existing}")
        return

    if args.command == "scrape-jepx":
        rustfs = RustFSClient()
        scraper = JEPXSpotSummaryScraper()

        if args.timestamp_ms:
            target_at = datetime.fromtimestamp(
                args.timestamp_ms / 1000,
                tz=UTC,
            )
        else:
            target_at = datetime.now(UTC)

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

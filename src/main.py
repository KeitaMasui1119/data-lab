import argparse
import logging

from core.storage_client import RustFSClient
from pipeline.bootstrap.rustfs_bootstrap import BucketPlan, apply_bucket_plans

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
    parser = argparse.ArgumentParser(description="RustFS bucket bootstrap orchestrator")
    parser.add_argument(
        "--bucket",
        action="append",
        dest="buckets",
        help="Bucket name to initialize. Default is dev/prd plans.",
    )
    args = parser.parse_args()

    rustfs = RustFSClient()
    if args.buckets:
        target_set = set(args.buckets)
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


if __name__ == "__main__":
    main()

"""bootstrap-storage command: create and configure RustFS buckets."""

from __future__ import annotations

import argparse
import logging

from cli.registry import CommandSpec
from common.storage_client import RustFSClient
from setup.rustfs_bucket_setup import BucketPlan, apply_bucket_plans

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


def _configure(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--bucket",
        action="append",
        dest="buckets",
        help="Bucket name to initialize. Default is dev/prd plans.",
    )


def _handle(args: argparse.Namespace, parser: argparse.ArgumentParser) -> None:
    rustfs = RustFSClient()

    # getattr(): when main.py falls back to this command for a bare,
    # argument-less invocation, no subparser ran, so args has no `buckets`
    # attribute at all.
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


COMMAND = CommandSpec(
    name="bootstrap-storage",
    help="Create and configure RustFS buckets",
    configure=_configure,
    handler=_handle,
)

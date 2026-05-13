from collections.abc import Iterable
from dataclasses import dataclass

from core.storage_client import RustFSClient


@dataclass(frozen=True)
class BucketPlan:
    name: str
    prefixes: tuple[str, ...]
    retention_days: int | None = None
    retention_mode: str = "COMPLIANCE"


def ensure_buckets(
    client: RustFSClient, bucket_names: Iterable[str]
) -> tuple[list[str], list[str]]:
    """Ensure required buckets exist and return (created, already_existing)."""
    created: list[str] = []
    existing: list[str] = []

    for bucket_name in bucket_names:
        if client.ensure_bucket(bucket_name):
            created.append(bucket_name)
        else:
            existing.append(bucket_name)

    return created, existing


def apply_bucket_plan(client: RustFSClient, plan: BucketPlan) -> bool:
    created = client.ensure_bucket(
        plan.name,
        object_lock_enabled=plan.retention_days is not None,
    )
    client.ensure_prefixes(plan.name, list(plan.prefixes))

    if plan.retention_days is None:
        client.clear_default_object_lock_retention(plan.name)
    else:
        client.set_default_object_lock_retention(
            bucket_name=plan.name,
            mode=plan.retention_mode,
            days=plan.retention_days,
        )

    return created


def apply_bucket_plans(
    client: RustFSClient, plans: Iterable[BucketPlan]
) -> tuple[list[str], list[str]]:
    created: list[str] = []
    existing: list[str] = []

    for plan in plans:
        if apply_bucket_plan(client, plan):
            created.append(plan.name)
        else:
            existing.append(plan.name)

    return created, existing

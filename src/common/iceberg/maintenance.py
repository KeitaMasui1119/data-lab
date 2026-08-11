"""Snapshot expiration and orphan data file cleanup for Iceberg tables.

Iceberg's overwrite() only drops old files from the manifest; it never
deletes them from storage, because past snapshots keep referencing them for
time-travel. Expiring old snapshots (metadata only) is what makes their
exclusively-referenced files eligible for physical deletion -- PyIceberg
0.11.1 has no built-in orphan-file removal, so that half is implemented here
by diffing what remaining snapshots reference against what's actually in
storage. See docs/tasks/tasks.md section 7.
"""

from __future__ import annotations

from datetime import datetime

from pyiceberg.catalog import Catalog

from common.logging_utils import get_logger
from common.storage_client import RustFSClient

logger = get_logger(__name__)


def expire_old_snapshots(
    catalog: Catalog, identifier: str, older_than: datetime
) -> list[int]:
    """Expire snapshots older than a cutoff. The branch HEAD is always kept.

    Metadata-only: does not touch data files. Returns the expired snapshot
    IDs.
    """
    table = catalog.load_table(identifier)
    before_ids = {snapshot.snapshot_id for snapshot in table.snapshots()}

    table.maintenance.expire_snapshots().older_than(older_than).commit()

    table = catalog.load_table(identifier)
    after_ids = {snapshot.snapshot_id for snapshot in table.snapshots()}
    expired = sorted(before_ids - after_ids)

    if expired:
        logger.info(
            "Expired %s snapshot(s) for '%s': %s", len(expired), identifier, expired
        )
    else:
        logger.info("No snapshots older than %s for '%s'", older_than, identifier)

    return expired


def find_orphan_data_files(
    catalog: Catalog,
    identifier: str,
    client: RustFSClient,
    bucket_name: str,
) -> list[str]:
    """Return object keys under the table's data/ prefix that no surviving
    snapshot references.

    Read-only: lists storage and every remaining snapshot's manifest, but
    deletes nothing. Call this only after expire_old_snapshots() so the
    "surviving snapshots" set reflects what should actually be kept.
    """
    table = catalog.load_table(identifier)

    referenced: set[str] = set()
    for snapshot in table.snapshots():
        files = table.inspect.data_files(snapshot_id=snapshot.snapshot_id).to_pylist()
        referenced.update(entry["file_path"] for entry in files)

    namespace, table_name = identifier.split(".", 1)
    prefix = f"{namespace}/{table_name}/data/"
    paginator = client.s3.get_paginator("list_objects_v2")

    all_keys: list[str] = []
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        for obj in page.get("Contents", []):
            all_keys.append(f"s3://{bucket_name}/{obj['Key']}")

    orphans = [key for key in all_keys if key not in referenced]
    logger.info(
        "Table '%s': %s files in storage, %s referenced, %s orphaned",
        identifier,
        len(all_keys),
        len(referenced),
        len(orphans),
    )
    return orphans


def delete_orphan_data_files(
    client: RustFSClient, bucket_name: str, object_keys: list[str]
) -> int:
    """Physically delete orphaned data files. Irreversible.

    Batches in chunks of 1000 keys (the S3 DeleteObjects limit).
    """
    if not object_keys:
        return 0

    prefix = f"s3://{bucket_name}/"
    keys = [key.removeprefix(prefix) for key in object_keys]

    deleted = 0
    for start in range(0, len(keys), 1000):
        chunk = keys[start : start + 1000]
        response = client.s3.delete_objects(
            Bucket=bucket_name,
            Delete={"Objects": [{"Key": key} for key in chunk]},
        )
        deleted += len(response.get("Deleted", []))
        if "Errors" in response:
            raise RuntimeError(f"Errors deleting orphan files: {response['Errors']}")

    logger.info("Deleted %s orphan file(s) from bucket '%s'", deleted, bucket_name)
    return deleted

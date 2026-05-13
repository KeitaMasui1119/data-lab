"""
Script to migrate OCCTO CSV files from local data/occto to RustFS raw/occto.
"""

import logging
import shutil
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent.parent))

from core.storage_client import RustFSClient

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)


def migrate_occto_data(
    local_dir: str,
    bucket_name: str = "jp-power-grid-dev",
    s3_prefix: str = "raw/occto",
    delete_after: bool = True,
) -> int:
    """
    Migrate OCCTO CSV files from local directory to RustFS.

    Args:
        local_dir: Path to local data/occto directory
        bucket_name: Destination S3 bucket name
        s3_prefix: Destination S3 prefix (default: raw/occto)
        delete_after: Delete local directory after successful migration

    Returns:
        Number of files migrated
    """
    local_path = Path(local_dir)
    if not local_path.exists():
        raise FileNotFoundError(f"Local directory not found: {local_dir}")

    client = RustFSClient()

    # 1. Create s3://jp-power-grid-dev/raw/occto/ prefix
    logger.info("Creating S3 prefix: s3://%s/%s/", bucket_name, s3_prefix)
    client.ensure_prefixes(bucket_name, [s3_prefix])

    # 2. Find all ユニット別発電実績_*.csv files
    csv_files = list(local_path.glob("ユニット別発電実績_*.csv"))
    if not csv_files:
        logger.warning("No CSV files matching pattern 'ユニット別発電実績_*.csv' found")
        return 0

    logger.info("Found %d CSV files to migrate", len(csv_files))

    # 3. Upload each file
    migrated_count = 0
    for csv_file in csv_files:
        file_name = csv_file.name
        s3_key = f"{s3_prefix}/{file_name}"

        try:
            logger.info(
                "Uploading %s to s3://%s/%s",
                file_name,
                bucket_name,
                s3_key,
            )
            client.upload_file(
                bucket_name=bucket_name,
                file_path=str(csv_file),
                object_name=s3_key,
            )
            migrated_count += 1
        except Exception as e:
            logger.error("Failed to upload %s: %s", file_name, e)
            raise

    logger.info("Successfully migrated %d files to RustFS", migrated_count)

    # 4. Delete local directory if requested
    if delete_after:
        logger.info("Deleting local directory: %s", local_path)
        shutil.rmtree(local_path)
        logger.info("Local directory deleted successfully")

    return migrated_count


if __name__ == "__main__":
    try:
        migrated = migrate_occto_data(
            local_dir="/workspace/data/occto",
            bucket_name="jp-power-grid-dev",
            s3_prefix="raw/occto",
            delete_after=True,
        )
        logger.info("Migration completed: %d files", migrated)
    except Exception as e:
        logger.error("Migration failed: %s", e)
        sys.exit(1)

import argparse
import logging
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent.parent))

from core.storage_client import RustFSClient

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)


def upload_raw_file(
    client: RustFSClient,
    file_path: str,
    bucket_name: str,
    object_key: str,
) -> None:
    if not Path(file_path).exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    logger.info(f"Uploading {file_path} to s3://{bucket_name}/{object_key}...")
    client.upload_file(
        bucket_name=bucket_name,
        file_path=file_path,
        object_name=object_key,
    )
    logger.info("Upload completed successfully.")


def main():
    parser = argparse.ArgumentParser(
        description="Script to upload local CSV files to RustFS (raw layer)"
    )
    parser.add_argument(
        "--file", required=True, help="Path to the local CSV file to upload"
    )
    parser.add_argument(
        "--bucket",
        default="jp-power-grid-dev",
        help="Destination bucket name (default: jp-power-grid-dev)",
    )
    parser.add_argument(
        "--key",
        required=True,
        help="Destination path/filename (e.g., raw/jepx/20260412.csv)",
    )

    args = parser.parse_args()

    try:
        client = RustFSClient()
        upload_raw_file(
            client=client,
            file_path=args.file,
            bucket_name=args.bucket,
            object_key=args.key,
        )
    except Exception as e:
        logger.error(f"Upload failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

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

    if not Path(args.file).exists():
        logger.error(f"File not found: {args.file}")
        sys.exit(1)

    logger.info("Starting upload process...")

    try:
        client = RustFSClient()

        logger.info(f"Uploading {args.file} to s3://{args.bucket}/{args.key}...")
        client.upload_file(
            bucket_name=args.bucket, file_path=args.file, object_name=args.key
        )

        logger.info("Upload completed successfully.")
    except Exception as e:
        logger.error(f"Upload failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

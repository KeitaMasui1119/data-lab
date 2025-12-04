# src/stg/pydev/test_gcs_upload.py
from __future__ import annotations

from datetime import UTC, datetime

from src.stg.gcs.client import GCSClient

# ★ここはあなたの GCS バケット名に変えてください
BUCKET_NAME = "nf-nwa9-igvwik-l9y7"  # 例


def main() -> None:
    client = GCSClient()

    now = datetime.now(UTC)
    timestamp = now.strftime("%Y%m%dT%H%M%SZ")

    # raw のテストディレクトリに適当にアップロードしてみる
    object_path = f"raw/test/manual_upload/test_{timestamp}.txt"

    text = f"Hello GCS! timestamp={timestamp}"

    uri = client.upload_text(
        bucket_name=BUCKET_NAME,
        object_path=object_path,
        text=text,
    )

    print(f"Uploaded to {uri}")


if __name__ == "__main__":
    main()

# src/stg/gcs/client.py
from __future__ import annotations

from pathlib import Path
from typing import Any

from google.cloud import storage


class GCSClient:
    """GCS へのアップロード・ダウンロードを行う簡単なクライアント."""

    def __init__(self, project: str | None = None) -> None:
        # project=None なら GOOGLE_APPLICATION_CREDENTIALS から推論される
        self._client = storage.Client(project=project)

    def upload_file(
        self,
        bucket_name: str,
        object_path: str,
        local_path: Path,
        content_type: str | None = None,
    ) -> str:
        """ローカルファイルを GCS にアップロードする."""
        bucket = self._client.bucket(bucket_name)
        blob = bucket.blob(object_path)

        if content_type:
            blob.content_type = content_type

        blob.upload_from_filename(str(local_path))

        return f"gs://{bucket_name}/{object_path}"

    def upload_text(
        self,
        bucket_name: str,
        object_path: str,
        text: str,
        content_type: str = "text/plain; charset=utf-8",
    ) -> str:
        """テキストを直接アップロードする."""
        bucket = self._client.bucket(bucket_name)
        blob = bucket.blob(object_path)
        blob.upload_from_string(text, content_type=content_type)
        return f"gs://{bucket_name}/{object_path}"

    def upload_json(
        self,
        bucket_name: str,
        object_path: str,
        data: dict[str, Any],
    ) -> str:
        """dict を JSON としてアップロードする."""
        import json

        text = json.dumps(data, ensure_ascii=False)
        return self.upload_text(
            bucket_name=bucket_name,
            object_path=object_path,
            text=text,
            content_type="application/json; charset=utf-8",
        )

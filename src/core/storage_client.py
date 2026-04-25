import logging
import os
from dataclasses import dataclass

import boto3
from botocore.client import BaseClient
from botocore.config import Config
from botocore.exceptions import ClientError

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RustFSConfig:
    endpoint_url: str
    access_key: str
    secret_key: str
    region: str = "us-east-1"

    @classmethod
    def from_env(cls) -> "RustFSConfig":
        endpoint_url = os.environ.get("AWS_ENDPOINT_URL", "").strip()
        access_key = os.environ.get("AWS_ACCESS_KEY_ID", "").strip()
        secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY", "").strip()
        region = os.environ.get("AWS_REGION", "us-east-1").strip()

        if not endpoint_url:
            raise ValueError("AWS_ENDPOINT_URL is not set.")
        if not access_key or not secret_key:
            raise ValueError("AWS_ACCESS_KEY_ID or AWS_SECRET_ACCESS_KEY is not set.")

        return cls(
            endpoint_url=endpoint_url,
            access_key=access_key,
            secret_key=secret_key,
            region=region,
        )


class RustFSClient:
    def __init__(
        self,
        config: RustFSConfig | None = None,
        s3_client: BaseClient | None = None,
    ):
        self.config = config or RustFSConfig.from_env()
        self.s3 = s3_client or boto3.client(
            "s3",
            endpoint_url=self.config.endpoint_url,
            aws_access_key_id=self.config.access_key,
            aws_secret_access_key=self.config.secret_key,
            config=Config(signature_version="s3v4"),
            region_name=self.config.region,
        )

    def get_storage_options(self):
        """外部ライブラリ(Polars等)に渡すためのS3接続設定を辞書で返す"""
        return {
            "key": self.config.access_key,
            "secret": self.config.secret_key,
            "endpoint_url": self.config.endpoint_url,
            "client_kwargs": {"region_name": self.config.region},
        }

    def upload_file(
        self, bucket_name: str, file_path: str, object_name: str | None = None
    ) -> None:
        if object_name is None:
            object_name = os.path.basename(file_path)

        try:
            self.s3.upload_file(file_path, bucket_name, object_name)
            logger.info(f"File {file_path} uploaded to {bucket_name}/{object_name}")
        except Exception as e:
            logger.error(f"Error uploading file: {e}")
            raise

    def upload_bytes(
        self,
        bucket_name: str,
        object_name: str,
        body: bytes,
        content_type: str | None = None,
    ) -> None:
        params: dict[str, str | bytes] = {
            "Bucket": bucket_name,
            "Key": object_name,
            "Body": body,
        }
        if content_type:
            params["ContentType"] = content_type

        try:
            self.s3.put_object(**params)
            logger.info(f"Uploaded bytes to {bucket_name}/{object_name}")
        except Exception as e:
            logger.error(f"Error uploading bytes: {e}")
            raise

    def download_file(self, bucket_name: str, object_name: str, file_path: str) -> None:
        try:
            self.s3.download_file(bucket_name, object_name, file_path)
            logger.info(
                f"File {object_name} downloaded from {bucket_name} to {file_path}"
            )
        except Exception as e:
            logger.error(f"Error downloading file: {e}")
            raise

    def list_files(self, bucket_name: str, prefix: str = "") -> list[str]:
        try:
            response = self.s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
            files = [obj["Key"] for obj in response.get("Contents", [])]
            logger.info(f"Files in {bucket_name}/{prefix}: {files}")
            return files
        except Exception as e:
            logger.error(f"Error listing files: {e}")
            raise

    def delete_file(self, bucket_name: str, object_name: str) -> None:
        try:
            self.s3.delete_object(Bucket=bucket_name, Key=object_name)
            logger.info(f"File {object_name} deleted from {bucket_name}")
        except Exception as e:
            logger.error(f"Error deleting file: {e}")
            raise

    def delete_folder(self, bucket_name: str, prefix: str):
        """
        指定したプレフィックス（フォルダパス）に一致する全てのファイルを一括削除する。
        例: delete_folder("jepx-raw", "bronze.jepx_spot_price/")
        """
        try:
            # 1. プレフィックスに一致するオブジェクトをリストアップ
            response = self.s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

            if "Contents" not in response:
                logger.info(
                    f"No files found with prefix '{prefix}' in bucket '{bucket_name}'"
                )
                return

            # 2. bto3 の delete_objects が要求する形式のリストを作成
            objects_to_delete = [{"Key": obj["Key"]} for obj in response["Contents"]]

            # 3. 一括削除を実行
            delete_response = self.s3.delete_objects(
                Bucket=bucket_name, Delete={"Objects": objects_to_delete}
            )

            deleted_count = len(delete_response.get("Deleted", []))
            logger.info(
                f"Successfully deleted {deleted_count} objects under prefix '{prefix}'"
            )

            # エラーがあった場合はログに出力
            if "Errors" in delete_response:
                logger.error(
                    f"Errors occurred during batch delete: {delete_response['Errors']}"
                )
                raise Exception(
                    f"Errors occurred during batch delete: {delete_response['Errors']}"
                )
        except Exception as e:
            logger.error(f"Error deleting folder '{prefix}': {e}")
            raise

    def create_bucket(
        self, bucket_name: str, object_lock_enabled: bool = False
    ) -> None:
        try:
            params: dict[str, str | bool] = {"Bucket": bucket_name}
            if object_lock_enabled:
                params["ObjectLockEnabledForBucket"] = True

            self.s3.create_bucket(**params)
            logger.info(f"Bucket {bucket_name} created")
        except Exception as e:
            logger.error(f"Error creating bucket: {e}")
            raise

    def bucket_exists(self, bucket_name: str) -> bool:
        try:
            self.s3.head_bucket(Bucket=bucket_name)
            return True
        except ClientError as error:
            error_code = error.response.get("Error", {}).get("Code", "")
            if error_code in {"404", "NoSuchBucket", "NotFound"}:
                return False

            logger.error(f"Error checking bucket existence: {error}")
            raise

    def ensure_bucket(
        self, bucket_name: str, object_lock_enabled: bool = False
    ) -> bool:
        if self.bucket_exists(bucket_name):
            logger.info(f"Bucket {bucket_name} already exists")
            return False

        self.create_bucket(bucket_name, object_lock_enabled=object_lock_enabled)
        return True

    def ensure_prefixes(self, bucket_name: str, prefixes: list[str]) -> None:
        for prefix in prefixes:
            normalized = prefix if prefix.endswith("/") else f"{prefix}/"
            self.s3.put_object(Bucket=bucket_name, Key=normalized, Body=b"")
            logger.info(f"Ensured prefix s3://{bucket_name}/{normalized}")

    def set_default_object_lock_retention(
        self, bucket_name: str, mode: str, days: int
    ) -> None:
        if mode not in {"COMPLIANCE", "GOVERNANCE"}:
            raise ValueError("mode must be COMPLIANCE or GOVERNANCE")
        if days <= 0:
            raise ValueError("days must be greater than 0")

        if not self.bucket_supports_object_lock(bucket_name):
            raise RuntimeError(
                "Bucket does not support Object Lock. "
                "Create the bucket with ObjectLockEnabledForBucket=True first."
            )

        self.s3.put_object_lock_configuration(
            Bucket=bucket_name,
            ObjectLockConfiguration={
                "ObjectLockEnabled": "Enabled",
                "Rule": {
                    "DefaultRetention": {
                        "Mode": mode,
                        "Days": days,
                    }
                },
            },
        )
        logger.info(
            "Set default object lock retention for %s: mode=%s, days=%s",
            bucket_name,
            mode,
            days,
        )

    def bucket_supports_object_lock(self, bucket_name: str) -> bool:
        try:
            response = self.s3.get_object_lock_configuration(Bucket=bucket_name)
            configuration = response.get("ObjectLockConfiguration", {})
            return configuration.get("ObjectLockEnabled") == "Enabled"
        except ClientError as error:
            error_code = error.response.get("Error", {}).get("Code", "")
            if error_code in {
                "ObjectLockConfigurationNotFoundError",
                "InvalidBucketState",
                "InvalidRequest",
            }:
                return False
            raise

    def clear_default_object_lock_retention(self, bucket_name: str) -> bool:
        if not self.bucket_supports_object_lock(bucket_name):
            logger.info(
                "Skip clearing object lock retention for %s because object lock is "
                "disabled",
                bucket_name,
            )
            return False

        self.s3.put_object_lock_configuration(
            Bucket=bucket_name,
            ObjectLockConfiguration={"ObjectLockEnabled": "Enabled"},
        )
        logger.info("Cleared default object lock retention for %s", bucket_name)
        return True

    def get_object(self, bucket_name: str, object_name: str) -> bytes:
        try:
            response = self.s3.get_object(Bucket=bucket_name, Key=object_name)
            logger.info(f"Object {object_name} retrieved from {bucket_name}")
            body = response.get("Body")
            if body is None:
                raise ValueError(
                    f"Body is empty for object: {bucket_name}/{object_name}"
                )
            return body.read()
        except Exception as e:
            logger.error(f"Error getting object: {e}")
            raise

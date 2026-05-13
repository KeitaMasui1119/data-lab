"""Shared RustFS (S3-compatible) client utilities."""

from __future__ import annotations

import os
from dataclasses import dataclass

import boto3
from botocore.client import BaseClient
from botocore.config import Config
from botocore.exceptions import ClientError

from common.module.logging_utils import get_logger

logger = get_logger(__name__)


@dataclass(frozen=True)
class RustFSConfig:
    """Connection settings for RustFS/S3-compatible object storage."""

    endpoint_url: str
    access_key: str
    secret_key: str
    region: str = "us-east-1"

    @classmethod
    def from_env(cls) -> RustFSConfig:
        """Build storage configuration from environment variables."""
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
    """Wrapper around boto3 S3 client for RustFS operations."""

    def __init__(
        self,
        config: RustFSConfig | None = None,
        s3_client: BaseClient | None = None,
    ) -> None:
        self.config = config or RustFSConfig.from_env()
        self.s3 = s3_client or boto3.client(
            "s3",
            endpoint_url=self.config.endpoint_url,
            aws_access_key_id=self.config.access_key,
            aws_secret_access_key=self.config.secret_key,
            config=Config(signature_version="s3v4"),
            region_name=self.config.region,
        )

    def get_storage_options(self) -> dict[str, str | dict[str, str]]:
        """Return object storage options consumable by libraries like Polars."""
        return {
            "key": self.config.access_key,
            "secret": self.config.secret_key,
            "endpoint_url": self.config.endpoint_url,
            "client_kwargs": {"region_name": self.config.region},
        }

    def upload_file(
        self, bucket_name: str, file_path: str, object_name: str | None = None
    ) -> None:
        """Upload a local file to object storage."""
        if object_name is None:
            object_name = os.path.basename(file_path)

        try:
            self.s3.upload_file(file_path, bucket_name, object_name)
            logger.info(
                "File %s uploaded to %s/%s", file_path, bucket_name, object_name
            )
        except Exception as error:  # noqa: BLE001
            logger.error("Error uploading file: %s", error)
            raise

    def upload_bytes(
        self,
        bucket_name: str,
        object_name: str,
        body: bytes,
        content_type: str | None = None,
    ) -> None:
        """Upload in-memory bytes to object storage."""
        params: dict[str, str | bytes] = {
            "Bucket": bucket_name,
            "Key": object_name,
            "Body": body,
        }
        if content_type:
            params["ContentType"] = content_type

        try:
            self.s3.put_object(**params)
            logger.info("Uploaded bytes to %s/%s", bucket_name, object_name)
        except Exception as error:  # noqa: BLE001
            logger.error("Error uploading bytes: %s", error)
            raise

    def download_file(self, bucket_name: str, object_name: str, file_path: str) -> None:
        """Download an object into a local file path."""
        try:
            self.s3.download_file(bucket_name, object_name, file_path)
            logger.info(
                "File %s downloaded from %s to %s",
                object_name,
                bucket_name,
                file_path,
            )
        except Exception as error:  # noqa: BLE001
            logger.error("Error downloading file: %s", error)
            raise

    def list_files(self, bucket_name: str, prefix: str = "") -> list[str]:
        """List object keys under the bucket/prefix."""
        try:
            response = self.s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
            files = [obj["Key"] for obj in response.get("Contents", [])]
            logger.info("Files in %s/%s: %s", bucket_name, prefix, files)
            return files
        except Exception as error:  # noqa: BLE001
            logger.error("Error listing files: %s", error)
            raise

    def delete_file(self, bucket_name: str, object_name: str) -> None:
        """Delete a single object from a bucket."""
        try:
            self.s3.delete_object(Bucket=bucket_name, Key=object_name)
            logger.info("File %s deleted from %s", object_name, bucket_name)
        except Exception as error:  # noqa: BLE001
            logger.error("Error deleting file: %s", error)
            raise

    def delete_folder(self, bucket_name: str, prefix: str) -> None:
        """Delete all objects matching the provided prefix."""
        try:
            response = self.s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

            if "Contents" not in response:
                logger.info(
                    "No files found with prefix '%s' in bucket '%s'",
                    prefix,
                    bucket_name,
                )
                return

            objects_to_delete = [{"Key": obj["Key"]} for obj in response["Contents"]]

            delete_response = self.s3.delete_objects(
                Bucket=bucket_name,
                Delete={"Objects": objects_to_delete},
            )

            deleted_count = len(delete_response.get("Deleted", []))
            logger.info(
                "Successfully deleted %s objects under prefix '%s'",
                deleted_count,
                prefix,
            )

            if "Errors" in delete_response:
                raise RuntimeError(
                    f"Errors occurred during batch delete: {delete_response['Errors']}"
                )
        except Exception as error:  # noqa: BLE001
            logger.error("Error deleting folder '%s': %s", prefix, error)
            raise

    def create_bucket(
        self,
        bucket_name: str,
        object_lock_enabled: bool = False,
    ) -> None:
        """Create a bucket with optional Object Lock support."""
        try:
            params: dict[str, str | bool] = {"Bucket": bucket_name}
            if object_lock_enabled:
                params["ObjectLockEnabledForBucket"] = True

            self.s3.create_bucket(**params)
            logger.info("Bucket %s created", bucket_name)
        except Exception as error:  # noqa: BLE001
            logger.error("Error creating bucket: %s", error)
            raise

    def bucket_exists(self, bucket_name: str) -> bool:
        """Return True when bucket exists, False when not found."""
        try:
            self.s3.head_bucket(Bucket=bucket_name)
            return True
        except ClientError as error:
            error_code = error.response.get("Error", {}).get("Code", "")
            if error_code in {"404", "NoSuchBucket", "NotFound"}:
                return False

            logger.error("Error checking bucket existence: %s", error)
            raise

    def ensure_bucket(
        self,
        bucket_name: str,
        object_lock_enabled: bool = False,
    ) -> bool:
        """Ensure bucket existence; returns True only when newly created."""
        if self.bucket_exists(bucket_name):
            logger.info("Bucket %s already exists", bucket_name)
            return False

        self.create_bucket(bucket_name, object_lock_enabled=object_lock_enabled)
        return True

    def ensure_prefixes(self, bucket_name: str, prefixes: list[str]) -> None:
        """Ensure prefix placeholders exist under a bucket."""
        for prefix in prefixes:
            normalized = prefix if prefix.endswith("/") else f"{prefix}/"
            self.s3.put_object(Bucket=bucket_name, Key=normalized, Body=b"")
            logger.info("Ensured prefix s3://%s/%s", bucket_name, normalized)

    def set_default_object_lock_retention(
        self,
        bucket_name: str,
        mode: str,
        days: int,
    ) -> None:
        """Set default Object Lock retention policy for a bucket."""
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
                "Rule": {"DefaultRetention": {"Mode": mode, "Days": days}},
            },
        )
        logger.info(
            "Set default object lock retention for %s: mode=%s, days=%s",
            bucket_name,
            mode,
            days,
        )

    def bucket_supports_object_lock(self, bucket_name: str) -> bool:
        """Check whether Object Lock is enabled on the bucket."""
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
        """Clear default Object Lock retention policy when supported."""
        if not self.bucket_supports_object_lock(bucket_name):
            logger.info(
                "Skip clearing object lock retention for %s because object "
                "lock is disabled",
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
        """Get object bytes from storage."""
        try:
            response = self.s3.get_object(Bucket=bucket_name, Key=object_name)
            logger.info("Object %s retrieved from %s", object_name, bucket_name)
            return response["Body"].read()
        except Exception as error:  # noqa: BLE001
            logger.error(
                "Error getting object %s from %s: %s", object_name, bucket_name, error
            )
            raise

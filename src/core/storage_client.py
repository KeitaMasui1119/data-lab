import logging
import os

import boto3
from botocore.config import Config
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)


class RustFSClient:
    def __init__(self):
        self.endpoint_url = os.environ.get("AWS_ENDPOINT_URL")
        self.access_key = os.environ.get("AWS_ACCESS_KEY_ID")
        self.secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
        self.region = os.environ.get("AWS_REGION")

        if not self.access_key or not self.secret_key:
            raise ValueError("RUSTFS_ACCESS_KEY or RUSTFS_SECRET_KEY is not set.")

        self.s3 = boto3.client(
            "s3",
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            config=Config(signature_version="s3v4"),
            region_name=self.region,
        )

    def get_storage_options(self):
        """外部ライブラリ(Polars等)に渡すためのS3接続設定を辞書で返す"""
        return {
            "key": self.access_key,
            "secret": self.secret_key,
            "endpoint_url": self.endpoint_url,
            "client_kwargs": {"region_name": self.region},
        }

    def upload_file(self, bucket_name, file_path, object_name=None):
        if object_name is None:
            object_name = os.path.basename(file_path)

        try:
            self.s3.upload_file(file_path, bucket_name, object_name)
            logger.info(f"File {file_path} uploaded to {bucket_name}/{object_name}")
        except Exception as e:
            logger.error(f"Error uploading file: {e}")
            raise

    def download_file(self, bucket_name, object_name, file_path):
        try:
            self.s3.download_file(bucket_name, object_name, file_path)
            logger.info(
                f"File {object_name} downloaded from {bucket_name} to {file_path}"
            )
        except Exception as e:
            logger.error(f"Error downloading file: {e}")
            raise

    def list_files(self, bucket_name, prefix=""):
        try:
            response = self.s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
            files = [obj["Key"] for obj in response.get("Contents", [])]
            logger.info(f"Files in {bucket_name}/{prefix}: {files}")
            return files
        except Exception as e:
            logger.error(f"Error listing files: {e}")
            raise

    def delete_file(self, bucket_name, object_name):
        try:
            self.s3.delete_object(Bucket=bucket_name, Key=object_name)
            logger.info(f"File {object_name} deleted from {bucket_name}")
        except Exception as e:
            logger.error(f"Error deleting file: {e}")
            raise

    def create_bucket(self, bucket_name):
        try:
            self.s3.create_bucket(Bucket=bucket_name)
            logger.info(f"Bucket {bucket_name} created")
        except Exception as e:
            logger.error(f"Error creating bucket: {e}")
            raise

    def get_object(self, bucket_name, object_name):
        try:
            response = self.s3.get_object(Bucket=bucket_name, Key=object_name)
            logger.info(f"Object {object_name} retrieved from {bucket_name}")
            return response["Body"].read()
        except Exception as e:
            logger.error(f"Error getting object: {e}")
            raise

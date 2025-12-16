import os

import boto3
from botocore.config import Config

ACCESS_KEY = os.environ.get("RUSTFS_ACCESS_KEY")
SECRET_KEY = os.environ.get("RUSTFS_SECRET_KEY")

# Docker Composeのサービス名とポートを使用
ENDPOINT_URL = "http://rustfs:9000"

# RustFS上に作成するバケット名
BUCKET_NAME = "nexsol-data-lake-stg-usc1"

# 転送したいローカルファイルのルートディレクトリ
LOCAL_DATA_DIR = (
    "/workspace/src/stg/data_lake/electric_forecast"  # 適切なパスに修正してください
)

# --- S3クライアントの初期化 ---
s3_config = Config(signature_version="s3v4")

s3 = boto3.client(
    "s3",
    endpoint_url=ENDPOINT_URL,
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    config=s3_config,
    verify=False,  # 開発環境での自己署名証明書対策
)


# --- 実行ロジック ---
def upload_files_to_s3(local_dir, bucket):
    print(f"--- Connection test：Checking the existance of  {bucket} ... ---")
    try:
        # バケットが存在しない場合は作成
        s3.head_bucket(Bucket=bucket)
        print(f"{bucket} has already exist.")
    except Exception:
        s3.create_bucket(Bucket=bucket)
        print(f"Create a {bucket}")

    print(f"--- Start file transfer from {local_dir}  ---")

    # os.walkを使ってディレクトリ内の全ファイルを走査
    for root, _, files in os.walk(local_dir):
        for file in files:
            local_path = os.path.join(root, file)
            # S3上のキー（パス）を決定。LOCAL_DATA_DIRからの相対パスを使用
            s3_key = os.path.relpath(local_path, local_dir).replace("\\", "/")

            try:
                s3.upload_file(local_path, bucket, s3_key)
                print(f"SUCCESS: {local_path} -> s3://{bucket}/{s3_key}")
            except Exception as e:
                print(f"FAILED: {local_path} -> Error : {e}")


# --- メイン処理 ---
if __name__ == "__main__":
    if not ACCESS_KEY or not SECRET_KEY:
        print("Error: RUSTFS_ACCESS_KEY or RUSTFS_SECRET_KEY is not set.")
    elif not os.path.exists(LOCAL_DATA_DIR):
        print(f"Error: Cannot find local directory {LOCAL_DATA_DIR}.")
    else:
        upload_files_to_s3(LOCAL_DATA_DIR, BUCKET_NAME)

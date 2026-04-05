import boto3
from botocore.exceptions import ClientError

# --- ここを君の正解の環境に合わせて書き換える ---
ENDPOINT_URL = "http://rustfs:9000"
ACCESS_KEY = "RUSTFS_DEV_USER"
SECRET_KEY = "5lfrip8glbo39capHlpis9e3r09rasw9crEdestit5usteZ8keyu1up3Uw4madru"
REGION = "us-east-1"
BUCKET_NAME = "jp-power-grid-dev"

print(f"🔗 エンドポイント {ENDPOINT_URL} に接続を試みます...")

s3 = boto3.client(
    "s3",
    endpoint_url=ENDPOINT_URL,
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    region_name=REGION,
)

try:
    # 1. 認証テスト（バケット一覧の取得）
    print("\n[テスト1] 認証とバケット一覧の取得")
    response = s3.list_buckets()
    buckets = [b["Name"] for b in response["Buckets"]]
    print(f"✅ 成功: 認証を通過しました。存在するバケット -> {buckets}")

    if BUCKET_NAME not in buckets:
        print(
            f"❌ 失敗: バケット '{BUCKET_NAME}' が存在しません。"
            "RustFSのWebUI等で作ってください。"
        )
        exit(1)

    # 2. 権限テスト（テストファイルの書き込み）
    print(f"\n[テスト2] バケット '{BUCKET_NAME}' へのファイル書き込み権限")
    test_key = "test_dir/test_file.txt"
    s3.put_object(Bucket=BUCKET_NAME, Key=test_key, Body=b"hello rustfs")
    print("✅ 成功: ファイルの書き込み権限があります。")

    # 3. 読み込み・HeadObjectテスト（Icebergが躓いている処理）
    print("\n[テスト3] 書き込んだファイルのメタデータ取得（HeadObject）")
    s3.head_object(Bucket=BUCKET_NAME, Key=test_key)
    print("✅ 成功: HeadObjectの実行権限があります。")

except ClientError as e:
    error_code = e.response["Error"]["Code"]
    print(f"\n❌ AWS通信エラー発生: {error_code}")
    print(f"詳細: {e}")
except Exception as e:
    print(f"\n❌ 予期せぬエラー: {e}")

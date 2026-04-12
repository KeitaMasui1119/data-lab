import boto3
from botocore.client import Config

s3 = boto3.client(
    "s3",
    endpoint_url="http://rustfs:9000",
    aws_access_key_id="RUSTFS_DEV_USER",
    aws_secret_access_key="5lfrip8glbo39capHlpis9e3r09rasw9crEdestit5usteZ8keyu1up3Uw4madru",
    config=Config(signature_version="s3v4"),
    region_name="us-east-1",
)

bucket_name = "jp-power-grid-dev"

response = s3.list_objects_v2(Bucket=bucket_name)
for obj in response.get("Contents", []):
    print(f"- {obj['Key']} ({obj['Size']} bytes)")

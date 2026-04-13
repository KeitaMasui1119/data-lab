import logging

import polars as pl

from core.storage_client import RustFSClient

# ログ設定
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)

container = RustFSClient()
logger.info("RustFSClient initialized successfully.")

storage_options = container.get_storage_options()
logger.info(f"Storage options retrieved: {storage_options}")

path_url = "s3://jp-power-grid-dev/raw/jepx/spot_summary/spot_summary_2026.csv"

df = pl.read_csv(path_url, storage_options=storage_options, encoding="cp932")
print(df.head(5))

import logging

from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.exceptions import NoSuchTableError

# ログ設定
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)


def get_catalog(catalog_name: str) -> Catalog:
    try:
        logger.info(f"Successfully loaded catalog: '{catalog_name}'")
        return load_catalog(catalog_name)
    except Exception as e:
        raise ValueError(f"Failed to load catalog '{catalog_name}': {e}") from e


def drop_table(catalog: Catalog, table_name: str):
    try:
        catalog.drop_table(table_name)
        logger.info(f"テーブル削除成功: {table_name}")
    except NoSuchTableError:
        logger.warning(f"テーブルが存在しません: {table_name}")
    except Exception as e:
        logger.error(f"テーブル削除中にエラー: {e}")

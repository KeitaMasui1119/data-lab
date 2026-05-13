import logging
from pathlib import Path

from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchTableError
from schema_builder import build_table_schema

# ログの中央集権設定(ここで出力形式を固定する)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
    handlers=[logging.FileHandler("pipeline.log", mode="a"), logging.StreamHandler()],
)

logger = logging.getLogger(__name__)


def provision_table(
    catalog_name: str, namespace: str, table_name: str, schema_csv_path: str
):
    """_summary_

    Args:
        catalog_name (str): _description_
        namespace (str): _description_
        table_name (str): _description_
        schema_csv_path (str): _description_
    """
    # 1. カタログのロード
    catalog = load_catalog(catalog_name)

    # 2. Namespaceの作成
    try:
        catalog.create_namespace(namespace)
        logger.info(f"Create new namespace: '{namespace}'.")
    except NamespaceAlreadyExistsError:
        logger.info(f"Skipping namespace creation: '{namespace}' already exists.")

    # 3. CSVから最新のスキーマ定義を読み込む
    identifier = f"{namespace}.{table_name}"
    new_schema = build_table_schema(schema_csv_path)

    # 4. テーブルの作成 or 更新
    try:
        # テーブルが存在するか確認(存在しない場合はNoSuchTableErrorが発生)
        existing_table = catalog.load_table(identifier)
        logger.info(f"Table '{identifier}' exists. Checking schema diff.")

        # ここからスキーマ進化の処理
        existing_schema = existing_table.schema()
        existing_col_names = {field.name for field in existing_schema.fields}
        new_col_names = {field.name for field in new_schema.fields}

        # 差分検知
        added_cols = new_col_names - existing_col_names
        removed_cols = existing_col_names - new_col_names

        if removed_cols:
            logger.warning(
                f"Column '{removed_cols}' may be removed or modified in the CSV."
            )

        if added_cols:
            with existing_table.update_schema() as update:
                for col_name in added_cols:
                    new_field = new_schema.find_field(col_name)
                    update.add_column(
                        path=new_field.name,
                        field_type=new_field.field_type,
                        doc=new_field.doc,
                    )
            logger.info(f"Add new columns: {added_cols}")
        else:
            logger.info("No schema changes detected")
    except NoSuchTableError:
        # テーブルが存在しない場合は新規作成
        logger.info(f"Table '{identifier}' does not exist. Creating.")
        catalog.create_table(identifier=identifier, schema=new_schema)
        logger.info(f"Table '{identifier}' created successfully.")


if __name__ == "__main__":
    CATALOG_NAME = "dlh_dev"

    SCHEMA_BASE_DIR = Path("/workspace/configuration/iceberg/schema")
    logger.info("=== Provisioning began ===")
    for csv_file in SCHEMA_BASE_DIR.rglob("*.csv"):
        namespace = csv_file.parent.name
        table_name = csv_file.stem
        try:
            provision_table(
                catalog_name=CATALOG_NAME,
                namespace=namespace,
                table_name=table_name,
                schema_csv_path=str(csv_file),
            )
        except Exception as e:
            logger.error(f"Failed to provision table '{namespace}.{table_name}': {e}")
            continue
    logger.info("=== Provisioning completed ===")

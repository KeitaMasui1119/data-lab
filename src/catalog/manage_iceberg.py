import argparse
import logging

from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.schema import Schema

from catalog.schema_builder import build_table_schema

# ログ設定
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)


def get_catalog(catalog_name: str) -> Catalog:
    """_summary_

    Args:
        catalog_name (str): _description_

    Raises:
        ValueError: _description_

    Returns:
        Catalog: _description_
    """
    try:
        catalog = load_catalog(catalog_name)
        logger.info(f"Successfully loaded catalog: '{catalog_name}'")
        return catalog
    except Exception as e:
        raise ValueError(f"Failed to load catalog '{catalog_name}': {e}") from e


# --- Manage namespace ---
def build_namespace(catalog: Catalog, namespace: str):
    """_summary_

    Args:
        catalog (Catalog): _description_
        namespace (str): _description_
    """
    try:
        catalog.create_namespace_if_not_exists(namespace)
        logger.info(f"Successed to create namespace: {namespace}")
    except Exception as e:
        logger.error(f"Failed to create namespace : {e}")


def delete_namespace(catalog: Catalog, namespace: str):
    """_summary_

    Args:
        catalog (Catalog): _description_
        namespace (str): _description_
    """
    try:
        catalog.drop_namespace(namespace)
        logger.info(f"Successed to delete namespace: {namespace}")
    except Exception as e:
        logger.error(f"Failed to delete namespace : {e}")


def view_namespace(catalog: Catalog, namespace: str):
    """_summary_

    Args:
        catalog (Catalog): _description_
        namespace (str): _description_

    Returns:
        list[Identifier]: _description_
    """
    try:
        namespaces = catalog.list_namespaces(namespace)
        logger.info(f"Fetched {len(namespaces)} namespaces.")
        for ns in namespaces:
            logger.info(f" - {ns}")
        return namespaces
    except Exception as e:
        logger.error(f"Failed to fetch namespaces : {e}")


# --- Manage table ---
def delete_table(catalog: Catalog, table_name: str, purge: bool = False):
    """_summary_

    Args:
        catalog (Catalog): _description_
        table_name (str): _description_
        purge (bool): _description_
    """
    try:
        if purge:
            catalog.purge_table(table_name)
            logger.info(
                f"Successed to PURGE (drop & delete files) table.: {table_name}"
            )
        else:
            catalog.drop_table(table_name)
            logger.info(f"Successed to drop table.: {table_name}")
    except NoSuchTableError:
        logger.warning(f"Table doesn't exist.: {table_name}")
    except Exception as e:
        logger.error(f"Failed to drop table.: {e}")


def provision_table(catalog: Catalog, identifier: str, schema_csv_path: str):
    """_summary_

    Args:
        catalog (Catalog): _description_
        identifier (str): _description_
        schema_csv_path (str): _description_
    """
    # identifierから全階層のnamespaceを抽出して順番に作成
    # 例: "bronze.occto.unit_generation" → ["bronze", "bronze.occto"] を両方作成
    parts = identifier.split(".")
    namespace_parts = parts[:-1]
    for i in range(1, len(namespace_parts) + 1):
        ns = ".".join(namespace_parts[:i])
        build_namespace(catalog, ns)

    # CSVから最新のスキーマ定義を読み込む
    # build_table_schema が Schema と PartitionSpec の dict 等を返す想定
    new_schema = build_table_schema(schema_csv_path)

    if new_schema is None:
        raise ValueError(f"Failed to generate schema. Check CSV. : {schema_csv_path}")

    if not isinstance(new_schema, Schema):
        raise TypeError(
            f"build_table_schema returned a non-Schema type.: {type(new_schema)}"
        )

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


def main():
    parser = argparse.ArgumentParser(description="Iceberg Admin CLI")
    parser.add_argument(
        "--catalog", default="dlh_dev", help="カタログ名 (default: dlh_dev)"
    )

    # サブコマンドの定義
    subparsers = parser.add_subparsers(
        dest="command", required=True, help="操作対象 (namespace または table)"
    )

    # 1. namespace コマンド定義
    parser_ns = subparsers.add_parser("namespace", help="ネームスペースの操作")
    parser_ns.add_argument(
        "action", choices=["create", "drop", "view"], help="実行する操作"
    )
    parser_ns.add_argument(
        "--name", required=True, help="ネームスペース名 (例: bronze)"
    )

    # 2. table コマンド定義
    parser_table = subparsers.add_parser("table", help="テーブルの操作")
    parser_table.add_argument(
        "action", choices=["create", "drop", "recreate"], help="実行する操作"
    )
    parser_table.add_argument(
        "--name", required=True, help="テーブル名 (例: bronze.jepx_spot_price)"
    )
    # create / recreate の時はCSVファイルが必要
    parser_table.add_argument(
        "--csv", help="テーブル定義CSVのパス (create/recreate時に必須)"
    )

    args = parser.parse_args()
    catalog = get_catalog(args.catalog)

    # 実行ルーティング
    if args.command == "namespace":
        if args.action == "create":
            build_namespace(catalog, args.name)
        elif args.action == "drop":
            logger.warning(
                f"ネームスペースの削除は安全のため現在スキップされます: {args.name}"
            )
        elif args.action == "view":
            view_namespace(catalog, args.name)

    elif args.command == "table":
        if args.action == "create":
            if not args.csv:
                logger.error("create アクションには --csv 引数が必須です。")
                return
            provision_table(catalog, args.name, args.csv)
        elif args.action == "drop":
            delete_table(catalog, args.name)
        elif args.action == "recreate":
            if not args.csv:
                logger.error("❌ recreate アクションには --csv 引数が必須です。")
                return
            delete_table(catalog, args.name)
            provision_table(catalog, args.name, args.csv)


if __name__ == "__main__":
    main()

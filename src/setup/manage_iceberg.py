import argparse

from common.iceberg.catalog import (
    build_namespace,
    delete_namespace,
    delete_table,
    get_catalog,
    provision_table,
    view_namespace,
)
from common.logging_utils import configure_logging, get_logger

configure_logging()
logger = get_logger(__name__)


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
            delete_namespace(catalog, args.name)
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

import argparse
import sys

from common.iceberg.catalog import (
    build_namespace,
    delete_namespace,
    delete_table,
    get_catalog,
    provision_table,
    view_namespace,
)
from common.utils import configure_logging, get_logger

configure_logging()
logger = get_logger(__name__)

EXIT_OK = 0
EXIT_USAGE = 2


def _configure_namespace(subparsers: argparse._SubParsersAction) -> None:
    parser_ns = subparsers.add_parser("namespace", help="ネームスペースの操作")
    parser_ns.add_argument(
        "action", choices=["create", "drop", "view"], help="実行する操作"
    )
    parser_ns.add_argument(
        "--name", required=True, help="ネームスペース名 (例: bronze)"
    )


def _configure_table(subparsers: argparse._SubParsersAction) -> None:
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


def build_parser() -> argparse.ArgumentParser:
    """Build the admin CLI parser."""
    parser = argparse.ArgumentParser(description="Iceberg Admin CLI")
    parser.add_argument(
        "--catalog", default="dlh_dev", help="カタログ名 (default: dlh_dev)"
    )
    subparsers = parser.add_subparsers(
        dest="command", required=True, help="操作対象 (namespace または table)"
    )
    _configure_namespace(subparsers)
    _configure_table(subparsers)
    return parser


def handle_namespace(catalog, args: argparse.Namespace) -> int:
    """Route a namespace subcommand. Returns the process exit code."""
    if args.action == "create":
        build_namespace(catalog, args.name)
    elif args.action == "drop":
        delete_namespace(catalog, args.name)
    elif args.action == "view":
        view_namespace(catalog, args.name)
    return EXIT_OK


def handle_table(catalog, args: argparse.Namespace) -> int:
    """Route a table subcommand. Returns the process exit code.

    create and recreate need --csv. Missing it exits non-zero rather than
    logging and falling through: this CLI is scripted against, and an early
    return used to report success while doing nothing.
    """
    if args.action in {"create", "recreate"} and not args.csv:
        logger.error("%s アクションには --csv 引数が必須です。", args.action)
        return EXIT_USAGE

    if args.action == "create":
        provision_table(catalog, args.name, args.csv)
    elif args.action == "drop":
        delete_table(catalog, args.name)
    elif args.action == "recreate":
        delete_table(catalog, args.name)
        provision_table(catalog, args.name, args.csv)
    return EXIT_OK


def main() -> int:
    args = build_parser().parse_args()
    catalog = get_catalog(args.catalog)

    if args.command == "namespace":
        return handle_namespace(catalog, args)
    return handle_table(catalog, args)


if __name__ == "__main__":
    sys.exit(main())

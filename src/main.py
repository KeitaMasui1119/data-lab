"""CLI entry point. Thin orchestration only -- see CLAUDE.md.

Builds the argparse parser from the command registry (src/cli/) and
dispatches to the matching handler. See docs/tasks/refactaring_20260817.md
for the extraction history of this module.
"""

import argparse
import logging

from cli.commands import ALL_COMMANDS
from cli.registry import build_parser as _build_registry_parser
from cli.registry import dispatch

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)


def build_parser() -> argparse.ArgumentParser:
    return _build_registry_parser(ALL_COMMANDS)


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    if args.command is None:
        logger.info(
            "No command was provided. Running bootstrap-storage for compatibility."
        )
        args.command = "bootstrap-storage"

    dispatch(ALL_COMMANDS, args, parser)


if __name__ == "__main__":
    main()

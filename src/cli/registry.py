"""Command registry: builds/dispatches the top-level argparse parser from a
list of CommandSpec definitions contributed by src/cli/commands/*.
"""

from __future__ import annotations

import argparse
from collections.abc import Callable
from dataclasses import dataclass


@dataclass(frozen=True)
class CommandSpec:
    """One subcommand: its name/help text, argument wiring, and dispatch body.

    handler takes the top-level parser (not the subparser) so it can call
    parser.error() the same way the pre-extraction main() did -- every
    parser.error() call in this codebase prints the full top-level usage
    listing all commands, not just the matched subcommand's usage. That is
    pre-existing behavior this refactor intentionally preserves.
    """

    name: str
    help: str
    configure: Callable[[argparse.ArgumentParser], None]
    handler: Callable[[argparse.Namespace, argparse.ArgumentParser], None]


def build_parser(commands: list[CommandSpec]) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Data platform orchestrator")
    subparsers = parser.add_subparsers(dest="command")
    for spec in commands:
        sub = subparsers.add_parser(spec.name, help=spec.help)
        spec.configure(sub)
    return parser


def dispatch(
    commands: list[CommandSpec],
    args: argparse.Namespace,
    parser: argparse.ArgumentParser,
) -> None:
    by_name = {spec.name: spec for spec in commands}
    by_name[args.command].handler(args, parser)

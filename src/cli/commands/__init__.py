"""Aggregates every command module's CommandSpec list into one registry."""

from __future__ import annotations

from cli.commands import (
    jepx,
    metadata_admin,
    occto,
    power_usage,
    silver_admin,
    storage,
    supply_demand,
)
from cli.registry import CommandSpec

ALL_COMMANDS: list[CommandSpec] = [
    storage.COMMAND,
    *jepx.COMMANDS,
    *occto.COMMANDS,
    *power_usage.COMMANDS,
    *supply_demand.COMMANDS,
    *silver_admin.COMMANDS,
    *metadata_admin.COMMANDS,
]

"""Regression net for src/main.py's CLI surface.

Pins down the current argparse structure (25 subcommands, their defaults,
and the parser.error() validations that fire before any external system is
touched) before docs/tasks/refactaring_20260817.md's Phase 2/3 extraction.
Intentionally does not exercise the dispatch bodies that call RustFS/
Iceberg/DuckDB -- those remain integration-only, as elsewhere in this repo.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pytest

import main as main_module

JEPX_BRONZE_SCHEMA_PATH = (
    "/workspace/configuration/iceberg/schema/bronze/jepx_spot_price/jepx_spot_price.csv"
)
OCCTO_BRONZE_SCHEMA_PATH = (
    "/workspace/configuration/iceberg/schema/bronze/"
    "occto_unit_generation_actuals/occto_unit_generation_actuals.csv"
)
JEPX_SILVER_SCHEMA_DIR = (
    "/workspace/configuration/iceberg/schema/silver/jepx_spot_price"
)
OCCTO_SILVER_SCHEMA_DIR = (
    "/workspace/configuration/iceberg/schema/silver/occto_unit_generation_actuals"
)
JEPX_GOLD_SCHEMA_DIR = "/workspace/configuration/iceberg/schema/gold/jepx_spot_price"
POWER_USAGE_HOKURIKU_SILVER_SCHEMA_DIR = (
    "/workspace/configuration/iceberg/schema/silver/power_usage_hokuriku"
)

# command -> (minimal args needed to satisfy required=True fields, expected defaults)
EXPECTED_COMMANDS: dict[str, tuple[list[str], dict[str, object]]] = {
    "bootstrap-storage": ([], {"buckets": None}),
    "scrape-jepx": (
        [],
        {"bucket": "jp-power-grid-dev", "timestamp_ms": None, "fiscal_year": None},
    ),
    "ingest-jepx-raw-to-bronze": (
        [],
        {
            "bucket": "jp-power-grid-dev",
            "object_key": None,
            "source_file_name": None,
            "timestamp_ms": None,
            "catalog": "dlh_dev",
            "table": "bronze.jepx_spot_price",
            "schema_path": JEPX_BRONZE_SCHEMA_PATH,
            "allow_duplicate_source": False,
            "use_ingestion_log": False,
            "require_unprocessed": False,
        },
    ),
    "run-jepx-raw-pipeline": (
        [],
        {
            "bucket": "jp-power-grid-dev",
            "timestamp_ms": None,
            "catalog": "dlh_dev",
            "table": "bronze.jepx_spot_price",
            "schema_path": JEPX_BRONZE_SCHEMA_PATH,
            "allow_duplicate_source": False,
        },
    ),
    "run-jepx-orchestrator": (
        [],
        {
            "bucket": "jp-power-grid-dev",
            "timestamp_ms": None,
            "catalog": "dlh_dev",
            "bronze_table": "bronze.jepx_spot_price",
            "bronze_schema_path": JEPX_BRONZE_SCHEMA_PATH,
            "allow_duplicate_source": False,
            "dbt_project_dir": "/workspace/src/dbt/jepx_power",
            "dbt_profiles_dir": None,
            "bronze_location": "s3://jp-power-grid-dev/bronze/jepx_spot_price",
            "silver_schema_dir": JEPX_SILVER_SCHEMA_DIR,
            "silver_fiscal_year": None,
            "silver_all_fiscal_years": False,
            "run_gold_step": False,
            "gold_select": "tag:gold",
            "dbt_full_refresh": False,
        },
    ),
    "ingest-occto-raw-to-bronze": (
        [],
        {
            "bucket": "jp-power-grid-dev",
            "object_key": None,
            "source_file_name": None,
            "target_date": None,
            "catalog": "dlh_dev",
            "table": "bronze.occto_unit_generation_actuals",
            "schema_path": OCCTO_BRONZE_SCHEMA_PATH,
            "allow_duplicate_source": False,
            "use_ingestion_log": False,
            "require_unprocessed": False,
        },
    ),
    "ingest-power-usage-hokuriku-raw-to-bronze": (
        ["--target-date", "2026-08-16"],
        {
            "bucket": "jp-power-grid-dev",
            "object_key": None,
            "source_file_name": None,
            "target_date": "2026-08-16",
            "catalog": "dlh_dev",
            "allow_duplicate_source": False,
            "use_ingestion_log": False,
            "require_unprocessed": False,
        },
    ),
    "ingest-supply-demand-actuals-raw-to-bronze-tohoku": (
        ["--object-key", "x", "--target-date", "2026-08-16"],
        {
            "bucket": "jp-power-grid-dev",
            "source_file_name": None,
            "catalog": "dlh_dev",
            "allow_duplicate_target_date": False,
        },
    ),
    "ingest-supply-demand-actuals-raw-to-bronze-chugoku": (
        ["--object-key", "x", "--target-date", "2026-08-16"],
        {
            "bucket": "jp-power-grid-dev",
            "source_file_name": None,
            "catalog": "dlh_dev",
            "allow_duplicate_target_date": False,
        },
    ),
    "ingest-supply-demand-actuals-raw-to-bronze-shikoku": (
        ["--object-key", "x", "--target-date", "2026-08-16"],
        {
            "bucket": "jp-power-grid-dev",
            "source_file_name": None,
            "catalog": "dlh_dev",
            "allow_duplicate_target_date": False,
        },
    ),
    "ingest-supply-demand-actuals-bronze-to-silver-tohoku": (
        [],
        {"catalog": "dlh_dev", "target_date": None, "from_date": None, "to_date": None},
    ),
    "ingest-supply-demand-actuals-bronze-to-silver-chugoku": (
        [],
        {"catalog": "dlh_dev", "target_date": None, "from_date": None, "to_date": None},
    ),
    "ingest-supply-demand-actuals-bronze-to-silver-shikoku": (
        [],
        {"catalog": "dlh_dev", "target_date": None, "from_date": None, "to_date": None},
    ),
    "ingest-power-usage-hokuriku-bronze-to-silver": (
        [],
        {
            "catalog": "dlh_dev",
            "schema_dir": POWER_USAGE_HOKURIKU_SILVER_SCHEMA_DIR,
            "target_date": None,
            "from_date": None,
            "to_date": None,
        },
    ),
    "ingest-occto-bronze-to-silver": (
        [],
        {
            "catalog": "dlh_dev",
            "bronze_location": "s3://jp-power-grid-dev/bronze/occto_unit_generation_actuals",
            "schema_dir": OCCTO_SILVER_SCHEMA_DIR,
            "target_date": None,
            "from_date": None,
            "to_date": None,
        },
    ),
    "run-occto-orchestrator": (
        [],
        {
            "bucket": "jp-power-grid-dev",
            "target_date": None,
            "to_date": None,
            "catalog": "dlh_dev",
            "bronze_table": "bronze.occto_unit_generation_actuals",
            "bronze_schema_path": OCCTO_BRONZE_SCHEMA_PATH,
            "allow_duplicate_source": False,
            "bronze_location": "s3://jp-power-grid-dev/bronze/occto_unit_generation_actuals",
            "silver_schema_dir": OCCTO_SILVER_SCHEMA_DIR,
            "silver_from_date": None,
            "silver_to_date": None,
            "silver_all_dates": False,
        },
    ),
    "scrape-occto": (
        [],
        {"bucket": "jp-power-grid-dev", "target_date": None, "to_date": None},
    ),
    "scrape-power-usage-hokuriku": (
        [],
        {"bucket": "jp-power-grid-dev", "target_date": None, "to_date": None},
    ),
    "scrape-supply-demand-actuals-tohoku": (
        [],
        {"bucket": "jp-power-grid-dev", "target_date": None},
    ),
    "scrape-supply-demand-actuals-chugoku": (
        [],
        {"bucket": "jp-power-grid-dev", "target_date": None},
    ),
    "scrape-supply-demand-actuals-shikoku": (
        [],
        {"bucket": "jp-power-grid-dev", "target_date": None},
    ),
    "provision-silver-tables": (
        [],
        {
            "catalog": "dlh_dev",
            "schema_dir": "/workspace/configuration/iceberg/schema/silver",
        },
    ),
    "evolve-silver-partition-spec": (
        [],
        {
            "catalog": "dlh_dev",
            "schema_dir": "/workspace/configuration/iceberg/schema/silver",
        },
    ),
    "expire-silver-snapshots": (
        [],
        {
            "catalog": "dlh_dev",
            "schema_dir": "/workspace/configuration/iceberg/schema/silver",
            "bucket": "jp-power-grid-dev",
            "older_than_days": 7,
            "delete": False,
        },
    ),
    "ingest-jepx-bronze-to-silver": (
        [],
        {
            "catalog": "dlh_dev",
            "bronze_location": "s3://jp-power-grid-dev/bronze/jepx_spot_price",
            "schema_dir": JEPX_SILVER_SCHEMA_DIR,
            "fiscal_year": None,
        },
    ),
    "ingest-jepx-silver-to-gold": (
        [],
        {
            "catalog": "dlh_dev",
            "area_location": "s3://jp-power-grid-dev/silver/jepx_spot_price_area",
            "base_location": "s3://jp-power-grid-dev/silver/jepx_spot_price_base",
            "schema_dir": JEPX_GOLD_SCHEMA_DIR,
            "fiscal_year": None,
        },
    ),
    "backfill-jepx": (
        ["--from-fiscal-year", "2005"],
        {
            "from_fiscal_year": 2005,
            "to_fiscal_year": None,
            "from_raw": False,
            "request_delay_seconds": 3.0,
            "bucket": "jp-power-grid-dev",
            "catalog": "dlh_dev",
            "bronze_table": "bronze.jepx_spot_price",
            "bronze_schema_path": JEPX_BRONZE_SCHEMA_PATH,
            "allow_duplicate_source": False,
            "bronze_location": "s3://jp-power-grid-dev/bronze/jepx_spot_price",
            "silver_schema_dir": JEPX_SILVER_SCHEMA_DIR,
        },
    ),
}


def _subparsers_action(parser: argparse.ArgumentParser) -> argparse._SubParsersAction:
    for action in parser._actions:  # noqa: SLF001 -- introspection is the point of this test
        if isinstance(action, argparse._SubParsersAction):
            return action
    raise AssertionError("no subparsers action registered on the top-level parser")


@pytest.fixture
def parser() -> argparse.ArgumentParser:
    return main_module.build_parser()


def test_registers_exactly_the_expected_command_set(
    parser: argparse.ArgumentParser,
) -> None:
    registered = set(_subparsers_action(parser).choices)
    assert registered == set(EXPECTED_COMMANDS)


@pytest.mark.parametrize("command", sorted(EXPECTED_COMMANDS))
def test_command_defaults(parser: argparse.ArgumentParser, command: str) -> None:
    minimal_args, expected_defaults = EXPECTED_COMMANDS[command]
    args = parser.parse_args([command, *minimal_args])
    for dest, expected_value in expected_defaults.items():
        assert getattr(args, dest) == expected_value, (
            f"{command}: unexpected default for --{dest.replace('_', '-')}"
        )


@pytest.mark.parametrize(
    "command",
    [
        "ingest-power-usage-hokuriku-raw-to-bronze",
        "ingest-supply-demand-actuals-raw-to-bronze-tohoku",
        "ingest-supply-demand-actuals-raw-to-bronze-chugoku",
        "ingest-supply-demand-actuals-raw-to-bronze-shikoku",
    ],
)
def test_required_target_date_is_enforced(
    parser: argparse.ArgumentParser, command: str
) -> None:
    minimal_args, _ = EXPECTED_COMMANDS[command]
    args_without_target_date = [
        a for a in minimal_args if a not in {"--target-date", "2026-08-16"}
    ]
    with pytest.raises(SystemExit):
        parser.parse_args([command, *args_without_target_date])


@pytest.mark.parametrize(
    "argv",
    [
        pytest.param(
            [
                "run-jepx-orchestrator",
                "--silver-all-fiscal-years",
                "--silver-fiscal-year",
                "2026",
            ],
            id="jepx-orchestrator-mutually-exclusive-silver-scope",
        ),
        pytest.param(
            [
                "run-occto-orchestrator",
                "--silver-all-dates",
                "--silver-from-date",
                "2026-08-01",
            ],
            id="occto-orchestrator-mutually-exclusive-silver-scope",
        ),
        pytest.param(
            [
                "ingest-occto-raw-to-bronze",
                "--object-key",
                "x",
                "--target-date",
                "not-a-date",
            ],
            id="occto-bronze-invalid-target-date",
        ),
        pytest.param(
            [
                "ingest-power-usage-hokuriku-raw-to-bronze",
                "--target-date",
                "not-a-date",
            ],
            id="power-usage-hokuriku-bronze-invalid-target-date",
        ),
        pytest.param(
            [
                "backfill-jepx",
                "--from-fiscal-year",
                "2026",
                "--to-fiscal-year",
                "2005",
            ],
            id="backfill-jepx-reversed-fiscal-year-range",
        ),
    ],
)
def test_dispatch_validation_exits_before_touching_external_systems(
    monkeypatch: pytest.MonkeyPatch, argv: list[str]
) -> None:
    """These parser.error() calls sit before the first RustFSClient()/catalog call
    in main()'s dispatch body, so this exercises main() end-to-end without needing
    RustFS/Iceberg -- see docs/tasks/tasks.md section 8.2's unaddressed test gap.
    """
    monkeypatch.setattr("sys.argv", ["main.py", *argv])
    with pytest.raises(SystemExit):
        main_module.main()


def test_backfill_jepx_requires_a_starting_fiscal_year(
    parser: argparse.ArgumentParser,
) -> None:
    """Without a range the command has nothing to rebuild; there is no default."""
    with pytest.raises(SystemExit):
        parser.parse_args(["backfill-jepx"])


def test_backfill_jepx_accepts_a_single_fiscal_year(
    parser: argparse.ArgumentParser,
) -> None:
    """--to-fiscal-year is optional; omitting it rebuilds the one year given."""
    args = parser.parse_args(["backfill-jepx", "--from-fiscal-year", "2005"])

    assert args.from_fiscal_year == 2005
    assert args.to_fiscal_year is None


def test_dbt_project_dir_validation_exits_before_touching_external_systems(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    missing_dir = tmp_path / "does-not-exist"
    monkeypatch.setattr(
        "sys.argv",
        [
            "main.py",
            "run-jepx-orchestrator",
            "--dbt-project-dir",
            str(missing_dir),
        ],
    )
    with pytest.raises(SystemExit):
        main_module.main()

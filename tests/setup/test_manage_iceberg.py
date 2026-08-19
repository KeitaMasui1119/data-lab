"""Tests for the Iceberg admin CLI's argument parsing and routing.

The catalog itself is never built here -- every test passes a sentinel in its
place and checks which helper was reached with which arguments. That keeps the
routing testable without RustFS, which is what splitting main() bought.
"""

from __future__ import annotations

import pytest

from setup import manage_iceberg

CATALOG = object()  # stand-in; the routing never inspects it


def test_parser_reads_namespace_subcommand() -> None:
    # Arrange
    parser = manage_iceberg.build_parser()

    # Act
    args = parser.parse_args(["namespace", "create", "--name", "bronze"])

    # Assert
    assert args.command == "namespace"
    assert args.action == "create"
    assert args.name == "bronze"
    assert args.catalog == "dlh_dev"


def test_parser_reads_table_subcommand_with_csv() -> None:
    # Arrange
    parser = manage_iceberg.build_parser()

    # Act
    args = parser.parse_args(
        [
            "--catalog",
            "dlh_prd",
            "table",
            "create",
            "--name",
            "bronze.x",
            "--csv",
            "a.csv",
        ]
    )

    # Assert
    assert args.command == "table"
    assert args.csv == "a.csv"
    assert args.catalog == "dlh_prd"


@pytest.mark.parametrize("action", ["create", "recreate"])
def test_table_action_needing_csv_exits_non_zero_without_it(
    action: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A scripted caller must be able to tell that nothing happened.

    This used to log an error and return None, so the process exited 0 while
    the table was never touched.
    """
    # Arrange
    called: list[str] = []
    monkeypatch.setattr(
        manage_iceberg, "provision_table", lambda *a, **k: called.append("provision")
    )
    monkeypatch.setattr(
        manage_iceberg, "delete_table", lambda *a, **k: called.append("delete")
    )
    args = manage_iceberg.build_parser().parse_args(
        ["table", action, "--name", "bronze.x"]
    )

    # Act
    exit_code = manage_iceberg.handle_table(CATALOG, args)

    # Assert
    assert exit_code == manage_iceberg.EXIT_USAGE
    assert called == []


def test_table_create_provisions_once(monkeypatch: pytest.MonkeyPatch) -> None:
    # Arrange
    calls: list[tuple] = []
    monkeypatch.setattr(
        manage_iceberg, "provision_table", lambda *a: calls.append(("provision", *a))
    )
    args = manage_iceberg.build_parser().parse_args(
        ["table", "create", "--name", "bronze.x", "--csv", "a.csv"]
    )

    # Act
    exit_code = manage_iceberg.handle_table(CATALOG, args)

    # Assert
    assert exit_code == manage_iceberg.EXIT_OK
    assert calls == [("provision", CATALOG, "bronze.x", "a.csv")]


def test_table_recreate_drops_before_provisioning(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Order matters: provisioning first would evolve the table it is about to drop."""
    # Arrange
    calls: list[str] = []
    monkeypatch.setattr(
        manage_iceberg, "delete_table", lambda *a: calls.append("delete")
    )
    monkeypatch.setattr(
        manage_iceberg, "provision_table", lambda *a: calls.append("provision")
    )
    args = manage_iceberg.build_parser().parse_args(
        ["table", "recreate", "--name", "bronze.x", "--csv", "a.csv"]
    )

    # Act
    exit_code = manage_iceberg.handle_table(CATALOG, args)

    # Assert
    assert exit_code == manage_iceberg.EXIT_OK
    assert calls == ["delete", "provision"]


@pytest.mark.parametrize(
    ("action", "expected"),
    [("create", "build"), ("drop", "delete"), ("view", "view")],
)
def test_namespace_actions_route_to_their_helper(
    action: str, expected: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Arrange
    calls: list[str] = []
    monkeypatch.setattr(
        manage_iceberg, "build_namespace", lambda *a: calls.append("build")
    )
    monkeypatch.setattr(
        manage_iceberg, "delete_namespace", lambda *a: calls.append("delete")
    )
    monkeypatch.setattr(
        manage_iceberg, "view_namespace", lambda *a: calls.append("view")
    )
    args = manage_iceberg.build_parser().parse_args(
        ["namespace", action, "--name", "bronze"]
    )

    # Act
    exit_code = manage_iceberg.handle_namespace(CATALOG, args)

    # Assert
    assert exit_code == manage_iceberg.EXIT_OK
    assert calls == [expected]

"""Guards that JEPX stays on the Python transform path and never returns to dbt.

JEPX silver moved from dbt models to Python + DuckDB + PyIceberg; the dbt
project is kept only as a shell for a possible future gold layer. These tests
fail if a JEPX dbt model reappears or if the docs start advertising the
retired dbt commands again.
"""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

DBT_PROJECT_DIR = ROOT / "src/dbt/jepx_power"
README = ROOT / "README.md"


def test_jepx_has_no_dbt_models() -> None:
    """No .sql model may exist anywhere in the dbt project.

    The dbt project directory is asserted to exist first on purpose. The
    previous version of this test globbed src/dbt/jepx_power/models, a
    directory that no longer exists, and Path.rglob on a missing directory
    returns nothing -- so it passed no matter what, and would have kept
    passing even if the whole dbt project were deleted.
    """
    assert DBT_PROJECT_DIR.is_dir(), (
        f"{DBT_PROJECT_DIR} is missing, so this test would silently pass "
        "without checking anything. Update the path or drop the test."
    )

    # target/ holds dbt build artifacts (gitignored), not authored models.
    models = [
        path
        for path in DBT_PROJECT_DIR.rglob("*.sql")
        if "target" not in path.relative_to(DBT_PROJECT_DIR).parts
    ]

    assert models == []


def test_readme_mentions_jepx_silver_command() -> None:
    readme = README.read_text(encoding="utf-8")

    assert "ingest-jepx-bronze-to-silver" in readme
    assert "run-jepx-staging-dbt" not in readme
    assert "run-jepx-silver-dbt" not in readme
    assert "JEPX" in readme

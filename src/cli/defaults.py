"""Shared default values for the CLI (src/main.py and src/cli/commands/*).

Single source of truth for bucket/catalog/schema-path defaults that used to
be repeated as literals across ~20 argparse.add_argument() calls in
src/main.py. See docs/tasks/refactaring_20260817.md section 2.7.
"""

from __future__ import annotations

from pathlib import Path

DEFAULT_BUCKET = "jp-power-grid-dev"
DEFAULT_CATALOG = "dlh_dev"
DEFAULT_DBT_PROJECT_DIR = "/workspace/src/dbt/jepx_power"

SCHEMA_ROOT = Path("/workspace/configuration/iceberg/schema")
BRONZE_SCHEMA_ROOT = SCHEMA_ROOT / "bronze"
SILVER_SCHEMA_ROOT = SCHEMA_ROOT / "silver"


def bronze_schema_path(dataset: str, file_name: str) -> str:
    """Resolve a bronze schema CSV path from the dataset-subfolder convention."""
    return str(BRONZE_SCHEMA_ROOT / dataset / file_name)

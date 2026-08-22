"""Resolve (table_identifier, schema_csv_path) pairs from a schema directory.

`configuration/iceberg/schema/` is the source of truth for every table, and
the provisioning commands walk it under the "CSV file name == table name"
convention. That walk started in silver_admin.py with the `silver.` namespace
baked in; it lives here now so the metadata namespace can reuse it rather
than copy it.
"""

from __future__ import annotations

import argparse
from pathlib import Path


def iter_schema_files(
    parser: argparse.ArgumentParser, schema_dir: str, namespace: str
) -> list[tuple[str, Path]]:
    """Pair every schema CSV under a directory with its table identifier.

    Errors through the parser rather than raising: a missing or empty schema
    directory is a mistake in the invocation, not a runtime fault, and exiting
    2 with usage is more useful than a traceback.
    """
    directory = Path(schema_dir)
    if not directory.exists():
        parser.error(f"Schema directory does not exist: {directory}")

    schema_files = sorted(directory.rglob("*.csv"))
    if not schema_files:
        parser.error(f"No schema CSV files found in: {directory}")

    return [
        (f"{namespace}.{schema_file.stem}", schema_file) for schema_file in schema_files
    ]

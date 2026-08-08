"""Unit tests for provision_table(), including partition spec behavior.

Uses a local SQLite catalog with a tmp_path warehouse, so these tests need
neither RustFS nor the project's real dlh_dev catalog.
"""

from __future__ import annotations

import csv
import logging
from pathlib import Path

from pyiceberg.catalog import Catalog
from pyiceberg.catalog.sql import SqlCatalog

from common.iceberg import provision_table

CSV_HEADER = [
    "field_id",
    "name",
    "type",
    "is_identifier",
    "required",
    "doc",
    "partition_transform",
    "source_name",
    "comment",
]


def _write_schema_csv(path: Path, rows: list[dict[str, str]]) -> Path:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_HEADER)
        writer.writeheader()
        for row in rows:
            writer.writerow({column: row.get(column, "") for column in CSV_HEADER})
    return path


def _local_catalog(tmp_path: Path) -> Catalog:
    warehouse = tmp_path / "warehouse"
    warehouse.mkdir()
    catalog = SqlCatalog(
        "test",
        **{
            "uri": f"sqlite:///{tmp_path / 'catalog.db'}",
            "warehouse": f"file://{warehouse}",
        },
    )
    catalog.create_namespace_if_not_exists("bronze")
    catalog.create_namespace_if_not_exists("silver")
    return catalog


def test_provision_table_creates_partitioned_table_from_partition_transform(
    tmp_path: Path,
) -> None:
    catalog = _local_catalog(tmp_path)
    csv_path = _write_schema_csv(
        tmp_path / "schema.csv",
        [
            {
                "field_id": "1",
                "name": "target_date",
                "type": "date",
                "is_identifier": "TRUE",
                "required": "TRUE",
                "doc": "target date",
                "partition_transform": "year",
            },
        ],
    )

    provision_table(catalog, "silver.partitioned_table", str(csv_path))

    table = catalog.load_table("silver.partitioned_table")
    assert not table.spec().is_unpartitioned()


def test_provision_table_creates_unpartitioned_table_when_transforms_empty(
    tmp_path: Path,
) -> None:
    catalog = _local_catalog(tmp_path)
    csv_path = _write_schema_csv(
        tmp_path / "schema.csv",
        [
            {
                "field_id": "1",
                "name": "id",
                "type": "string",
                "is_identifier": "TRUE",
                "required": "TRUE",
                "doc": "id",
            },
        ],
    )

    provision_table(catalog, "silver.unpartitioned_table", str(csv_path))

    table = catalog.load_table("silver.unpartitioned_table")
    assert table.spec().is_unpartitioned()


def test_provision_table_warns_on_partition_spec_drift_without_raising(
    tmp_path: Path, caplog
) -> None:
    catalog = _local_catalog(tmp_path)
    unpartitioned_csv = _write_schema_csv(
        tmp_path / "schema_v1.csv",
        [
            {
                "field_id": "1",
                "name": "target_date",
                "type": "date",
                "is_identifier": "TRUE",
                "required": "TRUE",
                "doc": "target date",
            },
        ],
    )
    provision_table(catalog, "silver.drifted_table", str(unpartitioned_csv))
    assert catalog.load_table("silver.drifted_table").spec().is_unpartitioned()

    partitioned_csv = _write_schema_csv(
        tmp_path / "schema_v2.csv",
        [
            {
                "field_id": "1",
                "name": "target_date",
                "type": "date",
                "is_identifier": "TRUE",
                "required": "TRUE",
                "doc": "target date",
                "partition_transform": "year",
            },
        ],
    )

    with caplog.at_level(logging.WARNING):
        provision_table(catalog, "silver.drifted_table", str(partitioned_csv))

    # Existing tables are never auto-evolved (see docs/tasks/tasks.md §8.1),
    # so the table must stay unpartitioned even though the CSV now asks for one.
    assert catalog.load_table("silver.drifted_table").spec().is_unpartitioned()
    assert any(
        "drift" in record.message.lower() or "partition" in record.message.lower()
        for record in caplog.records
    )

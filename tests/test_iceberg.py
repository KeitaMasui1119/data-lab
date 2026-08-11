"""Unit tests for provision_table() and evolve_partition_spec().

Uses a local SQLite catalog with a tmp_path warehouse, so these tests need
neither RustFS nor the project's real dlh_dev catalog.
"""

from __future__ import annotations

import csv
import logging
from datetime import date
from pathlib import Path

import pyarrow as pa
from pyiceberg.catalog import Catalog
from pyiceberg.catalog.sql import SqlCatalog

from common.iceberg import evolve_partition_spec, provision_table

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


def test_evolve_partition_spec_adds_field_missing_from_the_live_table(
    tmp_path: Path,
) -> None:
    catalog = _local_catalog(tmp_path)
    unpartitioned_csv = _write_schema_csv(
        tmp_path / "schema_v1.csv",
        [
            {
                "field_id": "1",
                "name": "delivery_date",
                "type": "date",
                "is_identifier": "TRUE",
                "required": "TRUE",
                "doc": "delivery date",
            },
        ],
    )
    provision_table(catalog, "silver.evolve_me", str(unpartitioned_csv))
    assert catalog.load_table("silver.evolve_me").spec().is_unpartitioned()

    partitioned_csv = _write_schema_csv(
        tmp_path / "schema_v2.csv",
        [
            {
                "field_id": "1",
                "name": "delivery_date",
                "type": "date",
                "is_identifier": "TRUE",
                "required": "TRUE",
                "doc": "delivery date",
                "partition_transform": "year",
            },
        ],
    )

    added = evolve_partition_spec(catalog, "silver.evolve_me", str(partitioned_csv))

    assert added == ["delivery_date"]
    evolved_table = catalog.load_table("silver.evolve_me")
    assert not evolved_table.spec().is_unpartitioned()
    assert [field.name for field in evolved_table.spec().fields] == [
        "delivery_date_year"
    ]


def test_evolve_partition_spec_is_a_noop_when_already_up_to_date(
    tmp_path: Path,
) -> None:
    catalog = _local_catalog(tmp_path)
    partitioned_csv = _write_schema_csv(
        tmp_path / "schema.csv",
        [
            {
                "field_id": "1",
                "name": "delivery_date",
                "type": "date",
                "is_identifier": "TRUE",
                "required": "TRUE",
                "doc": "delivery date",
                "partition_transform": "year",
            },
        ],
    )
    provision_table(catalog, "silver.already_partitioned", str(partitioned_csv))
    spec_before = catalog.load_table("silver.already_partitioned").spec()
    assert not spec_before.is_unpartitioned()

    added = evolve_partition_spec(
        catalog, "silver.already_partitioned", str(partitioned_csv)
    )

    assert added == []
    assert catalog.load_table("silver.already_partitioned").spec() == spec_before


def test_provision_table_does_not_warn_after_evolve_partition_spec(
    tmp_path: Path, caplog
) -> None:
    """A spec matched via evolve_partition_spec() must not look drifted.

    update_spec() bumps the live table's spec_id even when the resulting
    fields exactly match the schema CSV, so a spec_id-inclusive comparison in
    provision_table() would warn on every subsequent write forever.
    """
    catalog = _local_catalog(tmp_path)
    unpartitioned_csv = _write_schema_csv(
        tmp_path / "schema_v1.csv",
        [
            {
                "field_id": "1",
                "name": "delivery_date",
                "type": "date",
                "is_identifier": "TRUE",
                "required": "TRUE",
                "doc": "delivery date",
            },
        ],
    )
    provision_table(
        catalog, "silver.evolved_then_reprovisioned", str(unpartitioned_csv)
    )

    partitioned_csv = _write_schema_csv(
        tmp_path / "schema_v2.csv",
        [
            {
                "field_id": "1",
                "name": "delivery_date",
                "type": "date",
                "is_identifier": "TRUE",
                "required": "TRUE",
                "doc": "delivery date",
                "partition_transform": "year",
            },
        ],
    )
    evolve_partition_spec(
        catalog, "silver.evolved_then_reprovisioned", str(partitioned_csv)
    )

    with caplog.at_level(logging.WARNING):
        provision_table(
            catalog, "silver.evolved_then_reprovisioned", str(partitioned_csv)
        )

    assert not any("drift" in record.message.lower() for record in caplog.records)


def test_evolve_partition_spec_leaves_existing_data_files_in_their_old_layout(
    tmp_path: Path,
) -> None:
    """Spec evolution is metadata-only; it must not touch data already written.

    Historical files stay unpartitioned until something rewrites them (e.g. a
    full-refresh silver run); see docs/tasks/tasks.md §8.1.
    """
    catalog = _local_catalog(tmp_path)
    unpartitioned_csv = _write_schema_csv(
        tmp_path / "schema_v1.csv",
        [
            {
                "field_id": "1",
                "name": "delivery_date",
                "type": "date",
                "is_identifier": "TRUE",
                "required": "TRUE",
                "doc": "delivery date",
            },
        ],
    )
    provision_table(catalog, "silver.evolve_with_data", str(unpartitioned_csv))
    table = catalog.load_table("silver.evolve_with_data")

    arrow_table = pa.Table.from_pylist(
        [{"delivery_date": date(2026, 4, 1)}], schema=table.schema().as_arrow()
    )
    table.append(arrow_table)
    data_files_before = {
        entry["file_path"] for entry in table.inspect.data_files().to_pylist()
    }

    partitioned_csv = _write_schema_csv(
        tmp_path / "schema_v2.csv",
        [
            {
                "field_id": "1",
                "name": "delivery_date",
                "type": "date",
                "is_identifier": "TRUE",
                "required": "TRUE",
                "doc": "delivery date",
                "partition_transform": "year",
            },
        ],
    )
    evolve_partition_spec(catalog, "silver.evolve_with_data", str(partitioned_csv))

    data_files_after = {
        entry["file_path"]
        for entry in catalog.load_table("silver.evolve_with_data")
        .inspect.data_files()
        .to_pylist()
    }
    assert data_files_after == data_files_before

"""Unit tests for schema CSV parsing, including partition_transform handling."""

from __future__ import annotations

import csv
from pathlib import Path

import pytest

from common.iceberg.schema import build_partition_spec, build_table_schema

ROOT = Path(__file__).resolve().parents[1]
SCHEMA_DIR = ROOT / "configuration/iceberg/schema"

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


def test_build_partition_spec_returns_unpartitioned_when_all_transforms_empty(
    tmp_path: Path,
) -> None:
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
    schema = build_table_schema(str(csv_path))

    spec = build_partition_spec(str(csv_path), schema)

    assert spec.is_unpartitioned()


def test_build_partition_spec_builds_field_for_valid_transform(tmp_path: Path) -> None:
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
            {
                "field_id": "2",
                "name": "target_date",
                "type": "date",
                "is_identifier": "TRUE",
                "required": "TRUE",
                "doc": "target date",
                "partition_transform": "year",
            },
        ],
    )
    schema = build_table_schema(str(csv_path))

    spec = build_partition_spec(str(csv_path), schema)

    assert not spec.is_unpartitioned()
    assert len(spec.fields) == 1
    field = spec.fields[0]
    assert field.source_id == 2
    assert str(field.transform) == "year"


def test_build_partition_spec_raises_for_type_incompatible_transform(
    tmp_path: Path,
) -> None:
    csv_path = _write_schema_csv(
        tmp_path / "schema.csv",
        [
            {
                "field_id": "1",
                "name": "delivery_date",
                "type": "string",
                "is_identifier": "TRUE",
                "required": "TRUE",
                "doc": "delivery date",
                "partition_transform": "month",
            },
        ],
    )
    schema = build_table_schema(str(csv_path))

    with pytest.raises(ValueError, match="month"):
        build_partition_spec(str(csv_path), schema)


def test_build_partition_spec_assigns_sequential_field_ids_from_start(
    tmp_path: Path,
) -> None:
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
            {
                "field_id": "2",
                "name": "area_code",
                "type": "string",
                "is_identifier": "TRUE",
                "required": "TRUE",
                "doc": "area code",
                "partition_transform": "identity",
            },
        ],
    )
    schema = build_table_schema(str(csv_path))

    spec = build_partition_spec(str(csv_path), schema)

    field_ids = sorted(field.field_id for field in spec.fields)
    assert field_ids == [1000, 1001]


@pytest.mark.parametrize(
    "csv_path",
    sorted((SCHEMA_DIR / "bronze").glob("*.csv"))
    + sorted((SCHEMA_DIR / "silver").glob("*.csv")),
    ids=lambda path: str(path.relative_to(SCHEMA_DIR)),
)
def test_every_repo_schema_csv_produces_a_valid_partition_spec(csv_path: Path) -> None:
    """Regression guard: catches type-incompatible partition_transform values.

    bronze/jepx_spot_price.csv previously declared `month` on a `string`
    delivery_date column, which PyIceberg's own create_table() accepts
    silently and turns into a broken spec. This test would have failed on
    that value.
    """
    schema = build_table_schema(str(csv_path))

    build_partition_spec(str(csv_path), schema)

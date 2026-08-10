from __future__ import annotations

import csv
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

JEPX_BRONZE_SCHEMA = ROOT / "configuration/iceberg/schema/bronze/jepx_spot_price.csv"
JEPX_DBT_MODEL_DIR = ROOT / "src/dbt/jepx_power/models"

README = ROOT / "README.md"


def _read_csv_rows(path: Path) -> list[list[str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.reader(handle))


def test_jepx_bronze_schema_has_expected_columns() -> None:
    rows = _read_csv_rows(JEPX_BRONZE_SCHEMA)

    assert rows[0] == [
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
    assert rows[1][1] == "delivery_date"
    assert rows[1][2] == "string"
    assert rows[2][1] == "time_code"
    assert rows[2][2] == "string"
    assert rows[-1][1] == "block_purchase_contracted_volume"


def test_jepx_has_no_dbt_models() -> None:
    """JEPX is transformed in Python, so no JEPX dbt model may remain."""
    jepx_models = list(JEPX_DBT_MODEL_DIR.rglob("*jepx*.sql"))

    assert jepx_models == []


def test_readme_mentions_jepx_silver_command() -> None:
    readme = README.read_text(encoding="utf-8")

    assert "ingest-jepx-bronze-to-silver" in readme
    assert "run-jepx-staging-dbt" not in readme
    assert "run-jepx-silver-dbt" not in readme
    assert "JEPX" in readme

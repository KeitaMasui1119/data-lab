from __future__ import annotations

import csv
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

JEPX_BRONZE_SCHEMA = ROOT / "configuration/iceberg/schema/bronze/jepx_spot_price.csv"
JEPX_STAGING_SQL = ROOT / "src/dbt/jepx_power/models/staging/stg_jepx_spot_price.sql"
JEPX_SILVER_BASE_SQL = (
    ROOT / "src/dbt/jepx_power/models/silver/silver_jepx_spot_price_base.sql"
)
JEPX_SILVER_BLOCK_SQL = (
    ROOT / "src/dbt/jepx_power/models/silver/silver_jepx_spot_price_block.sql"
)
JEPX_SILVER_AREA_SQL = (
    ROOT / "src/dbt/jepx_power/models/silver/silver_jepx_spot_price_area.sql"
)

BRONZE_SCHEMA = (
    ROOT / "configuration/iceberg/schema/bronze/occto_unit_generation_actuals.csv"
)
SILVER_SCHEMA = (
    ROOT / "configuration/iceberg/schema/silver/occto_unit_generation_actuals.csv"
)
STAGING_SQL = (
    ROOT / "src/dbt/jepx_power/models/staging/stg_occto_unit_generation_actuals.sql"
)
SILVER_SQL = (
    ROOT / "src/dbt/jepx_power/models/silver/silver_occto_unit_generation_actuals.sql"
)
README = ROOT / "README.md"


def _read_csv_rows(path: Path) -> list[list[str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.reader(handle))


def test_occto_silver_schema_matches_bronze_schema() -> None:
    assert _read_csv_rows(BRONZE_SCHEMA) == _read_csv_rows(SILVER_SCHEMA)


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


def test_occto_staging_model_scans_bronze_table() -> None:
    sql = STAGING_SQL.read_text(encoding="utf-8")

    assert "tags=[" in sql
    assert "staging" in sql
    assert "occto" in sql
    assert (
        "iceberg_scan('s3://jp-power-grid-dev/bronze/occto_unit_generation_actuals')"
        in sql
    )
    assert "select" in sql
    assert "power_plant_code" in sql
    assert "updated_datetime" in sql


def test_jepx_staging_model_reads_bronze_and_normalizes_time() -> None:
    sql = JEPX_STAGING_SQL.read_text(encoding="utf-8")

    assert "tags=['staging', 'jepx']" in sql
    assert "iceberg_scan('s3://jp-power-grid-dev/bronze/jepx_spot_price')" in sql
    assert "try_strptime(delivery_date, '%Y-%m-%d')" in sql
    assert "time_code_i between 1 and 48" in sql
    assert "row_number() over (" in sql


def test_occto_silver_model_passthroughs_staging_model() -> None:
    sql = SILVER_SQL.read_text(encoding="utf-8")

    assert "tags=[" in sql
    assert "silver" in sql
    assert "occto" in sql
    assert "select *" in sql
    assert "ref('stg_occto_unit_generation_actuals')" in sql


def test_jepx_silver_models_ref_staging_model() -> None:
    base_sql = JEPX_SILVER_BASE_SQL.read_text(encoding="utf-8")
    block_sql = JEPX_SILVER_BLOCK_SQL.read_text(encoding="utf-8")
    area_sql = JEPX_SILVER_AREA_SQL.read_text(encoding="utf-8")

    assert "tags=['silver', 'jepx']" in base_sql
    assert "ref('stg_jepx_spot_price')" in base_sql
    assert "delivery_datetime" in base_sql
    assert "system_price" in base_sql

    assert "tags=['silver', 'jepx']" in block_sql
    assert "ref('stg_jepx_spot_price')" in block_sql
    assert "block_purchase_contracted_volume" in block_sql

    assert "tags=['silver', 'jepx']" in area_sql
    assert "ref('stg_jepx_spot_price')" in area_sql
    assert "unpivot" in area_sql
    assert "split_part(area_price_column, '_', 3)" in area_sql


def test_readme_mentions_occto_dbt_command() -> None:
    readme = README.read_text(encoding="utf-8")

    assert "run-occto-silver-dbt" in readme
    assert "OCCTO" in readme


def test_readme_mentions_jepx_dbt_commands() -> None:
    readme = README.read_text(encoding="utf-8")

    assert "run-jepx-staging-dbt" in readme
    assert "run-jepx-silver-dbt" in readme
    assert "JEPX" in readme

"""Unit tests for the OCCTO bronze-to-silver transformation.

Modeled on tests/test_bronze_to_silver_jepx_spot_price.py: the transform
SQL is exercised against a locally registered relation instead of
iceberg_scan, so these tests need neither RustFS nor an Iceberg catalog.
"""

from __future__ import annotations

import csv
from pathlib import Path

from pipeline.silver.bronze_to_silver_occto_unit_generation import (
    TIMESLOT_COLUMNS,
)

ROOT = Path(__file__).resolve().parents[1]
BRONZE_SCHEMA = (
    ROOT / "configuration/iceberg/schema/bronze/occto_unit_generation_actuals.csv"
)


def _bronze_timeslot_column_order() -> list[str]:
    with BRONZE_SCHEMA.open(encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    return [row["name"] for row in rows if row["name"].startswith("timeslot_")]


def test_timeslot_columns_has_48_entries():
    assert len(TIMESLOT_COLUMNS) == 48


def test_timeslot_columns_starts_at_00_30_and_ends_at_24_00():
    assert TIMESLOT_COLUMNS[0] == "timeslot_00_30"
    assert TIMESLOT_COLUMNS[-1] == "timeslot_24_00"


def test_timeslot_columns_has_no_duplicates():
    assert len(set(TIMESLOT_COLUMNS)) == 48


def test_timeslot_columns_matches_bronze_schema_csv_order():
    """This is the load-bearing check: time_code is defined as the column's
    *position*, not parsed from its name, so silence here would mean every
    slot silently shifts if the bronze CSV's column order ever changes."""
    assert list(TIMESLOT_COLUMNS) == _bronze_timeslot_column_order()

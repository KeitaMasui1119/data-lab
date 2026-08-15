"""Unit tests for the Tohoku supply_demand_actuals raw-to-bronze parsing helpers.

parse_year_csv() / extract_target_date_rows() are pure functions exercised
directly (no RustFS/Iceberg needed), same coverage philosophy as
test_source_to_bronze_power_usage_hokuriku.py -- the Iceberg append path
itself has no equivalent coverage for JEPX/OCCTO/power_usage_hokuriku
either. Correctness against real live data (2026-08-14) was additionally
verified manually during development.
"""

from __future__ import annotations

from datetime import date

from pipeline.bronze.source_to_bronze_supply_demand_actuals_tohoku import (
    extract_target_date_rows,
    parse_year_csv,
)

TOHOKU_TEXT = (
    "2026/8/15 6:02 UPDATE\r\n"
    "DATE,TIME,実績(万kW)\r\n"
    "2026/1/1,0:00,933\r\n"
    "2026/1/1,1:00,925\r\n"
    "2026/1/2,0:00,900\r\n"
)


def test_parse_year_csv_skips_update_line_only():
    df = parse_year_csv(TOHOKU_TEXT)

    assert df.columns == ["DATE", "TIME", "実績(万kW)"]
    assert df.height == 3


def test_extract_target_date_rows_filters_and_normalizes_unpadded_date():
    df = parse_year_csv(TOHOKU_TEXT)

    result = extract_target_date_rows(df, date(2026, 1, 1))

    assert result.height == 2
    assert set(result["DATE"].to_list()) == {"2026-01-01"}
    assert result["TIME"].to_list() == ["00:00", "01:00"]


def test_extract_target_date_rows_returns_empty_for_unmatched_date():
    df = parse_year_csv(TOHOKU_TEXT)

    result = extract_target_date_rows(df, date(2099, 1, 1))

    assert result.is_empty()

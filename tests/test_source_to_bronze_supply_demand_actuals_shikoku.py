"""Unit tests for the Shikoku supply_demand_actuals raw-to-bronze parsing helpers.

parse_year_csv() / extract_target_date_rows() are pure functions exercised
directly (no RustFS/Iceberg needed), same coverage philosophy as
test_source_to_bronze_power_usage_hokuriku.py -- the Iceberg append path
itself has no equivalent coverage for JEPX/OCCTO/power_usage_hokuriku
either. Correctness against real live data (2026-08-14) was additionally
verified manually during development.
"""

from __future__ import annotations

from datetime import date

from pipeline.bronze.source_to_bronze_supply_demand_actuals_shikoku import (
    extract_target_date_rows,
    parse_year_csv,
)

SHIKOKU_TEXT = (
    "2026/08/15 00:00 UPDATE\r\n"
    "\r\n"
    "DATE,TIME,実績(万kW),供給力想定値(万kW)\r\n"
    "2026/01/01,0:00,251,307\r\n"
    "2026/01/01,1:00,251,310\r\n"
)


def test_parse_year_csv_keeps_4_columns():
    df = parse_year_csv(SHIKOKU_TEXT)

    assert df.columns == ["DATE", "TIME", "実績(万kW)", "供給力想定値(万kW)"]
    assert df.height == 2


def test_extract_target_date_rows_normalizes_zero_padded_source_date():
    df = parse_year_csv(SHIKOKU_TEXT)

    result = extract_target_date_rows(df, date(2026, 1, 1))

    assert result.height == 2
    assert result["DATE"].to_list() == ["2026-01-01", "2026-01-01"]
    assert result["供給力想定値(万kW)"].to_list() == ["307", "310"]


def test_extract_target_date_rows_returns_empty_for_unmatched_date():
    df = parse_year_csv(SHIKOKU_TEXT)

    result = extract_target_date_rows(df, date(2099, 1, 1))

    assert result.is_empty()

"""Unit tests for the Hokuriku power_usage raw-to-bronze parsing and
ingestion helpers.

parse_snapshot() is the core testable unit — a pure function exercised
against real sample files under data/electric_forecast/hokuriku/ — plus the
same ingestion-log helper coverage pattern as
tests/test_source_to_bronze_occto.py (the Iceberg append path itself has no
equivalent coverage for JEPX or OCCTO either).
"""

from __future__ import annotations

import io
from datetime import UTC, date, datetime
from pathlib import Path
from unittest.mock import MagicMock

import polars as pl
import pytest

from pipeline.bronze.source_to_bronze_power_usage_hokuriku import (
    INGESTION_LOG_KEY,
    _resolve_effective_source_file_name,
    mark_ingestion_log_processed,
    parse_snapshot,
    resolve_raw_object_from_ingestion_log,
)

DATA_DIR = (
    Path(__file__).resolve().parents[1] / "data" / "electric_forecast" / "hokuriku"
)


def _load(file_name: str) -> str:
    return (DATA_DIR / file_name).read_bytes().decode("cp932")


# ---------------------------------------------------------------------------
# parse_snapshot() — ordinary file, truly-blank-line-separated blocks
# ---------------------------------------------------------------------------


def test_parse_snapshot_splits_into_three_rows_with_expected_key_counts():
    text = _load("juyo_05_20200401.csv")

    snapshot = parse_snapshot(text, date(2020, 4, 1))

    assert len(snapshot.daily_summary) == 44
    assert len(snapshot.hourly) == 98
    assert len(snapshot.interval5) == 578


def test_parse_snapshot_extracts_known_daily_summary_values():
    text = _load("juyo_05_20200401.csv")

    snapshot = parse_snapshot(text, date(2020, 4, 1))

    assert snapshot.daily_summary["target_date"] == "2020-04-01"
    assert snapshot.daily_summary["file_updated_at"] == "2020/04/02 00:10 UPDATE"
    assert snapshot.daily_summary["today_peak_supply_capacity"] == "123"
    assert snapshot.daily_summary["today_peak_supply_time_range"] == "19:00～20:00"
    assert snapshot.daily_summary["today_max_usage_rate_pct"] == "69"
    assert snapshot.daily_summary["next_day_peak_supply_capacity"] == "144"


def test_parse_snapshot_extracts_known_hourly_values_by_position():
    text = _load("juyo_05_20200401.csv")

    snapshot = parse_snapshot(text, date(2020, 4, 1))

    assert snapshot.hourly["hourly_00_00_actual_demand"] == "68"
    assert snapshot.hourly["hourly_00_00_supply_capacity"] == "111"
    assert snapshot.hourly["hourly_23_00_usage_rate_pct"] == "55"


def test_parse_snapshot_extracts_known_interval5_values():
    text = _load("juyo_05_20200401.csv")

    snapshot = parse_snapshot(text, date(2020, 4, 1))

    assert snapshot.interval5["interval5_00_00_actual_demand"] == "71"
    assert snapshot.interval5["interval5_23_55_solar_generation_actual"] == "0"


# ---------------------------------------------------------------------------
# parse_snapshot() — comma-padded separators (observed on a handful of 2023
# dates: blank block separators rendered as ',,,,,' instead of '')
# ---------------------------------------------------------------------------


def test_parse_snapshot_handles_comma_padded_separators():
    text = _load("juyo_05_20230101.csv")

    snapshot = parse_snapshot(text, date(2023, 1, 1))

    assert snapshot.daily_summary["today_peak_supply_capacity"] == "119"
    assert snapshot.daily_summary["next_day_peak_supply_capacity"] == "121"
    assert snapshot.hourly["hourly_00_00_supply_capacity"] == "122"
    assert snapshot.interval5["interval5_00_00_actual_demand"] == "73"


# ---------------------------------------------------------------------------
# parse_snapshot() — legitimate all-empty-values data line (a "next day"
# block fetched before next day's figures are published), which must NOT be
# mistaken for a comma-padded separator
# ---------------------------------------------------------------------------


def test_parse_snapshot_keeps_genuinely_empty_next_day_block_as_data():
    text = _load("juyo_05_20251212.csv")

    snapshot = parse_snapshot(text, date(2025, 12, 12))

    assert snapshot.daily_summary["next_day_peak_supply_capacity"] == ""
    assert snapshot.daily_summary["next_day_peak_supply_time_range"] == ""
    assert snapshot.hourly["hourly_14_00_actual_demand"] == ""
    assert snapshot.interval5["interval5_23_55_actual_demand"] == ""


# ---------------------------------------------------------------------------
# parse_snapshot() — structural errors raise clearly rather than silently
# producing wrong data
# ---------------------------------------------------------------------------


def test_parse_snapshot_raises_on_empty_text():
    with pytest.raises(ValueError, match="Empty snapshot text"):
        parse_snapshot("", date(2026, 1, 1))


def test_parse_snapshot_raises_on_unexpected_block_count():
    text = "2026/01/02 00:00 UPDATE\nheader\n1,2,3\n"

    with pytest.raises(ValueError, match="Expected .* blocks"):
        parse_snapshot(text, date(2026, 1, 1))


# ---------------------------------------------------------------------------
# source_data defaulting — same rationale as OCCTO: a revision (different
# ingested_at) must not collide with the previous one on source_data
# ---------------------------------------------------------------------------


def test_source_file_name_defaults_to_full_object_key_when_not_provided():
    object_key = (
        "raw/power_usage/hokuriku/target_date=2026-08-07/"
        "ingested_at=20260807T093000/juyo_05_20260807.csv"
    )

    assert _resolve_effective_source_file_name(object_key, None) == object_key


def test_source_file_name_uses_explicit_override_when_given():
    object_key = "raw/power_usage/hokuriku/target_date=2026-08-07/.../file.csv"

    assert (
        _resolve_effective_source_file_name(object_key, "custom-label")
        == "custom-label"
    )


# ---------------------------------------------------------------------------
# ingestion log resolution / marking — same helpers as OCCTO's, keyed by
# dataset="power_usage_hokuriku"
# ---------------------------------------------------------------------------


def _to_parquet_bytes(df: pl.DataFrame) -> bytes:
    buffer = io.BytesIO()
    df.write_parquet(buffer)
    return buffer.getvalue()


def _log_df(rows: list[dict]) -> pl.DataFrame:
    columns = {
        "dataset": pl.Utf8,
        "fiscal_year": pl.Int64,
        "snapshot_date": pl.Utf8,
        "ingested_at": pl.Utf8,
        "file_hash": pl.Utf8,
        "file_path": pl.Utf8,
        "content_length": pl.Int64,
        "etag": pl.Utf8,
        "last_modified": pl.Utf8,
        "is_latest": pl.Boolean,
        "bronze_status": pl.Utf8,
        "bronze_processed_at": pl.Utf8,
    }
    return pl.DataFrame(
        {
            name: pl.Series([row.get(name) for row in rows], dtype=dtype)
            for name, dtype in columns.items()
        }
    )


def test_resolve_raw_object_from_ingestion_log_returns_latest_unprocessed():
    log_df = _log_df(
        [
            {
                "dataset": "power_usage_hokuriku",
                "snapshot_date": "2026-08-07",
                "ingested_at": "2026-08-07T09:00:00+00:00",
                "file_hash": "hash-1",
                "file_path": (
                    "raw/power_usage/hokuriku/target_date=2026-08-07/"
                    "ingested_at=20260807T090000/file.csv"
                ),
                "content_length": 10,
                "is_latest": False,
                "bronze_status": "processed",
                "bronze_processed_at": "2026-08-07T09:30:00+00:00",
            },
            {
                "dataset": "power_usage_hokuriku",
                "snapshot_date": "2026-08-07",
                "ingested_at": "2026-08-07T15:30:00+00:00",
                "file_hash": "hash-2",
                "file_path": (
                    "raw/power_usage/hokuriku/target_date=2026-08-07/"
                    "ingested_at=20260807T153000/file.csv"
                ),
                "content_length": 11,
                "is_latest": True,
                "bronze_status": "pending",
                "bronze_processed_at": None,
            },
        ]
    )
    client = MagicMock()
    client.get_object_or_none.return_value = _to_parquet_bytes(log_df)

    object_key, source_file_name = resolve_raw_object_from_ingestion_log(
        client=client,
        bucket_name="test-bucket",
        target_date=datetime(2026, 8, 7, tzinfo=UTC).date(),
        require_unprocessed=True,
    )

    assert object_key == (
        "raw/power_usage/hokuriku/target_date=2026-08-07/"
        "ingested_at=20260807T153000/file.csv"
    )
    assert source_file_name == object_key


def test_mark_ingestion_log_processed_updates_target_row():
    log_df = _log_df(
        [
            {
                "dataset": "power_usage_hokuriku",
                "snapshot_date": "2026-08-07",
                "ingested_at": "2026-08-07T15:30:00+00:00",
                "file_hash": "hash-2",
                "file_path": (
                    "raw/power_usage/hokuriku/target_date=2026-08-07/"
                    "ingested_at=20260807T153000/file.csv"
                ),
                "content_length": 11,
                "is_latest": True,
                "bronze_status": "pending",
                "bronze_processed_at": None,
            },
        ]
    )
    client = MagicMock()
    client.get_object_or_none.return_value = _to_parquet_bytes(log_df)

    target_object = (
        "raw/power_usage/hokuriku/target_date=2026-08-07/"
        "ingested_at=20260807T153000/file.csv"
    )
    updated = mark_ingestion_log_processed(
        client=client,
        bucket_name="test-bucket",
        object_key=target_object,
        processed_at=datetime(2026, 8, 7, 16, 0, 0, tzinfo=UTC),
    )

    assert updated is True
    client.upload_bytes.assert_called_once()

    upload_call = client.upload_bytes.call_args
    assert upload_call.kwargs["object_name"] == INGESTION_LOG_KEY

    updated_df = pl.read_parquet(io.BytesIO(upload_call.kwargs["body"]))
    target = updated_df.filter(pl.col("file_path") == target_object)
    assert target.item(0, "bronze_status") == "processed"
    assert target.item(0, "bronze_processed_at") == "2026-08-07T16:00:00+00:00"

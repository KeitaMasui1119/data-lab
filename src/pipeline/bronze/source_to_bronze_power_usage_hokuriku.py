"""Hokuriku power_usage source-to-bronze parsing and ingestion workflow.

The source CSV (juyo_05_YYYYMMDD.csv) is a multi-section daily snapshot
report, not a flat 1-row-per-CSV-column file, so
common.pipeline_utilities.build_schema_exprs() cannot be used directly (see
docs/tasks/tasks.md). This module parses the snapshot into three wide rows —
daily_summary / hourly / interval5 — one per split Bronze table, before
casting and appending each.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date

import polars as pl

from common.iceberg.catalog import get_catalog
from common.pipeline_utilities import add_metadata
from common.raw_ingestion_log import (
    DEFAULT_INGESTION_LOG_KEY,
    mark_raw_object_processed,
    resolve_latest_raw_object_by_snapshot_date,
)
from common.raw_object_io import read_object_text
from common.storage_client import RustFSClient
from common.utilities import gen_uuid

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)

SCHEMA_DIR = "/workspace/configuration/iceberg/schema/bronze/power_usage_hokuriku"
DATASET_NAME = "power_usage_hokuriku"
INGESTION_LOG_KEY = DEFAULT_INGESTION_LOG_KEY

# name -> (table identifier, schema CSV path)
TABLES: dict[str, tuple[str, str]] = {
    "daily_summary": (
        "bronze.power_usage_hokuriku_daily_summary",
        f"{SCHEMA_DIR}/power_usage_hokuriku_daily_summary.csv",
    ),
    "hourly": (
        "bronze.power_usage_hokuriku_hourly",
        f"{SCHEMA_DIR}/power_usage_hokuriku_hourly.csv",
    ),
    "interval5": (
        "bronze.power_usage_hokuriku_interval5",
        f"{SCHEMA_DIR}/power_usage_hokuriku_interval5.csv",
    ),
}

TODAY_BLOCK_SPECS: list[list[str]] = [
    [
        "today_peak_supply_capacity",
        "today_peak_supply_time_range",
        "today_peak_supply_updated_date",
        "today_peak_supply_updated_time",
        "today_peak_supply_reserve_margin_pct",
        "today_peak_supply_usage_rate_pct",
    ],
    [
        "today_forecasted_peak_demand",
        "today_forecasted_peak_time_range",
        "today_forecasted_peak_updated_date",
        "today_forecasted_peak_updated_time",
    ],
    [
        "today_usage_rate_peak_supply_capacity",
        "today_usage_rate_peak_supply_time_range",
        "today_usage_rate_peak_supply_updated_date",
        "today_usage_rate_peak_supply_updated_time",
        "today_usage_rate_peak_supply_reserve_margin_pct",
        "today_usage_rate_peak_supply_usage_rate_pct",
    ],
    [
        "today_usage_rate_peak_forecasted_demand",
        "today_usage_rate_peak_forecasted_time_range",
        "today_usage_rate_peak_forecasted_updated_date",
        "today_usage_rate_peak_forecasted_updated_time",
    ],
]

MAX_USAGE_RATE_FIELDS = ["today_max_usage_rate_pct", "today_max_usage_rate_time_range"]

NEXT_DAY_BLOCK_SPECS: list[list[str]] = [
    [
        "next_day_peak_supply_capacity",
        "next_day_peak_supply_time_range",
        "next_day_peak_supply_updated_date",
        "next_day_peak_supply_updated_time",
        "next_day_peak_supply_reserve_margin_pct",
        "next_day_peak_supply_usage_rate_pct",
    ],
    [
        "next_day_forecasted_peak_demand",
        "next_day_forecasted_peak_time_range",
        "next_day_forecasted_peak_updated_date",
        "next_day_forecasted_peak_updated_time",
    ],
    [
        "next_day_usage_rate_peak_supply_capacity",
        "next_day_usage_rate_peak_supply_time_range",
        "next_day_usage_rate_peak_supply_updated_date",
        "next_day_usage_rate_peak_supply_updated_time",
        "next_day_usage_rate_peak_supply_reserve_margin_pct",
        "next_day_usage_rate_peak_supply_usage_rate_pct",
    ],
    [
        "next_day_usage_rate_peak_forecasted_demand",
        "next_day_usage_rate_peak_forecasted_time_range",
        "next_day_usage_rate_peak_forecasted_updated_date",
        "next_day_usage_rate_peak_forecasted_updated_time",
    ],
]

HOURLY_METRICS = [
    "actual_demand",
    "forecasted_demand",
    "usage_rate_pct",
    "supply_capacity",
]
HOURLY_ROW_COUNT = 24

INTERVAL5_METRICS = ["actual_demand", "solar_generation_actual"]
INTERVAL5_ROW_COUNT = 288
INTERVAL5_STEP_MINUTES = 5

EXPECTED_BLOCK_COUNT = len(TODAY_BLOCK_SPECS) + 1 + 1 + len(NEXT_DAY_BLOCK_SPECS) + 1


@dataclass(frozen=True)
class HokurikuSnapshotRows:
    """One parsed daily snapshot, split by destination Bronze table."""

    daily_summary: dict[str, str]
    hourly: dict[str, str]
    interval5: dict[str, str]


def _is_blank_line(line: str) -> bool:
    """True for a truly empty line — the normal block separator."""
    return line == ""


def _is_padded_separator_line(line: str) -> bool:
    """True for a line padded to a fixed column width with only empty fields
    (e.g. ',,,,,'). Some file vintages (observed on a handful of 2023 dates)
    pad every row, including separators, to a fixed comma count instead of
    using a truly empty line.

    This is ambiguous on its own — a legitimate data line can also be
    entirely empty values (e.g. a "next day" block fetched before next day's
    figures are published, observed 2025-12-12) — so it is only used as a
    fallback when splitting on true blank lines doesn't yield the expected
    block structure (see parse_snapshot()), never as the primary rule.
    """
    return all(part == "" for part in line.split(","))


def _split_into_blocks(
    lines: list[str], is_separator: Callable[[str], bool]
) -> list[list[str]]:
    """Group lines into separator-delimited blocks, dropping empty blocks.

    Block order (not absolute line numbers) is what parse_snapshot() relies
    on, so this is tolerant of incidental blank-line-count drift between
    file vintages.
    """
    blocks: list[list[str]] = []
    current: list[str] = []
    for line in lines:
        if is_separator(line):
            if current:
                blocks.append(current)
                current = []
            continue
        current.append(line)
    if current:
        blocks.append(current)
    return blocks


def _take_expected_values(
    values: list[str], expected_count: int, context: str
) -> list[str]:
    """Return the first `expected_count` values, tolerating trailing empty
    padding fields beyond that count (see _is_separator_line); raises if
    there are too few values, or if an excess trailing value is non-empty.
    """
    if len(values) < expected_count:
        raise ValueError(
            f"Expected at least {expected_count} values, got {len(values)}: {context!r}"
        )
    extra = values[expected_count:]
    if any(v != "" for v in extra):
        raise ValueError(
            f"Expected {expected_count} values, got non-empty extras {extra!r}: "
            f"{context!r}"
        )
    return values[:expected_count]


def _parse_simple_block(block: list[str], field_names: list[str]) -> dict[str, str]:
    """A header line + one data line; zip data values to field_names by position."""
    if len(block) != 2:
        raise ValueError(
            f"Expected a 2-line block (header + data), got {len(block)} lines: "
            f"{block!r}"
        )
    values = _take_expected_values(block[1].split(","), len(field_names), block[1])
    return dict(zip(field_names, values, strict=True))


def _parse_table_block(
    block: list[str],
    row_count: int,
    metrics: list[str],
    field_name_for: Callable[[int, str], str],
) -> dict[str, str]:
    """A table block is a header line + `row_count` data rows (DATE,TIME,<metrics>).

    Metric values are extracted by position, not by header text — the hourly
    table's supply-capacity column label is known to drift between file
    vintages (供給力(万kW) vs 供給力想定値(万kW)).
    """
    data_lines = block[1:]
    if len(data_lines) != row_count:
        raise ValueError(
            f"Expected {row_count} data rows, got {len(data_lines)}: {block!r}"
        )

    result: dict[str, str] = {}
    for row_index, line in enumerate(data_lines):
        values = line.split(",")
        metric_values = _take_expected_values(
            values[2:], len(metrics), line
        )  # skip DATE,TIME
        for metric, value in zip(metrics, metric_values, strict=True):
            result[field_name_for(row_index, metric)] = value
    return result


def _hourly_field_name(row_index: int, metric: str) -> str:
    return f"hourly_{row_index:02d}_00_{metric}"


def _interval5_field_name(row_index: int, metric: str) -> str:
    total_minutes = row_index * INTERVAL5_STEP_MINUTES
    hour, minute = divmod(total_minutes, 60)
    return f"interval5_{hour:02d}_{minute:02d}_{metric}"


def _assemble_snapshot(
    blocks: list[list[str]], file_updated_at: str, target_date: date
) -> HokurikuSnapshotRows:
    if len(blocks) != EXPECTED_BLOCK_COUNT:
        raise ValueError(f"Expected {EXPECTED_BLOCK_COUNT} blocks, got {len(blocks)}")

    block_iter = iter(blocks)
    daily_summary: dict[str, str] = {}

    for field_names in TODAY_BLOCK_SPECS:
        daily_summary.update(_parse_simple_block(next(block_iter), field_names))

    hourly = _parse_table_block(
        next(block_iter), HOURLY_ROW_COUNT, HOURLY_METRICS, _hourly_field_name
    )

    daily_summary.update(_parse_simple_block(next(block_iter), MAX_USAGE_RATE_FIELDS))

    for field_names in NEXT_DAY_BLOCK_SPECS:
        daily_summary.update(_parse_simple_block(next(block_iter), field_names))

    interval5 = _parse_table_block(
        next(block_iter), INTERVAL5_ROW_COUNT, INTERVAL5_METRICS, _interval5_field_name
    )

    keys = {"target_date": target_date.isoformat(), "file_updated_at": file_updated_at}
    return HokurikuSnapshotRows(
        daily_summary={**keys, **daily_summary},
        hourly={**keys, **hourly},
        interval5={**keys, **interval5},
    )


def parse_snapshot(text: str, target_date: date) -> HokurikuSnapshotRows:
    """Parse one juyo_05_YYYYMMDD.csv snapshot into three wide rows.

    Fixed block order: 4 "today" summary blocks, an hourly table (24 rows), a
    max-usage-rate block, 4 "next day" summary blocks, and a 5-minute-interval
    table (288 rows), each separated by a blank line.

    Blocks are normally separated by a truly empty line, which is tried
    first. A handful of file vintages instead pad every row — including
    separators — to a fixed comma count (',,,,,' instead of ''); if blank-line
    splitting doesn't yield the expected block structure, padded-separator
    splitting is tried as a fallback. Padded splitting isn't used as the
    primary rule because it's ambiguous with a legitimate all-empty-values
    data line (e.g. a "next day" block fetched before next day's figures are
    published).
    """
    lines = text.splitlines()
    if not lines:
        raise ValueError("Empty snapshot text")

    file_updated_at = lines[0]
    body_lines = lines[1:]

    try:
        blocks = _split_into_blocks(body_lines, is_separator=_is_blank_line)
        return _assemble_snapshot(blocks, file_updated_at, target_date)
    except ValueError:
        blocks = _split_into_blocks(body_lines, is_separator=_is_padded_separator_line)
        return _assemble_snapshot(blocks, file_updated_at, target_date)


def source_data_exists(table, source_file_name: str) -> bool:
    row_filter = f"source_data == '{source_file_name}'"
    existing = table.scan(row_filter=row_filter).to_arrow()
    return existing.num_rows > 0


def _resolve_effective_source_file_name(
    object_key: str, source_file_name: str | None
) -> str:
    """Resolve the value to store in source_data for dedup/versioning.

    Defaults to the full snapshot object key (including its ingested_at=
    component), not just the trailing file name segment. Hokuriku republishes
    the same target_date's file multiple times per day as the snapshot
    updates, and the file name alone (juyo_05_YYYYMMDD.csv) is identical
    across those revisions, so using it as source_data would make a revision
    look like an already-ingested duplicate and silently skip it.
    """
    return source_file_name or object_key


def resolve_raw_object_from_ingestion_log(
    client: RustFSClient,
    bucket_name: str,
    target_date: date,
    require_unprocessed: bool = True,
) -> tuple[str, str]:
    """Resolve latest raw snapshot object key from ingestion log metadata."""
    object_key = resolve_latest_raw_object_by_snapshot_date(
        client=client,
        bucket_name=bucket_name,
        dataset=DATASET_NAME,
        snapshot_date=target_date.isoformat(),
        require_unprocessed=require_unprocessed,
        log_key=INGESTION_LOG_KEY,
    )
    return object_key, object_key


def mark_ingestion_log_processed(
    client: RustFSClient,
    bucket_name: str,
    object_key: str,
    processed_at=None,
) -> bool:
    """Mark one raw snapshot as processed in the ingestion log."""
    updated = mark_raw_object_processed(
        client=client,
        bucket_name=bucket_name,
        object_key=object_key,
        processed_at=processed_at,
        log_key=INGESTION_LOG_KEY,
    )
    if updated:
        logger.info("Updated ingestion log status to processed for %s", object_key)
    else:
        logger.warning(
            "Skipping ingestion-log update because target row was not found: %s",
            object_key,
        )
    return updated


def _build_row_dataframe(
    row: dict[str, str], source_file_name: str, execution_id: str | None = None
) -> pl.DataFrame:
    df = pl.DataFrame({name: [value] for name, value in row.items()})
    df = df.with_columns(
        pl.lit(source_file_name).alias("source_data"),
        pl.lit("new").alias("status"),
    )
    return add_metadata(df, execution_id=execution_id)


def run_source_to_bronze_power_usage_hokuriku(
    client: RustFSClient,
    bucket_name: str,
    object_key: str | None,
    source_file_name: str | None,
    catalog_name: str = "dlh_dev",
    skip_if_exists: bool = True,
    target_date: date | None = None,
    use_ingestion_log: bool = False,
    require_unprocessed: bool = True,
    update_ingestion_log_status: bool = True,
    execution_id: str | None = None,
) -> dict[str, int]:
    """Ingest one Hokuriku power_usage raw snapshot into its 3 Bronze tables.

    Writes daily_summary, hourly, and interval5 as 3 separate, non-atomic
    Iceberg appends (one raw file -> 3 tables); a failure partway through can
    leave the tables out of sync for that source_data. This is an accepted
    tradeoff of the 3-table split (see docs/architecture/data_model.md 3.1).
    """
    # Resolved once for all three tables. Letting add_metadata() default it
    # per call stamped the same ingestion with three different ids, which made
    # the three halves of one run impossible to line up afterwards.
    execution_id = execution_id or gen_uuid()

    if use_ingestion_log and object_key is None:
        if target_date is None:
            raise ValueError(
                "target_date is required when use_ingestion_log is enabled"
            )
        object_key, source_file_name = resolve_raw_object_from_ingestion_log(
            client=client,
            bucket_name=bucket_name,
            target_date=target_date,
            require_unprocessed=require_unprocessed,
        )

    if object_key is None:
        raise ValueError("object_key is required")
    if target_date is None:
        raise ValueError("target_date is required")

    source_file_name = _resolve_effective_source_file_name(object_key, source_file_name)

    logger.info("Starting raw-to-bronze ingestion: s3://%s/%s", bucket_name, object_key)

    catalog = get_catalog(catalog_name)
    daily_summary_table_id, _ = TABLES["daily_summary"]
    daily_summary_table = catalog.load_table(daily_summary_table_id)

    if skip_if_exists and source_data_exists(daily_summary_table, source_file_name):
        logger.info(
            "Skipped ingestion because source_data already exists: %s", source_file_name
        )
        if use_ingestion_log and update_ingestion_log_status:
            mark_ingestion_log_processed(
                client=client, bucket_name=bucket_name, object_key=object_key
            )
        return {name: 0 for name in TABLES}

    text = read_object_text(
        client=client, bucket_name=bucket_name, object_key=object_key, encoding="cp932"
    )
    snapshot = parse_snapshot(text, target_date)

    rows_by_table = {
        "daily_summary": snapshot.daily_summary,
        "hourly": snapshot.hourly,
        "interval5": snapshot.interval5,
    }

    row_counts: dict[str, int] = {}
    for name, (table_id, _schema_path) in TABLES.items():
        table = (
            daily_summary_table
            if name == "daily_summary"
            else catalog.load_table(table_id)
        )
        df = _build_row_dataframe(
            rows_by_table[name], source_file_name, execution_id=execution_id
        )
        target_schema = table.schema().as_arrow()
        arrow_table = df.to_arrow().cast(target_schema)
        table.append(arrow_table)
        row_counts[name] = len(df)
        logger.info(
            "Ingested %s row(s) into %s from source_data=%s",
            len(df),
            table_id,
            source_file_name,
        )

    if use_ingestion_log and update_ingestion_log_status:
        mark_ingestion_log_processed(
            client=client, bucket_name=bucket_name, object_key=object_key
        )

    return row_counts

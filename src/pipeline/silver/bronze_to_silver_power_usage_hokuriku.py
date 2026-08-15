"""Transform Hokuriku power_usage from the 3 bronze tables into 3 silver tables.

Modeled on bronze_to_silver_occto_unit_generation_actuals.py: DuckDB reads
each bronze Iceberg table directly, casts/deduplicates the rows (one row per
target_date, latest file_updated_at wins), and PyIceberg replaces the
affected target_date window in each silver table.

daily_summary needs no unpivot (bronze is already 1 row per target_date).
hourly and interval5 unpivot their wide per-slot columns into one row per
slot, the same idea as OCCTO's 48-timeslot unpivot — except each slot here
holds multiple metrics (not one), so each metric family is unpivoted
separately and the results are joined back together on (target_date, slot).
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date

import duckdb
import polars as pl
from pyiceberg.expressions import (
    And,
    BooleanExpression,
    GreaterThanOrEqual,
    LessThanOrEqual,
    LiteralPredicate,
)

from common.duckdb_utils import create_duckdb_connection
from common.iceberg.catalog import get_catalog
from common.silver_write import SilverWriteResult, column_bound, write_silver_table
from common.utilities import gen_uuid

logger = logging.getLogger(__name__)

TARGET_DATE_COLUMN = "target_date"
DELIVERY_TIMEZONE = "Asia/Tokyo"

DEFAULT_CATALOG_NAME = "dlh_dev"
DEFAULT_SILVER_SCHEMA_DIR = (
    "/workspace/configuration/iceberg/schema/silver/power_usage_hokuriku"
)
BRONZE_LOCATIONS = {
    "daily_summary": "s3://jp-power-grid-dev/bronze/power_usage_hokuriku_daily_summary",
    "hourly": "s3://jp-power-grid-dev/bronze/power_usage_hokuriku_hourly",
    "interval5": "s3://jp-power-grid-dev/bronze/power_usage_hokuriku_interval5",
}
SILVER_TABLES = {
    "daily_summary": "silver.power_usage_hokuriku_daily_summary",
    "hourly": "silver.power_usage_hokuriku_hourly",
    "interval5": "silver.power_usage_hokuriku_interval5",
}

HOURLY_METRICS = (
    "actual_demand",
    "forecasted_demand",
    "usage_rate_pct",
    "supply_capacity",
)
HOURLY_ROW_COUNT = 24
HOURLY_KEY_COLUMNS = ("target_date", "hour_of_day")

INTERVAL5_METRICS = ("actual_demand", "solar_generation_actual")
INTERVAL5_ROW_COUNT = 288
INTERVAL5_STEP_MINUTES = 5
INTERVAL5_KEY_COLUMNS = ("target_date", "slot_index")

DAILY_SUMMARY_KEY_COLUMNS = ("target_date",)

# Bronze daily_summary field-name suffix -> silver cast rule (mirrors
# configuration/iceberg/schema/silver/power_usage_hokuriku/
# power_usage_hokuriku_daily_summary.csv generation logic; see the schema CSV
# as the single source of truth for the actual column list and types).
DAILY_SUMMARY_PASSTHROUGH_STRING_SUFFIXES = (
    "_time_range",
    "_updated_date",
    "_updated_time",
)


def _hourly_column(hour: int, metric: str) -> str:
    return f"hourly_{hour:02d}_00_{metric}"


def _interval5_column(slot_index: int, metric: str) -> str:
    total_minutes = slot_index * INTERVAL5_STEP_MINUTES
    hour, minute = divmod(total_minutes, 60)
    return f"interval5_{hour:02d}_{minute:02d}_{metric}"


def _dedup_latest_revision_cte(source_relation: str, non_key_select: str) -> str:
    """Cast target_date/file_updated_at and keep the latest revision per date.

    Bronze may hold multiple revisions of the same target_date (re-ingested
    snapshots); the newest file_updated_at wins, with ingestion_time as a
    tiebreak for identical content re-ingested at different times.
    """
    return f"""
    typed AS (
        SELECT
            try_strptime(target_date, '%Y-%m-%d') AS target_date_d,
            (try_strptime(
                regexp_replace(file_updated_at, ' UPDATE$', ''), '%Y/%m/%d %H:%M'
            ) AT TIME ZONE '{DELIVERY_TIMEZONE}') AS file_updated_at_ts,
            ingestion_time,
            {non_key_select}
        FROM {source_relation}
    ),
    deduplicated AS (
        SELECT * FROM typed
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY target_date_d
            ORDER BY file_updated_at_ts DESC NULLS LAST, ingestion_time DESC NULLS LAST
        ) = 1
    )
    """


def _slot_case_expression(
    column_var: str, row_count: int, column_for: Callable[[int], str]
) -> str:
    cases = "\n            ".join(
        f"WHEN '{column_for(i)}' THEN {i}" for i in range(row_count)
    )
    return f"CASE {column_var}\n            {cases}\n        END"


def _build_target_date_filter(from_date: date | None, to_date: date | None) -> str:
    conditions = []
    if from_date is not None:
        conditions.append(f"target_date_d >= DATE '{from_date.isoformat()}'")
    if to_date is not None:
        conditions.append(f"target_date_d <= DATE '{to_date.isoformat()}'")
    if not conditions:
        return ""
    return "WHERE " + " AND ".join(conditions)


def extract_daily_summary_frame(
    conn: duckdb.DuckDBPyConnection,
    *,
    source_relation: str,
    silver_columns: list[str],
    from_date: date | None = None,
    to_date: date | None = None,
) -> pl.DataFrame:
    """Cast bronze daily_summary rows 1:1 into silver (no unpivot needed)."""
    non_key_columns = [
        c for c in silver_columns if c not in ("target_date", "file_updated_at")
    ]

    def _cast_expr(name: str) -> str:
        if name.endswith("_capacity") or name.endswith("_demand"):
            return f"TRY_CAST({name} AS BIGINT) AS {name}"
        if name.endswith("_pct"):
            return f"TRY_CAST({name} AS DOUBLE) AS {name}"
        if name.endswith(DAILY_SUMMARY_PASSTHROUGH_STRING_SUFFIXES):
            return name
        raise ValueError(f"No cast rule for daily_summary column: {name}")

    non_key_select = ",\n            ".join(_cast_expr(c) for c in non_key_columns)
    dedup_cte = _dedup_latest_revision_cte(source_relation, non_key_select)
    filter_clause = _build_target_date_filter(from_date, to_date)

    return conn.execute(f"""
        WITH {dedup_cte}
        SELECT
            CAST(target_date_d AS DATE) AS target_date,
            file_updated_at_ts AS file_updated_at,
            {",\n            ".join(non_key_columns)}
        FROM deduplicated
        {filter_clause}
    """).pl()


def extract_hourly_frame(
    conn: duckdb.DuckDBPyConnection,
    *,
    source_relation: str,
    from_date: date | None = None,
    to_date: date | None = None,
) -> pl.DataFrame:
    """Unpivot the 96 hourly bronze columns (24 hours x 4 metrics) into 24 rows."""
    all_columns = [
        _hourly_column(h, m) for m in HOURLY_METRICS for h in range(HOURLY_ROW_COUNT)
    ]
    non_key_select = ",\n            ".join(
        f"TRY_CAST({c} AS BIGINT) AS {c}"
        if not c.endswith("_usage_rate_pct")
        else f"TRY_CAST({c} AS DOUBLE) AS {c}"
        for c in all_columns
    )
    dedup_cte = _dedup_latest_revision_cte(source_relation, non_key_select)
    filter_clause = _build_target_date_filter(from_date, to_date)

    metric_ctes = []
    for metric in HOURLY_METRICS:
        columns = ", ".join(_hourly_column(h, metric) for h in range(HOURLY_ROW_COUNT))
        case_expr = _slot_case_expression(
            "hour_col", HOURLY_ROW_COUNT, lambda h, m=metric: _hourly_column(h, m)
        )
        metric_ctes.append(f"""
        {metric}_long AS (
            SELECT target_date_d, {case_expr} AS hour_of_day, {metric}
            FROM (
                SELECT target_date_d, {columns} FROM deduplicated {filter_clause}
            )
            UNPIVOT INCLUDE NULLS ({metric} FOR hour_col IN ({columns}))
        )""")

    joins = "\n        ".join(
        f"JOIN {metric}_long USING (target_date_d, hour_of_day)"
        for metric in HOURLY_METRICS[1:]
    )

    return conn.execute(f"""
        WITH {dedup_cte},
        {",".join(metric_ctes)}
        SELECT
            CAST({HOURLY_METRICS[0]}_long.target_date_d AS DATE) AS target_date,
            {HOURLY_METRICS[0]}_long.hour_of_day,
            (target_date_d + (hour_of_day * INTERVAL 1 HOUR))
                AT TIME ZONE '{DELIVERY_TIMEZONE}' AS delivery_datetime,
            {", ".join(HOURLY_METRICS)}
        FROM {HOURLY_METRICS[0]}_long
        {joins}
    """).pl()


def extract_interval5_frame(
    conn: duckdb.DuckDBPyConnection,
    *,
    source_relation: str,
    from_date: date | None = None,
    to_date: date | None = None,
) -> pl.DataFrame:
    """Unpivot the 576 interval5 bronze columns (288 slots, 2 metrics) into 288 rows."""
    all_columns = [
        _interval5_column(s, m)
        for m in INTERVAL5_METRICS
        for s in range(INTERVAL5_ROW_COUNT)
    ]
    non_key_select = ",\n            ".join(
        f"TRY_CAST({c} AS BIGINT) AS {c}" for c in all_columns
    )
    dedup_cte = _dedup_latest_revision_cte(source_relation, non_key_select)
    filter_clause = _build_target_date_filter(from_date, to_date)

    metric_ctes = []
    for metric in INTERVAL5_METRICS:
        columns = ", ".join(
            _interval5_column(s, metric) for s in range(INTERVAL5_ROW_COUNT)
        )
        case_expr = _slot_case_expression(
            "slot_col", INTERVAL5_ROW_COUNT, lambda s, m=metric: _interval5_column(s, m)
        )
        metric_ctes.append(f"""
        {metric}_long AS (
            SELECT target_date_d, {case_expr} AS slot_index, {metric}
            FROM (
                SELECT target_date_d, {columns} FROM deduplicated {filter_clause}
            )
            UNPIVOT INCLUDE NULLS ({metric} FOR slot_col IN ({columns}))
        )""")

    joins = "\n        ".join(
        f"JOIN {metric}_long USING (target_date_d, slot_index)"
        for metric in INTERVAL5_METRICS[1:]
    )

    return conn.execute(f"""
        WITH {dedup_cte},
        {",".join(metric_ctes)}
        SELECT
            CAST({INTERVAL5_METRICS[0]}_long.target_date_d AS DATE) AS target_date,
            {INTERVAL5_METRICS[0]}_long.slot_index,
            (target_date_d + (slot_index * INTERVAL {INTERVAL5_STEP_MINUTES} MINUTE))
                AT TIME ZONE '{DELIVERY_TIMEZONE}' AS delivery_datetime,
            {", ".join(INTERVAL5_METRICS)}
        FROM {INTERVAL5_METRICS[0]}_long
        {joins}
    """).pl()


def target_date_bound(
    predicate: type[LiteralPredicate], boundary: date
) -> BooleanExpression:
    return column_bound(TARGET_DATE_COLUMN, predicate, boundary)


def resolve_staged_target_date_range(frame: pl.DataFrame) -> tuple[date, date] | None:
    if frame.is_empty():
        return None
    column = frame[TARGET_DATE_COLUMN]
    return column.min(), column.max()  # pyright: ignore[reportReturnType]


def build_target_date_window(
    *,
    from_date: date | None,
    to_date: date | None,
    staged_range: tuple[date, date] | None,
) -> BooleanExpression | None:
    if from_date is not None:
        upper = to_date or from_date
        return And(
            left=target_date_bound(GreaterThanOrEqual, from_date),
            right=target_date_bound(LessThanOrEqual, upper),
        )
    if staged_range is None:
        return None
    earliest, latest = staged_range
    return And(
        left=target_date_bound(GreaterThanOrEqual, earliest),
        right=target_date_bound(LessThanOrEqual, latest),
    )


@dataclass(frozen=True)
class PowerUsageHokurikuBronzeToSilverResult:
    execution_id: str
    writes: dict[str, SilverWriteResult]


def run_bronze_to_silver_power_usage_hokuriku(
    *,
    catalog_name: str = DEFAULT_CATALOG_NAME,
    schema_dir: str = DEFAULT_SILVER_SCHEMA_DIR,
    from_date: date | None = None,
    to_date: date | None = None,
    execution_id: str | None = None,
) -> PowerUsageHokurikuBronzeToSilverResult:
    """Run the full bronze-to-silver transform for all 3 power_usage_hokuriku tables."""
    run_execution_id = execution_id or gen_uuid()
    catalog = get_catalog(catalog_name)

    daily_summary_silver_columns = [
        row["name"]
        for row in pl.read_csv(
            f"{schema_dir}/power_usage_hokuriku_daily_summary.csv"
        ).iter_rows(named=True)
    ]

    writes: dict[str, SilverWriteResult] = {}
    conn = create_duckdb_connection()
    try:
        for name, extract in (
            (
                "daily_summary",
                lambda c, r: extract_daily_summary_frame(
                    c,
                    source_relation=r,
                    silver_columns=daily_summary_silver_columns,
                    from_date=from_date,
                    to_date=to_date,
                ),
            ),
            (
                "hourly",
                lambda c, r: extract_hourly_frame(
                    c, source_relation=r, from_date=from_date, to_date=to_date
                ),
            ),
            (
                "interval5",
                lambda c, r: extract_interval5_frame(
                    c, source_relation=r, from_date=from_date, to_date=to_date
                ),
            ),
        ):
            source_relation = f"iceberg_scan('{BRONZE_LOCATIONS[name]}')"
            frame = extract(conn, source_relation)

            overwrite_filter = build_target_date_window(
                from_date=from_date,
                to_date=to_date,
                staged_range=resolve_staged_target_date_range(frame),
            )

            key_cols = {
                "daily_summary": DAILY_SUMMARY_KEY_COLUMNS,
                "hourly": HOURLY_KEY_COLUMNS,
                "interval5": INTERVAL5_KEY_COLUMNS,
            }[name]

            writes[name] = write_silver_table(
                catalog,
                table_identifier=SILVER_TABLES[name],
                schema_path=f"{schema_dir}/power_usage_hokuriku_{name}.csv",
                frame=frame,
                key_cols=key_cols,
                overwrite_filter=overwrite_filter,
                execution_id=run_execution_id,
            )
    finally:
        conn.close()

    return PowerUsageHokurikuBronzeToSilverResult(
        execution_id=run_execution_id, writes=writes
    )

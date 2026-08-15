"""Transform Chugoku supply_demand_actuals from bronze into silver.

Bronze is already 1 row per (target_date, target_time), so unlike
power_usage_hokuriku's hourly/interval5 tables no UNPIVOT is needed here --
just type-casting and deriving hour_of_day/delivery_datetime (same
convention as power_usage_hokuriku's hourly silver table). Multiple bronze
revisions of the same (target_date, hour_of_day) are deduped by latest
ingestion_time (bronze already guards against this in the common case,
since each day's run processes a new target_date, but a backfill or rerun
could still produce a duplicate).
"""

from __future__ import annotations

import logging
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
DEFAULT_SCHEMA_PATH = (
    "/workspace/configuration/iceberg/schema/silver/supply_demand_actuals/"
    "supply_demand_actuals_chugoku.csv"
)
BRONZE_LOCATION = "s3://jp-power-grid-dev/bronze/supply_demand_actuals_chugoku"
SILVER_TABLE = "silver.supply_demand_actuals_chugoku"
EXTRA_COLUMNS: tuple[str, ...] = ()

SILVER_KEY_COLUMNS = ("target_date", "hour_of_day")


def _build_target_date_filter(from_date: date | None, to_date: date | None) -> str:
    conditions = []
    if from_date is not None:
        conditions.append(f"target_date_d >= DATE '{from_date.isoformat()}'")
    if to_date is not None:
        conditions.append(f"target_date_d <= DATE '{to_date.isoformat()}'")
    if not conditions:
        return ""
    return "WHERE " + " AND ".join(conditions)


def extract_supply_demand_actuals_frame(
    conn: duckdb.DuckDBPyConnection,
    *,
    source_relation: str,
    extra_columns: tuple[str, ...] = EXTRA_COLUMNS,
    from_date: date | None = None,
    to_date: date | None = None,
) -> pl.DataFrame:
    """Cast bronze rows, dedupe to 1 per (target_date, hour_of_day), and
    derive hour_of_day/delivery_datetime from target_time/target_date."""
    extra_typed_select = "".join(
        f", TRY_CAST({c} AS BIGINT) AS {c}" for c in extra_columns
    )
    extra_passthrough_select = "".join(f", {c}" for c in extra_columns)
    filter_clause = _build_target_date_filter(from_date, to_date)

    return conn.execute(f"""
        WITH typed AS (
            SELECT
                try_strptime(target_date, '%Y-%m-%d') AS target_date_d,
                CAST(split_part(target_time, ':', 1) AS INTEGER) AS hour_of_day,
                TRY_CAST(actual_demand_10k_kw AS BIGINT)
                    AS actual_demand_10k_kw{extra_typed_select},
                ingestion_time
            FROM {source_relation}
        ),
        deduplicated AS (
            SELECT * FROM typed
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY target_date_d, hour_of_day
                ORDER BY ingestion_time DESC NULLS LAST
            ) = 1
        )
        SELECT
            CAST(target_date_d AS DATE) AS target_date,
            hour_of_day,
            (target_date_d + (hour_of_day * INTERVAL 1 HOUR))
                AT TIME ZONE '{DELIVERY_TIMEZONE}' AS delivery_datetime,
            actual_demand_10k_kw{extra_passthrough_select}
        FROM deduplicated
        {filter_clause}
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
class SupplyDemandActualsBronzeToSilverResult:
    execution_id: str
    write: SilverWriteResult


def run_bronze_to_silver_supply_demand_actuals_chugoku(
    *,
    catalog_name: str = DEFAULT_CATALOG_NAME,
    schema_path: str = DEFAULT_SCHEMA_PATH,
    from_date: date | None = None,
    to_date: date | None = None,
    execution_id: str | None = None,
) -> SupplyDemandActualsBronzeToSilverResult:
    """Run the bronze-to-silver transform for Chugoku's supply_demand_actuals."""
    run_execution_id = execution_id or gen_uuid()
    catalog = get_catalog(catalog_name)

    conn = create_duckdb_connection()
    try:
        source_relation = f"iceberg_scan('{BRONZE_LOCATION}')"
        frame = extract_supply_demand_actuals_frame(
            conn,
            source_relation=source_relation,
            extra_columns=EXTRA_COLUMNS,
            from_date=from_date,
            to_date=to_date,
        )

        overwrite_filter = build_target_date_window(
            from_date=from_date,
            to_date=to_date,
            staged_range=resolve_staged_target_date_range(frame),
        )

        write = write_silver_table(
            catalog,
            table_identifier=SILVER_TABLE,
            schema_path=schema_path,
            frame=frame,
            key_cols=SILVER_KEY_COLUMNS,
            overwrite_filter=overwrite_filter,
            execution_id=run_execution_id,
        )
    finally:
        conn.close()

    return SupplyDemandActualsBronzeToSilverResult(
        execution_id=run_execution_id, write=write
    )

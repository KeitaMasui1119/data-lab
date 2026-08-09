"""Transform OCCTO unit generation actuals from the bronze layer into silver.

Modeled on bronze_to_silver_jepx_spot_price.py: DuckDB reads the bronze
Iceberg table directly, casts/deduplicates/validates the rows, unpivots the
48 timeslot columns into one row per 30-minute slot, and PyIceberg replaces
the affected target_date window in the silver table.
"""

from __future__ import annotations

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

from common.silver_write import column_bound

STAGING_RELATION = "occto_silver_staging"

TARGET_DATE_COLUMN = "target_date"

# A time code identifies a 30-minute slot and denotes its START time in JST.
# Time code 1 covers 00:00-00:30 and is stored as 00:00 JST (15:00 UTC the
# previous day). Same convention and constants as JEPX's silver transform.
SLOT_MINUTES = 30
SLOT_OFFSET = 1
DELIVERY_TIMEZONE = "Asia/Tokyo"

# The natural key for a unit-generation record. unit_name may legitimately
# be an empty string (Phase 0 confirmed real plants publish it that way), so
# it is COALESCE'd rather than dropped or treated as missing.
NATURAL_KEY_COLUMNS = ("power_plant_code", "unit_name", "target_date_d")


def _generate_timeslot_columns() -> tuple[str, ...]:
    """Build the 48 timeslot column names in bronze schema CSV order.

    OCCTO labels each slot by its END time (e.g. "timeslot_00_30" covers
    00:00-00:30), so column N (1-indexed) always covers minutes
    ((N-1)*30, N*30]. Generating from that offset instead of hand-typing 48
    strings keeps the labels and their position in lockstep by construction.
    """
    columns = []
    for slot_number in range(1, 49):
        end_minutes = slot_number * 30
        hour, minute = divmod(end_minutes, 60)
        columns.append(f"timeslot_{hour:02d}_{minute:02d}")
    return tuple(columns)


# OCCTO labels each slot by its END time, JEPX's time_code by its START time.
# The column order therefore IS the time_code: timeslot_00_30 covers
# 00:00-00:30 and maps to time_code 1, the same slot JEPX calls 1. This must
# match configuration/iceberg/schema/bronze/occto_unit_generation_actuals.csv
# column order exactly; see the regression test tying the two together.
TIMESLOT_COLUMNS: tuple[str, ...] = _generate_timeslot_columns()
assert len(TIMESLOT_COLUMNS) == 48


def _build_typed_projection() -> str:
    """Build the cast expressions that turn bronze strings into typed values."""
    expressions = [
        "power_plant_code",
        "COALESCE(unit_name, '') AS unit_name",
        "COALESCE("
        "try_strptime(target_date, '%Y-%m-%d'), "
        "try_strptime(target_date, '%Y/%m/%d')"
        ") AS target_date_d",
        "try_strptime(updated_datetime, '%Y/%m/%d %H:%M:%S') AS updated_datetime_ts",
        "area",
        "power_plant_name",
        "power_generation_method_and_fuel_type",
    ]
    for column in (*TIMESLOT_COLUMNS, "daily_amount"):
        expressions.append(
            f"TRY_CAST(REPLACE({column}, ',', '') AS BIGINT) AS {column}"
        )
    expressions.extend(["source_data", "ingestion_time", "execution_id"])
    return ",\n        ".join(expressions)


def _build_violation_expression() -> str:
    """Build the list expression that records why a row is not usable."""
    timeslot_list = ", ".join(TIMESLOT_COLUMNS)
    checks = [
        "CASE WHEN target_date_d IS NULL THEN 'target_date_null' END",
        "CASE WHEN power_plant_code IS NULL THEN 'power_plant_code_null' END",
        f"CASE WHEN list_min([{timeslot_list}]) < 0 THEN 'generation_negative' END",
        # list_min ignores NULLs and only returns NULL itself when every
        # element is NULL, so this is true exactly when all 48 slots are.
        f"CASE WHEN list_min([{timeslot_list}]) IS NULL THEN 'all_timeslots_null' END",
    ]
    joined = ",\n            ".join(checks)
    return f"list_filter([\n            {joined}\n        ], x -> x IS NOT NULL)"


def build_staging_relation(
    conn: duckdb.DuckDBPyConnection,
    *,
    source_relation: str,
) -> None:
    """Cast, deduplicate and validate bronze rows into a staging relation.

    ``source_relation`` is a relation name so that tests can pass a locally
    registered frame in place of ``iceberg_scan``. The timeslot unpivot is
    added in a later step; this stage stops at one row per business key
    with a ``violations`` column recording why it should not be written.
    """
    passthrough = ",\n    ".join((*TIMESLOT_COLUMNS, "daily_amount"))
    key_columns = ", ".join(NATURAL_KEY_COLUMNS)
    conn.execute(f"""
CREATE OR REPLACE TEMP TABLE {STAGING_RELATION} AS
WITH bronze_raw AS (
    SELECT * FROM {source_relation}
),
typed AS (
    SELECT
        {_build_typed_projection()}
    FROM bronze_raw
),
deduplicated AS (
    SELECT *
    FROM typed
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY {key_columns}
        ORDER BY updated_datetime_ts DESC NULLS LAST,
                 ingestion_time      DESC NULLS LAST,
                 execution_id        DESC NULLS LAST
    ) = 1
),
validated AS (
    SELECT
        *,
        {_build_violation_expression()} AS violations
    FROM deduplicated
)
SELECT
    power_plant_code,
    unit_name,
    target_date_d,
    updated_datetime_ts,
    area,
    power_plant_name,
    power_generation_method_and_fuel_type,
    {passthrough},
    source_data,
    ingestion_time,
    execution_id,
    violations
FROM validated
""")


def count_dropped_rows(conn: duckdb.DuckDBPyConnection) -> int:
    """Count staged rows excluded from silver because they failed validation."""
    row = conn.execute(
        f"SELECT count(*) FROM {STAGING_RELATION} WHERE len(violations) > 0"
    ).fetchone()
    return int(row[0]) if row else 0


def summarize_violations(conn: duckdb.DuckDBPyConnection) -> dict[str, int]:
    """Return how many rows hit each violation reason."""
    rows = conn.execute(f"""
        SELECT reason, count(*) AS row_count
        FROM (SELECT unnest(violations) AS reason FROM {STAGING_RELATION})
        GROUP BY reason
        ORDER BY row_count DESC
    """).fetchall()
    return {reason: int(count) for reason, count in rows}


def _build_time_code_case_expression() -> str:
    """Build the CASE expression mapping an unpivoted column name to time_code.

    time_code is defined by TIMESLOT_COLUMNS' position (1-indexed), not by
    parsing the column name, so this only ever has to agree with that one
    tuple.
    """
    cases = "\n            ".join(
        f"WHEN '{column}' THEN {index}"
        for index, column in enumerate(TIMESLOT_COLUMNS, start=1)
    )
    return f"CASE timeslot_column\n            {cases}\n        END"


def extract_unit_generation_frame(conn: duckdb.DuckDBPyConnection) -> pl.DataFrame:
    """Unpivot the 48 timeslot columns into one row per 30-minute slot.

    The inner SELECT narrows the columns before UNPIVOT because DuckDB
    carries every unlisted column through, which would otherwise pull
    daily_amount into this frame alongside the timeslot columns.
    """
    timeslot_columns = ", ".join(TIMESLOT_COLUMNS)
    return conn.execute(f"""
        WITH source AS (
            SELECT
                power_plant_code,
                unit_name,
                target_date_d,
                updated_datetime_ts,
                area,
                power_plant_name,
                power_generation_method_and_fuel_type,
                source_data,
                {timeslot_columns}
            FROM {STAGING_RELATION}
            WHERE len(violations) = 0
        ),
        unpivoted AS (
            SELECT * FROM source
            UNPIVOT (generation_kwh FOR timeslot_column IN ({timeslot_columns}))
        ),
        with_time_code AS (
            SELECT
                *,
                {_build_time_code_case_expression()} AS time_code
            FROM unpivoted
        )
        SELECT
            power_plant_code,
            unit_name,
            CAST(target_date_d AS DATE) AS target_date,
            time_code,
            (target_date_d
                + ((time_code - {SLOT_OFFSET}) * INTERVAL {SLOT_MINUTES} MINUTE))
                AT TIME ZONE '{DELIVERY_TIMEZONE}' AS delivery_datetime,
            generation_kwh,
            area,
            power_plant_name,
            power_generation_method_and_fuel_type,
            updated_datetime_ts AS updated_datetime,
            source_data
        FROM with_time_code
    """).pl()


def summarize_daily_amount_mismatches(
    conn: duckdb.DuckDBPyConnection,
) -> dict[str, int]:
    """Compare each staged row's 48-slot sum against its reported daily_amount.

    This is a quality signal only (docs/tasks/plan_occto_pipeline.md 4-5):
    OCCTO's published daily total can diverge slightly from the sum of its
    own slots, and dropping rows over that would lose real generation data
    for a cosmetic reconciliation issue. Callers log the result; no row is
    ever excluded because of it.
    """
    timeslot_list = ", ".join(TIMESLOT_COLUMNS)
    row = conn.execute(f"""
        SELECT
            count(*) FILTER (
                WHERE list_sum([{timeslot_list}]) IS DISTINCT FROM daily_amount
            ) AS mismatch_count,
            max(abs(list_sum([{timeslot_list}]) - daily_amount)) AS max_deviation
        FROM {STAGING_RELATION}
    """).fetchone()
    if row is None:
        return {"mismatch_count": 0, "max_deviation": 0}
    mismatch_count, max_deviation = row
    return {
        "mismatch_count": int(mismatch_count or 0),
        "max_deviation": int(max_deviation or 0),
    }


def target_date_bound(
    predicate: type[LiteralPredicate],
    boundary: date,
) -> BooleanExpression:
    """Build one target_date comparison against ``boundary``."""
    return column_bound(TARGET_DATE_COLUMN, predicate, boundary)


def resolve_staged_target_date_range(
    frame: pl.DataFrame,
) -> tuple[date, date] | None:
    """Return the target_date bounds of the extracted (already-valid) frame.

    Unlike JEPX, which queries the DuckDB staging relation directly, OCCTO
    only ever produces one output frame, so the range is read straight off
    it after extract_unit_generation_frame() has already dropped invalid
    rows.
    """
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
    """Build the predicate identifying the silver rows this run replaces.

    An explicit --from-date (with --to-date defaulting to the same day)
    always wins, so a backfill can clear a range even where it produced no
    valid staged rows. Without one, the window falls back to exactly what
    was staged, keeping a run from touching anything it did not rebuild.
    """
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

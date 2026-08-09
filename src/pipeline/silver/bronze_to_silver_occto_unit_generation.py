"""Transform OCCTO unit generation actuals from the bronze layer into silver.

Modeled on bronze_to_silver_jepx_spot_price.py: DuckDB reads the bronze
Iceberg table directly, casts/deduplicates/validates the rows, unpivots the
48 timeslot columns into one row per 30-minute slot, and PyIceberg replaces
the affected target_date window in the silver table.
"""

from __future__ import annotations

import duckdb

STAGING_RELATION = "occto_silver_staging"

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


def build_staging_relation(
    conn: duckdb.DuckDBPyConnection,
    *,
    source_relation: str,
) -> None:
    """Cast and deduplicate bronze rows into a staging relation.

    ``source_relation`` is a relation name so that tests can pass a locally
    registered frame in place of ``iceberg_scan``. Validation and the
    timeslot unpivot are added in later steps; this stage only establishes
    the typed, deduplicated shape they build on.
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
    execution_id
FROM deduplicated
""")

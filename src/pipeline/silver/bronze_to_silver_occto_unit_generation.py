"""Transform OCCTO unit generation actuals from the bronze layer into silver.

Modeled on bronze_to_silver_jepx_spot_price.py: DuckDB reads the bronze
Iceberg table directly, casts/deduplicates/validates the rows, unpivots the
48 timeslot columns into one row per 30-minute slot, and PyIceberg replaces
the affected target_date window in the silver table.
"""

from __future__ import annotations


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

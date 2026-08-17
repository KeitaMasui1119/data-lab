"""Aggregate JEPX silver spot prices into the gold daily table.

DuckDB joins the area and base silver tables, rolls the 48 time codes of each
delivery date up to one row per area, and PyIceberg replaces the affected
delivery window. Same shape as the bronze-to-silver transform: daily and
full-refresh runs share this code path and only the fiscal year filter differs.

Grain and what is deliberately absent
-------------------------------------
The grain is one row per (delivery_date, area_name), nine rows per day.
``system_price`` is denormalized onto every area row because it is an
*intensive* quantity: averaging it across areas still returns the system
price. The volume columns are *extensive* and are left out entirely -- they
are national figures, so repeating them across nine rows would make any
SUM over areas nine times too large. They belong in a national-grain table
of their own, which this pipeline does not have yet.

See docs/tasks/plan_jepx_gold.md for the surrounding plan.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date

import duckdb
import polars as pl

from common.duckdb_utils import create_duckdb_connection
from common.iceberg.catalog import get_catalog

# The window-replace write path is layer-agnostic despite its name: it
# provisions from a schema CSV, guards the business key and replaces a
# window. Gold needs exactly that. Worth renaming once a second gold table
# lands rather than churning the silver modules for the first one.
from common.silver_write import SilverWriteResult, write_silver_table
from common.utilities import gen_uuid
from pipeline.silver.bronze_to_silver_jepx_spot_price import (
    FISCAL_YEAR_START_MONTH,
    build_delivery_window,
)

logger = logging.getLogger(__name__)

DEFAULT_CATALOG_NAME = "dlh_dev"
DEFAULT_AREA_LOCATION = "s3://jp-power-grid-dev/silver/jepx_spot_price_area"
DEFAULT_BASE_LOCATION = "s3://jp-power-grid-dev/silver/jepx_spot_price_base"
DEFAULT_GOLD_SCHEMA_DIR = "/workspace/configuration/iceberg/schema/gold/jepx_spot_price"

DAILY_TABLE_IDENTIFIER = "gold.jepx_spot_price_daily"
DAILY_KEY_COLUMNS = ("delivery_date", "area_name")

STAGING_RELATION = "jepx_gold_daily_staging"

# Lineage for the audit columns: gold rows come from two silver tables rather
# than from a file, so source_data names them instead of an object key.
SOURCE_DATA = "silver.jepx_spot_price_area+silver.jepx_spot_price_base"

# A day is complete when all 48 half-hour slots are present. Japan has no DST,
# so 48 is always the expected count and anything less is a gap.
EXPECTED_TIME_CODES_PER_DAY = 48

# Prices carry two decimal places, so one tick is the smallest real difference.
# Market splitting is "the area cleared at a different price from the system",
# which is any non-zero gap -- comparing against a tick avoids depending on
# exact decimal equality.
SPLIT_THRESHOLD = "0.01"

# A fixed spike threshold rather than a sigma rule: 2021's standard deviation
# was 23.1 against 3-5 in a normal year, so a sigma rule raises the bar in
# exactly the years worth detecting. See docs/tasks/plan_jepx_gold.md section 7.
SPIKE_THRESHOLD = "50.0"

# JEPX's price floor. It appears from 2020 onward as solar output grows.
FLOOR_THRESHOLD = "0.01"

PRICE_TYPE = "DECIMAL(32, 3)"


@dataclass(frozen=True)
class SilverToGoldResult:
    """Outcome of one silver-to-gold run."""

    execution_id: str
    write: SilverWriteResult
    staged_row_count: int
    delivery_date_count: int

    @property
    def expected_row_count(self) -> int:
        """Rows the gold table should receive for this run.

        The aggregation emits exactly one row per staged group, so the staged
        count is the expectation rather than a multiple of it.
        """
        return self.staged_row_count


def _build_fiscal_year_filter(fiscal_year: int | None) -> str:
    """Build the WHERE clause that narrows a run to one fiscal year."""
    if fiscal_year is None:
        return ""
    year = int(fiscal_year)
    return (
        f"WHERE a.delivery_date >= DATE '{year}-{FISCAL_YEAR_START_MONTH:02d}-01' "
        f"AND a.delivery_date < DATE '{year + 1}-{FISCAL_YEAR_START_MONTH:02d}-01'"
    )


def build_daily_relation(
    conn: duckdb.DuckDBPyConnection,
    *,
    area_relation: str,
    base_relation: str,
    fiscal_year: int | None = None,
) -> None:
    """Roll the silver time codes up to one row per delivery date and area.

    The relations are names so that tests can register local frames in place
    of ``iceberg_scan``. The join is inner: a delivery key missing from either
    silver table cannot be aggregated, and ``time_code_count`` surfaces the
    resulting gap rather than hiding it.
    """
    conn.execute(f"""
CREATE OR REPLACE TEMP TABLE {STAGING_RELATION} AS
WITH joined AS (
    SELECT
        a.delivery_date,
        a.area_name,
        a.area_price,
        b.system_price,
        a.area_price - b.system_price AS spread
    FROM {area_relation} a
    JOIN {base_relation} b
      ON a.delivery_date = b.delivery_date AND a.time_code = b.time_code
    {_build_fiscal_year_filter(fiscal_year)}
)
SELECT
    delivery_date,
    area_name,
    CAST(avg(area_price) AS {PRICE_TYPE}) AS avg_price,
    CAST(min(area_price) AS {PRICE_TYPE}) AS min_price,
    CAST(max(area_price) AS {PRICE_TYPE}) AS max_price,
    CAST(median(area_price) AS {PRICE_TYPE}) AS median_price,
    CAST(quantile_cont(area_price, 0.05) AS {PRICE_TYPE}) AS p05_price,
    CAST(quantile_cont(area_price, 0.95) AS {PRICE_TYPE}) AS p95_price,
    -- Population, not sample: the day's time codes are the whole population,
    -- and stddev_samp would return NULL for a day holding a single slot.
    CAST(stddev_pop(area_price) AS {PRICE_TYPE}) AS stddev_price,
    CAST(max(area_price) - min(area_price) AS {PRICE_TYPE}) AS intraday_range,
    CAST(avg(system_price) AS {PRICE_TYPE}) AS avg_system_price,
    CAST(avg(spread) AS {PRICE_TYPE}) AS avg_spread,
    CAST(max(abs(spread)) AS {PRICE_TYPE}) AS max_abs_spread,
    CAST(count(*) FILTER (
        WHERE abs(spread) >= {SPLIT_THRESHOLD}
    ) AS INTEGER) AS split_time_code_count,
    CAST(count(*) FILTER (
        WHERE area_price >= {SPIKE_THRESHOLD}
    ) AS INTEGER) AS spike_time_code_count,
    CAST(count(*) FILTER (
        WHERE area_price <= {FLOOR_THRESHOLD}
    ) AS INTEGER) AS floor_time_code_count,
    CAST(count(*) AS INTEGER) AS time_code_count
FROM joined
GROUP BY delivery_date, area_name
""")


def count_staged_rows(conn: duckdb.DuckDBPyConnection) -> int:
    """Count the aggregated rows this run will write."""
    row = conn.execute(f"SELECT count(*) FROM {STAGING_RELATION}").fetchone()
    return int(row[0]) if row else 0


def count_incomplete_days(conn: duckdb.DuckDBPyConnection) -> int:
    """Count staged rows covering fewer than a full day of time codes."""
    row = conn.execute(
        f"SELECT count(*) FROM {STAGING_RELATION} "
        f"WHERE time_code_count <> {EXPECTED_TIME_CODES_PER_DAY}"
    ).fetchone()
    return int(row[0]) if row else 0


def summarize_incomplete_days(
    conn: duckdb.DuckDBPyConnection, *, limit: int = 10
) -> list[tuple[date, str, int]]:
    """Return the first few incomplete (date, area) pairs for the run log."""
    rows = conn.execute(f"""
        SELECT delivery_date, area_name, time_code_count
        FROM {STAGING_RELATION}
        WHERE time_code_count <> {EXPECTED_TIME_CODES_PER_DAY}
        ORDER BY delivery_date, area_name
        LIMIT {int(limit)}
    """).fetchall()
    return [(row[0], row[1], int(row[2])) for row in rows]


def extract_daily_frame(conn: duckdb.DuckDBPyConnection) -> pl.DataFrame:
    """Select the staged daily rows, stamped with their lineage."""
    return conn.execute(f"""
        SELECT *, '{SOURCE_DATA}' AS source_data
        FROM {STAGING_RELATION}
    """).pl()


def resolve_staged_delivery_range(
    conn: duckdb.DuckDBPyConnection,
) -> tuple[date, date] | None:
    """Return the delivery date bounds of the staged rows."""
    row = conn.execute(
        f"SELECT min(delivery_date), max(delivery_date) FROM {STAGING_RELATION}"
    ).fetchone()
    if row is None or row[0] is None:
        return None
    return row[0], row[1]


def count_delivery_dates(conn: duckdb.DuckDBPyConnection) -> int:
    """Count the distinct delivery dates covered by this run."""
    row = conn.execute(
        f"SELECT count(DISTINCT delivery_date) FROM {STAGING_RELATION}"
    ).fetchone()
    return int(row[0]) if row else 0


def run_silver_to_gold_jepx_spot_price(
    *,
    catalog_name: str = DEFAULT_CATALOG_NAME,
    area_location: str = DEFAULT_AREA_LOCATION,
    base_location: str = DEFAULT_BASE_LOCATION,
    schema_dir: str = DEFAULT_GOLD_SCHEMA_DIR,
    fiscal_year: int | None = None,
    execution_id: str | None = None,
) -> SilverToGoldResult:
    """Build the JEPX gold daily table from the silver layer."""
    run_execution_id = execution_id or gen_uuid()
    catalog = get_catalog(catalog_name)

    conn = create_duckdb_connection()
    try:
        build_daily_relation(
            conn,
            area_relation=f"iceberg_scan('{area_location}')",
            base_relation=f"iceberg_scan('{base_location}')",
            fiscal_year=fiscal_year,
        )

        staged_row_count = count_staged_rows(conn)
        delivery_date_count = count_delivery_dates(conn)

        incomplete_count = count_incomplete_days(conn)
        if incomplete_count:
            logger.warning(
                (
                    "%s staged rows do not hold %s time codes; the rows are "
                    "written and the count is kept so the gap stays visible: %s"
                ),
                incomplete_count,
                EXPECTED_TIME_CODES_PER_DAY,
                summarize_incomplete_days(conn),
            )

        write = write_silver_table(
            catalog,
            table_identifier=DAILY_TABLE_IDENTIFIER,
            schema_path=f"{schema_dir}/jepx_spot_price_daily.csv",
            frame=extract_daily_frame(conn),
            key_cols=DAILY_KEY_COLUMNS,
            overwrite_filter=build_delivery_window(
                fiscal_year=fiscal_year,
                staged_range=resolve_staged_delivery_range(conn),
            ),
            execution_id=run_execution_id,
        )
    finally:
        conn.close()

    return SilverToGoldResult(
        execution_id=run_execution_id,
        write=write,
        staged_row_count=staged_row_count,
        delivery_date_count=delivery_date_count,
    )

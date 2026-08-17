"""Aggregate JEPX silver spot prices into the gold tables.

DuckDB joins the area and base silver tables and rolls them up two ways;
PyIceberg then replaces the affected window of each gold table. Same shape as
the bronze-to-silver transform: daily and full-refresh runs share this code
path and only the fiscal year filter differs.

- ``gold.jepx_spot_price_daily`` collapses each delivery date's 48 time codes
  into one row per area.
- ``gold.jepx_spot_price_period_profile`` keeps the time code and collapses
  the dates instead, giving the intraday price curve per month, area and day
  type. This is the shape that shows the duck curve emerging.

Grain and what is deliberately absent
-------------------------------------
Both tables are keyed per area. ``system_price`` is denormalized onto every
area row because it is an *intensive* quantity: averaging it across areas
still returns the system price. The volume columns are *extensive* and are
left out entirely -- they are national figures, so repeating them across nine
rows would make any SUM over areas nine times too large. They belong in a
national-grain table of their own, which this pipeline does not have yet.

The profile stores counts rather than rates for the same reason: counts roll
up correctly when a caller aggregates months or day types together, whereas
averaging a stored percentage silently ignores how many observations each
cell held. ``avg_price`` can be rolled up as
``sum(avg_price * observation_count) / sum(observation_count)``; the median
and percentiles cannot be rolled up at all, which is inherent to quantiles.

See docs/tasks/plan_jepx_gold.md for the surrounding plan.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date

import duckdb
import jpholiday
import polars as pl
from pyiceberg.expressions import (
    And,
    BooleanExpression,
    GreaterThanOrEqual,
    LessThan,
    LessThanOrEqual,
)

from common.duckdb_utils import create_duckdb_connection
from common.iceberg.catalog import get_catalog

# The window-replace write path is layer-agnostic despite its name: it
# provisions from a schema CSV, guards the business key and replaces a
# window. Gold needs exactly that. Worth renaming now that a second gold
# table has landed; filed in docs/tasks/tasks.md section 9.
from common.silver_write import column_bound, ensure_unique_keys, write_silver_table
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

PROFILE_TABLE_IDENTIFIER = "gold.jepx_spot_price_period_profile"
PROFILE_KEY_COLUMNS = ("profile_month", "area_name", "day_type", "time_code")
PROFILE_MONTH_COLUMN = "profile_month"

DAILY_STAGING_RELATION = "jepx_gold_daily_staging"
PROFILE_STAGING_RELATION = "jepx_gold_profile_staging"
HOLIDAY_RELATION = "jepx_gold_holidays"

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

WEEKDAY = "weekday"
HOLIDAY = "holiday"


@dataclass(frozen=True)
class GoldTableResult:
    """What one gold table staged and what it received."""

    table_identifier: str
    staged_row_count: int
    rows_written: int

    @property
    def matches(self) -> bool:
        """Whether every staged row reached the table."""
        return self.staged_row_count == self.rows_written


@dataclass(frozen=True)
class SilverToGoldResult:
    """Outcome of one silver-to-gold run."""

    execution_id: str
    tables: list[GoldTableResult]
    delivery_date_count: int


def _build_fiscal_year_filter(fiscal_year: int | None, *, alias: str) -> str:
    """Build the WHERE clause that narrows a run to one fiscal year."""
    if fiscal_year is None:
        return ""
    year = int(fiscal_year)
    return (
        f"WHERE {alias}.delivery_date >= "
        f"DATE '{year}-{FISCAL_YEAR_START_MONTH:02d}-01' "
        f"AND {alias}.delivery_date < "
        f"DATE '{year + 1}-{FISCAL_YEAR_START_MONTH:02d}-01'"
    )


def _joined_cte(
    *, area_relation: str, base_relation: str, fiscal_year: int | None
) -> str:
    """Build the CTE that pairs each area price with its system price.

    The join is inner: a delivery key missing from either silver table cannot
    be aggregated, and the row counts surface the gap rather than hide it.
    """
    return f"""
SELECT
    a.delivery_date,
    a.time_code,
    a.area_name,
    a.area_price,
    b.system_price,
    a.area_price - b.system_price AS spread
FROM {area_relation} a
JOIN {base_relation} b
  ON a.delivery_date = b.delivery_date AND a.time_code = b.time_code
{_build_fiscal_year_filter(fiscal_year, alias="a")}
"""


def _price_aggregates() -> str:
    """Build the price statistics both roll-ups share."""
    return f"""
    CAST(avg(area_price) AS {PRICE_TYPE}) AS avg_price,
    CAST(median(area_price) AS {PRICE_TYPE}) AS median_price,
    CAST(quantile_cont(area_price, 0.05) AS {PRICE_TYPE}) AS p05_price,
    CAST(quantile_cont(area_price, 0.95) AS {PRICE_TYPE}) AS p95_price,
    CAST(avg(system_price) AS {PRICE_TYPE}) AS avg_system_price,
    CAST(avg(spread) AS {PRICE_TYPE}) AS avg_spread"""


def _event_counts(count_alias_suffix: str) -> str:
    """Build the split/spike/floor counters both roll-ups share."""
    return f"""
    CAST(count(*) FILTER (
        WHERE abs(spread) >= {SPLIT_THRESHOLD}
    ) AS INTEGER) AS split_{count_alias_suffix},
    CAST(count(*) FILTER (
        WHERE area_price >= {SPIKE_THRESHOLD}
    ) AS INTEGER) AS spike_{count_alias_suffix},
    CAST(count(*) FILTER (
        WHERE area_price <= {FLOOR_THRESHOLD}
    ) AS INTEGER) AS floor_{count_alias_suffix}"""


def build_daily_relation(
    conn: duckdb.DuckDBPyConnection,
    *,
    area_relation: str,
    base_relation: str,
    fiscal_year: int | None = None,
) -> None:
    """Roll the silver time codes up to one row per delivery date and area.

    The relations are names so that tests can register local frames in place
    of ``iceberg_scan``.
    """
    conn.execute(f"""
CREATE OR REPLACE TEMP TABLE {DAILY_STAGING_RELATION} AS
WITH joined AS ({
        _joined_cte(
            area_relation=area_relation,
            base_relation=base_relation,
            fiscal_year=fiscal_year,
        )
    })
SELECT
    delivery_date,
    area_name,
    {_price_aggregates()},
    CAST(min(area_price) AS {PRICE_TYPE}) AS min_price,
    CAST(max(area_price) AS {PRICE_TYPE}) AS max_price,
    -- Population, not sample: the day's time codes are the whole population,
    -- and stddev_samp would return NULL for a day holding a single slot.
    CAST(stddev_pop(area_price) AS {PRICE_TYPE}) AS stddev_price,
    CAST(max(area_price) - min(area_price) AS {PRICE_TYPE}) AS intraday_range,
    CAST(max(abs(spread)) AS {PRICE_TYPE}) AS max_abs_spread,
    {_event_counts("time_code_count")},
    CAST(count(*) AS INTEGER) AS time_code_count
FROM joined
GROUP BY delivery_date, area_name
""")


def register_holidays(
    conn: duckdb.DuckDBPyConnection, *, from_date: date, to_date: date
) -> str:
    """Register Japanese national holidays covering the range as a relation.

    Saturdays and Sundays are handled in SQL; this only supplies the dates
    that need a calendar. Returns the relation name so callers can pass it on.
    """
    holiday_dates = [
        holiday_date
        for year in range(from_date.year, to_date.year + 1)
        for holiday_date, _name in jpholiday.year_holidays(year)
        if from_date <= holiday_date <= to_date
    ]
    conn.register(
        HOLIDAY_RELATION,
        pl.DataFrame({"holiday_date": holiday_dates}, schema={"holiday_date": pl.Date}),
    )
    return HOLIDAY_RELATION


def resolve_scanned_date_range(
    conn: duckdb.DuckDBPyConnection,
    *,
    base_relation: str,
    fiscal_year: int | None = None,
) -> tuple[date, date] | None:
    """Return the delivery date bounds a run will read, before aggregating."""
    row = conn.execute(f"""
        SELECT min(b.delivery_date), max(b.delivery_date)
        FROM {base_relation} b
        {_build_fiscal_year_filter(fiscal_year, alias="b")}
    """).fetchone()
    if row is None or row[0] is None:
        return None
    return row[0], row[1]


def build_period_profile_relation(
    conn: duckdb.DuckDBPyConnection,
    *,
    area_relation: str,
    base_relation: str,
    holiday_relation: str,
    fiscal_year: int | None = None,
) -> None:
    """Collapse the dates instead of the time codes, giving the intraday curve.

    Grouping by month rather than by season keeps the finest axis the plan
    calls for; a caller can roll months up into seasons or eras, but cannot
    recover months from a table that only stored seasons.
    """
    conn.execute(f"""
CREATE OR REPLACE TEMP TABLE {PROFILE_STAGING_RELATION} AS
WITH joined AS ({
        _joined_cte(
            area_relation=area_relation,
            base_relation=base_relation,
            fiscal_year=fiscal_year,
        )
    }),
classified AS (
    SELECT
        j.*,
        CAST(date_trunc('month', j.delivery_date) AS DATE) AS profile_month,
        CASE
            WHEN isodow(j.delivery_date) IN (6, 7) OR h.holiday_date IS NOT NULL
            THEN '{HOLIDAY}'
            ELSE '{WEEKDAY}'
        END AS day_type
    FROM joined j
    LEFT JOIN {holiday_relation} h ON j.delivery_date = h.holiday_date
)
SELECT
    profile_month,
    area_name,
    day_type,
    time_code,
    {_price_aggregates()},
    {_event_counts("observation_count")},
    CAST(count(*) AS INTEGER) AS observation_count
FROM classified
GROUP BY profile_month, area_name, day_type, time_code
""")


def count_staged_rows(conn: duckdb.DuckDBPyConnection, relation: str) -> int:
    """Count the aggregated rows a staging relation will write."""
    row = conn.execute(f"SELECT count(*) FROM {relation}").fetchone()
    return int(row[0]) if row else 0


def count_incomplete_days(conn: duckdb.DuckDBPyConnection) -> int:
    """Count staged daily rows covering fewer than a full day of time codes."""
    row = conn.execute(
        f"SELECT count(*) FROM {DAILY_STAGING_RELATION} "
        f"WHERE time_code_count <> {EXPECTED_TIME_CODES_PER_DAY}"
    ).fetchone()
    return int(row[0]) if row else 0


def summarize_incomplete_days(
    conn: duckdb.DuckDBPyConnection, *, limit: int = 10
) -> list[tuple[date, str, int]]:
    """Return the first few incomplete (date, area) pairs for the run log."""
    rows = conn.execute(f"""
        SELECT delivery_date, area_name, time_code_count
        FROM {DAILY_STAGING_RELATION}
        WHERE time_code_count <> {EXPECTED_TIME_CODES_PER_DAY}
        ORDER BY delivery_date, area_name
        LIMIT {int(limit)}
    """).fetchall()
    return [(row[0], row[1], int(row[2])) for row in rows]


def extract_daily_frame(conn: duckdb.DuckDBPyConnection) -> pl.DataFrame:
    """Select the staged daily rows, stamped with their lineage."""
    return conn.execute(f"""
        SELECT *, '{SOURCE_DATA}' AS source_data FROM {DAILY_STAGING_RELATION}
    """).pl()


def extract_period_profile_frame(conn: duckdb.DuckDBPyConnection) -> pl.DataFrame:
    """Select the staged profile rows, stamped with their lineage."""
    return conn.execute(f"""
        SELECT *, '{SOURCE_DATA}' AS source_data FROM {PROFILE_STAGING_RELATION}
    """).pl()


def resolve_staged_delivery_range(
    conn: duckdb.DuckDBPyConnection,
) -> tuple[date, date] | None:
    """Return the delivery date bounds of the staged daily rows."""
    row = conn.execute(
        f"SELECT min(delivery_date), max(delivery_date) FROM {DAILY_STAGING_RELATION}"
    ).fetchone()
    if row is None or row[0] is None:
        return None
    return row[0], row[1]


def resolve_staged_profile_month_range(
    conn: duckdb.DuckDBPyConnection,
) -> tuple[date, date] | None:
    """Return the profile month bounds of the staged profile rows."""
    row = conn.execute(
        f"SELECT min(profile_month), max(profile_month) FROM {PROFILE_STAGING_RELATION}"
    ).fetchone()
    if row is None or row[0] is None:
        return None
    return row[0], row[1]


def build_profile_month_window(
    *,
    fiscal_year: int | None,
    staged_range: tuple[date, date] | None,
) -> BooleanExpression | None:
    """Build the predicate identifying the profile rows this run replaces.

    A fiscal year covers the twelve months from April, so its months run from
    April 1 up to (not including) the next April 1. Without a fiscal year the
    window falls back to exactly the months that were staged.
    """
    if fiscal_year is not None:
        return And(
            left=column_bound(
                PROFILE_MONTH_COLUMN,
                GreaterThanOrEqual,
                date(fiscal_year, FISCAL_YEAR_START_MONTH, 1),
            ),
            right=column_bound(
                PROFILE_MONTH_COLUMN,
                LessThan,
                date(fiscal_year + 1, FISCAL_YEAR_START_MONTH, 1),
            ),
        )
    if staged_range is None:
        return None
    earliest, latest = staged_range
    return And(
        left=column_bound(PROFILE_MONTH_COLUMN, GreaterThanOrEqual, earliest),
        right=column_bound(PROFILE_MONTH_COLUMN, LessThanOrEqual, latest),
    )


def count_delivery_dates(conn: duckdb.DuckDBPyConnection) -> int:
    """Count the distinct delivery dates covered by this run."""
    row = conn.execute(
        f"SELECT count(DISTINCT delivery_date) FROM {DAILY_STAGING_RELATION}"
    ).fetchone()
    return int(row[0]) if row else 0


@dataclass(frozen=True)
class _GoldTarget:
    """One gold table's staged frame and everything needed to write it."""

    table_identifier: str
    schema_file: str
    key_cols: tuple[str, ...]
    frame: pl.DataFrame
    overwrite_filter: BooleanExpression | None
    staged_row_count: int


def run_silver_to_gold_jepx_spot_price(
    *,
    catalog_name: str = DEFAULT_CATALOG_NAME,
    area_location: str = DEFAULT_AREA_LOCATION,
    base_location: str = DEFAULT_BASE_LOCATION,
    schema_dir: str = DEFAULT_GOLD_SCHEMA_DIR,
    fiscal_year: int | None = None,
    execution_id: str | None = None,
) -> SilverToGoldResult:
    """Build the JEPX gold tables from the silver layer."""
    run_execution_id = execution_id or gen_uuid()
    catalog = get_catalog(catalog_name)

    area_relation = f"iceberg_scan('{area_location}')"
    base_relation = f"iceberg_scan('{base_location}')"

    conn = create_duckdb_connection()
    try:
        build_daily_relation(
            conn,
            area_relation=area_relation,
            base_relation=base_relation,
            fiscal_year=fiscal_year,
        )
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

        targets = [
            _GoldTarget(
                table_identifier=DAILY_TABLE_IDENTIFIER,
                schema_file="jepx_spot_price_daily.csv",
                key_cols=DAILY_KEY_COLUMNS,
                frame=extract_daily_frame(conn),
                overwrite_filter=build_delivery_window(
                    fiscal_year=fiscal_year,
                    staged_range=resolve_staged_delivery_range(conn),
                ),
                staged_row_count=count_staged_rows(conn, DAILY_STAGING_RELATION),
            )
        ]

        scanned_range = resolve_scanned_date_range(
            conn, base_relation=base_relation, fiscal_year=fiscal_year
        )
        if scanned_range is None:
            logger.warning(
                "No silver rows in scope, so %s was left untouched",
                PROFILE_TABLE_IDENTIFIER,
            )
        else:
            build_period_profile_relation(
                conn,
                area_relation=area_relation,
                base_relation=base_relation,
                holiday_relation=register_holidays(
                    conn, from_date=scanned_range[0], to_date=scanned_range[1]
                ),
                fiscal_year=fiscal_year,
            )
            targets.append(
                _GoldTarget(
                    table_identifier=PROFILE_TABLE_IDENTIFIER,
                    schema_file="jepx_spot_price_period_profile.csv",
                    key_cols=PROFILE_KEY_COLUMNS,
                    frame=extract_period_profile_frame(conn),
                    overwrite_filter=build_profile_month_window(
                        fiscal_year=fiscal_year,
                        staged_range=resolve_staged_profile_month_range(conn),
                    ),
                    staged_row_count=count_staged_rows(conn, PROFILE_STAGING_RELATION),
                )
            )

        # Each table is replaced in its own transaction, so a key collision
        # found while writing the second would leave the first already
        # rebuilt against the new snapshot. Validate every frame first, the
        # same guard the silver transform carries.
        for target in targets:
            if not target.frame.is_empty():
                ensure_unique_keys(
                    target.frame,
                    key_cols=target.key_cols,
                    table_identifier=target.table_identifier,
                )

        tables = [
            GoldTableResult(
                table_identifier=target.table_identifier,
                staged_row_count=target.staged_row_count,
                rows_written=write_silver_table(
                    catalog,
                    table_identifier=target.table_identifier,
                    schema_path=f"{schema_dir}/{target.schema_file}",
                    frame=target.frame,
                    key_cols=target.key_cols,
                    overwrite_filter=target.overwrite_filter,
                    execution_id=run_execution_id,
                ).rows_written,
            )
            for target in targets
        ]
    finally:
        conn.close()

    return SilverToGoldResult(
        execution_id=run_execution_id,
        tables=tables,
        delivery_date_count=delivery_date_count,
    )

"""Gold-layer reads for the JEPX dashboard.

DuckDB scans the gold Iceberg tables directly, the same way the pipelines
read silver. Relation names are parameters rather than constants so tests can
register local frames in place of ``iceberg_scan``, and so nothing here
imports Streamlit -- the caching layer lives in ``app.py``.

Every query reads gold, never silver. That is the point of
``gold.jepx_spot_price_area_spread`` existing at the interval grain: the
heatmap would otherwise have to join 3.3M silver rows against the base table
on each render.
"""

from __future__ import annotations

import duckdb
import polars as pl

GOLD_DAILY_LOCATION = "s3://jp-power-grid-dev/gold/jepx_spot_price_daily"
GOLD_PROFILE_LOCATION = "s3://jp-power-grid-dev/gold/jepx_spot_price_period_profile"
GOLD_AREA_SPREAD_LOCATION = "s3://jp-power-grid-dev/gold/jepx_spot_price_area_spread"

FISCAL_YEAR_START_MONTH = 4

# The fiscal year is the pipeline's own scope unit, so the dashboard uses it
# too rather than inventing a second calendar.
_FISCAL_YEAR_EXPR = (
    f"CASE WHEN month({{column}}) >= {FISCAL_YEAR_START_MONTH} "
    f"THEN year({{column}}) ELSE year({{column}}) - 1 END"
)


def iceberg(location: str) -> str:
    """Build the DuckDB relation expression for a gold table location."""
    return f"iceberg_scan('{location}')"


def fiscal_year_of(column: str) -> str:
    """Build the SQL expression mapping a date column to its fiscal year."""
    return _FISCAL_YEAR_EXPR.format(column=column)


def fetch_areas(conn: duckdb.DuckDBPyConnection, *, daily_relation: str) -> list[str]:
    """List the areas present in gold, in a stable order."""
    rows = conn.execute(
        f"SELECT DISTINCT area_name FROM {daily_relation} ORDER BY area_name"
    ).fetchall()
    return [row[0] for row in rows]


def fetch_fiscal_years(
    conn: duckdb.DuckDBPyConnection, *, daily_relation: str
) -> list[int]:
    """List the fiscal years present in gold, newest first."""
    rows = conn.execute(f"""
        SELECT DISTINCT {fiscal_year_of("delivery_date")} AS fiscal_year
        FROM {daily_relation}
        ORDER BY fiscal_year DESC
    """).fetchall()
    return [int(row[0]) for row in rows]


def fetch_price_heatmap(
    conn: duckdb.DuckDBPyConnection,
    *,
    area_spread_relation: str,
    fiscal_year: int,
    area_name: str,
) -> pl.DataFrame:
    """Read one fiscal year of one area at the delivery date x slot grain.

    Roughly 17,500 rows, which is what keeps a 365x48 heatmap cheap enough to
    redraw on every filter change.
    """
    return conn.execute(
        f"""
        SELECT delivery_date, time_code, area_price, spread, is_split
        FROM {area_spread_relation}
        WHERE {fiscal_year_of("delivery_date")} = {int(fiscal_year)}
          AND area_name = ?
        ORDER BY delivery_date, time_code
    """,
        [area_name],
    ).pl()


def fetch_split_rate_matrix(
    conn: duckdb.DuckDBPyConnection, *, daily_relation: str
) -> pl.DataFrame:
    """Read the share of slots that split, per fiscal year and area.

    Counts are summed and divided once, rather than averaging a stored rate:
    a mean of daily percentages would weight a short day the same as a full
    one.
    """
    return conn.execute(f"""
        SELECT
            {fiscal_year_of("delivery_date")} AS fiscal_year,
            area_name,
            sum(split_time_code_count) AS split_count,
            sum(time_code_count) AS slot_count,
            100.0 * sum(split_time_code_count) / sum(time_code_count) AS split_pct
        FROM {daily_relation}
        GROUP BY 1, 2
        ORDER BY 1, 2
    """).pl()


def fetch_intraday_profile(
    conn: duckdb.DuckDBPyConnection,
    *,
    profile_relation: str,
    area_name: str,
    day_type: str,
    fiscal_years: list[int],
) -> pl.DataFrame:
    """Read the intraday curve for each requested fiscal year.

    The profile stores monthly cells, so collapsing them to a year means a
    weighted mean: ``sum(avg_price * observation_count) / sum(...)``. Taking a
    plain mean of the monthly averages would let a short month count as much
    as a long one.
    """
    if not fiscal_years:
        return pl.DataFrame(
            schema={
                "fiscal_year": pl.Int64,
                "time_code": pl.Int32,
                "avg_price": pl.Float64,
                "observation_count": pl.Int64,
            }
        )
    years = ", ".join(str(int(year)) for year in fiscal_years)
    return conn.execute(
        f"""
        SELECT
            {fiscal_year_of("profile_month")} AS fiscal_year,
            time_code,
            sum(avg_price * observation_count) / sum(observation_count) AS avg_price,
            sum(observation_count) AS observation_count
        FROM {profile_relation}
        WHERE area_name = ?
          AND day_type = ?
          AND {fiscal_year_of("profile_month")} IN ({years})
        GROUP BY 1, 2
        ORDER BY 1, 2
    """,
        [area_name, day_type],
    ).pl()


def slot_label(time_code: int) -> str:
    """Render a time code as the JST clock time its 30-minute slot starts at."""
    minutes = (int(time_code) - 1) * 30
    return f"{minutes // 60:02d}:{minutes % 60:02d}"

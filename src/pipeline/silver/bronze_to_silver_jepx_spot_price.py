"""Transform JEPX spot price data from the bronze layer into silver tables.

DuckDB reads the bronze Iceberg table directly, casts and deduplicates the
rows, derives the delivery timestamp, and PyIceberg replaces the affected
delivery window in the three silver tables. Daily and full-refresh runs share
this code path; the only difference is the fiscal year filter.

The write is a window replace rather than a row-level upsert. ``upsert()``
builds a match predicate holding every join key in the source frame and scans
the target table with it, so its cost grows with both the batch and the table.
At roughly 56k keys against a 3.3M row area table that scan exhausted memory
and killed the process. The staging relation already holds the complete,
deduplicated set of rows for the window being loaded, so replacing that window
reaches the same result without ever building that predicate.

Replacing rather than merging changes one behaviour: a delivery key whose
newest bronze row fails validation is dropped from the frame, and because the
whole window is rewritten from the frame alone, that key disappears from silver
instead of keeping the value an earlier run wrote. Silver therefore mirrors what
bronze currently says rather than accumulating the last-known-good value; the
run logs a warning naming every violation that caused it.
"""

from __future__ import annotations

import argparse
import logging
from dataclasses import dataclass
from datetime import date

import duckdb
import polars as pl
from pyiceberg.expressions import (
    And,
    BooleanExpression,
    GreaterThanOrEqual,
    LessThan,
    LessThanOrEqual,
    LiteralPredicate,
)

from common.duckdb_utils import create_duckdb_connection
from common.iceberg import get_catalog
from common.silver_write import (
    SilverWriteResult,
    column_bound,
    ensure_unique_keys,
    write_silver_table,
)
from common.utilities import gen_uuid

logger = logging.getLogger(__name__)

DEFAULT_CATALOG_NAME = "dlh_dev"
DEFAULT_BRONZE_LOCATION = "s3://jp-power-grid-dev/bronze/jepx_spot_price"
DEFAULT_SILVER_SCHEMA_DIR = "/workspace/configuration/iceberg/schema/silver"

STAGING_RELATION = "jepx_silver_staging"

# The column every silver table is windowed on.
#
# The silver tables are currently unpartitioned: the schema CSVs carry a
# `partition_transform` column, but `provision_table()` never passes a
# partition spec to `create_table`, so it has no effect. Iceberg therefore
# prunes the window delete using each data file's min/max metrics, and only
# drops a file outright when every row in it falls inside the window. A file
# straddling the window boundary is rewritten instead, which pulls it through
# memory. That stays bounded while files are written per fiscal year, but a
# full-refresh run writes files spanning every year, so the next scoped run
# rewrites the whole table. Partitioning on `year(delivery_date)` would make
# the pruning exact; see the follow-up in
# docs/reports/reports_20260808_10c4679e-4370-49d3-9b8c-92f890c5eade.md.
DELIVERY_DATE_COLUMN = "delivery_date"

# A time code identifies a 30-minute slot and denotes its START time in JST.
# Time code 1 covers 00:00-00:30 and is stored as 00:00 JST (15:00 UTC the
# previous day). If JEPX ever turns out to label slots by their end time,
# every row shifts by exactly one slot and only SLOT_OFFSET needs to change.
SLOT_MINUTES = 30
SLOT_OFFSET = 1
DELIVERY_TIMEZONE = "Asia/Tokyo"
MIN_TIME_CODE = 1
MAX_TIME_CODE = 48

# JEPX prices carry two decimal places. Casting them to an integer type does
# not fail in DuckDB, it rounds, so the decimal type here is what keeps the
# fractional part from being silently discarded.
PRICE_TYPE = "DECIMAL(32, 3)"
FISCAL_YEAR_START_MONTH = 4

AREA_NAMES = (
    "hokkaido",
    "tohoku",
    "tokyo",
    "chubu",
    "hokuriku",
    "kansai",
    "chugoku",
    "shikoku",
    "kyushu",
)
AREA_PRICE_COLUMNS = tuple(f"area_price_{name}" for name in AREA_NAMES)
VOLUME_COLUMNS = (
    "selling_bid_volume",
    "purchase_bid_volume",
    "contracted_volume",
)
BLOCK_COLUMNS = (
    "block_selling_bid_volume",
    "block_selling_contracted_volume",
    "block_purchase_bid_volume",
    "block_purchase_contracted_volume",
)


@dataclass(frozen=True)
class BronzeToSilverResult:
    """Outcome of one bronze-to-silver run."""

    execution_id: str
    writes: list[SilverWriteResult]
    dropped_row_count: int


def _build_fiscal_year_filter(fiscal_year: int | None) -> str:
    """Build the WHERE clause that narrows a run to one fiscal year."""
    if fiscal_year is None:
        return ""
    year = int(fiscal_year)
    return (
        f"WHERE delivery_date_d >= DATE '{year}-{FISCAL_YEAR_START_MONTH:02d}-01' "
        f"AND delivery_date_d < DATE '{year + 1}-{FISCAL_YEAR_START_MONTH:02d}-01'"
    )


def _build_typed_projection() -> str:
    """Build the cast expressions that turn bronze strings into typed values."""
    expressions = [
        "COALESCE("
        "try_strptime(delivery_date, '%Y-%m-%d'), "
        "try_strptime(delivery_date, '%Y/%m/%d')"
        ") AS delivery_date_d",
        "TRY_CAST(time_code AS INTEGER) AS time_code_i",
    ]
    for column in (*VOLUME_COLUMNS, *BLOCK_COLUMNS):
        expressions.append(
            f"TRY_CAST(REPLACE({column}, ',', '') AS BIGINT) AS {column}"
        )
    for column in ("system_price", *AREA_PRICE_COLUMNS):
        expressions.append(
            f"TRY_CAST(REPLACE({column}, ',', '') AS {PRICE_TYPE}) AS {column}"
        )
    expressions.extend(["source_data", "ingestion_time", "execution_id"])
    return ",\n        ".join(expressions)


def _build_violation_expression() -> str:
    """Build the list expression that records why a row is not usable."""
    area_price_list = ", ".join(AREA_PRICE_COLUMNS)
    checks = [
        "CASE WHEN delivery_date_d IS NULL THEN 'delivery_date_null' END",
        "CASE WHEN time_code_i IS NULL THEN 'time_code_null' END",
        f"CASE WHEN time_code_i NOT BETWEEN {MIN_TIME_CODE} AND {MAX_TIME_CODE} "
        "THEN 'time_code_out_of_range' END",
        "CASE WHEN system_price < 0 THEN 'system_price_negative' END",
        f"CASE WHEN list_min([{area_price_list}]) < 0 THEN 'area_price_negative' END",
    ]
    joined = ",\n            ".join(checks)
    return f"list_filter([\n            {joined}\n        ], x -> x IS NOT NULL)"


def build_staging_relation(
    conn: duckdb.DuckDBPyConnection,
    *,
    source_relation: str,
    fiscal_year: int | None = None,
) -> None:
    """Cast, deduplicate and validate bronze rows into a staging relation.

    ``source_relation`` is a relation name so that tests can pass a locally
    registered frame in place of ``iceberg_scan``.
    """
    passthrough = ",\n    ".join(
        (*VOLUME_COLUMNS, *BLOCK_COLUMNS, "system_price", *AREA_PRICE_COLUMNS)
    )
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
    {_build_fiscal_year_filter(fiscal_year)}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY delivery_date_d, time_code_i
        ORDER BY ingestion_time DESC NULLS LAST, execution_id DESC NULLS LAST
    ) = 1
),
validated AS (
    SELECT
        *,
        {_build_violation_expression()} AS violations
    FROM deduplicated
)
SELECT
    CAST(delivery_date_d AS DATE) AS delivery_date,
    time_code_i AS time_code,
    (delivery_date_d + ((time_code_i - {SLOT_OFFSET}) * INTERVAL {SLOT_MINUTES} MINUTE))
        AT TIME ZONE '{DELIVERY_TIMEZONE}' AS delivery_datetime,
    {passthrough},
    source_data,
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


def extract_base_frame(conn: duckdb.DuckDBPyConnection) -> pl.DataFrame:
    """Select the base measures for valid staged rows."""
    columns = ",\n            ".join(("system_price", *VOLUME_COLUMNS))
    return conn.execute(f"""
        SELECT
            delivery_date,
            time_code,
            delivery_datetime,
            {columns},
            source_data
        FROM {STAGING_RELATION}
        WHERE len(violations) = 0
    """).pl()


def extract_block_frame(conn: duckdb.DuckDBPyConnection) -> pl.DataFrame:
    """Select the block volumes for valid staged rows."""
    columns = ",\n            ".join(BLOCK_COLUMNS)
    return conn.execute(f"""
        SELECT
            delivery_date,
            time_code,
            delivery_datetime,
            {columns},
            source_data
        FROM {STAGING_RELATION}
        WHERE len(violations) = 0
    """).pl()


def extract_area_frame(conn: duckdb.DuckDBPyConnection) -> pl.DataFrame:
    """Unpivot the per-area price columns into one row per area.

    The inner SELECT narrows the columns before UNPIVOT because DuckDB
    carries every unlisted column through, which would otherwise pull the
    block volumes into this frame.
    """
    area_columns = ",\n                   ".join(AREA_PRICE_COLUMNS)
    return conn.execute(f"""
        WITH source AS (
            SELECT delivery_date,
                   time_code,
                   delivery_datetime,
                   source_data,
                   {area_columns}
            FROM {STAGING_RELATION}
            WHERE len(violations) = 0
        ),
        area_unpivoted AS (
            SELECT delivery_date,
                   time_code,
                   delivery_datetime,
                   source_data,
                   area_price_column,
                   area_price
            FROM source
            UNPIVOT (
                area_price FOR area_price_column IN ({", ".join(AREA_PRICE_COLUMNS)})
            )
        )
        SELECT
            delivery_date,
            time_code,
            delivery_datetime,
            split_part(area_price_column, '_', 3) AS area_name,
            area_price,
            source_data
        FROM area_unpivoted
    """).pl()


def resolve_staged_delivery_range(
    conn: duckdb.DuckDBPyConnection,
) -> tuple[date, date] | None:
    """Return the delivery date bounds of the staged rows that passed validation."""
    row = conn.execute(f"""
        SELECT min(delivery_date), max(delivery_date)
        FROM {STAGING_RELATION}
        WHERE len(violations) = 0
    """).fetchone()
    if row is None or row[0] is None:
        return None
    return row[0], row[1]


def delivery_date_bound(
    predicate: type[LiteralPredicate], boundary: date
) -> BooleanExpression:
    """Build one delivery-date comparison against ``boundary``."""
    return column_bound(DELIVERY_DATE_COLUMN, predicate, boundary)


def build_delivery_window(
    *,
    fiscal_year: int | None,
    staged_range: tuple[date, date] | None,
) -> BooleanExpression | None:
    """Build the predicate identifying the silver rows this run replaces.

    A fiscal year spans April 1 through the following March 31, so scoping to
    it clears any stale row in that window even when the new snapshot covers
    fewer days. Without a fiscal year the window falls back to exactly what was
    staged, which keeps a full-refresh run from touching anything it did not
    rebuild.
    """
    if fiscal_year is not None:
        return And(
            left=delivery_date_bound(
                GreaterThanOrEqual, date(fiscal_year, FISCAL_YEAR_START_MONTH, 1)
            ),
            right=delivery_date_bound(
                LessThan, date(fiscal_year + 1, FISCAL_YEAR_START_MONTH, 1)
            ),
        )
    if staged_range is None:
        return None
    earliest, latest = staged_range
    return And(
        left=delivery_date_bound(GreaterThanOrEqual, earliest),
        right=delivery_date_bound(LessThanOrEqual, latest),
    )


def run_bronze_to_silver_jepx_spot_price(
    *,
    catalog_name: str = DEFAULT_CATALOG_NAME,
    bronze_location: str = DEFAULT_BRONZE_LOCATION,
    schema_dir: str = DEFAULT_SILVER_SCHEMA_DIR,
    fiscal_year: int | None = None,
    execution_id: str | None = None,
) -> BronzeToSilverResult:
    """Run the full bronze-to-silver transformation for JEPX spot prices."""
    run_execution_id = execution_id or gen_uuid()
    catalog = get_catalog(catalog_name)

    conn = create_duckdb_connection()
    try:
        build_staging_relation(
            conn,
            source_relation=f"iceberg_scan('{bronze_location}')",
            fiscal_year=fiscal_year,
        )

        dropped_row_count = count_dropped_rows(conn)
        if dropped_row_count:
            logger.warning(
                (
                    "Dropped %s invalid rows; their delivery keys are removed "
                    "from silver because the window is replaced by the valid "
                    "rows alone: %s"
                ),
                dropped_row_count,
                summarize_violations(conn),
            )

        overwrite_filter = build_delivery_window(
            fiscal_year=fiscal_year,
            staged_range=resolve_staged_delivery_range(conn),
        )

        targets = (
            (
                "silver.jepx_spot_price_base",
                ("delivery_date", "time_code"),
                extract_base_frame(conn),
            ),
            (
                "silver.jepx_spot_price_block",
                ("delivery_date", "time_code"),
                extract_block_frame(conn),
            ),
            (
                "silver.jepx_spot_price_area",
                ("delivery_date", "time_code", "area_name"),
                extract_area_frame(conn),
            ),
        )

        # Each table is replaced in its own transaction, so a key collision
        # discovered while writing the third target would leave the first two
        # already rebuilt against the new snapshot. Validate every frame before
        # touching any table.
        for identifier, key_cols, frame in targets:
            if not frame.is_empty():
                ensure_unique_keys(
                    frame, key_cols=key_cols, table_identifier=identifier
                )

        writes = [
            write_silver_table(
                catalog,
                table_identifier=identifier,
                schema_path=f"{schema_dir}/{identifier.split('.')[-1]}.csv",
                frame=frame,
                key_cols=key_cols,
                overwrite_filter=overwrite_filter,
                execution_id=run_execution_id,
            )
            for identifier, key_cols, frame in targets
        ]
    finally:
        conn.close()

    return BronzeToSilverResult(
        execution_id=run_execution_id,
        writes=writes,
        dropped_row_count=dropped_row_count,
    )


def build_parser() -> argparse.ArgumentParser:
    """Build the argument parser for the bronze-to-silver CLI."""
    parser = argparse.ArgumentParser(
        description="Transform JEPX bronze spot prices into silver Iceberg tables"
    )
    parser.add_argument(
        "--catalog",
        default=DEFAULT_CATALOG_NAME,
        help=f"Iceberg catalog name (default: {DEFAULT_CATALOG_NAME})",
    )
    parser.add_argument(
        "--bronze-location",
        default=DEFAULT_BRONZE_LOCATION,
        help="Bronze table location scanned by DuckDB",
    )
    parser.add_argument(
        "--schema-dir",
        default=DEFAULT_SILVER_SCHEMA_DIR,
        help="Directory containing the silver schema CSV files",
    )
    parser.add_argument(
        "--fiscal-year",
        type=int,
        help="Limit the run to one fiscal year (default: rebuild every year)",
    )
    return parser


def main() -> None:
    """CLI entrypoint for the JEPX bronze-to-silver transformation."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )
    args = build_parser().parse_args()

    result = run_bronze_to_silver_jepx_spot_price(
        catalog_name=args.catalog,
        bronze_location=args.bronze_location,
        schema_dir=args.schema_dir,
        fiscal_year=args.fiscal_year,
    )

    logger.info("JEPX bronze-to-silver summary (execution_id=%s):", result.execution_id)
    for write in result.writes:
        logger.info(
            " - table=%s, written=%s",
            write.table_identifier,
            write.rows_written,
        )
    logger.info(" - dropped rows: %s", result.dropped_row_count)


if __name__ == "__main__":
    main()

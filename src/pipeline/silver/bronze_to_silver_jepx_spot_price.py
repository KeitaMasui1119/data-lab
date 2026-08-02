"""Transform JEPX spot price data from the bronze layer into silver tables.

DuckDB reads the bronze Iceberg table directly, casts and deduplicates the
rows, derives the delivery timestamp, and PyIceberg upserts the result into
the three silver tables. Daily and full-refresh runs share this code path;
the only difference is the fiscal year filter.
"""

from __future__ import annotations

import argparse
import logging
import os
from dataclasses import dataclass
from urllib.parse import urlparse

import duckdb
import polars as pl
from pyiceberg.catalog import Catalog

from common.iceberg import get_catalog, provision_table
from common.pipeline_utilities import add_metadata
from common.utilities import gen_uuid

logger = logging.getLogger(__name__)

DEFAULT_CATALOG_NAME = "dlh_dev"
DEFAULT_BRONZE_LOCATION = "s3://jp-power-grid-dev/bronze/jepx_spot_price"
DEFAULT_SILVER_SCHEMA_DIR = "/workspace/configuration/iceberg/schema/silver"

STAGING_RELATION = "jepx_silver_staging"

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
SILVER_STATUS_LOADED = "loaded"

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
class SilverUpsertResult:
    """Outcome of upserting one silver table."""

    table_identifier: str
    rows_updated: int
    rows_inserted: int


@dataclass(frozen=True)
class BronzeToSilverResult:
    """Outcome of one bronze-to-silver run."""

    execution_id: str
    upserts: list[SilverUpsertResult]
    dropped_row_count: int


def _split_endpoint(endpoint_url: str) -> tuple[str, bool]:
    """Split an endpoint URL into a DuckDB host:port and a TLS flag."""
    parsed = urlparse(endpoint_url)
    if parsed.netloc:
        return parsed.netloc, parsed.scheme == "https"
    return endpoint_url, False


def create_duckdb_connection(*, configure_s3: bool = True) -> duckdb.DuckDBPyConnection:
    """Open a DuckDB connection prepared for reading the bronze Iceberg table.

    ``icu`` is statically linked into DuckDB and auto-loads, so no extension
    install is needed for ``AT TIME ZONE``. Pass ``configure_s3=False`` to
    read from a locally registered relation instead of object storage.
    """
    conn = duckdb.connect()
    if not configure_s3:
        return conn

    conn.execute("INSTALL httpfs; LOAD httpfs;")
    conn.execute("INSTALL iceberg; LOAD iceberg;")
    conn.execute("SET unsafe_enable_version_guessing = true;")

    endpoint_url = os.environ.get("AWS_ENDPOINT_URL")
    if not endpoint_url:
        raise ValueError("AWS_ENDPOINT_URL is required to read the bronze table")
    access_key = os.environ.get("AWS_ACCESS_KEY_ID")
    secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
    if not access_key or not secret_key:
        raise ValueError(
            "AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY are required "
            "to read the bronze table"
        )

    endpoint, use_ssl = _split_endpoint(endpoint_url)
    conn.execute("SET s3_endpoint = ?;", [endpoint])
    conn.execute("SET s3_use_ssl = ?;", [use_ssl])
    conn.execute("SET s3_url_style = 'path';")
    conn.execute("SET s3_region = ?;", [os.environ.get("AWS_REGION", "us-east-1")])
    conn.execute("SET s3_access_key_id = ?;", [access_key])
    conn.execute("SET s3_secret_access_key = ?;", [secret_key])
    return conn


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


def _align_to_target_schema(frame: pl.DataFrame, target_field_names: list[str]):
    """Add any missing target columns as nulls and order columns to match."""
    aligned = frame
    for field_name in target_field_names:
        if field_name not in aligned.columns:
            aligned = aligned.with_columns(pl.lit(None).alias(field_name))
    return aligned.select(target_field_names)


def upsert_silver_table(
    catalog: Catalog,
    *,
    table_identifier: str,
    schema_path: str,
    frame: pl.DataFrame,
    join_cols: tuple[str, ...],
    execution_id: str,
) -> SilverUpsertResult:
    """Upsert one silver table, stamping the audit columns on the way in."""
    provision_table(catalog, table_identifier, schema_path)
    table = catalog.load_table(table_identifier)

    if frame.is_empty():
        logger.info("Skipped %s because there are no valid rows", table_identifier)
        return SilverUpsertResult(table_identifier, rows_updated=0, rows_inserted=0)

    stamped = add_metadata(
        frame.with_columns(pl.lit(SILVER_STATUS_LOADED).alias("status")),
        execution_id=execution_id,
    )

    target_schema = table.schema().as_arrow()
    aligned = _align_to_target_schema(stamped, [field.name for field in target_schema])
    result = table.upsert(
        aligned.to_arrow().cast(target_schema), join_cols=list(join_cols)
    )

    logger.info(
        "Upserted %s: updated=%s, inserted=%s",
        table_identifier,
        result.rows_updated,
        result.rows_inserted,
    )
    return SilverUpsertResult(
        table_identifier=table_identifier,
        rows_updated=result.rows_updated,
        rows_inserted=result.rows_inserted,
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
                "Excluded %s invalid rows from silver: %s",
                dropped_row_count,
                summarize_violations(conn),
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

        upserts = [
            upsert_silver_table(
                catalog,
                table_identifier=identifier,
                schema_path=f"{schema_dir}/{identifier.split('.')[-1]}.csv",
                frame=frame,
                join_cols=join_cols,
                execution_id=run_execution_id,
            )
            for identifier, join_cols, frame in targets
        ]
    finally:
        conn.close()

    return BronzeToSilverResult(
        execution_id=run_execution_id,
        upserts=upserts,
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
        help="Limit the run to one fiscal year (default: upsert every year)",
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
    for upsert in result.upserts:
        logger.info(
            " - table=%s, updated=%s, inserted=%s",
            upsert.table_identifier,
            upsert.rows_updated,
            upsert.rows_inserted,
        )
    logger.info(" - dropped rows: %s", result.dropped_row_count)


if __name__ == "__main__":
    main()

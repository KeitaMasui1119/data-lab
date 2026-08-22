"""Shared silver-layer write helpers: window-replace with schema alignment.

Both JEPX and OCCTO's bronze-to-silver transforms replace an Iceberg
table's affected window with a freshly staged frame rather than perform a
row-level upsert. ``upsert()`` builds a match predicate holding every join
key in the source frame and scans the target table with it, so its cost
grows with both the batch and the table; at JEPX's row counts that scan
exhausted memory. The staging frame already holds the complete,
deduplicated set of rows for the window being loaded, so replacing that
window reaches the same result without ever building that predicate.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date

import polars as pl
from pyiceberg.catalog import Catalog
from pyiceberg.expressions import BooleanExpression, LiteralPredicate

from common.iceberg.catalog import provision_table
from common.polars_utils import add_metadata

logger = logging.getLogger(__name__)

SILVER_STATUS_LOADED = "loaded"


@dataclass(frozen=True)
class SilverWriteResult:
    """Outcome of writing one silver table."""

    table_identifier: str
    rows_written: int


def column_bound(
    column_name: str, predicate: type[LiteralPredicate], boundary: date
) -> BooleanExpression:
    """Build one column comparison against ``boundary``.

    PyIceberg predicates accept a column name and a plain Python value at
    runtime, but pyright type-checks the ``__init__`` Pydantic synthesizes
    from the model fields rather than the hand-written one, and rejects both
    arguments. Wrapping the call keeps that suppression to a single site.
    """
    return predicate(column_name, boundary)  # pyright: ignore[reportCallIssue]


def _align_to_target_schema(frame: pl.DataFrame, target_field_names: list[str]):
    """Add any missing target columns as nulls and order columns to match."""
    aligned = frame
    for field_name in target_field_names:
        if field_name not in aligned.columns:
            aligned = aligned.with_columns(pl.lit(None).alias(field_name))
    return aligned.select(target_field_names)


def ensure_unique_keys(
    frame: pl.DataFrame,
    *,
    key_cols: tuple[str, ...],
    table_identifier: str,
) -> None:
    """Reject frames holding more than one row per business key.

    ``upsert()`` refused duplicate source keys outright. Replacing a window
    would instead write them straight through, so the guard has to be
    explicit now that the write no longer performs the match itself.
    """
    duplicate_count = frame.height - frame.select(key_cols).n_unique()
    if duplicate_count:
        raise ValueError(
            f"Refusing to write {table_identifier}: found {duplicate_count} "
            f"duplicate rows for key {key_cols}"
        )


def write_silver_table(
    catalog: Catalog,
    *,
    table_identifier: str,
    schema_path: str,
    frame: pl.DataFrame,
    key_cols: tuple[str, ...],
    overwrite_filter: BooleanExpression | None,
    execution_id: str,
) -> SilverWriteResult:
    """Replace one silver table's target window, stamping the audit columns."""
    provision_table(catalog, table_identifier, schema_path)
    table = catalog.load_table(table_identifier)

    if frame.is_empty():
        logger.info("Skipped %s because there are no valid rows", table_identifier)
        return SilverWriteResult(table_identifier, rows_written=0)

    ensure_unique_keys(frame, key_cols=key_cols, table_identifier=table_identifier)

    if overwrite_filter is None:
        raise ValueError(
            f"Refusing to write {table_identifier} without a delivery window; "
            "a non-empty frame must always resolve to one"
        )

    stamped = add_metadata(
        frame.with_columns(pl.lit(SILVER_STATUS_LOADED).alias("status")),
        execution_id=execution_id,
    )

    target_schema = table.schema().as_arrow()
    aligned = _align_to_target_schema(stamped, [field.name for field in target_schema])
    table.overwrite(
        aligned.to_arrow().cast(target_schema),
        overwrite_filter=overwrite_filter,
    )

    row_count = aligned.height
    logger.info(
        "Replaced the target window of %s with %s rows",
        table_identifier,
        row_count,
    )
    return SilverWriteResult(
        table_identifier=table_identifier,
        rows_written=row_count,
    )

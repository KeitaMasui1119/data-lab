"""Durable execution history for orchestrated pipeline runs.

Before this table existed, an orchestrator built its ``PipelineStepResult``
list, logged it and dropped it, so nothing on disk could answer "did last
night's run succeed, how long did it take, and how many rows reached
silver". ``metadata.pipeline_run_log`` is that record.

Grain is one row per step, keyed by ``run_id`` + ``step_seq``. A run is
recovered by grouping on ``run_id`` rather than kept in a second table: the
orchestrators already hand over a list of steps, so step grain is what they
produce and run-level rollups are a GROUP BY away.

``run_id`` is the same value the run stamps as ``execution_id`` on the silver
rows it writes, which is what lets a run in this table be joined to the data
it produced. Raw and bronze do not accept a caller-supplied execution id yet,
so that half of the join is still open (docs/tasks/tasks.md).

The write is a plain append. Unlike silver, nothing here is ever replaced --
a step that ran is a fact about the past, and a re-run is a new ``run_id``.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from datetime import datetime

import polars as pl

from common.iceberg.catalog import get_catalog
from common.polars_utils import add_metadata
from common.utils import get_now_utc
from orchestration.pipeline_result import PipelineStepResult

logger = logging.getLogger(__name__)

DEFAULT_RUN_LOG_IDENTIFIER = "metadata.pipeline_run_log"

# Column order mirrors the schema CSV so the cast-to-Iceberg diff stays readable.
RUN_LOG_COLUMNS = (
    "run_id",
    "step_seq",
    "pipeline_name",
    "step_name",
    "step_status",
    "started_at",
    "ended_at",
    "duration_seconds",
    "expected_row_count",
    "actual_row_count",
    "target_scope",
    "detail",
    "logged_date",
)

# Polars dtypes for the columns that would otherwise be inferred from data.
# An all-null count column infers as Null and fails the Iceberg cast, and
# step_seq has to be Int32 because the schema declares `int`, not `long`.
_RUN_LOG_DTYPES: dict[str, pl.DataType] = {
    "run_id": pl.Utf8(),
    "step_seq": pl.Int32(),
    "pipeline_name": pl.Utf8(),
    "step_name": pl.Utf8(),
    "step_status": pl.Utf8(),
    "started_at": pl.Datetime(time_unit="us", time_zone="UTC"),
    "ended_at": pl.Datetime(time_unit="us", time_zone="UTC"),
    "duration_seconds": pl.Float64(),
    "expected_row_count": pl.Int64(),
    "actual_row_count": pl.Int64(),
    "target_scope": pl.Utf8(),
    "detail": pl.Utf8(),
    "logged_date": pl.Date(),
}


def build_run_log_frame(
    *,
    run_id: str,
    pipeline_name: str,
    target_scope: str,
    results: Sequence[PipelineStepResult],
    logged_at: datetime | None = None,
) -> pl.DataFrame:
    """Turn one run's step results into the frame appended to the run log.

    ``pipeline_name`` and ``target_scope`` are denormalized onto every row:
    at step grain there is nowhere else to put them, and a GROUP BY that had
    to look them up elsewhere would lose the run's context.
    """
    if not results:
        raise ValueError(
            "Cannot build a run log frame with no pipeline steps; an "
            "orchestrated run always produces at least one"
        )

    logged_at = logged_at or get_now_utc()
    rows = [
        {
            "run_id": run_id,
            "step_seq": step_seq,
            "pipeline_name": pipeline_name,
            "step_name": result.name,
            "step_status": result.status,
            "started_at": result.started_at,
            "ended_at": result.ended_at,
            "duration_seconds": result.duration_seconds,
            "expected_row_count": result.expected_row_count,
            "actual_row_count": result.actual_row_count,
            "target_scope": target_scope,
            "detail": result.detail,
            "logged_date": logged_at.date(),
        }
        for step_seq, result in enumerate(results, start=1)
    ]

    frame = pl.DataFrame(rows, schema=_RUN_LOG_DTYPES)
    return frame.select(RUN_LOG_COLUMNS)


def build_run_log_write_frame(
    *,
    run_id: str,
    pipeline_name: str,
    target_scope: str,
    results: Sequence[PipelineStepResult],
    logged_at: datetime | None = None,
) -> pl.DataFrame:
    """Build the run log frame complete with the injected audit columns.

    Every table gets five audit fields injected by the schema builder.
    ``add_metadata()`` supplies three of them; ``source_data`` and ``status``
    are each writer's job, so they are set here or the cast to the Iceberg
    schema fails on field names not matching.

    ``source_data`` stays null: unlike a bronze row, a run log row is not
    derived from a source file -- it describes the run itself. ``status`` is
    always "new" because the log is append-only and no row is ever revised.
    """
    frame = build_run_log_frame(
        run_id=run_id,
        pipeline_name=pipeline_name,
        target_scope=target_scope,
        results=results,
        logged_at=logged_at,
    )
    with_source = frame.with_columns(
        pl.lit(None, dtype=pl.Utf8).alias("source_data"),
        pl.lit("new", dtype=pl.Utf8).alias("status"),
    )
    return add_metadata(with_source, execution_id=run_id)


def write_run_log(
    *,
    run_id: str,
    pipeline_name: str,
    target_scope: str,
    results: Sequence[PipelineStepResult],
    catalog_name: str,
    table_identifier: str = DEFAULT_RUN_LOG_IDENTIFIER,
    logged_at: datetime | None = None,
) -> int:
    """Append one run's steps to the run log, returning how many rows landed."""
    frame = build_run_log_write_frame(
        run_id=run_id,
        pipeline_name=pipeline_name,
        target_scope=target_scope,
        results=results,
        logged_at=logged_at,
    )

    catalog = get_catalog(catalog_name)
    table = catalog.load_table(table_identifier)
    table.append(frame.to_arrow().cast(table.schema().as_arrow()))
    return frame.height


def record_pipeline_run(
    *,
    run_id: str,
    pipeline_name: str,
    target_scope: str,
    results: Sequence[PipelineStepResult],
    catalog_name: str,
    table_identifier: str = DEFAULT_RUN_LOG_IDENTIFIER,
) -> int:
    """Persist a finished run, reporting rather than raising on failure.

    The run log is observability, not the product: failing an ETL run that
    already moved its rows because the audit row could not be written would
    turn a bookkeeping problem into a data one. The exception is logged with
    its traceback rather than swallowed, so a run log that stops recording is
    still visible in the run's own output.
    """
    try:
        return write_run_log(
            run_id=run_id,
            pipeline_name=pipeline_name,
            target_scope=target_scope,
            results=results,
            catalog_name=catalog_name,
            table_identifier=table_identifier,
        )
    except Exception:
        logger.exception(
            "Failed to write the run log for run_id=%s pipeline=%s; the run "
            "itself is unaffected but has left no durable record",
            run_id,
            pipeline_name,
        )
        return 0

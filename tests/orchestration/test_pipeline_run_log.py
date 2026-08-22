"""Unit tests for the pipeline run log frame builder.

``metadata.pipeline_run_log`` is the only durable record that an orchestrated
run happened at all: before it existed the orchestrators returned their
``PipelineStepResult`` list, logged it and dropped it, so "did last night's
run succeed, and how many rows reached silver" could not be answered after
the fact. These tests cover the frame that gets appended to that table;
the append itself needs a live catalog and is exercised by the integration
suite.
"""

from __future__ import annotations

from datetime import UTC, date, datetime

import polars as pl
import pytest

from common.iceberg.schema import build_table_schema
from orchestration.pipeline_result import (
    STATUS_FAILED,
    STATUS_SKIPPED,
    STATUS_SUCCESS,
    PipelineStepResult,
    stamp_step_timing,
)
from orchestration.pipeline_run_log import (
    RUN_LOG_SCHEMA_PATH,
    build_run_log_frame,
    build_run_log_write_frame,
)

RUN_ID = "11111111-2222-3333-4444-555555555555"
STARTED_AT = datetime(2026, 8, 22, 1, 0, 0, tzinfo=UTC)
ENDED_AT = datetime(2026, 8, 22, 1, 0, 30, tzinfo=UTC)


def _timed(result: PipelineStepResult) -> PipelineStepResult:
    """Stamp a result with a fixed window so assertions stay deterministic."""
    return stamp_step_timing(result, started_at=STARTED_AT, ended_at=ENDED_AT)


def _build(**overrides: object) -> pl.DataFrame:
    """Build a run log frame for one successful silver step, with overrides."""
    kwargs: dict[str, object] = {
        "run_id": RUN_ID,
        "pipeline_name": "jepx_spot_price",
        "target_scope": "fiscal_year=2026",
        "results": [
            _timed(
                PipelineStepResult(
                    name="bronze_to_silver",
                    status=STATUS_SUCCESS,
                    detail="written=48",
                    expected_row_count=48,
                    actual_row_count=48,
                )
            )
        ],
    }
    kwargs.update(overrides)
    return build_run_log_frame(**kwargs)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Shape: one row per step, keyed by run_id + step_seq
# ---------------------------------------------------------------------------


def test_builds_one_row_per_step() -> None:
    # Arrange
    results = [
        _timed(PipelineStepResult("source_to_raw", STATUS_SUCCESS, "saved")),
        _timed(PipelineStepResult("raw_to_bronze", STATUS_SUCCESS, "rows=48")),
        _timed(PipelineStepResult("bronze_to_silver", STATUS_SUCCESS, "written=48")),
    ]

    # Act
    frame = _build(results=results)

    # Assert
    assert frame.height == 3
    assert frame["step_name"].to_list() == [
        "source_to_raw",
        "raw_to_bronze",
        "bronze_to_silver",
    ]


def test_step_seq_numbers_the_steps_from_one_in_execution_order() -> None:
    """The steps carry no timestamp of their own ordering, so seq is the key."""
    # Arrange
    results = [
        _timed(PipelineStepResult("source_to_raw", STATUS_SKIPPED, "no change")),
        _timed(PipelineStepResult("raw_to_bronze", STATUS_SUCCESS, "rows=48")),
    ]

    # Act
    frame = _build(results=results)

    # Assert
    assert frame["step_seq"].to_list() == [1, 2]


def test_every_row_carries_the_same_run_id() -> None:
    """run_id is what groups a run's steps back together and joins to silver."""
    # Arrange
    results = [
        _timed(PipelineStepResult("source_to_raw", STATUS_SUCCESS, "saved")),
        _timed(PipelineStepResult("raw_to_bronze", STATUS_SUCCESS, "rows=48")),
    ]

    # Act
    frame = _build(results=results)

    # Assert
    assert frame["run_id"].to_list() == [RUN_ID, RUN_ID]


def test_pipeline_name_and_target_scope_are_denormalized_onto_every_row() -> None:
    """Step grain means run-level context has to repeat, or a GROUP BY loses it."""
    # Act
    frame = _build(pipeline_name="occto_unit_generation_actuals", target_scope="d=1")

    # Assert
    assert frame["pipeline_name"].to_list() == ["occto_unit_generation_actuals"]
    assert frame["target_scope"].to_list() == ["d=1"]


# ---------------------------------------------------------------------------
# Column mapping: PipelineStepResult -> run log row
# ---------------------------------------------------------------------------


def test_status_lands_in_step_status_not_status() -> None:
    """`status` is taken by the injected audit column, so the step's own
    status has to use a different name or the schema has two of them."""
    # Act
    frame = _build(
        results=[_timed(PipelineStepResult("bronze_to_silver", STATUS_FAILED, "boom"))]
    )

    # Assert
    assert "status" not in frame.columns
    assert frame["step_status"].to_list() == [STATUS_FAILED]


def test_row_counts_carry_through() -> None:
    # Act
    frame = _build()

    # Assert
    assert frame["expected_row_count"].to_list() == [48]
    assert frame["actual_row_count"].to_list() == [48]


def test_row_counts_stay_null_for_steps_that_move_no_rows() -> None:
    """Scraping and dbt steps must not report a fabricated zero."""
    # Act
    frame = _build(
        results=[_timed(PipelineStepResult("silver_to_gold", STATUS_SKIPPED, "off"))]
    )

    # Assert
    assert frame["expected_row_count"].to_list() == [None]
    assert frame["actual_row_count"].to_list() == [None]


def test_timing_columns_carry_the_stamped_window() -> None:
    # Act
    frame = _build()

    # Assert
    assert frame["started_at"].to_list() == [STARTED_AT]
    assert frame["ended_at"].to_list() == [ENDED_AT]
    assert frame["duration_seconds"].to_list() == [30.0]


def test_untimed_steps_record_null_timings_rather_than_a_zero_duration() -> None:
    """A zero would read as an instant step instead of an unrecorded one."""
    # Act
    frame = _build(
        results=[PipelineStepResult("silver_to_gold", STATUS_SKIPPED, "off")]
    )

    # Assert
    assert frame["started_at"].to_list() == [None]
    assert frame["ended_at"].to_list() == [None]
    assert frame["duration_seconds"].to_list() == [None]


def test_logged_date_is_the_partition_key_and_defaults_to_today() -> None:
    # Act
    frame = _build(logged_at=datetime(2026, 8, 22, 23, 59, tzinfo=UTC))

    # Assert
    assert frame["logged_date"].to_list() == [date(2026, 8, 22)]


# ---------------------------------------------------------------------------
# Typing: the frame is cast into an Iceberg schema, so dtypes are not incidental
# ---------------------------------------------------------------------------


def test_column_dtypes_match_the_iceberg_schema() -> None:
    """A wrong dtype only surfaces at append time against a live catalog."""
    # Act
    frame = _build()

    # Assert
    assert frame.schema["run_id"] == pl.Utf8
    assert frame.schema["step_seq"] == pl.Int32
    assert frame.schema["expected_row_count"] == pl.Int64
    assert frame.schema["duration_seconds"] == pl.Float64
    assert frame.schema["logged_date"] == pl.Date


def test_frame_column_order_matches_the_schema_csv() -> None:
    """Keeping the order stable makes the cast-to-Iceberg diff readable."""
    # Act
    frame = _build()

    # Assert
    assert frame.columns == [
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
    ]


# ---------------------------------------------------------------------------
# Guard rails
# ---------------------------------------------------------------------------


def test_empty_results_raise_rather_than_writing_nothing_silently() -> None:
    """An orchestrator always produces steps; an empty list is a caller bug."""
    # Act / Assert
    with pytest.raises(ValueError, match="no pipeline steps"):
        _build(results=[])


# ---------------------------------------------------------------------------
# Audit columns — the frame is cast against the Iceberg schema, which injects
# five audit fields the business frame knows nothing about
# ---------------------------------------------------------------------------


def test_write_frame_carries_every_column_the_iceberg_schema_declares() -> None:
    """A missing audit column only fails at append time against a live catalog.

    ``add_metadata()`` supplies three of the five injected audit fields;
    ``source_data`` and ``status`` are set by each writer, so the run log has
    to set them too or the cast raises on field names not matching.
    """
    # Arrange
    schema = build_table_schema(RUN_LOG_SCHEMA_PATH)
    expected = {field.name for field in schema.fields}

    # Act
    frame = build_run_log_write_frame(
        run_id=RUN_ID,
        pipeline_name="jepx_spot_price",
        target_scope="fiscal_year=2026",
        results=[_timed(PipelineStepResult("bronze_to_silver", STATUS_SUCCESS, "ok"))],
    )

    # Assert
    assert set(frame.columns) == expected


def test_write_frame_stamps_the_audit_execution_id_with_the_run_id() -> None:
    """Both columns name the same run, so a query can reach for either."""
    # Act
    frame = build_run_log_write_frame(
        run_id=RUN_ID,
        pipeline_name="jepx_spot_price",
        target_scope="fiscal_year=2026",
        results=[_timed(PipelineStepResult("bronze_to_silver", STATUS_SUCCESS, "ok"))],
    )

    # Assert
    assert frame["execution_id"].to_list() == [RUN_ID]


def test_write_frame_marks_rows_new_because_the_log_is_append_only() -> None:
    """Nothing here is ever replaced, so no row can be an update."""
    # Act
    frame = build_run_log_write_frame(
        run_id=RUN_ID,
        pipeline_name="jepx_spot_price",
        target_scope="fiscal_year=2026",
        results=[_timed(PipelineStepResult("bronze_to_silver", STATUS_SUCCESS, "ok"))],
    )

    # Assert
    assert frame["status"].to_list() == ["new"]

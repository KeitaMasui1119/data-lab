"""Unit tests for the shared pipeline step result helpers.

``verify_silver_row_counts()`` is what stops a bronze-to-silver step from
reporting success while silver goes unwritten (docs/tasks/tasks.md section
8.4). Both orchestrators call it, so it is tested here rather than twice
over through each of them.
"""

from __future__ import annotations

import pytest

from orchestration.pipeline_result import (
    STATUS_FAILED,
    STATUS_SKIPPED,
    STATUS_SUCCESS,
    PipelineStepResult,
    has_failed_step,
    verify_silver_row_counts,
)


def _verify(**overrides: object) -> tuple[str, str]:
    """Verify a run whose counts all agree, with per-test overrides."""
    kwargs: dict[str, object] = {
        "staged_row_count": 10,
        "expected_row_count": 10,
        "actual_row_count": 10,
        "target_description": "silver.example",
    }
    kwargs.update(overrides)
    return verify_silver_row_counts(**kwargs)  # pyright: ignore[reportArgumentType]


def test_agreeing_counts_are_a_success() -> None:
    # Arrange / Act
    status, reason = _verify()

    # Assert
    assert status == STATUS_SUCCESS
    assert reason == ""


def test_nothing_staged_is_a_failure() -> None:
    """dropped=0 and written=0 with an empty staging relation is the silent no-op."""
    # Arrange / Act
    status, reason = _verify(
        staged_row_count=0, expected_row_count=0, actual_row_count=0
    )

    # Assert
    assert status == STATUS_FAILED
    assert "no bronze rows" in reason


def test_every_staged_row_dropped_is_a_failure() -> None:
    """Nothing is written, so the target window keeps the previous run's rows."""
    # Arrange / Act
    status, reason = _verify(expected_row_count=0, actual_row_count=0)

    # Assert
    assert status == STATUS_FAILED
    assert "all 10 staged rows failed validation" in reason


@pytest.mark.parametrize("actual", [9, 11, None])
def test_row_count_divergence_is_a_failure(actual: int | None) -> None:
    """Writing fewer or more rows than staged both mean rows went astray."""
    # Arrange / Act
    status, reason = _verify(actual_row_count=actual)

    # Assert
    assert status == STATUS_FAILED
    assert "expected 10 rows in silver.example" in reason


def test_expected_row_count_may_be_a_multiple_of_the_staged_rows() -> None:
    """An unpivoting transform writes many silver rows per staged row."""
    # Arrange / Act
    status, _ = _verify(
        staged_row_count=3, expected_row_count=144, actual_row_count=144
    )

    # Assert
    assert status == STATUS_SUCCESS


def test_has_failed_step_detects_a_failure_anywhere_in_the_run() -> None:
    # Arrange
    results = [
        PipelineStepResult("source_to_raw", STATUS_SUCCESS, "ok"),
        PipelineStepResult("bronze_to_silver", STATUS_FAILED, "no rows"),
        PipelineStepResult("silver_to_gold", STATUS_SKIPPED, "disabled"),
    ]

    # Act / Assert
    assert has_failed_step(results) is True


def test_has_failed_step_is_false_without_failures() -> None:
    # Arrange
    results = [
        PipelineStepResult("source_to_raw", STATUS_SKIPPED, "no change"),
        PipelineStepResult("bronze_to_silver", STATUS_SUCCESS, "ok"),
    ]

    # Act / Assert
    assert has_failed_step(results) is False


def test_row_counts_default_to_unset_for_steps_that_move_no_rows() -> None:
    """dbt and scraping steps leave both counts empty rather than faking zeros."""
    # Arrange
    result = PipelineStepResult("silver_to_gold", STATUS_SUCCESS, "dbt select=tag:gold")

    # Act / Assert
    assert result.expected_row_count is None
    assert result.actual_row_count is None

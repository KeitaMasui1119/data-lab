"""Shared result type for orchestrated pipeline steps.

Extracted from pl_jepx_spot_price.py: it has nothing JEPX-specific in it, and
pl_occto_unit_generation_actuals.py needs the same shape rather than a copy.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, replace
from datetime import datetime

from common.utils import get_now_utc

STATUS_SUCCESS = "success"
STATUS_SKIPPED = "skipped"
STATUS_FAILED = "failed"


@dataclass(frozen=True)
class PipelineStepResult:
    """Represents the status of a single orchestrated pipeline step.

    A step that moves rows also records how many it expected to move against
    how many it observed. Both JEPX backfill incidents ended in a step that
    logged success while silver went unwritten, so a step that cannot show
    those two numbers agreeing has no basis to call itself successful. Steps
    that move no rows (dbt, scraping) leave both counts unset.

    ``started_at`` / ``ended_at`` are stamped by the orchestrator around the
    call rather than set by the step itself, so a step that returns from
    several places does not have to remember to time each of them. They stay
    unset for a result nobody timed: the run log records that as null instead
    of a zero duration, which would read as an instant step.
    """

    name: str
    status: str
    detail: str
    expected_row_count: int | None = None
    actual_row_count: int | None = None
    started_at: datetime | None = None
    ended_at: datetime | None = None

    @property
    def duration_seconds(self) -> float | None:
        """Wall-clock seconds the step took, or None when it was not timed."""
        if self.started_at is None or self.ended_at is None:
            return None
        return (self.ended_at - self.started_at).total_seconds()


def stamp_step_timing(
    result: PipelineStepResult,
    *,
    started_at: datetime,
    ended_at: datetime | None = None,
) -> PipelineStepResult:
    """Return a copy of a step result carrying its wall-clock window.

    The caller notes ``started_at`` before invoking the step and lets this
    close the window, which keeps the timing at the call site even for the
    steps that return a tuple rather than a bare result.
    """
    return replace(result, started_at=started_at, ended_at=ended_at or get_now_utc())


def has_failed_step(results: Iterable[PipelineStepResult]) -> bool:
    """Report whether any step failed, so a caller can exit non-zero."""
    return any(result.status == STATUS_FAILED for result in results)


def verify_silver_row_counts(
    *,
    staged_row_count: int,
    expected_row_count: int,
    actual_row_count: int | None,
    target_description: str,
) -> tuple[str, str]:
    """Judge a bronze-to-silver run from its row counts.

    Returns the step's status and, when it failed, the reason. Both silver
    transforms stage rows, drop the ones that fail validation, and replace a
    window with what is left, so all three ways of quietly writing nothing
    look the same from the outside:

    - nothing was staged. Bronze held no row in scope, which means either a
      scope that was never ingested or a date column that stopped parsing --
      the scope filter discards unparseable dates before validation runs, so
      the run reports dropped=0/written=0 and leaves silver untouched. This is
      the shape both JEPX backfill incidents took.
    - every staged row failed validation. Nothing is written, so the target
      window keeps whatever an earlier run left there.
    - fewer (or more) rows reached the table than the staged rows account
      for, meaning rows went missing between the staging relation and the
      write.

    No case is carved out as a legitimate zero: an orchestrated run always
    builds silver over a scope it has just ingested, so bronze necessarily
    holds rows for it, and a standalone rebuild of a scope that was never
    ingested is a mistake worth surfacing rather than reporting as success.
    """
    if staged_row_count == 0:
        return (
            STATUS_FAILED,
            "no bronze rows reached the staging relation, so silver was not "
            "written; check the run's scope and that the date column still parses",
        )
    if expected_row_count == 0:
        return (
            STATUS_FAILED,
            f"all {staged_row_count} staged rows failed validation, so the "
            "target window still holds whatever an earlier run wrote",
        )
    if actual_row_count != expected_row_count:
        return (
            STATUS_FAILED,
            f"expected {expected_row_count} rows in {target_description} but "
            f"wrote {actual_row_count}",
        )
    return STATUS_SUCCESS, ""

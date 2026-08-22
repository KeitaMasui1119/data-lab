"""Regression test for cli/commands/occto.py's orchestrator dispatch wiring.

orchestration/pl_occto_unit_generation_actuals.py used to carry its own
argparse `main()`, a second CLI surface duplicating this module's
--silver-from-date / --silver-all-dates validation (see
docs/tasks/refactaring_20260817.md section 2.10). That module-level main() is
gone now that this module is the sole entry point (CLAUDE.md); the flag
validation it used to cover is already exercised end-to-end via
tests/test_main_cli.py. This pins down the one behaviour that file's
docstring deliberately leaves untested -- dispatch bodies that call into the
pipeline -- for the one place it matters: a failed step has to reach the
shell as a non-zero exit code.
"""

from __future__ import annotations

import pytest

import main as main_module
from cli.commands import occto as occto_commands
from orchestration.pipeline_result import PipelineStepResult

ORCHESTRATOR_ARGV = ["main.py", "run-occto-orchestrator", "--target-date", "2026-08-07"]


def test_handle_orchestrator_exits_nonzero_when_a_step_failed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failed step has to reach the shell, or the run still looks clean."""
    # Arrange
    monkeypatch.setattr(
        occto_commands,
        "run_occto_orchestrated_pipeline",
        lambda **_: [
            PipelineStepResult("source_to_raw", "success", "ok"),
            PipelineStepResult("bronze_to_silver", "failed", "no rows"),
        ],
    )
    monkeypatch.setattr("sys.argv", ORCHESTRATOR_ARGV)

    # Act / Assert
    with pytest.raises(SystemExit) as exit_info:
        main_module.main()

    assert exit_info.value.code == 1


def test_handle_orchestrator_exits_zero_when_every_step_succeeded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The guard fires on failure only, not on any completed run."""
    # Arrange
    monkeypatch.setattr(
        occto_commands,
        "run_occto_orchestrated_pipeline",
        lambda **_: [
            PipelineStepResult("source_to_raw", "skipped", "no change"),
            PipelineStepResult("bronze_to_silver", "success", "ok"),
        ],
    )
    monkeypatch.setattr("sys.argv", ORCHESTRATOR_ARGV)

    # Act / Assert
    main_module.main()

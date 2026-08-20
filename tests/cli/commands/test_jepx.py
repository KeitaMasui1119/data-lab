"""Regression test for cli/commands/jepx.py's orchestrator dispatch wiring.

orchestration/pl_jepx_spot_price.py used to carry its own argparse `main()`,
a second CLI surface duplicating this module's --silver-fiscal-year /
--silver-all-fiscal-years validation (see docs/tasks/refactaring_20260817.md
section 2.10). That module-level main() is gone now that this module is the
sole entry point (CLAUDE.md); the two validation checks it used to cover are
already exercised end-to-end via tests/test_main_cli.py. This pins down the
one behaviour that file's docstring deliberately leaves untested -- dispatch
bodies that call into the pipeline -- for the one place it matters: a failed
step has to reach the shell as a non-zero exit code.
"""

from __future__ import annotations

from pathlib import Path

import pytest

import main as main_module
from cli.commands import jepx as jepx_commands
from orchestration.pipeline_result import PipelineStepResult


def test_handle_orchestrator_exits_nonzero_when_a_step_failed(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """A failed step has to reach the shell, or the run still looks clean."""
    # Arrange
    monkeypatch.setattr(
        jepx_commands,
        "run_jepx_orchestrated_pipeline",
        lambda **_: [
            PipelineStepResult("source_to_raw", "success", "ok"),
            PipelineStepResult("bronze_to_silver", "failed", "no rows"),
        ],
    )
    monkeypatch.setattr(
        "sys.argv",
        [
            "main.py",
            "run-jepx-orchestrator",
            "--dbt-project-dir",
            str(tmp_path),
            "--dbt-profiles-dir",
            str(tmp_path),
        ],
    )

    # Act / Assert
    with pytest.raises(SystemExit) as exit_info:
        main_module.main()

    assert exit_info.value.code == 1

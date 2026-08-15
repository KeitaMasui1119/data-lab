"""Shared result type for orchestrated pipeline steps.

Extracted from pl_jepx_spot_price.py: it has nothing JEPX-specific in it, and
pl_occto_unit_generation_actuals.py needs the same shape rather than a copy.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class PipelineStepResult:
    """Represents the status of a single orchestrated pipeline step."""

    name: str
    status: str
    detail: str

from __future__ import annotations

import importlib.metadata
import sys
import tomllib
from pathlib import Path

import pytest
from packaging.requirements import Requirement
from packaging.version import Version

ROOT = Path(__file__).resolve().parents[1]
PYPROJECT = ROOT / "pyproject.toml"


def _load_required_dependencies() -> list[str]:
    """Load direct runtime and development dependencies from pyproject.toml."""
    with PYPROJECT.open("rb") as handle:
        pyproject = tomllib.load(handle)

    project_dependencies = list(pyproject["project"]["dependencies"])
    dev_dependencies = list(pyproject.get("dependency-groups", {}).get("dev", []))
    return project_dependencies + dev_dependencies


@pytest.mark.parametrize("requirement_text", _load_required_dependencies())
def test_installed_dependency_matches_pyproject(requirement_text: str) -> None:
    """Every declared dependency should be installed at a compatible version."""
    requirement = Requirement(requirement_text)

    if requirement.marker is not None and not requirement.marker.evaluate():
        pytest.skip(
            f"Requirement does not apply in this environment: {requirement_text}"
        )

    installed_version = Version(importlib.metadata.version(requirement.name))

    assert requirement.specifier.contains(
        installed_version,
        prereleases=True,
    ), (
        f"{requirement.name} {installed_version} does not satisfy "
        f"{requirement.specifier or 'an unconstrained requirement'}"
    )


def test_runtime_uses_python_313_or_newer() -> None:
    """The active interpreter should be Python 3.13 or newer."""
    assert sys.version_info >= (3, 13)

"""Task definitions shared by local runs and CI.

The point is that `nox -s lint` here and the `lint` job in ci.yml run the same
command. Before this file existed the CI job held the only copy of each
invocation, so a flag drifted the moment someone typed a shorter version
locally -- and the shorter version is the one that passes.

Sessions reuse the project virtualenv rather than building their own: uv
already resolves and pins everything through uv.lock, and a second environment
per session would install the same packages again from a different resolver.

    uv run nox              # lint, format check, typecheck, tests
    uv run nox -s test      # one session
    uv run nox -s fmt       # rewrite files instead of checking them
"""

from __future__ import annotations

import nox

nox.options.default_venv_backend = "none"
nox.options.sessions = ["lint", "format_check", "typecheck", "test"]

PYTHON_PATHS = ("src/", "tests/")

# Keep in step with the `test` job in .github/workflows/ci.yml. Branch coverage
# is on because an untaken else-path is where this pipeline's bugs live; the
# floor sits just under the current measurement and ratchets up as the untested
# surface shrinks. See docs/reports/comparison_results_20260819.md 10d-1.
COVERAGE_FLOOR = "71"


@nox.session
def lint(session: nox.Session) -> None:
    """Ruff lint, including the S (bandit) security rules."""
    session.run("ruff", "check", *PYTHON_PATHS, external=True)


@nox.session(name="format_check")
def format_check(session: nox.Session) -> None:
    """Fail if anything is unformatted. `fmt` is the one that rewrites."""
    session.run("ruff", "format", "--check", *PYTHON_PATHS, external=True)


@nox.session
def fmt(session: nox.Session) -> None:
    """Rewrite files: format, then apply the fixable lint rules."""
    session.run("ruff", "format", *PYTHON_PATHS, external=True)
    session.run("ruff", "check", "--fix", *PYTHON_PATHS, external=True)


@nox.session
def typecheck(session: nox.Session) -> None:
    """Pyright over the paths pyrightconfig.json names."""
    session.run("pyright", external=True)


@nox.session
def test(session: nox.Session) -> None:
    """Unit tests with the coverage gate. Excludes integration by default.

    Pass positional args through, e.g. `nox -s test -- -k jepx -x`.
    """
    session.run(
        "pytest",
        "-m",
        "not integration",
        "--cov=src",
        "--cov-branch",
        "--cov-report=term-missing",
        f"--cov-fail-under={COVERAGE_FLOOR}",
        *session.posargs,
        external=True,
    )


@nox.session
def integration(session: nox.Session) -> None:
    """The tests CI does not run: RustFS, Iceberg, DuckDB-on-storage.

    Needs the compose stack up and the AWS_* variables set. A green CI run is
    not evidence the pipeline still ingests -- this session is.
    """
    session.run("pytest", "-m", "integration", *session.posargs, external=True)

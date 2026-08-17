from __future__ import annotations

import importlib.util
import sys
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from common.silver_write import SilverWriteResult  # noqa: E402 -- needs SRC on sys.path
from orchestration.pipeline_result import has_failed_step  # noqa: E402 -- same
from pipeline.silver.bronze_to_silver_jepx_spot_price import (  # noqa: E402 -- same
    BronzeToSilverResult,
)

ORCHESTRATOR_PATH = SRC / "orchestration/pl_jepx_spot_price.py"
SPEC = importlib.util.spec_from_file_location("jepx_pipeline", ORCHESTRATOR_PATH)
assert SPEC is not None
assert SPEC.loader is not None
jepx_pipeline = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = jepx_pipeline
SPEC.loader.exec_module(jepx_pipeline)

BRONZE_SCHEMA_PATH = (
    "/workspace/configuration/iceberg/schema/bronze/jepx_spot_price/jepx_spot_price.csv"
)
SILVER_SCHEMA_DIR = "/workspace/configuration/iceberg/schema/silver/jepx_spot_price"
DBT_DIR = Path("/workspace/src/dbt/jepx_power")


def _pipeline_kwargs(**overrides: object) -> dict[str, object]:
    """Build the orchestrator arguments with per-test overrides."""
    kwargs: dict[str, object] = {
        "bucket_name": "jp-power-grid-dev",
        "timestamp_ms": 123,
        "catalog_name": "dlh_dev",
        "bronze_table_identifier": "bronze.jepx_spot_price",
        "bronze_schema_path": BRONZE_SCHEMA_PATH,
        "allow_duplicate_source": False,
        "dbt_project_dir": DBT_DIR,
        "dbt_profiles_dir": DBT_DIR,
        "bronze_location": "s3://jp-power-grid-dev/bronze/jepx_spot_price",
        "silver_schema_dir": SILVER_SCHEMA_DIR,
        "silver_fiscal_year": None,
        "silver_all_fiscal_years": False,
        "run_gold_step": False,
        "gold_select": "tag:gold",
        "dbt_full_refresh": False,
    }
    kwargs.update(overrides)
    return kwargs


def _patch_raw_step(monkeypatch, scraper: object, *, skipped: bool = False) -> None:
    """Stub out everything the source-to-raw step touches."""
    monkeypatch.setattr(jepx_pipeline, "resolve_target_at", lambda _: "target-datetime")
    monkeypatch.setattr(jepx_pipeline, "RustFSClient", lambda: "rustfs-client")
    monkeypatch.setattr(jepx_pipeline, "JEPXSpotSummaryScraper", lambda: scraper)
    monkeypatch.setattr(
        jepx_pipeline,
        "scrape_jepx_spot_price_raw",
        lambda **_: SimpleNamespace(
            skipped=skipped,
            year=2026,
            sha256="abc12345",
            snapshot_prefix=(
                None
                if skipped
                else "raw/jepx/spot_price/year=2026/ingested_at=20260524T010203"
            ),
        ),
    )


def test_run_dbt_step_executes_expected_command(monkeypatch) -> None:
    """run_dbt_step should call subprocess with a dbt run command."""
    calls: list[tuple[list[str], bool]] = []

    def fake_run(command: list[str], check: bool) -> None:
        calls.append((command, check))

    monkeypatch.setattr(jepx_pipeline.subprocess, "run", fake_run)

    result = jepx_pipeline.run_dbt_step(
        step_name="silver_to_gold",
        select_expr="tag:gold",
        project_dir=Path("/tmp/project"),
        profiles_dir=Path("/tmp/profiles"),
        full_refresh=True,
    )

    assert calls == [
        (
            [
                "uv",
                "run",
                "dbt",
                "run",
                "--project-dir",
                "/tmp/project",
                "--profiles-dir",
                "/tmp/profiles",
                "--select",
                "tag:gold",
                "--full-refresh",
            ],
            True,
        )
    ]
    assert result == jepx_pipeline.PipelineStepResult(
        name="silver_to_gold",
        status="success",
        detail="dbt select=tag:gold",
    )


def _transform_result(
    *,
    staged: int,
    dropped: int = 0,
    base_written: int | None = None,
) -> BronzeToSilverResult:
    """Build the transform's result with the row counts a real run reports.

    ``base_written`` defaults to the number of rows that passed validation,
    which is what the base table receives when nothing goes wrong.
    """
    written = staged - dropped if base_written is None else base_written
    return BronzeToSilverResult(
        execution_id="exec-1",
        writes=[
            SilverWriteResult("silver.jepx_spot_price_base", written),
            SilverWriteResult("silver.jepx_spot_price_block", written),
            SilverWriteResult("silver.jepx_spot_price_area", written * 9),
        ],
        dropped_row_count=dropped,
        staged_row_count=staged,
    )


def _run_silver_step(monkeypatch, transform_result: BronzeToSilverResult, **overrides):
    """Run the silver step against a stubbed transform."""
    monkeypatch.setattr(
        jepx_pipeline,
        "run_bronze_to_silver_jepx_spot_price",
        lambda **_: transform_result,
    )
    kwargs: dict[str, object] = {
        "catalog_name": "dlh_dev",
        "bronze_location": "s3://bucket/bronze/jepx_spot_price",
        "silver_schema_dir": SILVER_SCHEMA_DIR,
        "fiscal_year": 2026,
    }
    kwargs.update(overrides)
    return jepx_pipeline.run_bronze_to_silver_step(**kwargs)


def test_run_bronze_to_silver_step_summarizes_written_rows(monkeypatch) -> None:
    """The silver step should total the per-table written row counts."""
    # Arrange
    captured: dict[str, object] = {}

    def fake_run(**kwargs: object) -> object:
        captured.update(kwargs)
        return _transform_result(staged=12, dropped=2)

    monkeypatch.setattr(jepx_pipeline, "run_bronze_to_silver_jepx_spot_price", fake_run)

    # Act
    result = jepx_pipeline.run_bronze_to_silver_step(
        catalog_name="dlh_dev",
        bronze_location="s3://bucket/bronze/jepx_spot_price",
        silver_schema_dir=SILVER_SCHEMA_DIR,
        fiscal_year=2026,
    )

    # Assert
    assert captured["fiscal_year"] == 2026
    assert result.name == "bronze_to_silver"
    assert result.status == "success"
    assert "written=110" in result.detail
    assert "dropped=2" in result.detail


# --- Step result verification (docs/tasks/tasks.md section 8.4) ---------------
#
# Both backfill incidents looked like clean exits. The step now carries the
# row counts it expected against the ones it observed so a run that quietly
# wrote nothing cannot report success.


def test_silver_step_reports_expected_and_actual_row_counts(monkeypatch) -> None:
    """A healthy run records both counts so the numbers are auditable."""
    # Arrange / Act
    result = _run_silver_step(monkeypatch, _transform_result(staged=12, dropped=2))

    # Assert
    assert result.status == "success"
    assert result.expected_row_count == 10
    assert result.actual_row_count == 10


def test_silver_step_fails_when_nothing_reached_the_staging_relation(
    monkeypatch,
) -> None:
    """The silent no-op that hid the backfill incident must not read as success.

    An unparseable delivery_date is discarded by the fiscal year filter before
    validation ever runs, so the step reported dropped=0, written=0 and
    status=success while silver was left untouched.
    """
    # Arrange / Act
    result = _run_silver_step(monkeypatch, _transform_result(staged=0))

    # Assert
    assert result.status == "failed"
    assert result.expected_row_count == 0
    assert result.actual_row_count == 0
    assert "no bronze rows" in result.detail


def test_silver_step_fails_when_every_staged_row_failed_validation(monkeypatch) -> None:
    """Staging rows that all fail validation leave the window holding stale rows."""
    # Arrange / Act
    result = _run_silver_step(monkeypatch, _transform_result(staged=8, dropped=8))

    # Assert
    assert result.status == "failed"
    assert "dropped=8" in result.detail


def test_silver_step_fails_when_written_rows_diverge_from_valid_rows(
    monkeypatch,
) -> None:
    """Losing rows between the staging relation and the table is a failure."""
    # Arrange / Act
    result = _run_silver_step(
        monkeypatch, _transform_result(staged=12, dropped=2, base_written=7)
    )

    # Assert
    assert result.status == "failed"
    assert result.expected_row_count == 10
    assert result.actual_row_count == 7


def test_run_jepx_orchestrated_pipeline_skips_gold_step(monkeypatch) -> None:
    """Pipeline should execute source/raw/bronze/silver and skip gold by default."""

    # Arrange
    class DummyScraper:
        def __init__(self) -> None:
            self.closed = False

        def close(self) -> None:
            self.closed = True

    scraper_instance = DummyScraper()
    dbt_calls: list[dict[str, object]] = []
    ingest_calls: list[dict[str, object]] = []
    silver_calls: list[dict[str, object]] = []

    _patch_raw_step(monkeypatch, scraper_instance)

    def fake_ingest(**kwargs: object) -> int:
        ingest_calls.append(kwargs)
        return 12

    def fake_run_dbt_step(**kwargs: object) -> object:
        dbt_calls.append(kwargs)
        return jepx_pipeline.PipelineStepResult(
            name=str(kwargs["step_name"]),
            status="success",
            detail="ok",
        )

    def fake_silver_step(**kwargs: object) -> object:
        silver_calls.append(kwargs)
        return jepx_pipeline.PipelineStepResult(
            name="bronze_to_silver",
            status="success",
            detail="ok",
        )

    monkeypatch.setattr(jepx_pipeline, "ingest_jepx_spot_summary", fake_ingest)
    monkeypatch.setattr(jepx_pipeline, "run_dbt_step", fake_run_dbt_step)
    monkeypatch.setattr(jepx_pipeline, "run_bronze_to_silver_step", fake_silver_step)

    # Act
    results = jepx_pipeline.run_jepx_orchestrated_pipeline(**_pipeline_kwargs())

    # Assert
    assert scraper_instance.closed is True
    assert len(ingest_calls) == 1
    assert ingest_calls[0]["skip_if_exists"] is True
    assert ingest_calls[0]["use_ingestion_log"] is True
    assert ingest_calls[0]["require_unprocessed"] is True
    assert ingest_calls[0]["fiscal_year"] == 2026

    assert len(silver_calls) == 1
    assert silver_calls[0]["fiscal_year"] == 2026
    assert dbt_calls == []

    assert [result.name for result in results] == [
        "source_to_raw",
        "raw_to_bronze",
        "bronze_to_silver",
        "silver_to_gold",
    ]
    assert results[-1].status == "skipped"


def test_run_jepx_orchestrated_pipeline_runs_gold_step(monkeypatch) -> None:
    """Pipeline should execute the gold step when explicitly enabled."""

    # Arrange
    class DummyScraper:
        def close(self) -> None:
            return None

    dbt_calls: list[dict[str, object]] = []
    ingest_calls: list[dict[str, object]] = []

    _patch_raw_step(monkeypatch, DummyScraper())

    def fake_ingest(**kwargs: object) -> int:
        ingest_calls.append(kwargs)
        return 34

    def fake_run_dbt_step(**kwargs: object) -> object:
        dbt_calls.append(kwargs)
        return jepx_pipeline.PipelineStepResult(
            name=str(kwargs["step_name"]),
            status="success",
            detail="ok",
        )

    monkeypatch.setattr(jepx_pipeline, "ingest_jepx_spot_summary", fake_ingest)
    monkeypatch.setattr(jepx_pipeline, "run_dbt_step", fake_run_dbt_step)
    monkeypatch.setattr(
        jepx_pipeline,
        "run_bronze_to_silver_step",
        lambda **_: jepx_pipeline.PipelineStepResult(
            name="bronze_to_silver",
            status="success",
            detail="ok",
        ),
    )

    # Act
    results = jepx_pipeline.run_jepx_orchestrated_pipeline(
        **_pipeline_kwargs(
            allow_duplicate_source=True,
            run_gold_step=True,
            dbt_full_refresh=True,
            silver_fiscal_year=2026,
        )
    )

    # Assert
    assert len(ingest_calls) == 1
    assert ingest_calls[0]["skip_if_exists"] is False

    assert len(dbt_calls) == 1
    assert dbt_calls[0]["step_name"] == "silver_to_gold"
    assert dbt_calls[0]["full_refresh"] is True

    assert [result.name for result in results] == [
        "source_to_raw",
        "raw_to_bronze",
        "bronze_to_silver",
        "silver_to_gold",
    ]
    assert results[-1].status == "success"


def _run_pipeline_capturing_silver(
    monkeypatch, **overrides: object
) -> dict[str, object]:
    """Run the orchestrator with every step stubbed and return the silver kwargs."""

    class DummyScraper:
        def close(self) -> None:
            return None

    silver_calls: list[dict[str, object]] = []

    _patch_raw_step(monkeypatch, DummyScraper())
    monkeypatch.setattr(jepx_pipeline, "ingest_jepx_spot_summary", lambda **_: 12)

    def fake_silver_step(**kwargs: object) -> object:
        silver_calls.append(kwargs)
        return jepx_pipeline.PipelineStepResult(
            name="bronze_to_silver", status="success", detail="ok"
        )

    monkeypatch.setattr(jepx_pipeline, "run_bronze_to_silver_step", fake_silver_step)

    jepx_pipeline.run_jepx_orchestrated_pipeline(**_pipeline_kwargs(**overrides))

    assert len(silver_calls) == 1
    return silver_calls[0]


def test_silver_step_defaults_to_the_fiscal_year_being_processed(monkeypatch) -> None:
    """A daily run must scope silver to its own fiscal year, not every year.

    Rebuilding every fiscal year on each run is what made the silver step
    unusable once the tables held two decades of data.
    """
    # Arrange / Act
    silver_kwargs = _run_pipeline_capturing_silver(monkeypatch)

    # Assert
    assert silver_kwargs["fiscal_year"] == 2026


def test_silver_step_honours_an_explicit_fiscal_year(monkeypatch) -> None:
    """An explicit fiscal year overrides the snapshot's own year."""
    # Arrange / Act
    silver_kwargs = _run_pipeline_capturing_silver(monkeypatch, silver_fiscal_year=2020)

    # Assert
    assert silver_kwargs["fiscal_year"] == 2020


def test_silver_step_can_rebuild_every_fiscal_year_on_request(monkeypatch) -> None:
    """Full refresh stays available, but only when asked for explicitly."""
    # Arrange / Act
    silver_kwargs = _run_pipeline_capturing_silver(
        monkeypatch, silver_all_fiscal_years=True
    )

    # Assert
    assert silver_kwargs["fiscal_year"] is None


def test_run_jepx_orchestrated_pipeline_skips_raw_to_bronze_when_no_unprocessed(
    monkeypatch,
) -> None:
    """Pipeline should continue when bronze step has no unprocessed latest snapshot."""

    # Arrange
    class DummyScraper:
        def close(self) -> None:
            return None

    silver_calls: list[dict[str, object]] = []

    _patch_raw_step(monkeypatch, DummyScraper(), skipped=True)

    def fake_ingest(**_: object) -> int:
        raise ValueError("No unprocessed latest snapshot found in ingestion log")

    def fake_silver_step(**kwargs: object) -> object:
        silver_calls.append(kwargs)
        return jepx_pipeline.PipelineStepResult(
            name="bronze_to_silver",
            status="success",
            detail="ok",
        )

    monkeypatch.setattr(jepx_pipeline, "ingest_jepx_spot_summary", fake_ingest)
    monkeypatch.setattr(jepx_pipeline, "run_bronze_to_silver_step", fake_silver_step)

    # Act
    results = jepx_pipeline.run_jepx_orchestrated_pipeline(**_pipeline_kwargs())

    # Assert
    assert results[0].name == "source_to_raw"
    assert results[0].status == "skipped"
    assert results[1].name == "raw_to_bronze"
    assert results[1].status == "skipped"
    assert len(silver_calls) == 1


# --- The module's own CLI entrypoint ------------------------------------------
#
# src/cli/commands/jepx.py carries a second copy of the silver-scope validation
# for `python src/main.py run-jepx-orchestrator`; tests/test_main_cli.py covers
# that one. These cover running this module directly, which is the path
# docs/tasks/tasks.md section 8.2 lists as untested.


def _run_module_main(
    monkeypatch, argv: list[str], results: list[object], dbt_dir: Path
) -> None:
    """Invoke the module's main() with every external system stubbed out.

    ``main()`` calls parser.error() when the dbt directories do not exist,
    and its defaults point at /workspace, which only exists in the dev
    container. Pointing them at a real directory keeps these tests about the
    checks they are named for rather than about where the repo is checked out.
    """
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "pl_jepx_spot_price.py",
            "--dbt-project-dir",
            str(dbt_dir),
            "--dbt-profiles-dir",
            str(dbt_dir),
            *argv,
        ],
    )
    monkeypatch.setattr(
        jepx_pipeline, "run_jepx_orchestrated_pipeline", lambda **_: results
    )


def test_main_rejects_both_silver_scope_flags(monkeypatch, tmp_path: Path) -> None:
    """--silver-all-fiscal-years would discard the year --silver-fiscal-year names."""
    # Arrange
    pipeline_calls: list[dict[str, object]] = []

    _run_module_main(
        monkeypatch,
        ["--silver-all-fiscal-years", "--silver-fiscal-year", "2026"],
        [],
        tmp_path,
    )
    monkeypatch.setattr(
        jepx_pipeline,
        "run_jepx_orchestrated_pipeline",
        lambda **kwargs: pipeline_calls.append(kwargs),
    )

    # Act / Assert
    with pytest.raises(SystemExit) as exit_info:
        jepx_pipeline.main()

    assert exit_info.value.code == 2  # argparse's usage-error exit code
    assert pipeline_calls == []


@pytest.mark.parametrize(
    "argv",
    [
        pytest.param(["--silver-all-fiscal-years"], id="all-fiscal-years"),
        pytest.param(["--silver-fiscal-year", "2026"], id="one-fiscal-year"),
        pytest.param([], id="neither"),
    ],
)
def test_main_accepts_either_silver_scope_flag_alone(
    monkeypatch, argv, tmp_path: Path
) -> None:
    """The guard rejects the combination only, not each flag on its own."""
    # Arrange
    _run_module_main(
        monkeypatch,
        argv,
        [jepx_pipeline.PipelineStepResult("bronze_to_silver", "success", "ok")],
        tmp_path,
    )

    # Act / Assert
    jepx_pipeline.main()


def test_main_exits_nonzero_when_a_step_failed(monkeypatch, tmp_path: Path) -> None:
    """A failed step has to reach the shell, or the run still looks clean."""
    # Arrange
    _run_module_main(
        monkeypatch,
        [],
        [
            jepx_pipeline.PipelineStepResult("source_to_raw", "success", "ok"),
            jepx_pipeline.PipelineStepResult("bronze_to_silver", "failed", "no rows"),
        ],
        tmp_path,
    )

    # Act / Assert
    with pytest.raises(SystemExit) as exit_info:
        jepx_pipeline.main()

    assert exit_info.value.code == 1


# --- Fiscal-year backfill (docs/tasks/tasks.md section 8.5) -------------------
#
# The FY2005-FY2026 backfill ran as a throwaway shell loop, so the rebuild
# procedure docs/architecture/replay_strategy.md defines was not executable.
# These pin down the loop: one raw+bronze pass per fiscal year, then a single
# silver rebuild covering every year.


def _backfill_kwargs(**overrides: object) -> dict[str, object]:
    """Build the backfill arguments with per-test overrides."""
    kwargs: dict[str, object] = {
        "bucket_name": "jp-power-grid-dev",
        "from_fiscal_year": 2005,
        "to_fiscal_year": 2007,
        "catalog_name": "dlh_dev",
        "bronze_table_identifier": "bronze.jepx_spot_price",
        "bronze_schema_path": BRONZE_SCHEMA_PATH,
        "allow_duplicate_source": False,
        "bronze_location": "s3://jp-power-grid-dev/bronze/jepx_spot_price",
        "silver_schema_dir": SILVER_SCHEMA_DIR,
        "from_raw": False,
        "request_delay_seconds": 3.0,
    }
    kwargs.update(overrides)
    return kwargs


class _RecordingScraper:
    """Scraper stand-in that records whether it was closed."""

    instances: list[_RecordingScraper] = []

    def __init__(self) -> None:
        self.closed = False
        _RecordingScraper.instances.append(self)

    def close(self) -> None:
        self.closed = True


@pytest.fixture
def backfill_calls(monkeypatch):
    """Stub every step the backfill drives and record how it called them."""
    _RecordingScraper.instances = []
    calls: dict[str, list] = {"raw": [], "bronze": [], "silver": [], "sleep": []}

    monkeypatch.setattr(jepx_pipeline, "RustFSClient", lambda: "rustfs-client")
    monkeypatch.setattr(jepx_pipeline, "JEPXSpotSummaryScraper", _RecordingScraper)
    monkeypatch.setattr(jepx_pipeline.time, "sleep", lambda s: calls["sleep"].append(s))

    def fake_raw_step(*, target_at: datetime, **kwargs: object):
        calls["raw"].append({"target_at": target_at, **kwargs})
        return (
            jepx_pipeline.PipelineStepResult("source_to_raw", "success", "ok"),
            # Every backfill target is April 1, so the year names the fiscal year.
            target_at.year,
        )

    def fake_bronze_step(**kwargs: object):
        calls["bronze"].append(kwargs)
        return jepx_pipeline.PipelineStepResult("raw_to_bronze", "success", "ok")

    def fake_silver_step(**kwargs: object):
        calls["silver"].append(kwargs)
        return jepx_pipeline.PipelineStepResult("bronze_to_silver", "success", "ok")

    monkeypatch.setattr(jepx_pipeline, "run_source_to_raw_step", fake_raw_step)
    monkeypatch.setattr(jepx_pipeline, "run_raw_to_bronze_step", fake_bronze_step)
    monkeypatch.setattr(jepx_pipeline, "run_bronze_to_silver_step", fake_silver_step)
    return calls


def test_backfill_runs_one_raw_and_bronze_pass_per_fiscal_year(
    monkeypatch, backfill_calls
) -> None:
    """Every year in the range is scraped and ingested exactly once."""
    # Arrange / Act
    jepx_pipeline.run_jepx_backfill_pipeline(**_backfill_kwargs())

    # Assert
    scraped_years = [call["target_at"].year for call in backfill_calls["raw"]]
    assert scraped_years == [2005, 2006, 2007]
    assert [call["fiscal_year"] for call in backfill_calls["bronze"]] == [
        2005,
        2006,
        2007,
    ]


def test_backfill_targets_the_first_day_of_each_fiscal_year(
    monkeypatch, backfill_calls
) -> None:
    """The scrape target follows scrape-jepx --fiscal-year's own convention."""
    # Arrange / Act
    jepx_pipeline.run_jepx_backfill_pipeline(**_backfill_kwargs(to_fiscal_year=2005))

    # Assert
    assert backfill_calls["raw"][0]["target_at"] == datetime(2005, 4, 1, tzinfo=UTC)


def test_backfill_builds_silver_once_for_every_fiscal_year(
    monkeypatch, backfill_calls
) -> None:
    """Silver runs once at the end, not per year.

    A scoped run re-scans the whole bronze table, so 22 of them would repeat
    that scan 22 times; one unscoped pass rebuilds every year in a single scan.
    """
    # Arrange / Act
    results = jepx_pipeline.run_jepx_backfill_pipeline(**_backfill_kwargs())

    # Assert
    assert len(backfill_calls["silver"]) == 1
    assert backfill_calls["silver"][0]["fiscal_year"] is None
    assert [result.name for result in results][-1] == "bronze_to_silver"


def test_backfill_waits_between_years_but_not_after_the_last(
    monkeypatch, backfill_calls
) -> None:
    """Three years means two waits; the source is hit once per year."""
    # Arrange / Act
    jepx_pipeline.run_jepx_backfill_pipeline(**_backfill_kwargs())

    # Assert
    assert backfill_calls["sleep"] == [3.0, 3.0]


def test_backfill_from_raw_never_touches_the_source(
    monkeypatch, backfill_calls
) -> None:
    """Replaying from raw must not scrape, wait, or open a session."""
    # Arrange / Act
    results = jepx_pipeline.run_jepx_backfill_pipeline(
        **_backfill_kwargs(from_raw=True)
    )

    # Assert
    assert backfill_calls["raw"] == []
    assert backfill_calls["sleep"] == []
    assert _RecordingScraper.instances == []
    assert [call["fiscal_year"] for call in backfill_calls["bronze"]] == [
        2005,
        2006,
        2007,
    ]
    assert "source_to_raw" not in [result.name for result in results]


def test_backfill_from_raw_reingests_already_processed_snapshots(
    monkeypatch, backfill_calls
) -> None:
    """The ingestion log marks replayed snapshots processed, so the filter must go.

    With require_unprocessed left on, every already-ingested year resolves to
    no snapshot and the whole replay silently does nothing.
    """
    # Arrange / Act
    jepx_pipeline.run_jepx_backfill_pipeline(**_backfill_kwargs(from_raw=True))

    # Assert
    assert all(
        call["require_unprocessed"] is False for call in backfill_calls["bronze"]
    )


def test_backfill_keeps_require_unprocessed_when_scraping(
    monkeypatch, backfill_calls
) -> None:
    """A scraping run has just written a fresh, unprocessed snapshot per year."""
    # Arrange / Act
    jepx_pipeline.run_jepx_backfill_pipeline(**_backfill_kwargs())

    # Assert
    assert all(call["require_unprocessed"] is True for call in backfill_calls["bronze"])


def test_backfill_continues_after_a_year_fails(monkeypatch, backfill_calls) -> None:
    """One bad year must not cost the other twenty-one."""

    # Arrange
    def failing_bronze_step(**kwargs: object):
        if kwargs["fiscal_year"] == 2006:
            raise RuntimeError("bronze exploded")
        return jepx_pipeline.PipelineStepResult("raw_to_bronze", "success", "ok")

    monkeypatch.setattr(jepx_pipeline, "run_raw_to_bronze_step", failing_bronze_step)

    # Act
    results = jepx_pipeline.run_jepx_backfill_pipeline(**_backfill_kwargs())

    # Assert
    failed = [result for result in results if result.status == "failed"]
    assert len(failed) == 1
    assert "2006" in failed[0].detail
    assert len(backfill_calls["raw"]) == 3  # the loop kept going
    assert len(backfill_calls["silver"]) == 1
    assert has_failed_step(results) is True


def test_backfill_closes_the_scraper_even_when_a_year_fails(
    monkeypatch, backfill_calls
) -> None:
    """The session is opened once for the whole range and always closed."""

    # Arrange
    def failing_raw_step(**kwargs: object):
        raise RuntimeError("scrape exploded")

    monkeypatch.setattr(jepx_pipeline, "run_source_to_raw_step", failing_raw_step)

    # Act
    jepx_pipeline.run_jepx_backfill_pipeline(**_backfill_kwargs())

    # Assert
    assert len(_RecordingScraper.instances) == 1  # one session for every year
    assert _RecordingScraper.instances[0].closed is True


def test_backfill_rejects_a_reversed_fiscal_year_range() -> None:
    """to < from would silently loop zero times."""
    # Arrange / Act / Assert
    with pytest.raises(ValueError, match="to_fiscal_year"):
        jepx_pipeline.run_jepx_backfill_pipeline(
            **_backfill_kwargs(from_fiscal_year=2026, to_fiscal_year=2005)
        )

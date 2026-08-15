from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

ORCHESTRATOR_PATH = SRC / "pipeline/orchestrator/jepx_pipeline.py"
if not ORCHESTRATOR_PATH.exists():
    ORCHESTRATOR_PATH = SRC / "orchestration/jepx_pipeline.py"
SPEC = importlib.util.spec_from_file_location("jepx_pipeline", ORCHESTRATOR_PATH)
assert SPEC is not None
assert SPEC.loader is not None
jepx_pipeline = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = jepx_pipeline
SPEC.loader.exec_module(jepx_pipeline)

BRONZE_SCHEMA_PATH = (
    "/workspace/configuration/iceberg/schema/bronze/jepx_spot_price.csv"
)
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
        "silver_schema_dir": "/workspace/configuration/iceberg/schema/silver",
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


def test_run_bronze_to_silver_step_summarizes_written_rows(monkeypatch) -> None:
    """The silver step should total the per-table written row counts."""
    # Arrange
    captured: dict[str, object] = {}

    def fake_run(**kwargs: object) -> object:
        captured.update(kwargs)
        return SimpleNamespace(
            execution_id="exec-1",
            dropped_row_count=2,
            writes=[
                SimpleNamespace(rows_written=10),
                SimpleNamespace(rows_written=20),
            ],
        )

    monkeypatch.setattr(jepx_pipeline, "run_bronze_to_silver_jepx_spot_price", fake_run)

    # Act
    result = jepx_pipeline.run_bronze_to_silver_step(
        catalog_name="dlh_dev",
        bronze_location="s3://bucket/bronze/jepx_spot_price",
        silver_schema_dir="/workspace/configuration/iceberg/schema/silver",
        fiscal_year=2026,
    )

    # Assert
    assert captured["fiscal_year"] == 2026
    assert result.name == "bronze_to_silver"
    assert result.status == "success"
    assert "written=30" in result.detail
    assert "dropped=2" in result.detail


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

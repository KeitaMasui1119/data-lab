from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

ORCHESTRATOR_PATH = SRC / "pipeline/orchestrator/jepx_pipeline.py"
SPEC = importlib.util.spec_from_file_location("jepx_pipeline", ORCHESTRATOR_PATH)
assert SPEC is not None
assert SPEC.loader is not None
jepx_pipeline = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = jepx_pipeline
SPEC.loader.exec_module(jepx_pipeline)


def test_run_dbt_step_executes_expected_command(monkeypatch) -> None:
    """run_dbt_step should call subprocess with a dbt run command."""
    calls: list[tuple[list[str], bool]] = []

    def fake_run(command: list[str], check: bool) -> None:
        calls.append((command, check))

    monkeypatch.setattr(jepx_pipeline.subprocess, "run", fake_run)

    result = jepx_pipeline.run_dbt_step(
        step_name="silver_build",
        select_expr="tag:silver",
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
                "tag:silver",
                "--full-refresh",
            ],
            True,
        )
    ]
    assert result == jepx_pipeline.PipelineStepResult(
        name="silver_build",
        status="success",
        detail="dbt select=tag:silver",
    )


def test_run_jepx_orchestrated_pipeline_skips_gold_step(monkeypatch) -> None:
    """Pipeline should execute source/raw/bronze/silver and skip gold by default."""

    class DummyScraper:
        def __init__(self) -> None:
            self.closed = False

        def close(self) -> None:
            self.closed = True

    scraper_instance = DummyScraper()
    dbt_calls: list[dict[str, object]] = []
    ingest_calls: list[dict[str, object]] = []

    monkeypatch.setattr(jepx_pipeline, "resolve_target_at", lambda _: "target-datetime")
    monkeypatch.setattr(jepx_pipeline, "RustFSClient", lambda: "rustfs-client")
    monkeypatch.setattr(
        jepx_pipeline, "JEPXSpotSummaryScraper", lambda: scraper_instance
    )
    monkeypatch.setattr(
        jepx_pipeline,
        "scrape_jepx_to_rustfs",
        lambda **_: SimpleNamespace(
            bucket_name="jp-power-grid-dev",
            object_key="raw/jepx/spot_summary/spot_summary_2026.csv",
            file_name="spot_summary_2026.csv",
            size_bytes=100,
        ),
    )

    def fake_ingest(**kwargs: object) -> int:
        ingest_calls.append(kwargs)
        return 12

    def fake_run_dbt_step(**kwargs: object) -> jepx_pipeline.PipelineStepResult:
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
        "export_jepx_silver_to_iceberg",
        lambda **_: jepx_pipeline.PipelineStepResult(
            name="silver_to_iceberg",
            status="success",
            detail="ok",
        ),
    )

    results = jepx_pipeline.run_jepx_orchestrated_pipeline(
        bucket_name="jp-power-grid-dev",
        timestamp_ms=123,
        catalog_name="dlh_dev",
        bronze_table_identifier="bronze.jepx_spot_price",
        bronze_schema_path="/workspace/configuration/iceberg/schema/bronze/jepx_spot_price.csv",
        allow_duplicate_source=False,
        dbt_project_dir=Path("/workspace/src/dbt/jepx_power"),
        dbt_profiles_dir=Path("/workspace/src/dbt/jepx_power"),
        staging_select="tag:staging",
        silver_select="tag:silver",
        run_gold_step=False,
        gold_select="tag:gold",
        dbt_full_refresh=False,
        export_silver_to_iceberg=False,
        dbt_duckdb_path=Path("/workspace/src/dbt/jepx_power/jepx_power.duckdb"),
    )

    assert scraper_instance.closed is True
    assert len(ingest_calls) == 1
    assert ingest_calls[0]["skip_if_exists"] is True

    assert len(dbt_calls) == 2
    assert [call["step_name"] for call in dbt_calls] == [
        "bronze_to_silver_staging",
        "silver_build",
    ]

    assert [result.name for result in results] == [
        "source_to_raw",
        "raw_to_bronze",
        "bronze_to_silver_staging",
        "silver_build",
        "silver_to_iceberg",
        "silver_to_gold",
    ]
    assert results[-2].status == "skipped"
    assert results[-1].status == "skipped"


def test_run_jepx_orchestrated_pipeline_runs_gold_step(monkeypatch) -> None:
    """Pipeline should execute the gold step when explicitly enabled."""

    class DummyScraper:
        def close(self) -> None:
            return None

    dbt_calls: list[dict[str, object]] = []
    ingest_calls: list[dict[str, object]] = []

    monkeypatch.setattr(jepx_pipeline, "resolve_target_at", lambda _: "target-datetime")
    monkeypatch.setattr(jepx_pipeline, "RustFSClient", lambda: "rustfs-client")
    monkeypatch.setattr(jepx_pipeline, "JEPXSpotSummaryScraper", lambda: DummyScraper())
    monkeypatch.setattr(
        jepx_pipeline,
        "scrape_jepx_to_rustfs",
        lambda **_: SimpleNamespace(
            bucket_name="jp-power-grid-dev",
            object_key="raw/jepx/spot_summary/spot_summary_2026.csv",
            file_name="spot_summary_2026.csv",
            size_bytes=100,
        ),
    )

    def fake_ingest(**kwargs: object) -> int:
        ingest_calls.append(kwargs)
        return 34

    def fake_run_dbt_step(**kwargs: object) -> jepx_pipeline.PipelineStepResult:
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
        "export_jepx_silver_to_iceberg",
        lambda **_: jepx_pipeline.PipelineStepResult(
            name="silver_to_iceberg",
            status="success",
            detail="ok",
        ),
    )

    results = jepx_pipeline.run_jepx_orchestrated_pipeline(
        bucket_name="jp-power-grid-dev",
        timestamp_ms=123,
        catalog_name="dlh_dev",
        bronze_table_identifier="bronze.jepx_spot_price",
        bronze_schema_path="/workspace/configuration/iceberg/schema/bronze/jepx_spot_price.csv",
        allow_duplicate_source=True,
        dbt_project_dir=Path("/workspace/src/dbt/jepx_power"),
        dbt_profiles_dir=Path("/workspace/src/dbt/jepx_power"),
        staging_select="tag:staging",
        silver_select="tag:silver",
        run_gold_step=True,
        gold_select="tag:gold",
        dbt_full_refresh=True,
        export_silver_to_iceberg=True,
        dbt_duckdb_path=Path("/workspace/src/dbt/jepx_power/jepx_power.duckdb"),
    )

    assert len(ingest_calls) == 1
    assert ingest_calls[0]["skip_if_exists"] is False

    assert len(dbt_calls) == 3
    assert [call["step_name"] for call in dbt_calls] == [
        "bronze_to_silver_staging",
        "silver_build",
        "silver_to_gold",
    ]
    assert [call["full_refresh"] for call in dbt_calls] == [True, True, True]

    assert [result.name for result in results] == [
        "source_to_raw",
        "raw_to_bronze",
        "bronze_to_silver_staging",
        "silver_build",
        "silver_to_iceberg",
        "silver_to_gold",
    ]
    assert results[-2].status == "success"
    assert results[-1].status == "success"

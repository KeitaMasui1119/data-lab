from __future__ import annotations

from datetime import date
from types import SimpleNamespace
from typing import Any

from orchestration import pl_occto_unit_generation_actuals as occto_pipeline

BRONZE_SCHEMA_PATH = (
    "/workspace/configuration/iceberg/schema/bronze/occto_unit_generation_actuals/"
    "occto_unit_generation_actuals.csv"
)
SILVER_SCHEMA_DIR = (
    "/workspace/configuration/iceberg/schema/silver/occto_unit_generation_actuals"
)


def _pipeline_kwargs(**overrides: Any) -> dict[str, Any]:
    """Build the orchestrator arguments with per-test overrides."""
    kwargs: dict[str, Any] = {
        "bucket_name": "jp-power-grid-dev",
        "from_date": date(2026, 8, 7),
        "to_date": None,
        "catalog_name": "dlh_dev",
        "bronze_table_identifier": "bronze.occto_unit_generation_actuals",
        "bronze_schema_path": BRONZE_SCHEMA_PATH,
        "allow_duplicate_source": False,
        "bronze_location": "s3://jp-power-grid-dev/bronze/occto_unit_generation_actuals",
        "silver_schema_dir": SILVER_SCHEMA_DIR,
        "silver_from_date": None,
        "silver_to_date": None,
        "silver_all_dates": False,
    }
    kwargs.update(overrides)
    return kwargs


def _patch_raw_step(
    monkeypatch,
    scraper: object,
    *,
    skipped: bool = False,
    from_date: date = date(2026, 8, 7),
    to_date: date = date(2026, 8, 7),
) -> None:
    """Stub out everything the source-to-raw step touches."""
    monkeypatch.setattr(occto_pipeline, "RustFSClient", lambda: "rustfs-client")
    monkeypatch.setattr(occto_pipeline, "OCCTOUnitGenerationScraper", lambda: scraper)
    monkeypatch.setattr(
        occto_pipeline,
        "scrape_occto_unit_generation_raw",
        lambda **_: SimpleNamespace(
            skipped=skipped,
            from_date=from_date,
            to_date=to_date,
            sha256="abc12345",
            snapshot_prefix=(
                None
                if skipped
                else (
                    f"raw/occto/unit_generation/target_date={from_date.isoformat()}/"
                    "ingested_at=20260807T153000"
                )
            ),
        ),
    )


def test_resolve_silver_target_date_window_defaults_to_snapshot_range() -> None:
    """A daily run must scope silver to the range it just ingested."""
    window = occto_pipeline.resolve_silver_target_date_window(
        silver_from_date=None,
        silver_to_date=None,
        silver_all_dates=False,
        snapshot_from_date=date(2026, 8, 7),
        snapshot_to_date=date(2026, 8, 7),
    )

    assert window == (date(2026, 8, 7), date(2026, 8, 7))


def test_resolve_silver_target_date_window_honours_explicit_range() -> None:
    window = occto_pipeline.resolve_silver_target_date_window(
        silver_from_date=date(2026, 1, 1),
        silver_to_date=date(2026, 1, 31),
        silver_all_dates=False,
        snapshot_from_date=date(2026, 8, 7),
        snapshot_to_date=date(2026, 8, 7),
    )

    assert window == (date(2026, 1, 1), date(2026, 1, 31))


def test_resolve_silver_target_date_window_defaults_to_date_range_end() -> None:
    """An explicit --silver-from-date without --silver-to-date is a single day."""
    window = occto_pipeline.resolve_silver_target_date_window(
        silver_from_date=date(2026, 1, 1),
        silver_to_date=None,
        silver_all_dates=False,
        snapshot_from_date=date(2026, 8, 7),
        snapshot_to_date=date(2026, 8, 7),
    )

    assert window == (date(2026, 1, 1), date(2026, 1, 1))


def test_resolve_silver_target_date_window_can_rebuild_every_date() -> None:
    """Full refresh stays available, but only when asked for explicitly."""
    window = occto_pipeline.resolve_silver_target_date_window(
        silver_from_date=None,
        silver_to_date=None,
        silver_all_dates=True,
        snapshot_from_date=date(2026, 8, 7),
        snapshot_to_date=date(2026, 8, 7),
    )

    assert window == (None, None)


def test_run_bronze_to_silver_step_reports_written_and_dropped(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_run(**kwargs: object) -> object:
        captured.update(kwargs)
        return SimpleNamespace(
            execution_id="exec-1",
            dropped_row_count=5,
            write=SimpleNamespace(rows_written=48),
        )

    monkeypatch.setattr(
        occto_pipeline, "run_bronze_to_silver_occto_unit_generation", fake_run
    )

    result = occto_pipeline.run_bronze_to_silver_step(
        catalog_name="dlh_dev",
        bronze_location="s3://bucket/bronze/occto_unit_generation_actuals",
        silver_schema_dir=SILVER_SCHEMA_DIR,
        from_date=date(2026, 8, 7),
        to_date=date(2026, 8, 7),
    )

    assert captured["from_date"] == date(2026, 8, 7)
    assert result.name == "bronze_to_silver"
    assert result.status == "success"
    assert "written=48" in result.detail
    assert "dropped=5" in result.detail


def test_run_occto_orchestrated_pipeline_runs_all_three_steps(monkeypatch) -> None:
    """Pipeline should execute source_to_raw, raw_to_bronze, bronze_to_silver."""

    class DummyScraper:
        def __init__(self) -> None:
            self.closed = False

        def close(self) -> None:
            self.closed = True

    scraper_instance = DummyScraper()
    ingest_calls: list[dict[str, object]] = []
    silver_calls: list[dict[str, object]] = []

    _patch_raw_step(monkeypatch, scraper_instance)

    def fake_ingest(**kwargs: object) -> int:
        ingest_calls.append(kwargs)
        return 471

    def fake_silver_step(**kwargs: object) -> object:
        silver_calls.append(kwargs)
        return occto_pipeline.PipelineStepResult(
            name="bronze_to_silver", status="success", detail="ok"
        )

    monkeypatch.setattr(occto_pipeline, "ingest_occto_unit_generation", fake_ingest)
    monkeypatch.setattr(occto_pipeline, "run_bronze_to_silver_step", fake_silver_step)

    results = occto_pipeline.run_occto_orchestrated_pipeline(**_pipeline_kwargs())

    assert scraper_instance.closed is True
    assert len(ingest_calls) == 1
    assert ingest_calls[0]["skip_if_exists"] is True
    assert ingest_calls[0]["use_ingestion_log"] is True
    assert ingest_calls[0]["require_unprocessed"] is True
    assert ingest_calls[0]["target_date"] == date(2026, 8, 7)

    assert len(silver_calls) == 1
    assert silver_calls[0]["from_date"] == date(2026, 8, 7)
    assert silver_calls[0]["to_date"] == date(2026, 8, 7)

    assert [result.name for result in results] == [
        "source_to_raw",
        "raw_to_bronze",
        "bronze_to_silver",
    ]
    assert results[0].status == "success"
    assert results[1].status == "success"


def test_run_occto_orchestrated_pipeline_skips_raw_to_bronze_when_no_unprocessed(
    monkeypatch,
) -> None:
    """Pipeline should continue when bronze step has no unprocessed latest snapshot."""

    class DummyScraper:
        def close(self) -> None:
            return None

    silver_calls: list[dict[str, object]] = []

    _patch_raw_step(monkeypatch, DummyScraper(), skipped=True)

    def fake_ingest(**_: object) -> int:
        raise ValueError("No unprocessed latest snapshot found in ingestion log")

    def fake_silver_step(**kwargs: object) -> object:
        silver_calls.append(kwargs)
        return occto_pipeline.PipelineStepResult(
            name="bronze_to_silver", status="success", detail="ok"
        )

    monkeypatch.setattr(occto_pipeline, "ingest_occto_unit_generation", fake_ingest)
    monkeypatch.setattr(occto_pipeline, "run_bronze_to_silver_step", fake_silver_step)

    results = occto_pipeline.run_occto_orchestrated_pipeline(**_pipeline_kwargs())

    assert results[0].name == "source_to_raw"
    assert results[0].status == "skipped"
    assert results[1].name == "raw_to_bronze"
    assert results[1].status == "skipped"
    assert len(silver_calls) == 1


def test_run_occto_orchestrated_pipeline_loops_per_day_for_multi_day_range(
    monkeypatch,
) -> None:
    """For a 3-day range each day should trigger its own scrape and bronze call;
    silver runs once at the end covering the full from..to window."""

    class DummyScraper:
        def close(self) -> None:
            return None

    scrape_calls: list[dict[str, object]] = []
    ingest_calls: list[dict[str, object]] = []
    silver_calls: list[dict[str, object]] = []

    monkeypatch.setattr(occto_pipeline, "RustFSClient", lambda: "rustfs-client")
    monkeypatch.setattr(
        occto_pipeline, "OCCTOUnitGenerationScraper", lambda: DummyScraper()
    )

    def fake_scrape(**kwargs: object) -> object:
        scrape_calls.append(kwargs)
        d = kwargs["from_date"]
        return SimpleNamespace(
            skipped=False,
            from_date=d,
            to_date=d,
            sha256="abc12345",
            snapshot_prefix=f"raw/.../target_date={d}/ingested_at=...",
        )

    def fake_ingest(**kwargs: object) -> int:
        ingest_calls.append(kwargs)
        return 48

    def fake_silver_step(**kwargs: object) -> object:
        silver_calls.append(kwargs)
        return occto_pipeline.PipelineStepResult(
            name="bronze_to_silver", status="success", detail="ok"
        )

    monkeypatch.setattr(occto_pipeline, "scrape_occto_unit_generation_raw", fake_scrape)
    monkeypatch.setattr(occto_pipeline, "ingest_occto_unit_generation", fake_ingest)
    monkeypatch.setattr(occto_pipeline, "run_bronze_to_silver_step", fake_silver_step)

    results = occto_pipeline.run_occto_orchestrated_pipeline(
        **_pipeline_kwargs(from_date=date(2026, 8, 7), to_date=date(2026, 8, 9))
    )

    # scrape called once per day, each with a single-day window
    assert len(scrape_calls) == 3
    assert [c["from_date"] for c in scrape_calls] == [
        date(2026, 8, 7),
        date(2026, 8, 8),
        date(2026, 8, 9),
    ]
    assert all(c["to_date"] == c["from_date"] for c in scrape_calls)

    # bronze called once per day
    assert len(ingest_calls) == 3
    assert [c["target_date"] for c in ingest_calls] == [
        date(2026, 8, 7),
        date(2026, 8, 8),
        date(2026, 8, 9),
    ]

    # silver called once with the full ingested range
    assert len(silver_calls) == 1
    assert silver_calls[0]["from_date"] == date(2026, 8, 7)
    assert silver_calls[0]["to_date"] == date(2026, 8, 9)

    # result list: 3×(source_to_raw + raw_to_bronze) + 1×bronze_to_silver
    result_names = [r.name for r in results]
    assert result_names.count("source_to_raw") == 3
    assert result_names.count("raw_to_bronze") == 3
    assert result_names.count("bronze_to_silver") == 1


def test_run_occto_orchestrated_pipeline_defaults_from_date_to_yesterday_jst(
    monkeypatch,
) -> None:
    """No --target-date given should fall back to yesterday in JST."""

    class DummyScraper:
        def close(self) -> None:
            return None

    scrape_calls: list[dict[str, object]] = []

    monkeypatch.setattr(occto_pipeline, "RustFSClient", lambda: "rustfs-client")
    monkeypatch.setattr(
        occto_pipeline, "OCCTOUnitGenerationScraper", lambda: DummyScraper()
    )
    monkeypatch.setattr(
        occto_pipeline, "resolve_default_target_date", lambda _now: date(2026, 8, 9)
    )

    def fake_scrape(**kwargs: object) -> object:
        scrape_calls.append(kwargs)
        return SimpleNamespace(
            skipped=False,
            from_date=date(2026, 8, 9),
            to_date=date(2026, 8, 9),
            sha256="abc12345",
            snapshot_prefix="raw/occto/unit_generation/target_date=2026-08-09/...",
        )

    monkeypatch.setattr(occto_pipeline, "scrape_occto_unit_generation_raw", fake_scrape)
    monkeypatch.setattr(occto_pipeline, "ingest_occto_unit_generation", lambda **_: 1)
    monkeypatch.setattr(
        occto_pipeline,
        "run_bronze_to_silver_step",
        lambda **_: occto_pipeline.PipelineStepResult(
            name="bronze_to_silver", status="success", detail="ok"
        ),
    )

    occto_pipeline.run_occto_orchestrated_pipeline(
        **_pipeline_kwargs(from_date=None, to_date=None)
    )

    assert len(scrape_calls) == 1
    assert scrape_calls[0]["from_date"] == date(2026, 8, 9)
    assert scrape_calls[0]["to_date"] == date(2026, 8, 9)

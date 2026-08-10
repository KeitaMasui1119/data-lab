# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Lint and format
uv run ruff check src/
uv run ruff format src/

# Type checking
uv run pyright

# Run all tests
uv run pytest tests/

# Run a single test
uv run pytest tests/test_jepx_pipeline_orchestrator.py::test_run_dbt_step_executes_expected_command

# Pipeline execution (all commands go through src/main.py)
uv run python src/main.py bootstrap-storage
uv run python src/main.py scrape-jepx
uv run python src/main.py ingest-jepx-raw-to-bronze
uv run python src/main.py run-jepx-orchestrator
uv run python src/main.py scrape-occto --target-date YYYY-MM-DD
uv run python src/main.py ingest-occto-raw-to-bronze --object-key raw/occto/unit_generation/<file>.csv
uv run python src/main.py provision-silver-tables
uv run python src/main.py ingest-jepx-bronze-to-silver
uv run python src/main.py ingest-occto-bronze-to-silver
```

## Architecture

This is a medallion data platform for Japanese power market data. Storage is RustFS (S3-compatible), tables are PyIceberg, transforms use Polars and DuckDB, and an empty dbt project is kept in reserve for the optional gold layer.

### Data layers

```
Source → Raw (RustFS s3://jp-power-grid-dev/raw/)
       → Bronze (PyIceberg, Polars cast + metadata)
       → Silver (JEPX and OCCTO both: DuckDB transform + PyIceberg window replace)
       → Gold (dbt, optional, not yet implemented)
```

### Module responsibilities

- **`src/main.py`** — sole CLI entry point; routes all commands via argparse subparsers. Keep this as thin orchestration only.
- **`src/orchestration/jepx_pipeline.py`** — ADF-like end-to-end orchestrator for JEPX; returns `list[PipelineStepResult]` per step for structured result tracking.
- **`src/pipeline/raw/`** — HTTP scraping and raw upload. `JEPXSpotSummaryScraper` and `OCCTOUnitGenerationScraper` both extend `BaseHttpScraper`; only `build_request()` needs to be implemented, plus `prepare()` for scrapers that must establish session state first (OCCTO's disclaimer-agreement flow) before the download request can be built.
- **`src/pipeline/bronze/`** — Raw CSV → Iceberg table ingestion. Decodes cp932, casts via schema CSV, appends metadata columns (`source_data`, `status`, `ingestion_time`, `ingestion_date`, `execution_id`).
- **`src/pipeline/silver/`** — Bronze → Silver for both datasets, same DuckDB-scan-then-PyIceberg-window-replace shape. JEPX (`bronze_to_silver_jepx_spot_price.py`) replaces the affected `delivery_date` window in the base, block and area tables; daily and full-refresh runs share one code path, only `--fiscal-year` differs. OCCTO (`bronze_to_silver_occto_unit_generation.py`) unpivots the 48 timeslot columns into one row per unit per 30-minute slot and replaces the affected `target_date` window in a single long table. Shared window-replace/write logic (`write_silver_table`, `ensure_unique_keys`, `column_bound`) lives in `common/silver_write.py`. The write is a window replace, not an upsert: `upsert()` builds a match predicate over every source key and scans the target with it, which exhausted memory once the JEPX area table reached a few million rows.
- **`src/common/`** — Shared primitives: `BaseHttpScraper`, `RustFSClient` (boto3 wrapper), `get_catalog` / `provision_table` (PyIceberg helpers), `build_schema_exprs` / `add_metadata` (Polars pipeline utilities), `create_duckdb_connection` (DuckDB + S3 setup), `silver_write` (window-replace write path).
- **`src/setup/`** — Infra provisioning: bucket creation with Object Lock, prefix initialization.
- **`src/dbt/jepx_power/`** — dbt project using DuckDB adapter. No models remain (both JEPX and OCCTO silver are pure Python/DuckDB/PyIceberg); kept only for a possible future gold layer. profiles.yml lives in the same directory.
- **`configuration/iceberg/schema/`** — **Source of truth for all table schemas.** CSV format has columns `source_name`, `name`, `type`. `provision_table()` creates or evolves tables from these files.
- **`configuration/iceberg/.pyiceberg.yaml`** — Catalog config. The `dlh_dev` catalog is SQLite-backed (`catalog/dlh_dev.db`), warehoused on RustFS at `http://rustfs:9000`.

### Key design patterns

**Deduplication**: `ingest_jepx_spot_summary` and `ingest_occto_unit_generation` check `source_data` column before appending. Pass `--allow-duplicate-source` to override.

**Schema evolution**: `provision_table()` diffs the schema CSV against the existing Iceberg table and adds new columns. It does not drop columns — removals only log a warning.

**Scraper lifecycle**: All scrapers hold an HTTP session. Always call `scraper.close()` or use as a context manager; `main.py` uses `try/finally` for this.

**Fiscal year logic**: JEPX files are keyed by fiscal year (April start). The function `resolve_fiscal_year()` in `common/jepx_common.py` handles the April boundary.

**Silver scope**: `run-jepx-orchestrator` rebuilds only the fiscal year it just ingested. Scoping every run to the whole table makes its cost grow with accumulated history rather than with new data, so a full rebuild has to be requested with `--silver-all-fiscal-years`.

### Environment variables required at runtime

```
AWS_ENDPOINT_URL       # e.g. http://rustfs:9000
AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY
AWS_REGION             # defaults to us-east-1
```

RustFS runs as the `rustfs` Docker Compose service. The PyIceberg catalog config is read automatically from `configuration/iceberg/.pyiceberg.yaml` when the working directory is `/workspace`.

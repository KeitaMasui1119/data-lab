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
uv run python src/main.py ingest-jepx-silver-to-gold

# Dashboard
PYTHONPATH=src uv run streamlit run src/dashboard/app.py
uv run python src/main.py ingest-occto-bronze-to-silver
```

## Git Workflow

Before any work that edits files, create and switch to a feature branch first — never edit files directly on `main`. This applies even to small follow-up changes (doc fixes, one-off scripts, migrations) made later in the same session, not just the first task of a session.

## Architecture

This is a medallion data platform for Japanese power market data. Storage is RustFS (S3-compatible), tables are PyIceberg, transforms use Polars and DuckDB. Every layer including gold is Python/DuckDB/PyIceberg; the dbt project is empty and currently unused.

### Data layers

```
Source → Raw (RustFS s3://jp-power-grid-dev/raw/)
       → Bronze (PyIceberg, Polars cast + metadata)
       → Silver (JEPX and OCCTO both: DuckDB transform + PyIceberg window replace)
       → Gold (DuckDB aggregate + PyIceberg window replace; JEPX daily / profile / interval spread)
```

### Module responsibilities

- **`src/main.py`** — sole CLI entry point; routes all commands via argparse subparsers. Keep this as thin orchestration only.
- **`src/orchestration/jepx_pipeline.py`** — ADF-like end-to-end orchestrator for JEPX; returns `list[PipelineStepResult]` per step for structured result tracking.
- **`src/pipeline/raw/`** — HTTP scraping and raw upload. `JEPXSpotSummaryScraper` and `OCCTOUnitGenerationScraper` both extend `BaseHttpScraper`; only `build_request()` needs to be implemented, plus `prepare()` for scrapers that must establish session state first (OCCTO's disclaimer-agreement flow) before the download request can be built.
- **`src/pipeline/bronze/`** — Raw CSV → Iceberg table ingestion. Decodes cp932, casts via schema CSV, appends metadata columns (`source_data`, `status`, `ingestion_time`, `ingestion_date`, `execution_id`).
- **`src/pipeline/silver/`** — Bronze → Silver for both datasets, same DuckDB-scan-then-PyIceberg-window-replace shape. JEPX (`bronze_to_silver_jepx_spot_price.py`) replaces the affected `delivery_date` window in the base, block and area tables; daily and full-refresh runs share one code path, only `--fiscal-year` differs. OCCTO (`bronze_to_silver_occto_unit_generation_actuals.py`) unpivots the 48 timeslot columns into one row per unit per 30-minute slot and replaces the affected `target_date` window in a single long table. Shared window-replace/write logic (`write_silver_table`, `ensure_unique_keys`, `column_bound`) lives in `common/silver_write.py`. The write is a window replace, not an upsert: `upsert()` builds a match predicate over every source key and scans the target with it, which exhausted memory once the JEPX area table reached a few million rows.
- **`src/common/`** — Dataset-agnostic shared primitives only; anything dataset-specific belongs under `src/pipeline/` (e.g. `pipeline/jepx_common.py`). Contents: `BaseHttpScraper` (`http_scraper.py`), `RustFSClient` boto3 wrapper (`storage_client.py`), `build_schema_exprs` / `add_metadata` Polars utilities (`pipeline_utilities.py`), `create_duckdb_connection` (`duckdb_utils.py`), window-replace write path (`silver_write.py`), plus the `common/iceberg/` subpackage: `catalog.py` (`get_catalog` / `provision_table` / `evolve_partition_spec`), `schema.py` (schema CSV → PyIceberg `Schema` / `PartitionSpec`), `maintenance.py` (snapshot expiry, orphan file cleanup).
- **`src/setup/`** — Infra provisioning: bucket creation with Object Lock, prefix initialization.
- **`src/pipeline/gold/`** — Silver → Gold aggregation. `silver_to_gold_jepx_spot_price.py` joins the area and base silver tables and writes three tables in one run: `jepx_spot_price_daily` (collapse the time codes), `jepx_spot_price_period_profile` (collapse the dates, keeping the intraday curve per month/area/day type) and `jepx_spot_price_area_spread` (no aggregation -- the pre-joined interval fact for heatmaps). Grain notes: prices are denormalized across areas but volumes are not (they are national, so repeating them would make a SUM over areas nine times too large), and the profile stores counts rather than rates so rollups stay correct. Both frames are key-checked before either is written.
- **`src/dashboard/`** — Streamlit app over gold. `queries.py` (DuckDB reads) and `charts.py` (Plotly figures) are Streamlit-free so they can be tested without a server; `app.py` is glue. Palette and its validation rationale live in `theme.py` — three categorical slots is a hard cap, and colour follows the entity rather than its rank.
- **`src/dbt/jepx_power/`** — dbt project using DuckDB adapter, currently unused. Gold took the same Python/DuckDB/PyIceberg route as silver, so no models remain. profiles.yml lives in the same directory.
- **`configuration/iceberg/schema/`** — **Source of truth for all table schemas.** CSV format has columns `source_name`, `name`, `type`. `provision_table()` creates or evolves tables from these files.
- **`configuration/iceberg/.pyiceberg.yaml`** — Catalog config. The `dlh_dev` catalog is SQLite-backed (`catalog/dlh_dev.db`), warehoused on RustFS at `http://rustfs:9000`.

### Key design patterns

**Deduplication**: `ingest_jepx_spot_summary` and `ingest_occto_unit_generation` check `source_data` column before appending. Pass `--allow-duplicate-source` to override.

**Schema evolution**: `provision_table()` diffs the schema CSV against the existing Iceberg table and adds new columns. It does not drop columns — removals only log a warning.

**Scraper lifecycle**: All scrapers hold an HTTP session. Always call `scraper.close()` or use as a context manager; `main.py` uses `try/finally` for this.

**Fiscal year logic**: JEPX files are keyed by fiscal year (April start). The function `resolve_fiscal_year()` in `pipeline/jepx_common.py` handles the April boundary.

**Silver scope**: `run-jepx-orchestrator` rebuilds only the fiscal year it just ingested. Scoping every run to the whole table makes its cost grow with accumulated history rather than with new data, so a full rebuild has to be requested with `--silver-all-fiscal-years`.

### Environment variables required at runtime

```
AWS_ENDPOINT_URL       # e.g. http://rustfs:9000
AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY
AWS_REGION             # defaults to us-east-1
```

RustFS runs as the `rustfs` Docker Compose service. The PyIceberg catalog config is read automatically from `configuration/iceberg/.pyiceberg.yaml` when the working directory is `/workspace`.

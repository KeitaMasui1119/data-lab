# Data Platform Practice: RustFS + PyIceberg + Polars

This repository is a local data engineering practice environment for building a
medallion-style data platform focused on Japanese power market data.

Current implementation emphasizes:

- object storage on RustFS (S3-compatible)
- table management and storage on PyIceberg
- transformations with Polars
- orchestrated execution from a thin `src/main.py`

## Overview

The project follows a medallion architecture.

- Raw: source files stored as-is in object storage
- Bronze: schema-conformed Iceberg tables with metadata columns
- Silver/Gold: planned downstream layers

Current primary data flow:

1. Scrape JEPX spot summary CSV from the website
2. Save file to `s3://jp-power-grid-dev/raw/jepx/spot_summary/`
3. Ingest raw CSV into `bronze.jepx_spot_price`

## Storage Layout

Primary bucket layout:

- `jp-power-grid-dev`
	- `raw/`
	- `bronze/`
	- `silver/`
	- `gold/`
	- `sandbox/`
- `jp-power-grid-prd`
	- same folder layout as dev
	- default object lock retention policy (COMPLIANCE, 7 days)

## Tech Stack

- Python 3.13+
- RustFS (S3-compatible object storage)
- PyIceberg
- Polars
- uv (dependency management)
- Ruff (linting/format)

## Environment Setup

1. Start the dev container (or run services defined in `compose.yaml`)
2. Install dependencies:

```bash
uv sync --all-groups
```

1. Authenticate GitHub CLI and enable Copilot CLI:

```bash
gh auth login
gh auth refresh -h github.com -s copilot
```

Validation examples:

```bash
gh copilot -- --help
gh copilot -p "explain this command" --allow-tool 'shell(uv)'
```

### PyIceberg Catalog Config

- PyIceberg catalog settings are managed in `configuration/iceberg/.pyiceberg.yaml`.
- For SQL catalog metadata DB files, prefer naming aligned to catalog names.
	- Example: `dlh_dev` -> `/workspace/configuration/iceberg/catalog/dlh_dev.db`
	- Example: `dlh_prd` -> `/workspace/configuration/iceberg/catalog/dlh_prd.db`
- Updating `uri` in `.pyiceberg.yaml` switches the referenced metadata DB.
	It does not automatically migrate or rename existing DB files.

1. Ensure `.env` includes S3-compatible credentials and endpoint:

- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`
- `AWS_REGION`
- `AWS_ENDPOINT_URL`

## Orchestrator Commands

All operational commands are exposed from `src/main.py`.

### 1) Bootstrap storage

```bash
uv run python src/main.py bootstrap-storage
```

Optional (specific bucket):

```bash
uv run python src/main.py bootstrap-storage --bucket jp-power-grid-dev
```

### 2) Scrape JEPX to raw

```bash
uv run python src/main.py scrape-jepx --bucket jp-power-grid-dev
```

Optional timestamp (UNIX ms):

```bash
uv run python src/main.py scrape-jepx --bucket jp-power-grid-dev --timestamp-ms 1711929600000
```

### 3) Ingest JEPX raw to bronze

```bash
uv run python src/main.py ingest-jepx-raw-to-bronze --bucket jp-power-grid-dev
```

Optional explicit source object:

```bash
uv run python src/main.py ingest-jepx-raw-to-bronze \
	--bucket jp-power-grid-dev \
	--object-key raw/jepx/spot_summary/spot_summary_2025.csv \
	--source-file-name spot_summary_2025.csv
```

By default, ingestion skips append when the same `source_data` already exists.
Use `--allow-duplicate-source` only when intentional re-append is required.

### 4) Provision silver tables from schema CSV

```bash
uv run python src/main.py provision-silver-tables --catalog dlh_dev
```

Optional schema directory:

```bash
uv run python src/main.py provision-silver-tables \
	--catalog dlh_dev \
	--schema-dir /workspace/configuration/iceberg/schema/silver
```

### 5) Build the JEPX silver layer

Transform the JEPX bronze table into the base, block and area silver tables.
DuckDB casts and deduplicates the rows and PyIceberg upserts the result, so
re-running the command is safe:

```bash
uv run python src/main.py ingest-jepx-bronze-to-silver
```

Every fiscal year is upserted by default. Narrow the run to one year with:

```bash
uv run python src/main.py ingest-jepx-bronze-to-silver --fiscal-year 2026
```

### 6) Build OCCTO silver layer with DuckDB + dbt

Run OCCTO staging and silver models from the OCCTO bronze table:

```bash
uv run python src/main.py run-occto-silver-dbt
```

Optional full refresh:

```bash
uv run python src/main.py run-occto-silver-dbt --full-refresh
```

This command reads `bronze.occto_unit_generation_actuals`, materializes
`main_staging.stg_occto_unit_generation_actuals`, and then writes
`main_silver.silver_occto_unit_generation_actuals` in DuckDB.

## Bronze Table Schema

JEPX spot price schema is managed from:

- `configuration/iceberg/schema/bronze/jepx_spot_price.csv`

Iceberg table creation (if needed):

```bash
uv run python src/catalog/manage_iceberg.py \
	--catalog dlh_dev table create \
	--name bronze.jepx_spot_price \
	--csv /workspace/configuration/iceberg/schema/bronze/jepx_spot_price.csv
```

## Code Structure

- `src/main.py`: thin orchestrator (execution flow only)
- `src/orchestration/`: pipeline-level orchestration entrypoints (planned)
- `src/pipeline/scraper/`: scraping modules (shared + source-specific)
- `src/pipeline/ingestion/`: raw to Iceberg ingestion steps
- `src/catalog/`: Iceberg catalog and table management
- `src/utility/`: reusable transformation helpers

## Target Source Layout (Current Design)

The project is being standardized around medallion-aware module boundaries.

- `src/common/`
	- Shared reusable components used across pipelines.
	- The extra `module` directory is not required in the target layout.
- `src/pipeline/`
	- `raw/`: `source_to_raw_<pipeline>.py`
	- `bronze/`: `raw_to_bronze_<pipeline>.py`, `source_to_bronze_<pipeline>.py`
	- `silver/`: `bronze_to_silver_<pipeline>.py`
	- `gold/`: `silver_to_gold_<pipeline>.py`, `vw_silver_to_gold_<pipeline>.py`
- `src/orchestration/`
	- End-to-end orchestration files named `pl_<pipeline>.py`.
	- Example: `pl_jepx.py` calls each layer in order
		(`source/raw -> bronze -> silver -> gold`).

Execution model:

- `src/main.py` remains the single CLI entrypoint.
- `src/main.py` calls `src/orchestration/pl_<pipeline>.py` to run full ingestion
	pipelines end-to-end.

## Development Environment Snapshot

This workspace is a local data lakehouse practice environment focused on Japanese power-market data.

Current environment facts:

- OS: Debian GNU/Linux 13 (trixie)
- Kernel: Linux 6.6.87.2-microsoft-standard-WSL2
- Shell: zsh
- Python requirement: 3.13+
- uv: 0.11.8
- git: 2.47.3

Notes:

- `rg` is not installed in this environment; use `grep` as a fallback.
- Validate touched files with narrow checks first, such as `uv run ruff check <changed paths>`.

## Pipeline Design Notes

The project uses a Python module + CLI orchestration model instead of notebook-first orchestration.

Mapping from the notebook-based model to this repository:

| Practice | Repository pattern |
|---|---|
| Databricks Notebook | Python module / function |
| Azure Data Factory Pipeline | CLI orchestration layer |
| Pipeline parameters | CLI args / config |
| Secrets / Linked Services | `.env` + config |

Implementation guidance:

- Keep each task as a reusable Python module or function.
- Keep orchestration thin and push business logic into testable modules.
- Use the Bronze layer for string-preserving ingestion only.
- Use the Silver layer for casting, timestamp derivation, and deduplication.
- Prefer hierarchical Iceberg namespaces such as `bronze.jepx.spot_price`.

## Development Rules

- Use feature branches for each task; avoid direct work on `main`
- Keep orchestration separated from reusable processing logic
- Prefer dependency injection (pass clients/catalog to functions)
- Validate touched files with narrow checks first:

```bash
uv run ruff check <changed paths>
```

## Current Status Snapshot

- JEPX scraping to RustFS raw: implemented
- Bronze table provisioning for JEPX spot price: implemented
- Raw-to-bronze ingestion with duplicate guard: implemented
- OCCTO raw-to-bronze ingestion and bronze-to-silver dbt pipeline: implemented
- Silver/Gold pipeline steps: in progress

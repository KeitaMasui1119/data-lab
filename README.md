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
- Bronze: schema-conformed Iceberg tables (string-typed, source structure preserved, audit metadata columns)
- Silver: DuckDB-transformed, typed, deduplicated Iceberg tables written via a window-replace (not upsert)
- Gold: planned, not yet implemented

Datasets implemented so far, each following Raw -> Bronze -> Silver:

| Dataset | Source | Bronze | Silver |
|---|---|---|---|
| JEPX spot price | JEPX website | `bronze.jepx_spot_price` | `silver.jepx_spot_price` (base/block/area) |
| OCCTO unit generation actuals | OCCTO disclosure system | `bronze.occto_unit_generation_actuals` | `silver.occto_unit_generation_actuals` |
| Hokuriku power_usage (でんき予報) | rikuden.co.jp daily snapshot CSV | `bronze.power_usage_hokuriku_{daily_summary,hourly,interval5}` | `silver.power_usage_hokuriku_{daily_summary,hourly,interval5}` |
| supply_demand_actuals (需給実績) | Tohoku/Chugoku/Shikoku yearly actuals CSV | `bronze.supply_demand_actuals_{tohoku,chugoku,shikoku}` | `silver.supply_demand_actuals_{tohoku,chugoku,shikoku}` |

See `docs/architecture/data_model.md` for the full table/column design and
`docs/tasks/tasks.md` for what is implemented vs. still open per company.

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
- Polars (Bronze layer: cast + metadata)
- DuckDB (Silver layer: cast, dedupe, unpivot where needed)
- uv (dependency management)
- Ruff (linting/format), Pyright (type checking), pytest (tests)

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
DuckDB casts and deduplicates the rows, then PyIceberg replaces (overwrites)
the `delivery_date` window covered by the run -- not an upsert -- so
re-running the command is safe:

```bash
uv run python src/main.py ingest-jepx-bronze-to-silver
```

Without `--fiscal-year`, this command rebuilds every fiscal year currently in
bronze. Narrow the run to one year with:

```bash
uv run python src/main.py ingest-jepx-bronze-to-silver --fiscal-year 2026
```

`run-jepx-orchestrator` (the end-to-end pipeline) scopes silver differently:
by default it rebuilds only the fiscal year it just ingested, not every year,
because rebuilding everything on every run made the write cost grow with the
table's full history rather than with new data. Pass
`--silver-all-fiscal-years` there to force a full rebuild across every year,
or `--silver-fiscal-year <year>` to target one explicitly.

### 6) Build OCCTO silver layer with Python + DuckDB + PyIceberg

Transform OCCTO bronze unit generation actuals into the silver Iceberg table:

```bash
uv run python src/main.py ingest-occto-bronze-to-silver
```

Optionally scope the run to a target_date or range:

```bash
uv run python src/main.py ingest-occto-bronze-to-silver --target-date 2026-08-07
uv run python src/main.py ingest-occto-bronze-to-silver --from-date 2026-08-01 --to-date 2026-08-07
```

This command reads `bronze.occto_unit_generation_actuals`, unpivots the 48
timeslot columns into one row per unit per 30-minute slot, and writes
`silver.occto_unit_generation_actuals` via a window (`target_date`) replace,
the same DuckDB + PyIceberg approach as JEPX's silver layer (no dbt).

### 7) OCCTO: scrape and raw-to-bronze

OCCTO requires a 4-step session flow (GET home -> POST disclaimer-agree ->
POST search -> GET downloadCsv), handled internally by the scraper:

```bash
uv run python src/main.py scrape-occto --target-date 2026-08-14
uv run python src/main.py ingest-occto-raw-to-bronze \
	--object-key raw/occto/unit_generation/target_date=2026-08-14/ingested_at=.../file.csv \
	--target-date 2026-08-14
```

`--target-date` defaults to the previous day in Asia/Tokyo (OCCTO publishes
each day's actuals ~15:30 JST the following day). `run-occto-orchestrator`
runs scrape -> bronze -> silver end to end for one day or a range.

### 8) Hokuriku power_usage (でんき予報)

Hokuriku's daily snapshot CSV is a multi-section report (today/next-day
peak-summary blocks, an hourly table, a 5-minute-interval table). Bronze
splits it into 3 tables instead of one wide table, and silver mirrors that
1:1 (unpivoting the hourly/interval5 tables, casting the rest):

```bash
uv run python src/main.py scrape-power-usage-hokuriku --target-date 2026-08-14
uv run python src/main.py ingest-power-usage-hokuriku-raw-to-bronze \
	--object-key raw/power_usage/hokuriku/target_date=2026-08-14/ingested_at=.../juyo_05_20260814.csv \
	--target-date 2026-08-14
uv run python src/main.py ingest-power-usage-hokuriku-bronze-to-silver --target-date 2026-08-14
```

`--target-date` defaults to the previous day in Asia/Tokyo -- today's
snapshot is still live/incomplete (a mid-day fetch has empty values for
hours that have not elapsed yet); a date's data is only fully finalized
shortly after midnight JST the following day.

### 9) supply_demand_actuals (需給実績): Tohoku / Chugoku / Shikoku

Unlike Hokuriku, these sources publish one cumulative CSV per calendar
year (growing by a day's rows daily) rather than a per-day file, so the
raw scraper downloads the whole current year and bronze ingestion filters
it down to one `target_date`'s rows. One shared, `--company`-parameterized
module covers all 3 companies (their mechanics are identical apart from
URL and Shikoku's extra supply-capacity column):

```bash
uv run python src/main.py scrape-supply-demand-actuals --company tohoku --target-date 2026-08-14
uv run python src/main.py ingest-supply-demand-actuals-raw-to-bronze \
	--company tohoku \
	--object-key raw/supply_demand_actuals/tohoku/year=2026/ingested_at=.../juyo_2026_tohoku.csv \
	--target-date 2026-08-14
uv run python src/main.py ingest-supply-demand-actuals-bronze-to-silver --company tohoku --target-date 2026-08-14
```

`--company` accepts `tohoku`, `chugoku`, or `shikoku`. Tokyo/TEPCO is not
yet implemented -- its current year's actuals archive is not published
live; only a Hokuriku-style rich snapshot (`juyo-d1-j.csv`) is available
for it, which needs a different extraction approach.

## Bronze Table Schema

`configuration/iceberg/schema/{bronze,silver}/` is the source of truth for
every table's columns (`field_id,name,type,is_identifier,required,doc,
partition_transform,source_name,comment`). Examples:

- `configuration/iceberg/schema/bronze/jepx_spot_price.csv`
- `configuration/iceberg/schema/bronze/occto_unit_generation_actuals.csv`
- `configuration/iceberg/schema/bronze/power_usage_hokuriku_{daily_summary,hourly,interval5}.csv`
- `configuration/iceberg/schema/bronze/supply_demand_actuals_{tokyo,tohoku,chugoku,shikoku}.csv`

Iceberg table creation/evolution (if needed) via the admin CLI:

```bash
uv run python -m setup.manage_iceberg table \
	--name bronze.jepx_spot_price \
	--csv /workspace/configuration/iceberg/schema/bronze/jepx_spot_price.csv \
	create
```

`--catalog` (default `dlh_dev`) goes before the `table` subcommand; `create`
is idempotent (diffs and evolves an existing table's schema, additive only
-- column removals only warn, never drop).

## Code Structure

- `src/main.py`: thin CLI orchestrator (argparse subcommands, execution flow only)
- `src/orchestration/`: end-to-end pipeline orchestrators (currently JEPX and OCCTO; `pl_<pipeline>.py`)
- `src/pipeline/raw/`: HTTP scraping and raw-layer upload (`source_to_raw_<pipeline>.py`)
- `src/pipeline/bronze/`: raw-to-bronze ingestion (`source_to_bronze_<pipeline>.py`)
- `src/pipeline/silver/`: bronze-to-silver DuckDB transforms (`bronze_to_silver_<pipeline>.py`)
- `src/pipeline/gold/`: reserved for a future dbt-based gold layer (empty)
- `src/pipeline/jepx_common.py`: JEPX-specific shared helpers (fiscal-year resolution, etc.)
- `src/common/`: dataset-agnostic shared primitives (`BaseHttpScraper`, `RustFSClient`,
	Polars/DuckDB utilities, the window-replace silver write path, `common/iceberg/`
	catalog+schema+maintenance helpers) -- anything dataset-specific belongs under `src/pipeline/` instead
- `src/setup/`: infra provisioning (bucket creation, `manage_iceberg.py` admin CLI)
- `src/dbt/jepx_power/`: dbt project kept in reserve for a possible future gold layer (no models currently)
- `src/Jupyter/`: scraping prototypes and ad-hoc analysis notebooks (not part of the production path)
- `tests/`: pytest unit tests, one file per `src/pipeline/**` module

## Development Environment Snapshot

This workspace is a local data lakehouse practice environment focused on Japanese power-market data.

Current environment facts:

- OS: Debian GNU/Linux 13 (trixie)
- Kernel: Linux 6.18.33.2-microsoft-standard-WSL2
- Shell: zsh
- Python requirement: 3.13+
- uv: 0.12.4
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
- Namespace tables as `{layer}.{table_name}` (e.g. `bronze.jepx_spot_price`,
	`silver.power_usage_hokuriku_hourly`) -- flat, underscore-joined names, not
	nested namespaces.

## Development Rules

- Use feature branches for each task; avoid direct work on `main`
- Keep orchestration separated from reusable processing logic
- Prefer dependency injection (pass clients/catalog to functions)
- Validate touched files with narrow checks first:

```bash
uv run ruff check <changed paths>
```

## Current Status Snapshot

- JEPX: raw -> bronze -> silver (base/block/area), end-to-end orchestrator: implemented
- OCCTO: raw -> bronze -> silver, end-to-end orchestrator: implemented
- Hokuriku power_usage（電力使用状況／でんき予報）: raw -> bronze (3-table split) -> silver:
	implemented; backfilled against 2,082 real historical snapshots
- supply_demand_actuals（需給実績）for Tohoku/Chugoku/Shikoku: raw -> bronze -> silver:
	implemented; Tokyo/TEPCO not yet implemented (needs a different extraction
	approach -- see `docs/architecture/data_model.md` 3.2)
- End-to-end orchestrator for power_usage_hokuriku / supply_demand_actuals
	(each step currently run as separate CLI commands): not yet implemented
- Other utility companies (Kansai, Hokkaido, Okinawa, Chubu, Kyushu) and the
	`grid_supply_demand`（系統の需給）category: not yet investigated
- Gold layer: not yet implemented

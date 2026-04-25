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

- Python 3.12+
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

3. Ensure `.env` includes S3-compatible credentials and endpoint:

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

## Bronze Table Schema

JEPX spot price schema is managed from:

- `data/schema/bronze/jepx_spot_price.csv`

Iceberg table creation (if needed):

```bash
uv run python src/catalog/manage_iceberg.py \
	--catalog dlh_dev table create \
	--name bronze.jepx_spot_price \
	--csv /workspace/data/schema/bronze/jepx_spot_price.csv
```

## Code Structure

- `src/main.py`: thin orchestrator (execution flow only)
- `src/core/`: infrastructure clients (RustFS client)
- `src/pipeline/scraper/`: scraping modules (shared + source-specific)
- `src/pipeline/ingestion/`: raw to Iceberg ingestion steps
- `src/catalog/`: Iceberg catalog and table management
- `src/utility/`: reusable transformation helpers

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
- Silver/Gold pipeline steps: in progress

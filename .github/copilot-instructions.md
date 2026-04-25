# Project Guidelines

## Project Focus

- This repository is a local data lakehouse and data engineering practice workspace centered on Python, Polars, PyIceberg, DuckDB, dbt, object storage (S3/RustFS), and Japanese power-market datasets.
- Prefer production-quality logic in `src/` and `utility/`. Treat notebooks as exploration, validation, or reporting surfaces.
- Avoid editing large raw files under `data/` unless the task explicitly requires data regeneration or fixture updates.

## Architecture

- `src/catalog/`, `src/core/`, `src/gcs/`, and `src/pipeline/` contain reusable application code.
- `src/Jupyter/` and `src/others/` are useful for exploration and experiments, but reusable logic should be extracted into modules.
- `configuration/` and compose-related files define the local development environment and supporting services.

## Build And Validation

- Sync dependencies with `uv sync --all-groups`.
- Prefer narrow validation first: run `uv run ruff check <changed paths>` before broader checks.
- When type checking is useful, run `uv run pyright <changed paths>`.
- If no targeted test exists, validate the smallest affected command or script instead of running broad checks by default.

## Conventions

- Keep side effects at the edges: modules should favor reusable functions over import-time execution.
- Preserve existing project style and keep edits focused; do not refactor unrelated files opportunistically.
- When handling tabular data, prefer explicit column names and clear transformation steps over implicit positional logic.
- Call out assumptions about encoding, date granularity, and external data shape when they affect correctness.

## Data Architecture (Medallion)
- **Bronze Layer (Raw)**: STRICT RULE. Always read source data (CSV/JSON) strictly as `String` (Utf8). Do NOT perform any type casting, date formatting, or business logic here. Only perform column renaming (Japanese to English) and append ETL metadata.
- **Silver Layer (Conformed)**: Perform type casting (String to Date/Int), generate timestamps (e.g., converting JEPX time_code 1-48 to datetime), and handle deduplication logic here.
- **Iceberg Namespaces**: Use hierarchical dot-notation for table identifiers to avoid S3 flat-structure clutter (e.g., use `bronze.jepx.spot_price` instead of `bronze.jepx_spot_price`).

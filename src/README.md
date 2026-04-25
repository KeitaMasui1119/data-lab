# src layout

This directory uses an orchestrator-first layout.

- `main.py`: Thin orchestrator for execution flow only.
- `core/`: Shared infrastructure clients (e.g., RustFS client).
- `catalog/`: Iceberg catalog and table management.
- `pipeline/`: Reusable pipeline steps.
  - `bootstrap/`: Environment/bootstrap steps.
  - `ingestion/`: Data ingestion steps.
  - `scraper/`: Data collection components.
- `utility/`: Reusable transformation/helper functions.
- `Jupyter/`: Exploration and validation notebooks.
- `others/`: Legacy scripts under migration to reusable modules.
- `dbt101/`: Learning assets and examples.

## refactor rule

When adding a new processing step:

1. Create a reusable module in `pipeline/` (or `core/` when infra-related).
2. Keep side effects inside callable functions.
3. Wire execution from `main.py` as orchestrator.

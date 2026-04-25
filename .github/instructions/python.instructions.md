---
description: "Use when editing Python modules, pipeline code, storage clients, scraper code, utilities, or reusable data-processing logic in this workspace."
applyTo:
  - "src/**/*.py"
  - "utility/**/*.py"
name: "Python Data Engineering Guidelines"
---

# Python Guidelines

- Prefer reusable functions and small modules over script-style logic.
- Add or preserve type hints on public functions and data boundaries.
- Keep network, file system, and object storage access near the edges so transformation code stays testable.
- For dataframe-style transformations, make column selection, rename, and casting explicit.
- Reuse existing modules before adding new utility layers or duplicate helpers.
- Validate touched Python files with `uv run ruff check <changed paths>` and use `uv run pyright <changed paths>` when interfaces or types changed.
- Strictly prefer `polars` over `pandas` for all dataframe operations.
- When reading raw files for the Bronze layer using Polars, use `infer_schema_length=0` or explicit string schemas to prevent accidental type parsing.
- Be aware of S3 Object Lock (COMPLIANCE mode) behavior. When destroying environments or tables in Iceberg during development, explicitly use `purge_table` instead of `drop_table` to ensure physical deletion of files.
- Documentation: All new Python files, classes, and public functions MUST include descriptive docstrings in English.
- Comments: All inline comments and explanations within the code must be written in English to maintain a global engineering standard.

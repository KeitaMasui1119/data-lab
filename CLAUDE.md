# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Everything CI runs, in one command. Prefer this over the raw tools below --
# noxfile.py holds the flags, so local and CI cannot enforce different things.
uv run nox                      # lint, format check, typecheck, test
uv run nox -s lint              # one session
uv run nox -s fmt               # rewrite instead of check
uv run nox -s test -- -k jepx   # extra pytest args pass through
uv run nox -s integration       # the tests CI skips; needs compose up + AWS_*

# Narrow checks while iterating on specific files
uv run ruff check <changed paths>
uv run pyright

# Run a single test
uv run pytest tests/orchestration/test_jepx_pipeline_orchestrator.py::test_run_dbt_step_executes_expected_command

# Pipeline execution (all commands go through src/main.py; --help lists all 27)
uv run python src/main.py bootstrap-storage
uv run python src/main.py run-jepx-orchestrator
uv run python src/main.py backfill-jepx --from-fiscal-year 2005 --to-fiscal-year 2026
uv run python src/main.py run-occto-orchestrator --target-date YYYY-MM-DD
uv run python src/main.py scrape-power-usage-hokuriku --target-date YYYY-MM-DD
uv run python src/main.py scrape-supply-demand-actuals-tohoku --target-date YYYY-MM-DD
uv run python src/main.py provision-silver-tables
uv run python src/main.py provision-metadata-tables
uv run python src/main.py ingest-jepx-bronze-to-silver
uv run python src/main.py ingest-jepx-silver-to-gold

# Dashboard
PYTHONPATH=src uv run streamlit run src/dashboard/app.py
```

## Git Workflow

Before any work that edits files, create and switch to a feature branch first — never edit files directly on `main`. This applies even to small follow-up changes (doc fixes, one-off scripts, migrations) made later in the same session, not just the first task of a session.

## Architecture

This is a medallion data platform for Japanese power market data. Storage is RustFS (S3-compatible), tables are PyIceberg, transforms use Polars and DuckDB. Every layer including gold is Python/DuckDB/PyIceberg; the dbt project has no models yet but is planned, which is why `dbt-core` / `dbt-duckdb` stay declared despite importing nowhere.

### Data layers

```
Source → Raw (RustFS s3://jp-power-grid-dev/raw/)
       → Bronze (PyIceberg, Polars cast + metadata)
       → Silver (every dataset: DuckDB transform + PyIceberg window replace)
       → Gold (DuckDB aggregate + PyIceberg window replace; JEPX daily / profile / interval spread)

metadata/ sits beside the layers rather than in them:
  metadata/raw_ingestion_log.parquet  — which file each scrape fetched (RustFS parquet)
  metadata.pipeline_run_log           — which run executed which step (Iceberg, append-only)
```

`execution_id` threads a single run through all of it: the orchestrator mints
one, the run log keys its steps by it, and raw/bronze/silver each stamp it on
what they write. A standalone CLI command passes none and gets its own, which
is correct — it is its own one-step run.

Datasets through Raw → Bronze → Silver: JEPX spot price, OCCTO unit generation actuals, Hokuriku power_usage (でんき予報, split into 3 bronze/silver tables), supply_demand_actuals for Tohoku / Chugoku / Shikoku. Gold exists for JEPX only. See `README.md` for per-dataset command examples and `docs/tasks/tasks.md` for what is still open.

### Module responsibilities

- **`src/main.py`** — The sole CLI entry point, under 40 lines. Builds the parser from `src/cli/`'s registry and dispatches; holds no command logic of its own. No pipeline module carries its own `argparse` any more (`docs/tasks/refactaring_20260817.md` 2.10); the one deliberate exception is `setup/manage_iceberg.py`, which README and data_model.md document as a directly-invoked admin command.
- **`src/cli/`** — argparse wiring, extracted from `main.py` (history: `docs/tasks/refactaring_20260817.md`). `commands/*.py` is one file per dataset (`jepx`, `occto`, `power_usage`, `supply_demand`, `silver_admin`, `storage`), each exporting `CommandSpec`s that `commands/__init__.py` concatenates into `ALL_COMMANDS`. `registry.py` turns that list into the parser and dispatch table. `args.py` / `dates.py` / `defaults.py` / `scraping.py` hold helpers shared across commands. **Add a new command by adding a `CommandSpec`, not by editing `main.py`.**
- **`src/orchestration/`** — ADF-like end-to-end orchestrators, `pl_<dataset>.py` (`pl_jepx_spot_price.py`, `pl_occto_unit_generation_actuals.py`); each returns `list[PipelineStepResult]` per step for structured result tracking. Every orchestrated run also persists those steps to `metadata.pipeline_run_log` via `pipeline_run_log.py` — one row per step, keyed by `run_id` + `step_seq`, so "did last night's run succeed and how many rows reached silver" survives the process. `record_pipeline_run()` reports rather than raises: a failed audit write must not fail a run that already moved its rows.
- **`src/pipeline/raw/`** — HTTP scraping and raw upload, one `source_to_raw_<dataset>.py` per source (6 today). All extend `BaseHttpScraper`; only `build_request()` needs to be implemented, plus `prepare()` for scrapers that must establish session state first (OCCTO's disclaimer-agreement flow) before the download request can be built. Every one records its snapshot through `common/raw_ingestion_log.py`'s `append_ingestion_log_entry()`; the six private copies this replaced had drifted, and JEPX's scoped its `is_latest` flip by fiscal year alone, clearing the flag on every other dataset's row for that year.
- **`src/pipeline/bronze/`** — Raw CSV → Iceberg table ingestion. Decodes cp932, casts via schema CSV, appends metadata columns (`source_data`, `status`, `ingestion_time`, `ingestion_date`, `execution_id`).
- **`src/pipeline/silver/`** — Bronze → Silver, one `bronze_to_silver_<dataset>.py` per dataset (6 today), all the same DuckDB-scan-then-PyIceberg-window-replace shape. JEPX replaces the affected `delivery_date` window in the base, block and area tables; daily and full-refresh runs share one code path, only `--fiscal-year` differs. OCCTO unpivots the 48 timeslot columns into one row per unit per 30-minute slot and replaces the affected `target_date` window in a single long table. Shared window-replace/write logic (`write_silver_table`, `ensure_unique_keys`, `column_bound`) lives in `common/silver_write.py`. The write is a window replace, not an upsert: `upsert()` builds a match predicate over every source key and scans the target with it, which exhausted memory once the JEPX area table reached a few million rows.
- **`src/common/`** — Dataset-agnostic shared primitives only; anything dataset-specific belongs under `src/pipeline/` (e.g. `pipeline/jepx_common.py`). Every module is named for what it knows about, so the import says where a helper came from: `utils.py` (the clock, `gen_uuid`, logging setup — the only deliberately generic one, and kept narrow on purpose), `polars_utils.py` (`build_schema_exprs` / `add_metadata`), `duckdb_utils.py` (`create_duckdb_connection`), `http_scraper.py` (`BaseHttpScraper`), `storage_client.py` (`RustFSClient` boto3 wrapper), `silver_write.py` (window-replace write path), `raw_ingestion_log.py` (the raw ingestion log's schema and read/append path), `raw_object_io.py` (`read_object_text`), plus the `common/iceberg/` subpackage: `catalog.py` (`get_catalog` / `provision_table` / `evolve_partition_spec`), `schema.py` (schema CSV → PyIceberg `Schema` / `PartitionSpec`), `maintenance.py` (snapshot expiry, orphan file cleanup). **Nothing new goes in `utils.py`** unless it has no domain at all — that is how the old `utilities.py` / `pipeline_utilities.py` pair became indistinguishable.
- **`src/setup/`** — Infra provisioning. `rustfs_bucket_setup.py` creates buckets with Object Lock and initializes prefixes; `manage_iceberg.py` is the admin CLI for namespace/table create/drop/recreate, split into `build_parser()` / `handle_namespace()` / `handle_table()` so the routing is testable without a live catalog (`tests/setup/`). Handlers return an exit code — a missing `--csv` exits 2, not 0.
- **`src/pipeline/gold/`** — Silver → Gold aggregation. `silver_to_gold_jepx_spot_price.py` joins the area and base silver tables and writes three tables in one run: `jepx_spot_price_daily` (collapse the time codes), `jepx_spot_price_period_profile` (collapse the dates, keeping the intraday curve per month/area/day type) and `jepx_spot_price_area_spread` (no aggregation -- the pre-joined interval fact for heatmaps). Grain notes: prices are denormalized across areas but volumes are not (they are national, so repeating them would make a SUM over areas nine times too large), and the profile stores counts rather than rates so rollups stay correct. Both frames are key-checked before either is written.
- **`src/dashboard/`** — Streamlit app over gold. `queries.py` (DuckDB reads) and `charts.py` (Plotly figures) are Streamlit-free so they can be tested without a server; `app.py` is glue. Palette and its validation rationale live in `theme.py` — three categorical slots is a hard cap, and colour follows the entity rather than its rank.
- **`src/dbt/jepx_power/`** — dbt project using the DuckDB adapter, no models yet. Gold took the same Python/DuckDB/PyIceberg route as silver, so nothing runs from here today, but dbt is planned and SQLFluff comes in with the first models. profiles.yml lives in the same directory.
- **`configuration/iceberg/schema/`** — **Source of truth for all table schemas.** One subfolder per dataset under `bronze/` and `silver/`, plus `metadata/` for the run log; the provisioning commands walk them recursively (`cli/schema_files.py` does the walk, parameterized by namespace). CSV columns: `field_id,name,type,is_identifier,required,doc,partition_transform,source_name,comment`. `provision_table()` creates or evolves tables from these files. **`build_table_schema()` injects five audit fields (`source_data`, `status`, `ingestion_time`, `ingestion_date`, `execution_id`) into every table**, so a CSV must not declare a column with one of those names, and a writer must populate all five or the arrow cast fails on mismatched field names.
- **`configuration/iceberg/.pyiceberg.yaml`** — Catalog config. The `dlh_dev` catalog is SQLite-backed (`catalog/dlh_dev.db`), warehoused on RustFS at `http://rustfs:9000`.

### Key design patterns

**Deduplication**: `run_source_to_bronze_jepx_spot_price` and `run_source_to_bronze_occto_unit_generation_actuals` check `source_data` column before appending. Pass `--allow-duplicate-source` to override.

**Pipeline entrypoint naming**: every layer module's main entrypoint is `run_<filename>` — `run_source_to_raw_<dataset>`, `run_source_to_bronze_<dataset>`, `run_bronze_to_silver_<dataset>`, `run_silver_to_gold_<dataset>`. The one exception is `scrape_jepx_to_rustfs`, a separate backfill entrypoint that uploads without the snapshot/manifest logic.

**Schema evolution**: `provision_table()` diffs the schema CSV against the existing Iceberg table and adds new columns. It does not drop columns — removals only log a warning.

**Scraper lifecycle**: All scrapers hold an HTTP session. Always call `scraper.close()` or use as a context manager; the handlers in `src/cli/commands/` use `try/finally` for this.

**Fiscal year logic**: JEPX files are keyed by fiscal year (April start). The function `resolve_fiscal_year()` in `pipeline/jepx_common.py` handles the April boundary.

**Silver scope**: `run-jepx-orchestrator` rebuilds only the fiscal year it just ingested. Scoping every run to the whole table makes its cost grow with accumulated history rather than with new data, so a full rebuild has to be requested with `--silver-all-fiscal-years`.

### Quality gates

`uv run nox` runs what CI runs. The flags live in `noxfile.py` and the CI jobs call the sessions, so the two cannot enforce different things — never re-spell a check inline in `ci.yml`.

**ruff** (`ruff.toml`) selects `E,F,W,I,B,UP,PTH,SIM,RUF,S,C90`. Two deliberate silences, both with the reasoning written next to them:

- `S608` — fires on all 45 silver/gold DuckDB queries, which interpolate Iceberg table/column names from the schema CSVs plus bound literals. No external input reaches those strings. **If a query ever takes a caller-supplied fragment, drop the ignore and annotate the safe sites individually.** Everything else in `S` stays on, including `S602` (shell injection).
- `RUF001` / `RUF003` — fullwidth parentheses and tildes are intentional in Japanese labels and comments.

**Coverage**: floor is `--cov-fail-under=71` measured with `--cov-branch`, one point under the 71.81% baseline. It is a regression catch, not a target — the target is 80%, ratcheted as untested modules get their first tests. The branch figure is lower than the line figure it replaced (75.01%) because the measure is stricter, not because coverage fell.

**Test markers**: `integration` means the test needs RustFS / Iceberg / DuckDB-on-storage; everything else is `unit` by default. CI runs `-m "not integration"`, so **a green CI run is not evidence the pipeline still ingests** — use `uv run nox -s integration` for that.

**PostToolUse hook caveat**: the hook runs `ruff check --fix` after every edit, which strips an import or `# noqa` written before the code that uses it exists. Enable the rule in `ruff.toml` first, then add the annotation.

### Environment variables required at runtime

```
AWS_ENDPOINT_URL       # e.g. http://rustfs:9000
AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY
AWS_REGION             # defaults to us-east-1
```

RustFS runs as the `rustfs` Docker Compose service. The PyIceberg catalog config is read automatically from `configuration/iceberg/.pyiceberg.yaml` when the working directory is `/workspace`.

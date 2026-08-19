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
- Gold: JEPX daily aggregates, intraday profile and interval spread fact (Python/DuckDB/PyIceberg, same shape as silver); other datasets not yet implemented

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

- Python 3.13 (a ceiling, not just the current choice -- see below)
- RustFS (S3-compatible object storage)
- PyIceberg
- Polars (Bronze layer: cast + metadata)
- DuckDB (Silver and Gold layers: cast, dedupe, unpivot, aggregate)
- uv (dependency management)
- Ruff (linting/format), Pyright (type checking), pytest (tests)
- pre-commit (local gate), GitHub Actions (CI), Renovate (dependency updates)

### Python 3.13 is a ceiling

PyIceberg, Polars and DuckDB all stop their published support at 3.13, and
those three carry every silver and gold transform. `requires-python` on each is
loose enough that a resolver will happily install them under 3.14, but that
puts the core of the pipeline on wheels their own CI never tested. 3.14 is
therefore off the table until all three ship for it.

`.python-version` is the source of truth -- CI reads it through
`.github/actions/setup-python-with-uv` rather than repeating the number. The
Dockerfile's `ARG VARIANT`, `ruff.toml`'s `target-version` and
`pyrightconfig.json`'s `pythonVersion` are separate declarations that have to
be kept in agreement by hand. Renovate is configured not to propose Python
updates at all, so this does not resurface as a recurring PR.

## Environment Setup

1. Start the dev container (or run the services defined in `compose.yaml`).

	Git identity is not configured by the container. VS Code's Dev Containers
	extension copies `user.name` and `user.email` from the host's `~/.gitconfig`
	on startup, and injects its own credential helper alongside them. If you
	open the container some other way and `git commit` complains it does not
	know who you are, set it once inside the container:

	```bash
	git config --global user.name "<your name>"
	git config --global user.email "<your email>"
	```

	It used to be hardcoded in `postCreateCommand`, which meant the container
	stamped one specific person's name onto anyone else's commits.

2. Install dependencies:

	```bash
	uv sync --locked
	```

	`--locked` fails when `uv.lock` no longer matches `pyproject.toml`, which is
	what CI does as well -- a dependency edit that never got relocked should be
	an error, not a silent re-resolve. Drop the flag only when you intend to
	update the lock.

	The environment lives at `/workspace/.venv`. `UV_PROJECT_ENVIRONMENT` in
	`.devcontainer/devcontainer.json` is the only place that decides this; the
	VS Code interpreter setting and the debug configuration point at the same
	path deliberately, so the terminal, the debugger and `uv run` cannot end up
	using different environments.

3. Install the pre-commit hooks:

	```bash
	uv run pre-commit install
	```

	The dev container's `postStartCommand` already runs this. Re-run it by hand
	if the virtualenv is ever recreated somewhere else: `.git/hooks/pre-commit`
	is generated with an absolute interpreter path baked in, and fails with
	`pre-commit not found` once that path stops existing.

4. Authenticate GitHub CLI and enable Copilot CLI:

	```bash
	gh auth login
	gh auth refresh -h github.com -s copilot
	```

	Validation examples:

	```bash
	gh copilot -- --help
	gh copilot -p "explain this command" --allow-tool 'shell(uv)'
	```

5. Ensure `.env` includes S3-compatible credentials and endpoint:

	- `AWS_ACCESS_KEY_ID`
	- `AWS_SECRET_ACCESS_KEY`
	- `AWS_REGION`
	- `AWS_ENDPOINT_URL`

### PyIceberg Catalog Config

- PyIceberg catalog settings are managed in `configuration/iceberg/.pyiceberg.yaml`.
- For SQL catalog metadata DB files, prefer naming aligned to catalog names.
	- Example: `dlh_dev` -> `/workspace/configuration/iceberg/catalog/dlh_dev.db`
	- Example: `dlh_prd` -> `/workspace/configuration/iceberg/catalog/dlh_prd.db`
- Updating `uri` in `.pyiceberg.yaml` switches the referenced metadata DB.
	It does not automatically migrate or rename existing DB files.

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

Both orchestrators (`run-jepx-orchestrator` and `run-occto-orchestrator`)
verify the silver step's row counts rather than reporting success on
completion alone. A step fails -- and the command exits non-zero -- when no
bronze row reached the staging relation, when every staged row failed
validation, or when the table received a different number of rows than the
staged rows account for. A run whose date column stops parsing is discarded
by the scope filter before validation ever sees it, so without that check it
reported `dropped=0, written=0, status=success` while silver went unwritten.
See `docs/tasks/tasks.md` section 8.4.

#### Backfilling and replaying a range of fiscal years

`backfill-jepx` runs one raw+bronze pass per fiscal year and then rebuilds
silver once for every year:

```bash
uv run python src/main.py backfill-jepx \
	--from-fiscal-year 2005 --to-fiscal-year 2026
```

`--to-fiscal-year` defaults to `--from-fiscal-year`, so a single year needs
only the one flag. Silver runs once at the end rather than per year: a scoped
silver run re-scans the whole bronze table (the fiscal year filter applies to
a cast column, so it cannot be pushed into the Iceberg scan), and one unscoped
pass covers every year for the cost of a single scan. A year that fails is
recorded and the range continues; the command exits non-zero afterwards and
names the years needing attention.

Add `--from-raw` to replay from the raw snapshots already in storage instead
of fetching them again -- the rebuild procedure
`docs/architecture/replay_strategy.md` defines, where raw is the source of
truth. This makes no HTTP request at all, so `--request-delay-seconds`
(default 3, applied between each year's request otherwise) does not apply:

```bash
uv run python src/main.py backfill-jepx \
	--from-fiscal-year 2005 --to-fiscal-year 2026 --from-raw
```

Bronze ingestion still skips a snapshot whose `source_data` is already
present, so replaying into an intact bronze table is a per-year no-op that
leaves only the silver rebuild to do (Silver Corruption Recovery). Clearing
the affected bronze rows first is what makes it re-ingest them (Bronze
Corruption Recovery).

#### Build the JEPX gold daily table

```bash
uv run python src/main.py ingest-jepx-silver-to-gold
uv run python src/main.py ingest-jepx-silver-to-gold --fiscal-year 2026
```

Builds two tables from the area and base silver tables. Scoping and the
window replace work exactly as they do for silver, so a rerun is safe.

`gold.jepx_spot_price_daily` rolls each delivery date's 48 time codes up to
one row per area: price statistics, the spread against the system price, and
counts of the time codes that split, spiked or sat at the price floor.

`gold.jepx_spot_price_area_spread` aggregates nothing at all: one row per
slot per area carrying the area price, the system price and their gap, plus
an `is_split` flag. It is the pre-joined interval fact a dashboard reads, so
a heatmap over date x time code does not join 3.3M area rows against the base
table on every query, and the split threshold is applied in exactly one
place. Partitioned on `year(delivery_date)` like the silver tables it mirrors.

`gold.jepx_spot_price_period_profile` keeps the time code and collapses the
dates instead, giving the intraday price curve per month, area and day type
(`weekday` / `holiday`, the latter covering Saturdays, Sundays and Japanese
national holidays via `jpholiday`). Month is the finest axis: a caller can
roll months up into seasons or eras, but cannot recover months from a table
that only stored seasons. It stores counts rather than rates so that rollups
stay correct -- `avg_price` rolls up as
`sum(avg_price * observation_count) / sum(observation_count)`, while medians
and percentiles cannot be rolled up at all.

The grain is one row per (`delivery_date`, `area_name`). `system_price` is
denormalized onto every area row because averaging it across areas still
returns the system price. The volume columns are deliberately absent: they
are national figures, so repeating them across nine rows would make any SUM
over areas nine times too large. They belong in a national-grain table that
does not exist yet.

`time_code_count` doubles as a completeness check -- Japan has no DST, so
anything other than 48 is a gap. A full rebuild currently produces 70,102
rows rather than 7,800 x 9 because JEPX suspended area trading twice:
Tokyo for 2011-03-15..2011-05-31 after the Great East Japan Earthquake, and
Hokkaido for 2018-09-07..2018-09-26 after the Iburi earthquake blackout.
Those area-days have no silver rows to aggregate.

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
it down to one `target_date`'s rows. Raw, bronze, and silver are each one
independent module per company
(`source_to_raw_supply_demand_actuals_{tohoku,chugoku,shikoku}.py`,
`source_to_bronze_supply_demand_actuals_{tohoku,chugoku,shikoku}.py`,
`bronze_to_silver_supply_demand_actuals_{tohoku,chugoku,shikoku}.py`),
same "one company, one file" convention as Hokuriku:

```bash
uv run python src/main.py scrape-supply-demand-actuals-tohoku --target-date 2026-08-14
uv run python src/main.py ingest-supply-demand-actuals-raw-to-bronze-tohoku \
	--object-key raw/supply_demand_actuals/tohoku/year=2026/ingested_at=.../juyo_2026_tohoku.csv \
	--target-date 2026-08-14
uv run python src/main.py ingest-supply-demand-actuals-bronze-to-silver-tohoku --target-date 2026-08-14
```

Company-specific commands throughout: `scrape-supply-demand-actuals-
{tohoku,chugoku,shikoku}`, `ingest-supply-demand-actuals-raw-to-bronze-
{tohoku,chugoku,shikoku}`, `ingest-supply-demand-actuals-bronze-to-silver-
{tohoku,chugoku,shikoku}`. Tokyo/TEPCO is not yet implemented -- its
current year's actuals archive is not published live;
only a Hokuriku-style rich snapshot (`juyo-d1-j.csv`) is available for
it, which needs a different extraction approach.

## Dashboard

A Streamlit app over the gold layer. It reads gold only -- never silver --
which is what `gold.jepx_spot_price_area_spread` exists for.

```bash
# locally
PYTHONPATH=src uv run streamlit run src/dashboard/app.py

# containerised, alongside RustFS
docker compose up dashboard   # http://localhost:8501
```

Three tabs, one per finding the gold tables were built to carry:

| Tab | Source table | Shows |
|---|---|---|
| 価格ヒートマップ | `_area_spread` | delivery date x time code for one fiscal year and area |
| 市場分断 | `_daily` | share of slots that split, per fiscal year and area |
| 日内カーブ | `_period_profile` | intraday price curve, up to three fiscal years overlaid |

Design notes worth knowing before editing the charts:

- The palette is validated, not chosen by eye: three categorical slots
  (`#2a78d6`, `#eb6834`, `#1baf7a`) clear the colourblind and normal-vision
  separation floors on the light surface. Three is the cap -- a fourth series
  is not a generated hue. One slot sits below 3:1 contrast, so every line
  carries a direct label and every chart ships a table view.
- Colour follows the entity, not its rank. Deselecting a year does not
  repaint the others (`charts.assign_series_slots`).
- Both heatmaps use one sequential hue. The price ramp clips at the 99th
  percentile because a single 2021-style spike would otherwise flatten every
  ordinary day into the lightest step; the caption states the true peak.
- The app commits to light mode (`.streamlit/config.toml`) rather than
  shipping a dark mode whose colours were never validated.

`src/dashboard/queries.py` and `src/dashboard/charts.py` are free of
Streamlit so both can be tested without a server; `app.py` is only glue.

## Bronze Table Schema

`configuration/iceberg/schema/{bronze,silver}/` is the source of truth for
every table's columns (`field_id,name,type,is_identifier,required,doc,
partition_transform,source_name,comment`). Each dataset has its own
subfolder (filenames unchanged); `provision-silver-tables` and friends walk
these directories recursively. Examples:

- `configuration/iceberg/schema/bronze/jepx_spot_price/jepx_spot_price.csv`
- `configuration/iceberg/schema/bronze/occto_unit_generation_actuals/occto_unit_generation_actuals.csv`
- `configuration/iceberg/schema/bronze/power_usage_hokuriku/power_usage_hokuriku_{daily_summary,hourly,interval5}.csv`
- `configuration/iceberg/schema/bronze/supply_demand_actuals/supply_demand_actuals_{tokyo,tohoku,chugoku,shikoku}.csv`

Iceberg table creation/evolution (if needed) via the admin CLI:

```bash
uv run python -m setup.manage_iceberg table \
	--name bronze.jepx_spot_price \
	--csv /workspace/configuration/iceberg/schema/bronze/jepx_spot_price/jepx_spot_price.csv \
	create
```

`--catalog` (default `dlh_dev`) goes before the `table` subcommand; `create`
is idempotent (diffs and evolves an existing table's schema, additive only
-- column removals only warn, never drop).

## Code Structure

- `src/main.py`: thin CLI entry point -- builds the parser from `src/cli/`'s
	command registry and dispatches to the matching handler, execution flow only
- `src/cli/`: argparse wiring, split out of `main.py` (see
	`docs/tasks/refactaring_20260817.md`). `commands/*.py` (one file per dataset:
	`jepx.py`, `occto.py`, `power_usage.py`, `supply_demand.py`, `silver_admin.py`,
	`storage.py`) each build a list of `CommandSpec`s; `commands/__init__.py`
	concatenates them into `ALL_COMMANDS`. `registry.py` turns that list into the
	actual argparse parser and dispatch table; `args.py` holds `add_argument()`
	helpers shared across commands; `dates.py` / `defaults.py` /
	`scraping.py` hold the remaining cross-command helpers extracted the same way
- `src/orchestration/`: end-to-end pipeline orchestrators (currently JEPX and OCCTO; `pl_<pipeline>.py`)
- `src/pipeline/raw/`: HTTP scraping and raw-layer upload (`source_to_raw_<pipeline>.py`)
- `src/pipeline/bronze/`: raw-to-bronze ingestion (`source_to_bronze_<pipeline>.py`)
- `src/pipeline/silver/`: bronze-to-silver DuckDB transforms (`bronze_to_silver_<pipeline>.py`)
- `src/pipeline/gold/`: silver-to-gold aggregation (`silver_to_gold_<pipeline>.py`)
- `src/pipeline/jepx_common.py`: JEPX-specific shared helpers (fiscal-year resolution, etc.)
- `src/common/`: dataset-agnostic shared primitives (`BaseHttpScraper`, `RustFSClient`,
	Polars/DuckDB utilities, the window-replace silver write path, `common/iceberg/`
	catalog+schema+maintenance helpers) -- anything dataset-specific belongs under `src/pipeline/` instead
- `src/setup/`: infra provisioning (bucket creation, `manage_iceberg.py` admin CLI --
	`build_parser()` / `handle_namespace()` / `handle_table()` are split out and
	independently testable; see `tests/setup/`)
- `src/dbt/jepx_power/`: dbt project with no models yet. Gold went the same
	Python/DuckDB/PyIceberg route as silver, so dbt is unused today, but it is
	planned -- which is why `dbt-core` and `dbt-duckdb` stay in `pyproject.toml`
	despite importing nowhere. SQLFluff comes in with the first models
- `src/Jupyter/`: scraping prototypes and ad-hoc analysis notebooks (not part of the production path)
- `tests/`: pytest tests mirroring `src/`'s package layout (`pipeline/`,
	`orchestration/`, `common/`, `dashboard/`, `setup/`, one file per module).
	`integration` marks a test that needs RustFS/Iceberg/DuckDB-on-storage;
	everything else is `unit` by default -- see Quality Gates below for how CI
	and `nox` use the split

## Development Environment Snapshot

This workspace is a local data lakehouse practice environment focused on Japanese power-market data.

Current environment facts (verified 2026-08-19):

- OS: Debian GNU/Linux 13 (trixie)
- Kernel: Linux 6.18.33.2-microsoft-standard-WSL2
- Shell: zsh
- Python: 3.13.15 (3.13 is a ceiling -- see Tech Stack)
- uv: 0.12.5
- git: 2.47.3
- Ruff: 0.16.3, Pyright: 1.1.408
- Virtualenv: `/workspace/.venv`

Notes:

- Validate touched files with narrow checks first, such as `uv run ruff check <changed paths>`.
- Docker builds read `.dockerignore`, which keeps the context to `src/`,
	`pyproject.toml`, `uv.lock` and `.streamlit/`. Without it the context is
	~1.7GB, and `.secrets/` and `.env` travel with it.

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

## Quality Gates

Three layers, each catching what the previous one cannot.

### pre-commit (local, on every commit)

```bash
uv run pre-commit run --all-files   # run the whole set by hand
```

| Hook | Covers |
|---|---|
| `end-of-file-fixer`, `trailing-whitespace` | whitespace hygiene |
| `check-json`, `check-toml`, `check-yaml`, `check-xml` | config files parse |
| `detect-private-key` | keys committed by accident |
| `ruff`, `ruff-format` | Python lint, format, security (`S`, the bandit port), and complexity (`C90`, max 10) |
| `actionlint` | GitHub Actions workflow files |
| `hadolint` | `Dockerfile` |
| `pyright` | static types |
| `pytest` | unit tests -- **manual stage only**, run with `pre-commit run pytest --hook-stage manual` |

The ruff pin appears twice, in `pyproject.toml` and as the `ruff-pre-commit`
rev. They must move together: pre-commit runs ruff from its own isolated
environment, so a drift means the hook guarding a commit and the CI guarding a
merge enforce different rules. Renovate groups the two for this reason.

Security linting runs through ruff's `S` rules rather than a separate bandit
install -- `S` *is* bandit, ported. One rule is switched off: `S608` fires on
all 45 of the silver/gold DuckDB queries, which interpolate Iceberg table and
column names from the schema CSVs and bound literals like a fiscal year. No
external input reaches those strings. Everything else in the set stays on,
including `S602`, which is the rule that catches a real shell injection. Should
a query ever take a caller-supplied fragment, drop the `S608` entry from
`ruff.toml` and annotate the safe sites individually instead.

### GitHub Actions (on push and pull request to `main`)

`ci.yml` runs seven jobs: `lint`, `format`, `typecheck`, `test`, `actionlint`,
`hadolint` and `docker-build`. The first four share
`.github/actions/setup-python-with-uv`, which reads `.python-version`, installs
uv with caching and runs `uv sync --locked`, and each one delegates to a nox
session (`uv run nox -s lint`, `-s test`, …) rather than spelling the command
out. `noxfile.py` is where the flags live, so what CI enforces and what you get
locally cannot drift apart -- see Task runner below. `docker-build` is a matrix
over the three shippable Dockerfile stages (`dev`, `prd`, `dashboard`); it
builds each on every PR without pushing so a Dockerfile break gets caught before
`deploy.yml` runs on `main`.

`deploy.yml` builds and pushes to GHCR after CI succeeds on `main`. The
Dockerfile has two shippable stages and both are published, each named
explicitly:

| Image | Stage | Entrypoint |
|---|---|---|
| `ghcr.io/keitamasui1119/voltlake/app` | `prd` | `python -m src.main` |
| `ghcr.io/keitamasui1119/voltlake/dashboard` | `dashboard` | `streamlit run src/dashboard/app.py` |

Naming the target matters: with none given, Docker builds the *last* stage in
the file, which is `dashboard`. That is how the CLI image spent a while
containing Streamlit.

The `test` job enforces a coverage floor with `--cov-fail-under=71`, measured
with `--cov-branch`. That is 1 point below the current 71.81% baseline, not a
target -- the target is 80%, ratcheted upward as untested modules get their
first tests. The floor exists to catch regressions, not to describe what "good"
looks like.

The number is lower than the line-only figure it replaced (75.01%) because
branch coverage is the stricter measure, not because coverage fell. Line
coverage marks an `if` covered as soon as either side runs; branch coverage
wants both. The paths that go untaken are where this pipeline's bugs have
actually lived -- a scope filter that matched nothing, a date column that
stopped parsing -- so the stricter measure is the one worth gating on.
`.coveragerc` holds the settings.

Note what CI does **not** cover: `pytest -m "not integration"` excludes every
test that touches RustFS, Iceberg or DuckDB-on-storage. A green CI run says the
code imports and the unit tests pass. It is not evidence that the pipeline
still ingests -- and the coverage number does not include the
integration-marked tests that exercise `common/storage_client.py`, so its low
line in the report is an artifact of the CI-only run, not a real gap. Run those
with `uv run nox -s integration` against a live compose stack.

### Task runner

```bash
uv run nox                 # lint, format check, typecheck, test
uv run nox -s test         # one session
uv run nox -s test -- -k jepx -x   # extra pytest args pass through
uv run nox -s fmt          # rewrite files instead of checking them
uv run nox -s integration  # the tests CI skips; needs compose up + AWS_* set
```

Sessions run against the project virtualenv (`default_venv_backend = "none"`)
rather than building one each. uv already pins everything through `uv.lock`, and
a per-session environment would reinstall the same packages from a second
resolver.

### Renovate (weekly)

`renovate.json` splits automerge by blast radius, which follows directly from
the gap above:

| Scope | Behaviour |
|---|---|
| GitHub Actions (minor/patch/digest) | grouped, automerged -- CI failing *is* the test |
| dev dependencies (minor/patch) | grouped, automerged -- build-only, no data path |
| `polars`, `pyiceberg`, `duckdb`, `pyarrow`, `boto3`, dbt, airflow | PR for review, held 7 days after release |
| `python` | updates disabled entirely (see the ceiling above) |

Lock file maintenance runs monthly and is never automerged.

> Renovate does nothing until its GitHub App is installed on the repository.

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
- Gold layer: `gold.jepx_spot_price_daily` (70,102 rows), `gold.jepx_spot_price_period_profile` (221,856 rows) and `gold.jepx_spot_price_area_spread` (3,364,896 rows) implemented for FY2005-FY2026; monthly, price events, monitoring and the dashboard in `docs/tasks/plan_jepx_gold.md` are open

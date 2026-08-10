# Layers

## Purpose

Define clear responsibilities and boundaries for each medallion layer.

## Scope

- Applies to all ingest pipelines in this repository (for example JEPX, OCCTO, demand forecast).
- Covers responsibility boundaries only.
- Excludes implementation details and tool-level configuration.

## Raw Layer

### Definition - Raw Layer

Raw is an immutable source archive layer.

### Responsibilities - Raw Layer

- Preserve source fidelity from external providers.
- Store source snapshots as received.
- Keep enough metadata to replay downstream processing.

### Allowed Operations - Raw Layer

- Source file acquisition.
- Minimal file normalization required only for storage consistency.
- Metadata capture (for example source name, acquired timestamp, object key).

### Not Responsible For - Raw Layer

- Type conversion.
- Deduplication.
- Business transformations.
- Data quality judgment beyond file integrity checks.

## Bronze Layer

### Definition - Bronze Layer

Bronze is the canonical conformed layer.

### Responsibilities - Bronze Layer

- Convert data types from source representation into canonical schema.
- Normalize schema and column naming.
- Apply deterministic deduplication.
- Run minimal data quality checks needed for safe downstream use.

### Allowed Operations - Bronze Layer

- Parsing and type casting.
- Standardized timestamp construction (for example market time slot mapping).
- Canonical metadata enrichment.

### Not Responsible For - Bronze Layer

- Reporting aggregates.
- Business KPI logic.
- Cross-domain semantic modeling.

## Silver Layer

### Definition - Silver Layer

Silver is the business-ready analytics layer.

### Responsibilities - Silver Layer

- Apply business rules and domain calculations.
- Build reporting-ready datasets.
- Stabilize joins and semantic naming for analytical consumption.

### Allowed Operations - Silver Layer

- Domain-specific transformations.
- Reusable modeled tables and curated marts.

### Not Responsible For - Silver Layer

- Source archival duties.
- Raw fidelity guarantees.

## Cross-Layer Principles

- Data must move forward by contract, not by implicit assumptions.
- Each layer must be independently testable.
- Side effects are isolated to orchestration boundaries.
- Rebuildability is mandatory from upstream source-of-truth layers.

## Source of truth

RawデータをSource of truthにする
BronzeテーブルやSilverテーブルは変換や読み込み処理が入るので、再構築される可能性がある。

## State ownership



## Layer Entry / Exit Criteria

## Partitioning / Granularity View

| Dataset | Layer  | Granularity | Partition Column | Transform |
|---|---|---|---|---|
| JEPX | Silver | 1 row = delivery_date × time_code × area | delivery_date | (none yet — see `docs/tasks/tasks.md` §8.1) |
| OCCTO | Bronze | 1 row = power_plant_code × unit_name × target_date (wide, 48 timeslot columns) | (none) | (none) |
| OCCTO | Silver | 1 row = power_plant_code × unit_name × target_date × time_code (long, unpivoted) | target_date | day |

OCCTO silverは`day(target_date)`でパーティションを切る。long化により年間約2,600万行規模になるため、
`year`だと日次実行のたびに年間ファイル全体を書き換えてしまう。`day`にすることで、日次実行の書き込み
コストが1パーティション（1日分）に留まり、蓄積した履歴量に依存しなくなる。詳細は
`docs/tasks/plan_occto_pipeline.md` Phase 1／Phase 4-6 を参照。

## Evolution Policy

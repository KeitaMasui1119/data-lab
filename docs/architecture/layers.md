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

## Evolution Policy

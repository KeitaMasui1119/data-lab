# Architecture Documentation Structure

## Purpose

This directory contains architecture decisions and design principles for the data platform.

The goal is to document:

- Layer responsibilities
- Data flow
- State ownership
- Replay strategy
- Orchestration strategy

These documents are intended to guide future implementation and AI-assisted development.

---

# architecture/

```text
architecture/
├── layers.md
├── pipeline_flow.md
├── metadata_strategy.md
├── replay_strategy.md
└── orchestration_strategy.md
```

---

# 1. layers.md

## Purpose

Define responsibilities and boundaries for each layer.

## Decisions to Make

### Raw Layer

Questions:

- What is the purpose of Raw?
- What data formats are allowed?
- Is Raw immutable?
- Is transformation allowed?

Expected decisions:

```text
Raw is an immutable source archive.

Responsibilities:
- Preserve source fidelity
- Store source snapshots
- Support replay

Not responsible for:
- Type conversion
- Deduplication
- Business transformations
```

---

### Bronze Layer

Questions:

- What transformations are allowed?
- How much cleansing is permitted?

Expected decisions:

```text
Bronze contains canonical records.

Responsibilities:
- Type conversion
- Schema normalization
- Deduplication
- Minimal data quality checks

Not responsible for:
- Aggregations
- Business logic
```

---

### Silver Layer

Questions:

- What makes a dataset Silver?
- What transformations belong here?

Expected decisions:

```text
Silver contains business-ready datasets.

Responsibilities:
- Business rules
- Domain calculations
- Reporting-ready structures
```

---

# 2. pipeline_flow.md

## Purpose

Describe how data moves through the platform.

## Decisions to Make

### Pipeline Stages

```text
Source
→ Raw
→ Bronze
→ Silver
```

---

### For each stage define:

- Input
- Output
- Trigger
- Failure behavior

Example:

```text
Source → Raw

Input:
- JEPX source file

Output:
- Snapshot file
- Metadata record

Failure:
- No Raw update
```

---

# 3. metadata_strategy.md

## Purpose

Define where state is stored.

This is one of the most important documents.

## Decisions to Make

### What metadata exists?

Examples:

```text
- Snapshot metadata
- Processing state
- Pipeline execution logs
- Schema metadata
```

---

### Ownership

Example:

| State | Owner |
|---------|---------|
| Latest snapshot | Raw metadata |
| Processed snapshot | Bronze metadata |
| Schema version | Bronze metadata |

---

### Storage Technology

Initial decision:

```text
Store metadata as Parquet files.

Future migration to Iceberg is possible.
```

---

# 4. replay_strategy.md

## Purpose

Define recovery and rebuild procedures.

## Decisions to Make

### Replay Rules

Questions:

- Can Bronze be rebuilt?
- Can Silver be rebuilt?
- What is the source of truth?

Expected decisions:

```text
Source of truth:
Raw layer

Bronze:
Rebuildable from Raw

Silver:
Rebuildable from Bronze
```

---

### Failure Recovery

Document:

```text
If Bronze corruption occurs:

1. Delete Bronze partition
2. Reload from Raw snapshots
3. Reprocess metadata state
```

---

# 5. orchestration_strategy.md

## Purpose

Define how pipelines are orchestrated.

## Decisions to Make

### Current Approach

```text
Python orchestrator

pl_jepx.py
    ├── source_to_raw()
    ├── raw_to_bronze()
    └── bronze_to_silver()
```

---

### Future Approach

```text
Dagster
```

---

### Pipeline Ownership

Define:

- Dataset pipeline structure
- Dependency management
- Retry policy
- Scheduling approach

---

# Completion Criteria

The architecture is considered defined when:

- Layer responsibilities are documented
- Data flow is documented
- State ownership is documented
- Replay strategy is documented
- Orchestration strategy is documented

Implementation details are intentionally excluded.

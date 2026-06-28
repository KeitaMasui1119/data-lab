# Metadata Strategy

## Purpose

Define what state exists, who owns it, and where it is stored.

## Metadata Categories

- Snapshot metadata: source, object key, acquired timestamp, content fingerprint.
- Processing state: stage-level status for each snapshot.
- Pipeline execution logs: run identifier, step status, error summary, durations.
- Schema metadata: canonical schema version and evolution history.

## Ownership Model

| State | Owner |
| --- | --- |
| Latest available snapshot | Raw metadata |
| Snapshot ingestion status | Bronze metadata |
| Silver model refresh status | Silver orchestration metadata |
| Schema version lineage | Bronze metadata |
| Pipeline run history | Orchestrator execution metadata |

## Storage Decision

### Initial State

- Metadata is stored as Parquet files under managed metadata paths.
- Storage layout is append-friendly and partitionable by date and dataset.

### Future State

- Migration to Iceberg-backed metadata tables is allowed when consistency and query requirements increase.

## Metadata Design Principles

- Metadata is immutable at the event level (append logs, avoid in-place mutation where possible).
- Every state transition must be attributable to a run identifier.
- Metadata schema changes require backward-compatible evolution.
- Metadata must be queryable for replay, audit, and troubleshooting.

## Minimum Required Fields

- dataset_id
- source_snapshot_id
- execution_id
- stage
- status
- event_time_utc
- error_code (nullable)
- error_message (nullable)

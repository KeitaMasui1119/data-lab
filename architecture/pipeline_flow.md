# Pipeline Flow

## Purpose

Describe how data moves from external sources to analytics-ready outputs.

## Canonical Stage Flow

Source -> Raw -> Bronze -> Silver

## Stage Contracts

## Source -> Raw

### Input

- External provider files or API responses (for example JEPX, OCCTO, utility demand files).

### Output

- Immutable snapshot object in storage.
- Snapshot metadata record.

### Trigger

- Scheduled run or manual replay command.

### Failure Behavior

- No Raw write on acquisition failure.
- Error state recorded in execution log.
- Downstream stages are blocked for the failed snapshot.

## Raw -> Bronze

### Input

- Raw snapshot objects and snapshot metadata.

### Output

- Canonical Bronze records aligned to managed schema.
- Processing metadata (ingestion timestamp, execution identifier, source key).

### Trigger

- Successful Raw snapshot detection.

### Failure Behavior

- No partial commit of invalid batch.
- Failure state recorded against source snapshot.
- Replay is allowed from the same Raw snapshot.

## Bronze -> Silver

### Input

- Canonical Bronze tables.

### Output

- Business-ready Silver datasets and derived domain tables.

### Trigger

- Bronze update completion or orchestrator command.

### Failure Behavior

- Silver outputs for the failed run are not promoted.
- Failure state is logged with step-level context.
- Rebuild can be executed once blocking issue is fixed.

## Operational Principles

- Pipeline steps are idempotent per input snapshot.
- Stage boundaries are explicit and logged.
- Retries are managed at step level, not hidden inside transformations.

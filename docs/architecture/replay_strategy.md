# Replay Strategy

## Purpose

Define rebuild and recovery rules for each layer.

## Source of Truth

- Raw layer is the source of truth for platform replay.

## Rebuild Rules

## Bronze Rebuildability

- Bronze is fully rebuildable from Raw snapshots.
- Rebuild unit is dataset + snapshot range.
- Rebuild must recreate deterministic outputs for the same input and schema.

## Silver Rebuildability

- Silver is rebuildable from Bronze canonical tables.
- Rebuild scope can be full refresh or bounded incremental window.

## Recovery Procedures

## Bronze Corruption Recovery

1. Isolate affected Bronze partition or snapshot window.
2. Remove invalid Bronze data for the affected scope.
3. Reprocess from corresponding Raw snapshots.
4. Reconcile processing metadata and mark recovery completion.

## Silver Corruption Recovery

1. Isolate affected Silver models or partitions.
2. Re-run transformation from verified Bronze inputs.
3. Validate row counts, key constraints, and freshness.
4. Publish only after validation checks pass.

## Replay Preconditions

- Input snapshots are available and immutable.
- Required schema version and transformation logic are available.
- Replay execution is traceable by execution identifier.

## Replay Guarantees

- No hidden dependency on volatile external state.
- Replayed outputs can be audited against source snapshots.
- Recovery actions are logged in metadata.

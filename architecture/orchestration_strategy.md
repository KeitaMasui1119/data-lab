# Orchestration Strategy

## Purpose

Define how dataset pipelines are orchestrated, retried, and scheduled.

## Current Approach

- Python-based orchestrator coordinates stage execution.
- Pipeline shape is sequential by contract:
  - source_to_raw()
  - raw_to_bronze()
  - bronze_to_silver()

## Pipeline Ownership

- Each dataset pipeline owns:
  - Stage definitions and contracts.
  - Validation checkpoints.
  - Retry and failure policies.
  - Scheduling configuration.

## Dependency Management

- Upstream stage completion is required before downstream execution.
- Cross-dataset dependencies are explicit and declared in orchestration configuration.
- Hidden runtime coupling is prohibited.

## Retry Policy

- Retries apply per step with bounded attempts.
- Non-transient errors require operator intervention.
- Idempotency is mandatory for retried steps.

## Scheduling Approach

- Primary scheduling is periodic batch aligned to source publication cadence.
- Manual backfill and replay triggers are supported.
- Scheduling metadata is recorded for audit and troubleshooting.

## Future Direction

- The architecture allows migration to a workflow orchestrator platform (for example Dagster) without changing stage contracts.
- Migration target must preserve run metadata, replay semantics, and dependency declarations.

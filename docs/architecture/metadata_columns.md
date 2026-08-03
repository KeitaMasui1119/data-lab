# Metadata Columns

## Purpose

Define standard metadata columns attached to all tables across layers.

## Scope

- Applies to all tables in Bronze and Silver layers.
- Excludes business columns (defined per pipeline).
- Excludes Raw layer (file-based storage, no tabular metadata).

---

## Common Metadata Columns (All Layers)

Attached to every Bronze and Silver table.

| Column Name    | Type     | Nullable | Description                              |
|---------------|----------|----------|------------------------------------------|
| source        | String   | No       | Source file name or API URL              |
| status        | String   | No       | Processing state. See status definitions |
| ingestion_time | Datetime | No      | Pipeline execution timestamp             |
| ingestion_date | Date     | No      | Pipeline execution date (for partitioning)|
| execution_id  | String   | No       | UUID assigned at pipeline startup        |

---

## Silver-Only Metadata Columns

Attached to Silver tables only.

| Column Name           | Type     | Nullable | Description                                      |
|----------------------|----------|----------|--------------------------------------------------|
| record_ingestion_time | Datetime | No      | Timestamp when record was first written to Silver |
| record_updated_time   | Datetime | Yes     | Timestamp of last update. Null means never updated|
| is_deleted            | Boolean  | No      | Soft delete flag. True = logically deleted        |

---

## Status Definitions

| Value   | Layer          | Description                                          |
|---------|---------------|------------------------------------------------------|
| new     | Bronze        | Record first written to Bronze. Not yet loaded to Silver |
| loaded  | Bronze/Silver | Bronze: Record has been loaded to Silver. Silver: Record first written from Bronze |
| updated | Silver        | Record updated via Upsert in Silver                  |

### Status Transition

```
Bronze write
└─→ status = "new"

Silver upsert success (new record)
├─→ Silver: status = "loaded"
└─→ Bronze: status = "loaded" (updated immediately after Silver write)

Silver upsert success (existing record)
├─→ Silver: status = "updated"
└─→ Bronze: status = "loaded" (updated immediately after Silver write)
```

---

## Deletion Policy

- Physical deletion is avoided to minimize Iceberg file rewrites.
- Logical deletion is applied via `is_deleted` flag (Silver only).
- Deleted records are excluded from analytical queries by convention.

```sql
-- Standard filter for active records
WHERE is_deleted = FALSE
```

---

## execution_id

- Assigned once at pipeline startup.
- Shared across all records written in the same pipeline execution.
- Implemented as UUID (uuid4).

```python
import uuid
execution_id = str(uuid.uuid4())
```

---

## ingestion_date

- Derived from `ingestion_time` at write time.
- Retained as an explicit column for partitioning purposes.

```python
ingestion_date = ingestion_time.date()
```

---

## record_ingestion_time vs ingestion_time

| Column          | Updated On         | Purpose                              |
|----------------|--------------------|--------------------------------------|
| ingestion_time  | Every pipeline run | Track when pipeline executed         |
| record_ingestion_time | First write only | Track when record entered Silver |

---

## Implementation Notes

### Bronze write
```python
metadata = {
    "source":         "spot_summary_2024_20240401.csv",
    "status":         "new",
    "ingestion_time": datetime.now(),
    "ingestion_date": date.today(),
    "execution_id":   str(uuid.uuid4()),
}
```

### Silver upsert (new record)
```python
metadata = {
    "source":                "spot_summary_2024_20240401.csv",
    "status":                "loaded",
    "ingestion_time":        datetime.now(),
    "ingestion_date":        date.today(),
    "execution_id":          execution_id,
    "record_ingestion_time": datetime.now(),
    "record_updated_time":   None,
    "is_deleted":            False,
}
```

### Silver upsert (existing record)
```python
metadata = {
    "source":                "spot_summary_2024_20240401_revised.csv",
    "status":                "updated",
    "ingestion_time":        datetime.now(),
    "ingestion_date":        date.today(),
    "execution_id":          execution_id,
    "record_ingestion_time": existing_record_ingestion_time,  # 変更しない
    "record_updated_time":   datetime.now(),
    "is_deleted":            False,
}
```

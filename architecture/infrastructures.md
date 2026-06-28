# Infrastructure

## Purpose

Define infrastructure configuration for the data platform.

## Scope

- Applies to local development environment only.
- RustFS as object storage.
- SQLite as Iceberg catalog.
- Future migration to REST catalog is anticipated.

---

## Architecture Overview

```
Devcontainer (Python, DuckDB, PyIceberg)
│
├── SQLite Catalog (local)
│   └── catalog/iceberg.db
│
└── RustFS (object storage, Docker)
    └── dlh_dev/
        ├── raw/
        │   ├── jepx/
        │   │   └── {year}/{month}/
        │   └── occto/
        │       └── {year}/{month}/
        └── warehouse/
            ├── bronze/
            │   ├── jepx_spot/
            │   └── occto_power_plant/
            └── silver/
                ├── jepx_spot/
                └── occto_power_plant/
```

---

## Object Storage: RustFS

### Bucket Structure

| Path | Description |
|------|-------------|
| `dlh_dev/raw/{domain}/{year}/{month}/` | Raw layer files |
| `dlh_dev/warehouse/bronze/{table}/` | Bronze Iceberg tables |
| `dlh_dev/warehouse/silver/{table}/` | Silver Iceberg tables |

### Raw Layer File Path Convention

```
dlh_dev/raw/{domain}/{year}/{month}/{filename}

# Examples
dlh_dev/raw/jepx/2024/04/spot_summary_20240401.csv
dlh_dev/raw/occto/2024/04/power_plant_20240401.csv
dlh_dev/raw/jma/2024/04/forecast_20240401.json
```

### Docker Compose

```yaml
# docker-compose.yml
services:
  rustfs:
    image: rustfs/rustfs:latest
    container_name: rustfs
    ports:
      - "9000:9000"   # S3 API
      - "9001:9001"   # Console UI
    environment:
      RUSTFS_ACCESS_KEY: ${RUSTFS_ACCESS_KEY}
      RUSTFS_SECRET_KEY: ${RUSTFS_SECRET_KEY}
    volumes:
      - rustfs_data:/data

volumes:
  rustfs_data:
```

### Environment Variables

Managed via `.env.local`.

```bash
# .env.local
RUSTFS_ENDPOINT=http://localhost:9000
RUSTFS_ACCESS_KEY=your_access_key
RUSTFS_SECRET_KEY=your_secret_key
RUSTFS_BUCKET=dlh_dev
```

---

## Iceberg Catalog: SQLite

### Configuration

```python
# catalog/config.py
from pyiceberg.catalog.sqlite import SqliteCatalog

catalog = SqliteCatalog(
    "dev",
    **{
        "uri": "sqlite:///catalog/iceberg.db",
        "warehouse": "s3://dlh_dev/warehouse",
        "s3.endpoint": "http://localhost:9000",
        "s3.access-key-id": os.environ["RUSTFS_ACCESS_KEY"],
        "s3.secret-access-key": os.environ["RUSTFS_SECRET_KEY"],
    }
)
```

### Namespace Structure

```python
# Namespaces
catalog.create_namespace("bronze")
catalog.create_namespace("silver")

# Tables
# bronze.jepx_spot
# bronze.occto_power_plant
# silver.jepx_spot
# silver.occto_power_plant
```

### Local Directory Structure

```
catalog/
└── iceberg.db    ← SQLite catalog file (local, not in RustFS)
```

---

## DuckDB

### Configuration

```python
# DuckDB + RustFS (S3 compatible)
import duckdb

conn = duckdb.connect()
conn.execute("INSTALL httpfs; LOAD httpfs;")
conn.execute("INSTALL iceberg; LOAD iceberg;")

conn.execute(f"""
    SET s3_endpoint='{os.environ["RUSTFS_ENDPOINT"]}';
    SET s3_access_key_id='{os.environ["RUSTFS_ACCESS_KEY"]}';
    SET s3_secret_access_key='{os.environ["RUSTFS_SECRET_KEY"]}';
    SET s3_use_ssl=false;
    SET s3_url_style='path';
""")
```

### Usage

```python
# RustFS上のCSVを直接読み込む
df = conn.execute("""
    SELECT *
    FROM read_csv('s3://dlh_dev/raw/jepx/2024/04/*.csv')
""").pl()

# RustFS上のIcebergテーブルを読み込む
df = conn.execute("""
    SELECT *
    FROM iceberg_scan('s3://dlh_dev/warehouse/bronze/jepx_spot')
""").pl()
```

---

## Migration Path: SQLite → REST Catalog

将来的にRESTカタログへ移行する際の変更箇所を最小化するため、
カタログの初期化は必ず設定ファイル経由で行う。

```python
# catalog/config.py
def get_catalog() -> Catalog:
    catalog_type = os.environ.get("CATALOG_TYPE", "sqlite")

    if catalog_type == "sqlite":
        return SqliteCatalog(
            "dev",
            **{
                "uri": "sqlite:///catalog/iceberg.db",
                "warehouse": f"s3://{os.environ['RUSTFS_BUCKET']}/warehouse",
                "s3.endpoint": os.environ["RUSTFS_ENDPOINT"],
                "s3.access-key-id": os.environ["RUSTFS_ACCESS_KEY"],
                "s3.secret-access-key": os.environ["RUSTFS_SECRET_KEY"],
            }
        )

    if catalog_type == "rest":
        return RestCatalog(
            "prod",
            **{
                "uri": os.environ["REST_CATALOG_URI"],
                "warehouse": os.environ["REST_CATALOG_WAREHOUSE"],
                "credential": os.environ["REST_CATALOG_CREDENTIAL"],
            }
        )

    raise ValueError(f"Unsupported catalog type: {catalog_type}")
```

```bash
# .env.local（開発環境）
CATALOG_TYPE=sqlite

# .env.prod（本番環境）
CATALOG_TYPE=rest
REST_CATALOG_URI=https://catalog.example.com
REST_CATALOG_WAREHOUSE=s3://prod-bucket/warehouse
REST_CATALOG_CREDENTIAL=your_credential
```

---

## Open Questions

- [ ] RustFSのDockerイメージバージョンを確定する
- [ ] SQLiteカタログファイルのバックアップ方針を決める
- [ ] Devcontainerのボリュームマウント設定を確認する
``

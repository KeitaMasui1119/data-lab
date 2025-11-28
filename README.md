# 🚀 Data Engineering Practice & Portfolio: Local Data Lakehouse

<div align="center">

[![uv](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/uv/main/assets/badge/v0.json)](https://github.com/astral-sh/uv)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)

</div>

## 🎯 Project Overview & Portfolio Value

This repository serves as a **training platform and portfolio** designed to acquire and demonstrate **versatile data engineering skills**.

The project focuses on building a **local Data Lakehouse** infrastructure, simulating a cloud environment using **MinIO** (S3-compatible storage) and **DuckDB/dbt**. This approach allows for cost-effective, reproducible development of modern ELT pipelines.

### Key Portfolio Highlights
1.  **Cloud Simulation**: Avoids vendor lock-in and costs by using **MinIO** to replicate core **S3/GCS API** functionality locally.
2.  **Data Lakehouse Proficiency**: Implements the Lakehouse pattern by having **DuckDB** directly query **Parquet files** stored in MinIO, facilitating powerful, SQL-based ELT.
3.  **Code Quality & Efficiency**: Enforces high standards using the **Rust-based ecosystem** (`uv` for ultra-fast package management and `Ruff` for strict, efficient linting).

***

## ⚙️ Technical Stack & Rationale

| Category | Tool | Rationale (Why this tool was chosen) |
| :--- | :--- | :--- |
| **Development** | **Python** | The industry standard for data engineering. Focus on practicing advanced concepts like **Type Hinting** and robust exception handling. |
| **Data Lake** | **MinIO** (Docker) | To master **cloud object storage concepts** (objects, buckets, S3 API) and **Parquet file storage** in a fully isolated, local environment. |
| **Analytics Engine/DWH** | **DuckDB** | Used as the **analytical core** of the Lakehouse. Chosen for its extreme speed in **columnar processing** and ability to query external files directly. |
| **Data Transformation (ELT)** | **dbt (Data Build Tool)** | To practice **Data as Code**. Ensures **data lineage** and **idempotency** by managing complex SQL transformations and dependencies. |
| **Environment/Quality** | **uv, Ruff** | Focus on performance and quality control. **`uv`** provides instant dependency resolution; **`Ruff`** enforces Python best practices efficiently. |

***

## 🗺️ Training Roadmap & Progress

The project structure follows the core categories of a comprehensive data engineering curriculum.

| # | Category | Goal | Status | Relevant Modules/Files |
| :-: | :--- | :--- | :--- | :--- |
| 01 | **Data Scraping** | Learn various methods for data acquisition (requests, APIs, Scrapy). | ✅ Done | `scripts/ingestion.py` |
| 02 | **ETL/ELT** | Master data loading, cleaning, and transformation logic. | 🔄 In Progress | `dbt/models/` |
| 03 | **Data Pipeline** | Practice automation and orchestration techniques. | ⬜ To Do | |
| 04 | **Data Lake** | Build a structured storage layer using MinIO and Parquet. | ✅ Done | `docker-compose.yml` |
| 05 | **Data Warehouse** | Design and build normalized data marts. | ⬜ To Do | |
| 06 | **Data Lakehouse** | Establish the MinIO + DuckDB seamless querying mechanism. | 🔄 In Progress | `scripts/duckdb_connector.py` |
| 07 | **Streaming Data** | Fundamentals of real-time data processing. | ⬜ To Do | |
| 08 | **Data Analytics & Visualization**| Automated reporting and dashboarding. | ⬜ To Do | |
| 09 | **Data Governance/Security** | Data cataloging and access control basics. | ⬜ To Do | |
| 10 | **Monitoring & Logging** | Implementing pipeline health checks and logging. | ⬜ To Do | |

***

## 🛠️ Environment Setup & Usage

The entire platform is defined in **Docker Compose** for a reproducible and isolated development environment.

### 1. Setup Instructions

1.  Ensure **Docker Desktop** is running.
2.  Open the repository in VS Code and select **Rebuild and Reopen in Container**. (This launches the `app` service and the `minio` service together).

### 2. MinIO Access Details

Use these credentials to connect DuckDB or Python S3 clients from within the Dev Container.

| Item | Value | Note |
| :--- | :--- | :--- |
| **MinIO API Endpoint** | `minio:9000` | The service name within the Docker network. |
| **MinIO Console (Web UI)** | `http://localhost:9001` | Access this URL from your host machine. |
| **Access Key** | `minioadmin` | |
| **Secret Key** | `minioadmin` | |

### 3. Execution Flow

Use the terminal inside the Dev Container to run the pipeline steps.

```sh
# 1. Synchronize Dependencies (using uv)
uv sync

# 2. Extract & Load to MinIO (Raw Layer)
# Runs the scraping script and uploads Parquet/CSV to MinIO (e.g., s3://datalake/raw/...)
uv run python scripts/ingestion.py

# 3. Transform (ELT with dbt)
# Launches dbt, uses DuckDB to read raw data, and writes clean data to the Curated Layer.
dbt run

# 4. Analytics / Query
# DuckDB is used to query the Parquet files in the Curated Layer directly.
uv run python scripts/analytics/query_data.py

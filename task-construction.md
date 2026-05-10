# 個人開発向けデータパイプライン設計ベストプラクティス

## 背景

実務では以下のような構成になっている。

- **Azure Databricks**
  - Bronze / Silver などレイヤごとに Notebook を分割
  - Source → Bronze、Bronze → Silver などを Notebook 単位で実装

- **Azure Data Factory (ADF)**
  - Notebook 間の依存関係を管理
  - パイプライン全体の orchestration を担当

一方、個人開発では Notebook + GUI orchestration がないため、
これを **Python module + CLI orchestration** に置き換える。

---

# 基本思想

## 実務との対応関係

| 実務 | 個人開発 |
|---|---|
| Databricks Notebook | Python module / function |
| Azure Data Factory Pipeline | CLI orchestration layer |
| Pipeline parameters | CLI args / config |
| Secrets / Linked Services | `.env` + config |

つまり、

- **Notebook = task**
- **ADF = orchestration**

として考える。

---

# 推奨アーキテクチャ

## 1. Task を Python module として管理する

Notebook の代わりに、
各処理を Python module / function に分割する。

例:

```text
scrape_jepx
raw_to_bronze
bronze_to_silver
run_dbt_staging
run_dbt_silver

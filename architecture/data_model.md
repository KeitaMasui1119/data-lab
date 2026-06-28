# Data Model

## Purpose

Define namespace, primary keys, and partition design for all tables across layers.

## Scope

- Applies to Bronze and Silver layers.
- Excludes Raw layer (file-based storage).
- Excludes Gold layer (to be defined after Silver is stable).

---

## Namespace Design

### Convention

```
{layer}.{domain}_{entity}
```

### Namespace List

| Layer   | Namespace |
|---------|-----------|
| Bronze  | bronze    |
| Silver  | silver    |

---

## Table Definitions

### 1. JEPX Spot Market

**Source**: JEPX スポット市場データ

| Layer  | Table Name       |
|--------|-----------------|
| Bronze | bronze.jepx_spot |
| Silver | silver.jepx_spot |

**Primary Keys**

| Column      | Type   | Description       |
|-------------|--------|-------------------|
| trade_date  | Date   | 約定日             |
| time_slot   | Int    | コマ番号（1〜48）   |
| area_code   | String | エリアコード        |

**Partition Design**

| Layer  | Partition Column | Transform | Reason                    |
|--------|-----------------|-----------|---------------------------|
| Bronze | ingestion_date  | Year      | パイプライン管理目的         |
| Silver | trade_date      | Year      | 分析クエリの年月絞り込みに対応 |

**Notes**
- area_codeはパーティションに含めない
- Icebergのカラム統計によるファイルスキップで対応する

---

### 2. OCCTO Power Plant

**Source**: OCCTO 発電所データ

| Layer  | Table Name              |
|--------|------------------------|
| Bronze | bronze.occto_power_plant |
| Silver | silver.occto_power_plant |

**Primary Keys**

| Column       | Type   | Description  |
|--------------|--------|--------------|
| area_code    | String | エリアコード  |
| plant_id     | String | 発電所ID      |
| plant_number | String | 発電所番号    |
| record_date  | Date   | 実績日        |

**Partition Design**

| Layer  | Partition Column | Transform | Reason           |
|--------|-----------------|-----------|------------------|
| Bronze | ingestion_date  | Year      | パイプライン管理目的 |
| Silver | record_date     | Year      | 実績日での絞り込み  |

---

### 3. Utility Demand / Forecast

**Source**: 各電力会社公開データ（需給・電気予報）

各電力会社ごとに独立したテーブルとして管理する。

| Layer  | Table Name              | Company   |
|--------|------------------------|-----------|
| Bronze | bronze.tepco_demand     | 東京電力   |
| Silver | silver.tepco_demand     | 東京電力   |
| Bronze | bronze.kepco_demand     | 関西電力   |
| Silver | silver.kepco_demand     | 関西電力   |
| Bronze | bronze.chubu_demand     | 中部電力   |
| Silver | silver.chubu_demand     | 中部電力   |

> 他の電力会社も同様の命名規則で追加する。

**Primary Keys（各社共通）**

| Column      | Type   | Description       |
|-------------|--------|-------------------|
| record_date | Date   | 実績日             |
| time_slot   | Int    | コマ番号（1〜48）   |

**Partition Design**

| Layer  | Partition Column | Transform | Reason           |
|--------|-----------------|-----------|------------------|
| Bronze | ingestion_date  | Year      | パイプライン管理目的 |
| Silver | record_date     | Year      | 実績日での絞り込み  |

---

### 4. Weather Forecast

**Source**: 気象庁API

| Layer  | Table Name              |
|--------|------------------------|
| Bronze | bronze.jma_forecast     |
| Silver | silver.jma_forecast     |

**Primary Keys**

| Column          | Type   | Description      |
|-----------------|--------|------------------|
| forecast_date   | Date   | 予報対象日         |
| area_code       | String | 地域コード         |
| forecast_time   | String | 予報時刻           |

> 気象庁APIの地域コードとJEPXエリアコードのマッピングはdbt seedで管理する。

**Partition Design**

| Layer  | Partition Column | Transform | Reason           |
|--------|-----------------|-----------|------------------|
| Bronze | ingestion_date  | Year      | パイプライン管理目的 |
| Silver | forecast_date   | Year      | 予報日での絞り込み  |

---

### 5. Stock Index

**Source**: Yahoo Finance

| Layer  | Table Name            |
|--------|-----------------------|
| Bronze | bronze.yahoo_stock    |
| Silver | silver.yahoo_stock    |

**Primary Keys**

| Column       | Type   | Description     |
|--------------|--------|-----------------|
| trade_date   | Date   | 取引日           |
| ticker       | String | ティッカーシンボル |

**Partition Design**

| Layer  | Partition Column | Transform | Reason           |
|--------|-----------------|-----------|------------------|
| Bronze | ingestion_date  | Year      | パイプライン管理目的 |
| Silver | trade_date      | Year      | 取引日での絞り込み  |

---

## Partition Design Principles

- データ量が少ない場合（年間数十万レコード以下）は年単位を基本とする。
- small file problemを避けるため、細かいパーティションは切らない。
- 高カーディナリティのカラム（area_code等）はパーティションに含めず、Icebergのカラム統計によるファイルスキップで対応する。
- Bronzeは常に`ingestion_date`（年単位）でパーティションを切る。
- Silverはクエリの主要な絞り込みカラムでパーティションを切る。

---

## Mapping Tables (dbt seeds)

Silver層から参照するマッピングテーブル。

| Seed Name          | Description                          |
|--------------------|--------------------------------------|
| area_code          | JEPXエリアコードと名称のマッピング      |
| jma_area_mapping   | 気象庁地域コードとJEPXエリアコードのマッピング |
| utility_company    | 電力会社コードと名称のマッピング        |
| stock_ticker       | ティッカーシンボルと銘柄名のマッピング   |

---

## Open Questions

- [ ] 電力各社の需給データの対象会社を確定する
- [ ] 気象庁APIの地域コード体系を確認する
- [ ] 株価インデックスの対象ティッカーを確定する
- [ ] OCCTOのplant_idとplant_numberの一意性を確認する

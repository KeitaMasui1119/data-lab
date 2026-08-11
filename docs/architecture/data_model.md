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

### 2. OCCTO Unit Generation Actuals

**Source**: OCCTO 発電実績公表システム（ユニット別発電実績）

| Layer  | Table Name                              |
|--------|------------------------------------------|
| Bronze | bronze.occto_unit_generation_actuals      |
| Silver | silver.occto_unit_generation_actuals      |

**Primary Keys**

| Layer  | Columns                                                        | Description |
|--------|-----------------------------------------------------------------|--------------|
| Bronze | power_plant_code, unit_name, target_date                        | 発電所コード・ユニット名・対象日。`unit_name`は空文字になるユニット（単機発電所）が実在するため`COALESCE(unit_name, '')`で正規化してからキーに使う |
| Silver | power_plant_code, unit_name, target_date, time_code              | Bronzeのキーに30分コマ（1〜48、開始時刻基準）を加えたlong粒度のキー |

`area`・`power_plant_name`・`power_generation_method_and_fuel_type`は`power_plant_code`に従属する属性であり、キーには含めない。改訂公表（同一対象日の再公表）は`updated_datetime`で最新版を`ORDER BY updated_datetime DESC`選択する（版管理列であってキーではない）。

**Partition Design**

| Layer  | Partition Column | Transform | Reason           |
|--------|-----------------|-----------|------------------|
| Silver | target_date     | Day       | long化により年間約2,600万行規模になるため、日次実行が1パーティションのみを置換できるよう`day`単位にする（`year`だと日次実行のたびに年間ファイル全体を書き換えてしまう） |

Bronzeは全列string・横持ち48コマのまま保持し、Silverへの変換（unpivot・型付け・`time_code`導出）はPython + DuckDB + PyIcebergで行う（dbtは使わない）。詳細は`docs/tasks/plan_occto_pipeline.md`を参照。

---

### 3. Utility Demand / Forecast（でんき予報）

**Source**: 各電力会社公開データ（でんき予報）

各電力会社ごとに独立したテーブルとして管理する。当初案（`record_date, time_slot`の共通2列PK）は
実ファイルを調査した結果成立しないことが判明し、以下の通り改訂した。`data/electric_forecast/`
配下を調査すると、電力会社ごとに大きく2種類のフォーマットが存在する：

- **リッチなスナップショット形式**（北陸・沖縄・関西で確認）：1日1ファイル、複数セクション構成
  （ピーク時供給力・予備率のサマリブロック×4種類、毎時の実績/予測/使用率/供給力テーブル、
  翌日予想ブロック、5分間隔の実績＋太陽光テーブル）。実績に加え予測値・使用率・供給力を含む、
  本来の「でんき予報」に相当するデータ。
- **単純な実績のみの時系列**（東京電力・中部電力・中国電力等の過去アーカイブ）：`DATE,TIME,実績(万kW)`
  のみのフラットな時系列。予測・使用率・供給力データはない。

このため「各社共通の2列PK」は成立せず、フォーマットごと（場合によっては会社ごと）に個別のBronzeスキーマが必要。

#### 3.1 Hokuriku（北陸電力）— リッチなスナップショット形式、パイロット実装済み

| Layer  | Table Name                  |
|--------|------------------------------|
| Bronze | bronze.hokuriku_denki_yohou  |

**設計**: ソースファイル（`juyo_05_YYYYMMDD.csv`）はセクションごとに粒度が異なる
（日次サマリ1行／毎時24行／5分間隔288行）複数セクション構成のレポートファイル。
Bronzeではファイル全体を`target_date`単位で1行に横持ちフラット化する（OCCTOの
48コマ列パターンを踏襲し、毎時×4指標×24=96列、5分間隔×2指標×288=576列、
日次サマリ約20列×2（当日／翌日）を横持ち）。列数716（＋監査列5）。
スキーマCSV: `configuration/iceberg/schema/bronze/hokuriku_denki_yohou.csv`。

**Primary Keys**

| Layer  | Columns     | Description |
|--------|-------------|--------------|
| Bronze | target_date | ファイル名由来の対象日（ファイル内にDATE列はあるがセクションごとに分散しており行の一意キーにはならない）。同日内の複数回更新は`file_updated_at`と`source_data`（スナップショットのオブジェクトキー）で追跡し、Silverで最新版を選択する想定。 |

**Partition Design**: 現時点で未設定（既存のOCCTO/JEPX Bronzeスキーマも
`partition_transform`は未使用のため、方針が定まるまで踏襲）。

**未解決**: 東京電力・関西電力・北海道電力など他社への横展開（フォーマット調査未了）。
リッチ形式とシンプル形式のどちらを各社で採用するかは会社ごとに個別判断が必要。Raw取り込み・
Bronze取り込みスクリプト・Silver変換は未実装（スキーマのみ確定）。

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

- [x] ~~電力各社の需給データの対象会社を確定する~~ →
      北陸電力をパイロットとして確定（[3.1](#31-hokuriku北陸電力-リッチなスナップショット形式パイロット実装済み)参照）。
      他社（東京・関西・北海道等）は個別調査が必要で未確定。
- [ ] 気象庁APIの地域コード体系を確認する
- [ ] 株価インデックスの対象ティッカーを確定する
- [x] ~~OCCTOのplant_idとplant_numberの一意性を確認する~~ →
      実データ（1日・全国471レコード）で`(power_plant_code, unit_name, target_date)`の重複ゼロを確認し、
      `area`をキーに含める必要はないと判断。2024-03-25〜2026-08-09の実データ backfill（約42万bronze行、
      約1,968万silver行）でも重複キー違反は発生していない。

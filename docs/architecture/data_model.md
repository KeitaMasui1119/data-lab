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

でんき予報サイトで取得できるデータは大きく3分類に分かれる。英訳は以下の通り統一する：

| 分類 | 英訳 |
|------|------|
| 電力使用状況 | `power_usage` |
| 需給実績 | `supply_demand_actuals` |
| 系統の需給 | `grid_supply_demand` |

各電力会社ごとに独立したテーブルとして管理する。当初案（`record_date, time_slot`の共通2列PK）は
実ファイルを調査した結果成立しないことが判明し、以下の通り改訂した。`data/electric_forecast/`
配下を調査すると、電力会社ごとに大きく2種類のフォーマットが存在する：

- **リッチなスナップショット形式**（北陸・沖縄・関西で確認）：1日1ファイル、複数セクション構成
  （ピーク時供給力・予備率のサマリブロック×4種類、毎時の実績/予測/使用率/供給力テーブル、
  翌日予想ブロック、5分間隔の実績＋太陽光テーブル）。実績に加え予測値・使用率・供給力を含む、
  上表の「電力使用状況（`power_usage`）」に相当するデータ。
- **単純な実績のみの時系列**（東京電力・中部電力・中国電力等の過去アーカイブ）：`DATE,TIME,実績(万kW)`
  のみのフラットな時系列。予測・使用率・供給力データはない。

このため「各社共通の2列PK」は成立せず、フォーマットごと（場合によっては会社ごと）に個別のBronzeスキーマが必要。
「需給実績（`supply_demand_actuals`）」は東北・中国・四国電力でRaw/Bronze実装済み（3.2参照、東京電力は
別途検討中）。「系統の需給（`grid_supply_demand`）」は未調査・未実装（`docs/tasks/tasks.md`参照）。

#### 3.1 Hokuriku（北陸電力）— 電力使用状況（`power_usage`）、リッチなスナップショット形式、Raw/Bronze/Silver実装済み

**Source URL**: `https://www.rikuden.co.jp/nw/denki-yoho/csv/juyo_05_{YYYYMMDD}.csv`
（単純GET、セッション事前準備不要。2020-04-01以降のみ提供）。過去のスクレイピングプロトタイプ
（`src/Jupyter/scraping_prototypes/electric_forecast_scraping.ipynb`）で確認。

**設計**: ソースファイル（`juyo_05_YYYYMMDD.csv`）はセクションごとに粒度が異なる
（日次サマリ計42列／毎時96列／5分間隔576列）複数セクション構成のレポートファイル。
当初は716列の単一Bronzeテーブル案だったが、扱いにくさと性質の異なるブロックの混在を理由に
**3テーブルに分割**した（Bronzeは1ファイル=1行のアトミックな着地、という既存原則からは
意図的に逸脱しており、1ファイルの取り込みが3つの独立したIcebergテーブルへの非アトミックな
書き込みになる点はトレードオフとして許容している）。

| Layer  | Table Name                             | 内容                                   | 列数（PK含む） |
|--------|-----------------------------------------|-----------------------------------------|----------------|
| Bronze | bronze.power_usage_hokuriku_daily_summary | 当日/翌日ピーク4ブロック×2＋最大使用率 | 44             |
| Bronze | bronze.power_usage_hokuriku_hourly        | 毎時テーブル（24行×4指標）             | 98             |
| Bronze | bronze.power_usage_hokuriku_interval5     | 5分間隔テーブル（288行×2指標）         | 578            |

各Bronzeパーサ（`src/pipeline/bronze/source_to_bronze_power_usage_hokuriku.py`の`parse_snapshot()`）は
OCCTOの48コマ列パターンと同様に生ファイルの構造をそのまま保持しつつ、セクション（空行区切りの
ブロック）順序に基づいて1ファイルを3行（daily_summary/hourly/interval5各1行）に展開する。
毎時テーブルの供給力列は列ラベルがファイル年代により変わる（`供給力(万kW)`/`供給力想定値(万kW)`）
ため、ヘッダ文字列ではなく列位置でパースする。実データ2,083ファイル（2020-2025）で検証済み
（99.95%成功、唯一の失敗は真の404 HTMLページが保存されたファイル）。
スキーマCSV: `configuration/iceberg/schema/bronze/power_usage_hokuriku_{daily_summary,hourly,interval5}.csv`。

**Primary Keys**

| Layer  | Columns                     | Description |
|--------|------------------------------|--------------|
| Bronze（3テーブル共通） | target_date, file_updated_at | `target_date`はファイル名由来の対象日。`file_updated_at`はファイル1行目のUPDATE時刻文字列（生のまま保持）で、同日内の複数回更新を`source_data`（スナップショットのオブジェクトキー）とあわせて追跡し、Silverで最新版を選択する想定。 |

**Partition Design**: 現時点で未設定（既存のOCCTO/JEPX Bronzeスキーマも
`partition_transform`は未使用のため、方針が定まるまで踏襲）。

**実装状況**: Raw取り込み（`src/pipeline/raw/source_to_raw_power_usage_hokuriku.py`、
OCCTOと同じSHA256差分検知＋manifest＋ingestion_logパターン）、Bronze取り込み（3テーブルへの
専用パーサ＋append）、`src/main.py`の`scrape-power-usage-hokuriku`・
`ingest-power-usage-hokuriku-raw-to-bronze`コマンドまで実装済み。テーブルは
`python -m setup.manage_iceberg table create --name bronze.power_usage_hokuriku_<name> --csv ...`
で事前作成が必要（`ingest_power_usage_hokuriku()`は`provision_table()`を呼ばない、
OCCTO/JEPXと同じ設計）。`scrape-power-usage-hokuriku`の`--target-date`省略時のデフォルトは
「JSTで前日」（OCCTOと同じ方針）。当日分のスナップショットは実行中に随時更新される未確定データ
（実測: 当日午後に取得すると残り時間帯・翌日予想ブロックが空文字になる）で、対象日のデータは
翌日午前0時過ぎ（実測UPDATE時刻: 例`2020/04/02 00:10 UPDATE`）にならないと確定しないため。

**Silver**: Bronzeの3テーブルに1:1対応する3テーブル（`silver.power_usage_hokuriku_{daily_summary,hourly,interval5}`）。
`daily_summary`はunpivotなしで型付けのみ（容量/需要→long、%→double、時間帯・更新日時系は
文字列のまま）。`hourly`/`interval5`はOCCTOの48コマUNPIVOTパターンを踏襲しつつ、1コマに
複数指標（毎時4種／5分間隔2種）があるため指標ごとに個別UNPIVOTしてから`(target_date, コマ)`で
再JOINする。3テーブルとも`target_date`の複数リビジョンは`file_updated_at`最新優先で1件に
デデュープしてから書き込む（`common/silver_write.py`のwindow-replaceを再利用）。
実データ全件（2,082日分）で変換・値検証済み（コンマパディング区切り年代・当日未確定空値・
404破損ファイルの欠落日、いずれも想定通り反映）。パーティションはJEPXと同じ`year`
（OCCTOのようなday分割が必要な行数規模ではないため）。CLI:
`src/main.py`の`ingest-power-usage-hokuriku-bronze-to-silver`。
スキーマCSV: `configuration/iceberg/schema/silver/power_usage_hokuriku_{daily_summary,hourly,interval5}.csv`。

**未解決**: 東京電力・関西電力・北海道電力など他社への横展開（フォーマット調査未了）。
リッチ形式とシンプル形式のどちらを各社で採用するかは会社ごとに個別判断が必要。オーケストレーター
（Raw→Bronze→Silverを1コマンドで実行）は未実装。

#### 3.2 東北・中国・四国電力 — 需給実績（`supply_demand_actuals`）、単純な実績時系列、Raw/Bronze実装済み

**Source URL**（年単位でまとめて公開、北陸のような日別URLではない。実データで確認済み）:

| 会社 | URL | ヘッダー構成 |
|------|-----|-------------|
| 東北電力 | `https://setsuden.nw.tohoku-epco.co.jp/common/demand/juyo_{year}_tohoku.csv` | UPDATE行→ヘッダー行（空行なし） |
| 中国電力 | `https://www.energia.co.jp/nw/jukyuu/sys/juyo-{year}.csv` | UPDATE行→空行→ヘッダー行 |
| 四国電力 | `https://www.yonden.co.jp/nw/denkiyoho/csv/juyo_shikoku_{year}.csv` | UPDATE行→空行→ヘッダー行（4列目`供給力想定値(万kW)`あり） |

いずれもcp932、`DATE,TIME,実績(万kW)`のみのフラットな時系列（四国のみ+`供給力想定値(万kW)`）。
北陸と違い「その年のCSVをまるごと公開・日次で1日分の行が追記されていく」形式（JEPXのfiscal-year
CSVと同じ考え方）で、日別に取得できるURLは存在しない。3社とも最新行は常に前日分（当日分はまだ
未確定・未公開）であることをライブで確認済み。

**設計**: 電力会社ごとに独立したBronze/Silverテーブル（`power_usage`と同じ「会社ごと独立」方針。
`supply_demand_actuals`という1つの共有テーブルにまとめる案も検討したが、`power_usage`の方針との
一貫性を優先しエリアごとに分割）。

| Layer  | Table Name（会社ごと） | PK |
|--------|------------------------|-----|
| Bronze | bronze.supply_demand_actuals_{tohoku,chugoku,shikoku} | target_date, target_time |
| Silver | silver.supply_demand_actuals_{tohoku,chugoku,shikoku} | target_date, hour_of_day |

Bronzeは年間CSV全体ではなく、`target_date`（デフォルト: JSTで前日、`power_usage_hokuriku`と同じ方針）
1日分のみを抽出してappendする。ソースは`DATE,TIME,実績(万kW)`の1行1コマ形式で、Hokurikuと違い
`build_schema_exprs()`（source_name一致による単純キャスト）がそのまま使える。DATE表記が会社により
`2026/1/1`（非ゼロ埋め）と`2026/01/01`（ゼロ埋め）で揺れるため、Bronze取り込み時にISO形式へ正規化する。
重複排除は`source_data`ではなく`(company, target_date)`の存在チェック（年次スナップショットは実行の
たびに異なるオブジェクトキーになるため）。

実装: RawとBronzeはともに会社ごとに独立したモジュール
（`src/pipeline/raw/source_to_raw_supply_demand_actuals_{tohoku,chugoku,shikoku}.py`・
`src/pipeline/bronze/source_to_bronze_supply_demand_actuals_{tohoku,chugoku,shikoku}.py`、
`power_usage_hokuriku`と同じ「1社1ファイル」方針。当初はBronzeも3社共通のパラメータ化モジュール
だったが、Rawの分割に合わせて統一）。CLI:
`scrape-supply-demand-actuals-{tohoku,chugoku,shikoku}`・
`ingest-supply-demand-actuals-raw-to-bronze-{tohoku,chugoku,shikoku} --target-date ...`。
実データ（2026-08-14分、3社）でRaw保存・Bronze取り込みまで動作確認済み。

**Silver**: `src/pipeline/silver/bronze_to_silver_supply_demand_actuals.py`
（こちらは3社共通のパラメータ化モジュールのまま。Bronzeが既に`(target_date, target_time)`
1行=1レコードのため、`power_usage_hokuriku`のhourly/interval5と違いUNPIVOTは不要で、型付けと
`hour_of_day`／`delivery_datetime`（JST→UTC変換）の導出のみ。同一`(target_date, hour_of_day)`の
複数リビジョンは`ingestion_time`最新優先でデデュープしてから`write_silver_table()`のwindow-replace
で書き込む。CLI: `ingest-supply-demand-actuals-bronze-to-silver --company ...`。
実データ（2026-08-14分、3社）で変換・値検証済み。

**未解決**: 東京電力は需給実績用の「育っていく年次CSV」が今年分まだ存在せず（過去完了年分のみ）、
代わりに電力使用状況型のリッチなスナップショット（`juyo-d1-j.csv`）しか見つかっていないため、
別方式での実装を検討中。関西・北海道・沖縄・中部電力は需給実績側も未調査。

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
      北陸電力をパイロットとして確定（[3.1](#31-hokuriku北陸電力-電力使用状況power_usageリッチなスナップショット形式rawbronze実装済み)参照）。
      他社（東京・関西・北海道等）は個別調査が必要で未確定。
- [ ] 気象庁APIの地域コード体系を確認する
- [ ] 株価インデックスの対象ティッカーを確定する
- [x] ~~OCCTOのplant_idとplant_numberの一意性を確認する~~ →
      実データ（1日・全国471レコード）で`(power_plant_code, unit_name, target_date)`の重複ゼロを確認し、
      `area`をキーに含める必要はないと判断。2024-03-25〜2026-08-09の実データ backfill（約42万bronze行、
      約1,968万silver行）でも重複キー違反は発生していない。

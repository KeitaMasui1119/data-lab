# Tasks

今後の実装・整理タスク一覧。アーキテクチャドキュメントをもとに整理。

---

## 1. 電力需給データ パイプライン

`data/electric_forecast/` および `data/tepco_juyo/` にローカルファイルが存在する。
これらを Raw → Bronze → Silver の流れで取り込むパイプラインを構築する。

対象電力会社:

| 会社 | データ形式 | ディレクトリ |
|------|-----------|-------------|
| 東京電力 (tepco) | CSV | `data/tepco_juyo/` |
| 中部電力 (chubu) | CSV / ZIP | `data/electric_forecast/chubu/` |
| 中国電力 (chugoku) | Parquet | `data/electric_forecast/chugoku/` |
| 北海道電力 (hokkaido) | ZIP | `data/electric_forecast/hokkaido/` |
| 北陸電力 (hokuriku) | CSV | `data/electric_forecast/hokuriku/` |
| 関西電力 (kansai) | ZIP | `data/electric_forecast/kansai/` |
| 九州電力 (kyushu) | Parquet | `data/electric_forecast/kyushu/` |
| 沖縄電力 (okinawa) | CSV | `data/electric_forecast/okinawa/` |
| 四国電力 (shikoku) | Parquet | `data/electric_forecast/shikoku/` |
| 東北電力 (tohoku) | Parquet | `data/electric_forecast/tohoku/` |

### タスク

- [ ] 各社のファイル形式・カラム構成を調査し、スキーマCSV（`configuration/iceberg/schema/bronze/`）を定義する
- [ ] ローカルファイルを Raw（RustFS）にアップロードするスクリプトを実装する
- [ ] Raw → Bronze Ingestion を実装する（`src/pipeline/bronze/`）
- [ ] Bronze → Silver dbt モデルを実装する（`src/dbt/jepx_power/models/silver/`）
- [ ] 各社オーケストレーターを実装する（`src/orchestration/`）
- [ ] `src/main.py` にコマンドを追加する

---

## 2. 気象庁 天気予報 パイプライン

気象庁APIからデータを取得し、Bronze・Silver に格納する。

### タスク

- [ ] 気象庁APIの地域コード体系を調査し、JEPXエリアコードとのマッピングを確定する
- [ ] `JMAForecastScraper` を実装する（`src/pipeline/raw/`）
- [ ] スキーマCSVを定義する（`configuration/iceberg/schema/bronze/`）
- [ ] Raw → Bronze Ingestion を実装する
- [ ] Bronze → Silver dbt モデルを実装する
- [ ] dbt seed でエリアコードマッピングテーブルを追加する（`jma_area_mapping`）
- [ ] `src/main.py` にコマンドを追加する

---

## 3. Yahoo Finance 株価 パイプライン

株価インデックスを取得し、Bronze・Silver に格納する。

### タスク

- [ ] 対象ティッカーシンボルを確定する
- [ ] `YahooFinanceScraper` を実装する（`src/pipeline/raw/`）
- [ ] スキーマCSVを定義する
- [ ] Raw → Bronze Ingestion を実装する
- [ ] Bronze → Silver dbt モデルを実装する
- [ ] dbt seed でティッカーマッピングテーブルを追加する（`stock_ticker`）
- [ ] `src/main.py` にコマンドを追加する

---

## 4. Silver レイヤー メタデータ対応

`metadata_columns.md` に定義された Silver 専用メタデータ列と、ステータス遷移を実装する。

### タスク

- [ ] Silver テーブルに3列を追加する（`record_ingestion_time`, `record_updated_time`, `is_deleted`）
- [ ] Bronze → Silver 書き込み時のステータス遷移を実装する（`new` → `loaded`）
- [ ] Silver Upsert 時のステータス遷移を実装する（`loaded` → `updated`）
- [ ] Bronze 側のステータスを Silver 書き込み完了後に `loaded` へ更新する処理を実装する
- [ ] 論理削除フィルター（`WHERE is_deleted = FALSE`）を dbt モデルのベースクエリに追加する

---

## 5. オーケストレーター拡張

現状は JEPX のみ。全データセットに対応したオーケストレーターを整備する。

### タスク

- [ ] OCCTO オーケストレーターを実装する（`src/orchestration/occto_pipeline.py`）
- [ ] 電力需給 オーケストレーターを実装する（会社ごと or 統合）
- [ ] クロスデータセット依存関係を宣言する仕組みを設計する
- [ ] リトライポリシー（上限回数・非一時エラーの除外）を各オーケストレーターに組み込む

---

## 6. ドキュメント補完

`layers.md` に未記載のセクションがある。

### タスク

- [ ] `layers.md` の「State ownership」を記述する
- [ ] `layers.md` の「Layer Entry / Exit Criteria」を記述する
- [ ] `layers.md` の「Partitioning / Granularity View」を記述する
- [ ] `layers.md` の「Evolution Policy」を記述する
- [ ] `data_model.md` の Open Questions を解決する
  - [ ] 電力各社の対象会社を確定する
  - [ ] OCCTOの `plant_id` と `plant_number` の一意性を確認する

---

## 7. インフラ整備

### タスク

- [ ] RustFS の Docker イメージバージョンを確定する（`compose.yaml`）
- [ ] SQLite カタログファイル（`catalog/dlh_dev.db`）のバックアップ方針を決める
- [ ] Devcontainer のボリュームマウント設定を確認する

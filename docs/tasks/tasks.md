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

---

## 8. JEPX Silver 書き込み経路の残タスク

FY2005–FY2026 バックフィル時に発生した障害の恒久対応（`upsert` → 区間 `overwrite`）で
出た残件。経緯・実測値は
[`docs/reports/reports_20260808_10c4679e-4370-49d3-9b8c-92f890c5eade.md`](../reports/reports_20260808_10c4679e-4370-49d3-9b8c-92f890c5eade.md)
を参照。コミットは `25c385a`。

### 8.1 Silver テーブルへのパーティション適用（優先度: 高）

**現状の問題。** `provision_table()`（`src/common/iceberg.py`）は `create_table()` に
partition spec を渡しておらず、スキーマCSVの `partition_transform` 列は完全に無視されている。
実測で silver 3テーブルとも `spec: []`（未パーティション）を確認済み。

そのため区間 `overwrite` の削除側は各データファイルの min/max メトリクスでしか枝刈りできず、
窓の境界をまたぐファイルは丸ごと書き換えられる。**書き込みコストがテーブル全体のサイズに
比例したまま**で、「履歴量から切り離す」という恒久対応の狙いは半分しか達成できていない。

実測（全年度実行の直後 = 全22年度が2〜3ファイルに同居した最悪配置で、年度スコープ実行）:
9.3秒 / ピークRSS 1.7GB。現時点では耐えるが、履歴が伸びれば線形に悪化する。

- [ ] `provision_table()` でスキーマCSVの `partition_transform` を partition spec に反映する
- [ ] 既存 silver テーブルの移行方針を決める（`update_spec()` での spec 進化か、作り直しか）
  - spec を進化させても既存データファイルは旧レイアウトのまま残るため、
    再書き込みが必要かを判断する
- [ ] 移行後に同じ最悪配置で再実測し、ファイル書き換えが起きなくなったことを確認する

### 8.2 新しいガードのテスト追加（優先度: 中）

コードレビュー指摘で入れた防御が実挙動確認のみで、回帰防止のテストがない。

- [ ] `ensure_unique_keys()` を全フレームに対し**書き込み前に**一括実行することのテスト
      （3テーブル目で重複を検出した際に、base/block だけ更新済みになる部分適用が起きないこと）
- [ ] `--silver-all-fiscal-years` と `--silver-fiscal-year` の同時指定が
      `parser.error` で弾かれることのテスト（`src/main.py` と
      `src/orchestration/jepx_pipeline.py` の2箇所）

### 8.3 ドキュメントの追従（優先度: 中）

- [ ] `README.md`（152行目付近）が旧仕様のまま。「PyIceberg upserts the result」
      「Every fiscal year is upserted by default」という記述を実態に合わせ、
      `--silver-all-fiscal-years` を追記する。**オーケストレーターの既定が
      「全年度」から「取り込んだ会計年度」に変わったため、運用者が最初に読むこのファイルの
      更新が最優先**
- [ ] `docs/architecture/metadata_columns.md` の削除方針（67行目付近
      「Physical deletion is avoided... Logical deletion is applied via `is_deleted`」）が
      区間物理削除する新実装と矛盾している。方針を改めるか、Silver 書き込みの例外を明記する
- [ ] `docs/tasks/tasks_bts_jepx_sp.md` の「確定済みの設計判断（再検討不要）」表にある
      `upsert 方式: PyIceberg ネイティブ Table.upsert` と
      `実行範囲: 全期間 upsert が既定` は、どちらも今回の変更で覆っている。
      完了済みタスクの記録だが、読んだ人が誤解するため注記を入れる

### 8.4 実行結果判定の厳格化（優先度: 中）

障害2件はいずれも「正常終了」に見えた。さらに現在の実装では、
`delivery_date` が両フォーマットとも解釈できなくなった場合、
`_build_fiscal_year_filter` が violation 判定より前に全行を捨てるため、
`dropped=0 / written=0 / status=success` で Silver が静かに更新されなくなる。

- [ ] `PipelineStepResult` に想定行数と実測行数を持たせ、乖離時に `status="failed"` とする
- [ ] 書き込み0行かつ除外0行を異常として扱う（正常に0行となるケースの切り分けも含めて設計する）

### 8.5 バックフィル用 CLI コマンドの追加（優先度: 中）

今回の22年度バックフィルは使い捨てのシェルループで実施しており、
`docs/architecture/replay_strategy.md` が定める再構築手順が実行可能な形で残っていない。

- [ ] 年度範囲を受け取る一次コマンドを `src/main.py` に実装する
      （例: `backfill-jepx --from-fiscal-year 2005 --to-fiscal-year 2026`）

### 8.6 メタデータ列と変更検知（優先度: 低）

`add_metadata()` は実行ごとに `ingestion_time` / `execution_id` を新規採番するため、
業務値が不変でも全行が「変更あり」と判定される。区間置換に移行したことで
実害は解消しているが、将来 `upsert` 系の処理を書く場合は再燃する。

- [ ] 変更検知の比較対象からメタデータ列を除外する方針を決める

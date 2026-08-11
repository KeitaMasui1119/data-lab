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
      → 北陸電力（パイロット）は完了。`configuration/iceberg/schema/bronze/hokuriku_denki_yohou.csv`
      （716列＋監査列5、`build_table_schema()`/`build_partition_spec()`で検証済み）。詳細は
      `docs/architecture/data_model.md` 3.1 を参照。フォーマットは会社ごとに
      「リッチなスナップショット形式」（北陸・沖縄・関西）と「単純な実績のみの時系列」
      （東京電力・中部電力・中国電力等）の2系統に分かれることが判明したため、他社は個別調査が必要。
- [ ] ローカルファイルを Raw（RustFS）にアップロードするスクリプトを実装する
- [ ] Raw → Bronze Ingestion を実装する（`src/pipeline/bronze/`）
      → 北陸電力は`build_schema_exprs()`（1 CSVカラム→1ターゲット列の単純リネーム）がそのまま使えない
      （ソースが複数セクションのレポート形式で、716列は日次1行に展開する専用パーサが必要）。
      実装時は専用パーサを書く前提で見積もる。
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

- [x] OCCTO オーケストレーターを実装する（`src/orchestration/pl_occto_unit_generation_actuals.py`）
      → 完了。`run-occto-orchestrator` CLI を追加。共通の `PipelineStepResult` は
      `src/orchestration/pipeline_result.py` に切り出し、JEPX/OCCTO 両オーケストレーターで共有。
      詳細は `docs/tasks/plan_occto_pipeline.md` Phase 5-3 を参照
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
  - [x] OCCTOの `plant_id` と `plant_number` の一意性を確認する
        → 解決済み。`(power_plant_code, unit_name, target_date)` で重複ゼロを実データ確認

---

## 7. インフラ整備

### タスク

- [ ] RustFS の Docker イメージバージョンを確定する（`compose.yaml`）
- [ ] SQLite カタログファイル（`catalog/dlh_dev.db`）のバックアップ方針を決める
- [ ] Devcontainer のボリュームマウント設定を確認する
- [x] ~~Silver テーブルの snapshot 保持ポリシーを実装する（orphan ファイル削除）~~ →
      実装・実行済み。詳細は本セクション末尾の注記を参照。

---

### Silver snapshot 保持ポリシー（実装・実行済み）

**問題**: Iceberg の `overwrite()` はマニフェストから古いファイルの参照を外すだけで、
物理ファイルは削除しない（タイムトラベル用に過去 snapshot が参照し続けるため）。

**方針**: `docs/architecture/replay_strategy.md` は「Silver の復旧は Bronze からの
再構築が正の手順」と定めており（snapshot ロールバックは正式な復旧経路ではない）、
Silver 自身の snapshot 履歴を長期保持する必要性は薄い。よって Silver テーブルの
snapshot 保持期間は**直近7日分のみ**とする（直近の誤実行をロールバックできる程度の
運用上のバッファであり、正式な復旧手段ではない。それより古い障害復旧は
replay_strategy.md 通り Bronze からの再構築で行う）。Bronze テーブルの保持方針は
別途検討（本タスクのスコープ外）。

**実装**: PyIceberg 0.11.1 の `table.maintenance.expire_snapshots()` は snapshot
メタデータの削除のみ行い、それによって不要になった物理ファイルの削除
（orphan file removal）は行わない（PySpark 等にある同等機能が PyIceberg には無い）ため、
`src/common/iceberg/maintenance.py` に自前で実装した：

- `expire_old_snapshots()` — `expire_snapshots().older_than(...)` のラッパー。
  branch HEAD（現行 snapshot）は cutoff より古くても常に保護されることを
  ローカルカタログで確認済み。
- `find_orphan_data_files()` — expire 後に**残った全 snapshot**（current だけでなく）の
  マニフェストが参照するファイル集合の和集合を取り、ストレージ上の実ファイル一覧との
  差分を返す。生存中の非current snapshotが参照するファイルを誤って orphan 扱いしない
  ことをテストで担保。
- `delete_orphan_data_files()` — 実際の物理削除（S3 DeleteObjects、1000件単位でバッチ）。
- CLI: `expire-silver-snapshots`（`--older-than-days`既定7、`--delete`未指定ならdry run）。
  `provision-silver-tables` 等と同様スキーマディレクトリ全走査のため、実行対象は
  silver配下の全テーブル（OCCTOも含む）になる点に注意。

**実行結果・訂正**: 実装前に「3テーブル合計99ファイル・約65MBがorphan」と見積もったが、
これは current snapshot のみとストレージを比較した誤った測定だった（生存中の他snapshotが
参照するファイルも誤ってorphanとカウントしていた）。全snapshot横断で正しく計算した結果、
JEPXの3テーブル（base/block/area）の**真のorphanは合計7ファイル**（base 3, block 3,
area 1、ディレクトリプレースホルダー1件含む）で、これを実際に削除した。削除後も
current snapshotのファイル数・行数（base/block 374,400行、area 3,364,896行）は
削除前と完全一致することを確認済み。なお同時に実行したdry runで OCCTO
（`silver.occto_unit_generation_actuals`）に315件のorphanが見つかったが、
今回はJEPXのみを対象とする判断のため未削除のまま残している。

---

## 8. JEPX Silver 書き込み経路の残タスク

FY2005–FY2026 バックフィル時に発生した障害の恒久対応（`upsert` → 区間 `overwrite`）で
出た残件。経緯・実測値は
[`docs/reports/reports_20260808_10c4679e-4370-49d3-9b8c-92f890c5eade.md`](../reports/reports_20260808_10c4679e-4370-49d3-9b8c-92f890c5eade.md)
を参照。コミットは `25c385a`。

### 8.1 Silver テーブルへのパーティション適用（優先度: 高、解決済み）

以下は解決前の状況の記録。移行手順と実測結果はチェックリストを参照。

**現状の問題（コード側は解決済み）。** `provision_table()`（`src/common/iceberg/catalog.py`）は
`create_table()` に partition spec を渡しておらず、スキーマCSVの `partition_transform` 列は
完全に無視されていた。→ **`ba66297`（`docs/tasks/plan_occto_pipeline.md` Phase 1）で解決済み。**
`build_partition_spec()` がスキーマCSVの `partition_transform` を読み、新規テーブル作成時に
`partition_spec` を渡すようになった。OCCTO silver（`silver.occto_unit_generation_actuals`）は
このコードで最初から `day(target_date)` パーティション付きで作成され、実データ backfill
（約1,968万行）でも `spec: [1000: target_date_day: day(3)]` を実測確認済み。

**残っているのは JEPX 側の既存テーブル移行のみ。** `provision_table()` は新規作成時にしか
partition spec を渡さず、既存テーブルの spec 差分は警告ログのみで自動 evolve しない
（意図的な設計。`update_spec()` しても既存データファイルは旧レイアウトのまま残るため）。
実測で JEPX silver 3テーブルは依然 `spec: []`（未パーティション）のまま。

そのため JEPX の区間 `overwrite` の削除側は各データファイルの min/max メトリクスでしか
枝刈りできず、窓の境界をまたぐファイルは丸ごと書き換えられる。**書き込みコストがテーブル全体の
サイズに比例したまま**で、「履歴量から切り離す」という恒久対応の狙いは半分しか達成できていない。

実測（全年度実行の直後 = 全22年度が2〜3ファイルに同居した最悪配置で、年度スコープ実行）:
9.3秒 / ピークRSS 1.7GB。現時点では耐えるが、履歴が伸びれば線形に悪化する。

- [x] `provision_table()` でスキーマCSVの `partition_transform` を partition spec に反映する
- [x] ~~既存 JEPX silver テーブルの移行方針を決める（`update_spec()` での spec 進化か、作り直しか）~~ →
      解決済み。3テーブルとも `delivery_date` に `year` のみ（`area_name` は付与しない。エリア横断
      クエリの方が多い想定のため、列統計によるファイルスキップに任せる方針）。移行手順は
      2ステップ：①`evolve_partition_spec()`（`src/common/iceberg/catalog.py`、新規CLI
      `evolve-silver-partition-spec`）で `update_spec()` によりメタデータのみ追加（データ再書き込み
      不要）。②既存データファイルは旧レイアウトのまま残るため、`ingest-jepx-bronze-to-silver`
      （`--fiscal-year` 省略で全年度）を1回実行し、全履歴を新パーティション配置で書き直した。
      実行結果：base/block 各374,400行、area 3,364,896行、計約12秒。各テーブルとも
      FY2005〜FY2026の22ファイル（1ファイル/年度）に分割された（従来は2〜3ファイルに同居）。
      副産物として、spec evolution後は `provision_table()` の drift 警告が spec_id の違いだけで
      誤検知することが判明し、`fields` 比較に修正（回帰テスト
      `test_provision_table_does_not_warn_after_evolve_partition_spec` 追加）。
- [x] ~~移行後に同じ最悪配置で再実測し、ファイル書き換えが起きなくなったことを確認する~~ →
      確認済み。単一年度スコープ実行（`--fiscal-year 2026`）の前後でファイルパスを比較し、
      22ファイル中 **変更は対象年度の1ファイルのみ**、残り21ファイルは完全に不変だったことを実測。
      書き込みコストがテーブル全体の履歴量ではなく対象年度のみに比例するようになったことを確認。

### 8.2 新しいガードのテスト追加（優先度: 中）

コードレビュー指摘で入れた防御が実挙動確認のみで、回帰防止のテストがない。

- [ ] `ensure_unique_keys()` を全フレームに対し**書き込み前に**一括実行することのテスト
      （3テーブル目で重複を検出した際に、base/block だけ更新済みになる部分適用が起きないこと）
- [ ] `--silver-all-fiscal-years` と `--silver-fiscal-year` の同時指定が
      `parser.error` で弾かれることのテスト（`src/main.py` と
      `src/orchestration/jepx_pipeline.py` の2箇所）

### 8.3 ドキュメントの追従（優先度: 中、解決済み）

- [x] ~~`README.md`（152行目付近）が旧仕様のまま。「PyIceberg upserts the result」
      「Every fiscal year is upserted by default」という記述を実態に合わせ、
      `--silver-all-fiscal-years` を追記する。~~ →
      「5) Build the JEPX silver layer」節を書き換え、区間overwrite方式である旨と、
      `ingest-jepx-bronze-to-silver`（無指定なら全年度）と`run-jepx-orchestrator`
      （既定は取り込んだ会計年度のみ、`--silver-all-fiscal-years`/`--silver-fiscal-year`で変更）
      の既定挙動の違いを明記した。
- [x] ~~`docs/architecture/metadata_columns.md` の削除方針が区間物理削除する新実装と矛盾している。~~ →
      「Implementation Status」節を新設し、Silver専用列（`record_ingestion_time`/
      `record_updated_time`/`is_deleted`）とupsert前提のステータス遷移・削除方針は
      JEPX/OCCTOどちらも未実装であることを明記。「Deletion Policy」節にも、実装済みの
      区間物理削除（overwrite）方式との違いを注記した。
- [x] ~~`docs/tasks/tasks_bts_jepx_sp.md` の「確定済みの設計判断（再検討不要）」表にある
      `upsert 方式: PyIceberg ネイティブ Table.upsert` と
      `実行範囲: 全期間 upsert が既定` は、どちらも今回の変更で覆っている。~~ →
      該当2行を取り消し線付きで残し、後日`upsert`→区間`overwrite`へ置き換わった経緯
      （コミット`5525fab`/PR #72、障害の詳細、`run-jepx-orchestrator`の既定変更）を注記として追加。

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

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
      → 北陸電力（パイロット、電力使用状況＝`power_usage`カテゴリ）は完了。当初は716列＋監査列5の
      単一Bronzeテーブル案だったが、扱いにくさと性質の異なるブロックの混在を理由に3テーブルへ分割：
      `configuration/iceberg/schema/bronze/power_usage_hokuriku/power_usage_hokuriku_{daily_summary,hourly,interval5}.csv`
      （44/98/578列、`build_table_schema()`/`build_partition_spec()`で検証済み）。詳細は
      `docs/architecture/data_model.md` 3.1 を参照。フォーマットは会社ごとに
      「リッチなスナップショット形式」（北陸・沖縄・関西）と「単純な実績のみの時系列」
      （東京電力・中部電力・中国電力等）の2系統に分かれることが判明したため、他社は個別調査が必要。
      でんき予報データは「電力使用状況（`power_usage`）」「需給実績（`supply_demand_actuals`）」
      「系統の需給（`grid_supply_demand`）」の3分類に整理する（`docs/architecture/data_model.md` 3参照）。
      → 需給実績も東北・中国・四国電力で完了。`configuration/iceberg/schema/bronze/supply_demand_actuals/supply_demand_actuals_{tohoku,chugoku,shikoku}.csv`
      （`power_usage`と同じくエリアごとに独立テーブル、共有1テーブル案は不採用）。東京電力は
      今年分の年次アーカイブが未公開で別方式検討中、関西・北海道・沖縄・中部・系統の需給は未調査。
- [ ] ローカルファイルを Raw（RustFS）にアップロードするスクリプトを実装する
- [ ] Raw → Bronze Ingestion を実装する（`src/pipeline/bronze/`）
      → 北陸電力（power_usage）は完了。`build_schema_exprs()`（1 CSVカラム→1ターゲット列の単純リネーム）は
      そのまま使えず（ソースが複数セクションのレポート形式）、
      `src/pipeline/bronze/source_to_bronze_power_usage_hokuriku.py`の`parse_snapshot()`で
      専用パーサを実装（空行区切りのブロック順序に基づき1ファイルを3行に展開、実データ2,083ファイルで
      検証済み）。Rawスクレイパー（`src/pipeline/raw/source_to_raw_power_usage_hokuriku.py`、
      URLは`https://www.rikuden.co.jp/nw/denki-yoho/csv/juyo_05_{YYYYMMDD}.csv`）と、
      `src/main.py`の`scrape-power-usage-hokuriku`/`ingest-power-usage-hokuriku-raw-to-bronze`
      コマンドも実装済み。
      → 東北・中国・四国電力（supply_demand_actuals）も完了。こちらはソースが`DATE,TIME,実績(万kW)`の
      フラットなCSVなので`build_schema_exprs()`がそのまま使える。年次CSV全体から`target_date`
      （デフォルト前日）分だけ抽出してBronzeにappendする方式。RawとBronzeはともに
      `power_usage_hokuriku`と同じ「1社1ファイル」方針で
      `source_to_raw_supply_demand_actuals_{tohoku,chugoku,shikoku}.py`／
      `source_to_bronze_supply_demand_actuals_{tohoku,chugoku,shikoku}.py`の3本ずつに分割
      （Bronzeも当初は3社共通の1モジュールだったが、Rawの分割に合わせて統一）。CLI:
      `scrape-supply-demand-actuals-{tohoku,chugoku,shikoku}`／
      `ingest-supply-demand-actuals-raw-to-bronze-{tohoku,chugoku,shikoku}`（いずれも会社ごと独立
      コマンド）。実データ（2026-08-14分、3社）で動作確認済み。東京電力は未着手
      （電力使用状況型のリッチなスナップショット`juyo-d1-j.csv`から実績列を抜く方式が必要になりそうで別途検討）。
- [ ] Bronze → Silver 変換を実装する（JEPX/OCCTOに倣いPython+DuckDB+PyIcebergで実装、dbtは使わない）
      → 北陸電力は完了。`src/pipeline/silver/bronze_to_silver_power_usage_hokuriku.py`。
      Bronzeの3テーブルに1:1対応する3つのSilverテーブル（`silver.power_usage_hokuriku_{daily_summary,hourly,interval5}`）。
      `hourly`/`interval5`はOCCTOの48コマUNPIVOTパターンを指標ごとに個別UNPIVOT→再JOINする形に拡張。
      `src/main.py`の`ingest-power-usage-hokuriku-bronze-to-silver`コマンドも実装済み。
      実データ全件（2,082日分）で変換・値検証済み。
      → 東北・中国・四国電力（supply_demand_actuals）も完了。RawとBronzeに続きSilverも会社ごとに
      独立したモジュール（`bronze_to_silver_supply_demand_actuals_{tohoku,chugoku,shikoku}.py`、
      当初は3社共通の1モジュールだったが統一）。BronzeがすでにDATE,TIME1行=1レコードのため
      UNPIVOT不要、型付けと`hour_of_day`/`delivery_datetime`導出のみ。CLI:
      `ingest-supply-demand-actuals-bronze-to-silver-{tohoku,chugoku,shikoku}`。
      実データ（2026-08-14分）で検証済み。他社は未着手。
- [ ] 各社オーケストレーターを実装する（`src/orchestration/`）
      → 北陸電力power_usage・東北/中国/四国電力supply_demand_actualsともRaw→Bronze→Silverが
      個別コマンドとして実装済みだが、1コマンドで通しで実行するオーケストレーターは未実装。
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
- [x] 後日追記: `bronze_to_silver_jepx_spot_price.py` の `DELIVERY_DATE_COLUMN` に付いていた
      コメントが「silverテーブルは未パーティション、`provision_table()` は partition spec を
      渡さない」という移行前の記述のまま残っていたため、現状（3テーブルとも
      `year(delivery_date)`）に合わせて書き直した。カタログ実測でも
      `silver.jepx_spot_price_{base,block,area}` は `delivery_date_year: year(...)`、
      `silver.occto_unit_generation_actuals` は `target_date_day: day(3)` を確認済み。

### 8.2 新しいガードのテスト追加（優先度: 中、解決済み）

コードレビュー指摘で入れた防御が実挙動確認のみで、回帰防止のテストがなかった。

- [x] ~~`ensure_unique_keys()` を全フレームに対し**書き込み前に**一括実行することのテスト
      （3テーブル目で重複を検出した際に、base/block だけ更新済みになる部分適用が起きないこと）~~ →
      `tests/pipeline/silver/test_bronze_to_silver_jepx_spot_price.py` に
      `test_run_validates_every_frame_before_writing_any_table` を追加。
      `run_bronze_to_silver_jepx_spot_price()` を通しで実行し、`write_silver_table` を
      呼び出し記録用に差し替えたうえで、3番目の `extract_area_frame` だけ重複キーを持つ
      フレームを返すよう注入する。`ValueError` が上がり、**どのテーブルも書かれていない**
      （記録が空）ことを検証する。対になる `test_run_writes_every_target_table`
      （正常時は3テーブルとも書かれる）も追加し、テストが「そもそも到達していないから
      空」で通ってしまわないようにした。なお重複はステージングSQLからは発生し得ない
      （`(delivery_date, time_code)` で dedup 済み、UNPIVOTは各エリア1回）ため、
      将来の `extract_*` のバグを模した注入という形をとっている。
- [x] ~~`--silver-all-fiscal-years` と `--silver-fiscal-year` の同時指定が
      `parser.error` で弾かれることのテスト（`src/main.py` と
      `src/orchestration/pl_jepx_spot_price.py` の2箇所）~~ →
      CLI側（現 `src/cli/commands/jepx.py`）は `tests/test_main_cli.py` の
      `test_dispatch_validation_exits_before_touching_external_systems` で既にカバー済みだった。
      未カバーだったオーケストレーターモジュール自身の `main()` について
      `tests/orchestration/test_jepx_pipeline_orchestrator.py` に
      `test_main_rejects_both_silver_scope_flags`（`SystemExit` かつ終了コード2、
      さらに `run_jepx_orchestrated_pipeline` が呼ばれていないこと）と、
      ガードが広すぎないことを示す `test_main_accepts_either_silver_scope_flag_alone`
      （片方のみ／どちらも無しの3パターン）を追加した。

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

### 8.4 実行結果判定の厳格化（優先度: 中、解決済み）

障害2件はいずれも「正常終了」に見えた。さらに当時の実装では、
`delivery_date` が両フォーマットとも解釈できなくなった場合、
`_build_fiscal_year_filter` が violation 判定より前に全行を捨てるため、
`dropped=0 / written=0 / status=success` で Silver が静かに更新されなくなっていた。

- [x] ~~`PipelineStepResult` に想定行数と実測行数を持たせ、乖離時に `status="failed"` とする~~ →
      `expected_row_count` / `actual_row_count`（いずれも既定 `None`）を追加。
      行を動かさないステップ（dbt・スクレイピング）は未設定のまま。
      あわせて `STATUS_SUCCESS` / `STATUS_SKIPPED` / `STATUS_FAILED` 定数と
      `has_failed_step()` を `src/orchestration/pipeline_result.py` に置いた。
- [x] ~~書き込み0行かつ除外0行を異常として扱う（正常に0行となるケースの切り分けも含めて設計する）~~ →
      `BronzeToSilverResult` に `staged_row_count`（ステージング関係の全行数、
      正常・異常を問わない）と `valid_row_count` プロパティを追加し、
      `count_staged_rows()` で採取するようにした。これが「スコープに該当行が無かった」と
      「行はあったが検証で落ちた」を区別する。

**判定ルール**（`verify_silver_row_counts()`、`src/orchestration/pipeline_result.py`。
JEPX/OCCTO 両オーケストレーターが呼ぶ共通実装）。次の3つを success ではなく failed とする：

1. `staged_row_count == 0` — Bronze にスコープ内の行が1行も無い。これが本節冒頭の
   静かな失敗そのもの（解釈不能な `delivery_date` は検証より前に会計年度フィルタで
   捨てられるため `dropped=0 / written=0` になる）。
2. `valid_row_count == 0` — ステージングした行が全て検証で落ちた。何も書かれないので
   対象ウィンドウには前回実行の値が残ったままになる。
3. `actual != expected` — 期待行数と実際の書き込み行数が食い違う
   （ステージングからテーブルまでの間で行が失われた）。

期待行数の取り方はデータセットごとに異なる：

- **JEPX**: 検証通過行数 = `silver.jepx_spot_price_base` の行数。base のみを見るのは、
  block も同数・area はエリア数倍という関係のうち、base だけが「1検証行=1行」で
  UNPIVOT の枚数に依存しないため（area の UNPIVOT は NULL のエリア価格を落とすので
  固定倍にならない）。
- **OCCTO**: 検証通過行数 × 48（`expected_silver_row_count` プロパティ）。
  こちらの UNPIVOT は `INCLUDE NULLS` なので、空きコマも1行になり倍率が厳密に確定する。

**「正常に0行」の切り分け**: どちらのパイプラインにも該当ケースが無いと判断した。
オーケストレーターは直前に取り込んだスコープに対して Silver を走らせるので Bronze は
必ず行を持つ。単体実行で未取り込みのスコープを指定した場合も、成功と報告されるより
失敗として気づけた方がよい。よって carve-out 用のフラグは設けていない（判断理由は
`verify_silver_row_counts()` の docstring にも記載）。

**終了コード**: `status="failed"` がログに出るだけでは終了コード0のままで、
「正常終了に見える」問題が半分残るため、`has_failed_step()` で失敗ステップを検出したら
`SystemExit(1)` とするようにした（`src/cli/commands/{jepx,occto}.py` の
オーケストレーターハンドラと、各 `pl_*.py` の `main()` の計4箇所）。あわせて
`status` の文字列リテラルを `STATUS_SUCCESS` / `STATUS_SKIPPED` / `STATUS_FAILED`
定数に置き換えた。

**実データ確認**（いずれも読み取りのみ、Silver への書き込みなし）:

- JEPX: FY1999（該当行なし）→ `staged=0 dropped=0` で failed。
  FY2024 → `staged=17,520`（365日×48コマ）、FY2026 → `staged=6,288` で success。
  書き込み行数を1行減らすと failed。
- OCCTO: 1999年範囲（該当行なし）→ failed。全期間 → `staged=421,071 dropped=9,180`、
  期待 19,770,768 行で success、1行減らすと failed。
- 期待値の式が実態と一致することを Silver の実行数と突き合わせて確認した
  （誤検知で毎回 failed になる回帰を避けるため）。OCCTO 2026-08-13 / 2026-08-12 は
  ともに期待 22,224 行に対し Silver 実測 22,224 行、JEPX は FY2024 → 17,520 行、
  FY2026 → 6,288 行で完全一致。

### 8.5 バックフィル用 CLI コマンドの追加（優先度: 中、解決済み）

今回の22年度バックフィルは使い捨てのシェルループで実施しており、
`docs/architecture/replay_strategy.md` が定める再構築手順が実行可能な形で残っていなかった。

- [x] ~~年度範囲を受け取る一次コマンドを `src/main.py` に実装する
      （例: `backfill-jepx --from-fiscal-year 2005 --to-fiscal-year 2026`）~~ →
      `backfill-jepx` を追加（`src/cli/commands/jepx.py` + `run_jepx_backfill_pipeline()`）。
      `--to-fiscal-year` の既定は `--from-fiscal-year`（OCCTO の `--to-date` と同じ規約）、
      逆順範囲は `parser.error`。

**処理フロー**: 年度ごとに raw→bronze を回し、**Silver は最後に全年度1回**。
スコープ付き Silver 実行は会計年度フィルタがキャスト後の列に掛かるため Iceberg
スキャンに押し込めず Bronze 全体を再スキャンする。22回繰り返すより、スコープ無しの
1パスで全年度を1スキャン分のコストで賄う方が安い（8.1 の実測で約12秒）。

**`--from-raw`（replay_strategy.md の再構築手順）**: 実装時に、単にスクレイプを
飛ばすだけでは**機能しない**ことが判明した。Bronze 取り込みは
`require_unprocessed=True` で呼ばれており、取り込み済みスナップショットは ingestion log で
`bronze_status='processed'` になるため、全年度が `ValueError` → skipped となって
Raw→Bronze が何もしない（`src/common/raw_ingestion_log.py:52-60`）。よって
`--from-raw` では `require_unprocessed=False` を渡す。`skip_if_exists` は既定のままなので、

- Bronze が健全 → 年度ごとに no-op、最後に Silver だけ再構築（Silver Corruption Recovery）
- Bronze を消したあと → `source_data` が無いので再 append（Bronze Corruption Recovery）

と、消したかどうかで自然に分岐する。専用フラグは不要。

**その他の設計判断**:

- 年度が1つ失敗しても範囲は継続し、`status="failed"` のステップを記録して最後に
  失敗年度一覧をログ出力、8.4 の `has_failed_step()` で `SystemExit(1)`。
  1年度で止まるより、対応が必要な年度が分かる方が replay として有用と判断。
- スクレイパーには sleep も retry も無く、範囲実行は1年度=1リクエストが連続するため、
  年度間に固定待機（`--request-delay-seconds`、既定3秒）を入れた。最終年度の後と
  `--from-raw` 時は待機しない。
- スクレイパーセッションは範囲全体で1つだけ開き、`finally` で必ず閉じる（OCCTO の
  日次ループと同じ形）。
- gold ステップは持たせない（モデルが無く、バックフィルに不要）。
- `run_jepx_orchestrated_pipeline` に埋まっていた raw / bronze のステップ本体を
  `run_source_to_raw_step()` / `run_raw_to_bronze_step()` に抽出し、日次とバックフィルで
  共有した。会計年度→対象日時の変換も `resolve_fiscal_year_start()`
  （`pipeline/jepx_common.py`）に集約し、`scrape-jepx --fiscal-year` と共用している。

**実データ確認**: `backfill-jepx --from-fiscal-year 2024 --to-fiscal-year 2026 --from-raw`
を実行。3年度とも `raw_to_bronze status=success rows=0`（＝スナップショットは解決できた
が `source_data` 既存のため append 無し）、Silver は `staged=374,400 / written=4,113,696`
（374,400 + 374,400 + 3,364,896）で success、終了コード0。実行前後で
bronze 380,256 / base 374,400 / block 374,400 / area 3,364,896 と**全テーブル行数が不変**
であることを確認した。あわせて ingestion log 上で対象3年度が `bronze_status='processed'`
であることも確認しており、`require_unprocessed=False` が無ければ全年度 skipped に
なっていた状況で正しく再解決できたことが裏付けられている。

**スコープ外**: Bronze の行を範囲削除する CLI は作っていない
（replay_strategy.md の "Remove invalid Bronze data" は手動前提）。必要になったら別タスク。

### 8.6 メタデータ列と変更検知（優先度: 低）

`add_metadata()` は実行ごとに `ingestion_time` / `execution_id` を新規採番するため、
業務値が不変でも全行が「変更あり」と判定される。区間置換に移行したことで
実害は解消しているが、将来 `upsert` 系の処理を書く場合は再燃する。

- [ ] 変更検知の比較対象からメタデータ列を除外する方針を決める

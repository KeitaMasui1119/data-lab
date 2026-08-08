# Plan: OCCTO ユニット別発電実績 パイプライン構築

> **実装担当への前提**
> このドキュメントだけで実装できるように、変更対象ファイル・確定済みの設計判断・検証可能な受け入れ条件を明記している。
> JEPX 側の同等実装（`src/pipeline/silver/bronze_to_silver_jepx_spot_price.py`）が全面的な雛形になる。
> 経緯は `docs/tasks/tasks_bts_jepx_sp.md` と `docs/reports/reports_20260808_10c4679e-4370-49d3-9b8c-92f890c5eade.md` を参照。

## ゴール

OCCTO「ユニット別発電実績」を Raw → Bronze → Silver で取り込む経路を、JEPX と同一のアーキテクチャで完成させる。

```
OCCTO 公表システム
  → Raw    (RustFS s3://jp-power-grid-dev/raw/occto/unit_generation/)
  → Bronze (PyIceberg bronze.occto_unit_generation_actuals / 全列 string)
  → Silver (PyIceberg silver.occto_unit_generation_actuals / long・型付き・パーティション付き)
```

## 確定済みの設計判断（再検討不要）

| 項目 | 決定 |
|---|---|
| 取得方式 | **GET → POST → GET の3段（Phase 0 で実測確定）。** ブラウザ不要、`requests.Session` で完結する。詳細は Phase 0 参照 |
| bronze_to_silver の実装 | **Python + DuckDB + PyIceberg**。JEPX と統一。既存の dbt occto モデルは削除する |
| Silver の粒度 | **long**（1行 = 発電所 × ユニット × 30分コマ）。48コマ列を unpivot する |
| dbt の扱い | **空の dbt プロジェクトを残す。** モデル2本と `run-occto-silver-dbt` は削除するが、`dbt_project.yml` / `profiles.yml` と依存（dbt-core, dbt-duckdb）は gold 層用に維持 |
| パーティション対応 | **OCCTO silver より先に実施**（`docs/tasks/tasks.md` §8.1）。OCCTO silver は最初からパーティション付きで作る |
| Silver 書き込み方式 | **区間 overwrite**（`Table.overwrite(overwrite_filter=...)`）。`upsert()` は使わない（JEPX で OOM を起こした経緯があるため） |
| daily_amount（日量） | long テーブルには**含めない**。48コマ合計との突合に使う品質チェック専用とする |

---

## 現状把握

### 既に存在するが「一度も通っていない」

OCCTO は3層ともコードが存在するが、**エンドツーエンドで一度も実行されていない**。

| 確認項目 | 実測値 |
|---|---|
| `raw/occto/` のオブジェクト数 | **0** |
| `bronze.occto_unit_generation_actuals` の行数 | **0** |
| `silver.occto_unit_generation_actuals`（Iceberg）の行数 | **0**（provision 済みだが書き込み経路なし） |
| `.env` の `OCCTO_DOWNLOAD_CSV_URL` | **未設定**（= `scrape-occto` は実行不可能だった） |
| リポジトリ内の OCCTO サンプル CSV | **なし**（`data/occto/` も存在しない） |

したがってこれは新規実装ではなく、**既存の骨組みを実データで動くようにする**タスクである。

### 既存ファイル

| 層 | ファイル | 状態 |
|---|---|---|
| Raw | `src/pipeline/raw/source_to_raw_occto.py` | 単一 GET 前提。ページ遷移未対応。ingestion log 未記録 |
| Bronze | `src/pipeline/bronze/source_to_bronze_occto.py` | ロジックはほぼ流用可。エンコーディングが要検証 |
| Silver | `src/dbt/jepx_power/models/{staging,silver}/*occto*.sql` | `select *` 素通し。DuckDB 内に書かれ Iceberg には届かない |
| スキーマ | `configuration/iceberg/schema/{bronze,silver}/occto_unit_generation_actuals.csv` | bronze と silver が**バイト単位で同一**（56列すべて string・横持ち） |
| CLI | `src/main.py` の `scrape-occto` / `ingest-occto-raw-to-bronze` / `run-occto-silver-dbt` / `migrate-occto-to-rustfs` | |
| テスト | `tests/test_occto_dbt_assets.py` | 現行の素通し設計を固定している。新設計と矛盾するため書き換え必須 |

---

## 現状の問題（この実装で解消する）

### 1. 重複排除キーが壊れている（最重要）

`configuration/iceberg/schema/bronze/occto_unit_generation_actuals.csv` の `is_identifier=TRUE` は
`power_plant_code, area, power_plant_name, target_date, updated_datetime`。

- **`unit_name` がキーに入っていない。** 「ユニット別」発電実績なのに、1発電所に複数ユニットがあると衝突する
- **`updated_datetime` がキーに入っている。** これは改訂版の版管理列であり、キーではなく「最新版を選ぶ ORDER BY」に使うべき列。キーに含めると、OCCTO が実績を改訂公表するたびに旧版と新版が別レコードとして積み上がる

正しい自然キーは **`(power_plant_code, unit_name, target_date)`**、版の選択は `ORDER BY updated_datetime DESC`。

> `docs/architecture/data_model.md` の Open Questions「OCCTOの plant_id と plant_number の一意性を確認する」は未解決。Phase 0 で実データから確定させる。

### 2. Silver が Silver になっていない

- `silver_occto_unit_generation_actuals.sql` は `select *` の素通し
- Silver スキーマ CSV が Bronze と完全に同一（全列 string・48コマ横持ち）
- 書き込み先が Iceberg ではなく DuckDB ファイル（`src/dbt/jepx_power/jepx_power.duckdb`）
- `docs/architecture/layers.md` が定める Silver の責務（型付け・業務ルール適用・分析可能な形）を何も満たしていない

### 3. コマ番号の意味が JEPX と逆

OCCTO の列は `timeslot_00_30` 〜 `timeslot_24_00` で、コマを**終了時刻**でラベリングしている。
JEPX の `time_code` は**開始時刻**基準（`SLOT_OFFSET = 1`）。

| OCCTO 列名 | 対象区間 | 開始時刻(JST) | JEPX 相当 `time_code` |
|---|---|---|---|
| `timeslot_00_30` | 00:00–00:30 | 00:00 | 1 |
| `timeslot_01_00` | 00:30–01:00 | 00:30 | 2 |
| … | … | … | … |
| `timeslot_24_00` | 23:30–24:00 | 23:30 | 48 |

**`time_code` は列の並び順（1始まり）そのもの**になる。この対応を1箇所で定義しないと、JEPX と join した瞬間に30分ずれる。

### 4. エンコーディングが未検証

`src/pipeline/bronze/source_to_bronze_occto.py:56` は `utf-8-sig` でデコードする。
JEPX は `cp932`。日本の公的機関 CSV で UTF-8 はやや珍しく、実データ未検証のためここが最初の失敗点になる可能性が高い。

### 5. パーティションが効いていない（リポジトリ全体の問題）

`src/common/schema_builder.py` に `partition_transform` を扱うコードは**1行もない**。
スキーマ CSV の `partition_transform` 列は完全に無視され、`provision_table()` は partition spec を渡さずに `create_table()` している。

JEPX silver（330万行）では「9.3秒 / ピーク RSS 1.7GB」で耐えているが、
**OCCTO を long 化すると年間826万行規模**（Phase 0 実測ベース。後述 4-6）になり、パーティション無しでは日次実行のたびに全期間のファイルが書き換わる。

---

## Phase 0 — 実データと取得フローの確定【完了・実測済み】

> **2026-08-08 実施結果。** Playwright はこの devcontainer に Chromium が入っておらず
> （`npx playwright install chrome` は root 権限が必要で失敗）、代わりに `curl` で Cookie を
> 手動管理しながら実フローを再現した。**`requests.Session` で完全に代替可能なフローと確認済み**
> （JS 実行が必要な箇所はゼロ）。取得した実 CSV はローカルにも残していない（調査後に破棄済み）。

### 0-1. 確定した HTTP フロー

ブラウザ不要。3ステップの `requests.Session` で完結する。

| # | メソッド | URL | 内容 |
|---|---|---|---|
| 1 | GET | `https://hatsuden-kokai.occto.or.jp/hks-web-public/home` | セッション確立。`/disclaimer-agree` へ 302 リダイレクトされる。`JSESSIONID` Cookie を受け取る |
| 2 | POST | `https://hatsuden-kokai.occto.or.jp/hks-web-public/disclaimer-agree/next` | 免責事項同意。ボディは `agreed=0`（**注意: `1` ではなく `0`**。下記参照） |
| 3 | GET | `https://hatsuden-kokai.occto.or.jp/hks-web-public/info/hks/downloadCsv?<query>` | CSV 本体。クエリパラメータは下記 |

**セッション確立に `info/hks/search`（一覧のAJAX検索）や `info/hks`（画面表示）を経由する必要はない。**
ステップ2の直後にステップ3を叩けば CSV が返る（実測で確認済み）。

**`agreed` の値が `0` である理由（重要・ハマりどころ）:**
同意チェックボックス自体は `id="agreed"`（`name` なし）、実際に送信される
`<input type="hidden" name="agreed" value="">` は別要素で、
`assets/js/HKSHS004.js` の `changeCheckbox()` が次のトグルロジックで値を書き換える。

```js
// pageshow 時に無条件で '1' がセットされた後、
// チェックボックスクリック時にトグルされる:
function changeCheckbox(checkbox){
    checkbox.nextElementSibling.value =
        (checkbox.nextElementSibling.value == 1 || checkbox.nextElementSibling.value == '') ? '0' : '1'
}
```

初期値 `'1'`（`pageshow` で強制セット）→ チェックボックスを1回クリックして「同意します」を
ON にすると `'0'` に反転する。つまり**同意成立時に実際に送信される値は `'0'`**。
`agreed=1` や `agreed=on` を送ると免責事項ページがそのまま返ってきて先に進めない
（実測で両方失敗を確認済み）。この実装のクセであり、他ページでも同型のチェックボックスが
出てきたら同じ反転ロジックを疑うこと。

### 0-2. CSV ダウンロードのクエリパラメータ

`info/hks/downloadCsv` は素の HTML フォーム GET 送信（`$("#menuhatsudenkokai").submit()`、
`method` 属性未指定 = デフォルト GET）。指定しない項目を省略すると 0 件になるため、
**全項目を明示する必要がある。**

| パラメータ名 | 意味 | 全件指定時の値 |
|---|---|---|
| `htdnsCd` | 発電所コードで絞り込み | 空文字（絞り込みなし） |
| `htdnsNm` | 発電所名で絞り込み | 空文字 |
| `unitNm` | ユニット名で絞り込み | 空文字 |
| `areaCheckbox`（複数指定） | エリア絞り込み | `99,01,02,03,04,05,06,07,08,09,10` を個別パラメータとして列挙 |
| `hatudenHosikiCheckbox`（複数指定） | 発電方式絞り込み | `99,01,02,...,09` を個別パラメータとして列挙 |
| `tgtDateDateFrom` / `tgtDateDateTo` | 対象日範囲（`YYYY/MM/DD`） | 取得したい範囲 |

実測: `tgtDateDateFrom=tgtDateDateTo=2026/08/01` で1日分を取得 →
`Content-Disposition: attachment; filename=ユニット別発電実績_<タイムスタンプ>.csv`、
`Content-Type: application/csv` で 471 行（1日・全国・全方式）のCSVが返った。

**日付範囲を広げれば複数日分が1本の CSV にまとまって返ってくる**
（`tgtDateDateFrom` ≠ `tgtDateDateTo` で範囲取得が可能）。
Phase 2 のバックフィルは日次ループではなく、**範囲一括ダウンロード**にできる可能性が高い
（サーバ負荷の観点でもこちらが望ましい。ただし上限日数は未検証 — 大きすぎる範囲でタイムアウト
　/ 上限エラーになる可能性があるため、実装時に段階的に広げて確認すること）。

### 0-3. 実 CSV の性質（確定値）

| 確定事項 | 実測結果 |
|---|---|
| 文字エンコーディング | **`utf-8-sig`（UTF-8 with BOM）。現行コードの前提は正しかった** |
| ヘッダ行の位置 | 先頭に注記行なし。1行目がそのままヘッダー |
| 列名・列数 | **56列、現行スキーマ CSV の `source_name` と完全一致**（`発電所コード,エリア,発電所名,ユニット名,発電方式・燃種,対象日,00:30[kWh],...,24:00[kWh],日量[kWh],更新日時`） |
| 1日あたりのユニット数 | **471**（2026-08-01・全国・全方式）。年間見積りは 471 × 365 ≈ 172,000 レコード、long化後は ×48 ≈ **826万行/年**（Phase 4-6 の見積り 2,600万行/年は過大だったので下方修正） |
| `unit_name` が空になるケース | **実在する。** 例: `発電所コード=52271`（北陸・電源開発 手取川第一・水力）の `ユニット名` が空文字。**Phase 4 の identifier 設計に直接影響**（後述） |
| `updated_datetime` のフォーマット | `"YYYY/MM/DD HH:MM:SS"`（秒精度。例 `2026/08/02 15:30:28`） |
| 同一ファイル内の重複 | **`(発電所コード, ユニット名, 対象日)` で重複なし**（471行 = 471 unique keys） |
| 過去何日分まで遡れるか | ホーム画面の「お知らせ」に明記あり: **2024-03-25 分から本運用公表開始**（2024-03-26 15:30頃〜）。**2024-03-24 以前は試験データ**と明記されており、バックフィル対象から除外するか要検討 |
| `daily_amount` と48コマ合計の一致 | サンプル5件で**完全一致**（丸め誤差なし）。Phase 4-5 の品質チェックの閾値は「1件でも不一致なら警告」で問題ない |

### 0-4. Phase 4 の identifier 設計への影響（要修正）

`unit_name` が空文字になる実例が確認できたため、当初案の
「`unit_name` を `is_identifier=TRUE, required=TRUE` にする」は**そのままでは採用できない**。

空文字はSQLの`NOT NULL`制約は満たす（NULLではない）ので、**`COALESCE(unit_name, '')` の正規化さえ
入れれば `required=TRUE` は維持できる。** 空文字はそれ自体が「ユニット名の区別が無い1本設備」を表す
正当な値として扱う。自然キー `(power_plant_code, unit_name, target_date)` はこのままで成立する
（空文字も含めて一意性が保たれることは実測で確認済み）。Phase 3-3 の記述はこの前提で変更不要。

### 受け入れ条件（Phase 0）— 完了

- [x] `requests` のみで（ブラウザ無しで）任意の対象日の CSV を取得できるフローを確認した
- [x] 上表の項目が実測値で埋まった（改訂公表の頻度のみ未確定 — 下記「未解決事項」に残す）
- [x] 判明した内容で Phase 2〜4 の記述を更新した（本ドキュメント内）

---

## Phase 1 — パーティション対応（`tasks.md` §8.1）

OCCTO silver を最初からパーティション付きで作るための前提整備。

### 変更対象

- `src/common/schema_builder.py` — スキーマ CSV の `partition_transform` 列を読む
- `src/common/iceberg.py` — `provision_table()` が `create_table()` に `partition_spec` を渡す

### 仕様

`partition_transform` は**その列に適用する変換名**を書く（列単位）。

| 値 | 意味 |
|---|---|
| （空） | パーティションキーにしない |
| `identity` | 値そのまま |
| `year` / `month` / `day` / `hour` | 日付・時刻の切り出し |
| `bucket[N]` | N 分割ハッシュ |
| `truncate[N]` | 先頭 N 文字/桁 |

- 新規テーブル: `create_table(..., partition_spec=...)` で作成
- 既存テーブル: spec の差分を検出したら**警告ログのみ**（自動 evolve はしない）。
  `update_spec()` で spec を進化させても既存データファイルは旧レイアウトのまま残るため、
  再書き込みの要否判断が必要になる。JEPX silver 3テーブルの移行は**このフェーズのスコープ外**とし、
  `tasks.md` §8.1 に残す

### 受け入れ条件（Phase 1）

- [ ] `partition_transform` を持つスキーマ CSV から新規作成したテーブルの `spec` が空でないことをテストで確認
- [ ] `partition_transform` が全て空のスキーマ CSV では従来どおり未パーティションで作成される（既存 JEPX テーブルの挙動が変わらない）
- [ ] 既存テーブルの spec 差分検出時に警告が出て、例外にはならない

---

## Phase 2 — source_to_raw

### 2-1. `BaseHttpScraper` の多段リクエスト対応

`src/common/http_scraper.py` の現行契約は「`build_request()` が返す1本の `RequestSpec` を `fetch()` が実行する」で、
セッション確立を伴う多段フローが表現できない。Phase 0 の実測で OCCTO 側は
**GET → POST → GET の3段構成**（0-1参照）と確定したので、以下のフックを追加する。

**方針:** `prepare()` フックを追加する。既存の `build_request()` のシグネチャは変更しない。

```python
class BaseHttpScraper(ABC):
    def prepare(self, target_at: datetime) -> None:
        """Perform preparatory requests before the download request is built.

        Default is a no-op. Scrapers that must establish a session (cookies,
        form tokens) override this; the state they capture is read back by
        their own build_request().
        """
        return None

    def fetch_response(self, target_at: datetime) -> requests.Response:
        self.prepare(target_at)
        spec = self.build_request(target_at)
        ...
```

- **JEPX スクレイパーと既存テストは無変更で動く**（`prepare()` の既定が no-op のため）
- OCCTO 側は `prepare()` で「ステップ1: GET `/home`」「ステップ2: POST `/disclaimer-agree/next`
  （`agreed=0`）」を実行し、`requests.Session` の Cookie に `JSESSIONID` を溜め込む。
  `build_request()` はステップ3（CSV GET）の `RequestSpec` を返すだけでよい
- JS実行が必要な箇所はゼロと確認済みなので、`prepare()` はブラウザなしで完結する

### 2-2. `OCCTOUnitGenerationScraper` の書き換え

`src/pipeline/raw/source_to_raw_occto.py` を Phase 0 で確定したフローに合わせて全面的に書き換える。

```python
DISCLAIMER_HOME_URL = "https://hatsuden-kokai.occto.or.jp/hks-web-public/home"
DISCLAIMER_AGREE_URL = "https://hatsuden-kokai.occto.or.jp/hks-web-public/disclaimer-agree/next"
DOWNLOAD_CSV_URL = "https://hatsuden-kokai.occto.or.jp/hks-web-public/info/hks/downloadCsv"

# All area/method codes must be listed explicitly; omitting any narrows the
# result set instead of erroring, so "all" is spelled out rather than assumed.
ALL_AREA_CODES = ("99", "01", "02", "03", "04", "05", "06", "07", "08", "09", "10")
ALL_METHOD_CODES = ("99", "01", "02", "03", "04", "05", "06", "07", "08", "09")
```

- `prepare()`: `session.get(DISCLAIMER_HOME_URL)` → `session.post(DISCLAIMER_AGREE_URL, data={"agreed": "0"})`
- `build_request()`: `GET DOWNLOAD_CSV_URL` に `params` として
  `htdnsCd=/htdnsNm=/unitNm=` 空文字、`areaCheckbox` と `hatudenHosikiCheckbox` を
  `ALL_AREA_CODES` / `ALL_METHOD_CODES` から**複数値として**構築
  （`requests` は同名キーへの list 値渡しで `?areaCheckbox=99&areaCheckbox=01&...` を自動生成する）、
  `tgtDateDateFrom` / `tgtDateDateTo` に `YYYY/MM/DD` 形式の対象日（範囲取得も対応、2-5参照）
- **2024-03-24 以前は試験データと公式に明記されている**（0-3参照）。デフォルトの取得下限を
  `2024-03-25` にするか、試験データもそのまま取り込んで `status` 列で区別するかは実装時に判断する
  （個人的な推奨は前者 — 試験データを実績として扱うと下流の分析を汚染するため）

### 2-3. オブジェクトキーをスナップショット方式にする

OCCTO は実績を**改訂公表する**ため、同じ対象日を複数回取得する。現行のフラットなキーだと上書きされ、改訂履歴が残らない。
JEPX のスナップショット方式に合わせる。

```
raw/occto/unit_generation/target_date={YYYY-MM-DD}/ingested_at={YYYYMMDDTHHMMSS}/ユニット別発電実績_{YYYY-MM-DD}.csv
```

範囲取得（2-5）で複数日分が1ファイルに収まる場合は `target_date=` を範囲の開始日にし、
ファイル名に終了日も含める（例: `ユニット別発電実績_{from}_{to}.csv`）。

### 2-4. ingestion log への記録

現行の OCCTO は `metadata/raw_ingestion_log.parquet` に何も記録していない（JEPX は記録済み）。

- `dataset='occto_unit_generation'`、`snapshot_date=target_date` で記録する
  （ログのスキーマには `snapshot_date` 列が既に存在する）
- `file_hash` が直近スナップショットと一致する場合は**アップロードをスキップ**する。
  バックフィル追いかけ時に、改訂のない日を無駄に再取り込みしないため
- `src/common/raw_ingestion_log.py` の `resolve_latest_raw_object()` は
  `dataset + fiscal_year` でしか絞れない。**対象日で絞る経路を追加する**

### 2-5. 日付範囲バックフィル

- `--target-date` に加えて `--from-date` / `--to-date` を受け付ける
- **`tgtDateDateFrom` ≠ `tgtDateDateTo` で範囲を1リクエストにまとめて取得できることを実測で確認済み**
  （0-2参照）。日次ループではなく範囲一括ダウンロードを既定にする
- ただし上限日数は未検証。実装時に段階的に範囲を広げてタイムアウト/上限エラーの有無を確認し、
  安全な上限（例: 1ヶ月分ずつ）で分割する方針をコードコメントに残す
- 複数リクエストに分割する場合は**サーバ負荷に配慮したウェイトを入れる**（既存 notebook の
  `time.sleep(np.random.uniform(1, 5))` と同水準）

### 受け入れ条件（Phase 2）

- [ ] `requests.Session` をモックした単体テストで、3段フロー（GET home → POST disclaimer-agree/next
      with `agreed=0` → GET downloadCsv）が期待どおりの順序・パラメータで発行される
- [ ] `areaCheckbox` / `hatudenHosikiCheckbox` が全コード列挙で送信される（一部だけ送ると結果が絞られてしまうことをテストで固定する）
- [ ] オブジェクトキーが `target_date=` / `ingested_at=` を含む形で構築される
- [ ] 同一内容を2回取得した際、2回目は `file_hash` 一致でアップロードがスキップされる
- [ ] ingestion log に `dataset='occto_unit_generation'` の行が追加される
- [ ] `scraper.close()` が `try/finally` で必ず呼ばれる（`src/main.py` の既存パターン）

---

## Phase 3 — raw_to_bronze

`src/pipeline/bronze/source_to_bronze_occto.py` を修正する。ロジックの骨格は流用可。

### 3-1. エンコーディング

Phase 0 の実測値に合わせる。**決め打ちにせず、失敗時に判別可能なエラーメッセージを出す**
（現行の `ValueError(f"Failed to decode object with utf-8-sig: ...")` は方向性としては正しい）。

### 3-2. ヘッダ行

Phase 0 で先頭に注記行があると判明した場合は `skip_rows` で対応する。

### 3-3. スキーマ CSV の修正

`configuration/iceberg/schema/bronze/occto_unit_generation_actuals.csv`

| 列 | 現行 | 修正後 | 理由 |
|---|---|---|---|
| `unit_name` | `is_identifier=FALSE`, `required=FALSE` | **`is_identifier=TRUE`, `required=TRUE`** | ユニット別テーブルの自然キー。ただし Phase 0 で空値ありと判明した場合は要再検討 |
| `updated_datetime` | `is_identifier=TRUE` | **`is_identifier=FALSE`** | 版管理列であってキーではない |
| `area` / `power_plant_name` | `is_identifier=TRUE` | **`is_identifier=FALSE`** | `power_plant_code` に従属する属性。キーに含める必要がない |

- Bronze は**全列 string のまま維持**する（JEPX と同じ。型変換は Silver の責務）
- `partition_transform`: bronze は `ingestion_date` に `year`（`data_model.md` の設計に合わせる）

### 3-4. 再取り込み（改訂版）の扱い

現行は `source_data` の存在チェックで重複を弾く（`--allow-duplicate-source` で上書き）。
スナップショット方式にすると `source_data` にファイル名だけを入れた場合、改訂版が「既存」と判定されて取り込めない。

**`source_data` にはオブジェクトキー全体（`ingested_at=` を含む）を入れる。**
これで改訂版は別スナップショットとして bronze に追記され、最新版の選択は Silver の重複排除に委ねられる
（Bronze は追記専用・Silver が最新版を決める、という JEPX と同じ責務分割）。

### 受け入れ条件（Phase 3）

- [ ] 実 CSV 1日分が bronze に取り込まれ、行数がユニット数と一致する
- [ ] 同一スナップショットの二重取り込みがスキップされる
- [ ] 改訂版（別 `ingested_at`）は別行として追記される
- [ ] メタデータ列5種（`source_data`, `status`, `ingestion_time`, `ingestion_date`, `execution_id`）が全て非 NULL

---

## Phase 4 — bronze_to_silver【本体】

新規ファイル `src/pipeline/silver/bronze_to_silver_occto_unit_generation.py`。
**`src/pipeline/silver/bronze_to_silver_jepx_spot_price.py` を雛形にする**（構造・命名・docstring の粒度を揃える）。

### 4-1. Silver スキーマ

`configuration/iceberg/schema/silver/occto_unit_generation_actuals.csv` を**全面的に書き換える**（現行は bronze のコピー）。

| # | 列名 | 型 | identifier | required | partition_transform | 備考 |
|---|---|---|---|---|---|---|
| 1 | `power_plant_code` | string | TRUE | TRUE | | |
| 2 | `unit_name` | string | TRUE | TRUE | | 空値は `''` に正規化 |
| 3 | `target_date` | date | TRUE | TRUE | **`day`** | パーティションキー |
| 4 | `time_code` | int | TRUE | TRUE | | 1–48（開始時刻基準・JEPX と同義） |
| 5 | `delivery_datetime` | timestamptz | FALSE | TRUE | | JEPX silver と join 可能な時刻 |
| 6 | `generation_kwh` | long | FALSE | FALSE | | 当該コマの発電実績 |
| 7 | `area` | string | FALSE | FALSE | | |
| 8 | `power_plant_name` | string | FALSE | FALSE | | |
| 9 | `power_generation_method_and_fuel_type` | string | FALSE | FALSE | | |
| 10 | `updated_datetime` | timestamptz | FALSE | FALSE | | どの版を採用したかの証跡 |
| 11–15 | `source_data`, `status`, `ingestion_time`, `ingestion_date`, `execution_id` | | | | | メタデータ列（`add_metadata()`） |

- **`daily_amount` は含めない**（48コマに対して同じ値が48回重複するため）。品質チェックにのみ使う（4-5）
- **パーティションは `target_date` の `day`。** 日次実行が1パーティションだけを置換できるようにする。
  `month` だと日次実行のたびに約220万行のパーティションを書き換えることになる

### 4-2. コマ列 → `time_code` の対応（最重要）

モジュール定数として**並び順どおりに**定義し、`time_code` は 1 始まりの位置とする。

```python
# OCCTO labels each slot by its END time, JEPX time_code by its START time.
# The column order therefore IS the time_code: timeslot_00_30 covers
# 00:00-00:30 and maps to time_code 1, the same slot JEPX calls 1.
TIMESLOT_COLUMNS = (
    "timeslot_00_30", "timeslot_01_00", ..., "timeslot_24_00",
)
assert len(TIMESLOT_COLUMNS) == 48
```

- UNPIVOT 後の列名 → `time_code` の対応は、**この定数から生成した `CASE` 式**で行う。
  文字列パース（JEPX の `split_part(area_price_column, '_', 3)` 相当）に頼らない
- `TIMESLOT_COLUMNS` の並びが bronze スキーマ CSV の並びと一致することを**テストで固定する**
- 日本に夏時間はないため、コマ数は常に48で欠落・重複は発生しない

### 4-3. 変換パイプライン

JEPX 版と同じ段構成にする。

1. **scan** — `iceberg_scan('s3://jp-power-grid-dev/bronze/occto_unit_generation_actuals')`
2. **typed** — 型変換
   - `target_date` → `DATE`（`try_strptime` で複数フォーマットを `COALESCE`）
   - 48コマ列 + `daily_amount` → `BIGINT`（`REPLACE(col, ',', '')` でカンマ除去してから `TRY_CAST`）
   - `updated_datetime` → `TIMESTAMP`
   - `unit_name` → `COALESCE(unit_name, '')`
3. **deduplicated** — 最新版の選択
   ```sql
   QUALIFY ROW_NUMBER() OVER (
       PARTITION BY power_plant_code, unit_name, target_date_d
       ORDER BY updated_datetime_ts DESC NULLS LAST,
                ingestion_time      DESC NULLS LAST,
                execution_id        DESC NULLS LAST
   ) = 1
   ```
4. **validated** — `violations` リストを付与（JEPX の `_build_violation_expression()` と同形式）
   - `target_date_null`
   - `power_plant_code_null`
   - `generation_negative`（48コマのいずれかが負）
   - `all_timeslots_null`（48コマが全て NULL = 実質データ無し）
5. **unpivot** — 48列 → `(time_code, generation_kwh)` の48行に展開
6. **delivery_datetime の導出** — JEPX と**完全に同じ式**にする
   ```sql
   (target_date_d + ((time_code - 1) * INTERVAL 30 MINUTE))
       AT TIME ZONE 'Asia/Tokyo' AS delivery_datetime
   ```
7. **write** — PyIceberg 区間 overwrite

### 4-4. 書き込みウィンドウ

- ウィンドウ列は **`target_date`**（JEPX の `delivery_date` 相当）
- `--target-date` 指定時: その1日
- `--from-date` / `--to-date` 指定時: その範囲
- 無指定時: ステージングされた行の `min(target_date)` 〜 `max(target_date)`
- JEPX の `build_delivery_window()` / `delivery_date_bound()` と同じ構造で実装する
- 書き込み前に `ensure_unique_keys(frame, key_cols=("power_plant_code", "unit_name", "target_date", "time_code"))` で重複を弾く

### 4-5. `daily_amount` による品質チェック

`daily_amount` は long テーブルには入れないが、**捨てずに検算に使う**。

- ステージング時点で `sum(48コマ)` と `daily_amount` を比較する
- 一致しない行の**件数と最大乖離幅を WARNING でログ出力**する
- **行は落とさない。** 丸め誤差や OCCTO 側の集計差で軽微な乖離が出る可能性があるため、
  乖離を理由に実績を欠落させるほうが害が大きい
- 閾値を設けるかは Phase 0 の実測（乖離の実際の分布）を見て決める

### 4-6. 行数とコストの見積り

| 項目 | 見積り（Phase 0 実測: 471ユニット/日、2026-08-01・全国・全方式） |
|---|---|
| 1日あたり行数 | 471 × 48 = **22,608 行/日** |
| 年間行数 | 約826万行/年（365日 × 22,608） |
| 日次実行が置換する範囲 | 1パーティション（= 22,608 行）**のみ**（`day` パーティション前提） |

Phase 1 のパーティション対応が入っていれば、日次実行のコストは**履歴量に依存しない**。
入っていない場合、日次実行のたびに全期間のファイルが書き換わる。

### 受け入れ条件（Phase 4）

- [ ] bronze 1行 → silver 48行に展開される
- [ ] `timeslot_00_30` が `time_code=1` / `delivery_datetime = target_date 00:00 JST` にマッピングされる
- [ ] `timeslot_24_00` が `time_code=48` / `delivery_datetime = target_date 23:30 JST` にマッピングされる
- [ ] 同一キーで `updated_datetime` の異なる2版がある場合、新しいほうだけが残る
- [ ] `TIMESLOT_COLUMNS` の並びが bronze スキーマ CSV の並びと一致する
- [ ] 負の発電量・日付パース失敗が `violations` に記録され、件数がログに出る
- [ ] 同じ対象日に対して2回実行しても行が二重にならない（区間 overwrite の冪等性）
- [ ] silver テーブルの `spec` が `target_date` の `day` パーティションになっている
- [ ] JEPX silver と `delivery_datetime` で join したとき、コマがずれない

---

## Phase 5 — 撤去・CLI・オーケストレーター・ドキュメント

### 5-1. dbt occto の撤去

削除するもの:
- `src/dbt/jepx_power/models/staging/stg_occto_unit_generation_actuals.sql`
- `src/dbt/jepx_power/models/silver/silver_occto_unit_generation_actuals.sql`
- `src/dbt/jepx_power/models/staging/_staging__models.yml` の occto エントリ
- `src/dbt/jepx_power/models/silver/_silver__models.yml` の occto エントリ
- `src/main.py` の `run-occto-silver-dbt` サブコマンドと実行分岐

残すもの:
- `src/dbt/jepx_power/dbt_project.yml` / `profiles.yml`
- `pyproject.toml` の `dbt-core` / `dbt-duckdb` / `shandy-sqlfmt`

> **結果として dbt モデルは0本になる。**
> なお `.github/workflows/ci.yml` に dbt を実行するジョブは**存在しない**ため、CI 側の対応は不要
> （`docs/tasks/plan_jepx_pipeline_and_ci.md` に `dbt-parse` ジョブの構想があるが、未実装のまま）。

### 5-2. CLI（`src/main.py`）

| コマンド | 変更 |
|---|---|
| `scrape-occto` | 引数を実フローに合わせて再定義。`--from-date` / `--to-date` を追加 |
| `ingest-occto-raw-to-bronze` | 大きな変更なし。`--object-key` は維持 |
| `ingest-occto-bronze-to-silver` | **新規。** `--catalog` / `--bronze-location` / `--schema-dir` / `--target-date` / `--from-date` / `--to-date` |
| `run-occto-silver-dbt` | **削除** |
| `run-occto-orchestrator` | **新規**（5-3） |
| `migrate-occto-to-rustfs` | **削除する。** Phase 2 の新フローが完全に代替し、参照元 `data/occto/` も存在しない |

`src/main.py` は薄いオーケストレーションのみに保つ（`CLAUDE.md` の方針）。

`migrate-occto-to-rustfs` の削除に伴い `src/pipeline/bronze/migrate_occto_data.py` 本体も削除する。

### 5-3. オーケストレーター

`src/orchestration/occto_pipeline.py` を新規作成（`docs/tasks/tasks.md` §5 の未着手項目）。

- `src/orchestration/jepx_pipeline.py` と同じ `PipelineStepResult` を返す
- ステップ: `source_to_raw` → `raw_to_bronze` → `bronze_to_silver`
- Silver のスコープ既定は**取り込んだ対象日のみ**（JEPX が会計年度スコープを既定にしたのと同じ思想）

### 5-4. テスト

**書き換え対象:** `tests/test_occto_dbt_assets.py`

削除すべきテスト（新設計と矛盾するもの）:
- `test_occto_silver_schema_matches_bronze_schema` — silver は bronze と別スキーマになる
- `test_occto_staging_model_scans_bronze_table` — dbt モデルが消える
- `test_occto_silver_model_passthroughs_staging_model` — 同上
- `test_readme_mentions_occto_dbt_command` — コマンドが消える

新規テストファイル:
- `tests/test_source_to_raw_occto.py` — 多段フロー、オブジェクトキー、ingestion log、ハッシュ一致スキップ
- `tests/test_source_to_bronze_occto.py` — エンコーディング、スキーマキャスト、重複スキップ
- `tests/test_bronze_to_silver_occto_unit_generation.py` — **本体**。
  `tests/test_bronze_to_silver_jepx_spot_price.py` と同じ方式で、
  `iceberg_scan` の代わりに**ローカル登録したリレーション**を `source_relation` に渡し、
  RustFS も Iceberg カタログも不要な単体テストにする
- `tests/test_partition_spec.py` — Phase 1 のパーティション対応

テスト方針は `~/.claude/rules/ecc/common/testing.md` に従い、TDD（RED → GREEN → REFACTOR）で進め、カバレッジ80%以上を維持する。

### 5-5. ドキュメント

| ファイル | 更新内容 |
|---|---|
| `CLAUDE.md` | 「Silver（JEPX: DuckDB transform + PyIceberg window replace / **OCCTO: dbt**）」を実態に合わせる。コマンド一覧の更新 |
| `README.md` | **§6「Build OCCTO silver layer with DuckDB + dbt」（165–181行目）を全面書き換え。** `main_silver.silver_occto_...`（DuckDB）に書くという記述は実態と合わなくなる。285行目の「bronze-to-silver dbt pipeline: implemented」も更新 |
| `docs/architecture/data_model.md` | OCCTO の主キーを確定させ、Open Questions の「plant_id と plant_number の一意性」を解決済みにする |
| `docs/tasks/tasks.md` | §5 の「OCCTO オーケストレーターを実装する」を完了に。§8.1 のパーティション残件（JEPX 移行分）を更新 |
| `docs/architecture/layers.md` | 空欄の「Partitioning / Granularity View」に OCCTO の設計を記述（任意） |

---

## 実施順序とPR分割

| # | Phase | 内容 | PR 分割の目安 |
|---|---|---|---|
| 1 | Phase 0 | 取得フロー・実データ確定 | PR なし（調査。結果をこのドキュメントに反映） |
| 2 | Phase 1 | パーティション対応 | **独立 PR**。OCCTO と無関係にレビュー可能 |
| 3 | Phase 2 + 3 | source_to_raw + raw_to_bronze | **1 PR**。「実データが bronze に入る」で完結する単位 |
| 4 | Phase 4 | bronze_to_silver | **独立 PR**。変更量が最大 |
| 5 | Phase 5 | 撤去・CLI・オーケストレーター・ドキュメント | **1 PR** |

---

## 未解決事項

Phase 0 の実測でほぼ解消した。残るのは以下のみ。

- [x] ~~`unit_name` が空になるユニットが存在するか~~ → **実在する。** `COALESCE(unit_name, '')` 正規化で対応（0-4参照）
- [x] ~~`power_plant_code` が全国で一意か、`area` との複合が必要か~~ →
      1日・全国471行で `(power_plant_code, unit_name, target_date)` の重複ゼロを確認。
      `area` を複合キーに含める必要はないと判断。
      **ただし将来この前提が崩れた場合は `ensure_unique_keys()`（Phase 4-4）が書き込み時に例外で検知するため、
      追加の事前検証なしにこの前提で実装を進めてよい**
- [x] ~~`daily_amount` と48コマ合計の乖離が常態か例外か~~ → サンプルでは完全一致。
      設計（4-5）は既に「乖離があっても行を落とさず警告のみ」なので、常態/例外いずれでも安全に倒してある
- [x] ~~`migrate-occto-to-rustfs` コマンドを削除するか~~ → **削除する。**
      Phase 2 の新フローが完全に代替し、参照元 `data/occto/` も存在しないため、残す理由がない（Phase 5-2 に反映済み）
- [ ] **OCCTO の改訂公表の頻度**（未解決・非ブロッキング）。
      複数日にわたって同じ対象日を再取得しないと観測できないため、今回の調査では確認していない。
      ただし Silver 側の最新版選択（`ORDER BY updated_datetime DESC`）は頻度に依存しない設計にしてあるため、
      **実装のブロッカーにはならない**。運用開始後の実データで自然に判明する
- [ ] **範囲取得（`tgtDateDateFrom`≠`tgtDateDateTo`）の上限日数**（未解決・非ブロッキング）。
      1日分の取得で確認は取れたが、大きな範囲でのタイムアウト/上限挙動は未検証。
      Phase 2 実装時に段階的に範囲を広げて確認する（2-5に記載済み）

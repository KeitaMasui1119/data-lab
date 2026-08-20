# Plan: 気象庁 天気予報 パイプライン

> **策定日**: 2026-08-20
> **状態**: 未着手（Phase 0 の調査のみ完了。本ドキュメントがその成果物）
> **既存の枠**: `docs/tasks/tasks.md` §2 / `docs/architecture/data_model.md` §4 に
> プレースホルダが存在する。本プランはそれを実測で置き換え、確定させるもの。
> **雛形**: `docs/tasks/plan_occto_pipeline.md`（同じ粒度で書いている）

## ゴール

気象庁の予報 JSON API から**電力10エリア**（北海道・東北・東京・北陸・中部・関西・
中国・四国・九州・沖縄）の天気予報を取得し、Raw → Bronze → Silver で取り込む。

```
気象庁 bosai API (JSON)
  → Raw    (RustFS s3://jp-power-grid-dev/raw/jma/forecast/)
  → Bronze (PyIceberg bronze.jma_forecast / 全列 string・縦持ち)
  → Silver (PyIceberg silver.jma_forecast_* / 粒度別・型付き)
```

最終的な用途は **JEPX スポット価格・でんき予報の需給データと同一エリア軸で結合すること**。
ユーザが挙げた10エリアは `docs/electric_forecast.txt` が列挙する
でんき予報10社のエリア区分と完全に一致しており、そこが結合キーになる。

---

## ⚠️ 最重要制約：このデータは遡って取得できない

**気象庁の予報 API は「現時点の最新スナップショット」しか返さない。過去分のアーカイブ
エンドポイントは存在しない。**

JEPX（年度別 CSV が丸ごと再取得できる）や OCCTO（日付指定で過去分が取れる）と決定的に
違う点で、パイプライン設計の順序そのものを変える：

| | JEPX / OCCTO | 気象庁 予報 |
|---|---|---|
| 過去データ | 何度でも再取得できる | **取得不可能** |
| 取りこぼしの回復 | `backfill-*` で回復 | **永久に欠測** |
| 実装順序の最適解 | Bronze/Silver を固めてから Raw を回す | **Raw を最優先で稼働させる** |

したがって本プランは **Phase 1（Raw 収集の常時稼働）を Bronze/Silver より先に完了させる**
構成を取る。Bronze の設計に迷っている間の1日は、そのまま失われる1日である。

> 補足: 実況（過去の観測値）は別系統なら取得できる。アメダスの
> `https://www.jma.go.jp/bosai/amedas/data/point/{amedas_id}/{yyyyMMdd}_{HH}.json`
> は疎通確認済み（HTTP 200 / 9.8 KB）。ただし保持期間には限りがあり、
> 長期の実況が要るなら「過去の気象データ・ダウンロード」（CSV、API ではない）が別途必要。
> **実況は Phase 5 の任意課題**とし、本プランの主対象は予報とする。

---

## Phase 0: 調査結果（2026-08-20 実測）

以下はすべて実際に叩いて確認した値であり、推測ではない。

### エンドポイント

| 用途 | URL | 実測 |
|---|---|---|
| 府県天気予報 | `https://www.jma.go.jp/bosai/forecast/data/forecast/{office}.json` | HTTP 200 / 2.4〜5.3 KB / 27〜65 ms |
| 天気概況（文章） | `https://www.jma.go.jp/bosai/forecast/data/overview_forecast/{office}.json` | HTTP 200 |
| 地域コード定義 | `https://www.jma.go.jp/bosai/common/const/area.json` | HTTP 200 / 256 KB |
| アメダス実況 | `https://www.jma.go.jp/bosai/amedas/data/point/{id}/{yyyyMMdd}_{HH}.json` | HTTP 200 |

**10官署ぶんの1回の取得は合計 34.2 KiB。** 認証・セッション確立・同意フローはいずれも不要で、
`requests` の素の GET だけで完結する（OCCTO のような `prepare()` は要らない）。

### レスポンスヘッダ（重複排除に使える）

```
last-modified: Thu, 20 Aug 2026 07:44:21 GMT
etag: "c6f6d57fd4f13f36780ef90773a14afb"
cache-control: max-age=60
```

`ETag` と `Last-Modified` の両方が返る。既存の ingestion log
（`common/raw_ingestion_log.py`）は `etag` / `last_modified` / `file_hash` の3列を
すでに持っており、そのまま使える。

> 注意: 上の `Last-Modified` は **16:44 JST**、本文の `reportDatetime` は **17:00 JST**。
> 気象庁は定時より前に「定時の名目時刻を刻んだファイル」を置く。
> **`Last-Modified` と `reportDatetime` は一致しない。**重複排除は本文の sha256 で行い、
> ヘッダは補助情報として記録するだけに留める（既存 JEPX と同じ方針）。

### 地域コード体系

`area.json` は4階層。実測件数は次のとおり：

| 階層 | 件数 | 内容 |
|---|---|---|
| `centers` | 11 | 地方（北海道地方、東北地方 …） |
| `offices` | 58 | **府県予報区。予報 API の URL キーはこれ** |
| `class10s` | 142 | 予報細分区（「東京地方」「多摩西部」…） |
| `class15s` | 375 | |
| `class20s` | 1805 | 市区町村 |

**`centers`（11地方）は電力10エリアと一致しない。** 実測した対応のズレ：

| JMA center | 電力エリアとのズレ |
|---|---|
| 中国地方（山口県を除く） | 中国電力は山口県を含む |
| 九州北部地方（山口県を含む） | 逆に山口県を含んでいる |
| 北陸地方 | 新潟県を含む（新潟は東北電力エリア） |
| 関東甲信地方 | 長野県を含む（長野は中部電力エリア） |
| 九州 | 「九州北部」「九州南部・奄美」の2 center に割れている |

そのため **center を使わず、エリアごとに代表官署（office）を1つ選ぶ**方式を採る（後述）。

### 予報 JSON の構造（ここが実装上の最大の罠）

トップレベルは**2要素の配列**。10官署すべてで構造が一致することを確認済み：

| 要素 | 内容 | timeSeries の timeDefines 長 |
|---|---|---|
| `[0]` | 短期予報（3日） | `[3, 5, 2]` |
| `[1]` | 週間予報（7日） | `[7, 7]` + `tempAverage` / `precipAverage` |

**罠 1 — timeSeries は zip できない。**
`[0]` の3系列は timeDefines の長さが 3 / 5 / 2 と**すべて異なる**。それぞれ
「日別の天気」「6時間別の降水確率」「最低/最高気温」という別の粒度である。
1つの横持ちテーブルに畳むことはできない。

```
element[0].timeSeries[0]  3 defines  weatherCodes, weathers, winds, waves   ← 日別
element[0].timeSeries[1]  5 defines  pops                                   ← 6時間別
element[0].timeSeries[2]  2 defines  temps                                  ← 最低/最高
element[1].timeSeries[0]  7 defines  weatherCodes, pops, reliabilities      ← 日別（週間）
element[1].timeSeries[1]  7 defines  tempsMin/Max(+Upper/Lower)             ← 日別（週間）
```

**罠 2 — `areas[].area.code` の意味が系列によって違う。**
同じ `area.code` というキー名なのに、指しているコード体系が2種類ある：

| 系列 | code の正体 | 実測例 |
|---|---|---|
| 天気・降水確率・風・波 | **class10 コード（6桁）** | `130010` 東京地方 |
| 気温（`temps`, `tempsMin/Max`） | **アメダス観測所番号（5桁）** | `44132` 東京 |

これを1本の `area_code` 列として持つと、意味の異なる値が混ざる。
**気温は別テーブルに分ける**（後述の Silver 設計）。

**罠 3 — 短期と週間で class10 の粒度が違う。**
官署によって、週間予報側が office コードまで粗くなる：

| 官署 | 短期の class10 | 週間の class10 |
|---|---|---|
| 040000 宮城県 | `040010` 東部 / `040020` 西部 | `040010` / `040020`（同じ） |
| 230000 愛知県 | `230010` 西部 / `230020` 東部 | **`230000` 愛知県（粗い）** |
| 400000 福岡県 | `400010`〜`400040` の4区分 | **`400000` 福岡県（粗い）** |

短期と週間を同じ `class10_code` で単純結合すると、官署によって繋がったり繋がらなかったり
する。**結合はエリア代表 class10 に正規化してから行う。**

**罠 4 — 配列長は公表時刻で変わる。**
上の `[3, 5, 2]` / `[7, 7]` は **17:00 JST 発表時点の実測値**である。気象庁の短期予報は
05:00 / 11:00 / 17:00 の3回発表で、発表時刻によって「今日」が含まれるか否かが変わり、
特に `temps` の要素数は 2 と 4 のあいだで変動する（05:00 発表は今日の最高気温を含む）。
**要素数をハードコードしてはならない。`timeDefines` の長さを読んで回すこと。**
→ Phase 1 稼働後、3発表時刻ぶんの実物を採取して確定させる（受け入れ条件に含める）。

---

## 確定済みの設計判断（再検討不要）

| 項目 | 決定 | 理由 |
|---|---|---|
| 実装順序 | **Raw を最優先で常時稼働。Bronze/Silver は後追い** | 過去データが取れないため（上記） |
| 取得方式 | 素の GET 10本。`BaseHttpScraper` を継承、`prepare()` は不要 | セッション確立フローが無いことを実測で確認済み |
| エリア対応 | **1エリア = 代表官署1つ**（下表） | center が電力エリアと一致しないため |
| Bronze の形 | **全列 string の縦持ち（EAV 風）1テーブル** | JSON の配列長が発表時刻で変わるため、横持ちは壊れる |
| Silver の形 | **粒度別に4テーブル**へピボット | timeSeries が zip できないため |
| 気温の扱い | class10 系とは**別テーブル** | `area.code` の体系が違うため |
| 重複排除 | 本文 sha256。既存 ingestion log をそのまま利用 | JEPX / 北陸と同一方式 |
| Silver 書き込み | `common/silver_write.py` の区間 replace | 既存6データセットと同一。`upsert()` は使わない |
| エリア名の表記 | JEPX に合わせた英語名（`Hokkaido`, `Tohoku`, …） | `silver/jepx_spot_price/jepx_spot_price_area.csv` の `area_name` と結合するため |
| マッピングの置き場 | **Python の定数 + スキーマ CSV**（dbt seed ではない） | dbt にモデルが1本も無く、seed だけ導入するのは経路が増える。`data_model.md` の「dbt seed で管理」という記述は本プランで**上書き**する |

### エリア ↔ 代表官署の対応表（全件 HTTP 200 で疎通確認済み）

| 電力エリア | `area_name` | 代表官署 | 官署名 | 代表 class10 | 代表アメダス | 代表都市 |
|---|---|---|---|---|---|---|
| 北海道 | `Hokkaido` | `016000` | 札幌管区気象台 | `016010` 石狩地方 | `14163` | 札幌 |
| 東北 | `Tohoku` | `040000` | 仙台管区気象台 | `040010` 東部 | `34392` | 仙台 |
| 東京 | `Tokyo` | `130000` | 気象庁 | `130010` 東京地方 | `44132` | 東京 |
| 北陸 | `Hokuriku` | `170000` | 金沢地方気象台 | `170010` 加賀 | `56227` | 金沢 |
| 中部 | `Chubu` | `230000` | 名古屋地方気象台 | `230010` 西部 | `51106` | 名古屋 |
| 関西 | `Kansai` | `270000` | 大阪管区気象台 | `270000` 大阪府 | `62078` | 大阪 |
| 中国 | `Chugoku` | `340000` | 広島地方気象台 | `340010` 南部 | `67437` | 広島 |
| 四国 | `Shikoku` | `370000` | 高松地方気象台 | `370000` 香川県 | `72086` | 高松 |
| 九州 | `Kyushu` | `400000` | 福岡管区気象台 | `400010` 福岡地方 | `82182` | 福岡 |
| 沖縄 | `Okinawa` | `471000` | 沖縄気象台 | `471010` 本島中南部 | `91197` | 那覇 |

選定基準は**各エリアの最大需要地を管轄する官署**。電力需要との相関を見るのが目的なので、
エリアの地理的重心ではなく人口・需要の集中点を採る。

> **既知の割り切り（承知のうえで受け入れる）**
> - 1官署は電力エリア全域を代表しない。北海道電力エリアは札幌の天気だけでは説明しきれず、
>   九州は福岡と鹿児島で気温が数度違う。**エリア加重平均が要るとわかった時点で、
>   同一エリア内の複数官署を取りに行く**。Bronze が縦持ちなので、官署を足すのは
>   行が増えるだけでスキーマ変更を伴わない（この設計を選んだ理由のひとつ）。
> - `Okinawa` に対応する JEPX エリアプライスは存在しない（JEPX は9エリア）。
>   需給データ側とは繋がるが、スポット価格とは繋がらない。

---

## Phase 1: Raw 収集を稼働させる（最優先）

### 成果物

| ファイル | 内容 |
|---|---|
| `src/pipeline/raw/source_to_raw_jma_forecast.py` | `JMAForecastScraper` + `scrape_jma_forecast_raw()` |
| `src/cli/commands/jma.py` | `CommandSpec`（`scrape-jma-forecast`） |
| `src/cli/commands/__init__.py` | `*jma.COMMANDS` を `ALL_COMMANDS` に追加 |
| `tests/pipeline/raw/test_source_to_raw_jma_forecast.py` | |

`src/main.py` は**触らない**（`CommandSpec` を足すだけ。CLAUDE.md の規約）。

### 設計

```python
JMA_FORECAST_URL = "https://www.jma.go.jp/bosai/forecast/data/forecast/{office_code}.json"
OBJECT_PREFIX    = "raw/jma/forecast"
DATASET_NAME     = "jma_forecast"
```

オブジェクトキーは既存の北陸 power_usage と同じ「スナップショット堆積」型：

```
raw/jma/forecast/office=130000/ingested_at=20260820T080000/forecast.json
raw/jma/forecast/manifests/office=130000/latest.json
```

- 1回の実行で10官署ぶんを順に取得する。官署間は 1 秒スリープ（10リクエストなので
  JEPX の 3 秒より短くてよいが、無遅延にはしない）。
- 本文の sha256 が ingestion log の直近と一致したら**その官署だけスキップ**し、
  他官署は続行する。10官署が一斉に更新されるとは限らないため。
- 1官署の失敗は他官署を止めない（OCCTO backfill と同じ「記録して続行」方式）。
  最後に失敗官署を列挙して非ゼロ終了する。
- ingestion log の `fiscal_year` 列は JEPX 由来で本データセットに意味を持たない。
  **`snapshot_date`（JST の取得日）を使い、`fiscal_year` は null とする。**
  引き当てには既存の `resolve_latest_raw_object_by_snapshot_date()`
  （`common/raw_ingestion_log.py`）をそのまま使える。北陸 power_usage が同じ理由で
  同じ関数を使っており、新規実装は不要。

### スケジューリング（要判断・下の「未決事項」参照）

Raw を「常時稼働」させる仕組みが**現時点でリポジトリに存在しない**。
Airflow は `[dependency-groups] airflow` に退避済みで、DAG も実行環境も無い。
Phase 1 は**この決着を含めて完了**とする。決まるまでは手動実行でもよいので毎日叩く。

### 受け入れ条件

- [ ] `uv run python src/main.py scrape-jma-forecast` が10官署ぶんの JSON を Raw に置く
- [ ] 同じコマンドを連続実行すると2回目は10官署とも `skipped` になる
- [ ] 1官署を故意に 404 にしても残り9官署は成功し、終了コードが非ゼロになる
- [ ] ingestion log に10行が `is_latest=true` / `bronze_status='pending'` で載る
- [ ] **毎日自動で走る経路が決まっている**（cron / GitHub Actions / 手動運用のいずれか明記）

---

## Phase 2: Bronze

### テーブル: `bronze.jma_forecast`（1テーブル・縦持ち）

JSON の構造を**そのまま平坦化した不変スキーマ**にする。横持ちにしないのは罠4の通り、
配列長が発表時刻で変わるため。この形なら気象庁が系列を1本増やしても行が増えるだけで済む。

`configuration/iceberg/schema/bronze/jma_forecast/jma_forecast.csv`

| field_id | name | type | is_identifier | 内容 |
|---|---|---|---|---|
| 1 | `office_code` | string | TRUE | `130000` |
| 2 | `report_datetime` | string | TRUE | `2026-08-20T17:00:00+09:00` |
| 3 | `block_index` | string | TRUE | `0` = 短期 / `1` = 週間 |
| 4 | `series_index` | string | TRUE | element 内の timeSeries 添字 |
| 5 | `area_code` | string | TRUE | class10 コード or アメダス番号 |
| 6 | `element_name` | string | TRUE | `weathers`, `pops`, `tempsMax` … |
| 7 | `time_index` | string | TRUE | timeDefines の添字 |
| 8 | `time_define` | string | FALSE | `2026-08-21T00:00:00+09:00` |
| 9 | `area_name` | string | FALSE | `東京地方` |
| 10 | `value` | string | FALSE | 値（空文字あり） |
| 11 | `publishing_office` | string | FALSE | `気象庁` |
| + | `source_data` / `status` / `ingestion_time` / `ingestion_date` / `execution_id` | | | 既存メタデータ列 |

パーティション: `ingestion_date` の `year`（`data_model.md` の原則どおり）。

`tempAverage` / `precipAverage`（平年値）は時系列ではないので、
`block_index='1'` / `series_index='normal'` / `element_name='tempAverage.min'` のように
同じ縦持ちに載せる。専用テーブルは作らない。

### 実装上の注意

**このリポジトリ初の JSON ソースである。** 既存6データセットはすべて CSV で、
`build_schema_exprs()`（`common/pipeline_utilities.py`）は
「CSV の列名 → Polars キャスト」を前提にしている。

- cp932 デコードは**不要**。JMA は UTF-8。
- 平坦化は Python 側で行い、`pl.DataFrame` を組んでから `add_metadata()` を掛ける。
  全列 string なので `build_schema_exprs()` は実質パススルーになる。
  **`build_schema_exprs()` を JSON 対応に改造しない**こと（CSV 6データセットへの影響が出る）。
- 想定行数（2026-08-20 実測）: 東京 `130000` が1スナップショット **277 行**、
  10官署合計で **1,323 行**。1日3発表なら **約 3,969 行/日 ≒ 145 万行/年**。
  年パーティションで妥当（月次に切ると small file problem 側に倒れる）。

### 受け入れ条件

- [ ] `ingest-jma-forecast-bronze` が Raw の JSON を読んで Bronze に append する
- [ ] 処理済みスナップショットは `--allow-duplicate-source` なしでは再取込されない
- [ ] `[3,5,2]` 以外の配列長（05:00 発表の実物）でも例外なく取り込める
- [ ] 空文字の値（週間予報の初日の `pops` は `""`）が null ではなく空文字として残る

---

## Phase 3: Silver

Bronze の縦持ちを **粒度ごとに4テーブルへピボット**する。粒度が違うものを1本に畳めない
（罠1）ので、テーブルを分けるのが最小の設計になる。

すべて `configuration/iceberg/schema/silver/jma_forecast/` 配下。
パーティションは `forecast_date` / `valid_from` の `year`。

### 3-1. `silver.jma_forecast_daily`（日別の天気）

短期3日と週間7日を**統合**し、`source_kind` で出所を区別する。重複する2〜3日目は
両方の行が残る（短期のほうが精度が高いので、利用側は `source_kind='short'` を優先する）。

| 列 | 型 | key | 内容 |
|---|---|---|---|
| `area_name` | string | ✓ | `Tokyo`（電力エリア名） |
| `forecast_date` | date | ✓ | 予報対象日（JST） |
| `source_kind` | string | ✓ | `short` / `weekly` |
| `report_datetime` | timestamptz | ✓ | 発表時刻 |
| `office_code` | string | | `130000` |
| `class10_code` | string | | `130010` |
| `weather_code` | string | | `111` |
| `weather_text` | string | | `晴れ　夜　くもり…`（短期のみ） |
| `wind_text` | string | | 短期のみ |
| `wave_text` | string | | 短期のみ |
| `pop_pct` | int | | 週間のみ（短期の pops は 6 時間別なので 3-2 へ） |
| `reliability` | string | | `A`〜`C`。週間のみ |

`report_datetime` をキーに含めるのは、**同じ予報対象日に対する複数回の発表を履歴として
残すため**。「発表ごとに予報がどう変わったか」は電力需要予測の重要な特徴量であり、
最新だけ上書きすると失われる。区間 replace の窓は `forecast_date` で取る。

### 3-2. `silver.jma_forecast_pop`（6時間別 降水確率）

| 列 | 型 | key | 内容 |
|---|---|---|---|
| `area_name` | string | ✓ | |
| `valid_from` | timestamptz | ✓ | 対象6時間の開始（JST） |
| `report_datetime` | timestamptz | ✓ | |
| `class10_code` | string | | |
| `pop_pct` | int | | 0〜100 |

### 3-3. `silver.jma_forecast_temp`（気温）

`area.code` がアメダス番号である系列（罠2）を独立させたテーブル。

| 列 | 型 | key | 内容 |
|---|---|---|---|
| `area_name` | string | ✓ | |
| `forecast_date` | date | ✓ | |
| `source_kind` | string | ✓ | `short` / `weekly` |
| `report_datetime` | timestamptz | ✓ | |
| `amedas_code` | string | | `44132` |
| `amedas_name` | string | | `東京` |
| `temp_min_c` | double | | |
| `temp_max_c` | double | | |
| `temp_min_lower_c` / `temp_min_upper_c` | double | | 週間のみ（信頼区間） |
| `temp_max_lower_c` / `temp_max_upper_c` | double | | 週間のみ |

短期の `temps`（2要素、`00:00` と `09:00` の timeDefine）は
**「00:00 の値 = その日の最低、09:00 の値 = その日の最高」として `forecast_date` に畳む。**
timeDefine を素直に時刻として扱うと「深夜0時の気温25℃」という誤った意味になる。

### 3-4. `silver.jma_area_mapping`（静的マッピング）

上のエリア対応表をそのままテーブル化した10行の静的表。
`area_name`, `office_code`, `office_name`, `class10_code`, `amedas_code`, `city_name`,
`has_jepx_area_price`（沖縄のみ false）。

Python の定数を単一の出典とし、provision 時に書き出す。dbt seed は使わない（前述）。

### 受け入れ条件

- [ ] 4テーブルが `provision-silver-tables` で作成できる
- [ ] 10エリアすべてに `jma_forecast_daily` の行が存在する
- [ ] `ensure_unique_keys()` がキー重複ゼロを確認する
- [ ] `jma_forecast_daily` を `jepx_spot_price_daily` に `area_name` + 日付で内部結合し、
      沖縄以外の9エリアぶんが結合できる
- [ ] 同じ `forecast_date` に対する複数 `report_datetime` の行が残っている

---

## Phase 4: オーケストレーター・ドキュメント

| 成果物 | 内容 |
|---|---|
| `src/orchestration/pl_jma_weather_forecast.py` | `run-jma-orchestrator`。Raw→Bronze→Silver を1コマンドで。既存の `PipelineStepResult` を返す形に揃える |
| `docs/architecture/pl_jma_weather_forecast_design.md` | `pl_jepx_spot_price_design.md` と同形式の Mermaid 設計図 |
| `docs/architecture/data_model.md` §4 | プレースホルダを本プランの確定内容で置換。「dbt seed で管理」の記述を撤回 |
| `docs/tasks/tasks.md` §2 | チェックリストを本プランの Phase に差し替え |
| `README.md` | データセット表・コマンド例に追加 |
| `CLAUDE.md` | データセット一覧に `jma_forecast` を追記 |

---

## Phase 5（任意・別プラン化してよい）

- **アメダス実況の取り込み** — 予報の精度検証と、需要実績との相関分析に必要。
  疎通は確認済み（上記）。保持期間の実測が必要。
- **Gold 層** — 需要予測用の特徴量テーブル（エリア×日×気温・HDD/CDD・予報誤差）。
  `plan_jepx_gold.md` §D+14 が「2週間先は気象予報の精度が無いので点予測にしない」と
  書いており、本パイプラインはその判断の裏付けデータを供給する立場になる。
- **複数官署への拡張** — エリア加重平均が必要になった場合。Bronze 縦持ちなので
  スキーマ変更なしで官署を追加できる。

---

## 未決事項（着手前に決めること）

| # | 論点 | 選択肢 | 推奨 |
|---|---|---|---|
| 1 | **Raw を毎日走らせる仕組み** | (a) GitHub Actions の `schedule` (b) ホスト側 cron (c) Airflow を戻す (d) 当面手動 | **(a)**。実行環境が既にあり追加インフラが要らない。ただし RustFS がローカル Compose なので、CI から書き込めるストレージが必要という前提条件がある。ここが崩れるなら (b) |
| 2 | **取得頻度** | (a) 1日3回（発表時刻直後 05:10/11:10/17:10 JST） (b) 1時間ごと | **(a)**。定時発表が3回で、随時更新は警報級の変化時のみ。sha256 で弾くので (b) でも害は無いが、リクエスト数が8倍になる割に増える情報が少ない |
| 3 | **`overview_forecast`（文章）を取るか** | 取る / 取らない | **当面取らない**。自由文で分析に使いにくく、`headlineText` が要るとわかってからでよい |
| 4 | **Silver に発表履歴を残すか** | 残す / 最新のみ | **残す**（3-1 に記載済み。予報の改訂履歴は特徴量になる）。ただし行数が発表回数ぶん増える点は承知しておく |
| 5 | **`data_model.md` の既存記述の扱い** | 上書き / 併記 | **上書き**。`bronze.jma_forecast` の PK を `forecast_date, area_code, forecast_time` としているが、実データの構造と合わない |

---

## リスク

| リスク | 影響 | 対策 |
|---|---|---|
| **収集開始が遅れるほどデータが失われる** | 回復不能 | Phase 1 を最優先。Bronze 未完成でも Raw だけ回す |
| 配列長がハードコードされる | 05:00 発表で例外 | `timeDefines` 長を読んで回す。3発表時刻の実物でテストする（Phase 1 受け入れ条件） |
| `area.code` の二重の意味を取り違える | 気温とclass10 が混ざった無意味なテーブル | Silver でテーブルを分離（3-3） |
| 短期と週間の class10 粒度差（罠3） | 官署によって結合できない | エリア代表 class10 に正規化してから結合 |
| 代表都市1点がエリアを代表しない | 相関分析の精度が頭打ち | 承知のうえで採用。必要になれば官署を追加（Bronze 縦持ちで吸収可能） |
| 気象庁 API の非公式性 | 予告なく仕様変更・停止 | 縦持ち Bronze なら構造変化を行として吸収できる。Raw に生 JSON を残すので再解釈も可能 |
| RustFS がローカル Compose のみ | CI からの定期実行ができない | 未決事項 #1 で決着させる |

---

## 関連ドキュメント

- `docs/architecture/data_model.md` §4 — 本プランで置換する既存記述
- `docs/tasks/tasks.md` §2 — 本プランで差し替えるチェックリスト
- `docs/architecture/pipeline_flow.md` — 層ごとの契約（ソース非依存）
- `docs/tasks/plan_occto_pipeline.md` — 本プランの書式の雛形
- `docs/tasks/plan_jepx_gold.md` §D+14 — 気象予報の精度に関する既存の判断
- `docs/electric_forecast.txt` — 電力10エリアの出典

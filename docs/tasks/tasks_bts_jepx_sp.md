# Task: JEPX bronze_to_silver の Python 一本化

> **実装担当への前提**
> このドキュメントだけで実装できるように、変更対象ファイル・確定済みの設計判断・検証可能な受け入れ条件を明記しています。
> 設計の背景・根拠は `docs/data_platform_summary.md` を参照。参照が必要な箇所には doc の節番号を併記しています。

## ゴール

DuckDB で bronze Iceberg テーブルを直接 `iceberg_scan` し、変換後に PyIceberg `upsert` で silver Iceberg テーブルへ書き込む単一経路を `src/pipeline/silver/bronze_to_silver_jepx_spot_price.py` に実装する。
中間 DuckDB ファイル (`src/dbt/jepx_power/jepx_power.duckdb`) を経由する現行の dbt 経路は撤去する。

## 確定済みの設計判断（再検討不要）

| 項目 | 決定 |
|---|---|
| dbt との関係 | **Python に一本化。** JEPX の dbt staging / silver モデルは削除する（dbt プロジェクト自体は OCCTO が使うので残す） |
| テーブル構成 | **現行の3分割を維持**（`silver.jepx_spot_price_base` / `_area` / `_block`）。doc の単一テーブル案は採用しない |
| upsert 方式 | PyIceberg ネイティブ `Table.upsert(df, join_cols=[...])`（方式A / doc §6） |
| 実行範囲 | **全期間 upsert が既定。** 日次とフルリフレッシュは同一コードパスで、差は WHERE 句のみ（doc §5） |
| quarantine | **今回のスコープ外（Phase 4）。** ただし Phase 2 で `violations` 列を作り、除外件数をログ・戻り値に出すところまでは実装する |

## 現状の問題（この実装で解消する）

`src/dbt/jepx_power/models/staging/stg_jepx_spot_price.sql` と `src/orchestration/jepx_pipeline.py` に以下の乖離がある。

1. **タイムゾーンが 9 時間ずれている（最重要）**
   現行 `stg_jepx_spot_price.sql:45` は `cast(... as timestamptz)` で素キャストしており、JST の受渡日時を UTC として解釈している。実測で確認済み:

   | 入力 | 現行の出力（誤） | 正しい出力 |
   |---|---|---|
   | 受渡日 `2024-04-01` / コマ `1` | `2024-04-01T00:00:00Z` | `2024-03-31T15:00:00Z` |
   | 受渡日 `2024-04-01` / コマ `48` | `2024-04-01T23:30:00Z` | `2024-04-01T14:30:00Z` |

2. **導出元の列が silver に残っていない**（doc §9 原則1違反）
   `silver_jepx_spot_price_{base,area,block}.sql` が `delivery_datetime` のみ出力し、`delivery_date` / `time_code` を drop している。JST 暦での日次集計に逆変換が必要になり、日跨ぎ事故のリスクが残る。

3. **監査列が全 NULL**
   `src/orchestration/jepx_pipeline.py:133-140` が不足列を `pl.lit(None)` で埋めるため、silver の `source_data` / `status` / `ingestion_time` / `execution_id` が全て NULL。

4. **upsert の join キーが未指定**
   `src/orchestration/jepx_pipeline.py:144` は `target_table.upsert(casted_arrow_table)` で `join_cols` 省略。スキーマ CSV の identifier にフォールバックしている。

5. **異常行が理由なく捨てられている**
   `stg_jepx_spot_price.sql:69-70` の `where` で除外するのみ。件数も理由も残らない。

6. **価格が整数に丸められている（Phase 1 で発見・修正済み）**
   スキーマ CSV が `system_price` / `area_price` を `long` と定義しており、staging SQL も `TRY_CAST(... AS BIGINT)` していた。
   **DuckDB の `TRY_CAST('17.75' AS BIGINT)` は NULL ではなく `18` を返す**ため、エラーにも NULL にもならず静かに精度が失われていた。

   | | bronze（生値） | 現行 silver |
   |---|---|---|
   | `system_price` | `'17.75'`, `'14.83'`, `'12.70'` | `15`, `26`, `35` |
   | `area_price` | 同様に小数2桁 | `17`, `19`, `19` |

   doc §9 の通り `decimal` が正しい。スキーマ CSV は Phase 1 で修正済み（`decimal` → `DecimalType(32, 3)`）。
   **Phase 2 の staging SQL でも価格列は `DECIMAL` にキャストすること**（後述 2-3 を参照）。
   入札量・約定量・ブロック量は kWh の整数なので `long` のままでよい。

---

## 確定した前提

**コマ番号は「開始時刻」を指す**（ユーザー確認済み / 2026-08-02）。

コマ1 = 00:00〜00:30 の区間で、`delivery_datetime` にはその **開始時刻 00:00 (JST)** を格納する。
JEPX の生 CSV ヘッダーは `受渡日,時刻コード,...` のみで定義の記載がないため、この前提はコード上の定数とコメントで明示すること。もし将来「終了時刻」と判明した場合は全レコードが一律 30 分ずれるだけなので、オフセット定数の一箇所修正で是正できる構造にしておく。

---

# Phase 1 — Silver スキーマ拡張とテーブル再作成

## 1-1. スキーマ CSV の書き換え

対象: `configuration/iceberg/schema/silver/jepx_spot_price_{base,area,block}.csv`

変更内容は3ファイル共通:

- `delivery_date` (date) と `time_code` (int) を **末尾の field_id で追記**
- 追記した2列を `is_identifier=TRUE, required=TRUE` にする
- 既存 `delivery_datetime` の `is_identifier` を **TRUE → FALSE**（物理事実として保持するが、キーはビジネス区分に移す / doc §9）
- `partition_transform` を `delivery_datetime` の `hour` から外し、`delivery_date` に `year` を付与（doc §7。実効化は Phase 5）

監査列 (`source_data`, `status`, `ingestion_time`, `ingestion_date`, `execution_id` / field_id 9001–9005) は `src/common/schema_builder.py:126-164` が自動注入するので **CSV には書かない**。

### `jepx_spot_price_base.csv`（書き換え後の全文）

```csv
field_id,name,type,is_identifier,required,doc,partition_transform,source_name,comment
1,delivery_datetime,timestamptz,FALSE,TRUE,Delivery timestamp in UTC derived from delivery_date and time_code,,受渡日時,
2,selling_bid_volume,long,FALSE,FALSE,Selling bid volume (kWh),,売り入札量(kWh),
3,purchase_bid_volume,long,FALSE,FALSE,Purchase bid volume (kWh),,買い入札量(kWh),
4,contracted_volume,long,FALSE,FALSE,Total contracted volume (kWh),,約定総量(kWh),
5,system_price,decimal,FALSE,FALSE,System price (Yen/kWh),,システムプライス(円/kWh),
6,delivery_date,date,TRUE,TRUE,Business delivery date in JST calendar,year,受渡日,
7,time_code,int,TRUE,TRUE,Business time code (1-48),,時刻コード,
```

### `jepx_spot_price_area.csv`（書き換え後の全文）

```csv
field_id,name,type,is_identifier,required,doc,partition_transform,source_name,comment
1,delivery_datetime,timestamptz,FALSE,TRUE,Delivery timestamp in UTC derived from delivery_date and time_code,,受渡日時,
2,area_name,string,TRUE,TRUE,"Area name(Hokkaido, Tohoku, Tokyo, Chubu, Hokuriku, Kansai, Chugoku, Shikoku, Kyushu)",,エリア名,
3,area_price,decimal,FALSE,FALSE,Area price(Yen/kWh),,エリア価格(円/kWh),
4,delivery_date,date,TRUE,TRUE,Business delivery date in JST calendar,year,受渡日,
5,time_code,int,TRUE,TRUE,Business time code (1-48),,時刻コード,
```

### `jepx_spot_price_block.csv`（書き換え後の全文）

```csv
field_id,name,type,is_identifier,required,doc,partition_transform,source_name,comment
1,delivery_datetime,timestamptz,FALSE,TRUE,Delivery timestamp in UTC derived from delivery_date and time_code,,受渡日時,
2,block_selling_bid_volume,long,FALSE,FALSE,Block selling bid volume (kWh),,売りブロック入札総量(kWh),
3,block_selling_contracted_volume,long,FALSE,FALSE,Block selling contracted volume (kWh),,売りブロック約定総量(kWh),
4,block_purchase_bid_volume,long,FALSE,FALSE,Block purchase bid volume (kWh),,買いブロック入札総量(kWh),
5,block_purchase_contracted_volume,long,FALSE,FALSE,Block purchase contracted volume (kWh),,買いブロック約定総量(kWh),
6,delivery_date,date,TRUE,TRUE,Business delivery date in JST calendar,year,受渡日,
7,time_code,int,TRUE,TRUE,Business time code (1-48),,時刻コード,
```

## 1-2. テーブル再作成 → **Phase 3 の切り替え時に実行する（決定済み）**

**`provision_table()` では今回の変更を反映できない。** `src/common/iceberg.py:104-113` は `add_column` しか行わず、identifier / required の変更は無視される。したがって silver テーブルの drop → 再 provision が必要になる。

> **⚠️ ただし Phase 1 の時点では実行しないこと（ユーザー判断で決定済み）。**
> 新スキーマでは `delivery_date` / `time_code` が `required=TRUE` だが、既存の dbt silver モデルはこの2列を出力しない。そのため Phase 1 でテーブルを作り直すと、Phase 2 が完成するまで **silver が空のまま既存 dbt 経路も動かなくなる**。
> drop → provision → 初回フル実行は **Phase 3 の切り替えと同時に、一連の作業として実行する**。

実行する際の手順:

```bash
# 1. 既存 silver テーブルを削除
#    src/setup/drop_table.py は bronze.jepx_spot_price がハードコードされているので使わないこと。
#    common.iceberg.delete_table(catalog, "silver.jepx_spot_price_{base,area,block}") を使う。
# 2. 新スキーマで再作成（occto の CSV も同ディレクトリにあるが、差分なしとして素通りする）
uv run python src/main.py provision-silver-tables
# 3. 初回フル実行で復元（fiscal_year 指定なし = 全期間 upsert）
uv run python src/main.py ingest-jepx-bronze-to-silver
```

### 復元可能性の確認結果（2026-08-02 時点で検証済み）

| 対象 | 実測値 |
|---|---|
| `bronze.jepx_spot_price` | 5,856 行 / `delivery_date` 2026-04-01〜2026-07-31 / `source_data` は1ファイルのみ |
| `silver.jepx_spot_price_base` | 5,856 行（= 122日 × 48コマ、bronze と一致） |
| `silver.jepx_spot_price_block` | 5,856 行 |
| `silver.jepx_spot_price_area` | 52,704 行（= 5,856 × 9エリア） |

silver は bronze の内容と過不足なく一致しており、**bronze からの全期間 upsert で完全に復元できる**ことを確認済み。なお現行 silver の `delivery_datetime` はいずれも 9 時間ずれた値なので、どのみち作り直しが必要。

## Phase 1 の完了条件
- [x] 3つのスキーマ CSV が上記の通り更新されている
- [x] `build_table_schema()` が3ファイルとも正しく解釈する（identifier: base/block = `delivery_date`,`time_code` / area = 加えて `area_name`）
- [ ] ~~テーブル再作成~~ → Phase 3 へ移動

---

# Phase 2 — 変換モジュール本体（TDD）

## 2-1. ファイル構成

新規: `src/pipeline/silver/bronze_to_silver_jepx_spot_price.py`

**設計上の要点:** SQL を実行する関数は `source_relation`（テーブル名の文字列）を引数に取ること。
テストが `iceberg_scan('s3://...')` の代わりにローカル TEMP TABLE 名を渡せるようになり、**RustFS も Iceberg も無しで変換ロジックを単体テストできる。** これが本モジュールのテスト容易性の核。

```python
DEFAULT_BRONZE_LOCATION = "s3://jp-power-grid-dev/bronze/jepx_spot_price"
DEFAULT_SILVER_SCHEMA_DIR = "/workspace/configuration/iceberg/schema/silver"
STAGING_RELATION = "jepx_silver_staging"

AREA_PRICE_COLUMNS = (
    "area_price_hokkaido", "area_price_tohoku", "area_price_tokyo",
    "area_price_chubu", "area_price_hokuriku", "area_price_kansai",
    "area_price_chugoku", "area_price_shikoku", "area_price_kyushu",
)


@dataclass(frozen=True)
class SilverUpsertResult:
    table_identifier: str
    rows_updated: int
    rows_inserted: int


@dataclass(frozen=True)
class BronzeToSilverResult:
    execution_id: str
    upserts: list[SilverUpsertResult]
    dropped_row_count: int


def create_duckdb_connection() -> duckdb.DuckDBPyConnection:
    """httpfs / iceberg / icu をロードし S3 設定を適用した接続を返す。"""


def build_staging_relation(
    conn: duckdb.DuckDBPyConnection,
    *,
    source_relation: str,
    fiscal_year: int | None = None,
) -> None:
    """型変換 → dedup → violations 判定を行い TEMP TABLE を作る。"""


def extract_base_frame(conn: duckdb.DuckDBPyConnection) -> pl.DataFrame: ...
def extract_area_frame(conn: duckdb.DuckDBPyConnection) -> pl.DataFrame: ...
def extract_block_frame(conn: duckdb.DuckDBPyConnection) -> pl.DataFrame: ...


def count_dropped_rows(conn: duckdb.DuckDBPyConnection) -> int:
    """violations が非空の行数を返す（doc §10「可視性」の最小実装）。"""


def upsert_silver_table(
    catalog: Catalog,
    *,
    table_identifier: str,
    frame: pl.DataFrame,
    join_cols: list[str],
) -> SilverUpsertResult:
    """監査列の付与 → ターゲットスキーマへ整列 → PyIceberg upsert。"""


def run_bronze_to_silver_jepx_spot_price(
    *,
    catalog_name: str = "dlh_dev",
    bronze_location: str = DEFAULT_BRONZE_LOCATION,
    fiscal_year: int | None = None,
    execution_id: str | None = None,
) -> BronzeToSilverResult:
    """公開 API。CLI とオーケストレーターの両方からこれを呼ぶ。"""
```

## 2-2. DuckDB 接続設定

`src/dbt/jepx_power/profiles.yml` と同じ設定を Python 側で行う。**`icu` 拡張が必須**（`AT TIME ZONE 'Asia/Tokyo'` のような名前付きタイムゾーンは ICU が無いと解決できない。ロード可能なことは確認済み）。

```python
conn.execute("INSTALL httpfs; LOAD httpfs;")
conn.execute("INSTALL iceberg; LOAD iceberg;")
conn.execute("INSTALL icu; LOAD icu;")          # AT TIME ZONE に必須
conn.execute("SET unsafe_enable_version_guessing = true;")
```

S3 設定は環境変数から取得する（`AWS_ENDPOINT_URL` / `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_REGION`)。
**注意:** DuckDB の `s3_endpoint` はスキーム無しの `host:port` 形式（例 `rustfs:9000`）。`AWS_ENDPOINT_URL` は `http://rustfs:9000` 形式なのでスキームを除去し、`s3_use_ssl` は元のスキームから判定すること。認証情報は必ず環境変数から読み、ハードコードしない。

## 2-3. staging SQL（`build_staging_relation` の中身）

現行 `stg_jepx_spot_price.sql` からの差分に注意。

```sql
CREATE OR REPLACE TEMP TABLE jepx_silver_staging AS
WITH bronze_raw AS (
    SELECT * FROM {source_relation}
),
typed AS (
    SELECT
        COALESCE(
            try_strptime(delivery_date, '%Y-%m-%d'),
            try_strptime(delivery_date, '%Y/%m/%d')
        )                                                            AS delivery_date_d,
        TRY_CAST(time_code AS INTEGER)                               AS time_code_i,
        TRY_CAST(REPLACE(selling_bid_volume, ',', '') AS BIGINT)     AS selling_bid_volume,
        TRY_CAST(REPLACE(purchase_bid_volume, ',', '') AS BIGINT)    AS purchase_bid_volume,
        TRY_CAST(REPLACE(contracted_volume, ',', '') AS BIGINT)      AS contracted_volume,
        -- ★ 価格は必ず DECIMAL。BIGINT だと 17.75 が 18 に丸められる（前述「現状の問題」6）
        TRY_CAST(REPLACE(system_price, ',', '') AS DECIMAL(32, 3))   AS system_price,
        -- area_price_* 9列も同様に DECIMAL(32, 3) へ
        -- block_* 4列は kWh の整数なので BIGINT のまま
        ...
        source_data,
        ingestion_time,
        execution_id
    FROM bronze_raw
),
deduplicated AS (
    SELECT *
    FROM typed
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY delivery_date_d, time_code_i
        ORDER BY ingestion_time DESC NULLS LAST, execution_id DESC NULLS LAST
    ) = 1
),
validated AS (
    SELECT
        *,
        list_filter([
            CASE WHEN delivery_date_d IS NULL THEN 'delivery_date_null' END,
            CASE WHEN time_code_i IS NULL THEN 'time_code_null' END,
            CASE WHEN time_code_i NOT BETWEEN 1 AND 48 THEN 'time_code_out_of_range' END,
            CASE WHEN system_price < 0 THEN 'system_price_negative' END
        ], x -> x IS NOT NULL)                                       AS violations
    FROM deduplicated
)
SELECT
    CAST(delivery_date_d AS DATE)                                    AS delivery_date,
    time_code_i                                                      AS time_code,
    -- ★ 修正の核心: 素キャストではなく JST として解釈してから UTC 化する
    -- (time_code - 1) * 30分 = コマの「開始時刻」（確認済みの前提。定数化してコメントを残すこと）
    (delivery_date_d + ((time_code_i - 1) * INTERVAL 30 MINUTE))
        AT TIME ZONE 'Asia/Tokyo'                                    AS delivery_datetime,
    selling_bid_volume,
    purchase_bid_volume,
    contracted_volume,
    system_price,
    -- area_price_* / block_* をそのまま通す
    ...,
    source_data,
    violations
FROM validated
{fiscal_year_filter}   -- fiscal_year が None なら空文字（全期間 upsert が既定 / doc §5）
```

### 会計年度フィルタ

日次とフルの唯一の差はここだけにすること（doc §5）。bronze の `delivery_date` は string なので **キャスト後の `delivery_date_d` に対して**適用する。

```sql
WHERE delivery_date_d >= DATE '{fiscal_year}-04-01'
  AND delivery_date_d <  DATE '{fiscal_year + 1}-04-01'
```

`fiscal_year` は int としてバリデーションしてから埋め込むこと（`int()` 変換を通し、文字列を直接連結しない）。

### 実装上の注意

- **dedup と validation の順序:** doc §11 のスケルトンに合わせて dedup → validation の順とする。ただし `delivery_date_d` が NULL の行が複数あると dedup で 1 行に潰れるため、Phase 4 で quarantine を実装する際に「全ての異常行を残すか」を再検討すること（このプランの範囲では既知の制約として許容）。
- **`TRY_CAST` の限界:** 「元から NULL」と「変換失敗で NULL」を区別できない（doc §11 注意点）。Phase 4 で cast 前の生値保持と併せて解決する。

## 2-4. 抽出クエリ

### base

```sql
SELECT delivery_date, time_code, delivery_datetime,
       selling_bid_volume, purchase_bid_volume, contracted_volume, system_price,
       source_data
FROM jepx_silver_staging
WHERE len(violations) = 0
```

### block

```sql
SELECT delivery_date, time_code, delivery_datetime,
       block_selling_bid_volume, block_selling_contracted_volume,
       block_purchase_bid_volume, block_purchase_contracted_volume,
       source_data
FROM jepx_silver_staging
WHERE len(violations) = 0
```

### area

**UNPIVOT に渡す前に必要な列だけへ絞り込むこと。** DuckDB の UNPIVOT は列挙外の列をそのまま持ち回るため、絞らないと `block_*` 列まで結果に混入する。

```sql
WITH source AS (
    SELECT delivery_date, time_code, delivery_datetime, source_data,
           area_price_hokkaido, area_price_tohoku, area_price_tokyo,
           area_price_chubu, area_price_hokuriku, area_price_kansai,
           area_price_chugoku, area_price_shikoku, area_price_kyushu
    FROM jepx_silver_staging
    WHERE len(violations) = 0
),
area_unpivoted AS (
    SELECT delivery_date, time_code, delivery_datetime, source_data,
           area_price_column, area_price
    FROM source
    UNPIVOT (
        area_price FOR area_price_column IN (
            area_price_hokkaido, area_price_tohoku, area_price_tokyo,
            area_price_chubu, area_price_hokuriku, area_price_kansai,
            area_price_chugoku, area_price_shikoku, area_price_kyushu
        )
    )
)
SELECT delivery_date, time_code, delivery_datetime,
       split_part(area_price_column, '_', 3) AS area_name,
       area_price, source_data
FROM area_unpivoted
```

> **既知の挙動:** DuckDB の `UNPIVOT` は既定で NULL 値の行を除外する（`INCLUDE NULLS` を付けない限り）。現行 dbt モデルも同じ挙動なので変更しないが、エリア価格が NULL の枠は area テーブルに現れないことを認識しておくこと。

## 2-5. 監査列の付与と upsert

現行は不足列を `pl.lit(None)` で埋めるだけで監査列が全 NULL になっている。これを解消する。

- `source_data` — bronze から引き継いだ値をそのまま使う
- `status` — `'loaded'` を付与
- `ingestion_time` / `ingestion_date` / `execution_id` — `src/common/pipeline_utilities.py` の `add_metadata(df, execution_id)` を再利用する。**実行全体で同一の `execution_id` を使うこと**（3テーブルで揃える）

ターゲットスキーマへの整列とキャストは、既存の `src/orchestration/jepx_pipeline.py:129-144` のパターンを踏襲してよい。

### upsert の join キー（明示指定すること / doc §6）

| テーブル | `join_cols` |
|---|---|
| `silver.jepx_spot_price_base` | `["delivery_date", "time_code"]` |
| `silver.jepx_spot_price_block` | `["delivery_date", "time_code"]` |
| `silver.jepx_spot_price_area` | `["delivery_date", "time_code", "area_name"]` |

```python
result = target_table.upsert(casted_arrow_table, join_cols=join_cols)
```

> `join_cols` を省略すると identifier-field-ids にフォールバックする。Phase 1 でスキーマを直しているため結果的には一致するが、**意図を明示するため必ず渡すこと。**

## 2-6. テスト（実装前に書く / RED → GREEN）

新規: `tests/test_bronze_to_silver_jepx_spot_price.py`

`build_staging_relation` に `source_relation` としてローカル TEMP TABLE 名を渡すことで、S3 / Iceberg なしで実行できる。bronze の全列を string 型で持つ TEMP TABLE を作るフィクスチャを用意すること（bronze スキーマは全列 string / `configuration/iceberg/schema/bronze/jepx_spot_price.csv` 参照）。

| # | テスト名 | 検証内容 |
|---|---|---|
| 1 | `test_delivery_datetime_interprets_time_code_as_jst` | 受渡日 `2024-04-01` / コマ `1` → `2024-03-31T15:00:00Z`。**現行バグの回帰テスト** |
| 2 | `test_time_code_48_maps_to_last_slot` | 受渡日 `2024-04-01` / コマ `48` → `2024-04-01T14:30:00Z` |
| 3 | `test_keeps_latest_row_per_delivery_key` | 同一 `(delivery_date, time_code)` の2行 → `ingestion_time` が最新の行のみ残る |
| 4 | `test_reports_dropped_row_count_for_invalid_rows` | `time_code=0` / `delivery_date=NULL` が除外され、`dropped_row_count` に計上される |
| 5 | `test_area_frame_unpivots_nine_areas` | 1行 → 9行に展開され、`area_name` が `hokkaido`…`kyushu` になる |
| 6 | `test_silver_frames_retain_delivery_date_and_time_code` | base / area / block の全てに導出元列が残っている（doc §9 原則1） |
| 7 | `test_area_frame_excludes_block_columns` | UNPIVOT で `block_*` 列が混入していない |
| 8 | `test_prices_keep_decimal_precision` | `system_price='17.75'` → `Decimal('17.750')`（`18` に丸められない）。**現行バグの回帰テスト** |
| 9 | `test_area_prices_keep_decimal_precision` | area 側も同様に小数が保たれる |
| 10 | `test_upsert_is_idempotent` | `@pytest.mark.integration` — 同じ入力で2回実行しても行数が増えない |

`@pytest.mark.integration` を付けたテストは RustFS / Iceberg を要するため CI では skip される方針（`docs/tasks/plan_jepx_pipeline_and_ci.md` Phase A に準拠）。

## Phase 2 の完了条件
- [x] 上記テストが green（計画の 9 件を 14 件に拡充。日付書式・千桁区切り・fiscal_year・source_data 引き継ぎを追加）
- [x] `uv run ruff check src/ tests/` / `uv run ruff format --check src/ tests/` が通る
- [x] `uv run pyright` が通る
- [x] 除外行が発生した場合に件数と理由別内訳が WARNING でログ出力される

---

# Phase 3 — 経路の一本化（dbt 撤去）

## 3-1. `src/main.py`

- **追加:** サブコマンド `ingest-jepx-bronze-to-silver`
  - 引数: `--catalog` (default `dlh_dev`) / `--bronze-location` / `--fiscal-year`（省略時は全期間）
  - `run_bronze_to_silver_jepx_spot_price()` を呼び、`BronzeToSilverResult` をログ出力
- **削除:** サブコマンド `run-jepx-staging-dbt` / `run-jepx-silver-dbt` とそのハンドラ（`src/main.py:685-727`）
- **維持:** `run-occto-silver-dbt`（OCCTO は dbt のまま）

## 3-2. `src/orchestration/jepx_pipeline.py`

- **置換:** staging / silver の `run_dbt_step` 2 回と `export_jepx_silver_to_iceberg` を、`bronze_to_silver` の 1 ステップに統合
- **削除:** `export_jepx_silver_to_iceberg()`、`DEFAULT_SILVER_EXPORT_MAPPINGS`、`DEFAULT_DBT_DUCKDB_PATH`
- **削除する CLI 引数:** `--staging-select` / `--silver-select` / `--export-silver-to-iceberg` / `--dbt-duckdb-path`
- **維持:** gold ステップ（`--run-gold-step` / `--gold-select`）は dbt のまま。`duckdb` import は不要になれば削除
- `main()` 内の `dbt_duckdb_path` 検証（`src/main.py:560` および `jepx_pipeline.py:416-418`）も併せて削除

## 3-3. dbt モデルの削除

| 削除対象 | 備考 |
|---|---|
| `src/dbt/jepx_power/models/staging/stg_jepx_spot_price.sql` | |
| `src/dbt/jepx_power/models/silver/silver_jepx_spot_price_base.sql` | |
| `src/dbt/jepx_power/models/silver/silver_jepx_spot_price_area.sql` | |
| `src/dbt/jepx_power/models/silver/silver_jepx_spot_price_block.sql` | |
| `_staging__models.yml` / `_silver__models.yml` の JEPX 該当エントリ | OCCTO のエントリは残す |

**dbt プロジェクト `src/dbt/jepx_power/` 自体は削除しない**（`stg_occto_unit_generation_actuals.sql` と `silver_occto_unit_generation_actuals.sql` が使用中）。

> **⚠️ 検証の移管:** `_silver__models.yml` にある JEPX の `not_null` テスト（`delivery_datetime` / `area_name` / `area_price`）が失われる。同等の検証を Phase 2 の `violations` 判定に含めること。

## 3-4. silver テーブルの再作成（Phase 1-2 から移動）

Phase 1 で保留した drop → provision → 初回フル実行をここで実施する。手順と復元可能性の確認結果は **Phase 1-2 の節**を参照。

このステップを 3-1〜3-3 の後に置くのは、新スキーマ（`delivery_date` / `time_code` が required）を満たせるのが新しい Python 経路だけであり、それが動く状態になってから作り直す必要があるため。

## 3-5. テストとドキュメントの更新

- `tests/test_jepx_pipeline_orchestrator.py` の 3 テスト（`test_run_jepx_orchestrated_pipeline_skips_gold_step` / `_runs_gold_step` / `_skips_raw_to_bronze_when_no_unprocessed`）を新しいステップ構成に合わせて更新
- `CLAUDE.md` の Commands セクションから `run-jepx-staging-dbt` / `run-jepx-silver-dbt` を削除し、`ingest-jepx-bronze-to-silver` を追加

## Phase 3 の完了条件
- [x] `uv run pytest tests/` が全て green（70 passed）
- [x] silver 3 テーブルを新スキーマで再作成し、bronze から全量復元（base 5,856 / block 5,856 / area 52,704 行、削除前と一致）
- [x] silver 3 テーブルの `source_data` / `status` / `ingestion_time` / `execution_id` が NULL でない
- [x] 同じ実行を 2 回流しても silver の行数が増えない（2 回目は `inserted=0`）

### 2 回目の実行で `updated` が全行になる件（既知・Phase C 向けの申し送り）

`add_metadata()` が実行ごとに新しい `ingestion_time` / `execution_id` を採番するため、
値が変わらない行でも upsert が「更新あり」と判定し、毎回全行が書き直される。行数は
増えないので冪等性は保たれているが、`plan_jepx_pipeline_and_ci.md` Phase C の
`record_ingestion_time` / `record_updated_time` を導入する際に、
「実データに変化がない行は更新しない」形へ整理するのが望ましい。

---

# Phase 4（後続）— quarantine

**ユーザー確認待ちのため未着手。** Phase 2 で `violations` 列を作ってあるため、以下の追加で済む構成になっている。

- `configuration/iceberg/schema/silver/jepx_spot_price_quarantine.csv` の作成（`reason` / `quarantined_at` / cast 前の生値 / `source_data`）
- `violations` が非空の行を `table.append()` で履歴として蓄積（doc §10「書き込み方式」）
- Phase 2 で保留した論点の解決: dedup による NULL キー行の潰れ、`TRY_CAST` の NULL 判別（doc §12-1, §12-2)

# Phase 5（任意）— パーティションの実効化

Phase 1 で CSV を `year` に直しても、**`src/common/schema_builder.py` と `src/common/iceberg.py` が `partition_transform` 列を読んでいないため実テーブルは無パーティションのまま。**

- `build_table_schema()` に `PartitionSpec` の生成を追加
- `provision_table()` の `create_table()` に `partition_spec` を渡す
- 既存テーブルへのパーティション仕様追加は Iceberg のパーティション進化で対応可能

---

# 参考: 変更対象ファイル一覧

| 種別 | パス |
|---|---|
| 新規 | `src/pipeline/silver/bronze_to_silver_jepx_spot_price.py` |
| 新規 | `tests/test_bronze_to_silver_jepx_spot_price.py` |
| 変更 | `configuration/iceberg/schema/silver/jepx_spot_price_base.csv` |
| 変更 | `configuration/iceberg/schema/silver/jepx_spot_price_area.csv` |
| 変更 | `configuration/iceberg/schema/silver/jepx_spot_price_block.csv` |
| 変更 | `src/main.py` |
| 変更 | `src/orchestration/jepx_pipeline.py` |
| 変更 | `tests/test_jepx_pipeline_orchestrator.py` |
| 変更 | `src/dbt/jepx_power/models/staging/_staging__models.yml` |
| 変更 | `src/dbt/jepx_power/models/silver/_silver__models.yml` |
| 変更 | `CLAUDE.md` |
| 削除 | `src/dbt/jepx_power/models/staging/stg_jepx_spot_price.sql` |
| 削除 | `src/dbt/jepx_power/models/silver/silver_jepx_spot_price_{base,area,block}.sql` |

# 参考: 再利用する既存コード

| 用途 | 場所 |
|---|---|
| カタログ取得 / テーブル provision | `src/common/iceberg.py` の `get_catalog()` / `provision_table()` |
| 監査列の付与 | `src/common/pipeline_utilities.py` の `add_metadata()` |
| ターゲットスキーマへの整列と cast | `src/orchestration/jepx_pipeline.py:129-144` のパターン |
| S3 / DuckDB 設定値 | `src/dbt/jepx_power/profiles.yml` |
| ステップ結果の型 | `src/orchestration/jepx_pipeline.py` の `PipelineStepResult` |

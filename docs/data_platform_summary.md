# 個人開発データプラットフォーム 設計メモ

> 本ドキュメントは、個人開発しているデータレイクハウス基盤(特に JEPX スポット市場価格パイプラインの `bronze_to_silver`)に関する設計判断をまとめたものです。

---

## 1. 全体構成

業務で利用している Azure Databricks + Azure Data Factory を、OSS でローカル再現することを目的とした基盤。

| レイヤー | 採用技術 | 役割 |
| --- | --- | --- |
| 開発環境 | WSL2 + devcontainer + Docker | 環境の再現性の土台 |
| ストレージ | RustFS (S3 互換オブジェクトストレージ) | ストレージ層(本番の ADLS Gen2 / Blob 相当) |
| テーブルフォーマット | PyIceberg 0.11.1 | レイクハウスのテーブル管理 |
| 処理エンジン | Polars / DuckDB | 変換処理 |
| 変換 | dbt(必須ではない) | モデリング/変換 |

**設計上の狙い:** S3 互換で層を疎結合にし、ローカルは RustFS、本番は Azure Storage とエンドポイントの付け替えで移植できる構成にする。

**目的の本質:** 「dbt ありき」ではなく、**レコード数が増えても安定して高速に処理できるパイプライン**を作ること。

---

## 2. RustFS 利用上の注意点

- **現状 alpha/beta 段階。** 公開元は本番利用を非推奨。ローカルの学習・実験基盤としては問題ないが、本番運用時は安定性を別途見極める。
- **注目される背景:** MinIO Community Edition がメンテナンスモードに入り、Web コンソールやレプリケーション等が商用の AIStor 側に移されたため、S3 互換の代替として選ばれている。
- **非 root ユーザー (UID/GID 10001) で動作。** bind mount するホストディレクトリの所有者を `chown -R 10001:10001` で合わせないと permission denied で起動失敗する。
- **`latest` タグを使わずバージョンを固定する**(再現性の確保。numpy/pandas のバージョン競合事故と同じ教訓)。
- **デフォルト認証情報 (`rustfsadmin` 等) は既知の値なので必ず上書き。** `.env` に自分の値を書き、`.gitignore` に入れる。
- **プロジェクト一式は WSL の Linux FS 側 (`~/` 配下) に置く。** `/mnt/c/...` 配置は大量ファイル I/O で顕著に遅くなる。

---

## 3. JEPX スポット価格パイプライン

### 進捗
- `source_to_bronze`(JEPX サイトから生 CSV 取得 → RustFS 保存 → Bronze 書き込み)は**完成済み**。
- `bronze_to_silver` を設計中。

### bronze_to_silver で実施する 5 工程
1. 重複削除 (dedup)
2. 各カラムの型変換
3. 受渡日とコマを組み合わせて datetime 列を作成
4. バリデーション(Null チェック、不適切な値の確認)
5. Upsert 処理

### JEPX データの前提
- 30 分コマ = 1 日 48 コマ。
- データは年度ごとにファイル分割。1 ファイル最大 = 365 日 × 48 コマ = **17,520 行**(エリア数分だけ増える)。
- 確報の差し替え(改訂)がありうるため upsert が必要。

---

## 4. 処理エンジンの選定:DuckDB 主軸

### 結論
- **(1)〜(4) の変換は DuckDB (SQL) で書く。**
- **(5) Iceberg への書き込みは PyIceberg が担う。**
- DuckDB の結果を **Arrow 経由**(ゼロコピーに近い形)で PyIceberg に渡す。

### DuckDB を主軸にする理由
- 5 工程(dedup=ウィンドウ関数、型変換=`TRY_CAST`、datetime 合成、join 主体の upsert 対象算出)は SQL の世界観に素直に乗る。
- out-of-core 処理とクエリオプティマイザにより、データが増えても安定しやすい。
- Iceberg/Parquet のスキャン最適化(パーティションプルーニング、述語プッシュダウン)が効く。
- upsert のように「間違うとデータが壊れる」処理は、宣言的な SQL で書くと意図が明示的で壊れにくい。

### 補足:dbt に関する誤解の解消
「dbt に移行すると orchestrator から Python で実行できないのでは」という懸念は誤り。dbt は以下の方法で Python から実行できる。
- `subprocess.run(["dbt", "run", ...])`
- `dbt.cli.main.dbtRunner` による Python API 呼び出し
- Dagster / Airflow の公式連携 (`dagster-dbt` 等)

ただし目的は性能・安定性なので、dbt 採用は必須ではない。

---

## 5. スケール要件と実データ量

| 実行モード | 対象 | 概算行数 | 重視点 |
| --- | --- | --- | --- |
| 日次パイプライン | 取り込んだ年度ファイル分 | 2 万弱 | 速度 |
| フルリフレッシュ | 10 年分 | 17.5 万〜(×エリア数) | 安定・正確さ |

### 重要な認識
- **この規模は DuckDB にとって小さい。** 日次は瞬時、フルリフレッシュも out-of-core が不要なレベル。**エンジンの性能限界を心配するフェーズではない。**
- **スケールの本丸はエンジン選定ではなく、Upsert 設計とパーティション設計。**
- フルリフレッシュは **「全期間 upsert」** に決定 → 日次とフルの違いを「対象データの範囲だけ」に集約でき、**同じコードパス**で処理できる(フル時だけ挙動が違って事故るリスクが構造的に消える)。
- 留意点:upsert は「既存にあるが新データにないキー」を削除しない。真の作り直しが必要なときのみ別途 drop→再作成の経路を用意する。

---

## 6. PyIceberg 0.11.1 と Upsert 実装

- **upsert 機能は 0.9.0 から導入済み**、以降バグ修正を重ねて成熟。**dynamic overwrite**(パーティション全置換の最適化)も利用可能。0.11.1 では両方使える。

### 2 つの書き込み方式
- **方式 A:PyIceberg ネイティブ `upsert`(行レベルマージ)** — `table.upsert(df, join_cols=[...])`。キー指定で差分をマージ。コード量少なく意図が明快。
- **方式 B:パーティション上書き (dynamic overwrite)** — 対象パーティションを丸ごと置換。操作が枯れていて壊れにくい。

### 採用方針
- 日次・フルリフレッシュとも **方式 A(全期間 upsert)で統一**。
- join キーは `delivery_date, period, area_code`。

---

## 7. パーティション設計

### 一般原則(重要な認識の修正)
- **「パーティションは細かいほど良い」は誤解。** 細かすぎると **small files problem**(小ファイル問題)を招く。
  - メタデータ肥大化 → プランニングが遅延。
  - オブジェクトストレージではファイルごとに HTTP リクエストが飛び、小ファイル大量は非効率。
  - Parquet の圧縮・エンコーディングが効きにくくなる。
- **目安:1 パーティション(1 ファイル)あたり数百 MB〜1GB。**
- 決め方は「1 パーティションが適切サイズになる粒度」×「よく使う WHERE 句の絞り込み条件」から逆算。
- 高カーディナリティのキー(ユーザー ID 等)でのパーティションはアンチパターン。

### JEPX への結論
- 1 日 = 48 コマ × エリア数程度で **1 日あたり数十 KB** しかない → **日単位パーティションは切りすぎ(小ファイルの典型)**。
- **年単位で十分。** Iceberg の **hidden partitioning** で `year(delivery_date)` を使うのが素直(生の日付列を保ちつつ物理パーティションは年単位)。
- データが将来高頻度化(秒単位の実績値等)したら、そのテーブルは日単位を検討、という粒度の使い分け。

---

## 8. タイムゾーン設計

### 方針
- **datetime 列は必ず timezone aware で持つ。**
  - パフォーマンス影響は実質なし(naive も aware も内部は UTC エポックの 64bit 整数。ストレージ・スキャン速度は変わらない)。
- **Silver は UTC に統一、Gold 以降で JST 等の各タイムゾーンへ変換。**
  - Silver = 「物理的な一瞬」を UTC で一元管理する信頼できる単一の事実の層。
  - Gold = 用途に合わせてローカルタイムを付与する層。
  - 「生の一瞬は UTC で一元管理し、ローカルタイムは末端で付与する」時刻設計のベストプラクティスに合致。

### naive と aware の違い
- **aware** は地球上の特定の一瞬を一意に指せる(オフセット/地域情報を持つ)。
- **naive** はタイムゾーン情報を持たず、同じ文字列でも指す瞬間が曖昧。
- 単一国なら naive でも回るが「暗黙の了解」が外に出た瞬間に壊れる。将来の市場またぎを考えるなら最初から aware が安全(後から naive→aware への移行は激痛)。

### 電力データ特有の落とし穴(重要)
**受渡日・コマは JST の暦で定義されたビジネス区分なので、UTC に変換してはいけない。**
- UTC 変換すると受渡「日」が前日にずれる(00:00 JST = 前日 15:00 UTC)問題が発生。
- **列の役割を分けて両方保持する**のが正解。

---

## 9. Silver テーブルの列設計(確定)

| 列名 | 型 | 意味・区分 |
| --- | --- | --- |
| `delivery_ts` | `TIMESTAMPTZ` (UTC) | 受渡日+コマから合成した**物理的な一瞬**(aware) |
| `delivery_date` | `DATE` (JST 暦日) | **ビジネス上の受渡日**。UTC 変換しない |
| `period` | `INTEGER` (1–48) | **ビジネス上のコマ** |
| `area_code` | — | エリアコード |
| `area_price` | `DECIMAL` | エリアプライス |
| `system_price` | `DECIMAL` | システムプライス |
| `volume` | `DECIMAL` | 約定量 |
| `ingested_at` | timestamp | 取り込み時刻(dedup の根拠・監査用) |

### 「合成したら元データは捨てる」ではなく「元も導出も残す」のが定石
- **原則 1:** 導出元(`delivery_date`/`period`)を捨てると逆変換が必要になり、UTC↔JST の日跨ぎで間違えやすい。最初から両方持てば逆変換不要。
- **原則 2:** 物理的事実(`delivery_ts`)とビジネスラベル(`delivery_date`/`period`)は別種の情報。両方を一次情報として持つと、時系列処理とビジネス集計の双方が変換なしで正確にできる。
- **原則 3:** 再現性・監査証跡。元データが同じ行に残っていれば、導出ロジックにバグが出ても再計算でき、Silver の行だけで事実と導出を検証できる(トレーディングデータで特に重要)。
- **層による使い分け:** Silver は「残す」、Gold は用途に絞って「削ぐ」。方針が逆になる。

### dedup キー
`delivery_date, period, area_code`(JST ビジネス区分)で取り、取り込み時刻が最新の行を残す。

---

## 10. バリデーション:quarantine 方式

異常行を Silver に入れず **quarantine(隔離)テーブルへ退避**し、正常行だけ Silver へ進める(可用性を保つ)。fail-fast より柔軟だが、**隔離行を後で必ず見る運用**が伴う点に注意。

### 異常の定義(quarantine 対象)
- 型変換失敗(`TRY_CAST` が NULL を返した)
- 必須列の NULL(`delivery_date`, `period`, `area_code` 等)
- 範囲違反(`period` が 1〜48 外、`area_price` が負値 等)
- dedup 後の重複残存(想定外検知)

### quarantine テーブルに持たせる情報
- 元の行データ(できれば **cast 前の生の値**。失敗原因が追える)
- **違反理由**(`reason`。構造化しておくと傾向分析が可能)
- 検知時刻(`quarantined_at`)
- どのバッチ・ソースファイル由来か(追跡用)

### 運用面(quarantine 方式の成否を分ける)
- **可視性:** 隔離件数をパイプライン実行時に出力。閾値超過でアラート。
- **再処理:** JEPX は後日の確報で正しい値が来れば次回取り込みで自然に upsert 解決するケースあり。「次回取り込みで直る」と「人手で直す」を区別できるとよい。
- **保持期間:** 無限に貯めるか一定期間で消すか。
- **書き込み方式:** quarantine は **append** で履歴として貯める。

---

## 11. 実装スケルトン(bronze_to_silver)

```python
import duckdb
import pyarrow as pa
from pyiceberg.catalog import load_catalog

# --- 0. 対象範囲の決定(日次 or フルの唯一の違いはここだけ) ---
con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs; INSTALL iceberg; LOAD iceberg;")
con.execute("""
    SET s3_endpoint='rustfs:9000';
    SET s3_access_key_id='...';
    SET s3_secret_access_key='...';
    SET s3_use_ssl=false;
    SET s3_url_style='path';
""")

# --- 1〜3. 変換・dedup・datetime 合成を SQL で ---
con.execute("""
    CREATE OR REPLACE TEMP TABLE deduped AS
    WITH raw AS (
        SELECT * FROM iceberg_scan('s3://warehouse/bronze/jepx_spot')
        -- 日次なら WHERE で取り込みバッチ/年度を絞る
    ),
    typed AS (
        SELECT
            TRY_CAST(delivery_date AS DATE)        AS delivery_date,
            TRY_CAST(period AS INTEGER)            AS period,        -- コマ 1..48
            area_code,
            TRY_CAST(area_price AS DECIMAL(10,2))  AS area_price,
            TRY_CAST(system_price AS DECIMAL(10,2))AS system_price,
            TRY_CAST(volume AS DECIMAL(14,2))      AS volume,
            ingested_at
        FROM raw
    ),
    with_ts AS (
        SELECT
            *,
            -- 受渡日 + (コマ-1)*30分 を JST として解釈し、内部は UTC(TIMESTAMPTZ)
            (delivery_date::TIMESTAMP + ((period - 1) * INTERVAL '30 minutes'))
                AT TIME ZONE 'Asia/Tokyo'          AS delivery_ts
        FROM typed
    )
    SELECT * FROM with_ts
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY delivery_date, period, area_code
        ORDER BY ingested_at DESC
    ) = 1
""")

# --- 4. バリデーション:正常/異常に振り分け ---
con.execute("""
    CREATE OR REPLACE TEMP TABLE validated AS
    SELECT
        *,
        list_filter([
            CASE WHEN delivery_date IS NULL THEN 'delivery_date_null' END,
            CASE WHEN period IS NULL OR period NOT BETWEEN 1 AND 48
                 THEN 'period_invalid' END,
            CASE WHEN area_code IS NULL THEN 'area_code_null' END,
            CASE WHEN area_price IS NULL THEN 'area_price_cast_failed_or_null' END,
            CASE WHEN area_price < 0 THEN 'area_price_negative' END
        ], x -> x IS NOT NULL) AS violations
    FROM deduped
""")

silver_df = con.execute("""
    SELECT * EXCLUDE (violations) FROM validated WHERE len(violations) = 0
""").arrow()

quarantine_df = con.execute("""
    SELECT *, violations AS reason, now() AS quarantined_at
    FROM validated WHERE len(violations) > 0
""").arrow()

# --- 5. 書き込み(日次・フル共通) ---
catalog = load_catalog("local")
silver = catalog.load_table("silver.jepx_spot")
quarantine_tbl = catalog.load_table("silver.jepx_spot_quarantine")

if silver_df.num_rows > 0:
    silver.upsert(df=silver_df, join_cols=["delivery_date", "period", "area_code"])
if quarantine_df.num_rows > 0:
    quarantine_tbl.append(quarantine_df)   # quarantine は履歴として append

print(f"Silver upserted: {silver_df.num_rows}, Quarantined: {quarantine_df.num_rows}")
```

> **注意点(実装時)**
> - `TRY_CAST` は「元から NULL」と「変換失敗で NULL 化」を区別できない。変換失敗を検知したいなら cast 前後の NULL 数比較や生の値の保持が必要。
> - datetime のコマ→時刻ロジックは「コマ 1 = 00:00〜00:30 の開始時刻を 00:00 とする」前提。開始時刻/終了時刻のどちらを持つか要確認(通常は開始時刻)。

---

## 12. 未決定事項(次に詰める論点)

1. **部分異常の扱い:** 1 行の一部の列だけ異常な場合、行まるごと隔離するか、異常列だけ NULL で通すか。
   → トレーディングデータ(価格絡み)では**行まるごと隔離を推奨**。
2. **cast 前の生の値を quarantine に残すか。**
   → 原因追跡のため残す方を推奨。
3. datetime のコマ→時刻が開始時刻/終了時刻どちらか(要確認)。
4. Iceberg カタログの実装選択(SQL catalog / REST catalog 等)。
5. ADF 相当のオーケストレーション層を何で担うか(dbt+スクリプト / Dagster / Airflow)。

---

## 付録:横断的に確認された設計原則

- **再現性のためにバージョンは固定する**(RustFS のタグ、Python パッケージ)。numpy 2.x と pandas/pyarrow の ABI 競合のように、pin し忘れが事故を生む。
- **日次とフルリフレッシュは同じコードパスに寄せる**(挙動の食い違いによる事故防止)。
- **Silver は情報を残す層、Gold は削ぐ層。**
- **物理的事実(UTC タイムスタンプ)とビジネスラベル(受渡日・コマ)を分けて保持する。**
- **スケールの本丸はエンジン選定ではなく、upsert 設計とパーティション設計。**

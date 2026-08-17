# JEPX スポット価格パイプライン — Gold 層以降 実装計画

元の計画（Gold 層以降の構想）を、**実装済み Silver の実態**と**実データの実測値**に突き合わせて更新したもの。
フェーズ構成と設計判断は元計画を踏襲し、前提のズレの訂正・実装順序の入れ替え・実測に基づく追加のみを行っている。

## 現在地

| 層 | 状態 |
| --- | --- |
| source → RustFS | 実装済み |
| Bronze | 実装済み（`bronze.jepx_spot_price` 380,256行） |
| Silver | 実装済み（3テーブル、FY2005–FY2026） |
| **Gold** | 0-1 / 0-3 / 0-4 実装済み。0-2 `monthly` と 0-5 `price_events` は未着手 |
| 可視化・予測 | 未実装 |

---

## 0. 前提の訂正（着手前に必読）

元計画は Silver のカラム名を `docs/data_platform_summary.md` §9 の設計メモから引いていたが、
**実装は別の名前で確定している。** Gold の SQL を書き始める前にここを揃えないと全クエリを書き直すことになる。

| 元計画の記載 | 実装での名前 |
| --- | --- |
| `delivery_ts` | `delivery_datetime`（timestamptz, UTC） |
| `period`（1–48） | `time_code`（int, 1–48） |
| `area_code` | `area_name`（string, `tokyo` 等） |
| `volume` | `selling_bid_volume` / `purchase_bid_volume` / `contracted_volume` |
| `ingested_at` | `ingestion_time`（監査列） |

**`area_price` と `system_price` は同じテーブルにない。**

| テーブル | 行数 | 主な列 |
| --- | ---: | --- |
| `silver.jepx_spot_price_base` | 374,400 | `system_price`, 各種 volume |
| `silver.jepx_spot_price_block` | 374,400 | block 系 volume |
| `silver.jepx_spot_price_area` | 3,364,896 | `area_name`, `area_price` |

いずれも `delivery_date` / `time_code` / `delivery_datetime` を共通で持ち、`year(delivery_date)` でパーティションされている。
乖離を見るには `(delivery_date, time_code)` での JOIN が必要:

```sql
SELECT a.delivery_date, a.time_code, a.area_name,
       a.area_price - b.system_price AS spread
FROM silver.jepx_spot_price_area a
JOIN silver.jepx_spot_price_base b USING (delivery_date, time_code)
```

### その他の古くなった前提

- **Silver に quarantine テーブルは存在しない。** 設計メモの案止まりで、実装は「異常行を除外し警告ログを出す」。
  よってフェーズ2の品質監視は quarantine の補完ではなく **唯一の検知経路**になる。位置づけが変わる。
- **`ops.ingestion_log` というテーブルはない。** 実体は `metadata/raw_ingestion_log.parquet`（RustFS 上）。
- **Databricks SQL は選択肢に入らない。** 本環境はローカル OSS で、Databricks は再現元。

---

## 1. 実データが示していること（2026-08-17 実測）

Gold の設計はこの4点を説明することに集約される。

| 現象 | 実測 |
| --- | --- |
| **市場分断の常態化** | エリア価格がシステム価格と乖離したコマの割合が 2007年 17.3% → **2026年 92.1%**。最大乖離は2021年の170円/kWh |
| **ダックカーブの出現** | 九州・春季(4–5月)平均。昼(10–14時) / 夕(17–20時) が 2010–15年 `14.1 / 14.2`（平坦）→ 2021–26年 **`3.5 / 14.3`** |
| **下限張り付き** | 2020年以降 0.01円が出現。九州 8.38% に対し東京 0.96% と地理的勾配が明確 |
| **価格危機** | 2021年 最高251円/kWh・標準偏差23.1（平年3〜5の約5倍）。2022年は平均22.4円 |

補足: 約定量は2010→2020で約60倍。2016年全面自由化・2020年の余剰電力供出義務との対応が見える。

---

## 2. フェーズ0. Gold 集計テーブル

外部データ不要。Silver から直接導出できるもの。

### 0-1. `gold_jepx_daily` → **実装済み: `gold.jepx_spot_price_daily`**

日次 × エリアの基本統計。ダッシュボードと後続分析の土台。

> **実装時の変更点**（`src/pipeline/gold/silver_to_gold_jepx_spot_price.py`、CLI `ingest-jepx-silver-to-gold`）
>
> - **テーブル名**は `gold.jepx_spot_price_daily`。既存の `silver.jepx_spot_price_*` と
>   `{layer}.{table}` 規約に合わせた（`gold.gold_jepx_daily` は冗長）
> - **volume 系は持たせなかった**。全国値なので9エリア行に複製すると SUM が9倍になる。
>   `system_price` は intensive（横断平均してもシステム価格に戻る）なので非正規化して保持
> - **パーティションなし**。全期間で7万行しかなく、年単位で切ると小ファイル化するため
> - 下記の追加列を入れた: `avg_system_price` / `avg_spread` / `max_abs_spread` /
>   `split_time_code_count`（0-4 の分断分析を daily の粒度で先取り）
> - 実測 **70,102行**（FY2005–FY2026、7,800日 × 9エリア − 停止期間98日分）

実装された列（元計画の `total_contracted_volume` は上記の理由で不採用）:

| カラム | 内容 |
| --- | --- |
| `delivery_date` / `area_name` | 業務キー |
| `avg_price` / `min_price` / `max_price` | 平均・最小・最大 |
| `median_price` / `p05_price` / `p95_price` | 中央値・分位点 |
| `stddev_price` | 標準偏差（母集団。1コマの日でも NULL にしないため） |
| `intraday_range` | 日中レンジ = max − min |
| `avg_system_price` / `avg_spread` / `max_abs_spread` | システム価格と乖離 |
| `split_time_code_count` | 市場分断コマ数 |
| `spike_time_code_count` | 閾値（50円/kWh）超過コマ数 |
| `floor_time_code_count` | 下限（0.01円）張り付きコマ数 |
| `time_code_count` | 実在コマ数（48 でなければ欠損） |

`time_code_count` は品質チェックを兼ねる。日本には DST がないため 48 が常に正。
FY2005 のみ 274日分（2005-04-02 開始）である点に注意。

### 0-2. `gold_jepx_monthly`

`gold_jepx_daily` からの再集計。年月 × エリア。前年同月比を持たせる。

### 0-3. `gold_jepx_period_profile` → **実装済み: `gold.jepx_spot_price_period_profile`**

日内の価格カーブ。集計軸は `time_code`(1–48) × `area_name` × 季節 or 月 × 曜日区分（平日/土日祝）。

朝夕のピーク形成、昼間の太陽光による価格低下、季節ごとのカーブ差が見える。
**上記「ダックカーブ」の実測はこのテーブルの粒度で得られたもので、効果は確認済み。**

> **実装時の判断**
>
> - 集計軸は「季節 or 月」のうち **月**。月→季節/年代へは畳めるが逆はできないため
> - 曜日区分は `weekday` / `holiday`（土日＋祝日）。祝日判定に `jpholiday` を追加した。
>   年16〜18日 ≒ 平日の6% にあたり、平日カーブに混ぜると濁るため
> - **レート値ではなくカウントを保存**（`observation_count` など）。畳むときに
>   観測数の重みが消えるのを防ぐ
> - 実測 **221,856行**。Gold から再計算したダックカーブ（九州・平日・4-5月、
>   昼/夕の平均）は 2010-15年 `15.1 / 14.7` → 2021-26年 **`4.3 / 15.4`**

### 0-4. `gold_jepx_area_spread` → **実装済み: `gold.jepx_spot_price_area_spread`**

**簡易版（システム価格との乖離）から始める。** 元計画にある通り実装が軽く、実測で 92.1% という明確な結果が既に出ている。

| カラム | 内容 |
| --- | --- |
| `delivery_date` / `time_code` / `delivery_datetime` | 時刻軸 |
| `area_name` | エリア |
| `spread` | `area_price − system_price` |
| `is_split` | `abs(spread) > 閾値`（市場分断フラグ） |

1コマあたり9行。全エリアペア版なら36行になるが、物理的意味が明確になる代わりに
連系線の接続グラフが必要。**必要になってから**でよい。

> **実装時の判断**
>
> - 列は `delivery_date` / `time_code` / `area_name` / `delivery_datetime` /
>   `area_price` / `system_price` / `spread` / `is_split`。両方の価格を持たせたのは、
>   持たせないとヒートマップ描画のたびに Silver へ JOIN し直すことになり、
>   このテーブルを置く意味が消えるため
> - `year(delivery_date)` でパーティション。daily（7万行・パーティションなし）と違い
>   336万行あり、silver の area テーブルと同じ粒度・同じ分割が妥当
> - スパイク/下限フラグは入れていない。名前どおり乖離に焦点を絞った。
>   0-5 の `price_events` を作るときに必要なら widen する
> - 実測 **3,364,896行**（silver area テーブルと完全一致）

### 0-5. `gold_jepx_price_events`

価格イベントを1レコード1事象として抽出。

| カラム | 内容 |
| --- | --- |
| `event_type` | `SPIKE` / `FLOOR_PRICE` |
| `area_name` | エリア |
| `start_datetime` / `end_datetime` | 開始・終了 |
| `duration_time_codes` | 継続コマ数 |
| `peak_price` / `avg_price` | 事象中の価格 |
| `detection_method` | `THRESHOLD` / `SIGMA` |

---

## 3. 実装順序（元計画から入れ替え）

元計画は `daily` → `monthly` → `period_profile` → `area_spread` の順だったが、
**成果物としての強さは逆順**である。`area_spread` と `period_profile` が §1 の物語を運び、
`monthly` は `daily` の再集計なので後回しでも失うものがない。

1. `gold_jepx_daily`（土台。他が全部これに乗る） ✅
2. `gold_jepx_period_profile` → **日内カーブ可視化** ✅
3. `gold_jepx_area_spread`（簡易版）→ **分断ヒートマップ** ✅
4. 可視化をデプロイ（ここで一度完成品にする） ✅
5. `gold_jepx_monthly` / `gold_jepx_price_events` ← **次はここ**
6. データ品質モニタリング
7. 予測

**1〜4 で成果物として成立する。** ここまでで一度ドキュメント化・公開に回すのが現実的。

---

## 4. フェーズ1. 可視化

### 優先実装

1. **ヒートマップ** — 縦軸=日付、横軸=コマ、色=価格。季節性・日内変動・異常日が一目で分かる主役の図
2. **市場分断率の推移** — 年 × エリアの折れ線 or ヒートマップ（17%→92% を見せる）
3. **48コマ平均プロファイル** — 年代別に重ねる（ダックカーブ）
4. **月次サマリテーブル** — 前年同月比つき
5. **直近イベント一覧** — `gold_jepx_price_events` から

### 注意: ヒートマップと `period_profile` は粒度が違う

`gold_jepx_period_profile` は季節/月で集約済みなので **そこからヒートマップは描けない。**
→ **解決済み**: 0-4 の `gold.jepx_spot_price_area_spread` が日付 × コマ × エリアの
粒度を持つので、ヒートマップはこちらを読む。

### 実行環境（元計画の未決定事項への回答）

**Streamlit を推す。** Gold が数千〜数万行に収まるため重い基盤が不要で、
リポジトリに Docker ビルドの CI（`.github/workflows/deploy.yml`）が既にありデプロイ経路も揃っている。
Databricks SQL は本環境では選択肢外。

→ **実装済み**（`src/dashboard/`）。`compose.yaml` に `dashboard` サービス、
Dockerfile に `dashboard` ステージを追加。詳細は README の Dashboard 節。

---

## 5. フェーズ2. データ品質モニタリング

**Silver に quarantine がないため、これが唯一の検知経路になる。**

- 前日分が 48 コマ × 全エリア揃っているか（`gold_jepx_daily.time_code_count`）
- 前日比で異常な水準変化がないか
- `metadata/raw_ingestion_log.parquet` と突き合わせ、取得成功にもかかわらず Gold に反映されていないケースを検知

なお Silver 側には既に行数検証がある（`verify_silver_row_counts()`、`docs/tasks/tasks.md` 8.4）。
Gold でも同じ思想を踏襲する。**「正常終了に見えて0行」が最も危険な失敗モード。**

---

## 6. フェーズ3. 予測モデル

段階的にホライズンを延ばす。いきなり2週間先を狙わない。

| 段階 | ホライズン | 位置づけ |
| --- | --- | --- |
| 3-1 | **D+1（翌日）** | 本命。スポット市場の実態（前日約定）と一致 |
| 3-2 | D+2 〜 D+7 | 精度劣化の観測 |
| 3-3 | D+14 | 点予測ではなく **分布予測**（P10/P50/P90 + スパイク発生確率） |

D+14 を点予測にしないのは、2週間先の気象予報に実用的な精度がなく主要な説明変数が存在しないため。
結果として季節・曜日・コマの平均パターンに収束し、モデルを使う意義が薄れる。

### 3-1. D+1 モデル

**特徴量（JEPX 単体で構成可能）**

- ラグ: 前日同コマ、前週同曜日同コマ、前日の日平均
- 移動統計: 直近7日/28日の同コマ平均・標準偏差
- カレンダー: 曜日、月、コマ番号（sin/cos 変換）、祝日フラグ
- エリア: `area_name`（カテゴリ変数）

祝日フラグは `jpholiday` 等のライブラリで足りる見込み。専用パイプラインを作る必要はない。

**ベースライン（必須）**

- B1: 前日同コマの価格
- B2: 前週同曜日同コマ
- B3: 直近7日の同コマ平均

**これに勝てないモデルは価値がない。** 必ず先に実装しスコアを記録する。

**モデル候補**: 勾配ブースティング（LightGBM / XGBoost）。表形式・非線形・特徴量重要度が解釈できる。

**評価設計**

- 分割は **時系列順**。ランダム分割は厳禁（未来の情報が学習に混入する）
- 学習期間 < 検証期間 < テスト期間
- 指標: MAE / RMSE / MAPE
- **通常時とスパイク時を分けて評価する**。全体平均ではスパイクでの大外しが埋もれる

**リーク防止（最重要）**: 予測時点で入手可能な情報のみを使う。
ここを誤ると検証では高精度だが実運用で機能しないモデルになる。

### 追加: ターゲットが下限で打ち切られている

2020年以降、九州は **8.38%** のコマが 0.01円 に張り付いている（東京は 0.96%）。
これは通常の回帰が最も苦手とする zero-inflated / censored な分布であり、
そのまま回帰すると下限付近を系統的に外す。

→ **「0.01円になるか否かの分類」と「価格の回帰」を分ける**構成を検討する。
スパイク時と通常時を分けて評価するのと同じ発想の、下限側の対応。

### 出力テーブル `gold_jepx_forecast`

```
forecast_run_ts      -- 予測を実行した時刻
target_datetime      -- 予測対象コマ（UTC）
target_date          -- JST 暦日
time_code            -- コマ
area_name
horizon_days         -- D+1 / D+7 / D+14
predicted_price      -- 点予測
pred_p10 / p50 / p90 -- 分布予測（D+14 用）
model_version
baseline_prediction  -- 比較用
```

`forecast_run_ts` を持たせ、**予測は上書きせず append** する。過去の予測を後から実績と突き合わせて精度検証できる。

---

## 7. 着手前に決めること（回答案つき）

| 論点 | 回答案 |
| --- | --- |
| 可視化の実行環境 | **Streamlit**。Gold が小さく、Docker ビルドの CI が既にある |
| スパイク判定の閾値 | **まず固定値（例: 50円/kWh）**。2021年は標準偏差23.1で平年3〜5なので、σ方式だと危機の年ほど閾値が跳ね上がり検知漏れする。σ方式は後から追加 |
| スプレッドの対象ペア | **システム価格との乖離（簡易版）から**。1コマ9行で済み、実測で92.1%という結果が出ている。連系線ペア限定版は必要になってから |

---

## 8. フェーズ2以降の伸びしろ（本計画のスコープ外）

このリポジトリには **OCCTO 発電実績（`silver.occto_unit_generation_actuals`、1,977万行、2024-03〜）** が既にある。
価格 × 実発電量をエリア × コマで突合できるため、
「九州の昼間が0.01円のとき実際に何が動いていたか」まで踏み込める。

でんき予報の需要データが揃えば 需給 × 価格 が繋がる。
**JEPX 単体の分析では出せない、このリポジトリ固有の強み。** ただし MVP のスコープ外とする。

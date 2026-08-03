# Plan: JEPX パイプライン強化 + フォルダ整理 + CI/CD 構築

## Purpose

JEPX パイプラインを完成形へ引き上げつつ、後続の TOCOM / 電気予報 / 天気予報 横展開に耐えるフォルダ構造と CI/CD 基盤を整える。

## 現状把握

### JEPX パイプライン (既存)
- Raw: `src/pipeline/raw/source_to_raw_jepx_spot_price.py`
- Bronze: `src/pipeline/bronze/source_to_bronze_jepx_spot_price.py`
- Silver dbt: `silver_jepx_spot_price_{base,block,area}.sql`
- オーケストレーター: `src/orchestration/jepx_pipeline.py` (Raw → Bronze → dbt staging → silver → Iceberg export → (Gold))
- テスト: 3 ファイル (`tests/test_jepx_*.py`)

### フォルダ整理で気になる点
1. `src/dbt/jepx_power/` の中に OCCTO モデル (`stg_occto_...`, `silver_occto_...`) が同居 — プロジェクト名がミスマッチ
2. `src/pipeline/bronze/migrate_occto_data.py` と `src/pipeline/bronze/upload_raw.py` は実質 raw アップロード処理なのに `bronze/` 配下
3. `src/pipeline/silver/`, `src/pipeline/gold/` が空
4. `src/Jupyter/` は notebooks なので repo ルート `notebooks/` に移動が妥当
5. Silver テーブルに `record_ingestion_time` / `record_updated_time` / `is_deleted` が未追加 (`metadata_columns.md` 未反映)

### CI/CD 現状
- `.github/workflows/deploy.yml` — Docker build & push のみ
- テスト / lint / 型チェックを回す CI がない
- `.pre-commit-config.yaml` は ruff + hadolint のみ (pyright / pytest なし)

## 進め方 (合意済み)

Phase A → B → C → D の順で進める。dbt プロジェクトはソース別に分割 (`jepx`, `occto`, ...)。

| Phase | 内容 | 目的 |
|-------|------|-----|
| A. CI 整備 (先出し) | `.github/workflows/ci.yml` を追加。ruff / pyright / pytest を PR で回す | 以降のリファクタで壊れを検知できる土台を先に作る |
| B. フォルダ整理 | dbt project 分割、`raw/bronze/silver/gold` の再配置、notebooks 移動、空ディレクトリ整理 | 横展開時のパターンを固定する |
| C. JEPX Silver メタデータ拡張 | Silver スキーマ CSV に3列追加、export ロジックで status 遷移 (`new→loaded`) 実装、`is_deleted` フィルタを dbt に追加 | `metadata_columns.md` の設計を JEPX で確立してから他社へ |
| D. JEPX 補強 | オーケストレーターのリトライ、失敗時の bronze status 更新など残タスク | Production 品質へ |

---

## Phase A — CI 整備

### 新規ファイル

`.github/workflows/ci.yml` を追加。以下の job を PR (target: main) と push (main) で実行。

| Job | ステップ | Fail 条件 |
|-----|---------|----------|
| `lint` | `uv sync` → `uv run ruff check src/ tests/` | ruff 違反あり |
| `format` | `uv run ruff format --check src/ tests/` | フォーマット未適用 |
| `typecheck` | `uv run pyright` | 型エラーあり |
| `test` | `uv run pytest tests/ --cov=src --cov-report=term-missing` | 失敗テストあり |
| `dbt-parse` (jepx) | `uv run dbt parse --project-dir src/dbt/jepx --profiles-dir src/dbt/jepx` | dbt モデル構文エラー |
| `dbt-parse` (occto) | 同上 (occto) | 同上 |

- Python 3.13 (`pyproject.toml` の `requires-python` に合わせる)
- キャッシュは `uv` の lock ベース (`astral-sh/setup-uv@v3` の built-in cache を利用)
- **DuckDB / RustFS を要する結合テストは skip する** マーカー方針で分離 (`@pytest.mark.integration`)

### `.pre-commit-config.yaml` 追加項目
- `uv run pyright` (ローカル hook)
- `uv run pytest -m "not integration"` (オプションで開発者選択)

### `.github/workflows/deploy.yml` 修正
- 既存の Docker build & push は残す
- `needs: [test, typecheck, lint]` で CI 通過後のみ実行するよう修正

---

## Phase B — フォルダ整理

### dbt プロジェクト分割

現状 `src/dbt/jepx_power/` に JEPX と OCCTO のモデルが混在。ソース別に分割。

```
src/dbt/
├── jepx/                          # (旧 jepx_power から改名 + OCCTO 除去)
│   ├── dbt_project.yml            # name: "jepx"
│   ├── profiles.yml               # jepx.duckdb を参照
│   ├── jepx.duckdb                # (rename)
│   └── models/
│       ├── staging/stg_jepx_spot_price.sql
│       └── silver/silver_jepx_spot_price_{base,block,area}.sql
└── occto/                         # (新規)
    ├── dbt_project.yml            # name: "occto"
    ├── profiles.yml
    ├── occto.duckdb
    └── models/
        ├── staging/stg_occto_unit_generation_actuals.sql
        └── silver/silver_occto_unit_generation_actuals.sql
```

- `dbt_packages/`, `logs/`, `target/` は `.gitignore` 済みか確認 (未除外なら追加)
- **参照更新箇所**: `src/main.py` の `run-jepx-{staging,silver}-dbt` / `run-occto-silver-dbt` のプロジェクトパス、`src/orchestration/jepx_pipeline.py` の `DEFAULT_DBT_PROJECT_DIR` / `DEFAULT_DBT_DUCKDB_PATH` / `DEFAULT_SILVER_EXPORT_MAPPINGS` (スキーマ prefix が `main_silver` から変わる)

### pipeline ディレクトリ再編

`src/pipeline/bronze/` に置かれている「raw アップロード系」を `raw/` へ移動:

| 移動元 | 移動先 | 理由 |
|--------|--------|------|
| `pipeline/bronze/migrate_occto_data.py` | `pipeline/raw/migrate_occto_data.py` | 実態は local → RustFS raw アップロード |
| `pipeline/bronze/upload_raw.py` | `pipeline/raw/upload_raw.py` | 同上 |

- 空の `pipeline/silver/` → 将来 Iceberg export が入るので **残す** (`.gitkeep`)
- 空の `pipeline/gold/` → 予約枠として **残す**
- 命名規則を統一: `source_to_raw_*.py` / `source_to_bronze_*.py` / `bronze_to_silver_*.py`

### notebooks 移動
- `src/Jupyter/` → `notebooks/` (repo ルート、小文字)
- notebook はプロダクション対象外である旨を `notebooks/README.md` に明記
- `.pre-commit-config.yaml` の `--exclude notebooks` は既に対応済み

### import path 変更の影響
- `src/main.py` (import 元パス)
- `src/orchestration/jepx_pipeline.py`
- `tests/test_*.py` 全ファイル

Phase A の CI が緑になってから B を着手することで、リファクタでの regression を検知できる状態にする。

---

## Phase C — JEPX Silver メタデータ拡張

### スキーマ CSV 更新

`configuration/iceberg/schema/silver/jepx_spot_price_{base,block,area}.csv` に3列追記:

```csv
,record_ingestion_time,timestamp
,record_updated_time,timestamp
,is_deleted,boolean
```

### `export_jepx_silver_to_iceberg` の変更

- 現状: `target_table.append(...)` で単純追記
- 変更後: **PK 単位の upsert** (Iceberg の `upsert` API / `overwrite_rows` + `append` の合成)

- PK: `(trade_date, time_slot, area_code)` (`data_model.md` #1 準拠)
- **新規 record** の付与列:
  - `status = 'loaded'`
  - `record_ingestion_time = now()`
  - `record_updated_time = NULL`
  - `is_deleted = False`
- **既存 record 更新** の付与列:
  - `status = 'updated'`
  - `record_ingestion_time` は変更しない (既存値保持)
  - `record_updated_time = now()`
  - `is_deleted = False`

### Bronze status 遷移

Silver upsert 成功後、対象 Bronze record の `status` を `'new'` → `'loaded'` に更新するステップを追加。Iceberg の overwrite で該当行だけ書き換える (execution_id で絞り込み)。

### dbt モデルへの soft delete フィルタ

`silver_jepx_spot_price_*.sql` のベースクエリに `WHERE is_deleted = FALSE` を追加 (現状 dbt が Iceberg の Silver を読んでいるわけではないので、対象は「Silver → Gold」参照時の下流モデル)。Gold 未実装なので実質は**将来のガード**として dbt テンプレの `with base as` に組み込む方針で OK。

### テスト追加
- `test_jepx_silver_upsert_new_record.py` — 初回書き込みで `status=loaded`, `record_ingestion_time` セットを確認
- `test_jepx_silver_upsert_existing_record.py` — 2 回目で `status=updated`, `record_ingestion_time` 変わらず, `record_updated_time` セット
- `test_jepx_bronze_status_transition.py` — Silver 完了後 Bronze が `loaded` になっていること

---

## Phase D — JEPX 補強

### リトライポリシー

`orchestration/jepx_pipeline.py` にリトライラッパー追加:
- `max_attempts` (default 3)
- 非一時エラー (`ValueError`, `SchemaError` 等) はリトライしない
- 一時エラー (`ConnectionError`, `TimeoutError`, `botocore.exceptions.ClientError` の 5xx) はリトライ
- 指数バックオフ (1s, 2s, 4s)

### 失敗時のロールバック
- Bronze append 失敗 → 途中書き込みが起きないよう `ingest_jepx_spot_summary` を transaction 単位でチェック (現状 PyIceberg の append は atomic だが、metadata catalog 更新との整合性を確認)
- Silver upsert 途中で失敗 → Bronze の status を `'new'` のまま維持 (更新をコミット前に行わない)

### 実行メタデータ永続化

既存の `common/raw_ingestion_log.py` は raw 用。同じパターンで:
- `common/bronze_ingestion_log.py`
- `common/silver_ingestion_log.py`

を追加、または統一の `ingestion_log.py` (stage フィールドで区別) にリファクタ。Parquet 永続化 (`metadata_strategy.md` 準拠)。

### オーケストレーター結果の永続化

`run_jepx_orchestrated_pipeline` の返り値 `list[PipelineStepResult]` を Parquet として `s3://<bucket>/metadata/execution_log/execution_id=<uuid>/` に保存。

---

## 横展開に向けた効果

Phase C が完了した時点で「1 ソースの完成形テンプレ」が JEPX で確立するので、
- **E-1**: OCCTO も同じ Silver メタデータ設計に合流
- **E-2**: TOCOM は JEPX と近い市場データなので `source_to_raw_tocom.py` + `source_to_bronze_tocom.py` + dbt モデルで型どおり追加
- **E-3**: 電気予報 10 社は `data/electric_forecast/` のローカルファイルが起点なので `raw/local_to_raw_electric_forecast.py` を 1 つ書き、社別の unpacker (CSV/ZIP/Parquet) を dispatch する構造
- **E-4**: 気象庁 API は新規スクレイパ

を Phase D 後に着手する順が自然。

---

## 未確定事項 (Phase 開始前に確定したいもの)

1. **Iceberg upsert 実装方法** — PyIceberg 0.11 の `upsert` API を使うか、`overwrite_rows` + `append` の合成で書くか。事前に PoC が必要。
2. **Bronze status 更新の粒度** — execution_id 単位で全行更新か、Silver に取り込まれた PK に絞るか (前者が実装簡単・後者が正確)。
3. **dbt プロジェクト分割後の共通マクロ** — `area_code` シードなど共通したくなる場合 `src/dbt/_common/` を dbt package として抱える構成にするか、当面は重複を許容するか。

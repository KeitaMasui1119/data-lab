# src 実装サマリー

このディレクトリは、オーケストレーター中心の構成で実装されています。
実行入口は `main.py` に集約し、処理本体は `pipeline/` `core/` `catalog/` `utility/` に分離しています。

## ディレクトリ構成

- `main.py`
  - 全CLIコマンドのエントリポイント
  - 引数解釈と各処理のルーティングを担当
- `core/`
  - RustFS (S3互換) クライアント
  - バケット作成、プレフィックス作成、アップロード/ダウンロード、Object Lock 設定
- `catalog/`
  - PyIceberg カタログ操作
  - Namespace/Table の作成・削除、スキーマCSVからのテーブルプロビジョニング
- `pipeline/bootstrap/`
  - 開発/本番バケットの初期化計画を適用
- `pipeline/scraper/`
  - JEPX/OCCTO のスクレイピングと raw レイヤーへの保存
- `pipeline/ingestion/`
  - raw CSV を Bronze Iceberg テーブルへ投入
  - OCCTO ローカルCSVの RustFS への移行
- `dbt/jepx_power/`
  - DuckDB + dbt モデル（staging/silver）
- `utility/`
  - メタデータ付与、スキーマ式生成などの共通処理
- `Jupyter/`
  - 検証・分析ノートブック

## 実装済みデータフロー

### 1. ストレージ初期化

- `jp-power-grid-dev` と `jp-power-grid-prd` を作成/検証
- `raw/ bronze/ silver/ gold/ sandbox/` プレフィックスを作成
- prd バケットにデフォルト保持期間（COMPLIANCE 7日）を設定

### 2. JEPX 取り込み

- スクレイピングで JEPX CSV を取得し raw へ保存
- Bronze テーブル `bronze.jepx_spot_price` に投入
- 同一 `source_data` の重複投入をスキップ可能

### 3. OCCTO 取り込み

- OCCTO CSV を日付指定で取得し raw へ保存
- ローカル `data/occto` から RustFS への一括移行
- Bronze テーブル `bronze.occto_unit_generation_actuals` へ投入
- 同一 `source_data` の重複投入をスキップ可能

### 4. Silver プロビジョニング

- `data/schema/silver/*.csv` を走査し、`silver.<schema_file_stem>` を作成/更新

### 5. dbt 変換（DuckDB）

- JEPX staging 実行
- JEPX silver 実行
- OCCTO staging + silver 実行

## 利用可能CLIコマンド

実行はすべて `src/main.py` から行います。

```bash
uv run python src/main.py bootstrap-storage
uv run python src/main.py scrape-jepx
uv run python src/main.py ingest-jepx-raw-to-bronze

uv run python src/main.py scrape-occto
uv run python src/main.py migrate-occto-to-rustfs
uv run python src/main.py ingest-occto-raw-to-bronze --object-key raw/occto/unit_generation/<file>.csv

uv run python src/main.py provision-silver-tables

uv run python src/main.py run-jepx-staging-dbt
uv run python src/main.py run-jepx-silver-dbt
uv run python src/main.py run-occto-silver-dbt
```

主なオプション:

- `bootstrap-storage --bucket <name>`
- `scrape-jepx --bucket <name> --timestamp-ms <unix_ms>`
- `ingest-jepx-raw-to-bronze --object-key <key> --source-file-name <name> --allow-duplicate-source`
- `scrape-occto --target-date YYYY-MM-DD --download-url <url>`
- `migrate-occto-to-rustfs --local-dir <dir> --s3-prefix <prefix> --keep-local`
- `ingest-occto-raw-to-bronze --object-key <key> --allow-duplicate-source`
- `run-*-dbt --select <selector> --full-refresh`

## 運用メモ

- 新しい処理は `pipeline/` か `core/` に関数として実装し、副作用は呼び出し境界に閉じ込める
- `main.py` はオーケストレーション専用に保つ
- スキーマは `data/schema` を正本として、テーブル定義と整合させる

## 現在の到達点

- RustFS のバケット/保持設定の初期化: 実装済み
- JEPX raw 取得と Bronze 取り込み: 実装済み
- OCCTO raw 取得・移行・Bronze 取り込み: 実装済み
- JEPX/OCCTO の dbt staging/silver 実行: 実装済み
- Gold レイヤーの本格モデル化: これから

# 開発環境サマリー

更新日: 2026-05-10

## 1. 概要
このワークスペースは、ローカルのデータレイクハウス/データエンジニアリング検証を目的とした開発環境です。主な対象は日本の電力市場データで、RustFS (S3互換)・PyIceberg・Polars・DuckDB/dbt を中心に構成されています。

## 2. 実行環境
- OS: Debian GNU/Linux 13 (trixie)
- カーネル: Linux 6.6.87.2-microsoft-standard-WSL2
- シェル: zsh
- ワークスペースルート: /workspace
- Git ブランチ: feature/catalog-wrapper-practice

## 3. 主要ツール/バージョン
- uv: 0.11.8
- git: 2.47.3
- Python 要件: 3.13 以上 (pyproject.toml)

補足:
- 現在の環境では rg (ripgrep) は未インストールです。

## 4. プロジェクトの主な目的とデータアーキテクチャ
- 目的: ローカルでのデータ基盤構築・ETL実践・分析検証
- アーキテクチャ: Medallion (Raw / Bronze / Silver / Gold)
- ストレージ: RustFS を S3 互換ストレージとして利用
- テーブル管理: PyIceberg
- 変換処理: Polars
- 下流変換: DuckDB + dbt

## 5. 依存関係の特徴 (pyproject.toml ベース)
主なライブラリ:
- データ処理: polars, pandas, numpy, pyarrow
- データ基盤: pyiceberg[s3fs, sql-sqlite], boto3
- 変換/モデリング: dbt-core, dbt-duckdb
- スクレイピング: requests, beautifulsoup4, lxml, Scrapy
- 分析/可視化: seaborn, plotly, scikit-learn
- 開発支援: ruff, pyright, pytest, pre-commit

## 6. サービス構成 (compose.yaml)
サービス:
- app: Dockerfile の prd ターゲットを使用
- rustfs: rustfs/rustfs:latest

公開ポート:
- app: 8000
- rustfs: 9000 (API), 9001 (Console)

永続化:
- rustfs-data ボリュームを利用

設定:
- .env を app/rustfs の両方で参照
- rustfs はコンソール有効化設定あり

## 7. コード配置の要点
- src/main.py: 実行オーケストレーション
- src/core/: インフラ/クライアント層
- src/pipeline/scraper/: スクレイピング処理
- src/pipeline/ingestion/: Raw -> Iceberg 取込処理
- src/catalog/: カタログ・テーブル管理
- src/utility/: 再利用ユーティリティ
- src/Jupyter/: 検証/分析ノートブック

## 8. 代表的な運用コマンド
依存関係同期:
- uv sync --all-groups

ストレージ初期化:
- uv run python src/main.py bootstrap-storage

JEPX スクレイピング:
- uv run python src/main.py scrape-jepx --bucket jp-power-grid-dev

Raw -> Bronze 取込:
- uv run python src/main.py ingest-jepx-raw-to-bronze --bucket jp-power-grid-dev

dbt ステージング:
- uv run python src/main.py run-jepx-staging-dbt

dbt シルバー:
- uv run python src/main.py run-jepx-silver-dbt

## 9. 開発上の運用ルール (現状反映)
- main 直作業を避け、機能ごとにブランチを切る
- オーケストレーションと再利用ロジックを分離する
- 変更範囲に対して狭い単位で検証する
  - 例: uv run ruff check <changed paths>

## 10. データレイヤ実装指針
- Bronze: 取り込み時は String (Utf8) を維持し、型変換や業務ロジックを持ち込まない
- Silver: 型変換、時刻展開、重複排除などの整形を実施
- Iceberg 名前空間: 階層ドット記法を使用 (例: bronze.jepx.spot_price)

## 11. 引き継ぎ用メモ
- この資料は環境の事実情報を中心にしているため、後続の資料生成時は以下を追加すると用途別に使いやすくなります。
  - 目的別セットアップ手順 (初学者向け/運用者向け)
  - 障害対応手順 (接続不良、S3認証、テーブル作成失敗)
  - 検証チェックリスト (lint/type/test/dbt run)

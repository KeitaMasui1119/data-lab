# /workspace フォルダ構造

```
/workspace/
├── CLAUDE.md — Claude Code 向けのリポジトリガイド（アーキテクチャ・コマンド・設計判断）
├── README.md — 運用者向けのセットアップと CLI コマンド一覧
├── Dockerfile
├── LICENSE
├── compose.yaml — RustFS を含む開発用サービス定義
├── pyproject.toml, uv.lock, ruff.toml, pyrightconfig.json, .python-version
├── **.github/**
│   ├── **workflows/** — 2x `*.yml`（CI: ruff check / ruff format / pytest / pyright）
│   └── **instructions/** — 2x `*.md`
├── **configuration/**
│   └── **iceberg/**
│       ├── `.pyiceberg.yaml` — カタログ設定（`dlh_dev` は SQLite バックエンド）
│       ├── **catalog/** — `dlh_dev.db`（SQLite カタログ実体）
│       └── **schema/** — **全テーブルスキーマの正本**
│           ├── **bronze/** — 3x `*.csv`
│           ├── **gold/** — 空（未実装）
│           └── **silver/** — 4x `*.csv`
├── **data/** — 各地域・各市場の生データ置き場（parquet/csv/zip）。パイプライン処理前の原始データ。
├── **docs/**
│   ├── `data_platform_summary.md`
│   ├── **architecture/** — 10x `*.md`（レイヤー定義、データモデル、メタデータ戦略、リプレイ戦略など）
│   ├── **reports/** — 1x `*.md`（障害レポート）
│   └── **tasks/** — 4x `*.md`（タスク一覧と実装計画）
├── **git/** — `branch_strategy.md`
├── **src/** — `main.py`（唯一の CLI エントリポイント）
│   ├── **common/** — 10x `*.py`（データセット非依存の共通基盤）
│   │   └── **iceberg/** — 4x `*.py`（`catalog.py` / `schema.py` / `maintenance.py`）
│   ├── **orchestration/** — 4x `*.py`（JEPX / OCCTO のエンドツーエンド実行）
│   ├── **pipeline/** — `__init__.py`, `jepx_common.py`
│   │   ├── **raw/** — 2x `*.py`（スクレイピングと raw 保存）
│   │   ├── **bronze/** — 3x `*.py`（raw CSV → Bronze Iceberg）
│   │   ├── **silver/** — 2x `*.py`（Bronze → Silver、DuckDB + PyIceberg）
│   │   └── **gold/** — 空（未実装）
│   ├── **setup/** — 5x `*.py`（バケット作成、Iceberg 管理 CLI）
│   ├── **dbt/**
│   │   └── **jepx_power/** — `dbt_project.yml`, `profiles.yml`, `.user.yml`
│   │       （**モデルは残っていない**。JEPX / OCCTO の silver は Python + DuckDB +
│   │       PyIceberg に移行済みで、将来の gold レイヤー用に枠だけ保持）
│   └── **Jupyter/**
│       ├── **analysis/** — 2x `*.ipynb`
│       └── **scraping_prototypes/** — 2x `*.ipynb`
└── **tests/** — 18x `*.py`
```

> ファイル数は変動します。実装の責務分担は `CLAUDE.md` の「Module responsibilities」、
> レイヤーの定義は `layers.md` を参照してください。

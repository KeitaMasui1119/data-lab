# /workspace フォルダ構造

```
/workspace/
├── CLAUDE.md
├── Dockerfile
├── LICENSE
├── README.md
├── compose.yaml
├── pipeline.log
├── pyproject.toml
├── pyrightconfig.json
├── ruff.toml
├── uv.lock
├── **architecture/** — 8x `*.md`
├── **configuration/**
│   └── **iceberg/**
│       └── **schema/**
│           ├── **bronze/** — 2x `*.csv`
│           ├── **gold/**
│           └── **silver/** — 4x `*.csv`
├── **data/** — 各地域・各市場の生データ置き場（parquet/csv/zip）。パイプライン処理前の原始データ。
├── **src/** — `README.md`, `main.py`
│   ├── **Jupyter/** — 5x `*.ipynb`
│   ├── **common/** — 11x `*.py`
│   ├── **dbt/**
│   │   └── **jepx_power/** — `jepx_power.duckdb`, 2x `*.yml`
│   │       ├── **dbt_packages/**
│   │       ├── **logs/** — `dbt.log`
│   │       ├── **models/**
│   │       │   ├── **silver/** — 4x `*.sql`, `_silver__models.yml`
│   │       │   └── **staging/** — 2x `*.sql`, `_staging__models.yml`
│   │       └── **target/** — `graph.gpickle`, 4x `*.json`, `partial_parse.msgpack`
│   │           ├── **compiled/**
│   │           │   └── **jepx_power/**
│   │           │       └── **models/**
│   │           │           ├── **silver/** — 4x `*.sql`
│   │           │           └── **staging/** — 2x `*.sql`
│   │           └── **run/**
│   │               └── **jepx_power/**
│   │                   └── **models/**
│   │                       ├── **silver/** — 4x `*.sql`
│   │                       └── **staging/** — 2x `*.sql`
│   ├── **orchestration/** — 2x `*.py`
│   ├── **pipeline/** — `__init__.py`
│   │   ├── **bronze/** — 4x `*.py`
│   │   ├── **gold/**
│   │   ├── **raw/** — 2x `*.py`
│   │   └── **silver/**
│   └── **setup/** — 5x `*.py`
└── **tests/** — 7x `*.py`
```

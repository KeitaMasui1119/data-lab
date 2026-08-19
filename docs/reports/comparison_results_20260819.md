# 開発環境比較レポート: voltlake × a5chin/python-uv

| 項目 | 値 |
|------|-----|
| 作成日 | 2026-08-19 |
| 比較元 (自環境) | `KeitaMasui1119/voltlake` — ブランチ `chore/update-dev-environment` (base: `94ed2ac`) |
| 比較先 (参照元) | [`a5chin/python-uv`](https://github.com/a5chin/python-uv) — `c9aa25d` (2026-08-17 時点) |
| 目的 | 参照元テンプレートの現在の姿と自環境の乖離を洗い出し、取り込む/取り込まない判断材料を作る |
| 結論 | **取り込むべき差分あり。ただし全面追従は不適切**(下記「方針」参照) |

> 本レポートは調査のみ。ファイル変更は行っていない。

---

## 0. 前提: 2つのリポジトリの性格差

`a5chin/python-uv` は **汎用 Python 開発環境テンプレート**であり、リポジトリそのものが成果物。
`voltlake` は **日本の電力市場データ基盤**という具体的アプリケーションで、medallion アーキテクチャ・RustFS・PyIceberg という固有の関心事を持つ。

したがって差分は 3 種類に分かれる。本レポートはこの分類で整理する。

| 分類 | 意味 | 扱い |
|------|------|------|
| **A. 追従すべき** | 単純に参照元のほうが新しい/優れており、voltlake に固有の理由がない | 取り込み推奨 |
| **B. 意図的な差分** | voltlake の要件から生じた正当な差分 | 現状維持 |
| **C. voltlake が先行** | voltlake のほうが進んでいる | 現状維持(参照元に戻す必要なし) |

さらに、比較の過程で参照元とは無関係に **voltlake 側の不整合・不具合**が複数見つかったため、独立した章(§7)で扱う。

---

## 1. サマリ

### 取り込み推奨(優先度順)

| # | 項目 | 現状 | 参照元 | 優先度 |
|---|------|------|--------|--------|
| 1 | `.dockerignore` が存在しない | ビルドコンテキストに `.venv` (1.4GB) / `.git` / `data/` が入る | 3.6KB の除外定義あり | **HIGH** |
| 2 | 依存自動更新の仕組みなし | Renovate も Dependabot も未設定 | Renovate + Dependabot 両方 | **HIGH** |
| 3 | GitHub Actions のバージョンが古い | `checkout@v4` / `setup-uv@v6` | `checkout@v7` / `setup-uv@v10.0.1` | **HIGH** |
| 4 | CI に setup の重複 | 4 ジョブが同じ 3 ステップを複製、Python 3.13 をハードコード | composite action で DRY 化、`.python-version` を読む | MEDIUM |
| 5 | CI に actionlint / hadolint なし | Dockerfile・workflow の lint 未実施 | 専用 workflow + pre-commit hook | MEDIUM |
| 6 | カバレッジ閾値の強制なし | 計測のみ、下限なし | `--cov-fail-under=75` + Codecov | MEDIUM |
| 7 | `ruff.toml` の `target-version` 不整合 | `py312` (実際は 3.13) | `py314` (`.python-version` と一致) | MEDIUM |
| 8 | pre-commit の ruff rev が古い | `v0.12.8` (pyproject は `>=0.15.8`) | `v0.12.8` — **参照元も同じ問題** | LOW |

### 意図的な差分として維持すべきもの

- **Python バージョン: 3.13 が上限**(参照元は 3.14)。PyIceberg / Polars / DuckDB が 3.13 までしか対応していないため、3.14 は選択肢に入らない。§2 参照
- 型チェッカー: voltlake は **pyright** 継続を推奨(参照元は `ty` へ移行済みだが `ty` は v0.0.40 のプレリリース)
- ruff の `select`: voltlake は限定セット、参照元は `ALL`。段階的拡大は可能だが急ぐ必要なし
- mkdocs / cookiecutter: 参照元は使うが voltlake では実質不要
- **dbt / Airflow 系の依存は保持**(いずれも将来利用予定、dbt が先行)。SQLFluff は dbt 着手時に導入

### voltlake が先行している領域

- **Dockerfile**: 参照元は単純な 2 ファイル構成、voltlake は `base`/`dev`/`builder`/`prd`/`dashboard` の 5 ステージ multi-stage。明確に voltlake が上。
- **compose**: RustFS を含む実サービス構成を持つ
- **Claude Code 統合**: `.claude/settings.json` のフック、`.mcp.json`、`.github/instructions/` — 参照元にはない

---

## 2. ランタイム / 言語バージョン

| 項目 | voltlake | python-uv | 判定 |
|------|----------|-----------|------|
| `.python-version` | `3.13` | `3.14` | **B — 3.13 が上限(下記制約)** |
| `requires-python` | `>=3.13` | `>=3.11` | B(アプリなので上限を絞るのは妥当) |
| `ruff.toml` `target-version` | **`py312`** | `py314` | **A — 不整合** |
| `pyrightconfig.json` `pythonVersion` | `3.13` | (pyright 不使用) | — |
| Dockerfile `ARG VARIANT` | `3.13` | `3.14` | B(同上) |
| uv | 0.12.5 | (lock のみ) | — |

### 制約: Python 3.13 が上限

参照元は 3.14 に移行済みだが、**voltlake は 3.13 が天井**。中核依存が 3.14 に未対応のため。

| パッケージ | インストール版 | `Requires-Python` | classifier の対応上限 |
|-----------|---------|-----------------|-----------------|
| **pyiceberg** | 0.11.1 | `>=3.10.0,<4.0.0` | **3.13** |
| **polars** | 1.39.3 | `>=3.10` | **3.13** |
| **duckdb** | 1.4.1 | `>=3.9.0` | **3.13** |
| streamlit | 1.61.1 | `>=3.10` | 3.14 |
| pyarrow | 23.0.1 | `>=3.10` | 3.14 |

`Requires-Python` 上は 3.14 でも解決しうるが、classifier が 3.13 止まりということはビルド済み wheel と CI 検証の対象外を意味する。PyIceberg・Polars・DuckDB は silver / gold レイヤーの全処理が乗っている中核であり、未検証バージョンで動かす価値はない。

**したがって、参照元の 3.14 化は追従しない。** この 3 パッケージが 3.14 対応の wheel を出すまで 3.13 に固定する。`.python-version` / Dockerfile `ARG VARIANT` / CI のハードコード値はいずれも 3.13 のままでよい。

### 指摘: `ruff.toml` だけ `py312` を向いている

実行環境が 3.13 なのに ruff の `target-version` は `py312`。3.13 で追加された構文・非推奨化を ruff が検出できず、`UP` (pyupgrade) ルールも 3.13 相当の近代化を提案しなくなる。

**`py313` へ修正すべき。** 上限が 3.13 で固定される以上、この値は当面変わらないので一度直せば済む。

---

## 3. 静的解析 / フォーマット

### 3.1 型チェッカー — 最大の思想差

| | voltlake | python-uv |
|---|---|---|
| ツール | **pyright 1.1.408** | **ty 0.0.40** (Astral 製) |
| 設定 | `pyrightconfig.json` | `ty.toml` |
| pre-commit | local hook `uv run pyright` | local hook `uv run ty check` |
| CI | `typecheck` ジョブ | `lint.yml` の `ty` ジョブ |
| エディタ | Pylance (`ms-python.vscode-pylance`) | `astral-sh.ty` + `python.languageServer: "None"` |

参照元は pyright/basedpyright を捨てて `ty` に全面移行している(`.zed/settings.json` でも `"!basedpyright"` と明示的に無効化)。

**判定: B(追従しない)**。`ty` はバージョンが `0.0.40` であり、Astral 自身がプロダクション利用を推奨する段階に達していない。voltlake は PyIceberg / Polars / DuckDB という型スタブの質が一様でないライブラリ群に依存しており、型チェッカーの成熟度が直接開発効率に響く。pyright 継続が妥当。

`ty` が 1.0 に到達した時点で再評価する、という形で保留するのが良い。

### 3.2 ruff

| 項目 | voltlake | python-uv |
|------|----------|-----------|
| `line-length` / `indent-width` | 88 / 4 | 88 / 4(同一) |
| `select` | `["E","F","W","I","B","UP"]` | `["ALL"]` |
| `ignore` | 12 個 | 19 個(`ALL` 前提の抑制) |
| `force-exclude` | `true` | 未設定 |
| exclude | notebook・旧 stg ディレクトリを追加除外 | 標準のみ |
| 依存バージョン | `ruff>=0.15.8` | `ruff>=0.15.15` |

**判定: 主に B**。`select = ["ALL"]` は docstring 必須 (`D`)、セキュリティ (`S`)、複雑度 (`C90`) などを一気に有効化するため、既存コードベースへの後付けは大量の違反を生む。ただし段階的に価値の高いカテゴリを足すのは有効:

- `S` (bandit) — セキュリティ。グローバル規約が「コミット前セキュリティチェック」を要求している以上、整合性が高い
- `PTH` (pathlib) — `os.path` の排除
- `RUF` — ruff 固有の有用ルール
- `SIM` — 簡約

`force-exclude = true` は voltlake 側のみ。pre-commit がファイルパスを直接渡すときに exclude を効かせるために必要な設定で、**voltlake のほうが正しい**(C)。

### 3.3 その他の lint

| ツール | voltlake | python-uv | 判定 |
|--------|----------|-----------|------|
| **actionlint** | なし | pre-commit hook + `actionlint.yml` workflow | **A — 取り込み推奨** |
| **hadolint** | devcontainer feature + pre-commit のみ | 同左 + `docker.yml`/`devcontainer.yml` の CI で実行 | A(CI 化) |
| **SQLFluff** | なし(`shandy-sqlfmt` を宣言のみ、未使用) | `.sqlfluff` + pre-commit + CI ジョブ | **A(保留)— dbt 着手時に導入** |
| **codebook / cspell** (スペルチェック) | なし | `codebook.toml` (129 語) + `.vscode/cspell.json` | B(任意) |
| **prettier** | なし | `.prettierrc.json` (JSON のみ) | B |

`voltlake` は GitHub Actions を 2 ファイル持つが lint していない。actionlint は導入コストがほぼゼロ(pre-commit に 3 行)で、workflow の typo を CI 実行前に潰せる。**取り込み推奨**。

**SQLFluff について(2026-08-19 方針決定)**: dbt は近い将来に着手予定であることが確認された。現時点では SQL ファイルが存在しないため導入しても対象ゼロだが、**dbt モデルを書き始めるタイミングで参照元の `.sqlfluff` + pre-commit hook + CI ジョブをまとめて導入する**のが望ましい。その際、既存の `shandy-sqlfmt` (フォーマッタ) と SQLFluff (リンタ + フォーマッタ) の役割が重複するため、どちらかに寄せる判断が必要になる。参照元の `.sqlfluff` は `dialect = bigquery` なので、voltlake では `duckdb` へ変更すること。

---

## 4. テスト / カバレッジ

| 項目 | voltlake | python-uv |
|------|----------|-----------|
| 設定場所 | `pyproject.toml` の `[tool.pytest.ini_options]` | `pytest.ini` 単独ファイル |
| `testpaths` | `tests` | `tests` |
| テストファイル命名 | `test_*.py` (既定) | `test__*.py` |
| カバレッジ既定 | **なし**(CI で `--cov` を都度指定) | `addopts` に常時付与 |
| **カバレッジ下限** | **なし** | **`--cov-fail-under=75`** |
| ブランチカバレッジ | なし | `--cov-branch` |
| 除外設定 | なし | `.coveragerc` |
| Codecov 連携 | なし | `codecov.yaml` + `codecov-action@v7` + `test-results-action@v1` |
| マーカー | `integration` マーカー定義あり | なし |
| import mode | 既定 | `importlib` |

**指摘**: グローバル規約(`~/.claude/rules/ecc/common/testing.md`)は **最低 80% のカバレッジ**を要求しているが、voltlake にはそれを強制する仕組みがない。CI は `--cov-report=term-missing` を出力するだけで、数値が下がっても PR は通る。

**推奨**: `--cov-fail-under` を設定する。ただし現在の実カバレッジを計測してから閾値を決めること(いきなり 80 を入れると CI が即赤化する可能性)。段階的に上げるのが現実的。

`integration` マーカーによる分離は voltlake 独自で、外部サービス依存を切り分ける良い設計(C)。

---

## 5. タスクランナー / CI/CD

### 5.1 nox

参照元は `noxfile.py` で `fmt` / `lint` / `test` セッションを定義し、**ローカルと CI が同一コマンドを叩く**構造になっている。

```
uv run nox -s lint -- --ruff --ty
uv run nox -s test -- --cov_report xml --junitxml junit.xml
```

voltlake は `nox>=2026.2.9` を dev 依存に**宣言しているが `noxfile.py` が存在しない**(§7-3 参照)。CI は生の `uv run ruff check ...` を直書きしている。

**判定: A(中優先)**。voltlake の CI ジョブは 4 つで、それぞれが以下 3 ステップを完全に重複させている:

```yaml
- uses: actions/checkout@v4
- uses: astral-sh/setup-uv@v6
  with: { enable-cache: true, python-version: "3.13" }
- run: uv sync --frozen
```

Python バージョンが **4 箇所にハードコード**されている。3.13 に固定される前提(§2)なので値がずれる事故のリスクは低いが、`.python-version` を単一の情報源にしておくほうが素直ではある。この項目の主眼はバージョン管理よりも **setup 3 ステップの重複排除**にある。

### 5.2 GitHub Actions のバージョン

| Action | voltlake | python-uv | 差 |
|--------|----------|-----------|-----|
| `actions/checkout` | **v4** | v7 | 3 メジャー遅れ |
| `astral-sh/setup-uv` | **v6** | v10.0.1 | 4 メジャー遅れ |
| `docker/build-push-action` | v5 | (v6 系を使用) | 1 メジャー遅れ |
| `docker/login-action` | v3 | — | 最新 |
| `docker/metadata-action` | v5 | — | 最新 |

**A — HIGH**。特に `checkout@v4` は Node 20 ランナー世代であり、GitHub 側の非推奨化スケジュールに乗る。

### 5.3 workflow の網羅性

| 目的 | voltlake | python-uv |
|------|----------|-----------|
| lint | `ci.yml` の 1 ジョブ | `lint.yml` (ruff/sqlfluff/ty の 3 ジョブ) |
| format 検査 | `ci.yml` の 1 ジョブ | `format.yml` |
| 型チェック | `ci.yml` の 1 ジョブ | `lint.yml` に統合 |
| テスト | `ci.yml` の 1 ジョブ | `test.yml` + Codecov |
| workflow lint | **なし** | `actionlint.yml` |
| Dockerfile lint + build 検証 | **なし** | `docker.yml` |
| devcontainer build 検証 | **なし** | `devcontainer.yml` |
| イメージ公開 | `deploy.yml` (GHCR) | `publish-app.yml` / `publish-devcontainer.yml` |
| ドキュメント公開 | **なし** | `gh-deploy.yml` (GitHub Pages) |
| リリースノート自動生成 | **なし** | `release.yml` + `release-drafter.yml` |
| PR ラベル自動付与 | **なし** | `labeler.yml` + `labeler.yml` 設定 |
| PR assignee 自動設定 | **なし** | `assign.yml` |
| bot PR 自動 approve | **なし** | `approve.yml` |
| AI レビュー | **なし** | `pr-agent.yml` (Qodo) |
| リポジトリ設定の IaC 化 | **なし** | `setting.yml` + `environments.json` / `protection.json` |
| PR テンプレート | **なし** | `PULL_REQUEST_TEMPLATE.md` |
| CODEOWNERS | **なし** | あり |

参照元は 13 workflow + 2 composite action。voltlake は 2 workflow。

ただし後半の多く(assign / approve / pr-agent / setting / release-drafter)は **複数人チーム・公開 OSS 前提**の自動化であり、個人開発の voltlake には過剰。`actionlint.yml` / `docker.yml` / `devcontainer.yml` の 3 つが費用対効果の高い候補。

### 5.4 依存の自動更新 — 最大のギャップ

| | voltlake | python-uv |
|---|---|---|
| Renovate | **なし** | `renovate.json`(毎時実行、minor/patch 自動マージ、lockFileMaintenance 有効、Asia/Tokyo) |
| Dependabot | **なし** | `.github/dependabot.yml`(pip + github-actions、日次) |

参照元の git log は直近 8 コミット中 6 件が Renovate/Dependabot による依存更新。**この仕組みがあるからこそ参照元は常に最新に保たれている**。

「開発環境のアップデート」を継続的な営みにするなら、単発でバージョンを上げるより **Renovate を入れることが本質的な解決**になる。voltlake には現在この仕組みが一切なく、GitHub Actions が 3〜4 メジャー遅れているのもその帰結。

**A — HIGH。最優先で取り込むべき項目。**

---

## 6. コンテナ / DevContainer

### 6.1 Dockerfile — voltlake が明確に先行

| | voltlake | python-uv |
|---|---|---|
| 構成 | 5 ステージ (`base`→`dev`/`builder`→`prd`→`dashboard`) | 単一ステージ + devcontainer 用に別ファイル |
| 本番の依存解決 | `builder` で `--no-dev` 分離 | `uv sync --frozen --no-install-project` のみ |
| 非 root 実行 | `prd` で `appuser` (uid 1000) | devcontainer の `vscode` のみ |
| 追加ツール | gh CLI / Node 22 / Claude Code / Gemini CLI をピン留めして導入 | なし |
| devcontainer 用イメージ | 同一 Dockerfile の `dev` target | `.devcontainer/Dockerfile` に分離 |

**判定: C**。voltlake の multi-stage 構成は参照元より明確に洗練されている。追従不要。

参照元から拾える細部:
- `libgl1 libglib2.0-0` の導入(OpenCV 系)→ voltlake には不要
- devcontainer Dockerfile を分離する設計 → voltlake の統合方式のほうが DRY で優位

### 6.2 devcontainer.json

| 項目 | voltlake | python-uv | 判定 |
|------|----------|-----------|------|
| `UV_PROJECT_ENVIRONMENT` | `/home/vscode/.venv` | `${containerWorkspaceFolder}/.venv` | **§7-1 参照(不整合あり)** |
| `.venv` の永続化 | なし(bind mount 配下) | **named volume** `venv-${devcontainerId}` | A(検討価値あり) |
| cache の永続化 | なし | named volume `cache-${devcontainerId}` | A(検討価値あり) |
| `postCreateCommand` | `uv sync --locked` + git config + gh | `uv sync --frozen` | B |
| `postStartCommand` | `uv run pre-commit install \|\| true` | `uv run pre-commit install` | B |
| SSH マウント | なし | `~/.ssh` を bind | B |
| 独自マウント | `~/.claude`, `~/.config/gh` | なし | C |
| features | hadolint | hadolint | 同一 |
| devcontainer-lock.json | あり | なし | C |

参照元の **named volume による `.venv` 分離**は、bind mount 上に venv を置くことによる I/O 劣化(特に macOS/Windows)を避ける定番手法。voltlake は WSL2 上なので影響は相対的に小さいが、`.venv` が bind mount 経由でホストに 1.4GB 展開されている現状を考えると検討価値がある。

### 6.3 `.dockerignore` — 存在しない

参照元は 3.6KB の `.dockerignore` を持つ。**voltlake には存在しない**。

現在の `Dockerfile` は `COPY pyproject.toml uv.lock ./` と `COPY src ./src` しかしていないため直接の混入は起きないが、**Docker のビルドコンテキスト送信は `.dockerignore` に従うため、ビルドのたびに以下が daemon へ転送される**:

| 対象 | サイズ |
|------|--------|
| `.venv/` | **1.4 GB** |
| `.git/` | 数十 MB |
| `data/` | 可変 |
| `.pytest_cache` / `.ruff_cache` / `.coverage` | 小 |
| **`.secrets/`** | 小だが**機密** |
| `.env` | 小だが**機密** |

`.secrets/` と `.env` がビルドコンテキストに入る点は、`COPY . .` を将来書いた瞬間にイメージへ機密が焼き込まれるリスクになる。

**A — HIGH。最優先クラスで取り込むべき。**

---

## 7. voltlake 側で発見した不整合・問題

参照元との比較とは独立に見つかった問題。いずれも今回の「環境アップデート」で併せて解消する価値がある。

### 7-1. `.venv` が 2 つ存在し、合計 2.8GB を消費している【HIGH】

```
/workspace/.venv        1.4G   ← uv run が実際に使っているのはこちら
/home/vscode/.venv      1.4G   ← devcontainer.json が指定しているのはこちら
```

原因は設定の競合:

| ファイル | 設定 | 値 |
|---------|------|-----|
| `.devcontainer/devcontainer.json` | `containerEnv.UV_PROJECT_ENVIRONMENT` | `/home/vscode/.venv` |
| `.vscode/settings.json` | `terminal.integrated.env.linux.UV_PROJECT_ENVIRONMENT` | `.venv` |
| `.vscode/settings.json` | `python.defaultInterpreterPath` | `/workspace/.venv/bin/python` |
| `.vscode/launch.json` | `python` | `/home/vscode/.venv/bin/python3` |

VS Code のターミナルでは `terminal.integrated.env.linux` が `containerEnv` を上書きするため `uv run` は `/workspace/.venv` を使う。一方 `postCreateCommand` の `uv sync` は `containerEnv` 下で走るので `/home/vscode/.venv` を作る。**デバッガ (`launch.json`) は `/home/vscode/.venv` を見るので、ターミナルで動くコードとデバッガで動くコードの依存が食い違いうる。**

**推奨**: どちらか一方に統一する。参照元と同じく `${containerWorkspaceFolder}/.venv`(= `/workspace/.venv`)に寄せ、`launch.json` もそれに合わせるのが素直。統一後に不要な側を削除すれば 1.4GB 回収できる。

### 7-2. `duckdb` が直接依存として宣言されていない【HIGH】

```
duckdb v1.4.1
└── dbt-duckdb v1.10.1
    └── default v0.1.0
```

`duckdb` は **19 ファイルで直接 import されている中核ライブラリ**(silver / gold 変換の全体がこれに乗っている)にもかかわらず、`pyproject.toml` に直接の依存として書かれていない。`dbt-duckdb` 経由の推移的依存として偶然入っているだけ。

`CLAUDE.md` は「dbt プロジェクトは空で現在未使用」と明記している。整理のつもりで `dbt-duckdb` を削除した瞬間、**silver / gold パイプラインが全滅する。**

**推奨**: `duckdb` を直接依存として明示追加する(dbt を消すかどうかとは無関係に、今すぐやるべき)。

### 7-3. 宣言されているが未使用の依存が多数【MEDIUM】

`src/` と `tests/` の全 import を走査した結果:

| パッケージ | import 箇所 | 判断 | 備考 |
|-----------|:---:|------|------|
| `dbt-core` | 0 | **保持** | **近い将来に着手予定**(2026-08-19 確認) |
| `dbt-duckdb` | 0 | **保持** | 同上。§7-2 の通り `duckdb` の供給源も兼ねている |
| `shandy-sqlfmt[jinjafmt]` | 0 | **保持** | dbt の SQL フォーマッタ。着手時に SQLFluff と役割整理 |
| `apache-airflow` | 0 | **保持(要再検討)** | 将来利用予定。ただし着手は dbt より後。依存ツリーを最も肥大化させるため §7-3b 参照 |
| `scikit-learn` | 0 | 削除候補 | |
| `Scrapy` | 0 | 削除候補 | スクレイピングは `common/http_scraper.py` の自前実装 |
| `seaborn` | 0 | 削除候補 | 可視化は plotly |
| `beautifulsoup4` | 0 | 削除候補 | |
| `lxml` | 0 | 削除候補 | `read_html` 等の間接利用も検出されず |
| `nbformat` | 0 | 削除候補 | |
| `numpy` | 0 | 削除候補 | 推移的には入る |
| `pandas` | 0 | 削除候補 | 推移的には入る |
| `pydantic` / `pydantic-settings` | 0 | 削除候補 | |
| `mkdocs-material` (dev) | 0 | 要判断 | `mkdocs.yml` 自体が存在しない(§8) |
| `nox` (dev) | 0 | 要判断 | `noxfile.py` 自体が存在しない。§5.1 を実施するなら使う |
| `cookiecutter` 系 (dev) | 0 | 削除候補 | |

実際に使われているのは `polars`(41)/`pyiceberg`(21)/`duckdb`(19)/`boto3`/`requests`/`plotly`/`streamlit`/`jpholiday`/`pyarrow` 程度。

影響: `uv sync` の所要時間、devcontainer 起動時間、`.venv` の 1.4GB、Docker イメージサイズ、そして Renovate 導入後は**更新 PR のノイズ**。

**推奨**: dbt / Airflow 系(`dbt-core` / `dbt-duckdb` / `shandy-sqlfmt` / `apache-airflow`)は将来利用予定のため**保持**。それ以外の削除候補を段階的に外す。§7-2 の `duckdb` 明示を**先に**済ませること。

### 7-3b. `apache-airflow` を先行して入れておくコスト

Airflow は将来利用予定だが着手は dbt より後、という位置づけ。それでも今すぐ削除を提案しないのは方針どおりとして、以下のコストは認識しておく価値がある。

- 依存ツリーが単独で最も大きく、`uv sync` / devcontainer 起動 / イメージビルドの所要時間に恒常的に効く
- Renovate 導入後、実際には使っていない Airflow とその依存群の更新 PR が定常的に発生する
- Airflow は Python バージョンと依存ピンの制約が厳しく、**他パッケージの更新を阻害する**ことがある(§2 の 3.13 制約と競合しうる)

実際に着手するまでは `[dependency-groups]` の任意グループ(例: `airflow`)へ移し、既定の `uv sync` からは外しておく、という折衷案が取れる。dbt 側は着手が近いので `dependencies` に残したままでよい。**今回の 5 項目には含めず、判断が必要な項目として残す。**

### 7-4. `devcontainer.json` に個人情報・機密パスがハードコードされている【MEDIUM】

```json
"GOOGLE_APPLICATION_CREDENTIALS": "/workspace/.secrets/keita-masui-firstproject-46adeb7731b9.json",
"postCreateCommand": "... git config --global user.name 'Keita Masui' && git config --global user.email 'rfrnndam.tennis365@gmail.com' ..."
```

鍵ファイルの実ファイル名(プロジェクト ID 由来)がリポジトリにコミットされている。鍵そのものではないため直ちに危険ではないが、GCP プロジェクト識別子の露出であり、リポジトリを公開する場合は問題。git config の個人名/メールも、他者がこの環境を使うと上書きされてしまう。

> **解消済み (2026-08-19)** — §10c-1 参照。`GOOGLE_APPLICATION_CREDENTIALS` は
> 参照元が `devcontainer.json` の 1 行だけの完全な死に設定だったため削除。
> git config は VS Code Dev Containers がホストの `~/.gitconfig` から自動コピーするため削除。

### 7-5b. `deploy.yml` が意図しないステージをビルドしている【MEDIUM】

`.github/workflows/deploy.yml` の `docker/build-push-action` に `target:` の指定がない。

```yaml
- name: Build and push Docker image
  uses: docker/build-push-action@v7
  with:
    context: .
    push: true          # target 未指定
```

`target` 省略時は **Dockerfile の最終ステージ**がビルドされる。voltlake の Dockerfile のステージ順は
`uv` → `base` → `dev` → `builder` → `prd` → **`dashboard`** であり、最終ステージは `dashboard`。

つまり `ghcr.io/KeitaMasui1119/voltlake:latest` として公開されているイメージは、
リポジトリ名から想像される CLI アプリ (`prd`, `CMD ["python","-m","src.main"]`) ではなく
**Streamlit ダッシュボード** (`CMD ["streamlit","run",...]`)。

**推奨**: `target: prd` を明示する。あるいは参照元の `publish-app.yml` / `publish-devcontainer.yml` に倣い、
`prd` と `dashboard` を別タグ(`:latest` と `:dashboard`)で publish する matrix 構成にする。
どちらが意図だったかは要判断のため、今回の 5 項目には含めていない。

> **解消済み (2026-08-19)** — §10c-2 参照。両ステージを別パッケージで publish する構成に変更。
> なお PR #89 のマージで実際に走ったビルドログにより、**推測ではなく実測で確認できた**:
> `#18 [dashboard 1/1] COPY .streamlit` の直後に
> `#19 naming to ghcr.io/keitamasui1119/voltlake:latest` が出ており、
> `:latest` が dashboard ステージであったことは確定。

### 7-5. pre-commit の ruff バージョンが実依存と乖離【LOW】

| | pre-commit `rev` | pyproject |
|---|---|---|
| voltlake | `v0.12.8` | `ruff>=0.15.8`(実インストール 0.15.8) |
| python-uv | `v0.12.8` | `ruff>=0.15.15` |

pre-commit は独自の隔離環境で `v0.12.8` を使うため、**ローカルの `uv run ruff` (0.15.8) とコミットフックの ruff (0.12.8) でルール解釈が異なる**。CI は `uv run ruff` なので、フックを通ったのに CI で落ちる/その逆が起こりうる。

なお参照元も同じ問題を抱えている(参照元の Renovate は `rev` を追っていない模様)ため、**これは追従ではなく voltlake が先に直すべき項目**。

`.coverage` / `.pytest_cache` / `.ruff_cache` がリポジトリ直下に残っている点も併せて `.gitignore` の確認対象(`.coverage` は gitignore 済みだがファイルは残存)。

---

## 8. エディタ / ドキュメント

| 項目 | voltlake | python-uv | 判定 |
|------|----------|-----------|------|
| `.vscode/settings.json` | 言語別フォーマッタ設定あり | ほぼ同等 + sqlfluff/zsh 設定 | ほぼ同一 |
| `.vscode/extensions.json` | Copilot / Pylance 系 | ty / jupyter / cspell / sqlfluff 系 | B |
| `.vscode/launch.json` | あり(デバッグ構成) | なし | C |
| `.vscode/mcp.json` | あり | なし | C |
| `.zed/settings.json` | なし | あり | B(Zed 未使用なら不要) |
| `.vscode/cspell.json` + `codebook.toml` | なし | あり | B |
| mkdocs サイト | **なし**(依存だけ宣言) | `mkdocs.yml` + `docs/` 28 ページ + Pages 公開 | B〜A |
| `CONTRIBUTING.md` / `CODE_OF_CONDUCT.md` | なし | あり | B(個人開発なら不要) |

voltlake の `docs/` は `architecture/` `reports/` `tasks/` という**設計記録**の場であり、参照元の `docs/`(ツール利用ガイド)とは目的が違う。ただし mkdocs-material を dev 依存に持ちながら `mkdocs.yml` がないのは中途半端。**使うなら設定を書く、使わないなら依存を落とす**、どちらかに寄せるべき。

---

## 9. 推奨アクション

### フェーズ 1: 低リスク・高効果(すぐやる)

1. **`.dockerignore` を追加** — 参照元のものをベースに `data/` `.secrets/` `.env` `configuration/iceberg/catalog/` を追記
2. **`duckdb` を直接依存に明示** — §7-2。他の依存整理より前に必ず
3. **`ruff.toml` の `target-version` を `py313` へ修正** — §2
4. **GitHub Actions を更新** — `checkout@v4→v7`、`setup-uv@v6→v10`、`build-push-action@v5→v6`
5. **`.venv` の二重化を解消** — §7-1。`/workspace/.venv` に統一し `launch.json` を合わせる

### フェーズ 2: 継続性の担保(これが本命)

6. **Renovate を導入** — 参照元の `renovate.json` をベースに、schedule を毎時から週次程度に緩め、automerge は patch のみから始める
7. **`actionlint` を pre-commit + CI に追加**
8. **CI を composite action で DRY 化** — `.python-version` を読ませ、Python バージョンのハードコードを排除
9. **pre-commit の ruff `rev` を pyproject と揃える** — §7-5

### フェーズ 3: 判断が要るもの

10. **カバレッジ下限の設定** — まず現状値を計測 → 少し下の値を `--cov-fail-under` に置き、段階的に 80% へ
11. **未使用依存の削除** — dbt / Airflow 系は保持。`scikit-learn` / `Scrapy` / `seaborn` / `beautifulsoup4` / `lxml` / `nbformat` / `cookiecutter` 系から。`apache-airflow` は任意グループへの移設を検討(§7-3b)
12. **`devcontainer.json` の個人情報を `${localEnv:}` 化** — §7-4
13. **Docker / devcontainer の build 検証 workflow 追加**
14. **ruff `select` の段階的拡大** — `S` / `RUF` / `SIM` / `PTH` から

### 見送り推奨

- **Python 3.14 化** — PyIceberg / Polars / DuckDB が 3.13 までしか対応しておらず不可(§2)
- `ty` への移行(v0.0.40 のプレリリース、pyright 継続が妥当)
- `select = ["ALL"]` の一括適用
- release-drafter / labeler / assign / approve / pr-agent / setting.yml(個人開発には過剰)
- mkdocs サイト公開(必要になってから)

---

## 10. 実施記録 (2026-08-19)

フェーズ 1 の 5 項目をブランチ `chore/update-dev-environment` で実施した。

### 10-1. `.dockerignore` を新規作成

参照元の構成をベースに、voltlake 固有の除外を追加(`.secrets/` / `data/` / `configuration/iceberg/catalog/` / dbt 成果物 / `src/Jupyter/`)。

ビルドコンテキストの推定サイズ:

| | サイズ |
|---|---|
| 変更前 | **約 1.7 GB** (`.venv` 1.4GB + `.git` 149MB + `data/` 163MB + その他) |
| 変更後 | **約 1 MB** (`src/` の実コード + `pyproject.toml` + `uv.lock` + `.streamlit/`) |

`.secrets/` と `.env` がコンテキストから外れたことで、将来 `COPY . .` を書いても機密がイメージへ焼き込まれない。

> Docker CLI が devcontainer 内に無いため、実ビルドによる検証は未実施。CI (`deploy.yml`) の初回実行で確認すること。

### 10-2. `duckdb` を直接依存に明示

`pyproject.toml` に `'duckdb>=1.4.1'` を追加し `uv lock` を実行(287 packages 解決、他パッケージのバージョン変動なし)。

```
変更前: duckdb v1.4.1 └── dbt-duckdb v1.10.1 └── default v0.1.0
変更後: duckdb v1.4.1 ├── dbt-duckdb v1.10.1 └── default v0.1.0
                      └── default v0.1.0          ← 直接依存として出現
```

### 10-3. `ruff.toml` の `target-version` を `py313` へ

`py312` → `py313`。誤った `# Assume Python 3.14` コメントも実態(3.13 が上限である理由つき)に修正。
**新規の lint 違反は発生しなかった** (`All checks passed!` / 95 files already formatted)。

### 10-4. GitHub Actions を更新

| Action | 変更前 | 変更後 | 破壊的変更の確認 |
|--------|-------|-------|------------|
| `actions/checkout` | v4 | **v7** | なし |
| `astral-sh/setup-uv` | v6 | **v10** | なし |
| `docker/login-action` | v3 | **v4** | Node 24 化 + ESM のみ |
| `docker/metadata-action` | v5 | **v6** | リスト入力の `#` 扱い変更。当リポジトリの `tags:` は `#` を含まず影響なし |
| `docker/build-push-action` | v5 | **v7** | v6 でビルドサマリ追加、v7 で未使用の非推奨 env 削除。影響なし |

docker 系 3 つはいずれもランナー v2.327.1 以上を要求するが、GitHub ホストランナーは充足済み。

### 10-5. `.venv` の二重化を解消

`/workspace/.venv` に一本化した。

| ファイル | 変更前 | 変更後 |
|---------|-------|-------|
| `.devcontainer/devcontainer.json` | `UV_PROJECT_ENVIRONMENT: /home/vscode/.venv` | `${containerWorkspaceFolder}/.venv` |
| `.vscode/launch.json` | `python: /home/vscode/.venv/bin/python3` | `${workspaceFolder}/.venv/bin/python` |
| `.vscode/settings.json` | `terminal.integrated.env.linux` で `.venv` を上書き | **削除**(相対パスで cwd 依存だったため。`containerEnv` を単一の情報源にする) |

`python.defaultInterpreterPath` は元から `/workspace/.venv/bin/python` なので変更なし。これで
**ターミナル・デバッガ・`uv run`・エディタの 4 者がすべて同じ環境を指す**。

> `/home/vscode/.venv` (1.4GB) は §10-7 で削除済み。

**注意: `pre-commit install` の再実行が必要**

`.git/hooks/pre-commit` は pre-commit が生成する shim で、**インストール時の Python 絶対パスを埋め込む**。

```sh
INSTALL_PYTHON=/home/vscode/.venv/bin/python   # 旧 venv を指したまま
```

旧 venv を消した直後の `git commit` はこれで失敗した(`pre-commit not found`)。
`uv run pre-commit install --overwrite` で `/workspace/.venv/bin/python` を指すよう再生成して解消。

devcontainer の `postStartCommand` が `uv run pre-commit install` を実行するため
**コンテナを作り直す場合は自動で直る**が、既存コンテナで作業を続ける場合は手動実行が要る。

### 10-6. 検証結果

| 検査 | 結果 |
|------|------|
| `uv sync --locked --group dev` | 284 packages checked、差分なし |
| `uv run pytest -m "not integration"` | **471 passed** |
| `uv run ruff check src/ tests/` | All checks passed |
| `uv run ruff format --check src/ tests/` | 95 files already formatted |
| `uv run pyright` | 0 errors, 0 warnings |
| JSON 構文 (devcontainer / launch / settings) | OK |
| YAML 構文 (ci.yml / deploy.yml) | OK |
| Docker ビルド | **未検証**(devcontainer 内に docker CLI なし) |

### 10-7. 後片付け

- 参照リポジトリのクローン (13MB) を削除
- `/home/vscode/.venv` (1.4GB) を削除。`uv run` は `/workspace/.venv` を使い続けることを確認済み

---

## 10b. フェーズ 2 実施記録 (2026-08-19)

### 10b-1. Renovate を導入 (`renovate.json` 新規)

参照元の設定をそのまま持ってくると voltlake では危険な箇所があるため、方針を変えて書いた。

| 設定 | 参照元 | voltlake | 理由 |
|------|-------|---------|------|
| `schedule` | 毎時 (`* 0-23 * * *`) | **週次** (月曜 9 時前) | 個人開発で毎時は PR ノイズが過大 |
| `lockFileMaintenance` | 有効・automerge | 有効・**月次・automerge なし** | 全依存が動くため目視したい |
| minor/patch の automerge | **全パッケージ** | **限定**(下記) | CI が RustFS/Iceberg を検証しないため |
| `separateMajorMinor` | `false` | 既定 (`true`) | major は分けて見たい |
| `constraints.python` | なし | **`<3.14`** | §2 の上限を Renovate に守らせる |

automerge の線引き:

| 対象 | automerge | 根拠 |
|------|:---:|------|
| GitHub Actions (minor/patch/digest) | **する** | CI の合否がそのまま検証になる |
| dev 依存 (minor/patch) | **する** | ビルドにしか影響せず、データを壊さない |
| `polars` / `pyiceberg` / `duckdb` / `pyarrow` / `boto3` / dbt / airflow | **しない** + `minimumReleaseAge: 7 days` | **CI は `-m "not integration"` で RustFS・Iceberg を叩かない。CI が緑でもパイプラインが無事な証拠にならない** |
| `python` | **更新自体を無効化** | 3.13 上限(§2)。Renovate が 3.14 を提案してくるのを防ぐ |
| `ruff` + `astral-sh/ruff-pre-commit` | 同一グループ | 2 箇所ピンなので必ず一緒に動かす(§7-5) |

> **未完了**: `renovate.json` を置いただけでは動かない。GitHub 側で
> [Renovate App](https://github.com/apps/renovate) をこのリポジトリにインストールする必要がある。
> インストール後、Renovate が「Dependency Dashboard」issue を作って初回 PR を出す。

検証: `renovate-config-validator --strict` で `Config validated successfully`。

### 10b-2. actionlint を導入

| 場所 | 内容 |
|------|------|
| `.pre-commit-config.yaml` | `rhysd/actionlint` v1.7.12 を追加 |
| `.github/workflows/ci.yml` | `reviewdog/action-actionlint@v1` のジョブを追加 |

既存の 2 workflow に対して実行し、いずれも Passed。hadolint も同様に CI ジョブ化した
(`hadolint/hadolint-action@v3.4.0`)。ローカルの `hadolint Dockerfile` も問題なし。

### 10b-3. CI を composite action で DRY 化

`.github/actions/setup-python-with-uv/action.yml` を新設し、4 ジョブが重複していた 3 ステップを 1 行に置換。

```yaml
# 変更前(4 ジョブで完全に同一の記述を反復)
- uses: actions/checkout@v7
- uses: astral-sh/setup-uv@v10
  with:
    enable-cache: true
    python-version: "3.13"     # ← 4 箇所にハードコード
- run: uv sync --frozen

# 変更後
- uses: actions/checkout@v7
- uses: ./.github/actions/setup-python-with-uv
```

Python バージョンは `.python-version` から読むため、ハードコードは消えた。
併せて `uv sync --frozen` → **`--locked`** に変更した。`--frozen` はロックファイルの
鮮度を検査しないため、`pyproject.toml` だけ編集して `uv.lock` を更新し忘れた PR が
CI を通ってしまう。`--locked` なら差分がある時点で落ちる。

`ci.yml` は 4 ジョブ → 6 ジョブになったが、行数は 69 → 66 行。

### 10b-4. ruff のバージョン乖離を解消 (§7-5)

pre-commit と pyproject が別バージョンを使っていた問題。**両方を最新の 0.16.3 に揃えた。**

| | 変更前 | 変更後 |
|---|-------|-------|
| `.pre-commit-config.yaml` の `rev` | `v0.12.8` | `v0.16.3` |
| `pyproject.toml` の pin | `ruff>=0.15.8` | `ruff>=0.16.3` |

0.15.8 → 0.16.3 で**新規の指摘・整形差分はゼロ**(事前に `uvx ruff@0.16.3` で確認してから上げた)。
両ファイルに「もう一方と揃えること」というコメントを入れ、`renovate.json` でも同一グループにした。

ついでに他の pre-commit hook も更新:

| hook | 変更前 | 変更後 | 備考 |
|------|-------|-------|------|
| `pre-commit/pre-commit-hooks` | v5.0.0 | **v6.0.0** | 削除された `check-byte-order-marker` / `fix-encoding-pragma` は未使用 |
| `hadolint/hadolint` | v2.12.0 | **v2.15.1** | |

### 10b-5. 検証結果

| 検査 | 結果 |
|------|------|
| `pre-commit run --all-files` | **全 12 hook Passed**(Actionlint / Lint Dockerfiles / Pyright 含む) |
| `uv run pytest -m "not integration"` | **471 passed** |
| `uv run ruff check` / `format --check` | All checks passed / 95 files already formatted |
| `uv run pyright` | 0 errors, 0 warnings |
| `renovate-config-validator --strict` | Config validated successfully |
| GitHub Actions の実行 | **未検証**(push 後の初回 CI で確認) |

### 10b-6. 残タスク

**要操作(GitHub 側)**

- **Renovate App のインストール** — これをやるまで `renovate.json` は効かない → **完了 (2026-08-19)**

**未対応(判断が必要)**

- ~~§7-5b: `deploy.yml` が `dashboard` ステージを publish している~~ → **§10c-2 で解消**
- ~~§7-4: `devcontainer.json` の個人情報・GCP 鍵ファイル名のハードコード~~ → **§10c-1 で解消**
- ~~§4: カバレッジ下限 (`--cov-fail-under`) の設定 — 現状値の計測が先~~ → **§10d-1 で解消**
- ~~§7-3b: `apache-airflow` を任意 dependency-group へ移すか~~ → **§10d-2 で解消**
- ~~§3.2: ruff `select` の段階的拡大 (`S` / `RUF` / `SIM` / `PTH`)~~ → **§10d-3 で PTH/SIM/RUF 採用、S は保留**
- ~~`ruff.toml` の `exclude` に実在しないパス (`src/stg/pydev`, `src/stg/scraper`) が残存~~ → **§10d-3 で解消**
- ~~§7-3: 宣言されているが未使用の依存 (10 件 + dev 3 件)~~ → **§10d-4 で解消**
- ~~§5.3: Docker / devcontainer の build 検証 workflow 追加~~ → **§10d-5 で解消**
- §10d-3 追加: `S` (bandit) の採用は保留(S608 が DuckDB 内部クエリで 45 件の誤検知)

---

## 10c. フェーズ 3 実施記録 (2026-08-19)

PR #89 マージ後の追加対応。ブランチ `chore/devcontainer-drop-hardcoded-identity`。

### 10c-0. `.dockerignore` の効果が実測で確認できた

§10-1 で「Docker CLI がないため未検証」としていた項目。PR #89 のマージで `deploy.yml` が
実行され、ビルドログに転送量が出た。

```
#7 transferring context: 1.14MB 0.0s done
```

**推定 約 1MB に対し実測 1.14MB。** 変更前は約 1.7GB だったので、およそ 1,500 分の 1。

### 10c-1. `devcontainer.json` のハードコード除去 (§7-4)

**GCP 鍵** — `GOOGLE_APPLICATION_CREDENTIALS` を削除。調査の結果、**完全な死に設定**だった:

| 確認項目 | 結果 |
|---------|------|
| `google.cloud` を import するモジュール | **0 件** |
| `google-cloud-*` の依存宣言 | **なし**(`pyproject.toml` に存在しない) |
| 環境変数の参照箇所 | `devcontainer.json` の 1 行のみ |

つまり認証情報を全コンテナに設定しておきながら、誰も使っていなかった。
副作用として public リポジトリに GCP プロジェクト識別子 (`keita-masui-firstproject`) が
露出していた(ファイル名由来)。

ローカルの鍵ファイル `.secrets/keita-masui-firstproject-46adeb7731b9.json` も削除した。

> **⚠️ GCP 側の失効は未実施。** サービスアカウントキーはローカルファイルを消しても
> 失効しない。削除前に控えた識別情報:
>
> - プロジェクト: `keita-masui-firstproject`
> - サービスアカウント: `data-platformer@keita-masui-firstproject.iam.gserviceaccount.com`
> - キー ID: `46adeb7731b9124ea628a2f7d0fcf0e2ce7cf803`
>
> ```sh
> gcloud iam service-accounts keys delete 46adeb7731b9124ea628a2f7d0fcf0e2ce7cf803 \
>   --iam-account=data-platformer@keita-masui-firstproject.iam.gserviceaccount.com
> ```
>
> 手元にコピーが残っていない今、失効させない限り「誰も管理していない有効な認証情報」になる。

**git config** — `postCreateCommand` から `user.name` / `user.email` の設定を削除。

VS Code の Dev Containers 拡張がホストの `~/.gitconfig` から両者を自動コピーするため冗長だった。
根拠: コンテナ内の `~/.gitconfig` に VS Code が書き込んだ `credential.helper` 行が同居しており、
VS Code がこのファイルを管理していることが確認できる。

> **検証は再ビルド時。** 既存コンテナは `~/.gitconfig` に書き込み済みの値を使い続けるため、
> 実際に効くのは次回リビルド以降。引き継がれなかった場合は README の手順で 1 回設定すれば済む。

### 10c-2. `deploy.yml` が両ステージを publish するよう修正 (§7-5b)

`target` 未指定で最終ステージ (`dashboard`) がビルドされていた問題。
**両方とも publish 対象なので、`target: prd` 単独ではなく別パッケージ 2 本に分けた。**

| イメージ | ステージ | エントリポイント |
|---|---|---|
| `ghcr.io/keitamasui1119/voltlake/app` | `prd` | `python -m src.main` |
| `ghcr.io/keitamasui1119/voltlake/dashboard` | `dashboard` | `streamlit run src/dashboard/app.py` |

**matrix を使わず 1 ジョブ内で順にビルドしている。** `dashboard` は `FROM prd` なので、
同一ランナー上なら prd のレイヤーがビルドキャッシュに乗っており 2 本目はほぼ無料。
matrix で 2 ランナーに分けると共通の `base` → `builder` → `prd` を二重にビルドすることになり、
それを避けるにはリモートキャッシュ (`type=gha` + `setup-buildx-action`) の追加が必要になる。
1 ジョブなら構成要素を増やさずに同じ効果が得られる。

> **旧パッケージ `ghcr.io/keitamasui1119/voltlake` は以後 push されなくなる。**
> 中身は dashboard ステージなので残す価値はなく、手動削除してよい。
> CLI から消すには `gh auth refresh -h github.com -s delete:packages` でスコープ追加が必要
> (現在のトークンスコープは `gist`, `read:org`, `repo`, `workflow` のみ)。

検証: `actionlint` / `check-yaml` 通過。実ビルドは main マージ後の `deploy.yml` 実行で確認する。

---

## 10d. フェーズ 4 実施記録 (2026-08-19)

PR #90 マージ後の追加対応。ブランチは項目ごとに分ける。

### 10d-1. カバレッジ下限を `--cov-fail-under=73` で強制 (§4)

ブランチ: `chore/coverage-floor`。

現状値の実測から:

```
TOTAL   3354  880  74%
Total coverage: 73.76%
```

グローバル規約 (`~/.claude/rules/ecc/common/testing.md`) の 80% には届いていないが、
参照元 (`python-uv`) と同じく **今の実測より少し下に floor を置き、段階的に上げる**
方針を採る。**73** は 73.76% に対し 0.76 ポイントの余裕で、小さな削除や flakiness で
CI が即赤化するのを防ぎつつ、下方への逸走は止める。

**変更点**:

```yaml
# .github/workflows/ci.yml
- run: uv run pytest -m "not integration" --cov=src --cov-report=term-missing --cov-fail-under=73
```

CI 限定に置いた理由: ローカル開発中の細かい試行(小さなテスト削除やスケルトンを書きかけの状態)
まで即赤化させると生産性を落とす。CI をゲートにすれば「マージ時に守る」だけで済む。

**ラチェット計画**:

| 節目 | 下限 |
|---|---|
| 現在 | 73 |
| 未カバー領域 (§7-3 で削除しない `storage_client.py` / `manage_iceberg.py` / bronze ingestor) にテストが 1 系統でも足せた時 | 76 |
| §7-3 の未使用依存を削除して分母が縮んだ時 | 分母縮小分を実測して再設定 |
| 上記の積算で 80% を実測で超えた時 | **80** (グローバル規約準拠) |

未カバレッジの大きい上位:

| ファイル | 未カバー | 全体比 |
|---|---:|---:|
| `src/setup/manage_iceberg.py` | 41/41 (0%) | admin CLI、テスト未整備 |
| `src/common/storage_client.py` | 131/166 (21%) | boto3 wrapper、integration マーク側 |
| `src/pipeline/bronze/source_to_bronze_jepx_spot_price.py` | 66/99 (33%) | |
| bronze/supply_demand_actuals × 3 社 | 各 26/49 (47%) | |
| `src/pipeline/bronze/source_to_bronze_occto_unit_generation_actuals.py` | 39/71 (45%) | |

`storage_client.py` は `integration` マーク付きテストが本体を叩くため、`-m "not integration"`
から抜けている分がそのまま未カバーとして現れている。これは integration 実行時に別途カバレッジを
合成しない限り 21% のまま出続けるので、ラチェットの対象からは外して扱う。

### 10d-2. `apache-airflow` を任意 dependency-group へ移送 (§7-3b)

ブランチ: `chore/airflow-optional-group`。

`apache-airflow` を `[project.dependencies]` から `[dependency-groups] airflow`
へ移し、通常の `uv sync` からは外した。

**変更内容**:

```toml
# pyproject.toml
[project]
dependencies = [
    # 'apache-airflow>=3.1.8',   # 削除
    ...
]

[dependency-groups]
airflow = [
    'apache-airflow>=3.1.8',
]

[tool.uv]
default-groups = ["dev"]   # ← 追加
```

**`default-groups` の追加が本質**。uv 0.12 系は `[tool.uv.default-groups]` が
未設定だと **全ての依存グループを既定インストール対象として扱う**。単純に
airflow を新グループへ移しただけでは `uv sync` が airflow を引き続きインストールする。
`default-groups = ["dev"]` を明示することで、`airflow` グループは
`uv sync --group airflow` を書いた時だけ入るようになる。

**節約されたコスト**:

| 指標 | 変更前 | 変更後 |
|---|---:|---:|
| `uv sync` の対象パッケージ | 287 | 221 (推定、airflow 依存 66 減) |
| `uv sync --locked` のドライラン (削除される件数) | — | 66 packages |
| Renovate の毎週スキャン対象 | airflow 依存を含む | airflow は「明示グループの中」扱いで scan は続くが CI の負担は減る |

Renovate の `matchPackageNames` にある `apache-airflow` はそのまま残す。airflow を
opt-in 化しても Renovate は `[dependency-groups]` を含めて解析するため、
リリース時に PR は引き続き来る(7 日ホールド・非 automerge のまま)。

**検証**:

| 項目 | 結果 |
|---|---|
| `uv sync --locked --dry-run` で airflow が Would remove として出る | ✓ |
| `uv sync --locked` 実行後 `python -c "import airflow"` | `ModuleNotFoundError` |
| `uv run pytest -m "not integration"` | **470 passed** (前回 471。airflow の `test_installed_dependency_matches_pyproject` パラメータが 1 件消えた分) |
| カバレッジ | 73.76% (変わらず) |

**運用注意**:

- 既存の devcontainer にはまだ airflow が残っている。次回の `postCreateCommand`
  (`uv sync --locked` を含む) 実行時に自動的に除去される。手動で `uv sync --locked`
  を叩けば即時反映。
- Dockerfile は `uv sync --frozen --no-install-project --no-dev` を叩いているため、
  `airflow` は `--no-dev` によって最初から prd/dashboard イメージには入らない。
  この変更で本番イメージも同じ 66 パッケージ分軽くなる (§10c-0 の
  ビルドコンテキスト 1,500 分の 1 とは別の効果)。
- 将来 airflow のオーケストレーション実装に着手する時は
  `uv sync --group airflow` を実行。運用イメージにも含めるなら Dockerfile を
  `uv sync --frozen --no-install-project --group airflow`(または
  専用ステージ) に切り替える。

### 10d-3. `ruff.toml` を整理して 3 ルールセットを追加 (§3.2, §10b-6 の exclude 残存)

ブランチ: `chore/ruff-config-cleanup`。

#### 追加した select

各ルールの違反件数を先に計測してから採否を決めた。

| ルール | 違反件数 | 採否 | 理由 |
|---|---:|---|---|
| **PTH** (pathlib) | 5 | **採用** | 機械的に `Path` 化して終わり。誤検知なし |
| **SIM** (簡約) | 1 | **採用** | 1 件だけ、if-branch の or 統合 |
| **RUF** (ruff 固有) | 56 | **採用**(+ RUF001/003 を ignore) | 内訳: RUF100 unused noqa 23 件、RUF001/003 = 全角括弧の誤検知 29 件、実質バグ 4 件 |
| **S** (bandit) | 51 | **見送り** | S608 が DuckDB 内部クエリで 45 件。テーブル名を f-string で埋める既定パターンで、user input ではないため誤検知。全部に `# noqa: S608` を付けるとノイズ、`ignore = ["S608"]` にすると S セット全体の価値が薄まる |

RUF001 / RUF003 の追加 ignore は、ダッシュボードや Japanese の comment で全角括弧
`（` `）` や `～` を意図的に使っているため。日本語文脈では ASCII 版と混同する余地が
なく、警告するとむしろ誤変換の温床になる。

#### 実施内容

**`ruff.toml`**:

```diff
 select = [
     "E", "F", "W", "I", "B", "UP",
+    "PTH", "SIM", "RUF",
 ]
 ignore = [
     "D", "T201", "COM812", "COM819", ...,
+    "RUF001",  # 全角括弧・チルダは日本語ラベルで意図的
+    "RUF003",
 ]
 exclude = [
     ..., ".venv", ".vscode", ...,
-    "src/stg/pydev",     # 実在しない
-    "src/stg/scraper",   # 実在しない
     "*.ipynb", "notebooks",
 ]
```

**自動 fix**: 22 件が `ruff check --fix` で解消(主に RUF100 の unused noqa 除去)。
`src/common/iceberg/catalog.py`, `src/common/storage_client.py` などで
`# noqa: BLE001` が 15 箇所以上除去された。BLE001 は voltlake の select にないため、
これらの noqa は元々効いていなかった。

**手動 fix (9 件)**:

| 修正 | ルール | 件数 |
|---|---|---:|
| `os.path.exists()` → `Path().exists()` | PTH110 | 3 |
| `open()` → `Path().open()` | PTH123 | 2 |
| `os.path.basename()` → `Path().name` | PTH119 | 1 |
| 3 分岐 if 文の or 統合(88 桁 wrap) | SIM114 + E501 | 1 |
| `list = []` クラス属性 → `ClassVar[list[...]] = []` | RUF012 | 1 |
| 正規表現に `r"..."` プリフィクス追加 | RUF043 | 1 |
| 未使用のアンパック変数を `_default_file_name` へ | RUF059 | 1 |

`Path()` 化に合わせて `build_partition_spec` / `build_table_schema` のシグネチャを
`str` → `str | Path` に広げた。呼び出し側は既存のまま動く(str も Path も受ける)。

#### 検証結果

| 項目 | 結果 |
|---|---|
| `uv run ruff check src/ tests/` | All checks passed |
| `uv run ruff format --check src/ tests/` | 95 files already formatted |
| `uv run pyright` | 0 errors |
| `uv run pytest -m "not integration" --cov=src --cov-fail-under=73` | **471 passed, 73.80%** |

`Path()` 化で `os.path.exists` の失敗ブランチが薄くなり、実測カバレッジが 73.76%
→ 73.80% に微増。

#### 見送った `S` (bandit) の扱い

S608 45 件は「テーブル名やカラム名を f-string で埋める DuckDB クエリ」で、これは
`common/silver_write.py` や gold の各集計スクリプトが依拠する既定パターン。
Iceberg テーブルの完全修飾名は固定文字列 + fiscal year 等のバインド済み値で構築
されており、任意の user input が入る経路はない。

対応の候補:

- **A**: `ignore = ["S608"]` を足す — S の他ルール(S101 assert、S108 tmp、S603 subprocess)は
  取れるが、実装量に対する効果が薄い(全 51 のうち 6 しか残らない)
- **B**: `# noqa: S608` を各所に付ける — 45 箇所ノイズ
- **C**: 見送る — 今回の選択

`S101` (assert): 1 件 `bronze_to_silver_occto_unit_generation_actuals.py:72` — テスト
専用 assert ではなく本番コードなので `raise` 化すべき。
`S108` (tmp): 4 件 — 検討要。
`S603` (subprocess): 1 件 `orchestration/pl_jepx_spot_price.py:83` — 内部の DuckDB CLI
起動と思われる、要検討。

これら 6 件は独立した PR で対応した方が review が薄く済む。

### 10d-4. 未使用依存 13 件を削除 (§7-3)

ブランチ: `chore/prune-unused-dependencies`。

`src/` と `tests/` の import を全走査して 0 件だった依存を pyproject から外した。
将来使う予定のあるもの (dbt / Airflow 系 / SQLFluff 待ち / ipykernel) は保持。

#### 削除した依存

**`[project.dependencies]` (10 件)**:

| パッケージ | 削除理由 |
|---|---|
| `beautifulsoup4` | 0 imports。HTML スクレイピングは行っていない |
| `lxml` | 0 imports。`read_html` 経由の間接利用も検出されず |
| `nbformat` | 0 imports |
| `numpy` | 0 imports。streamlit / pyarrow 経由で推移的にはインストールされる |
| `pandas` | 0 imports。同上 |
| `pydantic` | 0 imports |
| `pydantic-settings` | 0 imports |
| `scikit-learn` | 0 imports |
| `Scrapy` | 0 imports。voltlake のスクレイピングは `common/http_scraper.py` の自前実装 |
| `seaborn` | 0 imports。可視化は plotly |

**`[dependency-groups] dev` (3 件)**:

| パッケージ | 削除理由 |
|---|---|
| `cookiecutter` | 0 imports。テンプレートスキャフォールディングを使う予定なし |
| `cookiecutter-data-science` | 同上 |
| `mkdocs-material` | 0 imports、かつ `mkdocs.yml` も存在しない |

#### 保持したもの

- `dbt-core`, `dbt-duckdb`, `shandy-sqlfmt[jinjafmt]` — 将来 dbt を使う予定
- `apache-airflow` — §10d-2 で任意グループ化済み
- `ipykernel` — VSCode の Jupyter kernel 統合で必要
- `duckdb` — 直接依存として明示 (§10-2)

#### インパクト

| 指標 | 変更前 | 変更後 | 削減 |
|---|---:|---:|---:|
| `uv.lock` パッケージ総数 | 287 | 230 | **-57** |
| `uv sync --locked` (推移的依存を含む削減件数) | — | 57 | — |
| pytest 総数 (パラメトリック) | 470 | 457 | -13 |
| カバレッジ | 73.80% | **73.80%** (変化なし) | - |

> 上表は §10d-2 (airflow を任意グループへ) と §10d-3 (ruff) をマージ済みの
> main を基準にした実測値。pytest 総数は 10d-2 で airflow の 1 件が既に消えているため
> 471 ではなく 470 から始まる。パッケージ総数の 287 は airflow を含む元の値
> (10d-2 は `uv.lock` からは消さず、既定インストール対象から外しただけ)。

パッケージ数削減 57 は削除したの 13 件 + それらが引き連れていた推移的依存 44 件の合計。
Renovate の scan 対象縮小 + `uv sync` の所要時間短縮 + イメージビルド時間短縮の 3 点で
継続的に効く。

`renovate.json` は削除したパッケージを一切 matchPackageNames に含めていなかったため
変更不要。


#### 検証結果

| 項目 | 結果 |
|---|---|
| `uv sync --locked` | 57 packages removed |
| `uv run ruff check src/ tests/` | All checks passed |
| `uv run pyright` | 0 errors |
| `uv run pytest -m "not integration" --cov-fail-under=73` | **457 passed, 73.80%** |

### 10d-5. PR 時に Docker image を build 検証する CI ジョブを追加 (§5.3)

ブランチ: `chore/pr-docker-build-check`。

`ci.yml` に `docker-build` ジョブを追加した。3 つの shippable ステージ
(`dev` / `prd` / `dashboard`) を matrix で並列ビルドし、**push はしない**。

#### 動機

これまでは Dockerfile の壊れは `deploy.yml` (main への push トリガー) でしか
検知されなかった。つまり main が壊れてからでないと分からない。同じチェックを
PR で走らせれば、マージ前に落とせる。

hadolint は Dockerfile の**静的解析**なので、シェル文法エラー・COPY 先の不在・
apt レイヤの実行時失敗といった「実際にビルドしないと分からない」種類の問題は
検知しない。docker-build はその補完。

#### `ci.yml` の追加

```yaml
docker-build:
  name: Build Docker image (${{ matrix.target }})
  runs-on: ubuntu-latest
  strategy:
    fail-fast: false
    matrix:
      target: [dev, prd, dashboard]
  steps:
    - uses: actions/checkout@v7
    - uses: docker/setup-buildx-action@v3
    - name: Build ${{ matrix.target }}
      uses: docker/build-push-action@v7
      with:
        context: .
        target: ${{ matrix.target }}
        push: false
        load: false
        cache-from: type=gha,scope=${{ matrix.target }}
        cache-to: type=gha,mode=max,scope=${{ matrix.target }}
```

**設計選択**:

| 論点 | 選択 | 理由 |
|---|---|---|
| ステージ | `dev` / `prd` / `dashboard` の 3 つ | shippable なもの全部。builder は prd の依存側なので単独ビルドしても意味薄い |
| ワークフロー分割 | `ci.yml` に統合 | 参照元の `docker.yml` / `devcontainer.yml` 分割は複数人チーム向け。個人開発では追跡箇所が増えるだけ |
| キャッシュ | `type=gha` を target ごとに scope 分離 | dev と prd/dashboard は共通レイヤ (`base`) を持つが、target を跨いで cache が混ざると reproducibility に影響。target ごとに独立 |
| 並列度 | matrix | fail-fast=false で 1 つ壊れても他を最後まで見る |
| `docker/setup-buildx-action` | 必要 | `type=gha` キャッシュを使うには buildx が必須 |
| push | false | PR で GHCR に触りたくない。build できることだけを検証 |
| load | false | run しないので docker daemon に image をロードする必要もない |

#### `docker-build` 単独時間の予測

初回: 3 target × 各 90-120 秒(dev は npm install があるので長め) ≒ **3-4 分**。
2 回目以降: gha cache hit で **各 30-60 秒**。

CI 全体としては `test` ジョブ (17 秒) と並列で走るのでクリティカルパスは伸びない。

#### 検証

`actionlint` 通過。実挙動は main マージ後の初回 CI で確認する。

---

## 11. 補足: 調査環境について

参照元リポジトリは以下にクローンして調査した(**削除済み 2026-08-19**)。

```
/home/vscode/.claude/jobs/4cbf3938/tmp/python-uv-reference
```

このパスは Claude Code のジョブ用一時ディレクトリであり、`/workspace` の git 管理外。ジョブ削除時に併せて消えるが、明示的に削除してもよい。

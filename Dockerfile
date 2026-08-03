ARG UV_VERSION=latest
ARG VARIANT=3.13
ARG CLAUDE_CODE_VERSION=2.1.141
ARG GEMINI_CLI_VERSION=0.42.0

# uvのバイナリを取得するステージ
FROM ghcr.io/astral-sh/uv:$UV_VERSION AS uv

# === 1. Base Stage(共通基盤) ===
FROM python:$VARIANT-slim AS base
WORKDIR /app
COPY --from=uv /uv /uvx /bin/
ENV PYTHONDONTWRITEBYTECODE=True \
    PYTHONUNBUFFERED=True \
    UV_LINK_MODE=copy \
    PATH="/app/.venv/bin:$PATH"

# hadolint ignore=DL3008
RUN apt-get update \
    && apt-get install -y --no-install-recommends sqlite3 zsh \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# === 2. Dev Stage(DevContainer用) ===
FROM base AS dev
ARG CLAUDE_CODE_VERSION
ARG GEMINI_CLI_VERSION
# DevContainerが要求する標準ユーザー(vscode)と必須ツールを追加
# hadolint ignore=DL3008
RUN useradd -m -s /bin/zsh -u 1000 vscode \
    && apt-get update \
    && apt-get install -y --no-install-recommends git curl ca-certificates \
    && curl -fsSL https://cli.github.com/packages/githubcli-archive-keyring.gpg \
        -o /usr/share/keyrings/githubcli-archive-keyring.gpg \
    && chmod go+r /usr/share/keyrings/githubcli-archive-keyring.gpg \
    && echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/githubcli-archive-keyring.gpg] https://cli.github.com/packages stable main" \
        > /etc/apt/sources.list.d/github-cli.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends gh \
    && curl -fsSL https://deb.nodesource.com/setup_22.x -o /tmp/setup_node.sh \
    && bash /tmp/setup_node.sh \
    && rm /tmp/setup_node.sh \
    && apt-get install -y --no-install-recommends nodejs \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# CLI群はvscode所有のprefixへ導入し、CLI自身の自動アップデートを可能にする
# (root所有の/usr/lib/node_modulesだとvscodeユーザーが書き込めずupdateが失敗する)
ENV NPM_CONFIG_PREFIX=/home/vscode/.npm-global
ENV PATH="/home/vscode/.npm-global/bin:/home/vscode/.local/bin:${PATH}"
USER 1000
RUN npm install -g @anthropic-ai/claude-code@${CLAUDE_CODE_VERSION} \
    && npm install -g @google/gemini-cli@${GEMINI_CLI_VERSION} \
    && npm cache clean --force
WORKDIR /workspace
CMD ["sleep", "infinity"]

# === 3. Builder Stage(Prd依存解決用) ===
FROM base AS builder
COPY pyproject.toml uv.lock ./
# 本番に必要なパッケージのみをインストール(devグループなどを除外)
RUN uv sync --frozen --no-install-project --no-dev

# === 4. Prd Stage(本番稼働用) ===
FROM base AS prd
RUN useradd -m -s /bin/zsh -u 1000 appuser
# builderから完成したクリーンな仮想環境のみをコピー
COPY --from=builder /app/.venv /app/.venv
# ソースコードのコピー
COPY --chown=appuser:appuser src ./src
USER 1000
CMD ["python", "-m", "src.main"]

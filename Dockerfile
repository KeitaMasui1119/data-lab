ARG UV_VERSION=latest
ARG VARIANT=3.12

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
    && apt-get install -y --no-install-recommends sqlite3 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# === 2. Dev Stage(DevContainer用) ===
FROM base AS dev
# DevContainerが要求する標準ユーザー(vscode)と必須ツールを追加
# hadolint ignore=DL3008
RUN useradd -m -s /bin/bash -u 1000 vscode \
    && apt-get update \
    && apt-get install -y --no-install-recommends git curl ca-certificates \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
USER vscode
WORKDIR /workspace
CMD ["sleep", "infinity"]

# === 3. Builder Stage(Prd依存解決用) ===
FROM base AS builder
COPY pyproject.toml uv.lock ./
# 本番に必要なパッケージのみをインストール(devグループなどを除外)
RUN uv sync --frozen --no-install-project --no-dev

# === 4. Prd Stage(本番稼働用) ===
FROM base AS prd
RUN useradd -m -s /bin/bash -u 1000 appuser
# builderから完成したクリーンな仮想環境のみをコピー
COPY --from=builder /app/.venv /app/.venv
# ソースコードのコピー
COPY --chown=appuser:appuser src ./src
USER appuser
CMD ["python", "-m", "src.main"]

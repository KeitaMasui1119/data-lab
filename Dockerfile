ARG UV_VERSION=latest
ARG VARIANT=3.12

# uvのバイナリを取得するステージ
FROM ghcr.io/astral-sh/uv:$UV_VERSION AS uv

# 本番実行用のステージ
FROM python:$VARIANT-slim

WORKDIR /app

RUN useradd -m appuser

COPY --from=uv /uv /uvx /bin/
COPY pyproject.toml uv.lock ./

ENV PYTHONDONTWRITEBYTECODE=True \
    PYTHONUNBUFFERED=True \
    UV_LINK_MODE=copy \
    # venvのパスをPATHに追加
    PATH="/app/.venv/bin:$PATH"

# hadolint ignore=DL3008
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    sqlite3 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

RUN uv sync --frozen --no-install-project --no-dev

# アプリケーションコードのコピー
COPY --chown=appuser:appuser src ./src

# プロジェクト自体のインストール
RUN uv sync --frozen --no-dev

# セキュリティ設定(非rootユーザーで実行)
USER appuser

CMD ["python", "src/main.py"]

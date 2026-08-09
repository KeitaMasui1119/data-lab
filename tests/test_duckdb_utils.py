"""Unit tests for the shared DuckDB connection helper.

Extracted from bronze_to_silver_jepx_spot_price.py (Phase 4 Step 4-0 of
docs/tasks/plan_occto_pipeline.md) so both JEPX and OCCTO's bronze-to-silver
modules can share the same S3/Iceberg extension setup.
"""

from __future__ import annotations

import duckdb
import pytest

from common.duckdb_utils import _split_endpoint, create_duckdb_connection


def test_split_endpoint_returns_netloc_and_tls_flag_for_https_url():
    endpoint, use_ssl = _split_endpoint("https://rustfs.internal:9000")

    assert endpoint == "rustfs.internal:9000"
    assert use_ssl is True


def test_split_endpoint_returns_netloc_and_no_tls_for_http_url():
    endpoint, use_ssl = _split_endpoint("http://rustfs:9000")

    assert endpoint == "rustfs:9000"
    assert use_ssl is False


def test_split_endpoint_returns_bare_value_when_no_scheme():
    endpoint, use_ssl = _split_endpoint("rustfs:9000")

    assert endpoint == "rustfs:9000"
    assert use_ssl is False


def test_create_duckdb_connection_skips_s3_setup_when_disabled(monkeypatch):
    monkeypatch.delenv("AWS_ENDPOINT_URL", raising=False)

    conn = create_duckdb_connection(configure_s3=False)

    assert isinstance(conn, duckdb.DuckDBPyConnection)
    conn.close()


def test_create_duckdb_connection_raises_without_endpoint_url(monkeypatch):
    monkeypatch.delenv("AWS_ENDPOINT_URL", raising=False)

    with pytest.raises(ValueError, match="AWS_ENDPOINT_URL"):
        create_duckdb_connection(configure_s3=True)


def test_create_duckdb_connection_raises_without_credentials(monkeypatch):
    monkeypatch.setenv("AWS_ENDPOINT_URL", "http://rustfs:9000")
    monkeypatch.delenv("AWS_ACCESS_KEY_ID", raising=False)
    monkeypatch.delenv("AWS_SECRET_ACCESS_KEY", raising=False)

    with pytest.raises(ValueError, match="AWS_ACCESS_KEY_ID"):
        create_duckdb_connection(configure_s3=True)

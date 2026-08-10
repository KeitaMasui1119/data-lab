"""Shared DuckDB connection helper for bronze-to-silver transformations.

Both JEPX and OCCTO scan their bronze Iceberg table directly from DuckDB via
the httpfs/iceberg extensions, so the S3 endpoint/credential wiring is
identical across datasets and lives here once instead of per-module.
"""

from __future__ import annotations

import os
from urllib.parse import urlparse

import duckdb


def _split_endpoint(endpoint_url: str) -> tuple[str, bool]:
    """Split an endpoint URL into a DuckDB host:port and a TLS flag."""
    parsed = urlparse(endpoint_url)
    if parsed.netloc:
        return parsed.netloc, parsed.scheme == "https"
    return endpoint_url, False


def create_duckdb_connection(*, configure_s3: bool = True) -> duckdb.DuckDBPyConnection:
    """Open a DuckDB connection prepared for reading a bronze Iceberg table.

    ``icu`` is statically linked into DuckDB and auto-loads, so no extension
    install is needed for ``AT TIME ZONE``. Pass ``configure_s3=False`` to
    read from a locally registered relation instead of object storage.
    """
    conn = duckdb.connect()
    if not configure_s3:
        return conn

    conn.execute("INSTALL httpfs; LOAD httpfs;")
    conn.execute("INSTALL iceberg; LOAD iceberg;")
    conn.execute("SET unsafe_enable_version_guessing = true;")

    endpoint_url = os.environ.get("AWS_ENDPOINT_URL")
    if not endpoint_url:
        raise ValueError("AWS_ENDPOINT_URL is required to read the bronze table")
    access_key = os.environ.get("AWS_ACCESS_KEY_ID")
    secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
    if not access_key or not secret_key:
        raise ValueError(
            "AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY are required "
            "to read the bronze table"
        )

    endpoint, use_ssl = _split_endpoint(endpoint_url)
    conn.execute("SET s3_endpoint = ?;", [endpoint])
    conn.execute("SET s3_use_ssl = ?;", [use_ssl])
    conn.execute("SET s3_url_style = 'path';")
    conn.execute("SET s3_region = ?;", [os.environ.get("AWS_REGION", "us-east-1")])
    conn.execute("SET s3_access_key_id = ?;", [access_key])
    conn.execute("SET s3_secret_access_key = ?;", [secret_key])
    return conn

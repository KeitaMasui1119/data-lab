"""Reusable object-storage IO helpers for raw data processing."""

from __future__ import annotations

import gzip

from common.storage_client import RustFSClient


def read_object_text(
    client: RustFSClient,
    bucket_name: str,
    object_key: str,
    encoding: str,
) -> str:
    """Read object bytes and decode text, handling .gz files automatically."""
    payload = client.get_object(bucket_name=bucket_name, object_name=object_key)
    raw_bytes = gzip.decompress(payload) if object_key.endswith(".gz") else payload
    return raw_bytes.decode(encoding)

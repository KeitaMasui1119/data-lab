"""Compatibility shim for storage client imports.

The implementation has been moved to `common.storage_client`.
"""

from common.storage_client import RustFSClient, RustFSConfig

__all__ = ["RustFSClient", "RustFSConfig"]

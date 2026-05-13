"""Compatibility shim for storage client imports.

The implementation has been moved to `common.module.storage_client`.
"""

from common.module.storage_client import RustFSClient, RustFSConfig

__all__ = ["RustFSClient", "RustFSConfig"]

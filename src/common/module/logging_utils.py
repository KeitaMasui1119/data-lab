"""Logging helpers shared across the codebase."""

from __future__ import annotations

import logging

DEFAULT_LOG_FORMAT = "%(asctime)s | %(levelname)s | %(message)s"


def configure_logging(level: int = logging.INFO) -> None:
    """Configure root logging once with the project default format."""
    logging.basicConfig(level=level, format=DEFAULT_LOG_FORMAT)


def get_logger(name: str) -> logging.Logger:
    """Return a logger with the given module name."""
    return logging.getLogger(name)

"""Compatibility shim for scraper base classes.

The implementation has been moved to `common.module.http_scraper`.
"""

from common.module.http_scraper import BaseHttpScraper, RequestSpec

__all__ = ["BaseHttpScraper", "RequestSpec"]

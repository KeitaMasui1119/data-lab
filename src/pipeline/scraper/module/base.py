"""Compatibility shim for scraper base classes.

The implementation has been moved to `common.http_scraper`.
"""

from common.http_scraper import BaseHttpScraper, RequestSpec

__all__ = ["BaseHttpScraper", "RequestSpec"]

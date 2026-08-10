"""Shared HTTP scraper primitives for pipeline modules."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import requests


@dataclass(frozen=True)
class RequestSpec:
    """HTTP request definition for a scraper run."""

    method: str
    url: str
    headers: dict[str, str] | None = None
    params: dict[str, Any] | None = None
    data: dict[str, Any] | None = None


class BaseHttpScraper(ABC):
    """Abstract base class for HTTP-based scrapers."""

    def __init__(
        self,
        *,
        timeout_seconds: int = 30,
        session: requests.Session | None = None,
    ) -> None:
        self.timeout_seconds = timeout_seconds
        self.session = session or requests.Session()

    @abstractmethod
    def build_request(self, target_at: datetime) -> RequestSpec:
        """Build an HTTP request specification for the target time."""

    def prepare(self, target_at: datetime) -> None:
        """Perform preparatory requests before build_request() is called.

        Default is a no-op. Scrapers that must establish session state
        (cookies, disclaimer agreement, form tokens) before the main
        request can be built should override this and issue requests
        through self.session; build_request() then only needs to
        construct the final RequestSpec.
        """
        return None

    def _execute(self, spec: RequestSpec) -> requests.Response:
        """Execute a pre-built RequestSpec and return the raw response."""
        response = self.session.request(
            method=spec.method,
            url=spec.url,
            headers=spec.headers,
            params=spec.params,
            data=spec.data,
            timeout=self.timeout_seconds,
        )
        response.raise_for_status()
        return response

    def fetch_response(self, target_at: datetime) -> requests.Response:
        """Execute request and return the raw response."""
        self.prepare(target_at)
        spec = self.build_request(target_at)
        return self._execute(spec)

    def fetch(self, target_at: datetime) -> bytes:
        """Execute request and return response bytes."""
        return self.fetch_response(target_at).content

    def close(self) -> None:
        """Close the underlying HTTP session."""
        self.session.close()

    def __enter__(self) -> BaseHttpScraper:
        """Enable context manager usage."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Close session when exiting context manager."""
        self.close()

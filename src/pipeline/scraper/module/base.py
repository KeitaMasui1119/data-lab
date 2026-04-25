from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import requests


@dataclass(frozen=True)
class RequestSpec:
    method: str
    url: str
    headers: dict[str, str] | None = None
    params: dict[str, Any] | None = None
    data: dict[str, Any] | None = None


class BaseHttpScraper(ABC):
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

    def fetch(self, target_at: datetime) -> bytes:
        spec = self.build_request(target_at)
        response = self.session.request(
            method=spec.method,
            url=spec.url,
            headers=spec.headers,
            params=spec.params,
            data=spec.data,
            timeout=self.timeout_seconds,
        )
        response.raise_for_status()
        return response.content

    def close(self) -> None:
        self.session.close()

    def __enter__(self) -> BaseHttpScraper:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

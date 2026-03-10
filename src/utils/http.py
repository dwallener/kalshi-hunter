from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any

import requests


DEFAULT_TIMEOUT_SECONDS = 20


@dataclass
class HttpClient:
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS
    user_agent: str = "kalshi-hunter/0.1"
    max_retries: int = 3
    backoff_seconds: float = 1.0

    def __post_init__(self) -> None:
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": self.user_agent})

    def get_json(self, url: str, params: dict[str, Any] | None = None) -> Any:
        last_error: Exception | None = None
        for attempt in range(self.max_retries + 1):
            response = self.session.get(url, params=params, timeout=self.timeout_seconds)
            if response.status_code != 429:
                response.raise_for_status()
                return response.json()
            retry_after = response.headers.get("Retry-After")
            sleep_for = float(retry_after) if retry_after else self.backoff_seconds * (attempt + 1)
            time.sleep(sleep_for)
            last_error = requests.HTTPError(
                f"429 Too Many Requests for url: {response.url}",
                response=response,
            )
        if last_error is not None:
            raise last_error
        raise RuntimeError("HTTP request failed without a response")

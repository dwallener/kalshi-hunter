from __future__ import annotations

import re
from typing import Any

from src.utils.http import HttpClient


def _query_tokens(value: str | None) -> list[str]:
    if not value:
        return []
    return [token for token in re.findall(r"[a-z0-9]+", value.lower()) if token]


def _matches_query(haystack: str, query: str | None) -> bool:
    tokens = _query_tokens(query)
    if not tokens:
        return True
    return all(token in haystack for token in tokens)


def _metadata_haystack(market: dict[str, Any]) -> str:
    tag_values = market.get("tags") or market.get("tag") or []
    if isinstance(tag_values, list):
        tag_text = " ".join(str(tag) for tag in tag_values if tag)
    else:
        tag_text = str(tag_values)
    fields = [
        market.get("question"),
        market.get("title"),
        market.get("description"),
        market.get("category"),
        market.get("slug"),
        market.get("market_slug"),
        market.get("groupTitle"),
        market.get("groupItemTitle"),
        tag_text,
    ]
    return " ".join(str(value) for value in fields if value).lower()


def _has_category_metadata(market: dict[str, Any]) -> bool:
    if market.get("category"):
        return True
    tags = market.get("tags") or market.get("tag")
    if isinstance(tags, list):
        return any(tag for tag in tags)
    return bool(tags)


CATEGORY_DISCOVERY_HINTS: dict[str, tuple[str, ...]] = {
    "politics": (
        "president",
        "prime minister",
        "leader",
        "election",
        "trump",
        "biden",
        "taiwan",
        "china",
        "ukraine",
        "russia",
        "israel",
        "government",
        "senate",
        "minister",
    ),
}


def _matches_category_request(market: dict[str, Any], haystack: str, category: str | None) -> bool:
    category_tokens = _query_tokens(category)
    if not category_tokens:
        return True
    if _has_category_metadata(market):
        return all(token in haystack for token in category_tokens)
    requested = (category or "").lower().strip()
    hints = CATEGORY_DISCOVERY_HINTS.get(requested)
    if hints:
        return any(hint in haystack for hint in hints)
    return all(token in haystack for token in category_tokens)


class PolymarketClient:
    def __init__(
        self,
        gamma_base: str = "https://gamma-api.polymarket.com",
        clob_base: str = "https://clob.polymarket.com",
        timeout_seconds: int = 20,
    ) -> None:
        self.gamma_base = gamma_base.rstrip("/")
        self.clob_base = clob_base.rstrip("/")
        self.http = HttpClient(timeout_seconds=timeout_seconds)

    def list_markets(
        self,
        *,
        query: str | None = None,
        category: str | None = None,
        limit: int = 50,
        active: bool = True,
    ) -> list[dict[str, Any]]:
        filtered: list[dict[str, Any]] = []
        page_size = min(max(min(limit, 100), 50), 100)
        max_pages = 30
        offset = 0

        for _ in range(max_pages):
            params: dict[str, Any] = {"limit": page_size, "offset": offset}
            if active:
                params["active"] = "true"
                params["closed"] = "false"
            markets = self.http.get_json(f"{self.gamma_base}/markets", params=params)
            if not markets:
                break
            for market in markets:
                haystack = _metadata_haystack(market)
                if not _matches_query(haystack, query):
                    continue
                if not _matches_category_request(market, haystack, category):
                    continue
                filtered.append(market)
                if len(filtered) >= limit:
                    return filtered
            offset += len(markets)
            if len(markets) < page_size:
                break
        return filtered

    def get_price_history(
        self,
        token_id: str,
        *,
        interval: str = "1d",
        fidelity: int = 60,
        start_ts: int | None = None,
        end_ts: int | None = None,
    ) -> Any:
        params: dict[str, Any] = {"market": token_id, "interval": interval, "fidelity": fidelity}
        if start_ts is not None:
            params["startTs"] = start_ts
        if end_ts is not None:
            params["endTs"] = end_ts
        return self.http.get_json(f"{self.clob_base}/prices-history", params=params)

    def get_order_book(self, token_id: str) -> Any:
        return self.http.get_json(f"{self.clob_base}/book", params={"token_id": token_id})

    def get_last_trade_price(self, token_id: str) -> Any:
        return self.http.get_json(f"{self.clob_base}/last-trade-price", params={"token_id": token_id})

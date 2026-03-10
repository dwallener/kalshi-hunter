from __future__ import annotations

import re
from typing import Any
from urllib.parse import urlparse

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


NOISE_TICKER_PATTERNS = (
    "KXMVE",
    "SPORTSMULTIGAME",
    "CROSSCATEGORY",
)


def _looks_like_bundle_market(market: dict[str, Any]) -> bool:
    ticker = str(market.get("ticker") or "").upper()
    title = str(market.get("title") or "").lower()

    if any(pattern in ticker for pattern in NOISE_TICKER_PATTERNS):
        return True

    comma_count = title.count(",")
    repeated_binary_prefixes = title.count("yes ") + title.count("no ")
    if comma_count >= 3 and repeated_binary_prefixes >= 4:
        return True

    sports_terms = (
        "points",
        "goals",
        "rebounds",
        "assists",
        "wins by over",
        "over ",
        "under ",
    )
    if repeated_binary_prefixes >= 2 and any(term in title for term in sports_terms):
        return True

    return False


class KalshiClient:
    def __init__(
        self,
        base_url: str = "https://api.elections.kalshi.com/trade-api/v2",
        timeout_seconds: int = 20,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.http = HttpClient(timeout_seconds=timeout_seconds)

    def list_events(
        self,
        *,
        query: str | None = None,
        category: str | None = None,
        limit: int = 50,
        status: str = "open",
        with_nested_markets: bool = True,
        exclude_noise: bool = True,
    ) -> list[dict[str, Any]]:
        filtered: list[dict[str, Any]] = []
        category_tokens = _query_tokens(category)
        cursor: str | None = None
        page_size = min(max(min(limit, 100), 50), 100)
        max_pages = 20
        for _ in range(max_pages):
            params: dict[str, Any] = {
                "limit": page_size,
                "status": status,
                "with_nested_markets": str(with_nested_markets).lower(),
            }
            if cursor:
                params["cursor"] = cursor
            payload = self.http.get_json(f"{self.base_url}/events", params=params)
            events = payload.get("events", [])
            if not events:
                break
            for event in events:
                event_haystack = " ".join(
                    str(value)
                    for value in [
                        event.get("title"),
                        event.get("sub_title"),
                        event.get("category"),
                        event.get("series_ticker"),
                        event.get("event_ticker"),
                    ]
                    if value
                ).lower()
                event_category = str(event.get("category") or "").lower()
                if not _matches_query(event_haystack, query):
                    continue
                if category_tokens and not any(token in event_category for token in category_tokens):
                    continue
                event_markets = event.get("markets") or []
                for market in event_markets:
                    merged_market = dict(market)
                    merged_market.setdefault("event_ticker", event.get("event_ticker"))
                    merged_market.setdefault("category", event.get("category"))
                    merged_market.setdefault("series_ticker", event.get("series_ticker"))
                    merged_market.setdefault("event_title", event.get("title"))
                    merged_market.setdefault("event_sub_title", event.get("sub_title"))
                    if exclude_noise and _looks_like_bundle_market(merged_market):
                        continue
                    filtered.append(merged_market)
                    if len(filtered) >= limit:
                        return filtered
            cursor = payload.get("cursor")
            if not cursor:
                break
        return filtered

    def list_markets(
        self,
        *,
        query: str | None = None,
        category: str | None = None,
        limit: int = 50,
        status: str = "open",
        exclude_noise: bool = True,
    ) -> list[dict[str, Any]]:
        if query or category:
            return self.list_events(
                query=query,
                category=category,
                limit=limit,
                status=status,
                with_nested_markets=True,
                exclude_noise=exclude_noise,
            )

        filtered: list[dict[str, Any]] = []
        category_tokens = _query_tokens(category)
        cursor: str | None = None
        page_size = min(max(min(limit, 100), 50), 100)
        max_pages = 20
        for _ in range(max_pages):
            params: dict[str, Any] = {"limit": page_size, "status": status}
            if cursor:
                params["cursor"] = cursor
            payload = self.http.get_json(f"{self.base_url}/markets", params=params)
            markets = payload.get("markets", [])
            if not markets:
                break
            for market in markets:
                haystack = " ".join(
                    str(value)
                    for value in [
                        market.get("title"),
                        market.get("subtitle"),
                        market.get("ticker"),
                        market.get("category"),
                        market.get("series_category"),
                    ]
                    if value
                ).lower()
                if exclude_noise and _looks_like_bundle_market(market):
                    continue
                market_category = " ".join(
                    str(value).lower()
                    for value in [market.get("category"), market.get("series_category")]
                    if value
                )
                if not _matches_query(haystack, query):
                    continue
                if category_tokens and not any(token in market_category for token in category_tokens):
                    continue
                filtered.append(market)
                if len(filtered) >= limit:
                    return filtered
            cursor = payload.get("cursor")
            if not cursor:
                break
        return filtered

    def get_market_details(self, ticker: str) -> Any:
        payload = self.http.get_json(f"{self.base_url}/markets/{ticker}")
        return payload.get("market", payload)

    def list_markets_for_series(self, series_ticker: str, *, limit: int = 500) -> list[dict[str, Any]]:
        filtered: list[dict[str, Any]] = []
        cursor: str | None = None
        page_size = min(max(min(limit, 100), 50), 100)
        max_pages = max(1, (limit + page_size - 1) // page_size + 2)
        for _ in range(max_pages):
            params: dict[str, Any] = {
                "limit": page_size,
                "series_ticker": series_ticker.upper(),
            }
            if cursor:
                params["cursor"] = cursor
            payload = self.http.get_json(f"{self.base_url}/markets", params=params)
            markets = payload.get("markets", [])
            if not markets:
                break
            filtered.extend(markets)
            if len(filtered) >= limit:
                return filtered[:limit]
            cursor = payload.get("cursor")
            if not cursor:
                break
        return filtered

    def find_market_by_url(self, market_url: str, *, status: str | None = None) -> dict[str, Any] | None:
        target = market_url.rstrip("/").lower()
        path_parts = [part for part in urlparse(target).path.strip("/").split("/") if part]
        target_series = path_parts[1] if len(path_parts) >= 2 else None
        target_event = path_parts[2] if len(path_parts) >= 3 else None
        target_ticker = path_parts[-1] if path_parts else None
        cursor: str | None = None
        page_size = 100
        max_pages = 30
        for _ in range(max_pages):
            params: dict[str, Any] = {
                "limit": page_size,
                "with_nested_markets": "true",
            }
            if status:
                params["status"] = status
            if cursor:
                params["cursor"] = cursor
            payload = self.http.get_json(f"{self.base_url}/events", params=params)
            events = payload.get("events", [])
            if not events:
                break
            for event in events:
                for market in event.get("markets") or []:
                    ticker = str(market.get("ticker") or "").strip()
                    candidate_urls = {
                        f"https://kalshi.com/markets/{ticker}".rstrip("/").lower(),
                    }
                    series_ticker = event.get("series_ticker")
                    event_ticker = event.get("event_ticker")
                    if series_ticker and event_ticker and ticker:
                        candidate_urls.add(
                            f"https://kalshi.com/markets/{series_ticker.lower()}/{event_ticker.lower()}/{ticker.lower()}".rstrip("/").lower()
                        )
                    ticker_match = target_ticker and ticker.lower() == target_ticker.lower()
                    series_match = not target_series or (series_ticker and series_ticker.lower() == target_series.lower())
                    event_match = not target_event or (event_ticker and event_ticker.lower() == target_event.lower())
                    if target in candidate_urls or (ticker_match and series_match) or (ticker_match and event_match):
                        merged_market = dict(market)
                        merged_market.setdefault("event_ticker", event_ticker)
                        merged_market.setdefault("category", event.get("category"))
                        merged_market.setdefault("series_ticker", series_ticker)
                        merged_market.setdefault("event_title", event.get("title"))
                        merged_market.setdefault("event_sub_title", event.get("sub_title"))
                        return merged_market
            cursor = payload.get("cursor")
            if not cursor:
                break
        return None

    def get_order_book(self, ticker: str) -> Any:
        payload = self.http.get_json(f"{self.base_url}/markets/{ticker}/orderbook")
        return payload.get("orderbook", payload)

    def get_trades(
        self,
        *,
        ticker: str | None = None,
        limit: int = 100,
        min_ts: int | None = None,
        max_ts: int | None = None,
        historical: bool = False,
    ) -> Any:
        params: dict[str, Any] = {"limit": min(limit, 1000)}
        if ticker:
            params["ticker"] = ticker
        if min_ts is not None:
            params["min_ts"] = min_ts
        if max_ts is not None:
            params["max_ts"] = max_ts
        path = "/historical/trades" if historical else "/markets/trades"
        return self.http.get_json(f"{self.base_url}{path}", params=params)

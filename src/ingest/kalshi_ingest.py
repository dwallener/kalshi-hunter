from __future__ import annotations

from pathlib import Path

from src.clients.kalshi_client import KalshiClient
from src.normalize.kalshi_normalize import normalize_kalshi_market
from src.normalize.schema import NormalizedMarket
from src.utils.io import ensure_dir, write_csv, write_json, write_jsonl
from src.utils.topic import category_matches_request
from src.utils.time import utc_now


def ingest_kalshi_markets(
    *,
    query: str | None,
    category: str | None,
    limit: int,
    raw_root: str,
    normalized_root: str,
    client: KalshiClient | None = None,
) -> list[NormalizedMarket]:
    venue_client = client or KalshiClient()
    fetched_at = utc_now()
    markets = venue_client.list_markets(query=query, category=category, limit=limit)
    hydrated_markets: list[dict] = []
    for market in markets:
        ticker = str(market["ticker"])
        try:
            details = venue_client.get_market_details(ticker)
            details["orderbook"] = venue_client.get_order_book(ticker)
            details["recent_trades"] = venue_client.get_trades(ticker=ticker, limit=25)
            hydrated_markets.append(details)
        except Exception:
            hydrated_markets.append(market)

    stamp = fetched_at.strftime("%Y%m%dT%H%M%SZ")
    raw_path = Path(raw_root) / "kalshi" / f"kalshi_markets_{stamp}.json"
    write_json(raw_path, hydrated_markets)

    normalized = []
    for market in hydrated_markets:
        normalized_market = normalize_kalshi_market(
            raw_market=market,
            fetched_at=fetched_at,
            raw_payload_path=str(raw_path),
        )
        if not category_matches_request(normalized_market.category, category):
            continue
        normalized.append(normalized_market)
    ensure_dir(normalized_root)
    jsonl_path = Path(normalized_root) / f"kalshi_markets_{stamp}.jsonl"
    csv_path = Path(normalized_root) / f"kalshi_markets_{stamp}.csv"
    rows = [market.model_dump(mode="json") for market in normalized]
    write_jsonl(jsonl_path, rows)
    write_csv(csv_path, rows, columns=list(NormalizedMarket.model_fields.keys()))
    return normalized

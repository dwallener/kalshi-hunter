from __future__ import annotations

from pathlib import Path

from src.clients.polymarket_client import PolymarketClient
from src.normalize.polymarket_normalize import normalize_polymarket_market
from src.normalize.schema import NormalizedMarket
from src.utils.io import ensure_dir, write_csv, write_json, write_jsonl
from src.utils.topic import category_matches_request
from src.utils.time import utc_now


def ingest_polymarket_markets(
    *,
    query: str | None,
    category: str | None,
    limit: int,
    raw_root: str,
    normalized_root: str,
    client: PolymarketClient | None = None,
) -> list[NormalizedMarket]:
    venue_client = client or PolymarketClient()
    fetched_at = utc_now()
    markets = venue_client.list_markets(query=query, category=category, limit=limit)
    stamp = fetched_at.strftime("%Y%m%dT%H%M%SZ")
    raw_path = Path(raw_root) / "polymarket" / f"polymarket_markets_{stamp}.json"
    write_json(raw_path, markets)

    normalized: list[NormalizedMarket] = []
    for market in markets:
        yes_token_id = None
        for token in market.get("tokens") or []:
            if str(token.get("outcome", "")).lower() == "yes":
                yes_token_id = token.get("token_id") or token.get("tokenId")
                break
        if yes_token_id:
            try:
                market["last_trade_price"] = venue_client.get_last_trade_price(str(yes_token_id))
                market["order_book"] = venue_client.get_order_book(str(yes_token_id))
            except Exception:
                market["last_trade_price"] = None
                market["order_book"] = None
        normalized_market = normalize_polymarket_market(
            raw_market=market,
            fetched_at=fetched_at,
            raw_payload_path=str(raw_path),
        )
        if not category_matches_request(normalized_market.category, category):
            continue
        normalized.append(normalized_market)

    ensure_dir(normalized_root)
    jsonl_path = Path(normalized_root) / f"polymarket_markets_{stamp}.jsonl"
    csv_path = Path(normalized_root) / f"polymarket_markets_{stamp}.csv"
    rows = [market.model_dump(mode="json") for market in normalized]
    write_jsonl(jsonl_path, rows)
    write_csv(csv_path, rows, columns=list(NormalizedMarket.model_fields.keys()))
    return normalized

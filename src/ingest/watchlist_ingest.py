from __future__ import annotations

from pathlib import Path
from urllib.parse import urlparse

import yaml

from src.clients.kalshi_client import KalshiClient
from src.clients.polymarket_client import PolymarketClient
from src.normalize.kalshi_normalize import normalize_kalshi_market
from src.normalize.polymarket_normalize import normalize_polymarket_market
from src.normalize.schema import NormalizedMarket
from src.utils.io import ensure_dir, write_csv, write_json, write_jsonl
from src.utils.time import utc_now


def extract_kalshi_ticker(value: str) -> str:
    parsed = urlparse(value)
    path = parsed.path.strip("/")
    if not path:
        return value.strip().upper()
    return path.split("/")[-1].upper()


def extract_polymarket_slug(value: str) -> str:
    parsed = urlparse(value)
    path = parsed.path.strip("/")
    if not path:
        return value.strip()
    parts = path.split("/")
    if "event" in parts:
        index = parts.index("event")
        if index + 1 < len(parts):
            return parts[index + 1]
    return parts[-1]


def extract_kalshi_series_ticker(value: str) -> str | None:
    parsed = urlparse(value)
    parts = [part for part in parsed.path.strip("/").split("/") if part]
    if len(parts) >= 2 and parts[0] == "markets":
        return parts[1].upper()
    return None


def _pick_numeric(value) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _score_kalshi_market_candidate(market: dict, target: str) -> tuple[float, float]:
    event_ticker = str(market.get("event_ticker") or "").upper()
    ticker = str(market.get("ticker") or "").upper()
    volume = _pick_numeric(market.get("volume")) or 0.0
    if target == ticker:
        return (10.0, volume)
    if target and event_ticker == target:
        return (9.0, volume)
    if target and target in ticker:
        return (7.0, volume)
    return (0.0, volume)


def _pick_kalshi_event_representative(markets: list[dict]) -> dict:
    def rank_key(item: dict) -> tuple[float, float]:
        price = _pick_numeric(item.get("last_price_dollars"))
        if price is None:
            cents = _pick_numeric(item.get("last_price"))
            price = (cents / 100.0) if cents is not None else 0.0
        volume = _pick_numeric(item.get("volume")) or 0.0
        return (price, volume)

    return max(markets, key=rank_key)


def _pick_kalshi_candidate_from_url(
    *,
    pair: dict,
    ref_text: str,
    kalshi: KalshiClient,
) -> dict | None:
    ticker = extract_kalshi_ticker(ref_text)
    market = None
    try:
        market = kalshi.get_market_details(ticker)
    except Exception:
        market = None
    if market is not None:
        market["market_url"] = ref_text.rstrip("/")
        return market

    series_ticker = extract_kalshi_series_ticker(ref_text)
    if not series_ticker:
        return None
    candidates = kalshi.list_markets_for_series(series_ticker, limit=500)
    if not candidates:
        return None

    event_matches = [item for item in candidates if str(item.get("event_ticker") or "").upper() == ticker]
    if event_matches:
        best = _pick_kalshi_event_representative(event_matches)
        synthetic = dict(best)
        synthetic["market_url"] = ref_text.rstrip("/")
        synthetic["title"] = pair.get("label") or synthetic.get("title")
        synthetic["subtitle"] = best.get("title")
        return synthetic

    best = max(candidates, key=lambda item: _score_kalshi_market_candidate(item, ticker))
    if _score_kalshi_market_candidate(best, ticker)[0] <= 0:
        return None

    synthetic = dict(best)
    synthetic["market_url"] = ref_text.rstrip("/")
    synthetic["title"] = pair.get("label") or synthetic.get("title")
    synthetic["subtitle"] = synthetic.get("subtitle") or best.get("title")
    return synthetic


def _pick_polymarket_event_market(event_payload: dict) -> dict | None:
    markets = event_payload.get("markets") or []
    if not markets:
        return None

    def rank_key(item: dict) -> tuple[float, float]:
        outcome_prices = item.get("outcomePrices")
        price = None
        if outcome_prices not in (None, ""):
            try:
                import json

                parsed = json.loads(outcome_prices) if isinstance(outcome_prices, str) else outcome_prices
                if isinstance(parsed, list) and parsed:
                    price = _pick_numeric(parsed[0])
            except Exception:
                price = None
        if price is None:
            price = _pick_numeric(item.get("lastTradePrice")) or 0.0
        liquidity = _pick_numeric(item.get("liquidity")) or _pick_numeric(item.get("liquidityNum")) or 0.0
        return (price, liquidity)

    return max(markets, key=rank_key)


def load_manual_pairs_config(path: str | Path = "config/manual_pairs.yaml") -> list[dict]:
    target = Path(path)
    if not target.exists():
        return []
    payload = yaml.safe_load(target.read_text(encoding="utf-8")) or {}
    pairs = payload.get("pairs") or []
    return [pair for pair in pairs if isinstance(pair, dict)]


def refresh_watchlist_markets(
    *,
    raw_root: str,
    normalized_root: str,
    manual_pairs_path: str | Path = "config/manual_pairs.yaml",
    kalshi_client: KalshiClient | None = None,
    polymarket_client: PolymarketClient | None = None,
) -> dict[str, list[NormalizedMarket]]:
    pairs = load_manual_pairs_config(manual_pairs_path)
    kalshi = kalshi_client or KalshiClient()
    polymarket = polymarket_client or PolymarketClient()
    fetched_at = utc_now()
    stamp = fetched_at.strftime("%Y%m%dT%H%M%SZ")

    kalshi_payloads: list[dict] = []
    polymarket_payloads: list[dict] = []
    normalized_kalshi: list[NormalizedMarket] = []
    normalized_polymarket: list[NormalizedMarket] = []
    seen_kalshi: set[str] = set()
    seen_poly: set[str] = set()

    for pair in pairs:
        kalshi_ref = pair.get("kalshi_url") or pair.get("kalshi_ticker") or pair.get("kalshi_market_id")
        if kalshi_ref:
            ref_text = str(kalshi_ref)
            dedupe_key = ref_text.rstrip("/") if ref_text.startswith("http") else extract_kalshi_ticker(ref_text)
            if dedupe_key not in seen_kalshi:
                market = None
                if ref_text.startswith("http"):
                    market = _pick_kalshi_candidate_from_url(pair=pair, ref_text=ref_text, kalshi=kalshi)
                else:
                    ticker = extract_kalshi_ticker(ref_text)
                    try:
                        market = kalshi.get_market_details(ticker)
                    except Exception:
                        market = None
                if market is None:
                    continue
                try:
                    market["orderbook"] = kalshi.get_order_book(str(market.get("ticker") or extract_kalshi_ticker(ref_text)))
                except Exception:
                    market["orderbook"] = None
                kalshi_payloads.append(market)
                seen_kalshi.add(dedupe_key)
        poly_ref = pair.get("polymarket_url") or pair.get("polymarket_slug") or pair.get("polymarket_id")
        if poly_ref:
            ref_text = str(poly_ref)
            slug = extract_polymarket_slug(ref_text)
            if slug not in seen_poly:
                market = None
                if ref_text.startswith("http"):
                    try:
                        event_payload = polymarket.get_event_by_slug(slug)
                    except Exception:
                        event_payload = None
                    if event_payload and event_payload.get("markets"):
                        representative = _pick_polymarket_event_market(event_payload)
                        if representative is not None:
                            market = dict(representative)
                            market["title"] = pair.get("label") or event_payload.get("title") or representative.get("question")
                            market["subtitle"] = representative.get("question")
                            market["description"] = event_payload.get("description") or representative.get("description")
                            market["slug"] = event_payload.get("slug") or slug
                            market["market_url"] = ref_text.rstrip("/")
                            market["tags"] = event_payload.get("tags")
                    if market is None:
                        market = polymarket.get_market_by_slug(slug)
                        market["market_url"] = ref_text.rstrip("/")
                else:
                    market = polymarket.get_market_by_slug(slug)
                yes_token = None
                token_ids = market.get("clobTokenIds")
                if token_ids:
                    try:
                        import json

                        ids = json.loads(token_ids) if isinstance(token_ids, str) else token_ids
                        if ids:
                            yes_token = ids[0]
                    except Exception:
                        yes_token = None
                if yes_token:
                    try:
                        market["last_trade_price_payload"] = polymarket.get_last_trade_price(str(yes_token))
                        market["order_book"] = polymarket.get_order_book(str(yes_token))
                    except Exception:
                        market["last_trade_price_payload"] = None
                        market["order_book"] = None
                polymarket_payloads.append(market)
                seen_poly.add(slug)

    kalshi_raw_path = Path(raw_root) / "kalshi" / f"kalshi_watchlist_{stamp}.json"
    poly_raw_path = Path(raw_root) / "polymarket" / f"polymarket_watchlist_{stamp}.json"
    write_json(kalshi_raw_path, kalshi_payloads)
    write_json(poly_raw_path, polymarket_payloads)

    for market in kalshi_payloads:
        normalized_kalshi.append(
            normalize_kalshi_market(market, fetched_at=fetched_at, raw_payload_path=str(kalshi_raw_path))
        )
    for market in polymarket_payloads:
        normalized_polymarket.append(
            normalize_polymarket_market(market, fetched_at=fetched_at, raw_payload_path=str(poly_raw_path))
        )

    ensure_dir(normalized_root)
    kalshi_jsonl = Path(normalized_root) / f"kalshi_watchlist_{stamp}.jsonl"
    kalshi_csv = Path(normalized_root) / f"kalshi_watchlist_{stamp}.csv"
    poly_jsonl = Path(normalized_root) / f"polymarket_watchlist_{stamp}.jsonl"
    poly_csv = Path(normalized_root) / f"polymarket_watchlist_{stamp}.csv"
    write_jsonl(kalshi_jsonl, [m.model_dump(mode="json") for m in normalized_kalshi])
    write_csv(kalshi_csv, [m.model_dump(mode="json") for m in normalized_kalshi], columns=list(NormalizedMarket.model_fields.keys()))
    write_jsonl(poly_jsonl, [m.model_dump(mode="json") for m in normalized_polymarket])
    write_csv(poly_csv, [m.model_dump(mode="json") for m in normalized_polymarket], columns=list(NormalizedMarket.model_fields.keys()))
    return {"kalshi": normalized_kalshi, "polymarket": normalized_polymarket}

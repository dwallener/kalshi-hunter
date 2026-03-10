from __future__ import annotations

from datetime import datetime

from src.normalize.schema import NormalizedMarket
from src.utils.topic import infer_market_category
from src.utils.time import parse_datetime


def _infer_resolution_status(raw_status: str | None, result: str | None) -> str:
    status = (raw_status or "").lower()
    result_value = (result or "").lower()
    if "disputed" in status:
        return "disputed"
    if "settled" in status or "finalized" in status or result_value in {"yes", "no"}:
        return "resolved"
    if "void" in status:
        return "voided"
    if "refund" in status or "canceled" in status or "cancelled" in status:
        return "refunded"
    if status in {"open", "active", "closed", "paused"}:
        return "open"
    return "unknown"


def _infer_resolved_outcome(result: str | None) -> str:
    result_value = (result or "").strip().lower()
    if result_value in {"yes", "no", "refund", "void"}:
        return result_value
    return "unknown"


def _pick_price(raw_market: dict, cents_key: str, dollars_key: str) -> float | None:
    if dollars_key in raw_market and raw_market[dollars_key] not in (None, ""):
        return float(raw_market[dollars_key])
    if cents_key in raw_market and raw_market[cents_key] not in (None, ""):
        return float(raw_market[cents_key]) / 100.0
    return None


def normalize_kalshi_market(
    raw_market: dict,
    fetched_at: datetime,
    raw_payload_path: str,
) -> NormalizedMarket:
    raw_status = raw_market.get("status")
    rules_text = raw_market.get("rules_primary") or raw_market.get("rules")
    source_url = raw_market.get("result_source") or raw_market.get("rules_secondary")
    category = raw_market.get("category") or raw_market.get("series_category") or infer_market_category(
        raw_market.get("title"),
        raw_market.get("subtitle"),
        rules_text,
        raw_market.get("event_title"),
        raw_market.get("event_sub_title"),
    )
    return NormalizedMarket(
        venue="kalshi",
        venue_market_id=str(raw_market["ticker"]),
        event_id=raw_market.get("event_ticker"),
        title=raw_market.get("title") or raw_market["ticker"],
        subtitle=raw_market.get("subtitle"),
        category=category,
        rules_text=rules_text,
        source_url=source_url,
        market_url=raw_market.get("market_url") or f"https://kalshi.com/markets/{raw_market['ticker']}",
        open_time=parse_datetime(raw_market.get("open_time")),
        close_time=parse_datetime(raw_market.get("close_time")),
        expiration_time=parse_datetime(raw_market.get("expiration_time")),
        active=raw_status == "open" or raw_market.get("status") == "active",
        closed=raw_status in {"closed", "settled", "finalized"},
        archived=raw_status in {"settled", "finalized"},
        ticker=raw_market.get("ticker"),
        last_price_yes=_pick_price(raw_market, "last_price", "last_price_dollars")
        or _pick_price(raw_market, "yes_bid", "yes_bid_dollars"),
        last_price_no=_pick_price(raw_market, "no_bid", "no_bid_dollars"),
        volume=float(raw_market["volume"]) if raw_market.get("volume") not in (None, "") else None,
        liquidity=float(raw_market["liquidity"]) if raw_market.get("liquidity") not in (None, "") else None,
        raw_status=raw_status,
        resolution_status=_infer_resolution_status(raw_status, raw_market.get("result")),
        resolved_outcome=_infer_resolved_outcome(raw_market.get("result")),
        fetched_at=fetched_at,
        raw_payload_path=raw_payload_path,
    )

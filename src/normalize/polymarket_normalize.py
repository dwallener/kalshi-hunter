from __future__ import annotations

import json
from datetime import datetime

from src.normalize.schema import NormalizedMarket
from src.utils.topic import infer_market_category
from src.utils.time import parse_datetime


def _infer_resolution_status(raw_market: dict) -> str:
    if raw_market.get("uma_resolution_status"):
        status = str(raw_market["uma_resolution_status"]).lower()
        if "resolved" in status:
            return "resolved"
        if "dispute" in status:
            return "disputed"
        if "refund" in status:
            return "refunded"
        if "void" in status:
            return "voided"
    if raw_market.get("closed") is False and raw_market.get("active") is True:
        return "open"
    return "unknown"


def _infer_outcome(tokens: list[dict] | None) -> str:
    for token in tokens or []:
        if token.get("winner") is True:
            outcome = str(token.get("outcome", "")).lower()
            if outcome in {"yes", "no"}:
                return outcome
    return "unknown"


def _parse_list_field(value) -> list:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        value = value.strip()
        if not value:
            return []
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, list) else []
        except json.JSONDecodeError:
            return []
    return []


def _tokens_from_market(raw_market: dict) -> list[dict]:
    tokens = raw_market.get("tokens") or []
    if tokens:
        return tokens
    outcomes = _parse_list_field(raw_market.get("outcomes"))
    prices = _parse_list_field(raw_market.get("outcomePrices"))
    token_ids = _parse_list_field(raw_market.get("clobTokenIds"))
    derived_tokens: list[dict] = []
    for index, outcome in enumerate(outcomes):
        token: dict = {"outcome": outcome}
        if index < len(prices):
            token["price"] = prices[index]
        if index < len(token_ids):
            token["token_id"] = token_ids[index]
        derived_tokens.append(token)
    return derived_tokens


def _token_id(tokens: list[dict] | None, outcome_name: str) -> str | None:
    for token in tokens or []:
        if str(token.get("outcome", "")).lower() == outcome_name:
            value = token.get("token_id") or token.get("tokenId")
            return str(value) if value is not None else None
    return None


def _token_price(tokens: list[dict] | None, outcome_name: str) -> float | None:
    for token in tokens or []:
        if str(token.get("outcome", "")).lower() == outcome_name:
            price = token.get("price")
            if price is not None:
                return float(price)
    return None


def normalize_polymarket_market(
    raw_market: dict,
    fetched_at: datetime,
    raw_payload_path: str,
) -> NormalizedMarket:
    tokens = _tokens_from_market(raw_market)
    slug = raw_market.get("slug") or raw_market.get("market_slug")
    market_id = raw_market.get("id") or raw_market.get("conditionId") or raw_market.get("condition_id")
    title = raw_market.get("question") or raw_market.get("title") or slug or str(market_id)
    rules_text = raw_market.get("description") or raw_market.get("rules")
    tag_text = " ".join(str(tag) for tag in (raw_market.get("tags") or []) if tag)
    category = raw_market.get("category") or infer_market_category(
        title,
        raw_market.get("subtitle"),
        slug,
        tag_text,
    )
    if category is None:
        category = infer_market_category(
            title,
            raw_market.get("subtitle"),
            rules_text,
            slug,
            tag_text,
        )
    return NormalizedMarket(
        venue="polymarket",
        venue_market_id=str(market_id),
        event_id=raw_market.get("questionID") or raw_market.get("question_id"),
        title=title,
        subtitle=raw_market.get("subtitle"),
        category=category,
        rules_text=rules_text,
        source_url=raw_market.get("resolutionSource") or raw_market.get("resolution_source"),
        market_url=raw_market.get("market_url") or (f"https://polymarket.com/event/{slug}" if slug else None),
        open_time=parse_datetime(raw_market.get("startDate") or raw_market.get("start_date")),
        close_time=parse_datetime(raw_market.get("endDate") or raw_market.get("end_date") or raw_market.get("end_date_iso")),
        expiration_time=parse_datetime(raw_market.get("endDate") or raw_market.get("end_date") or raw_market.get("end_date_iso")),
        active=raw_market.get("active"),
        closed=raw_market.get("closed"),
        archived=raw_market.get("archived"),
        yes_token_id=_token_id(tokens, "yes"),
        no_token_id=_token_id(tokens, "no"),
        last_price_yes=_token_price(tokens, "yes") or (
            float(raw_market["lastTradePrice"]) if raw_market.get("lastTradePrice") not in (None, "") else None
        ),
        last_price_no=_token_price(tokens, "no"),
        volume=float(raw_market["volume"]) if raw_market.get("volume") not in (None, "") else (
            float(raw_market["volumeNum"]) if raw_market.get("volumeNum") not in (None, "") else None
        ),
        liquidity=float(raw_market["liquidity"]) if raw_market.get("liquidity") not in (None, "") else (
            float(raw_market["liquidityNum"]) if raw_market.get("liquidityNum") not in (None, "") else None
        ),
        raw_status=str(raw_market.get("uma_resolution_status") or raw_market.get("status") or ""),
        resolution_status=_infer_resolution_status(raw_market),
        resolved_outcome=_infer_outcome(tokens),
        fetched_at=fetched_at,
        raw_payload_path=raw_payload_path,
    )

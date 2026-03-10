from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field


ResolutionStatus = Literal["open", "resolved", "refunded", "voided", "disputed", "unknown"]
ResolvedOutcome = Literal["yes", "no", "refund", "void", "unknown"]


class NormalizedMarket(BaseModel):
    venue: Literal["kalshi", "polymarket"]
    venue_market_id: str
    event_id: str | None = None
    title: str
    subtitle: str | None = None
    category: str | None = None
    rules_text: str | None = None
    source_url: str | None = None
    market_url: str | None = None
    open_time: datetime | None = None
    close_time: datetime | None = None
    expiration_time: datetime | None = None
    active: bool | None = None
    closed: bool | None = None
    archived: bool | None = None
    yes_token_id: str | None = None
    no_token_id: str | None = None
    ticker: str | None = None
    last_price_yes: float | None = None
    last_price_no: float | None = None
    volume: float | None = None
    liquidity: float | None = None
    raw_status: str | None = None
    resolution_status: ResolutionStatus = "unknown"
    resolved_outcome: ResolvedOutcome = "unknown"
    fetched_at: datetime
    raw_payload_path: str


class MatchedPair(BaseModel):
    pair_id: str
    kalshi_market_id: str
    polymarket_market_id: str
    kalshi_title: str
    polymarket_title: str
    kalshi_ticker: str | None = None
    kalshi_market_url: str | None = None
    polymarket_slug: str | None = None
    kalshi_close_time: datetime | None = None
    polymarket_close_time: datetime | None = None
    kalshi_last_price_yes: float | None = None
    polymarket_last_price_yes: float | None = None
    kalshi_rules_text: str | None = None
    polymarket_rules_text: str | None = None
    kalshi_status: str | None = None
    polymarket_status: str | None = None
    title_similarity: float = Field(ge=0.0, le=1.0)
    time_similarity: float = Field(ge=0.0, le=1.0)
    category_similarity: float = Field(ge=0.0, le=1.0)
    keyword_overlap: float = Field(ge=0.0, le=1.0)
    rules_definition_penalty: float = Field(ge=0.0, le=1.0)
    overall_match_score: float = Field(ge=0.0, le=1.0)
    requires_manual_review: bool
    manual_seeded: bool = False
    seed_label: str | None = None
    seed_notes: str | None = None


class AnomalyRecord(BaseModel):
    pair_id: str
    kalshi_title: str
    polymarket_title: str
    kalshi_ticker: str | None = None
    polymarket_id: str
    kalshi_expiry: datetime | None = None
    polymarket_expiry: datetime | None = None
    kalshi_last_yes_price: float | None = None
    polymarket_last_yes_price: float | None = None
    absolute_divergence: float
    rule_flags: list[str]
    kalshi_status: str | None = None
    polymarket_status: str | None = None
    match_confidence: float
    anomaly_score: float
    note: str

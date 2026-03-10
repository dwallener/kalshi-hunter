from datetime import datetime, timezone

from src.normalize.kalshi_normalize import normalize_kalshi_market
from src.normalize.polymarket_normalize import normalize_polymarket_market


def test_kalshi_schema_accepts_sample_payload() -> None:
    raw = {
        "ticker": "KXIRAN-LEADER",
        "event_ticker": "KXIRAN",
        "title": "Will Iran's leader leave office by June 30?",
        "subtitle": "Official departure from office",
        "category": "Politics",
        "rules_primary": "Market resolves Yes if the leader resigns, is removed, or dies.",
        "result_source": "Reuters",
        "open_time": "2026-03-01T00:00:00Z",
        "close_time": "2026-06-30T23:59:00Z",
        "expiration_time": "2026-06-30T23:59:00Z",
        "status": "open",
        "last_price_dollars": "0.42",
        "no_bid_dollars": "0.58",
        "volume": "1200",
        "liquidity": "7500",
        "result": None,
    }
    market = normalize_kalshi_market(raw, datetime.now(timezone.utc), "data/raw/kalshi/sample.json")
    assert market.venue == "kalshi"
    assert market.venue_market_id == "KXIRAN-LEADER"
    assert market.last_price_yes == 0.42
    assert market.category == "Politics"


def test_polymarket_schema_accepts_sample_payload() -> None:
    raw = {
        "id": "123",
        "question_id": "event-1",
        "question": "Will Iran's leader leave office by June 30?",
        "description": "Resolves YES if the supreme leader resigns or is removed by June 30.",
        "category": "Politics",
        "resolutionSource": "Reuters",
        "endDate": "2026-06-30T23:59:00Z",
        "active": True,
        "closed": False,
        "archived": False,
        "slug": "iran-leader-june-30",
        "volume": "5000",
        "liquidity": "25000",
        "tokens": [
            {"outcome": "Yes", "price": 0.56, "token_id": "yes-123", "winner": False},
            {"outcome": "No", "price": 0.44, "token_id": "no-123", "winner": False},
        ],
    }
    market = normalize_polymarket_market(raw, datetime.now(timezone.utc), "data/raw/polymarket/sample.json")
    assert market.venue == "polymarket"
    assert market.yes_token_id == "yes-123"
    assert market.last_price_yes == 0.56
    assert market.market_url and "iran-leader-june-30" in market.market_url

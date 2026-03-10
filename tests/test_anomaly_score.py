from datetime import datetime, timezone

from src.normalize.schema import MatchedPair
from src.score.anomaly_score import compute_anomaly_score


def _pair(price_gap: tuple[float, float], rules_text: str) -> MatchedPair:
    return MatchedPair(
        pair_id="pair-1",
        kalshi_market_id="K1",
        polymarket_market_id="P1",
        kalshi_title="Will candidate leave office?",
        polymarket_title="Will candidate leave office?",
        kalshi_ticker="K1",
        polymarket_slug="poly-1",
        kalshi_close_time=datetime.fromisoformat("2026-06-30T23:59:00+00:00"),
        polymarket_close_time=datetime.fromisoformat("2026-06-30T23:59:00+00:00"),
        kalshi_last_price_yes=price_gap[0],
        polymarket_last_price_yes=price_gap[1],
        kalshi_rules_text=rules_text,
        polymarket_rules_text="Standard official source resolution",
        kalshi_status="open",
        polymarket_status="open",
        title_similarity=0.95,
        time_similarity=0.99,
        category_similarity=1.0,
        keyword_overlap=0.9,
        rules_definition_penalty=0.0,
        overall_match_score=0.9,
        requires_manual_review=False,
    )


def test_anomaly_score_rises_with_divergence_and_ambiguity() -> None:
    low = _pair((0.48, 0.50), "Standard market.")
    high = _pair((0.15, 0.85), "May void or refund at sole discretion in case of death or resignation ambiguity.")
    assert compute_anomaly_score(high) > compute_anomaly_score(low)

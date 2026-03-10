from datetime import datetime, timezone

from src.clients.kalshi_client import _looks_like_bundle_market
from pathlib import Path

from src.match.market_matcher import build_candidate_pairs, load_manual_pairs, parse_rules_risk_flags
from src.normalize.schema import NormalizedMarket


def _market(venue: str, market_id: str, title: str, close_time: str, category: str, rules_text: str, price: float):
    return NormalizedMarket(
        venue=venue,
        venue_market_id=market_id,
        title=title,
        category=category,
        rules_text=rules_text,
        close_time=datetime.fromisoformat(close_time.replace("Z", "+00:00")),
        expiration_time=datetime.fromisoformat(close_time.replace("Z", "+00:00")),
        last_price_yes=price,
        fetched_at=datetime.now(timezone.utc),
        raw_payload_path="fixture.json",
        ticker=market_id if venue == "kalshi" else None,
    )


def test_matcher_pairs_obvious_duplicates() -> None:
    kalshi = _market(
        "kalshi",
        "KX1",
        "Will Iran's leader leave office by June 30?",
        "2026-06-30T23:59:00Z",
        "Politics",
        "Resolves yes if he resigns, is removed, or dies.",
        0.41,
    )
    polymarket = _market(
        "polymarket",
        "P1",
        "Will Iran leader leave office before July?",
        "2026-06-30T23:00:00Z",
        "Politics",
        "Resolves YES if the leader resigns or is removed by an official source.",
        0.57,
    )
    pairs = build_candidate_pairs([kalshi], [polymarket])
    assert len(pairs) == 1
    assert pairs[0].overall_match_score >= 0.75
    assert pairs[0].requires_manual_review is False


def test_rules_parser_flags_risky_terms() -> None:
    flags = parse_rules_risk_flags(
        "Settlement uses an official source and may void or refund in cases of death, resignation, or ambiguity."
    )
    assert "official source" in flags
    assert "void" in flags
    assert "refund" in flags
    assert "death" in flags


def test_kalshi_bundle_noise_filter_flags_live_style_titles() -> None:
    assert _looks_like_bundle_market(
        {
            "ticker": "KXMVECROSSCATEGORY-S2026ABC",
            "title": "yes Golden State,yes New York,no Oklahoma City wins by over 8.5 Points,yes Montana",
        }
    )


def test_manual_pairs_are_loaded_and_emitted() -> None:
    kalshi = _market(
        "kalshi",
        "KX1",
        "Will Iran's leader leave office by June 30?",
        "2026-06-30T23:59:00Z",
        "Politics",
        "Resolves yes if he resigns, is removed, or dies.",
        0.41,
    )
    polymarket = _market(
        "polymarket",
        "P1",
        "Will Iran leader leave office before July?",
        "2026-06-30T23:00:00Z",
        "Politics",
        "Resolves YES if the leader resigns or is removed by an official source.",
        0.57,
    )
    polymarket = polymarket.model_copy(
        update={"market_url": "https://polymarket.com/event/iran-leader-july"}
    )
    config = Path("tests/manual_pairs_test.yaml")
    config.write_text(
        "pairs:\n"
        "  - kalshi_ticker: KX1\n"
        "    polymarket_slug: iran-leader-july\n"
        "    label: seeded\n"
        "    notes: manual seed\n",
        encoding="utf-8",
    )
    try:
        manual_pairs = load_manual_pairs(config)
        pairs = build_candidate_pairs([kalshi], [polymarket], manual_pairs=manual_pairs)
    finally:
        config.unlink(missing_ok=True)
    assert len(pairs) == 1
    assert pairs[0].manual_seeded is True
    assert pairs[0].overall_match_score == 1.0
    assert pairs[0].seed_label == "seeded"


def test_manual_pairs_can_match_by_full_urls() -> None:
    kalshi = _market(
        "kalshi",
        "KX2",
        "US credit rating downgrade this year?",
        "2026-12-31T23:59:00Z",
        "Politics",
        "Standard resolution.",
        0.37,
    ).model_copy(update={"market_url": "https://kalshi.com/markets/kxcreditrating/us-credit-rating-downgrade/kxcreditrating-26dec31"})
    polymarket = _market(
        "polymarket",
        "P2",
        "Another US debt downgrade before 2027?",
        "2026-12-31T23:59:00Z",
        "Politics",
        "Standard resolution.",
        0.61,
    ).model_copy(update={"market_url": "https://polymarket.com/event/another-us-debt-downgrade-before-2027"})
    config = Path("tests/manual_pairs_url_test.yaml")
    config.write_text(
        "pairs:\n"
        "  - kalshi_url: https://kalshi.com/markets/kxcreditrating/us-credit-rating-downgrade/kxcreditrating-26dec31\n"
        "    polymarket_url: https://polymarket.com/event/another-us-debt-downgrade-before-2027\n",
        encoding="utf-8",
    )
    try:
        manual_pairs = load_manual_pairs(config)
        pairs = build_candidate_pairs([kalshi], [polymarket], manual_pairs=manual_pairs)
    finally:
        config.unlink(missing_ok=True)
    assert len(pairs) == 1
    assert pairs[0].manual_seeded is True
    assert not _looks_like_bundle_market(
        {
            "ticker": "KXIRANLEADER-2026",
            "title": "Will Iran's leader leave office by June 30?",
        }
    )

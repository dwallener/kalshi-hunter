from __future__ import annotations

import re
import string
from pathlib import Path
from difflib import SequenceMatcher
from typing import Any

import yaml

try:
    from rapidfuzz import fuzz
except ImportError:  # pragma: no cover
    class _FuzzFallback:
        @staticmethod
        def token_set_ratio(left: str, right: str) -> float:
            left_tokens = sorted(set(left.split()))
            right_tokens = sorted(set(right.split()))
            return SequenceMatcher(None, " ".join(left_tokens), " ".join(right_tokens)).ratio() * 100.0

    fuzz = _FuzzFallback()

from src.normalize.schema import MatchedPair, NormalizedMarket
from src.utils.io import ensure_dir, write_csv
from src.utils.time import hours_between


RULES_RISK_TERMS = [
    "death",
    "resign",
    "resignation",
    "removed",
    "ousted",
    "incapacitated",
    "official source",
    "sole discretion",
    "void",
    "refund",
    "cancel",
    "ambiguity",
]

STOPWORDS = {"the", "a", "an", "will", "be", "of", "by", "to", "on", "in", "for", "if", "is"}


def normalize_text(value: str | None) -> str:
    if not value:
        return ""
    table = str.maketrans("", "", string.punctuation)
    return re.sub(r"\s+", " ", value.lower().translate(table)).strip()


def tokenize(value: str | None) -> set[str]:
    return {
        token
        for token in normalize_text(value).split()
        if token and token not in STOPWORDS
    }


def parse_rules_risk_flags(rules_text: str | None) -> list[str]:
    normalized = normalize_text(rules_text)
    return [term for term in RULES_RISK_TERMS if term in normalized]


def _category_similarity(left: str | None, right: str | None) -> float:
    left_norm = normalize_text(left)
    right_norm = normalize_text(right)
    if not left_norm or not right_norm:
        return 0.0
    return 1.0 if left_norm == right_norm else 0.4 if left_norm in right_norm or right_norm in left_norm else 0.0


def _keyword_overlap(left: str | None, right: str | None) -> float:
    left_tokens = tokenize(left)
    right_tokens = tokenize(right)
    if not left_tokens or not right_tokens:
        return 0.0
    overlap = len(left_tokens & right_tokens)
    universe = len(left_tokens | right_tokens)
    return overlap / universe if universe else 0.0


def _time_similarity(left: NormalizedMarket, right: NormalizedMarket, max_gap_hours: float = 168.0) -> float:
    left_time = left.expiration_time or left.close_time
    right_time = right.expiration_time or right.close_time
    gap_hours = hours_between(left_time, right_time)
    if gap_hours is None:
        return 0.5
    bounded_gap = min(gap_hours, max_gap_hours)
    return max(0.0, 1.0 - (bounded_gap / max_gap_hours))


def _rules_definition_penalty(left: str | None, right: str | None) -> float:
    left_flags = set(parse_rules_risk_flags(left))
    right_flags = set(parse_rules_risk_flags(right))
    if not left_flags and not right_flags:
        return 0.0
    symmetric_difference = len(left_flags ^ right_flags)
    universe = len(left_flags | right_flags) or 1
    return min(1.0, symmetric_difference / universe)


def _build_pair(
    kalshi_market: NormalizedMarket,
    polymarket_market: NormalizedMarket,
    *,
    overall_score: float,
    high_confidence_threshold: float,
    manual_seeded: bool = False,
    seed_label: str | None = None,
    seed_notes: str | None = None,
) -> MatchedPair:
    title_similarity = fuzz.token_set_ratio(
        normalize_text(kalshi_market.title),
        normalize_text(polymarket_market.title),
    ) / 100.0
    time_similarity = _time_similarity(kalshi_market, polymarket_market)
    category_similarity = _category_similarity(kalshi_market.category, polymarket_market.category)
    keyword_overlap = _keyword_overlap(kalshi_market.title, polymarket_market.title)
    rules_penalty = _rules_definition_penalty(kalshi_market.rules_text, polymarket_market.rules_text)
    return MatchedPair(
        pair_id=f"{kalshi_market.venue_market_id}__{polymarket_market.venue_market_id}",
        kalshi_market_id=kalshi_market.venue_market_id,
        polymarket_market_id=polymarket_market.venue_market_id,
        kalshi_title=kalshi_market.title,
        polymarket_title=polymarket_market.title,
        kalshi_ticker=kalshi_market.ticker,
        kalshi_market_url=kalshi_market.market_url,
        polymarket_slug=polymarket_market.market_url,
        kalshi_close_time=kalshi_market.close_time or kalshi_market.expiration_time,
        polymarket_close_time=polymarket_market.close_time or polymarket_market.expiration_time,
        kalshi_last_price_yes=kalshi_market.last_price_yes,
        polymarket_last_price_yes=polymarket_market.last_price_yes,
        kalshi_rules_text=kalshi_market.rules_text,
        polymarket_rules_text=polymarket_market.rules_text,
        kalshi_status=kalshi_market.resolution_status,
        polymarket_status=polymarket_market.resolution_status,
        title_similarity=title_similarity,
        time_similarity=time_similarity,
        category_similarity=category_similarity,
        keyword_overlap=keyword_overlap,
        rules_definition_penalty=rules_penalty,
        overall_match_score=overall_score,
        requires_manual_review=False if manual_seeded else overall_score < high_confidence_threshold,
        manual_seeded=manual_seeded,
        seed_label=seed_label,
        seed_notes=seed_notes,
    )


def load_manual_pairs(path: str | Path) -> list[dict[str, Any]]:
    config_path = Path(path)
    if not config_path.exists():
        return []
    payload = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    pairs = payload.get("pairs") or []
    return [pair for pair in pairs if isinstance(pair, dict)]


def build_manual_seeded_pairs(
    kalshi_markets: list[NormalizedMarket],
    polymarket_markets: list[NormalizedMarket],
    manual_pairs: list[dict[str, Any]],
) -> list[MatchedPair]:
    kalshi_by_ticker = {market.venue_market_id: market for market in kalshi_markets}
    kalshi_by_url = {market.market_url: market for market in kalshi_markets if market.market_url}
    polymarket_by_id = {market.venue_market_id: market for market in polymarket_markets}
    polymarket_by_url = {market.market_url: market for market in polymarket_markets if market.market_url}
    seeded_pairs: list[MatchedPair] = []
    for pair in manual_pairs:
        kalshi_key = str(pair.get("kalshi_ticker") or pair.get("kalshi_market_id") or "").strip()
        kalshi_url = str(pair.get("kalshi_url") or "").strip()
        polymarket_key = str(pair.get("polymarket_id") or "").strip()
        polymarket_slug = str(pair.get("polymarket_slug") or "").strip()
        polymarket_url = str(pair.get("polymarket_url") or "").strip()
        if not kalshi_key and not kalshi_url:
            continue
        kalshi_market = kalshi_by_ticker.get(kalshi_key) if kalshi_key else None
        if kalshi_market is None and kalshi_url:
            kalshi_market = kalshi_by_url.get(kalshi_url)
        polymarket_market = polymarket_by_id.get(polymarket_key) if polymarket_key else None
        if polymarket_market is None and polymarket_url:
            polymarket_market = polymarket_by_url.get(polymarket_url)
        if polymarket_market is None and polymarket_slug:
            target_url = polymarket_slug
            if not target_url.startswith("http"):
                target_url = f"https://polymarket.com/event/{polymarket_slug}"
            polymarket_market = polymarket_by_url.get(target_url)
        if kalshi_market is None or polymarket_market is None:
            continue
        seeded_pairs.append(
            _build_pair(
                kalshi_market,
                polymarket_market,
                overall_score=1.0,
                high_confidence_threshold=1.0,
                manual_seeded=True,
                seed_label=pair.get("label"),
                seed_notes=pair.get("notes"),
            )
        )
    return seeded_pairs


def build_candidate_pairs(
    kalshi_markets: list[NormalizedMarket],
    polymarket_markets: list[NormalizedMarket],
    high_confidence_threshold: float = 0.75,
    manual_review_threshold: float = 0.55,
    manual_pairs: list[dict[str, Any]] | None = None,
) -> list[MatchedPair]:
    pairs: list[MatchedPair] = []
    seen_pair_ids: set[str] = set()
    for pair in build_manual_seeded_pairs(kalshi_markets, polymarket_markets, manual_pairs or []):
        pairs.append(pair)
        seen_pair_ids.add(pair.pair_id)
    for kalshi_market in kalshi_markets:
        for polymarket_market in polymarket_markets:
            pair_id = f"{kalshi_market.venue_market_id}__{polymarket_market.venue_market_id}"
            if pair_id in seen_pair_ids:
                continue
            title_similarity = fuzz.token_set_ratio(
                normalize_text(kalshi_market.title),
                normalize_text(polymarket_market.title),
            ) / 100.0
            time_similarity = _time_similarity(kalshi_market, polymarket_market)
            category_similarity = _category_similarity(kalshi_market.category, polymarket_market.category)
            keyword_overlap = _keyword_overlap(kalshi_market.title, polymarket_market.title)
            rules_penalty = _rules_definition_penalty(kalshi_market.rules_text, polymarket_market.rules_text)
            overall_score = (
                0.5 * title_similarity
                + 0.2 * time_similarity
                + 0.15 * category_similarity
                + 0.15 * keyword_overlap
                - 0.15 * rules_penalty
            )
            overall_score = max(0.0, min(1.0, overall_score))
            if overall_score < manual_review_threshold:
                continue
            pairs.append(
                _build_pair(
                    kalshi_market,
                    polymarket_market,
                    overall_score=overall_score,
                    high_confidence_threshold=high_confidence_threshold,
                )
            )
    pairs.sort(key=lambda pair: pair.overall_match_score, reverse=True)
    return pairs


def export_matched_pairs(
    pairs: list[MatchedPair],
    *,
    matched_root: str,
    high_confidence_threshold: float = 0.75,
) -> tuple[Path, Path]:
    ensure_dir(matched_root)
    candidate_rows = [pair.model_dump(mode="json") for pair in pairs]
    high_confidence_rows = [
        pair.model_dump(mode="json")
        for pair in pairs
        if pair.overall_match_score >= high_confidence_threshold and not pair.requires_manual_review
    ]
    columns = list(MatchedPair.model_fields.keys())
    candidate_path = write_csv(Path(matched_root) / "candidate_pairs.csv", candidate_rows, columns=columns)
    high_confidence_path = write_csv(
        Path(matched_root) / "high_confidence_pairs.csv",
        high_confidence_rows,
        columns=columns,
    )
    return candidate_path, high_confidence_path

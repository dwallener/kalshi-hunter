from __future__ import annotations

import re


TOPIC_KEYWORDS: dict[str, tuple[str, ...]] = {
    "politics": (
        "president",
        "prime minister",
        "leader",
        "election",
        "senate",
        "house speaker",
        "governor",
        "minister",
        "office",
        "administration",
        "parliament",
        "government",
        "ccp",
        "communist party",
        "trump",
        "biden",
        "israel",
        "iran",
        "taiwan",
        "china",
        "ukraine",
        "russia",
    ),
    "sports": (
        "nba",
        "nfl",
        "nhl",
        "fifa",
        "world cup",
        "stanley cup",
        "goals",
        "points",
        "rebounds",
        "assists",
        "wins by over",
        "score",
        "match",
        "team",
        "hurricanes",
        "panthers",
        "oilers",
        "stars",
        "avalanche",
    ),
    "entertainment": (
        "gta vi",
        "album",
        "rihanna",
        "playboi carti",
        "jesus christ return",
        "released before",
    ),
    "crypto": (
        "bitcoin",
        "eth",
        "ethereum",
        "crypto",
        "solana",
        "dogecoin",
    ),
    "legal": (
        "convicted",
        "sentenced",
        "prison",
        "trial",
        "lawsuit",
        "weinstein",
    ),
    "technology": (
        "openai",
        "hardware product",
        "ai",
        "launch",
    ),
}

TOPIC_KEYWORD_WEIGHTS: dict[str, dict[str, int]] = {
    "entertainment": {
        "gta vi": 4,
        "album": 3,
        "rihanna": 3,
        "playboi carti": 3,
        "released before": 2,
    },
    "sports": {
        "world cup": 3,
        "stanley cup": 3,
        "wins by over": 3,
        "points": 2,
        "goals": 2,
    },
    "politics": {
        "prime minister": 3,
        "president": 2,
        "leader": 2,
        "election": 2,
        "government": 2,
    },
}


CATEGORY_ALIASES: dict[str, set[str]] = {
    "politics": {"politics"},
    "sports": {"sports"},
    "entertainment": {"entertainment"},
    "crypto": {"crypto"},
    "legal": {"legal"},
    "technology": {"technology"},
}


def _normalize_text(value: str | None) -> str:
    if not value:
        return ""
    return re.sub(r"\s+", " ", value.lower()).strip()


def infer_market_category(*parts: str | None) -> str | None:
    haystack = " ".join(_normalize_text(part) for part in parts if part).strip()
    if not haystack:
        return None
    scores: dict[str, int] = {}
    for topic, keywords in TOPIC_KEYWORDS.items():
        score = sum(1 for keyword in keywords if keyword in haystack)
        for keyword, weight in TOPIC_KEYWORD_WEIGHTS.get(topic, {}).items():
            if keyword in haystack:
                score += weight
        if score:
            scores[topic] = score
    if not scores:
        return None
    return max(scores.items(), key=lambda item: item[1])[0]


def category_matches_request(category: str | None, requested_category: str | None) -> bool:
    if not requested_category:
        return True
    requested = _normalize_text(requested_category)
    effective_category = _normalize_text(category)
    if not effective_category:
        return False
    allowed = CATEGORY_ALIASES.get(requested, {requested})
    return effective_category in allowed

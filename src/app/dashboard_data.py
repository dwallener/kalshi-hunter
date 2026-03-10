from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import yaml


def _safe_read_csv(path: str | Path) -> pd.DataFrame:
    target = Path(path)
    if not target.exists() or target.stat().st_size == 0:
        return pd.DataFrame()
    try:
        return pd.read_csv(target)
    except pd.errors.EmptyDataError:
        return pd.DataFrame()


def _normalize_text(value: object) -> str | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    text = str(value).strip()
    return text or None


def _normalize_url(value: object) -> str | None:
    text = _normalize_text(value)
    if not text:
        return None
    return text.rstrip("/")


def load_manual_pairs(path: str | Path = "config/manual_pairs.yaml") -> pd.DataFrame:
    target = Path(path)
    if not target.exists():
        return pd.DataFrame()
    payload = yaml.safe_load(target.read_text(encoding="utf-8")) or {}
    pairs = payload.get("pairs") or []
    frame = pd.DataFrame(pairs)
    if frame.empty:
        return frame
    frame = frame.copy()
    frame["seed_label"] = frame.get("label")
    frame["seed_notes"] = frame.get("notes")
    if "kalshi_url" in frame:
        frame["kalshi_url"] = frame["kalshi_url"].map(_normalize_url)
    if "polymarket_url" in frame:
        frame["polymarket_url"] = frame["polymarket_url"].map(_normalize_url)
    return frame


def load_candidate_pairs(path: str | Path = "data/matched/candidate_pairs.csv") -> pd.DataFrame:
    frame = _safe_read_csv(path)
    if frame.empty:
        return frame
    frame = frame.copy()
    for column in ("kalshi_market_url", "polymarket_slug"):
        if column in frame:
            frame[column] = frame[column].map(_normalize_url)
    for column in ("manual_seeded", "requires_manual_review"):
        if column in frame:
            frame[column] = frame[column].fillna(False).astype(bool)
    for column in ("kalshi_last_price_yes", "polymarket_last_price_yes", "overall_match_score"):
        if column in frame:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
    return frame


def load_anomaly_report(path: str | Path = "data/reports/anomaly_report.csv") -> pd.DataFrame:
    frame = _safe_read_csv(path)
    if frame.empty:
        return frame
    frame = frame.copy()
    for column in ("absolute_divergence", "match_confidence", "anomaly_score"):
        if column in frame:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
    return frame


def load_normalized_markets(path: str | Path = "data/normalized") -> pd.DataFrame:
    root = Path(path)
    rows: list[dict] = []
    for jsonl_path in sorted(root.glob("*.jsonl"), key=lambda item: item.stat().st_mtime):
        if jsonl_path.stat().st_size == 0:
            continue
        with jsonl_path.open("r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                rows.append(json.loads(line))
    if not rows:
        return pd.DataFrame()
    frame = pd.DataFrame(rows)
    if frame.empty:
        return frame
    if "market_url" in frame:
        frame["market_url"] = frame["market_url"].map(_normalize_url)
    if "ticker" in frame:
        frame["ticker"] = frame["ticker"].map(_normalize_text)
    if "venue_market_id" in frame:
        frame["venue_market_id"] = frame["venue_market_id"].map(_normalize_text)
    for column in ("last_price_yes", "last_price_no"):
        if column in frame:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame = frame.sort_values(by="fetched_at") if "fetched_at" in frame else frame
    dedupe_key = "market_url" if "market_url" in frame else "venue_market_id"
    frame = frame.drop_duplicates(subset=[dedupe_key], keep="last")
    return frame


def build_watchlist_frame(
    manual_pairs_path: str | Path = "config/manual_pairs.yaml",
    candidate_pairs_path: str | Path = "data/matched/candidate_pairs.csv",
    normalized_root: str | Path = "data/normalized",
) -> pd.DataFrame:
    manual = load_manual_pairs(manual_pairs_path)
    candidates = load_candidate_pairs(candidate_pairs_path)
    markets = load_normalized_markets(normalized_root)
    if manual.empty:
        return pd.DataFrame()

    seeded = candidates[candidates.get("manual_seeded", False)] if not candidates.empty else pd.DataFrame()
    kalshi_markets = markets[markets["venue"] == "kalshi"] if not markets.empty and "venue" in markets else pd.DataFrame()
    polymarket_markets = markets[markets["venue"] == "polymarket"] if not markets.empty and "venue" in markets else pd.DataFrame()
    rows: list[dict] = []
    for _, pair in manual.iterrows():
        kalshi_url = _normalize_url(pair.get("kalshi_url"))
        polymarket_url = _normalize_url(pair.get("polymarket_url"))
        kalshi_ticker = _normalize_text(pair.get("kalshi_ticker"))
        polymarket_slug = _normalize_text(pair.get("polymarket_slug"))
        match = None
        kalshi_market = None
        polymarket_market = None
        if not seeded.empty:
            matched = seeded
            if kalshi_url and "kalshi_market_url" in matched:
                matched = matched[matched["kalshi_market_url"] == kalshi_url]
            elif kalshi_ticker and "kalshi_ticker" in matched:
                matched = matched[matched["kalshi_ticker"] == kalshi_ticker]
            if polymarket_url and "polymarket_slug" in matched:
                matched = matched[matched["polymarket_slug"] == polymarket_url]
            elif polymarket_slug and "polymarket_slug" in matched:
                target_url = polymarket_slug if polymarket_slug.startswith("http") else f"https://polymarket.com/event/{polymarket_slug}"
                matched = matched[matched["polymarket_slug"] == target_url]
            if not matched.empty:
                match = matched.iloc[0]
        if not kalshi_markets.empty:
            if kalshi_url and "market_url" in kalshi_markets:
                matched_kalshi = kalshi_markets[kalshi_markets["market_url"] == kalshi_url]
            elif kalshi_ticker and "ticker" in kalshi_markets:
                matched_kalshi = kalshi_markets[kalshi_markets["ticker"] == kalshi_ticker]
            else:
                matched_kalshi = pd.DataFrame()
            if not matched_kalshi.empty:
                kalshi_market = matched_kalshi.iloc[0]
        if not polymarket_markets.empty:
            if polymarket_url and "market_url" in polymarket_markets:
                matched_poly = polymarket_markets[polymarket_markets["market_url"] == polymarket_url]
            elif polymarket_slug and "market_url" in polymarket_markets:
                target_url = polymarket_slug if polymarket_slug.startswith("http") else f"https://polymarket.com/event/{polymarket_slug}"
                matched_poly = polymarket_markets[polymarket_markets["market_url"] == target_url]
            else:
                matched_poly = pd.DataFrame()
            if not matched_poly.empty:
                polymarket_market = matched_poly.iloc[0]

        row = {
            "label": pair.get("label"),
            "seed_notes": pair.get("notes"),
            "kalshi_url": kalshi_url,
            "polymarket_url": polymarket_url,
            "kalshi_ref": kalshi_ticker or kalshi_url,
            "polymarket_ref": polymarket_slug or polymarket_url,
            "seed_resolved": kalshi_market is not None and polymarket_market is not None,
            "candidate_resolved": match is not None,
            "kalshi_title": (
                kalshi_market.get("title") if kalshi_market is not None else (match.get("kalshi_title") if match is not None else None)
            ),
            "polymarket_title": (
                polymarket_market.get("title") if polymarket_market is not None else (match.get("polymarket_title") if match is not None else None)
            ),
            "kalshi_last_price_yes": (
                kalshi_market.get("last_price_yes") if kalshi_market is not None else (match.get("kalshi_last_price_yes") if match is not None else None)
            ),
            "polymarket_last_price_yes": (
                polymarket_market.get("last_price_yes") if polymarket_market is not None else (match.get("polymarket_last_price_yes") if match is not None else None)
            ),
            "match_confidence": match.get("overall_match_score") if match is not None else None,
            "requires_manual_review": match.get("requires_manual_review") if match is not None else None,
        }
        left = row["kalshi_last_price_yes"]
        right = row["polymarket_last_price_yes"]
        if pd.notna(left) and pd.notna(right):
            row["absolute_divergence"] = abs(float(left) - float(right))
        else:
            row["absolute_divergence"] = None
        rows.append(row)

    frame = pd.DataFrame(rows)
    if not frame.empty and "absolute_divergence" in frame:
        frame["absolute_divergence"] = pd.to_numeric(frame["absolute_divergence"], errors="coerce")
        frame = frame.sort_values(
            by=["seed_resolved", "absolute_divergence"],
            ascending=[False, False],
            na_position="last",
        )
    return frame


def build_overview_metrics(watchlist: pd.DataFrame, anomaly_report: pd.DataFrame, candidate_pairs: pd.DataFrame) -> dict[str, int | float]:
    seeded_total = int(len(watchlist)) if not watchlist.empty else 0
    seeded_resolved = int(watchlist["seed_resolved"].sum()) if seeded_total else 0
    seeded_with_prices = int(
        watchlist["absolute_divergence"].notna().sum()
    ) if seeded_total and "absolute_divergence" in watchlist else 0
    top_divergence = float(watchlist["absolute_divergence"].max()) if seeded_with_prices else 0.0
    return {
        "seeded_total": seeded_total,
        "seeded_resolved": seeded_resolved,
        "candidate_resolved": int(watchlist["candidate_resolved"].sum()) if seeded_total and "candidate_resolved" in watchlist else 0,
        "seeded_with_prices": seeded_with_prices,
        "top_divergence": round(top_divergence, 4),
        "candidate_pairs": int(len(candidate_pairs)) if not candidate_pairs.empty else 0,
        "anomaly_rows": int(len(anomaly_report)) if not anomaly_report.empty else 0,
    }

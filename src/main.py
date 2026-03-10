from __future__ import annotations

import argparse
import math
from pathlib import Path

import pandas as pd
import yaml
from dotenv import load_dotenv

from src.ingest.kalshi_ingest import ingest_kalshi_markets
from src.ingest.polymarket_ingest import ingest_polymarket_markets
from src.ingest.watchlist_ingest import refresh_watchlist_markets
from src.match.market_matcher import build_candidate_pairs, export_matched_pairs, load_manual_pairs
from src.normalize.schema import MatchedPair, NormalizedMarket
from src.score.anomaly_score import export_anomaly_reports, score_pairs
from src.utils.io import list_jsonl_files, load_jsonl
from src.utils.logging import configure_logging


def load_settings(path: str = "config/settings.yaml") -> dict:
    with open(path, "r", encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def _normalize_scalar(value):
    if pd.isna(value):
        return None
    if isinstance(value, float) and value.is_integer():
        return int(value)
    return value


def load_normalized_markets(normalized_root: str, venue: str) -> list[NormalizedMarket]:
    deduped: dict[str, NormalizedMarket] = {}
    matching_files = sorted(
        [path for path in list_jsonl_files(normalized_root) if venue in path.name],
        key=lambda path: path.stat().st_mtime,
    )
    for path in matching_files:
        for row in load_jsonl(path):
            market = NormalizedMarket.model_validate(row)
            deduped[market.venue_market_id] = market
    return list(deduped.values())


def load_matched_pairs(matched_root: str) -> list[MatchedPair]:
    path = Path(matched_root) / "candidate_pairs.csv"
    if not path.exists():
        return []
    try:
        frame = pd.read_csv(path)
    except pd.errors.EmptyDataError:
        return []
    string_fields = {
        "pair_id",
        "kalshi_market_id",
        "polymarket_market_id",
        "kalshi_title",
        "polymarket_title",
        "kalshi_ticker",
        "kalshi_market_url",
        "polymarket_slug",
        "kalshi_rules_text",
        "polymarket_rules_text",
        "kalshi_status",
        "polymarket_status",
        "seed_label",
        "seed_notes",
    }
    records = []
    for row in frame.to_dict(orient="records"):
        normalized = {key: _normalize_scalar(value) for key, value in row.items()}
        for field in string_fields:
            if normalized.get(field) is not None:
                normalized[field] = str(normalized[field])
        records.append(MatchedPair.model_validate(normalized))
    return records


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Cross-venue prediction market anomaly detector")
    subparsers = parser.add_subparsers(dest="command", required=True)

    def add_ingest_parser(name: str) -> argparse.ArgumentParser:
        subparser = subparsers.add_parser(name)
        subparser.add_argument("--query", default=None)
        subparser.add_argument("--category", default=None)
        subparser.add_argument("--limit", type=int, default=None)
        return subparser

    add_ingest_parser("ingest-polymarket")
    add_ingest_parser("ingest-kalshi")
    add_ingest_parser("ingest-all")
    subparsers.add_parser("refresh-watchlist")

    subparsers.add_parser("match-markets")
    subparsers.add_parser("score-anomalies")

    full_refresh = add_ingest_parser("full-refresh")
    full_refresh.add_argument("--skip-score", action="store_true")
    return parser


def main() -> None:
    load_dotenv()
    configure_logging()
    settings = load_settings()
    parser = build_parser()
    args = parser.parse_args()

    raw_root = settings["paths"]["raw_root"]
    normalized_root = settings["paths"]["normalized_root"]
    matched_root = settings["paths"]["matched_root"]
    reports_root = settings["paths"]["reports_root"]
    manual_pairs = load_manual_pairs("config/manual_pairs.yaml")
    limit = getattr(args, "limit", None) or settings["project"]["default_limit"]

    if args.command == "ingest-polymarket":
        ingest_polymarket_markets(
            query=args.query,
            category=args.category,
            limit=limit,
            raw_root=raw_root,
            normalized_root=normalized_root,
        )
        return

    if args.command == "ingest-kalshi":
        ingest_kalshi_markets(
            query=args.query,
            category=args.category,
            limit=limit,
            raw_root=raw_root,
            normalized_root=normalized_root,
        )
        return

    if args.command == "refresh-watchlist":
        refresh_watchlist_markets(
            raw_root=raw_root,
            normalized_root=normalized_root,
        )
        return

    if args.command in {"ingest-all", "full-refresh"}:
        ingest_polymarket_markets(
            query=args.query,
            category=args.category,
            limit=limit,
            raw_root=raw_root,
            normalized_root=normalized_root,
        )
        ingest_kalshi_markets(
            query=args.query,
            category=args.category,
            limit=limit,
            raw_root=raw_root,
            normalized_root=normalized_root,
        )
        if args.command == "ingest-all":
            return

    if args.command in {"match-markets", "full-refresh"}:
        kalshi_markets = load_normalized_markets(normalized_root, "kalshi")
        polymarket_markets = load_normalized_markets(normalized_root, "polymarket")
        pairs = build_candidate_pairs(
            kalshi_markets,
            polymarket_markets,
            high_confidence_threshold=settings["matching"]["high_confidence_threshold"],
            manual_review_threshold=settings["matching"]["manual_review_threshold"],
            manual_pairs=manual_pairs,
        )
        export_matched_pairs(
            pairs,
            matched_root=matched_root,
            high_confidence_threshold=settings["matching"]["high_confidence_threshold"],
        )
        if args.command == "match-markets":
            return
        if getattr(args, "skip_score", False):
            return

    if args.command in {"score-anomalies", "full-refresh"}:
        pairs = load_matched_pairs(matched_root)
        records = score_pairs(pairs)
        export_anomaly_reports(records, reports_root=reports_root)
        return

    raise ValueError(f"Unsupported command: {args.command}")


if __name__ == "__main__":
    main()

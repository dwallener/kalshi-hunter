from __future__ import annotations

from pathlib import Path

from src.match.market_matcher import parse_rules_risk_flags
from src.normalize.schema import AnomalyRecord, MatchedPair
from src.utils.io import ensure_dir, write_csv
from src.utils.time import hours_between


def price_divergence_score(pair: MatchedPair) -> float:
    if pair.kalshi_last_price_yes is None or pair.polymarket_last_price_yes is None:
        return 0.0
    return min(1.0, abs(pair.kalshi_last_price_yes - pair.polymarket_last_price_yes))


def close_time_mismatch_score(pair: MatchedPair) -> float:
    gap_hours = hours_between(pair.kalshi_close_time, pair.polymarket_close_time)
    if gap_hours is None:
        return 0.0
    return min(1.0, gap_hours / 72.0)


def rules_ambiguity_score(pair: MatchedPair) -> float:
    flags = set(parse_rules_risk_flags(pair.kalshi_rules_text)) | set(parse_rules_risk_flags(pair.polymarket_rules_text))
    return min(1.0, len(flags) / 5.0)


def status_mismatch_score(pair: MatchedPair) -> float:
    left = (pair.kalshi_status or "unknown").lower()
    right = (pair.polymarket_status or "unknown").lower()
    if left == right:
        return 0.0
    severe = {"resolved", "refunded", "voided", "disputed"}
    if left in severe or right in severe:
        return 1.0
    return 0.5


def compute_anomaly_score(pair: MatchedPair) -> float:
    return round(
        0.40 * price_divergence_score(pair)
        + 0.20 * close_time_mismatch_score(pair)
        + 0.20 * rules_ambiguity_score(pair)
        + 0.20 * status_mismatch_score(pair),
        4,
    )


def build_anomaly_note(pair: MatchedPair, flags: list[str], divergence: float, status_score: float) -> str:
    reasons: list[str] = []
    if divergence >= 0.2:
        reasons.append("Large price divergence")
    if close_time_mismatch_score(pair) >= 0.25:
        reasons.append("close expiries mismatch")
    if flags:
        reasons.append(f"ambiguous {'/'.join(flags[:3])} language")
    if status_score >= 1.0:
        reasons.append("settlement/status mismatch")
    return ", ".join(reasons) if reasons else "Low-signal cross-venue mismatch"


def score_pairs(pairs: list[MatchedPair]) -> list[AnomalyRecord]:
    records: list[AnomalyRecord] = []
    for pair in pairs:
        divergence = price_divergence_score(pair)
        flags = sorted(set(parse_rules_risk_flags(pair.kalshi_rules_text)) | set(parse_rules_risk_flags(pair.polymarket_rules_text)))
        status_score = status_mismatch_score(pair)
        score = compute_anomaly_score(pair)
        records.append(
            AnomalyRecord(
                pair_id=pair.pair_id,
                kalshi_title=pair.kalshi_title,
                polymarket_title=pair.polymarket_title,
                kalshi_ticker=pair.kalshi_ticker,
                polymarket_id=pair.polymarket_market_id,
                kalshi_expiry=pair.kalshi_close_time,
                polymarket_expiry=pair.polymarket_close_time,
                kalshi_last_yes_price=pair.kalshi_last_price_yes,
                polymarket_last_yes_price=pair.polymarket_last_price_yes,
                absolute_divergence=round(divergence, 4),
                rule_flags=flags,
                kalshi_status=pair.kalshi_status,
                polymarket_status=pair.polymarket_status,
                match_confidence=pair.overall_match_score,
                anomaly_score=score,
                note=build_anomaly_note(pair, flags, divergence, status_score),
            )
        )
    records.sort(key=lambda record: record.anomaly_score, reverse=True)
    return records


def export_anomaly_reports(records: list[AnomalyRecord], reports_root: str) -> tuple[Path, Path]:
    ensure_dir(reports_root)
    csv_path = write_csv(
        Path(reports_root) / "anomaly_report.csv",
        [record.model_dump(mode="json") for record in records],
        columns=list(AnomalyRecord.model_fields.keys()),
    )
    markdown_lines = ["# Top 25 Anomalies", ""]
    if not records:
        markdown_lines.append("No anomaly candidates were produced in this run.")
    for record in records[:25]:
        markdown_lines.append(
            f"- `{record.kalshi_ticker or record.pair_id}` vs `{record.polymarket_id}`: "
            f"{record.kalshi_title} / {record.polymarket_title} "
            f"(score={record.anomaly_score}, divergence={record.absolute_divergence})"
        )
        markdown_lines.append(f"  {record.note}")
    markdown_path = Path(reports_root) / "top_25_anomalies.md"
    markdown_path.write_text("\n".join(markdown_lines) + "\n", encoding="utf-8")
    return csv_path, markdown_path

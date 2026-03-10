from __future__ import annotations

import json
import platform
import sys
import traceback
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import streamlit as st

from src.app.dashboard_data import (
    build_overview_metrics,
    build_watchlist_frame,
    load_anomaly_report,
    load_candidate_pairs,
    load_manual_pairs,
)
from src.ingest.watchlist_ingest import refresh_watchlist_markets
from src.main import load_normalized_markets, load_settings
from src.match.market_matcher import build_candidate_pairs, export_matched_pairs, load_manual_pairs as load_manual_seed_pairs
from src.score.anomaly_score import export_anomaly_reports, score_pairs


st.set_page_config(
    page_title="kalshi-hunter",
    page_icon="KH",
    layout="wide",
)

st.markdown(
    """
    <style>
    .stApp {
        background:
            radial-gradient(circle at top left, rgba(11, 92, 171, 0.08), transparent 28%),
            radial-gradient(circle at top right, rgba(187, 52, 47, 0.08), transparent 24%),
            #f5f4ef;
    }
    .block-container {
        padding-top: 2rem;
        padding-bottom: 2rem;
    }
    h1, h2, h3 {
        letter-spacing: -0.02em;
    }
    </style>
    """,
    unsafe_allow_html=True,
)


def build_startup_diagnostics(error: Exception | None = None) -> dict[str, object]:
    root = Path.cwd()
    data_root = root / "data"
    diagnostics = {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "python_version": sys.version,
        "platform": platform.platform(),
        "streamlit_version": st.__version__,
        "pandas_version": pd.__version__,
        "cwd": str(root),
        "files_present": {
            "streamlit_app.py": (root / "streamlit_app.py").exists(),
            "requirements.txt": (root / "requirements.txt").exists(),
            "runtime.txt": (root / "runtime.txt").exists(),
            "config/manual_pairs.yaml": (root / "config" / "manual_pairs.yaml").exists(),
            "config/settings.yaml": (root / "config" / "settings.yaml").exists(),
            "data_dir": data_root.exists(),
        },
        "data_counts": {
            "normalized_jsonl": len(list((data_root / "normalized").glob("*.jsonl"))) if (data_root / "normalized").exists() else 0,
            "matched_csv": len(list((data_root / "matched").glob("*.csv"))) if (data_root / "matched").exists() else 0,
            "report_files": len(list((data_root / "reports").glob("*"))) if (data_root / "reports").exists() else 0,
        },
    }
    if error is not None:
        diagnostics["error_type"] = type(error).__name__
        diagnostics["error_message"] = str(error)
        diagnostics["traceback"] = traceback.format_exc()
    return diagnostics


def write_startup_diagnostics(diagnostics: dict[str, object]) -> None:
    target = Path("data/reports/startup_diagnostics.json")
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(diagnostics, indent=2), encoding="utf-8")


@st.cache_data(show_spinner=False)
def load_dashboard_data() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, dict[str, int | float]]:
    watchlist = build_watchlist_frame()
    candidate_pairs = load_candidate_pairs()
    anomaly_report = load_anomaly_report()
    metrics = build_overview_metrics(watchlist, anomaly_report, candidate_pairs)
    return watchlist, candidate_pairs, anomaly_report, metrics


def _format_divergence(value: object) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return "-"
    return f"{float(value):.3f}"


def run_live_refresh() -> None:
    settings = load_settings()
    refresh_watchlist_markets(
        raw_root=settings["paths"]["raw_root"],
        normalized_root=settings["paths"]["normalized_root"],
    )
    kalshi_markets = load_normalized_markets(settings["paths"]["normalized_root"], "kalshi")
    polymarket_markets = load_normalized_markets(settings["paths"]["normalized_root"], "polymarket")
    manual_pairs = load_manual_seed_pairs("config/manual_pairs.yaml")
    pairs = build_candidate_pairs(
        kalshi_markets,
        polymarket_markets,
        high_confidence_threshold=settings["matching"]["high_confidence_threshold"],
        manual_review_threshold=settings["matching"]["manual_review_threshold"],
        manual_pairs=manual_pairs,
    )
    export_matched_pairs(
        pairs,
        matched_root=settings["paths"]["matched_root"],
        high_confidence_threshold=settings["matching"]["high_confidence_threshold"],
    )
    export_anomaly_reports(score_pairs(pairs), reports_root=settings["paths"]["reports_root"])


def maybe_bootstrap_cloud_data(
    watchlist: pd.DataFrame,
    candidate_pairs: pd.DataFrame,
    anomaly_report: pd.DataFrame,
    metrics: dict[str, int | float],
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, dict[str, int | float]]:
    already_attempted = st.session_state.get("cloud_bootstrap_attempted", False)
    has_prices = bool(metrics.get("seeded_with_prices", 0))
    if already_attempted or has_prices:
        return watchlist, candidate_pairs, anomaly_report, metrics

    st.session_state["cloud_bootstrap_attempted"] = True
    with st.spinner("Fetching live watchlist data..."):
        try:
            run_live_refresh()
        except Exception as exc:  # pragma: no cover - runtime-only path
            diagnostics = build_startup_diagnostics(exc)
            write_startup_diagnostics(diagnostics)
            st.error(f"Live refresh failed: {exc}")
            with st.expander("Startup Diagnostics", expanded=True):
                st.code(json.dumps(diagnostics, indent=2), language="json")
            return watchlist, candidate_pairs, anomaly_report, metrics
    st.cache_data.clear()
    return load_dashboard_data()


def render_app() -> None:
    watchlist, candidate_pairs, anomaly_report, metrics = load_dashboard_data()
    watchlist, candidate_pairs, anomaly_report, metrics = maybe_bootstrap_cloud_data(
        watchlist,
        candidate_pairs,
        anomaly_report,
        metrics,
    )

    st.title("kalshi-hunter")
    st.caption("Public, file-based dashboard for seeded cross-venue market monitoring.")
    st.info(
        "This app flags cross-venue anomalies for review. It does not prove venue exposure, manipulation, or settlement misconduct."
    )

    action_cols = st.columns([1, 5])
    if action_cols[0].button("Refresh Live Data", use_container_width=True):
        with st.spinner("Refreshing live watchlist and reports..."):
            run_live_refresh()
        st.cache_data.clear()
        st.rerun()
    action_cols[1].caption(
        "Community Cloud runs from the repo only. This button fetches the seeded watchlist and rebuilds local artifacts inside the app container."
    )

    metric_cols = st.columns(6)
    metric_cols[0].metric("Seeded Pairs", metrics.get("seeded_total", 0))
    metric_cols[1].metric("Resolved Seeds", metrics.get("seeded_resolved", 0))
    metric_cols[2].metric("Candidate-Resolved", metrics.get("candidate_resolved", 0))
    metric_cols[3].metric("Top Seed Divergence", _format_divergence(metrics.get("top_divergence", 0.0)))
    metric_cols[4].metric("Seeds With Prices", metrics.get("seeded_with_prices", 0))
    metric_cols[5].metric("Candidate Pairs", metrics.get("candidate_pairs", 0))

    watchlist_tab, anomalies_tab, candidates_tab, config_tab = st.tabs(
        ["Seed Watchlist", "Anomaly Report", "Candidate Pairs", "Manual Seeds"]
    )

    with watchlist_tab:
        st.subheader("Seed Watchlist")
        if watchlist.empty:
            st.warning("No manual seeds found. Add entries to `config/manual_pairs.yaml` and rerun matching.")
        else:
            resolved_only = st.toggle("Show only resolved seeds", value=True)
            table = watchlist.copy()
            if resolved_only:
                table = table[table["seed_resolved"]]
            st.dataframe(
                table[
                    [
                        "label",
                        "kalshi_url",
                        "polymarket_url",
                        "kalshi_title",
                        "polymarket_title",
                        "kalshi_last_price_yes",
                        "polymarket_last_price_yes",
                        "absolute_divergence",
                        "match_confidence",
                        "seed_notes",
                    ]
                ],
                use_container_width=True,
                hide_index=True,
                column_config={
                    "kalshi_url": st.column_config.LinkColumn("Kalshi"),
                    "polymarket_url": st.column_config.LinkColumn("Polymarket"),
                    "kalshi_last_price_yes": st.column_config.NumberColumn("Kalshi Yes", format="%.2f"),
                    "polymarket_last_price_yes": st.column_config.NumberColumn("Polymarket Yes", format="%.2f"),
                    "absolute_divergence": st.column_config.NumberColumn("Abs Divergence", format="%.3f"),
                    "match_confidence": st.column_config.NumberColumn("Match Confidence", format="%.3f"),
                },
            )
            if table["absolute_divergence"].notna().any():
                chart_data = (
                    table[["label", "absolute_divergence"]]
                    .dropna()
                    .sort_values("absolute_divergence", ascending=False)
                )
                st.caption("Top seeded divergences")
                st.dataframe(
                    chart_data,
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "absolute_divergence": st.column_config.NumberColumn("Abs Divergence", format="%.3f"),
                    },
                )

    with anomalies_tab:
        st.subheader("Anomaly Report")
        if anomaly_report.empty:
            st.warning("Run `python -m src.main score-anomalies` to populate the report.")
        else:
            manual_only = st.toggle("Show seeded/manual pairs only", value=True)
            display = anomaly_report.copy()
            if manual_only and not candidate_pairs.empty and "manual_seeded" in candidate_pairs:
                manual_ids = set(candidate_pairs[candidate_pairs["manual_seeded"]]["pair_id"])
                display = display[display["pair_id"].isin(manual_ids)]
            st.dataframe(
                display,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "absolute_divergence": st.column_config.NumberColumn("Abs Divergence", format="%.3f"),
                    "match_confidence": st.column_config.NumberColumn("Match Confidence", format="%.3f"),
                    "anomaly_score": st.column_config.NumberColumn("Anomaly Score", format="%.3f"),
                },
            )

    with candidates_tab:
        st.subheader("Candidate Matches")
        if candidate_pairs.empty:
            st.warning("Run `python -m src.main match-markets` to populate candidate pairs.")
        else:
            seeded_first = st.toggle("Show seeded pairs first", value=True)
            manual_only = st.toggle("Manual pairs only", value=True)
            display = candidate_pairs.copy()
            if manual_only and "manual_seeded" in display:
                display = display[display["manual_seeded"]]
            if seeded_first and "manual_seeded" in display:
                display = display.sort_values(
                    by=["manual_seeded", "overall_match_score"],
                    ascending=[False, False],
                )
            st.dataframe(
                display[
                    [
                        "pair_id",
                        "manual_seeded",
                        "seed_label",
                        "kalshi_title",
                        "polymarket_title",
                        "kalshi_last_price_yes",
                        "polymarket_last_price_yes",
                        "overall_match_score",
                        "requires_manual_review",
                    ]
                ],
                use_container_width=True,
                hide_index=True,
                column_config={
                    "manual_seeded": st.column_config.CheckboxColumn("Seeded"),
                    "kalshi_last_price_yes": st.column_config.NumberColumn("Kalshi Yes", format="%.2f"),
                    "polymarket_last_price_yes": st.column_config.NumberColumn("Polymarket Yes", format="%.2f"),
                    "overall_match_score": st.column_config.NumberColumn("Match Score", format="%.3f"),
                    "requires_manual_review": st.column_config.CheckboxColumn("Needs Review"),
                },
            )

    with config_tab:
        st.subheader("Manual Seed Config")
        manual_pairs = load_manual_pairs()
        if manual_pairs.empty:
            st.warning("`config/manual_pairs.yaml` is empty.")
        else:
            st.dataframe(
                manual_pairs[["label", "kalshi_url", "polymarket_url", "kalshi_ticker", "polymarket_slug", "notes"]],
                use_container_width=True,
                hide_index=True,
            )
        st.code(
            "python -m src.main refresh-watchlist\npython -m src.main match-markets\npython -m src.main score-anomalies\nstreamlit run streamlit_app.py",
            language="bash",
        )
        st.caption(f"Repo root: {Path.cwd()}")


try:
    render_app()
except Exception as exc:  # pragma: no cover - runtime-only path
    diagnostics = build_startup_diagnostics(exc)
    write_startup_diagnostics(diagnostics)
    st.title("kalshi-hunter")
    st.error(f"App startup failed: {exc}")
    st.exception(exc)
    st.subheader("Startup Diagnostics")
    st.code(json.dumps(diagnostics, indent=2), language="json")

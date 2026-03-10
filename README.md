# kalshi-hunter

Local Python MVP for cross-venue prediction-market anomaly detection.

## Purpose

This repo ingests comparable Kalshi and Polymarket markets, normalizes them into one schema, matches likely equivalents, and ranks first-pass anomalies for analyst review.

The target is not to prove venue-side exposure. The target is to flag cross-venue dislocations that may be explained by rule ambiguity, status mismatches, or differing settlement governance.

## Current Hypothesis

When two venues list materially similar markets, the highest-risk anomalies show up where:

- prices diverge despite close expiries
- one venue has more ambiguous or carveout-heavy rules
- one venue resolves, refunds, voids, or disputes while the other looks normal

## Data Sources

- Kalshi public market data:
  `GET /markets`, `GET /markets/{ticker}`, `GET /markets/{ticker}/orderbook`, `GET /markets/trades`, `GET /historical/trades`
- Polymarket public market data:
  Gamma API for market discovery and metadata, CLOB API for order books, last trade prices, and price history

Reference docs used during implementation:

- https://docs.kalshi.com/getting_started
- https://docs.kalshi.com/api-reference/market/get-markets
- https://docs.kalshi.com/api-reference/market/get-market-orderbook
- https://docs.kalshi.com/api-reference/historical/get-historical-trades
- https://docs.polymarket.com/api-reference/introduction
- https://docs.polymarket.com/market-data/fetching-markets
- https://docs.polymarket.com/developers/CLOB/clients/methods-public

## Project Layout

```text
config/
data/
  raw/
  normalized/
  matched/
  reports/
notebooks/
src/
tests/
```

Outputs are file-based for inspectability:

- raw JSON snapshots under `data/raw/{venue}/`
- normalized JSONL and CSV under `data/normalized/`
- candidate and high-confidence match tables under `data/matched/`
- scored anomaly reports under `data/reports/`
- Streamlit dashboard entrypoint at `streamlit_app.py`

Manual seeded pairs can be supplied in `config/manual_pairs.yaml` while automated discovery and matching are still being tuned.

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

No database is required for Phase 1.

## CLI Commands

Example narrow ingests:

```bash
python -m src.main ingest-polymarket --query "iran leader" --limit 50
python -m src.main ingest-kalshi --query "iran leader" --limit 50
python -m src.main ingest-polymarket --category politics --limit 200
python -m src.main ingest-kalshi --category politics --limit 200
```

Pipeline commands:

```bash
python -m src.main ingest-all --category politics --limit 200
python -m src.main refresh-watchlist
python -m src.main match-markets
python -m src.main score-anomalies
python -m src.main full-refresh --category politics --limit 200
```

Dashboard:

```bash
streamlit run streamlit_app.py
```

Example manual-pair seed:

```yaml
pairs:
  - kalshi_ticker: "KXTRUMP-OUT"
    polymarket_slug: "trump-out-as-president-before-term-end"
    label: "Trump out"
    notes: "Known comparable pair seeded manually."
```

## Phase 1 Scope

- thin Kalshi and Polymarket HTTP clients
- one shared `NormalizedMarket` schema
- title/date/category/keyword-based market matcher
- heuristic rules-risk parser for carveout language
- first-pass anomaly score:

```text
0.40 * price_divergence_score
+ 0.20 * close_time_mismatch_score
+ 0.20 * rules_ambiguity_score
+ 0.20 * status_mismatch_score
```

## Public Repo Notes

- The repo is designed to be public-safe: no private credentials are required for the dashboard.
- The Streamlit app reads local CSV/YAML artifacts only.
- Manual watchlist pairs live in `config/manual_pairs.yaml`.

## Tests

```bash
pytest -q
```

Tests cover schema normalization, rules-risk parsing, obvious market matching, and anomaly score monotonicity.

## Disclaimer

This system flags anomalies. It does not prove venue manipulation, proprietary exposure, or improper settlement behavior. Any ranked output is a triage signal for further review against venue-specific rules and authoritative outcome sources.

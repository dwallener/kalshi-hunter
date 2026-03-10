from src.ingest.watchlist_ingest import extract_kalshi_ticker, extract_polymarket_slug


def test_extract_kalshi_ticker_from_url() -> None:
    assert (
        extract_kalshi_ticker("https://kalshi.com/markets/kxfeddecision/fed-meeting/kxfeddecision-26mar")
        == "KXFEDDECISION-26MAR"
    )


def test_extract_polymarket_slug_from_url() -> None:
    assert (
        extract_polymarket_slug("https://polymarket.com/event/fed-decision-in-march-885")
        == "fed-decision-in-march-885"
    )

from src.utils.topic import category_matches_request, infer_market_category


def test_infer_market_category_for_politics_titles() -> None:
    assert infer_market_category("Will Trump remain President before 2028?") == "politics"
    assert infer_market_category("Will Wang Huning become the next leader of the CCP?") == "politics"


def test_infer_market_category_for_non_politics_titles() -> None:
    assert infer_market_category("Will Italy qualify for the 2026 FIFA World Cup?") == "sports"
    assert infer_market_category("New Rihanna Album before GTA VI?") == "entertainment"
    assert infer_market_category("Trump out as President before GTA VI?") == "entertainment"


def test_category_request_matching_is_strict() -> None:
    assert category_matches_request("politics", "politics")
    assert not category_matches_request("sports", "politics")

from apyx_monitor.config import get_rule_catalog
from apyx_monitor.routers.dashboard import _render_threshold_controls


def test_pool_arbitrage_profit_threshold_accepts_cent_values() -> None:
    rule = next(
        rule
        for rule in get_rule_catalog().rules
        if rule.rule_id == "pool_arb_net_profit_opportunity"
    )

    html = _render_threshold_controls(
        {rule.rule_id: rule},
        latest_map={},
        hours=24,
        threshold_updated=False,
    )

    assert 'min="0" step="0.01"' in html
    assert 'step="1000"' not in html

from __future__ import annotations

from datetime import timezone

from apyx_monitor.collectors.accountable import AccountableCollector


def test_extract_redemption_value_from_dashboard_payload():
    payload = {"data": {"reserves": {"redemption_value": 0.7907}}}

    assert AccountableCollector._extract_redemption_value(payload) == 0.7907


def test_extract_recorded_at_from_dashboard_payload_milliseconds():
    payload = {"data": {"ts": "1782721084666"}}

    recorded_at = AccountableCollector._extract_recorded_at(payload)

    assert recorded_at.tzinfo == timezone.utc
    assert int(recorded_at.timestamp() * 1000) == 1782721084666

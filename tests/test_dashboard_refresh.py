from pathlib import Path


def test_dashboard_refresh_aborts_hung_requests_and_releases_in_flight_state():
    source = (
        Path(__file__).parents[1] / "src" / "apyx_monitor" / "routers" / "dashboard.py"
    ).read_text(encoding="utf-8")

    assert "const DASHBOARD_REFRESH_TIMEOUT_MS = 15000;" in source
    assert "const fetchWithTimeout = async" in source
    assert "signal: controller.signal" in source
    assert "if (error.name === 'AbortError')" in source
    assert "refreshInFlight = false;" in source

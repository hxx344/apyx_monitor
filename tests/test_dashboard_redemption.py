from __future__ import annotations

from datetime import datetime, timedelta, timezone

from apyx_monitor.routers.dashboard import _moving_average_series


def test_moving_average_series_uses_time_window_and_sorts_points():
    base_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
    points = [
        (base_at + timedelta(hours=13), 12.0),
        (base_at, 2.0),
        (base_at + timedelta(hours=6), 8.0),
    ]

    averaged = _moving_average_series(points, 12)

    assert averaged == [
        (base_at, 2.0),
        (base_at + timedelta(hours=6), 5.0),
        (base_at + timedelta(hours=13), 10.0),
    ]

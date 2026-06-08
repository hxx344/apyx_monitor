from __future__ import annotations

from datetime import datetime, timedelta, timezone


RATE_LIMIT_COOLDOWN_SECONDS = 600
_rate_limited_until: datetime | None = None


def rate_limited_until() -> datetime | None:
    return _rate_limited_until


def is_rate_limited(now: datetime | None = None) -> bool:
    now = now or datetime.now(timezone.utc)
    return _rate_limited_until is not None and now < _rate_limited_until


def mark_rate_limited_until(value: datetime) -> datetime:
    global _rate_limited_until
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    if _rate_limited_until is None or value > _rate_limited_until:
        _rate_limited_until = value
    return _rate_limited_until


def mark_rate_limited_for(seconds: float) -> datetime:
    return mark_rate_limited_until(
        datetime.now(timezone.utc) + timedelta(seconds=max(seconds, 0.0))
    )


def clear_rate_limit() -> None:
    global _rate_limited_until
    _rate_limited_until = None

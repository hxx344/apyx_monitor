from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy.pool import StaticPool
from sqlmodel import Session, SQLModel, create_engine, select

from apyx_monitor.config import RuleCatalog, RuleDefinition, Settings
from apyx_monitor.models import AlertEvent
from apyx_monitor.services.alerting import FeishuNotifier
from apyx_monitor.services.rule_engine import RuleEngine


def _engine():
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(engine)
    return engine


def _rule(severity: str) -> RuleDefinition:
    return RuleDefinition(
        rule_id=f"{severity.lower()}_metric_floor",
        description=f"{severity} metric floor",
        entity_id="apyusd",
        metric_name="underlying_apy",
        comparator="lt",
        threshold=6,
        severity=severity,
        cooldown_seconds=0,
    )


def _metric(value: float) -> dict:
    return {
        "value": value,
        "unit": "%",
        "source": "test",
        "recorded_at": datetime.now(timezone.utc),
        "details": {},
    }


def _engine_for(rule: RuleDefinition) -> RuleEngine:
    return RuleEngine(RuleCatalog(rules=[rule]), FeishuNotifier(Settings()))


def test_p2_alerts_are_recorded_without_feishu_notifications():
    engine = _engine()
    rule = _rule("P2")
    rule_engine = _engine_for(rule)

    with Session(engine) as session:
        result = rule_engine.evaluate(session, {("apyusd", "underlying_apy"): _metric(5)})
        session.commit()

        alert = session.exec(select(AlertEvent)).one()
        assert len(result.events) == 1
        assert result.notifications == []
        assert alert.status == "firing"
        assert alert.severity == "P2"
        assert alert.notified_at is None

        repeat = rule_engine.evaluate(session, {("apyusd", "underlying_apy"): _metric(4)})
        session.commit()

        session.refresh(alert)
        assert repeat.notifications == []
        assert alert.occurrences == 2
        assert alert.notified_at is None

        resolved = rule_engine.evaluate(session, {("apyusd", "underlying_apy"): _metric(7)})
        session.commit()

        session.refresh(alert)
        assert len(resolved.events) == 1
        assert resolved.notifications == []
        assert alert.status == "resolved"


def test_p1_alerts_still_send_feishu_notifications():
    engine = _engine()
    rule = _rule("P1")
    rule_engine = _engine_for(rule)

    with Session(engine) as session:
        result = rule_engine.evaluate(session, {("apyusd", "underlying_apy"): _metric(5)})
        session.commit()

        alert = session.exec(select(AlertEvent)).one()
        assert len(result.notifications) == 1
        assert result.notifications[0].title.startswith("[P1]")
        assert alert.notified_at is not None

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Callable

from sqlmodel import Session, select

from ..config import RuleCatalog, RuleDefinition
from ..models import AlertEvent, AlertRuleOverride
from .alerting import FeishuNotifier


COMPARATORS: dict[str, Callable[[float, float], bool]] = {
    "lt": lambda current, threshold: current < threshold,
    "lte": lambda current, threshold: current <= threshold,
    "gt": lambda current, threshold: current > threshold,
    "gte": lambda current, threshold: current >= threshold,
}


@dataclass(frozen=True)
class NotificationMessage:
    title: str
    body: str


@dataclass(frozen=True)
class RuleEvaluationResult:
    events: list[AlertEvent]
    notifications: list[NotificationMessage]


class RuleEngine:
    def __init__(self, catalog: RuleCatalog, notifier: FeishuNotifier) -> None:
        self.catalog = catalog
        self.notifier = notifier
        self._hit_counters: dict[str, int] = {}

    def evaluate(
        self,
        session: Session,
        latest_metrics: dict[tuple[str, str], dict],
    ) -> RuleEvaluationResult:
        events: list[AlertEvent] = []
        notifications: list[NotificationMessage] = []
        now = datetime.now(timezone.utc)
        rules = self._effective_rules(session)

        for rule in rules:
            if not rule.enabled:
                continue
            metric = latest_metrics.get((rule.entity_id, rule.metric_name))
            fingerprint = f"{rule.rule_id}:{rule.entity_id}:{rule.metric_name}"
            active_alert = session.exec(
                select(AlertEvent).where(
                    AlertEvent.fingerprint == fingerprint,
                    AlertEvent.status == "firing",
                )
            ).first()

            if metric is None:
                continue

            is_match = COMPARATORS[rule.comparator](float(metric["value"]), rule.threshold)
            if is_match:
                self._hit_counters[fingerprint] = self._hit_counters.get(fingerprint, 0) + 1
                if self._hit_counters[fingerprint] < rule.required_consecutive_hits:
                    continue
                event = self._fire_alert(
                    session,
                    rule,
                    metric,
                    active_alert,
                    fingerprint,
                    now,
                    notifications,
                )
                if event is not None:
                    events.append(event)
            else:
                self._hit_counters[fingerprint] = 0
                if active_alert is not None:
                    resolved = self._resolve_alert(rule, active_alert, metric, now, notifications)
                    events.append(resolved)

        return RuleEvaluationResult(events=events, notifications=notifications)

    def _effective_rules(self, session: Session) -> list[RuleDefinition]:
        overrides = {
            row.rule_id: row
            for row in session.exec(select(AlertRuleOverride)).all()
        }
        return [
            rule.model_copy(update={"threshold": overrides[rule.rule_id].threshold})
            if rule.rule_id in overrides
            else rule
            for rule in self.catalog.rules
        ]

    def _fire_alert(
        self,
        session: Session,
        rule: RuleDefinition,
        metric: dict,
        active_alert: AlertEvent | None,
        fingerprint: str,
        now: datetime,
        notifications: list[NotificationMessage],
    ) -> AlertEvent | None:
        summary = self._build_summary(rule, metric["value"], status="firing")
        details_json = json.dumps(metric.get("details", {}), ensure_ascii=False)

        if active_alert is None:
            alert = AlertEvent(
                rule_id=rule.rule_id,
                entity_id=rule.entity_id,
                metric_name=rule.metric_name,
                fingerprint=fingerprint,
                severity=rule.severity,
                comparator=rule.comparator,
                threshold=rule.threshold,
                current_value=float(metric["value"]),
                summary=summary,
                status="firing",
                occurrences=1,
                first_triggered_at=now,
                last_triggered_at=now,
                notified_at=now,
                details_json=details_json,
            )
            session.add(alert)
            notifications.append(
                NotificationMessage(
                    title=f"[{rule.severity}] APYX 监控告警",
                    body=summary,
                )
            )
            return alert

        active_alert.threshold = rule.threshold
        active_alert.current_value = float(metric["value"])
        active_alert.summary = summary
        active_alert.last_triggered_at = now
        active_alert.occurrences += 1
        active_alert.details_json = details_json

        last_notified_at = self._ensure_aware(active_alert.notified_at)
        should_remind = (
            last_notified_at is None
            or now - last_notified_at >= timedelta(seconds=rule.cooldown_seconds)
        )
        if should_remind:
            notifications.append(
                NotificationMessage(
                    title=f"[{rule.severity}] APYX 监控持续告警",
                    body=summary,
                )
            )
            active_alert.notified_at = now
        return None

    def _resolve_alert(
        self,
        rule: RuleDefinition,
        active_alert: AlertEvent,
        metric: dict,
        now: datetime,
        notifications: list[NotificationMessage],
    ) -> AlertEvent:
        active_alert.status = "resolved"
        active_alert.threshold = rule.threshold
        active_alert.current_value = float(metric["value"])
        active_alert.summary = self._build_summary(rule, metric["value"], status="resolved")
        active_alert.resolved_at = now
        active_alert.last_triggered_at = now
        notifications.append(
            NotificationMessage(
                title=f"[{rule.severity}] APYX 监控恢复",
                body=active_alert.summary,
            )
        )
        return active_alert

    @staticmethod
    def _build_summary(rule: RuleDefinition, current_value: float, status: str) -> str:
        operator_map = {"lt": "<", "lte": "<=", "gt": ">", "gte": ">="}
        prefix = "触发" if status == "firing" else "恢复"
        return (
            f"{prefix}规则: {rule.description}\n"
            f"对象: {rule.entity_id}\n"
            f"指标: {rule.metric_name}\n"
            f"当前值: {current_value:.6f}\n"
            f"条件: {operator_map[rule.comparator]} {rule.threshold}"
        )

    @staticmethod
    def _ensure_aware(value: datetime | None) -> datetime | None:
        if value is None:
            return None
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value

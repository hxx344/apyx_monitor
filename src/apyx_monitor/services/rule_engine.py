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
            fingerprint = self._fingerprint(rule, metric)
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
        summary = self._build_summary(rule, metric["value"], status="firing", details=metric.get("details", {}))
        details_json = json.dumps(metric.get("details", {}), ensure_ascii=False)
        should_notify = self._should_notify(rule)

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
                notified_at=now if should_notify else None,
                details_json=details_json,
            )
            session.add(alert)
            if should_notify:
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
            not rule.notify_once
            and (
                last_notified_at is None
                or now - last_notified_at >= timedelta(seconds=rule.cooldown_seconds)
            )
        )
        if should_notify and should_remind:
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
        active_alert.summary = self._build_summary(
            rule,
            metric["value"],
            status="resolved",
            details=metric.get("details", {}),
        )
        active_alert.resolved_at = now
        active_alert.last_triggered_at = now
        if self._should_notify(rule) and not rule.notify_once:
            notifications.append(
                NotificationMessage(
                    title=f"[{rule.severity}] APYX 监控恢复",
                    body=active_alert.summary,
                )
            )
        return active_alert

    @staticmethod
    def _build_summary(
        rule: RuleDefinition,
        current_value: float,
        status: str,
        details: dict | None = None,
    ) -> str:
        if rule.rule_id == "crosschain_arb_edge_opportunity":
            return RuleEngine._build_arbitrage_summary(rule, current_value, status, details or {})
        if rule.rule_id == "eth_cd2a_336555_approval_detected":
            return RuleEngine._build_approval_summary(rule, status, details or {})
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
    def _build_arbitrage_summary(
        rule: RuleDefinition,
        current_value: float,
        status: str,
        details: dict,
    ) -> str:
        operator_map = {"lt": "<", "lte": "<=", "gt": ">", "gte": ">="}
        prefix = "触发" if status == "firing" else "恢复"
        lines = [
            f"{prefix}规则: {rule.description}",
            "指标: 闭环套利利润率",
            f"当前利润率: {current_value:.6f}%",
            f"阈值: {operator_map[rule.comparator]} {rule.threshold}%",
        ]
        strategy_label = details.get("strategy_label")
        if strategy_label:
            lines.append(f"策略: {strategy_label}")
        notional = details.get("notional_usd")
        if isinstance(notional, (int, float)):
            start_symbol = details.get("start_symbol") or "USDC"
            lines.append(f"本金: {notional:,.0f} {start_symbol}")
        final_amount = details.get("final_amount")
        if isinstance(final_amount, (int, float)):
            final_symbol = details.get("final_symbol") or "USDC"
            lines.append(f"最终 {final_symbol}: {final_amount:,.4f}")
        route = RuleEngine._format_arbitrage_route(details.get("route_steps"))
        if route:
            lines.append(f"路径: {route}")
        return "\n".join(lines)

    @staticmethod
    def _build_approval_summary(
        rule: RuleDefinition,
        status: str,
        details: dict,
    ) -> str:
        prefix = "触发" if status == "firing" else "恢复"
        lines = [
            f"{prefix}规则: {rule.description}",
            f"链: {details.get('chain', 'ethereum')}",
            f"Owner: {details.get('owner', '-')}",
            f"Token: {details.get('token', '-')}",
            f"Spender: {details.get('spender', '-')}",
            f"数量(raw): {details.get('approval_value_raw', '-')}",
        ]
        tx_hash = details.get("tx_hash")
        if tx_hash:
            lines.append(f"Tx: {tx_hash}")
        log_index = details.get("log_index")
        if log_index is not None:
            lines.append(f"LogIndex: {log_index}")
        block_number = details.get("block_number")
        if block_number is not None:
            lines.append(f"Block: {block_number}")
        events_in_scan = details.get("events_in_scan")
        if isinstance(events_in_scan, int) and events_in_scan > 1:
            lines.append(f"本轮命中事件数: {events_in_scan}")
        return "\n".join(lines)

    @staticmethod
    def _format_arbitrage_route(steps: object) -> str:
        if not isinstance(steps, list):
            return ""
        labels: list[str] = []
        for step in steps:
            if not isinstance(step, dict):
                continue
            from_symbol = step.get("from_symbol") or step.get("from_asset") or "-"
            to_symbol = step.get("to_symbol") or step.get("to_asset") or "-"
            if step.get("type") == "bridge":
                labels.append(f"{from_symbol} bridge {step.get('from_chain', '-')}->{step.get('to_chain', '-')}")
            elif step.get("type") == "swap":
                labels.append(f"{step.get('chain', '-')}: {from_symbol}->{to_symbol}")
        return " | ".join(labels)

    @staticmethod
    def _ensure_aware(value: datetime | None) -> datetime | None:
        if value is None:
            return None
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value

    @staticmethod
    def _should_notify(rule: RuleDefinition) -> bool:
        return rule.notify_feishu

    @staticmethod
    def _fingerprint(rule: RuleDefinition, metric: dict | None) -> str:
        base = f"{rule.rule_id}:{rule.entity_id}:{rule.metric_name}"
        if metric is None:
            return base
        details = metric.get("details", {})
        if not isinstance(details, dict):
            return base
        event_fingerprint = details.get("alert_fingerprint")
        if event_fingerprint:
            return f"{base}:{event_fingerprint}"
        return base

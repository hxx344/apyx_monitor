from __future__ import annotations

import base64
import hashlib
import hmac
import json
import re
import secrets
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from html import escape
from urllib.parse import quote

from fastapi import APIRouter, Depends, Form, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlalchemy import func
from sqlmodel import Session, select

from ..config import RuleDefinition, Settings, get_asset_catalog, get_rule_catalog, get_settings
from ..db import get_session
from ..models import AlertEvent, AlertRuleOverride, MetricSnapshot, utc_now

router = APIRouter(tags=["dashboard"])

BEIJING_TZ = timezone(timedelta(hours=8))
SESSION_COOKIE_NAME = "apyx_dashboard_session"

CARD_DEFS = [
    {"entity_id": "apxusd", "metric_name": "tvl_usd", "label": "apxUSD TVL"},
    {"entity_id": "apyusd", "metric_name": "tvl_usd", "label": "apyUSD TVL"},
    {
        "entity_id": "apyusd",
        "metric_name": "underlying_apy",
        "secondary_metric_name": "underlying_apr",
        "label": "apyUSD 底层 APR / APY",
        "display": "apr_apy_pair",
    },
    {"entity_id": "apyusd-ethereum", "metric_name": "convert_to_assets", "label": "apyUSD convertToAssets()"},
    {"entity_id": "curve-apyusd-apxusd", "metric_name": "exchange_rate", "label": "Curve 1 apyUSD → apxUSD"},
    {"entity_id": "curve-apyusd-apxusd", "metric_name": "curve_rate_vs_nav_deviation_pct", "label": "Curve 偏离净值"},
    {
        "entity_id": "apyusd-hedged-nav-discount",
        "metric_name": "annualized_apy_pct",
        "secondary_metric_name": "unlock_return_pct",
        "label": "apyUSD 20日对冲 APY",
        "display": "hedged_nav_discount",
    },
    {"entity_id": "morpho-apyusd-usdc", "metric_name": "capped_collateralization_ratio", "label": "Apyx Capped Ratio"},
    {"entity_id": "morpho-apyusd-usdc", "metric_name": "capped_collateralization_ratio_deviation_pct", "label": "Capped Ratio 脱锚幅度"},
    {
        "entity_id": "morpho-apyusd-usdc",
        "metric_name": "available_to_borrow_usd",
        "label": "apyUSD/USDC 可借款额",
    },
    {
        "entity_id": "arb-apyusd-apxusd-crosschain",
        "metric_name": "best_net_profit_usd",
        "label": "闭环套利最佳净利润",
    },
]

MORPHO_MARKETS = [market for market in get_asset_catalog().morpho_markets if market.enabled]
ARBITRAGE_MONITORS = [
    monitor for monitor in get_asset_catalog().arbitrage_monitors if monitor.enabled
]
ARBITRAGE_STRATEGY_IDS = ("buy-source-sell-target", "buy-target-sell-source")

CHART_DEFS = [
    {
        "title": "TVL 趋势",
        "series": [
            {"entity_id": "apxusd", "metric_name": "tvl_usd", "label": "apxUSD TVL", "color": "#60a5fa"},
            {"entity_id": "apyusd", "metric_name": "tvl_usd", "label": "apyUSD TVL", "color": "#34d399"},
        ],
    },
    {
        "title": "底层 APY 趋势",
        "series": [
            {"entity_id": "apxusd", "metric_name": "underlying_apy", "label": "apxUSD 底层 APY", "color": "#f59e0b"},
            {"entity_id": "apyusd", "metric_name": "underlying_apy", "label": "apyUSD 底层 APY", "color": "#f472b6"},
        ],
    },
    {
        "title": "apyUSD convertToAssets() 趋势",
        "series": [
            {"entity_id": "apyusd-ethereum", "metric_name": "convert_to_assets", "label": "1 apyUSD → apxUSD", "color": "#22c55e"},
        ],
    },
    {
        "title": "Curve apyUSD/apxUSD 汇率趋势",
        "series": [
            {"entity_id": "curve-apyusd-apxusd", "metric_name": "exchange_rate", "label": "Curve 1 apyUSD → apxUSD", "color": "#14b8a6"},
        ],
    },
    {
        "title": "偏离度趋势",
        "series": [
            {"entity_id": "curve-apyusd-apxusd", "metric_name": "curve_rate_vs_nav_deviation_pct", "label": "Curve 偏离净值", "color": "#f97316"},
            {"entity_id": "morpho-apyusd-usdc", "metric_name": "capped_collateralization_ratio_deviation_pct", "label": "Capped Ratio 脱锚幅度", "color": "#ef4444"},
        ],
    },
    {
        "title": "apyUSD 20日对冲 APY 趋势",
        "series": [
            {"entity_id": "apyusd-hedged-nav-discount", "metric_name": "annualized_apy_pct", "label": "20日对冲 APY", "color": "#a78bfa"},
            {"entity_id": "apyusd-hedged-nav-discount", "metric_name": "unlock_return_pct", "label": "20日收益率", "color": "#2dd4bf"},
        ],
    },
    {
        "title": "Apyx Capped Collateralization Ratio 趋势",
        "series": [
            {"entity_id": "morpho-apyusd-usdc", "metric_name": "capped_collateralization_ratio", "label": "Capped Ratio", "color": "#38bdf8"},
        ],
    },
    {
        "title": "闭环套利净利率趋势",
        "series": [
            {"entity_id": "arb-apyusd-apxusd-crosschain", "metric_name": "best_net_edge_pct", "label": "最佳净利率", "color": "#eab308"},
        ],
    },
]

THRESHOLD_RULE_IDS = [
    "morpho_apyusd_usdc_available_borrow_floor",
    "morpho_apyusd_usdc_borrow_apy_ceiling",
    "curve_apyusd_apxusd_rate_deviation_ceiling",
    "apyx_capped_ratio_deviation_ceiling",
    "crosschain_arb_edge_opportunity",
]


def _sign_session_payload(payload: str, settings: Settings) -> str:
    return hmac.new(
        settings.dashboard_session_secret.encode("utf-8"),
        payload.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()


def _create_session_token(username: str, settings: Settings) -> str:
    expires_at = int(time.time()) + settings.dashboard_session_ttl_seconds
    nonce = secrets.token_urlsafe(12)
    payload = f"{username}|{expires_at}|{nonce}"
    signature = _sign_session_payload(payload, settings)
    token = f"{payload}|{signature}".encode("utf-8")
    return base64.urlsafe_b64encode(token).decode("ascii")


def _read_session_username(request: Request, settings: Settings) -> str | None:
    token = request.cookies.get(SESSION_COOKIE_NAME)
    if not token:
        return None
    try:
        decoded = base64.urlsafe_b64decode(token.encode("ascii")).decode("utf-8")
        username, expires_at_raw, nonce, signature = decoded.split("|", 3)
        payload = f"{username}|{expires_at_raw}|{nonce}"
        expected_signature = _sign_session_payload(payload, settings)
        if not hmac.compare_digest(signature, expected_signature):
            return None
        if int(expires_at_raw) < int(time.time()):
            return None
        if not hmac.compare_digest(username, settings.dashboard_username):
            return None
        return username
    except Exception:  # noqa: BLE001
        return None


def _dashboard_next_url(request: Request) -> str:
    path = request.url.path
    if request.url.query:
        path = f"{path}?{request.url.query}"
    return path


def _require_dashboard_auth(request: Request) -> str:
    settings = get_settings()
    username = _read_session_username(request, settings)
    if username is None:
        next_url = quote(_dashboard_next_url(request), safe="")
        raise HTTPException(status_code=303, headers={"Location": f"/dashboard/login?next={next_url}"})
    return username


def _format_value(metric_name: str, value: float | None) -> str:
    if value is None:
        return "-"
    if metric_name in {"best_net_profit_usd", "net_profit_usd", "gross_profit_usd", "total_cost_usd"}:
        return f"${value:,.2f}"
    if metric_name in {"best_notional_usd"}:
        return f"${value:,.0f}"
    if metric_name in {"best_net_edge_pct", "net_edge_pct", "gross_edge_pct"}:
        return f"{value:.3f}%"
    if metric_name in {
        "bought_apyusd",
        "sold_apxusd",
        "source_apyusd",
        "target_apyusd",
        "target_apxusd",
        "entry_apxusd",
        "final_apxusd",
        "final_usdc",
        "intermediate_apyusd",
    }:
        return f"{value:,.4f}"
    if metric_name == "convert_to_assets":
        return f"{value:.6f} apxUSD"
    if metric_name == "exchange_rate":
        return f"{value:.6f}"
    if metric_name.endswith("_ratio"):
        return f"{value:.4f}x"
    if metric_name.endswith("_usd") or metric_name == "price_usd":
        return f"${value:,.0f}" if abs(value) >= 100 else f"${value:,.4f}"
    if metric_name.endswith("_apy") or metric_name.endswith("_apr") or metric_name.endswith("_pct"):
        return f"{value:.2f}%"
    return f"{value:,.4f}"


def _monthly_compounded_pct(apr_pct: float) -> float:
    periods = 12
    apr = apr_pct / 100
    return ((1 + apr / periods) ** periods - 1) * 100


def _latest_metric_map(session: Session) -> dict[tuple[str, str], MetricSnapshot]:
    ranked = (
        select(
            MetricSnapshot.id.label("id"),
            func.row_number()
            .over(
                partition_by=(MetricSnapshot.entity_id, MetricSnapshot.metric_name),
                order_by=(MetricSnapshot.recorded_at.desc(), MetricSnapshot.id.desc()),
            )
            .label("rn"),
        )
        .subquery()
    )
    rows = session.exec(
        select(MetricSnapshot)
        .join(ranked, MetricSnapshot.id == ranked.c.id)
        .where(ranked.c.rn == 1)
    ).all()
    return {(row.entity_id, row.metric_name): row for row in rows}


def _to_beijing(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(BEIJING_TZ)


def _ensure_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _format_dt(dt: datetime, pattern: str = "%Y-%m-%d %H:%M") -> str:
    return _to_beijing(dt).strftime(pattern)


def _effective_rule_map(session: Session) -> dict[str, RuleDefinition]:
    override_map = {
        row.rule_id: row
        for row in session.exec(select(AlertRuleOverride)).all()
    }
    return {
        rule.rule_id: (
            rule.model_copy(update={"threshold": override_map[rule.rule_id].threshold})
            if rule.rule_id in override_map
            else rule
        )
        for rule in get_rule_catalog().rules
    }


def _bucket_series(
    session: Session,
    entity_id: str,
    metric_name: str,
    hours: int,
    bucket_minutes: int,
) -> list[tuple[datetime, float]]:
    since_at = datetime.now(timezone.utc) - timedelta(hours=hours)
    rows = session.exec(
        select(MetricSnapshot).where(
            MetricSnapshot.entity_id == entity_id,
            MetricSnapshot.metric_name == metric_name,
            MetricSnapshot.recorded_at >= since_at,
        )
        .order_by(MetricSnapshot.recorded_at.asc(), MetricSnapshot.id.asc())
    ).all()
    buckets: dict[datetime, list[MetricSnapshot]] = defaultdict(list)
    interval_seconds = bucket_minutes * 60
    for row in rows:
        recorded_at = _ensure_utc(row.recorded_at)
        bucket_ts = int(recorded_at.timestamp() // interval_seconds * interval_seconds)
        bucket_at = datetime.fromtimestamp(bucket_ts, tz=timezone.utc)
        buckets[bucket_at].append(row)
    return [(bucket_at, bucket_rows[-1].value) for bucket_at, bucket_rows in sorted(buckets.items())]


def _metric_value_24h_ago(session: Session, latest_metric: MetricSnapshot) -> float | None:
    latest_at = _ensure_utc(latest_metric.recorded_at)
    cutoff_at = latest_at - timedelta(hours=24)
    earliest_acceptable_at = latest_at - timedelta(hours=23)
    row = session.exec(
        select(MetricSnapshot)
        .where(
            MetricSnapshot.entity_id == latest_metric.entity_id,
            MetricSnapshot.metric_name == latest_metric.metric_name,
            MetricSnapshot.recorded_at >= cutoff_at,
            MetricSnapshot.recorded_at <= earliest_acceptable_at,
        )
        .order_by(MetricSnapshot.recorded_at.asc(), MetricSnapshot.id.asc())
        .limit(1)
    ).first()
    return row.value if row else None


def _format_signed_value(metric_name: str, value: float) -> str:
    sign = "+" if value >= 0 else "-"
    return f"{sign}{_format_value(metric_name, abs(value))}"


def _format_24h_change(current_value: float, previous_value: float | None, metric_name: str) -> str:
    if previous_value is None:
        return "24h 变化：暂无足够历史数据"

    absolute_change = current_value - previous_value
    pct_change = (absolute_change / previous_value * 100) if previous_value else None
    pct_text = f"{pct_change:+.2f}%" if pct_change is not None else "N/A"
    return f"24h 变化：{pct_text} / {_format_signed_value(metric_name, absolute_change)}"


def _slugify(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")


def _build_svg(series_list: list[dict], chart_id: str, width: int = 760, height: int = 260) -> str:
    valid_series = [series for series in series_list if series["points"]]
    if not valid_series:
        return '<div class="empty">暂无历史数据</div>'

    padding_left = 52
    padding_right = 16
    padding_top = 16
    padding_bottom = 28
    plot_width = width - padding_left - padding_right
    plot_height = height - padding_top - padding_bottom

    all_values = [value for series in valid_series for _, value in series["points"]]
    min_value = min(all_values)
    max_value = max(all_values)
    if min_value == max_value:
        min_value -= 1
        max_value += 1
    else:
        value_span = max_value - min_value
        padding = value_span * 0.12
        min_value -= padding
        max_value += padding

    all_timestamps = sorted({timestamp for series in valid_series for timestamp, _ in series["points"]})
    step_x = plot_width / max(len(all_timestamps) - 1, 1)

    defs = []
    paths = []
    points_markup = []

    grid_lines = []
    for y_ratio in (0, 0.5, 1):
        y = padding_top + plot_height * y_ratio
        grid_lines.append(
            f'<line x1="{padding_left}" y1="{y:.1f}" x2="{width - padding_right}" y2="{y:.1f}" stroke="rgba(148,163,184,0.12)" stroke-width="1" />'
        )

    labels = [
        (max_value, padding_top + 10),
        ((max_value + min_value) / 2, padding_top + plot_height / 2 + 4),
        (min_value, padding_top + plot_height + 4),
    ]
    y_axis = "".join(
        f'<text x="6" y="{y:.1f}" fill="#94a3b8" font-size="11">{escape(_format_value("value", value))}</text>'
        for value, y in labels
    )

    for index, series in enumerate(valid_series):
        series_slug = f"{chart_id}-series-{index}"
        defs.append(
            f'<linearGradient id="{series_slug}-fill" x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stop-color="{series["color"]}" stop-opacity="0.25" /><stop offset="100%" stop-color="{series["color"]}" stop-opacity="0.02" /></linearGradient>'
        )
        path_commands = []
        area_commands = []
        point_map = {timestamp: value for timestamp, value in series["points"]}
        active_points: list[tuple[float, float, str, str]] = []
        for point_index, timestamp in enumerate(all_timestamps):
            value = point_map.get(timestamp)
            if value is None:
                continue
            x = padding_left + step_x * point_index
            ratio = (value - min_value) / (max_value - min_value)
            y = padding_top + plot_height * (1 - ratio)
            path_commands.append(f'{"M" if not path_commands else "L"} {x:.2f} {y:.2f}')
            area_commands.append(f'{"M" if not area_commands else "L"} {x:.2f} {y:.2f}')
            active_points.append((x, y, _format_dt(timestamp, "%m-%d %H:%M"), _format_value(series["metric_name"], value)))
        d_attr = " ".join(path_commands)
        if not d_attr:
            continue
        first_x, last_x = active_points[0][0], active_points[-1][0]
        area_attr = " ".join(area_commands) + f" L {last_x:.2f} {padding_top + plot_height:.2f} L {first_x:.2f} {padding_top + plot_height:.2f} Z"
        paths.append(
            f'<path class="series-area" data-series-index="{index}" d="{area_attr}" fill="url(#{series_slug}-fill)" />'
            f'<path class="series-path" data-series-index="{index}" d="{d_attr}" fill="none" stroke="{series["color"]}" stroke-width="2.5" stroke-linejoin="round" stroke-linecap="round" />'
        )
        for x, y, label, formatted_value in active_points:
            points_markup.append(
                f'<circle class="point-node" data-series-index="{index}" data-label="{escape(label)}" data-series-label="{escape(series["label"])}" data-value="{escape(formatted_value)}" cx="{x:.2f}" cy="{y:.2f}" r="3.5" fill="{series["color"]}" stroke="#0f172a" stroke-width="1.5" />'
            )

    x_labels = []
    for index, timestamp in enumerate(all_timestamps):
        if index not in {0, len(all_timestamps) // 2, len(all_timestamps) - 1}:
            continue
        x = padding_left + step_x * index
        label = _format_dt(timestamp, "%m-%d %H:%M")
        x_labels.append(
            f'<text x="{x:.1f}" y="{height - 6}" text-anchor="middle" fill="#94a3b8" font-size="11">{label}</text>'
        )

    return (
        f'<svg viewBox="0 0 {width} {height}" class="chart-svg interactive-chart" data-chart-id="{chart_id}" preserveAspectRatio="none"><defs>{"".join(defs)}</defs>'
        + "".join(grid_lines)
        + y_axis
        + "".join(paths)
        + "".join(points_markup)
        + "".join(x_labels)
        + "</svg>"
    )


def _build_chart_table(series_list: list[dict], limit: int = 12) -> str:
    valid_series = [series for series in series_list if series["points"]]
    if not valid_series:
        return '<div class="empty small">暂无历史数据</div>'

    timestamps = sorted({timestamp for series in valid_series for timestamp, _ in series["points"]}, reverse=True)[:limit]
    header = ["<th>时间</th>"] + [f"<th>{escape(series['label'])}</th>" for series in valid_series]
    rows = []
    for timestamp in timestamps:
        cells = [f"<td>{escape(_format_dt(timestamp, '%m-%d %H:%M'))}</td>"]
        for series in valid_series:
            point_map = {point_timestamp: value for point_timestamp, value in series["points"]}
            cells.append(f"<td>{escape(_format_value(series['metric_name'], point_map.get(timestamp)))}</td>")
        rows.append(f"<tr>{''.join(cells)}</tr>")
    return f'<div class="trend-table-wrap"><table class="trend-table"><thead><tr>{"".join(header)}</tr></thead><tbody>{"".join(rows)}</tbody></table></div>'


def _render_cards(session: Session, latest_map: dict[tuple[str, str], MetricSnapshot]) -> str:
    cards = []
    for item in CARD_DEFS:
        if item.get("display") == "apr_apy_pair":
            cards.append(_render_apr_apy_card(item, latest_map))
            continue
        if item.get("display") == "hedged_nav_discount":
            cards.append(_render_hedged_nav_discount_card(item, latest_map))
            continue

        metric = latest_map.get((item["entity_id"], item["metric_name"]))
        value = _format_value(item["metric_name"], metric.value if metric else None)
        recorded_at = f"{_format_dt(metric.recorded_at)} 北京时间" if metric else "-"
        tvl_delta = ""
        is_tvl_card = item["metric_name"] == "tvl_usd" and item["entity_id"] in {
            "apxusd",
            "apyusd",
        }
        if metric and is_tvl_card:
            previous_value = _metric_value_24h_ago(session, metric)
            delta_text = _format_24h_change(metric.value, previous_value, item["metric_name"])
            tvl_delta = f'<div class="delta">{escape(delta_text)}</div>'
        cards.append(
            f'''
            <div class="card">
              <div class="label">{escape(item["label"])}</div>
              <div class="value">{escape(value)}</div>
              {tvl_delta}
              <div class="meta">更新时间：{escape(recorded_at)}</div>
            </div>
            '''
        )
    return "".join(cards)


def _render_hedged_nav_discount_card(
    item: dict,
    latest_map: dict[tuple[str, str], MetricSnapshot],
) -> str:
    entity_id = item["entity_id"]
    apy_metric = latest_map.get((entity_id, item["metric_name"]))
    return_metric = latest_map.get((entity_id, item["secondary_metric_name"]))
    curve_metric = latest_map.get(("curve-apyusd-apxusd", "exchange_rate"))
    nav_metric = latest_map.get(("apyusd-ethereum", "convert_to_assets"))

    metric_times = [
        metric.recorded_at
        for metric in (apy_metric, return_metric, curve_metric, nav_metric)
        if metric
    ]
    recorded_at = f"{_format_dt(max(metric_times))} 北京时间" if metric_times else "-"

    return f'''
            <div class="card strategy-card">
              <div class="label">{escape(item["label"])}</div>
              <div class="value">{escape(_format_value("annualized_apy_pct", apy_metric.value if apy_metric else None))}</div>
              <div class="strategy-grid">
                <div><span>20天收益</span><strong>{escape(_format_value("unlock_return_pct", return_metric.value if return_metric else None))}</strong></div>
                <div><span>Curve 买入价</span><strong>{escape(_format_value("exchange_rate", curve_metric.value if curve_metric else None))}</strong></div>
                <div><span>NAV 结算价</span><strong>{escape(_format_value("convert_to_assets", nav_metric.value if nav_metric else None))}</strong></div>
              </div>
              <div class="meta">假设 Lighter 做空等额 STRC 对冲 apxUSD 风险；更新时间：{escape(recorded_at)}</div>
            </div>
            '''


def _render_apr_apy_card(item: dict, latest_map: dict[tuple[str, str], MetricSnapshot]) -> str:
    entity_id = item["entity_id"]
    apy_metric = latest_map.get((entity_id, item["metric_name"]))
    apr_metric = latest_map.get((entity_id, item["secondary_metric_name"]))

    apr_value = apr_metric.value if apr_metric else None
    apy_value = apy_metric.value if apy_metric else None

    if apr_value is None and apy_metric and apy_metric.source == "rpc:ethereum:apyx_rate_view":
        apr_value = apy_metric.value
    if apy_value is None and apr_value is not None:
        apy_value = _monthly_compounded_pct(apr_value)
    elif apy_metric and apy_metric.source == "rpc:ethereum:apyx_rate_view":
        apy_value = _monthly_compounded_pct(apy_metric.value)

    metric_times = [metric.recorded_at for metric in (apr_metric, apy_metric) if metric]
    recorded_at = f"{_format_dt(max(metric_times))} 北京时间" if metric_times else "-"
    apr_text = _format_value("underlying_apr", apr_value)
    apy_text = _format_value("underlying_apy", apy_value)

    return f'''
            <div class="card yield-card">
              <div class="label">{escape(item["label"])}</div>
              <div class="yield-pair">
                <div class="yield-stat">
                  <span>APR</span>
                  <strong>{escape(apr_text)}</strong>
                </div>
                <div class="yield-stat">
                  <span>APY</span>
                  <strong>{escape(apy_text)}</strong>
                </div>
              </div>
              <div class="meta">更新时间：{escape(recorded_at)}</div>
            </div>
            '''


def _render_threshold_controls(
        rule_map: dict[str, RuleDefinition],
        latest_map: dict[tuple[str, str], MetricSnapshot],
        hours: int,
        threshold_updated: bool,
) -> str:
        operator_label = {"lt": "低于", "lte": "低于等于", "gt": "高于", "gte": "高于等于"}
        rows = []
        for rule_id in THRESHOLD_RULE_IDS:
                rule = rule_map.get(rule_id)
                if rule is None:
                        continue
                metric = latest_map.get((rule.entity_id, rule.metric_name))
                current_value = metric.value if metric else None
                input_step = "1000" if rule.metric_name.endswith("_usd") else "0.1"
                input_min = "0"
                unit_label = "USD" if rule.metric_name.endswith("_usd") else "%"
                rows.append(
                        f'''
                        <form class="threshold-card" method="post" action="/dashboard/thresholds">
                            <input type="hidden" name="rule_id" value="{escape(rule.rule_id)}" />
                            <input type="hidden" name="hours" value="{hours}" />
                            <div class="threshold-title">{escape(rule.description)}</div>
                            <div class="threshold-meta">告警条件：当前值 {escape(operator_label.get(rule.comparator, rule.comparator))} 阈值时记录告警；仅闭环套利利润率规则发送飞书</div>
                            <div class="threshold-stats">
                                <div><span>当前值</span><strong>{escape(_format_value(rule.metric_name, current_value))}</strong></div>
                                <div><span>当前阈值</span><strong>{escape(_format_value(rule.metric_name, rule.threshold))}</strong></div>
                            </div>
                            <label class="threshold-input-group">
                                <span>新阈值（{unit_label}）</span>
                                <input name="threshold" type="number" min="{input_min}" step="{input_step}" value="{rule.threshold}" required />
                            </label>
                            <button type="submit">保存阈值</button>
                        </form>
                        '''
                )

        if not rows:
                return ""

        banner = '<div class="flash success">告警阈值已更新，后续采集会按新阈值触发；仅闭环套利利润率规则发送飞书。</div>' if threshold_updated else ""
        return f'''
        <div class="panel full threshold-panel">
            <div class="panel-head">
                <h3>风险监控 · 告警阈值</h3>
                <div class="legend">
                    <span class="legend-item">支持修改：可借款额、借款利率、Curve 偏离净值、Capped Ratio 脱锚、闭环套利利润率</span>
                </div>
            </div>
            {banner}
            <div class="threshold-grid">{"".join(rows)}</div>
        </div>
        '''


def _render_charts(session: Session, hours: int) -> str:
    bucket_minutes = 5 if hours <= 6 else 15 if hours <= 24 else 60
    panels = []
    for chart in CHART_DEFS:
        series_list = []
        chart_id = _slugify(chart["title"])
        legend_buttons = []
        for index, series in enumerate(chart["series"]):
            points = _bucket_series(session, series["entity_id"], series["metric_name"], hours, bucket_minutes)
            series_list.append({
                "label": series["label"],
                "color": series["color"],
                "points": points,
                "metric_name": series["metric_name"],
            })
            last_value = points[-1][1] if points else None
            legend_buttons.append(
                f'<button type="button" class="legend-chip active" data-chart-id="{chart_id}" data-series-index="{index}"><span class="legend-dot" style="background:{series["color"]}"></span><span>{escape(series["label"])}：{escape(_format_value(series["metric_name"], last_value))}</span></button>'
            )
        css_class = "panel full" if chart.get("full") else "panel"
        panels.append(
            f'''
            <div class="{css_class} chart-panel">
              <div class="panel-head">
                <h3>{escape(chart["title"])}</h3>
                <div class="panel-actions">
                  <div class="legend interactive-legend">{"".join(legend_buttons)}</div>
                  <div class="view-switch" data-chart-id="{chart_id}">
                    <button type="button" class="view-tab active" data-view="chart">图形</button>
                    <button type="button" class="view-tab" data-view="table">数据</button>
                  </div>
                </div>
              </div>
              <div class="chart-view active" data-view="chart">
                <div class="chart-wrap">{_build_svg(series_list, chart_id)}</div>
                <div class="chart-tooltip" hidden></div>
              </div>
              <div class="chart-view" data-view="table">{_build_chart_table(series_list)}</div>
            </div>
            '''
        )
    return "".join(panels)


def _render_alerts(session: Session) -> str:
    alerts = session.exec(
        select(AlertEvent)
        .where(AlertEvent.status == "firing")
        .order_by(AlertEvent.last_triggered_at.desc(), AlertEvent.id.desc())
        .limit(20)
    ).all()
    if not alerts:
        return '<tr><td colspan="6">当前无告警</td></tr>'
    rows = []
    for alert in alerts:
        rows.append(
            f'''
            <tr>
              <td><span class="badge {escape(alert.severity)}">{escape(alert.severity)}</span></td>
              <td>{escape(alert.entity_id)}</td>
              <td>{escape(alert.metric_name)}</td>
              <td>{escape(_format_value(alert.metric_name, alert.current_value))}</td>
              <td>{escape(alert.summary)}</td>
              <td>{escape(f"{_format_dt(alert.last_triggered_at)} 北京时间")}</td>
            </tr>
            '''
        )
    return "".join(rows)


def _render_morpho_history_table(
        session: Session,
        market_id: str,
        metrics: list[str],
        hours: int,
        bucket_minutes: int,
        limit: int = 8,
) -> str:
        timestamps: set[datetime] = set()
        series_map: dict[str, dict[datetime, float]] = {}
        for metric_name in metrics:
                points = _bucket_series(session, market_id, metric_name, hours, bucket_minutes)
                series_map[metric_name] = {timestamp: value for timestamp, value in points}
                timestamps.update(timestamp for timestamp, _ in points)

        if not timestamps:
                return '<tr><td colspan="99">暂无历史数据</td></tr>'

        rows = []
        ordered_timestamps = sorted(timestamps, reverse=True)[:limit]
        for timestamp in ordered_timestamps:
                cells = [f"<td>{escape(_format_dt(timestamp, '%m-%d %H:%M'))}</td>"]
                for metric_name in metrics:
                        value = series_map.get(metric_name, {}).get(timestamp)
                        cells.append(f"<td>{escape(_format_value(metric_name, value))}</td>")
                rows.append(f"<tr>{''.join(cells)}</tr>")
        return "".join(rows)


def _render_morpho_market_sections(session: Session, latest_map: dict[tuple[str, str], MetricSnapshot], hours: int) -> str:
    bucket_minutes = 5 if hours <= 6 else 15 if hours <= 24 else 60
    rows = []
    for market in MORPHO_MARKETS:
        borrow_apy = latest_map.get((market.market_id, "borrow_apy"))
        utilization = latest_map.get((market.market_id, "utilization_pct"))
        liquidity_chart_id = _slugify(f"{market.label}-liquidity")
        liquidity_series = [
            {
                "label": "可借款额",
                "color": "#60a5fa",
                "metric_name": "available_to_borrow_usd",
                "points": _bucket_series(session, market.market_id, "available_to_borrow_usd", hours, bucket_minutes),
            },
            {
                "label": "供给规模",
                "color": "#34d399",
                "metric_name": "supply_assets_usd",
                "points": _bucket_series(session, market.market_id, "supply_assets_usd", hours, bucket_minutes),
            },
            {
                "label": "借款规模",
                "color": "#f59e0b",
                "metric_name": "borrow_assets_usd",
                "points": _bucket_series(session, market.market_id, "borrow_assets_usd", hours, bucket_minutes),
            },
        ]
        liquidity_legend = []
        for index, series in enumerate(liquidity_series):
            last_value = series["points"][-1][1] if series["points"] else None
            liquidity_legend.append(
                f'<button type="button" class="legend-chip active" data-chart-id="{liquidity_chart_id}" data-series-index="{index}"><span class="legend-dot" style="background:{series["color"]}"></span><span>{escape(series["label"])}：{escape(_format_value(series["metric_name"], last_value))}</span></button>'
            )
        rows.append(
            f'''
            <div class="morpho-market-row">
                <div class="panel morpho-panel">
                    <div class="panel-head">
                        <h3>{escape(market.label)} · 利率 / 利用率</h3>
                        <div class="legend">
                            <span class="legend-item">当前借款利率：{escape(_format_value('borrow_apy', borrow_apy.value if borrow_apy else None))}</span>
                            <span class="legend-item">当前利用率：{escape(_format_value('utilization_pct', utilization.value if utilization else None))}</span>
                        </div>
                    </div>
                    <div class="table-wrap">
                        <table>
                            <thead>
                                <tr>
                                    <th>时间</th>
                                    <th>借款利率</th>
                                    <th>出借利率</th>
                                    <th>利用率</th>
                                </tr>
                            </thead>
                            <tbody>{_render_morpho_history_table(session, market.market_id, ['borrow_apy', 'supply_apy', 'utilization_pct'], hours, bucket_minutes)}</tbody>
                        </table>
                    </div>
                </div>
                <div class="panel morpho-panel chart-panel">
                    <div class="panel-head">
                        <h3>{escape(market.label)} · 可借款额趋势</h3>
                        <div class="panel-actions">
                            <div class="legend interactive-legend">{''.join(liquidity_legend)}</div>
                            <div class="view-switch" data-chart-id="{liquidity_chart_id}">
                                <button type="button" class="view-tab active" data-view="chart">图形</button>
                                <button type="button" class="view-tab" data-view="table">数据</button>
                            </div>
                        </div>
                    </div>
                    <div class="chart-view active" data-view="chart">
                        <div class="chart-wrap">{_build_svg(liquidity_series, liquidity_chart_id)}</div>
                        <div class="chart-tooltip" hidden></div>
                    </div>
                    <div class="chart-view" data-view="table">
                        {_build_chart_table(liquidity_series)}
                    </div>
                </div>
            </div>
            '''
        )
    return f'<div class="full morpho-section">{"".join(rows)}</div>'


def _metric_details(metric: MetricSnapshot | None) -> dict:
    if metric is None or not metric.details_json:
        return {}
    try:
        loaded = json.loads(metric.details_json)
    except json.JSONDecodeError:
        return {}
    return loaded if isinstance(loaded, dict) else {}


def _arbitrage_sample_entity_id(monitor_id: str, strategy_id: str, notional: float) -> str:
    notional_label = f"{int(notional)}" if float(notional).is_integer() else str(notional)
    return f"{monitor_id}-{strategy_id}-{notional_label}"


def _format_route_amount(value: object) -> str:
    if not isinstance(value, (int, float)):
        return "-"
    return f"{float(value):,.4f}"


def _short_address(value: object) -> str:
    if not isinstance(value, str) or len(value) < 12:
        return str(value or "-")
    return f"{value[:6]}...{value[-4:]}"


def _render_route_routing_summary(step: dict) -> str:
    routing = step.get("routing")
    if not isinstance(routing, dict):
        return ""

    provider = routing.get("provider")
    if provider == "derived_reverse_entry":
        return '<span class="arb-route-meta">derived from entry leg</span>'
    if provider == "derived_reverse":
        return '<span class="arb-route-meta">derived from reverse quote</span>'

    best_route = routing.get("best_route")
    if not isinstance(best_route, list) or not best_route:
        method = routing.get("contract_method") or provider
        return f'<span class="arb-route-meta">{escape(str(method))}</span>' if method else ""

    exchanges: list[str] = []
    pools: list[str] = []
    for route in best_route:
        if not isinstance(route, dict):
            continue
        for swap in route.get("swaps") or []:
            if not isinstance(swap, dict):
                continue
            for exchange in swap.get("swap_exchanges") or []:
                if not isinstance(exchange, dict):
                    continue
                exchange_name = exchange.get("exchange")
                if exchange_name and str(exchange_name) not in exchanges:
                    exchanges.append(str(exchange_name))
                for pool in exchange.get("pool_addresses") or []:
                    pool_label = _short_address(pool)
                    if pool_label not in pools:
                        pools.append(pool_label)

    parts = []
    if exchanges:
        parts.append(" / ".join(exchanges[:2]))
    if pools:
        parts.append("pool " + ", ".join(pools[:2]))
    method = routing.get("contract_method")
    if method:
        parts.append(str(method))
    gas_usd = routing.get("gas_cost_usd")
    if gas_usd:
        parts.append(f"gas ${gas_usd}")
    return f'<span class="arb-route-meta">{escape(" | ".join(parts))}</span>'


def _render_arbitrage_route_cards(steps: list) -> str:
    cards = []
    for step in steps:
        if not isinstance(step, dict):
            continue
        from_label = step.get("from_symbol") or step.get("from_asset", "-")
        to_label = step.get("to_symbol") or step.get("to_asset", "-")
        if step.get("type") == "swap":
            title = f'{step.get("chain", "-")} swap'
            body = (
                f'{from_label} {_format_route_amount(step.get("amount_in"))} '
                f'-> {to_label} {_format_route_amount(step.get("amount_out"))}'
            )
            meta = _render_route_routing_summary(step)
        elif step.get("type") == "bridge":
            title = f'bridge {step.get("from_chain", "-")} -> {step.get("to_chain", "-")}'
            body = (
                f'{from_label} {_format_route_amount(step.get("amount_in"))} '
                f'-> {to_label} {_format_route_amount(step.get("amount_out"))}'
            )
            cost = step.get("cost_usd")
            meta = (
                f'<span class="arb-route-meta">cost {escape(_format_value("total_cost_usd", cost))}</span>'
                if isinstance(cost, (int, float))
                else ""
            )
        else:
            continue
        cards.append(
            f'''
            <div class="arb-route-step {escape(str(step.get("type", "")))}">
              <div class="arb-route-dot"></div>
              <div class="arb-route-copy">
                <div class="arb-route-title">{escape(title)}</div>
                <div class="arb-route-body">{escape(body)}</div>
                {meta}
              </div>
            </div>
            '''
        )
    return '<div class="arb-route-flow">' + "".join(cards) + "</div>" if cards else "-"


def _render_arbitrage_route(details: dict) -> str:
    steps = details.get("route_steps")
    if not isinstance(steps, list) or not steps:
        return "-"
    return _render_arbitrage_route_cards(steps)
    labels = []
    for step in steps:
        if not isinstance(step, dict):
            continue
        from_label = step.get("from_symbol") or step.get("from_asset", "-")
        to_label = step.get("to_symbol") or step.get("to_asset", "-")
        if step.get("type") == "swap":
            labels.append(
                f'{step.get("chain", "-")}: {from_label} '
                f'{_format_route_amount(step.get("amount_in"))} → {to_label} '
                f'{_format_route_amount(step.get("amount_out"))}'
            )
        elif step.get("type") == "bridge":
            labels.append(
                f'bridge {step.get("from_chain", "-")} → {step.get("to_chain", "-")}: '
                f'{from_label} {_format_route_amount(step.get("amount_in"))}'
            )
    return '<span class="route-path">' + "</span><span class=\"route-separator\">→</span><span class=\"route-path\">".join(
        escape(label) for label in labels
    ) + "</span>"


def _render_arbitrage_section(latest_map: dict[tuple[str, str], MetricSnapshot]) -> str:
    best_profit = latest_map.get(("arb-apyusd-apxusd-crosschain", "best_net_profit_usd"))
    best_edge = latest_map.get(("arb-apyusd-apxusd-crosschain", "best_net_edge_pct"))
    best_notional = latest_map.get(("arb-apyusd-apxusd-crosschain", "best_notional_usd"))
    best_total_cost = None
    best_details = _metric_details(best_profit)
    best_label = best_details.get("strategy_label") or best_details.get("label", "-")
    sample_entity_id = best_details.get("sample_entity_id")
    if isinstance(sample_entity_id, str):
        best_total_cost = latest_map.get((sample_entity_id, "total_cost_usd"))
    best_updated_at = f"{_format_dt(best_profit.recorded_at)} 北京时间" if best_profit else "-"
    rows = []

    for monitor in ARBITRAGE_MONITORS:
        for notional in monitor.notionals_usd:
            for strategy_id in ARBITRAGE_STRATEGY_IDS:
                entity_id = _arbitrage_sample_entity_id(monitor.monitor_id, strategy_id, float(notional))
                net_profit = latest_map.get((entity_id, "net_profit_usd"))
                if net_profit is None:
                    continue
                net_edge = latest_map.get((entity_id, "net_edge_pct"))
                gross_profit = latest_map.get((entity_id, "gross_profit_usd"))
                final_apxusd = latest_map.get((entity_id, "final_apxusd"))
                sold_apxusd = latest_map.get((entity_id, "sold_apxusd")) or latest_map.get((entity_id, "target_apxusd"))
                final_usdc = latest_map.get((entity_id, "final_usdc"))
                total_cost = latest_map.get((entity_id, "total_cost_usd"))
                details = _metric_details(net_profit)
                updated_at = net_profit.recorded_at if net_profit else None
                row_class = "positive" if net_profit and net_profit.value > 0 else "negative"
                strategy_label = details.get("strategy_label") or strategy_id
                sold_apxusd_value = sold_apxusd.value if sold_apxusd else (final_apxusd.value if final_apxusd else None)
                route_markup = _render_arbitrage_route(details)
                row_html = f'''
                    <tr class="{row_class}">
                      <td>{escape(details.get("label", monitor.label))}</td>
                      <td>{escape(str(strategy_label))}</td>
                      <td>{escape(_format_value("best_notional_usd", float(notional)))}</td>
                      <td>{route_markup}</td>
                      <td>{escape(_format_value("net_profit_usd", net_profit.value if net_profit else None))}</td>
                      <td>{escape(_format_value("net_edge_pct", net_edge.value if net_edge else None))}</td>
                      <td>{escape(_format_value("gross_profit_usd", gross_profit.value if gross_profit else None))}</td>
                      <td>{escape(_format_value("sold_apxusd", sold_apxusd_value))}</td>
                      <td>{escape(_format_value("final_usdc", final_usdc.value if final_usdc else None))}</td>
                      <td>{escape(_format_value("total_cost_usd", total_cost.value if total_cost else None))}</td>
                      <td>{escape(f"{_format_dt(updated_at)} 北京时间" if updated_at else "-")}</td>
                    </tr>
                    '''
                rows.append((net_profit.value, row_html))

    sorted_rows = [row_html for _, row_html in sorted(rows, key=lambda row: row[0], reverse=True)]
    table_body = "".join(sorted_rows) if sorted_rows else '<tr><td colspan="11">暂无套利报价</td></tr>'
    return f'''
    <div class="panel full arbitrage-panel">
      <div class="panel-head">
        <div>
          <h3>闭环跨链套利监控 · apyUSD / apxUSD</h3>
          <p class="panel-subtitle">以 Ethereum USDC 为本金和最终结算资产；先用 USDC 买入 apxUSD，完成跨链 apyUSD / apxUSD 闭环后再换回 USDC 计算净利润。</p>
        </div>
        <div class="legend">
          <span class="legend-item">最佳策略：{escape(str(best_label))}</span>
          <span class="legend-item">最佳本金：{escape(_format_value("best_notional_usd", best_notional.value if best_notional else None))}</span>
          <span class="legend-item">更新时间：{escape(best_updated_at)}</span>
        </div>
      </div>
      <div class="arb-summary">
        <div><span>最佳净利润</span><strong>{escape(_format_value("best_net_profit_usd", best_profit.value if best_profit else None))}</strong></div>
        <div><span>最佳净利率</span><strong>{escape(_format_value("best_net_edge_pct", best_edge.value if best_edge else None))}</strong></div>
        <div><span>闭环总成本</span><strong>{escape(_format_value("total_cost_usd", best_total_cost.value if best_total_cost else None))}</strong></div>
      </div>
      <div class="trend-table-wrap">
        <table class="trend-table">
          <thead>
            <tr>
              <th>方向</th>
              <th>策略</th>
              <th>本金</th>
              <th>完整路径</th>
              <th>净利润</th>
              <th>净利率</th>
              <th>毛利润</th>
              <th>卖出得到 apxUSD</th>
              <th>最终 ETH USDC</th>
              <th>成本</th>
              <th>更新时间</th>
            </tr>
          </thead>
          <tbody>{table_body}</tbody>
        </table>
      </div>
    </div>
    '''


def _render_morpho_market_table(latest_map: dict[tuple[str, str], MetricSnapshot]) -> str:
    rows = []
    for market in MORPHO_MARKETS:
        available = latest_map.get((market.market_id, "available_to_borrow_usd"))
        borrow_apy = latest_map.get((market.market_id, "borrow_apy"))
        supply_apy = latest_map.get((market.market_id, "supply_apy"))
        utilization = latest_map.get((market.market_id, "utilization_pct"))
        supply_usd = latest_map.get((market.market_id, "supply_assets_usd"))
        borrow_usd = latest_map.get((market.market_id, "borrow_assets_usd"))
        updated_at = max(
            [metric.recorded_at for metric in [available, borrow_apy, supply_apy, utilization, supply_usd, borrow_usd] if metric],
            default=None,
        )
        rows.append(
            f'''
            <tr>
              <td>{escape(market.label)}</td>
              <td>{escape(_format_value("available_to_borrow_usd", available.value if available else None))}</td>
              <td>{escape(_format_value("borrow_apy", borrow_apy.value if borrow_apy else None))}</td>
              <td>{escape(_format_value("supply_apy", supply_apy.value if supply_apy else None))}</td>
              <td>{escape(_format_value("utilization_pct", utilization.value if utilization else None))}</td>
              <td>{escape(_format_value("supply_assets_usd", supply_usd.value if supply_usd else None))}</td>
              <td>{escape(_format_value("borrow_assets_usd", borrow_usd.value if borrow_usd else None))}</td>
              <td>{escape(f"{_format_dt(updated_at)} 北京时间" if updated_at else "-")}</td>
            </tr>
            '''
        )
    return "".join(rows)


def _render_login_page(next_url: str, failed: bool = False) -> str:
        error = '<div class="error">账号或密码错误</div>' if failed else ""
        return f"""
<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>APYX Monitor 登录</title>
    <style>
        :root {{ color-scheme: dark; }}
        * {{ box-sizing: border-box; }}
        body {{ margin: 0; min-height: 100vh; display: grid; place-items: center; font-family: Inter, "Segoe UI", Arial, sans-serif; background: radial-gradient(circle at top, rgba(96,165,250,0.2), transparent 34%), linear-gradient(180deg, #0b1020 0%, #0f172a 100%); color: #eef2ff; }}
        .login-card {{ width: min(420px, calc(100vw - 32px)); padding: 28px; border: 1px solid rgba(148,163,184,0.18); border-radius: 22px; background: rgba(17,24,45,0.94); box-shadow: 0 24px 70px rgba(0,0,0,0.34); }}
        h1 {{ margin: 0 0 8px; font-size: 26px; }}
        p {{ margin: 0 0 24px; color: #94a3b8; line-height: 1.6; }}
        label {{ display: flex; flex-direction: column; gap: 8px; color: #cbd5e1; font-size: 13px; margin-bottom: 16px; }}
        input {{ width: 100%; border: 1px solid rgba(148,163,184,0.22); border-radius: 14px; padding: 12px 14px; background: rgba(15,23,42,0.76); color: #eef2ff; font-size: 15px; outline: none; }}
        input:focus {{ border-color: rgba(96,165,250,0.7); box-shadow: 0 0 0 3px rgba(96,165,250,0.14); }}
        button {{ width: 100%; margin-top: 6px; border: 0; border-radius: 14px; padding: 12px 16px; background: linear-gradient(135deg, #2563eb, #14b8a6); color: white; font-size: 15px; font-weight: 700; cursor: pointer; }}
        .error {{ margin-bottom: 16px; padding: 10px 12px; border-radius: 12px; background: rgba(248,113,113,0.13); border: 1px solid rgba(248,113,113,0.32); color: #fecaca; font-size: 13px; }}
    </style>
</head>
<body>
    <form class="login-card" method="post" action="/dashboard/login">
        <h1>APYX Monitor</h1>
        <p>请输入账号密码访问监控面板。</p>
        {error}
        <input type="hidden" name="next_url" value="{escape(next_url)}" />
        <label>
            <span>账号</span>
            <input name="username" autocomplete="username" required autofocus />
        </label>
        <label>
            <span>密码</span>
            <input name="password" type="password" autocomplete="current-password" required />
        </label>
        <button type="submit">登录</button>
    </form>
</body>
</html>
        """


def _dashboard_status_text(latest_map: dict[tuple[str, str], MetricSnapshot]) -> str:
    latest_run = max((row.recorded_at for row in latest_map.values()), default=None)
    return f"最近数据：{_format_dt(latest_run)} 北京时间" if latest_run else "暂无数据"


def _render_hour_options(hours: int) -> str:
    return "".join(
        f'<option value="{value}" {"selected" if value == hours else ""}>近 {label}</option>'
        for value, label in ((6, "6 小时"), (24, "24 小时"), (72, "72 小时"), (168, "7 天"))
    )


def _render_dashboard_data(
    session: Session,
    latest_map: dict[tuple[str, str], MetricSnapshot],
    rule_map: dict[str, RuleDefinition],
    hours: int,
    threshold_updated: bool,
) -> str:
    return f"""
    <div class="cards">{_render_cards(session, latest_map)}</div>

    <div class="grid">
            {_render_arbitrage_section(latest_map)}
            {_render_threshold_controls(rule_map, latest_map, hours, threshold_updated)}
            {_render_charts(session, hours)}
            {_render_morpho_market_sections(session, latest_map, hours)}
            <div class="panel full">
                <h3>Morpho 池子状态</h3>
                <table>
                    <thead>
                        <tr>
                            <th>池子</th>
                            <th>可借款额</th>
                            <th>借款利率</th>
                            <th>出借利率</th>
                            <th>利用率</th>
                            <th>供给规模</th>
                            <th>借款规模</th>
                            <th>更新时间</th>
                        </tr>
                    </thead>
                    <tbody>{_render_morpho_market_table(latest_map)}</tbody>
                </table>
            </div>
      <div class="panel full">
        <h3>当前告警</h3>
        <table>
          <thead>
            <tr>
              <th>级别</th>
              <th>对象</th>
              <th>指标</th>
              <th>当前值</th>
              <th>摘要</th>
              <th>最近触发</th>
            </tr>
          </thead>
          <tbody>{_render_alerts(session)}</tbody>
        </table>
      </div>
    </div>
    """


@router.get("/dashboard/login", response_class=HTMLResponse)
def login_page(next_url: str = Query(default="/dashboard", alias="next"), failed: int = Query(default=0)) -> str:
        return _render_login_page(next_url, bool(failed))


@router.post("/dashboard/login")
def login(
        username: str = Form(...),
        password: str = Form(...),
        next_url: str = Form(default="/dashboard"),
):
        settings = get_settings()
        valid_username = hmac.compare_digest(username, settings.dashboard_username)
        valid_password = hmac.compare_digest(password, settings.dashboard_password)
        if not (valid_username and valid_password):
            return RedirectResponse(url=f"/dashboard/login?failed=1&next={quote(next_url, safe='')}", status_code=303)

        safe_next_url = next_url if next_url.startswith("/") and not next_url.startswith("//") else "/dashboard"
        response = RedirectResponse(url=safe_next_url, status_code=303)
        response.set_cookie(
                key=SESSION_COOKIE_NAME,
                value=_create_session_token(username, settings),
                max_age=settings.dashboard_session_ttl_seconds,
                httponly=True,
                samesite="lax",
                secure=settings.app_env.lower() in {"prod", "production"},
        )
        return response


@router.post("/dashboard/logout")
def logout():
        response = RedirectResponse(url="/dashboard/login", status_code=303)
        response.delete_cookie(SESSION_COOKIE_NAME)
        return response


@router.post("/dashboard/thresholds")
def update_threshold(
    request: Request,
    rule_id: str = Form(...),
    threshold: float = Form(...),
    hours: int = Form(default=24),
    session: Session = Depends(get_session),
):
    _require_dashboard_auth(request)
    rule_map = _effective_rule_map(session)
    if rule_id not in THRESHOLD_RULE_IDS or rule_id not in rule_map:
        return RedirectResponse(url=f"/dashboard?hours={hours}", status_code=303)

    override = session.exec(select(AlertRuleOverride).where(AlertRuleOverride.rule_id == rule_id)).first()
    if override is None:
        session.add(AlertRuleOverride(rule_id=rule_id, threshold=threshold, updated_at=utc_now()))
    else:
        override.threshold = threshold
        override.updated_at = utc_now()
    session.commit()
    return RedirectResponse(url=f"/dashboard?hours={hours}&threshold_updated=1", status_code=303)


@router.get("/dashboard/fragment", response_class=HTMLResponse)
def dashboard_fragment(
    request: Request,
    hours: int = Query(default=24, ge=1, le=24 * 30),
    session: Session = Depends(get_session),
) -> str:
    _require_dashboard_auth(request)
    latest_map = _latest_metric_map(session)
    rule_map = _effective_rule_map(session)
    return f"""
    <template data-dashboard-status="{escape(_dashboard_status_text(latest_map), quote=True)}"></template>
    {_render_dashboard_data(session, latest_map, rule_map, hours, False)}
    """


@router.get("/dashboard", response_class=HTMLResponse)
def dashboard(
    request: Request,
    hours: int = Query(default=24, ge=1, le=24 * 30),
    threshold_updated: int = Query(default=0),
    session: Session = Depends(get_session),
) -> str:
    _require_dashboard_auth(request)
    latest_map = _latest_metric_map(session)
    rule_map = _effective_rule_map(session)
    status_text = _dashboard_status_text(latest_map)
    hour_options = _render_hour_options(hours)
    dashboard_data = _render_dashboard_data(session, latest_map, rule_map, hours, bool(threshold_updated))

    return f"""
<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>APYX Monitor Dashboard</title>
  <style>
    :root {{
      color-scheme: dark;
      --bg: #0b1020;
      --panel: #11182d;
      --panel-2: #17203a;
      --text: #eef2ff;
      --muted: #94a3b8;
      --border: rgba(148, 163, 184, 0.18);
    }}
    * {{ box-sizing: border-box; }}
    body {{ margin: 0; font-family: Inter, "Segoe UI", Arial, sans-serif; background: linear-gradient(180deg, #0b1020 0%, #0f172a 100%); color: var(--text); }}
    .wrap {{ max-width: 1400px; margin: 0 auto; padding: 24px; }}
    .header {{ display: flex; justify-content: space-between; align-items: center; gap: 16px; margin-bottom: 20px; }}
    .title h1 {{ margin: 0 0 8px; font-size: 28px; }}
    .title p {{ margin: 0; color: var(--muted); }}
    .actions {{ display: flex; gap: 12px; align-items: center; }}
    button, select {{ background: var(--panel-2); color: var(--text); border: 1px solid var(--border); border-radius: 12px; padding: 10px 14px; cursor: pointer; }}
    .status {{ padding: 10px 14px; border-radius: 12px; background: var(--panel); border: 1px solid var(--border); color: var(--muted); }}
    .status.loading {{ color: #bfdbfe; border-color: rgba(96,165,250,0.35); }}
    .status.error {{ color: #fecaca; border-color: rgba(248,113,113,0.35); }}
    .cards {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 16px; margin-bottom: 20px; }}
    .card, .panel {{ background: rgba(17, 24, 45, 0.92); border: 1px solid var(--border); border-radius: 18px; padding: 18px; box-shadow: 0 10px 30px rgba(0, 0, 0, 0.18); }}
    .card .label {{ color: var(--muted); font-size: 13px; margin-bottom: 8px; }}
    .card .value {{ font-size: 28px; font-weight: 700; margin-bottom: 4px; }}
    .card .meta {{ color: var(--muted); font-size: 12px; }}
    .card .delta {{ color: #cbd5e1; font-size: 12px; margin-bottom: 6px; }}
    .yield-pair {{ display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 10px; margin: 8px 0; }}
    .yield-stat {{ min-width: 0; padding: 10px 12px; border-radius: 12px; border: 1px solid rgba(148,163,184,0.12); background: rgba(15,23,42,0.48); }}
    .yield-stat span {{ display: block; color: var(--muted); font-size: 11px; margin-bottom: 5px; }}
    .yield-stat strong {{ display: block; font-size: 22px; line-height: 1.1; white-space: nowrap; }}
    .strategy-card {{ min-width: 280px; }}
    .strategy-grid {{ display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 8px; margin: 10px 0 8px; }}
    .strategy-grid div {{ min-width: 0; padding: 9px 10px; border-radius: 12px; border: 1px solid rgba(148,163,184,0.12); background: rgba(15,23,42,0.48); }}
    .strategy-grid span {{ display: block; color: var(--muted); font-size: 11px; margin-bottom: 5px; white-space: nowrap; }}
    .strategy-grid strong {{ display: block; color: #e2e8f0; font-size: 15px; line-height: 1.15; overflow-wrap: anywhere; }}
    .grid {{ display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 16px; }}
    .threshold-grid {{ display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 16px; }}
    .threshold-card {{ display: flex; flex-direction: column; gap: 12px; padding: 16px; border-radius: 16px; border: 1px solid rgba(148,163,184,0.16); background: rgba(15,23,42,0.45); }}
    .threshold-title {{ font-size: 15px; font-weight: 700; }}
    .threshold-meta {{ color: var(--muted); font-size: 12px; line-height: 1.5; }}
    .threshold-stats {{ display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 10px; }}
    .threshold-stats div {{ padding: 12px; border-radius: 12px; background: rgba(15,23,42,0.55); border: 1px solid rgba(148,163,184,0.12); }}
    .threshold-stats span {{ display: block; color: var(--muted); font-size: 12px; margin-bottom: 4px; }}
    .threshold-stats strong {{ font-size: 16px; }}
    .threshold-input-group {{ display: flex; flex-direction: column; gap: 8px; color: var(--muted); font-size: 12px; }}
    .threshold-input-group input {{ width: 100%; }}
    .flash {{ margin-bottom: 14px; padding: 10px 12px; border-radius: 12px; font-size: 13px; }}
    .flash.success {{ background: rgba(52, 211, 153, 0.12); border: 1px solid rgba(52, 211, 153, 0.28); color: #a7f3d0; }}
    .panel-head {{ display: flex; justify-content: space-between; align-items: flex-start; gap: 12px; margin-bottom: 12px; }}
    .panel-actions {{ display: flex; flex-direction: column; align-items: flex-end; gap: 10px; max-width: 72%; }}
    .panel h3 {{ margin: 0; font-size: 16px; }}
    .panel-subtitle {{ margin: 6px 0 0; color: var(--muted); font-size: 12px; line-height: 1.5; }}
    .arb-summary {{ display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 12px; margin-bottom: 14px; }}
    .arb-summary div {{ padding: 12px; border-radius: 12px; background: rgba(15,23,42,0.55); border: 1px solid rgba(148,163,184,0.12); }}
    .arb-summary span {{ display: block; color: var(--muted); font-size: 12px; margin-bottom: 5px; }}
    .arb-summary strong {{ font-size: 20px; }}
    .arbitrage-panel table {{ min-width: 1320px; }}
    .arbitrage-panel th:nth-child(4), .arbitrage-panel td:nth-child(4) {{ width: 430px; }}
    .arb-route-flow {{ display: flex; flex-direction: column; gap: 7px; min-width: 360px; max-width: 520px; }}
    .arb-route-step {{ position: relative; display: grid; grid-template-columns: 14px minmax(0, 1fr); gap: 8px; padding: 8px 10px; border-radius: 10px; background: rgba(15,23,42,0.45); border: 1px solid rgba(148,163,184,0.12); }}
    .arb-route-step::before {{ content: ""; position: absolute; left: 16px; top: -8px; width: 1px; height: 8px; background: rgba(148,163,184,0.2); }}
    .arb-route-step:first-child::before {{ display: none; }}
    .arb-route-dot {{ width: 9px; height: 9px; margin-top: 5px; border-radius: 999px; background: #38bdf8; box-shadow: 0 0 0 3px rgba(56,189,248,0.14); }}
    .arb-route-step.bridge .arb-route-dot {{ background: #fbbf24; box-shadow: 0 0 0 3px rgba(251,191,36,0.14); }}
    .arb-route-copy {{ min-width: 0; }}
    .arb-route-title {{ color: #e2e8f0; font-size: 11px; font-weight: 700; text-transform: uppercase; letter-spacing: 0; margin-bottom: 3px; }}
    .arb-route-body {{ color: #cbd5e1; font-size: 12px; line-height: 1.35; overflow-wrap: anywhere; }}
    .arb-route-meta {{ display: block; color: #94a3b8; font-size: 11px; line-height: 1.35; margin-top: 3px; overflow-wrap: anywhere; }}
    .arbitrage-panel tr.positive td:nth-child(5), .arbitrage-panel tr.positive td:nth-child(6) {{ color: #86efac; }}
    .arbitrage-panel tr.negative td:nth-child(5), .arbitrage-panel tr.negative td:nth-child(6) {{ color: #fecaca; }}
    .chart-panel {{ overflow: hidden; }}
    .morpho-section {{ display: flex; flex-direction: column; gap: 16px; }}
    .morpho-market-row {{ display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 16px; align-items: stretch; }}
    .morpho-panel {{ height: 100%; display: flex; flex-direction: column; }}
    .table-wrap {{ flex: 1; overflow: auto; }}
    .chart-wrap {{ height: 280px; width: 100%; position: relative; border-radius: 14px; background: radial-gradient(circle at top left, rgba(96,165,250,0.06), transparent 35%), linear-gradient(180deg, rgba(255,255,255,0.02), rgba(255,255,255,0.01)); padding: 4px; }}
    .chart-svg {{ width: 100%; height: 100%; display: block; }}
    .legend {{ display: flex; gap: 12px; flex-wrap: wrap; justify-content: flex-end; }}
    .legend-item {{ color: var(--muted); font-size: 12px; display: inline-flex; align-items: center; gap: 6px; }}
    .legend-dot {{ width: 10px; height: 10px; border-radius: 999px; display: inline-block; }}
    .interactive-legend {{ gap: 8px; }}
    .legend-chip {{ display: inline-flex; align-items: center; gap: 8px; border-radius: 999px; border: 1px solid rgba(148,163,184,0.18); background: rgba(15,23,42,0.55); color: var(--text); padding: 6px 10px; font-size: 12px; line-height: 1.3; transition: all 0.2s ease; }}
    .legend-chip.active {{ border-color: rgba(96,165,250,0.4); background: rgba(30,41,59,0.9); }}
    .legend-chip.inactive {{ opacity: 0.45; }}
    .view-switch {{ display: inline-flex; align-items: center; background: rgba(15,23,42,0.72); border: 1px solid rgba(148,163,184,0.16); border-radius: 999px; padding: 3px; }}
    .view-tab {{ background: transparent; border: 0; border-radius: 999px; padding: 6px 12px; color: var(--muted); font-size: 12px; }}
    .view-tab.active {{ background: rgba(96,165,250,0.18); color: var(--text); }}
    .chart-view {{ display: none; }}
    .chart-view.active {{ display: block; }}
    .trend-table-wrap {{ max-height: 320px; overflow: auto; border: 1px solid rgba(148,163,184,0.12); border-radius: 14px; }}
    .trend-table thead th {{ position: sticky; top: 0; background: #121a31; z-index: 1; }}
    .trend-table tbody tr:nth-child(odd) {{ background: rgba(255,255,255,0.015); }}
    .trend-table tbody tr:hover {{ background: rgba(96,165,250,0.08); }}
    .point-node {{ cursor: pointer; transition: r 0.16s ease, opacity 0.16s ease; }}
    .point-node:hover {{ r: 5; }}
    .series-path, .series-area {{ transition: opacity 0.18s ease; }}
    .chart-tooltip {{ position: absolute; min-width: 140px; max-width: 220px; pointer-events: none; background: rgba(15,23,42,0.95); border: 1px solid rgba(96,165,250,0.25); box-shadow: 0 12px 30px rgba(2, 6, 23, 0.45); border-radius: 12px; padding: 10px 12px; color: var(--text); font-size: 12px; line-height: 1.5; z-index: 10; }}
    .chart-tooltip .muted {{ color: var(--muted); }}
    .full {{ grid-column: 1 / -1; }}
    table {{ width: 100%; border-collapse: collapse; font-size: 14px; }}
    th, td {{ padding: 10px 8px; border-bottom: 1px solid var(--border); text-align: left; vertical-align: top; }}
    th {{ color: var(--muted); font-weight: 500; }}
    .badge {{ display: inline-flex; padding: 4px 8px; border-radius: 999px; font-size: 12px; font-weight: 700; }}
    .badge.P1 {{ background: rgba(248, 113, 113, 0.16); color: #fca5a5; }}
    .badge.P2 {{ background: rgba(251, 191, 36, 0.16); color: #fcd34d; }}
    .badge.P3 {{ background: rgba(96, 165, 250, 0.16); color: #93c5fd; }}
        .empty {{ height: 100%; display: flex; align-items: center; justify-content: center; color: var(--muted); background: linear-gradient(180deg, rgba(255,255,255,0.02), rgba(255,255,255,0.01)); border-radius: 12px; }}
        .empty.small {{ min-height: 180px; }}
    @media (max-width: 960px) {{
      .grid {{ grid-template-columns: 1fr; }}
            .threshold-grid {{ grid-template-columns: 1fr; }}
            .morpho-market-row {{ grid-template-columns: 1fr; }}
      .header {{ flex-direction: column; align-items: flex-start; }}
      .panel-head {{ flex-direction: column; }}
            .panel-actions {{ align-items: flex-start; max-width: 100%; }}
      .legend {{ justify-content: flex-start; }}
      .arb-summary {{ grid-template-columns: 1fr; }}
    }}
  </style>
</head>
<body>
  <div class="wrap">
    <div class="header">
      <div class="title">
        <h1>APYX Monitor Dashboard</h1>
        <p>APYX / Pendle / Morpho 实时监控面板</p>
      </div>
      <form class="actions" method="get" action="/dashboard">
        <select name="hours">{hour_options}</select>
        <button type="submit" id="dashboard-refresh-button">刷新</button>
                <button type="submit" formmethod="post" formaction="/dashboard/logout">退出</button>
        <div class="status" id="dashboard-status">{escape(status_text)}</div>
      </form>
    </div>

    <div id="dashboard-data">{dashboard_data}</div>
  </div>
    <script>
        (() => {{
            const refreshForm = document.querySelector('.actions');
            const refreshButton = document.getElementById('dashboard-refresh-button');
            const statusNode = document.getElementById('dashboard-status');
            const dataNode = document.getElementById('dashboard-data');
            let refreshInFlight = false;
            let refreshTimer = null;

            const wireViewTabs = () => {{
                document.querySelectorAll('.chart-panel').forEach((panel) => {{
                    const switcher = panel.querySelector('.view-switch');
                    if (!switcher) return;
                    switcher.querySelectorAll('.view-tab').forEach((button) => {{
                        button.addEventListener('click', () => {{
                            const view = button.dataset.view;
                            switcher.querySelectorAll('.view-tab').forEach((tab) => tab.classList.toggle('active', tab === button));
                            panel.querySelectorAll('.chart-view').forEach((node) => node.classList.toggle('active', node.dataset.view === view));
                        }});
                    }});
                }});
            }};

            const wireLegends = () => {{
                document.querySelectorAll('.legend-chip').forEach((chip) => {{
                    chip.addEventListener('click', () => {{
                        chip.classList.toggle('active');
                        chip.classList.toggle('inactive', !chip.classList.contains('active'));
                        const chartId = chip.dataset.chartId;
                        const seriesIndex = chip.dataset.seriesIndex;
                        document.querySelectorAll(`[data-chart-id="${{chartId}}"] [data-series-index="${{seriesIndex}}"]`).forEach((node) => {{
                            node.style.opacity = chip.classList.contains('active') ? '1' : '0.08';
                        }});
                    }});
                }});
            }};

            const wireTooltips = () => {{
                document.querySelectorAll('.chart-panel').forEach((panel) => {{
                    const tooltip = panel.querySelector('.chart-tooltip');
                    if (!tooltip) return;
                    panel.querySelectorAll('.point-node').forEach((point) => {{
                        point.addEventListener('mouseenter', (event) => {{
                            tooltip.hidden = false;
                            tooltip.innerHTML = `<div><strong>${{point.dataset.seriesLabel}}</strong></div><div>${{point.dataset.value}}</div><div class="muted">${{point.dataset.label}}</div>`;
                            const panelRect = panel.getBoundingClientRect();
                            tooltip.style.left = `${{event.clientX - panelRect.left + 14}}px`;
                            tooltip.style.top = `${{event.clientY - panelRect.top - 8}}px`;
                        }});
                        point.addEventListener('mousemove', (event) => {{
                            const panelRect = panel.getBoundingClientRect();
                            tooltip.style.left = `${{event.clientX - panelRect.left + 14}}px`;
                            tooltip.style.top = `${{event.clientY - panelRect.top - 8}}px`;
                        }});
                        point.addEventListener('mouseleave', () => {{
                            tooltip.hidden = true;
                        }});
                    }});
                }});
            }};

            const wireDashboardInteractions = () => {{
                wireViewTabs();
                wireLegends();
                wireTooltips();
            }};

            const setStatus = (text, state) => {{
                statusNode.textContent = text;
                statusNode.classList.toggle('loading', state === 'loading');
                statusNode.classList.toggle('error', state === 'error');
            }};

            const refreshDashboard = async () => {{
                if (refreshInFlight) return;
                refreshInFlight = true;
                refreshButton.disabled = true;
                const previousStatus = statusNode.textContent;
                setStatus('刷新中...', 'loading');
                const hours = refreshForm.querySelector('select[name="hours"]').value;
                const url = `/dashboard/fragment?hours=${{encodeURIComponent(hours)}}`;

                try {{
                    const response = await fetch(url, {{
                        headers: {{ 'X-Requested-With': 'fetch' }},
                        cache: 'no-store',
                        credentials: 'same-origin',
                    }});
                    if (response.redirected || response.status === 401 || response.url.includes('/dashboard/login')) {{
                        window.location.href = '/dashboard/login?next=' + encodeURIComponent(window.location.pathname + window.location.search);
                        return;
                    }}
                    if (!response.ok) {{
                        throw new Error(`HTTP ${{response.status}}`);
                    }}
                    const contentType = response.headers.get('content-type') || '';
                    if (!contentType.includes('text/html')) {{
                        throw new Error('unexpected response');
                    }}
                    const html = await response.text();
                    const parser = new DOMParser();
                    const doc = parser.parseFromString(html, 'text/html');
                    const statusTemplate = doc.querySelector('template[data-dashboard-status]');
                    dataNode.innerHTML = html;
                    setStatus(statusTemplate?.dataset.dashboardStatus || previousStatus, '');
                    const currentUrl = new URL(window.location.href);
                    currentUrl.searchParams.set('hours', hours);
                    currentUrl.searchParams.delete('threshold_updated');
                    window.history.replaceState(null, '', currentUrl);
                    wireDashboardInteractions();
                }} catch (error) {{
                    setStatus(`刷新失败，保留当前页面：${{error.message}}`, 'error');
                }} finally {{
                    refreshInFlight = false;
                    refreshButton.disabled = false;
                }}
            }};

            refreshForm.addEventListener('submit', (event) => {{
                const submitter = event.submitter;
                if (submitter && submitter.formAction.endsWith('/dashboard/logout')) return;
                event.preventDefault();
                refreshDashboard();
            }});

            refreshForm.querySelector('select[name="hours"]').addEventListener('change', refreshDashboard);

            refreshTimer = window.setInterval(refreshDashboard, 60000);
            window.addEventListener('beforeunload', () => {{
                if (refreshTimer) window.clearInterval(refreshTimer);
            }});

            wireDashboardInteractions();
        }})();
    </script>
</body>
</html>
    """

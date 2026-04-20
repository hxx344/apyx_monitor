from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta, timezone
from html import escape
import re

from fastapi import APIRouter, Depends, Form, Query
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlmodel import Session, select

from ..config import RuleDefinition, get_asset_catalog, get_rule_catalog
from ..db import get_session
from ..models import AlertEvent, AlertRuleOverride, MetricSnapshot, utc_now

router = APIRouter(tags=["dashboard"])

CARD_DEFS = [
    {"entity_id": "apxusd", "metric_name": "tvl_usd", "label": "apxUSD TVL"},
    {"entity_id": "apyusd", "metric_name": "tvl_usd", "label": "apyUSD TVL"},
    {"entity_id": "apyusd", "metric_name": "underlying_apy", "label": "apyUSD 底层 APY"},
    {"entity_id": "yt-apxusd", "metric_name": "implied_apy", "label": "YT-apxUSD 隐含 APY"},
    {"entity_id": "yt-apyusd", "metric_name": "implied_apy", "label": "YT-apyUSD 隐含 APY"},
    {"entity_id": "morpho-apyusd-apxusd", "metric_name": "available_to_borrow_usd", "label": "Morpho 可借款额"},
    {"entity_id": "morpho-pt-apyusd-usdc", "metric_name": "available_to_borrow_usd", "label": "PT-apyUSD/USDC 可借款额"},
]

MORPHO_MARKETS = [market for market in get_asset_catalog().morpho_markets if market.enabled]

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
        "title": "YT 隐含 APY 趋势",
        "series": [
            {"entity_id": "yt-apxusd", "metric_name": "implied_apy", "label": "YT-apxUSD 隐含 APY", "color": "#a78bfa"},
            {"entity_id": "yt-apyusd", "metric_name": "implied_apy", "label": "YT-apyUSD 隐含 APY", "color": "#fb7185"},
        ],
    },
]

THRESHOLD_RULE_IDS = [
    "morpho_pt_apyusd_usdc_available_borrow_floor",
    "morpho_pt_apyusd_usdc_borrow_apy_ceiling",
]


def _format_value(metric_name: str, value: float | None) -> str:
    if value is None:
        return "-"
    if metric_name.endswith("_usd") or metric_name == "price_usd":
        return f"${value:,.0f}" if abs(value) >= 100 else f"${value:,.4f}"
    if metric_name.endswith("_apy") or metric_name.endswith("_pct"):
        return f"{value:.2f}%"
    return f"{value:,.4f}"


def _latest_metric_map(session: Session) -> dict[tuple[str, str], MetricSnapshot]:
    rows = session.exec(select(MetricSnapshot)).all()
    latest: dict[tuple[str, str], MetricSnapshot] = {}
    for row in sorted(rows, key=lambda item: item.recorded_at, reverse=True):
        key = (row.entity_id, row.metric_name)
        if key not in latest:
            latest[key] = row
    return latest


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
    ).all()
    buckets: dict[datetime, list[MetricSnapshot]] = defaultdict(list)
    interval_seconds = bucket_minutes * 60
    for row in sorted(rows, key=lambda item: item.recorded_at):
        bucket_ts = int(row.recorded_at.timestamp() // interval_seconds * interval_seconds)
        bucket_at = datetime.fromtimestamp(bucket_ts, tz=timezone.utc)
        buckets[bucket_at].append(row)
    return [(bucket_at, bucket_rows[-1].value) for bucket_at, bucket_rows in sorted(buckets.items())]


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
            active_points.append((x, y, timestamp.strftime("%m-%d %H:%M"), _format_value(series["metric_name"], value)))
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
        label = timestamp.strftime("%m-%d %H:%M")
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
        cells = [f"<td>{escape(timestamp.strftime('%m-%d %H:%M'))}</td>"]
        for series in valid_series:
            point_map = {point_timestamp: value for point_timestamp, value in series["points"]}
            cells.append(f"<td>{escape(_format_value(series['metric_name'], point_map.get(timestamp)))}</td>")
        rows.append(f"<tr>{''.join(cells)}</tr>")
    return f'<div class="trend-table-wrap"><table class="trend-table"><thead><tr>{"".join(header)}</tr></thead><tbody>{"".join(rows)}</tbody></table></div>'


def _render_cards(latest_map: dict[tuple[str, str], MetricSnapshot]) -> str:
    cards = []
    for item in CARD_DEFS:
        metric = latest_map.get((item["entity_id"], item["metric_name"]))
        value = _format_value(item["metric_name"], metric.value if metric else None)
        recorded_at = metric.recorded_at.strftime("%Y-%m-%d %H:%M UTC") if metric else "-"
        cards.append(
            f'''
            <div class="card">
              <div class="label">{escape(item["label"])}</div>
              <div class="value">{escape(value)}</div>
              <div class="meta">更新时间：{escape(recorded_at)}</div>
            </div>
            '''
        )
    return "".join(cards)


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
                            <div class="threshold-meta">告警条件：当前值 {escape(operator_label.get(rule.comparator, rule.comparator))} 阈值时发送飞书</div>
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

        banner = '<div class="flash success">Morpho PT-apyUSD-18JUN2026/USDC 告警阈值已更新，后续采集会按新阈值触发飞书通知。</div>' if threshold_updated else ""
        return f'''
        <div class="panel full threshold-panel">
            <div class="panel-head">
                <h3>Morpho PT-apyUSD-18JUN2026/USDC · 飞书告警阈值</h3>
                <div class="legend">
                    <span class="legend-item">支持修改：可借款额下限、借款利率上限</span>
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
    alerts = sorted(
        session.exec(select(AlertEvent).where(AlertEvent.status == "firing")).all(),
        key=lambda row: row.last_triggered_at,
        reverse=True,
    )[:20]
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
              <td>{escape(alert.last_triggered_at.strftime("%Y-%m-%d %H:%M UTC"))}</td>
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
                cells = [f"<td>{escape(timestamp.strftime('%m-%d %H:%M'))}</td>"]
                for metric_name in metrics:
                        value = series_map.get(metric_name, {}).get(timestamp)
                        cells.append(f"<td>{escape(_format_value(metric_name, value))}</td>")
                rows.append(f"<tr>{''.join(cells)}</tr>")
        return "".join(rows)


def _render_morpho_market_sections(session: Session, latest_map: dict[tuple[str, str], MetricSnapshot], hours: int) -> str:
    bucket_minutes = 5 if hours <= 6 else 15 if hours <= 24 else 60
    rows = []
    for market in MORPHO_MARKETS:
        available = latest_map.get((market.market_id, "available_to_borrow_usd"))
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
              <td>{escape(updated_at.strftime("%Y-%m-%d %H:%M UTC") if updated_at else "-")}</td>
            </tr>
            '''
        )
    return "".join(rows)


@router.post("/dashboard/thresholds")
def update_threshold(
    rule_id: str = Form(...),
    threshold: float = Form(...),
    hours: int = Form(default=24),
    session: Session = Depends(get_session),
):
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


@router.get("/dashboard", response_class=HTMLResponse)
def dashboard(
    hours: int = Query(default=24, ge=1, le=24 * 30),
    threshold_updated: int = Query(default=0),
    session: Session = Depends(get_session),
) -> str:
    latest_map = _latest_metric_map(session)
    rule_map = _effective_rule_map(session)
    latest_run = max((row.recorded_at for row in latest_map.values()), default=None)
    status_text = latest_run.strftime("最近数据：%Y-%m-%d %H:%M UTC") if latest_run else "暂无数据"
    hour_options = "".join(
        f'<option value="{value}" {"selected" if value == hours else ""}>近 {label}</option>'
        for value, label in ((6, "6 小时"), (24, "24 小时"), (72, "72 小时"), (168, "7 天"))
    )

    return f"""
<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>APYX Monitor Dashboard</title>
  <meta http-equiv="refresh" content="60" />
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
    .cards {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 16px; margin-bottom: 20px; }}
    .card, .panel {{ background: rgba(17, 24, 45, 0.92); border: 1px solid var(--border); border-radius: 18px; padding: 18px; box-shadow: 0 10px 30px rgba(0, 0, 0, 0.18); }}
    .card .label {{ color: var(--muted); font-size: 13px; margin-bottom: 8px; }}
    .card .value {{ font-size: 28px; font-weight: 700; margin-bottom: 4px; }}
    .card .meta {{ color: var(--muted); font-size: 12px; }}
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
        <button type="submit">刷新</button>
        <div class="status">{escape(status_text)}</div>
      </form>
    </div>

    <div class="cards">{_render_cards(latest_map)}</div>

    <div class="grid">
            {_render_threshold_controls(rule_map, latest_map, hours, bool(threshold_updated))}
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
  </div>
    <script>
        (() => {{
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

            wireViewTabs();
            wireLegends();
            wireTooltips();
        }})();
    </script>
</body>
</html>
    """

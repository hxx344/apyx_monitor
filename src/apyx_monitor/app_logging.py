from __future__ import annotations

import logging
import logging.config
import os
import re
from datetime import datetime, timedelta, timezone
from typing import Any


BEIJING_TZ = timezone(timedelta(hours=8))

LEVEL_LABELS = {
    "DEBUG": "调试",
    "INFO": "信息",
    "WARNING": "警告",
    "ERROR": "错误",
    "CRITICAL": "严重",
}

LEVEL_COLORS = {
    "DEBUG": "\033[36m",
    "INFO": "\033[32m",
    "WARNING": "\033[33m",
    "ERROR": "\033[31m",
    "CRITICAL": "\033[35m",
}

RESET = "\033[0m"
DIM = "\033[2m"
BOLD = "\033[1m"

COMPONENT_LABELS = {
    "apyx_monitor.collectors.arbitrage": "闭环套利",
    "apyx_monitor.collectors.morpho": "Morpho",
    "apyx_monitor.collectors.onchain": "链上采集",
    "apyx_monitor.services.monitoring": "采集服务",
    "apyx_monitor.services.alerting": "告警通知",
    "apyx_monitor.services.rule_engine": "规则引擎",
    "apscheduler.executors.default": "定时任务",
    "apscheduler.scheduler": "任务调度",
    "httpx": "外部请求",
    "uvicorn.access": "访问日志",
    "uvicorn.error": "服务运行",
}

JOB_LABELS = {
    "MonitoringService.poll_once": "全量采集",
    "MonitoringService.poll_nav_curve_once": "NAV/Curve 快扫",
    "MonitoringService.poll_arbitrage_once": "闭环套利刷新",
}

HTTP_TARGET_LABELS = {
    "api.morpho.org/graphql": "Morpho GraphQL",
    "api-v2.pendle.finance/core/v3/sdk": "Pendle SDK",
    "li.quest/v1/quote": "Jumper/LiFi",
    "api.paraswap.io/prices": "Velora/ParaSwap",
}

STATUS_TEXT = {
    200: "成功",
    201: "已创建",
    202: "已接受",
    204: "无内容",
    303: "重定向",
    400: "请求错误",
    401: "未登录",
    403: "无权限",
    404: "未找到",
    422: "参数无效",
    429: "限流",
    500: "服务错误",
    502: "网关错误",
    503: "服务不可用",
    504: "网关超时",
}


class ChineseLogFormatter(logging.Formatter):
    """Console formatter that turns noisy dependency logs into readable Chinese events."""

    def __init__(self, *, use_color: bool = True) -> None:
        super().__init__()
        self.use_color = use_color

    def format(self, record: logging.LogRecord) -> str:
        created_at = datetime.fromtimestamp(record.created, BEIJING_TZ)
        timestamp = created_at.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        level = LEVEL_LABELS.get(record.levelname, record.levelname)
        component = self._component_label(record.name)
        message = self._translate(record)

        if self.use_color:
            level_color = LEVEL_COLORS.get(record.levelname, "")
            level_text = f"{level_color}{level:<2}{RESET}"
            component_text = f"{BOLD}{component}{RESET}"
            timestamp_text = f"{DIM}{timestamp}{RESET}"
            line = f"{timestamp_text} │ {level_text} │ {component_text:<12} │ {message}"
        else:
            line = f"{timestamp} │ {level:<2} │ {component:<12} │ {message}"

        if record.exc_info:
            line += "\n" + self.formatException(record.exc_info)
        if record.stack_info:
            line += "\n" + self.formatStack(record.stack_info)
        return line

    @staticmethod
    def _component_label(logger_name: str) -> str:
        if logger_name in COMPONENT_LABELS:
            return COMPONENT_LABELS[logger_name]
        if logger_name.startswith("apyx_monitor."):
            return logger_name.removeprefix("apyx_monitor.")
        return logger_name

    def _translate(self, record: logging.LogRecord) -> str:
        if record.name == "uvicorn.access":
            return self._translate_uvicorn_access(record)

        message = record.getMessage()
        return (
            self._translate_apscheduler(message)
            or self._translate_httpx(message)
            or self._translate_arbitrage(message)
            or self._translate_monitoring(message)
            or message
        )

    @staticmethod
    def _translate_uvicorn_access(record: logging.LogRecord) -> str:
        args = record.args if isinstance(record.args, tuple) else ()
        if len(args) >= 5:
            client, method, path, http_version, status_code = args[:5]
            status = _status_label(status_code)
            return (
                f"访问完成 │ 客户端={client} │ 请求={method} {path} "
                f"│ HTTP/{http_version} │ 状态={status_code} {status}"
            )
        return record.getMessage()

    @staticmethod
    def _translate_apscheduler(message: str) -> str | None:
        running = re.search(r'Running job "([^"]+)"', message)
        if running:
            job = _job_label(running.group(1))
            next_run = _extract_next_run(message)
            suffix = f" │ 下次={next_run}" if next_run else ""
            return f"开始执行定时任务 │ 任务={job}{suffix}"

        finished = re.search(r'Job "([^"]+)" executed successfully', message)
        if finished:
            job = _job_label(finished.group(1))
            return f"定时任务完成 │ 任务={job} │ 结果=成功"

        return None

    @staticmethod
    def _translate_httpx(message: str) -> str | None:
        match = re.match(
            r'HTTP Request: (?P<method>\w+) (?P<url>\S+) "HTTP/(?P<version>[^"]+) '
            r'(?P<status>\d+) (?P<reason>[^"]+)"',
            message,
        )
        if not match:
            return None
        method = match.group("method")
        url = match.group("url")
        status_code = int(match.group("status"))
        target = _http_target_label(url)
        return (
            f"外部接口返回 │ 目标={target} │ 方法={method} "
            f"│ 状态={status_code} {_status_label(status_code)}"
        )

    @staticmethod
    def _translate_arbitrage(message: str) -> str | None:
        match = re.match(
            r"arbitrage collector sampling monitor (?P<monitor>\S+) "
            r"\((?P<index>\d+)/(?P<total>\d+)\)",
            message,
        )
        if match:
            return (
                "采样套利路径 │ "
                f"路径={match.group('monitor')} │ 进度={match.group('index')}/{match.group('total')}"
            )

        if message == "arbitrage collector entering path calculation because refresh was forced":
            return "开始计算套利路径 │ 触发=定时/手动强制刷新 │ Curve gate=已绕过"

        if "Curve/NAV deviation is not available yet" in message:
            return "暂不计算套利路径 │ 原因=暂无 Curve/NAV 偏离数据"

        match = re.search(
            r"Curve/NAV deviation is stale: age=(?P<age>[\d.]+)s max_age=(?P<max_age>\d+)s",
            message,
        )
        if match:
            return (
                "暂不计算套利路径 │ 原因=Curve/NAV 数据过期 │ "
                f"年龄={match.group('age')}秒 │ 最大允许={match.group('max_age')}秒"
            )

        match = re.search(
            r"curve_nav_deviation=(?P<deviation>[-\d.]+)% change=(?P<change>[-\d.]+)% "
            r"window=(?P<window>\d+)s",
            message,
        )
        if match and "entering path calculation" in message:
            return (
                "开始计算套利路径 │ 触发=Curve/NAV 波动 │ "
                f"偏离={match.group('deviation')}% │ 变化={match.group('change')}% "
                f"│ 窗口={match.group('window')}秒"
            )

        match = re.search(
            r"deviation=(?P<deviation>[-\d.]+)% change=(?P<change>[-\d.]+)% "
            r"min_change=(?P<min_change>[-\d.]+)% window=(?P<window>\d+)s",
            message,
        )
        if match and "Curve/NAV is quiet" in message:
            return (
                "暂不计算套利路径 │ 原因=Curve/NAV 波动未达阈值 │ "
                f"偏离={match.group('deviation')}% │ 变化={match.group('change')}% "
                f"│ 阈值={match.group('min_change')}% │ 窗口={match.group('window')}秒"
            )

        match = re.search(
            r"all quote providers are rate limited: monitor=(?P<monitor>\S+) "
            r"strategy=(?P<strategy>\S+) notional=(?P<notional>\S+) error=(?P<error>.+)",
            message,
        )
        if match:
            return (
                "套利报价暂停 │ 原因=全部报价源限流 │ "
                f"路径={match.group('monitor')} │ 策略={match.group('strategy')} "
                f"│ 本金=${match.group('notional')} │ 详情={match.group('error')}"
            )

        match = re.search(
            r"quote route is unavailable: monitor=(?P<monitor>\S+) "
            r"strategy=(?P<strategy>\S+) notional=(?P<notional>\S+) error=(?P<error>.+)",
            message,
        )
        if match:
            return (
                "跳过套利样本 │ 原因=报价路径不可用 │ "
                f"路径={match.group('monitor')} │ 策略={match.group('strategy')} "
                f"│ 本金=${match.group('notional')} │ 详情={match.group('error')}"
            )

        match = re.search(r"quote provider (?P<provider>\S+) is rate limited until (?P<until>[^;]+)", message)
        if match:
            return (
                "报价源限流 │ "
                f"来源={match.group('provider')} │ 恢复时间={match.group('until')} │ 动作=尝试下一个来源"
            )

        if message.startswith("arbitrage sample failed:"):
            return "套利样本计算失败 │ " + message.removeprefix("arbitrage sample failed:").strip()

        return None

    @staticmethod
    def _translate_monitoring(message: str) -> str | None:
        if message == "nav/curve collector failed":
            return "NAV/Curve 快扫失败"
        if message == "arbitrage collector failed":
            return "闭环套利采集失败"
        match = re.match(r"collector (?P<collector>\S+) failed", message)
        if match:
            return f"采集器失败 │ 名称={match.group('collector')}"
        if message == "alert notification failed":
            return "告警通知发送失败"
        return None


def configure_logging() -> None:
    use_color = os.getenv("NO_COLOR") is None and os.getenv("APYX_LOG_COLOR", "1") != "0"
    logging.config.dictConfig(_build_log_config(use_color))


def _build_log_config(use_color: bool) -> dict[str, Any]:
    return {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "zh_console": {
                "()": "apyx_monitor.app_logging.ChineseLogFormatter",
                "use_color": use_color,
            },
        },
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "formatter": "zh_console",
                "stream": "ext://sys.stderr",
            },
        },
        "root": {"handlers": ["console"], "level": "INFO"},
        "loggers": {
            "apyx_monitor": {"handlers": ["console"], "level": "INFO", "propagate": False},
            "apscheduler": {"handlers": ["console"], "level": "INFO", "propagate": False},
            "httpx": {"handlers": ["console"], "level": "INFO", "propagate": False},
            "httpcore": {"handlers": ["console"], "level": "WARNING", "propagate": False},
            "uvicorn": {"handlers": ["console"], "level": "INFO", "propagate": False},
            "uvicorn.error": {"handlers": ["console"], "level": "INFO", "propagate": False},
            "uvicorn.access": {"handlers": ["console"], "level": "INFO", "propagate": False},
        },
    }


def uvicorn_log_config() -> dict[str, Any]:
    use_color = os.getenv("NO_COLOR") is None and os.getenv("APYX_LOG_COLOR", "1") != "0"
    return _build_log_config(use_color)


def _job_label(raw: str) -> str:
    for key, label in JOB_LABELS.items():
        if key in raw:
            return label
    return raw.split(" (trigger:")[0]


def _extract_next_run(message: str) -> str | None:
    match = re.search(r"next run at: ([^)]+ UTC)", message)
    return match.group(1) if match else None


def _http_target_label(url: str) -> str:
    for pattern, label in HTTP_TARGET_LABELS.items():
        if pattern in url:
            return label
    return url


def _status_label(status_code: int | str) -> str:
    try:
        code = int(status_code)
    except (TypeError, ValueError):
        return ""
    return STATUS_TEXT.get(code, "完成" if code < 400 else "异常")

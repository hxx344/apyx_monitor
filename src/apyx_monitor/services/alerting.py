from __future__ import annotations

import base64
import hashlib
import hmac
import time

import httpx

from ..config import Settings


class FeishuNotifier:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    async def notify(self, title: str, body: str) -> bool:
        if not self.settings.feishu_webhook_url:
            return False

        payload = {
            "msg_type": "text",
            "content": {"text": f"{title}\n{body}"},
        }

        if self.settings.feishu_secret:
            timestamp = str(int(time.time()))
            sign = self._sign(timestamp, self.settings.feishu_secret)
            payload["timestamp"] = timestamp
            payload["sign"] = sign

        async with httpx.AsyncClient(timeout=httpx.Timeout(self.settings.http_timeout_seconds)) as client:
            response = await client.post(self.settings.feishu_webhook_url, json=payload)
            response.raise_for_status()
        return True

    @staticmethod
    def _sign(timestamp: str, secret: str) -> str:
        string_to_sign = f"{timestamp}\n{secret}".encode("utf-8")
        hmac_code = hmac.new(secret.encode("utf-8"), string_to_sign, digestmod=hashlib.sha256).digest()
        return base64.b64encode(hmac_code).decode("utf-8")

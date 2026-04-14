"""异步飞书 API 客户端（httpx）"""
import json
import logging
import time

import httpx

from incident_orchestrator.config import get_settings

logger = logging.getLogger(__name__)


class FeishuClient:
    """飞书 API 客户端，使用 tenant_access_token（bot 身份）"""

    def __init__(self):
        settings = get_settings()
        self.app_id = settings.lark_app_id
        self.app_secret = settings.lark_app_secret
        self.base_url = settings.lark_base_url
        self._token: str = ""
        self._token_expire: float = 0
        self._http: httpx.AsyncClient | None = None

    async def _ensure_http(self) -> httpx.AsyncClient:
        """延迟创建 httpx client，确保在正确的 event loop 中"""
        if self._http is None or self._http.is_closed:
            self._http = httpx.AsyncClient(timeout=15)
        return self._http

    async def _get_token(self) -> str:
        if self._token and time.time() < self._token_expire - 60:
            return self._token

        http = await self._ensure_http()
        resp = await http.post(
            f"{self.base_url}/open-apis/auth/v3/tenant_access_token/internal",
            json={"app_id": self.app_id, "app_secret": self.app_secret},
        )
        resp.raise_for_status()
        data = resp.json()
        if data.get("code") != 0:
            raise RuntimeError(f"获取 tenant_access_token 失败: {data}")

        self._token = data["tenant_access_token"]
        self._token_expire = time.time() + data.get("expire", 7200)
        return self._token

    async def _headers(self) -> dict:
        token = await self._get_token()
        return {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json; charset=utf-8",
        }

    async def reply_message(
        self,
        message_id: str,
        msg_type: str,
        content: str | dict,
        reply_in_thread: bool = True,
    ) -> dict:
        """回复消息（话题回复）"""
        body: dict = {
            "msg_type": msg_type,
            "content": content if isinstance(content, str) else json.dumps(content, ensure_ascii=False),
        }
        if reply_in_thread:
            body["reply_in_thread"] = "true"

        http = await self._ensure_http()
        resp = await http.post(
            f"{self.base_url}/open-apis/im/v1/messages/{message_id}/reply",
            headers=await self._headers(),
            json=body,
        )
        resp.raise_for_status()
        result = resp.json()
        if result.get("code") != 0:
            logger.error(f"飞书回复失败: {result}")
        return result

    async def send_card_reply(
        self, message_id: str, card: dict, reply_in_thread: bool = True
    ) -> dict:
        return await self.reply_message(
            message_id,
            "interactive",
            json.dumps(card, ensure_ascii=False),
            reply_in_thread=reply_in_thread,
        )

    async def reply_text(
        self, message_id: str, text: str, reply_in_thread: bool = True
    ) -> dict:
        content = json.dumps({"text": text}, ensure_ascii=False)
        return await self.reply_message(
            message_id, "text", content, reply_in_thread=reply_in_thread
        )

    async def get_message(self, message_id: str) -> dict:
        """获取单条消息详情"""
        http = await self._ensure_http()
        resp = await http.get(
            f"{self.base_url}/open-apis/im/v1/messages/{message_id}",
            headers=await self._headers(),
        )
        resp.raise_for_status()
        return resp.json()

    async def add_reaction(self, message_id: str, emoji_type: str = "OnIt") -> dict:
        """给消息添加 emoji 表情回应"""
        http = await self._ensure_http()
        resp = await http.post(
            f"{self.base_url}/open-apis/im/v1/messages/{message_id}/reactions",
            headers=await self._headers(),
            json={"reaction_type": {"emoji_type": emoji_type}},
        )
        resp.raise_for_status()
        return resp.json()

    async def close(self):
        if self._http:
            await self._http.aclose()
            self._http = None


def get_feishu_client() -> FeishuClient:
    """每次返回新实例，避免 event loop 不匹配"""
    return FeishuClient()

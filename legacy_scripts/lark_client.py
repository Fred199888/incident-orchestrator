"""飞书 REST API 客户端封装

认证策略:
  - 全部使用 tenant_access_token（bot 身份）
  - 新 bitable 由 bot 创建，bot 拥有完整权限，无需 UAT
"""
import json
import sys
import time
import requests

from config import LARK_APP_ID, LARK_APP_SECRET, LARK_BASE_URL


class LarkClient:
    """飞书 API 客户端，使用 tenant_access_token（bot 身份）"""

    def __init__(self, app_id: str = "", app_secret: str = ""):
        self.app_id = app_id or LARK_APP_ID
        self.app_secret = app_secret or LARK_APP_SECRET
        self._token: str = ""
        self._token_expire: float = 0

    # ── 认证 ──

    def _get_token(self) -> str:
        """获取 tenant_access_token，自动缓存和刷新"""
        if self._token and time.time() < self._token_expire - 60:
            return self._token

        resp = requests.post(
            f"{LARK_BASE_URL}/open-apis/auth/v3/tenant_access_token/internal",
            json={"app_id": self.app_id, "app_secret": self.app_secret},
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()

        if data.get("code") != 0:
            raise RuntimeError(f"获取 tenant_access_token 失败: {data}")

        self._token = data["tenant_access_token"]
        self._token_expire = time.time() + data.get("expire", 7200)
        return self._token

    def _headers(self, **_kwargs) -> dict:
        """构造请求头（统一使用 tenant_access_token）"""
        return {
            "Authorization": f"Bearer {self._get_token()}",
            "Content-Type": "application/json; charset=utf-8",
        }

    # ── 消息 API（使用 tenant_access_token）──

    def list_messages(
        self,
        container_id_type: str,
        container_id: str,
        page_size: int = 50,
        page_token: str | None = None,
        sort_type: str | None = None,
    ) -> dict:
        """
        列表消息
        GET /open-apis/im/v1/messages
        """
        params: dict = {
            "container_id_type": container_id_type,
            "container_id": container_id,
            "page_size": page_size,
        }
        if page_token:
            params["page_token"] = page_token
        if sort_type:
            params["sort_type"] = sort_type

        resp = requests.get(
            f"{LARK_BASE_URL}/open-apis/im/v1/messages",
            headers=self._headers(),
            params=params,
            timeout=15,
        )
        resp.raise_for_status()
        return resp.json()

    def list_thread_messages(
        self,
        message_id: str,
        page_size: int = 5,
    ) -> dict:
        """
        话题内消息列表（用于检测 bot 回复）
        GET /open-apis/im/v1/messages（container_id_type=thread）
        """
        return self.list_messages(
            container_id_type="thread",
            container_id=message_id,
            page_size=page_size,
            sort_type="ByCreateTimeAsc",
        )

    def reply_message(
        self,
        message_id: str,
        msg_type: str,
        content: str | dict,
        reply_in_thread: bool = True,
    ) -> dict:
        """
        回复消息（话题回复）
        POST /open-apis/im/v1/messages/{message_id}/reply
        """
        body: dict = {
            "msg_type": msg_type,
            "content": content if isinstance(content, str) else json.dumps(content, ensure_ascii=False),
        }
        if reply_in_thread:
            body["reply_in_thread"] = "true"

        resp = requests.post(
            f"{LARK_BASE_URL}/open-apis/im/v1/messages/{message_id}/reply",
            headers=self._headers(),
            json=body,
            timeout=15,
        )
        resp.raise_for_status()
        return resp.json()

    def get_message(self, message_id: str) -> dict:
        """
        获取单条消息详情
        GET /open-apis/im/v1/messages/{message_id}
        """
        resp = requests.get(
            f"{LARK_BASE_URL}/open-apis/im/v1/messages/{message_id}",
            headers=self._headers(),
            timeout=15,
        )
        resp.raise_for_status()
        return resp.json()

    # ── 多维表格 API（使用 tenant_access_token）──

    def search_bitable_records(
        self,
        app_token: str,
        table_id: str,
        field_name: str,
        operator: str,
        value: list,
        page_size: int = 1,
    ) -> dict:
        """
        搜索多维表格记录
        POST /open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/search
        """
        body = {
            "filter": {
                "conjunction": "and",
                "conditions": [
                    {
                        "field_name": field_name,
                        "operator": operator,
                        "value": value,
                    }
                ],
            },
            "page_size": page_size,
        }
        resp = requests.post(
            f"{LARK_BASE_URL}/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/search",
            headers=self._headers(),
            json=body,
            timeout=15,
        )
        resp.raise_for_status()
        return resp.json()

    def create_bitable_record(
        self,
        app_token: str,
        table_id: str,
        fields: dict,
    ) -> dict:
        """
        创建多维表格记录
        POST /open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records
        """
        resp = requests.post(
            f"{LARK_BASE_URL}/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records",
            headers=self._headers(),
            json={"fields": fields},
            timeout=15,
        )
        resp.raise_for_status()
        return resp.json()

    # ── 带重试的辅助方法 ──

    def reply_with_retry(
        self,
        message_id: str,
        msg_type: str,
        content: str | dict,
        reply_in_thread: bool = True,
        max_retries: int = 2,
    ) -> dict:
        """带重试的消息回复"""
        last_err = None
        for attempt in range(max_retries + 1):
            try:
                result = self.reply_message(message_id, msg_type, content, reply_in_thread)
                if result.get("code") == 0:
                    return result
                last_err = result
                print(f"  回复失败 (attempt {attempt+1}): code={result.get('code')} msg={result.get('msg')}", file=sys.stderr)
            except Exception as e:
                last_err = {"error": str(e)}
                print(f"  回复异常 (attempt {attempt+1}): {e}", file=sys.stderr)
            if attempt < max_retries:
                time.sleep(1)
        return last_err or {"error": "unknown"}

    def create_record_with_retry(
        self,
        app_token: str,
        table_id: str,
        fields: dict,
        max_retries: int = 2,
    ) -> dict:
        """带重试的表格记录创建。403 时立即返回不重试。"""
        last_err = None
        for attempt in range(max_retries + 1):
            try:
                result = self.create_bitable_record(app_token, table_id, fields)
                if result.get("code") == 0:
                    return result
                last_err = result
                # 权限不足不重试
                if result.get("code") == 403 or result.get("code") == 1062001:
                    last_err["is_permission_error"] = True
                    return last_err
                print(f"  表格写入失败 (attempt {attempt+1}): code={result.get('code')} msg={result.get('msg')}", file=sys.stderr)
            except requests.exceptions.HTTPError as e:
                if e.response is not None and e.response.status_code == 403:
                    last_err = {"error": str(e), "is_permission_error": True, "code": 403}
                    print(f"  表格写入 403 权限不足，停止重试", file=sys.stderr)
                    return last_err
                last_err = {"error": str(e)}
                print(f"  表格写入异常 (attempt {attempt+1}): {e}", file=sys.stderr)
            except Exception as e:
                last_err = {"error": str(e)}
                print(f"  表格写入异常 (attempt {attempt+1}): {e}", file=sys.stderr)
            if attempt < max_retries:
                time.sleep(1)
        return last_err or {"error": "unknown"}

    def search_bitable_with_retry(
        self,
        app_token: str,
        table_id: str,
        field_name: str,
        operator: str,
        value: list,
        page_size: int = 1,
        max_retries: int = 2,
    ) -> dict:
        """带重试的表格搜索。403 时立即返回不重试。"""
        last_err = None
        for attempt in range(max_retries + 1):
            try:
                result = self.search_bitable_records(
                    app_token, table_id, field_name, operator, value, page_size
                )
                if result.get("code") == 0:
                    return result
                last_err = result
                if result.get("code") == 403 or result.get("code") == 1062001:
                    last_err["is_permission_error"] = True
                    return last_err
                print(f"  表格查询失败 (attempt {attempt+1}): code={result.get('code')}", file=sys.stderr)
            except requests.exceptions.HTTPError as e:
                if e.response is not None and e.response.status_code == 403:
                    last_err = {"error": str(e), "is_permission_error": True, "code": 403}
                    return last_err
                last_err = {"error": str(e)}
                print(f"  表格查询异常 (attempt {attempt+1}): {e}", file=sys.stderr)
            except Exception as e:
                last_err = {"error": str(e)}
                print(f"  表格查询异常 (attempt {attempt+1}): {e}", file=sys.stderr)
            if attempt < max_retries:
                time.sleep(1)
        return last_err or {"error": "unknown"}

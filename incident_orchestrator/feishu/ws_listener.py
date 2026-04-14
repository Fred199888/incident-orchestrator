"""飞书 WebSocket 长连接事件监听

双回调分流：
- @bot 消息 → on_bot_mention（手动触发）
- 非 @bot 告警群消息 → on_alert_message（自动累积）
"""
import json
import threading
from typing import Callable

import lark_oapi as lark
from lark_oapi.api.im.v1 import P2ImMessageReceiveV1

from incident_orchestrator.config import get_settings
from incident_orchestrator.log import get_logger

logger = get_logger("WS")

_on_bot_mention: Callable | None = None
_on_alert_message: Callable | None = None
_alert_chat_id: str = ""


def _handle_message(data: P2ImMessageReceiveV1) -> None:
    """lark-oapi 回调 — 分流到手动/自动"""
    try:
        event = data.event
        message = event.message
        sender = event.sender

        message_id = message.message_id or ""
        chat_id = message.chat_id or ""
        chat_type = message.chat_type or ""
        root_id = message.root_id or ""
        msg_type = message.message_type or ""

        if sender and sender.sender_type == "app":
            return

        content_str = message.content or "{}"
        try:
            content = json.loads(content_str)
        except json.JSONDecodeError:
            return

        raw_text = content.get("text", "").strip()
        # 只有 @bot 自己才算 mention，@别人不管
        # SDK 的 MentionEvent 缺少 mentioned_type 字段，用 name 匹配
        bot_mentioned = False
        user_text = raw_text
        if message.mentions:
            for mention in message.mentions:
                if mention.name == "bug-fix" and mention.key:
                    bot_mentioned = True
                    user_text = user_text.replace(mention.key, "").strip()
                # @别人的 key 不移除

        sender_id = ""
        if sender and sender.sender_id:
            sender_id = sender.sender_id.open_id or ""

        if bot_mentioned and user_text and _on_bot_mention:
            parsed = {
                "root_id": root_id,
                "message_id": message_id,
                "chat_id": chat_id,
                "chat_type": chat_type,
                "user_text": user_text,
                "sender_id": sender_id,
            }
            logger.info("@bot: chat=%s, root=%s, text=%s", chat_id[-8:], root_id or "无", user_text[:50])
            _on_bot_mention(parsed)

        elif not bot_mentioned and chat_id == _alert_chat_id and _on_alert_message:
            parsed = {
                "message_id": message_id,
                "chat_id": chat_id,
                "msg_type": msg_type,
                "content_raw": content_str,
                "sender_id": sender_id,
                "create_time": message.create_time or "",
            }
            logger.debug("告警消息: msg_type=%s, id=%s", msg_type, message_id[-8:])
            _on_alert_message(parsed)

    except Exception:
        logger.exception("消息处理异常")


def _start_ws_client(app_id: str, app_secret: str) -> None:
    handler = (
        lark.EventDispatcherHandler.builder("", "")
        .register_p2_im_message_receive_v1(_handle_message)
        .build()
    )

    cli = lark.ws.Client(
        app_id=app_id,
        app_secret=app_secret,
        event_handler=handler,
        log_level=lark.LogLevel.DEBUG,
    )

    try:
        cli.start()
    except Exception:
        logger.exception("WebSocket 连接失败")


def start_ws_listener(
    on_bot_mention: Callable,
    on_alert_message: Callable | None = None,
) -> threading.Thread:
    global _on_bot_mention, _on_alert_message, _alert_chat_id

    _on_bot_mention = on_bot_mention
    _on_alert_message = on_alert_message

    settings = get_settings()
    _alert_chat_id = settings.lark_chat_id

    if not settings.lark_app_id or not settings.lark_app_secret:
        return None

    thread = threading.Thread(
        target=_start_ws_client,
        args=(settings.lark_app_id, settings.lark_app_secret),
        daemon=True,
        name="feishu-ws",
    )
    thread.start()
    return thread

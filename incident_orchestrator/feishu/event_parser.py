"""飞书事件订阅 payload 解析"""
import json
import logging

logger = logging.getLogger(__name__)


def parse_bot_mention_event(event: dict) -> dict | None:
    """解析 @bot 消息事件，返回结构化信息

    返回: {
        "root_id": str,        # 话题根消息 ID
        "message_id": str,     # 当前消息 ID
        "chat_id": str,        # 群 ID
        "user_text": str,      # 用户文本（去除 @bot 部分）
        "sender_id": str,      # 发送者 open_id
    }
    """
    message = event.get("message", {})
    chat_id = message.get("chat_id", "")
    root_id = message.get("root_id", "")
    message_id = message.get("message_id", "")
    sender = event.get("sender", {}).get("sender_id", {})
    sender_id = sender.get("open_id", "")

    # 解析消息内容
    content_str = message.get("content", "{}")
    try:
        content = json.loads(content_str)
    except json.JSONDecodeError:
        logger.warning(f"无法解析消息内容: {content_str}")
        return None

    user_text = content.get("text", "").strip()

    # 去除 @bot 的 mention 标记
    mentions = message.get("mentions", [])
    for mention in mentions:
        key = mention.get("key", "")
        if key:
            user_text = user_text.replace(key, "").strip()

    if not root_id:
        return None  # 非话题消息

    if not user_text:
        return None  # 空消息

    return {
        "root_id": root_id,
        "message_id": message_id,
        "chat_id": chat_id,
        "user_text": user_text,
        "sender_id": sender_id,
    }

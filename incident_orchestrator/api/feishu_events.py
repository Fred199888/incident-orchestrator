"""飞书事件订阅端点"""
import json
import logging

from fastapi import APIRouter, BackgroundTasks, Request

from incident_orchestrator.config import get_settings
from incident_orchestrator.db.engine import get_session_factory
from incident_orchestrator.db.repository import Repository
from incident_orchestrator.dependencies import get_mutex
from incident_orchestrator.feishu.client import get_feishu_client
from incident_orchestrator.feishu.crypto import verify_signature
from incident_orchestrator.feishu.event_parser import parse_bot_mention_event
from incident_orchestrator.services.claude_runner import get_runner

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/v1")


async def _handle_bot_mention(event: dict, background_tasks: BackgroundTasks) -> None:
    """处理 @bot 消息"""
    parsed = parse_bot_mention_event(event)
    if not parsed:
        return

    root_id = parsed["root_id"]
    message_id = parsed["message_id"]
    user_text = parsed["user_text"]

    # 查找 incident
    factory = get_session_factory()
    async with factory() as session:
        repo = Repository(session)
        incident = await repo.find_by_root_message(root_id)

    if not incident:
        logger.debug(f"话题 {root_id} 未关联到 incident，忽略")
        return

    if not incident.claude_session_id:
        logger.warning(f"incident {incident.incident_id} 尚无 Claude session")
        return

    # 后台处理
    background_tasks.add_task(
        _resume_and_reply,
        incident.incident_id,
        incident.claude_session_id,
        message_id,
        user_text,
    )


async def _resume_and_reply(
    incident_id: str,
    session_id: str,
    reply_to_message_id: str,
    user_text: str,
) -> None:
    """resume Claude session 并回复到飞书话题"""
    mutex = get_mutex()
    feishu = get_feishu_client()

    if not await mutex.try_acquire(incident_id):
        await feishu.reply_text(reply_to_message_id, "正在处理中，请稍候...")
        return

    try:
        # resume Claude session
        runner = get_runner()
        reply = await runner.resume_session(session_id, f"用户追问: {user_text}")

        # 回复到话题
        if reply:
            await feishu.reply_text(reply_to_message_id, reply[:4000])
        else:
            await feishu.reply_text(reply_to_message_id, "分析完成，但未产生文字回复。")

        # 记录消息
        factory = get_session_factory()
        async with factory() as session:
            repo = Repository(session)
            await repo.save_message(incident_id, "user", user_text)
            if reply:
                await repo.save_message(incident_id, "claude", reply[:10000])

    except Exception as e:
        logger.error(f"[{incident_id}] resume 失败: {e}", exc_info=True)
        await feishu.reply_text(
            reply_to_message_id, f"处理异常: {str(e)[:200]}"
        )
    finally:
        mutex.release(incident_id)


@router.post("/feishu/events")
async def handle_feishu_event(request: Request, background_tasks: BackgroundTasks):
    """飞书事件订阅回调"""
    body_bytes = await request.body()
    body_str = body_bytes.decode("utf-8")
    body = json.loads(body_str)

    # 1. Challenge 验证
    if body.get("type") == "url_verification":
        return {"challenge": body["challenge"]}

    # 2. 签名验证
    settings = get_settings()
    if settings.lark_encrypt_key:
        headers = request.headers
        sig_ok = verify_signature(
            timestamp=headers.get("x-lark-request-timestamp", ""),
            nonce=headers.get("x-lark-request-nonce", ""),
            encrypt_key=settings.lark_encrypt_key,
            body=body_str,
            signature=headers.get("x-lark-signature", ""),
        )
        if not sig_ok:
            logger.warning("飞书签名验证失败")
            return {"code": -1, "msg": "signature failed"}

    # 3. 事件分发
    header = body.get("header", {})
    event_type = header.get("event_type", "")
    event = body.get("event", {})

    if event_type == "im.message.receive_v1":
        await _handle_bot_mention(event, background_tasks)

    return {"code": 0}

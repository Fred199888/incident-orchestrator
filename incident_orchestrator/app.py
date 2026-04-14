import asyncio
import threading
from contextlib import asynccontextmanager

from fastapi import FastAPI

from incident_orchestrator.api.router import api_router
from incident_orchestrator.db.engine import init_db
from incident_orchestrator.feishu.client import get_feishu_client
from incident_orchestrator.feishu.ws_listener import start_ws_listener
from incident_orchestrator.log import get_logger
from incident_orchestrator.services.message_handler import handle_thread_message

logger = get_logger("APP")

_main_loop: asyncio.AbstractEventLoop | None = None
_processed_messages: set[str] = set()
_dedup_lock = threading.Lock()


# ── WebSocket 回调: @bot 消息 → 手动触发 ──

def _on_bot_mention(parsed: dict) -> None:
    if _main_loop is None or _main_loop.is_closed():
        return

    msg_id = parsed["message_id"]
    with _dedup_lock:
        if msg_id in _processed_messages:
            return
        _processed_messages.add(msg_id)
        if len(_processed_messages) > 1000:
            _processed_messages.clear()

    def _on_done(f):
        try:
            f.result()
        except Exception:
            logger.exception("_handle_mention 异常")

    future = asyncio.run_coroutine_threadsafe(_dispatch_mention(parsed), _main_loop)
    future.add_done_callback(_on_done)


async def _dispatch_mention(parsed: dict) -> None:
    root_id = parsed["root_id"]
    message_id = parsed["message_id"]
    chat_id = parsed.get("chat_id", "")
    user_text = parsed["user_text"]

    logger.info("@bot: root=%s, text=%s", root_id or "(无)", user_text[:80])

    if not root_id:
        feishu = get_feishu_client()
        await feishu.reply_text(
            message_id, "请在告警消息的话题内 @我 进行追问。", reply_in_thread=False,
        )
        return

    await handle_thread_message(root_id, message_id, chat_id, user_text)


# ── FastAPI 生命周期 ──

@asynccontextmanager
async def lifespan(app: FastAPI):
    global _main_loop

    await init_db()
    _main_loop = asyncio.get_running_loop()

    # WebSocket 监听（只处理 @bot 消息）
    ws_thread = start_ws_listener(on_bot_mention=_on_bot_mention)
    if ws_thread:
        logger.info("飞书 WebSocket 监听已启动")

    # 定时扫描（每 20 分钟扫描最近 100 条消息）
    from incident_orchestrator.services.scheduled_scan import start_scheduled_scan
    scan_task = asyncio.create_task(start_scheduled_scan())
    logger.info("定时扫描已启动（每 20 分钟）")

    yield

    scan_task.cancel()


def create_app() -> FastAPI:
    app = FastAPI(
        title="Incident Orchestrator",
        version="0.1.0",
        lifespan=lifespan,
    )
    app.include_router(api_router)
    return app

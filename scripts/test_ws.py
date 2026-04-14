"""最小化 WebSocket 测试 — 收到任何事件都打印"""
import os

import lark_oapi as lark
from lark_oapi.api.im.v1 import P2ImMessageReceiveV1

APP_ID = os.environ.get("LARK_APP_ID", "")
APP_SECRET = os.environ.get("LARK_APP_SECRET", "")


def on_message(data: P2ImMessageReceiveV1):
    print(f"\n{'='*60}")
    print(f"收到消息事件!")
    print(f"message_id: {data.event.message.message_id}")
    print(f"chat_id: {data.event.message.chat_id}")
    print(f"content: {data.event.message.content}")
    print(f"root_id: {data.event.message.root_id}")
    print(f"msg_type: {data.event.message.message_type}")
    print(f"{'='*60}\n", flush=True)


handler = (
    lark.EventDispatcherHandler.builder("", "")
    .register_p2_im_message_receive_v1(on_message)
    .build()
)

cli = lark.ws.Client(
    app_id=APP_ID,
    app_secret=APP_SECRET,
    event_handler=handler,
    log_level=lark.LogLevel.DEBUG,
)

print("启动 WebSocket 监听，等待消息... (Ctrl+C 退出)")
cli.start()

"""fingerprint 提取 — 自动和手动共用

从告警 content 提取 issue_fingerprint，写入和读取用同一个函数，格式一致。

优先级：
1. 全类名:行号 — com.mindverse.os.ws.websocket.handler.ClientMessageHandler:414
2. content 前两行拼接 — Failed_to_get_memory_count.NullPointerException

注：曾存在"仅全类名（无行号）"这一档，但在实际数据里会把不同行抛出的同类异常
错误合并成一条，造成过度去重（如 java.lang.NullPointerException 全部合成一个
指纹），已移除。提取不到行号时直接降级到前两行兜底，保留更多差异信息。
"""
import re

from incident_orchestrator.log import get_logger

logger = get_logger("FP")


def extract_fingerprint(service: str, content: str) -> str:
    """从告警 content 提取 fingerprint

    返回: 服务名.提取结果
    """
    if not service or not content:
        return f"{service or 'unknown'}.unknown"

    # 优先级 1: 全类名:行号
    m = re.search(r"((?:com|org|java|io)\.[\w.]+):(\d+)", content)
    if not m:
        m = re.search(r"([\w]+\.java):(\d+)", content)
    if m:
        fp = f"{service}.{m.group(1)}:{m.group(2)}"
        logger.info("提取(全类名+行号): %s", fp[:60])
        return fp

    # 优先级 2: content 前两行拼接，去动态参数
    lines = [l.strip() for l in content.strip().split("\n") if l.strip()][:2]
    if not lines:
        return f"{service}.unknown"

    # 去除动态参数（userId、数字 ID、UUID、时间戳等）
    key_parts = []
    for line in lines:
        cleaned = re.sub(r"\b\d{4,}\b", "", line)           # 长数字
        cleaned = re.sub(r"[0-9a-f]{8}-[0-9a-f]{4}-", "", cleaned)  # UUID
        cleaned = re.sub(r"userId[:\s]*\d+", "userId", cleaned)     # userId
        cleaned = cleaned.strip()
        if cleaned:
            key_parts.append(cleaned)

    key = ".".join(key_parts) if key_parts else "unknown"
    # 替换空格和特殊字符 → 先 strip 尾部 _ . :（避免 [Tomcat] 这类字符
    # 被替换+截断后残留末尾裸 `.` 造成同一内容产生两个不同 fp）→ 再统一截断
    key = re.sub(r"[^\w.:]+", "_", key).strip("_.:")[:80]

    fp = f"{service}.{key}"
    logger.info("提取(前两行兜底): %s", fp[:60])
    return fp

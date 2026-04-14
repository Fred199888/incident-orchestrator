"""告警 payload 解析 + 指纹生成"""
import hashlib
import re

from incident_orchestrator.models.schemas import AlertWebhookPayload


def normalize_api_path(api_path: str) -> str:
    """REST 路径归一化：/rest/x/y → x-y，去除动态 ID 段"""
    if not api_path:
        return ""
    path = api_path.strip("/")
    # 去除 /rest/ 前缀
    path = re.sub(r"^rest/", "", path)
    # 替换数字 ID 段
    parts = [p for p in path.split("/") if not p.isdigit()]
    return "-".join(parts) if parts else path


def compute_fingerprint(service: str, api_path: str, subcode: str) -> str:
    """计算告警指纹：service_api_subcode"""
    normalized = normalize_api_path(api_path)
    parts = [p for p in [service, normalized, subcode] if p]
    raw = "_".join(parts)
    if len(raw) > 200:
        return hashlib.md5(raw.encode()).hexdigest()
    return raw


def parse_alert(payload: AlertWebhookPayload) -> dict:
    """解析告警 payload，返回 incident 创建参数"""
    fingerprint = compute_fingerprint(
        payload.service, payload.api_path, payload.subcode
    )
    return {
        "service": payload.service,
        "env": payload.env,
        "severity": payload.severity,
        "summary": payload.summary,
        "subcode": payload.subcode,
        "api_path": payload.api_path,
        "trace_id": payload.trace_id,
        "fingerprint": fingerprint,
        "feishu_chat_id": payload.feishu_chat_id,
        "feishu_root_message_id": payload.feishu_message_id or None,
    }

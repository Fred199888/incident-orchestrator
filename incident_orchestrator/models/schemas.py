from pydantic import BaseModel


class AlertWebhookPayload(BaseModel):
    alert_id: str
    service: str
    env: str = "prod"
    severity: str = "P2"
    summary: str
    subcode: str = ""
    api_path: str = ""
    trace_id: str = ""
    logset_id: str = ""
    alert_time: int = 0
    feishu_chat_id: str = ""
    feishu_message_id: str = ""


class IncidentResponse(BaseModel):
    incident_id: str
    claude_session_id: str
    feishu_root_message_id: str | None
    service: str | None
    status: str
    summary: str | None
    fix_branch: str | None
    pr_url: str | None
    created_at: str
    updated_at: str


class MessageResponse(BaseModel):
    role: str
    content: str
    created_at: str


class HealthResponse(BaseModel):
    status: str = "ok"
    version: str = "0.1.0"

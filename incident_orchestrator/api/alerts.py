"""告警 webhook 端点"""
import logging

from fastapi import APIRouter, BackgroundTasks, Depends

from incident_orchestrator.dependencies import get_incident_service
from incident_orchestrator.models.schemas import AlertWebhookPayload, IncidentResponse
from incident_orchestrator.services.incident_service import IncidentService
from incident_orchestrator.services.orchestrator import process_alert

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/v1")


@router.post("/alerts", response_model=IncidentResponse)
async def receive_alert(
    payload: AlertWebhookPayload,
    background_tasks: BackgroundTasks,
    incident_service: IncidentService = Depends(get_incident_service),
):
    """接收告警 webhook，创建 incident 并触发分析流程"""
    incident, is_new = await incident_service.create_from_alert(payload)

    if is_new:
        logger.info(f"新 incident {incident.incident_id}，触发后台分析")
        background_tasks.add_task(process_alert, incident)
    else:
        logger.info(f"去重命中，关联到已有 incident {incident.incident_id}")

    return IncidentResponse(
        incident_id=incident.incident_id,
        claude_session_id=incident.claude_session_id,
        feishu_root_message_id=incident.feishu_root_message_id,
        service=incident.service,
        status=incident.status,
        summary=incident.summary,
        fix_branch=incident.fix_branch,
        pr_url=incident.pr_url,
        created_at=incident.created_at,
        updated_at=incident.updated_at,
    )

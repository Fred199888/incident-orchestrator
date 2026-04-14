"""Incident 管理端点"""
from fastapi import APIRouter, Depends, HTTPException

from incident_orchestrator.db.engine import get_session_factory
from incident_orchestrator.db.repository import Repository
from incident_orchestrator.models.schemas import IncidentResponse, MessageResponse

router = APIRouter(prefix="/api/v1")


async def _get_repo():
    factory = get_session_factory()
    async with factory() as session:
        yield Repository(session)


@router.get("/incidents", response_model=list[IncidentResponse])
async def list_incidents(limit: int = 50, repo: Repository = Depends(_get_repo)):
    incidents = await repo.list_incidents(limit=limit)
    return [
        IncidentResponse(
            incident_id=inc.incident_id,
            claude_session_id=inc.claude_session_id,
            feishu_root_message_id=inc.feishu_root_message_id,
            service=inc.service,
            status=inc.status,
            summary=inc.summary,
            fix_branch=inc.fix_branch,
            pr_url=inc.pr_url,
            created_at=inc.created_at,
            updated_at=inc.updated_at,
        )
        for inc in incidents
    ]


@router.get("/incidents/{incident_id}", response_model=IncidentResponse)
async def get_incident(incident_id: str, repo: Repository = Depends(_get_repo)):
    inc = await repo.get_incident(incident_id)
    if not inc:
        raise HTTPException(404, "Incident not found")
    return IncidentResponse(
        incident_id=inc.incident_id,
        claude_session_id=inc.claude_session_id,
        feishu_root_message_id=inc.feishu_root_message_id,
        service=inc.service,
        status=inc.status,
        summary=inc.summary,
        fix_branch=inc.fix_branch,
        pr_url=inc.pr_url,
        created_at=inc.created_at,
        updated_at=inc.updated_at,
    )


@router.get("/incidents/{incident_id}/messages", response_model=list[MessageResponse])
async def get_incident_messages(incident_id: str, repo: Repository = Depends(_get_repo)):
    """查询某个 incident 的完整对话历史"""
    inc = await repo.get_incident(incident_id)
    if not inc:
        raise HTTPException(404, "Incident not found")
    messages = await repo.list_messages(incident_id)
    return [
        MessageResponse(
            role=m.role,
            content=m.content,
            created_at=m.created_at,
        )
        for m in messages
    ]

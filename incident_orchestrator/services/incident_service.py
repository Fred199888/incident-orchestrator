"""事故生命周期管理"""
import logging
import uuid

from incident_orchestrator.db.repository import Repository
from incident_orchestrator.models.db import Incident
from incident_orchestrator.models.enums import IncidentStatus
from incident_orchestrator.services.alert_parser import parse_alert

logger = logging.getLogger(__name__)


class IncidentService:
    def __init__(self, repo: Repository):
        self.repo = repo

    async def create_from_alert(self, payload) -> tuple[Incident, bool]:
        """从告警创建 incident。返回 (incident, is_new)。

        如果 30 分钟内已存在相同 fingerprint 的 incident，返回已有记录。
        """
        parsed = parse_alert(payload)
        fingerprint = parsed.get("fingerprint")

        # 指纹去重
        if fingerprint:
            existing = await self.repo.find_by_fingerprint(fingerprint)
            if existing:
                logger.info(f"指纹去重命中: {fingerprint} → {existing.incident_id}")
                return existing, False

        incident_id = f"INC-{uuid.uuid4().hex[:12]}"
        incident = await self.repo.create_incident(
            incident_id=incident_id,
            claude_session_id="",  # 后续 Claude Runner 创建时填入
            status=IncidentStatus.NEW,
            **parsed,
        )
        logger.info(f"创建新 incident: {incident_id}")
        return incident, True

    async def update_session(
        self, incident_id: str, session_id: str, worktree_name: str
    ) -> None:
        await self.repo.update_incident(
            incident_id,
            claude_session_id=session_id,
            worktree_name=worktree_name,
        )

    async def update_status(self, incident_id: str, status: IncidentStatus) -> None:
        await self.repo.update_incident(incident_id, status=status.value)

    async def update_fix_result(
        self, incident_id: str, fix_branch: str = "", pr_url: str = ""
    ) -> None:
        kwargs = {}
        if fix_branch:
            kwargs["fix_branch"] = fix_branch
        if pr_url:
            kwargs["pr_url"] = pr_url
        if kwargs:
            await self.repo.update_incident(incident_id, **kwargs)

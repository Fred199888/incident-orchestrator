from datetime import datetime, timedelta, timezone

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from incident_orchestrator.models.db import Incident, IncidentMessage


class Repository:
    def __init__(self, session: AsyncSession):
        self.session = session

    # ── Incident ──

    async def create_incident(self, **kwargs) -> Incident:
        incident = Incident(**kwargs)
        self.session.add(incident)
        await self.session.commit()
        await self.session.refresh(incident)
        return incident

    async def get_incident(self, incident_id: str) -> Incident | None:
        return await self.session.get(Incident, incident_id)

    async def find_by_root_message(self, root_message_id: str) -> Incident | None:
        stmt = select(Incident).where(
            Incident.feishu_root_message_id == root_message_id
        )
        result = await self.session.execute(stmt)
        return result.scalar_one_or_none()

    async def find_by_fingerprint(
        self, fingerprint: str, within_minutes: int = 30
    ) -> Incident | None:
        cutoff = datetime.now(timezone.utc) - timedelta(minutes=within_minutes)
        cutoff_iso = cutoff.isoformat()
        stmt = (
            select(Incident)
            .where(
                Incident.fingerprint == fingerprint,
                Incident.created_at >= cutoff_iso,
            )
            .order_by(Incident.created_at.desc())
            .limit(1)
        )
        result = await self.session.execute(stmt)
        return result.scalar_one_or_none()

    async def update_incident(self, incident_id: str, **kwargs) -> None:
        kwargs["updated_at"] = datetime.now(timezone.utc).isoformat()
        stmt = (
            update(Incident).where(Incident.incident_id == incident_id).values(**kwargs)
        )
        await self.session.execute(stmt)
        await self.session.commit()

    async def list_incidents(self, limit: int = 50) -> list[Incident]:
        stmt = select(Incident).order_by(Incident.created_at.desc()).limit(limit)
        result = await self.session.execute(stmt)
        return list(result.scalars().all())

    # ── Messages ──

    async def save_message(
        self,
        incident_id: str,
        role: str,
        content: str,
        feishu_message_id: str | None = None,
    ) -> IncidentMessage:
        msg = IncidentMessage(
            incident_id=incident_id,
            role=role,
            content=content,
            feishu_message_id=feishu_message_id,
        )
        self.session.add(msg)
        await self.session.commit()
        return msg

    async def list_messages(self, incident_id: str) -> list[IncidentMessage]:
        stmt = (
            select(IncidentMessage)
            .where(IncidentMessage.incident_id == incident_id)
            .order_by(IncidentMessage.created_at.asc())
        )
        result = await self.session.execute(stmt)
        return list(result.scalars().all())

"""FastAPI 依赖注入"""
from collections.abc import AsyncGenerator

from sqlalchemy.ext.asyncio import AsyncSession

from incident_orchestrator.db.engine import get_session_factory
from incident_orchestrator.db.repository import Repository
from incident_orchestrator.services.incident_service import IncidentService
from incident_orchestrator.services.mutex import IncidentMutex

# 全局单例
_mutex = IncidentMutex()


async def get_db_session() -> AsyncGenerator[AsyncSession, None]:
    factory = get_session_factory()
    async with factory() as session:
        yield session


async def get_repository(
    session: AsyncSession = None,
) -> AsyncGenerator[Repository, None]:
    factory = get_session_factory()
    async with factory() as session:
        yield Repository(session)


async def get_incident_service() -> AsyncGenerator[IncidentService, None]:
    factory = get_session_factory()
    async with factory() as session:
        yield IncidentService(Repository(session))


def get_mutex() -> IncidentMutex:
    return _mutex

from fastapi import APIRouter

from incident_orchestrator.api.alerts import router as alerts_router
from incident_orchestrator.api.feishu_events import router as feishu_router
from incident_orchestrator.api.health import router as health_router
from incident_orchestrator.api.incidents import router as incidents_router
from incident_orchestrator.api.scan import router as scan_router

api_router = APIRouter()
api_router.include_router(health_router, tags=["health"])
api_router.include_router(alerts_router, tags=["alerts"])
api_router.include_router(incidents_router, tags=["incidents"])
api_router.include_router(feishu_router, tags=["feishu"])
api_router.include_router(scan_router, tags=["scan"])

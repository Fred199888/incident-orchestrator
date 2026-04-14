"""扫描端点 — 手动触发定时扫描"""
from fastapi import APIRouter, BackgroundTasks

from incident_orchestrator.services.scheduled_scan import scan_and_process

router = APIRouter(prefix="/api/v1")


@router.post("/scan")
async def trigger_scan(background_tasks: BackgroundTasks):
    """手动触发一次扫描（后台执行）"""
    background_tasks.add_task(scan_and_process)
    return {"status": "scan_started"}


@router.post("/scan/sync")
async def trigger_scan_sync():
    """同步扫描（等待结果返回）"""
    result = await scan_and_process()
    return result

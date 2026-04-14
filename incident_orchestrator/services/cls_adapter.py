"""CLS 日志查询适配层（复用现有 cls_query.py 的函数）"""
import logging

logger = logging.getLogger(__name__)

# 延迟导入 legacy 模块，避免启动时报错
_cls_loaded = False


def _ensure_legacy():
    global _cls_loaded
    if not _cls_loaded:
        import incident_orchestrator.legacy  # noqa: F401 — 注入 sys.path
        _cls_loaded = True


async def query_trace_logs(trace_id: str, alert_time_ms: int = 0) -> dict:
    """查询 traceId 对应的 CLS 日志链路

    返回: {
        "trace_chain": [...],
        "stack_trace_top3": [...],
        "cls_summary": str,
        "error_location": str,
        "raw_error_message": str,
    }
    """
    if not trace_id:
        return {"error": "no_trace_id"}

    _ensure_legacy()

    try:
        import asyncio

        from cls_query import query_all_topics_for_trace

        # cls_query 是同步的，用线程池执行
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            None, query_all_topics_for_trace, trace_id, alert_time_ms
        )
        return result or {"error": "no_results"}
    except ImportError:
        logger.warning("cls_query.py 未找到，跳过 CLS 查询")
        return {"error": "cls_query_not_available"}
    except Exception as e:
        logger.error(f"CLS 查询异常: {e}")
        return {"error": str(e)}

"""分类适配层（复用现有 triage.py 的函数）"""
import logging

logger = logging.getLogger(__name__)

_triage_loaded = False


def _ensure_legacy():
    global _triage_loaded
    if not _triage_loaded:
        import incident_orchestrator.legacy  # noqa: F401
        _triage_loaded = True


def classify_issue(
    subcode: str, cls_result: dict | None = None, service: str = "", api_path: str = ""
) -> tuple[str, str]:
    """分类告警，返回 (category, evidence)

    category: real_bug / business_expected / external_dependency / transient /
              attack / infrastructure / unfixable / unknown
    """
    _ensure_legacy()

    try:
        from triage import classify_issue as _classify

        # triage.py 签名: (issue: dict, cls_data: dict, duplicate_count: int)
        issue_dict = {
            "subcode": subcode,
            "service": service,
            "api_path": api_path,
        }
        return _classify(issue_dict, cls_result or {}, 1)
    except ImportError:
        logger.warning("triage.py 未找到，默认分类为 unknown")
        return ("unknown", "triage module not available")
    except Exception as e:
        logger.error(f"分类异常: {e}")
        return ("unknown", str(e))

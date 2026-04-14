"""告警处理编排：CLS 查询 → 分类 → Claude 分析 → 飞书回复"""
import json
import logging

from incident_orchestrator.db.engine import get_session_factory
from incident_orchestrator.db.repository import Repository
from incident_orchestrator.models.db import Incident
from incident_orchestrator.models.enums import IncidentStatus
from incident_orchestrator.feishu.card_templates import build_analysis_card
from incident_orchestrator.feishu.client import get_feishu_client
from incident_orchestrator.services.claude_runner import get_runner
from incident_orchestrator.services.cls_adapter import query_trace_logs
from incident_orchestrator.services.triage_adapter import classify_issue

logger = logging.getLogger(__name__)

# 非代码问题分类，跳过 Claude 分析
SKIP_CATEGORIES = {"business_expected", "external_dependency", "transient", "attack", "infrastructure"}


async def process_alert(incident: Incident) -> None:
    """后台任务：处理新告警 incident 的完整流程"""
    factory = get_session_factory()

    try:
        # 1. 更新状态 → ANALYZING
        async with factory() as session:
            repo = Repository(session)
            await repo.update_incident(incident.incident_id, status=IncidentStatus.ANALYZING)

        # 2. CLS 日志查询
        cls_result = {}
        if incident.trace_id:
            logger.info(f"[{incident.incident_id}] CLS 查询 trace_id={incident.trace_id}")
            cls_result = await query_trace_logs(incident.trace_id)

        # 3. 分类
        category, evidence = classify_issue(
            incident.subcode or "", cls_result,
            service=incident.service or "", api_path=incident.api_path or "",
        )
        logger.info(f"[{incident.incident_id}] 分类: {category} ({evidence})")

        if category in SKIP_CATEGORIES:
            async with factory() as session:
                repo = Repository(session)
                await repo.update_incident(
                    incident.incident_id,
                    status=IncidentStatus.REJECTED,
                    analysis_result=json.dumps(
                        {"category": category, "evidence": evidence}, ensure_ascii=False
                    ),
                )
            logger.info(f"[{incident.incident_id}] 非代码问题，跳过 ({category})")
            await _notify_skip(incident, category, evidence)
            return

        # 4. 构建 Claude 分析 prompt
        prompt = _build_analyze_prompt(incident, cls_result)

        # 5. 创建 Claude session + 分析
        runner = get_runner()
        session_id, result_text = await runner.create_session(
            incident.incident_id, prompt
        )
        worktree_name = f"inc-{incident.incident_id[:8]}"

        # 6. 更新 DB
        async with factory() as session:
            repo = Repository(session)
            await repo.update_incident(
                incident.incident_id,
                claude_session_id=session_id,
                worktree_name=worktree_name,
                status=IncidentStatus.ANALYZED,
                analysis_result=result_text[:10000] if result_text else "",
            )

        logger.info(f"[{incident.incident_id}] 分析完成，session={session_id}")
        await _send_analysis_card(incident, result_text)

    except Exception as e:
        logger.error(f"[{incident.incident_id}] 处理失败: {e}", exc_info=True)
        async with factory() as session:
            repo = Repository(session)
            await repo.update_incident(
                incident.incident_id,
                status=IncidentStatus.ESCALATED,
                analysis_result=json.dumps({"error": str(e)}, ensure_ascii=False),
            )


def _build_analyze_prompt(incident: Incident, cls_result: dict) -> str:
    """构建 Claude 分析 prompt"""
    parts = [
        "你是一个自动化 bug 修复助手。请分析以下告警并进行修复。",
        "",
        "## 告警信息",
        f"- 服务: {incident.service}",
        f"- 接口: {incident.api_path}",
        f"- 错误码: {incident.subcode}",
        f"- 严重度: {incident.severity}",
        f"- 摘要: {incident.summary}",
    ]

    if incident.trace_id:
        parts.append(f"- traceId: {incident.trace_id}")

    if cls_result and cls_result.get("trace_chain"):
        parts.append("")
        parts.append("## CLS 日志链路")
        parts.append("```")
        for entry in cls_result["trace_chain"][:20]:
            parts.append(str(entry))
        parts.append("```")

    if cls_result and cls_result.get("stack_trace_top3"):
        parts.append("")
        parts.append("## 堆栈摘要")
        for st in cls_result["stack_trace_top3"][:3]:
            parts.append(f"```\n{st}\n```")

    parts.append("")
    parts.append("## 要求")
    parts.append("1. 分析根因")
    parts.append("2. 在代码中定位问题")
    parts.append("3. 实施修复")
    parts.append("4. 创建修复分支并提交")
    parts.append("5. 输出修复总结")

    return "\n".join(parts)


async def _notify_skip(incident: Incident, category: str, evidence: str) -> None:
    """非代码问题，发飞书文本通知"""
    if not incident.feishu_root_message_id:
        return
    try:
        feishu = get_feishu_client()
        text = f"[自动分类] {category}: {evidence}"
        await feishu.reply_text(incident.feishu_root_message_id, text)
    except Exception as e:
        logger.error(f"[{incident.incident_id}] 飞书通知失败: {e}")


async def _send_analysis_card(incident: Incident, analysis_text: str) -> None:
    """分析完成后发飞书卡片到话题"""
    if not incident.feishu_root_message_id:
        return
    try:
        feishu = get_feishu_client()
        card = build_analysis_card(
            service=incident.service or "",
            api_path=incident.api_path or "",
            severity=incident.severity or "P2",
            analysis_summary=analysis_text[:2000] if analysis_text else "分析完成",
            fix_branch=incident.fix_branch or "",
            pr_url=incident.pr_url or "",
        )
        result = await feishu.send_card_reply(incident.feishu_root_message_id, card)
        # 更新 DB 中的 feishu_root_message_id（卡片消息可能不同于原告警消息）
        if result.get("code") == 0:
            logger.info(f"[{incident.incident_id}] 飞书卡片发送成功")
    except Exception as e:
        logger.error(f"[{incident.incident_id}] 飞书卡片发送失败: {e}")

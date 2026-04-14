"""定时扫描告警群 — 每 20 分钟拉最近 200 条消息

流程：
1. 拉最近 200 条 interactive 消息
2. 解析告警字段（service/tid/subcode/content）
3. 按 fingerprint 分组计数，>10 条的才处理
4. bitable 去重（已修复的跳过）
5. 检查消息是否已有话题回复（有的跳过）
6. 每个 unique fingerprint → 创建独立 Claude session 修复
7. 所有同 fingerprint 的消息 → 创建话题回复
"""
import asyncio
import json
import os
import re
from collections import Counter, defaultdict

from incident_orchestrator.config import get_settings
from incident_orchestrator.feishu.client import get_feishu_client
from incident_orchestrator.log import get_logger
from dataclasses import dataclass


@dataclass
class ParsedAlert:
    message_id: str
    chat_id: str
    service: str = ""
    subcode: str = ""
    trace_id: str = ""
    api_path: str = ""
    alert_time: str = ""
    fingerprint: str = ""
    alert_text: str = ""
from incident_orchestrator.services.alert_parser import normalize_api_path

logger = get_logger("SCAN")

SCAN_INTERVAL = 20 * 60  # 20 分钟
SCAN_MESSAGE_COUNT = 100
FREQUENCY_THRESHOLD = 10


def _extract_all_text(obj) -> list[str]:
    """递归提取所有 text/content 字符串"""
    parts = []
    if isinstance(obj, dict):
        if obj.get("tag") == "text" and "text" in obj:
            parts.append(obj["text"])
        if "content" in obj and isinstance(obj["content"], str) and not obj["content"].startswith("{"):
            parts.append(obj["content"])
        for v in obj.values():
            if isinstance(v, (dict, list)):
                parts.extend(_extract_all_text(v))
    elif isinstance(obj, list):
        for item in obj:
            parts.extend(_extract_all_text(item))
    return parts


def _parse_alert_from_message(msg: dict) -> ParsedAlert | None:
    """从飞书 list_messages 返回的消息解析告警字段"""
    if msg.get("msg_type") != "interactive":
        return None

    message_id = msg.get("message_id", "")
    chat_id = msg.get("chat_id", "") or ""

    body = msg.get("body", {})
    content_str = body.get("content", "")
    if not content_str:
        return None

    try:
        content = json.loads(content_str)
    except json.JSONDecodeError:
        return None

    texts = _extract_all_text(content)
    full_text = "".join(texts)

    if "监控对象" not in full_text:
        return None

    alert = ParsedAlert(message_id=message_id, chat_id=chat_id, alert_text=full_text)

    # 解析字段
    patterns = {
        "service": r"监控对象[：:]\s*(.+?)(?=\n|$)",
        "trace_id": r"tid[：:]\s*(.+?)(?=\n|$)",
        "subcode": r"[Ss]ub[Cc]ode[：:]\s*(.+?)(?=\n|$)",
        "api_path": r"接口[：:]\s*(.+?)(?=\n|$)",
        "alert_time": r"time[：:]\s*(.+?)(?=\n|$)",
    }
    for field, pattern in patterns.items():
        m = re.search(pattern, full_text, re.IGNORECASE)
        if m:
            setattr(alert, field, m.group(1).strip())

    # alert_text 保留完整文本（供 preprocess_alert 解析所有字段）
    # content 部分用于 fingerprint 计算
    content_m = re.search(r"content[：:]\s*(.+?)(?=\n前往|\Z)", full_text, re.DOTALL)
    error_content = content_m.group(1).strip()[:500] if content_m else ""

    # N/A 当作空
    if alert.trace_id == "N/A":
        alert.trace_id = ""

    if not alert.service:
        return None

    # 计算 fingerprint（用公共函数，写入和读取一致）
    from incident_orchestrator.services.fingerprint import extract_fingerprint
    alert.fingerprint = extract_fingerprint(alert.service, error_content)

    return alert


async def _fetch_messages(count: int = 200) -> list[dict]:
    """拉最近 count 条消息"""
    settings = get_settings()
    feishu = get_feishu_client()
    http = await feishu._ensure_http()
    headers = await feishu._headers()

    chat_id = settings.lark_chat_id
    messages = []
    page_token = ""

    while len(messages) < count:
        params = {
            "container_id_type": "chat",
            "container_id": chat_id,
            "page_size": min(50, count - len(messages)),
            "sort_type": "ByCreateTimeDesc",
        }
        if page_token:
            params["page_token"] = page_token

        resp = await http.get(
            f"{feishu.base_url}/open-apis/im/v1/messages",
            headers=headers,
            params=params,
        )
        data = resp.json()
        if data.get("code") != 0:
            logger.warning("拉消息失败: %s", data.get("msg"))
            break

        items = data.get("data", {}).get("items", [])
        if not items:
            break
        messages.extend(items)

        if not data.get("data", {}).get("has_more"):
            break
        page_token = data.get("data", {}).get("page_token", "")

    logger.info("拉取 %d 条消息", len(messages))
    return messages


async def _has_thread(feishu, message_id: str) -> bool:
    """通过 get_message 检查消息是否已有话题（thread_id 不为空）"""
    try:
        result = await feishu.get_message(message_id)
        if result.get("code") != 0:
            return False
        items = result.get("data", {}).get("items", [])
        if not items:
            return False
        return bool(items[0].get("thread_id"))
    except Exception:
        return False


async def sweep_pending_merge() -> dict:
    """扫全表把 ⏳等待合并 且实际已合入 release/stable 的记录升级为 ✅已合并

    场景：懒检查（`scan_and_process` 去重分支里的 check_branch_merged_async）
    只在"同一 fp 被新告警再次命中"时触发。PR 合并后，发版→线上不再报→
    该 fp 不再进 scheduled_scan → 懒检查永远不被触发 → bitable 永远停在
    ⏳等待合并。本函数是兜底：每轮扫描前跑一次，扫全表 ⏳等待合并 记录。

    流程：
      1. 拉全部 ⏳等待合并 记录的 (record_id, 分支)
      2. 批量 git fetch release/stable + 所有分支（减少网络往返）
      3. 逐条 check_branch_merged（同步版，因为已 fetch 过）
      4. 命中的调 mark_as_merged

    完全 0 token 消耗（只跑本地 git + bitable API）。任何异常都不向上
    抛，只打 warning，避免影响后续扫描。
    """
    from incident_orchestrator.services.bitable_service import (
        STATUS_PENDING_MERGE,
        mark_as_merged,
    )
    from incident_orchestrator.services.git_merge_check import (
        fetch_branches,
        check_branch_merged,
    )

    settings = get_settings()
    if not settings.monorepo_dir or not os.path.isdir(settings.monorepo_dir):
        logger.warning("sweep: monorepo_dir 不存在，跳过")
        return {"checked": 0, "merged": 0, "skipped": 0}
    if not settings.bitable_app_token:
        return {"checked": 0, "merged": 0, "skipped": 0}

    feishu = get_feishu_client()

    # 1. 拉 ⏳等待合并 的所有记录（只取 record_id + 分支 + fp 字段用途）
    pending: list[dict] = []
    try:
        http = await feishu._ensure_http()
        headers = await feishu._headers()
        page_token = None
        while True:
            body = {
                "filter": {
                    "conjunction": "and",
                    "conditions": [{
                        "field_name": "状态",
                        "operator": "is",
                        "value": [STATUS_PENDING_MERGE],
                    }],
                },
                "page_size": 500,
            }
            if page_token:
                body["page_token"] = page_token
            resp = await http.post(
                f"{feishu.base_url}/open-apis/bitable/v1/apps/{settings.bitable_app_token}"
                f"/tables/{settings.bitable_table_id}/records/search",
                headers=headers,
                json=body,
            )
            data = resp.json()
            if data.get("code") != 0:
                logger.warning("sweep: 查询失败: %s", data.get("msg"))
                return {"checked": 0, "merged": 0, "skipped": 0}
            payload = data.get("data", {})

            def _t(v):
                if isinstance(v, list) and v:
                    f = v[0]
                    return f.get("text", "") if isinstance(f, dict) else str(f)
                if isinstance(v, dict):
                    return v.get("text", "") or v.get("link", "")
                return str(v) if v else ""

            for rec in payload.get("items", []):
                fd = rec.get("fields", {})
                br = _t(fd.get("分支"))
                if not br:
                    continue
                pending.append({
                    "record_id": rec.get("record_id", ""),
                    "branch": br,
                    "fp": _t(fd.get("issue_fingerprint")),
                })
            if not payload.get("has_more"):
                break
            page_token = payload.get("page_token")
            if not page_token:
                break
    except Exception as e:
        logger.warning("sweep: 拉取 pending 异常: %s", e)
        return {"checked": 0, "merged": 0, "skipped": 0}

    if not pending:
        logger.info("sweep: 无 ⏳等待合并 记录，跳过")
        return {"checked": 0, "merged": 0, "skipped": 0}

    # 2. 批量 git fetch（同步调用扔线程池避免阻塞事件循环）
    branches = list({p["branch"] for p in pending})
    loop = asyncio.get_event_loop()
    try:
        await loop.run_in_executor(None, fetch_branches, settings.monorepo_dir, branches)
    except Exception as e:
        logger.warning("sweep: git fetch 批量异常: %s", e)
        # 继续往下跑，个别分支可能已经有本地 ref

    # 3. 逐条判定 + 升级
    checked = 0
    merged_cnt = 0
    for p in pending:
        checked += 1
        try:
            is_merged, detail = await loop.run_in_executor(
                None, check_branch_merged, settings.monorepo_dir, p["branch"]
            )
        except Exception as e:
            logger.warning("sweep: 检查 %s 异常: %s", p["branch"], e)
            continue

        if not is_merged:
            continue

        try:
            ok = await mark_as_merged(p["record_id"])
            if ok:
                merged_cnt += 1
                logger.info("sweep 升级: %s (%s) → ✅已合并 [%s]", p["fp"][:40], p["branch"], detail)
        except Exception as e:
            logger.warning("sweep: mark_as_merged 异常 %s: %s", p["record_id"], e)

    logger.info("sweep 完成: 检查 %d 条, 升级 %d 条", checked, merged_cnt)
    return {"checked": checked, "merged": merged_cnt, "skipped": checked - merged_cnt}


async def scan_and_process() -> dict:
    """执行一次扫描：sweep → 拉消息 → 分组 → 去重 → 修复 → 回复"""
    from incident_orchestrator.services.bitable_service import (
        query_existing_fingerprints,
        update_alert_count,
        mark_as_merged,
        write_record,
        STATUS_PENDING_MERGE,
        STATUS_MERGED,
        STATUS_BUSINESS_EXPECTED,
        STATUS_UNKNOWN,
        STATUS_NO_TRACE,
        TERMINAL_STATUSES,
    )
    from incident_orchestrator.services.git_merge_check import check_branch_merged_async
    from incident_orchestrator.config import get_mention_ids
    from incident_orchestrator.services.message_handler import (
        _build_fix_prompt,
        _extract_cause_and_fix,
    )
    from incident_orchestrator.services.fix_preprocessor import preprocess_alert
    from incident_orchestrator.services.fix_postprocessor import postprocess, format_fix_result
    from incident_orchestrator.services.claude_runner import get_runner

    feishu = get_feishu_client()
    runner = get_runner()

    # 0. sweep：升级已合并但不再冒告警的 ⏳等待合并 记录（0 token，纯 git）
    try:
        await sweep_pending_merge()
    except Exception:
        logger.exception("sweep 异常，继续执行扫描")

    # 1. 拉消息
    raw_messages = await _fetch_messages(SCAN_MESSAGE_COUNT)

    # 2. 解析告警
    alerts: list[ParsedAlert] = []
    for msg in raw_messages:
        alert = _parse_alert_from_message(msg)
        if alert:
            alerts.append(alert)

    logger.info("解析出 %d 条告警", len(alerts))

    # 3. 按 fingerprint 分组计数
    fp_groups: dict[str, list[ParsedAlert]] = defaultdict(list)
    for a in alerts:
        fp_groups[a.fingerprint].append(a)

    # 4. 过滤：>= FREQUENCY_THRESHOLD 才处理
    actionable_fps = {fp: group for fp, group in fp_groups.items() if len(group) >= FREQUENCY_THRESHOLD}
    logger.info(
        "频次过滤: %d 个 fingerprint, %d 个超过 %d 条",
        len(fp_groups), len(actionable_fps), FREQUENCY_THRESHOLD,
    )

    if not actionable_fps:
        return {"scanned": len(raw_messages), "alerts": len(alerts), "triggered": 0}

    # 5. bitable 去重（用 extract_fingerprint 生成的 fp 精准匹配）
    existing = await query_existing_fingerprints(list(actionable_fps.keys()))
    logger.info("bitable 已有 %d 个 fingerprint", len(existing))

    triggered = 0
    skipped = 0
    to_fix: list[tuple] = []

    def _extract_val(val):
        if isinstance(val, list) and val:
            return val[0].get("text", "") if isinstance(val[0], dict) else str(val[0])
        if isinstance(val, dict):
            return val.get("text", val.get("link", str(val)))
        return str(val) if val else ""

    settings = get_settings()

    for fp, group in actionable_fps.items():
        if fp in existing:
            # bitable 有历史记录 → 跳过修复
            info = existing[fp]
            status_val = _extract_val(info.get("status"))
            record_id = info.get("record_id", "")

            # 终态（如 ✅已合并）：完全静默，既不累计也不回复
            if status_val in TERMINAL_STATUSES:
                logger.info("去重 %s: 状态=%s，静默跳过（不累计不回复）", fp[:40], status_val)
                skipped += len(group)
                continue

            # ★ 懒检查：⏳等待合并 状态的记录，顺手查一次本地 git。
            # 若 fix 分支已合入 release/stable（尚未发布，告警还在涌入），
            # 就地升级为 ✅已合并，避免在已修复问题的话题下继续刷"累计告警次数"。
            if status_val == STATUS_PENDING_MERGE:
                branch = _extract_val(info.get("分支", ""))
                if branch and settings.monorepo_dir:
                    merged, detail = await check_branch_merged_async(
                        settings.monorepo_dir, branch
                    )
                    if merged:
                        if record_id:
                            await mark_as_merged(record_id)
                        logger.info(
                            "去重 %s: 懒检查命中合并（%s），升级为 ✅已合并并静默跳过",
                            fp[:40], detail,
                        )
                        skipped += len(group)
                        continue
                    # 未合入 → 落到下面累计+回复分支

            # 非终态：更新告警次数 + 在原话题下回复一条累计次数
            old_count = info.get("告警次数", 0)
            new_count = old_count + len(group)

            if record_id:
                await update_alert_count(record_id, new_count)
                logger.info("去重 %s: 告警次数 %d → %d", fp[:40], old_count, new_count)

            original_msg_id = _extract_val(info.get("message_id", ""))
            if original_msg_id:
                try:
                    await feishu.reply_text(
                        original_msg_id,
                        f"累计告警次数：{new_count}（本轮 +{len(group)}）",
                    )
                except Exception:
                    pass

            skipped += len(group)
            continue

        # 6. 找第一条没有话题的消息
        primary = None
        for alert in group:
            if not await _has_thread(feishu, alert.message_id):
                primary = alert
                break

        if not primary:
            logger.info("全部已回复: %s (%d 条)", fp[:40], len(group))
            skipped += len(group)
            continue

        # 收集需要修复的 issue
        to_fix.append((fp, group, primary))

    # 7. 并行修复（每个独立 worktree，互不影响）
    async def _fix_one(fp: str, group: list, primary) -> bool:
        """修复单个 issue，返回是否成功"""
        logger.info("处理 %s: %d 条, 回复 %s", fp[:40], len(group), primary.message_id[-8:])
        try:
            preprocess = await asyncio.get_event_loop().run_in_executor(
                None, preprocess_alert, primary.alert_text
            )

            if preprocess.cls_not_found:
                try:
                    await feishu.reply_text(primary.message_id, "未找到相关日志，无法定位根因。")
                except Exception:
                    pass
                return False

            prompt = _build_fix_prompt(primary.alert_text, "自动修复此告警", preprocess=preprocess)
            import uuid
            incident_id = f"INC-AUTO-{uuid.uuid4().hex[:8]}"
            session_id, result = await runner.create_session(incident_id, prompt)
            logger.info("session=%s, result_len=%d", session_id[:8], len(result))

            root_cause, fix_desc, error_type_hint = _extract_cause_and_fix(result or "")
            if preprocess.worktree_dir:
                post = await asyncio.get_event_loop().run_in_executor(
                    None,
                    lambda: postprocess(
                        preprocess.worktree_dir,
                        preprocess.branch_name,
                        preprocess.maven_module,
                        preprocess.module_path,
                        preprocess.service,
                        result or "",
                        root_cause=root_cause,
                        fix_desc=fix_desc,
                    ),
                )
                pr_url = post.pr_url
            else:
                pr_url = ""

            from incident_orchestrator.services.fix_postprocessor import git_blame_author
            blame_changed = post.changed_files if preprocess.worktree_dir else []
            blame_author = await asyncio.get_event_loop().run_in_executor(
                None, git_blame_author, preprocess.worktree_dir, blame_changed, root_cause,
            )


            from incident_orchestrator.services.reply_template import build_reply
            post_content = build_reply(
                service=preprocess.service,
                root_cause=root_cause,
                compile_ok=post.compile_success if preprocess.worktree_dir else False,
                branch=preprocess.branch_name,
                pr_url=pr_url,
                alert_count=len(group),
                owner=blame_author,
                has_fix=bool(post.changed_files) if preprocess.worktree_dir else False,
                has_worktree=bool(preprocess.worktree_dir),
            )

            await feishu.reply_message(primary.message_id, "post", post_content, reply_in_thread=True)

            await write_record(
                fingerprint=fp,
                service=preprocess.service,
                subcode=preprocess.subcode or "-",
                status=(
                    STATUS_PENDING_MERGE if (preprocess.worktree_dir and post.compile_success)
                    else STATUS_BUSINESS_EXPECTED if (preprocess.worktree_dir and not post.changed_files)
                    else STATUS_UNKNOWN
                ),
                task_name=fix_desc[:60] if fix_desc else root_cause[:60],
                pr_url=pr_url,
                branch=preprocess.branch_name,
                root_cause=post_content,
                root_cause_location=preprocess.error_location,
                error_type=preprocess.error_type or error_type_hint,
                tid=preprocess.tid or "-",
                owner=blame_author,
                alert_count=len(group),
                message_id=primary.message_id,
                claude_session_id=session_id,
            )
            return True

        except Exception as e:
            logger.exception("修复 %s 失败: %s", fp[:40], e)
            return False

    if to_fix:
        logger.info("并行修复 %d 个 issue", len(to_fix))
        results = await asyncio.gather(
            *[_fix_one(fp, group, primary) for fp, group, primary in to_fix],
            return_exceptions=True,
        )
        triggered = sum(1 for r in results if r is True)

    summary = {
        "scanned": len(raw_messages),
        "alerts": len(alerts),
        "unique_fps": len(fp_groups),
        "actionable": len(actionable_fps),
        "triggered": triggered,
        "skipped": skipped,
    }
    logger.info("扫描完成: %s", summary)
    return summary


_scan_lock = asyncio.Lock()


async def start_scheduled_scan():
    """启动定时扫描循环"""
    logger.info("定时扫描启动，间隔 %d 秒", SCAN_INTERVAL)
    while True:
        if _scan_lock.locked():
            logger.info("上一轮扫描未完成，跳过本轮")
        else:
            async with _scan_lock:
                try:
                    result = await scan_and_process()
                    logger.info("本轮扫描结果: %s", result)
                except Exception:
                    logger.exception("扫描异常")
        await asyncio.sleep(SCAN_INTERVAL)

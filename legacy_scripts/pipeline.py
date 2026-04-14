#!/usr/bin/env python3
"""
pipeline.py — Phase 1~1.7 全流程整合脚本

一次调用完成：飞书消息扫描 → bitable 历史匹配 → CLS 并发查询 → 三层去重。
Orchestrator 只需调一次，输出最终的 actionable issues + duplicates + skipped。

用法:
  python3 pipeline.py \
    --target-count 100 \
    --output-dir /tmp/bugfix/20260311_120000/ \
    [--max-pages 20] [--start-time 0] [--page-token "xxx"]

输出文件（全部写到 output-dir）:
  - scan-result.json       扫描结果（兼容现有格式）
  - bitable-records.json   bitable 原始记录
  - precheck-result.json   bitable 参考匹配
  - cls-results.json       CLS 全链路查询结果
  - dedup-result.json      最终去重结果（actionable / duplicates / skipped）
  - duplicate-mapping.json duplicate 消息映射
"""
import argparse
import json
import os
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- 本地模块导入 ---
from config import CLS_TOPIC_ID_MAP, FILTERED_USER_IDS

# scanner.py 的核心函数
from scanner import (
    _parse_msg_to_item,
    batch_check_bot_replies,
)
from lark_client import LarkClient
from config import BOT_ID, CHAT_ID

# bitable_query.py
from bitable_query import search_all_records
from config import BITABLE_APP_TOKEN, BITABLE_TABLE_ID

# precheck.py
from precheck import build_bitable_index, match_issues, is_precise_fingerprint, get_coarse_prefix

# cls_query.py（并发版）
from cls_query import (
    get_cls_client,
    get_unique_topics,
    query_single_topic,
    extract_stack_trace,
    extract_log_summary,
)
from tencentcloud.cls.v20201016 import models

# precise_dedup.py
from precise_dedup import extract_precise_location, compute_precise_fingerprint

# triage.py（subcode 规则分类）
from triage import classify_issue

import subprocess


# ============================================================
# 发版时间检测（git log 分析）
# ============================================================

def get_recent_deploys(since_hours: int = 24) -> list[dict]:
    """
    从 git log 获取最近的发版（release/stable 合并到 master）时间。
    返回按时间倒序的 deploy 列表。
    """
    try:
        result = subprocess.run(
            ["git", "log", "origin/master",
             f"--since={since_hours} hours ago",
             "--format=%H|%ai|%s",
             "--grep=Merge pull request.*release/",
             "--perl-regexp"],
            capture_output=True, text=True, timeout=10,
        )
        if result.returncode != 0:
            # fallback: 不用 perl-regexp
            result = subprocess.run(
                ["git", "log", "origin/master",
                 f"--since={since_hours} hours ago",
                 "--format=%H|%ai|%s",
                 "--grep=release/stable",
                 ],
                capture_output=True, text=True, timeout=10,
            )
    except Exception as e:
        print(f"  获取发版记录失败: {e}", file=sys.stderr)
        return []

    deploys = []
    for line in result.stdout.strip().split("\n"):
        if not line or "|" not in line:
            continue
        parts = line.split("|", 2)
        if len(parts) < 3:
            continue
        commit_hash, date_str, subject = parts
        # 解析时间 → 毫秒时间戳
        # 格式: 2026-04-02 11:50:02 +0800
        try:
            from datetime import datetime, timezone, timedelta
            dt = datetime.strptime(date_str.strip()[:19], "%Y-%m-%d %H:%M:%S")
            # 假设 +0800
            dt = dt.replace(tzinfo=timezone(timedelta(hours=8)))
            ts_ms = int(dt.timestamp() * 1000)
        except Exception:
            ts_ms = 0

        # 提取 PR 号
        pr_num = ""
        import re as _re
        m = _re.search(r'#(\d+)', subject)
        if m:
            pr_num = f"#{m.group(1)}"

        deploys.append({
            "commit": commit_hash[:10],
            "time": date_str.strip(),
            "ts_ms": ts_ms,
            "pr": pr_num,
            "subject": subject.strip(),
        })

    return deploys


def enrich_deploy_context(issues: list[dict], deploys: list[dict], window_minutes: int = 30):
    """
    为每条 issue 添加 deploy_context：如果 alert_time 在某次发版后 window_minutes 分钟内，
    标记为可能与发版相关。
    """
    if not deploys:
        return

    for issue in issues:
        try:
            alert_ts = int(issue.get("alert_time", "0"))
        except (ValueError, TypeError):
            continue
        if not alert_ts:
            continue

        for deploy in deploys:
            deploy_ts = deploy.get("ts_ms", 0)
            if not deploy_ts:
                continue
            diff_ms = alert_ts - deploy_ts
            # 告警在发版后 0 ~ window 分钟内
            if 0 <= diff_ms <= window_minutes * 60 * 1000:
                issue["deploy_context"] = {
                    "near_deploy": True,
                    "deploy_commit": deploy["commit"],
                    "deploy_time": deploy["time"],
                    "deploy_pr": deploy["pr"],
                    "deploy_subject": deploy["subject"],
                    "minutes_after_deploy": round(diff_ms / 60000, 1),
                }
                break
        else:
            issue["deploy_context"] = {"near_deploy": False}


# ============================================================
# Step 1: 扫描飞书消息（复用 scanner.py 逻辑）
# ============================================================

def step1_scan(
    chat_id: str,
    scan_count: int = 100,
    start_time: int = 0,
    page_token: str | None = None,
) -> dict:
    """扫描指定告警群最近 scan_count 条消息，返回 scan-result 格式 dict。"""
    client = LarkClient()
    seen_fingerprints: dict[str, dict] = {}
    duplicate_msgs = []
    total_read = 0
    errors = []

    print(f"[Step 1] 扫描群 {chat_id} 最近 {scan_count} 条消息...", file=sys.stderr)

    while total_read < scan_count:
        try:
            resp = client.list_messages(
                container_id_type="chat",
                container_id=chat_id,
                page_size=50,
                page_token=page_token,
                sort_type="ByCreateTimeDesc",
            )
        except Exception as e:
            errors.append(f"读取失败: {e}")
            print(f"  读取失败: {e}", file=sys.stderr)
            break

        if resp.get("code") != 0:
            errors.append(f"API 错误: {resp.get('msg')}")
            print(f"  API 错误: {resp.get('msg')}", file=sys.stderr)
            break

        data = resp.get("data", {})
        items = data.get("items", [])
        if not items:
            break

        total_read += len(items)

        # 过滤告警 bot 消息（post + interactive 两种格式）
        bot_msgs = [
            msg for msg in items
            if msg.get("sender", {}).get("sender_type") == "app"
            and msg.get("msg_type") in ("post", "interactive")
        ]

        # 时间截止检查
        reached_cutoff = False
        if start_time:
            filtered = []
            for m in bot_msgs:
                ct = int(m.get("create_time", "0"))
                if ct and ct < start_time:
                    reached_cutoff = True
                else:
                    filtered.append(m)
            bot_msgs = filtered

        # 全部解析，不检查 bot reply（回复时再跳过已回复的）
        for msg in bot_msgs:
            item = _parse_msg_to_item(msg)
            fp = item["fingerprint"]
            if fp in seen_fingerprints:
                duplicate_msgs.append({
                    "message_id": item["message_id"],
                    "fingerprint": fp,
                    "primary_issue_id": seen_fingerprints[fp]["issue_id"],
                    "subcode": item.get("subcode", ""),
                    "service": item.get("service", ""),
                    "api_path": item.get("api_path", ""),
                })
            else:
                issue_id = f"I{len(seen_fingerprints) + 1:03d}"
                item["issue_id"] = issue_id
                seen_fingerprints[fp] = item

        if reached_cutoff:
            break

        has_more = data.get("has_more", False)
        next_token = data.get("page_token", "")
        if has_more and next_token:
            page_token = next_token
        else:
            break

    new_issues = list(seen_fingerprints.values())
    print(f"[Step 1] 完成: 读取 {total_read} 条, {len(new_issues)} unique, {len(duplicate_msgs)} duplicates", file=sys.stderr)

    return {
        "total_read": total_read,
        "business_expected": [],
        "new_issues": new_issues,
        "duplicate_msgs": duplicate_msgs,
        "errors": errors,
    }


# ============================================================
# Step 1.5: 频次聚合过滤
# ============================================================

def step1_5_frequency_filter(scan_result: dict, min_count: int = 10) -> dict:
    """
    频次聚合过滤：按 subcode 统计总消息数（unique + duplicates）。
    高频 subcode（> min_count）分离出来给 step1_7 批量回复。
    低频 subcode 留在 pipeline 继续走 precheck → CLS → worker。
    """
    new_issues = scan_result.get("new_issues", [])
    duplicate_msgs = scan_result.get("duplicate_msgs", [])

    # 统计每个 subcode 的总出现次数
    subcode_count: dict[str, int] = {}
    for issue in new_issues:
        sc = issue.get("subcode", "")
        subcode_count[sc] = subcode_count.get(sc, 0) + 1
    for dup in duplicate_msgs:
        sc = dup.get("subcode", "")
        subcode_count[sc] = subcode_count.get(sc, 0) + 1

    # 高频 subcode 集合
    high_freq = {sc for sc, count in subcode_count.items() if count > min_count}

    # 分离高频 items — 有 TID 的留在 pipeline（可查 CLS），无 TID 的才批量回复
    def _has_valid_tid(item):
        tid = item.get("traceId", "")
        return tid and tid != "N/A"

    high_freq_issues = [i for i in new_issues if i.get("subcode", "") in high_freq and not _has_valid_tid(i)]
    high_freq_dups = [d for d in duplicate_msgs if d.get("subcode", "") in high_freq and not _has_valid_tid(d)]

    # 有 TID 的高频 issue 也留在 pipeline
    remaining_issues = [i for i in new_issues if i not in high_freq_issues]
    remaining_dups = [d for d in duplicate_msgs if d not in high_freq_dups]

    print(f"[Step 1.5] 频次过滤 (阈值 >{min_count}):", file=sys.stderr)
    for sc, count in sorted(subcode_count.items(), key=lambda x: -x[1]):
        status = "高频→批量回复" if sc in high_freq else "低频→pipeline"
        print(f"  {status}: {sc} ({count} 次)", file=sys.stderr)
    print(f"[Step 1.5] 完成: 高频 {len(high_freq_issues)} issues + {len(high_freq_dups)} dups → 批量回复, "
          f"低频 {len(remaining_issues)} issues + {len(remaining_dups)} dups → pipeline", file=sys.stderr)

    scan_result["new_issues"] = remaining_issues
    scan_result["duplicate_msgs"] = remaining_dups
    scan_result["high_freq_issues"] = high_freq_issues
    scan_result["high_freq_dups"] = high_freq_dups
    scan_result["frequency_filtered"] = {
        "subcode_counts": subcode_count,
        "threshold": min_count,
        "high_freq_subcodes": list(high_freq),
        "high_freq_issue_count": len(high_freq_issues),
        "high_freq_dup_count": len(high_freq_dups),
        "remaining_issue_count": len(remaining_issues),
        "remaining_dup_count": len(remaining_dups),
    }

    return scan_result


# ============================================================
# Step 1.7: 高频 subcode 批量回复
# ============================================================

def step1_7_reply_high_freq(scan_result: dict, output_dir: str) -> dict:
    """
    对高频 subcode 的每条消息发送飞书回复。
    复用 triage 分类 + batch_reply 的标题/内容模板。
    """
    from batch_reply import TRIAGE_TITLE_MAP, DEFAULT_TRIAGE_TITLE, build_post_content, _text_line

    high_freq_issues = scan_result.get("high_freq_issues", [])
    high_freq_dups = scan_result.get("high_freq_dups", [])
    freq_data = scan_result.get("frequency_filtered", {})
    subcode_counts = freq_data.get("subcode_counts", {})

    all_msgs = high_freq_issues + high_freq_dups
    if not all_msgs:
        print("[Step 1.7] 无高频消息需要回复", file=sys.stderr)
        return scan_result

    print(f"[Step 1.7] 高频批量回复: {len(all_msgs)} 条消息...", file=sys.stderr)

    # 断点续传: 加载已回复集合
    result_path = os.path.join(output_dir, "high-freq-reply-result.json")
    replied_set: set[str] = set()
    if os.path.exists(result_path):
        try:
            with open(result_path, "r", encoding="utf-8") as f:
                prev = json.load(f)
            replied_set = set(prev.get("replied_message_ids", []))
            print(f"  续传: 已回复 {len(replied_set)} 条", file=sys.stderr)
        except (json.JSONDecodeError, IOError):
            pass

    # 按 subcode 分组
    subcode_msgs: dict[str, list[dict]] = {}
    for item in all_msgs:
        sc = item.get("subcode", "")
        if sc:
            subcode_msgs.setdefault(sc, []).append(item)

    # 为每个 subcode 生成回复内容
    subcode_content: dict[str, str] = {}
    for subcode, msgs in subcode_msgs.items():
        count = subcode_counts.get(subcode, len(msgs))
        representative = msgs[0]

        # 用 triage 分类
        category, evidence = classify_issue(representative, {}, count)

        service = representative.get("service", "-")
        api_path = representative.get("api_path", "-")

        # 根据分类确定标题和描述
        if category in TRIAGE_TITLE_MAP:
            title, desc = TRIAGE_TITLE_MAP[category]
        elif subcode.endswith("Exception") or subcode.endswith("Error"):
            # Java 异常类名 — 标记为已知异常，非"无法判断"
            title = "⚠️无法追踪"
            desc = f"高频异常 {subcode}，无 traceId 无法精确定位"
        else:
            title, desc = DEFAULT_TRIAGE_TITLE

        lines = [
            _text_line(f"服务: {service} | 接口: {api_path}" if api_path and api_path != "-" else f"服务: {service}"),
            _text_line(f"原因: 高频告警（{count} 次），{evidence or subcode}"),
            _text_line(f"处理: {desc}"),
        ]
        subcode_content[subcode] = build_post_content(title, lines)

    # 批量预检已有 bot 回复的消息，加入 replied_set 直接跳过
    client = LarkClient()
    to_check = [m for m in all_msgs if m.get("message_id", "") and m["message_id"] not in replied_set]
    if to_check:
        # 复用 scanner 的并发检测
        from scanner import check_has_bot_reply
        from concurrent.futures import ThreadPoolExecutor, as_completed
        print(f"  预检 {len(to_check)} 条消息是否已有回复...", file=sys.stderr)
        with ThreadPoolExecutor(max_workers=10) as pool:
            futures = {pool.submit(check_has_bot_reply, client, m["message_id"]): m["message_id"] for m in to_check}
            for fut in as_completed(futures):
                mid = futures[fut]
                try:
                    if fut.result():
                        replied_set.add(mid)
                except Exception:
                    pass
        pre_skip = len([m for m in to_check if m["message_id"] in replied_set])
        print(f"  预检完成: {pre_skip} 条已有回复，将跳过", file=sys.stderr)

    sent = 0
    skipped = 0
    failed = 0
    errors = []

    for i, item in enumerate(all_msgs):
        msg_id = item.get("message_id", "")
        if not msg_id or msg_id in replied_set:
            skipped += 1
            continue

        sc = item.get("subcode", "")
        content = subcode_content.get(sc)
        if not content:
            skipped += 1
            continue

        # 限速: 每 5 条暂停 1 秒
        if sent > 0 and sent % 5 == 0:
            time.sleep(1.0)

        try:
            resp = client.reply_with_retry(msg_id, "post", content, reply_in_thread=True)
            if resp.get("code") == 0:
                replied_set.add(msg_id)
                sent += 1
            else:
                failed += 1
                err_msg = resp.get("msg") or resp.get("error", "unknown")
                errors.append({"message_id": msg_id, "error": err_msg})
        except Exception as e:
            failed += 1
            errors.append({"message_id": msg_id, "error": str(e)})

        # 每 20 条打印进度
        if (sent + failed) % 20 == 0:
            print(f"  进度: {sent} sent, {failed} failed, {skipped} skipped / {len(all_msgs)} total", file=sys.stderr)

    # 持久化结果
    reply_result = {
        "sent": sent,
        "failed": failed,
        "skipped": skipped,
        "total_messages": len(all_msgs),
        "replied_message_ids": sorted(replied_set),
        "errors": errors[:50],
    }
    with open(result_path, "w", encoding="utf-8") as f:
        json.dump(reply_result, f, ensure_ascii=False, indent=2)

    print(f"[Step 1.7] 完成: {sent} sent, {failed} failed, {skipped} skipped", file=sys.stderr)
    scan_result["high_freq_reply_result"] = reply_result
    return scan_result


# ============================================================
# Step 2: bitable 历史参考匹配
# ============================================================

def step2_precheck(new_issues: list[dict]) -> tuple[dict, dict]:
    """
    查询 bitable 历史记录 + 参考匹配。
    返回: (precheck_result, bitable_raw_data)
    """
    print(f"[Step 2] 查询 bitable 历史记录...", file=sys.stderr)

    try:
        items = search_all_records(BITABLE_APP_TOKEN, BITABLE_TABLE_ID)
    except Exception as e:
        print(f"  bitable 查询失败: {e}，跳过参考匹配", file=sys.stderr)
        items = []

    bitable_raw = {"items": items, "total": len(items)}
    print(f"  bitable 记录: {len(items)} 条", file=sys.stderr)

    # 构建索引 + 匹配
    ref_index = build_bitable_index(items)
    print(f"  参考索引: {len(ref_index)} 条", file=sys.stderr)

    all_issues, refs = match_issues(new_issues, ref_index)

    precheck_result = {
        "new_issues": all_issues,
        "bitable_refs": refs,
        "stats": {
            "total_scan_issues": len(new_issues),
            "ref_count": len(refs),
            "remaining_count": len(all_issues),
        },
    }

    print(f"[Step 2] 完成: 参考匹配 {len(refs)} 条", file=sys.stderr)
    return precheck_result, bitable_raw


# ============================================================
# Step 2.5: 已知问题回复（Bitable 匹配的直接回复，不走 CLS/Worker）
# ============================================================

def step2_5_reply_known_issues(
    new_issues: list[dict],
    duplicate_msgs: list[dict],
    bitable_refs: dict,
    output_dir: str,
) -> tuple[list[dict], list[dict]]:
    """
    对有 Bitable 历史记录的 issue 及其 duplicates 直接回复，从 pipeline 移出。
    回复内容从 Bitable 记录提取（root_cause_location、PR 等）。

    返回: (remaining_issues, remaining_dups) — 没有 Bitable 匹配的，继续走 CLS/Worker。
    """
    from batch_reply import build_post_content, _text_line, _text_with_link

    if not bitable_refs:
        print("[Step 2.5] 无 bitable 匹配，全部进入 CLS/Worker", file=sys.stderr)
        return new_issues, duplicate_msgs

    # 找出有 bitable 记录的 issue_ids
    matched_issue_ids = set(bitable_refs.keys())

    matched_issues = [i for i in new_issues if i.get("issue_id", "") in matched_issue_ids]
    remaining_issues = [i for i in new_issues if i.get("issue_id", "") not in matched_issue_ids]

    # duplicates 也按 primary_issue_id 分流
    matched_dup_ids = set()
    for dup in duplicate_msgs:
        primary = dup.get("primary_issue_id", "")
        if primary in matched_issue_ids:
            matched_dup_ids.add(dup.get("message_id", ""))
    matched_dups = [d for d in duplicate_msgs if d.get("message_id", "") in matched_dup_ids]
    remaining_dups = [d for d in duplicate_msgs if d.get("message_id", "") not in matched_dup_ids]

    all_to_reply = matched_issues + matched_dups
    if not all_to_reply:
        print("[Step 2.5] 匹配的 issue 无需回复（无 message_id）", file=sys.stderr)
        return remaining_issues, remaining_dups

    print(f"[Step 2.5] Bitable 已知问题回复: {len(matched_issues)} issues + {len(matched_dups)} dups...", file=sys.stderr)

    # 断点续传
    result_path = os.path.join(output_dir, "known-issue-reply-result.json")
    replied_set: set[str] = set()
    if os.path.exists(result_path):
        try:
            with open(result_path, "r", encoding="utf-8") as f:
                prev = json.load(f)
            replied_set = set(prev.get("replied_message_ids", []))
        except (json.JSONDecodeError, IOError):
            pass

    # 为每个 matched issue 生成回复内容（从 Bitable 记录提取）
    issue_content: dict[str, str] = {}  # issue_id → reply content
    for issue in matched_issues:
        issue_id = issue.get("issue_id", "")
        refs = bitable_refs.get(issue_id, [])
        if not refs:
            continue

        ref = refs[0]  # 取最佳匹配
        service = issue.get("service", "-")
        api_path = issue.get("api_path", "-")
        root_cause = ref.get("root_cause_location", "")
        pr_url = ref.get("PR", "")
        task_name = ref.get("任务名称", "")
        status = ref.get("状态", "")

        # 确定标题
        if pr_url:
            title = "⏳等待合并"
        elif status == "已完成":
            title = "⏳等待合并"
        else:
            # 用 triage 分类
            category, evidence = classify_issue(issue, {}, 0)
            from batch_reply import TRIAGE_TITLE_MAP, DEFAULT_TRIAGE_TITLE
            title, _ = TRIAGE_TITLE_MAP.get(category, DEFAULT_TRIAGE_TITLE)

        lines = [
            _text_line(f"服务: {service} | 接口: {api_path}"),
            _text_line(f"原因: {root_cause}" if root_cause else f"原因: {task_name}"),
        ]

        if pr_url:
            lines.append(_text_with_link("处理: 已有修复 PR ", "查看 PR", pr_url))
        elif status == "已完成":
            lines.append(_text_line("处理: 已修复，等待发版"))
        else:
            lines.append(_text_line("处理: 已有历史记录，跟踪中"))

        issue_content[issue_id] = build_post_content(title, lines)

    # 批量预检已有 bot 回复的消息
    client = LarkClient()
    to_check = [m for m in all_to_reply if m.get("message_id", "") and m["message_id"] not in replied_set]
    if to_check:
        from scanner import check_has_bot_reply
        from concurrent.futures import ThreadPoolExecutor, as_completed
        print(f"  预检 {len(to_check)} 条消息是否已有回复...", file=sys.stderr)
        with ThreadPoolExecutor(max_workers=10) as pool:
            futures = {pool.submit(check_has_bot_reply, client, m["message_id"]): m["message_id"] for m in to_check}
            for fut in as_completed(futures):
                mid = futures[fut]
                try:
                    if fut.result():
                        replied_set.add(mid)
                except Exception:
                    pass
        pre_skip = len([m for m in to_check if m["message_id"] in replied_set])
        if pre_skip:
            print(f"  预检: {pre_skip} 条已有回复，将跳过", file=sys.stderr)

    sent = 0
    failed = 0

    for item in all_to_reply:
        msg_id = item.get("message_id", "")
        if not msg_id or msg_id in replied_set:
            continue

        # 找对应的 issue_id（issue 自身或 duplicate 的 primary）
        iid = item.get("issue_id", "") or item.get("primary_issue_id", "")
        content = issue_content.get(iid)
        if not content:
            continue

        if sent > 0 and sent % 5 == 0:
            time.sleep(1.0)

        try:
            resp = client.reply_with_retry(msg_id, "post", content, reply_in_thread=True)
            if resp.get("code") == 0:
                replied_set.add(msg_id)
                sent += 1
            else:
                failed += 1
        except Exception:
            failed += 1

    # 持久化
    reply_result = {
        "sent": sent,
        "failed": failed,
        "matched_issues": len(matched_issues),
        "matched_dups": len(matched_dups),
        "replied_message_ids": sorted(replied_set),
    }
    with open(result_path, "w", encoding="utf-8") as f:
        json.dump(reply_result, f, ensure_ascii=False, indent=2)

    print(f"[Step 2.5] 完成: {sent} sent, {failed} failed, "
          f"剩余 {len(remaining_issues)} issues + {len(remaining_dups)} dups 进入 CLS/Worker", file=sys.stderr)

    return remaining_issues, remaining_dups


# ============================================================
# Step 3: CLS 并发全链路查询
# ============================================================

_CLS_SEMAPHORE = threading.Semaphore(20)  # 全局最大 20 并发 API 请求


def _query_single_topic_concurrent(client, topic_id, trace_id, from_ts, to_ts):
    """带信号量限流的单 topic 查询"""
    with _CLS_SEMAPHORE:
        return query_single_topic(client, topic_id, trace_id, from_ts, to_ts)


def _query_single_topic_by_keyword(client, topic_id, keyword, from_ts, to_ts):
    """带信号量限流的关键词查询（subcode fallback 用）"""
    with _CLS_SEMAPHORE:
        results = []
        try:
            req = models.SearchLogRequest()
            req.TopicId = topic_id
            req.Query = f"{keyword} AND level:ERROR"
            req.From = from_ts
            req.To = to_ts
            req.Limit = 10

            resp = client.SearchLog(req)

            if not hasattr(resp, "Results") or not resp.Results:
                return []

            for r in resp.Results:
                if not hasattr(r, "LogJson") or not r.LogJson:
                    continue
                try:
                    log = json.loads(r.LogJson)
                except json.JSONDecodeError:
                    log = {"__CONTENT__": r.LogJson}

                content = log.get("__CONTENT__", "")
                lines = content.split("\n") if content else []
                head5 = "\n".join(lines[:5]) if lines else ""
                ts = 0
                if hasattr(r, "Time"):
                    ts = r.Time
                elif hasattr(r, "BTime"):
                    ts = r.BTime

                level = ""
                for lv in ("ERROR", "WARN", "INFO", "DEBUG"):
                    if lv in (head5[:50] if head5 else ""):
                        level = lv
                        break

                results.append({
                    "content_head5": head5[:500],
                    "content_full": content,
                    "timestamp": ts,
                    "level": level,
                })
        except Exception:
            pass
        return results


def query_subcode_all_topics_concurrent(subcode: str, alert_time: str) -> dict:
    """
    无 traceId 时，用 subcode 作为关键词并发查询全部日志库。
    时间窗口: alert_time 前后 5 分钟。
    """
    try:
        alert_ts = int(alert_time)
    except (ValueError, TypeError):
        alert_ts = int(time.time() * 1000)

    from_ts = alert_ts - 300_000  # 前 5 分钟
    to_ts = alert_ts + 300_000    # 后 5 分钟

    topics = get_unique_topics()
    client = get_cls_client()

    all_logs = []
    queried_topics = []

    with ThreadPoolExecutor(max_workers=8) as ex:
        futures = {}
        for topic_name, topic_id in topics:
            f = ex.submit(_query_single_topic_by_keyword, client, topic_id, subcode, from_ts, to_ts)
            futures[f] = (topic_name, topic_id)

        for future in as_completed(futures):
            topic_name, topic_id = futures[future]
            try:
                logs = future.result()
                if logs:
                    queried_topics.append({"name": topic_name, "id": topic_id, "count": len(logs)})
                    for log in logs:
                        log["topic_name"] = topic_name
                    all_logs.extend(logs)
            except Exception:
                pass

    if not all_logs:
        return {
            "trace_chain": [],
            "stack_trace_top3": "",
            "cls_summary": "",
            "log_count": 0,
            "query_status": "no_logs_subcode_fallback",
            "query_params": {
                "keyword": subcode,
                "time_from": from_ts,
                "time_to": to_ts,
                "topics_queried": len(topics),
                "topics_with_logs": [],
            },
            "raw_error_message": "",
            "error_location": "",
            "userId": "",
            "error_type": "",
            "involved_topics": "",
        }

    all_logs.sort(key=lambda x: x["timestamp"])

    trace_chain = [
        {
            "topic_name": log["topic_name"],
            "timestamp": log["timestamp"],
            "level": log.get("level", ""),
            "content_head5": log["content_head5"],
        }
        for log in all_logs
    ]

    full_log_dicts = [{"__CONTENT__": log["content_full"]} for log in all_logs]
    stack_top3, summary, raw_error_message, error_location, user_id = extract_stack_trace(full_log_dicts)

    error_type = ""
    if raw_error_message:
        m = re.search(r'((?:\w+\.)*\w+(?:Exception|Error))', raw_error_message)
        if m:
            error_type = m.group(1).split(".")[-1]

    involved_topics = ",".join(t["name"] for t in queried_topics)

    return {
        "trace_chain": trace_chain,
        "stack_trace_top3": stack_top3,
        "cls_summary": summary,
        "log_count": len(all_logs),
        "query_status": "success_subcode_fallback",
        "query_params": {
            "keyword": subcode,
            "time_from": from_ts,
            "time_to": to_ts,
            "topics_queried": len(topics),
            "topics_with_logs": queried_topics,
        },
        "raw_error_message": raw_error_message,
        "error_location": error_location,
        "userId": user_id,
        "error_type": error_type,
        "involved_topics": involved_topics,
    }


def query_trace_all_topics_concurrent(trace_id: str, alert_time: str) -> dict:
    """
    并发查询单条 traceId 的全部日志库。
    每条 traceId 内部 8 个 topic 并发查询。
    """
    try:
        alert_ts = int(alert_time)
    except (ValueError, TypeError):
        alert_ts = int(time.time() * 1000)

    from_ts = alert_ts - 900_000  # 前 15 分钟
    to_ts = alert_ts + 300_000    # 后 5 分钟

    # topic_id → name 反向映射
    tid_to_name = {}
    for name, tid in CLS_TOPIC_ID_MAP.items():
        if tid not in tid_to_name:
            tid_to_name[tid] = name

    topics = get_unique_topics()
    client = get_cls_client()  # 每个 traceId 独立客户端

    all_logs = []
    queried_topics = []

    # 并发查询 8 个 topic
    with ThreadPoolExecutor(max_workers=8) as ex:
        futures = {}
        for topic_name, topic_id in topics:
            f = ex.submit(_query_single_topic_concurrent, client, topic_id, trace_id, from_ts, to_ts)
            futures[f] = (topic_name, topic_id)

        for future in as_completed(futures):
            topic_name, topic_id = futures[future]
            try:
                logs = future.result()
                if logs:
                    queried_topics.append({"name": topic_name, "id": topic_id, "count": len(logs)})
                    for log in logs:
                        log["topic_name"] = topic_name
                    all_logs.extend(logs)
            except Exception as e:
                print(f"    查询 {topic_name} 异常: {e}", file=sys.stderr)

    if not all_logs:
        return {
            "trace_chain": [],
            "stack_trace_top3": "",
            "cls_summary": "",
            "log_count": 0,
            "query_params": {
                "trace_id": trace_id,
                "time_from": from_ts,
                "time_to": to_ts,
                "topics_queried": len(topics),
                "topics_with_logs": [],
            },
            "raw_error_message": "",
            "error_location": "",
            "userId": "",
            "error_type": "",
            "involved_topics": "",
        }

    # 按时间排序
    all_logs.sort(key=lambda x: x["timestamp"])

    trace_chain = [
        {
            "topic_name": log["topic_name"],
            "timestamp": log["timestamp"],
            "level": log.get("level", ""),
            "content_head5": log["content_head5"],
        }
        for log in all_logs
    ]

    full_log_dicts = [{"__CONTENT__": log["content_full"]} for log in all_logs]
    stack_top3, summary, raw_error_message, error_location, user_id = extract_stack_trace(full_log_dicts)

    # 提取异常类名（短名）
    error_type = ""
    if raw_error_message:
        m = re.search(r'((?:\w+\.)*\w+(?:Exception|Error))', raw_error_message)
        if m:
            error_type = m.group(1).split(".")[-1]

    involved_topics = ",".join(t["name"] for t in queried_topics)

    return {
        "trace_chain": trace_chain,
        "stack_trace_top3": stack_top3,
        "cls_summary": summary,
        "log_count": len(all_logs),
        "query_params": {
            "trace_id": trace_id,
            "time_from": from_ts,
            "time_to": to_ts,
            "topics_queried": len(topics),
            "topics_with_logs": queried_topics,
        },
        "raw_error_message": raw_error_message,
        "error_location": error_location,
        "userId": user_id,
        "error_type": error_type,
        "involved_topics": involved_topics,
    }


def step3_cls_batch(new_issues: list[dict]) -> dict:
    """
    并发批量 CLS 全链路查询。
    10 条 issue 并发 × 8 topic 并发，Semaphore(20) 限流。
    无 traceId 的 issue 用 subcode + alert_time 作为 fallback 查询。
    """
    print(f"[Step 3] CLS 并发查询 {len(new_issues)} 条 issue...", file=sys.stderr)

    issues_with_trace = []
    issues_no_trace = []
    results = {}

    for issue in new_issues:
        issue_id = issue["issue_id"]
        trace_id = issue.get("traceId", "")
        if not trace_id or trace_id in ("N/A", "null", "undefined", ""):
            issues_no_trace.append(issue)
        else:
            issues_with_trace.append(issue)

    print(f"  有 traceId: {len(issues_with_trace)} 条, 无 traceId(subcode fallback): {len(issues_no_trace)} 条", file=sys.stderr)

    start_time = time.time()
    success_count = 0

    # --- traceId 查询 ---
    if issues_with_trace:
        with ThreadPoolExecutor(max_workers=10) as ex:
            futures = {}
            for issue in issues_with_trace:
                f = ex.submit(
                    query_trace_all_topics_concurrent,
                    issue["traceId"],
                    issue.get("alert_time", ""),
                )
                futures[f] = issue

            for future in as_completed(futures):
                issue = futures[future]
                issue_id = issue["issue_id"]
                try:
                    cls_result = future.result()
                    log_count = cls_result["log_count"]
                    status = "success" if log_count > 0 else "no_logs"
                    if log_count > 0:
                        success_count += 1

                    results[issue_id] = {
                        "issue_id": issue_id,
                        "trace_chain": cls_result["trace_chain"],
                        "cls_summary": cls_result["cls_summary"],
                        "stack_trace_top3": cls_result["stack_trace_top3"],
                        "log_count": log_count,
                        "query_status": status,
                        "query_params": cls_result["query_params"],
                        "raw_error_message": cls_result["raw_error_message"],
                        "error_location": cls_result["error_location"],
                        "userId": cls_result["userId"],
                        "error_type": cls_result.get("error_type", ""),
                        "involved_topics": cls_result.get("involved_topics", ""),
                    }
                except Exception as e:
                    print(f"  CLS 查询 {issue_id} 异常: {e}", file=sys.stderr)
                    results[issue_id] = {
                        "issue_id": issue_id,
                        "trace_chain": [],
                        "cls_summary": "",
                        "stack_trace_top3": "",
                        "log_count": 0,
                        "query_status": "error",
                        "query_params": {},
                        "raw_error_message": str(e),
                        "error_location": "",
                        "userId": "",
                        "error_type": "",
                        "involved_topics": "",
                    }

    # --- subcode fallback 查询（无 traceId 时） ---
    subcode_success = 0
    if issues_no_trace:
        with ThreadPoolExecutor(max_workers=10) as ex:
            futures = {}
            for issue in issues_no_trace:
                subcode = issue.get("subcode", "")
                if not subcode:
                    results[issue["issue_id"]] = {
                        "issue_id": issue["issue_id"],
                        "trace_chain": [], "cls_summary": "", "stack_trace_top3": "",
                        "log_count": 0, "query_status": "skipped_no_trace_no_subcode",
                        "query_params": {}, "raw_error_message": "",
                        "error_location": "", "userId": "",
                        "error_type": "", "involved_topics": "",
                    }
                    continue
                f = ex.submit(
                    query_subcode_all_topics_concurrent,
                    subcode,
                    issue.get("alert_time", ""),
                )
                futures[f] = issue

            for future in as_completed(futures):
                issue = futures[future]
                issue_id = issue["issue_id"]
                try:
                    cls_result = future.result()
                    log_count = cls_result["log_count"]
                    if log_count > 0:
                        subcode_success += 1
                    results[issue_id] = {
                        "issue_id": issue_id,
                        "trace_chain": cls_result["trace_chain"],
                        "cls_summary": cls_result["cls_summary"],
                        "stack_trace_top3": cls_result["stack_trace_top3"],
                        "log_count": log_count,
                        "query_status": cls_result["query_status"],
                        "query_params": cls_result["query_params"],
                        "raw_error_message": cls_result["raw_error_message"],
                        "error_location": cls_result["error_location"],
                        "userId": cls_result.get("userId", ""),
                        "error_type": cls_result.get("error_type", ""),
                        "involved_topics": cls_result.get("involved_topics", ""),
                    }
                except Exception as e:
                    print(f"  CLS subcode fallback {issue_id} 异常: {e}", file=sys.stderr)
                    results[issue_id] = {
                        "issue_id": issue_id,
                        "trace_chain": [], "cls_summary": "", "stack_trace_top3": "",
                        "log_count": 0, "query_status": "error",
                        "query_params": {}, "raw_error_message": str(e),
                        "error_location": "", "userId": "",
                        "error_type": "", "involved_topics": "",
                    }

    elapsed = time.time() - start_time
    total_queries = len(issues_with_trace) + len(issues_no_trace)
    print(f"[Step 3] 完成: traceId {len(issues_with_trace)} 条({success_count} 有日志), "
          f"subcode fallback {len(issues_no_trace)} 条({subcode_success} 有日志), "
          f"耗时 {elapsed:.1f}s", file=sys.stderr)

    return {"results": results}


# ============================================================
# Step 4: 三层去重
# ============================================================

def _extract_exception_method(stack_trace_top3: str, raw_error_message: str) -> tuple[str, str]:
    """
    从堆栈和错误消息中提取异常类型和方法名，用于 L2 指纹。
    返回: (exception_type, method_name)
    """
    # 提取异常类型
    exception_type = ""
    if raw_error_message:
        # 匹配 java.lang.NullPointerException 等
        m = re.search(r'((?:\w+\.)*\w+(?:Exception|Error))', raw_error_message)
        if m:
            # 只取短名
            full = m.group(1)
            exception_type = full.split(".")[-1]

    # 提取方法名（第一个 com.mindverse 栈帧的方法名）
    method_name = ""
    if stack_trace_top3:
        m = re.search(r'at\s+com\.mindverse\.\S+\.(\w+)\(', stack_trace_top3)
        if m:
            method_name = m.group(1)
        else:
            # fallback: 任意栈帧的方法名
            m = re.search(r'at\s+\S+\.(\w+)\(', stack_trace_top3)
            if m:
                method_name = m.group(1)

    return exception_type, method_name


def step4_dedup(
    new_issues: list[dict],
    cls_results: dict,
    precheck_result: dict,
) -> tuple[dict, dict]:
    """
    三层去重:
      L3 精准: {coarse_fp}_{File.java:line}
      L2 异常: {coarse_fp}_{Exception}_{Method}
      L1 粗:   scanner 阶段已去重（coarse_fp）

    去重范围:
      - 跨轮: L3 与 bitable 已完成记录比对
      - 同批次: L3/L2 相同只保留第一条
      - 无 traceId → skipped_no_trace（不启动 Worker）

    返回: (dedup_result, duplicate_mapping)
    """
    print(f"[Step 4] 三层去重 {len(new_issues)} 条 issue...", file=sys.stderr)

    cls_data = cls_results.get("results", {})
    bitable_refs = precheck_result.get("bitable_refs", {})

    # 收集 bitable 已完成精准记录
    completed_precise_fps = set()
    completed_refs_map = {}
    for issue_id, refs in bitable_refs.items():
        if not isinstance(refs, list):
            continue
        for ref in refs:
            fp = ref.get("fingerprint", "")
            is_precise = ref.get("is_precise", False)
            status = ref.get("状态", "")
            if is_precise and status == "已完成" and fp:
                completed_precise_fps.add(fp)
                completed_refs_map[fp] = ref

    # 收集 bitable 已完成记录的 L2 跨轮索引: {coarse_fp}_{error_type}_{error_location}
    completed_l2_fps = set()
    completed_l2_refs_map = {}
    for issue_id, refs in bitable_refs.items():
        if not isinstance(refs, list):
            continue
        for ref in refs:
            fp = ref.get("fingerprint", "")
            is_precise = ref.get("is_precise", False)
            status = ref.get("状态", "")
            et = ref.get("error_type", "")
            el = ref.get("error_location", "")
            if is_precise and status == "已完成" and et and el:
                coarse = get_coarse_prefix(fp) if is_precise_fingerprint(fp) else fp
                l2_key = f"{coarse}_{et}_{el}"
                completed_l2_fps.add(l2_key)
                completed_l2_refs_map[l2_key] = ref

    actionable_issues = []
    duplicates = []
    skipped_no_trace = []
    stats = {
        "total": len(new_issues),
        "actionable": 0,
        "duplicate_cross": 0,
        "duplicate_intra": 0,
        "skipped_no_trace": 0,
        "no_precise_fp": 0,
    }

    # 同批次内追踪
    seen_l3_fps = {}  # L3 fp -> first issue_id
    seen_l2_fps = {}  # L2 fp -> first issue_id

    for issue in new_issues:
        issue_id = issue.get("issue_id", "")
        coarse_fp = issue.get("fingerprint", "")
        cls_entry = cls_data.get(issue_id, {})
        query_status = cls_entry.get("query_status", "")

        # --- 无 traceId 且 subcode fallback 也无日志 → skipped ---
        if query_status in ("skipped_no_trace_id", "skipped_no_trace_no_subcode", "no_logs_subcode_fallback"):
            skipped_no_trace.append({
                "issue_id": issue_id,
                "message_id": issue.get("message_id", ""),
                "fingerprint": coarse_fp,
                "service": issue.get("service", ""),
                "api_path": issue.get("api_path", ""),
                "subcode": issue.get("subcode", ""),
                "reason": query_status,
            })
            stats["skipped_no_trace"] += 1
            continue

        stack_trace = cls_entry.get("stack_trace_top3", "")
        raw_error_msg = cls_entry.get("raw_error_message", "")

        # --- 计算 L3 精准指纹 ---
        location = extract_precise_location(stack_trace)
        l3_fp = compute_precise_fingerprint(coarse_fp, location) if location else ""

        # --- 计算 L2 异常指纹 ---
        exception_type, method_name = _extract_exception_method(stack_trace, raw_error_msg)
        l2_fp = ""
        if exception_type and method_name:
            l2_fp = f"{coarse_fp}_{exception_type}_{method_name}"

        # --- L3 跨轮去重 ---
        if l3_fp and l3_fp in completed_precise_fps:
            matched_ref = completed_refs_map[l3_fp]
            duplicates.append({
                **issue,
                "precise_fingerprint": l3_fp,
                "dedup_reason": "cross_round",
                "dedup_level": "L3",
                "matched_ref": matched_ref,
            })
            stats["duplicate_cross"] += 1
            print(f"  去重(L3 跨轮): {issue_id} fp={l3_fp}", file=sys.stderr)
            continue

        # --- L2 跨轮去重（error_type + error_location） ---
        cls_error_type = cls_entry.get("error_type", "")
        cls_error_location = cls_entry.get("error_location", "")
        if not l3_fp and cls_error_type and cls_error_location:
            l2_cross_key = f"{coarse_fp}_{cls_error_type}_{cls_error_location}"
            if l2_cross_key in completed_l2_fps:
                matched_ref = completed_l2_refs_map[l2_cross_key]
                duplicates.append({
                    **issue,
                    "precise_fingerprint": l2_cross_key,
                    "dedup_reason": "cross_round",
                    "dedup_level": "L2",
                    "matched_ref": matched_ref,
                })
                stats["duplicate_cross"] += 1
                print(f"  去重(L2 跨轮): {issue_id} key={l2_cross_key}", file=sys.stderr)
                continue

        # --- L3 同批次去重 ---
        if l3_fp and l3_fp in seen_l3_fps:
            first_id = seen_l3_fps[l3_fp]
            duplicates.append({
                **issue,
                "precise_fingerprint": l3_fp,
                "dedup_reason": "intra_batch",
                "dedup_level": "L3",
                "duplicate_of_issue": first_id,
            })
            stats["duplicate_intra"] += 1
            print(f"  去重(L3 同批次): {issue_id} → {first_id}", file=sys.stderr)
            continue

        # --- L2 同批次去重（同根因不同入口） ---
        if l2_fp and l2_fp in seen_l2_fps:
            first_id = seen_l2_fps[l2_fp]
            duplicates.append({
                **issue,
                "precise_fingerprint": l3_fp or l2_fp,
                "l2_fingerprint": l2_fp,
                "dedup_reason": "intra_batch",
                "dedup_level": "L2",
                "duplicate_of_issue": first_id,
            })
            stats["duplicate_intra"] += 1
            print(f"  去重(L2 同批次): {issue_id} → {first_id}", file=sys.stderr)
            continue

        # --- 保留为 actionable ---
        if l3_fp:
            seen_l3_fps[l3_fp] = issue_id
            issue["precise_fingerprint"] = l3_fp
        else:
            stats["no_precise_fp"] += 1
            issue["_dedup_note"] = "no_precise_fp"

        if l2_fp:
            seen_l2_fps[l2_fp] = issue_id
            issue["l2_fingerprint"] = l2_fp

        actionable_issues.append(issue)

    stats["actionable"] = len(actionable_issues)

    dedup_result = {
        "actionable_issues": actionable_issues,
        "duplicates": duplicates,
        "skipped_no_trace": skipped_no_trace,
        "stats": stats,
    }

    # --- 生成 duplicate-mapping.json ---
    # 来自 scan-result 的 duplicate_msgs（L1 粗指纹重复消息，需要 Worker 回复）
    # 这部分在 step1 已经生成，这里不再处理
    # duplicate-mapping 格式: { issue_id: [message_id, ...] }

    print(
        f"[Step 4] 完成: actionable {stats['actionable']}, "
        f"跨轮去重 {stats['duplicate_cross']}, "
        f"同批次去重 {stats['duplicate_intra']}, "
        f"无 traceId {stats['skipped_no_trace']}, "
        f"无精准 fp {stats['no_precise_fp']}",
        file=sys.stderr,
    )

    return dedup_result, {}


def build_duplicate_mapping(scan_result: dict) -> dict:
    """从 scan-result 的 duplicate_msgs 生成 duplicate-mapping.json"""
    mapping = {}
    for dup in scan_result.get("duplicate_msgs", []):
        primary_id = dup.get("primary_issue_id", "")
        msg_id = dup.get("message_id", "")
        if primary_id and msg_id:
            if primary_id not in mapping:
                mapping[primary_id] = []
            mapping[primary_id].append(msg_id)
    return mapping


def step5_triage(dedup_result: dict, cls_results: dict) -> dict:
    """Step 5: 对 actionable_issues 执行 triage 分类过滤，只保留 real_bug/unknown"""
    actionable = dedup_result["actionable_issues"]
    filtered = []
    skipped_triage = []

    for issue in actionable:
        iid = issue["issue_id"]
        cls_data = cls_results.get("results", {}).get(iid, {})

        # userId 黑名单过滤（测试账号等）
        cls_user_id = str(cls_data.get("userId", "")).strip()
        if cls_user_id and cls_user_id in FILTERED_USER_IDS:
            skipped_triage.append({
                "issue_id": iid,
                "message_id": issue.get("message_id", ""),
                "subcode": issue.get("subcode", ""),
                "service": issue.get("service", ""),
                "api_path": issue.get("api_path", ""),
                "fingerprint": issue.get("fingerprint", ""),
                "triage_category": "filtered_user",
                "triage_evidence": f"测试账号 userId={cls_user_id}",
            })
            print(f"  triage 过滤: {iid} → filtered_user (userId={cls_user_id})", file=sys.stderr)
            continue

        category, evidence = classify_issue(issue, cls_data, 0)

        if category in ("real_bug", "unknown"):
            issue["triage_category"] = category
            issue["triage_evidence"] = evidence
            filtered.append(issue)
        else:
            skipped_triage.append({
                "issue_id": iid,
                "message_id": issue.get("message_id", ""),
                "subcode": issue.get("subcode", ""),
                "service": issue.get("service", ""),
                "api_path": issue.get("api_path", ""),
                "fingerprint": issue.get("fingerprint", ""),
                "triage_category": category,
                "triage_evidence": evidence,
            })
            print(f"  triage 过滤: {iid} → {category} ({evidence})", file=sys.stderr)

    dedup_result["actionable_issues"] = filtered
    dedup_result["skipped_triage"] = skipped_triage
    stats = dedup_result.get("stats", {})
    stats["pre_triage_actionable"] = stats.get("actionable", len(actionable))
    stats["skipped_triage"] = len(skipped_triage)
    stats["actionable"] = len(filtered)

    print(f"  triage: {len(actionable)} → {len(filtered)} actionable, "
          f"{len(skipped_triage)} 过滤", file=sys.stderr)
    return dedup_result


# ============================================================
# Main: 串联 Step 1-5
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="Phase 1~1.7 全流程整合脚本")
    parser.add_argument("--scan-count", type=int, default=100, help="扫描最近 N 条消息")
    parser.add_argument("--output-dir", required=True, help="输出目录")
    parser.add_argument("--start-time", type=int, default=0, help="最早消息时间戳 ms")
    parser.add_argument("--page-token", default=None, help="起始 page_token（断点续传）")
    parser.add_argument("--freq-threshold", type=int, default=10, help="频次过滤阈值（同 subcode 需 > N 次）")
    args = parser.parse_args()

    output_dir = args.output_dir
    os.makedirs(output_dir, exist_ok=True)

    overall_start = time.time()

    # ── Step 1: 扫描飞书消息 ──
    scan_result = step1_scan(
        chat_id=CHAT_ID,
        scan_count=args.scan_count,
        start_time=args.start_time,
        page_token=args.page_token,
    )

    # ── Step 1.5: 频次聚合过滤 ──
    scan_result = step1_5_frequency_filter(scan_result, min_count=args.freq_threshold)

    # ── Step 1.7: 高频 subcode 批量回复 ──
    scan_result = step1_7_reply_high_freq(scan_result, output_dir=output_dir)

    # ── Step 1.6: 发版时间关联 ──
    deploys = get_recent_deploys(since_hours=24)
    if deploys:
        print(f"[Step 1.6] 检测到 {len(deploys)} 次最近发版:", file=sys.stderr)
        for d in deploys[:5]:
            print(f"  {d['time']} {d['pr']} {d['subject'][:60]}", file=sys.stderr)
        all_msgs = scan_result.get("new_issues", []) + scan_result.get("duplicate_msgs", [])
        enrich_deploy_context(scan_result.get("new_issues", []), deploys)
        near_deploy_count = sum(1 for i in scan_result.get("new_issues", [])
                                if i.get("deploy_context", {}).get("near_deploy"))
        print(f"[Step 1.6] 完成: {near_deploy_count} 条告警与发版时间相关", file=sys.stderr)
    else:
        print(f"[Step 1.6] 最近 24h 无发版记录", file=sys.stderr)

    with open(os.path.join(output_dir, "scan-result.json"), "w", encoding="utf-8") as f:
        json.dump(scan_result, f, ensure_ascii=False, indent=2)

    new_issues = scan_result.get("new_issues", [])
    if not new_issues:
        print("无 new_issues，流程结束", file=sys.stderr)
        # 写空的输出文件
        for fname in ["precheck-result.json", "cls-results.json", "dedup-result.json", "duplicate-mapping.json"]:
            with open(os.path.join(output_dir, fname), "w", encoding="utf-8") as f:
                json.dump({}, f)
        _print_summary(scan_result, {}, {"stats": {"total": 0, "actionable": 0}}, 0)
        return

    # ── Step 2: bitable 历史参考匹配 ──
    precheck_result, bitable_raw = step2_precheck(new_issues)
    with open(os.path.join(output_dir, "bitable-records.json"), "w", encoding="utf-8") as f:
        json.dump(bitable_raw, f, ensure_ascii=False, indent=2)
    with open(os.path.join(output_dir, "precheck-result.json"), "w", encoding="utf-8") as f:
        json.dump(precheck_result, f, ensure_ascii=False, indent=2)

    # ── Step 2.5: 已知问题直接回复（有 Bitable 记录的跳过 CLS/Worker）──
    bitable_refs = precheck_result.get("bitable_refs", {})
    duplicate_msgs = scan_result.get("duplicate_msgs", [])
    new_issues, duplicate_msgs = step2_5_reply_known_issues(
        new_issues, duplicate_msgs, bitable_refs, output_dir
    )
    scan_result["duplicate_msgs"] = duplicate_msgs

    if not new_issues:
        print("所有 issue 均有 bitable 历史记录，已全部回复，流程结束", file=sys.stderr)
        for fname in ["cls-results.json", "dedup-result.json", "duplicate-mapping.json"]:
            with open(os.path.join(output_dir, fname), "w", encoding="utf-8") as f:
                json.dump({}, f)
        _print_summary(scan_result, {}, {"stats": {"total": 0, "actionable": 0}}, 0)
        return

    # ── Step 3: CLS 并发全链路查询 ──
    cls_results = step3_cls_batch(new_issues)
    with open(os.path.join(output_dir, "cls-results.json"), "w", encoding="utf-8") as f:
        json.dump(cls_results, f, ensure_ascii=False, indent=2)

    # ── Step 4: 三层去重 ──
    dedup_result, _ = step4_dedup(new_issues, cls_results, precheck_result)

    # ── Step 5: triage 分类过滤 ──
    dedup_result = step5_triage(dedup_result, cls_results)

    with open(os.path.join(output_dir, "dedup-result.json"), "w", encoding="utf-8") as f:
        json.dump(dedup_result, f, ensure_ascii=False, indent=2)

    # ── duplicate-mapping.json ──
    dup_mapping = build_duplicate_mapping(scan_result)
    with open(os.path.join(output_dir, "duplicate-mapping.json"), "w", encoding="utf-8") as f:
        json.dump(dup_mapping, f, ensure_ascii=False, indent=2)

    # ── Step 5: 输出汇总 ──
    elapsed = time.time() - overall_start
    _print_summary(scan_result, cls_results, dedup_result, elapsed)


def _print_summary(scan_result: dict, cls_results: dict, dedup_result: dict, elapsed: float):
    """输出最终汇总统计"""
    stats = dedup_result.get("stats", {})
    scan_issues = len(scan_result.get("new_issues", []))
    scan_dups = len(scan_result.get("duplicate_msgs", []))

    print(f"\n{'='*60}", file=sys.stderr)
    print(f"Pipeline 完成 (耗时 {elapsed:.1f}s)", file=sys.stderr)
    print(f"  扫描: {scan_issues} unique issues, {scan_dups} duplicate msgs", file=sys.stderr)
    print(f"  去重: actionable={stats.get('actionable', 0)}, "
          f"跨轮去重={stats.get('duplicate_cross', 0)}, "
          f"同批次去重={stats.get('duplicate_intra', 0)}, "
          f"无 traceId={stats.get('skipped_no_trace', 0)}", file=sys.stderr)
    skipped_triage = stats.get("skipped_triage", 0)
    if skipped_triage > 0:
        print(f"  triage: 过滤={skipped_triage} (业务预期/外部依赖/基础设施等)", file=sys.stderr)
    print(f"{'='*60}\n", file=sys.stderr)

    # stdout JSON 摘要供 Orchestrator 读取
    summary = {
        "scan_issues": scan_issues,
        "scan_duplicates": scan_dups,
        "actionable": stats.get("actionable", 0),
        "duplicate_cross": stats.get("duplicate_cross", 0),
        "duplicate_intra": stats.get("duplicate_intra", 0),
        "skipped_no_trace": stats.get("skipped_no_trace", 0),
        "skipped_triage": stats.get("skipped_triage", 0),
        "elapsed_seconds": round(elapsed, 1),
    }
    print(json.dumps(summary, ensure_ascii=False))


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
pipeline_cls.py — CLS 告警群专用 pipeline

扫描 CLS 告警群（interactive 格式），所有 issue 都用 CLS 内容搜索诊断，
然后统一回复（包括 duplicates）。

与 pipeline.py（老群）独立运行，互不干扰。

用法:
  python3 pipeline_cls.py --output-dir /tmp/bugfix/cls_20260402/
"""
import argparse
import hashlib
import json
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from lark_client import LarkClient
from config import (
    CHAT_ID_CLS, CLS_TOPIC_ID_MAP, SERVICE_TO_CLS_TOPIC_NAME,
)
from scanner import _parse_interactive_content, check_has_bot_reply
from cls_query import get_cls_client, extract_stack_trace, extract_log_summary
from tencentcloud.cls.v20201016 import models


# ============================================================
# Step 1: 扫描
# ============================================================

def step1_scan(scan_count: int = 100) -> dict:
    """扫描 CLS 告警群最近 scan_count 条消息。"""
    client = LarkClient()
    seen_fps: dict[str, dict] = {}
    dup_msgs = []
    total_read = 0
    page_token = None

    print(f"[Step 1] 扫描 CLS 告警群最近 {scan_count} 条消息...", file=sys.stderr)

    while total_read < scan_count:
        try:
            resp = client.list_messages(
                container_id_type="chat", container_id=CHAT_ID_CLS,
                page_size=50, page_token=page_token, sort_type="ByCreateTimeDesc",
            )
        except Exception as e:
            print(f"  读取失败: {e}", file=sys.stderr)
            break
        if resp.get("code") != 0:
            print(f"  API 错误: {resp.get('msg')}", file=sys.stderr)
            break

        items = resp.get("data", {}).get("items", [])
        if not items:
            break
        total_read += len(items)

        for msg in items:
            if msg.get("sender", {}).get("sender_type") != "app" or msg.get("msg_type") != "interactive":
                continue
            item = _parse_msg(msg)
            fp = item["fingerprint"]
            if fp in seen_fps:
                dup_msgs.append({
                    "message_id": item["message_id"],
                    "fingerprint": fp,
                    "primary_issue_id": seen_fps[fp]["issue_id"],
                    "service": item.get("service", ""),
                    "error_class": item.get("error_class", ""),
                })
            else:
                item["issue_id"] = f"C{len(seen_fps) + 1:03d}"
                seen_fps[fp] = item

        has_more = resp.get("data", {}).get("has_more", False)
        next_token = resp.get("data", {}).get("page_token", "")
        if has_more and next_token:
            page_token = next_token
        else:
            break

    issues = list(seen_fps.values())
    print(f"[Step 1] 完成: {total_read} 条消息, {len(issues)} unique, {len(dup_msgs)} dups", file=sys.stderr)
    return {"total_read": total_read, "new_issues": issues, "duplicate_msgs": dup_msgs}


def _parse_msg(msg: dict) -> dict:
    """解析 interactive 消息。"""
    message_id = msg.get("message_id", "")
    create_time = msg.get("create_time", "")
    body_content = msg.get("body", {}).get("content", "{}")
    try:
        content = json.loads(body_content)
    except (json.JSONDecodeError, TypeError):
        content = {}

    parsed = _parse_interactive_content(content)
    service = parsed.get("title", "")

    tid = ""
    error_lines = []
    for line in parsed.get("lines", []):
        m = re.match(r'^tid[：:]\s*(\S+)', line)
        if m:
            val = m.group(1).strip()
            if val and val != "N/A":
                tid = val
        else:
            error_lines.append(line)

    # 从 interactive content 提取告警时间（time 字段）
    alert_time_str = parsed.get("alert_time_str", "")

    error_class = ""
    for line in error_lines:
        m = re.search(r'([A-Z][a-zA-Z]*(?:Exception|Error))', line)
        if m:
            error_class = m.group(1)
            break

    content_key = error_lines[0] if error_lines else ""

    if service and error_class:
        fp = f"{service}_{error_class}"
    elif service and content_key:
        fp = f"{service}_{hashlib.md5(content_key.encode()).hexdigest()[:8]}"
    else:
        fp = f"unknown_{hashlib.md5(body_content.encode()).hexdigest()[:8]}"

    return {
        "message_id": message_id, "service": service, "tid": tid,
        "error_class": error_class, "content_key": content_key,
        "error_lines": error_lines[:5], "alert_time": create_time,
        "alert_time_str": alert_time_str,  # e.g. "2026-04-02 17:17:25.249"
        "fingerprint": fp, "cls_topic_id": parsed.get("cls_topic_id", ""),
    }


# ============================================================
# Step 2: CLS 内容搜索（所有 issue 都查）
# ============================================================

def step2_cls_search(issues: list[dict]) -> dict:
    """用 TID 或内容关键词搜索 CLS，不跳过任何 issue。"""
    if not issues:
        return {"results": {}}

    print(f"[Step 2] CLS 搜索 {len(issues)} 条 issue...", file=sys.stderr)
    client = get_cls_client()
    results = {}

    for iss in issues:
        issue_id = iss["issue_id"]
        service = iss.get("service", "")
        tid = iss.get("tid", "")
        content_key = iss.get("content_key", "")
        error_class = iss.get("error_class", "")

        # 用告警时间 ±15 分钟作为搜索窗口
        from_ms, to_ms = _calc_time_window(iss)

        topics = _resolve_topics(service)
        if not topics:
            print(f"  {issue_id}: 无可用 topic", file=sys.stderr)
            results[issue_id] = {"logs": [], "log_count": 0, "cls_summary": "", "stack_trace": ""}
            continue

        # 构建查询列表（按优先级）: TID → 首行内容 → 异常类名
        queries = []
        if tid:
            queries.append(tid)
        if content_key:
            safe = content_key[:40].replace('"', '\\"')
            queries.append(f'content:"{safe}"')
        if error_class:
            queries.append(f"content:{error_class} AND level:ERROR")

        if not queries:
            queries.append("level:ERROR")

        # 逐查询 × 逐 topic 搜索，找到就停
        logs = []
        query_used = ""
        for q in queries:
            for topic_id in topics:
                logs = _cls_query(client, topic_id, q, from_ms, to_ms)
                if logs:
                    query_used = q
                    break
            if logs:
                break

        stack = extract_stack_trace(logs[0]["content"]) if logs else ""
        summary = extract_log_summary(logs[0]["content"]) if logs else ""

        tag = f"✓ {len(logs)}" if logs else "✗ 0"
        print(f"  {issue_id} [{service}] {tag} logs ({query_used[:50]})", file=sys.stderr)

        results[issue_id] = {
            "logs": logs[:5], "log_count": len(logs),
            "cls_summary": summary, "stack_trace": stack,
            "full_content": logs[0]["content"][:2000] if logs else "",
            "query": query_used,
        }

    found = sum(1 for r in results.values() if r["log_count"] > 0)
    print(f"[Step 2] 完成: {found} 有日志 / {len(issues)} 总", file=sys.stderr)
    return {"results": results}


def _calc_time_window(issue: dict) -> tuple[int, int]:
    """从告警的 time 字段计算 ±15 分钟搜索窗口（毫秒）。"""
    WINDOW_MS = 15 * 60 * 1000  # 15 分钟

    alert_time_str = issue.get("alert_time_str", "")
    if alert_time_str:
        try:
            from datetime import datetime, timezone, timedelta
            # 格式: "2026-04-02 17:17:25.249"（北京时间）
            dt = datetime.strptime(alert_time_str.split(".")[0], "%Y-%m-%d %H:%M:%S")
            dt = dt.replace(tzinfo=timezone(timedelta(hours=8)))
            center_ms = int(dt.timestamp() * 1000)
            return center_ms - WINDOW_MS, center_ms + WINDOW_MS
        except Exception:
            pass

    # fallback: 用消息创建时间
    create_time = issue.get("alert_time", "")
    if create_time:
        try:
            center_ms = int(create_time)
            return center_ms - WINDOW_MS, center_ms + WINDOW_MS
        except (ValueError, TypeError):
            pass

    # 最后 fallback: 最近 1 小时
    now_ms = int(time.time() * 1000)
    return now_ms - 3600_000, now_ms


def _resolve_topics(service: str) -> list[str]:
    """返回服务相关的 topic_id 列表（优先匹配的在前）。"""
    seen = set()
    result = []
    # 1. 直接 service → topic（最精确）
    tid = CLS_TOPIC_ID_MAP.get(service, "")
    if tid and tid not in seen:
        result.append(tid)
        seen.add(tid)
    # 2. service → topic_name → topic（映射）
    topic_name = SERVICE_TO_CLS_TOPIC_NAME.get(service, "")
    if topic_name:
        tid = CLS_TOPIC_ID_MAP.get(topic_name, "")
        if tid and tid not in seen:
            result.append(tid)
            seen.add(tid)
    # 3. 同前缀的 topic（如 os-main-* 系列）
    if service:
        prefix = service.split("-")[0] + "-" + service.split("-")[1] if "-" in service else service
        for name, tid in CLS_TOPIC_ID_MAP.items():
            if name.startswith(prefix) and tid not in seen:
                result.append(tid)
                seen.add(tid)
    return result


def _cls_query(client, topic_id: str, query: str, from_ms: int, to_ms: int) -> list[dict]:
    try:
        req = models.SearchLogRequest()
        req.TopicId = topic_id
        req.Query = query
        req.From = from_ms
        req.To = to_ms
        req.Limit = 10
        resp = client.SearchLog(req)
        if not resp.Results:
            return []
        logs = []
        for r in resp.Results:
            if not r.LogJson:
                continue
            try:
                log = json.loads(r.LogJson)
            except json.JSONDecodeError:
                log = {"content": r.LogJson}
            content = log.get("content", "") or log.get("__CONTENT__", "") or log.get("A", "")
            if not content:
                content = json.dumps(log, ensure_ascii=False)
            logs.append({
                "content": content, "level": log.get("level", ""),
                "logger": log.get("logger", ""), "tid": log.get("tid", ""),
            })
        return logs
    except Exception as e:
        print(f"    CLS 异常: {e}", file=sys.stderr)
        return []


# ============================================================
# Step 3: 统一回复（issue + duplicates）
# ============================================================

def step3_reply_all(issues: list[dict], cls_results: dict, scan_result: dict, output_dir: str):
    """对所有 issue 和 duplicates 回复诊断结果。"""
    from batch_reply import build_post_content, _text_line

    result_path = os.path.join(output_dir, "reply-result.json")
    replied_set: set[str] = set()
    if os.path.exists(result_path):
        try:
            replied_set = set(json.load(open(result_path)).get("replied_message_ids", []))
        except Exception:
            pass

    client = LarkClient()
    results_map = cls_results.get("results", {})
    dup_msgs = scan_result.get("duplicate_msgs", [])
    issue_ids = {i["issue_id"] for i in issues}

    # 收集所有待回复 message_ids
    all_msg_ids = [i["message_id"] for i in issues if i.get("message_id")]
    for dup in dup_msgs:
        if dup.get("primary_issue_id") in issue_ids and dup.get("message_id"):
            all_msg_ids.append(dup["message_id"])

    # 预检已有回复
    to_check = [mid for mid in all_msg_ids if mid not in replied_set]
    if to_check:
        print(f"  预检 {len(to_check)} 条...", file=sys.stderr)
        with ThreadPoolExecutor(max_workers=10) as pool:
            futs = {pool.submit(check_has_bot_reply, client, mid): mid for mid in to_check}
            for fut in as_completed(futs):
                try:
                    if fut.result():
                        replied_set.add(futs[fut])
                except Exception:
                    pass
        skipped = sum(1 for mid in to_check if mid in replied_set)
        if skipped:
            print(f"  预检: {skipped} 条已有回复", file=sys.stderr)

    # 为每个 issue 生成回复内容
    issue_content: dict[str, str] = {}
    for iss in issues:
        iid = iss["issue_id"]
        service = iss.get("service", "-")
        error_class = iss.get("error_class", "")
        content_key = iss.get("content_key", "")

        cls = results_map.get(iid, {})
        summary = cls.get("cls_summary", "")
        stack = cls.get("stack_trace", "")
        log_count = cls.get("log_count", 0)

        if log_count > 0 and stack and "com.mindverse" in stack:
            title = "⏳等待合并"
            reason = summary[:100] if summary else (error_class or content_key[:60])
            action = f"堆栈: {stack[:100]}"
        elif log_count > 0:
            title = "⚠️无法追踪"
            reason = summary[:100] if summary else (error_class or content_key[:60])
            action = f"CLS 查到 {log_count} 条日志，堆栈不含 com.mindverse 业务代码"
        else:
            title = "⚠️无法追踪"
            reason = error_class or content_key[:60] or "未知异常"
            action = "CLS 未找到对应日志"

        lines = [
            _text_line(f"服务: {service}"),
            _text_line(f"原因: {reason}"),
            _text_line(f"处理: {action}"),
        ]
        issue_content[iid] = build_post_content(title, lines)

    # 发送回复
    sent = 0

    def _send(msg_id, content_str):
        nonlocal sent
        if not msg_id or msg_id in replied_set:
            return
        if sent > 0 and sent % 5 == 0:
            time.sleep(1.0)
        try:
            resp = client.reply_with_retry(msg_id, "post", content_str, reply_in_thread=True)
            if resp.get("code") == 0:
                replied_set.add(msg_id)
                sent += 1
        except Exception:
            pass

    for iss in issues:
        content = issue_content.get(iss["issue_id"], "")
        if content:
            _send(iss.get("message_id", ""), content)

    for dup in dup_msgs:
        pid = dup.get("primary_issue_id", "")
        content = issue_content.get(pid, "")
        if content:
            _send(dup.get("message_id", ""), content)

    with open(result_path, "w", encoding="utf-8") as f:
        json.dump({"sent": sent, "replied_message_ids": sorted(replied_set)}, f, ensure_ascii=False, indent=2)
    print(f"[Step 3] 回复: {sent} sent, {len(replied_set) - sent} skipped", file=sys.stderr)


# ============================================================
# Main
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="CLS 告警群专用 pipeline")
    parser.add_argument("--scan-count", type=int, default=100)
    parser.add_argument("--output-dir", required=True)
    args = parser.parse_args()

    output_dir = args.output_dir
    os.makedirs(output_dir, exist_ok=True)
    start = time.time()

    # Step 1: 扫描
    scan_result = step1_scan(args.scan_count)
    issues = scan_result["new_issues"]

    # Step 2: CLS 搜索（所有 issue 都查）
    cls_results = step2_cls_search(issues)

    with open(os.path.join(output_dir, "scan-result.json"), "w", encoding="utf-8") as f:
        json.dump(scan_result, f, ensure_ascii=False, indent=2)
    with open(os.path.join(output_dir, "cls-results.json"), "w", encoding="utf-8") as f:
        json.dump(cls_results, f, ensure_ascii=False, indent=2)

    # Step 3: 统一回复
    step3_reply_all(issues, cls_results, scan_result, output_dir)

    elapsed = time.time() - start
    found = sum(1 for r in cls_results.get("results", {}).values() if r.get("log_count", 0) > 0)
    print(f"\n{'='*50}", file=sys.stderr)
    print(f"Pipeline CLS 完成 ({elapsed:.1f}s)", file=sys.stderr)
    print(f"  {scan_result['total_read']} 条消息 → {len(issues)} unique → CLS: {found}/{len(issues)} 有日志", file=sys.stderr)
    print(f"{'='*50}", file=sys.stderr)


if __name__ == "__main__":
    main()

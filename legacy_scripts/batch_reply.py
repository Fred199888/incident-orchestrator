#!/usr/bin/env python3
"""
batch_reply.py — Phase 2 批量回复非 actionable 消息

为 duplicates、skipped_triage、skipped_no_trace、scan duplicate_msgs
批量发送飞书话题回复。直接使用 lark_client.py，无需启动 Worker。

用法:
  python3 batch_reply.py --round-dir /tmp/bugfix/20260323_120000/
"""
import argparse
import json
import os
import sys
import time

from lark_client import LarkClient
from triage import classify_issue

# ── 限速 ──
RATE_LIMIT_BATCH = 5
RATE_LIMIT_SLEEP = 1.0

# ── triage_category → (标题, 中文说明) ──
TRIAGE_TITLE_MAP = {
    "business_expected": ("ℹ️业务预期", "业务预期行为，无需代码修复"),
    "external_dependency": ("ℹ️业务预期", "外部服务异常，非本服务代码问题"),
    "infrastructure": ("ℹ️业务预期", "基础设施层面问题，需运维处理"),
    "filtered_user": ("ℹ️业务预期", "测试账号触发的告警，无需处理"),
    "attack": ("ℹ️业务预期", "疑似恶意请求，非代码缺陷"),
    "transient": ("ℹ️业务预期", "网络抖动等偶发问题，无需代码修复"),
}
DEFAULT_TRIAGE_TITLE = ("❓无法判断", "无法确定原因，需人工排查")


def build_post_content(title: str, lines: list[list[dict]]) -> str:
    """构建飞书 post 消息的 content JSON 字符串"""
    return json.dumps({
        "zh_cn": {
            "title": title,
            "content": lines,
        }
    }, ensure_ascii=False)


def _text_line(text: str) -> list[dict]:
    return [{"tag": "text", "text": text}]


def _text_with_link(text: str, link_text: str, href: str) -> list[dict]:
    return [{"tag": "text", "text": text}, {"tag": "a", "text": link_text, "href": href}]


def _derive_title_and_desc(dup: dict) -> tuple[str, str]:
    """根据 subcode 用 triage 规则推导标题和说明"""
    category, evidence = classify_issue(dup, {}, 0)
    if category in TRIAGE_TITLE_MAP:
        return TRIAGE_TITLE_MAP[category]
    # real_bug / unknown → 说明已处理过
    return DEFAULT_TRIAGE_TITLE


def _get_ref_info(dup: dict, bitable_refs: dict) -> dict | None:
    """从 matched_ref 或 bitable_refs 获取历史修复信息"""
    matched_ref = dup.get("matched_ref", {})
    if matched_ref and (matched_ref.get("root_cause_location") or matched_ref.get("PR")):
        return matched_ref
    issue_id = dup.get("issue_id", "")
    refs = bitable_refs.get(issue_id, [])
    if refs:
        return refs[0]
    return None


def build_duplicate_reply(dup: dict, bitable_refs: dict, issue_index: dict,
                          issue_category_index: dict) -> tuple[str, str]:
    """构建去重消息的回复 — 继承原始 issue 的实际状态"""
    service = dup.get("service", "") or issue_index.get(dup.get("issue_id", ""), {}).get("service", "-")
    api_path = dup.get("api_path", "") or issue_index.get(dup.get("issue_id", ""), {}).get("api_path", "-")

    lines = [_text_line(f"服务: {service} | 接口: {api_path}")]

    dup_of = dup.get("duplicate_of_issue", "")
    ref = _get_ref_info(dup, bitable_refs)

    if ref and ref.get("PR"):
        # 有 PR → 等待合并
        title = "⏳等待合并"
        root_cause = ref.get("root_cause_location", "")
        lines.append(_text_line(f"原因: {root_cause}" if root_cause else "原因: 已有修复方案"))
        lines.append(_text_with_link("处理: 等待 PR 合并 ", "查看 PR", ref["PR"]))
    elif ref:
        # 有历史记录但无 PR → 用 triage 规则推导
        title, desc = _derive_title_and_desc(dup)
        root_cause = ref.get("root_cause_location", "")
        lines.append(_text_line(f"原因: {root_cause}" if root_cause else f"原因: {desc}"))
        lines.append(_text_line(f"处理: {desc}"))
    elif dup_of:
        # 同批次去重 → 继承原始 issue 的分类
        cat_info = issue_category_index.get(dup_of, {})
        cat = cat_info.get("category", "")
        if cat == "actionable":
            title = "⏳等待合并"
            lines.append(_text_line("原因: 同批次已有修复任务处理中"))
            lines.append(_text_line("处理: 等待修复完成"))
        elif cat == "skipped_triage":
            title, desc = TRIAGE_TITLE_MAP.get(
                cat_info.get("triage_category", ""), DEFAULT_TRIAGE_TITLE)
            evidence = cat_info.get("triage_evidence", desc)
            lines.append(_text_line(f"原因: {evidence}"))
            lines.append(_text_line(f"处理: {desc}"))
        elif cat == "skipped_no_trace":
            title = "⚠️无法追踪"
            lines.append(_text_line("原因: 告警消息中未包含 traceId"))
            lines.append(_text_line("处理: 请检查告警配置"))
        else:
            title, desc = _derive_title_and_desc(dup)
            lines.append(_text_line(f"原因: {desc}"))
            lines.append(_text_line(f"处理: {desc}"))
    else:
        # 无 ref 无 dup_of → 用 triage 规则推导
        title, desc = _derive_title_and_desc(dup)
        lines.append(_text_line(f"原因: {desc}"))
        lines.append(_text_line(f"处理: {desc}"))

    content = build_post_content(title, lines)
    return title, content


def build_triage_reply(item: dict) -> tuple[str, str]:
    """构建 triage 过滤消息的回复"""
    category = item.get("triage_category", "")
    title, category_desc = TRIAGE_TITLE_MAP.get(category, DEFAULT_TRIAGE_TITLE)

    service = item.get("service", "-")
    api_path = item.get("api_path", "-")
    evidence = item.get("triage_evidence", "-")

    lines = [
        _text_line(f"服务: {service} | 接口: {api_path}"),
        _text_line(f"原因: {evidence}"),
        _text_line(f"处理: {category_desc}"),
    ]

    content = build_post_content(title, lines)
    return title, content


def build_no_trace_reply(item: dict) -> tuple[str, str]:
    """构建无 traceId 消息的回复"""
    title = "⚠️无法追踪"

    service = item.get("service", "-")
    api_path = item.get("api_path", "-")

    lines = [
        _text_line(f"服务: {service} | 接口: {api_path}"),
        _text_line("原因: 告警消息中未包含 traceId，无法查询完整链路日志"),
        _text_line("处理: 请检查告警配置，确保消息中包含 traceId 字段"),
    ]

    content = build_post_content(title, lines)
    return title, content


def build_scan_dup_reply(dup_msg: dict, issue_category_index: dict) -> tuple[str, str]:
    """构建扫描阶段重复消息的回复 — 继承原始 issue 的分类"""
    service = dup_msg.get("service", "-")
    api_path = dup_msg.get("api_path", "-")
    primary_id = dup_msg.get("primary_issue_id", "")

    lines = [_text_line(f"服务: {service} | 接口: {api_path}")]

    cat_info = issue_category_index.get(primary_id, {})
    cat = cat_info.get("category", "")

    if cat == "actionable":
        title = "⏳等待合并"
        lines.append(_text_line("原因: 同批次已有修复任务处理中"))
        lines.append(_text_line("处理: 等待修复完成"))
    elif cat == "skipped_triage":
        title, desc = TRIAGE_TITLE_MAP.get(
            cat_info.get("triage_category", ""), DEFAULT_TRIAGE_TITLE)
        evidence = cat_info.get("triage_evidence", desc)
        lines.append(_text_line(f"原因: {evidence}"))
        lines.append(_text_line(f"处理: {desc}"))
    elif cat == "skipped_no_trace":
        title = "⚠️无法追踪"
        lines.append(_text_line("原因: 告警消息中未包含 traceId"))
        lines.append(_text_line("处理: 请检查告警配置"))
    elif cat == "duplicate":
        # 原始 issue 本身也是 duplicate → 用 triage 推导
        title, desc = _derive_title_and_desc(dup_msg)
        lines.append(_text_line(f"原因: {desc}"))
        lines.append(_text_line(f"处理: {desc}"))
    else:
        # 未知分类 → 用 triage 推导
        title, desc = _derive_title_and_desc(dup_msg)
        lines.append(_text_line(f"原因: {desc}"))
        lines.append(_text_line(f"处理: {desc}"))

    content = build_post_content(title, lines)
    return title, content


def load_json(path: str) -> dict:
    """加载 JSON 文件，不存在则返回空 dict"""
    if not os.path.exists(path):
        print(f"  文件不存在，跳过: {path}", file=sys.stderr)
        return {}
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def _build_issue_category_index(dedup_result: dict) -> dict:
    """构建 issue_id → {category, triage_category, triage_evidence} 索引"""
    index = {}
    for issue in dedup_result.get("actionable_issues", []):
        iid = issue.get("issue_id", "")
        if iid:
            index[iid] = {"category": "actionable"}
    for dup in dedup_result.get("duplicates", []):
        iid = dup.get("issue_id", "")
        if iid:
            index[iid] = {"category": "duplicate"}
    for item in dedup_result.get("skipped_triage", []):
        iid = item.get("issue_id", "")
        if iid:
            index[iid] = {
                "category": "skipped_triage",
                "triage_category": item.get("triage_category", ""),
                "triage_evidence": item.get("triage_evidence", ""),
            }
    for item in dedup_result.get("skipped_no_trace", []):
        iid = item.get("issue_id", "")
        if iid:
            index[iid] = {"category": "skipped_no_trace"}
    return index


def main():
    parser = argparse.ArgumentParser(description="Phase 2: 批量回复非 actionable 消息")
    parser.add_argument("--round-dir", required=True, help="本轮工作目录")
    args = parser.parse_args()
    round_dir = args.round_dir

    print("=" * 50, file=sys.stderr)
    print("Phase 2: batch_reply.py — 批量回复非 actionable 消息", file=sys.stderr)
    print("=" * 50, file=sys.stderr)

    # 1. 加载输入文件
    dedup_result = load_json(os.path.join(round_dir, "dedup-result.json"))
    scan_result = load_json(os.path.join(round_dir, "scan-result.json"))
    precheck_result = load_json(os.path.join(round_dir, "precheck-result.json"))
    result_path = os.path.join(round_dir, "batch-reply-result.json")

    # 构建 issue_id → issue 索引（用于补充字段）
    issue_index = {}
    for issue in scan_result.get("new_issues", []):
        iid = issue.get("issue_id", "")
        if iid:
            issue_index[iid] = issue

    bitable_refs = precheck_result.get("bitable_refs", {})

    # 构建 issue_id → category 索引（用于继承原始 issue 的分类）
    issue_category_index = _build_issue_category_index(dedup_result)

    # 2. 加载断点续传状态
    replied_set = set()
    if os.path.exists(result_path):
        prev = load_json(result_path)
        replied_set = set(prev.get("replied_message_ids", []))
        print(f"  断点续传: 已有 {len(replied_set)} 条已回复消息", file=sys.stderr)

    # 合并 step1.7 + step2.5 已回复的消息，避免重复回复
    for fname, label in [
        ("high-freq-reply-result.json", "高频回复(step1.7)"),
        ("known-issue-reply-result.json", "已知问题(step2.5)"),
    ]:
        prev_path = os.path.join(round_dir, fname)
        if os.path.exists(prev_path):
            prev_data = load_json(prev_path)
            prev_ids = set(prev_data.get("replied_message_ids", []))
            if prev_ids:
                replied_set |= prev_ids
                print(f"  合并{label}: 跳过 {len(prev_ids)} 条已回复消息", file=sys.stderr)

    # 3. 构建待回复任务列表: (message_id, category, title, content)
    tasks = []

    # 3a. duplicates
    for dup in dedup_result.get("duplicates", []):
        msg_id = dup.get("message_id", "")
        if not msg_id or msg_id in replied_set:
            continue
        title, content = build_duplicate_reply(dup, bitable_refs, issue_index, issue_category_index)
        tasks.append((msg_id, "duplicate", title, content))

    # 3b. skipped_triage
    for item in dedup_result.get("skipped_triage", []):
        msg_id = item.get("message_id", "")
        if not msg_id or msg_id in replied_set:
            continue
        title, content = build_triage_reply(item)
        tasks.append((msg_id, "skipped_triage", title, content))

    # 3c. skipped_no_trace
    for item in dedup_result.get("skipped_no_trace", []):
        msg_id = item.get("message_id", "")
        if not msg_id or msg_id in replied_set:
            continue
        title, content = build_no_trace_reply(item)
        tasks.append((msg_id, "skipped_no_trace", title, content))

    # 3d. scan duplicate_msgs
    for dup_msg in scan_result.get("duplicate_msgs", []):
        msg_id = dup_msg.get("message_id", "")
        if not msg_id or msg_id in replied_set:
            continue
        title, content = build_scan_dup_reply(dup_msg, issue_category_index)
        tasks.append((msg_id, "scan_duplicate", title, content))

    print(f"  待回复: {len(tasks)} 条（跳过已回复 {len(replied_set)} 条）", file=sys.stderr)

    if not tasks:
        print("  无待回复消息，跳过", file=sys.stderr)
        result = {
            "total": 0, "sent": 0, "failed": 0, "skipped_already_replied": len(replied_set),
            "replied_message_ids": list(replied_set),
            "details": {}, "errors": [],
        }
        with open(result_path, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        print(json.dumps({"total": 0, "sent": 0, "failed": 0}))
        return

    # 4. 限速发送
    client = LarkClient()
    sent_count = 0
    fail_count = 0
    errors = []
    category_stats = {}

    for i, (msg_id, category, title, content) in enumerate(tasks):
        # 限速
        if i > 0 and i % RATE_LIMIT_BATCH == 0:
            time.sleep(RATE_LIMIT_SLEEP)

        try:
            result = client.reply_with_retry(msg_id, "post", content, reply_in_thread=True)
            if result.get("code") == 0:
                replied_set.add(msg_id)
                sent_count += 1
                cat_stat = category_stats.setdefault(category, {"sent": 0, "failed": 0})
                cat_stat["sent"] += 1
            else:
                fail_count += 1
                cat_stat = category_stats.setdefault(category, {"sent": 0, "failed": 0})
                cat_stat["failed"] += 1
                errors.append({
                    "message_id": msg_id, "category": category,
                    "error": f"code={result.get('code')} msg={result.get('msg')}",
                })
                print(f"  ❌ {msg_id} ({category}): code={result.get('code')}", file=sys.stderr)
        except Exception as e:
            fail_count += 1
            cat_stat = category_stats.setdefault(category, {"sent": 0, "failed": 0})
            cat_stat["failed"] += 1
            errors.append({"message_id": msg_id, "category": category, "error": str(e)})
            print(f"  ❌ {msg_id} ({category}): {e}", file=sys.stderr)

    # 5. 写结果文件
    final_result = {
        "total": len(tasks),
        "sent": sent_count,
        "failed": fail_count,
        "skipped_already_replied": len(replied_set) - sent_count,
        "replied_message_ids": list(replied_set),
        "details": category_stats,
        "errors": errors,
    }

    with open(result_path, "w", encoding="utf-8") as f:
        json.dump(final_result, f, ensure_ascii=False, indent=2)

    print(f"  完成: 发送 {sent_count}, 失败 {fail_count}", file=sys.stderr)
    for cat, stat in category_stats.items():
        print(f"    {cat}: 发送 {stat['sent']}, 失败 {stat['failed']}", file=sys.stderr)

    # stdout 输出 JSON 摘要
    print(json.dumps({"total": len(tasks), "sent": sent_count, "failed": fail_count, "details": category_stats}))


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
worker_post_process.py — Worker 完成后的统一后处理

遍历 round_dir/issues/*/fix-result.json，为每条 issue 执行：
1. 飞书话题回复（从 lark_reply_content 或 reply_summary 生成）
2. Bitable 写入（11 字段纯模板映射）
3. Duplicate 消息回复（同内容发送到 duplicate_message_ids）

用法:
  python3 worker_post_process.py --round-dir /tmp/bugfix/20260402_104214
  python3 worker_post_process.py --round-dir /tmp/bugfix/20260402_104214 --dry-run
"""
import argparse
import json
import os
import sys
import time

from lark_client import LarkClient
from config import BITABLE_APP_TOKEN, BITABLE_TABLE_ID, GITHUB_REPO_URL


# ============================================================
# 标题前缀规则（纯 if-else）
# ============================================================

def determine_title(fix_status: str, reply_summary: dict) -> str:
    """根据 fix_status 和 reply_summary 确定飞书回复标题前缀"""
    fix_type = reply_summary.get("fix_type", "")

    if fix_status == "success":
        return "⏳等待合并"
    elif fix_status == "diagnosed" and fix_type in ("已修复", "等待合并"):
        return "⏳等待合并"
    elif fix_type in ("业务预期", "external_dependency", "business_expected"):
        return "ℹ️业务预期"
    elif fix_status in ("skipped", "no_trace"):
        return "⚠️无法追踪"
    elif fix_status == "duplicate":
        # duplicate 继承原始状态
        return "⏳等待合并"
    else:
        return "❓无法判断"


# ============================================================
# 飞书 post 内容生成
# ============================================================

def _text_line(text: str) -> list[dict]:
    return [{"tag": "text", "text": text}]


def _text_with_link(text: str, link_text: str, href: str) -> list[dict]:
    return [{"tag": "text", "text": text}, {"tag": "a", "text": link_text, "href": href}]


def build_post_content(title: str, lines: list[list[dict]]) -> str:
    """构建飞书 post 消息的 content JSON 字符串"""
    return json.dumps({
        "zh_cn": {
            "title": title,
            "content": lines,
        }
    }, ensure_ascii=False)


def build_reply_content(title: str, fix_result: dict, issue_json: dict) -> str:
    """从 fix-result.json 生成飞书 post JSON"""
    service = issue_json.get("service", "-")
    api_path = issue_json.get("api_path", "-")
    reply_summary = fix_result.get("reply_summary", {})

    # 优先使用 Worker LLM 生成的自然语言内容
    lark_reply = fix_result.get("lark_reply_content", "")
    if lark_reply:
        # lark_reply_content 是纯文本，按行拆分
        text_lines = lark_reply.strip().split("\n")
        lines = [_text_line(line) for line in text_lines if line.strip()]

        # 追加 PR 链接（如果有 branch）
        branch = fix_result.get("branch", "")
        if branch and GITHUB_REPO_URL:
            pr_url = f"{GITHUB_REPO_URL}/compare/master...{branch}?expand=1"
            lines.append(_text_with_link("PR: ", pr_url, pr_url))

        # 追加发版关联（如果有）
        deploy_check = fix_result.get("deploy_check") or fix_result.get("diagnosis", {}).get("deploy_check")
        if deploy_check and deploy_check.get("introduced_by_release"):
            conclusion = deploy_check.get("conclusion", "")
            if conclusion:
                lines.append(_text_line(f"发版关联: {conclusion}"))

        return build_post_content(title, lines)

    # Fallback: 从 reply_summary 生成模板内容
    root_cause = reply_summary.get("root_cause_brief", "-")
    fix_desc = reply_summary.get("fix_description_brief", "-")

    lines = [
        _text_line(f"服务: {service} | 接口: {api_path}"),
        _text_line(f"原因: {root_cause}"),
        _text_line(f"处理: {fix_desc}"),
    ]

    branch = fix_result.get("branch", "")
    if branch and GITHUB_REPO_URL:
        pr_url = f"{GITHUB_REPO_URL}/compare/master...{branch}?expand=1"
        lines.append(_text_with_link("PR: ", pr_url, pr_url))

    return build_post_content(title, lines)


# ============================================================
# Bitable 字段映射（11 字段纯模板）
# ============================================================

def build_bitable_fields(issue_json: dict, fix_result: dict) -> dict:
    """从 issue + fix-result 映射到 bitable 字段"""
    reply_summary = fix_result.get("reply_summary", {})
    subcode = issue_json.get("subcode", "")
    brief = reply_summary.get("fix_description_brief", "")
    branch = fix_result.get("branch", "")

    task_name = f"{subcode}: {brief}" if brief else f"fix: {subcode}"

    pr_url = ""
    if branch and GITHUB_REPO_URL:
        pr_url = f"{GITHUB_REPO_URL}/compare/master...{branch}?expand=1"

    trace_id = issue_json.get("traceId", "") or "-"

    fields = {
        "任务名称": task_name,
        "服务名": issue_json.get("service", ""),
        "分支": branch,
        "状态": "已完成",
        "优先级": "高",
        "负责人": "Claude Code",
        "tid": trace_id,
        "PR": pr_url,
        "issue_fingerprint": fix_result.get("precise_fingerprint", "") or issue_json.get("fingerprint", ""),
        "root_cause_location": reply_summary.get("root_cause_brief", ""),
        "完成时间": int(time.time() * 1000),
    }

    # 可选字段
    error_type = fix_result.get("error_type", "")
    if error_type:
        fields["error_type"] = error_type
    error_location = fix_result.get("error_location", "")
    if error_location:
        fields["error_location"] = error_location

    return fields


# ============================================================
# 主流程
# ============================================================

def post_process_round(round_dir: str, dry_run: bool = False) -> dict:
    """遍历 round_dir/issues/*/fix-result.json 执行后处理"""

    issues_dir = os.path.join(round_dir, "issues")
    if not os.path.isdir(issues_dir):
        print(f"无 issues 目录: {issues_dir}", file=sys.stderr)
        return {"total": 0, "reply_sent": 0, "bitable_written": 0, "errors": []}

    # 加载 scan-result 获取 issue 原始信息
    scan_path = os.path.join(round_dir, "scan-result.json")
    issue_index = {}
    if os.path.exists(scan_path):
        scan = json.load(open(scan_path, encoding="utf-8"))
        for iss in scan.get("new_issues", []):
            issue_index[iss.get("issue_id", "")] = iss

    # 加载 duplicate-mapping
    dup_map_path = os.path.join(round_dir, "duplicate-mapping.json")
    dup_mapping = {}
    if os.path.exists(dup_map_path):
        dup_mapping = json.load(open(dup_map_path, encoding="utf-8"))

    client = None if dry_run else LarkClient()
    stats = {"total": 0, "reply_sent": 0, "bitable_written": 0, "dup_replied": 0, "skipped": 0, "errors": []}
    preview = []

    for issue_id in sorted(os.listdir(issues_dir)):
        issue_dir = os.path.join(issues_dir, issue_id)
        fix_path = os.path.join(issue_dir, "fix-result.json")
        if not os.path.isfile(fix_path):
            continue

        fix_result = json.load(open(fix_path, encoding="utf-8"))
        stats["total"] += 1

        # 跳过已处理的
        if fix_result.get("reply_status") == "sent" and fix_result.get("bitable_status") == "written":
            stats["skipped"] += 1
            continue

        # 跳过 duplicate（Phase 2 batch_reply 已回复）
        if fix_result.get("fix_status") == "duplicate":
            stats["skipped"] += 1
            continue

        issue_json = issue_index.get(issue_id, fix_result.get("issue_json", {}))
        if not issue_json:
            # 尝试从 fix-result 提取基本信息
            issue_json = {
                "message_id": fix_result.get("message_id", ""),
                "service": fix_result.get("service", ""),
                "api_path": fix_result.get("api_path", ""),
                "subcode": fix_result.get("subcode", ""),
                "traceId": fix_result.get("traceId", ""),
                "fingerprint": fix_result.get("fingerprint", ""),
            }

        message_id = issue_json.get("message_id", "") or fix_result.get("message_id", "")
        reply_summary = fix_result.get("reply_summary", {})
        fix_status = fix_result.get("fix_status", "")

        # 1. 确定标题
        title = determine_title(fix_status, reply_summary)

        # 2. 生成飞书 post 内容
        content = build_reply_content(title, fix_result, issue_json)

        if dry_run:
            preview.append({
                "issue_id": issue_id,
                "title": title,
                "content_preview": content[:300],
                "bitable_fields": build_bitable_fields(issue_json, fix_result),
                "dup_count": len(dup_mapping.get(issue_id, [])),
            })
            continue

        # 3. 飞书话题回复
        try:
            if message_id and fix_result.get("reply_status") != "sent":
                reply_resp = client.reply_with_retry(message_id, "post", content, reply_in_thread=True)
                if reply_resp.get("code") == 0:
                    fix_result["reply_status"] = "sent"
                    stats["reply_sent"] += 1
                else:
                    err_msg = reply_resp.get("msg") or reply_resp.get("error", "unknown")
                    fix_result["reply_status"] = f"error: {err_msg}"
                    stats["errors"].append(f"{issue_id} reply: {err_msg}")
        except Exception as e:
            stats["errors"].append(f"{issue_id} reply: {e}")
            fix_result["reply_status"] = f"error: {e}"

        # 4. Duplicate 消息回复
        dup_msg_ids = dup_mapping.get(issue_id, [])
        for msg_id in dup_msg_ids:
            try:
                client.reply_with_retry(msg_id, "post", content, reply_in_thread=True)
                stats["dup_replied"] += 1
            except Exception as e:
                stats["errors"].append(f"{issue_id} dup {msg_id}: {e}")

        # 5. Bitable 写入
        try:
            if fix_result.get("bitable_status") != "written":
                fields = build_bitable_fields(issue_json, fix_result)
                bt_resp = client.create_record_with_retry(BITABLE_APP_TOKEN, BITABLE_TABLE_ID, fields)
                if bt_resp.get("code") == 0:
                    fix_result["bitable_status"] = "written"
                    stats["bitable_written"] += 1
                else:
                    err_msg = bt_resp.get("msg") or bt_resp.get("error", "unknown")
                    fix_result["bitable_status"] = f"error: {err_msg}"
                    stats["errors"].append(f"{issue_id} bitable: {err_msg}")
        except Exception as e:
            stats["errors"].append(f"{issue_id} bitable: {e}")
            fix_result["bitable_status"] = f"error: {e}"

        # 6. 回写 fix-result.json
        with open(fix_path, "w", encoding="utf-8") as f:
            json.dump(fix_result, f, ensure_ascii=False, indent=2)

    if dry_run and preview:
        preview_path = os.path.join(round_dir, "post-process-preview.json")
        with open(preview_path, "w", encoding="utf-8") as f:
            json.dump(preview, f, ensure_ascii=False, indent=2)
        print(f"Dry-run 预览写入: {preview_path}", file=sys.stderr)

    return stats


def main():
    parser = argparse.ArgumentParser(description="Worker 后处理：飞书回复 + Bitable 写入")
    parser.add_argument("--round-dir", required=True, help="轮次目录")
    parser.add_argument("--dry-run", action="store_true", help="只生成预览不发送")
    args = parser.parse_args()

    print(f"{'='*50}", file=sys.stderr)
    print(f"Phase 4.5: worker_post_process.py", file=sys.stderr)
    print(f"{'='*50}", file=sys.stderr)

    stats = post_process_round(args.round_dir, dry_run=args.dry_run)

    print(f"  总计: {stats['total']} 条", file=sys.stderr)
    print(f"  飞书回复: {stats.get('reply_sent', 0)} 条", file=sys.stderr)
    print(f"  Bitable: {stats.get('bitable_written', 0)} 条", file=sys.stderr)
    print(f"  Duplicate: {stats.get('dup_replied', 0)} 条", file=sys.stderr)
    print(f"  跳过: {stats.get('skipped', 0)} 条", file=sys.stderr)
    if stats.get("errors"):
        print(f"  错误: {len(stats['errors'])} 条", file=sys.stderr)
        for e in stats["errors"]:
            print(f"    {e}", file=sys.stderr)

    print(json.dumps(stats, ensure_ascii=False))


if __name__ == "__main__":
    main()

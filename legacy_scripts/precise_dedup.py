#!/usr/bin/env python3
"""
precise_dedup.py — Phase 1.7-B 精准去重

读取 cls-results.json + precheck-result.json + scan-result.json，
基于 CLS 堆栈计算精准 fingerprint，与 bitable 历史记录 + 同批次内去重。

用法:
  python3 precise_dedup.py \
    --cls-results /tmp/bugfix/1/cls-results.json \
    --precheck-result /tmp/bugfix/1/precheck-result.json \
    --scan-result /tmp/bugfix/1/scan-result.json \
    --output /tmp/bugfix/1/dedup-result.json \
    [--dry-run]
"""
import argparse
import json
import os
import re
import sys


def extract_precise_location(stack_trace_top3: str) -> str:
    """
    从 stack_trace_top3 提取第一个 com.mindverse 业务代码调用点的 File.java:line。
    返回空字符串表示无法提取。
    """
    if not stack_trace_top3:
        return ""

    # 匹配 com.mindverse 开头的栈帧中的文件名和行号
    pattern = r'at\s+com\.mindverse\.\S+\((\w+\.java):(\d+)\)'
    match = re.search(pattern, stack_trace_top3)
    if match:
        return f"{match.group(1)}:{match.group(2)}"

    # fallback: 匹配任意栈帧的文件名和行号
    pattern_any = r'at\s+\S+\((\w+\.java):(\d+)\)'
    match_any = re.search(pattern_any, stack_trace_top3)
    if match_any:
        return f"{match_any.group(1)}:{match_any.group(2)}"

    return ""


def compute_precise_fingerprint(coarse_fp: str, location: str) -> str:
    """计算精准 fingerprint: {coarse_fp}_{File.java:line}"""
    if location:
        return f"{coarse_fp}_{location}"
    return ""


def run_dedup(cls_results: dict, precheck_result: dict, scan_result: dict, dry_run: bool = False) -> dict:
    """
    执行精准去重。

    Returns:
        {
            "actionable_issues": [...],
            "duplicates": [...],
            "stats": { "total", "actionable", "duplicate_cross", "duplicate_intra", "no_trace" }
        }
    """
    new_issues = scan_result.get("new_issues", [])
    cls_data = cls_results.get("results", {})

    # 从 precheck_result 获取 bitable_refs（历史记录）
    bitable_refs = precheck_result.get("bitable_refs", {})

    # 收集所有精准完成记录的 fingerprint（跨轮次去重）
    completed_precise_fps = set()
    completed_refs_map = {}  # fp -> ref record
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

    actionable_issues = []
    duplicates = []
    stats = {
        "total": len(new_issues),
        "actionable": 0,
        "duplicate_cross": 0,  # 与 bitable 历史去重
        "duplicate_intra": 0,  # 同批次内去重
        "no_trace": 0,         # 无堆栈，无法精准去重
    }

    # 同批次内精准 fp 追踪
    seen_precise_fps = {}  # fp -> first issue_id

    for issue in new_issues:
        issue_id = issue.get("issue_id", "")
        coarse_fp = issue.get("fingerprint", "")

        # 从 CLS 结果获取堆栈
        cls_entry = cls_data.get(issue_id, {})
        stack_trace = cls_entry.get("stack_trace_top3", "")

        # 提取精准位置
        location = extract_precise_location(stack_trace)

        if not location:
            # 无法计算精准 fp，保留为 actionable（Worker 自行处理）
            stats["no_trace"] += 1
            issue["_dedup_note"] = "no_precise_fp"
            actionable_issues.append(issue)
            continue

        precise_fp = compute_precise_fingerprint(coarse_fp, location)

        # 1. 跨轮次去重：与 bitable 历史精准记录匹配
        if precise_fp in completed_precise_fps:
            matched_ref = completed_refs_map[precise_fp]
            dup_entry = {
                **issue,
                "precise_fingerprint": precise_fp,
                "dedup_reason": "cross_round",
                "matched_ref": matched_ref,
            }
            duplicates.append(dup_entry)
            stats["duplicate_cross"] += 1
            print(f"  去重(跨轮次): {issue_id} fp={precise_fp}", file=sys.stderr)
            continue

        # 2. 同批次内去重：相同精准 fp 只保留第一条
        if precise_fp in seen_precise_fps:
            first_issue_id = seen_precise_fps[precise_fp]
            dup_entry = {
                **issue,
                "precise_fingerprint": precise_fp,
                "dedup_reason": "intra_batch",
                "duplicate_of_issue": first_issue_id,
            }
            duplicates.append(dup_entry)
            stats["duplicate_intra"] += 1
            print(f"  去重(同批次): {issue_id} → {first_issue_id} fp={precise_fp}", file=sys.stderr)
            continue

        # 保留为 actionable
        seen_precise_fps[precise_fp] = issue_id
        issue["precise_fingerprint"] = precise_fp
        actionable_issues.append(issue)

    stats["actionable"] = len(actionable_issues)

    return {
        "actionable_issues": actionable_issues,
        "duplicates": duplicates,
        "stats": stats,
    }


def main():
    parser = argparse.ArgumentParser(description="Phase 1.7-B 精准去重")
    parser.add_argument("--cls-results", required=True, help="cls-results.json 路径")
    parser.add_argument("--precheck-result", required=True, help="precheck-result.json 路径")
    parser.add_argument("--scan-result", required=True, help="scan-result.json 路径")
    parser.add_argument("--output", required=True, help="输出文件路径")
    parser.add_argument("--dry-run", action="store_true", help="只预览不写文件")
    args = parser.parse_args()

    with open(args.cls_results, "r", encoding="utf-8") as f:
        cls_results = json.load(f)
    with open(args.precheck_result, "r", encoding="utf-8") as f:
        precheck_result = json.load(f)
    with open(args.scan_result, "r", encoding="utf-8") as f:
        scan_result = json.load(f)

    result = run_dedup(cls_results, precheck_result, scan_result, dry_run=args.dry_run)

    stats = result["stats"]
    print(
        f"精准去重完成: 总计 {stats['total']} 条, "
        f"可处理 {stats['actionable']} 条, "
        f"跨轮次去重 {stats['duplicate_cross']} 条, "
        f"同批次去重 {stats['duplicate_intra']} 条, "
        f"无堆栈 {stats['no_trace']} 条",
        file=sys.stderr,
    )

    if args.dry_run:
        print("[dry-run] 预览结果:")
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return

    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    # 输出 JSON 摘要到 stdout（供 Orchestrator 读取）
    print(json.dumps(stats, ensure_ascii=False))


if __name__ == "__main__":
    main()

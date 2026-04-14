#!/usr/bin/env python3
"""
self_upgrade.py — Phase 5 自升级（每轮结束后执行）

分析本轮 + 全历史 worker 结果，学习新的分类规则，输出 token 节省分析。
学到的规则持久化到 ~/.cache/bugfix/learned-rules.json。
注意：triage.py 读取的是 ~/.claude/skills/bug-fix/triage-rules.json（人工维护），
需人工审核后将 learned-rules 中的建议手动迁移到 triage-rules.json 才能生效。

用法:
  python3 self_upgrade.py --round-dir /tmp/bugfix/1
  python3 self_upgrade.py --scan-all                     # 扫描所有历史轮次重建规则
"""
import argparse
import glob
import json
import os
import sys
from datetime import datetime

from config import LEARNED_RULES_PATH
PROMOTE_THRESHOLD = 2         # 出现 ≥2 次且 0 成功 → 提升为过滤规则
TOKEN_PER_WORKER_ESTIMATE = 15000  # 每个 worker dispatch 估算 token 消耗


def load_learned_rules() -> dict:
    if os.path.exists(LEARNED_RULES_PATH):
        try:
            with open(LEARNED_RULES_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            pass
    return {
        "version": 1,
        "updated_at": None,
        "subcodes": {},
        "round_stats": [],
    }


def save_learned_rules(rules: dict):
    rules["updated_at"] = datetime.now().isoformat()
    os.makedirs(os.path.dirname(LEARNED_RULES_PATH), exist_ok=True)
    with open(LEARNED_RULES_PATH, "w", encoding="utf-8") as f:
        json.dump(rules, f, ensure_ascii=False, indent=2)


def classify_worker_error(status: str, error: str) -> str:
    """从 worker 结果推断应学习的分类。"""
    error_lower = error.lower()

    if status == "success":
        return "fixable"

    if "business_expected" in error_lower:
        return "business_expected"
    if "external" in error_lower or "transient" in error_lower:
        return "external_dependency"
    if error in ("no_diagnostic_info", "no_cls_logs_cannot_diagnose", "diagnosis_failed"):
        return "unfixable_no_info"
    if "no_logs_no_trace" in error_lower:
        return "unfixable_no_info"
    if error in ("not_fixable_code_bug",):
        return "not_fixable"
    if "no_subcode" in error_lower or "empty_subcode" in error_lower:
        return "no_subcode"
    if "hallucination" in error_lower:
        return "unfixable_hallucination"

    return "other_failure"


def learn_from_worker_results(worker_results: dict, round_id: str, rules: dict) -> dict:
    """
    从单轮 worker 结果学习。
    返回本轮统计。
    """
    subcodes = rules["subcodes"]
    round_stat = {
        "round": round_id,
        "total": 0,
        "success": 0,
        "skipped": 0,
        "failed": 0,
        "wasted_issues": 0,
    }

    for issue_id, wr in worker_results.items():
        status = wr.get("status", "")
        error = wr.get("error", "")
        subcode = wr.get("subcode", "")

        if not status:
            continue

        round_stat["total"] += 1
        if status == "success":
            round_stat["success"] += 1
        elif status == "skipped":
            round_stat["skipped"] += 1
            round_stat["wasted_issues"] += 1
        elif status == "failed":
            round_stat["failed"] += 1
            # 只有 error 不是代码相关的才算浪费
            cat = classify_worker_error(status, error)
            if cat != "other_failure":
                round_stat["wasted_issues"] += 1

        if not subcode:
            continue

        learned_cat = classify_worker_error(status, error)

        # 更新 subcodes 统计
        if subcode not in subcodes:
            subcodes[subcode] = {
                "total": 0,
                "success": 0,
                "categories": {},
                "last_round": "",
                "promoted_to": None,
            }

        entry = subcodes[subcode]
        entry["total"] += 1
        entry["last_round"] = round_id
        if learned_cat == "fixable":
            entry["success"] += 1
        entry["categories"][learned_cat] = entry["categories"].get(learned_cat, 0) + 1

    return round_stat


def promote_rules(rules: dict) -> list[dict]:
    """
    根据统计自动提升规则。
    选择出现次数最多的失败分类作为提升目标。
    有成功修复记录的 subcode 需要更高门槛。
    返回新提升的规则列表。
    """
    from triage import BUSINESS_EXPECTED_SUBCODES, EXTERNAL_OR_TRANSIENT_SUBCODES

    newly_promoted = []

    for subcode, entry in rules["subcodes"].items():
        if entry.get("promoted_to"):
            continue  # 已经提升过

        total = entry["total"]
        success = entry["success"]
        cats = entry["categories"]

        if total < PROMOTE_THRESHOLD:
            continue

        # 有成功记录的 subcode 需要更高门槛
        if success > 0:
            # 成功率 > 5%：不提升（说明有一定概率能修）
            if success / total > 0.05:
                continue
            # 成功率 ≤ 5% 但有成功：需要 ≥ 10 次数据才提升
            if total < 10:
                continue

        # 各分类计数
        be_count = cats.get("business_expected", 0) + cats.get("not_fixable", 0)
        ext_count = cats.get("external_dependency", 0)
        unfixable_count = cats.get("unfixable_no_info", 0) + cats.get("unfixable_hallucination", 0)

        # 选择出现次数最多的分类
        candidates = [
            ("business_expected", be_count),
            ("external_dependency", ext_count),
            ("unfixable", unfixable_count),
        ]
        candidates.sort(key=lambda x: x[1], reverse=True)
        best_cat, best_count = candidates[0]

        if best_count < PROMOTE_THRESHOLD:
            continue

        # 检查是否已在硬编码规则中
        if best_cat == "business_expected" and subcode in BUSINESS_EXPECTED_SUBCODES:
            continue
        if best_cat == "external_dependency" and subcode in EXTERNAL_OR_TRANSIENT_SUBCODES:
            continue

        label_map = {
            "business_expected": "业务预期",
            "external_dependency": "外部依赖",
            "unfixable": "无法诊断",
        }

        entry["promoted_to"] = best_cat
        newly_promoted.append({
            "subcode": subcode,
            "promoted_to": best_cat,
            "evidence": f"历史 {total} 次: {best_count} 次{label_map.get(best_cat, best_cat)}, {success} 次成功",
        })

    return newly_promoted


def compute_token_analysis(rules: dict) -> dict:
    """计算 token 节省分析。"""
    total_wasted = 0
    total_dispatched = 0
    could_save = 0

    for stat in rules.get("round_stats", []):
        total_dispatched += stat.get("total", 0)
        total_wasted += stat.get("wasted_issues", 0)

    promoted_subcodes = [
        sc for sc, info in rules["subcodes"].items()
        if info.get("promoted_to")
    ]

    could_save = total_wasted * TOKEN_PER_WORKER_ESTIMATE
    actual_useful = total_dispatched - total_wasted

    return {
        "total_worker_dispatches": total_dispatched,
        "total_wasted_dispatches": total_wasted,
        "total_successful_fixes": sum(s.get("success", 0) for s in rules.get("round_stats", [])),
        "waste_rate": f"{total_wasted / total_dispatched * 100:.1f}%" if total_dispatched else "0%",
        "estimated_wasted_tokens": could_save,
        "estimated_wasted_tokens_readable": f"~{could_save // 1000}K",
        "promoted_rules_count": len(promoted_subcodes),
        "promoted_subcodes": promoted_subcodes,
    }


def load_worker_results(round_dir: str) -> dict:
    """
    加载一个轮次的 worker 结果。
    优先读 worker-results.json，不存在时从 issues/*/fix-result.json + issue.json 构建。
    """
    worker_path = os.path.join(round_dir, "worker-results.json")
    if os.path.exists(worker_path):
        try:
            with open(worker_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            pass

    # Fallback: 从 issues/*/fix-result.json 构建
    results = {}
    issues_dir = os.path.join(round_dir, "issues")
    if not os.path.isdir(issues_dir):
        # 兼容旧目录结构 fix/*/
        issues_dir = os.path.join(round_dir, "fix")
    if not os.path.isdir(issues_dir):
        return results

    for issue_id in sorted(os.listdir(issues_dir)):
        issue_dir = os.path.join(issues_dir, issue_id)
        if not os.path.isdir(issue_dir):
            continue

        fix_path = os.path.join(issue_dir, "fix-result.json")
        issue_path = os.path.join(issue_dir, "issue.json")

        if not os.path.exists(fix_path):
            continue

        try:
            with open(fix_path, "r", encoding="utf-8") as f:
                fix_data = json.load(f)
        except (json.JSONDecodeError, IOError):
            continue

        # 读取 issue.json 补充字段
        issue_data = {}
        if os.path.exists(issue_path):
            try:
                with open(issue_path, "r", encoding="utf-8") as f:
                    issue_data = json.load(f)
            except (json.JSONDecodeError, IOError):
                pass

        fix_status = fix_data.get("fix_status", "")
        error = fix_data.get("error", "")
        # 映射 fix_status → worker-results status
        if fix_status == "success":
            status = "success"
        elif fix_status == "skipped":
            status = "skipped"
        else:
            status = "failed"

        results[issue_id] = {
            "status": status,
            "error": error,
            "subcode": issue_data.get("subcode", fix_data.get("subcode", "")),
            "message_id": issue_data.get("message_id", ""),
            "fingerprint": issue_data.get("fingerprint", ""),
        }

    return results


def scan_all_rounds() -> list[str]:
    """找到所有历史轮次目录（支持 worker-results.json 和 issues/*/fix-result.json 两种结构）。"""
    dirs = []
    for d in sorted(glob.glob("/tmp/bugfix/*/"), key=lambda x: _round_sort_key(x)):
        d_stripped = d.rstrip("/")
        # 支持两种结构：有 worker-results.json 或有 issues/ 目录
        worker_path = os.path.join(d_stripped, "worker-results.json")
        issues_dir = os.path.join(d_stripped, "issues")
        fix_dir = os.path.join(d_stripped, "fix")
        if os.path.exists(worker_path) or os.path.isdir(issues_dir) or os.path.isdir(fix_dir):
            dirs.append(d_stripped)
    return dirs


def _round_sort_key(path: str):
    """排序键：数字轮次优先，非数字在后。"""
    basename = os.path.basename(path.rstrip("/"))
    try:
        return (0, int(basename))
    except ValueError:
        return (1, basename)


def main():
    parser = argparse.ArgumentParser(description="自升级：分析 worker 结果，学习分类规则")
    parser.add_argument("--round-dir", help="当前轮次目录")
    parser.add_argument("--scan-all", action="store_true", help="扫描所有历史轮次重建规则")
    parser.add_argument("--output", help="输出文件路径（默认 {round_dir}/upgrade-result.json）")
    args = parser.parse_args()

    if not args.round_dir and not args.scan_all:
        print("错误: 需要 --round-dir 或 --scan-all", file=sys.stderr)
        sys.exit(1)

    print("═══ Phase 5: 自升级 ═══", file=sys.stderr)

    if args.scan_all:
        # 全量重建
        rules = load_learned_rules()
        rules["subcodes"] = {}
        rules["round_stats"] = []

        round_dirs = scan_all_rounds()
        print(f"  扫描 {len(round_dirs)} 个历史轮次...", file=sys.stderr)

        for rd in round_dirs:
            round_id = os.path.basename(rd)
            try:
                wr = load_worker_results(rd)
                if wr:
                    stat = learn_from_worker_results(wr, round_id, rules)
                    rules["round_stats"].append(stat)
                else:
                    print(f"  跳过 {round_id}: 无 worker 结果", file=sys.stderr)
            except Exception as e:
                print(f"  跳过 {round_id}: {e}", file=sys.stderr)

    else:
        rules = load_learned_rules()
        round_dir = args.round_dir
        round_id = os.path.basename(round_dir)

        # 检查是否已处理过这一轮
        processed_rounds = {s["round"] for s in rules.get("round_stats", [])}
        if round_id in processed_rounds:
            print(f"  第 {round_id} 轮已处理过，跳过学习", file=sys.stderr)
        else:
            try:
                wr = load_worker_results(round_dir)
                if wr:
                    stat = learn_from_worker_results(wr, round_id, rules)
                    rules["round_stats"].append(stat)
                    print(f"  本轮: {stat['total']} 个 worker, {stat['success']} 成功, "
                          f"{stat['wasted_issues']} 浪费", file=sys.stderr)
                else:
                    print("  本轮无 worker 结果", file=sys.stderr)
            except Exception as e:
                print(f"  加载 worker 结果失败: {e}", file=sys.stderr)

    # 提升规则
    newly_promoted = promote_rules(rules)
    if newly_promoted:
        print(f"  新提升 {len(newly_promoted)} 条规则:", file=sys.stderr)
        for p in newly_promoted:
            print(f"    + {p['subcode']} → {p['promoted_to']} ({p['evidence']})", file=sys.stderr)
    else:
        print("  无新规则需要提升", file=sys.stderr)

    # Token 分析
    analysis = compute_token_analysis(rules)
    print(f"  Token 分析:", file=sys.stderr)
    print(f"    总 worker 调度: {analysis['total_worker_dispatches']}", file=sys.stderr)
    print(f"    成功修复: {analysis['total_successful_fixes']}", file=sys.stderr)
    print(f"    浪费调度: {analysis['total_wasted_dispatches']} ({analysis['waste_rate']})", file=sys.stderr)
    print(f"    预估浪费 token: {analysis['estimated_wasted_tokens_readable']}", file=sys.stderr)
    print(f"    已提升规则数: {analysis['promoted_rules_count']}", file=sys.stderr)

    # 保存规则
    save_learned_rules(rules)
    print(f"  规则已保存到 {LEARNED_RULES_PATH}", file=sys.stderr)

    # 输出结果
    result = {
        "newly_promoted": newly_promoted,
        "token_analysis": analysis,
        "total_learned_subcodes": len(rules["subcodes"]),
        "total_promoted": sum(1 for v in rules["subcodes"].values() if v.get("promoted_to")),
    }

    output_path = args.output
    if not output_path and args.round_dir:
        output_path = os.path.join(args.round_dir, "upgrade-result.json")

    if output_path:
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2)

    print("═══ 自升级完成 ═══", file=sys.stderr)
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()

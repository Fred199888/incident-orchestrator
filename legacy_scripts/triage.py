#!/usr/bin/env python3
"""
triage.py — Phase 2b 分类（脚本化，零 AI token）

读取 precheck-result.json + cls-results.json，
按关键词规则对 new_issues 分类，输出 triage-result.json。

分类规则（按优先级）:
  1. attack: SQL注入/XSS/路径遍历
  2. infrastructure: OOM/连接池耗尽/Pod Evicted
  3. external_dependency: 堆栈不含 com.mindverse 或 feign/timeout
  4. transient: 单次出现 + 网络关键词
  5. real_bug: 堆栈含 com.mindverse
  6. unknown: 其他

用法:
  python3 triage.py \
    --scan-result /tmp/bugfix/1/precheck-result.json \
    --cls-results /tmp/bugfix/1/cls-results.json \
    --output /tmp/bugfix/1/triage-result.json
"""
import argparse
import json
import os
import sys

# ── subcode 规则（优先级最高，直接按 subcode 分类） ──

# 业务预期错误码：用户输入错误、权限校验、业务规则等
BUSINESS_EXPECTED_SUBCODES = {
    # 登录/认证
    "not.login", "token.expired", "token.invalid", "login.forbidden",
    # 权限/参数
    "permission.denied", "param.invalid", "param.not.be.null",
    # 用户输入
    "phone.invalid", "code.invalid", "email.invalid",
    # 频率限制
    "sms.send.limit", "rate.limit.exceeded",
    # 业务规则
    "call.template.deleted", "circle.dissolved", "resource.not.found",
    "bind.failed.yourself", "user.not.in.group", "route.user.all.black",
    "data.not.found", "route.format.invalid", "user.notFound",
    "merchant.invalid", "method.notAllowed", "operator.profile.empty",
    # 用户关系（屏蔽/拉黑）
    "blocked.by.me", "blocked.by.user",
    # 内容审核
    "avatar.violation", "image.is.not.compliant", "sensitive.rules",
}

# 外部依赖/瞬时错误码
EXTERNAL_OR_TRANSIENT_SUBCODES = {
    "Connection", "connection.refused", "connection.timeout",
    "voice.clone.failed", "validate.image.fail", "get.ogi.failed",
    "image.convert.video.failed", "record.exposure.fail",
}

# 外部服务超时（模式匹配）
EXTERNAL_SUBCODE_PATTERNS = [
    "openai.", "APITimeoutError", "timeout", "Timeout",
]

# ── 不可修复（已废弃，全部归入 business_expected） ──
UNFIXABLE_SUBCODES: set[str] = set()  # 保留兼容，加载时转入 BUSINESS_EXPECTED_SUBCODES

# ── 加载人工维护规则（triage-rules.json，替代自动学习的 learned-rules.json） ──
_SKILL_RULES_PATH = os.path.expanduser("~/.claude/skills/bug-fix/triage-rules.json")
if os.path.exists(_SKILL_RULES_PATH):
    try:
        with open(_SKILL_RULES_PATH, "r", encoding="utf-8") as _f:
            _skill_rules = json.load(_f)
        _loaded = 0
        for _rule in _skill_rules.get("rules", []):
            _sc = _rule.get("subcode", "")
            _cat = _rule.get("category", "")
            if not _sc or not _cat:
                continue
            if _cat == "business_expected" and _sc not in BUSINESS_EXPECTED_SUBCODES:
                BUSINESS_EXPECTED_SUBCODES.add(_sc)
                _loaded += 1
            elif _cat == "external_dependency" and _sc not in EXTERNAL_OR_TRANSIENT_SUBCODES:
                EXTERNAL_OR_TRANSIENT_SUBCODES.add(_sc)
                _loaded += 1
            elif _cat == "unfixable":
                # 不可修复已废弃，统一归入 business_expected
                BUSINESS_EXPECTED_SUBCODES.add(_sc)
                _loaded += 1
        if _loaded:
            print(f"  规则加载: 从 triage-rules.json 加载 {_loaded} 条人工维护规则", file=sys.stderr)
    except (json.JSONDecodeError, IOError) as _e:
        print(f"  规则加载: 读取 triage-rules.json 失败: {_e}", file=sys.stderr)

# ── CLS 日志内容关键词 ──
ATTACK_KEYWORDS = [
    "sql injection", "' or 1=1", "xss", "<script>", "../", "path traversal",
]
INFRASTRUCTURE_KEYWORDS = [
    "outofmemory", "oom", "connection pool", "pod evicted",
]
EXTERNAL_DEP_KEYWORDS = [
    "feign", "timeout", "connection refused",
]
TRANSIENT_KEYWORDS = [
    "dns", "network", "econnreset", "socket",
]


def classify_issue(
    issue: dict,
    cls_data: dict,
    duplicate_count: int,
) -> tuple[str, str]:
    """
    对单个 issue 分类。
    返回: (category, evidence)
    """
    subcode = issue.get("subcode", "")

    # 0a. subcode 直接匹配 — 业务预期
    if subcode in BUSINESS_EXPECTED_SUBCODES:
        return "business_expected", f"subcode={subcode}"

    # 0b. subcode 直接匹配 — 外部依赖/瞬时
    if subcode in EXTERNAL_OR_TRANSIENT_SUBCODES:
        return "external_dependency", f"subcode={subcode}"

    # 0c. subcode 模式匹配 — 外部服务
    if subcode and any(p in subcode for p in EXTERNAL_SUBCODE_PATTERNS):
        return "external_dependency", f"subcode 含外部服务关键词: {subcode}"

    # 0d. 已废弃 — 原 unfixable 规则已全部归入 business_expected（在 0a 处理）

    # 0e. 空 subcode — 无法诊断
    if not subcode:
        return "unknown_no_subcode", "告警无 subcode，格式解析可能失败"

    cls_summary = cls_data.get("cls_summary", "")
    stack_top3 = cls_data.get("stack_trace_top3", "")
    combined = f"{cls_summary} {stack_top3}".lower()

    # 1. attack
    if any(kw in combined for kw in ATTACK_KEYWORDS):
        return "attack", "请求参数包含注入/XSS/遍历特征"

    # 2. infrastructure
    if any(kw in combined for kw in INFRASTRUCTURE_KEYWORDS):
        return "infrastructure", f"基础设施问题: {cls_summary[:100]}"

    # 3. external_dependency
    if ("com.mindverse" not in combined and stack_top3) or \
       any(kw in combined for kw in EXTERNAL_DEP_KEYWORDS):
        if stack_top3 and "com.mindverse" not in stack_top3.lower():
            return "external_dependency", "堆栈不含 com.mindverse 业务代码"
        if any(kw in combined for kw in EXTERNAL_DEP_KEYWORDS):
            return "external_dependency", "第三方服务调用异常"

    # 4. transient（无重复消息 + 网络关键词 → 偶发）
    if any(kw in combined for kw in TRANSIENT_KEYWORDS) and duplicate_count == 0:
        return "transient", "偶发网络问题"

    # 5. real_bug（堆栈含业务代码）
    if "com.mindverse" in combined:
        return "real_bug", ""

    # 6. unknown（无堆栈或堆栈不含业务代码）
    return "unknown", ""


def main():
    parser = argparse.ArgumentParser(description="问题分类")
    parser.add_argument("--scan-result", required=True, help="precheck-result.json 路径")
    parser.add_argument("--cls-results", required=True, help="cls-results.json 路径")
    parser.add_argument("--output", required=True, help="输出 triage-result.json 路径")
    args = parser.parse_args()

    with open(args.scan_result, "r", encoding="utf-8") as f:
        scan_result = json.load(f)
    with open(args.cls_results, "r", encoding="utf-8") as f:
        cls_results = json.load(f)

    new_issues = scan_result.get("new_issues", [])
    results_map = cls_results.get("results", {})
    duplicate_count = len(scan_result.get("duplicate_msgs", []))

    fix_issues = []
    non_code_issues = []

    for issue in new_issues:
        issue_id = issue.get("issue_id", "")
        cls_data = results_map.get(issue_id, {})

        category, evidence = classify_issue(issue, cls_data, duplicate_count)

        if category in ("real_bug", "unknown"):
            fix_issues.append({
                **issue,
                "category": category,
                "cls_summary": cls_data.get("cls_summary", ""),
                "stack_trace_top3": cls_data.get("stack_trace_top3", ""),
            })
        else:
            non_code_issues.append({
                "issue_id": issue_id,
                "message_id": issue.get("message_id", ""),
                "fingerprint": issue.get("fingerprint", ""),
                "subcode": issue.get("subcode", ""),
                "service": issue.get("service", ""),
                "api_path": issue.get("api_path", ""),
                "trace_id": issue.get("trace_id", ""),
                "category": category,
                "evidence": evidence,
                "cls_summary": cls_data.get("cls_summary", ""),
                "stack_trace_top3": cls_data.get("stack_trace_top3", ""),
            })

        print(f"  {issue_id}: {category}" + (f" ({evidence})" if evidence else ""), file=sys.stderr)

    # 输出 triage-result.json（与 SKILL.md 定义格式一致）
    triage_result = {
        "fix_issues": fix_issues,
        "non_code_issues": non_code_issues,
        "replies_sent": {},
    }

    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(triage_result, f, ensure_ascii=False, indent=2)

    print(f"分类完成: {len(fix_issues)} 需修复, {len(non_code_issues)} 非代码", file=sys.stderr)
    print(json.dumps({
        "fix_count": len(fix_issues),
        "non_code_count": len(non_code_issues),
    }, ensure_ascii=False))


if __name__ == "__main__":
    main()

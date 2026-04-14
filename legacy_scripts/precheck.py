#!/usr/bin/env python3
"""
precheck.py — Phase 1.5 历史参考匹配

输入：
  --scan-result    scan-result.json 路径
  --bitable-json   bitable 全量记录 JSON 文件路径（由 Orchestrator 通过 MCP 查询并存为文件）
  --output         precheck-result.json 输出路径

逻辑：
  1. 读取 bitable 记录，区分"精准 fingerprint"和"粗 fingerprint"
  2. 按 coarse prefix 聚合所有记录作为参考，标记 is_precise 字段
  3. 匹配 scan-result 的 new_issues，输出 precheck-result.json
  4. 不做去重：所有 issue 全部进入 Phase 3，去重推迟到 Worker CLS 查询后精准执行
"""
import argparse
import json
import re
import sys

# 精准 fingerprint 后缀正则：_FileName.ext:lineNumber
# 文件名部分不含下划线（Java 类名用 camelCase），避免贪婪匹配吃掉 fingerprint 段
PRECISE_SUFFIX_RE = re.compile(r'_([A-Za-z][A-Za-z0-9]*\.[a-zA-Z]+:\d+)$')


def extract_text(value) -> str:
    """从 bitable 富文本字段提取纯文本。支持 str / list[{text:...}] / 其他类型。"""
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        return "".join(item.get("text", "") if isinstance(item, dict) else str(item) for item in value)
    return str(value) if value else ""


def is_precise_fingerprint(fp: str) -> bool:
    """判断 fingerprint 是否为精准格式（末尾含 _File.ext:lineNumber）"""
    return bool(PRECISE_SUFFIX_RE.search(fp))


def get_coarse_prefix(fp: str) -> str:
    """从精准 fingerprint 中截取粗 fingerprint 前缀（去掉末尾 _File.ext:lineNumber）"""
    m = PRECISE_SUFFIX_RE.search(fp)
    if m:
        return fp[:m.start()]
    return fp


def build_bitable_index(bitable_records: list[dict]) -> dict:
    """
    构建 bitable 参考索引。

    返回:
      ref_index: dict[coarse_prefix -> list[bitable_record]]
        所有记录按 coarse prefix 聚合，每条记录标记 is_precise 字段
    """
    ref_index: dict[str, list[dict]] = {}

    for record in bitable_records:
        fields = record.get("fields", {})
        fp = extract_text(fields.get("issue_fingerprint", ""))
        status = extract_text(fields.get("状态", ""))

        if not fp or fp == "-":
            continue

        precise = is_precise_fingerprint(fp)
        coarse = get_coarse_prefix(fp) if precise else fp

        if coarse not in ref_index:
            ref_index[coarse] = []
        ref_index[coarse].append({
            "record_id": record.get("record_id", ""),
            "fingerprint": fp,
            "is_precise": precise,
            "任务名称": extract_text(fields.get("任务名称", "")),
            "状态": status,
            "分支": extract_text(fields.get("分支", "")),
            "root_cause_location": extract_text(fields.get("root_cause_location", "")),
            "PR": extract_text(fields.get("PR", "")),
            "error_type": extract_text(fields.get("error_type", "")),
            "error_location": extract_text(fields.get("error_location", "")),
        })

    return ref_index


def match_issues(new_issues: list[dict], ref_index: dict) -> tuple[list[dict], dict]:
    """
    匹配 new_issues 与 bitable 参考索引。

    返回:
      all_issues: 全部 issue（不再去重，全部进入 Phase 3）
      refs: 参考信息字典（按 issue_id 索引）
    """
    refs: dict[str, list[dict]] = {}

    for issue in new_issues:
        fp = issue.get("fingerprint", "")

        # 检查参考匹配（coarse fp 命中 ref_index）
        issue_refs = ref_index.get(fp, [])

        if issue_refs:
            refs[issue.get("issue_id", "")] = issue_refs

    return new_issues, refs


def main():
    parser = argparse.ArgumentParser(description="Phase 1.5 历史参考匹配")
    parser.add_argument("--scan-result", required=True, help="scan-result.json 路径")
    parser.add_argument("--bitable-json", required=True, help="bitable 全量记录 JSON 文件路径")
    parser.add_argument("--output", required=True, help="precheck-result.json 输出路径")
    args = parser.parse_args()

    # 读取 scan-result.json
    with open(args.scan_result, "r", encoding="utf-8") as f:
        scan_result = json.load(f)

    new_issues = scan_result.get("new_issues", [])

    # 读取 bitable 记录
    try:
        with open(args.bitable_json, "r", encoding="utf-8") as f:
            bitable_data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"⚠️ bitable 记录读取失败: {e}，跳过参考匹配", file=sys.stderr)
        bitable_data = []

    # bitable_data 可能是 list 或 dict（MCP 返回的 items）
    if isinstance(bitable_data, dict):
        bitable_records = bitable_data.get("items", [])
    elif isinstance(bitable_data, list):
        bitable_records = bitable_data
    else:
        bitable_records = []

    print(f"读取 {len(new_issues)} 条 new_issues，{len(bitable_records)} 条 bitable 记录", file=sys.stderr)

    # 构建索引
    ref_index = build_bitable_index(bitable_records)
    print(f"参考索引: {len(ref_index)} 条", file=sys.stderr)

    # 匹配
    all_issues, refs = match_issues(new_issues, ref_index)

    print(f"结果: 参考 {len(refs)} 条，总 issue {len(all_issues)} 条", file=sys.stderr)

    # 输出 precheck-result.json
    result = {
        "new_issues": all_issues,
        "bitable_refs": refs,
        "stats": {
            "total_scan_issues": len(new_issues),
            "ref_count": len(refs),
            "remaining_count": len(all_issues),
        },
    }

    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    print(f"输出: {args.output}", file=sys.stderr)

    # stdout 摘要供 Orchestrator 读取
    summary = {
        "ref_count": len(refs),
        "remaining_count": len(all_issues),
    }
    print(json.dumps(summary, ensure_ascii=False))


if __name__ == "__main__":
    main()

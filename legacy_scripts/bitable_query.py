#!/usr/bin/env python3
"""
bitable_query.py — Phase 1.5 多维表格历史记录查询

替代 MCP bitable_v1_appTableRecord_search，直接调飞书 REST API。
查询全部记录（不传 filter），分页获取，输出 bitable-records.json。

用法:
  python3 bitable_query.py --output /tmp/bugfix/1/bitable-records.json
"""
import argparse
import json
import os
import sys
import time

import requests

from config import (
    BITABLE_APP_TOKEN,
    BITABLE_TABLE_ID,
    LARK_APP_ID,
    LARK_APP_SECRET,
    LARK_BASE_URL,
)

FIELD_NAMES = [
    "issue_fingerprint",
    "任务名称",
    "状态",
    "分支",
    "root_cause_location",
    "PR",
    "error_type",
    "error_location",
    "stack_trace",
]


def get_tenant_token() -> str:
    """获取 tenant_access_token"""
    resp = requests.post(
        f"{LARK_BASE_URL}/open-apis/auth/v3/tenant_access_token/internal",
        json={"app_id": LARK_APP_ID, "app_secret": LARK_APP_SECRET},
        timeout=10,
    )
    resp.raise_for_status()
    data = resp.json()
    if data.get("code") != 0:
        raise RuntimeError(f"获取 tenant_access_token 失败: {data}")
    return data["tenant_access_token"]


def search_all_records(app_token: str, table_id: str) -> list[dict]:
    """分页查询 bitable 全部记录"""
    token = get_tenant_token()
    print(f"使用 tenant_access_token 查询 bitable", file=sys.stderr)

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json; charset=utf-8",
    }

    all_items = []
    page_token = None
    page = 0
    total_expected = None  # 飞书返回的 total 字段

    while True:
        page += 1
        body: dict = {
            "page_size": 500,
            "field_names": FIELD_NAMES,
        }
        if page_token:
            body["page_token"] = page_token

        resp = requests.post(
            f"{LARK_BASE_URL}/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/search",
            headers=headers,
            json=body,
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()

        if data.get("code") != 0:
            print(f"  bitable 查询失败: code={data.get('code')} msg={data.get('msg')}", file=sys.stderr)
            if data.get("code") in (403, 1062001):
                print(f"  权限不足，返回已获取的 {len(all_items)} 条记录", file=sys.stderr)
                break
            raise RuntimeError(f"bitable 查询失败: {data}")

        items = data.get("data", {}).get("items", [])
        all_items.extend(items)

        # 第一页获取 total
        if total_expected is None:
            total_expected = data.get("data", {}).get("total", 0)
            print(f"  bitable 总记录数: {total_expected}", file=sys.stderr)

        print(f"  第 {page} 页: {len(items)} 条，累计 {len(all_items)} 条", file=sys.stderr)

        # 停止条件：已拿够 total 条 / 空页 / has_more=false / 无 page_token
        if len(all_items) >= total_expected:
            break
        if not items:
            break

        has_more = data.get("data", {}).get("has_more", False)
        page_token = data.get("data", {}).get("page_token")

        if not has_more or not page_token:
            break

        time.sleep(0.2)

    # 飞书 search API 分页可能返回重复记录，按 record_id 去重
    seen_ids = set()
    deduped = []
    for item in all_items:
        rid = item.get("record_id", "")
        if rid and rid not in seen_ids:
            seen_ids.add(rid)
            deduped.append(item)
    if len(deduped) < len(all_items):
        print(f"  去重: {len(all_items)} → {len(deduped)} 条（去除 {len(all_items)-len(deduped)} 条重复）", file=sys.stderr)

    return deduped


def filter_by_fingerprints(items: list[dict], fingerprints: set[str]) -> list[dict]:
    """按 fingerprint 前缀过滤 bitable 记录（支持精准 fp 的 coarse 前缀匹配）"""
    import re
    PRECISE_SUFFIX_RE = re.compile(r'_([A-Za-z][A-Za-z0-9]*\.[a-zA-Z]+:\d+)$')

    filtered = []
    for item in items:
        fields = item.get("fields", {})
        fp = fields.get("issue_fingerprint", "")
        if isinstance(fp, list):
            fp = "".join(x.get("text", "") if isinstance(x, dict) else str(x) for x in fp)
        elif not isinstance(fp, str):
            fp = str(fp) if fp else ""

        if not fp or fp == "-":
            continue

        # 提取 coarse 前缀
        m = PRECISE_SUFFIX_RE.search(fp)
        coarse = fp[:m.start()] if m else fp

        if coarse in fingerprints or fp in fingerprints:
            filtered.append(item)

    return filtered


def main():
    parser = argparse.ArgumentParser(description="Phase 1.5 bitable 历史记录查询")
    parser.add_argument("--output", required=True, help="输出文件路径")
    parser.add_argument("--app-token", default=BITABLE_APP_TOKEN, help="bitable app_token")
    parser.add_argument("--table-id", default=BITABLE_TABLE_ID, help="bitable table_id")
    parser.add_argument("--fingerprints", default=None,
                        help="逗号分隔的 fingerprint 列表（只返回匹配的记录）")
    args = parser.parse_args()

    print(f"查询 bitable: app_token={args.app_token}, table_id={args.table_id}", file=sys.stderr)

    items = search_all_records(args.app_token, args.table_id)

    # 按 fingerprint 过滤（可选）
    if args.fingerprints:
        fp_set = set(fp.strip() for fp in args.fingerprints.split(",") if fp.strip())
        before = len(items)
        items = filter_by_fingerprints(items, fp_set)
        print(f"  fingerprint 过滤: {before} → {len(items)} 条", file=sys.stderr)

    # 输出格式与 MCP 返回兼容
    output = {"items": items, "total": len(items)}

    os.makedirs(os.path.dirname(args.output) or ".", exist_ok=True)
    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"bitable 查询完成: {len(items)} 条记录", file=sys.stderr)
    print(json.dumps({"total": len(items)}))


if __name__ == "__main__":
    main()

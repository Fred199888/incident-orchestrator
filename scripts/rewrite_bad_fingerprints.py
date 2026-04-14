"""重算 bitable 中"仅全类名"历史 fp 记录的 issue_fingerprint

背景：fingerprint.py 原本有三档，中间档"仅全类名"已移除（见
incident_orchestrator/services/fingerprint.py）。本脚本把 bitable 里那些
旧档产物（形如 `service.java.lang.Xxx` 无 `:行号` 无 `_`）就地重算成
新规则下的 fp：

  新规则两档：
    1. 全类名:行号（com.x.y.Foo:123 / Foo.java:123）
    2. 前两行兜底（去参数化的 key）

流程：
  1. 拉全表 → 筛出不合理 fp（和 cleanup_bad_fingerprints.py 一样的判定）
  2. 对每条按 message_id 调飞书 get_message 拿原始告警卡片 content
  3. 复用 scheduled_scan._extract_all_text 抽出全文本 → 提取 `content:` 段
  4. 用 fingerprint.extract_fingerprint 重算
  5. 对比旧/新 fp，dry-run 打印；--apply 则写回 bitable

用法：
  python scripts/rewrite_bad_fingerprints.py           # dry-run
  python scripts/rewrite_bad_fingerprints.py --apply
"""
import argparse
import asyncio
import json
import os
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
os.chdir(ROOT)

from incident_orchestrator.config import get_settings  # noqa: E402
from incident_orchestrator.feishu.client import get_feishu_client  # noqa: E402
from incident_orchestrator.services.fingerprint import extract_fingerprint  # noqa: E402
from incident_orchestrator.services.scheduled_scan import _extract_all_text  # noqa: E402
from scripts.cleanup_bad_fingerprints import (  # noqa: E402
    is_bad_fp,
    list_all_records,
    _extract_text,
)


async def fetch_message_content(message_id: str) -> str:
    """通过 get_message 拿到告警卡片的全文本"""
    feishu = get_feishu_client()
    result = await feishu.get_message(message_id)
    if result.get("code") != 0:
        return ""
    items = result.get("data", {}).get("items", [])
    if not items:
        return ""
    body = items[0].get("body", {})
    content_str = body.get("content", "")
    if not content_str:
        return ""
    try:
        content = json.loads(content_str)
    except json.JSONDecodeError:
        return ""
    return "".join(_extract_all_text(content))


def extract_error_block(full_text: str) -> str:
    """从告警全文本里切出 content: 段（和 _parse_alert_from_message 一致）"""
    m = re.search(r"content[：:]\s*(.+?)(?=\n前往|\Z)", full_text, re.DOTALL)
    return m.group(1).strip()[:500] if m else ""


async def update_fingerprint(record_id: str, new_fp: str) -> tuple[bool, str]:
    """更新单条记录的 issue_fingerprint 字段"""
    settings = get_settings()
    feishu = get_feishu_client()
    http = await feishu._ensure_http()
    headers = await feishu._headers()

    resp = await http.put(
        f"{feishu.base_url}/open-apis/bitable/v1/apps/{settings.bitable_app_token}"
        f"/tables/{settings.bitable_table_id}/records/{record_id}",
        headers=headers,
        json={"fields": {"issue_fingerprint": new_fp}},
    )
    data = resp.json()
    if data.get("code") != 0:
        return False, data.get("msg", "unknown")
    return True, ""


async def main():
    parser = argparse.ArgumentParser(description="重算历史 fp 记录")
    parser.add_argument("--apply", action="store_true", help="真的写回 bitable")
    args = parser.parse_args()

    print("拉取 bitable 全表…")
    records = await list_all_records()
    print(f"共 {len(records)} 条，筛选 fp 不合理的…")

    # list_all_records 返回的是 {record_id, fp, status, task, branch, count}
    # 我们还需要 message_id → 另外拉一次带 message_id 字段
    # 简化：直接再调一次 feishu API
    settings = get_settings()
    feishu = get_feishu_client()
    http = await feishu._ensure_http()
    headers = await feishu._headers()

    # 获取完整记录（含 message_id 和 服务名 字段）
    full_items: list[dict] = []
    page_token = None
    while True:
        body = {"page_size": 500}
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
            raise RuntimeError(data.get("msg"))
        payload = data.get("data", {})
        full_items.extend(payload.get("items", []))
        if not payload.get("has_more"):
            break
        page_token = payload.get("page_token")
        if not page_token:
            break

    targets = []
    for record in full_items:
        f = record.get("fields", {})
        fp = _extract_text(f.get("issue_fingerprint"))
        if not is_bad_fp(fp):
            continue
        targets.append({
            "record_id": record.get("record_id"),
            "old_fp": fp,
            "service": _extract_text(f.get("服务名")),
            "message_id": _extract_text(f.get("message_id")),
        })

    print(f"发现 {len(targets)} 条待重算\n")

    resolved: list[tuple[dict, str]] = []  # (target, new_fp)
    unresolved: list[tuple[dict, str]] = []  # (target, reason)

    for t in targets:
        print(f"  [{t['old_fp']}]")
        if not t["message_id"]:
            unresolved.append((t, "缺 message_id"))
            print("    ✗ 缺 message_id")
            continue

        full_text = await fetch_message_content(t["message_id"])
        if not full_text:
            unresolved.append((t, "get_message 失败"))
            print("    ✗ get_message 失败")
            continue

        error_block = extract_error_block(full_text)
        if not error_block:
            unresolved.append((t, "未解析到 content: 段"))
            print("    ✗ 未解析到 content: 段")
            continue

        new_fp = extract_fingerprint(t["service"], error_block)
        if is_bad_fp(new_fp):
            # 新规则跑出来仍然不合理（罕见）
            unresolved.append((t, f"重算仍不合理: {new_fp}"))
            print(f"    ✗ 重算仍不合理: {new_fp}")
            continue

        print(f"    → {new_fp}")
        resolved.append((t, new_fp))

    print(f"\n汇总: 可重算 {len(resolved)} 条 / 无法重算 {len(unresolved)} 条")

    if unresolved:
        print("\n无法重算的记录：")
        for t, reason in unresolved:
            print(f"  - {t['old_fp']}  [{reason}]")

    if not resolved:
        return

    if not args.apply:
        print(f"\n[dry-run] 加 --apply 真的更新这 {len(resolved)} 条 fp")
        return

    print(f"\n开始更新 {len(resolved)} 条…")
    ok = 0
    for t, new_fp in resolved:
        success, err = await update_fingerprint(t["record_id"], new_fp)
        if success:
            ok += 1
            print(f"  ✓ {t['old_fp']} → {new_fp}")
        else:
            print(f"  ✗ {t['old_fp']}: {err}")
    print(f"\n更新完成: {ok}/{len(resolved)}")


if __name__ == "__main__":
    asyncio.run(main())

"""清理 bitable 中不合理的 issue_fingerprint 记录

历史上 fingerprint.py 曾有三档提取规则：
  1. 全类名:行号    — e.g. com.mindverse.xxx.Foo:414
  2. 仅全类名       — e.g. java.lang.NullPointerException   ← 已移除
  3. content 前两行 — e.g. generateMemoryLoop_error_chatId_:_mes

档 2 会把不同行抛出的同类异常合并成一个 fp，导致过度去重，已在
incident_orchestrator/services/fingerprint.py 移除。本脚本用来把历史
遗留的"仅全类名" fp 记录从 bitable 里清理掉。

判定规则（满足即视为不合理）：
  - fp 不以 ":<数字>" 结尾（没行号）
  - fp 不含 "_"（不是前两行兜底）
  - fp 点段数 >= 3（避免误伤非常短的服务名.key）

用法：
  python scripts/cleanup_bad_fingerprints.py           # dry-run
  python scripts/cleanup_bad_fingerprints.py --apply   # 真删
"""
import argparse
import asyncio
import os
import re
import sys
from pathlib import Path

# 让脚本能以工程根目录为 CWD 运行
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from incident_orchestrator.config import get_settings  # noqa: E402
from incident_orchestrator.feishu.client import get_feishu_client  # noqa: E402


BAD_FP_RE_LINE_END = re.compile(r":\d+$")


def is_bad_fp(fp: str) -> bool:
    if not fp:
        return False
    if BAD_FP_RE_LINE_END.search(fp):
        return False  # 带行号，合法
    if "_" in fp:
        return False  # 前两行兜底，合法
    # 剩下的是"仅全类名"形态：service.com.xx.Yy 或 service.java.lang.Zz
    return fp.count(".") >= 2


def _extract_text(val):
    """bitable 字段格式化：Text 字段返回 [{text: ...}] 或 str"""
    if isinstance(val, list) and val:
        first = val[0]
        if isinstance(first, dict):
            return first.get("text", "") or first.get("link", "")
        return str(first)
    if isinstance(val, dict):
        return val.get("text", "") or val.get("link", "")
    return str(val) if val else ""


async def list_all_records() -> list[dict]:
    """分页拉取 bitable 全部记录，返回 [{record_id, fp, status, task_name}, ...]"""
    settings = get_settings()
    feishu = get_feishu_client()
    http = await feishu._ensure_http()
    headers = await feishu._headers()

    items: list[dict] = []
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
            raise RuntimeError(f"查询失败: {data.get('msg')}")

        payload = data.get("data", {})
        for record in payload.get("items", []):
            fields = record.get("fields", {})
            items.append({
                "record_id": record.get("record_id", ""),
                "fp": _extract_text(fields.get("issue_fingerprint")),
                "status": _extract_text(fields.get("状态")),
                "task": _extract_text(fields.get("任务名称")),
                "branch": _extract_text(fields.get("分支")),
                "count": fields.get("告警次数", 0),
            })

        if not payload.get("has_more"):
            break
        page_token = payload.get("page_token")
        if not page_token:
            break

    return items


async def delete_records(record_ids: list[str]) -> tuple[int, int]:
    """批量删除，返回 (成功, 失败)"""
    settings = get_settings()
    feishu = get_feishu_client()
    http = await feishu._ensure_http()
    headers = await feishu._headers()

    ok = 0
    fail = 0
    # 飞书 batch_delete 一次最多 500 条
    for i in range(0, len(record_ids), 500):
        batch = record_ids[i:i + 500]
        resp = await http.post(
            f"{feishu.base_url}/open-apis/bitable/v1/apps/{settings.bitable_app_token}"
            f"/tables/{settings.bitable_table_id}/records/batch_delete",
            headers=headers,
            json={"records": batch},
        )
        data = resp.json()
        if data.get("code") != 0:
            print(f"批量删除失败: {data.get('msg')}", file=sys.stderr)
            fail += len(batch)
        else:
            ok += len(batch)
    return ok, fail


async def main():
    parser = argparse.ArgumentParser(description="清理 bitable 中不合理的 fingerprint 记录")
    parser.add_argument("--apply", action="store_true", help="真的删除（默认只 dry-run）")
    args = parser.parse_args()

    print("拉取 bitable 全表…")
    records = await list_all_records()
    print(f"共 {len(records)} 条记录")

    bad = [r for r in records if is_bad_fp(r["fp"])]
    print(f"\n发现 {len(bad)} 条不合理 fingerprint：\n")
    for r in bad:
        print(
            f"  [{r['status'] or '?':<12}] "
            f"fp={r['fp']}  "
            f"count={r['count']}  "
            f"branch={r['branch'] or '-'}  "
            f"task={r['task'][:40]}"
        )

    if not bad:
        print("无需清理。")
        return

    if not args.apply:
        print(f"\n[dry-run] 加 --apply 真的删除这 {len(bad)} 条记录")
        return

    print(f"\n开始删除 {len(bad)} 条…")
    ok, fail = await delete_records([r["record_id"] for r in bad])
    print(f"删除完成: 成功 {ok}，失败 {fail}")


if __name__ == "__main__":
    # 让 .env 生效（config.get_settings 会读 .env，但需保证 CWD 正确）
    os.chdir(ROOT)
    asyncio.run(main())

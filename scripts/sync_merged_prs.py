"""同步 bitable 中"等待合并"记录的 PR 合并状态（本地 git 版）

场景：bot 给每个告警推了一个 fix 分支，状态写 `⏳等待合并`。人类合并后，
bitable 还停在"等待合并"，下一轮自动扫描就会继续在原话题追加"累计告警次数"
打扰提醒。本脚本定期跑一次，把实际已合并的记录标记掉，scheduled_scan
命中去重且状态在 TERMINAL_STATUSES 里时会静默跳过（见
incident_orchestrator/services/scheduled_scan.py）。

检测策略：**走本地 monorepo 的 git**，不依赖 GitHub API（避开 PAT 权限问题）。
  1. `git fetch origin release/stable <branch>` 把两端刷新到最新
  2. `git cherry origin/release/stable origin/<branch>`
     - 所有行以 `-` 开头  → 已合入（patch-id 等价，能识别 squash merge）
     - 任一行以 `+` 开头  → 有未合入 commit

优势：能正确识别 squash/rebase merge；不需要 PAT；只需本地 monorepo 存在。

标记方式：把记录的 `状态` 字段改为 `✅已合并`，告警次数原样保留。

⚠️ 前置条件：飞书 bitable 的"状态"字段（SingleSelect）必须预先添加
   `✅已合并` 这个选项，否则更新会失败。apply 前请先人工在表头加好。

用法：
  python scripts/sync_merged_prs.py            # dry-run
  python scripts/sync_merged_prs.py --apply    # 真的写回 bitable
"""
import argparse
import asyncio
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from incident_orchestrator.config import get_settings  # noqa: E402
from incident_orchestrator.feishu.client import get_feishu_client  # noqa: E402
from incident_orchestrator.services.bitable_service import (  # noqa: E402
    STATUS_PENDING_MERGE,
    STATUS_MERGED,
    mark_as_merged,
)
from incident_orchestrator.services.git_merge_check import (  # noqa: E402
    fetch_branches,
    check_branch_merged,
)


def _extract_text(val):
    if isinstance(val, list) and val:
        first = val[0]
        if isinstance(first, dict):
            return first.get("text", "") or first.get("link", "")
        return str(first)
    if isinstance(val, dict):
        return val.get("text", "") or val.get("link", "")
    return str(val) if val else ""


# git 合并判断逻辑已抽到 incident_orchestrator.services.git_merge_check，
# 本脚本只做批量编排（拉记录 + 批量 fetch + 批量判定 + 批量写回）。


async def list_pending_merge_records() -> list[dict]:
    """拉取状态 == ⏳等待合并 的所有记录"""
    settings = get_settings()
    feishu = get_feishu_client()
    http = await feishu._ensure_http()
    headers = await feishu._headers()

    items: list[dict] = []
    page_token = None
    while True:
        body = {
            "filter": {
                "conjunction": "and",
                "conditions": [
                    {
                        "field_name": "状态",
                        "operator": "is",
                        "value": [STATUS_PENDING_MERGE],
                    }
                ],
            },
            "page_size": 500,
        }
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
                "branch": _extract_text(fields.get("分支")),
                "task": _extract_text(fields.get("任务名称")),
                "count": fields.get("告警次数", 0),
            })

        if not payload.get("has_more"):
            break
        page_token = payload.get("page_token")
        if not page_token:
            break

    return items


# 标记函数已抽到 bitable_service.mark_as_merged


async def main():
    parser = argparse.ArgumentParser(description="同步 bitable 中等待合并记录的 PR 状态")
    parser.add_argument("--apply", action="store_true", help="真的写回 bitable（默认 dry-run）")
    args = parser.parse_args()

    settings = get_settings()
    monorepo_dir = getattr(settings, "monorepo_dir", "") or os.environ.get("MONOREPO_DIR", "")
    if not monorepo_dir or not Path(monorepo_dir).is_dir():
        print(f"错误: monorepo_dir 不存在: {monorepo_dir}", file=sys.stderr)
        sys.exit(1)

    print(f"本地仓库: {monorepo_dir}")

    print("\n拉取 ⏳等待合并 记录…")
    records = await list_pending_merge_records()
    print(f"共 {len(records)} 条待检查\n")

    branches = [r["branch"] for r in records if r["branch"]]
    print(f"git fetch origin release/stable + {len(branches)} 个 fix 分支 …")
    fetch_branches(monorepo_dir, branches)

    merged: list[tuple[dict, str]] = []
    still_pending: list[dict] = []

    for r in records:
        if not r["branch"]:
            print(f"  [skip] 无分支: fp={r['fp']}")
            still_pending.append(r)
            continue

        ok, detail = check_branch_merged(monorepo_dir, r["branch"])
        marker = "✅" if ok else "·"
        print(f"  {marker} {r['branch']}  →  {detail}")
        if ok:
            merged.append((r, detail))
        else:
            still_pending.append(r)

    print(
        f"\n汇总: {len(merged)} 条已合并 / "
        f"{len(still_pending)} 条仍等待"
    )

    if not merged:
        return

    if not args.apply:
        print(f"\n[dry-run] 加 --apply 真的把这 {len(merged)} 条改为 {STATUS_MERGED}")
        print("  ⚠️ 请先确认飞书 bitable 的'状态'SingleSelect 字段里已加 '✅已合并' 选项")
        return

    print(f"\n开始更新 {len(merged)} 条…")
    ok_count = 0
    for r, detail in merged:
        if await mark_as_merged(r["record_id"]):
            ok_count += 1
        else:
            print(f"  ✗ 更新失败: {r['branch']}", file=sys.stderr)
    print(f"更新完成: {ok_count}/{len(merged)}")


if __name__ == "__main__":
    os.chdir(ROOT)
    asyncio.run(main())

"""判断 fix 分支是否已合入 release/stable（本地 git 实现，无需 GitHub API）

被两处使用：
  1. scheduled_scan.py 的去重分支 — 懒检查：每次命中 `⏳等待合并` 记录时调用一次，
     合入后立即把 bitable 升级为 `✅已合并` 并静默跳过本轮告警回复。
  2. scripts/sync_merged_prs.py — 批量检查 + 批量改状态（可作 cron 兜底）。

检测策略（从宽到严）：
  1. tip 是 release/stable 的祖先 → 已合入（fast-forward / tip 已直接在主干）
  2. git cherry 全部 `-` → patch 已合入（能识别 squash / rebase merge）
  3. 否则 → 未合入

为什么不走 GitHub API：.env 里的 fine-grained PAT resource owner 绑定个人账号，
不跨 second-me-01 组织，返回 401/404。本地 git 无此限制。
"""
import asyncio
import subprocess
from pathlib import Path

from incident_orchestrator.log import get_logger

logger = get_logger("GITCHK")

RELEASE_REF = "release/stable"


def _git(monorepo_dir: str, *args: str, timeout: int = 60) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["git", *args],
        cwd=monorepo_dir,
        capture_output=True,
        text=True,
        timeout=timeout,
    )


# ── 同步版本（给脚本用） ─────────────────────────────────────────────


def fetch_release_stable(monorepo_dir: str) -> bool:
    """fetch origin release/stable，返回是否成功"""
    r = _git(monorepo_dir, "fetch", "--quiet", "origin", RELEASE_REF, timeout=120)
    return r.returncode == 0


def fetch_branch(monorepo_dir: str, branch: str) -> bool:
    """fetch 单个 fix 分支，返回是否成功"""
    r = _git(monorepo_dir, "fetch", "--quiet", "origin", branch, timeout=60)
    return r.returncode == 0


def fetch_branches(monorepo_dir: str, branches: list[str]) -> None:
    """批量 fetch release/stable + 所有 fix 分支（脚本用，减少网络往返）"""
    if not branches:
        fetch_release_stable(monorepo_dir)
        return
    refs = [RELEASE_REF] + branches
    _git(monorepo_dir, "fetch", "--quiet", "origin", *refs, timeout=300)


def check_branch_merged(monorepo_dir: str, branch: str) -> tuple[bool, str]:
    """判断 branch 是否已合入 release/stable

    返回 (is_merged, detail)。不做 fetch，调用方负责先 fetch。
    """
    if not branch:
        return False, "无 branch"

    if not Path(monorepo_dir).is_dir():
        return False, f"monorepo 目录不存在: {monorepo_dir}"

    # 0. 远端分支是否存在
    r = _git(monorepo_dir, "rev-parse", "--verify", f"refs/remotes/origin/{branch}")
    if r.returncode != 0:
        return False, "远端分支不存在（被删或未 fetch）"

    # 1. tip 是否在 release/stable 历史链里（fast-forward / 已进主干）
    r = _git(
        monorepo_dir,
        "merge-base", "--is-ancestor",
        f"origin/{branch}", f"origin/{RELEASE_REF}",
    )
    if r.returncode == 0:
        return True, "已合入（tip 在 release/stable 祖先链）"

    # 2. git cherry: + 未合入；- patch 已合入（识别 squash / rebase merge）
    r = _git(monorepo_dir, "cherry", f"origin/{RELEASE_REF}", f"origin/{branch}")
    if r.returncode != 0:
        return False, f"git cherry 失败: {r.stderr.strip()[:120]}"

    lines = [line for line in r.stdout.splitlines() if line.strip()]
    if not lines:
        # 理论上走不到（merge-base 已处理），兜底判定为合入
        return True, "已合入（无分叉 commit）"

    plus = [l for l in lines if l.startswith("+")]
    minus = [l for l in lines if l.startswith("-")]
    if plus:
        return False, f"未合入（{len(plus)} commit 未在 release/stable）"
    return True, f"已合入（{len(minus)} commit squash/rebase 后匹配）"


# ── 异步版本（给 scheduled_scan 用，不阻塞事件循环） ─────────────────


async def check_branch_merged_async(monorepo_dir: str, branch: str) -> tuple[bool, str]:
    """异步懒检查：先 fetch 该分支 + release/stable，再判定

    用 run_in_executor 把同步 subprocess 调用扔到线程池，避免阻塞 asyncio 事件循环。
    单个分支 fetch + 判定通常在 1-3 秒完成。
    """
    loop = asyncio.get_event_loop()

    def _sync() -> tuple[bool, str]:
        # 懒检查每次只 fetch 需要的两个 ref，开销小
        fetch_ok = _git(
            monorepo_dir, "fetch", "--quiet", "origin", RELEASE_REF, branch, timeout=60
        )
        if fetch_ok.returncode != 0:
            # fetch 失败仍然尝试用现有 remote refs 判定
            logger.warning("fetch 失败: %s", fetch_ok.stderr.strip()[:120])
        return check_branch_merged(monorepo_dir, branch)

    try:
        return await loop.run_in_executor(None, _sync)
    except Exception as e:
        return False, f"git 检查异常: {e}"

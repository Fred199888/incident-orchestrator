"""飞书话题回复模板 — 全局统一格式

修改此文件即可影响所有回复（手动/自动/去重）。
"""
import json

from incident_orchestrator.config import get_mention_ids


def build_reply(
    *,
    service: str,
    root_cause: str,
    compile_ok: bool,
    branch: str,
    pr_url: str,
    alert_count: int = 0,
    owner: str = "",
    has_fix: bool = True,       # Claude 是否修改了代码
    has_worktree: bool = True,  # worktree 是否成功创建（未建成时语义≠"没改代码"）
) -> str:
    """构建飞书 post 格式回复

    返回 post content JSON 字符串
    """
    # 标题（优先级：worktree 缺失 → 编译成功+PR → 无改动 → 其他）
    if not has_worktree:
        # worktree 都没建起来，Claude 压根没机会改代码，不能叫"业务预期"
        title = "❓无法判断"
    elif compile_ok and pr_url:
        title = "⏳等待合并"
    elif not has_fix:
        title = "ℹ️业务预期"
    else:
        title = "❓无法判断"

    content = {"zh_cn": {"title": title, "content": []}}
    paragraphs = content["zh_cn"]["content"]

    # 服务
    paragraphs.append([
        {"tag": "text", "text": f"服务：{service}"},
    ])

    # 根因
    paragraphs.append([
        {"tag": "text", "text": f"根因：{root_cause[:500]}"},
    ])

    # 编译 + 分支（只有改了代码才显示）
    if has_fix:
        compile_text = "BUILD SUCCESS" if compile_ok else "BUILD FAILURE"
        paragraphs.append([
            {"tag": "text", "text": f"编译：{compile_text}　分支：{branch}"},
        ])

    # PR
    if pr_url:
        paragraphs.append([
            {"tag": "text", "text": "PR："},
            {"tag": "a", "text": "查看 PR", "href": pr_url},
        ])

    # 负责人
    if owner:
        paragraphs.append([
            {"tag": "text", "text": f"负责人：{owner}"},
        ])

    # 告警次数
    if alert_count > 0:
        paragraphs.append([
            {"tag": "text", "text": f"告警次数：{alert_count}"},
        ])

    # @人
    mention_ids = get_mention_ids()
    if mention_ids:
        at_line = []
        for uid in mention_ids:
            at_line.append({"tag": "at", "user_id": uid})
            at_line.append({"tag": "text", "text": " "})
        paragraphs.append(at_line)

    return json.dumps(content, ensure_ascii=False)

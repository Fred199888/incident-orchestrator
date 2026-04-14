"""修复后处理器 — 编译 → 提交 → 推送 → 格式化输出

Claude 只做定位+修复代码，剩下全部由 Python 完成。
"""
import os
import re
import subprocess
from dataclasses import dataclass, field

from incident_orchestrator.log import get_logger
from incident_orchestrator.config import get_settings

logger = get_logger("POSTPROCESS")


@dataclass
class PostprocessResult:
    compile_success: bool = False
    compile_output: str = ""
    commit_hash: str = ""
    branch_name: str = ""
    pr_url: str = ""
    changed_files: list[str] = field(default_factory=list)
    error: str = ""


def git_blame_author(
    worktree_dir: str,
    changed_files: list[str],
    root_cause: str = "",
) -> str:
    """从修改的文件 + 根因行号，git blame 查原始作者

    用 origin/release/stable 做 blame（修复前的代码），找到谁写了那行 bug。
    """
    import re as _re

    if not changed_files or not worktree_dir:
        return ""

    # 从 root_cause 提取行号
    line_num = ""
    m = _re.search(r"(\d+)", root_cause)
    if m:
        line_num = m.group(1)

    for file_path in changed_files:
        try:
            if not line_num:
                # 没有行号，blame 文件最后修改的那行（取 diff 的第一个改动行）
                diff = subprocess.run(
                    ["git", "diff", "origin/release/stable", "--", file_path],
                    cwd=worktree_dir, capture_output=True, text=True, timeout=10,
                )
                # 从 diff 提取 @@ -行号 @@
                hunk_m = _re.search(r"@@ -(\d+)", diff.stdout)
                if hunk_m:
                    line_num = hunk_m.group(1)

            if not line_num:
                continue

            # blame origin/release/stable 的代码（修复前）
            blame = subprocess.run(
                ["git", "blame", "--porcelain", "-L", f"{line_num},{line_num}", "origin/release/stable", "--", file_path],
                cwd=worktree_dir, capture_output=True, text=True, timeout=10,
            )
            for line in blame.stdout.split("\n"):
                if line.startswith("author "):
                    author = line[7:].strip()
                    if author and author != "Not Committed Yet":
                        return author
        except Exception:
            continue

    return ""


def compile_module(worktree_dir: str, maven_module: str) -> tuple[bool, str]:
    """编译指定模块，返回 (成功, 输出)

    maven_module 格式: "kernel/os-main/os-main-component"
    编译策略: cd 到父 pom 目录（kernel/os-main/），-pl 用子模块名（os-main-component）
    """
    if not worktree_dir or not os.path.isdir(worktree_dir):
        return False, "worktree 目录不存在"

    # 从 maven_module 拆分出 parent_dir 和 submodule
    # "kernel/os-main/os-main-component" → parent="kernel/os-main", sub="os-main-component"
    parts = maven_module.rstrip("/").rsplit("/", 1)
    if len(parts) == 2:
        parent_dir = os.path.join(worktree_dir, parts[0])
        submodule = parts[1]
    else:
        parent_dir = worktree_dir
        submodule = maven_module

    # 确认 parent_dir 有 pom.xml
    if not os.path.exists(os.path.join(parent_dir, "pom.xml")):
        # fallback: 直接在 worktree_dir 用完整路径
        parent_dir = worktree_dir
        submodule = maven_module

    cmd = [
        "mvn", "clean", "compile",
        "-pl", submodule, "-am",
        "-Dmaven.gitcommitid.skip=true",
        "-T", "4",
    ]

    try:
        result = subprocess.run(
            cmd, cwd=parent_dir,
            capture_output=True, text=True, timeout=300,
        )
        output = result.stdout[-3000:] if result.stdout else ""
        if result.returncode == 0 and "BUILD SUCCESS" in result.stdout:
            return True, output
        else:
            # 提取关键错误信息
            error_lines = []
            for line in (result.stdout + result.stderr).split("\n"):
                if "[ERROR]" in line:
                    error_lines.append(line.strip())
            return False, "\n".join(error_lines[-20:]) if error_lines else output
    except subprocess.TimeoutExpired:
        return False, "编译超时（5分钟）"
    except Exception as e:
        return False, str(e)


def get_changed_files(worktree_dir: str) -> list[str]:
    """获取修改的文件列表"""
    try:
        result = subprocess.run(
            ["git", "diff", "--name-only", "HEAD"],
            cwd=worktree_dir, capture_output=True, text=True, timeout=10,
        )
        # 加上 untracked
        result2 = subprocess.run(
            ["git", "ls-files", "--others", "--exclude-standard"],
            cwd=worktree_dir, capture_output=True, text=True, timeout=10,
        )
        files = result.stdout.strip().split("\n") + result2.stdout.strip().split("\n")
        return [f for f in files if f.strip()]
    except Exception:
        return []


def commit_and_push(
    worktree_dir: str,
    branch_name: str,
    module_name: str,
    summary: str,
    description: str = "",
) -> tuple[str, str]:
    """提交并推送，返回 (commit_hash, error)"""
    try:
        # git config
        subprocess.run(
            ["git", "config", "user.email", "bot@mindverse.ai"],
            cwd=worktree_dir, capture_output=True, timeout=5,
        )
        subprocess.run(
            ["git", "config", "user.name", "Claude Bot"],
            cwd=worktree_dir, capture_output=True, timeout=5,
        )

        # git add
        subprocess.run(
            ["git", "add", "-A"],
            cwd=worktree_dir, capture_output=True, timeout=10,
        )

        # 检查是否有改动
        status = subprocess.run(
            ["git", "status", "--porcelain"],
            cwd=worktree_dir, capture_output=True, text=True, timeout=5,
        )
        if not status.stdout.strip():
            return "", "无文件改动"

        # git commit — 用 stdin 传完整 message（保留多行 description）
        commit_title = f"fix({module_name}): {summary[:80]}"
        if description:
            commit_msg = f"{commit_title}\n\n{description}"
        else:
            commit_msg = commit_title

        result = subprocess.run(
            ["git", "commit", "-F", "-"],
            input=commit_msg,
            cwd=worktree_dir, capture_output=True, text=True, timeout=30,
        )
        if result.returncode != 0:
            return "", f"commit 失败: {result.stderr[:200]}"

        # 获取 commit hash
        hash_result = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=worktree_dir, capture_output=True, text=True, timeout=5,
        )
        commit_hash = hash_result.stdout.strip()

        # git push
        push_result = subprocess.run(
            ["git", "push", "-u", "origin", branch_name],
            cwd=worktree_dir, capture_output=True, text=True, timeout=60,
        )
        if push_result.returncode != 0:
            return commit_hash, f"push 失败: {push_result.stderr[:200]}"

        return commit_hash, ""
    except Exception as e:
        return "", str(e)


def format_fix_result(
    *,
    service: str,
    maven_module: str,
    branch_name: str,
    claude_analysis: str,
    compile_success: bool,
    changed_files: list[str],
    pr_url: str,
    error: str = "",
) -> str:
    """格式化最终输出（发到飞书的内容）"""
    settings = get_settings()

    # 从 Claude 的分析中提取根因和修复描述
    root_cause = ""
    fix_desc = ""

    for line in claude_analysis.split("\n"):
        line = line.strip()
        if re.match(r"\*{0,2}根因\*{0,2}[：:]", line):
            root_cause = re.sub(r"^\*{0,2}根因\*{0,2}[：:]\s*", "", line)
        elif re.match(r"\*{0,2}修复\*{0,2}[：:]", line):
            fix_desc = re.sub(r"^\*{0,2}修复\*{0,2}[：:]\s*", "", line)

    if not root_cause:
        # fallback: 取 Claude 回复的前 200 字符
        root_cause = claude_analysis[:200]

    # 格式化修改文件列表
    files_section = ""
    if changed_files:
        file_lines = [f"- {f}" for f in changed_files[:10]]
        files_section = "\n".join(file_lines)

    compile_status = "BUILD SUCCESS" if compile_success else "BUILD FAILURE"

    result = f"""## 修复结果

**服务**: {service}
**模块**: {maven_module}
**根因**: {root_cause}
**修复**: {fix_desc if fix_desc else '见修改文件'}
**编译**: {compile_status}
**分支**: {branch_name}
**PR**: {pr_url}

**修改文件**:
{files_section}"""

    if error:
        result += f"\n\n**异常**: {error}"

    return result


def postprocess(
    worktree_dir: str,
    branch_name: str,
    maven_module: str,
    module_path: str,
    service: str,
    claude_analysis: str,
    root_cause: str = "",
    fix_desc: str = "",
) -> PostprocessResult:
    """完整后处理：编译 → 提交（带详细 description）→ 推送 → 格式化"""
    settings = get_settings()
    github_url = settings.github_repo_url
    result = PostprocessResult(branch_name=branch_name)

    # 获取改动文件
    result.changed_files = get_changed_files(worktree_dir)
    if not result.changed_files:
        result.error = "Claude 未修改任何文件"
        logger.info("无文件改动")
        return result

    logger.info("改动文件: %s", result.changed_files)

    # 编译
    result.compile_success, result.compile_output = compile_module(worktree_dir, maven_module)
    logger.info("编译: %s", "成功" if result.compile_success else "失败")

    # 构建 commit description（和飞书回复格式一致）
    compile_text = "BUILD SUCCESS" if result.compile_success else "BUILD FAILURE"
    files_list = "\n".join(f"• {f.split('/')[-1]}" for f in result.changed_files[:10])
    description = (
        f"服务：{service}\n"
        f"模块：{maven_module}\n"
        f"根因：{root_cause}\n"
        f"修复：{fix_desc}\n"
        f"编译：{compile_text}\n"
        f"分支：{branch_name}\n"
        f"修改文件：\n{files_list}"
    )

    # 提交 + 推送
    module_name = module_path.rstrip("/").split("/")[-1] if module_path else "unknown"
    summary = (root_cause or claude_analysis)[:80].replace("\n", " ")
    result.commit_hash, commit_error = commit_and_push(
        worktree_dir, branch_name, module_name, summary, description
    )

    if commit_error:
        result.error = commit_error
        logger.info("提交/推送: %s", commit_error)
    else:
        # PR 目标分支: release/stable
        result.pr_url = f"{github_url}/compare/release/stable...{branch_name}?expand=1"
        logger.info("推送成功: %s", result.pr_url)

    return result

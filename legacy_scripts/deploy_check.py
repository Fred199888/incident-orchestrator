#!/usr/bin/env python3
"""
deploy_check.py -- 检查根因代码行是否由最近一次发版引入

通过 git blame 获取目标行的最后修改 commit，
然后检查该 commit 是否在最近一次 release/stable 合并到 master 之后引入。

用法:
  python3 deploy_check.py --file kernel/os-main/.../SomeService.java --line 214
  python3 deploy_check.py --file kernel/os-main/.../SomeService.java --line-start 210 --line-end 220

输出: JSON 到 stdout
"""
import argparse
import json
import re
import subprocess
import sys


def run_git(args: list[str], timeout: int = 10) -> tuple[int, str]:
    """执行 git 命令，返回 (returncode, stdout)"""
    try:
        result = subprocess.run(
            ["git"] + args,
            capture_output=True, text=True, timeout=timeout,
        )
        return result.returncode, result.stdout.strip()
    except Exception as e:
        return -1, str(e)


def find_last_release() -> dict | None:
    """找到最近一次 release/stable 合并到 master 的 commit"""
    rc, out = run_git([
        "log", "origin/master",
        "--format=%H|%ai|%s",
        "--grep=release/stable",
        "-1",
    ])
    if rc != 0 or not out:
        return None

    parts = out.split("|", 2)
    if len(parts) < 3:
        return None

    commit_hash, time_str, subject = parts[0].strip(), parts[1].strip(), parts[2].strip()

    pr_num = ""
    m = re.search(r'#(\d+)', subject)
    if m:
        pr_num = f"#{m.group(1)}"

    return {
        "commit": commit_hash,
        "time": time_str,
        "pr": pr_num,
        "subject": subject,
    }


def find_prev_master(release_commit: str) -> str | None:
    """获取 release merge 前的 master 状态（第一个 parent）"""
    rc, out = run_git(["rev-parse", f"{release_commit}^1"])
    if rc != 0 or not out:
        return None
    return out.strip()


def blame_line(file_path: str, line: int) -> dict | None:
    """git blame 单行，返回 commit 信息"""
    rc, out = run_git(["blame", "-L", f"{line},{line}", "--porcelain", file_path])
    if rc != 0 or not out:
        return None

    lines = out.split("\n")
    if not lines:
        return None

    # 第一行: commit_hash orig_line final_line [num_lines]
    commit_hash = lines[0].split()[0]

    # 解析 porcelain 输出
    author = ""
    author_time = ""
    summary = ""
    for ln in lines[1:]:
        if ln.startswith("author "):
            author = ln[7:]
        elif ln.startswith("author-time "):
            # Unix timestamp → human readable
            try:
                import datetime
                ts = int(ln[12:])
                author_time = datetime.datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")
            except Exception:
                author_time = ln[12:]
        elif ln.startswith("summary "):
            summary = ln[8:]

    return {
        "commit": commit_hash,
        "author": author,
        "author_time": author_time,
        "summary": summary,
    }


def is_ancestor(commit_a: str, commit_b: str) -> bool:
    """检查 commit_a 是否是 commit_b 的祖先（即 a 在 b 之前）"""
    rc, _ = run_git(["merge-base", "--is-ancestor", commit_a, commit_b])
    return rc == 0


def check_deploy(file_path: str, lines: list[int]) -> dict:
    """主逻辑：检查指定文件的指定行是否由最近发版引入"""

    # 1. 找最近一次发版
    release = find_last_release()
    if not release:
        return {
            "introduced_by_release": None,
            "error": "未找到 release/stable 合并记录",
        }

    # 2. 找发版前的 master
    prev_master = find_prev_master(release["commit"])
    if not prev_master:
        return {
            "introduced_by_release": None,
            "error": f"无法解析 {release['commit']}^1",
            "last_release": {"commit": release["commit"][:10], "time": release["time"], "pr": release["pr"]},
        }

    # 3. 逐行 blame + 判断
    blame_details = []
    any_in_release = False

    for line in lines:
        blame = blame_line(file_path, line)
        if not blame:
            blame_details.append({"line": line, "error": "blame 失败"})
            continue

        # 4. 判断 blame commit 是否在发版前就存在
        #    如果 blame_commit 是 prev_master 的祖先 → 发版前就有 → 非发版引入
        #    如果不是 → 发版引入
        in_release = not is_ancestor(blame["commit"], prev_master)

        if in_release:
            any_in_release = True

        blame_details.append({
            "line": line,
            "commit": blame["commit"][:10],
            "author": blame["author"],
            "author_time": blame["author_time"],
            "summary": blame["summary"],
            "in_release": in_release,
        })

    # 生成结论
    release_short = {
        "commit": release["commit"][:10],
        "time": release["time"],
        "pr": release["pr"],
        "subject": release["subject"][:80],
    }

    if any_in_release:
        # 找第一个 in_release 的 blame
        rel_blame = next(b for b in blame_details if b.get("in_release"))
        conclusion = (
            f"该行代码由最近发版 {release['pr']} ({release['time'][:10]}) 引入，"
            f"commit: {rel_blame['commit']} ({rel_blame['summary'][:50]})"
        )
    else:
        oldest = min(
            (b for b in blame_details if "author_time" in b),
            key=lambda b: b["author_time"],
            default=None,
        )
        if oldest:
            conclusion = (
                f"该行代码在最近发版之前就已存在"
                f"（{oldest['summary'][:40]}, {oldest['author_time'][:10]}），非发版引入"
            )
        else:
            conclusion = "无法确定代码变更时间"

    return {
        "file": file_path,
        "lines_checked": lines,
        "introduced_by_release": any_in_release,
        "last_release": release_short,
        "blame_details": blame_details,
        "conclusion": conclusion,
    }


def main():
    parser = argparse.ArgumentParser(description="检查根因代码行是否由最近发版引入")
    parser.add_argument("--file", required=True, help="文件路径（相对于 monorepo 根目录）")
    parser.add_argument("--line", type=int, help="行号")
    parser.add_argument("--line-start", type=int, help="行范围起始")
    parser.add_argument("--line-end", type=int, help="行范围结束")
    args = parser.parse_args()

    if args.line:
        lines = [args.line]
    elif args.line_start and args.line_end:
        lines = list(range(args.line_start, args.line_end + 1))
    else:
        print(json.dumps({"error": "需要 --line 或 --line-start/--line-end"}))
        sys.exit(0)

    result = check_deploy(args.file, lines)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

"""飞书消息处理核心逻辑

模式 1: 主动触发 — 用户在告警话题 @bot，带告警上下文创建/resume session
模式 2: 修复 — 用户指令包含修复关键词，预处理 → Claude 定位修复 → 后处理
"""
import asyncio
import json
import uuid

from incident_orchestrator.dependencies import get_mutex
from incident_orchestrator.feishu.client import get_feishu_client
from incident_orchestrator.log import get_logger, set_session_id
from incident_orchestrator.services.claude_runner import get_runner

logger = get_logger("HANDLER")

# 修复相关关键词 — 触发模式 2
FIX_KEYWORDS = {"修复", "修一下", "fix", "修bug", "帮我修", "修这个", "修复这个", "修复一下"}


def _extract_text_recursive(obj) -> list[str]:
    """递归提取所有 text 字段"""
    parts = []
    if isinstance(obj, dict):
        for key, val in obj.items():
            if key == "text" and isinstance(val, str):
                parts.append(val)
            elif key == "content" and isinstance(val, str) and not val.startswith("{"):
                parts.append(val)
            else:
                parts.extend(_extract_text_recursive(val))
    elif isinstance(obj, list):
        for item in obj:
            parts.extend(_extract_text_recursive(item))
    return parts


async def read_alert_context(feishu, root_id: str) -> str:
    """读取话题根消息（告警原文），递归提取所有文本"""
    try:
        root_msg = await feishu.get_message(root_id)
        if root_msg.get("code") != 0:
            return ""
        items = root_msg.get("data", {}).get("items", [])
        if not items:
            return ""

        content_str = items[0].get("body", {}).get("content", "")
        if not content_str:
            return ""

        try:
            content = json.loads(content_str)
            parts = _extract_text_recursive(content)
            return "\n".join(p.strip() for p in parts if p.strip())
        except json.JSONDecodeError:
            return content_str[:2000]
    except Exception as e:
        logger.warning("读取告警原文失败: %s", e)
        return ""


def _detect_mode(user_text: str) -> int:
    """检测用户意图 → 返回模式编号 (1=追问, 2=修复)"""
    text_lower = user_text.lower().strip()
    for kw in FIX_KEYWORDS:
        if kw in text_lower:
            return 2
    return 1


def _build_fix_prompt(alert_context: str, user_text: str, preprocess=None) -> str:
    """构建修复 prompt"""
    from incident_orchestrator.config import get_settings
    settings = get_settings()
    github_url = settings.github_repo_url

    if preprocess and preprocess.worktree_dir:
        return _build_fix_prompt_preprocessed(preprocess, user_text, github_url)

    return f"""你是一个自动化 bug 修复助手。

## 告警信息

{alert_context if alert_context else "(无告警原文)"}

## 用户指令

{user_text}

请分析告警根因并尝试修复。输出中文。"""


def _build_fix_prompt_preprocessed(p, user_text: str, github_url: str) -> str:
    """预处理完成后的精简 prompt — Claude 只做定位+修复"""

    cls_section = ""
    if p.cls_logs:
        cls_section = f"""### CLS 日志（已查询完成）

```
{p.cls_logs[:3000]}
```
"""
    if p.stack_trace:
        cls_section += f"""
### 堆栈（已提取）

```
{p.stack_trace}
```
"""

    return f"""你是一个自动化 bug 修复助手。以下信息已由系统预处理完成，直接使用即可。

## 告警摘要

| 字段 | 值 |
|------|------|
| 服务 | {p.service} |
| 模块路径 | {p.module_path} |
| Maven 模块 | {p.maven_module} |
| subcode | {p.subcode} |
| 接口 | {p.api_path} |
| 异常类型 | {p.error_type} |
| 错误位置 | {p.error_location} |
| tid | {p.tid} |
| 时间 | {p.alert_time} |

### 错误内容

{p.error_content}

{cls_section}
## 工作目录（已创建）

- **Worktree**: `{p.worktree_dir}`
- **分支**: `{p.branch_name}`
- 已基于 `origin/release/stable` 创建，直接在此目录操作

## 用户指令

{user_text}

## 你的任务

### Step 1: 根因追溯（至少读 3 个文件）

在 `{p.worktree_dir}` 内定位，**必须从根源找问题，禁止只看报错位置就改代码**。

注意：os-main 业务代码在 **os-main-component**（不是 os-main-service），包名 `com.mindverse.os.main`。

追溯步骤：
1. 有堆栈/错误位置 → Read 抛出文件
2. 向上追 2-3 层调用链：
   - **NPE** → 这个值哪里来的？为什么是 null？上游方法有没有可能返回 null？参数是不是可选的？
   - **RPC 异常** → 被调用方是否需要扩容/超时？调用方是不是传错了参数？能不能降级？
   - **数据库异常** → 数据是怎么进来的？字段约束是不是不匹配？是不是并发问题？
   - **业务异常** → 是不是用户输入合法但代码没处理？

**必须 Read 至少 3 个文件**：抛出点 + 直接调用方 + 数据源头。只看 1 个文件修的话一定是止血。

### Step 2: 从根源修复

**严禁止血式修复**（这些都是错的）：
- ❌ 直接 `if (x != null)` 包起来
- ❌ `try {{ ... }} catch {{ log.warn }}` 吞异常
- ❌ `return new ArrayList()` 返回空列表糊弄
- ❌ 把 error 日志改成 warn 就认为修了

**正确的修复方向**：
- ✓ 找到为什么值是 null，在**赋值点**保证非空（或显式用 Optional）
- ✓ 找到为什么 RPC 超时，在**调用处**调整超时/重试/降级策略
- ✓ 找到为什么数据库约束冲突，在**写入前**保证数据一致性
- ✓ 如果确实是外部输入不可控，在**入口层**（Controller/Service 参数校验）拒绝非法输入
- ✓ 如果是业务允许的情况（如客户端主动断连），在**识别点**用 warn 并**明确注释原因**

**判断标准**：修复后，你能一句话解释"为什么之前会出现这个错误，现在怎么保证不再出现"。如果只能说"我加了 null check"，就是止血。

如果分析后确认这不是代码 bug（如客户端正常断连、第三方服务不可达等），**不要改代码**，直接在 Step 3 输出说明原因。

### Step 3: 输出分析

修改完代码后，输出以下内容（系统会自动编译、提交、推送，你不需要做这些）：

```
**根因**: {{1-2句话，包含文件名和行号}}
**修复**: {{具体改了什么，1-2句话}}
```

## 约束

- 所有操作在 worktree `{p.worktree_dir}` 内
- 输出中文
- 不需要编译、提交、推送，系统会自动完成"""


def _extract_cause_and_fix(claude_analysis: str) -> tuple[str, str, str]:
    """从 Claude 回复中提取 (root_cause, fix_desc, error_type_hint)

    error_type_hint 是从 Claude 输出文本里正则抓到的异常类短名
    （NullPointerException / FeignException 等），给 preprocess.error_type 空
    的情况兜底。调用方可以 `preprocess.error_type or error_type_hint` 合并使用。
    """
    import re
    root_cause = ""
    fix_desc = ""
    for line in claude_analysis.split("\n"):
        line = line.strip()
        if re.match(r"\*{0,2}根因\*{0,2}[：:]", line):
            root_cause = re.sub(r"^\*{0,2}根因\*{0,2}[：:]\s*", "", line)
        elif re.match(r"\*{0,2}修复\*{0,2}[：:]", line):
            fix_desc = re.sub(r"^\*{0,2}修复\*{0,2}[：:]\s*", "", line)
    if not root_cause and claude_analysis:
        # fallback：取第一条非 Markdown-标题、非空的行作为根因
        # 否则像 "## 分析与修复结果" 这种无意义的 H2 会被当成根因
        for line in claude_analysis.splitlines():
            s = line.strip()
            if not s or s.startswith("#") or s.startswith("---"):
                continue
            root_cause = s[:200]
            break
        if not root_cause:
            root_cause = "未知"
    if not root_cause:
        root_cause = "未知"
    if not fix_desc:
        fix_desc = "Claude 未按约定格式输出修复描述"

    # error_type 兜底：从 Claude 输出文本里抓一个 Java 异常类短名
    # 优先匹配 java.xx.YyyException / YyyException / YyyError 这种裸词
    error_type_hint = ""
    if claude_analysis:
        m = re.search(r"\b(\w+(?:Exception|Error))\b", claude_analysis)
        if m:
            error_type_hint = m.group(1)

    return root_cause, fix_desc, error_type_hint


def _build_fix_post_reply(
    *,
    service: str,
    module: str,
    root_cause: str,
    fix_desc: str,
    compile_ok: bool,
    branch: str,
    pr_url: str,
    changed_files: list[str],
    mention_user_ids: list[str] | None = None,
    alert_count: int = 0,
) -> str:
    """构建飞书 post 格式的修复结果回复"""
    if compile_ok and pr_url:
        title_prefix = "⏳等待合并"
    elif pr_url:
        # 有 PR 但编译失败
        title_prefix = "⏳等待合并（编译失败）"
    else:
        title_prefix = "❓无法判断"
    compile_text = "BUILD SUCCESS" if compile_ok else "BUILD FAILURE"

    # 修改文件列表
    files_text = ""
    if changed_files:
        files_text = "\n".join(f"• {f.split('/')[-1]}" for f in changed_files[:5])
    else:
        files_text = "（无文件改动）"

    # 构建 post content
    content = {"zh_cn": {"title": title_prefix, "content": []}}
    paragraphs = content["zh_cn"]["content"]

    # 第一行：服务 + 模块
    paragraphs.append([
        {"tag": "text", "text": f"服务：{service}\n模块：{module}"},
    ])

    # 第二行：根因
    paragraphs.append([
        {"tag": "text", "text": f"根因：{root_cause[:300]}"},
    ])

    # 第三行：修复
    paragraphs.append([
        {"tag": "text", "text": f"修复：{fix_desc[:200]}"},
    ])

    # 第四行：编译 + 分支
    paragraphs.append([
        {"tag": "text", "text": f"编译：{compile_text}　分支：{branch}"},
    ])

    # 第五行：PR 链接
    if pr_url:
        paragraphs.append([
            {"tag": "text", "text": "PR："},
            {"tag": "a", "text": "查看 PR", "href": pr_url},
        ])

    # 第六行：修改文件
    paragraphs.append([
        {"tag": "text", "text": f"修改文件：\n{files_text}"},
    ])

    # 第七行：告警次数
    if alert_count > 0:
        paragraphs.append([
            {"tag": "text", "text": f"告警次数：{alert_count}"},
        ])

    # 最后一行：@ 负责人 + 管理员
    if mention_user_ids:
        at_line = []
        for uid in mention_user_ids:
            at_line.append({"tag": "at", "user_id": uid})
            at_line.append({"tag": "text", "text": " "})
        paragraphs.append(at_line)

    return json.dumps(content, ensure_ascii=False)


def _build_chat_prompt(alert_context: str, user_text: str) -> str:
    """模式 1: 构建普通对话 prompt"""
    prompt = ""
    if alert_context:
        prompt += f"以下是告警消息原文：\n\n{alert_context}\n\n---\n\n"
    prompt += f"用户指令：{user_text}"
    return prompt


async def handle_thread_message(
    root_id: str,
    message_id: str,
    chat_id: str,
    user_text: str,
) -> None:
    """处理话题内 @bot 消息"""
    feishu = get_feishu_client()

    # emoji 确认
    try:
        await feishu.add_reaction(message_id, "OnIt")
    except Exception:
        pass

    from incident_orchestrator.services.bitable_service import find_session_by_message, find_session_by_fingerprint
    mutex = get_mutex()
    runner = get_runner()

    await mutex.acquire(root_id)

    try:
        # 查 session：先用 root_id（同一条消息下的追问），再用 fingerprint（同类问题跨消息）
        existing_session = await find_session_by_message(root_id)
        if existing_session:
            logger.info("message_id 匹配 session: %s → %s", root_id[-8:], existing_session[:8])

        if not existing_session:
            # fallback: 从告警原文提取 fingerprint 查 session
            import re as _re_fp
            from incident_orchestrator.services.fingerprint import extract_fingerprint as _extract_fp
            alert_context_for_fp = await read_alert_context(feishu, root_id)
            if alert_context_for_fp:
                svc_m = _re_fp.search(r"监控对象[：:]\s*(.+?)(?=\n|$)", alert_context_for_fp)
                svc = svc_m.group(1).strip() if svc_m else ""
                content_m = _re_fp.search(r"content[：:]\s*(.+?)$", alert_context_for_fp, _re_fp.DOTALL)
                err = content_m.group(1).strip() if content_m else ""

                if svc and err:
                    fp = _extract_fp(svc, err)
                    existing_session = await find_session_by_fingerprint(fp)
                    if existing_session:
                        logger.info("fingerprint 匹配 session: %s → %s", fp[:40], existing_session[:8])

        if existing_session:
            # ── 已有 session → resume ──
            set_session_id(existing_session)
            logger.info("resume 开始 session=%s, text=%s", existing_session[:8], user_text[:80])
            import time as _time
            _t0 = _time.monotonic()
            try:
                reply = await runner.resume_session(existing_session, user_text)
            except Exception as e:
                elapsed = _time.monotonic() - _t0
                logger.exception(
                    "resume 异常 session=%s, 耗时=%.1fs",
                    existing_session[:8], elapsed,
                )
                try:
                    await feishu.reply_text(
                        message_id,
                        f"Claude resume 异常（{elapsed:.0f}s）：{str(e)[:200]}",
                    )
                except Exception:
                    pass
                return

            elapsed = _time.monotonic() - _t0
            reply_len = len(reply) if reply else 0
            logger.info(
                "resume 返回 session=%s, 耗时=%.1fs, reply_len=%d",
                existing_session[:8], elapsed, reply_len,
            )

            if reply:
                await feishu.reply_text(message_id, reply[:4000])
            else:
                # Claude 返回空 → 不能静默，必须告知用户
                logger.warning(
                    "resume 返回空 session=%s, 耗时=%.1fs",
                    existing_session[:8], elapsed,
                )
                try:
                    await feishu.reply_text(
                        message_id,
                        f"Claude 未返回内容（{elapsed:.0f}s），请重试或换个问法。",
                    )
                except Exception:
                    pass

        else:
            # ── 无 session → 首次创建 ──
            mode = _detect_mode(user_text)
            preprocess = None

            alert_context = await read_alert_context(feishu, root_id)
            if alert_context:
                logger.info("告警原文: %s", alert_context[:100])

            if mode == 2:
                # 先从告警内容提取 fingerprint，查 bitable 是否已修复
                import re as _re
                from incident_orchestrator.services.bitable_service import query_existing_fingerprints

                from incident_orchestrator.services.fingerprint import extract_fingerprint as _extract_fp

                svc_m = _re.search(r"监控对象[：:]\s*(.+?)(?=\n|$)", alert_context)
                svc = svc_m.group(1).strip() if svc_m else ""
                content_m = _re.search(r"content[：:]\s*(.+?)$", alert_context, _re.DOTALL)
                err = content_m.group(1).strip() if content_m else ""

                check_fp = None
                if svc and err:
                    fp = _extract_fp(svc, err)
                    logger.info("手动去重查: fp=%s", fp[:50])
                    existing = await query_existing_fingerprints([fp])
                    if fp in existing:
                        check_fp = fp
                if check_fp:
                    info = existing[check_fp]
                    def _val(v):
                        if isinstance(v, list) and v:
                            return v[0].get("text", "") if isinstance(v[0], dict) else str(v[0])
                        if isinstance(v, dict):
                            return v.get("text", str(v))
                        return str(v) if v else ""

                    stored = _val(info.get("根本原因", ""))
                    if stored and stored.startswith("{"):
                        await feishu.reply_message(message_id, "post", stored, reply_in_thread=True)
                    else:
                        await feishu.reply_text(message_id, stored or "该问题已有修复记录。")

                    logger.info("手动去重命中: %s", check_fp[:40])
                    return

                logger.info("模式2: 预处理开始")
                from incident_orchestrator.services.fix_preprocessor import preprocess_alert
                preprocess = await asyncio.get_event_loop().run_in_executor(
                    None, preprocess_alert, alert_context
                )
                logger.info("预处理完成: service=%s, branch=%s", preprocess.service, preprocess.branch_name)

                # CLS 查不到日志 → 回复飞书并停止修复
                if preprocess.cls_not_found:
                    await feishu.reply_text(
                        message_id,
                        "未找到相关日志，无法定位根因。请确认告警时间和服务是否正确，或手动提供 traceId。",
                    )
                    return

                prompt = _build_fix_prompt(alert_context, user_text, preprocess=preprocess)
            else:
                logger.info("模式1: 对话")
                prompt = _build_chat_prompt(alert_context, user_text)

            incident_id = f"INC-{uuid.uuid4().hex[:12]}"

            session_id, result = await runner.create_session(incident_id, prompt)
            set_session_id(session_id)
            logger.info("session 创建完成: %s", session_id)

            # ── 模式 2: 后处理（编译 → 重试 → 提交 → 推送 → 飞书回复 → bitable） ──
            use_post_reply = False
            post_data = None

            if mode == 2 and preprocess and preprocess.worktree_dir:
                from incident_orchestrator.services.fix_postprocessor import postprocess
                from incident_orchestrator.services.bitable_service import (
                    write_record, STATUS_PENDING_MERGE, STATUS_UNKNOWN,
                )

                # 先提取根因和修复描述（传给 postprocess 写入 commit）
                root_cause, fix_desc, error_type_hint = _extract_cause_and_fix(result or "")

                logger.info("后处理开始: 编译 → 提交 → 推送")
                post = await asyncio.get_event_loop().run_in_executor(
                    None,
                    lambda: postprocess(
                        preprocess.worktree_dir,
                        preprocess.branch_name,
                        preprocess.maven_module,
                        preprocess.module_path,
                        preprocess.service,
                        result or "",
                        root_cause=root_cause,
                        fix_desc=fix_desc,
                    ),
                )

                # 保留第一次的 PR（编译重试时可能丢失）
                first_pr = post.pr_url
                first_changed = post.changed_files

                # 编译失败 → resume Claude 修编译错误 → 再编译一次
                if not post.compile_success and post.compile_output:
                    logger.info("编译失败，resume Claude 修复编译错误")
                    retry_prompt = f"编译失败，请修复以下编译错误：\n\n```\n{post.compile_output[:2000]}\n```"
                    retry_result = await runner.resume_session(session_id, retry_prompt)
                    if retry_result:
                        result = (result or "") + "\n\n[编译重试]\n" + retry_result
                        root_cause2, fix_desc2, error_type_hint2 = _extract_cause_and_fix(result)
                        error_type_hint = error_type_hint or error_type_hint2
                        post = await asyncio.get_event_loop().run_in_executor(
                            None,
                            lambda: postprocess(
                                preprocess.worktree_dir,
                                preprocess.branch_name,
                                preprocess.maven_module,
                                preprocess.module_path,
                                preprocess.service,
                                result,
                                root_cause=root_cause2 or root_cause,
                                fix_desc=fix_desc2 or fix_desc,
                            ),
                        )

                # 合并：第二次没 PR 就用第一次的
                final_pr = post.pr_url or first_pr
                final_changed = post.changed_files or first_changed

                logger.info("后处理完成: compile=%s, pr=%s", post.compile_success, final_pr or "无")

                # bitable 状态
                if post.compile_success:
                    bitable_status = STATUS_PENDING_MERGE
                elif not post.changed_files:
                    bitable_status = "ℹ️业务预期"
                else:
                    bitable_status = STATUS_UNKNOWN

                # git blame
                from incident_orchestrator.services.fix_postprocessor import git_blame_author
                blame_author = await asyncio.get_event_loop().run_in_executor(
                    None, git_blame_author, preprocess.worktree_dir, final_changed, root_cause,
                )

                # fingerprint: 从告警 content 提取（和扫描阶段同一个函数）
                from incident_orchestrator.services.fingerprint import extract_fingerprint
                final_fingerprint = extract_fingerprint(preprocess.service, preprocess.error_content)

                task_name = fix_desc[:60] if fix_desc else root_cause[:60]

                # 构建飞书回复
                from incident_orchestrator.services.reply_template import build_reply
                post_data = build_reply(
                    service=preprocess.service,
                    root_cause=root_cause,
                    compile_ok=post.compile_success,
                    branch=preprocess.branch_name,
                    pr_url=final_pr,
                    alert_count=1,
                    owner=blame_author,
                    has_fix=bool(final_changed),
                )
                use_post_reply = True

                await write_record(
                    fingerprint=final_fingerprint,
                    service=preprocess.service,
                    subcode=preprocess.subcode or "-",
                    status=bitable_status,
                    task_name=task_name,
                    pr_url=final_pr,
                    branch=preprocess.branch_name,
                    root_cause=post_data,  # 完整话题回复内容（post JSON），去重时直接复用
                    root_cause_location=preprocess.error_location,
                    error_type=preprocess.error_type or error_type_hint,
                    tid=preprocess.tid or "-",
                    owner=blame_author,
                    alert_count=1,
                    message_id=root_id,
                    claude_session_id=session_id,
                )

            # 飞书回复
            if use_post_reply and post_data:
                await feishu.reply_message(message_id, "post", post_data, reply_in_thread=True)
            elif result:
                await feishu.reply_text(message_id, result[:4000])

    except Exception as e:
        logger.exception("处理失败")
        try:
            await feishu.reply_text(message_id, f"处理异常: {str(e)[:200]}")
        except Exception:
            pass
    finally:
        mutex.release(root_id)

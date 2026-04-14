"""修复预处理器 — Step 1~3 在 Python 层完成，节省 Claude token

1. 解析告警字段（复用 scanner.py）
2. CLS 日志查询（复用 cls_query.py）
3. 创建 Worktree + 分支
4. 查服务路径映射（复用 config.py）

输出结构化数据，喂给 Claude prompt，Claude 只做 Step 4~8。
"""
import json
import os
import re
import subprocess
import time
from dataclasses import dataclass, field

from incident_orchestrator.log import get_logger
from incident_orchestrator.config import get_settings

logger = get_logger("PREPROCESS")


@dataclass
class PreprocessResult:
    """预处理结果，直接注入 prompt"""
    # Step 1: 告警字段
    service: str = ""
    tid: str = ""
    alert_time: str = ""
    alert_time_ms: int = 0
    subcode: str = ""
    api_path: str = ""
    error_content: str = ""

    # Step 2: CLS 日志
    cls_logs: str = ""  # 格式化后的日志摘要
    stack_trace: str = ""
    error_type: str = ""
    error_location: str = ""

    # Step 3: Worktree
    worktree_dir: str = ""
    branch_name: str = ""

    # Step 4: 服务映射
    module_path: str = ""  # kernel/os-main/ etc
    maven_module: str = ""  # kernel/os-main/os-main-component etc

    # 原始告警文本
    raw_alert: str = ""
    # CLS 是否未找到日志
    cls_not_found: bool = False


def _parse_alert_fields(alert_text: str) -> dict:
    """Step 1: 解析告警字段（复用 scanner.py 的逻辑）"""
    fields = {}
    patterns = {
        "service": r"监控对象[：:]\s*\n?\s*(.+?)(?=\n|$)",
        "tid": r"tid[：:]\s*\n?\s*(.+?)(?=\n|$)",
        "subcode": r"[Ss]ub[Cc]ode[：:]\s*\n?\s*(.+?)(?=\n|$)",
        "api_path": r"接口[：:]\s*\n?\s*(.+?)(?=\n|$)",
        "alert_time": r"time[：:]\s*\n?\s*(.+?)(?=\n|$)",
    }
    for key, pattern in patterns.items():
        m = re.search(pattern, alert_text, re.IGNORECASE)
        if m:
            fields[key] = m.group(1).strip()

    # content 字段
    content_m = re.search(r"content[：:]\s*\n?\s*(.+?)(?=\n前往|$)", alert_text, re.IGNORECASE | re.DOTALL)
    if content_m:
        fields["error_content"] = content_m.group(1).strip()[:2000]

    return fields


def _parse_alert_time_ms(alert_time: str) -> int:
    """将告警时间字符串转为毫秒时间戳"""
    if not alert_time:
        return 0
    try:
        from datetime import datetime
        # "2026-04-07 14:22:21.038"
        dt = datetime.strptime(alert_time[:23], "%Y-%m-%d %H:%M:%S.%f")
        return int(dt.timestamp() * 1000)
    except Exception:
        try:
            from datetime import datetime
            dt = datetime.strptime(alert_time[:19], "%Y-%m-%d %H:%M:%S")
            return int(dt.timestamp() * 1000)
        except Exception:
            return 0


def _load_env_file() -> dict:
    """加载 .env 文件为环境变量 dict"""
    env = os.environ.copy()
    env_file = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), ".env")
    if os.path.exists(env_file):
        with open(env_file) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    key, _, value = line.partition("=")
                    env[key.strip()] = value.strip()
    return env


def _query_cls(
    tid: str,
    alert_time_ms: int,
    scripts_dir: str,
    service: str = "",
    error_content: str = "",
    subcode: str = "",
) -> dict:
    """Step 2: CLS 日志查询

    策略 1: 有 TID → cls_query.py --single（全链路追踪）
    策略 2: 无 TID/无结果 → Claude 分析告警 content 生成 3 个索引 → 查 8 库
    策略 3: 全部无结果 → 返回 not_found，由调用方回复飞书
    """
    empty = {"logs": "", "stack_trace": "", "error_type": "", "error_location": "", "not_found": False}

    if not alert_time_ms:
        empty["not_found"] = True
        return empty

    env = _load_env_file()
    output_file = f"/tmp/incident-fix/cls-{int(time.time())}.json"
    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    # ── 策略 1: 有 TID → 全链路查询 ──
    if tid:
        ok = _run_cls_single(tid, alert_time_ms, scripts_dir, output_file, env)
        if ok:
            parsed = _parse_cls_output(output_file)
            if parsed["logs"]:
                logger.info("CLS TID 查询成功: %d chars", len(parsed["logs"]))
                return parsed
            logger.info("CLS TID 查询无结果，降级到 Claude 分析索引")

    # ── 策略 2: Claude 分析生成索引 → 查 8 库 ──
    if error_content:
        keywords = _claude_generate_keywords(error_content)
        if keywords:
            logger.info("Claude 生成索引: %s", keywords)
            parsed = _keyword_search_with_exclusion(keywords, alert_time_ms, scripts_dir, env)
            if parsed["logs"]:
                logger.info("CLS 关键词查询成功: %d chars", len(parsed["logs"]))
                return parsed

    # ── 策略 3: 全部无结果 ──
    logger.warning("CLS 全部查询策略均无结果")
    empty["not_found"] = True
    return empty


# CLS 固定排除条件（MoreException 噪音过滤）
CLS_EXCLUSION_FILTER = (
    'AND NOT "MoreException" '
    'AND NOT "com.mindverse.os.framework.sdk.exception.MoreException" '
    'AND NOT "com.mindverse.os.main.sdk.exception.MoreException"'
)


def _claude_generate_keywords(error_content: str) -> list[str]:
    """调用 Claude 分析告警 content，生成 3 个 CLS 搜索索引"""
    prompt = f"""分析以下告警错误内容，提取 3 个最适合在日志系统中搜索的关键词。

要求：
- 每个关键词是一个精确的搜索项（异常类名、方法名、错误码等）
- 优先选择：异常类全名 > 方法名 > 错误消息关键片段
- 不要选通用词（如 error, exception, failed）
- 严格输出 3 行，每行一个关键词，不要其他内容

告警内容：
{error_content[:500]}"""

    try:
        result = subprocess.run(
            [
                "claude", "--print",
                "--output-format", "text",
                "--model", "opus",
                prompt,
            ],
            capture_output=True, text=True, timeout=30,
        )
        if result.returncode != 0:
            logger.warning("Claude 索引生成失败: %s", result.stderr[:100])
            return []

        lines = [l.strip() for l in result.stdout.strip().split("\n") if l.strip()]
        # 取前 3 个非空行
        keywords = lines[:3]
        return keywords
    except Exception as e:
        logger.warning("Claude 索引生成异常: %s", e)
        return []


def _keyword_search_with_exclusion(
    keywords: list[str],
    alert_time_ms: int,
    scripts_dir: str,
    env: dict,
) -> dict:
    """用 Claude 生成的关键词查 8 库，带固定排除条件"""
    empty = {"logs": "", "stack_trace": "", "error_type": "", "error_location": ""}

    from_ts = alert_time_ms - 900_000  # 前 15 分钟
    to_ts = alert_time_ms + 300_000    # 后 5 分钟

    # 每个关键词构造查询：level:ERROR AND NOT MoreException AND keyword
    queries = []
    for kw in keywords:
        safe_kw = kw.replace('"', '\\"')
        q = f'level:ERROR {CLS_EXCLUSION_FILTER} AND "{safe_kw}"'
        queries.append(q)

    search_script = f"""
import sys, json
sys.path.insert(0, '{scripts_dir}')
from cls_query import get_cls_client, query_single_topic, get_unique_topics

queries = {queries!r}
from_ts = {from_ts}
to_ts = {to_ts}

client = get_cls_client()
topics = get_unique_topics()

all_logs = []
query_used = ''

for q in queries:
    for topic_name, topic_id in topics:
        logs = query_single_topic(client, topic_id, q, from_ts, to_ts)
        if logs:
            for log in logs:
                log['topic_name'] = topic_name
            all_logs.extend(logs)
            query_used = q
            print(f'  {{topic_name}}: {{len(logs)}} 条', file=sys.stderr)
    if all_logs:
        break

all_logs.sort(key=lambda x: x.get('timestamp', 0))
print(json.dumps({{'trace_chain': all_logs[:20], 'query_used': query_used, 'log_count': len(all_logs)}}, ensure_ascii=False))
"""

    try:
        result = subprocess.run(
            ["python3", "-c", search_script],
            capture_output=True, text=True, timeout=60,
            cwd=scripts_dir,
            env=env,
        )

        if result.returncode != 0:
            logger.warning("CLS 关键词查询失败: %s", result.stderr[:200])
            return empty

        if not result.stdout.strip():
            return empty

        data = json.loads(result.stdout)
        logs = data.get("trace_chain", [])
        if not logs:
            return empty

        logger.info("CLS 关键词命中: query=%s, count=%d", data.get("query_used", "")[:60], data.get("log_count", 0))
        return _extract_from_logs(logs)

    except Exception as e:
        logger.warning("CLS 关键词查询异常: %s", e)
        return empty


def _run_cls_single(tid: str, alert_time_ms: int, scripts_dir: str, output_file: str, env: dict) -> bool:
    """调用 cls_query.py --single，返回是否成功"""
    try:
        result = subprocess.run(
            [
                "python3", f"{scripts_dir}/cls_query.py",
                "--single",
                "--issue-id", "preprocess",
                "--trace-id", tid,
                "--alert-time", str(alert_time_ms),
                "--output", output_file,
            ],
            capture_output=True, text=True, timeout=60,
            cwd=scripts_dir,
            env=env,
        )
        if result.returncode != 0:
            logger.warning("CLS --single 查询失败: %s", result.stderr[:200])
            return False
        return os.path.exists(output_file)
    except Exception as e:
        logger.warning("CLS --single 异常: %s", e)
        return False



def _extract_from_logs(logs: list[dict]) -> dict:
    """从日志列表提取摘要、堆栈、异常类型、错误位置"""
    log_lines = []
    stack_trace = ""
    error_type = ""
    error_location = ""

    for log in logs[:20]:
        if not isinstance(log, dict):
            continue
        content = log.get("content_head5", log.get("content", ""))
        topic = log.get("topic_name", "")
        log_lines.append(f"[{topic}] {content[:500]}")

        full_content = log.get("content_full", log.get("content", ""))
        if not stack_trace and "com.mindverse" in full_content:
            lines = full_content.split("\n")
            trace_lines = [l for l in lines if "com.mindverse" in l][:5]
            if trace_lines:
                stack_trace = "\n".join(trace_lines)

        if not error_type:
            err_m = re.search(r"(\w+(?:Exception|Error))", full_content)
            if err_m:
                error_type = err_m.group(1)

        if not error_location:
            loc_m = re.search(r"at\s+(com\.mindverse\.\S+)\((\w+\.java):(\d+)\)", full_content)
            if loc_m:
                error_location = f"{loc_m.group(2)}:{loc_m.group(3)}"

    return {
        "logs": "\n".join(log_lines),
        "stack_trace": stack_trace,
        "error_type": error_type,
        "error_location": error_location,
    }


def _parse_cls_output(output_file: str) -> dict:
    """解析 cls_query.py --single 的输出文件"""
    try:
        with open(output_file) as f:
            data = json.load(f)

        # --single 输出格式: {trace_chain: [...], stack_trace_top3, ...}
        logs = data.get("trace_chain", data if isinstance(data, list) else [])
        return _extract_from_logs(logs)
    except Exception as e:
        logger.warning("解析 CLS 输出失败: %s", e)
        return {"logs": "", "stack_trace": "", "error_type": "", "error_location": ""}


def _get_service_mapping(service: str, scripts_dir: str) -> tuple[str, str]:
    """Step 4: 查服务路径映射（从 config.py 读）"""
    try:
        result = subprocess.run(
            [
                "python3", "-c",
                f"import sys; sys.path.insert(0, '{scripts_dir}'); "
                f"from config import SERVICE_PATH_MAP; "
                f"path = SERVICE_PATH_MAP.get('{service}', ''); "
                f"print(path)",
            ],
            capture_output=True, text=True, timeout=5,
            cwd=scripts_dir,
            env=_load_env_file(),
        )
        module_path = result.stdout.strip()
    except Exception:
        module_path = ""

    # 推断 Maven 编译模块
    maven_map = {
        "kernel/os-main/": "kernel/os-main/os-main-component",
        "kernel/os-ws/": "kernel/os-ws/os-ws-component",
        "kernel/base-datahub/": "kernel/base-datahub/base-datahub-component",
        "biz/os-user/": "biz/os-user/os-user-component",
    }
    maven_module = maven_map.get(module_path, module_path.rstrip("/") if module_path else "")

    return module_path, maven_module


def _create_worktree(monorepo_dir: str, issue_slug: str) -> tuple[str, str]:
    """Step 3: 创建 Worktree + 分支

    关键：slug 必须对并发任务唯一。之前用 int(time.time()) 秒级时间戳，
    同一轮扫描里的多个修复任务几乎同时调用会拿到同一秒 → 目录冲突
    ("fatal: 'fix-XXX' already exists")，第二个任务的 worktree_dir 返回空串，
    导致下游判定跑偏（status 落到 ❓无法判断，但 Claude 已经跑过了）。
    改用 uuid 后缀 → 几乎零冲突。
    """
    import uuid as _uuid
    date_str = time.strftime("%Y%m%d")
    unique = _uuid.uuid4().hex[:6]
    branch_name = f"fix/cc/{date_str}/{issue_slug}-{unique}"
    worktree_dir = os.path.join(
        monorepo_dir, ".claude", "worktrees", f"fix-{int(time.time())}-{unique}"
    )

    try:
        # fetch latest master
        subprocess.run(
            ["git", "fetch", "origin", "release/stable"],
            cwd=monorepo_dir, capture_output=True, timeout=30,
        )
        # create worktree based on release/stable
        subprocess.run(
            ["git", "worktree", "add", worktree_dir, "origin/release/stable", "-b", branch_name],
            cwd=monorepo_dir, capture_output=True, timeout=30, check=True,
        )
        logger.info("worktree 创建成功: %s", worktree_dir)
        return worktree_dir, branch_name
    except subprocess.CalledProcessError as e:
        logger.warning("worktree 创建失败: %s", e.stderr)
        return "", ""


def _make_issue_slug(service: str, subcode: str, error_type: str) -> str:
    """生成分支名用的 slug"""
    parts = []
    if subcode:
        parts.append(subcode.replace(".", "-"))
    elif error_type:
        # NullPointerException → npe
        short = re.sub(r"([a-z])([A-Z])", r"\1-\2", error_type).lower()
        parts.append(short[:20])
    if service:
        # os-main-inner-api → main
        s = service.replace("os-", "").split("-")[0]
        parts.insert(0, s)
    return "-".join(parts) if parts else "unknown"


def preprocess_alert(alert_text: str) -> PreprocessResult:
    """完整预处理：解析 → CLS → Worktree → 映射"""
    settings = get_settings()
    scripts_dir = os.path.expanduser(settings.legacy_scripts_dir)
    monorepo_dir = settings.monorepo_dir

    result = PreprocessResult(raw_alert=alert_text)

    # Step 1: 解析告警字段
    fields = _parse_alert_fields(alert_text)
    result.service = fields.get("service", "")
    result.tid = fields.get("tid", "")
    result.alert_time = fields.get("alert_time", "")
    result.alert_time_ms = _parse_alert_time_ms(result.alert_time)
    result.subcode = fields.get("subcode", "")
    result.api_path = fields.get("api_path", "")
    result.error_content = fields.get("error_content", "")

    logger.info("Step 1: service=%s, tid=%s..., subcode=%s", result.service, result.tid[:20], result.subcode)

    # Step 2: CLS 日志查询（TID → 关键词 → 异常类名 → ERROR 降级）
    cls_result = _query_cls(
        result.tid, result.alert_time_ms, scripts_dir,
        service=result.service,
        error_content=result.error_content,
        subcode=result.subcode,
    )
    result.cls_logs = cls_result["logs"]
    result.stack_trace = cls_result["stack_trace"]
    result.error_type = cls_result["error_type"]
    result.error_location = cls_result["error_location"]
    result.cls_not_found = cls_result.get("not_found", False)

    logger.info("Step 2: logs=%d chars, error_type=%s, not_found=%s", len(result.cls_logs), result.error_type, result.cls_not_found)

    # CLS 没找到日志 → 不创建 worktree，直接返回
    if result.cls_not_found:
        logger.warning("CLS 未找到日志，跳过 worktree 和后续步骤")
        return result

    # Step 3: Worktree
    slug = _make_issue_slug(result.service, result.subcode, result.error_type)
    result.worktree_dir, result.branch_name = _create_worktree(monorepo_dir, slug)

    logger.info("Step 3: branch=%s", result.branch_name)

    # Step 4: 服务映射
    result.module_path, result.maven_module = _get_service_mapping(result.service, scripts_dir)

    logger.info("Step 4: module=%s, maven=%s", result.module_path, result.maven_module)

    return result

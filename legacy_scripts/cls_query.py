#!/usr/bin/env python3
"""
cls_query.py — CLS 日志全链路查询

对每个有 traceId 的 issue，搜索全部 8 个日志库（跨服务），
每条日志取前 5 行，按时间排序，返回完整调用链。

用法:
  批量: python3 cls_query.py --scan-result scan-result.json --output cls-results.json
  单条: python3 cls_query.py --single --issue-id I001 --trace-id xxx --alert-time 123 --output cls-result.json

注意:
  - 批量模式为串行查询（向后兼容），并发版本见 pipeline.py
  - 核心函数 get_cls_client / query_single_topic / extract_stack_trace 等
    被 pipeline.py 导入复用，修改时注意保持签名兼容
"""
import argparse
import json
import os
import re
import sys
import time

from config import (
    CLS_REGION,
    CLS_TOPIC_ID_MAP,
    TENCENTCLOUD_SECRET_ID,
    TENCENTCLOUD_SECRET_KEY,
)

try:
    from tencentcloud.common import credential
    from tencentcloud.common.profile.client_profile import ClientProfile
    from tencentcloud.common.profile.http_profile import HttpProfile
    from tencentcloud.cls.v20201016 import cls_client, models
except ImportError:
    print("错误: 请安装 tencentcloud-sdk-python-cls: pip install tencentcloud-sdk-python-cls", file=sys.stderr)
    sys.exit(1)


def get_cls_client() -> cls_client.ClsClient:
    """创建 CLS 客户端"""
    cred = credential.Credential(TENCENTCLOUD_SECRET_ID, TENCENTCLOUD_SECRET_KEY)
    http_profile = HttpProfile()
    http_profile.endpoint = "cls.tencentcloudapi.com"
    client_profile = ClientProfile()
    client_profile.httpProfile = http_profile
    return cls_client.ClsClient(cred, CLS_REGION, client_profile)



def extract_stack_trace(log_results: list[dict]) -> tuple[str, str, str, str, str]:
    """
    从 CLS 日志结果中提取堆栈信息
    返回: (stack_trace_top3, cls_summary, raw_error_message, error_location, user_id)
    """
    stack_lines = []
    error_msg = ""
    user_id = ""

    for log in log_results:
        content = ""
        # CLS 日志结构：log 是一个 dict，可能有多种字段
        if isinstance(log, dict):
            # 常见字段: A(腾讯云日志格式), __CONTENT__, message, log, Message
            for key in ["A", "__CONTENT__", "message", "log", "Message"]:
                if key in log and log[key]:
                    content = str(log[key])
                    break
            if not content:
                content = json.dumps(log, ensure_ascii=False)
        else:
            content = str(log)

        # 提取异常信息
        exception_patterns = [
            r'((?:java\.\S+Exception|java\.\S+Error|RuntimeException|NullPointerException|IllegalArgumentException|IllegalStateException)\S*:?\s*[^\n]*)',
            r'(Caused by:\s*\S+[^\n]*)',
        ]
        for pattern in exception_patterns:
            matches = re.findall(pattern, content)
            if matches and not error_msg:
                error_msg = matches[0].strip()[:200]

        # 提取 userId（从日志内容中匹配）
        if not user_id:
            for uid_pattern in [r'userId\s*[:：=]\s*(\d+)', r'user[_-]?[iI][dD]\s*[:：=]\s*(\d+)']:
                m = re.search(uid_pattern, content)
                if m:
                    user_id = m.group(1)
                    break

        # 提取 at 行（堆栈帧）
        at_lines = re.findall(r'(at\s+com\.mindverse\.\S+\([^)]+\))', content)
        stack_lines.extend(at_lines)

        # 如果没有 com.mindverse，也收集其他 at 行
        if not at_lines:
            other_at = re.findall(r'(at\s+\S+\([^)]+\))', content)
            stack_lines.extend(other_at)

    # 去重保留前3行业务代码
    seen = set()
    unique_stack = []
    for line in stack_lines:
        if line not in seen:
            seen.add(line)
            unique_stack.append(line)
            if len(unique_stack) >= 3:
                break

    stack_trace_top3 = "\n".join(unique_stack)

    # 提取 error_location（第一个业务代码调用点的 File.java:line）
    error_location = ""
    loc_match = re.search(r'at\s+com\.mindverse\.\S+\((\w+\.java:\d+)\)', stack_trace_top3)
    if loc_match:
        error_location = loc_match.group(1)
    elif stack_trace_top3:
        loc_match_any = re.search(r'at\s+\S+\((\w+\.java:\d+)\)', stack_trace_top3)
        if loc_match_any:
            error_location = loc_match_any.group(1)

    # 生成 cls_summary
    if error_msg:
        loc_str = f" at {error_location}" if error_location else ""
        cls_summary = f"{error_msg}{loc_str}"
    elif stack_trace_top3:
        cls_summary = f"堆栈: {unique_stack[0]}" if unique_stack else ""
    else:
        cls_summary = ""

    # 截断 summary
    if len(cls_summary) > 300:
        cls_summary = cls_summary[:297] + "..."

    # raw_error_message: 原始异常消息（未截断版）
    raw_error_message = error_msg

    return stack_trace_top3, cls_summary, raw_error_message, error_location, user_id


def get_unique_topics() -> list[tuple[str, str]]:
    """获取去重后的 (topic_name, topic_id) 列表"""
    seen_ids = set()
    topics = []
    for name, tid in CLS_TOPIC_ID_MAP.items():
        if tid not in seen_ids:
            seen_ids.add(tid)
            topics.append((name, tid))
    return topics


def extract_log_summary(content: str) -> str:
    """从日志内容中提取精简摘要：time [level] content（前 5 行）"""
    # 尝试解析 JSON 格式日志
    try:
        log = json.loads(content) if content.startswith("{") else None
    except (json.JSONDecodeError, ValueError):
        log = None

    if log and isinstance(log, dict):
        time_str = log.get("time", "")
        level = log.get("level", "")
        body = log.get("content", "") or log.get("message", "") or log.get("log", "")
        # 取 content 前 5 行
        lines = str(body).split("\n")
        body_head = "\n".join(lines[:5])
        if len(lines) > 5:
            body_head += f"\n... (共 {len(lines)} 行)"
        return f"{time_str} [{level}] {body_head}"

    # 非 JSON，直接截取前 5 行
    lines = content.split("\n")
    if len(lines) <= 5:
        return content
    return "\n".join(lines[:5]) + f"\n... (共 {len(lines)} 行)"


def query_single_topic(
    client: cls_client.ClsClient,
    topic_id: str,
    trace_id: str,
    from_ts: int,
    to_ts: int,
) -> list[dict]:
    """查询单个 topic，返回日志条目列表（每条含 content_head5, timestamp, topic_id）"""
    results = []
    try:
        # 优先查 ERROR 级别
        req = models.SearchLogRequest()
        req.TopicId = topic_id
        req.Query = f"{trace_id} AND level:ERROR"
        req.From = from_ts
        req.To = to_ts
        req.Limit = 20

        resp = client.SearchLog(req)

        # ERROR 无结果则 fallback 全量
        if not resp.Results or len(resp.Results) == 0:
            req2 = models.SearchLogRequest()
            req2.TopicId = topic_id
            req2.Query = trace_id
            req2.From = from_ts
            req2.To = to_ts
            req2.Limit = 20
            resp = client.SearchLog(req2)

        if not hasattr(resp, "Results") or not resp.Results:
            return []

        for r in resp.Results:
            if not hasattr(r, "LogJson") or not r.LogJson:
                continue
            try:
                log = json.loads(r.LogJson)
            except json.JSONDecodeError:
                log = {"__CONTENT__": r.LogJson}

            # 提取日志内容
            content = ""
            for key in ["A", "__CONTENT__", "message", "log", "Message"]:
                if key in log and log[key]:
                    content = str(log[key])
                    break
            if not content:
                content = json.dumps(log, ensure_ascii=False)

            # 提取时间戳（毫秒）
            log_ts = 0
            for ts_key in ["__TIMESTAMP__", "timestamp", "time", "@timestamp"]:
                if ts_key in log and log[ts_key]:
                    try:
                        ts_val = str(log[ts_key])
                        # ISO 格式或纯数字
                        if ts_val.isdigit():
                            log_ts = int(ts_val)
                        else:
                            # 尝试解析 ISO 格式
                            from datetime import datetime
                            dt = datetime.fromisoformat(ts_val.replace("Z", "+00:00"))
                            log_ts = int(dt.timestamp() * 1000)
                    except Exception:
                        pass
                    if log_ts > 0:
                        break

            # 提取日志级别
            log_level = ""
            if isinstance(log, dict):
                log_level = log.get("level", "")

            results.append({
                "topic_id": topic_id,
                "content_head5": extract_log_summary(content),
                "content_full": content,
                "timestamp": log_ts,
                "level": log_level,
            })
    except Exception as e:
        print(f"    查询 topic {topic_id[:12]}... 异常: {e}", file=sys.stderr)

    return results


def query_all_topics_for_trace(
    client: cls_client.ClsClient,
    trace_id: str,
    alert_time: str,
) -> dict:
    """
    全链路查询：用 traceId 搜索全部日志库，按时间排序。
    返回: dict with trace_chain, stack_trace_top3, cls_summary, log_count,
          query_params, raw_error_message, error_location, userId
    """
    try:
        alert_ts = int(alert_time)
    except (ValueError, TypeError):
        alert_ts = int(time.time() * 1000)

    from_ts = alert_ts - 900_000  # 前 15 分钟
    to_ts = alert_ts + 300_000    # 后 5 分钟

    # 构建 topic_name → topic_id 反向映射
    tid_to_name = {}
    for name, tid in CLS_TOPIC_ID_MAP.items():
        if tid not in tid_to_name:
            tid_to_name[tid] = name

    # 搜索全部唯一 topic
    all_logs = []
    topics = get_unique_topics()
    queried_topics = []

    for topic_name, topic_id in topics:
        logs = query_single_topic(client, topic_id, trace_id, from_ts, to_ts)
        if logs:
            queried_topics.append({"name": topic_name, "id": topic_id, "count": len(logs)})
            print(f"    {topic_name}: {len(logs)} 条日志", file=sys.stderr)
        for log in logs:
            log["topic_name"] = topic_name
        all_logs.extend(logs)
        time.sleep(0.1)  # 避免 API 限流

    if not all_logs:
        return {
            "trace_chain": [],
            "stack_trace_top3": "",
            "cls_summary": "",
            "log_count": 0,
            "query_params": {
                "trace_id": trace_id,
                "time_from": from_ts,
                "time_to": to_ts,
                "topics_queried": len(topics),
                "topics_with_logs": [],
            },
            "raw_error_message": "",
            "error_location": "",
            "userId": "",
            "error_type": "",
            "involved_topics": "",
        }

    # 按时间排序
    all_logs.sort(key=lambda x: x["timestamp"])

    # 构建 trace_chain（每条日志前 5 行 + topic 来源 + 时间 + 级别）
    trace_chain = []
    for log in all_logs:
        trace_chain.append({
            "topic_name": log["topic_name"],
            "timestamp": log["timestamp"],
            "level": log.get("level", ""),
            "content_head5": log["content_head5"],
        })

    # 用全部日志提取堆栈信息（兼容旧逻辑）
    full_log_dicts = []
    for log in all_logs:
        full_log_dicts.append({"__CONTENT__": log["content_full"]})

    stack_top3, summary, raw_error_message, error_location, user_id = extract_stack_trace(full_log_dicts)

    # 提取异常类名（短名）
    error_type = ""
    if raw_error_message:
        m = re.search(r'((?:\w+\.)*\w+(?:Exception|Error))', raw_error_message)
        if m:
            error_type = m.group(1).split(".")[-1]

    # 有日志的 topic 名称列表
    involved_topics = ",".join(t["name"] for t in queried_topics)

    return {
        "trace_chain": trace_chain,
        "stack_trace_top3": stack_top3,
        "cls_summary": summary,
        "log_count": len(all_logs),
        "query_params": {
            "trace_id": trace_id,
            "time_from": from_ts,
            "time_to": to_ts,
            "topics_queried": len(topics),
            "topics_with_logs": queried_topics,
        },
        "raw_error_message": raw_error_message,
        "error_location": error_location,
        "userId": user_id,
        "error_type": error_type,
        "involved_topics": involved_topics,
    }


def query_single_issue(args) -> None:
    """
    --single 模式：对单条 issue 全链路查询 CLS，输出 cls-result.json。
    搜索全部日志库，按时间排序返回完整调用链。
    """
    issue_id = args.issue_id
    trace_id = args.trace_id or ""
    alert_time = args.alert_time or ""

    print(f"[--single] 全链路查询 {issue_id}: traceId={trace_id}", file=sys.stderr)

    # 无效 traceId 快速返回
    if not trace_id or trace_id in ("N/A", "null", "undefined", ""):
        result = {
            "issue_id": issue_id,
            "cls_summary": "",
            "stack_trace_top3": "",
            "log_count": 0,
            "query_status": "skipped_no_trace_id",
        }
        os.makedirs(os.path.dirname(args.output), exist_ok=True)
        with open(args.output, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        print(json.dumps(result))
        return

    client = get_cls_client()

    cls_result = query_all_topics_for_trace(client, trace_id, alert_time)
    log_count = cls_result["log_count"]
    status = "success" if log_count > 0 else "no_logs"

    result = {
        "issue_id": issue_id,
        "trace_chain": cls_result["trace_chain"],
        "cls_summary": cls_result["cls_summary"],
        "stack_trace_top3": cls_result["stack_trace_top3"],
        "log_count": log_count,
        "query_status": status,
        "query_params": cls_result["query_params"],
        "raw_error_message": cls_result["raw_error_message"],
        "error_location": cls_result["error_location"],
        "userId": cls_result["userId"],
        "error_type": cls_result.get("error_type", ""),
        "involved_topics": cls_result.get("involved_topics", ""),
    }

    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    topics_hit = len(cls_result["query_params"].get("topics_with_logs", []))
    print(f"  → {status}: {log_count} 条日志, {topics_hit} 个日志库命中", file=sys.stderr)
    print(json.dumps(result))


def query_batch_issues(args) -> None:
    """
    批量模式：读 scan-result.json，对所有有 traceId 的 issue 全链路查询。
    每条 issue 搜索全部 8 个日志库，按时间排序返回跨服务调用链。
    """
    with open(args.scan_result, "r", encoding="utf-8") as f:
        scan_result = json.load(f)

    new_issues = scan_result.get("new_issues", [])
    if not new_issues:
        print("无 new_issues，跳过 CLS 查询", file=sys.stderr)
        result = {"results": {}}
        with open(args.output, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        print(json.dumps({"queried": 0, "success": 0}))
        return

    client = get_cls_client()
    topics = get_unique_topics()
    print(f"全链路模式: 每条 issue 搜索 {len(topics)} 个日志库", file=sys.stderr)

    results = {}
    success_count = 0

    for issue in new_issues:
        issue_id = issue["issue_id"]
        trace_id = issue.get("traceId", "")
        alert_time = issue.get("alert_time", "")

        print(f"查询 {issue_id}: traceId={trace_id[:30]}..." if trace_id else f"查询 {issue_id}: 无 traceId", file=sys.stderr)

        if not trace_id or trace_id in ("N/A", "null", "undefined", ""):
            results[issue_id] = {
                "issue_id": issue_id,
                "trace_chain": [],
                "cls_summary": "",
                "stack_trace_top3": "",
                "log_count": 0,
                "query_status": "skipped_no_trace_id",
                "query_params": {},
                "raw_error_message": "",
                "error_location": "",
                "userId": "",
            }
            continue

        cls_result = query_all_topics_for_trace(client, trace_id, alert_time)

        log_count = cls_result["log_count"]
        status = "success" if log_count > 0 else "no_logs"
        if log_count > 0:
            success_count += 1

        topics_hit = len(cls_result["query_params"].get("topics_with_logs", []))

        results[issue_id] = {
            "issue_id": issue_id,
            "trace_chain": cls_result["trace_chain"],
            "cls_summary": cls_result["cls_summary"],
            "stack_trace_top3": cls_result["stack_trace_top3"],
            "log_count": log_count,
            "query_status": status,
            "query_params": cls_result["query_params"],
            "raw_error_message": cls_result["raw_error_message"],
            "error_location": cls_result["error_location"],
            "userId": cls_result["userId"],
        }

        print(f"  → {status}: {log_count} 条日志, {topics_hit} 个日志库命中", file=sys.stderr)
        time.sleep(0.3)  # 每条 issue 之间间隔

    output = {"results": results}
    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"CLS 全链路查询完成: {len(results)} 个 issue, {success_count} 个有日志", file=sys.stderr)
    print(json.dumps({"queried": len(results), "success": success_count}))


def main():
    parser = argparse.ArgumentParser(description="CLS 日志查询")

    # 单条模式（Worker 端到端使用）
    parser.add_argument("--single", action="store_true", help="单条 issue 查询模式（Worker 使用）")
    parser.add_argument("--issue-id", help="issue ID（--single 模式必填）")
    parser.add_argument("--trace-id", help="traceId（--single 模式）")
    parser.add_argument("--service", help="服务名（兼容旧调用，当前全链路模式下忽略）")
    parser.add_argument("--alert-time", help="告警时间戳 ms（--single 模式）")

    # 批量模式（原有）
    parser.add_argument("--scan-result", help="scan-result.json 路径（批量模式必填）")

    # 公共
    parser.add_argument("--output", required=True, help="输出文件路径")

    args = parser.parse_args()

    if args.single:
        if not args.issue_id:
            print("错误: --single 模式需要 --issue-id", file=sys.stderr)
            sys.exit(1)
        query_single_issue(args)
    else:
        if not args.scan_result:
            print("错误: 批量模式需要 --scan-result", file=sys.stderr)
            sys.exit(1)
        query_batch_issues(args)


if __name__ == "__main__":
    main()

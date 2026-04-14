#!/usr/bin/env python3
"""
scanner.py — 替代 Scanner sub-agent（v6 unique issue 目标模式）

持续翻页读取飞书消息，跳过已有话题回复的消息，
在累积循环内实时做 fingerprint 去重，按 unique issue 数量计数，
累积到 target_count 个 unique issue 后停止。

用法:
  python3 scanner.py --output /tmp/bugfix/1/scan-result.json [--page-token "xxx"] [--target-count 100]
"""
import argparse
import hashlib
import json
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from config import (
    BOT_ID,
    CHAT_ID,
    SERVICE_PATH_MAP,
)
from lark_client import LarkClient


def _parse_interactive_content(content: dict) -> dict:
    """
    解析 interactive 格式消息（新群 CLS 告警）。
    格式: {"title": null, "elements": [[{tag:text, text:"监控对象："}, {tag:text, text:"service\n"}, ...]]}
    按标签名拆分字段，返回与 post 格式相同的 {"title", "lines", "cls_topic_id"}。
    """
    elements = content.get("elements", [])
    if not elements:
        return {"title": "", "lines": [], "cls_topic_id": ""}

    flat = elements[0] if elements else []

    # 按标签拆分: 收集 label→value 对
    fields: dict[str, str] = {}
    cls_topic_id = ""
    current_label = ""

    for elem in flat:
        if not isinstance(elem, dict):
            continue

        # 提取 CLS 链接
        href = elem.get("href", "")
        if href and not cls_topic_id:
            if "topic_id=" in href:
                m = re.search(r'topic_id=([a-f0-9\-]+)', href)
                if m:
                    cls_topic_id = m.group(1)

        text = elem.get("text", "").strip()
        if not text:
            continue

        # 检测是否是标签（以中文冒号结尾）
        if text.endswith("：") or text.endswith(":"):
            current_label = text.rstrip("：:").strip()
        elif current_label:
            fields[current_label] = text.strip()
            current_label = ""

    # 映射到统一格式
    service = fields.get("监控对象", "")
    title = service  # 监控对象 = 服务名，等价于现有群的 title

    lines = []
    # 把 tid 放入 lines 供 extract_fields 提取
    tid = fields.get("tid", "")
    if tid and tid != "N/A":
        lines.append(f"tid：{tid}")
    # 把 content 放入 lines（可能含异常栈）
    error_content = fields.get("content", "")
    if error_content:
        for line in error_content.split("\n"):
            stripped = line.strip()
            if stripped:
                lines.append(stripped)

    alert_time_str = fields.get("time", "")  # e.g. "2026-04-02 17:17:25.249"
    return {"title": title, "lines": lines, "cls_topic_id": cls_topic_id, "alert_time_str": alert_time_str}


def parse_post_content(body_content: str) -> dict:
    """
    解析飞书消息的 body.content（JSON 字符串）。
    支持两种格式:
      - post 格式: {"title": "...", "content": [[...]]}
      - interactive 格式: {"title": null, "elements": [[...]]}
    返回: {"title": str, "lines": [str], "cls_topic_id": str}
    """
    try:
        content = json.loads(body_content)
    except (json.JSONDecodeError, TypeError):
        return {"title": "", "lines": [], "cls_topic_id": ""}

    # interactive 格式（新群 CLS 告警）
    if "elements" in content:
        return _parse_interactive_content(content)

    # post 格式（现有群）
    # 兼容两种格式: 直接 {"title":..} 或 {"zh_cn": {"title":..}}
    post = content
    if "zh_cn" in post:
        post = post["zh_cn"]
    elif "en_us" in post:
        post = post["en_us"]

    title = post.get("title", "")
    lines = []
    cls_topic_id = ""

    for row in post.get("content", []):
        line_text = ""
        for elem in row:
            if isinstance(elem, dict):
                text = elem.get("text", elem.get("content", ""))
                line_text += text
                # 从 CLS 链接中提取 topic_id
                href = elem.get("href", "")
                if not cls_topic_id and "topic_id=" in href:
                    m = re.search(r'topic_id=([a-f0-9\-]+)', href)
                    if m:
                        cls_topic_id = m.group(1)
        if line_text.strip():
            lines.append(line_text.strip())

    return {"title": title, "lines": lines, "cls_topic_id": cls_topic_id}


def extract_fields(title: str, lines: list[str]) -> dict:
    """
    从 post 标题和内容行中提取告警字段:
    service, api_path, subcode, traceId
    """
    result = {
        "service": "",
        "api_path": "",
        "subcode": "",
        "traceId": "",
    }

    # --- 从标题提取服务名 ---
    # 常见格式: "os-main-inner-api 硅谷prod 错误告警"
    # 或者: "[os-main-inner-api] error alert"
    if title:
        # 尝试匹配已知服务名
        for svc in SERVICE_PATH_MAP:
            if svc in title:
                result["service"] = svc
                break
        # 如果没匹配到，取标题第一个空格前的部分
        if not result["service"]:
            parts = title.strip().split()
            if parts:
                candidate = parts[0].strip("[]")
                result["service"] = candidate

    # --- 从内容行提取字段 ---
    for line in lines:
        # 日志名格式（base-datahub 等）: "日志：embeddingsV3.fail"
        if not result["subcode"]:
            log_match = re.search(r'日志\s*[:：]\s*(\S+)', line)
            if log_match:
                result["subcode"] = log_match.group(1).strip()

        # traceId — 支持三种格式:
        #   traceIdList：["xxx", "yyy"]  |  traceId：xxx  |  tid：xxx（新群）
        if not result["traceId"]:
            # 先尝试 traceIdList 格式
            tid_list_match = re.search(r'traceIdList\s*[:：]\s*\[([^\]]+)\]', line, re.IGNORECASE)
            if tid_list_match:
                ids = re.findall(r'"([^"]+)"', tid_list_match.group(1))
                if ids:
                    result["traceId"] = ids[0].strip()
            else:
                # traceId：xxx 或 tid：xxx（新群格式）
                tid_match = re.search(r'(?:trace[_\-]?[iI][dD]|^tid)\s*[:：]\s*(\S+)', line)
                if tid_match:
                    val = tid_match.group(1).strip().strip('"')
                    if val and val != "N/A":
                        result["traceId"] = val

        # subcode — 支持: Subcode：xxx 或 subCodeList：["xxx"]
        if not result["subcode"]:
            # 先尝试 subCodeList 格式
            sc_list_match = re.search(r'subCodeList\s*[:：]\s*\[([^\]]+)\]', line, re.IGNORECASE)
            if sc_list_match:
                codes = re.findall(r'"([^"]+)"', sc_list_match.group(1))
                if codes:
                    result["subcode"] = codes[0].strip()
            else:
                for pattern in [
                    r'[Ss]ubcode\s*[:：]\s*(\S+)',
                    r'错误码\s*[:：]\s*(\S+)',
                    r'error[_\-]?code\s*[:：]\s*(\S+)',
                ]:
                    m = re.search(pattern, line)
                    if m:
                        result["subcode"] = m.group(1).strip()
                        break

        # api_path / 接口
        if not result["api_path"]:
            for pattern in [
                r'接口\s*[:：]\s*(\S+)',
                r'api[_\-]?path\s*[:：]\s*(\S+)',
                r'path\s*[:：]\s*(\/\S+)',
                r'(\/rest\/\S+)',
            ]:
                m = re.search(pattern, line, re.IGNORECASE)
                if m:
                    result["api_path"] = m.group(1).strip()
                    break

        # service（如果标题没提取到，从内容行补充）
        if not result["service"]:
            for pattern in [
                r'服务\s*[:：]\s*(\S+)',
                r'service\s*[:：]\s*(\S+)',
            ]:
                m = re.search(pattern, line, re.IGNORECASE)
                if m:
                    result["service"] = m.group(1).strip()
                    break

    # subcode fallback: 新群无 Subcode 字段，从异常栈提取异常类名
    if not result["subcode"]:
        for line in lines:
            # 匹配 Java 异常类名（如 NullPointerException, ClientAbortException）
            m = re.search(r'([A-Z][a-zA-Z]*(?:Exception|Error))', line)
            if m:
                result["subcode"] = m.group(1)
                break
        # 再 fallback: 用首行错误描述
        if not result["subcode"] and lines:
            first = lines[0] if not lines[0].startswith("tid") else (lines[1] if len(lines) > 1 else "")
            if first:
                result["subcode"] = first[:50].strip()

    return result


def _normalize_api_path(api_path: str) -> str:
    """
    将 api_path 归一化为可用于 fingerprint 的短字符串。
    - REST: "/rest/os/homepage/cover/to/video" → "os-homepage-cover-to-video"
    - Dubbo: "com.mindverse...DubboNoteService:1.0:searchNoteByChunk" → "searchNoteByChunk"
    - 空值: "" → ""
    """
    if not api_path:
        return ""
    path = api_path.strip()
    # Dubbo 风格（含 : 分隔符），取最后一段方法名
    if ":" in path and "." in path:
        parts = path.split(":")
        return parts[-1] if parts else ""
    # REST 风格，去掉 /rest/ 前缀，/ 替换为 -
    path = re.sub(r"^/rest/", "", path)
    path = path.strip("/")
    return path.replace("/", "-")


def _parse_msg_to_item(msg: dict) -> dict:
    """
    将原始飞书消息解析为 issue item dict。
    提取 service, subcode, traceId, api_path, fingerprint 等字段。
    """
    message_id = msg.get("message_id", "")
    create_time = msg.get("create_time", "")
    body_content = msg.get("body", {}).get("content", "")
    parsed = parse_post_content(body_content)
    fields = extract_fields(parsed["title"], parsed["lines"])

    service = fields["service"]
    subcode = fields["subcode"]
    trace_id = fields["traceId"]
    api_path = fields["api_path"]
    cls_topic_id = parsed.get("cls_topic_id", "")
    service_path = SERVICE_PATH_MAP.get(service, "")

    # fingerprint 包含 api_path，区分同 subcode 不同接口的不同问题
    api_norm = _normalize_api_path(api_path)
    if service and subcode:
        fingerprint = f"{service}_{api_norm}_{subcode}" if api_norm else f"{service}_{subcode}"
    else:
        content_hash = hashlib.md5(body_content.encode()).hexdigest()[:8]
        fingerprint = f"unknown_{service}_{content_hash}" if service else f"unknown_{content_hash}"

    return {
        "message_id": message_id,
        "service": service,
        "service_path": service_path,
        "api_path": api_path,
        "subcode": subcode,
        "traceId": trace_id,
        "alert_time": create_time,
        "fingerprint": fingerprint,
        "cls_topic_id": cls_topic_id,
    }


def check_has_bot_reply(client: LarkClient, message_id: str) -> bool:
    """
    检测消息是否已有话题回复。
    正确流程：GET message → 取 thread_id → 查 thread 内有无 app 回复。
    """
    try:
        # Step 1: 获取消息详情，检查 thread_id
        resp = client.get_message(message_id)
        if resp.get("code") != 0:
            return False
        items = resp.get("data", {}).get("items", [])
        if not items:
            return False
        thread_id = items[0].get("thread_id", "")
        if not thread_id:
            return False  # 没有话题 → 未处理

        # Step 2: 用 thread_id 查话题内消息
        thread_resp = client.list_messages(
            container_id_type="thread",
            container_id=thread_id,
            page_size=5,
            sort_type="ByCreateTimeAsc",
        )
        if thread_resp.get("code") != 0:
            return False
        for item in thread_resp.get("data", {}).get("items", []):
            if item.get("message_id") == message_id:
                continue
            if item.get("sender", {}).get("sender_type") == "app":
                return True
        return False
    except Exception as e:
        print(f"  话题检测异常 ({message_id}): {e}", file=sys.stderr)
        return False


def batch_check_bot_replies(client: LarkClient, bot_msgs: list[dict], max_workers: int = 10) -> tuple[list[dict], int]:
    """
    并发检测一批消息是否已有 bot 回复。
    返回: (unprocessed_msgs, already_processed_count)
    """
    unprocessed = []
    processed_count = 0

    def check_one(msg):
        mid = msg.get("message_id", "")
        has_reply = check_has_bot_reply(client, mid)
        return msg, has_reply

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(check_one, m): m for m in bot_msgs}
        for future in as_completed(futures):
            msg, has_reply = future.result()
            if has_reply:
                processed_count += 1
            else:
                unprocessed.append(msg)

    return unprocessed, processed_count


def main():
    parser = argparse.ArgumentParser(description="扫描飞书告警消息")
    parser.add_argument("--output", required=True, help="输出 scan-result.json 路径")
    parser.add_argument("--page-token", default=None, help="起始 page_token（断点续传）")
    parser.add_argument("--target-count", type=int, default=100, help="目标累积 unique issue 数")
    parser.add_argument("--max-pages", type=int, default=20, help="最大翻页数（防止无限翻页）")
    parser.add_argument("--start-time", type=int, default=0, help="最早消息时间戳（毫秒），早于此时间的消息停止扫描")
    args = parser.parse_args()

    client = LarkClient()
    page_token = args.page_token
    target_count = args.target_count
    max_pages = args.max_pages
    start_time = args.start_time

    # 收集结果（循环内实时 fingerprint 去重）
    seen_fingerprints: dict[str, dict] = {}  # fp → first item (with issue_id)
    duplicate_msgs = []
    already_processed_count = 0
    total_read = 0
    pages_read = 0
    last_page_token = page_token
    has_more = False
    errors = []

    print(f"开始扫描，目标累积 {target_count} 个 unique issue，最大 {max_pages} 页...", file=sys.stderr)

    while len(seen_fingerprints) < target_count and pages_read < max_pages:
        pages_read += 1
        print(f"  读取第 {pages_read}/{max_pages} 页 (已累积 {len(seen_fingerprints)} 个 unique issue)...", file=sys.stderr)

        try:
            resp = client.list_messages(
                container_id_type="chat",
                container_id=CHAT_ID,
                page_size=50,
                page_token=page_token,
                sort_type="ByCreateTimeDesc",
            )
        except Exception as e:
            errors.append(f"读取第 {pages_read} 页失败: {e}")
            print(f"  读取失败: {e}", file=sys.stderr)
            break

        if resp.get("code") != 0:
            errors.append(f"API 错误: code={resp.get('code')} msg={resp.get('msg')}")
            print(f"  API 错误: {resp.get('msg')}", file=sys.stderr)
            break

        data = resp.get("data", {})
        items = data.get("items", [])
        has_more = data.get("has_more", False)
        next_token = data.get("page_token", "")

        if not items:
            print(f"  第 {pages_read} 页无消息", file=sys.stderr)
            break

        total_read += len(items)

        # 过滤告警 bot 的 post 消息
        # sender 格式: {"id": "cli_xxx", "id_type": "app_id", "sender_type": "app"}
        bot_msgs = []
        for msg in items:
            sender = msg.get("sender", {})
            sender_id = sender.get("id", "")
            msg_type = msg.get("msg_type", "")
            if sender.get("sender_type") == "app" and msg_type == "post":
                bot_msgs.append(msg)

        # 时间截止检查（ByCreateTimeDesc: 从新到旧，遇到早于 start_time 的消息即停止）
        reached_cutoff = False
        if start_time:
            filtered_bot_msgs = []
            for m in bot_msgs:
                ct = int(m.get("create_time", "0"))
                if ct and ct < start_time:
                    reached_cutoff = True
                else:
                    filtered_bot_msgs.append(m)
            if len(filtered_bot_msgs) < len(bot_msgs):
                print(f"    时间截止: 过滤掉 {len(bot_msgs) - len(filtered_bot_msgs)} 条早于 start_time 的消息", file=sys.stderr)
            bot_msgs = filtered_bot_msgs

        if bot_msgs:
            # 并发检测话题回复（10 并发）
            page_unprocessed, page_processed = batch_check_bot_replies(client, bot_msgs, max_workers=10)
            already_processed_count += page_processed
            print(f"    本页 bot 消息: {len(bot_msgs)}, 未处理: {len(page_unprocessed)}, 已处理: {page_processed}", file=sys.stderr)

            # 实时解析 + fingerprint 去重
            for msg in page_unprocessed:
                item = _parse_msg_to_item(msg)
                fp = item["fingerprint"]

                if fp in seen_fingerprints:
                    duplicate_msgs.append({
                        "message_id": item["message_id"],
                        "fingerprint": fp,
                        "primary_issue_id": seen_fingerprints[fp]["issue_id"],
                        "subcode": item.get("subcode", ""),
                        "service": item.get("service", ""),
                        "api_path": item.get("api_path", ""),
                    })
                else:
                    issue_id = f"I{len(seen_fingerprints) + 1:03d}"
                    item["issue_id"] = issue_id
                    seen_fingerprints[fp] = item

            print(f"    unique issues: {len(seen_fingerprints)}, duplicates: {len(duplicate_msgs)}", file=sys.stderr)

            if page_processed > 0:
                print(f"  跳过 {page_processed} 条已处理消息（继续扫描）", file=sys.stderr)

        if reached_cutoff:
            print(f"  已到达时间截止，停止扫描", file=sys.stderr)
            break

        # 更新 page_token
        if has_more and next_token:
            last_page_token = next_token
            page_token = next_token
        else:
            has_more = False
            break

        # 已够数，停止
        if len(seen_fingerprints) >= target_count:
            break

    new_issues = list(seen_fingerprints.values())

    print(f"扫描完成: 读 {pages_read} 页, 共 {total_read} 条, "
          f"unique issues {len(new_issues)}, duplicates {len(duplicate_msgs)}, "
          f"已处理 {already_processed_count}", file=sys.stderr)

    # --- 输出 scan-result.json ---
    result = {
        "total_read": total_read,
        "already_processed": already_processed_count,
        "business_expected": [],  # 保留字段兼容，不再过滤
        "new_issues": new_issues,
        "duplicate_msgs": duplicate_msgs,
        "errors": errors,
        "has_more": has_more,
        "next_page_token": last_page_token or "",
        "pages_read": pages_read,
    }

    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    print(f"输出: {args.output}", file=sys.stderr)
    print(f"  new_issues={len(new_issues)}, duplicates={len(duplicate_msgs)}", file=sys.stderr)

    # 输出摘要到 stdout 供 Dispatcher 读取
    summary = {
        "new_issues_count": len(new_issues),
        "business_expected_count": 0,
        "duplicate_count": len(duplicate_msgs),
        "already_processed": already_processed_count,
        "has_more": has_more,
    }
    print(json.dumps(summary, ensure_ascii=False))


if __name__ == "__main__":
    main()

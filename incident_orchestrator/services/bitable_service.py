"""统一的 Bitable 读写服务

手动触发和自动执行共用同一套逻辑，严格规范每个字段的输入格式。

## 字段规范

| 字段               | 类型          | 格式要求                                          | 示例                                           |
|--------------------|---------------|---------------------------------------------------|------------------------------------------------|
| 任务名称           | Text          | "{subcode}: {修复描述}"                            | "unexpected.error: NPE null check"             |
| 状态               | SingleSelect  | 见 STATUS_* 常量                                   | "已完成"                                       |
| 完成时间           | DateTime(ms)  | 毫秒时间戳，修复完成时写入                          | 1775203498033                                  |
| PR                 | Text          | GitHub compare URL                                | "https://github.com/.../compare/master...fix/cc/..." |
| 负责人             | Text          | "Claude Bot" / "Claude Bot (手动)" / "Claude Bot (自动)" | "Claude Bot (自动)"                           |
| 分支               | Text          | fix/cc/YYYYMMDD/问题概述                           | "fix/cc/20260403/profile-npe"                  |
| 服务名             | Text          | K8s 服务名                                         | "os-main-inner-api"                            |
| tid                | Text          | traceId，无值写 "-"                                | "0ecb9feac4004344bf4d01325b401b63"              |
| issue_fingerprint  | Text          | service_api_subcode（去重主键）                     | "os-main_os-mind-profile_unexpected.error"     |
| root_cause_location| Text          | "FileName.java 根因描述"                            | "MindController.java NPE on null profile"      |
| error_detail       | Text          | 完整错误描述                                        | "simplePublicHomepage 为 null 时调用 getCover()"|
| error_type         | Text          | 异常类短名                                          | "NullPointerException"                         |
| error_location     | Text          | "File.java:行号"                                    | "MindController.java:548"                      |
| api_path           | Text          | 接口路径                                            | "/rest/os/mind/public/profile"                 |
| userId             | Text          | 触发用户 ID，无值写 "-"                              | "197920"                                       |
| stack_trace        | Text          | 堆栈前3行                                           | "at com.mindverse.os..."                       |
| 优先级             | SingleSelect  | "高" / "中" / "低"                                  | "高"                                           |
| alert_time         | DateTime(ms)  | 告警时间毫秒时间戳                                   | 1775203498033                                  |
| cls_log_count      | Number        | CLS 查到的日志条数                                   | 15                                             |

## 状态定义

| 状态值         | 含义                                     | 触发场景                       |
|----------------|------------------------------------------|--------------------------------|
| 待处理         | 已识别，尚未开始分析                       | 告警入队                        |
| 进行中         | Claude 正在分析/修复                       | session 创建后                  |
| 已完成         | 修复代码已推送，有 PR                      | git push 成功                   |
| 已诊断         | 已分析根因但未自动修复（需人工）            | 无法自动修复的场景              |
| 已诊断-基础设施 | 根因是基础设施问题（线程池/连接池/OOM 等）  | triage = infrastructure         |
| 已取消         | 人工判断不需要修复                         | 用户主动取消                    |
| 已废弃         | 过期或重复                                 | 超过 30 天未处理                |
| 已跳过         | 自动分类为非代码问题                       | triage = business_expected 等   |
| 重复告警       | 与已有修复 fingerprint 相同                | bitable 去重命中                |
| 待删除         | 标记删除                                   | 清理用                          |
"""
import time

from incident_orchestrator.log import get_logger
from incident_orchestrator.config import get_settings

logger = get_logger("BITABLE")
from incident_orchestrator.feishu.client import get_feishu_client

# ── 状态常量（和飞书回复标题一致） ──
STATUS_PENDING_MERGE = "⏳等待合并"       # 有 PR，等审核合并
STATUS_MERGED = "✅已合并"                # PR 已合并到 release/stable，后续告警不再回复
STATUS_BUSINESS_EXPECTED = "ℹ️业务预期"   # 不是 bug，是正常业务行为
STATUS_NO_TRACE = "⚠️无法追踪"           # CLS 没找到日志
STATUS_UNKNOWN = "❓无法判断"             # 找到日志但无法确定根因

# 终态：命中去重且状态在此集合中时，scheduled_scan 静默跳过，不再回复累计告警次数
TERMINAL_STATUSES = frozenset({STATUS_MERGED})

# triage category → bitable 状态映射
TRIAGE_STATUS_MAP = {
    "business_expected": STATUS_BUSINESS_EXPECTED,
    "external_dependency": STATUS_BUSINESS_EXPECTED,
    "transient": STATUS_BUSINESS_EXPECTED,
    "attack": STATUS_BUSINESS_EXPECTED,
    "infrastructure": STATUS_UNKNOWN,
    "unfixable": STATUS_UNKNOWN,
    "unfixable_learned": STATUS_UNKNOWN,
}


async def query_existing_fingerprints(fingerprints: list[str]) -> dict[str, dict]:
    """查询 bitable 中已存在的 fingerprint 记录

    返回: {fingerprint: {status, 任务名称, PR, ...}}
    """
    if not fingerprints:
        return {}

    settings = get_settings()
    if not settings.bitable_app_token or not settings.bitable_table_id:
        return {}

    feishu = get_feishu_client()
    result = {}

    try:
        http = await feishu._ensure_http()
        headers = await feishu._headers()

        # 分批查询（每批最多 20 个 fingerprint）
        for i in range(0, len(fingerprints), 20):
            batch = fingerprints[i:i+20]
            body = {
                "filter": {
                    "conjunction": "or",
                    "conditions": [
                        {
                            "field_name": "issue_fingerprint",
                            "operator": "is",
                            "value": [fp],
                        }
                        for fp in batch
                    ],
                },
                "page_size": 100,
            }

            resp = await http.post(
                f"{feishu.base_url}/open-apis/bitable/v1/apps/{settings.bitable_app_token}"
                f"/tables/{settings.bitable_table_id}/records/search",
                headers=headers,
                json=body,
            )
            data = resp.json()
            if data.get("code") != 0:
                logger.warning("查询失败: %s", data.get("msg"))
                continue

            for record in data.get("data", {}).get("items", []):
                fields = record.get("fields", {})
                raw_fp = fields.get("issue_fingerprint")
                # bitable Text 字段返回 [{"text": "xxx"}] 格式
                if isinstance(raw_fp, list) and raw_fp:
                    fp = raw_fp[0].get("text", "") if isinstance(raw_fp[0], dict) else str(raw_fp[0])
                elif isinstance(raw_fp, str):
                    fp = raw_fp
                else:
                    continue
                if fp:
                    # 告警次数字段
                    raw_count = fields.get("告警次数", 0)
                    count = int(raw_count) if isinstance(raw_count, (int, float)) else 0

                    result[fp] = {
                        "record_id": record.get("record_id", ""),
                        "status": fields.get("状态"),
                        "任务名称": fields.get("任务名称"),
                        "PR": fields.get("PR"),
                        "分支": fields.get("分支"),
                        "根本原因": fields.get("根本原因"),
                        "告警次数": count,
                        "message_id": fields.get("message_id"),
                    }
    except Exception as e:
        logger.warning("查询异常: %s", e)

    return result



async def find_session_by_message(message_id: str) -> str | None:
    """通过 message_id（话题根消息）查找 claude_session_id"""
    if not message_id:
        return None

    settings = get_settings()
    if not settings.bitable_app_token or not settings.bitable_table_id:
        return None

    feishu = get_feishu_client()
    try:
        http = await feishu._ensure_http()
        headers = await feishu._headers()

        body = {
            "filter": {
                "conjunction": "or",
                "conditions": [
                    {"field_name": "message_id", "operator": "is", "value": [message_id]},
                ],
            },
            "page_size": 1,
        }

        resp = await http.post(
            f"{feishu.base_url}/open-apis/bitable/v1/apps/{settings.bitable_app_token}"
            f"/tables/{settings.bitable_table_id}/records/search",
            headers=headers,
            json=body,
        )
        data = resp.json()
        if data.get("code") != 0:
            return None

        items = data.get("data", {}).get("items", [])
        if not items:
            return None

        fields = items[0].get("fields", {})
        raw = fields.get("claude_session_id")
        if isinstance(raw, list) and raw:
            return raw[0].get("text", "") if isinstance(raw[0], dict) else str(raw[0])
        return str(raw) if raw else None
    except Exception as e:
        logger.warning("查 session by message 异常: %s", e)
        return None


async def find_session_by_fingerprint(fingerprint: str) -> str | None:
    """通过 fingerprint 查找 claude_session_id（同类问题共享 session）"""
    if not fingerprint:
        return None

    settings = get_settings()
    if not settings.bitable_app_token or not settings.bitable_table_id:
        return None

    feishu = get_feishu_client()
    try:
        http = await feishu._ensure_http()
        headers = await feishu._headers()

        body = {
            "filter": {
                "conjunction": "or",
                "conditions": [
                    {"field_name": "issue_fingerprint", "operator": "is", "value": [fingerprint]},
                ],
            },
            "page_size": 1,
        }

        resp = await http.post(
            f"{feishu.base_url}/open-apis/bitable/v1/apps/{settings.bitable_app_token}"
            f"/tables/{settings.bitable_table_id}/records/search",
            headers=headers,
            json=body,
        )
        data = resp.json()
        if data.get("code") != 0:
            return None

        items = data.get("data", {}).get("items", [])
        if not items:
            return None

        fields = items[0].get("fields", {})
        raw = fields.get("claude_session_id")
        if isinstance(raw, list) and raw:
            return raw[0].get("text", "") if isinstance(raw[0], dict) else str(raw[0])
        return str(raw) if raw else None
    except Exception as e:
        logger.warning("查 session 异常: %s", e)
        return None



async def mark_as_merged(record_id: str) -> bool:
    """把一条记录的状态改为 ✅已合并（scheduled_scan 懒检查命中后调用）"""
    settings = get_settings()
    if not record_id or not settings.bitable_app_token:
        return False

    feishu = get_feishu_client()
    try:
        http = await feishu._ensure_http()
        resp = await http.put(
            f"{feishu.base_url}/open-apis/bitable/v1/apps/{settings.bitable_app_token}"
            f"/tables/{settings.bitable_table_id}/records/{record_id}",
            headers=await feishu._headers(),
            json={"fields": {"状态": STATUS_MERGED}},
        )
        data = resp.json()
        if data.get("code") != 0:
            logger.warning("mark_as_merged 失败: %s", data.get("msg"))
            return False
        return True
    except Exception as e:
        logger.warning("mark_as_merged 异常: %s", e)
        return False


async def update_alert_count(record_id: str, new_count: int) -> bool:
    """更新已有记录的告警次数"""
    settings = get_settings()
    if not record_id or not settings.bitable_app_token:
        return False

    feishu = get_feishu_client()
    try:
        http = await feishu._ensure_http()
        resp = await http.put(
            f"{feishu.base_url}/open-apis/bitable/v1/apps/{settings.bitable_app_token}"
            f"/tables/{settings.bitable_table_id}/records/{record_id}",
            headers=await feishu._headers(),
            json={"fields": {"告警次数": new_count}},
        )
        data = resp.json()
        if data.get("code") != 0:
            logger.warning("更新告警次数失败: %s", data.get("msg"))
            return False
        return True
    except Exception as e:
        logger.warning("更新告警次数异常: %s", e)
        return False


async def write_record(
    *,
    # 必填
    fingerprint: str,
    service: str,
    subcode: str,
    status: str,
    # 修复相关
    task_name: str = "",
    pr_url: str = "",
    branch: str = "",
    root_cause: str = "",  # 「根本原因」列：存完整话题回复 post JSON（去重命中时原样发回飞书，0 token）
    root_cause_location: str = "",  # 根因代码位置，格式 "Foo.java:123"，给人看
    error_type: str = "",
    # 告警信息
    tid: str = "",
    owner: str = "",  # 负责人（git blame 的作者）
    alert_count: int = 0,  # 本次扫描命中次数
    message_id: str = "",  # 第一次回复的消息 ID
    claude_session_id: str = "",  # Claude session ID（用于 resume）
    priority: str = "中",
) -> dict | None:
    """写入一条 bitable 记录

    返回飞书 API 响应，失败返回 None
    """
    settings = get_settings()
    if not settings.bitable_app_token or not settings.bitable_table_id:
        logger.warning("未配置 APP_TOKEN/TABLE_ID，跳过写入")
        return None

    # 构建字段
    fields = {
        "任务名称": task_name or f"{subcode}: {service}",
        "状态": status,
        "服务名": service,
        "issue_fingerprint": fingerprint,
        "tid": tid or "-",
        "负责人": owner or "-",
        "优先级": priority,
    }

    # PR 字段（URL 类型）
    if pr_url:
        fields["PR"] = {"text": "查看 PR", "link": pr_url}
    if branch:
        fields["分支"] = branch
    if root_cause:
        fields["根本原因"] = root_cause
    if root_cause_location:
        fields["root_cause_location"] = root_cause_location
    if error_type:
        fields["error_type"] = error_type
    if alert_count > 0:
        fields["告警次数"] = alert_count
    if message_id:
        fields["message_id"] = message_id
    if claude_session_id:
        fields["claude_session_id"] = claude_session_id

    if status in (STATUS_PENDING_MERGE, STATUS_BUSINESS_EXPECTED, STATUS_NO_TRACE, STATUS_UNKNOWN):
        fields["完成时间"] = int(time.time() * 1000)

    feishu = get_feishu_client()
    try:
        http = await feishu._ensure_http()
        resp = await http.post(
            f"{feishu.base_url}/open-apis/bitable/v1/apps/{settings.bitable_app_token}"
            f"/tables/{settings.bitable_table_id}/records",
            headers=await feishu._headers(),
            json={"fields": fields},
        )
        data = resp.json()
        if data.get("code") != 0:
            logger.warning("写入失败: %s", data.get("msg"))
            return None
        logger.info("写入成功: %s → %s", fingerprint, status)
        return data
    except Exception as e:
        logger.warning("写入异常: %s", e)
        return None



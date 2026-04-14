"""配置常量 + 凭据（所有脚本共享）

所有配置从环境变量读取（.env 为唯一真相源），保留默认值作为 fallback。
"""
import os

# --- 飞书凭据 ---
LARK_APP_ID = os.environ.get("LARK_APP_ID", "")
LARK_APP_SECRET = os.environ.get("LARK_APP_SECRET", "")

# --- 腾讯云 CLS 凭据 ---
TENCENTCLOUD_SECRET_ID = os.environ.get("TENCENTCLOUD_SECRET_ID", "")
TENCENTCLOUD_SECRET_KEY = os.environ.get("TENCENTCLOUD_SECRET_KEY", "")

# --- 飞书告警群配置 ---
CHAT_ID = os.environ.get("LARK_CHAT_ID", "")
CHAT_ID_CLS = os.environ.get("LARK_CHAT_ID_CLS", "")
BOT_ID = os.environ.get("LARK_BOT_ID", "")

# --- 飞书多维表格 ---
BITABLE_APP_TOKEN = os.environ.get("BITABLE_APP_TOKEN", "")
BITABLE_TABLE_ID = os.environ.get("BITABLE_TABLE_ID", "")

# --- GitHub ---
GITHUB_REPO_URL = os.environ.get("GITHUB_REPO_URL", "")

# --- CLS 配置 ---
CLS_REGION = os.environ.get("CLS_REGION", "na-siliconvalley")

# --- 飞书 API ---
LARK_BASE_URL = os.environ.get("LARK_BASE_URL", "https://open.feishu.cn")

# --- 路径配置 ---
SCRIPTS_DIR = os.path.expanduser(os.environ.get("SCRIPTS_DIR", "~/bug-fix-scripts"))
MONOREPO_DIR = os.environ.get("MONOREPO_DIR", "/mnt/code/secondme")
BUGFIX_WORK_DIR = os.environ.get("BUGFIX_WORK_DIR", "/tmp/bugfix")
BUGFIX_CACHE_DIR = os.path.expanduser(os.environ.get("BUGFIX_CACHE_DIR", "~/.cache/bugfix"))

# --- 派生路径 ---
CLS_TOPIC_CACHE_FILE = os.path.join(BUGFIX_CACHE_DIR, "cls-topics.json")
LEARNED_RULES_PATH = os.path.join(BUGFIX_CACHE_DIR, "learned-rules.json")

# CLS Topic ID 映射表（topic_name -> topic_id）
# 从环境变量 CLS_TOPIC_ID_MAP_JSON 读取（JSON 格式），无则空 dict
import json as _json
CLS_TOPIC_ID_MAP: dict[str, str] = _json.loads(
    os.environ.get("CLS_TOPIC_ID_MAP_JSON", "{}")
)

# CLS Topic ID 本地缓存路径（使用派生路径 CLS_TOPIC_CACHE_FILE，见上方）

# K8s 服务名 → CLS 日志 Topic 名称
SERVICE_TO_CLS_TOPIC_NAME: dict[str, str] = {
    "os-main-inner-api": "os-main-inner-prod",
    "os-main-inner-prod": "os-main-inner-prod",
    "os-main-out-prod": "os-main-inner-prod",
    "os-main-runner-prod": "os-main-inner-prod",
    "os-ws-api-prodk8sNew": "os-ws-api-prod",
    "os-ws-websocket-prodk8sNew": "os-ws-api-prod",
    "os-ws-runner-prodk8sNew": "os-ws-api-prod",
    "base-datahub-prod": "base-datahub-prod",
    "base-datahub-api": "base-datahub-prod",
    "os-user-prodk8s": "os-user-prod",
}

# --- 服务路径映射（K8s 服务名 → monorepo 路径）---
# 整个 secondme monorepo（kernel/ + biz/）都是 Worker 的代码搜索范围。
# 若服务名不在此 map 中，Worker 应先 Glob 搜索确认是否在 monorepo 内，再决定是否为真正外部依赖。
SERVICE_PATH_MAP: dict[str, str] = {
    # kernel 服务
    "os-main-inner-api": "kernel/os-main/",
    "os-main-inner-prod": "kernel/os-main/",
    "os-main-out-prod": "kernel/os-main/",
    "os-main-runner-prod": "kernel/os-main/",
    "os-ws-api-prodk8sNew": "kernel/os-ws/",
    "os-ws-websocket-prodk8sNew": "kernel/os-ws/",
    "os-ws-runner-prodk8sNew": "kernel/os-ws/",
    "base-datahub-prod": "kernel/base-datahub/",
    "base-datahub-api": "kernel/base-datahub/",
    "mind-kernel": "kernel/mind-kernel/",       # Python 服务，在 monorepo 内
    # biz 服务
    "os-user-prodk8s": "biz/os-user/",
    "god_of_life_memory": "biz/god_of_life_memory/",
}

# --- 测试账号 userId 黑名单（CLS 查出的 userId 匹配则跳过）---
FILTERED_USER_IDS: set[str] = {
    "197920",  # 内部测试账号，31万条 smart_topic_doc_relation 导致 Dubbo Payload 超限
}

# --- 业务预期错误码（自动跳过，非代码 bug）---
BUSINESS_EXPECTED_CODES: set[str] = {
    "not.login",
    "call.template.deleted",
    "circle.dissolved",
    "token.expired",
    "token.invalid",
    "param.invalid",
    "permission.denied",
    "resource.not.found",
    "rate.limit.exceeded",
}

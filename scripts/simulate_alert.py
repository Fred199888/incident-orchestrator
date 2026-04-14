"""模拟告警 webhook（开发测试用）"""
import json
import sys

import requests

BASE_URL = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:8900"

payload = {
    "alert_id": "ALT-20260403-001",
    "service": "os-main-inner-api",
    "env": "prod-sv",
    "severity": "P2",
    "summary": "NullPointerException at MindController.java:548",
    "subcode": "unexpected.error",
    "api_path": "/rest/os/mind/public/profile",
    "trace_id": "abc123def456",
    "logset_id": "your-cls-topic-id",
    "alert_time": 1743580800000,
    "feishu_chat_id": "your-feishu-chat-id",
    "feishu_message_id": "om_test_001",
}

resp = requests.post(f"{BASE_URL}/api/v1/alerts", json=payload, timeout=10)
print(f"Status: {resp.status_code}")
print(json.dumps(resp.json(), indent=2, ensure_ascii=False))

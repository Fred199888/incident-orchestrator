# 🤖 Incident Orchestrator

**Alert-driven auto-fix service powered by Claude Code Agent**

[English](#english) | [中文](#中文)

---

## English

### What is this?

A self-hosted service that **automatically fixes production bugs** end-to-end:

```
Feishu Alert → Parse & Dedup → Query CLS Logs → Claude Code Agent Diagnoses & Fixes
    → Compile & Verify → Push Fix Branch → Create PR → Reply in Feishu Thread → Record in Bitable
```

No human intervention needed. Your team just reviews PRs on the dashboard.

### Key Features

| Feature | Description |
|---------|-------------|
| **Scheduled Scan** | Polls alert group every 20 min, processes top alerts by frequency |
| **@Bot Interaction** | Team members can @bot in any alert thread for follow-up analysis |
| **Smart Dedup** | Fingerprint-based dedup compresses 4800+ raw alerts → 48 actionable tasks (100:1) |
| **Parallel Fix** | Each issue gets an isolated git worktree, multiple fixes run concurrently |
| **Auto Sweep** | Detects merged PRs via local git and updates bitable status automatically |
| **Lazy Merge Check** | When a known issue re-alerts, checks if its PR was already merged before replying |
| **Audit Trail** | Every fix is recorded in Feishu Bitable with root cause, PR link, owner, and alert count |

### Architecture

```
┌──────────────────────────────────────────────────────┐
│                  Incident Orchestrator                │
│                   (FastAPI + asyncio)                 │
├──────────────┬───────────────────┬───────────────────┤
│  WebSocket   │  Scheduled Scan   │   HTTP API        │
│  Listener    │  (every 20 min)   │   /health /scan   │
│  (@bot msgs) │                   │                   │
├──────────────┴───────────────────┴───────────────────┤
│                    Core Services                      │
│  ┌─────────────┐ ┌──────────────┐ ┌────────────────┐ │
│  │ Fingerprint │ │  Bitable     │ │ Git Merge      │ │
│  │ Extraction  │ │  Service     │ │ Check          │ │
│  └─────────────┘ └──────────────┘ └────────────────┘ │
│  ┌─────────────┐ ┌──────────────┐ ┌────────────────┐ │
│  │ Fix         │ │ Claude       │ │ Fix            │ │
│  │ Preprocessor│ │ Runner       │ │ Postprocessor  │ │
│  │ (CLS+Parse) │ │ (CLI subprocess)│ │ (Compile+Push)│ │
│  └─────────────┘ └──────────────┘ └────────────────┘ │
├──────────────────────────────────────────────────────┤
│               External Dependencies                   │
│  Feishu API · Tencent CLS · GitHub · Claude Code CLI │
└──────────────────────────────────────────────────────┘
```

### Quick Start

#### Prerequisites

- Python 3.11+
- [Claude Code CLI](https://docs.anthropic.com/en/docs/claude-code) installed and authenticated
- A Feishu (Lark) bot application with WebSocket enabled
- Tencent Cloud CLS access (for log queries)
- A Java monorepo with Maven (the codebase being fixed)

#### 1. Clone & Install

```bash
git clone https://github.com/Fred199888/incident-orchestrator.git
cd incident-orchestrator
pip install -r requirements.txt
```

#### 2. Configure

```bash
cp .env.example .env
```

Edit `.env` with your credentials:

```ini
# Feishu Bot
LARK_APP_ID=your_app_id
LARK_APP_SECRET=your_app_secret
LARK_CHAT_ID=your_alert_group_chat_id
LARK_BOT_ID=your_bot_id

# Tencent Cloud CLS
TENCENTCLOUD_SECRET_ID=your_secret_id
TENCENTCLOUD_SECRET_KEY=your_secret_key
CLS_REGION=your-region

# GitHub
GITHUB_REPO_URL=https://github.com/your-org/your-repo
GH_TOKEN=your_github_pat

# Your Java Monorepo
MONOREPO_DIR=/path/to/your/monorepo

# Feishu Bitable (task dashboard)
BITABLE_APP_TOKEN=your_bitable_app_token
BITABLE_TABLE_ID=your_bitable_table_id
```

#### 3. Set Up Bitable

Create a Feishu Bitable table with these fields:

| Field Name | Type | Description |
|------------|------|-------------|
| 任务名称 | Text | Task title (auto-generated) |
| 状态 | SingleSelect | `⏳等待合并` / `✅已合并` / `ℹ️业务预期` / `❓无法判断` |
| 完成时间 | DateTime | When the fix was completed |
| PR | URL | GitHub PR link |
| 负责人 | Text | Code owner (from git blame) |
| 分支 | Text | Fix branch name |
| 服务名 | Text | K8s service name |
| tid | Text | Trace ID |
| issue_fingerprint | Text | Dedup key |
| 根本原因 | Text | Cached reply payload |
| root_cause_location | Text | e.g. `Foo.java:123` |
| error_type | Text | e.g. `NullPointerException` |
| 优先级 | SingleSelect | 高 / 中 / 低 |
| 告警次数 | Number | Cumulative alert count |
| message_id | Text | Feishu message ID (for thread replies) |
| claude_session_id | Text | Claude session ID (for follow-ups) |

#### 4. Run

```bash
python run.py
```

The service will:
1. Start a FastAPI server on port 8900
2. Connect to Feishu WebSocket (for @bot messages)
3. Begin scheduled scanning every 20 minutes

#### 5. Verify

```bash
# Health check
curl http://localhost:8900/health

# Manually trigger a scan
curl -X POST http://localhost:8900/api/v1/scan/sync
```

### How It Works

#### Scan Cycle (every 20 minutes)

1. **Sweep** — Check all `⏳等待合并` records; if the fix branch is merged into `release/stable`, auto-upgrade to `✅已合并`
2. **Fetch** — Pull latest 100 messages from alert group
3. **Parse** — Extract service name, trace ID, error content from each alert card
4. **Fingerprint** — Generate dedup key (class name + line number, or content-based fallback)
5. **Frequency Filter** — Only process fingerprints with 10+ occurrences
6. **Bitable Dedup** — Skip already-known issues (update alert count); for `⏳等待合并`, check if PR was merged
7. **Parallel Fix** — For each new issue:
   - Query CLS logs (by trace ID or keyword)
   - Create isolated git worktree
   - Launch Claude Code Agent to diagnose & fix
   - Compile with Maven
   - Push fix branch to GitHub
   - Reply in Feishu alert thread
   - Write record to Bitable

#### @Bot Interaction

When someone @mentions the bot in an alert thread:
- If the issue has a previous Claude session → **resume** that session with the new message
- If the issue fingerprint matches a known fix → **replay** the cached fix reply (0 tokens)
- Otherwise → **create** a new Claude session for diagnosis

### Project Structure

```
incident_orchestrator/
├── app.py                    # FastAPI app + lifespan (WS + scheduler)
├── config.py                 # Pydantic Settings (reads .env)
├── log.py                    # Unified logging (file + stdout, session-aware)
├── api/                      # HTTP endpoints
│   ├── health.py             # GET /health
│   ├── scan.py               # POST /api/v1/scan, /api/v1/scan/sync
│   ├── alerts.py             # POST /api/v1/alerts (webhook)
│   └── feishu_events.py      # Feishu event callback
├── feishu/                   # Feishu integration
│   ├── client.py             # HTTP client (tenant_access_token)
│   ├── ws_listener.py        # WebSocket long-connection listener
│   ├── crypto.py             # Event signature verification
│   └── event_parser.py       # Event payload parsing
├── services/                 # Core business logic
│   ├── scheduled_scan.py     # Timer loop + scan_and_process + sweep
│   ├── message_handler.py    # @bot message handling + Claude prompt
│   ├── fix_preprocessor.py   # Alert parsing → CLS query → worktree
│   ├── fix_postprocessor.py  # Compile → commit → push
│   ├── claude_runner.py      # Claude CLI subprocess management
│   ├── bitable_service.py    # Bitable CRUD + dedup + status constants
│   ├── fingerprint.py        # Issue fingerprint extraction
│   ├── git_merge_check.py    # Local git merge detection
│   ├── reply_template.py     # Feishu post reply builder
│   └── alert_parser.py       # Alert field normalization
├── models/                   # Data models
└── db/                       # SQLite (incident history)
scripts/
├── sync_merged_prs.py        # Batch check & mark merged PRs
├── cleanup_bad_fingerprints.py
├── rewrite_bad_fingerprints.py
└── simulate_alert.py         # Dev testing
legacy_scripts/               # Standalone scripts (pre-service era)
```

### Configuration Reference

All configuration is via environment variables (`.env` file). See [`.env.example`](.env.example) for the full list.

| Variable | Required | Description |
|----------|----------|-------------|
| `LARK_APP_ID` | Yes | Feishu bot app ID |
| `LARK_APP_SECRET` | Yes | Feishu bot app secret |
| `LARK_CHAT_ID` | Yes | Alert group chat ID |
| `TENCENTCLOUD_SECRET_ID` | Yes | Tencent Cloud access key |
| `TENCENTCLOUD_SECRET_KEY` | Yes | Tencent Cloud secret key |
| `CLS_REGION` | Yes | CLS region (e.g. `na-siliconvalley`) |
| `GITHUB_REPO_URL` | Yes | Target repo URL |
| `MONOREPO_DIR` | Yes | Local path to the Java monorepo |
| `BITABLE_APP_TOKEN` | Yes | Feishu Bitable app token |
| `BITABLE_TABLE_ID` | Yes | Feishu Bitable table ID |
| `MAX_CONCURRENT_RUNS` | No | Max parallel Claude sessions (default: 5) |
| `HOST` | No | Server host (default: `0.0.0.0`) |
| `PORT` | No | Server port (default: `8900`) |

### Useful Scripts

```bash
# Check which PRs have been merged (dry-run)
python scripts/sync_merged_prs.py

# Actually mark merged PRs in bitable
python scripts/sync_merged_prs.py --apply

# Clean up duplicate fingerprint records (dry-run)
python scripts/cleanup_bad_fingerprints.py

# Recalculate fingerprints for legacy records
python scripts/rewrite_bad_fingerprints.py
```

### License

MIT

---

## 中文

### 这是什么？

一个自托管的服务，**端到端自动修复线上 bug**：

```
飞书告警 → 解析去重 → 查腾讯云 CLS 日志 → Claude Code Agent 诊断并修复
    → 编译验证 → 推送修复分支 → 创建 PR → 飞书话题回复 → 多维表格记录
```

无需人工介入。团队只需要在看板上审核 PR。

### 核心能力

| 功能 | 说明 |
|------|------|
| **定时扫描** | 每 20 分钟拉取告警群最新消息，按频次处理高频告警 |
| **@Bot 追问** | 团队成员在任意告警话题下 @bot，基于同一上下文追问 |
| **智能去重** | 基于指纹算法，实测 4800+ 次原始告警压缩为 48 条任务（100:1） |
| **并行修复** | 每个问题分配独立 git worktree，多个修复并发执行 |
| **自动 Sweep** | 通过本地 git 检测已合并的 PR，自动更新多维表格状态 |
| **懒检查** | 已知问题再次告警时，先查 PR 是否已合并再决定是否回复 |
| **审计看板** | 每条修复记录含根因、PR 链接、负责人、告警次数，可追溯 |

### 快速开始

#### 环境要求

- Python 3.11+
- [Claude Code CLI](https://docs.anthropic.com/en/docs/claude-code) 已安装并认证
- 飞书机器人应用（需开启 WebSocket）
- 腾讯云 CLS 日志服务访问权限
- 一个 Java Maven 单仓（被修复的代码库）

#### 1. 克隆安装

```bash
git clone https://github.com/Fred199888/incident-orchestrator.git
cd incident-orchestrator
pip install -r requirements.txt
```

#### 2. 配置

```bash
cp .env.example .env
```

编辑 `.env` 填入你的凭据（参考上方英文配置表）。

#### 3. 创建多维表格

在飞书多维表格中创建任务看板，字段参考上方英文"Set Up Bitable"部分。

#### 4. 启动

```bash
python run.py
```

服务启动后会：
1. 在 8900 端口启动 FastAPI
2. 连接飞书 WebSocket（接收 @bot 消息）
3. 每 20 分钟自动扫描一次告警群

#### 5. 验证

```bash
# 健康检查
curl http://localhost:8900/health

# 手动触发一次扫描
curl -X POST http://localhost:8900/api/v1/scan/sync
```

### 工作流程

#### 扫描周期（每 20 分钟）

1. **Sweep** — 全表扫描 `⏳等待合并` 记录，若 fix 分支已合入主干则自动升级为 `✅已合并`
2. **拉消息** — 从告警群拉取最近 100 条消息
3. **解析** — 从告警卡片中提取服务名、trace ID、错误内容
4. **指纹** — 生成去重 key（类名+行号，或内容兜底）
5. **频次过滤** — 只处理出现 10 次以上的指纹
6. **去重** — 跳过已知问题（累加告警次数）；`⏳等待合并` 的顺手检查 PR 是否已合并
7. **并行修复** — 对每个新问题：
   - 查 CLS 日志（按 trace ID 或关键词）
   - 创建隔离的 git worktree
   - 启动 Claude Code Agent 诊断并修复
   - Maven 编译验证
   - 推送 fix 分支到 GitHub
   - 在飞书告警话题下回复
   - 写入多维表格记录

#### @Bot 追问

在告警话题下 @机器人 时：
- 若该问题有历史 Claude session → **续接**上下文回复
- 若指纹命中已有修复记录 → **直接复用**缓存回复（0 token）
- 否则 → **新建** Claude session 进行诊断

### 常用脚本

```bash
# 查看哪些 PR 已合并（dry-run）
python scripts/sync_merged_prs.py

# 实际标记已合并的 PR
python scripts/sync_merged_prs.py --apply

# 清理重复指纹记录（dry-run）
python scripts/cleanup_bad_fingerprints.py

# 重算历史指纹
python scripts/rewrite_bad_fingerprints.py
```

### 许可证

MIT

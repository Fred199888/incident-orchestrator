# Incident Orchestrator

告警驱动的自动修复服务。飞书告警 → CLS 日志 → Claude 修复 → PR 推送 → 飞书回复 → bitable 记录。

## 项目结构

```
bug-fix/
├── incident_orchestrator/       # 服务代码
│   ├── app.py                   # FastAPI + WebSocket + 定时扫描
│   ├── config.py                # Settings（读 .env）
│   ├── log.py                   # 统一日志
│   ├── api/                     # HTTP 端点
│   ├── feishu/                  # 飞书 client + WebSocket
│   ├── services/
│   │   ├── message_handler.py   # 手动 @bot 处理
│   │   ├── scheduled_scan.py    # 自动定时扫描（每 20 分钟）
│   │   ├── fix_preprocessor.py  # 预处理（解析 → CLS → worktree → 映射）
│   │   ├── fix_postprocessor.py # 后处理（编译 → 提交 → 推送）
│   │   ├── bitable_service.py   # 多维表格读写 + 去重
│   │   ├── reply_template.py    # 飞书回复格式（改这里全局生效）
│   │   └── claude_runner.py     # Claude CLI 子进程管理
│   └── models/                  # ORM
├── legacy_scripts/              # CLS 查询、scanner 等复用脚本
├── logs/                        # 日志文件
├── data/                        # SQLite
├── .env                         # 凭据配置（不提交）
└── run.py                       # 启动入口
```

## 两种模式

### 手动：用户 @bot
WebSocket 收到 @bot → 从告警提取 fingerprint → bitable 去重 → 命中则回复存储内容 → 未命中则预处理 → Claude 修复 → 后处理 → 回复 + bitable 写入

### 自动：定时扫描
每 20 分钟拉 100 条消息 → 按 fingerprint 分组 → >10 条才处理 → bitable 去重（命中则更新次数 + 追加回复）→ 新问题并行修复（最多 10 并发）

## 去重

主键：`issue_fingerprint` = `服务名.com.mindverse.xxx.ClassName.java:行号`
查 bitable 精确匹配。同 fingerprint 的问题共享同一个 Claude session。

## 关键配置

- 回复格式：`services/reply_template.py`
- bitable 字段：`services/bitable_service.py` 的 `write_record()`
- @人列表：`config.py` 的 `get_mention_ids()`
- 修复 prompt：`services/message_handler.py` 的 `_build_fix_prompt_preprocessed()`
- CLS 排除条件：`services/fix_preprocessor.py` 的 `CLS_EXCLUSION_FILTER`
- 扫描间隔/阈值：`services/scheduled_scan.py` 的 `SCAN_INTERVAL` / `FREQUENCY_THRESHOLD`

## 启动

```bash
python run.py
```

## 外部依赖

- Monorepo：通过 `.env` 的 `MONOREPO_DIR` 配置（worktree 在 `.claude/worktrees/` 下）
- 飞书应用 / 告警群 / Bitable：通过 `.env` 配置（参考 `.env.example`）

## 约束

- 禁止 git push --force、git reset --hard
- .env 不提交（含密钥）
- 修复必须从根本原因解决，禁止止血式 null check / try-catch
- 分支基于 `release/stable`，PR 目标也是 `release/stable`
- 分支命名：`fix/cc/YYYYMMDD/问题概述-时间戳`

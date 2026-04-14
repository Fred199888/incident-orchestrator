#!/bin/bash
set -euo pipefail

# ═══════════════════════════════════════
# Cron 入口：自动触发一轮 bug-fix-team
# 用法：
#   手动：  bash ~/bug-fix-scripts/run-bugfix-round.sh
#   Cron：  */30 * * * * ~/bug-fix-scripts/run-bugfix-round.sh >> /tmp/bugfix-cron.log 2>&1
# ═══════════════════════════════════════

SCRIPTS_DIR="${SCRIPTS_DIR:-$HOME/bug-fix-scripts}"
source "$SCRIPTS_DIR/.env"

CODE_DIR="${MONOREPO_DIR:-/mnt/code/secondme}"
LOG_DIR="/tmp/bugfix/logs"
mkdir -p "$LOG_DIR"

TIMESTAMP=$(date +%Y%m%d-%H%M%S)
LOG_FILE="$LOG_DIR/round-$TIMESTAMP.log"

echo "═══ Bug Fix Round: $TIMESTAMP ═══" | tee "$LOG_FILE"

# 确保 tmpfs 和代码就绪
if [ ! -d "$CODE_DIR/.git" ]; then
    echo "代码不存在，运行 server-init-code.sh ..." | tee -a "$LOG_FILE"
    bash "$SCRIPTS_DIR/server-init-code.sh" 2>&1 | tee -a "$LOG_FILE"
fi

# 更新到最新 master
cd "$CODE_DIR"
git fetch origin master 2>&1 | tee -a "$LOG_FILE"
git checkout master 2>&1 | tee -a "$LOG_FILE"
git pull origin master 2>&1 | tee -a "$LOG_FILE"

# 运行 Claude Code bug-fix-team
echo "启动 Claude Code bug-fix-team ..." | tee -a "$LOG_FILE"
cd "$CODE_DIR"
claude -p "/bug-fix-team" --dangerously-skip-permissions 2>&1 | tee -a "$LOG_FILE"

echo "═══ Round 完成: $TIMESTAMP ═══" | tee -a "$LOG_FILE"

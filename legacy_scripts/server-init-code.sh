#!/bin/bash
set -euo pipefail

# ═══════════════════════════════════════
# 每轮代码获取脚本（服务器启动/重启后执行）
# 用于 tmpfs 场景：重启后代码丢失，需重新获取
# ═══════════════════════════════════════

SCRIPTS_DIR="${SCRIPTS_DIR:-$HOME/bug-fix-scripts}"
source "$SCRIPTS_DIR/.env"

CODE_DIR="${MONOREPO_DIR:-/mnt/code/secondme}"
REPO_URL="${GITHUB_REPO_URL}"

echo "═══ 代码初始化 ═══"

# 确保 tmpfs 已挂载
if ! mount | grep -q '/mnt/code'; then
    echo "挂载 tmpfs ..."
    sudo mkdir -p /mnt/code
    sudo mount -t tmpfs -o size=8G tmpfs /mnt/code
fi

# 克隆或更新代码
if [ -d "$CODE_DIR/.git" ]; then
    echo "代码已存在，拉取最新 ..."
    cd "$CODE_DIR"
    git fetch origin master
    git checkout master
    git pull origin master
    echo "代码已更新"
else
    echo "克隆代码到 $CODE_DIR ..."
    git clone "$REPO_URL" "$CODE_DIR"
    echo "克隆完成"
fi

# 验证
cd "$CODE_DIR"
echo ""
echo "仓库: $(git remote get-url origin)"
echo "分支: $(git branch --show-current)"
echo "提交: $(git log --oneline -1)"
echo "目录检查:"
for dir in kernel/os-main kernel/os-ws kernel/base-datahub biz/os-user; do
    if [ -d "$dir" ]; then
        echo "  $dir"
    else
        echo "  $dir (缺失!)"
    fi
done
echo "═══ 代码就绪 ═══"

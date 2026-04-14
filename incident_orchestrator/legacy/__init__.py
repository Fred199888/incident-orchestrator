"""将现有 bugfix-automation scripts 目录加入 sys.path，以便 import 复用。

仅只读引用，不修改原文件。
"""
import os
import sys

LEGACY_SCRIPTS = os.path.expanduser(
    os.environ.get(
        "LEGACY_SCRIPTS_DIR",
        "~/Desktop/workspace/claude-code-deploy/bugfix-automation/scripts",
    )
)

if os.path.isdir(LEGACY_SCRIPTS) and LEGACY_SCRIPTS not in sys.path:
    sys.path.insert(0, LEGACY_SCRIPTS)

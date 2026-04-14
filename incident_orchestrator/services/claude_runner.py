"""Claude Code CLI 子进程管理 + session 持久化"""
import asyncio
import json
import logging
import uuid

from incident_orchestrator.config import get_settings

logger = logging.getLogger(__name__)


class ClaudeRunner:
    def __init__(self, max_concurrent: int = 5, timeout_seconds: int = 600):
        self._semaphore = asyncio.Semaphore(max_concurrent)
        self._timeout = timeout_seconds

    async def create_session(
        self, incident_id: str, prompt: str
    ) -> tuple[str, str]:
        """创建新 Claude session（独立 worktree），返回 (session_id, result_text)"""
        session_id = str(uuid.uuid4())
        settings = get_settings()

        cmd = [
            "claude",
            "--print",
            "--session-id", session_id,
            "--output-format", "json",
            "--dangerously-skip-permissions",
            "--model", "opus",
            prompt,
        ]

        import os
        cwd = settings.monorepo_dir if os.path.isdir(settings.monorepo_dir) else None
        result = await self._run(cmd, cwd=cwd)
        return session_id, result

    async def resume_session(self, session_id: str, message: str) -> str:
        """继续已有 Claude session，返回回复文本"""
        settings = get_settings()
        cmd = [
            "claude",
            "--print",
            "--resume", session_id,
            "--output-format", "json",
            "--dangerously-skip-permissions",
            message,
        ]

        import os
        cwd = settings.monorepo_dir if os.path.isdir(settings.monorepo_dir) else None
        return await self._run(cmd, cwd=cwd)

    async def _run(self, cmd: list[str], cwd: str | None = None) -> str:
        """异步子进程执行 + stream-json 解析 + 超时 + 并发控制"""
        async with self._semaphore:
            logger.info(f"启动 Claude: {' '.join(cmd[:6])}...")

            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=cwd,
            )

            try:
                stdout, stderr = await asyncio.wait_for(
                    process.communicate(), timeout=self._timeout
                )
            except asyncio.TimeoutError:
                logger.error(f"Claude 超时 ({self._timeout}s)，终止进程")
                process.terminate()
                try:
                    await asyncio.wait_for(process.wait(), timeout=5)
                except asyncio.TimeoutError:
                    process.kill()
                raise TimeoutError(f"Claude 执行超时 ({self._timeout}s)")

            if process.returncode != 0:
                err_msg = stderr.decode("utf-8", errors="replace").strip()
                out_msg = stdout.decode("utf-8", errors="replace").strip()
                logger.error("Claude 退出码 %d, stderr: %s, stdout: %s", process.returncode, err_msg[:300], out_msg[:300])
                raise RuntimeError(
                    f"Claude 退出码 {process.returncode}: {err_msg[:300] or out_msg[:300]}"
                )

            return self._parse_output(stdout.decode("utf-8", errors="replace"))

    def _parse_output(self, output: str) -> str:
        """从 json 格式输出中提取结果文本"""
        output = output.strip()
        if not output:
            return ""
        try:
            data = json.loads(output)
            # --output-format json 返回 {"type":"result","result":"..."}
            if isinstance(data, dict):
                return data.get("result", output)
            return output
        except json.JSONDecodeError:
            # 非 JSON，直接返回原文
            return output


# 全局单例
_runner: ClaudeRunner | None = None


def get_runner() -> ClaudeRunner:
    global _runner
    if _runner is None:
        settings = get_settings()
        _runner = ClaudeRunner(max_concurrent=settings.max_concurrent_runs)
    return _runner

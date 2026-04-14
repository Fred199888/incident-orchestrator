"""统一日志配置

所有日志写入 logs/incident-orchestrator.log，格式：
  2026-04-07 14:58:25 [INFO] [APP] [session=d961dcbf] 消息内容

用法：
  from incident_orchestrator.log import get_logger, set_session_id
  logger = get_logger("APP")
  set_session_id("d961dcbf-01f0-...")   # 设置后该线程/协程的日志自动带 session
  logger.info("处理完成")
"""
import logging
import os
import sys
import threading
from contextvars import ContextVar
from logging.handlers import RotatingFileHandler

# 当前 session ID（协程级别隔离）
_session_id: ContextVar[str] = ContextVar("session_id", default="-")

LOG_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "logs")
LOG_FILE = os.path.join(LOG_DIR, "incident-orchestrator.log")
LOG_FORMAT = "%(asctime)s [%(levelname)s] [%(tag)s] [session=%(session_id)s] %(message)s"
LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
LOG_MAX_BYTES = 50 * 1024 * 1024  # 50MB
LOG_BACKUP_COUNT = 5

_initialized = False
_init_lock = threading.Lock()


class SessionFilter(logging.Filter):
    """注入 session_id 和 tag 到 LogRecord"""
    def __init__(self, tag: str = ""):
        super().__init__()
        self.tag = tag

    def filter(self, record):
        record.session_id = _session_id.get("-")
        if not hasattr(record, "tag") or not record.tag:
            record.tag = self.tag
        return True


def _ensure_init():
    """初始化全局日志配置（只执行一次）"""
    global _initialized
    if _initialized:
        return
    with _init_lock:
        if _initialized:
            return

        os.makedirs(LOG_DIR, exist_ok=True)

        root = logging.getLogger()
        root.setLevel(logging.DEBUG)

        # 文件 handler（带轮转）
        file_handler = RotatingFileHandler(
            LOG_FILE,
            maxBytes=LOG_MAX_BYTES,
            backupCount=LOG_BACKUP_COUNT,
            encoding="utf-8",
        )
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(logging.Formatter(LOG_FORMAT, LOG_DATE_FORMAT))
        file_handler.addFilter(SessionFilter("ROOT"))
        root.addHandler(file_handler)

        # stdout handler（保留控制台输出，方便开发）
        stdout_handler = logging.StreamHandler(sys.stdout)
        stdout_handler.setLevel(logging.INFO)
        stdout_handler.setFormatter(logging.Formatter(LOG_FORMAT, LOG_DATE_FORMAT))
        stdout_handler.addFilter(SessionFilter("ROOT"))
        root.addHandler(stdout_handler)

        # 降低第三方库和框架日志级别
        for name in (
            "httpx", "httpcore", "uvicorn.access", "watchfiles",
            "lark_oapi", "lark", "Lark",
            "sqlalchemy", "aiosqlite",
            "websockets", "websocket",
        ):
            logging.getLogger(name).setLevel(logging.WARNING)

        _initialized = True


def get_logger(tag: str) -> logging.Logger:
    """获取带 tag 的 logger"""
    _ensure_init()
    logger = logging.getLogger(f"incident.{tag}")
    # 确保有 SessionFilter
    if not any(isinstance(f, SessionFilter) for f in logger.filters):
        logger.addFilter(SessionFilter(tag))
    return logger


def set_session_id(sid: str):
    """设置当前协程的 session ID（取前 8 位）"""
    short = sid[:8] if sid else "-"
    _session_id.set(short)


def get_session_id() -> str:
    return _session_id.get("-")

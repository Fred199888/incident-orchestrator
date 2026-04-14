from functools import lru_cache
from pathlib import Path

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # 飞书
    lark_app_id: str = ""
    lark_app_secret: str = ""
    lark_encrypt_key: str = ""
    lark_verification_token: str = ""
    lark_base_url: str = "https://open.feishu.cn"
    lark_chat_id: str = ""
    lark_bot_id: str = ""

    # 腾讯云 CLS
    tencentcloud_secret_id: str = ""
    tencentcloud_secret_key: str = ""
    cls_region: str = "na-siliconvalley"

    # GitHub
    github_repo_url: str = "https://github.com/second-me-01/secondme"
    gh_token: str = ""

    # Monorepo
    monorepo_dir: str = "/mnt/code/secondme"

    # 数据库
    db_path: str = "data/incidents.db"

    # 服务器
    host: str = "0.0.0.0"
    port: int = 8900

    # 并发
    max_concurrent_runs: int = 5

    # Legacy scripts 路径
    legacy_scripts_dir: str = "~/Desktop/workspace/claude-code-deploy/bugfix-automation/scripts"

    # Bitable
    bitable_app_token: str = ""
    bitable_table_id: str = ""

    # 管理员 open_id（每次修复都 @）
    admin_open_id: str = "ou_1f88475347f87f016dcc1cf53a594c3f"  # 宋学先
    # git 用户名 → 飞书 open_id 映射
    git_user_map: str = ""

    @property
    def db_url(self) -> str:
        path = Path(self.db_path)
        if not path.is_absolute():
            path = Path(__file__).parent.parent / path
        path.parent.mkdir(parents=True, exist_ok=True)
        return f"sqlite+aiosqlite:///{path}"

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}


@lru_cache
def get_settings() -> Settings:
    return Settings()


def get_mention_ids() -> list[str]:
    """返回固定 @ 的飞书 open_id 列表"""
    return [
        "ou_b2e8993e363ea46e69f7f4480a15ac9f",  # 戴豪辰
        "ou_1f88475347f87f016dcc1cf53a594c3f",  # 宋学先
    ]

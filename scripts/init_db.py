"""初始化数据库（建表 + WAL 模式）"""
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from incident_orchestrator.db.engine import init_db


async def main():
    await init_db()
    print("数据库初始化完成")


if __name__ == "__main__":
    asyncio.run(main())

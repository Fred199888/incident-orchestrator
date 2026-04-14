import asyncio
from collections import defaultdict


class IncidentMutex:
    """per-key 互斥锁，同一 key 排队串行执行"""

    def __init__(self):
        self._locks: dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)

    async def acquire(self, key: str) -> None:
        """排队等待获取锁"""
        await self._locks[key].acquire()

    def release(self, key: str) -> None:
        lock = self._locks.get(key)
        if lock and lock.locked():
            lock.release()

    def is_locked(self, key: str) -> bool:
        lock = self._locks.get(key)
        return lock.locked() if lock else False

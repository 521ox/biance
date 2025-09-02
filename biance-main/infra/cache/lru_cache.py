import asyncio
import time
from collections import OrderedDict
from typing import OrderedDict as OrderedDictType

class LRUCache:
    def __init__(self, max_items: int = 10000):
        self._d: OrderedDictType[str, tuple[bytes, float]] = OrderedDict()
        self._lock = asyncio.Lock()
        self.max_items = max_items
    async def get_bytes(self, key: str):
        async with self._lock:
            item = self._d.get(key)
            if not item:
                return None
            data, exp = item
            now = time.time()
            if exp < now:
                self._d.pop(key, None)
                return None
            self._d.move_to_end(key)
            return data
    async def set_bytes(self, key: str, data: bytes, ttl_s: int):
        async with self._lock:
            expire = time.time() + max(1, ttl_s)
            self._d[key] = (data, expire)
            self._d.move_to_end(key)
            while len(self._d) > self.max_items:
                self._d.popitem(last=False)

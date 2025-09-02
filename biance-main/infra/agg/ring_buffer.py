from collections import deque
from typing import Dict, Deque, Tuple

class RingBuffer:
    def __init__(self, capacity: int = 5):
        self.capacity = capacity
        self._buf: Dict[Tuple[str, str], Deque[dict]] = {}

    async def put(self, symbol: str, interval: str, bucket: dict):
        key = (symbol, interval)
        dq = self._buf.setdefault(key, deque(maxlen=self.capacity))
        dq.append(bucket)

    async def get_all(self, symbol: str, interval: str):
        return list(self._buf.get((symbol, interval), []))

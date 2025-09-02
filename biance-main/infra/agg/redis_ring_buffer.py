import json
import redis.asyncio as redis

class RedisRingBuffer:
    def __init__(self, url: str, capacity: int = 5):
        self._redis = redis.from_url(url, decode_responses=True)
        self.capacity = capacity

    async def put(self, symbol: str, interval: str, bucket: dict):
        key = f"agg:{symbol}:{interval}"
        val = json.dumps(bucket)
        async with self._redis.pipeline() as pipe:
            await pipe.rpush(key, val)
            await pipe.ltrim(key, -self.capacity, -1)
            await pipe.execute()

    async def get_all(self, symbol: str, interval: str):
        key = f"agg:{symbol}:{interval}"
        data = await self._redis.lrange(key, 0, -1)
        return [json.loads(x) for x in data]

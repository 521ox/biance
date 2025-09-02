import redis.asyncio as redis

class RedisCache:
    """Redis-based cache implementing the Cache port."""
    def __init__(self, url: str):
        self._redis = redis.from_url(url, decode_responses=False)

        
    async def get_bytes(self, key: str):
        return await self._redis.get(key)

    async def set_bytes(self, key: str, data: bytes, ttl_s: int):
        await self._redis.set(key, data, ex=max(1, ttl_s))

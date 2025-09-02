import os
import asyncio
from dataclasses import dataclass, field
from typing import List, Optional

from app.settings import Settings
from domain.ports import KlineRepo, Cache
from infra.observability.logging import configure_logging
from infra.db.sqlite_repo import SqliteKlineRepo, ensure_schema as ensure_sqlite_schema
from infra.db.postgres_repo import PostgresKlineRepo, ensure_schema as ensure_pg_schema
from infra.cache.lru_cache import LRUCache
from infra.agg.ring_buffer import RingBuffer
from domain.usecases import GetKlines, HealthSnapshot
from infra.fetch.fetcher_impl import Fetcher
from infra.agg.aggregator_impl import Aggregator

@dataclass
class AppState:
    settings: Settings
    kline_repo: KlineRepo
    l1_cache: Cache
    ring_buffer: RingBuffer
    use_get_klines: GetKlines
    use_health: HealthSnapshot
    fetcher: Optional[Fetcher] = None
    aggregator: Optional[Aggregator] = None
    tasks: List[asyncio.Task] = field(default_factory=list)

async def build_app_state() -> AppState:
    settings = Settings()
    os.makedirs("./data", exist_ok=True)
    configure_logging(settings.log_level)

    if settings.db_url.startswith("postgres"):
        kline_repo = PostgresKlineRepo(settings.db_url, pool_size=settings.db_pool_size)
        await ensure_pg_schema(settings.db_url)
    else:
        kline_repo = SqliteKlineRepo(settings.db_url, pool_size=settings.db_pool_size)
        await ensure_sqlite_schema(settings.db_url)
    await kline_repo.connect()

    if settings.cache_url:
        from infra.cache.redis_cache import RedisCache
        from infra.agg.redis_ring_buffer import RedisRingBuffer
        l1_cache = RedisCache(settings.cache_url)
        ring_buffer = RedisRingBuffer(settings.cache_url, capacity=5)
    else:
        l1_cache = LRUCache(max_items=10000)
        ring_buffer = RingBuffer(capacity=5)

    use_get_klines = GetKlines(kline_repo, l1_cache, ttl_s=settings.cache_ttl_sec_klines)
    use_health = HealthSnapshot(kline_repo)

    return AppState(
        settings=settings,
        kline_repo=kline_repo,
        l1_cache=l1_cache,
        ring_buffer=ring_buffer,
        use_get_klines=use_get_klines,
        use_health=use_health,
    )

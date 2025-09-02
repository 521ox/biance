import os
import asyncio
from dataclasses import dataclass, field
from typing import List, Optional

from app.settings import Settings
from infra.observability.logging import configure_logging
from infra.db.sqlite_repo import SqliteKlineRepo, ensure_schema
from infra.cache.lru_cache import LRUCache
from domain.usecases import GetKlines, HealthSnapshot
from infra.fetch.fetcher_impl import Fetcher
from infra.agg.aggregator_impl import Aggregator

@dataclass
class AppState:
    settings: Settings
    kline_repo: SqliteKlineRepo
    l1_cache: LRUCache
    use_get_klines: GetKlines
    use_health: HealthSnapshot
    fetcher: Optional[Fetcher] = None
    aggregator: Optional[Aggregator] = None
    tasks: List[asyncio.Task] = field(default_factory=list)

async def build_app_state() -> AppState:
    settings = Settings()
    os.makedirs("./data", exist_ok=True)
    configure_logging(settings.log_level)

    kline_repo = SqliteKlineRepo(settings.db_url)

    await ensure_schema(settings.db_url)
    await kline_repo.connect()

    l1_cache = LRUCache(max_items=10000)

    use_get_klines = GetKlines(kline_repo, l1_cache, ttl_ms=settings.cache_ttl_ms_klines)
    use_health = HealthSnapshot(kline_repo)

    return AppState(
        settings=settings,
        kline_repo=kline_repo,
        l1_cache=l1_cache,
        use_get_klines=use_get_klines,
        use_health=use_health,
    )

import pickle
from typing import Optional, List
from domain.ports import KlineRepo, Cache
from domain.models import Interval, Bar

class GetKlines:
    def __init__(self, repo: KlineRepo, cache: Cache, ttl_s: int=10):
        self.repo = repo
        self.cache = cache
        self.ttl_s = max(1, ttl_s)
    async def handle(self, symbol: str, interval: str,
                     start: Optional[int], end: Optional[int], limit: int,
                     only_final: bool=True):
        key=f"k:{symbol}:{interval}:{end}:{limit}:{1 if only_final else 0}:{start or 0}"
        if (b:=await self.cache.get_bytes(key)):
            return pickle.loads(b)
        bars: List[Bar] = await self.repo.query(
            symbol, Interval(interval), start, end, limit, only_final
        )
        await self.cache.set_bytes(key, pickle.dumps(bars), self.ttl_s)
        return bars

class HealthSnapshot:
    def __init__(self, kline_repo: KlineRepo):
        self.kline_repo=kline_repo
    async def handle(self):
        latest={}
        for itv in [Interval.m1, Interval.m3, Interval.m5, Interval.m15, Interval.h1, Interval.h4, Interval.d1]:
            latest[itv.value]=await self.kline_repo.max_open_time(itv)
        from time import time
        now_ms=int(time()*1000)
        def lag(ms): return None if ms is None else max(0,(now_ms-ms)//1000)
        return {
            "status":"ok","now":now_ms,
            "lag_sec_1m": lag(latest.get("1m")),
            "lag_sec_agg": { k:lag(v) for k,v in latest.items() if k!="1m" },
            "version":"mtf-node-days-0.3.0"
        }

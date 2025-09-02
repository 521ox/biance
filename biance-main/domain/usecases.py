import orjson
from typing import Optional
from domain.ports import KlineRepo, Cache
from domain.models import Interval

def serialize_binance_klines(bars):
    out=[]
    for b in bars:
        out.append([
            b.open_time, f"{b.open}", f"{b.high}", f"{b.low}", f"{b.close}",
            f"{b.volume}", b.close_time, f"{b.quote_volume}", b.trades,
            f"{b.taker_buy_base}", f"{b.taker_buy_quote}", "0"
        ])
    return orjson.dumps(out)

class GetKlines:
    def __init__(self, repo: KlineRepo, cache: Cache, ttl_ms: int=10000):
        self.repo=repo; self.cache=cache; self.ttl_s=max(1, ttl_ms//1000)
    async def handle(self, symbol: str, interval: str,
                     start: Optional[int], end: Optional[int], limit: int,
                     only_final: bool=True):
        key=f"k:{symbol}:{interval}:{end}:{limit}:{1 if only_final else 0}:{start or 0}"
        if (b:=await self.cache.get_bytes(key)): return b
        bars=await self.repo.query(symbol, Interval(interval), start, end, limit, only_final)
        payload=serialize_binance_klines(bars)
        await self.cache.set_bytes(key, payload, self.ttl_s)
        return payload

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

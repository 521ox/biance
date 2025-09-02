from typing import List, Dict, Optional
from time import time
from domain.models import Interval, Bar
from domain.ports import KlineRepo
from .ring_buffer import RingBuffer

MS = {
    Interval.m1: 60_000,
    Interval.m3: 180_000,
    Interval.m5: 300_000,
    Interval.m15: 900_000,
    Interval.h1: 3_600_000,
    Interval.h4: 14_400_000,
    Interval.d1: 86_400_000,
}

def bucket_start_ms(ts_ms: int, interval_ms: int) -> int:
    return (ts_ms // interval_ms) * interval_ms

class Aggregator:
    def __init__(self, repo: KlineRepo):
        self.repo = repo
        self.ring = RingBuffer(capacity=5)

    async def aggregate_symbol(self, symbol: str, target: Interval):
        assert target in (Interval.m3, Interval.m5, Interval.m15, Interval.h1, Interval.h4, Interval.d1)
        itv_ms = MS[target]
        last_t: Optional[int] = await self.repo.max_open_time(target)
        min_1m: Optional[int] = await self.repo.min_open_time(Interval.m1)
        if min_1m is None:
            return
        start_t = bucket_start_ms((last_t + itv_ms) if last_t else min_1m, itv_ms)
        # last closed bucket
        now_ms = int(time()*1000)
        end_bucket = bucket_start_ms(now_ms - 1, itv_ms)

        # chunk by time window (e.g., 3 days per iteration)
        window_ms = 3 * MS[Interval.d1]
        cur_start = start_t
        out: List[Bar] = []
        while cur_start <= end_bucket:
            cur_end = min(end_bucket + itv_ms - 1, cur_start + window_ms - 1)
            # pull 1m bars in this window
            src_bars = await self.repo.query(symbol, Interval.m1, start=cur_start, end=cur_end, limit=500000, only_final=True)
            if not src_bars:
                cur_start = cur_end + 1
                continue
            buckets: Dict[int, List[Bar]] = {}
            for b in src_bars:
                bs = bucket_start_ms(b.open_time, itv_ms)
                buckets.setdefault(bs, []).append(b)
            for bs in sorted(buckets.keys()):
                bars = buckets[bs]
                o = bars[0].open
                h = max(x.high for x in bars)
                l = min(x.low for x in bars)
                c = bars[-1].close
                vol = sum(x.volume for x in bars)
                qv = sum(x.quote_volume for x in bars)
                trades = sum(x.trades for x in bars)
                tb = sum(x.taker_buy_base for x in bars)
                tq = sum(x.taker_buy_quote for x in bars)
                close_time = bs + itv_ms - 1
                out.append(Bar(
                    symbol=symbol, interval=target, open_time=bs,
                    open=o, high=h, low=l, close=c,
                    volume=vol, quote_volume=qv,
                    close_time=close_time, trades=trades,
                    taker_buy_base=tb, taker_buy_quote=tq, is_final=True
                ))
            # flush periodically to reduce memory
            if len(out) >= 5000:
                await self.repo.upsert(out)
                for b in out[-5:]:
                    self.ring.put(symbol, target.value, {
                        "open_time": b.open_time, "close_time": b.close_time,
                        "open": b.open, "high": b.high, "low": b.low, "close": b.close
                    })
                out.clear()
            cur_start = cur_end + 1

        if out:
            await self.repo.upsert(out)
            for b in out[-5:]:
                self.ring.put(symbol, target.value, {
                    "open_time": b.open_time, "close_time": b.close_time,
                    "open": b.open, "high": b.high, "low": b.low, "close": b.close
                })

    async def aggregate_all(self, symbol: str):
        for t in (Interval.m3, Interval.m5, Interval.m15, Interval.h1, Interval.h4, Interval.d1):
            await self.aggregate_symbol(symbol, t)

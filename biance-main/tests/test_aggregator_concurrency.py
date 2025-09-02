import asyncio
import time
import sys
from pathlib import Path

# Ensure project root on path for imports
sys.path.append(str(Path(__file__).resolve().parents[1]))

from domain.models import Bar, Interval
from domain.ports import KlineRepo
from infra.agg.aggregator_impl import Aggregator, bucket_start_ms


class DummyRepo(KlineRepo):
    """In-memory repo with artificial delays to test concurrency."""

    def __init__(self, data, delay: float = 0.05):
        self.data = data
        self.delay = delay
        self.cur_symbol = None

    async def upsert_1m(self, bars):
        await asyncio.sleep(self.delay)
        for b in bars:
            self.data.setdefault((b.symbol, b.interval), []).append(b)

    async def upsert(self, bars):
        await asyncio.sleep(self.delay)
        for b in bars:
            self.data.setdefault((b.symbol, b.interval), []).append(b)

    async def query(self, symbol, interval, start, end, limit, only_final=True):
        await asyncio.sleep(self.delay)
        rows = self.data.get((symbol, interval), [])
        if start is not None:
            rows = [b for b in rows if b.open_time >= start]
        if end is not None:
            rows = [b for b in rows if b.open_time <= end]
        return rows[:limit]

    async def max_open_time(self, interval):
        await asyncio.sleep(self.delay)
        times = [b.open_time for (s, itv), bars in self.data.items()
                 if itv == interval and (self.cur_symbol is None or s == self.cur_symbol)
                 for b in bars]
        return max(times) if times else None

    async def min_open_time(self, interval):
        await asyncio.sleep(self.delay)
        times = [b.open_time for (s, itv), bars in self.data.items()
                 if itv == interval and (self.cur_symbol is None or s == self.cur_symbol)
                 for b in bars]
        return min(times) if times else None

    async def close(self):
        pass


symbols = ["AAA", "BBB", "CCC", "DDD", "EEE"]


def make_repo(delay: float = 0.05):
    now_ms = int(time.time() * 1000)
    base = bucket_start_ms(now_ms - 10 * 60_000, 60_000)
    data = {}
    for sym in symbols:
        bars = []
        for i in range(10):
            t = base + i * 60_000
            bars.append(Bar(
                symbol=sym,
                interval=Interval.m1,
                open_time=t,
                open=1,
                high=1,
                low=1,
                close=1,
                volume=1,
                quote_volume=1,
                close_time=t + 59_999,
            ))
        data[(sym, Interval.m1)] = bars
    return DummyRepo(data, delay)


def test_aggregate_all_concurrent():
    async def run(sym_conc: int, itv_conc: int):
        repo = make_repo()
        agg = Aggregator(repo)

        sem = asyncio.Semaphore(sym_conc)

        async def agg_sym(sym):
            async with sem:
                repo.cur_symbol = sym
                await agg.aggregate_all(sym, limit=itv_conc)

        start = time.perf_counter()
        await asyncio.gather(*(agg_sym(s) for s in symbols))
        duration = time.perf_counter() - start

        for s in symbols:
            repo.cur_symbol = s
            m3 = await repo.query(s, Interval.m3, None, None, 10)
            m5 = await repo.query(s, Interval.m5, None, None, 10)
            assert len(m3) == 4
            assert len(m5) == 3
        return duration

    seq = asyncio.run(run(1, 1))
    par = asyncio.run(run(len(symbols), 6))
    assert par <= seq * 0.6

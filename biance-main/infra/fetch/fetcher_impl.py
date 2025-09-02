import math, time, asyncio
from typing import List, Optional
from app.settings import Settings
from infra.fetch.binance_client import BinanceClient
from domain.ports import KlineRepo
from domain.models import Bar, Interval

MS = {
    Interval.m1: 60_000,
    Interval.h4: 14_400_000,
}

def bars_for_days(days: int, interval: Interval) -> int:
    if interval == Interval.d1: return 1 * days
    if interval == Interval.h4: return 6 * days
    if interval == Interval.h1: return 24 * days
    if interval == Interval.m15: return 96 * days
    if interval == Interval.m5: return 288 * days
    if interval == Interval.m3: return 480 * days
    if interval == Interval.m1: return 1440 * days
    raise ValueError("unsupported interval")

async def _rows_to_bars(rows: list, symbol: str, interval: Interval):
    out = []
    for arr in rows:
        out.append(Bar(
            symbol=symbol, interval=interval, open_time=int(arr[0]),
            open=float(arr[1]), high=float(arr[2]), low=float(arr[3]), close=float(arr[4]),
            volume=float(arr[5]), quote_volume=float(arr[7]), close_time=int(arr[6]),
            trades=int(arr[8]), taker_buy_base=float(arr[9]), taker_buy_quote=float(arr[10]),
            is_final=True
        ))
    return out

class Fetcher:
    def __init__(self, settings: Settings, repo: KlineRepo):
        self.s = settings
        self.repo = repo
        self.client = BinanceClient(settings.binance_base, concurrency=settings.fetch_concurrency)

    async def aclose(self):
        await self.client.aclose()

    async def initial_fetch_symbol(self, symbol: str):
        days = max(0, int(self.s.init_backfill_days))
        if days > 0:
            # days-driven coverage
            bars_1m = bars_for_days(days, Interval.m1)
            await self._ensure_coverage(symbol, Interval.m1, bars_1m)
            if self.s.backfill_pull_4h:
                bars_4h = bars_for_days(days, Interval.h4)
                await self._ensure_coverage(symbol, Interval.h4, bars_4h)
        else:
            # legacy behavior by INIT_PULL_*
            if self.s.init_pull_4h and self.s.init_pull_4h > 0:
                await self._ensure_coverage(symbol, Interval.h4, self.s.init_pull_4h)
            if self.s.init_pull_1m and self.s.init_pull_1m > 0:
                await self._ensure_coverage(symbol, Interval.m1, self.s.init_pull_1m)

    async def incremental_fetch_symbol(self, symbol: str):
        rows = await self.client.klines(symbol, Interval.m1.value, limit=2)
        bars = await _rows_to_bars(rows, symbol, Interval.m1)
        await self._upsert_bars(bars)

    async def initial_fetch_all(self, symbols: List[str]):
        await self.repo.connect()
        sem = asyncio.Semaphore(self.s.fetch_concurrency)

        async def run(sym: str):
            async with sem:
                await self.initial_fetch_symbol(sym)

        await asyncio.gather(*(run(s) for s in symbols))

    async def incremental_fetch_all(self, symbols: List[str]):
        sem = asyncio.Semaphore(self.s.fetch_concurrency)
        async def run(sym):
            async with sem:
                await self.incremental_fetch_symbol(sym)
        await asyncio.gather(*(run(s) for s in symbols))

    # ---------- coverage helpers ----------

    async def _ensure_coverage(self, symbol: str, interval: Interval, coverage_bars: int):
        interval_ms = MS.get(interval, None)
        if interval_ms is None:
            # only 1m/4h are pulled directly; others由聚合产生
            return
        now_ms = int(time.time() * 1000)
        target_start = now_ms - coverage_bars * interval_ms
        last_in_db: Optional[int] = await self.repo.max_open_time(interval)

        if last_in_db is None:
            await self._page_forward(symbol, interval, start_ms=target_start, until_ms=now_ms)
            return

        # backfill to target_start if needed
        await self._page_backward(symbol, interval, end_ms=last_in_db, until_ms=target_start)
        # forward to now
        await self._page_forward(symbol, interval, start_ms=last_in_db + interval_ms, until_ms=now_ms)

    async def _page_forward(self, symbol: str, interval: Interval, start_ms: int, until_ms: int):
        step = 1500
        interval_ms = MS[interval]
        cur = start_ms
        while cur <= until_ms:
            rows = await self.client.klines(symbol, interval.value, limit=step, startTime=cur)
            if not rows:
                break
            bars = await _rows_to_bars(rows, symbol, interval)
            await self._upsert_bars(bars)
            last_open = int(rows[-1][0])
            if len(rows) < step and last_open + interval_ms > until_ms:
                break
            cur = last_open + interval_ms

    async def _page_backward(self, symbol: str, interval: Interval, end_ms: int, until_ms: int):
        step = 1500
        while end_ms > until_ms:
            rows = await self.client.klines(symbol, interval.value, limit=step, endTime=end_ms)
            if not rows:
                break
            bars = await _rows_to_bars(rows, symbol, interval)
            await self._upsert_bars(bars)
            first_open = int(rows[0][0])
            if first_open <= until_ms:
                break
            end_ms = first_open - 1

    async def _upsert_bars(self, bars: List[Bar]):
        await self.repo.upsert(bars)

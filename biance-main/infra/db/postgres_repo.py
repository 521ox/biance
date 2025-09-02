import asyncpg
from typing import Iterable, List, Optional

from domain.models import Bar, Interval


DDL = [
    """
    CREATE TABLE IF NOT EXISTS kline_1m (
      symbol TEXT NOT NULL,
      open_time BIGINT NOT NULL,
      open DOUBLE PRECISION NOT NULL,
      high DOUBLE PRECISION NOT NULL,
      low DOUBLE PRECISION NOT NULL,
      close DOUBLE PRECISION NOT NULL,
      volume DOUBLE PRECISION NOT NULL,
      close_time BIGINT NOT NULL,
      quote_volume DOUBLE PRECISION NOT NULL DEFAULT 0,
      trades BIGINT NOT NULL DEFAULT 0,
      taker_buy_base DOUBLE PRECISION NOT NULL DEFAULT 0,
      taker_buy_quote DOUBLE PRECISION NOT NULL DEFAULT 0,
      is_final BOOLEAN NOT NULL DEFAULT TRUE,
      PRIMARY KEY(symbol, open_time)
    );
    """,
    """CREATE TABLE IF NOT EXISTS kline_3m (... same columns ...);""",
    """CREATE TABLE IF NOT EXISTS kline_5m (... same columns ...);""",
    """CREATE TABLE IF NOT EXISTS kline_15m (... same columns ...);""",
    """CREATE TABLE IF NOT EXISTS kline_1h (... same columns ...);""",
    """CREATE TABLE IF NOT EXISTS kline_4h (... same columns ...);""",
    """CREATE TABLE IF NOT EXISTS kline_1d (... same columns ...);""",
]


def table_for_interval(interval: Interval) -> str:
    return {
        Interval.m1: "kline_1m",
        Interval.m3: "kline_3m",
        Interval.m5: "kline_5m",
        Interval.m15: "kline_15m",
        Interval.h1: "kline_1h",
        Interval.h4: "kline_4h",
        Interval.d1: "kline_1d",
    }[interval]


async def ensure_schema(db_url: str) -> None:
    conn = await asyncpg.connect(db_url)
    try:
        for template in DDL:
            stmt = template
            if "... same columns ..." in stmt:
                base = DDL[0].split("(", 1)[1].rsplit(");", 1)[0]
                stmt = template.replace("... same columns ...", base)
            await conn.execute(stmt)
    finally:
        await conn.close()


class PostgresKlineRepo:
    def __init__(self, db_url: str, pool_size: int = 5):
        self.db_url = db_url
        self.pool_size = pool_size
        self._pool: Optional[asyncpg.pool.Pool] = None

    async def connect(self) -> None:
        if self._pool is None:
            self._pool = await asyncpg.create_pool(self.db_url, min_size=1, max_size=self.pool_size)

    async def close(self) -> None:
        if self._pool is not None:
            await self._pool.close()
            self._pool = None

    async def upsert_1m(self, bars: Iterable[Bar]) -> None:
        await self.upsert(bars)

    async def upsert(self, bars: Iterable[Bar]) -> None:
        bars = list(bars)
        if not bars:
            return
        await self.connect()
        tbl = table_for_interval(bars[0].interval)
        q = f"""
            INSERT INTO {tbl} (symbol, open_time, open, high, low, close, volume, close_time,
                               quote_volume, trades, taker_buy_base, taker_buy_quote, is_final)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
            ON CONFLICT (symbol, open_time) DO UPDATE SET
              open=EXCLUDED.open, high=EXCLUDED.high, low=EXCLUDED.low, close=EXCLUDED.close,
              volume=EXCLUDED.volume, close_time=EXCLUDED.close_time,
              quote_volume=EXCLUDED.quote_volume, trades=EXCLUDED.trades,
              taker_buy_base=EXCLUDED.taker_buy_base, taker_buy_quote=EXCLUDED.taker_buy_quote,
              is_final=EXCLUDED.is_final
        """
        assert self._pool is not None
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                await conn.executemany(q, [
                    (
                        b.symbol,
                        b.open_time,
                        b.open,
                        b.high,
                        b.low,
                        b.close,
                        b.volume,
                        b.close_time,
                        b.quote_volume,
                        b.trades,
                        b.taker_buy_base,
                        b.taker_buy_quote,
                        b.is_final,
                    )
                    for b in bars
                ])

    async def query(
        self,
        symbol: str,
        interval: Interval,
        start: Optional[int],
        end: Optional[int],
        limit: int,
        only_final: bool = True,
    ) -> List[Bar]:
        await self.connect()
        tbl = table_for_interval(interval)
        where = ["symbol = $1"]
        args: List[object] = [symbol]
        idx = 2
        if start is not None:
            where.append(f"open_time >= ${idx}")
            args.append(start)
            idx += 1
        if end is not None:
            where.append(f"open_time <= ${idx}")
            args.append(end)
            idx += 1
        if only_final:
            where.append("is_final = TRUE")
        where_sql = " AND ".join(where)
        sql = f"""
            SELECT symbol, open_time, open, high, low, close, volume, close_time,
                   quote_volume, trades, taker_buy_base, taker_buy_quote, is_final
            FROM {tbl}
            WHERE {where_sql}
            ORDER BY open_time DESC
            LIMIT ${idx}
        """
        args.append(limit)
        assert self._pool is not None
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(sql, *args)
        out: List[Bar] = []
        for r in reversed(rows):
            out.append(
                Bar(
                    symbol=r["symbol"],
                    interval=interval,
                    open_time=r["open_time"],
                    open=r["open"],
                    high=r["high"],
                    low=r["low"],
                    close=r["close"],
                    volume=r["volume"],
                    quote_volume=r["quote_volume"],
                    close_time=r["close_time"],
                    trades=r["trades"],
                    taker_buy_base=r["taker_buy_base"],
                    taker_buy_quote=r["taker_buy_quote"],
                    is_final=r["is_final"],
                )
            )
        return out

    async def max_open_time(self, interval: Interval) -> Optional[int]:
        await self.connect()
        tbl = table_for_interval(interval)
        assert self._pool is not None
        async with self._pool.acquire() as conn:
            val = await conn.fetchval(f"SELECT MAX(open_time) FROM {tbl}")
        return int(val) if val is not None else None

    async def min_open_time(self, interval: Interval) -> Optional[int]:
        await self.connect()
        tbl = table_for_interval(interval)
        assert self._pool is not None
        async with self._pool.acquire() as conn:
            val = await conn.fetchval(f"SELECT MIN(open_time) FROM {tbl}")
        return int(val) if val is not None else None


import os
import asyncio
import aiosqlite
from contextlib import asynccontextmanager
from typing import Iterable, List, Optional

from domain.models import Bar, Interval

DDL = [
"""
CREATE TABLE IF NOT EXISTS kline_1m (
  symbol TEXT NOT NULL,
  open_time INTEGER NOT NULL,
  open REAL NOT NULL,
  high REAL NOT NULL,
  low REAL NOT NULL,
  close REAL NOT NULL,
  volume REAL NOT NULL,
  close_time INTEGER NOT NULL,
  quote_volume REAL NOT NULL DEFAULT 0,
  trades INTEGER NOT NULL DEFAULT 0,
  taker_buy_base REAL NOT NULL DEFAULT 0,
  taker_buy_quote REAL NOT NULL DEFAULT 0,
  is_final INTEGER NOT NULL DEFAULT 1,
  PRIMARY KEY(symbol, open_time)
);
""",
"""
CREATE TABLE IF NOT EXISTS kline_3m (... same columns ...);
""",
"""
CREATE TABLE IF NOT EXISTS kline_5m (... same columns ...);
""",
"""
CREATE TABLE IF NOT EXISTS kline_15m (... same columns ...);
""",
"""
CREATE TABLE IF NOT EXISTS kline_1h (... same columns ...);
""",
"""
CREATE TABLE IF NOT EXISTS kline_4h (... same columns ...);
""",
"""
CREATE TABLE IF NOT EXISTS kline_1d (... same columns ...);
"""
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

async def ensure_schema(db_url: str):
    path = db_url.replace("sqlite:///", "")
    if "/" in path:
        os.makedirs(os.path.dirname(path), exist_ok=True)
    async with aiosqlite.connect(f"file:{path}?cache=shared", uri=True) as db:
        await db.execute("PRAGMA journal_mode=WAL;")
        await db.execute("PRAGMA busy_timeout=5000;")
        # Expand DDL bodies (fill '... same columns ...')
        for template in DDL:
            stmt = template
            if "... same columns ..." in stmt:
                base = DDL[0].split("(", 1)[1].rsplit(");", 1)[0]  # columns of 1m without trailing ');'
                stmt = template.replace("... same columns ...", base)
            await db.execute(stmt)
        await db.commit()

class SqliteConnectionPool:
    """A very small async connection pool for sqlite."""

    def __init__(self, path: str, size: int = 5):
        self.path = path
        self.size = size
        self._pool: asyncio.Queue[aiosqlite.Connection] = asyncio.Queue(maxsize=size)
        self._initialized = False

    async def _open_connection(self) -> aiosqlite.Connection:
        db = await aiosqlite.connect(f"file:{self.path}?cache=shared", uri=True)
        await db.execute("PRAGMA journal_mode=WAL;")
        await db.execute("PRAGMA busy_timeout=5000;")
        await db.commit()
        return db

    async def init(self) -> None:
        if self._initialized:
            return
        for _ in range(self.size):
            conn = await self._open_connection()
            await self._pool.put(conn)
        self._initialized = True

    @asynccontextmanager
    async def acquire(self) -> aiosqlite.Connection:
        if not self._initialized:
            await self.init()
        conn = await self._pool.get()
        try:
            yield conn
        finally:
            await self._pool.put(conn)

    async def close(self) -> None:
        while not self._pool.empty():
            conn = await self._pool.get()
            await conn.close()
        self._initialized = False


class SqliteKlineRepo:
    def __init__(self, db_url: str, pool_size: int = 5):
        self.path = db_url.replace("sqlite:///", "")
        self._pool = SqliteConnectionPool(self.path, pool_size)

    async def connect(self) -> None:
        await self._pool.init()

    async def close(self) -> None:
        await self._pool.close()

    async def upsert_1m(self, bars: Iterable[Bar]) -> None:
        await self.upsert(bars)

    async def upsert(self, bars: Iterable[Bar]) -> None:
        bars = list(bars)
        if not bars:
            return
        interval = bars[0].interval
        tbl = table_for_interval(interval)
        await self.connect()
        q = f"""
            INSERT INTO {tbl}
            (symbol, open_time, open, high, low, close, volume, close_time,
             quote_volume, trades, taker_buy_base, taker_buy_quote, is_final)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(symbol, open_time) DO UPDATE SET
              open=excluded.open, high=excluded.high, low=excluded.low, close=excluded.close,
              volume=excluded.volume, close_time=excluded.close_time, quote_volume=excluded.quote_volume,
              trades=excluded.trades, taker_buy_base=excluded.taker_buy_base,
              taker_buy_quote=excluded.taker_buy_quote, is_final=excluded.is_final
        """
        async with self._pool.acquire() as db:
            await db.execute("BEGIN")
            await db.executemany(q, [
                (b.symbol, b.open_time, b.open, b.high, b.low, b.close, b.volume,
                 b.close_time, b.quote_volume, b.trades, b.taker_buy_base, b.taker_buy_quote,
                 1 if b.is_final else 0)
                for b in bars
            ])
            await db.commit()

    async def query(self, symbol: str, interval: Interval,
                    start: Optional[int], end: Optional[int], limit: int,
                    only_final: bool = True) -> List[Bar]:
        tbl = table_for_interval(interval)
        where = ["symbol = ?"]
        args = [symbol]
        if start is not None:
            where.append("open_time >= ?")
            args.append(start)
        if end is not None:
            where.append("open_time <= ?")
            args.append(end)
        if only_final:
            where.append("is_final = 1")
        wsql = " AND ".join(where)
        sql = f"""
            SELECT symbol, open_time, open, high, low, close, volume, close_time,
                   quote_volume, trades, taker_buy_base, taker_buy_quote, is_final
            FROM {tbl}
            WHERE {wsql}
            ORDER BY open_time DESC
            LIMIT ?
        """
        args.append(limit)
        await self.connect()
        async with self._pool.acquire() as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute(sql, args)
            rows = await cur.fetchall()
        out: List[Bar] = []
        for r in rows[::-1]:
            out.append(Bar(
                symbol=r["symbol"], interval=interval, open_time=r["open_time"],
                open=r["open"], high=r["high"], low=r["low"], close=r["close"],
                volume=r["volume"], quote_volume=r["quote_volume"], close_time=r["close_time"],
                trades=r["trades"], taker_buy_base=r["taker_buy_base"], taker_buy_quote=r["taker_buy_quote"],
                is_final=bool(r["is_final"])
            ))
        return out

    async def max_open_time(self, interval: Interval) -> Optional[int]:
        tbl = table_for_interval(interval)
        await self.connect()
        async with self._pool.acquire() as db:
            cur = await db.execute(f"SELECT MAX(open_time) FROM {tbl}")
            row = await cur.fetchone()
        return row[0] if row and row[0] is not None else None

    async def min_open_time(self, interval: Interval) -> Optional[int]:
        tbl = table_for_interval(interval)
        await self.connect()
        async with self._pool.acquire() as db:
            cur = await db.execute(f"SELECT MIN(open_time) FROM {tbl}")
            row = await cur.fetchone()
        return row[0] if row and row[0] is not None else None

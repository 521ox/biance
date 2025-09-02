import asyncio
import time
import sys
from pathlib import Path

# Ensure project root on path for imports
sys.path.append(str(Path(__file__).resolve().parents[1]))

from app.settings import Settings
from domain.models import Bar, Interval
from infra.db.sqlite_repo import SqliteKlineRepo, ensure_schema
from infra.fetch.fetcher_impl import Fetcher


class DummyFetcher(Fetcher):
    """Fetcher with stubbed symbol fetch to test concurrency."""

    def __init__(self, settings: Settings, repo: SqliteKlineRepo):
        self.s = settings
        self.repo = repo
        self.client = None  # no network client needed
        self._write_lock = asyncio.Lock()

    async def aclose(self):
        pass

    async def initial_fetch_symbol(self, symbol: str):
        bar = Bar(
            symbol=symbol,
            interval=Interval.m1,
            open_time=0,
            open=1,
            high=1,
            low=1,
            close=1,
            volume=1,
            quote_volume=1,
            close_time=60,
        )
        # simulate network delay
        await asyncio.sleep(0.05)
        await self._upsert_bars([bar])


def test_initial_fetch_all_concurrent(tmp_path: Path):
    symbols = ["AAA", "BBB", "CCC", "DDD", "EEE"]

    async def run(concurrency: int, db_name: str):
        db_url = f"sqlite:///{tmp_path / db_name}"
        await ensure_schema(db_url)
        repo = SqliteKlineRepo(db_url, pool_size=5)
        settings = Settings(FETCH_CONCURRENCY=concurrency)
        fetcher = DummyFetcher(settings, repo)

        start = time.perf_counter()
        await fetcher.initial_fetch_all(symbols)
        duration = time.perf_counter() - start

        # verify each symbol written once
        for sym in symbols:
            rows = await repo.query(sym, Interval.m1, None, None, 10)
            assert len(rows) == 1

        await fetcher.aclose()
        await repo.close()
        return duration

    seq_time = asyncio.run(run(1, "seq.db"))
    par_time = asyncio.run(run(5, "par.db"))

    assert par_time < seq_time

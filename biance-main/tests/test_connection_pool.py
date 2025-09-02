import asyncio
import time
import sys
from pathlib import Path

# Ensure project root on path for imports
sys.path.append(str(Path(__file__).resolve().parents[1]))

from domain.models import Bar, Interval
from infra.db.sqlite_repo import SqliteKlineRepo, ensure_schema


def _sample_bars(n: int):
    bars = []
    for i in range(n):
        bars.append(
            Bar(
                symbol="BTCUSDT",
                interval=Interval.m1,
                open_time=i,
                open=1,
                high=1,
                low=1,
                close=1,
                volume=1,
                quote_volume=1,
                close_time=i + 60,
            )
        )
    return bars


def test_parallel_faster_than_sequential(tmp_path: Path):
    db_url = f"sqlite:///{tmp_path / 'test.db'}"

    async def run():
        await ensure_schema(db_url)
        repo = SqliteKlineRepo(db_url, pool_size=5)
        bars = _sample_bars(1000)
        # populate once
        await repo.upsert(bars)

        async def read_job():
            # simulate some extra work per query to amplify concurrency benefits
            await repo.query("BTCUSDT", Interval.m1, None, None, 100)
            await asyncio.sleep(0.01)

        # sequential read execution
        start = time.perf_counter()
        for _ in range(20):
            await read_job()
        seq_time = time.perf_counter() - start

        # parallel read execution
        start = time.perf_counter()
        await asyncio.gather(*(read_job() for _ in range(20)))
        par_time = time.perf_counter() - start

        await repo.close()
        return seq_time, par_time

    seq, par = asyncio.run(run())
    # Parallel time should be lower than sequential when connections are pooled
    assert par < seq

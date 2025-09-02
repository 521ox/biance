import asyncio
import logging
from typing import Callable
from app.bootstrap import AppState
from infra.fetch.fetcher_impl import Fetcher
from infra.agg.aggregator_impl import Aggregator

logger = logging.getLogger(__name__)

def on_startup(state: AppState) -> Callable[[], None]:
    async def _bg_runner():
        if not (state.settings.enable_fetcher or state.settings.enable_aggregator):
            return
        state.fetcher = Fetcher(state.settings, state.kline_repo)
        state.aggregator = Aggregator(state.kline_repo)

        if state.settings.enable_fetcher:
            await state.fetcher.initial_fetch_all(state.settings.symbols)
        async def agg_all_symbols():
            sem = asyncio.Semaphore(5)
            async def _run(sym: str):
                async with sem:
                    await state.aggregator.aggregate_all(sym)
            await asyncio.gather(*(_run(sym) for sym in state.settings.symbols))

        if state.settings.enable_aggregator:
            await agg_all_symbols()

        async def loop_fetch():
            retry = 0
            while state.settings.enable_fetcher:
                try:
                    await state.fetcher.incremental_fetch_all(state.settings.symbols)
                    retry = 0
                    await asyncio.sleep(55)
                except Exception as e:
                    retry += 1
                    logger.exception("incremental fetch failed", exc_info=e)
                    if retry > 5:
                        logger.error("loop_fetch max retries exceeded, exiting")
                        raise
                    delay = min(2 ** retry, 60)
                    await asyncio.sleep(delay)

        async def loop_agg():
            retry = 0
            while state.settings.enable_aggregator:
                try:
                    await agg_all_symbols()
                    retry = 0
                    await asyncio.sleep(60)
                except Exception as e:
                    retry += 1
                    logger.exception("aggregation failed", exc_info=e)
                    if retry > 5:
                        logger.error("loop_agg max retries exceeded, exiting")
                        raise
                    delay = min(2 ** retry, 60)
                    await asyncio.sleep(delay)

        async def start_loop(coro, name: str):
            while True:
                try:
                    await coro()
                    return
                except Exception as e:
                    logger.exception("%s loop crashed, restarting", name, exc_info=e)
                    await asyncio.sleep(5)

        if state.settings.enable_fetcher:
            state.tasks.append(asyncio.create_task(start_loop(loop_fetch, "fetch")))
        if state.settings.enable_aggregator:
            state.tasks.append(asyncio.create_task(start_loop(loop_agg, "agg")))

    def _start():
        task = asyncio.get_event_loop().create_task(_bg_runner())
        state.tasks.append(task)
    return _start

def on_shutdown(state: AppState) -> Callable[[], None]:
    async def _stop():
        for t in state.tasks:
            t.cancel()
        for t in state.tasks:
            try:
                await t
            except asyncio.CancelledError:
                pass
            except Exception as e:
                logger.exception("task error during shutdown", exc_info=e)
        if state.fetcher is not None:
            await state.fetcher.aclose()
        await state.kline_repo.close()
    return _stop

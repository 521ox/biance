import asyncio
from typing import Optional, List
import httpx
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

class BinanceClient:
    def __init__(self, base: str, concurrency: int = 8, timeout_s: float = 10.0):
        self.base = base.rstrip("/")
        self._client = httpx.AsyncClient(base_url=self.base, timeout=timeout_s, headers={
            "User-Agent": "mtf-node/days-backfill"
        })
        self._sem = asyncio.Semaphore(concurrency)

    async def aclose(self):
        await self._client.aclose()

    @retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=0.5, min=0.5, max=6.0),
           retry=retry_if_exception_type(httpx.HTTPError))
    async def klines(self, symbol: str, interval: str, limit: int = 1500,
                     startTime: Optional[int] = None, endTime: Optional[int] = None) -> List[list]:
        params = {"symbol": symbol, "interval": interval, "limit": limit}
        if startTime is not None:
            params["startTime"] = startTime
        if endTime is not None:
            params["endTime"] = endTime
        async with self._sem:
            r = await self._client.get("/fapi/v1/klines", params=params)
            r.raise_for_status()
            return r.json()

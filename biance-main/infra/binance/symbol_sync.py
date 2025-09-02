import asyncio
import time
import logging
from typing import List, Set, Tuple
import httpx

log = logging.getLogger("symbol_sync")

class SymbolRegistry:
    def __init__(self, initial: List[str] | None = None):
        self._set: Set[str] = set(initial or [])
        self._lock = asyncio.Lock()
    async def get_all(self) -> List[str]:
        async with self._lock:
            return list(self._set)
    async def replace(self, new_list: List[str]):
        async with self._lock:
            newset = set(new_list)
            added = newset - self._set
            removed = self._set - newset
            self._set = newset
            return added, removed

async def fetch_perp_symbols(client: httpx.AsyncClient, quote_assets: List[str]) -> List[str]:
    r = await client.get("/fapi/v1/exchangeInfo", timeout=15)
    r.raise_for_status()
    data = r.json()
    now_ms = int(time.time() * 1000)
    out: List[str] = []
    for s in data.get("symbols", []):
        if s.get("contractType") != "PERPETUAL":
            continue
        if s.get("status") != "TRADING":
            continue
        if s.get("quoteAsset") not in quote_assets:
            continue
        delivery = int(s.get("deliveryDate", 0) or 0)
        if delivery and delivery <= now_ms:
            continue
        sym = s.get("symbol")
        if sym:
            out.append(sym)
    return sorted(set(out))

async def run_symbol_sync(registry: SymbolRegistry, client: httpx.AsyncClient, quote_assets: List[str], interval_sec: int):
    while True:
        try:
            new_list = await fetch_perp_symbols(client, quote_assets)
            added, removed = await registry.replace(new_list)
            if added or removed:
                log.info("symbol_sync changed: +%d, -%d; added=%s removed=%s",
                         len(added), len(removed),
                         ",".join(sorted(added))[:200],
                         ",".join(sorted(removed))[:200])
        except Exception as e:
            log.exception("symbol_sync error: %s", e)
        await asyncio.sleep(max(30, interval_sec))

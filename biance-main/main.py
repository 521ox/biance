import asyncio
import importlib
import logging
import httpx
from fastapi import FastAPI
from prometheus_fastapi_instrumentator import Instrumentator
from app.settings import Settings
from infra.binance.symbol_sync import SymbolRegistry, run_symbol_sync
from infra.http.etag_middleware import KlineETagMiddleware

app = FastAPI(title="MTF Data Node", version="0.4.0")
settings = Settings()
log = logging.getLogger("app")

Instrumentator().instrument(app).expose(app, include_in_schema=False, endpoint="/metrics")
app.add_middleware(KlineETagMiddleware)

app.state.settings = settings
symbol_registry = SymbolRegistry(initial=settings.symbols)
app.state.symbol_registry = symbol_registry

for mod in ("infra.http.api", "infra.http.admin"):
    try:
        m = importlib.import_module(mod)
        router = getattr(m, "router", None)
        if router:
            app.include_router(router)
            log.info("Included router from %s", mod)
    except Exception as e:
        log.warning("Skip include %s: %s", mod, e)

@app.on_event("startup")
async def _start_symbol_sync():
    if settings.auto_sync_symbols:
        client = httpx.AsyncClient(base_url="https://fapi.binance.com")
        app.state._sym_client = client
        app.state._sym_task = asyncio.create_task(
            run_symbol_sync(registry=symbol_registry, client=client,
                            quote_assets=settings.quote_assets,
                            interval_sec=settings.symbol_sync_interval_sec)
        )
        log.info("AUTO_SYNC_SYMBOLS enabled interval=%s quote=%s",
                 settings.symbol_sync_interval_sec, settings.quote_assets)

@app.on_event("shutdown")
async def _stop_symbol_sync():
    t = getattr(app.state, "_sym_task", None)
    if t: t.cancel()
    c = getattr(app.state, "_sym_client", None)
    if c: await c.aclose()

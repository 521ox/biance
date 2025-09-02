from fastapi import APIRouter, Depends, Query, Response
from app.bootstrap import AppState
from infra.serialization import serialize_binance_klines

router = APIRouter()

def get_state() -> AppState:
    from main import app_state
    return app_state

@router.get("/fapi/v1/klines")
async def get_klines(symbol: str,
                     interval: str,
                     startTime: int | None = Query(default=None),
                     endTime: int | None = Query(default=None),
                     limit: int = Query(default=500, ge=1, le=1500),
                     includeCurrent: bool = Query(default=False),
                     state: AppState = Depends(get_state)):
    bars = await state.use_get_klines.handle(
        symbol, interval, startTime, endTime, limit, only_final=(not includeCurrent)
    )
    payload = serialize_binance_klines(bars)
    return Response(content=payload, media_type="application/json")

@router.get("/v1/health")
async def health(state: AppState = Depends(get_state)):
    return await state.use_health.handle()

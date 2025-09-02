from fastapi import APIRouter, Request, HTTPException
from infra.binance.symbol_sync import fetch_perp_symbols

router = APIRouter()

@router.post("/v1/admin/symbols/refresh")
async def refresh_symbols(request: Request):
    app = request.app
    client = getattr(app.state, "_sym_client", None)
    if not client:
        raise HTTPException(503, "sync client not ready")
    settings = app.state.settings
    new_list = await fetch_perp_symbols(client, settings.quote_assets)
    added, removed = await app.state.symbol_registry.replace(new_list)
    return {"ok": True, "added": sorted(added), "removed": sorted(removed)}

import hashlib
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response, StreamingResponse

class KlineETagMiddleware(BaseHTTPMiddleware):
    def _should_apply(self, request: Request) -> bool:
        return request.method.upper() == "GET" and request.url.path.startswith("/fapi/v1/klines")
    async def dispatch(self, request: Request, call_next):
        if not self._should_apply(request):
            return await call_next(request)
        resp = await call_next(request)
        if resp.status_code != 200:
            return resp
        if isinstance(resp, StreamingResponse) and not hasattr(resp, "body"):
            body = b"".join([chunk async for chunk in resp.body_iterator])  # type: ignore
            new_resp = Response(content=body, status_code=resp.status_code,
                                headers=dict(resp.headers), media_type=resp.media_type,
                                background=resp.background)
            resp = new_resp
        else:
            body = await resp.body()
        etag = hashlib.md5(body).hexdigest()
        inm = request.headers.get("if-none-match")
        if inm and inm == etag:
            return Response(status_code=304)
        resp.headers.setdefault("ETag", etag)
        resp.headers.setdefault("Cache-Control", "public, max-age=10")
        return resp

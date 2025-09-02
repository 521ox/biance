# Patch Notes (clean)
- Applied changes inside the project root folder to avoid duplicate `app/`.
- Auto symbol sync (added infra/binance/symbol_sync.py) and startup task in main.py
- ETag/304 middleware for /fapi/v1/klines (infra/http/etag_middleware.py)
- /metrics with Prometheus instrumentator
- O(1) LRU cache (infra/cache/lru_cache.py)
- SQLite partial index for kline_1m is_final=1 (if infra/db/sqlite_repo.py init runs)
- Nginx and systemd templates updated
- requirements.txt/dev tooling added

# MTF Data Node — Klines + Fetcher + Aggregator (Days Backfill)

本版本在配置中新增 **`BACKFILL_DAYS`**：按“天数覆盖”自动换算各周期需要补齐的根数：
- 1d: `1 * days`
- 4h: `6 * days`（24/4）
- 1h: `24 * days`
- 15m: `96 * days`（24*60/15）
- 5m: `288 * days`
- 3m: `480 * days`
- 1m: `1440 * days`

> 服务器端只做取数/存储/聚合；EMA 仍由主程序计算。

## Quickstart
```bash
cp .env.sample biance-main/.env
cd biance-main
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
uvicorn main:app --host 0.0.0.0 --port 8000 --workers 4
```

The default requirements now include [`orjson`](https://pypi.org/project/orjson/)
for fast JSON serialization and
[`pydantic-settings[dotenv]`](https://docs.pydantic.dev/latest/concepts/pydantic_settings/)
to load configuration from `.env` files.

Edit `biance-main/.env` as needed to configure variables such as `SYMBOLS` and `DB_URL` before starting the server.

## Configuration

The service is driven by environment variables loaded from a `.env` file. Copy
the provided `.env.sample` and adjust common settings as needed:

- `SYMBOLS` – comma‑separated trading pairs to fetch (e.g. `BTCUSDT,ETHUSDT`).
- `INTERVALS` – list of candlestick intervals to maintain (e.g. `1m,5m,1h`).
- `BACKFILL_DAYS` – number of days of historical data to pre‑load on startup.
- `AUTO_SYNC_SYMBOLS` – automatically refresh the tradable symbol list.
- `SYMBOL_SYNC_INTERVAL_SEC` – how frequently to perform the symbol refresh.
- `QUOTE_ASSETS` – quote assets to include when syncing symbols.
- `DB_URL` – database connection string (`sqlite:///...` by default).
- `DB_POOL_SIZE` – size of the async database connection pool (default 10).
- `CACHE_URL` – Redis connection used for shared caching across instances.
- `CACHE_TTL_SEC_KLINES` – TTL for recently fetched klines in the cache.
- `LOG_LEVEL` – logging verbosity (`INFO`, `DEBUG`, …).
- `FETCH_CONCURRENCY` – number of concurrent fetcher workers.

See `.env.sample` for additional tunables such as toggling the fetcher or
aggregator and controlling initial backfill behavior.

## Manual Deployment

1. **Install system packages**
   ```bash
   sudo apt update
   sudo apt install -y python3 python3-venv python3-dev build-essential
   ```
2. **Create a virtual environment and install dependencies**
   ```bash
   cd biance-main
   python -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```
3. **Launch the application**
   ```bash
   uvicorn main:app --host 0.0.0.0 --port 8000 --workers 4
   ```

Optional services:

- Set `CACHE_URL` to enable Redis caching (e.g., `redis://localhost:6379/0`).
- Set `DB_URL` to use PostgreSQL instead of the default SQLite (e.g., `postgresql://user:pass@localhost:5432/dbname`) for high-concurrency workloads and tune `DB_POOL_SIZE` accordingly.

## Example Usage

With the server running, you can exercise the API with tools like `curl`:

```bash
# Fetch recent klines
curl 'http://localhost:8000/fapi/v1/klines?symbol=BTCUSDT&interval=1m&limit=5'

# Refresh the symbol registry
curl -X POST http://localhost:8000/v1/admin/symbols/refresh

# Health check
curl http://localhost:8000/v1/health
```

## Storage Notes

- 默认使用 SQLite，应用启动时会复用单一连接，并启用 WAL 及 `busy_timeout` 以减少锁冲突。
- 通过环境变量 `DB_URL` 可以切换数据库，例如：
  - `sqlite:///data/klines.db`
  - `postgresql://user:pass@host:5432/dbname`
- `DB_POOL_SIZE` 控制连接池大小（默认 10）。
- 如需更高并发与稳定性，建议切换到 PostgreSQL 并相应调整 `DB_POOL_SIZE`（使用 `asyncpg` 驱动）。

## External Cache

- 通过环境变量 `CACHE_URL` 配置 Redis（例如 `redis://localhost:6379/0`）即可启用外部缓存。
- 启用后，应用的一级缓存与聚合结果 RingBuffer 都会使用 Redis 存储，实现多实例间共享。
- 如未设置 `CACHE_URL`，则使用进程内 LRU 缓存和本地 RingBuffer。

## Further Reading

- See [`biance-main/PATCH_NOTES.md`](biance-main/PATCH_NOTES.md) for recent
  changes and advanced configuration history.
- Deployment templates for systemd and Nginx are available in
  [`biance-main/deploy/`](biance-main/deploy/).

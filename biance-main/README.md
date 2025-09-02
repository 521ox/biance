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
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.sample .env
uvicorn main:app --host 0.0.0.0 --port 8000 --workers 4
```

## Storage Notes

- 默认使用 SQLite，应用启动时会复用单一连接，并启用 WAL 及 `busy_timeout` 以减少锁冲突。
- 通过环境变量 `DB_URL` 可以切换数据库，例如：
  - `sqlite:///data/klines.db`
  - `postgresql://user:pass@host:5432/dbname`
- `DB_POOL_SIZE` 控制连接池大小（默认 5）。
- 如需更强的并发与稳定性，可迁移到 PostgreSQL（使用 `asyncpg` 驱动）。

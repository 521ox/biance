Biance：K 线数据节点（使用说明 .txt 版）
=====================================

一个面向生产的 FastAPI 服务，用于高并发地提供 Binance 合约 K 线数据读取。
具备自动同步交易对、历史回补、本地存储与缓存、条件请求（ETag/304）、Prometheus 指标等工程化能力。

定位：只读型市场数据节点（不涉及下单/账户）。
适用：回测 / 行情服务 / 风控看板 / 内部数据 API。

-------------------------------------
一、功能总览
-------------------------------------
1) 交易对自动同步
   - 后台定时从 /fapi/v1/exchangeInfo 拉取 USDT 永续合约（contractType=PERPETUAL、status=TRADING）。
   - 基于 deliveryDate 预剔除将/已下架；新增/下架自动生效。

2) K 线抓取与历史回补
   - 支持 symbol/interval/startTime/endTime/limit 拉取；回补历史并写入本地库。
   - 带抖动（jitter）与重试退避，减少同秒冲突与限速风险。

3) 高并发读取与弱缓存
   - L1 内存缓存：O(1) LRU + TTL，热门区间直出 bytes。
   - HTTP 条件请求：自动附带 ETag 与 Cache-Control；If-None-Match 命中返回 304。

4) 本地存储（默认 SQLite）
   - WAL + busy_timeout，适合读多写少。
   - 各周期分表，主键 (symbol, open_time)；为 is_final=1 查询提供部分索引。
   - 可扩展到 Postgres（接口已预留，便于提升并发与容量）。

5) 可观测性
   - /metrics：Prometheus 指标（QPS、时延分布、状态码）。
   - 健康检查 /v1/health。

-------------------------------------
二、目录结构（节选）
-------------------------------------
biance-main/
  main.py                      # FastAPI 入口（或 app/main.py）
  app/
    settings.py                # .env 配置解析（CSV/JSON 列表）
    lifecycle.py               # 后台任务入口（可能在此）
  infra/
    http/
      api.py                   # REST 路由（/fapi/v1/klines 等）
      admin.py                 # 管理接口（手动刷新交易对）
      etag_middleware.py       # 为 K 线自动加 ETag/304
    binance/
      symbol_sync.py           # 交易对自动同步 & 注册表
    db/
      sqlite_repo.py           # SQLite 存储实现（WAL / 索引）
      # postgres_repo.py       # 可选：Postgres 实现雏形
    cache/
      lru_cache.py             # O(1) LRU + TTL
    agg/
      aggregator_impl.py       # 回补/聚合逻辑
      ring_buffer.py           # 进程内环形缓冲
      redis_ring_buffer.py     # 可选：Redis 版环形缓冲
requirements.txt
.env.sample

-------------------------------------
三、接口文档
-------------------------------------
1) GET /fapi/v1/klines
   - 查询 K 线（返回与 Binance 兼容的二维数组；服务器自动加 ETag 与 Cache-Control）。
   - Query：
     symbol (必填)：交易对，如 BTCUSDT
     interval (必填)：1m,3m,5m,15m,1h,4h,1d
     limit：默认 500
     startTime (ms)、endTime (ms)：时间范围
     only_final (bool)：默认 true，仅返已收盘 K 线
   - 示例：
     curl -i "http://localhost:8000/fapi/v1/klines?symbol=BTCUSDT&interval=1m&limit=500"
     # 第二次带 ETag：
     curl -i -H 'If-None-Match: "abc123..."' "http://localhost:8000/fapi/v1/klines?symbol=BTCUSDT&interval=1m&limit=500"

2) POST /v1/admin/symbols/refresh
   - 手动触发一次交易对同步（建议仅内网或加鉴权）。
   - 示例：curl -X POST http://localhost:8000/v1/admin/symbols/refresh

3) GET /v1/health
   - 健康检查（探活）。

4) GET /metrics
   - Prometheus 指标。

-------------------------------------
四、配置（.env）
-------------------------------------
放在运行目录（cwd）下；支持逗号 CSV 或 JSON 列表写法。

# 冷启动兜底；开启自动同步后可留空
SYMBOLS=BTCUSDT,ETHUSDT
INTERVALS=1m,3m,5m,15m,1h,4h,1d
BACKFILL_DAYS=365

# 自动同步交易对（推荐）
AUTO_SYNC_SYMBOLS=true
SYMBOL_SYNC_INTERVAL_SEC=300
QUOTE_ASSETS=USDT           # 可多写：USDT,USD

# 可选：缓存/数据库高级配置（若工程内支持）
# DB_URL=sqlite:///./kline.db
# CACHE_URL=redis://127.0.0.1:6379/0
# CACHE_TTL_SEC_KLINES=10
# FETCH_CONCURRENCY=4

说明：开启 AUTO_SYNC_SYMBOLS 后，运行时使用后台同步集合；.env 里的 SYMBOLS 仅作冷启动兜底。
抓取/回补循环应从 app.state.symbol_registry.get_all() 读取当前生效交易对集合。

-------------------------------------
五、启动与部署
-------------------------------------
A. 本地快速启动（开发）
1) cd biance-main
2) python -m venv .venv && source .venv/bin/activate
3) pip install -U pip && pip install -r requirements.txt
4) 入口尝试：
   uvicorn main:app --host 0.0.0.0 --port 8000
   # 或 uvicorn app.main:app --host 0.0.0.0 --port 8000

B. Debian 12 裸机部署（不使用 Docker）
1) 系统准备：
   sudo apt update
   sudo apt install -y python3 python3-venv python3-pip nginx
   sudo useradd -r -s /usr/sbin/nologin biance || true

2) 拉代码 & 依赖：
   sudo mkdir -p /opt/biance && sudo chown -R $USER /opt/biance
   cd /opt/biance
   git clone https://github.com/521ox/biance.git src
   cd src/biance-main
   python3 -m venv .venv && source .venv/bin/activate
   pip install -U pip && pip install -r requirements.txt
   cp .env.sample .env   # 按需编辑

3) systemd 自启（/etc/systemd/system/biance.service）：
[Unit]
Description=Biance Data Node
After=network-online.target

[Service]
User=biance
Group=biance
WorkingDirectory=/opt/biance/src/biance-main
Environment=PYTHONUNBUFFERED=1
ExecStart=/opt/biance/src/biance-main/.venv/bin/uvicorn main:app --host 0.0.0.0 --port 8000 --workers 4 --proxy-headers
Restart=always
RestartSec=2s
MemoryMax=512M
LimitNOFILE=65536
TasksMax=512

[Install]
WantedBy=multi-user.target

启用：
sudo chown -R biance:biance /opt/biance
sudo systemctl daemon-reload
sudo systemctl enable --now biance
sudo systemctl status biance -n 50

4) Nginx 反代（推荐，/etc/nginx/sites-available/biance.conf）：
server {
    listen 80;
    server_name _;

    location / {
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_pass http://127.0.0.1:8000;
    }

    # 可选：为 /fapi/v1/klines 增加频控（需在 http{} 里先定义 limit_req_zone）
    # location /fapi/v1/klines {
    #     limit_req zone=api_rate burst=200 nodelay;
    #     proxy_pass http://127.0.0.1:8000;
    # }
}

启用并重载：
sudo ln -s /etc/nginx/sites-available/biance.conf /etc/nginx/sites-enabled/
sudo nginx -t && sudo systemctl reload nginx

C. Docker / Compose（可选）
docker build -t biance:latest .
docker run --rm -p 8000:8000 --env-file .env biance:latest
# 或
docker compose up -d --build

-------------------------------------
六、并发与性能建议
-------------------------------------
- 进程模型：多进程（--workers = CPU 核数），50+ 并发只读稳妥；200 并发建议多实例或水平扩展。
- SQLite 调参：
  PRAGMA journal_mode=WAL;
  PRAGMA synchronous=NORMAL;
  PRAGMA busy_timeout=5000;
  写入批量控制在 1k~5k/事务，缩短持锁时间。
- HTTP 弱缓存：ETag + Cache-Control 已内建；结合 Nginx proxy_cache（10s）命中率更高。
- Nginx 频控：limit_req zone=api_rate rate=60r/s；在异常流量下保护后端。
- 进一步扩展：Redis 作为 L2 缓存；Postgres + asyncpg；多实例水平扩展 + 共享缓存。

-------------------------------------
七、运维与监控
-------------------------------------
- /metrics：Prometheus 拉取，关注 QPS、P95/P99、5xx。
- /v1/health：健康检查。
- 日志：关键路径有结构化日志，建议接入集中化日志。

-------------------------------------
八、安全建议
-------------------------------------
- POST /v1/admin/symbols/refresh 仅内网可用或反代层加鉴权。
- 对外仅开放 GET /fapi/v1/klines、/v1/health；/metrics 建议内网可见。
- 公网部署请开启 TLS/HTTP/2，并配合 WAF/速率限制。

-------------------------------------
九、常见问题
-------------------------------------
- 304 的正确使用：首次拿到 ETag，二次把 ETag 放到 If-None-Match 命中则 304。
- 没有交易对：冷启动用 .env 的 SYMBOLS；后台同步成功后自动替换。
- 锁等待/超时：提高 busy_timeout、缩小写入事务、加缓存/多进程。
- 入口导入失败：先 cd 到 biance-main，再运行；入口尝试 main:app 或 app.main:app。

-------------------------------------
十、未来路线图
-------------------------------------
- 数据层升级：Postgres（分区表/并行扫描）+ Redis 缓存。
- 更多端点：批量多符号查询、滚动窗口聚合接口。
- 权限与配额：按 key 限流/计费。
- 可观测性增强：Tracing（OpenTelemetry）、更细粒度业务指标。

许可：
仅供技术研究与内部系统集成使用；与 Binance 官方无关。请遵守所在司法辖区与数据来源的使用条款。

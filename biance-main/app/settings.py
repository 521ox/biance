from typing import List, Optional
try:
    from pydantic_settings import BaseSettings
except Exception:  # pragma: no cover
    from pydantic import BaseModel, ConfigDict

    class BaseSettings(BaseModel):  # type: ignore
        model_config = ConfigDict(populate_by_name=True)
from pydantic import Field, field_validator

class Settings(BaseSettings):
    symbols: List[str] = Field(default_factory=lambda: ["BTCUSDT","ETHUSDT"], alias="SYMBOLS")
    intervals: List[str] = Field(
        default_factory=lambda: ["1m","3m","5m","15m","1h","4h","1d"],
        alias="INTERVALS",
    )
    backfill_days: int = Field(default=365, alias="BACKFILL_DAYS")
    auto_sync_symbols: bool = Field(default=True, alias="AUTO_SYNC_SYMBOLS")
    symbol_sync_interval_sec: int = Field(default=300, alias="SYMBOL_SYNC_INTERVAL_SEC")
    quote_assets: List[str] = Field(default_factory=lambda: ["USDT"], alias="QUOTE_ASSETS")
    cache_ttl_sec_klines: int = Field(default=10, alias="CACHE_TTL_SEC_KLINES")
    cache_url: Optional[str] = Field(default=None, alias="CACHE_URL")

    # --- new configuration fields ---
    log_level: str = Field("INFO", alias="LOG_LEVEL")
    db_url: str = Field("sqlite:///data/klines.db", alias="DB_URL")
    db_pool_size: int = Field(10, alias="DB_POOL_SIZE")
    binance_base: str = Field("https://fapi.binance.com", alias="BINANCE_BASE")
    enable_fetcher: bool = Field(True, alias="ENABLE_FETCHER")
    enable_aggregator: bool = Field(True, alias="ENABLE_AGGREGATOR")
    cache_ttl_ms_klines: int = Field(60_000, alias="CACHE_TTL_MS_KLINES")
    fetch_concurrency: int = Field(8, alias="FETCH_CONCURRENCY")
    init_backfill_days: int = Field(0, alias="INIT_BACKFILL_DAYS")
    backfill_pull_4h: bool = Field(False, alias="BACKFILL_PULL_4H")
    init_pull_4h: Optional[int] = Field(None, alias="INIT_PULL_4H")
    init_pull_1m: Optional[int] = Field(None, alias="INIT_PULL_1M")

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False
        extra = "allow"

    @field_validator("symbols", "intervals", mode="before")
    @classmethod
    def _csv_to_list(cls, v):
        if isinstance(v, str):
            return [x.strip() for x in v.split(",") if x.strip()]
        return v

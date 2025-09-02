from typing import List
try:
    from pydantic_settings import BaseSettings
except Exception:  # pragma: no cover
    from pydantic import BaseSettings  # type: ignore
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
    fetch_concurrency: int = Field(default=1, alias="FETCH_CONCURRENCY")

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

from dataclasses import dataclass
from enum import Enum

class Interval(str, Enum):
    m1="1m"; m3="3m"; m5="5m"; m15="15m"; h1="1h"; h4="4h"; d1="1d"

@dataclass(frozen=True)
class Bar:
    symbol: str
    interval: Interval
    open_time: int
    open: float; high: float; low: float; close: float
    volume: float; quote_volume: float
    close_time: int
    trades: int = 0
    taker_buy_base: float = 0.0
    taker_buy_quote: float = 0.0
    is_final: bool = True

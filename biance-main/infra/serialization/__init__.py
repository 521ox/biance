import orjson
from typing import Iterable, List
from domain.models import Bar

def serialize_binance_klines(bars: Iterable[Bar]) -> bytes:
    out: List[list] = []
    for b in bars:
        out.append([
            b.open_time,
            f"{b.open}",
            f"{b.high}",
            f"{b.low}",
            f"{b.close}",
            f"{b.volume}",
            b.close_time,
            f"{b.quote_volume}",
            b.trades,
            f"{b.taker_buy_base}",
            f"{b.taker_buy_quote}",
            "0",
        ])
    return orjson.dumps(out)

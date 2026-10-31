"""Unit-safe liquidity helpers for OKX linear USDT swaps."""
from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any


def _f(value: Any) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def quote_volume_24h(ticker: dict[str, Any]) -> float:
    """Return quote notional without treating contracts/base units as USD."""
    explicit = _f(ticker.get("quoteVolume24h"))
    if explicit > 0.0:
        return explicit
    last = _f(ticker.get("last"))
    base_volume = _f(ticker.get("volCcy24h"))
    return last * base_volume if last > 0.0 and base_volume > 0.0 else 0.0


def spread_pct(ticker: dict[str, Any]) -> float | None:
    bid = _f(ticker.get("bidPx", ticker.get("bid")))
    ask = _f(ticker.get("askPx", ticker.get("ask")))
    if bid <= 0.0 or ask <= 0.0 or ask < bid:
        return None
    mid = (bid + ask) / 2.0
    return ((ask - bid) / mid) * 100.0 if mid > 0.0 else None


@dataclass(frozen=True)
class LiquiditySnapshot:
    quote_volume_24h_usdt: float
    spread_pct: float | None
    volume_available: bool
    spread_available: bool
    source: str = "okx_ticker"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def snapshot(ticker: dict[str, Any]) -> LiquiditySnapshot:
    volume = quote_volume_24h(ticker)
    spread = spread_pct(ticker)
    return LiquiditySnapshot(volume, spread, volume > 0.0, spread is not None)

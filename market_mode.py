"""
OxyPulse — market_mode.py
Futures-only mode. Spot trading removed entirely.
All instruments are perpetual swaps on OKX.
"""
from __future__ import annotations
import os
import logging

logger = logging.getLogger("OxyPulse.MarketMode")

FUTURES = "futures"

_leverage:    int = int(os.getenv("FUTURES_LEVERAGE", "5"))
_margin_mode: str = os.getenv("FUTURES_MARGIN_MODE", "isolated").lower()


def get_mode()       -> str:  return FUTURES
def is_futures()     -> bool: return True
def is_spot()        -> bool: return False
def get_leverage()   -> int:  return _leverage
def get_margin_mode()-> str:  return _margin_mode


def set_leverage(lev: int):
    global _leverage
    _leverage = max(1, min(lev, 125))
    logger.info("Leverage → %dx", _leverage)


def set_margin_mode(mode: str):
    global _margin_mode
    if mode.lower() in ("cross", "isolated"):
        _margin_mode = mode.lower()
        logger.info("Margin mode → %s", _margin_mode)


def inst_id(coin: str) -> str:
    """Always returns perpetual swap instrument ID."""
    return f"{coin.upper()}-USDT-SWAP"


def td_mode() -> str:
    return _margin_mode


def mode_label() -> str:
    return f"📈 FUTURES {_leverage}x {_margin_mode.upper()}"


def mode_emoji() -> str:
    return "📈"

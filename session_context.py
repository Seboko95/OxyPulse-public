"""
OxyPulse — session_context.py
Tracks which trading session is active and detects London/NY sweep patterns.

Sessions (UTC):
  ASIA    →  00:00 – 07:00   Low volume, tight ranges, higher fakeout rate
  LONDON  →  07:00 – 13:00   First big liquidity injection — sweeps Asia ranges
  NY      →  13:00 – 21:00   Highest volume, strongest breakouts
  OFF     →  21:00 – 00:00   Low activity

Key feature — London Sweep Detector:
  1. Records Asia session high/low (00:00–06:59 UTC)
  2. At London open (07:00–09:00), watches if price wicks beyond Asia range
  3. If price closes BACK inside Asia range → sweep confirmed → fade signal

Session sizing rules applied upstream:
  ASIA    → 70% of normal size (choppier, more fakeouts)
  LONDON  → 100% of normal size (high quality signals)
  NY      → 110% of normal size (strongest follow-through)
  OFF     → 50% of normal size (thin liquidity)
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from enum import Enum

logger = logging.getLogger("OxyPulse.Session")

# ─── Session boundaries (UTC hour, inclusive start) ────────────────────────
_SESSIONS = [
    (0,  7,  "ASIA"),
    (7,  13, "LONDON"),
    (13, 21, "NY"),
    (21, 24, "OFF"),
]

# ─── Size multipliers per session ──────────────────────────────────────────
SESSION_SIZE_MULT = {
    "ASIA":   0.70,
    "LONDON": 1.00,
    "NY":     1.10,
    "OFF":    0.50,
}

# ─── TP multiplier per session (NY gets wider TPs — stronger follow-through)
SESSION_TP_MULT = {
    "ASIA":   0.80,   # tighter TP — choppier, less follow-through
    "LONDON": 1.00,
    "NY":     1.20,   # wider TP — NY moves have legs
    "OFF":    0.80,
}

# ─── State ─────────────────────────────────────────────────────────────────
_asia_highs: dict[str, float] = {}   # {coin: high during Asia session}
_asia_lows:  dict[str, float] = {}   # {coin: low during Asia session}
_sweep_coins: set[str]        = set() # coins where London sweep detected this session
_last_session: str            = ""


class Session(str, Enum):
    ASIA   = "ASIA"
    LONDON = "LONDON"
    NY     = "NY"
    OFF    = "OFF"


def get_session() -> str:
    """Return current session name based on UTC hour."""
    hour = datetime.now(timezone.utc).hour
    for start, end, name in _SESSIONS:
        if start <= hour < end:
            return name
    return "OFF"


def get_session_emoji() -> str:
    s = get_session()
    return {"ASIA": "🌏", "LONDON": "🇬🇧", "NY": "🗽", "OFF": "🌙"}.get(s, "⏸")


def get_size_multiplier() -> float:
    """Return position size multiplier for the current session."""
    return SESSION_SIZE_MULT.get(get_session(), 1.0)


def get_tp_multiplier() -> float:
    """Return TP distance multiplier for the current session."""
    return SESSION_TP_MULT.get(get_session(), 1.0)


def is_london_open_window() -> bool:
    """True during the high-risk London sweep window (07:00–09:00 UTC)."""
    hour = datetime.now(timezone.utc).hour
    return 7 <= hour < 9


def is_ny_open_window() -> bool:
    """True during the NY open window (13:00–15:00 UTC)."""
    hour = datetime.now(timezone.utc).hour
    return 13 <= hour < 15


def update_session_range(coin: str, high: float, low: float) -> None:
    """
    Call every candle. Tracks Asia session high/low.
    Resets when session changes.
    """
    global _last_session, _sweep_coins
    session = get_session()

    # Session transition — reset tracking
    if session != _last_session:
        if session == "LONDON":
            # Moving into London — lock in Asia ranges
            logger.debug(
                "Session → LONDON | Asia ranges locked: %d coins tracked",
                len(_asia_highs),
            )
        elif session == "ASIA":
            # New Asia session — reset sweep state
            _asia_highs.clear()
            _asia_lows.clear()
            _sweep_coins.clear()
            logger.debug("Session → ASIA | Ranges reset.")
        _last_session = session

    if session == "ASIA":
        if coin not in _asia_highs or high > _asia_highs[coin]:
            _asia_highs[coin] = high
        if coin not in _asia_lows or low < _asia_lows[coin]:
            _asia_lows[coin] = low


def check_london_sweep(coin: str, current_price: float, candle_high: float, candle_low: float) -> str | None:
    """
    Detects the London sweep reversal pattern.

    Logic:
    - Only active during London open window (07:00–09:00 UTC)
    - If candle wick exceeds Asia high AND candle closes back inside → SHORT signal (fade the sweep)
    - If candle wick exceeds Asia low AND candle closes back inside → LONG signal (fade the sweep)

    Returns: "LONG", "SHORT", or None
    """
    if not is_london_open_window():
        return None
    if coin in _sweep_coins:
        return None  # Already fired for this coin this session

    asia_high = _asia_highs.get(coin)
    asia_low  = _asia_lows.get(coin)

    if not asia_high or not asia_low:
        return None

    # Sweep of Asia HIGH → price wicked above then closed back below
    if candle_high > asia_high and current_price < asia_high:
        _sweep_coins.add(coin)
        logger.info(
            "🔴 London sweep HIGH detected on %s | Asia high=%.6f wick=%.6f close=%.6f → SHORT",
            coin, asia_high, candle_high, current_price,
        )
        return "SHORT"

    # Sweep of Asia LOW → price wicked below then closed back above
    if candle_low < asia_low and current_price > asia_low:
        _sweep_coins.add(coin)
        logger.info(
            "🟢 London sweep LOW detected on %s | Asia low=%.6f wick=%.6f close=%.6f → LONG",
            coin, asia_low, candle_low, current_price,
        )
        return "LONG"

    return None


def get_asia_range(coin: str) -> tuple[float | None, float | None]:
    """Returns (asia_high, asia_low) for the coin, or (None, None) if not tracked."""
    return _asia_highs.get(coin), _asia_lows.get(coin)


def session_summary() -> str:
    """Human-readable session status for Telegram."""
    session = get_session()
    emoji   = get_session_emoji()
    mult    = get_size_multiplier()
    tp_mult = get_tp_multiplier()
    window  = ""
    if is_london_open_window():
        window = " ⚠️ _London sweep window_"
    elif is_ny_open_window():
        window = " ⚡ _NY open window_"
    hour    = datetime.now(timezone.utc).hour
    return (
        f"{emoji} *Session:* `{session}`{window}\n"
        f"  UTC hour: `{hour:02d}:xx` | Size mult: `{mult:.0%}` | TP mult: `{tp_mult:.0%}`\n"
        f"  Asia ranges tracked: `{len(_asia_highs)} coins`"
    )

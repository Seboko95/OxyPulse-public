"""
OxyPulse — entry_filter.py  v4
Filter stack for main engine trades.

Hard blocks (trade rejected):
  1. Blackswan halt     — BTC moved >15% in 1H
  2. 3-5-7 Rule         — total open risk ≥ 5%
  3. HTF trend (soft)   — only blocks if price >1% BEYOND 200 EMA, not at it
  4. Funding rate       — extreme funding in wrong direction
  5. OI minimum         — skip coins with OI < oi_min_usd (thin market / slippage)
  6. Session filter     — only blocks true dead zone (00:00–02:00 UTC on weekends)

Soft / logging only:
  8. OI context         — logs warning but does NOT block entry
"""
from __future__ import annotations
import logging
import time
from datetime import datetime, timezone

import settings_store as _s
import performance_tracker as _perf
import regime_detector as _regime
from indicators import ema
from market_data_stream import get_4h_closes, has_4h_data

logger = logging.getLogger("OxyPulse.EntryFilter")


def is_soft_reason(reason: str) -> bool:
    """Return True if this entry_filter failure is a soft (bypassable) reason.

    Soft = can be overridden by the soft-accept system if confidence is high.
    Hard (blackswan, 3-5-7 open risk) are never bypassable.
    """
    r = reason.lower()
    return any(kw in r for kw in (
        "htf:",        # HTF trend filter
        "funding ",    # funding rate filter
        "oi $",        # OI minimum (thin market)
        "oi below",    # OI minimum alternate phrasing
        "open interest",
        "session",     # session / dead zone filter
        "weekend",     # weekend dead zone
    ))


async def check(
    coin:         str,
    direction:    str,
    balance:      float,
    funding:      float = 0.0,
    oi_change:    float = 0.0,
    oi_now:       float = 0.0,
    oi_prev:      float = 0.0,
    price_now:    float = 0.0,
    price_prev:   float = 0.0,
    sl_price:     float = 0.0,
    open_trades:  dict  = None,
    capital_mode: str   = "",   # "MICRO" | "GROWTH" | "PRO" — MICRO gets relaxed thresholds
    candidate_quality: float = 0.0,
    confidence_score: float = 0.0,
) -> tuple[bool, str]:
    """Returns (allowed, reason). allowed=True → proceed with trade."""
    _micro = (capital_mode == "MICRO")

    # 1. Blackswan halt
    if _perf.is_blackswan_halted():
        return False, "Blackswan halt — BTC moved >15% recently."

    # 2. 3-5-7 Rule — total open risk cap
    if _s.get_val("rule_357_enabled", True):
        allowed, reason = _check_357(balance, open_trades or {})
        if not allowed:
            return False, reason

    # Participation master gate: when disabled, skip the quality-layer
    # entry filters but keep the hard survival checks above intact.
    if not _s.get_val("entry_filter_enabled", True):
        return True, "OK"

    # 4. HTF trend filter — only blocks if clearly on wrong side (MICRO: 2.5% buffer, else 1%)
    if _s.get_val("htf_filter_enabled", True):
        allowed, reason = _check_htf_soft(coin, direction, micro_mode=_micro)
        if not allowed:
            return False, reason

    # 5. Funding rate — hard block on extremes (MICRO: 2× looser thresholds)
    if _s.get_val("funding_filter_enabled", True):
        allowed, reason = _check_funding(direction, funding, micro_mode=_micro)
        if not allowed:
            return False, reason

    # 6. OI minimum — skip thin markets (MICRO: 50% lower threshold)
    if oi_now > 0:
        oi_min = float(_s.get_val("oi_min_usd", 500_000))
        if _micro:
            oi_min *= 0.5   # MICRO: allow smaller markets (liquidity less critical)
        if oi_now < oi_min:
            return False, f"OI ${oi_now:,.0f} below minimum ${oi_min:,.0f}."

    # 7. Session filter — only blocks true dead zone
    if _s.get_val("session_filter_enabled", True):
        allowed, reason = _check_session_dead_zone()
        if not allowed:
            return False, reason

    # 8. OI context — SOFT only (warning, no block)
    if oi_now > 0 and price_now > 0:
        ctx = _classify_oi(oi_now, oi_prev, price_now, price_prev)
        if (direction == "long"  and ctx == "LONGS_CLOSING") or \
           (direction == "short" and ctx == "SHORTS_CLOSING"):
            logger.debug("[%s] OI warning: %s — proceeding anyway.", coin, ctx)

    return True, "OK"


# ── Implementations ───────────────────────────────────────────────────────────

def _check_357(balance: float, open_trades: dict) -> tuple[bool, str]:
    if not open_trades or balance <= 0:
        return True, "OK"
    max_open = float(_s.get_val("rule_357_max_open_risk_pct", 5.0))
    total    = 0.0
    for coin, trade in open_trades.items():
        size  = float(trade.get("size_usdt", trade.get("size", 0)) or 0)
        lev   = float(trade.get("leverage", 1))
        entry = float(trade.get("entry", 0))
        sl    = float(trade.get("stop_loss", 0))
        if entry <= 0 or sl <= 0 or size <= 0:
            # Fallback: estimate risk using min SL distance (conservative but not extreme).
            # Avoids treating full notional as at-risk which would lock out all new trades.
            _est_sl_pct = float(_s.get_val("min_sl_dist_pct", 1.0))
            total += (size * _est_sl_pct / 100) / balance * 100
            continue
        sl_dist      = abs(entry - sl) / entry * 100
        # size is notional (already includes leverage) — do NOT multiply by lev again
        pos_risk_pct = (size * sl_dist / 100) / balance * 100
        total       += pos_risk_pct
    if total >= max_open:
        return False, f"3-5-7: open risk {total:.1f}% >= {max_open}% cap."
    return True, "OK"


def _check_htf_soft(coin: str, direction: str, micro_mode: bool = False) -> tuple[bool, str]:
    """
    Softer HTF filter: only blocks if price is MORE THAN buffer% beyond 200 EMA.
    Standard: 1% buffer.  MICRO mode: 2.5% buffer (allows more entries against trend).
    Old version blocked at 0.5% — too tight, killed many valid entries.
    """
    if not has_4h_data(coin):
        return True, "OK"
    closes_4h = get_4h_closes(coin)
    if len(closes_4h) < 200:
        return True, "OK"
    e200_list = ema(closes_4h, 200)
    if not e200_list:
        return True, "OK"
    e200  = e200_list[-1]
    price = closes_4h[-1]
    buf   = 0.025 if micro_mode else 0.01   # MICRO: wider allowance
    if direction == "long"  and price < e200 * (1.0 - buf):
        return False, f"HTF: price {price:.4f} > {buf*100:.0f}% below 200 EMA {e200:.4f}"
    if direction == "short" and price > e200 * (1.0 + buf):
        return False, f"HTF: price {price:.4f} > {buf*100:.0f}% above 200 EMA {e200:.4f}"
    return True, "OK"


def _check_funding(direction: str, funding: float, micro_mode: bool = False) -> tuple[bool, str]:
    mult      = 2.0 if micro_mode else 1.0   # MICRO: 2× looser funding thresholds
    long_max  = float(_s.get_val("funding_long_max",  0.001)) * mult
    short_min = float(_s.get_val("funding_short_min", -0.001)) * mult
    if direction == "long"  and funding > long_max:
        return False, f"Funding {funding*100:.3f}% too positive for long."
    if direction == "short" and funding < short_min:
        return False, f"Funding {funding*100:.3f}% too negative for short."
    return True, "OK"


def _check_session_dead_zone() -> tuple[bool, str]:
    """
    Only block during true low-liquidity dead zone: Sat/Sun 00:00–02:00 UTC.
    Old version blocked 21:00–00:00 UTC every day — too aggressive.
    """
    now     = datetime.now(timezone.utc)
    hour    = now.hour
    weekday = now.weekday()   # 5=Sat, 6=Sun

    is_weekend_dead = (weekday in (5, 6)) and (0 <= hour < 2)
    if is_weekend_dead:
        return False, "Weekend dead zone (00:00–02:00 UTC Sat/Sun)."
    return True, "OK"


def _classify_oi(oi_now, oi_prev, price_now, price_prev) -> str:
    if oi_prev <= 0 or price_prev <= 0:
        return "NEUTRAL"
    oi_chg    = (oi_now - oi_prev) / oi_prev * 100
    price_chg = (price_now - price_prev) / price_prev * 100
    oi_up   = oi_chg    >  0.5
    oi_dn   = oi_chg    < -0.5
    px_up   = price_chg >  0.1
    px_dn   = price_chg < -0.1
    if oi_up and px_up:   return "LONGS_OPENING"
    if oi_up and px_dn:   return "SHORTS_OPENING"
    if oi_dn and px_up:   return "SHORTS_CLOSING"
    if oi_dn and px_dn:   return "LONGS_CLOSING"
    return "NEUTRAL"


def check_blackswan_btc(btc_1h_move_pct: float) -> bool:
    threshold = float(_s.get_val("blackswan_btc_pct", 15.0))
    if abs(btc_1h_move_pct) >= threshold:
        _perf.set_blackswan(True)
        logger.critical("BLACKSWAN: BTC moved %.1f%% in 1H — halting!", btc_1h_move_pct)
        return True
    return False


def clear_blackswan() -> None:
    _perf.set_blackswan(False)
    logger.info("Blackswan halt cleared.")

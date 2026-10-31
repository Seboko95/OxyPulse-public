"""
OxyPulse — regime_detector.py
Classifies current market regime every 15 minutes using:
  - ADX(14)          — trend strength
  - Bollinger Width  — volatility compression/expansion
  - Hurst exponent   — trending vs mean-reverting character
  - Volume ratio     — activity level

Regimes:
  TRENDING   — ADX>25, Hurst>0.55, BBW expanding → trend-follow strategies
  RANGING    — ADX<20, Hurst<0.5,  BBW tight     → mean-reversion, funding
  VOLATILE   — Large ATR spikes, BBW very wide    → reduce size, liq hunt only
  DEAD       — Very low volume + ATR              → skip trading

Reference coin for regime classification: BTC (most liquid, sets market tone).
"""
from __future__ import annotations
import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

from indicators import adx, bollinger_band_width, hurst_exponent, atr, avg_volume
from market_data_stream import (
    get_5m_closes, get_5m_highs, get_5m_lows, get_5m_volumes, fetch_candles_5m,
    completed_5m_series, timeframe_snapshot,
)
# FIX 2026-03-27: was using get_closes/highs/lows/volumes (1m cache) which is only
# copied from 5m on cold-start. After first populate, _cache_1m["BTC"] never
# refreshes via fetch_candles_5m, causing regime to freeze at startup values.
# Now using get_5m_* accessors which always read the freshly fetched 5m cache.

logger = logging.getLogger("OxyPulse.Regime")

TRENDING  = "TRENDING"
RANGING   = "RANGING"
VOLATILE  = "VOLATILE"
DEAD      = "DEAD"
UNKNOWN   = "UNKNOWN"

SCAN_INTERVAL = 900   # 15 minutes


@dataclass
class RegimeState:
    regime:     str   = UNKNOWN
    adx_val:    float = 0.0
    bbw_val:    float = 0.0
    hurst_val:  float = 0.5
    atr_val:    float = 0.0
    vol_ratio:  float = 1.0
    updated_at: str   = ""
    readiness:  str   = "WARMING_UP"
    reason:     str   = "not_initialized"
    sample_count: int = 0
    latest_candle_ts: float = 0.0
    forming_candle_excluded: bool = False
    confidence: int   = 0    # 0–100


_state      = RegimeState()
_task: Optional[asyncio.Task] = None
_ref_coin   = "BTC"


def get_regime()     -> str:       return _state.regime
def get_state()      -> RegimeState: return _state
def is_trending()    -> bool:      return _state.regime == TRENDING
def is_ranging()     -> bool:      return _state.regime == RANGING
def is_volatile()    -> bool:      return _state.regime == VOLATILE
def is_dead()        -> bool:      return _state.regime == DEAD


def regime_emoji() -> str:
    return {
        TRENDING: "📈", RANGING: "↔️",
        VOLATILE: "⚡", DEAD: "💤", UNKNOWN: "❓",
    }.get(_state.regime, "❓")


def classify_from_data(
    closes: list[float],
    highs:  list[float],
    lows:   list[float],
    volumes:list[float],
) -> RegimeState:
    """Run full classification on provided candle data. Returns RegimeState."""
    state = RegimeState(updated_at=datetime.now(timezone.utc).isoformat())

    if len(closes) < 50 or not highs or not lows:
        state.regime = UNKNOWN
        state.readiness = "WARMING_UP"
        state.reason = "insufficient_5m_samples"
        state.sample_count = len(closes)
        return state

    adx_val  = adx(highs, lows, closes, 14)   or 0.0
    bbw_val  = bollinger_band_width(closes, 20) or 0.0
    hurst    = hurst_exponent(closes)           or 0.5
    atr_val  = atr(highs, lows, closes, 14)    or 0.0
    curr_vol = volumes[-1] if volumes else 0.0
    avg_vol  = avg_volume(volumes, 20)
    vol_ratio = curr_vol / avg_vol if avg_vol > 0 else 1.0

    state.adx_val   = round(adx_val, 2)
    state.bbw_val   = round(bbw_val, 4)
    state.hurst_val = hurst
    state.atr_val   = round(atr_val, 8)
    state.vol_ratio = round(vol_ratio, 2)
    state.readiness = "READY"
    state.reason = "classified"
    state.sample_count = len(closes)

    price    = closes[-1]
    atr_pct  = (atr_val / price * 100) if price > 0 else 0.0

    # DEAD: very low volume and tiny ATR
    if vol_ratio < 0.3 and atr_pct < 0.05:
        state.regime     = DEAD
        state.confidence = 80
        state.reason = "btc_low_volume_and_tiny_atr"
        return state

    # VOLATILE: very high ATR or extreme volume spike
    if atr_pct > 2.0 or vol_ratio > 5.0:
        state.regime     = VOLATILE
        state.confidence = 85
        state.reason = "btc_atr_or_volume_spike"
        return state

    # TRENDING: strong ADX + Hurst above 0.55
    if adx_val > 25 and hurst > 0.53:
        conf = min(100, int(adx_val * 2))
        state.regime     = TRENDING
        state.confidence = conf
        state.reason = "btc_adx_hurst_trending"
        return state

    # RANGING: weak ADX + Hurst below 0.5 + tight BBW
    if adx_val < 22 and hurst < 0.52:
        state.regime     = RANGING
        state.confidence = min(100, int((22 - adx_val) * 5))
        state.reason = "btc_adx_hurst_bbw_ranging"
        return state

    # Mixed signal — lean toward Hurst when strongly persistent, else lean toward ADX.
    # FIX 2026-03-27: Hurst > 0.70 = strongly trending character; override the ADX
    # threshold in this zone. Observed: ADX=21-22 with Hurst=0.93-0.95 was being
    # labelled RANGING, incorrectly silencing RYUU+ARASHI during trending conditions.
    if adx_val >= 22 or hurst > 0.70:
        state.regime     = TRENDING
        state.confidence = 50
        state.reason = "btc_mixed_lean_trending"
    else:
        state.regime     = RANGING
        state.confidence = 50
        state.reason = "btc_mixed_lean_ranging"

    return state


async def update_regime(coin: str = "BTC") -> RegimeState:
    """Reclassify from the maintained 5m cache without network side effects."""
    global _state
    completed = completed_5m_series(coin)
    closes  = completed.get("closes", [])
    highs   = completed.get("highs", [])
    lows    = completed.get("lows", [])
    volumes = completed.get("volumes", [])
    _state  = classify_from_data(closes, highs, lows, volumes)
    readiness = timeframe_snapshot(coin, "5m", 50)
    _state.latest_candle_ts = float(readiness.get("latest_timestamp", 0.0) or 0.0)
    _state.forming_candle_excluded = bool(readiness.get("forming_candle_present", False))
    if readiness.get("reason"):
        _state.regime = UNKNOWN
        _state.readiness = "WARMING_UP" if "insufficient" in str(readiness["reason"]) else "RESTRICTED"
        _state.reason = str(readiness["reason"])
    logger.info(
        "Regime → %s (ADX=%.1f BBW=%.4f Hurst=%.2f conf=%d%%)",
        _state.regime, _state.adx_val, _state.bbw_val,
        _state.hurst_val, _state.confidence,
    )
    return _state


async def start_regime_monitor(ref_coin: str = "BTC") -> None:
    """Background loop that refreshes regime every 15 minutes."""
    global _task, _ref_coin
    _ref_coin = ref_coin
    if _task and not _task.done():
        return
    _task = asyncio.create_task(_regime_loop())
    logger.info("Regime monitor started (ref=%s, interval=%ds)", ref_coin, SCAN_INTERVAL)


async def stop_regime_monitor() -> None:
    """Cancel the regime monitor background task cleanly."""
    global _task
    if _task and not _task.done():
        _task.cancel()
        try:
            await _task
        except asyncio.CancelledError:
            pass
    _task = None
    logger.info("Regime monitor stopped.")


async def _regime_loop():
    while True:
        try:
            await update_regime(_ref_coin)
        except asyncio.CancelledError:
            logger.info("Regime monitor shutting down.")
            raise
        except Exception as e:
            logger.error("Regime update error: %s", e)
        try:
            await asyncio.sleep(SCAN_INTERVAL)
        except asyncio.CancelledError:
            logger.info("Regime monitor shutting down.")
            raise


def format_regime() -> str:
    s = _state
    return (
        f"{regime_emoji()} *Market Regime: `{s.regime}`* (conf: {s.confidence}%)\n"
        f"  ADX: `{s.adx_val}` | BBW: `{s.bbw_val}` | "
        f"Hurst: `{s.hurst_val}` | Vol×: `{s.vol_ratio}`"
    )

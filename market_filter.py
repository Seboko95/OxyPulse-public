"""
OxyPulse V6.1 — market_filter.py
Pre-trade market quality gate: spread, volume, and volatility checks.

Blocks trades when market conditions are too poor for reliable execution:
  - bid-ask spread too wide (slippage risk)
  - 24h volume too low (illiquid, hard to close)
  - sudden ATR spike (abnormal volatility / manipulation)

This is a lightweight synchronous filter that uses already-fetched data
(ticker dict + computed ATR). It does NOT make additional exchange calls.

Returns a structured MarketFilterResult — never raises.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field

import settings_store as _ss
from liquidity_model import snapshot as liquidity_snapshot

logger = logging.getLogger("OxyPulse.V6.MarketFilter")


@dataclass
class MarketFilterResult:
    """Result of the market condition gate."""
    allowed: bool
    reason: str                         # human-readable verdict
    checks: dict = field(default_factory=dict)  # per-check results for telemetry


def check_market(
    coin: str,
    ticker: dict,
    atr_val: float,
    closes: list[float],
    highs: list[float],
    lows: list[float],
) -> MarketFilterResult:
    """Run all market quality checks for a coin.

    Args:
        coin:     Coin ticker (for logging).
        ticker:   OKX ticker dict with keys: last, bidPx, askPx, vol24h, volCcy24h.
        atr_val:  14-period ATR (price units).
        closes:   Recent close prices (for rolling ATR baseline).
        highs:    Recent highs.
        lows:     Recent lows.

    Returns:
        MarketFilterResult with allowed=True only if all checks pass.
    """
    if not _ss.get_val("v6_market_filter_enabled", True):
        return MarketFilterResult(allowed=True, reason="market filter disabled")

    checks: dict = {}
    failures: list[str] = []
    liquidity = liquidity_snapshot(ticker)
    allow_unknown = bool(_ss.get_val("market_filter_allow_unknown_liquidity", False))

    # ── 1. Spread check ───────────────────────────────────────────────────────
    try:
        if liquidity.spread_available:
            spread_pct = float(liquidity.spread_pct or 0.0)
            max_spread = float(_ss.get_val("v6_spread_max_pct", 0.30))
            checks["spread_pct"] = round(spread_pct, 4)
            checks["spread_ok"] = spread_pct <= max_spread
            if not checks["spread_ok"]:
                failures.append(f"spread {spread_pct:.3f}% > max {max_spread:.2f}%")
        else:
            checks["spread_ok"] = allow_unknown
            checks["spread_unavailable"] = True
            if not allow_unknown:
                failures.append("spread unavailable")
    except Exception:
        checks["spread_ok"] = allow_unknown
        checks["spread_unavailable"] = True
        if not allow_unknown:
            failures.append("spread unavailable")

    # ── 2. 24h volume check ───────────────────────────────────────────────────
    try:
        vol_usd = float(liquidity.quote_volume_24h_usdt or 0.0)
        min_vol = float(_ss.get_val("v6_min_volume_usd_24h", 2_000_000.0))
        checks["vol24h_usd"] = round(vol_usd, 0)
        checks["vol_ok"] = vol_usd >= min_vol if vol_usd > 0 else allow_unknown
        if vol_usd <= 0:
            checks["volume_unavailable"] = True
            if not allow_unknown:
                failures.append("24h quote volume unavailable")
        elif not checks["vol_ok"]:
            failures.append(f"24h vol ${vol_usd:,.0f} < min ${min_vol:,.0f}")
    except Exception:
        checks["vol_ok"] = allow_unknown
        checks["volume_unavailable"] = True
        if not allow_unknown:
            failures.append("24h quote volume unavailable")

    # ── 3. ATR volatility spike check ─────────────────────────────────────────
    # Compare current ATR to the rolling average of the last 20 ATR readings.
    # If current ATR > N× rolling avg → abnormal volatility spike → skip.
    try:
        if atr_val > 0 and len(closes) >= 20 and len(highs) >= 20 and len(lows) >= 20:
            from indicators import calc_atr  # use existing indicator
            # Compute a rough "average ATR" over a longer lookback
            _lookback_atrs: list[float] = []
            # Simple rolling: compute ATR on rolling 14-bar windows, last 10 positions
            step = max(1, len(closes) // 10)
            for offset in range(10):
                _i = max(14, len(closes) - offset * step)
                _c = closes[:_i]; _h = highs[:_i]; _l = lows[:_i]
                if len(_c) >= 14:
                    _a = calc_atr(_h, _l, _c, 14) or 0.0
                    if _a > 0:
                        _lookback_atrs.append(_a)
            if _lookback_atrs:
                avg_atr = sum(_lookback_atrs) / len(_lookback_atrs)
                spike_mult = float(_ss.get_val("v6_vol_spike_atr_mult", 3.0))
                checks["atr_ratio"] = round(atr_val / avg_atr, 2) if avg_atr > 0 else 1.0
                checks["atr_spike_ok"] = atr_val <= avg_atr * spike_mult
                if not checks["atr_spike_ok"]:
                    failures.append(
                        f"ATR spike {atr_val:.6f} > {spike_mult:.1f}× avg {avg_atr:.6f}"
                    )
            else:
                checks["atr_spike_ok"] = True
        else:
            checks["atr_spike_ok"] = True
    except Exception:
        checks["atr_spike_ok"] = True

    if failures:
        reason = " | ".join(failures)
        logger.debug("[V6.MarketFilter] %s BLOCKED: %s", coin, reason)
        return MarketFilterResult(allowed=False, reason=reason, checks=checks)

    return MarketFilterResult(allowed=True, reason="all checks passed", checks=checks)

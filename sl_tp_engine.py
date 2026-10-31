"""
OxyPulse — sl_tp_engine.py
Per-engine Stop Loss / Take Profit calculator.

Every engine has a unique SL/TP mechanism that matches how it discovers trades:
  • Structure-based engines: SL beyond last swing high/low
  • Volatility engines: SL at BB outer band / ATR multiple
  • Fixed-percentage engines: SL as a hard % from entry
  • Hybrid engines: combine structure confirmation with ATR fallback

Global rule (enforced on every engine):
  minimum 1:4 Risk-to-Reward Ratio — TP2 is always ≥ 4× SL distance.

Returns:
  (sl_price, tp1_price, tp2_price)

  sl_price   — where to put the stop loss (exchange algo SL order)
  tp1_price  — first partial-take target (1:1.5–2.0 RRR — exit 30–40 %)
  tp2_price  — main take-profit (1:4 RRR minimum — runner or final exit, up to 1:5)

Called from category_engine._place_trade() replacing the inline
SHOGUN/TENJIN overrides and the generic risk.calculate_stop_take().
"""
from __future__ import annotations

import logging

import settings_store as _s
from indicators import (
    atr          as calc_atr,
    ema,
    bollinger_bands,
    vwap,
    swing_low_price,
    swing_high_price,
    get_fib_extension,
    get_zigzag,
)

logger = logging.getLogger("OxyPulse.SLTPEngine")

# ── Safety bounds ─────────────────────────────────────────────────────────────
_MIN_RRR    = 4.0     # Golden Rule: minimum 1:4 RRR — TP2 always ≥ 4× SL distance
_is_long    = lambda side: side in ("buy", "long")


# ── Helpers ───────────────────────────────────────────────────────────────────

def _sl_pct_bounds() -> tuple[float, float]:
    """Return configured SL clamp bounds as decimal percentages."""
    min_pct = max(float(_s.get_val("min_sl_dist_pct", 1.0) or 1.0), 0.0) / 100.0
    max_pct = max(float(_s.get_val("max_sl_dist_pct", 3.0) or 3.0), min_pct) / 100.0
    return min_pct, max_pct


def _clamp_sl_dist(dist: float, entry: float) -> float:
    """Enforce SL distance within configured min_sl_dist_pct/max_sl_dist_pct."""
    min_pct, max_pct = _sl_pct_bounds()
    lo = entry * min_pct
    hi = entry * max_pct
    return max(lo, min(dist, hi))


def _build_result(entry: float, side: str, sl_dist: float,
                  tp1_rr: float = 1.5, tp2_rr: float = 4.0) -> tuple[float, float, float]:
    """
    Given an SL distance (price units), return (sl, tp1, tp2).
    tp1_rr / tp2_rr are Risk:Reward multiples (e.g. 1.0 = 1:1, 3.0 = 1:3).
    Ensures tp2_rr >= _MIN_RRR.
    """
    sl_dist  = _clamp_sl_dist(sl_dist, entry)
    tp1_scale, tp2_scale = _s.get_tp_distance_scales()
    tp1_rr   = max(tp1_rr * tp1_scale, 0.25)
    tp2_rr   = max(tp2_rr * tp2_scale, _MIN_RRR)
    tp1_rr   = min(tp1_rr, tp2_rr)   # tp1 must be below tp2

    if _is_long(side):
        sl  = round(entry - sl_dist, 8)
        tp1 = round(entry + sl_dist * tp1_rr, 8)
        tp2 = round(entry + sl_dist * tp2_rr, 8)
    else:
        sl  = round(entry + sl_dist, 8)
        tp1 = round(entry - sl_dist * tp1_rr, 8)
        tp2 = round(entry - sl_dist * tp2_rr, 8)

    return sl, tp1, tp2


def _atr_fallback(entry: float, side: str, atr_val: float | None,
                  sl_mult: float = 2.3, tp1_rr: float = 1.5,
                  tp2_rr: float = 5.0) -> tuple[float, float, float]:
    """Generic ATR-based fallback when structure data is unavailable."""
    if atr_val and atr_val > 0:
        sl_dist = atr_val * sl_mult
    else:
        sl_dist = entry * 0.018   # 1.8 % default
    return _build_result(entry, side, sl_dist, tp1_rr, tp2_rr)


def _structure_sl_dist(entry: float, side: str,
                       highs: list[float], lows: list[float],
                       lookback: int = 20,
                       buffer_pct: float = 0.002) -> float | None:
    """
    Return distance (price units) from entry to the last swing structure.
    For LONG: SL goes below swing_low  (+ small buffer below it).
    For SHORT: SL goes above swing_high (+ small buffer above it).
    Returns None if no swing is found in the lookback window.
    """
    if _is_long(side):
        sw = swing_low_price(lows, lookback)
        if sw is None or sw <= 0:
            return None
        sl_price = sw * (1 - buffer_pct)          # 0.2 % below the swing low
        dist     = entry - sl_price
    else:
        sw = swing_high_price(highs, lookback)
        if sw is None or sw <= 0:
            return None
        sl_price = sw * (1 + buffer_pct)
        dist     = sl_price - entry
    return dist if dist > 0 else None


# ═══════════════════════════════════════════════════════════════════════════════
# Main public function
# ═══════════════════════════════════════════════════════════════════════════════

def compute(
    engine:   str,
    side:     str,                      # "buy"/"long" or "sell"/"short"
    entry:    float,
    closes:   list[float],
    highs:    list[float],
    lows:     list[float],
    atr_val:  float | None,
    coin:     str = "",
    leverage: int = 1,
    volumes:  list[float] | None = None,
) -> tuple[float, float, float]:
    """
    Compute per-engine (sl_price, tp1_price, tp2_price).

    All prices are absolute (not percentage).
    Minimum 1:4 RRR is guaranteed on tp2 (enforced by _MIN_RRR constant).
    """
    try:
        return _dispatch(engine, side, entry, closes, highs, lows,
                         atr_val, coin, leverage, volumes or [])
    except Exception as exc:
        logger.warning("[SLTPEngine] %s %s: compute error — %s. Using ATR fallback.", engine, coin, exc)
        return _atr_fallback(entry, side, atr_val)


def _dispatch(engine, side, entry, closes, highs, lows, atr_val, coin, leverage, volumes):
    e = engine.upper()

    # ── RYUU ─────────────────────────────────────────────────────────────────
    # Signal logic: EMA trend + RSI momentum.
    # SL: beyond last swing structure.
    # TP: partial exits — 30 % at 1:1.5, 70 % runner at 1:5.
    if e == "RYUU":
        dist = _structure_sl_dist(entry, side, highs, lows, lookback=20)
        if dist is None:
            return _atr_fallback(entry, side, atr_val, sl_mult=2.3, tp1_rr=1.5, tp2_rr=5.0)
        return _build_result(entry, side, dist, tp1_rr=1.5, tp2_rr=5.0)

    # ── RAIJIN ────────────────────────────────────────────────────────────────
    # Signal logic: momentum breakout / RSI surge.
    # SL: structure-based.
    # TP: partial — 40 % at 1:2, runner at 1:5.
    if e == "RAIJIN":
        dist = _structure_sl_dist(entry, side, highs, lows, lookback=20)
        if dist is None:
            return _atr_fallback(entry, side, atr_val, sl_mult=2.3, tp1_rr=2.0, tp2_rr=5.0)
        return _build_result(entry, side, dist, tp1_rr=2.0, tp2_rr=5.0)

    # ── KAGE ──────────────────────────────────────────────────────────────────
    # Signal logic: RSI divergence / candle pattern (shadow play).
    # SL: beyond the current candle's wick (entry-candle structure).
    # TP: 1:1.5 partial, 1:4 runner (wider room for reversal to develop).
    if e == "KAGE":
        if _is_long(side):
            wick_low = lows[-1] if lows else entry
            dist     = entry - wick_low * 0.998   # just below the wick
        else:
            wick_high = highs[-1] if highs else entry
            dist      = wick_high * 1.002 - entry
        if dist <= 0:
            dist = entry * 0.015
        return _build_result(entry, side, dist, tp1_rr=1.5, tp2_rr=4.0)

    # ── ARASHI ───────────────────────────────────────────────────────────────
    # Signal logic: breakout above resistance / below support.
    # SL: just below the broken level (previous structure high/low).
    # TP: partial — 40 % at 1:2, 60 % at 1:5 (breakouts tend to extend).
    if e == "ARASHI":
        # For breakout: SL goes back below the broken swing
        dist = _structure_sl_dist(entry, side, highs, lows, lookback=15,
                                   buffer_pct=0.003)
        if dist is None:
            return _atr_fallback(entry, side, atr_val, sl_mult=2.0, tp1_rr=2.0, tp2_rr=5.0)
        return _build_result(entry, side, dist, tp1_rr=2.0, tp2_rr=5.0)

    # ── MUSASHI ───────────────────────────────────────────────────────────────
    # Signal logic: liquidity sweeps (equal lows / equal highs).
    # SL: beyond the liquidity zone (equal lows swept — SL below them).
    # TP: partial 1:2, runner at 1:5 — structure sweeps can run far.
    if e == "MUSASHI":
        dist = _structure_sl_dist(entry, side, highs, lows, lookback=25,
                                   buffer_pct=0.003)
        if dist is None:
            return _atr_fallback(entry, side, atr_val, sl_mult=2.5, tp1_rr=2.0, tp2_rr=5.0)
        return _build_result(entry, side, dist, tp1_rr=2.0, tp2_rr=5.0)

    # ── HACHIMAN ──────────────────────────────────────────────────────────────
    # Selector-led battlefield ranking. Keep the stop structure-aware and avoid
    # forcing narrow exits; the selector should improve target quality, not clip it.
    if e == "HACHIMAN":
        dist = _structure_sl_dist(entry, side, highs, lows, lookback=24, buffer_pct=0.003)
        if dist is None:
            return _atr_fallback(entry, side, atr_val, sl_mult=2.2, tp1_rr=1.8, tp2_rr=4.5)
        return _build_result(entry, side, dist, tp1_rr=1.8, tp2_rr=4.5)

    # ── YATAGARASU ────────────────────────────────────────────────────────────
    # Premium confluence should trade less often but earn more room when it does fire.
    if e == "YATAGARASU":
        dist = _structure_sl_dist(entry, side, highs, lows, lookback=30, buffer_pct=0.004)
        if dist is None:
            return _atr_fallback(entry, side, atr_val, sl_mult=2.6, tp1_rr=2.2, tp2_rr=6.0)
        return _build_result(entry, side, dist, tp1_rr=2.2, tp2_rr=6.0)

    # ── RYUJIN ────────────────────────────────────────────────────────────────
    # Flow continuation needs breathing room but should still bank momentum efficiently.
    if e == "RYUJIN":
        dist = _structure_sl_dist(entry, side, highs, lows, lookback=18, buffer_pct=0.003)
        if dist is None:
            return _atr_fallback(entry, side, atr_val, sl_mult=2.2, tp1_rr=2.0, tp2_rr=5.5)
        return _build_result(entry, side, dist, tp1_rr=2.0, tp2_rr=5.5)

    # ── KANNON / GENBU ────────────────────────────────────────────────────────
    # Advisory/recovery protectors should never invent exotic exit geometry.
    if e in {"KANNON", "GENBU"}:
        return _atr_fallback(entry, side, atr_val, sl_mult=2.3, tp1_rr=1.5, tp2_rr=4.0)

    # ── SHOGUN ────────────────────────────────────────────────────────────────
    # Signal logic: high-conviction scalp (3/3 vote needed).
    # SL: tight — 0.8× ATR (fast-exit engine).
    # TP: 1:4 from that tight SL — tight SL means TP still has real room.
    if e == "SHOGUN":
        if atr_val and atr_val > 0:
            sl_dist = atr_val * 0.8
        else:
            sl_dist = entry * 0.010   # 1 % tight fallback
        return _build_result(entry, side, sl_dist, tp1_rr=1.5, tp2_rr=4.0)

    # ── TENJIN ────────────────────────────────────────────────────────────────
    # Signal logic: trend-continuation / 4H swing trades.
    # SL/TP: percentage-based from settings (tenjin_sl_pct / tenjin_tp_pct).
    # TP2: 1.6× TP1 distance — wider runner target for swing trades.
    if e == "TENJIN":
        sl_pct  = float(_s.get_val("tenjin_sl_pct", 2.5)) / 100
        tp_pct  = float(_s.get_val("tenjin_tp_pct", 9.0)) / 100   # default matches DEFAULTS (9.0)
        tp2_pct = tp_pct * 1.6
        tp1_scale, tp2_scale = _s.get_tp_distance_scales()
        tp_pct *= tp1_scale
        tp2_pct *= tp2_scale
        # Clamp SL distance to configured min_sl_dist_pct/max_sl_dist_pct before placing the order.
        # Without this, a misconfigured tenjin_sl_pct (e.g. 0.3 %) would sit inside
        # normal noise at 25x and get stopped out every time.
        sl_dist = _clamp_sl_dist(entry * sl_pct, entry)
        if _is_long(side):
            sl  = round(entry - sl_dist, 8)
            tp1 = round(entry + entry * tp_pct, 8)
            tp2 = round(entry + entry * tp2_pct, 8)
        else:
            sl  = round(entry + sl_dist, 8)
            tp1 = round(entry - entry * tp_pct, 8)
            tp2 = round(entry - entry * tp2_pct, 8)
        # Enforce minimum RRR — widen TP2 if it falls short of 1:3
        tp2_dist = abs(entry - tp2)
        if tp2_dist < sl_dist * _MIN_RRR:
            tp2_dist = sl_dist * _MIN_RRR
            tp2 = round(entry + tp2_dist, 8) if _is_long(side) else round(entry - tp2_dist, 8)
        return sl, tp1, tp2

    # ── IZANAGI ───────────────────────────────────────────────────────────────
    # Signal logic: Elliott Wave impulse detection.
    # SL: beyond last significant swing (wider lookback for wave structure).
    # TP: Fibonacci 1.618 extension of Wave 3 projected from Wave 4 end.
    if e == "IZANAGI":
        dist = _structure_sl_dist(entry, side, highs, lows, lookback=30)
        if dist is None:
            return _atr_fallback(entry, side, atr_val, sl_mult=2.5, tp1_rr=1.0, tp2_rr=3.0)
        # Try Fibonacci extension TP
        try:
            zz = get_zigzag(highs, lows, order=3)
            if len(zz) >= 3:
                p_a, p_b, p_c = zz[-3], zz[-2], zz[-1]
                ext = get_fib_extension(p_a[1], p_b[1], p_c[1])
                fib_target = ext.get("1.618", 0.0)
                fib_dist   = abs(fib_target - entry)
                tp2_rr     = fib_dist / dist if dist > 0 else _MIN_RRR
                tp2_rr     = max(tp2_rr, _MIN_RRR)
            else:
                tp2_rr = _MIN_RRR
        except Exception:
            tp2_rr = _MIN_RRR
        return _build_result(entry, side, dist, tp1_rr=1.0, tp2_rr=tp2_rr)

    # ── SUSANOO ───────────────────────────────────────────────────────────────
    # Signal logic: funding squeeze fade.
    # SL: fixed 1.5 % — clean and fast.
    # TP: 1:5 — funding squeezes can produce strong sustained moves.
    if e == "SUSANOO":
        sl_dist = entry * 0.015
        return _build_result(entry, side, sl_dist, tp1_rr=2.0, tp2_rr=5.0)

    # ── FUJIN ─────────────────────────────────────────────────────────────────
    # Signal logic: order-flow / volume imbalance detection.
    # SL: below order-flow zone (swing low / high acts as zone edge).
    # TP: partial — 50 % at 1:2, runner at 1:5 — flow imbalances can run far.
    if e == "FUJIN":
        dist = _structure_sl_dist(entry, side, highs, lows, lookback=20,
                                   buffer_pct=0.003)
        if dist is None:
            return _atr_fallback(entry, side, atr_val, sl_mult=2.0, tp1_rr=2.0, tp2_rr=5.0)
        return _build_result(entry, side, dist, tp1_rr=2.0, tp2_rr=5.0)

    # ── AMATERASU ─────────────────────────────────────────────────────────────
    # Signal logic: higher-timeframe (1H) structure alignment.
    # SL: wider lookback (30 bars) to align with 1H swing structure.
    # TP: 1:2 partial, 1:5 runner — MTF confluence = highest conviction engine.
    if e == "AMATERASU":
        dist = _structure_sl_dist(entry, side, highs, lows, lookback=30,
                                   buffer_pct=0.004)
        if dist is None:
            return _atr_fallback(entry, side, atr_val, sl_mult=2.5, tp1_rr=2.0, tp2_rr=5.0)
        return _build_result(entry, side, dist, tp1_rr=2.0, tp2_rr=5.0)

    # ── TITAN ─────────────────────────────────────────────────────────────────
    # Signal logic: Bollinger Band mean-reversion / squeeze.
    # SL: beyond outer BB band (2.0σ) — price outside band = invalid setup.
    # TP: revert to BB middle (SMA20) or VWAP.
    if e == "TITAN":
        try:
            bb = bollinger_bands(closes, period=20, std_dev=2.0)
            if bb:
                upper, middle, lower = bb[-1]
                if _is_long(side):
                    # Entered near lower band — SL below the band
                    sl_price = lower * 0.997
                    tp_price = middle if middle > entry else entry + (entry - sl_price) * _MIN_RRR
                else:
                    sl_price = upper * 1.003
                    tp_price = middle if middle < entry else entry - (sl_price - entry) * _MIN_RRR
                sl_dist  = abs(entry - sl_price)
                tp2_dist = abs(entry - tp_price)
                tp2_rr   = max(tp2_dist / sl_dist if sl_dist > 0 else _MIN_RRR, _MIN_RRR)
                return _build_result(entry, side, sl_dist, tp1_rr=1.0, tp2_rr=tp2_rr)
        except Exception:
            pass
        return _atr_fallback(entry, side, atr_val, sl_mult=2.0, tp1_rr=2.0, tp2_rr=5.0)

    # ── RONIN ─────────────────────────────────────────────────────────────────
    # Signal logic: candle-pattern breakout (Ronin / lone-wolf breakout bar).
    # SL: below the breakout candle's low (lows[-2] or lows[-1]).
    # TP: partial — 40 % at 1:2, 60 % at 1:5.
    if e == "RONIN":
        if _is_long(side):
            bar_low  = min(lows[-2], lows[-1]) if len(lows) >= 2 else lows[-1]
            sl_price = bar_low * 0.998
            dist     = entry - sl_price
        else:
            bar_high = max(highs[-2], highs[-1]) if len(highs) >= 2 else highs[-1]
            sl_price = bar_high * 1.002
            dist     = sl_price - entry
        if dist <= 0:
            dist = entry * 0.015
        return _build_result(entry, side, dist, tp1_rr=2.0, tp2_rr=5.0)

    # ── OROCHI ────────────────────────────────────────────────────────────────
    # Signal logic: funding rate divergence + OI fade.
    # SL: fixed 1.5 % — simple and clean.
    # TP: 1:5 — funding extremes tend to produce large moves.
    if e == "OROCHI":
        sl_dist = entry * 0.015
        return _build_result(entry, side, sl_dist, tp1_rr=2.0, tp2_rr=5.0)

    # ── TSUNAMI ───────────────────────────────────────────────────────────────
    # Signal logic: liquidation cascade / wave detection.
    # SL: tight 1 % (riding the wave quickly).
    # TP: 1:4 — liq cascades can travel fast but also reverse.
    if e == "TSUNAMI":
        sl_dist = entry * 0.010
        return _build_result(entry, side, sl_dist, tp1_rr=2.0, tp2_rr=4.0)

    # ── SUIJIN ────────────────────────────────────────────────────────────────
    # Signal logic: VWAP deviation / mean reversion toward VWAP.
    # SL: at the VWAP deviation zone (entry is far from VWAP — SL beyond it).
    # TP: VWAP mean (for mean reversion) or 1:3 minimum.
    if e == "SUIJIN":
        try:
            if volumes and len(volumes) >= 10 and len(closes) >= 10 and len(highs) >= 10 and len(lows) >= 10:
                vwap_val = vwap(highs, lows, closes, volumes)
                if vwap_val and vwap_val > 0:
                    vwap_dist = abs(entry - vwap_val)
                    # Guard: if price is within 0.5 % of VWAP there's no meaningful
                    # reversion trade — the SL would be microscopic and at 25x
                    # leverage any noise tick would stop it out.  Fall through to ATR.
                    if vwap_dist < entry * 0.005:
                        raise ValueError("vwap_dist too small for reversion trade")
                    sl_dist = vwap_dist * 1.1    # SL slightly beyond current VWAP deviation
                    # tp1 = halfway back to VWAP (0.5× RR = quick partial exit)
                    # tp2 = enforced ≥ 1:3 by _build_result
                    tp2_rr = max(vwap_dist / sl_dist, _MIN_RRR)   # will be < 1 → clamped to 3.0
                    return _build_result(entry, side, sl_dist, tp1_rr=0.5, tp2_rr=tp2_rr)
        except Exception:
            pass
        return _atr_fallback(entry, side, atr_val, sl_mult=2.0, tp1_rr=1.5, tp2_rr=4.0)

    # ── RAIJUU ────────────────────────────────────────────────────────────────
    # Signal logic: OI divergence — leads to sharp directional moves.
    # SL: structure-based (catches the impulse before it extends).
    # TP: 1:2 partial, 1:5 runner — OI divergence reversal can extend strongly.
    if e == "RAIJUU":
        dist = _structure_sl_dist(entry, side, highs, lows, lookback=15)
        if dist is None:
            return _atr_fallback(entry, side, atr_val, sl_mult=2.0, tp1_rr=2.0, tp2_rr=5.0)
        return _build_result(entry, side, dist, tp1_rr=2.0, tp2_rr=5.0)

    # ── Unknown engine — safe ATR fallback ───────────────────────────────────
    logger.debug("[SLTPEngine] Unknown engine '%s' — using ATR fallback.", engine)
    return _atr_fallback(entry, side, atr_val, sl_mult=2.3, tp1_rr=1.5, tp2_rr=5.0)

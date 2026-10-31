"""
OxyPulse — indicators.py
Full technical indicator library. Pure Python, no dependencies.
Includes: EMA, RSI, MACD, Bollinger Bands, VWAP, ATR, ADX, BBW, Hurst,
          swing_high_price, swing_low_price (for structure-based SL/TP).
"""
from __future__ import annotations
import math


def ema(prices: list[float], period: int) -> list[float]:
    if len(prices) < period:
        return []
    k      = 2 / (period + 1)
    result = [sum(prices[:period]) / period]
    for p in prices[period:]:
        result.append(p * k + result[-1] * (1 - k))
    return result


def rsi(prices: list[float], period: int = 14) -> float | None:
    if len(prices) < period + 1:
        return None
    deltas   = [prices[i] - prices[i-1] for i in range(1, len(prices))]
    gains    = [d if d > 0 else 0.0 for d in deltas]
    losses   = [-d if d < 0 else 0.0 for d in deltas]
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    for i in range(period, len(deltas)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
    if avg_loss == 0:
        return 100.0
    return round(100 - (100 / (1 + avg_gain / avg_loss)), 2)


def macd(prices: list[float], fast=12, slow=26, signal=9) -> tuple | None:
    if len(prices) < slow + signal:
        return None
    fe        = ema(prices, fast)
    se        = ema(prices, slow)
    n         = min(len(fe), len(se))
    macd_line = [fe[i - n + len(fe)] - se[i - n + len(se)] for i in range(n)]
    if len(macd_line) < signal:
        return None
    sig_line = ema(macd_line, signal)
    if not sig_line:
        return None
    return round(macd_line[-1], 8), round(sig_line[-1], 8), round(macd_line[-1] - sig_line[-1], 8)


def bollinger_bands(prices: list[float], period=20, std_dev=2.0) -> tuple | None:
    if len(prices) < period:
        return None
    window   = prices[-period:]
    middle   = sum(window) / period
    variance = sum((p - middle) ** 2 for p in window) / period
    std      = variance ** 0.5
    return round(middle + std_dev * std, 8), round(middle, 8), round(middle - std_dev * std, 8)


def bollinger_band_width(prices: list[float], period=20, std_dev=2.0) -> float | None:
    """BBW = (Upper - Lower) / Middle. Low BBW = range-bound, high = trending/volatile."""
    bb = bollinger_bands(prices, period, std_dev)
    if not bb:
        return None
    upper, middle, lower = bb
    if middle == 0:
        return None
    return round((upper - lower) / middle, 6)


def vwap(highs, lows, closes, volumes) -> float | None:
    if not highs or not (len(highs) == len(lows) == len(closes) == len(volumes)):
        return None
    total_vol = sum(volumes)
    if total_vol == 0:
        return None
    tp = [(h + l + c) / 3 for h, l, c in zip(highs, lows, closes)]
    return round(sum(t * v for t, v in zip(tp, volumes)) / total_vol, 8)


def atr(highs: list[float], lows: list[float], closes: list[float], period: int = 14) -> float | None:
    """Average True Range. Measures volatility per candle."""
    if len(closes) < period + 1 or not highs or not lows:
        return None
    trs = []
    for i in range(1, len(closes)):
        h, l, pc = highs[i], lows[i], closes[i - 1]
        tr = max(h - l, abs(h - pc), abs(l - pc))
        trs.append(tr)
    if len(trs) < period:
        return None
    # Wilder smoothing
    result = sum(trs[:period]) / period
    for tr in trs[period:]:
        result = (result * (period - 1) + tr) / period
    return round(result, 8)


def adx(highs: list[float], lows: list[float], closes: list[float], period: int = 14) -> float | None:
    """
    Average Directional Index. Measures trend strength (not direction).
    < 20 = ranging/weak, 20–25 = emerging trend, > 25 = strong trend, > 40 = very strong.
    """
    if len(closes) < period * 2 + 1 or not highs or not lows:
        return None
    dm_plus  = []
    dm_minus = []
    trs      = []
    for i in range(1, len(closes)):
        up   = highs[i]  - highs[i-1]
        down = lows[i-1] - lows[i]
        dm_plus.append(up   if up > down and up > 0   else 0.0)
        dm_minus.append(down if down > up and down > 0 else 0.0)
        tr = max(highs[i] - lows[i], abs(highs[i] - closes[i-1]), abs(lows[i] - closes[i-1]))
        trs.append(tr)

    def wilder(data, p):
        if len(data) < p:
            return []
        r = [sum(data[:p])]
        for x in data[p:]:
            r.append(r[-1] - r[-1] / p + x)
        return r

    sm_tr  = wilder(trs, period)
    sm_dmp = wilder(dm_plus, period)
    sm_dmm = wilder(dm_minus, period)

    n = min(len(sm_tr), len(sm_dmp), len(sm_dmm))
    if n < period:
        return None

    dx_vals = []
    for i in range(n):
        if sm_tr[i] == 0:
            continue
        dip = 100 * sm_dmp[i] / sm_tr[i]
        dim = 100 * sm_dmm[i] / sm_tr[i]
        denom = dip + dim
        if denom == 0:
            continue
        dx_vals.append(100 * abs(dip - dim) / denom)

    if len(dx_vals) < period:
        return None

    result = sum(dx_vals[:period]) / period
    for dx in dx_vals[period:]:
        result = (result * (period - 1) + dx) / period
    return round(result, 2)


def hurst_exponent(prices: list[float], min_window: int = 8) -> float | None:
    """
    Simplified Hurst exponent via rescaled range analysis.
    ~0.5 = random walk, >0.5 = trending, <0.5 = mean-reverting.
    Requires at least 64 data points for reasonable accuracy.
    """
    n = len(prices)
    if n < max(min_window * 4, 32):
        return None
    try:
        lags    = [int(2 ** i) for i in range(int(math.log2(min_window)), int(math.log2(n // 2)))]
        rs_vals = []
        for lag in lags:
            sub_rs = []
            for start in range(0, n - lag, lag):
                sub = prices[start:start + lag]
                mean = sum(sub) / len(sub)
                dev  = [s - mean for s in sub]
                cumdev = []
                c = 0.0
                for d in dev:
                    c += d
                    cumdev.append(c)
                r = max(cumdev) - min(cumdev)
                std_val = (sum(d ** 2 for d in dev) / len(dev)) ** 0.5
                if std_val > 0:
                    sub_rs.append(r / std_val)
            if sub_rs:
                rs_vals.append((lag, sum(sub_rs) / len(sub_rs)))

        if len(rs_vals) < 2:
            return None
        xs = [math.log(r[0]) for r in rs_vals]
        ys = [math.log(r[1]) for r in rs_vals]
        n2 = len(xs)
        sx, sy   = sum(xs), sum(ys)
        sxy      = sum(xs[i] * ys[i] for i in range(n2))
        sx2      = sum(x ** 2 for x in xs)
        slope    = (n2 * sxy - sx * sy) / (n2 * sx2 - sx ** 2 + 1e-12)
        return round(slope, 3)
    except Exception:
        return None


def avg_volume(volumes: list[float], period=20) -> float:
    if not volumes:
        return 0
    if len(volumes) < period:
        return sum(volumes) / len(volumes)
    return sum(volumes[-period:]) / period


def ema_cross_signal(prices: list[float], fast=9, slow=21) -> str:
    if len(prices) < slow + 2:
        return "HOLD"
    fe = ema(prices, fast)
    se = ema(prices, slow)
    if len(fe) < 2 or len(se) < 2:
        return "HOLD"
    if fe[-2] <= se[-2] and fe[-1] > se[-1]:
        return "BUY"
    if fe[-2] >= se[-2] and fe[-1] < se[-1]:
        return "SELL"
    return "HOLD"


def ema200(prices: list[float]) -> float | None:
    """200-period EMA for HTF trend filter."""
    result = ema(prices, 200)
    return result[-1] if result else None


# ── Pattern recognition (for IZANAGI AI features + AMATERASU context) ─────────

def detect_double_bottom(closes: list[float], lows: list[float], lookback: int = 50) -> bool:
    """
    Detect a W-shaped double bottom in the last `lookback` bars.
    Conditions:
      1. Two distinct local lows within 2% of each other
      2. A local high between them (the middle peak) > both lows by ≥ 3%
      3. Most recent close above the middle peak (confirmation)
    Returns True if pattern detected, False otherwise.
    """
    if len(closes) < lookback or len(lows) < lookback:
        return False
    c = closes[-lookback:]
    l = lows[-lookback:]
    n = len(l)

    # Find first low (left trough) — minimum in first half
    half = n // 2
    left_idx  = l[:half].index(min(l[:half]))
    left_low  = l[left_idx]

    # Find middle peak between the two troughs
    mid_range = l[left_idx + 3 : half + 3]
    if not mid_range:
        return False
    mid_peak  = max(c[left_idx + 3 : left_idx + 3 + len(mid_range)])

    # Find second low (right trough) — minimum in second half
    right_range = l[half:]
    if not right_range:
        return False
    right_low = min(right_range)

    # Validate W shape
    lows_similar    = abs(left_low - right_low) / max(left_low, 1e-10) < 0.02  # within 2%
    peak_significant = mid_peak > left_low * 1.03 and mid_peak > right_low * 1.03
    confirmed        = c[-1] > mid_peak  # price broke above the middle peak

    return lows_similar and peak_significant and confirmed


def detect_double_top(closes: list[float], highs: list[float], lookback: int = 50) -> bool:
    """
    Detect an M-shaped double top in the last `lookback` bars.
    Conditions:
      1. Two distinct local highs within 2% of each other
      2. A local low between them (the middle trough) < both highs by ≥ 3%
      3. Most recent close below the middle trough (confirmation)
    Returns True if pattern detected, False otherwise.
    """
    if len(closes) < lookback or len(highs) < lookback:
        return False
    c = closes[-lookback:]
    h = highs[-lookback:]
    n = len(h)

    half      = n // 2
    left_idx  = h[:half].index(max(h[:half]))
    left_high = h[left_idx]

    mid_range = c[left_idx + 3 : half + 3]
    if not mid_range:
        return False
    mid_trough = min(mid_range)

    right_range = h[half:]
    if not right_range:
        return False
    right_high = max(right_range)

    highs_similar     = abs(left_high - right_high) / max(left_high, 1e-10) < 0.02
    trough_significant = mid_trough < left_high * 0.97 and mid_trough < right_high * 0.97
    confirmed          = c[-1] < mid_trough

    return highs_similar and trough_significant and confirmed


def get_fib_levels(swing_high: float, swing_low: float) -> dict[str, float]:
    """
    Return Fibonacci retracement levels between swing_high and swing_low.
    Keys: '23.6', '38.2', '50.0', '61.8', '78.6'
    """
    diff = swing_high - swing_low
    if diff <= 0:
        return {}
    return {
        "23.6": round(swing_high - diff * 0.236, 8),
        "38.2": round(swing_high - diff * 0.382, 8),
        "50.0": round(swing_high - diff * 0.500, 8),
        "61.8": round(swing_high - diff * 0.618, 8),
        "78.6": round(swing_high - diff * 0.786, 8),
    }


# ── Elliott Wave Engine ──────────────────────────────────────────────────────
# find_pivots → get_zigzag → identify_impulse → elliott_wave_signal
# Designed for 4H candles (200 bars ≈ 33 days) but works on any timeframe.
# Returns (signal, votes, info_dict) consumed by IZANAGI strategy.
# ─────────────────────────────────────────────────────────────────────────────

def find_pivots(
    highs: list[float],
    lows:  list[float],
    order: int = 3,
) -> list[tuple[int, float, str]]:
    """
    Find pivot highs ('H') and pivot lows ('L') using a rolling window.
    A pivot high at index i: highs[i] is the maximum in [i-order, i+order].
    A pivot low  at index i: lows[i]  is the minimum in [i-order, i+order].

    Returns list of (index, price, 'H'|'L') sorted by index.
    """
    n = len(highs)
    if n < order * 2 + 1 or not lows or len(lows) != n:
        return []
    pivots: list[tuple[int, float, str]] = []
    for i in range(order, n - order):
        win_h = highs[i - order : i + order + 1]
        win_l = lows[i  - order : i + order + 1]
        if highs[i] == max(win_h):
            pivots.append((i, highs[i], 'H'))
        elif lows[i] == min(win_l):
            pivots.append((i, lows[i], 'L'))
    return pivots


def get_zigzag(
    highs:  list[float],
    lows:   list[float],
    order:  int = 3,
) -> list[tuple[int, float, str]]:
    """
    Convert raw pivot list into an alternating zigzag (H/L/H/L…).
    Consecutive same-type pivots are merged: keep the more extreme one.
    Returns list of (index, price, 'H'|'L').
    """
    raw = find_pivots(highs, lows, order)
    if not raw:
        return []
    zz: list[tuple[int, float, str]] = []
    for p in raw:
        if not zz:
            zz.append(p)
            continue
        last = zz[-1]
        if p[2] == last[2]:
            # Same type — keep the more extreme pivot
            if p[2] == 'H' and p[1] > last[1]:
                zz[-1] = p
            elif p[2] == 'L' and p[1] < last[1]:
                zz[-1] = p
        else:
            zz.append(p)
    return zz


def _fib_near(ratio: float, target: float, tol: float = 0.10) -> bool:
    """Return True if ratio is within tol of target."""
    return abs(ratio - target) <= tol


def _identify_impulse_direction(
    zz: list[tuple[int, float, str]],
) -> tuple[str, float]:
    """
    Given the last 5-6 zigzag pivots, attempt to classify the most recent
    completed impulse wave (5-wave up or 5-wave down) and estimate where
    the current price sits within the structure.

    Returns:
        (bias, confidence_0_to_1)
        bias: 'LONG'  — entering wave 3 or wave 5 (price likely to rally)
              'SHORT' — entering wave A/C correction (price likely to fall)
              'HOLD'  — pattern unclear
    """
    if len(zz) < 4:
        return 'HOLD', 0.0

    # Use the last 4–6 pivots for pattern matching
    pts = zz[-6:] if len(zz) >= 6 else zz

    # ── Bullish impulse: L H L H (waves 1-4 visible, we want wave 5 or in wave 3)
    # Look for: L0 < H1 > L2 < H3  with ascending lows and highs
    # Then if current price has corrected from H3 by 23.6-61.8% → wave 4 done, BUY wave 5
    if len(pts) >= 4:
        # Check last 4 for L H L H pattern
        for start in range(len(pts) - 4, -1, -1):
            p0, p1, p2, p3 = pts[start], pts[start+1], pts[start+2], pts[start+3]
            if p0[2] == 'L' and p1[2] == 'H' and p2[2] == 'L' and p3[2] == 'H':
                w1 = p1[1] - p0[1]   # wave 1 height
                w2 = p1[1] - p2[1]   # wave 2 correction
                w3 = p3[1] - p2[1]   # wave 3 height (should be longest)
                if w1 <= 0 or w3 <= 0:
                    continue
                w2_retrace = w2 / w1
                w3_ratio   = w3 / w1
                # Validate: W2 retraces 38.2–78.6% of W1; W3 ≥ 1.0× W1
                if not _fib_near(w2_retrace, 0.618, 0.24):
                    # Accept 38.2–78.6% range
                    if not (0.382 <= w2_retrace <= 0.786):
                        continue
                if w3_ratio < 0.8:
                    continue
                # Ascending lows and highs confirm valid impulse
                if p2[1] <= p0[1]:
                    continue  # W4 can't violate W1 top (Elliott rule)
                conf = min(1.0, 0.5 + (w3_ratio - 1.0) * 0.2 + (1 - abs(w2_retrace - 0.618)) * 0.3)
                return 'LONG', round(conf, 3)

            # Check L H L H L — wave 4 visible, we look for wave 5 entry
            if start + 4 < len(pts):
                p4 = pts[start+4]
                if (p0[2] == 'L' and p1[2] == 'H' and p2[2] == 'L'
                        and p3[2] == 'H' and p4[2] == 'L'):
                    w1 = p1[1] - p0[1]
                    w3 = p3[1] - p2[1]
                    w4 = p3[1] - p4[1]  # wave 4 correction
                    if w1 <= 0 or w3 <= 0:
                        continue
                    w4_retrace = w4 / w3
                    if w4_retrace < 0.0:
                        continue
                    # W4 should retrace 23.6–50% of W3; W4 low must stay above W1 top
                    if not (0.236 <= w4_retrace <= 0.618):
                        continue
                    if p4[1] <= p1[1]:
                        continue  # Elliott rule: wave 4 can't enter wave 1 territory
                    conf = min(1.0, 0.6 + (1 - abs(w4_retrace - 0.382)) * 0.4)
                    return 'LONG', round(conf, 3)

        # ── Bearish impulse: H L H L (waves 1-4 visible)
        for start in range(len(pts) - 4, -1, -1):
            p0, p1, p2, p3 = pts[start], pts[start+1], pts[start+2], pts[start+3]
            if p0[2] == 'H' and p1[2] == 'L' and p2[2] == 'H' and p3[2] == 'L':
                w1 = p0[1] - p1[1]
                w2 = p2[1] - p1[1]
                w3 = p2[1] - p3[1]
                if w1 <= 0 or w3 <= 0:
                    continue
                w2_retrace = w2 / w1
                w3_ratio   = w3 / w1
                if not (0.382 <= w2_retrace <= 0.786):
                    continue
                if w3_ratio < 0.8:
                    continue
                if p2[1] >= p0[1]:
                    continue  # W2 can't exceed W1 start
                conf = min(1.0, 0.5 + (w3_ratio - 1.0) * 0.2 + (1 - abs(w2_retrace - 0.618)) * 0.3)
                return 'SHORT', round(conf, 3)

            if start + 4 < len(pts):
                p4 = pts[start+4]
                if (p0[2] == 'H' and p1[2] == 'L' and p2[2] == 'H'
                        and p3[2] == 'L' and p4[2] == 'H'):
                    w1 = p0[1] - p1[1]
                    w3 = p2[1] - p3[1]
                    w4 = p4[1] - p3[1]
                    if w1 <= 0 or w3 <= 0:
                        continue
                    w4_retrace = w4 / w3
                    if not (0.236 <= w4_retrace <= 0.618):
                        continue
                    if p4[1] >= p1[1]:
                        continue
                    conf = min(1.0, 0.6 + (1 - abs(w4_retrace - 0.382)) * 0.4)
                    return 'SHORT', round(conf, 3)

    return 'HOLD', 0.0


def get_fib_extension(wave_start: float, wave_end: float, retrace_end: float) -> dict[str, float]:
    """
    Return Fibonacci extension targets above a completed retracement.
    wave_start → wave_end = impulse; retrace_end = end of correction.
    Targets are projected from retrace_end in the direction of the impulse.
    """
    move = abs(wave_end - wave_start)
    if move <= 0:
        return {}
    direction = 1 if wave_end > wave_start else -1
    return {
        "0.618": round(retrace_end + direction * move * 0.618, 8),
        "1.000": round(retrace_end + direction * move * 1.000, 8),
        "1.272": round(retrace_end + direction * move * 1.272, 8),
        "1.618": round(retrace_end + direction * move * 1.618, 8),
        "2.618": round(retrace_end + direction * move * 2.618, 8),
    }


def elliott_wave_signal(
    highs:  list[float],
    lows:   list[float],
    closes: list[float],
    order:  int = 3,
) -> tuple[str, int, dict]:
    """
    Full Elliott Wave detector.

    Args:
        highs, lows, closes: candle arrays (4H recommended for best structure)
        order: pivot detection window (higher = fewer, more significant pivots)

    Returns:
        (signal, votes, info)
        signal: "LONG" | "SHORT" | "HOLD"
        votes:  0 = no signal, 2 = moderate, 3 = high-confidence
        info:   dict with wave details for logging
    """
    n = len(closes)
    if n < order * 2 + 10 or not highs or not lows:
        return "HOLD", 0, {}

    # Build zigzag from provided data
    zz = get_zigzag(highs, lows, order)
    if len(zz) < 4:
        return "HOLD", 0, {}

    bias, conf = _identify_impulse_direction(zz)

    if bias == 'HOLD':
        return "HOLD", 0, {}

    # Map confidence to votes: conf ≥ 0.65 → 3 votes, conf ≥ 0.45 → 2 votes
    if conf >= 0.65:
        votes = 3
    elif conf >= 0.45:
        votes = 2
    else:
        return "HOLD", 0, {}

    # Build info dict for logging and Telegram alerts
    last_pts = zz[-4:]
    info = {
        "pivots": len(zz),
        "last_pivot": f"{last_pts[-1][2]}@{last_pts[-1][1]:.6g}" if last_pts else "",
        "confidence": conf,
        "order": order,
    }
    # Add Fibonacci extension targets if we have enough structure
    if len(last_pts) >= 3:
        p_a, p_b, p_c = last_pts[-3], last_pts[-2], last_pts[-1]
        ext = get_fib_extension(p_a[1], p_b[1], p_c[1])
        if ext:
            info["fib_ext_1.618"] = ext.get("1.618", 0)
            info["fib_ext_1.000"] = ext.get("1.000", 0)

    return bias, votes, info


def fib_proximity(price: float, swing_high: float, swing_low: float, tolerance_pct: float = 0.5) -> float:
    """
    Return a score [-1, +1] based on proximity to Fibonacci levels.
    +1 = price at 61.8% / 50% retracement (bullish support zone)
    -1 = price at 38.2% / 23.6% retracement from below (bearish resistance zone)
    0  = price not near any key level
    """
    if swing_high <= swing_low or price <= 0:
        return 0.0
    levels = get_fib_levels(swing_high, swing_low)
    tol    = price * tolerance_pct / 100

    # Bullish support levels (price bouncing from below = long opportunity)
    for key in ("61.8", "50.0"):
        if abs(price - levels.get(key, 0)) < tol:
            return 0.8

    # Moderate support
    if abs(price - levels.get("38.2", 0)) < tol:
        return 0.4

    # Bearish resistance (price rejected from above = short opportunity)
    for key in ("23.6", "38.2"):
        if price > levels.get(key, 0) and price < levels.get(key, 0) * 1.01:
            return -0.5

    return 0.0


# ── Swing Structure Helpers ───────────────────────────────────────────────────
# Used by sl_tp_engine.py to place SL beyond the last significant swing
# high / low rather than using a fixed ATR multiple.
# ─────────────────────────────────────────────────────────────────────────────

def swing_low_price(lows: list[float], lookback: int = 20) -> float | None:
    """
    Return the most recent significant swing-low price in the last `lookback`
    candles.  A swing low at bar i requires lows[i] < lows[i-1] and
    lows[i] < lows[i+1] (simple 1-bar confirmation either side).

    Returns None when the slice is too short or no swing is found.
    """
    if len(lows) < 3:
        return None
    window = lows[-lookback:] if len(lows) >= lookback else lows[:]
    n = len(window)
    for i in range(n - 2, 0, -1):          # scan right-to-left, skip edges
        if window[i] < window[i - 1] and window[i] < window[i + 1]:
            return window[i]
    return None                             # fallback: no swing found


def swing_high_price(highs: list[float], lookback: int = 20) -> float | None:
    """
    Return the most recent significant swing-high price in the last `lookback`
    candles.  A swing high at bar i requires highs[i] > highs[i-1] and
    highs[i] > highs[i+1].

    Returns None when the slice is too short or no swing is found.
    """
    if len(highs) < 3:
        return None
    window = highs[-lookback:] if len(highs) >= lookback else highs[:]
    n = len(window)
    for i in range(n - 2, 0, -1):
        if window[i] > window[i - 1] and window[i] > window[i + 1]:
            return window[i]
    return None

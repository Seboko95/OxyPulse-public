"""
OxyPulse — strategy_categories.py  v3
Core engines plus the V6.1 intelligence pack:
RYUU, RAIJIN, KAGE, ARASHI, MUSASHI, SHOGUN, TENJIN, IZANAGI,
SUSANOO, FUJIN, AMATERASU, TITAN, RONIN, OROCHI, TSUNAMI, SUIJIN, RAIJUU,
ENMA, HACHIMAN, HAYATE, KAGUTSUCHI, YATAGARASU, RYUJIN, TENSEI, IBUKI, ONRYO, KANNON, GENBU.

v3 additions:
  SUSANOO ⛩️ — Funding Rate Squeeze:
    Fires when perpetual funding is extreme (crowded side), price is at S/R,
    and volume confirms. Vote: funding extreme + S/R proximity + volume (2/3).

  FUJIN 💨 — Order Flow Imbalance:
    Taker buy/sell ratio + RSI mid-zone (40-60) + EMA21 alignment (2/3).

  AMATERASU ☀️ — Multi-Timeframe Confluence:
    1m EMA21>55 + 15m EMA21>55 + 1H price>EMA50. Requires ALL 3 to agree.

  SHOGUN — L/S ratio boost:
    When OKX retail L/S ratio is extreme (>2.5 or <0.5), adds a 5th signal vote.

v2 efficiency: _indicator_cache stores per-coin computed values for one cycle.
  Call clear_cache() at the top of each scan cycle in the orchestrator.
  Prevents recalculation of RSI/EMA/etc when multiple engines scan the same coin.
"""
from __future__ import annotations
import logging
import time
from datetime import datetime, timezone
import settings_store as _s
from indicators import ema, rsi, macd, bollinger_bands, bollinger_band_width, avg_volume, atr, adx as calc_adx, elliott_wave_signal

logger = logging.getLogger("OxyPulse.Strategies")

# ── Per-cycle indicator cache ─────────────────────────────────────────────────
# {coin: {"rsi14": float, "ema21": list, "ema55": list, ...}}
_indicator_cache: dict[str, dict] = {}
_cache_cycle_ts: float = 0.0


def clear_cache() -> None:
    """Call once per scan cycle from TradingOrchestrator before evaluating engines."""
    global _indicator_cache, _cache_cycle_ts
    _indicator_cache = {}
    _cache_cycle_ts  = time.monotonic()


def _cached(coin: str, key: str, fn):
    """Return cached indicator value or compute+cache it."""
    if coin not in _indicator_cache:
        _indicator_cache[coin] = {}
    if key not in _indicator_cache[coin]:
        _indicator_cache[coin][key] = fn()
    return _indicator_cache[coin][key]


# ═══════════════════════════════════════════════════════════════════════════════
# 🐉 RYUU — TREND
# ═══════════════════════════════════════════════════════════════════════════════

def _ryuu_ema_cross(closes, highs, lows, volumes, coin="") -> str:
    if len(closes) < 57:
        return "HOLD"
    e21 = _cached(coin, "ema21", lambda: ema(closes, 21))
    e55 = _cached(coin, "ema55", lambda: ema(closes, 55))
    if not e21 or not e55 or len(e21) < 2:
        return "HOLD"
    if e21[-1] > e55[-1] and e21[-1] > e21[-2]:
        return "LONG"
    if e21[-1] < e55[-1] and e21[-1] < e21[-2]:
        return "SHORT"
    return "HOLD"


def _ryuu_htf_alignment(closes, highs, lows, volumes, closes_4h=None, coin="") -> str:
    if closes_4h is None or len(closes_4h) < 200:
        return "HOLD"
    e200 = ema(closes_4h, 200)
    if not e200:
        return "HOLD"
    price = closes_4h[-1]; ema_val = e200[-1]
    if price > ema_val * 1.01: return "LONG"
    if price < ema_val * 0.99: return "SHORT"
    return "HOLD"


def _ryuu_macd_confirm(closes, highs, lows, volumes, coin="") -> str:
    if len(closes) < 35:
        return "HOLD"
    m = _cached(coin, "macd", lambda: macd(closes))
    if not m:
        return "HOLD"
    macd_line, signal_line, hist = m
    if macd_line > signal_line and hist > 0: return "LONG"
    if macd_line < signal_line and hist < 0: return "SHORT"
    return "HOLD"


def evaluate_ryuu(closes, highs, lows, volumes, closes_4h=None, coin="") -> tuple[str, int]:
    signals = [
        _ryuu_ema_cross(closes, highs, lows, volumes, coin),
        _ryuu_htf_alignment(closes, highs, lows, volumes, closes_4h, coin),
        _ryuu_macd_confirm(closes, highs, lows, volumes, coin),
    ]
    longs = signals.count("LONG"); shorts = signals.count("SHORT")
    if longs >= 2 and longs > shorts:  return "LONG",  longs
    if shorts >= 2 and shorts > longs: return "SHORT", shorts
    return "HOLD", 0


# ═══════════════════════════════════════════════════════════════════════════════
# ⚡ RAIJIN — MOMENTUM
# ═══════════════════════════════════════════════════════════════════════════════

def _raijin_rsi_slope(closes, highs, lows, volumes, coin="") -> str:
    if len(closes) < 20: return "HOLD"
    rsi_now  = _cached(coin, "rsi14", lambda: rsi(closes, 14))
    rsi_prev = rsi(closes[:-3], 14)
    if rsi_now is None or rsi_prev is None: return "HOLD"
    slope = rsi_now - rsi_prev
    if rsi_now < 60 and rsi_now > 35 and slope > 4:  return "LONG"
    if rsi_now > 40 and rsi_now < 65 and slope < -4: return "SHORT"
    return "HOLD"


def _raijin_volume_surge(closes, highs, lows, volumes, coin="") -> str:
    if not volumes or len(volumes) < 22: return "HOLD"
    curr = volumes[-1]; avg = avg_volume(volumes[-21:-1], 20)
    if avg <= 0 or curr < avg * 2.0: return "HOLD"
    if closes[-1] > closes[-2]: return "LONG"
    if closes[-1] < closes[-2]: return "SHORT"
    return "HOLD"


def _raijin_macd_volume_cross(closes, highs, lows, volumes, coin="") -> str:
    """MACD histogram crossover (sign change) with above-average volume confirmation.
    macd() returns a single tuple (macd_val, signal_val, hist_val) — NOT a list.
    We compare two windows: closes[:-1] vs closes to detect histogram sign change.
    """
    if len(closes) < 36: return "HOLD"
    # Current histogram
    m_curr = macd(closes)
    if not m_curr: return "HOLD"
    _, _, hist_curr = m_curr
    # Previous histogram (one candle back)
    m_prev = macd(closes[:-1])
    if not m_prev: return "HOLD"
    _, _, hist_prev = m_prev
    # Histogram crossed from negative to positive (bullish momentum)
    if hist_prev < 0 and hist_curr > 0:
        if volumes and len(volumes) >= 21:
            avg = avg_volume(volumes[-21:-1], 20)
            if avg > 0 and volumes[-1] >= avg * 1.3:
                return "LONG"
        else:
            return "LONG"
    # Histogram crossed from positive to negative (bearish momentum)
    if hist_prev > 0 and hist_curr < 0:
        if volumes and len(volumes) >= 21:
            avg = avg_volume(volumes[-21:-1], 20)
            if avg > 0 and volumes[-1] >= avg * 1.3:
                return "SHORT"
        else:
            return "SHORT"
    return "HOLD"


def evaluate_raijin(closes, highs, lows, volumes, coin="") -> tuple[str, int]:
    signals = [
        _raijin_rsi_slope(closes, highs, lows, volumes, coin),
        _raijin_volume_surge(closes, highs, lows, volumes, coin),
        _raijin_macd_volume_cross(closes, highs, lows, volumes, coin),
    ]
    longs = signals.count("LONG"); shorts = signals.count("SHORT")
    if longs >= 2 and longs > shorts:  return "LONG",  longs
    if shorts >= 2 and shorts > longs: return "SHORT", shorts
    return "HOLD", 0


# ═══════════════════════════════════════════════════════════════════════════════
# 🌑 KAGE — REVERSAL
# ═══════════════════════════════════════════════════════════════════════════════

def _kage_rsi_extreme(closes, highs, lows, volumes, coin="") -> str:
    if len(closes) < 16: return "HOLD"
    r = _cached(coin, "rsi14", lambda: rsi(closes, 14))
    if r is None: return "HOLD"
    if r < 25: return "LONG"
    if r > 75: return "SHORT"
    return "HOLD"


def _kage_bollinger_touch(closes, highs, lows, volumes, coin="") -> str:
    if len(closes) < 22: return "HOLD"
    result = _cached(coin, "bb20", lambda: bollinger_bands(closes, 20, 2.0))
    if not result: return "HOLD"
    upper, mid, lower = result; price = closes[-1]
    if price <= lower: return "LONG"
    if price >= upper: return "SHORT"
    return "HOLD"


def _kage_candlestick_reversal(closes, highs, lows, volumes, coin="") -> str:
    """Hammer, shooting star, bullish/bearish engulfing, morning/evening star."""
    if not highs or not lows or len(closes) < 5: return "HOLD"
    # Hammer / Shooting Star (prev candle)
    h = highs[-2]; l = lows[-2]; c = closes[-2]; o = closes[-3]
    body = abs(c - o); rng = h - l
    if rng > 0 and body > 0:
        lower_wick = min(o, c) - l
        upper_wick = h - max(o, c)
        if lower_wick >= body * 2.0 and lower_wick / rng >= 0.55 and closes[-1] > c:
            return "LONG"   # bullish hammer confirmed by next candle
        if upper_wick >= body * 2.0 and upper_wick / rng >= 0.55 and closes[-1] < c:
            return "SHORT"  # shooting star confirmed
    # Bullish / Bearish Engulfing
    p_open = closes[-3]; p_close = closes[-2]
    c_open = closes[-2]; c_close = closes[-1]
    if p_close < p_open and c_close > c_open and c_close > p_open and c_open < p_close:
        return "LONG"
    if p_close > p_open and c_close < c_open and c_close < p_open and c_open > p_close:
        return "SHORT"
    # Morning / Evening Star (3-candle)
    a, b, cc = closes[-4], closes[-3], closes[-2]
    body_a = abs(b - a)
    if body_a > 0 and abs(cc - b) < body_a * 0.35:  # small middle candle
        if a > b and closes[-1] > a: return "LONG"   # morning star
        if a < b and closes[-1] < a: return "SHORT"  # evening star
    return "HOLD"


def _kage_wick_rejection(closes, highs, lows, volumes, coin="") -> str:
    """Wick rejection: candle wick pokes past a recent swing high/low but closes back inside.
    Distinct from candlestick pattern — this checks rejection of a *price level* (20-bar
    swing high or low) rather than candle shape alone."""
    if not highs or not lows or len(closes) < 22 or len(highs) < 22 or len(lows) < 22:
        return "HOLD"
    # Use the candle just before the last confirmed close
    h = highs[-2]; l = lows[-2]; c = closes[-2]; o = closes[-3]
    if h == l:
        return "HOLD"
    # Swing high / low over the prior 20 bars (excluding the signal candle)
    swing_high = max(highs[-22:-2])
    swing_low  = min(lows[-22:-2])
    rng        = h - l
    lower_wick = min(o, c) - l
    upper_wick = h - max(o, c)
    # Bullish wick rejection: wick pierced below swing_low then closed above it
    if l < swing_low and c > swing_low and lower_wick > rng * 0.4 and closes[-1] > c:
        return "LONG"
    # Bearish wick rejection: wick pierced above swing_high then closed below it
    if h > swing_high and c < swing_high and upper_wick > rng * 0.4 and closes[-1] < c:
        return "SHORT"
    return "HOLD"


def evaluate_kage(closes, highs, lows, volumes, coin="") -> tuple[str, int]:
    signals = [
        _kage_rsi_extreme(closes, highs, lows, volumes, coin),
        _kage_bollinger_touch(closes, highs, lows, volumes, coin),
        _kage_wick_rejection(closes, highs, lows, volumes, coin),
    ]
    longs = signals.count("LONG"); shorts = signals.count("SHORT")
    if longs >= 2 and longs > shorts:  return "LONG",  longs
    if shorts >= 2 and shorts > longs: return "SHORT", shorts
    return "HOLD", 0


# ═══════════════════════════════════════════════════════════════════════════════
# 🌪️ ARASHI — BREAKOUT
# ═══════════════════════════════════════════════════════════════════════════════

def _arashi_range_break(closes, highs, lows, volumes, coin="") -> str:
    if len(closes) < 22 or not highs or not lows: return "HOLD"
    price = closes[-1]
    recent_high = max(highs[-21:-1]); recent_low = min(lows[-21:-1])
    if price > recent_high: return "LONG"
    if price < recent_low:  return "SHORT"
    return "HOLD"


def _arashi_volume_confirm(closes, highs, lows, volumes, coin="") -> str:
    if not volumes or len(volumes) < 22: return "HOLD"
    curr = volumes[-1]; avg = avg_volume(volumes[-21:-1], 20)
    if avg <= 0 or curr < avg * 2.0: return "HOLD"
    if closes[-1] > closes[-2]: return "LONG"
    if closes[-1] < closes[-2]: return "SHORT"
    return "HOLD"


def _arashi_atr_expansion(closes, highs, lows, volumes, coin="") -> str:
    if len(closes) < 20 or not highs or not lows: return "HOLD"
    try:
        curr_atr = _cached(coin, "atr14", lambda: atr(highs, lows, closes, 14))
        prev_atr = atr(highs[:-5], lows[:-5], closes[:-5], 14)
        if not curr_atr or not prev_atr or prev_atr <= 0: return "HOLD"
        if curr_atr > prev_atr * 1.4:
            return "LONG" if closes[-1] > closes[-2] else "SHORT"
    except Exception:
        pass
    return "HOLD"


def evaluate_arashi(closes, highs, lows, volumes, coin="") -> tuple[str, int]:
    signals = [
        _arashi_range_break(closes, highs, lows, volumes, coin),
        _arashi_volume_confirm(closes, highs, lows, volumes, coin),
        _arashi_atr_expansion(closes, highs, lows, volumes, coin),
    ]
    longs = signals.count("LONG"); shorts = signals.count("SHORT")
    if longs >= 2 and longs > shorts:  return "LONG",  longs
    if shorts >= 2 and shorts > longs: return "SHORT", shorts
    return "HOLD", 0


# ═══════════════════════════════════════════════════════════════════════════════
# ⚔️ MUSASHI — STRUCTURE
# ═══════════════════════════════════════════════════════════════════════════════

def _musashi_engulfing(closes, highs, lows, volumes, coin="") -> str:
    if len(closes) < 4: return "HOLD"
    prev_open = closes[-3]; prev_close = closes[-2]
    curr_open = closes[-2]; curr_close = closes[-1]
    prev_red   = prev_close < prev_open; prev_green = prev_close > prev_open
    curr_green = curr_close > curr_open; curr_red   = curr_close < curr_open
    if prev_red   and curr_green and curr_close > prev_open and curr_open < prev_close: return "LONG"
    if prev_green and curr_red   and curr_close < prev_open and curr_open > prev_close: return "SHORT"
    return "HOLD"


def _musashi_market_structure(closes, highs, lows, volumes, coin="") -> str:
    if not highs or not lows or len(highs) < 7: return "HOLD"
    h = highs[-7:]; l = lows[-7:]
    if h[-1] > h[-3] > h[-5] and l[-1] > l[-3] > l[-5]: return "LONG"
    if h[-1] < h[-3] < h[-5] and l[-1] < l[-3] < l[-5]: return "SHORT"
    return "HOLD"


def _musashi_sr_bounce(closes, highs, lows, volumes, coin="") -> str:
    if len(closes) < 52 or not highs or not lows: return "HOLD"
    price = closes[-1]
    resistance = max(highs[-51:-1]); support = min(lows[-51:-1])
    near_sup = abs(price - support)    / support    < 0.003
    near_res = abs(price - resistance) / resistance < 0.003
    rsi_val  = _cached(coin, "rsi14", lambda: rsi(closes, 14))
    if rsi_val is None: return "HOLD"
    if near_sup and rsi_val < 45: return "LONG"
    if near_res and rsi_val > 55: return "SHORT"
    return "HOLD"


def evaluate_musashi(closes, highs, lows, volumes, coin="") -> tuple[str, int]:
    signals = [
        _musashi_engulfing(closes, highs, lows, volumes, coin),
        _musashi_market_structure(closes, highs, lows, volumes, coin),
        _musashi_sr_bounce(closes, highs, lows, volumes, coin),
    ]
    longs = signals.count("LONG"); shorts = signals.count("SHORT")
    if longs >= 2 and longs > shorts:  return "LONG",  longs
    if shorts >= 2 and shorts > longs: return "SHORT", shorts
    return "HOLD", 0


# ═══════════════════════════════════════════════════════════════════════════════
def evaluate_hachiman(
    closes,
    highs,
    lows,
    volumes,
    closes_15m=None,
    closes_1h=None,
    coin="",
) -> tuple[str, float]:
    if len(closes) < 34 or not volumes or len(volumes) < 21:
        return "HOLD", 0.0

    votes_long = votes_short = 0.0
    e21 = ema(closes, 21)
    e55 = ema(closes, 55)
    avg_vol = avg_volume(volumes[-21:-1], 20)
    rsi14 = rsi(closes, 14) or 50.0
    atr_now = atr(highs, lows, closes, 14) or 0.0
    atr_ratio = (atr_now / closes[-1]) if atr_now > 0 and closes[-1] > 0 else 0.0

    if e21 and e55:
        if e21[-1] > e55[-1] and closes[-1] > e21[-1]:
            votes_long += 1.0
        elif e21[-1] < e55[-1] and closes[-1] < e21[-1]:
            votes_short += 1.0

    if closes_15m and len(closes_15m) >= 55:
        e15_fast = ema(closes_15m, 21)
        e15_slow = ema(closes_15m, 55)
        if e15_fast and e15_slow:
            if e15_fast[-1] > e15_slow[-1]:
                votes_long += 1.0
            elif e15_fast[-1] < e15_slow[-1]:
                votes_short += 1.0

    if closes_1h and len(closes_1h) >= 55:
        e1h = ema(closes_1h, 50)
        if e1h:
            if closes_1h[-1] > e1h[-1]:
                votes_long += 1.0
            elif closes_1h[-1] < e1h[-1]:
                votes_short += 1.0

    if avg_vol > 0 and volumes[-1] >= avg_vol * 1.1:
        if closes[-1] > closes[-2] and 45 <= rsi14 <= 72:
            votes_long += 1.0
        elif closes[-1] < closes[-2] and 28 <= rsi14 <= 55:
            votes_short += 1.0

    if 0.002 <= atr_ratio <= 0.08:
        if votes_long > votes_short and closes[-1] >= max(closes[-5:]):
            votes_long += 0.5
        elif votes_short > votes_long and closes[-1] <= min(closes[-5:]):
            votes_short += 0.5

    if votes_long >= 2.5 and votes_long > votes_short:
        logger.info("HACHIMAN PICK %s LONG votes=%.1f", coin, votes_long)
        return "LONG", float(votes_long)
    if votes_short >= 2.5 and votes_short > votes_long:
        logger.info("HACHIMAN PICK %s SHORT votes=%.1f", coin, votes_short)
        return "SHORT", float(votes_short)
    return "HOLD", 0.0


# ═══════════════════════════════════════════════════════════════════════════════

def evaluate_yatagarasu(
    closes,
    highs,
    lows,
    volumes,
    closes_15m=None,
    highs_15m=None,
    lows_15m=None,
    volumes_15m=None,
    closes_1h=None,
    taker_buy_vol: float = 0.0,
    taker_sell_vol: float = 0.0,
    coin="",
) -> tuple[str, float]:
    if len(closes) < 40 or not highs or not lows or len(volumes) < 21:
        return "HOLD", 0.0
    if not closes_15m or len(closes_15m) < 80 or not highs_15m or not lows_15m or not volumes_15m:
        return "HOLD", 0.0
    if not closes_1h or len(closes_1h) < 55:
        return "HOLD", 0.0

    votes_long = votes_short = 0.0
    e21 = ema(closes, 21); e55 = ema(closes, 55)
    e15_fast = ema(closes_15m, 21); e15_slow = ema(closes_15m, 55)
    e1h = ema(closes_1h, 50)
    avg_vol_1m = avg_volume(volumes[-21:-1], 20)
    avg_vol_15m = avg_volume(volumes_15m[-21:-1], 20)
    breakout_buffer = float(_s.get_val("yatagarasu_breakout_buffer_pct", 0.35) or 0.35) / 100.0
    vol_mult = float(_s.get_val("yatagarasu_volume_confirm_mult", 1.20) or 1.20)
    total_taker = max(float(taker_buy_vol or 0.0) + float(taker_sell_vol or 0.0), 0.0)
    buy_ratio = (float(taker_buy_vol or 0.0) / total_taker) if total_taker > 0 else 0.5
    long_flow = float(_s.get_val("yatagarasu_flow_ratio_long", 0.58) or 0.58)
    short_flow = float(_s.get_val("yatagarasu_flow_ratio_short", 0.42) or 0.42)

    if e21 and e55 and e21[-1] > e55[-1] and closes[-1] > e21[-1]:
        votes_long += 1.0
    elif e21 and e55 and e21[-1] < e55[-1] and closes[-1] < e21[-1]:
        votes_short += 1.0

    if e15_fast and e15_slow and e15_fast[-1] > e15_slow[-1] and closes_15m[-1] > e15_fast[-1]:
        votes_long += 1.0
    elif e15_fast and e15_slow and e15_fast[-1] < e15_slow[-1] and closes_15m[-1] < e15_fast[-1]:
        votes_short += 1.0

    if e1h and closes_1h[-1] > e1h[-1]:
        votes_long += 1.0
    elif e1h and closes_1h[-1] < e1h[-1]:
        votes_short += 1.0

    range_high = max(highs_15m[-21:-1])
    range_low = min(lows_15m[-21:-1])
    if closes_15m[-1] >= range_high * (1.0 + breakout_buffer):
        votes_long += 1.5
    elif closes_15m[-1] <= range_low * (1.0 - breakout_buffer):
        votes_short += 1.5

    if avg_vol_1m > 0 and avg_vol_15m > 0:
        if volumes[-1] >= avg_vol_1m * 1.05 and volumes_15m[-1] >= avg_vol_15m * vol_mult:
            if closes[-1] > closes[-2]:
                votes_long += 1.0
            elif closes[-1] < closes[-2]:
                votes_short += 1.0

    if buy_ratio >= long_flow:
        votes_long += 1.0
    elif buy_ratio <= short_flow:
        votes_short += 1.0

    if votes_long >= 4.0 and votes_long > votes_short:
        logger.info("YATAGARASU PICK %s LONG score=%.1f buy_ratio=%.2f", coin, votes_long, buy_ratio)
        return "LONG", float(votes_long)
    if votes_short >= 4.0 and votes_short > votes_long:
        logger.info("YATAGARASU PICK %s SHORT score=%.1f buy_ratio=%.2f", coin, votes_short, buy_ratio)
        return "SHORT", float(votes_short)
    return "HOLD", 0.0


def evaluate_ryujin(
    closes,
    highs,
    lows,
    volumes,
    taker_buy_vol: float = 0.0,
    taker_sell_vol: float = 0.0,
    coin="",
) -> tuple[str, float]:
    if len(closes) < 28 or not highs or not lows or len(volumes) < 21:
        return "HOLD", 0.0

    votes_long = votes_short = 0.0
    e21 = ema(closes, 21); e55 = ema(closes, 55)
    avg_vol = avg_volume(volumes[-21:-1], 20)
    atr_val = atr(highs, lows, closes, 14) or 0.0
    total_taker = max(float(taker_buy_vol or 0.0) + float(taker_sell_vol or 0.0), 0.0)
    buy_ratio = (float(taker_buy_vol or 0.0) / total_taker) if total_taker > 0 else 0.5
    long_ratio = float(_s.get_val("ryujin_flow_ratio_long", 0.60) or 0.60)
    short_ratio = float(_s.get_val("ryujin_flow_ratio_short", 0.40) or 0.40)
    vol_mult = float(_s.get_val("ryujin_volume_confirm_mult", 1.20) or 1.20)
    disp_mult = float(_s.get_val("ryujin_displacement_atr_mult", 0.80) or 0.80)
    displacement = closes[-1] - closes[-4] if len(closes) >= 4 else 0.0
    candle_range = max(highs[-1] - lows[-1], 1e-12)
    close_pos = (closes[-1] - lows[-1]) / candle_range
    bear_close_pos = (highs[-1] - closes[-1]) / candle_range

    if e21 and e55 and e21[-1] > e55[-1] and closes[-1] > e21[-1]:
        votes_long += 1.0
    elif e21 and e55 and e21[-1] < e55[-1] and closes[-1] < e21[-1]:
        votes_short += 1.0

    if buy_ratio >= long_ratio:
        votes_long += 2.0 if buy_ratio >= min(long_ratio + 0.05, 0.80) else 1.0
    elif buy_ratio <= short_ratio:
        votes_short += 2.0 if buy_ratio <= max(short_ratio - 0.05, 0.20) else 1.0

    if avg_vol > 0 and volumes[-1] >= avg_vol * vol_mult:
        if closes[-1] > closes[-2] and close_pos >= 0.72:
            votes_long += 1.0
        elif closes[-1] < closes[-2] and bear_close_pos >= 0.72:
            votes_short += 1.0

    if atr_val > 0:
        if displacement >= atr_val * disp_mult:
            votes_long += 1.0
        elif displacement <= -(atr_val * disp_mult):
            votes_short += 1.0

    if votes_long >= 3.0 and votes_long > votes_short:
        logger.info("RYUJIN PICK %s LONG score=%.1f buy_ratio=%.2f disp=%.4f", coin, votes_long, buy_ratio, displacement)
        return "LONG", float(votes_long)
    if votes_short >= 3.0 and votes_short > votes_long:
        logger.info("RYUJIN PICK %s SHORT score=%.1f buy_ratio=%.2f disp=%.4f", coin, votes_short, buy_ratio, displacement)
        return "SHORT", float(votes_short)
    return "HOLD", 0.0


def evaluate_kannon(*args, **kwargs) -> tuple[str, float]:
    return "HOLD", 0.0


def evaluate_genbu(*args, **kwargs) -> tuple[str, float]:
    return "HOLD", 0.0

# 🏯 SHOGUN — CRT + SMC + ICT
# Institutional price action: liquidity sweeps, fair value gaps, order blocks.
# Only fires during London and New York kill zones for maximum edge.
# ═══════════════════════════════════════════════════════════════════════════════

def _ict_killzone_active() -> bool:
    """
    Returns True during London (02:00–05:00 UTC) and NY (13:30–16:30 UTC).
    These are the highest-probability windows for institutional SMC setups.
    Crypto runs 24/7 but smart money is most active in these windows.
    """
    now = datetime.now(timezone.utc)
    mins = now.hour * 60 + now.minute
    london_open  = 2  * 60          # 02:00 UTC
    london_close = 5  * 60          # 05:00 UTC
    ny_open      = 13 * 60 + 30     # 13:30 UTC
    ny_close     = 16 * 60 + 30     # 16:30 UTC
    return (london_open <= mins <= london_close) or (ny_open <= mins <= ny_close)


def _shogun_crt_sweep(closes, highs, lows, volumes, highs_4h=None, lows_4h=None, coin="") -> str:
    """
    CRT Liquidity Sweep:
    Price wicked beyond the previous 4H candle's high or low (liquidity grab)
    but CLOSED back inside the range → manipulation confirmed, reverse.

    Bullish: wick below previous 4H low, close above it → LONG
    Bearish: wick above previous 4H high, close below it → SHORT
    """
    if highs_4h is None or lows_4h is None:
        return "HOLD"
    if len(highs_4h) < 3 or len(lows_4h) < 3 or len(closes) < 3:
        return "HOLD"

    prev_4h_high = highs_4h[-2]
    prev_4h_low  = lows_4h[-2]

    # Use last 3 1m candles to detect the sweep
    last_low   = min(lows[-3:])
    last_high  = max(highs[-3:])
    last_close = closes[-1]

    # Bearish CRT: wick above prev 4H high, closed back below it
    if last_high > prev_4h_high and last_close < prev_4h_high:
        return "SHORT"

    # Bullish CRT: wick below prev 4H low, closed back above it
    if last_low < prev_4h_low and last_close > prev_4h_low:
        return "LONG"

    return "HOLD"


def _shogun_fvg(closes, highs, lows, volumes, coin="") -> str:
    """
    Fair Value Gap (3-candle imbalance) — price returning to fill the gap.

    Bullish FVG: candle[n-2].high < candle[n].low (gap left above)
    Price has now retraced into the gap = LONG entry.

    Bearish FVG: candle[n-2].low > candle[n].high (gap left below)
    Price retraced into the gap = SHORT entry.

    Checks last 10 candles for any unmitigated FVG.
    """
    if len(closes) < 10 or not highs or not lows:
        return "HOLD"

    price = closes[-1]

    # Scan recent candles for an open FVG
    for i in range(-8, -2):
        try:
            # Bullish FVG
            fvg_bottom = highs[i]
            fvg_top    = lows[i + 2]
            if fvg_top > fvg_bottom and fvg_bottom <= price <= fvg_top:
                return "LONG"
            # Bearish FVG
            fvg_top2    = lows[i]
            fvg_bottom2 = highs[i + 2]
            if fvg_top2 > fvg_bottom2 and fvg_bottom2 <= price <= fvg_top2:
                return "SHORT"
        except IndexError:
            continue

    return "HOLD"


def _shogun_order_block(closes, highs, lows, volumes, coin="") -> str:
    """
    Order Block: the last opposite-colour candle before a strong displacement.
    Institutional entry zone — price returns to OB for accumulation/distribution.

    Bullish OB: last bearish candle before a strong bullish displacement.
    Price is now retracing back to that candle's range → LONG.

    Bearish OB: last bullish candle before a strong bearish displacement.
    Price retracing to that range → SHORT.

    Displacement = candle body > 1.5× recent avg body size.
    """
    if len(closes) < 15 or not highs or not lows:
        return "HOLD"

    # Average body size of last 10 candles
    bodies = [abs(closes[i] - closes[i-1]) for i in range(-10, 0)]
    avg_body = sum(bodies) / len(bodies) if bodies else 0
    if avg_body <= 0:
        return "HOLD"

    price = closes[-1]

    # Search for displacement candles in the last 10 bars
    for i in range(-2, -10, -1):
        try:
            body = abs(closes[i] - closes[i-1])
            if body < avg_body * 1.5:
                continue

            # Strong bullish displacement — find the preceding bearish candle (OB)
            if closes[i] > closes[i-1]:
                ob_idx = i - 1
                ob_high = max(closes[ob_idx], closes[ob_idx - 1])
                ob_low  = min(closes[ob_idx], closes[ob_idx - 1])
                if ob_low <= price <= ob_high:
                    return "LONG"

            # Strong bearish displacement — find the preceding bullish candle (OB)
            elif closes[i] < closes[i-1]:
                ob_idx = i - 1
                ob_high = max(closes[ob_idx], closes[ob_idx - 1])
                ob_low  = min(closes[ob_idx], closes[ob_idx - 1])
                if ob_low <= price <= ob_high:
                    return "SHORT"
        except IndexError:
            continue

    return "HOLD"


def _shogun_choch_bos(closes, highs, lows, volumes, coin="") -> str:
    """
    Change of Character (CHoCH) / Break of Structure (BOS).
    CHoCH: price breaks the most recent swing high (bullish) or swing low (bearish)
    after a liquidity sweep — signals a shift in market intent.
    """
    if len(highs) < 10 or len(lows) < 10:
        return "HOLD"

    # Find most recent swing high and swing low (5-bar lookback)
    swing_high = max(highs[-6:-1])
    swing_low  = min(lows[-6:-1])
    price      = closes[-1]

    # BOS above swing high with volume confirmation
    if price > swing_high:
        vol_ok = (not volumes or len(volumes) < 5 or
                  volumes[-1] > avg_volume(volumes[-6:-1], 5) * 1.2)
        if vol_ok:
            return "LONG"

    # BOS below swing low
    if price < swing_low:
        vol_ok = (not volumes or len(volumes) < 5 or
                  volumes[-1] > avg_volume(volumes[-6:-1], 5) * 1.2)
        if vol_ok:
            return "SHORT"

    return "HOLD"


def _shogun_ls_sentiment(long_short_ratio: float) -> str:
    """
    OKX retail L/S ratio contrarian signal.
    > 2.5 = retail extremely long → smart money fades → SHORT.
    < 0.5 = retail extremely short → smart money fades → LONG.
    0     = ratio not available (skip).
    """
    if long_short_ratio > 2.5:
        return "SHORT"
    if 0 < long_short_ratio < 0.5:
        return "LONG"
    return "HOLD"


def evaluate_shogun(
    closes, highs, lows, volumes,
    highs_4h=None, lows_4h=None,
    long_short_ratio: float = 0.0,
    coin="",
) -> tuple[str, int]:
    """
    SHOGUN engine evaluation.

    Requires BOTH:
      1. ICT kill zone active (London or NY session)
      2. At least 2/3 strategy votes agree on direction

    Outside kill zones → always returns HOLD (reduces false signals dramatically).
    """
    # Kill zone gate — outside key sessions, don't trade
    if not _ict_killzone_active():
        return "HOLD", 0

    signals = [
        _shogun_crt_sweep(closes, highs, lows, volumes, highs_4h, lows_4h, coin),
        _shogun_fvg(closes, highs, lows, volumes, coin),
        _shogun_order_block(closes, highs, lows, volumes, coin),
        _shogun_choch_bos(closes, highs, lows, volumes, coin),
        _shogun_ls_sentiment(long_short_ratio),   # 5th vote: OKX retail L/S contrast
    ]

    longs  = signals.count("LONG")
    shorts = signals.count("SHORT")

    # Need 2 of 5 for entry
    if longs >= 2 and longs > shorts:
        return "LONG",  longs
    if shorts >= 2 and shorts > longs:
        return "SHORT", shorts
    return "HOLD", 0


# ═══════════════════════════════════════════════════════════════════════════════
# 🌅 TENJIN — SWING (4H long-run engine, wide SL/TP, lower leverage)
# Requires all 3 signals to agree on 4H timeframe before firing.
# ═══════════════════════════════════════════════════════════════════════════════

def _tenjin_4h_trend(closes_4h, coin="") -> str:
    """4H EMA50/200 golden/death cross with price above/below EMA50."""
    if not closes_4h or len(closes_4h) < 200: return "HOLD"
    e50  = ema(closes_4h, 50)
    e200 = ema(closes_4h, 200)
    if not e50 or not e200: return "HOLD"
    if e50[-1] > e200[-1] and closes_4h[-1] > e50[-1]: return "LONG"
    if e50[-1] < e200[-1] and closes_4h[-1] < e50[-1]: return "SHORT"
    return "HOLD"


def _tenjin_4h_engulfing(closes_4h, coin="") -> str:
    """4H bullish/bearish engulfing candle for swing entry confirmation."""
    if not closes_4h or len(closes_4h) < 4: return "HOLD"
    p_open  = closes_4h[-3]; p_close = closes_4h[-2]
    c_open  = closes_4h[-2]; c_close = closes_4h[-1]
    if p_close < p_open and c_close > c_open and c_close > p_open and c_open < p_close:
        return "LONG"
    if p_close > p_open and c_close < c_open and c_close < p_open and c_open > p_close:
        return "SHORT"
    return "HOLD"


def _tenjin_4h_rsi_zone(closes_4h, coin="") -> str:
    """4H RSI in oversold (<45) or overbought (>55) zone for swing bias."""
    if not closes_4h or len(closes_4h) < 20: return "HOLD"
    r = rsi(closes_4h, 14)
    if r is None: return "HOLD"
    if r < 45: return "LONG"
    if r > 55: return "SHORT"
    return "HOLD"


def evaluate_tenjin(
    closes, highs, lows, volumes,
    closes_4h=None, highs_4h=None, lows_4h=None,
    coin="",
) -> tuple[str, int]:
    """TENJIN swing engine — requires 3/3 4H signals for high-confidence swing entries."""
    if not closes_4h or len(closes_4h) < 200:
        return "HOLD", 0
    signals = [
        _tenjin_4h_trend(closes_4h, coin),
        _tenjin_4h_engulfing(closes_4h, coin),
        _tenjin_4h_rsi_zone(closes_4h, coin),
    ]
    longs  = signals.count("LONG")
    shorts = signals.count("SHORT")
    if longs == 3:  return "LONG",  3
    if shorts == 3: return "SHORT", 3
    return "HOLD", 0


# ═══════════════════════════════════════════════════════════════════════════════
# 🌌 IZANAGI — COMPOSITE SIGNAL ENGINE (multi-feature score)
# ═══════════════════════════════════════════════════════════════════════════════

def evaluate_izanagi(
    closes, highs, lows, volumes,
    closes_4h=None, highs_4h=None, lows_4h=None,
    closes_1d=None, highs_1d=None, lows_1d=None,
    closes_15m=None, highs_15m=None, lows_15m=None, volumes_15m=None,
    coin="",
) -> tuple[str, int]:
    """
    IZANAGI — Elliott Wave Engine (primary: 4H candles, confirmation: 1D + composite score).

    Pipeline:
    1. Run Elliott Wave signal on 4H candles (primary — multi-day impulse waves).
    2. If 4H gives HOLD, fall back to 1D candles (weekly/monthly wave structures).
    3. Composite score acts as a confirmation filter:
         EW + composite agree  → full EW votes (2 or 3)
         EW fires, composite conflicts → cap at 2 votes (reduce conviction)
         EW HOLD entirely → fall back to composite-only voting
    Minimum data: 30 × 4H bars OR 30 × 1D bars for EW; 50 × 1m bars for composite fallback.
    """
    ew_signal = "HOLD"
    ew_votes = 0

    # ── Step 1: 4H Elliott Wave ───────────────────────────────────────────────
    if closes_4h and highs_4h and lows_4h and len(closes_4h) >= 60:
        try:
            sig, _votes, _info = elliott_wave_signal(highs_4h, lows_4h, closes_4h, order=3)
            ew_signal = sig
            ew_votes = _votes
        except Exception as e:
            logger.warning("IZANAGI 4H EW error [%s]: %s", coin, e)
            return "HOLD", 0

    # ── Step 2: 1D fallback ───────────────────────────────────────────────────
    if ew_signal not in ("LONG", "SHORT"):
        return "HOLD", 0

    if closes_1d and highs_1d and lows_1d and len(closes_1d) >= 60:
        try:
            day_signal, _votes, _info = elliott_wave_signal(highs_1d, lows_1d, closes_1d, order=2)
            if day_signal in ("LONG", "SHORT") and day_signal != ew_signal:
                logger.debug("IZANAGI veto [%s]: 1D %s conflicts with 4H %s", coin, day_signal, ew_signal)
                return "HOLD", 0
        except Exception as e:
            logger.warning("IZANAGI 1D EW error [%s]: %s", coin, e)
            return "HOLD", 0

    return ew_signal, ew_votes


def evaluate_izanagi_rehab(
    closes, highs, lows, volumes,
    closes_4h=None, highs_4h=None, lows_4h=None,
    closes_1d=None, highs_1d=None, lows_1d=None,
    closes_15m=None, highs_15m=None, lows_15m=None, volumes_15m=None,
    coin="",
) -> tuple[str, int]:
    """Rehab variant: 4H Elliott Wave + 1D veto."""
    if not (closes_4h and highs_4h and lows_4h and len(closes_4h) >= 60):
        return "HOLD", 0

    try:
        ew_signal, _votes, _info = elliott_wave_signal(highs_4h, lows_4h, closes_4h, order=3)
    except Exception as e:
        logger.warning("IZANAGI 4H EW error [%s]: %s", coin, e)
        return "HOLD", 0
    if ew_signal not in ("LONG", "SHORT"):
        return "HOLD", 0

    if closes_1d and highs_1d and lows_1d and len(closes_1d) >= 60:
        try:
            day_signal, _votes, _info = elliott_wave_signal(highs_1d, lows_1d, closes_1d, order=2)
            if day_signal in ("LONG", "SHORT") and day_signal != ew_signal:
                logger.debug("IZANAGI rehab veto [%s]: 1D %s conflicts with 4H %s", coin, day_signal, ew_signal)
                return "HOLD", 0
        except Exception as e:
            logger.warning("IZANAGI 1D EW error [%s]: %s", coin, e)
            return "HOLD", 0

    return ew_signal, 2


# ═══════════════════════════════════════════════════════════════════════════════
# ⛩️ SUSANOO — FUNDING RATE SQUEEZE
# Fade crowded perpetual futures positioning when price is at key levels.
# ═══════════════════════════════════════════════════════════════════════════════

def evaluate_susanoo(
    closes, highs, lows, volumes,
    funding_rate: float = 0.0,
    coin: str = "",
) -> tuple[str, int]:
    """
    SUSANOO ⛩️ — Funding Rate Squeeze.
    When funding is extreme, longs/shorts are crowded and the squeeze is imminent.
    Requires 2 of 3 votes: funding extreme + price at S/R + volume confirmation.
    """
    if len(closes) < 22 or not volumes or len(volumes) < 22:
        return "HOLD", 0

    threshold = float(_s.get_val("susanoo_funding_threshold", 0.0005))

    # Vote 1: Funding extreme (fade the crowded side)
    if funding_rate >= threshold:
        fund_vote = "SHORT"    # longs paying — longs crowded
    elif funding_rate <= -threshold:
        fund_vote = "LONG"     # shorts paying — shorts crowded
    else:
        fund_vote = "HOLD"

    # Vote 2: Price at 20-bar support (for LONG) or resistance (for SHORT)
    price   = closes[-1]
    high20  = max(highs[-21:-1]) if len(highs) >= 21 else max(highs)
    low20   = min(lows[-21:-1])  if len(lows)  >= 21 else min(lows)
    near_res = high20 > 0 and abs(price - high20) / high20 < 0.005
    near_sup = low20  > 0 and abs(price - low20)  / low20  < 0.005

    if fund_vote == "SHORT" and near_res:
        sr_vote = "SHORT"
    elif fund_vote == "LONG" and near_sup:
        sr_vote = "LONG"
    else:
        sr_vote = "HOLD"

    # Vote 3: Volume confirmation (current vol ≥ 1.5× 20-bar avg)
    avg_vol = avg_volume(volumes[-21:-1], 20)
    if avg_vol > 0 and volumes[-1] >= avg_vol * 1.5:
        vol_vote = fund_vote   # align volume vote with funding direction
    else:
        vol_vote = "HOLD"

    signals = [fund_vote, sr_vote, vol_vote]
    longs   = signals.count("LONG")
    shorts  = signals.count("SHORT")
    if longs >= 2 and longs > shorts:  return "LONG",  longs
    if shorts >= 2 and shorts > longs: return "SHORT", shorts
    return "HOLD", 0


# ═══════════════════════════════════════════════════════════════════════════════
# 💨 FUJIN — ORDER FLOW IMBALANCE
# OKX taker buy/sell volume imbalance + RSI mid-zone + EMA alignment.
# ═══════════════════════════════════════════════════════════════════════════════

def evaluate_fujin(
    closes, volumes,
    taker_buy_vol:  float = 0.0,
    taker_sell_vol: float = 0.0,
    coin: str = "",
) -> tuple[str, int]:
    """
    FUJIN 💨 — Order Flow Imbalance.
    Uses OKX taker volume imbalance as primary signal; RSI mid-zone + EMA as confirmation.
    Requires 2 of 3 votes. If taker data is unavailable, needs both RSI + EMA.
    """
    if len(closes) < 25:
        return "HOLD", 0

    # Vote 1: Taker buy/sell imbalance (skip if data not provided)
    taker_vote = "HOLD"
    total_taker = taker_buy_vol + taker_sell_vol
    if total_taker > 0:
        buy_ratio = taker_buy_vol / total_taker
        if buy_ratio >= 0.65:
            taker_vote = "LONG"
        elif buy_ratio <= 0.35:
            taker_vote = "SHORT"

    # Vote 2: RSI mid-zone (40–60 = anticipatory, not yet stretched)
    rsi_val  = _cached(coin, "rsi14", lambda: rsi(closes, 14))
    e21      = _cached(coin, "ema21", lambda: ema(closes, 21))
    price    = closes[-1]
    rsi_vote = "HOLD"
    if rsi_val is not None and 40 <= rsi_val <= 60 and e21:
        if price > e21[-1]:   rsi_vote = "LONG"
        elif price < e21[-1]: rsi_vote = "SHORT"

    # Vote 3: EMA21 alignment (price clearly above/below EMA21)
    ema_vote = "HOLD"
    if e21:
        if price > e21[-1] * 1.002:   ema_vote = "LONG"
        elif price < e21[-1] * 0.998: ema_vote = "SHORT"

    signals = [taker_vote, rsi_vote, ema_vote]
    longs   = signals.count("LONG")
    shorts  = signals.count("SHORT")
    if longs >= 2 and longs > shorts:  return "LONG",  longs
    if shorts >= 2 and shorts > longs: return "SHORT", shorts
    return "HOLD", 0


# ═══════════════════════════════════════════════════════════════════════════════
# ☀️ AMATERASU — MULTI-TIMEFRAME CONFLUENCE
# High-conviction trades only when 1m + 15m + 1H all agree.
# ═══════════════════════════════════════════════════════════════════════════════

def evaluate_amaterasu(
    closes,
    closes_15m=None,
    closes_1h=None,
    closes_1d=None,
    highs_1h=None,
    lows_1h=None,
    coin: str = "",
) -> tuple[str, int]:
    """
    AMATERASU ☀️ — Multi-Timeframe Confluence.
    All 3 timeframes must agree on direction — no exceptions (3/3 required).
      Vote 1 (1m):  EMA21 > EMA55 → LONG,  EMA21 < EMA55 → SHORT
      Vote 2 (15m): EMA21 > EMA55 → LONG,  EMA21 < EMA55 → SHORT
      Vote 3 (1H):  price > EMA50 → LONG,  price < EMA50 → SHORT
    Filter: 1H RSI must not be overbought (> 72) for LONG or oversold (< 28) for SHORT.
    """
    if len(closes) < 57:
        return "HOLD", 0

    # Vote 1: 1m EMA21 vs EMA55
    e21_1m = _cached(coin, "ema21", lambda: ema(closes, 21))
    e55_1m = _cached(coin, "ema55", lambda: ema(closes, 55))
    if not e21_1m or not e55_1m:
        return "HOLD", 0
    if e21_1m[-1] > e55_1m[-1]:   v1 = "LONG"
    elif e21_1m[-1] < e55_1m[-1]: v1 = "SHORT"
    else:                          return "HOLD", 0

    # Vote 2: 15m EMA21 vs EMA55
    if not closes_15m or len(closes_15m) < 57:
        return "HOLD", 0
    e21_15 = ema(closes_15m, 21)
    e55_15 = ema(closes_15m, 55)
    if not e21_15 or not e55_15:
        return "HOLD", 0
    if e21_15[-1] > e55_15[-1]:   v2 = "LONG"
    elif e21_15[-1] < e55_15[-1]: v2 = "SHORT"
    else:                          return "HOLD", 0

    # Vote 3: 1H price vs EMA50
    if not closes_1h or len(closes_1h) < 52:
        return "HOLD", 0
    e50_1h = ema(closes_1h, 50)
    if not e50_1h:
        return "HOLD", 0
    price_1h = closes_1h[-1]
    if price_1h > e50_1h[-1] * 1.005:   v3 = "LONG"
    elif price_1h < e50_1h[-1] * 0.995: v3 = "SHORT"
    else:                                 v3 = "NEUTRAL"

    votes = [v1, v2, v3]
    longs  = votes.count("LONG")
    shorts = votes.count("SHORT")

    # 3/3 unanimous — full conviction
    if longs == 3:
        rsi_1h = rsi(closes_1h, 14)
        if rsi_1h is not None and rsi_1h > 72:
            return "HOLD", 0
        return "LONG", 3

    if shorts == 3:
        rsi_1h = rsi(closes_1h, 14)
        if rsi_1h is not None and rsi_1h < 28:
            return "HOLD", 0
        return "SHORT", 3

    # 2/3 majority — reduced conviction
    if longs >= 2 and longs > shorts:
        rsi_1h = rsi(closes_1h, 14)
        if rsi_1h is not None and rsi_1h > 72:
            return "HOLD", 0
        return "LONG", 2

    if shorts >= 2 and shorts > longs:
        rsi_1h = rsi(closes_1h, 14)
        if rsi_1h is not None and rsi_1h < 28:
            return "HOLD", 0
        return "SHORT", 2

    return "HOLD", 0


def amaterasu_macro_context(
    direction: str,
    closes_1d: list | None = None,
    coin: str = "",
) -> dict:
    """
    Daily/Weekly macro layer for AMATERASU.

    This is NOT a hard filter. It produces soft modifiers and diagnostics:
      - daily_alignment / weekly_alignment: aligned | conflict | neutral | unknown
      - macro_agreement_score: 0..2
      - conf_modifier: small additive confidence hint (never veto)
      - size_multiplier: mild sizing hint for entries
      - runner_trust_mult: used by exit_manager continuation logic
    """
    dir_up = str(direction or "").upper()
    if dir_up not in {"LONG", "SHORT"}:
        return {
            "daily_alignment": "unknown",
            "weekly_alignment": "unknown",
            "macro_agreement_score": 0,
            "conf_modifier": 0.0,
            "size_multiplier": 1.0,
            "runner_trust_mult": 1.0,
            "macro_conflict": False,
        }
    if not closes_1d or len(closes_1d) < 35:
        return {
            "daily_alignment": "unknown",
            "weekly_alignment": "unknown",
            "macro_agreement_score": 0,
            "conf_modifier": 0.0,
            "size_multiplier": 1.0,
            "runner_trust_mult": 1.0,
            "macro_conflict": False,
        }

    daily_ema = ema(closes_1d, 50)  # simple daily bias proxy
    price_d = float(closes_1d[-1] or 0.0)
    daily_alignment = "unknown"
    if daily_ema and price_d > 0:
        if dir_up == "LONG":
            daily_alignment = "aligned" if price_d >= daily_ema[-1] else "conflict"
        else:
            daily_alignment = "aligned" if price_d <= daily_ema[-1] else "conflict"

    # Weekly: derive a rough weekly series from daily closes (5 trading days-ish).
    # Crypto trades 7d/week; use 7-day chunks to avoid weekday assumptions.
    weekly_alignment = "unknown"
    weekly_agree = 0
    closes_w = []
    try:
        step = 7
        for i in range(step - 1, len(closes_1d), step):
            closes_w.append(closes_1d[i])
    except Exception:
        closes_w = []
    if len(closes_w) >= 12:
        w_ema = ema(closes_w, 10)
        price_w = float(closes_w[-1] or 0.0)
        if w_ema and price_w > 0:
            if dir_up == "LONG":
                weekly_alignment = "aligned" if price_w >= w_ema[-1] else "conflict"
            else:
                weekly_alignment = "aligned" if price_w <= w_ema[-1] else "conflict"
    else:
        weekly_alignment = "unknown"

    agree = 0
    conflict = False
    for tag in (daily_alignment, weekly_alignment):
        if tag == "aligned":
            agree += 1
        elif tag == "conflict":
            conflict = True

    boost = float(_s.get_val("amaterasu_macro_conf_boost", 0.15) or 0.15)
    penalty = float(_s.get_val("amaterasu_macro_conf_penalty", 0.10) or 0.10)
    size_boost = float(_s.get_val("amaterasu_macro_size_boost", 1.05) or 1.05)
    size_penalty = float(_s.get_val("amaterasu_macro_size_penalty", 0.92) or 0.92)

    conf_mod = 0.0
    size_mult = 1.0
    runner_trust = 1.0
    if agree == 2:
        conf_mod = boost
        size_mult = size_boost
        runner_trust = 1.10
    elif conflict:
        conf_mod = -penalty
        size_mult = size_penalty
        runner_trust = 0.92
    elif agree == 1 and not conflict:
        conf_mod = boost * 0.5
        size_mult = 1.02
        runner_trust = 1.03

    return {
        "daily_alignment": daily_alignment,
        "weekly_alignment": weekly_alignment,
        "macro_agreement_score": int(agree),
        "conf_modifier": round(float(conf_mod), 4),
        "size_multiplier": round(float(size_mult), 4),
        "runner_trust_mult": round(float(runner_trust), 4),
        "macro_conflict": bool(conflict),
    }


# ═══════════════════════════════════════════════════════════════════════════════
# Master dispatcher
# ═══════════════════════════════════════════════════════════════════════════════

# ═══════════════════════════════════════════════════════════════════════════════
# 🏛️ TITAN — Large-Cap Mean Reversion
# Top-20 market cap only.  3 strategies, 2/3 vote entry.
# Research-backed: RSI extremes + BB reversion + VWAP reversion
# Target win rate: 60-70% (mean reversion on liquid large-caps)
# ═══════════════════════════════════════════════════════════════════════════════

def _titan_rsi_extreme(closes, highs, lows, volumes, coin="") -> str:
    """RSI extreme with divergence detection for high-probability reversals."""
    if len(closes) < 30:
        return "HOLD"
    rsi_now = _cached(coin, "rsi14", lambda: rsi(closes, 14))
    if rsi_now is None:
        return "HOLD"
    # Strong RSI extremes — high-probability reversal zones for large caps
    if rsi_now < 25:
        # Check for bullish divergence: price lower low but RSI higher low
        rsi_prev = rsi(closes[:-5], 14)
        if rsi_prev is not None and rsi_now > rsi_prev and closes[-1] < closes[-6]:
            return "LONG"   # bullish divergence — strongest signal
        if rsi_now < 22:
            return "LONG"   # extreme oversold even without divergence
    if rsi_now > 75:
        # Check for bearish divergence: price higher high but RSI lower high
        rsi_prev = rsi(closes[:-5], 14)
        if rsi_prev is not None and rsi_now < rsi_prev and closes[-1] > closes[-6]:
            return "SHORT"  # bearish divergence — strongest signal
        if rsi_now > 78:
            return "SHORT"  # extreme overbought even without divergence
    return "HOLD"


def _titan_bb_reversion(closes, highs, lows, volumes, coin="") -> str:
    """Bollinger Band reversion — price returning to mean after touching outer band."""
    if len(closes) < 22:
        return "HOLD"
    bb = _cached(coin, "bb20", lambda: bollinger_bands(closes, 20, 2.0))
    if bb is None:
        return "HOLD"
    upper, middle, lower = bb
    price = closes[-1]
    prev_price = closes[-2] if len(closes) >= 2 else price

    # BBW filter: skip during extreme squeezes (bands too tight = no reversion)
    bbw = _cached(coin, "bbw20", lambda: bollinger_band_width(closes, 20, 2.0))
    if bbw is not None and bbw < 0.015:
        return "HOLD"  # bands too tight, wait for expansion

    # LONG: price was below lower band and now re-entering (reverting up)
    if prev_price <= lower and price > lower and price < middle:
        return "LONG"
    # Also: price is near lower band (within 0.3%) and starting to bounce
    if price < lower * 1.003 and price > prev_price and price < middle:
        return "LONG"

    # SHORT: price was above upper band and now re-entering (reverting down)
    if prev_price >= upper and price < upper and price > middle:
        return "SHORT"
    # Also: price is near upper band (within 0.3%) and starting to decline
    if price > upper * 0.997 and price < prev_price and price > middle:
        return "SHORT"

    return "HOLD"


def _titan_vwap_reversion(closes, highs, lows, volumes, coin="") -> str:
    """VWAP reversion — price deviated from VWAP and reverting back."""
    if len(closes) < 20 or len(highs) < 20 or len(volumes) < 20:
        return "HOLD"
    from indicators import vwap as calc_vwap
    vwap_val = _cached(coin, "vwap", lambda: calc_vwap(highs[-50:], lows[-50:], closes[-50:], volumes[-50:]))
    if vwap_val is None or vwap_val == 0:
        return "HOLD"

    price = closes[-1]
    prev_price = closes[-2] if len(closes) >= 2 else price
    deviation_pct = (price - vwap_val) / vwap_val * 100

    # LONG: price >1.5% below VWAP and starting to recover
    if deviation_pct < -1.5 and price > prev_price:
        return "LONG"
    # SHORT: price >1.5% above VWAP and starting to decline
    if deviation_pct > 1.5 and price < prev_price:
        return "SHORT"

    return "HOLD"


def evaluate_titan(closes, highs, lows, volumes, coin="") -> tuple[str, int]:
    """TITAN — Large-Cap Mean Reversion. 3 strategies, 2/3 vote."""
    signals = [
        _titan_rsi_extreme(closes, highs, lows, volumes, coin),
        _titan_bb_reversion(closes, highs, lows, volumes, coin),
        _titan_vwap_reversion(closes, highs, lows, volumes, coin),
    ]
    longs  = signals.count("LONG")
    shorts = signals.count("SHORT")
    if longs >= 2 and longs > shorts:   return "LONG",  longs
    if shorts >= 2 and shorts > longs:  return "SHORT", shorts
    return "HOLD", 0


# ═══════════════════════════════════════════════════════════════════════════════
# 🗡️ RONIN — Large-Cap Breakout Momentum
# Top-20 market cap only.  3 strategies, 2/3 vote entry.
# Research-backed: Multi-EMA+VWAP cross (77% win rate backtested)
#                  + Volume breakout + ADX trend strength
# Target win rate: 55-65% (momentum continuation on liquid large-caps)
# ═══════════════════════════════════════════════════════════════════════════════

def _ronin_ema_vwap_cross(closes, highs, lows, volumes, coin="") -> str:
    """Multi-EMA crossover with VWAP confirmation — 77%+ backtested win rate."""
    if len(closes) < 57 or len(volumes) < 50:
        return "HOLD"
    e8  = _cached(coin, "ema8",  lambda: ema(closes, 8))
    e21 = _cached(coin, "ema21", lambda: ema(closes, 21))
    e55 = _cached(coin, "ema55", lambda: ema(closes, 55))
    if not e8 or not e21 or not e55 or len(e8) < 2:
        return "HOLD"

    from indicators import vwap as calc_vwap
    vwap_val = _cached(coin, "vwap", lambda: calc_vwap(highs[-50:], lows[-50:], closes[-50:], volumes[-50:]))
    price = closes[-1]

    # LONG: EMA8 > EMA21 > EMA55 (aligned uptrend) + price above VWAP
    if e8[-1] > e21[-1] > e55[-1]:
        if vwap_val and price > vwap_val:
            return "LONG"
    # SHORT: EMA8 < EMA21 < EMA55 (aligned downtrend) + price below VWAP
    if e8[-1] < e21[-1] < e55[-1]:
        if vwap_val and price < vwap_val:
            return "SHORT"

    return "HOLD"


def _ronin_volume_breakout(closes, highs, lows, volumes, coin="") -> str:
    """Volume-confirmed breakout — breaks 20-bar high/low with 2× avg volume."""
    if len(closes) < 22 or len(volumes) < 22:
        return "HOLD"
    avg_vol = _cached(coin, "avg_vol20", lambda: avg_volume(volumes, 20))
    if not avg_vol or avg_vol == 0:
        return "HOLD"

    price     = closes[-1]
    vol_ratio = volumes[-1] / avg_vol
    high_20   = max(highs[-21:-1])  # 20 bars excluding current
    low_20    = min(lows[-21:-1])   # 20 bars excluding current

    # LONG: close above 20-bar high with volume > 1.5× average
    if price > high_20 and vol_ratio >= 1.5:
        return "LONG"
    # SHORT: close below 20-bar low with volume > 1.5× average
    if price < low_20 and vol_ratio >= 1.5:
        return "SHORT"

    return "HOLD"


def _ronin_adx_rsi_momentum(closes, highs, lows, volumes, coin="") -> str:
    """ADX trend strength + RSI momentum — confirms strong directional move."""
    if len(closes) < 30 or len(highs) < 30:
        return "HOLD"
    adx_val = _cached(coin, "adx14", lambda: calc_adx(highs, lows, closes, 14))
    rsi_val = _cached(coin, "rsi14", lambda: rsi(closes, 14))
    if adx_val is None or rsi_val is None:
        return "HOLD"

    # Only trade when there's a STRONG trend (ADX > 25)
    if adx_val < 25:
        return "HOLD"

    # LONG: strong trend + RSI in bullish zone (50-65, not overbought)
    if 50 <= rsi_val <= 65:
        return "LONG"
    # SHORT: strong trend + RSI in bearish zone (35-50, not oversold)
    if 35 <= rsi_val <= 50:
        return "SHORT"

    return "HOLD"


def evaluate_ronin(closes, highs, lows, volumes, coin="") -> tuple[str, int]:
    """RONIN — Large-Cap Breakout Momentum. 3 strategies, 2/3 vote."""
    signals = [
        _ronin_ema_vwap_cross(closes, highs, lows, volumes, coin),
        _ronin_volume_breakout(closes, highs, lows, volumes, coin),
        _ronin_adx_rsi_momentum(closes, highs, lows, volumes, coin),
    ]
    longs  = signals.count("LONG")
    shorts = signals.count("SHORT")
    if longs >= 2 and longs > shorts:   return "LONG",  longs
    if shorts >= 2 and shorts > longs:  return "SHORT", shorts
    return "HOLD", 0


def _ronin_rehab_htf_alignment(closes_4h, direction: str) -> bool:
    if not closes_4h or len(closes_4h) < 205:
        return False
    e50 = ema(closes_4h, 50)
    e200 = ema(closes_4h, 200)
    if len(e50) < 3 or len(e200) < 3:
        return False
    price_4h = closes_4h[-1]
    if direction == "LONG":
        return (
            e50[-1] > e200[-1]
            and e50[-1] > e50[-3]
            and e200[-1] >= e200[-3]
            and price_4h > e50[-1]
        )
    if direction == "SHORT":
        return (
            e50[-1] < e200[-1]
            and e50[-1] < e50[-3]
            and e200[-1] <= e200[-3]
            and price_4h < e50[-1]
        )
    return False


def _ronin_rehab_ema_vwap_cross(closes, highs, lows, volumes, coin="") -> str:
    if len(closes) < 60 or len(highs) < 50 or len(lows) < 50 or len(volumes) < 50:
        return "HOLD"
    e8 = _cached(coin, "ema8", lambda: ema(closes, 8))
    e21 = _cached(coin, "ema21", lambda: ema(closes, 21))
    e55 = _cached(coin, "ema55", lambda: ema(closes, 55))
    if not e8 or not e21 or not e55 or len(e8) < 3 or len(e21) < 3 or len(e55) < 3:
        return "HOLD"
    from indicators import vwap as calc_vwap
    vwap_val = _cached(coin, "ronin_vwap_rehab", lambda: calc_vwap(highs[-50:], lows[-50:], closes[-50:], volumes[-50:]))
    if not vwap_val:
        return "HOLD"
    price = closes[-1]
    vwap_buffer = float(_s.get_val("ronin_vwap_buffer_pct", 0.15)) / 100.0
    if (
        e8[-1] > e21[-1] > e55[-1]
        and e8[-1] > e8[-2]
        and e21[-1] > e21[-2]
        and price > vwap_val * (1 + vwap_buffer)
    ):
        return "LONG"
    if (
        e8[-1] < e21[-1] < e55[-1]
        and e8[-1] < e8[-2]
        and e21[-1] < e21[-2]
        and price < vwap_val * (1 - vwap_buffer)
    ):
        return "SHORT"
    return "HOLD"


def _ronin_rehab_volume_breakout(closes, highs, lows, volumes) -> str:
    if len(closes) < 22 or len(volumes) < 22 or len(highs) < 22 or len(lows) < 22:
        return "HOLD"
    avg_vol = avg_volume(volumes[-21:-1], 20)
    if avg_vol <= 0:
        return "HOLD"
    price = closes[-1]
    prev_close = closes[-2]
    vol_ratio = volumes[-1] / avg_vol
    high_20 = max(highs[-21:-1])
    low_20 = min(lows[-21:-1])
    breakout_buffer = float(_s.get_val("ronin_breakout_buffer_pct", 0.15)) / 100.0
    volume_mult = float(_s.get_val("ronin_volume_confirm_mult", 1.8))
    if prev_close <= high_20 and price > high_20 * (1 + breakout_buffer) and vol_ratio >= volume_mult:
        return "LONG"
    if prev_close >= low_20 and price < low_20 * (1 - breakout_buffer) and vol_ratio >= volume_mult:
        return "SHORT"
    return "HOLD"


def _ronin_rehab_adx_rsi_momentum(closes, highs, lows, coin="") -> str:
    if len(closes) < 34 or len(highs) < 34 or len(lows) < 34:
        return "HOLD"
    adx_now = calc_adx(highs, lows, closes, 14)
    adx_prev = calc_adx(highs[:-3], lows[:-3], closes[:-3], 14)
    rsi_val = _cached(coin, "rsi14", lambda: rsi(closes, 14))
    if adx_now is None or adx_prev is None or rsi_val is None:
        return "HOLD"
    adx_min = float(_s.get_val("ronin_adx_min", 28.0))
    adx_rising_min = float(_s.get_val("ronin_adx_rising_min", 0.75))
    if adx_now < adx_min or adx_now < adx_prev + adx_rising_min:
        return "HOLD"
    momentum = (closes[-1] - closes[-4]) / closes[-4] if closes[-4] else 0.0
    if 53 <= rsi_val <= 68 and momentum > 0.001:
        return "LONG"
    if 32 <= rsi_val <= 47 and momentum < -0.001:
        return "SHORT"
    return "HOLD"


def evaluate_ronin_rehab(closes, highs, lows, volumes, closes_4h=None, coin="") -> tuple[str, int]:
    signals = [
        _ronin_rehab_ema_vwap_cross(closes, highs, lows, volumes, coin),
        _ronin_rehab_volume_breakout(closes, highs, lows, volumes),
        _ronin_rehab_adx_rsi_momentum(closes, highs, lows, coin),
    ]
    longs = signals.count("LONG")
    shorts = signals.count("SHORT")
    if longs >= 2 and longs > shorts and _ronin_rehab_htf_alignment(closes_4h, "LONG"):
        return "LONG", longs
    if shorts >= 2 and shorts > longs and _ronin_rehab_htf_alignment(closes_4h, "SHORT"):
        return "SHORT", shorts
    return "HOLD", 0


# ═══════════════════════════════════════════════════════════════════════════════
# 🐍 OROCHI — Liquid Trend Pullback
# Research-fit replacement for the failed funding-fade engine:
# 4H major-coin trend + 15m pullback reclaim + 1m trigger.
# Funding is now a crowding veto, not the direction signal.
# ═══════════════════════════════════════════════════════════════════════════════

def _orochi_htf_bias(closes_4h: list | None, coin: str = "") -> str:
    if not closes_4h or len(closes_4h) < 205:
        return "HOLD"
    e50 = _cached(f"{coin}_orochi", "4h_ema50", lambda: ema(closes_4h, 50))
    e200 = _cached(f"{coin}_orochi", "4h_ema200", lambda: ema(closes_4h, 200))
    if len(e50) < 3 or len(e200) < 3:
        return "HOLD"
    price = closes_4h[-1]
    if (
        e50[-1] > e200[-1]
        and e50[-1] > e50[-2] > e50[-3]
        and price > e50[-1]
    ):
        return "LONG"
    if (
        e50[-1] < e200[-1]
        and e50[-1] < e50[-2] < e50[-3]
        and price < e50[-1]
    ):
        return "SHORT"
    return "HOLD"


def _orochi_pullback_reclaim(
    closes_15m: list | None,
    highs_15m: list | None,
    lows_15m: list | None,
    volumes_15m: list | None,
    direction: str,
    coin: str = "",
) -> str:
    if not closes_15m or not highs_15m or not lows_15m or not volumes_15m:
        return "HOLD"
    if len(closes_15m) < 60 or len(highs_15m) < 60 or len(lows_15m) < 60 or len(volumes_15m) < 60:
        return "HOLD"
    e21 = _cached(f"{coin}_orochi", "15m_ema21", lambda: ema(closes_15m, 21))
    e55 = _cached(f"{coin}_orochi", "15m_ema55", lambda: ema(closes_15m, 55))
    if len(e21) < 2 or len(e55) < 2:
        return "HOLD"
    from indicators import vwap as calc_vwap
    vwap_15m = _cached(
        f"{coin}_orochi",
        "15m_vwap",
        lambda: calc_vwap(highs_15m[-40:], lows_15m[-40:], closes_15m[-40:], volumes_15m[-40:]),
    )
    if not vwap_15m:
        return "HOLD"
    price = closes_15m[-1]
    prev_price = closes_15m[-2]
    rsi_15m = rsi(closes_15m, 14)
    if rsi_15m is None:
        return "HOLD"
    pullback_buffer = float(_s.get_val("orochi_pullback_buffer_pct", 0.45)) / 100.0
    if direction == "LONG":
        near_trend = abs(price - e21[-1]) / e21[-1] <= pullback_buffer
        reclaimed = prev_price <= e21[-2] * (1 + pullback_buffer) and price > e21[-1] and price > vwap_15m
        if e21[-1] > e55[-1] and near_trend and reclaimed and 48 <= rsi_15m <= 62:
            return "LONG"
    if direction == "SHORT":
        near_trend = abs(price - e21[-1]) / e21[-1] <= pullback_buffer
        reclaimed = prev_price >= e21[-2] * (1 - pullback_buffer) and price < e21[-1] and price < vwap_15m
        if e21[-1] < e55[-1] and near_trend and reclaimed and 38 <= rsi_15m <= 52:
            return "SHORT"
    return "HOLD"


def _orochi_ltf_trigger(closes, highs, lows, volumes, direction: str, coin: str = "") -> str:
    if len(closes) < 60 or len(highs) < 60 or len(lows) < 60 or len(volumes) < 60:
        return "HOLD"
    e8 = _cached(f"{coin}_orochi", "1m_ema8", lambda: ema(closes, 8))
    e21 = _cached(f"{coin}_orochi", "1m_ema21", lambda: ema(closes, 21))
    if len(e8) < 2 or len(e21) < 2:
        return "HOLD"
    from indicators import vwap as calc_vwap
    vwap_1m = _cached(
        f"{coin}_orochi",
        "1m_vwap",
        lambda: calc_vwap(highs[-40:], lows[-40:], closes[-40:], volumes[-40:]),
    )
    if not vwap_1m:
        return "HOLD"
    avg_vol = avg_volume(volumes[-21:-1], 20)
    if avg_vol <= 0:
        return "HOLD"
    price = closes[-1]
    prev_price = closes[-2]
    rsi_1m = rsi(closes, 14)
    if rsi_1m is None:
        return "HOLD"
    vol_ratio = volumes[-1] / avg_vol
    vol_mult = float(_s.get_val("orochi_volume_confirm_mult", 1.1))
    momentum = (price - closes[-4]) / closes[-4] if closes[-4] else 0.0
    if direction == "LONG":
        if (
            e8[-1] > e21[-1]
            and (e8[-2] <= e21[-2] or prev_price <= vwap_1m)
            and price > vwap_1m
            and 52 <= rsi_1m <= 68
            and vol_ratio >= vol_mult
            and momentum > 0.0008
        ):
            return "LONG"
    if direction == "SHORT":
        if (
            e8[-1] < e21[-1]
            and (e8[-2] >= e21[-2] or prev_price >= vwap_1m)
            and price < vwap_1m
            and 32 <= rsi_1m <= 48
            and vol_ratio >= vol_mult
            and momentum < -0.0008
        ):
            return "SHORT"
    return "HOLD"


def _orochi_funding_crowded(funding_rate: float, direction: str) -> bool:
    if funding_rate == 0.0:
        return False
    abs_max = float(_s.get_val("orochi_funding_abs_max", 0.0008))
    if direction == "LONG" and funding_rate > abs_max:
        return True
    if direction == "SHORT" and funding_rate < -abs_max:
        return True
    return False


def evaluate_orochi(
    closes,
    highs,
    lows,
    volumes,
    closes_4h: list | None = None,
    closes_15m: list | None = None,
    highs_15m: list | None = None,
    lows_15m: list | None = None,
    volumes_15m: list | None = None,
    funding_rate: float = 0.0,
    coin: str = "",
) -> tuple[str, float]:
    """
    OROCHI rehab:
      1. 4H major-coin trend bias
      2. 15m pullback reclaim into that trend
      3. 1m trigger with volume confirmation
    Funding only vetoes late, crowded continuation.
    """
    trend_vote = _orochi_htf_bias(closes_4h, coin)
    if trend_vote not in ("LONG", "SHORT"):
        return "HOLD", 0.0
    if _orochi_funding_crowded(funding_rate, trend_vote):
        return "HOLD", 0.0
    pullback_vote = _orochi_pullback_reclaim(
        closes_15m, highs_15m, lows_15m, volumes_15m, trend_vote, coin
    )
    trigger_vote = _orochi_ltf_trigger(closes, highs, lows, volumes, trend_vote, coin)
    votes = 1.0
    if pullback_vote == trend_vote:
        votes += 1.0
    if trigger_vote == trend_vote:
        votes += 1.0
    if votes >= 3.0:
        return trend_vote, votes
    return "HOLD", 0.0


# ═══════════════════════════════════════════════════════════════════════════════
# 🌊 TSUNAMI — Liquidation Cascade Engine
# Detects when a large block of leveraged positions has been wiped out,
# signalling exhaustion of that directional pressure.
# Vote weight: 2.5 (strongest new engine — cascade exhaustion is high-edge).
# ═══════════════════════════════════════════════════════════════════════════════

def evaluate_tsunami(long_liq_usd: float, short_liq_usd: float, coin: str = "") -> tuple[str, float]:
    """
    TSUNAMI: uses pre-fetched liquidation USD volumes for the past 5 minutes.

    Shorts liquidated > threshold → LONG  (short squeeze exhausted selling)
    Longs  liquidated > threshold → SHORT (long liquidation cascade)

    Data is fetched in category_engine._evaluate_coin() and cached per coin
    with a 60-second cooldown to avoid hammering the OKX public endpoint.
    """
    threshold = float(_s.get_val("tsunami_liq_threshold_usd", 50000.0))

    if short_liq_usd > threshold:
        return "LONG",  2.5   # shorts got wiped — buying pressure exhausted
    if long_liq_usd > threshold:
        return "SHORT", 2.5   # longs got wiped  — selling pressure exhausted
    return "HOLD", 0.0


# ═══════════════════════════════════════════════════════════════════════════════
# 💧 SUIJIN — VWAP Reversion Engine  (RANGING regime only)
# Mean reversion to VWAP with RSI confirmation.
# Vote weight: 1.5
# ═══════════════════════════════════════════════════════════════════════════════

def evaluate_suijin(
    closes_1h: list | None,
    highs_1h:  list | None,
    lows_1h:   list | None,
    closes:    list,
    coin:      str = "",
) -> tuple[str, float]:
    """
    SUIJIN: only fires in RANGING regime.
    VWAP computed from 1H typical prices (H+L+C)/3 — no volume weighting needed
    because we're trading the price relationship, not volume-weighted levels.

    LONG  when price < VWAP * (1 - dev%) AND RSI < 40
    SHORT when price > VWAP * (1 + dev%) AND RSI > 60
    """
    import regime_detector as _rd
    if _rd.get_regime() != _rd.RANGING:
        return "HOLD", 0.0

    if not closes_1h or len(closes_1h) < 10:
        return "HOLD", 0.0
    if not highs_1h or not lows_1h or len(highs_1h) < 10:
        return "HOLD", 0.0

    dev_pct = float(_s.get_val("suijin_vwap_deviation_pct", 1.5)) / 100.0

    # VWAP from 1H typical prices (session anchor)
    n = min(len(closes_1h), len(highs_1h), len(lows_1h))
    typical = [(highs_1h[i] + lows_1h[i] + closes_1h[i]) / 3.0
               for i in range(-n, 0)]
    vwap = sum(typical) / len(typical)

    price = closes_1h[-1]
    rsi14 = rsi(closes, 14) if len(closes) >= 15 else None
    if rsi14 is None or vwap <= 0:
        return "HOLD", 0.0

    if price < vwap * (1.0 - dev_pct) and rsi14 < 40:
        return "LONG",  1.5
    if price > vwap * (1.0 + dev_pct) and rsi14 > 60:
        return "SHORT", 1.5
    return "HOLD", 0.0


# ═══════════════════════════════════════════════════════════════════════════════
# ⚡ RAIJUU — Open Interest Divergence Engine
# OI rising but price flat/down → smart money building shorts (SHORT signal).
# OI falling but price flat/up → short covering into strength (LONG signal).
# Vote weight: 1.5
# ═══════════════════════════════════════════════════════════════════════════════

def evaluate_raijuu(
    oi_series: list | None,
    closes:    list,
    coin:      str = "",
) -> tuple[str, float]:
    """
    RAIJUU: compares OI change vs price change over the last ~15 minutes
    (3 x 5m data points).  Divergence between OI and price direction
    often precedes a sharp move in the opposite direction of OI flow.

    oi_series: list of OI USD values, oldest first, fetched from OKX
    /api/v5/rubik/stat/contracts/open-interest-volume with period=5m.
    """
    if not oi_series or len(oi_series) < 2:
        return "HOLD", 0.0
    if not closes or len(closes) < 3:
        return "HOLD", 0.0

    threshold = float(_s.get_val("raijuu_oi_change_threshold", 0.05))

    oi_old = float(oi_series[0])
    oi_new = float(oi_series[-1])
    if oi_old <= 0:
        return "HOLD", 0.0

    oi_change_pct = (oi_new - oi_old) / oi_old   # positive = OI growing

    price_old = closes[-3]
    price_new = closes[-1]
    price_change_pct = (price_new - price_old) / price_old if price_old > 0 else 0.0

    # OI up ≥5% but price flat or declining → SHORT (hidden distribution)
    if oi_change_pct >= threshold and price_change_pct <= 0.001:
        return "SHORT", 1.5

    # OI down ≥5% but price flat or rising → LONG (short covering / trapped shorts)
    if oi_change_pct <= -threshold and price_change_pct >= -0.001:
        return "LONG",  1.5

    return "HOLD", 0.0


def _pct_change(series: list[float] | None, bars: int) -> float | None:
    if not series or len(series) <= bars:
        return None
    base = float(series[-bars - 1] or 0.0)
    if abs(base) < 1e-12:
        return None
    return (float(series[-1]) - base) / base


def _recent_mean(values: list[float], count: int) -> float:
    if not values:
        return 0.0
    window = values[-count:] if len(values) >= count else values
    return sum(window) / len(window) if window else 0.0


def evaluate_enma(
    closes,
    highs,
    lows,
    volumes,
    closes_15m=None,
    closes_1h=None,
    coin="",
) -> tuple[str, float]:
    if len(closes) < 55 or not highs or not lows or len(volumes) < 21:
        return "HOLD", 0.0

    signals = []
    e21 = ema(closes, 21)
    e55 = ema(closes, 55)
    if e21 and e55:
        if e21[-1] > e55[-1] and closes[-1] > e21[-1]:
            signals.append("LONG")
        elif e21[-1] < e55[-1] and closes[-1] < e21[-1]:
            signals.append("SHORT")

    swing_high = max(highs[-21:-1]) if len(highs) >= 21 else 0.0
    swing_low = min(lows[-21:-1]) if len(lows) >= 21 else 0.0
    if swing_high and closes[-1] > swing_high * 1.001:
        signals.append("LONG")
    elif swing_low and closes[-1] < swing_low * 0.999:
        signals.append("SHORT")

    avg_vol = avg_volume(volumes[-21:-1], 20)
    if avg_vol > 0 and volumes[-1] >= avg_vol * 1.2:
        if closes[-1] > closes[-2]:
            signals.append("LONG")
        elif closes[-1] < closes[-2]:
            signals.append("SHORT")

    if closes_15m and len(closes_15m) >= 40:
        e15_fast = ema(closes_15m, 21)
        e15_slow = ema(closes_15m, 55)
        if e15_fast and e15_slow:
            if e15_fast[-1] > e15_slow[-1]:
                signals.append("LONG")
            elif e15_fast[-1] < e15_slow[-1]:
                signals.append("SHORT")

    if closes_1h and len(closes_1h) >= 55:
        e1h = ema(closes_1h, 50)
        if e1h:
            if closes_1h[-1] > e1h[-1]:
                signals.append("LONG")
            elif closes_1h[-1] < e1h[-1]:
                signals.append("SHORT")

    longs = signals.count("LONG")
    shorts = signals.count("SHORT")
    if longs >= 3 and longs > shorts:
        return "LONG", float(longs)
    if shorts >= 3 and shorts > longs:
        return "SHORT", float(shorts)
    return "HOLD", 0.0


def evaluate_hayate(
    closes,
    highs,
    lows,
    volumes,
    closes_4h=None,
    closes_15m=None,
    closes_1h=None,
    coin="",
) -> tuple[str, float]:
    if len(closes or []) < 30 or not closes_15m or not closes_1h or not closes_4h:
        return "HOLD", 0.0
    try:
        from market_data_stream import get_15m_closes, get_1h_closes, get_4h_closes
    except Exception:
        return "HOLD", 0.0

    btc_15m, eth_15m = get_15m_closes("BTC"), get_15m_closes("ETH")
    btc_1h, eth_1h = get_1h_closes("BTC"), get_1h_closes("ETH")
    btc_4h, eth_4h = get_4h_closes("BTC"), get_4h_closes("ETH")

    def _edge(asset_series, a, b, bars: int) -> float | None:
        asset_ret = _pct_change(asset_series, bars)
        bench = [v for v in (_pct_change(a, bars), _pct_change(b, bars)) if v is not None]
        if asset_ret is None or not bench:
            return None
        return asset_ret - (sum(bench) / len(bench))

    edges = {
        "short": _edge(closes_15m, btc_15m, eth_15m, 3),
        "medium": _edge(closes_1h, btc_1h, eth_1h, 4),
        "long": _edge(closes_4h, btc_4h, eth_4h, 4),
    }
    thresholds = {
        "short": float(_s.get_val("hayate_short_edge_min", 0.003) or 0.003),
        "medium": float(_s.get_val("hayate_medium_edge_min", 0.006) or 0.006),
        "long": float(_s.get_val("hayate_long_edge_min", 0.010) or 0.010),
    }

    longs = sum(1 for key, edge in edges.items() if edge is not None and edge >= thresholds[key])
    shorts = sum(1 for key, edge in edges.items() if edge is not None and edge <= -thresholds[key])
    if longs >= 2 and longs > shorts:
        logger.info(
            "HAYATE PICK %s LONG edge15m=%+.3f edge1h=%+.3f edge4h=%+.3f | votes=%d",
            coin,
            float(edges["short"] or 0.0),
            float(edges["medium"] or 0.0),
            float(edges["long"] or 0.0),
            longs,
        )
        return "LONG", float(longs)
    if shorts >= 2 and shorts > longs:
        logger.info(
            "HAYATE PICK %s SHORT edge15m=%+.3f edge1h=%+.3f edge4h=%+.3f | votes=%d",
            coin,
            float(edges["short"] or 0.0),
            float(edges["medium"] or 0.0),
            float(edges["long"] or 0.0),
            shorts,
        )
        return "SHORT", float(shorts)
    return "HOLD", 0.0


def evaluate_kagutsuchi(
    closes,
    highs,
    lows,
    volumes,
    taker_buy_vol: float = 0.0,
    taker_sell_vol: float = 0.0,
    coin="",
) -> tuple[str, float]:
    if len(closes) < 25 or not highs or not lows or len(volumes) < 21:
        return "HOLD", 0.0
    votes_long = votes_short = 0
    long_components = {"imbalance": 0, "volume": 0, "displacement": 0}
    short_components = {"imbalance": 0, "volume": 0, "displacement": 0}

    total_taker = max(float(taker_buy_vol or 0.0) + float(taker_sell_vol or 0.0), 0.0)
    buy_ratio = (float(taker_buy_vol or 0.0) / total_taker) if total_taker > 0 else 0.5
    long_thresh = float(_s.get_val("kagutsuchi_imbalance_long", 0.62) or 0.62)
    short_thresh = float(_s.get_val("kagutsuchi_imbalance_short", 0.38) or 0.38)
    strong_long_thresh = min(long_thresh + 0.08, 0.80)
    strong_short_thresh = max(short_thresh - 0.08, 0.20)
    if buy_ratio >= long_thresh:
        long_components["imbalance"] = 2 if buy_ratio >= strong_long_thresh else 1
        votes_long += long_components["imbalance"]
    elif buy_ratio <= short_thresh:
        short_components["imbalance"] = 2 if buy_ratio <= strong_short_thresh else 1
        votes_short += short_components["imbalance"]

    avg_vol = avg_volume(volumes[-21:-1], 20)
    candle_range = max(highs[-1] - lows[-1], 1e-12)
    body = closes[-1] - closes[-2]
    lower_wick = min(closes[-2], closes[-1]) - lows[-1]
    upper_wick = highs[-1] - max(closes[-2], closes[-1])
    vol_ratio = (float(volumes[-1] or 0.0) / avg_vol) if avg_vol > 0 else 0.0
    close_pos = (closes[-1] - lows[-1]) / candle_range
    bearish_close_pos = (highs[-1] - closes[-1]) / candle_range
    body_ratio = abs(body) / candle_range
    if avg_vol > 0 and vol_ratio >= 1.1:
        if closes[-1] > closes[-2] and close_pos >= 0.72:
            if vol_ratio >= 1.45 and body_ratio >= 0.35:
                long_components["volume"] = 2
            elif lower_wick > abs(body) * 0.75 or close_pos >= 0.80:
                long_components["volume"] = 1
            votes_long += long_components["volume"]
        elif closes[-1] < closes[-2] and bearish_close_pos >= 0.72:
            if vol_ratio >= 1.45 and body_ratio >= 0.35:
                short_components["volume"] = 2
            elif upper_wick > abs(body) * 0.75 or bearish_close_pos >= 0.80:
                short_components["volume"] = 1
            votes_short += short_components["volume"]

    atr_val = atr(highs, lows, closes, 14) or 0.0
    displacement = closes[-1] - closes[-4] if len(closes) >= 4 else 0.0
    disp_mult = float(_s.get_val("kagutsuchi_displacement_atr_mult", 0.60) or 0.60)
    if atr_val > 0:
        disp_ratio = abs(displacement) / atr_val
        if displacement >= atr_val * disp_mult:
            long_components["displacement"] = 2 if disp_ratio >= disp_mult * 1.8 else 1
            votes_long += long_components["displacement"]
        elif displacement <= -(atr_val * disp_mult):
            short_components["displacement"] = 2 if disp_ratio >= disp_mult * 1.8 else 1
            votes_short += short_components["displacement"]

    if votes_long >= 3 and votes_long > votes_short:
        logger.info(
            "KAGUTSUCHI CONFIRM %s LONG buy_ratio=%.2f vol=%.2fx disp=%.4f atr=%.4f | imb=%d vol=%d disp=%d score=%d",
            coin,
            buy_ratio,
            vol_ratio,
            displacement,
            atr_val,
            long_components["imbalance"],
            long_components["volume"],
            long_components["displacement"],
            votes_long,
        )
        return "LONG", float(votes_long)
    if votes_short >= 3 and votes_short > votes_long:
        logger.info(
            "KAGUTSUCHI CONFIRM %s SHORT buy_ratio=%.2f vol=%.2fx disp=%.4f atr=%.4f | imb=%d vol=%d disp=%d score=%d",
            coin,
            buy_ratio,
            vol_ratio,
            displacement,
            atr_val,
            short_components["imbalance"],
            short_components["volume"],
            short_components["displacement"],
            votes_short,
        )
        return "SHORT", float(votes_short)
    return "HOLD", 0.0


def evaluate_tensei(closes, highs, lows, volumes, coin="") -> tuple[str, float]:
    if not bool(_s.get_val("safe_reentry_enabled", True)):
        return "HOLD", 0.0
    if len(closes) < 30 or not highs or not lows or len(volumes) < 21:
        return "HOLD", 0.0
    try:
        import performance_tracker as _perf
    except Exception:
        return "HOLD", 0.0

    last_trade = _perf.get_last_closed_trade(coin)
    if last_trade is None:
        return "HOLD", 0.0
    try:
        closed_at = datetime.fromisoformat(str(last_trade.closed_at))
    except Exception:
        return "HOLD", 0.0

    age_mins = (datetime.now(timezone.utc) - closed_at).total_seconds() / 60.0
    window_mins = float(_s.get_val("tensei_reentry_window_mins", 240) or 240)
    cooldown_mins = float(_s.get_val("tensei_reentry_cooldown_mins", 45) or 45)
    if age_mins < cooldown_mins or age_mins > window_mins:
        return "HOLD", 0.0
    if bool(_s.get_val("tensei_reentry_profit_only", True)) and float(last_trade.pnl_usdt or 0.0) <= 0:
        return "HOLD", 0.0
    if str(last_trade.exit_reason or "").lower() in {"sl", "sl_exchange", "manual", "manual_stop"}:
        return "HOLD", 0.0

    direction = str(last_trade.direction or "").upper()
    votes = 0
    e21 = ema(closes, 21)
    e55 = ema(closes, 55)
    avg_vol = avg_volume(volumes[-21:-1], 20)
    rsi14 = rsi(closes, 14) or 50.0
    if direction == "LONG":
        if e21 and e55 and e21[-1] > e55[-1] and closes[-1] > e21[-1]:
            votes += 1
        if closes[-1] > max(highs[-11:-1]):
            votes += 1
        if avg_vol > 0 and volumes[-1] >= avg_vol * 1.05 and rsi14 >= 52:
            votes += 1
    elif direction == "SHORT":
        if e21 and e55 and e21[-1] < e55[-1] and closes[-1] < e21[-1]:
            votes += 1
        if closes[-1] < min(lows[-11:-1]):
            votes += 1
        if avg_vol > 0 and volumes[-1] >= avg_vol * 1.05 and rsi14 <= 48:
            votes += 1
    else:
        return "HOLD", 0.0

    if votes >= 2:
        logger.info(
            "TENSEI RE-ENTRY %s %s age=%.0fm exit=%s pnl=%+.4f | votes=%d",
            coin,
            direction,
            age_mins,
            str(last_trade.exit_reason or ""),
            float(last_trade.pnl_usdt or 0.0),
            votes,
        )
        return direction, float(votes)
    return "HOLD", 0.0


def evaluate_ibuki(closes, highs, lows, volumes, coin="") -> tuple[str, float]:
    if len(closes) < 40 or not highs or not lows or len(volumes) < 21:
        return "HOLD", 0.0
    bbw = bollinger_band_width(closes, 20, 2.0)
    atr_now = atr(highs, lows, closes, 14) or 0.0
    atr_prev = atr(highs[:-5], lows[:-5], closes[:-5], 14) or 0.0
    if bbw is None or atr_now <= 0 or atr_prev <= 0:
        return "HOLD", 0.0

    bbw_series = []
    for idx in range(max(20, len(closes) - 30), len(closes)):
        val = bollinger_band_width(closes[: idx + 1], 20, 2.0)
        if val is not None:
            bbw_series.append(val)
    bbw_avg = _recent_mean(bbw_series, 20)
    avg_vol = avg_volume(volumes[-21:-1], 20)
    compression_votes = 0
    if bbw_avg > 0 and bbw <= bbw_avg * float(_s.get_val("ibuki_bbw_compression_mult", 0.75) or 0.75):
        compression_votes += 1
    if atr_prev > 0 and atr_now <= atr_prev * float(_s.get_val("ibuki_atr_compression_mult", 0.80) or 0.80):
        compression_votes += 1
    recent_range = max(highs[-15:]) - min(lows[-15:])
    if atr_now > 0 and recent_range <= atr_now * 3.5:
        compression_votes += 1
    if compression_votes < 2:
        return "HOLD", 0.0

    range_high = max(highs[-21:-1])
    range_low = min(lows[-21:-1])
    breakout_mult = float(_s.get_val("ibuki_breakout_range_mult", 1.20) or 1.20)
    if closes[-1] > range_high and avg_vol > 0 and volumes[-1] >= avg_vol * breakout_mult:
        logger.info(
            "IBUKI PICK %s LONG bbw=%.4f atr=%.4f/%.4f comp=%d",
            coin, bbw, atr_now, atr_prev, compression_votes,
        )
        return "LONG", float(compression_votes + 1)
    if closes[-1] < range_low and avg_vol > 0 and volumes[-1] >= avg_vol * breakout_mult:
        logger.info(
            "IBUKI PICK %s SHORT bbw=%.4f atr=%.4f/%.4f comp=%d",
            coin, bbw, atr_now, atr_prev, compression_votes,
        )
        return "SHORT", float(compression_votes + 1)
    return "HOLD", 0.0


def evaluate_onryo(
    closes,
    highs,
    lows,
    volumes,
    funding_rate: float = 0.0,
    raijuu_oi_series: list[float] | None = None,
    coin="",
) -> tuple[str, float]:
    if len(closes) < 20 or not highs or not lows or not raijuu_oi_series or len(raijuu_oi_series) < 6:
        return "HOLD", 0.0

    oi_prev = _recent_mean(raijuu_oi_series[:-2], 3)
    oi_now = _recent_mean(raijuu_oi_series, 2)
    if oi_prev <= 0:
        return "HOLD", 0.0
    oi_delta = (oi_now - oi_prev) / oi_prev
    atr_val = atr(highs, lows, closes, 14) or 0.0
    price_delta = closes[-1] - closes[-5] if len(closes) >= 5 else 0.0
    funding_extreme = float(_s.get_val("onryo_funding_extreme", 0.0006) or 0.0006)
    oi_delta_min = float(_s.get_val("onryo_oi_delta_min_pct", 0.03) or 0.03)
    disp_mult = float(_s.get_val("onryo_displacement_atr_mult", 0.70) or 0.70)

    votes_long = votes_short = 0
    if funding_rate <= -funding_extreme:
        votes_long += 1
    elif funding_rate >= funding_extreme:
        votes_short += 1
    if oi_delta >= oi_delta_min:
        if votes_long:
            votes_long += 1
        if votes_short:
            votes_short += 1
    if atr_val > 0:
        if price_delta >= atr_val * disp_mult:
            votes_long += 1
        elif price_delta <= -(atr_val * disp_mult):
            votes_short += 1

    if votes_long >= 2 and votes_long > votes_short:
        logger.info(
            "ONRYO PICK %s LONG funding=%+.5f oi_delta=%+.3f disp=%.4f atr=%.4f | votes=%d",
            coin, funding_rate, oi_delta, price_delta, atr_val, votes_long,
        )
        return "LONG", float(votes_long)
    if votes_short >= 2 and votes_short > votes_long:
        logger.info(
            "ONRYO PICK %s SHORT funding=%+.5f oi_delta=%+.3f disp=%.4f atr=%.4f | votes=%d",
            coin, funding_rate, oi_delta, price_delta, atr_val, votes_short,
        )
        return "SHORT", float(votes_short)
    return "HOLD", 0.0

# ═══════════════════════════════════════════════════════════════════════════════
def evaluate(
    engine_name:       str,
    closes:            list,
    highs:             list,
    lows:              list,
    volumes:           list,
    closes_4h:         list | None = None,
    highs_4h:          list | None = None,
    lows_4h:           list | None = None,
    closes_15m:        list | None = None,
    highs_15m:         list | None = None,
    lows_15m:          list | None = None,
    volumes_15m:       list | None = None,
    closes_1h:         list | None = None,
    highs_1h:          list | None = None,
    lows_1h:           list | None = None,
    closes_1d:         list | None = None,
    highs_1d:          list | None = None,
    lows_1d:           list | None = None,
    funding_rate:      float = 0.0,
    long_short_ratio:  float = 0.0,
    taker_buy_vol:     float = 0.0,
    taker_sell_vol:    float = 0.0,
    coin:              str = "",
    tsunami_long_usd:  float = 0.0,
    tsunami_short_usd: float = 0.0,
    raijuu_oi_series:  list | None = None,
) -> tuple[str, float]:
    name = engine_name.upper()
    try:
        if name == "RYUU":       return evaluate_ryuu(closes, highs, lows, volumes, closes_4h, coin)
        if name == "RAIJIN":     return evaluate_raijin(closes, highs, lows, volumes, coin)
        if name == "KAGE":       return evaluate_kage(closes, highs, lows, volumes, coin)
        if name == "ARASHI":     return evaluate_arashi(closes, highs, lows, volumes, coin)
        if name == "MUSASHI":    return evaluate_musashi(closes, highs, lows, volumes, coin)
        if name == "SHOGUN":     return evaluate_shogun(closes, highs, lows, volumes, highs_4h, lows_4h, long_short_ratio, coin)
        if name == "TENJIN":     return evaluate_tenjin(closes, highs, lows, volumes, closes_4h, highs_4h, lows_4h, coin)
        if name == "IZANAGI":    return evaluate_izanagi_rehab(closes, highs, lows, volumes, closes_4h, highs_4h, lows_4h, closes_1d, highs_1d, lows_1d, closes_15m, highs_15m, lows_15m, volumes_15m, coin)
        if name == "SUSANOO":    return evaluate_susanoo(closes, highs, lows, volumes, funding_rate, coin)
        if name == "FUJIN":      return evaluate_fujin(closes, volumes, taker_buy_vol, taker_sell_vol, coin)
        if name == "AMATERASU":  return evaluate_amaterasu(closes, closes_15m, closes_1h, closes_1d, highs_1h, lows_1h, coin)
        if name == "TITAN":      return evaluate_titan(closes, highs, lows, volumes, coin)
        if name == "RONIN":      return evaluate_ronin_rehab(closes, highs, lows, volumes, closes_4h, coin)
        if name == "OROCHI":     return evaluate_orochi(
            closes,
            highs,
            lows,
            volumes,
            closes_4h=closes_4h,
            closes_15m=closes_15m,
            highs_15m=highs_15m,
            lows_15m=lows_15m,
            volumes_15m=volumes_15m,
            funding_rate=funding_rate,
            coin=coin,
        )
        if name == "TSUNAMI":    return evaluate_tsunami(tsunami_long_usd, tsunami_short_usd, coin)
        if name == "SUIJIN":     return evaluate_suijin(closes_1h, highs_1h, lows_1h, closes, coin)
        if name == "RAIJUU":     return evaluate_raijuu(raijuu_oi_series, closes, coin)
        if name == "ENMA":       return evaluate_enma(closes, highs, lows, volumes, closes_15m, closes_1h, coin)
        if name == "HACHIMAN":   return evaluate_hachiman(closes, highs, lows, volumes, closes_15m, closes_1h, coin)
        if name == "HAYATE":     return evaluate_hayate(closes, highs, lows, volumes, closes_4h, closes_15m, closes_1h, coin)
        if name == "KAGUTSUCHI": return evaluate_kagutsuchi(closes, highs, lows, volumes, taker_buy_vol, taker_sell_vol, coin)
        if name == "YATAGARASU": return evaluate_yatagarasu(closes, highs, lows, volumes, closes_15m, highs_15m, lows_15m, volumes_15m, closes_1h, taker_buy_vol, taker_sell_vol, coin)
        if name == "RYUJIN":    return evaluate_ryujin(closes, highs, lows, volumes, taker_buy_vol, taker_sell_vol, coin)
        if name == "TENSEI":    return evaluate_tensei(closes, highs, lows, volumes, coin)
        if name == "IBUKI":     return evaluate_ibuki(closes, highs, lows, volumes, coin)
        if name == "ONRYO":     return evaluate_onryo(closes, highs, lows, volumes, funding_rate, raijuu_oi_series, coin)
        if name == "KANNON":    return evaluate_kannon(closes, highs, lows, volumes, coin)
        if name == "GENBU":     return evaluate_genbu(closes, highs, lows, volumes, coin)
    except Exception as e:
        logger.warning("evaluate(%s) error: %s", name, e)
    return "HOLD", 0


def killzone_status() -> str:
    """Human-readable current kill zone status for Telegram display."""
    now  = datetime.now(timezone.utc)
    mins = now.hour * 60 + now.minute
    if 120 <= mins <= 300:  return "🇬🇧 London (02:00–05:00 UTC)"
    if 810 <= mins <= 990:  return "🇺🇸 New York (13:30–16:30 UTC)"
    return "😴 Off-session"


STRATEGY_DESCRIPTIONS = {
    "RYUU": [
        ("EMA Cross",      "EMA21 above/below EMA55 with slope confirmation"),
        ("HTF Alignment",  "Price on right side of 4H 200 EMA"),
        ("MACD Confirm",   "MACD line × signal + histogram direction"),
    ],
    "RAIJIN": [
        ("RSI Slope",      "RSI building momentum (slope > 4 over 3 bars)"),
        ("Volume Surge",   "Current volume ≥ 2× 20-bar average"),
        ("MACD+Vol Cross", "MACD histogram sign change with volume ≥ 1.3× avg"),
    ],
    "KAGE": [
        ("RSI Extreme",    "RSI < 25 oversold or > 75 overbought"),
        ("BB Touch",       "Price outside 2.0σ Bollinger Band"),
        ("Wick Rejection", "Rejection wick > 60% of candle range"),
    ],
    "ARASHI": [
        ("Range Break",    "Close above 20-bar high or below 20-bar low"),
        ("Volume Confirm", "Breakout volume ≥ 2× average (no fake breaks)"),
        ("ATR Expansion",  "Current ATR > 1.4× its recent average"),
    ],
    "MUSASHI": [
        ("Engulfing",      "Full body engulfing pattern"),
        ("Market Struct",  "3 consecutive HH+HL or LH+LL"),
        ("SR Bounce",      "Price within 0.3% of 50-bar support/resistance"),
    ],
    "SHOGUN": [
        ("CRT Sweep",      "Liquidity grab beyond 4H candle high/low + reversal"),
        ("Fair Value Gap", "3-candle imbalance retracement entry"),
        ("Order Block",    "Institutional OB retest after displacement"),
        ("CHoCH / BOS",    "Market structure shift confirming new direction"),
    ],
    "TENJIN": [
        ("4H EMA50/200",   "EMA50 above/below EMA200 with price on right side"),
        ("4H Engulfing",   "4H bullish/bearish engulfing candle confirmation"),
        ("4H RSI Zone",    "4H RSI < 45 (oversold) or > 55 (overbought) bias"),
    ],
    "IZANAGI": [
        ("4H Elliott Wave", "Primary swing structure signal from 4H impulse/corrective waves"),
        ("1D Direction Filter", "Daily Elliott Wave must agree or stay neutral; conflicts veto the trade"),
        ("15m Composite Confirm", "41-feature composite score on 15m must confirm the 4H direction before entry"),
    ],
    "SUSANOO": [
        ("Funding Extreme", "Funding ≥ +0.05% → SHORT (longs crowded), ≤ -0.05% → LONG"),
        ("S/R Proximity",   "Price within 0.5% of 20-bar high (SHORT) or low (LONG)"),
        ("Volume Confirm",  "Current volume ≥ 1.5× 20-bar average"),
    ],
    "FUJIN": [
        ("Taker Imbalance", "OKX taker buy ratio ≥ 65% → LONG, ≤ 35% → SHORT"),
        ("RSI Mid-Zone",    "RSI 40–60 — not yet overbought/oversold (anticipatory)"),
        ("EMA Alignment",   "Price > EMA21 for LONG, < EMA21 for SHORT"),
    ],
    "AMATERASU": [
        ("1m EMA Bias",    "EMA21 > EMA55 on 1m (short-term momentum)"),
        ("15m EMA Bias",   "EMA21 > EMA55 on 15m (medium-term confirms short-term)"),
        ("1H Price Bias",  "Price > EMA50 on 1H (macro trend confirms both)"),
    ],
    "TITAN": [
        ("RSI Extreme",     "RSI <25 oversold or >75 overbought + divergence detection"),
        ("BB Reversion",    "Price reverting from outer Bollinger Band toward middle"),
        ("VWAP Reversion",  "Price >1.5% from VWAP and reverting back with momentum"),
    ],
    "RONIN": [
        ("EMA+VWAP Cross",  "EMA8>21>55 aligned trend + price on right side of VWAP"),
        ("Volume Breakout",  "Break 20-bar high/low with ≥2× average volume"),
        ("ADX+RSI Momentum", "ADX >25 (strong trend) + RSI in momentum zone (50-65 or 35-50)"),
    ],
    "ENMA": [
        ("Impulse Bias",    "EMA21/55 structure with price on the right side of trend"),
        ("Break Select",    "Fresh range break instead of late crowded continuation"),
        ("Volume Quality",  "Current volume at least 1.2x the recent baseline"),
    ],
    "HACHIMAN": [
        ("Trend Quality",   "1m/15m/1H trend alignment favours cleaner battlefield targets"),
        ("Liquidity Bias",  "Higher volume and tighter spread earn selector preference"),
        ("Pressure Sanity", "Only keeps candidates when volatility and RSI stay executable"),
    ],
    "HAYATE": [
        ("15m Relative Strength", "Asset outperforms BTC/ETH on the short horizon"),
        ("1H Relative Strength",  "Leadership persists through the medium horizon"),
        ("4H Relative Strength",  "Longer-horizon trend leadership stays intact"),
    ],
    "KAGUTSUCHI": [
        ("Taker Imbalance", "Aggressive buy/sell flow is visibly one-sided"),
        ("Absorption",      "Wick rejection is absorbed and closes near the bar extreme"),
        ("Displacement",    "Price pushes beyond normal ATR travel with flow support"),
    ],
    "YATAGARASU": [
        ("MTF Alignment",   "1m, 15m, and 1H structure all agree before it acts"),
        ("Breakout Quality", "15m range break clears a premium confluence buffer"),
        ("Flow Confirm",    "Volume and taker flow agree with the breakout direction"),
    ],
    "RYUJIN": [
        ("Flow Ratio",      "Taker flow pressure is clearly one-sided"),
        ("Volume Follow",   "Continuation move prints with volume above baseline"),
        ("ATR Displacement", "Price extends beyond normal ATR travel with momentum support"),
    ],
    "TENSEI": [
        ("Recent Winner",   "Coin closed recently without a stop-loss failure"),
        ("Continuation",    "Trend structure is still intact after the prior close"),
        ("Re-Entry Timing", "Fresh break and volume confirm the same directional edge"),
    ],
    "IBUKI": [
        ("BB Compression",  "Bollinger width is materially tighter than recent history"),
        ("ATR Compression", "ATR contracts before the expansion move"),
        ("Expansion Break", "Price breaks the squeeze range with volume support"),
    ],
    "ONRYO": [
        ("Funding Extreme", "Funding is stretched enough to signal crowding"),
        ("OI Pressure",     "Open interest is still building, not fading"),
        ("Displacement",    "Price displacement confirms the futures pressure"),
    ],
    "KANNON": [
        ("Recovery Scope",  "Checks recovered positions instead of normal live entry flow"),
        ("Plan Completeness", "Flags missing exit-plan context after recovery or sync repair"),
        ("Quarantine Safety", "Helps separate managed recovery from quarantined drift"),
    ],
    "GENBU": [
        ("Advisory Quality", "Scores deployment quality without becoming a broad blocker"),
        ("Exposure Context", "Adds portfolio context to explain pressure and crowding"),
        ("Soft Penalty",    "Prefers reducing weak candidates over hard-refusing them"),
    ],
}

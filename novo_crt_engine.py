"""Pure advisory Candle Range Theory detector for Novo intelligence."""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any


_MIN_CANDLES = 5


def _safe_get_setting(settings: Any, key: str, default: Any = None) -> Any:
    if settings is None:
        return default
    try:
        if isinstance(settings, Mapping):
            return settings.get(key, default)
        getter = getattr(settings, "get_val", None)
        if callable(getter):
            return getter(key, default)
        return getattr(settings, key, default)
    except Exception:
        return default


def _empty_result(
    *,
    status: str,
    timeframe: Any = None,
    side: Any = None,
    bias: str = "NEUTRAL",
    crt_state: str = "UNKNOWN",
    score: int = 0,
) -> dict[str, Any]:
    return {
        "status": status,
        "timeframe": None if timeframe is None else str(timeframe),
        "side": None if side is None else str(side).upper(),
        "bias": bias,
        "crt_state": crt_state,
        "score": int(max(0, min(100, score))),
        "reference_candle": {
            "high": None,
            "low": None,
            "mid": None,
            "top_25": None,
            "bottom_25": None,
            "range": None,
        },
        "current_position": "UNKNOWN",
        "events": [],
        "explanation": "",
        "warnings": [],
        "errors": [],
    }


def _num(candle: Mapping[str, Any], *keys: str) -> float | None:
    for key in keys:
        if key in candle:
            try:
                value = float(candle[key])
            except Exception:
                return None
            if value == value:
                return value
            return None
    return None


def _normalize_candles(candles: Any) -> list[dict[str, float]]:
    if isinstance(candles, (str, bytes)) or not isinstance(candles, Sequence):
        return []
    normalized: list[dict[str, float]] = []
    for candle in candles:
        if not isinstance(candle, Mapping):
            continue
        open_ = _num(candle, "open", "o")
        high = _num(candle, "high", "h")
        low = _num(candle, "low", "l")
        close = _num(candle, "close", "c")
        volume = _num(candle, "volume", "v")
        timestamp = _num(candle, "timestamp", "ts")
        if open_ is None or high is None or low is None or close is None:
            continue
        if high < low:
            continue
        row = {"open": open_, "high": high, "low": low, "close": close}
        if volume is not None:
            row["volume"] = max(0.0, volume)
        if timestamp is not None:
            row["timestamp"] = timestamp
        normalized.append(row)
    return normalized


def _avg(values: list[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def _classify_side(side: Any) -> str | None:
    if side is None:
        return None
    value = str(side).strip().upper()
    if value in {"LONG", "BUY", "BULL", "BULLISH"}:
        return "LONG"
    if value in {"SHORT", "SELL", "BEAR", "BEARISH"}:
        return "SHORT"
    return value or None


def _reference_candle(candles: list[dict[str, float]], lookback: int) -> dict[str, float]:
    candidates = candles[-(lookback + 1) : -1]
    if not candidates:
        return candles[-2]
    return max(candidates, key=lambda c: c["high"] - c["low"])


def _zones(ref: dict[str, float]) -> dict[str, float]:
    low = ref["low"]
    high = ref["high"]
    rng = max(high - low, 0.0)
    return {
        "high": float(high),
        "low": float(low),
        "mid": float(low + rng * 0.50),
        "top_25": float(low + rng * 0.75),
        "bottom_25": float(low + rng * 0.25),
        "range": float(rng),
    }


def _position(close: float, z: dict[str, float]) -> str:
    if z["range"] <= 0:
        return "UNKNOWN"
    if close > z["high"]:
        return "OUTSIDE_ABOVE"
    if close < z["low"]:
        return "OUTSIDE_BELOW"
    if close >= z["top_25"]:
        return "TOP_25"
    if close <= z["bottom_25"]:
        return "BOTTOM_25"
    return "MIDDLE_50"


def analyze_crt(candles, side=None, timeframe=None, settings=None) -> dict:
    """Analyze Candle Range Theory context without side effects."""
    try:
        min_candles = int(_safe_get_setting(settings, "novo_crt_min_candles", _MIN_CANDLES) or _MIN_CANDLES)
        lookback = int(_safe_get_setting(settings, "novo_crt_reference_lookback", 6) or 6)
        normalized = _normalize_candles(candles)
        side_norm = _classify_side(side)
        required = max(4, min_candles)
        if len(normalized) < required:
            result = _empty_result(status="INSUFFICIENT_DATA", timeframe=timeframe, side=side_norm)
            result["explanation"] = f"Need at least {required} valid candles; received {len(normalized)}."
            return result

        ref = _reference_candle(normalized, max(1, lookback))
        current = normalized[-1]
        z = _zones(ref)
        current_position = _position(current["close"], z)
        events: list[str] = []

        ranges = [max(c["high"] - c["low"], 0.0) for c in normalized[:-1]]
        avg_range = _avg(ranges)
        current_range = max(current["high"] - current["low"], 0.0)
        expansion = bool(avg_range > 0 and current_range >= avg_range * 1.15)
        ref_bullish = ref["close"] > ref["open"]
        ref_bearish = ref["close"] < ref["open"]
        close_location = (current["close"] - current["low"]) / max(current_range, 1e-12)

        swept_below = current["low"] < z["low"]
        swept_above = current["high"] > z["high"]
        bullish_reclaim = swept_below and current["close"] > z["bottom_25"] and close_location >= 0.55
        bearish_reject = swept_above and current["close"] < z["top_25"] and close_location <= 0.45
        break_above = current["close"] > z["high"] and expansion
        break_below = current["close"] < z["low"] and expansion
        bullish_retrace_hold = (
            ref_bullish
            and z["low"] <= current["low"] <= z["mid"]
            and current["close"] >= z["mid"]
            and current["close"] > current["open"]
        )
        bearish_retrace_hold = (
            ref_bearish
            and z["mid"] <= current["high"] <= z["high"]
            and current["close"] <= z["mid"]
            and current["close"] < current["open"]
        )

        bias = "NEUTRAL"
        crt_state = "NO_SETUP"
        score = 35

        if bullish_reclaim:
            bias = "LONG"
            crt_state = "BULLISH_RECLAIM"
            score = 82
            events.append("BULLISH_RECLAIM")
        elif bearish_reject:
            bias = "SHORT"
            crt_state = "BEARISH_REJECT"
            score = 82
            events.append("BEARISH_REJECT")
        elif bullish_retrace_hold:
            bias = "LONG"
            crt_state = "BULLISH_RETRACE_HOLD"
            score = 74
            events.append("BULLISH_RETRACE_HOLD")
        elif bearish_retrace_hold:
            bias = "SHORT"
            crt_state = "BEARISH_RETRACE_HOLD"
            score = 74
            events.append("BEARISH_RETRACE_HOLD")
        elif break_above:
            bias = "LONG"
            crt_state = "RANGE_EXPANSION"
            score = 68
            events.append("RANGE_EXPANSION_ABOVE")
        elif break_below:
            bias = "SHORT"
            crt_state = "RANGE_EXPANSION"
            score = 68
            events.append("RANGE_EXPANSION_BELOW")
        else:
            events.append("NO_SETUP")

        if expansion:
            events.append("EXPANSION")

        if side_norm in {"LONG", "SHORT"}:
            if bias == side_norm and bias != "NEUTRAL":
                events.append("SIDE_ALIGNED")
                score = min(92, score + 8)
            elif bias != "NEUTRAL":
                events.append("SIDE_CONFLICT")
                score = max(0, score - 12)
            else:
                events.append("SIDE_NEUTRAL")

        result = _empty_result(status="OK", timeframe=timeframe, side=side_norm, bias=bias, crt_state=crt_state, score=score)
        result["reference_candle"] = z
        result["current_position"] = current_position
        result["events"] = events
        result["explanation"] = (
            f"CRT state {crt_state} with {bias} bias. "
            f"Reference low={z['low']:.8g}, mid={z['mid']:.8g}, high={z['high']:.8g}; "
            f"current close={current['close']:.8g} in {current_position}."
        )
        if side_norm in {"LONG", "SHORT"}:
            if "SIDE_ALIGNED" in events:
                result["explanation"] += f" CRT supports {side_norm}."
            elif "SIDE_CONFLICT" in events:
                result["explanation"] += f" CRT conflicts with {side_norm}."
            else:
                result["explanation"] += f" CRT is neutral for {side_norm}."
        return result
    except Exception as exc:
        result = _empty_result(status="ERROR", timeframe=timeframe, side=side)
        result["errors"].append(f"{type(exc).__name__}: {exc}")
        result["explanation"] = "CRT analysis failed safely."
        return result

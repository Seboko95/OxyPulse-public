"""Pure advisory 4H higher-timeframe bias detector for Novo intelligence."""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any


_MIN_CANDLES = 8


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
    side: Any = None,
    bias: str = "NEUTRAL",
    bias_type: str = "UNKNOWN",
    strength: int = 0,
    score: int = 0,
) -> dict[str, Any]:
    return {
        "status": status,
        "timeframe": "4H",
        "side": None if side is None else str(side).upper(),
        "bias": bias,
        "bias_type": bias_type,
        "strength": int(max(0, min(100, strength))),
        "score": int(max(0, min(100, score))),
        "events": [],
        "levels": {
            "last_high": None,
            "last_low": None,
            "last_close": None,
            "previous_high": None,
            "previous_low": None,
            "previous_close": None,
        },
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


def _overlap_ratio(candles: list[dict[str, float]]) -> float:
    if len(candles) < 2:
        return 0.0
    overlaps = 0
    comparisons = 0
    for prev, cur in zip(candles, candles[1:]):
        comparisons += 1
        if cur["low"] <= prev["high"] and cur["high"] >= prev["low"]:
            overlaps += 1
    return overlaps / comparisons if comparisons else 0.0


def analyze_4h_bias(candles_4h, side=None, settings=None) -> dict:
    """Analyze 4H bias from candle dictionaries without side effects."""
    try:
        min_candles = int(_safe_get_setting(settings, "novo_4h_bias_min_candles", _MIN_CANDLES) or _MIN_CANDLES)
        normalized = _normalize_candles(candles_4h)
        side_norm = _classify_side(side)
        required = max(5, min_candles)
        if len(normalized) < required:
            result = _empty_result(status="INSUFFICIENT_DATA", side=side_norm)
            result["explanation"] = f"Need at least {required} valid 4H candles; received {len(normalized)}."
            return result

        last = normalized[-1]
        previous = normalized[-2]
        recent_n = max(3, min(5, len(normalized) // 2))
        recent = normalized[-recent_n:]
        prior = normalized[:-recent_n] or normalized

        last_range = max(last["high"] - last["low"], 0.0)
        last_body = abs(last["close"] - last["open"])
        body_pct = last_body / last_range if last_range > 0 else 0.0
        upper_wick = last["high"] - max(last["open"], last["close"])
        lower_wick = min(last["open"], last["close"]) - last["low"]
        wick_pct = (upper_wick + lower_wick) / last_range if last_range > 0 else 0.0
        close_location = (last["close"] - last["low"]) / last_range if last_range > 0 else 0.5
        bullish_body = last["close"] > last["open"]
        bearish_body = last["close"] < last["open"]

        ranges = [max(c["high"] - c["low"], 0.0) for c in normalized]
        volumes = [c.get("volume", 0.0) for c in normalized]
        recent_range = _avg(ranges[-recent_n:])
        prior_range = _avg(ranges[:-recent_n] or ranges)
        range_ratio = recent_range / prior_range if prior_range > 0 else 1.0
        recent_volume = _avg(volumes[-recent_n:])
        prior_volume = _avg(volumes[:-recent_n] or volumes)
        volume_ratio = recent_volume / prior_volume if prior_volume > 0 else 1.0

        recent_closes = [c["close"] for c in recent]
        slope = recent_closes[-1] - recent_closes[0]
        slope_pct = slope / max(abs(recent_closes[0]), 1e-12)
        overlap = _overlap_ratio(recent)

        previous_high = previous["high"]
        previous_low = previous["low"]
        sweep_low_reclaim = last["low"] < previous_low and last["close"] > previous_low and close_location >= 0.55
        sweep_high_reject = last["high"] > previous_high and last["close"] < previous_high and close_location <= 0.45
        range_expansion = range_ratio >= 1.15
        volume_expansion = volume_ratio >= 1.15

        bias = "NEUTRAL"
        bias_type = "RANGE"
        strength = 45
        score = 45
        events: list[str] = []

        if bullish_body and body_pct >= 0.58 and close_location >= 0.72 and range_expansion and volume_expansion:
            bias = "LONG"
            bias_type = "BULLISH_EXPANSION"
            strength = 86
            score = 86
            events.append("BULLISH_EXPANSION")
        elif bearish_body and body_pct >= 0.58 and close_location <= 0.28 and range_expansion and volume_expansion:
            bias = "SHORT"
            bias_type = "BEARISH_EXPANSION"
            strength = 86
            score = 86
            events.append("BEARISH_EXPANSION")
        elif sweep_low_reclaim:
            bias = "LONG"
            bias_type = "MANIPULATION"
            strength = 78
            score = 78
            events.append("SWEEP_LOW_RECLAIM")
        elif sweep_high_reject:
            bias = "SHORT"
            bias_type = "MANIPULATION"
            strength = 78
            score = 78
            events.append("SWEEP_HIGH_REJECT")
        elif body_pct <= 0.28 and wick_pct >= 0.55:
            bias_type = "INDECISION"
            strength = 35
            score = 40
            events.append("INDECISION")
        elif overlap >= 0.80 and abs(slope_pct) < 0.02 and not range_expansion:
            bias_type = "RANGE"
            strength = 40
            score = 45
            events.append("RANGE")
        elif slope_pct > 0.02:
            bias = "LONG"
            bias_type = "BULLISH_EXPANSION" if range_expansion else "RANGE"
            strength = 62
            score = 62
            events.append("BULLISH_DRIFT")
        elif slope_pct < -0.02:
            bias = "SHORT"
            bias_type = "BEARISH_EXPANSION" if range_expansion else "RANGE"
            strength = 62
            score = 62
            events.append("BEARISH_DRIFT")
        else:
            events.append("RANGE")

        if range_expansion:
            events.append("RANGE_EXPANDING")
        if volume_expansion:
            events.append("VOLUME_EXPANDING")

        if side_norm in {"LONG", "SHORT"}:
            if bias == side_norm and bias != "NEUTRAL":
                events.append("SIDE_ALIGNED")
                score = min(95, score + 7)
                strength = min(95, strength + 5)
            elif bias != "NEUTRAL":
                events.append("SIDE_CONFLICT")
                score = max(0, score - 12)
                strength = max(0, strength - 8)
            else:
                events.append("SIDE_NEUTRAL")

        result = _empty_result(status="OK", side=side_norm, bias=bias, bias_type=bias_type, strength=strength, score=score)
        result["events"] = events
        result["levels"] = {
            "last_high": float(last["high"]),
            "last_low": float(last["low"]),
            "last_close": float(last["close"]),
            "previous_high": float(previous_high),
            "previous_low": float(previous_low),
            "previous_close": float(previous["close"]),
        }
        result["explanation"] = (
            f"4H bias {bias} via {bias_type}. "
            f"Body={body_pct:.2f}, wick={wick_pct:.2f}, close_location={close_location:.2f}, "
            f"range_ratio={range_ratio:.2f}, volume_ratio={volume_ratio:.2f}, slope={slope_pct:.2%}."
        )
        if side_norm in {"LONG", "SHORT"}:
            if "SIDE_ALIGNED" in events:
                result["explanation"] += f" 4H bias supports {side_norm}."
            elif "SIDE_CONFLICT" in events:
                result["explanation"] += f" 4H bias conflicts with {side_norm}."
            else:
                result["explanation"] += f" 4H bias is neutral for {side_norm}."
        return result
    except Exception as exc:
        result = _empty_result(status="ERROR", side=side)
        result["errors"].append(f"{type(exc).__name__}: {exc}")
        result["explanation"] = "4H bias analysis failed safely."
        return result


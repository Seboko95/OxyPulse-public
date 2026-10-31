"""Pure advisory market DNA classifier for Novo intelligence.

This module is standalone and side-effect free. It performs simple candle
analysis only and has no authority over trading decisions.
"""
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
    timeframe: Any = None,
    side: Any = None,
    bias: str = "NEUTRAL",
    score: int = 0,
) -> dict[str, Any]:
    return {
        "status": status,
        "timeframe": None if timeframe is None else str(timeframe),
        "side": None if side is None else str(side).upper(),
        "dna": {
            "volatility_state": "UNKNOWN",
            "volume_state": "UNKNOWN",
            "trend_state": "UNKNOWN",
            "candle_state": "UNKNOWN",
            "risk_texture": "UNKNOWN",
        },
        "bias": bias,
        "score": int(max(0, min(100, score))),
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


def _close_slope(closes: list[float]) -> float:
    if len(closes) < 2:
        return 0.0
    return closes[-1] - closes[0]


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


def analyze_market_dna(candles, side=None, timeframe=None, settings=None) -> dict:
    """Classify market DNA from candle dictionaries without side effects."""
    try:
        min_candles = int(_safe_get_setting(settings, "novo_market_dna_min_candles", _MIN_CANDLES) or _MIN_CANDLES)
        normalized = _normalize_candles(candles)
        side_norm = _classify_side(side)
        required = max(5, min_candles)
        if len(normalized) < required:
            result = _empty_result(status="INSUFFICIENT_DATA", timeframe=timeframe, side=side_norm)
            result["explanation"] = f"Need at least {required} valid candles; received {len(normalized)}."
            return result

        ranges = [max(c["high"] - c["low"], 0.0) for c in normalized]
        bodies = [abs(c["close"] - c["open"]) for c in normalized]
        closes = [c["close"] for c in normalized]
        volumes = [c.get("volume", 0.0) for c in normalized]
        recent_n = max(3, min(5, len(normalized) // 2))
        recent = normalized[-recent_n:]
        prior = normalized[:-recent_n] or normalized

        recent_ranges = ranges[-recent_n:]
        prior_ranges = ranges[: -recent_n] or ranges
        avg_range = _avg(prior_ranges)
        recent_range = _avg(recent_ranges)
        range_ratio = recent_range / avg_range if avg_range > 0 else 1.0

        prior_volumes = volumes[: -recent_n]
        recent_volumes = volumes[-recent_n:]
        avg_volume = _avg(prior_volumes)
        recent_volume = _avg(recent_volumes)
        volume_ratio = recent_volume / avg_volume if avg_volume > 0 else 1.0

        last = normalized[-1]
        last_range = max(last["high"] - last["low"], 0.0)
        last_body = abs(last["close"] - last["open"])
        body_pct = last_body / last_range if last_range > 0 else 0.0
        upper_wick = last["high"] - max(last["open"], last["close"])
        lower_wick = min(last["open"], last["close"]) - last["low"]
        wick_pct = (upper_wick + lower_wick) / last_range if last_range > 0 else 0.0
        close_location = (last["close"] - last["low"]) / last_range if last_range > 0 else 0.5

        recent_closes = closes[-recent_n:]
        slope = _close_slope(recent_closes)
        slope_pct = slope / max(abs(recent_closes[0]), 1e-12)
        alternating = 0
        recent_dirs = []
        for candle in recent:
            if candle["close"] > candle["open"]:
                recent_dirs.append(1)
            elif candle["close"] < candle["open"]:
                recent_dirs.append(-1)
            else:
                recent_dirs.append(0)
        for left, right in zip(recent_dirs, recent_dirs[1:]):
            if left and right and left != right:
                alternating += 1
        alternation_ratio = alternating / max(len(recent_dirs) - 1, 1)
        overlap = _overlap_ratio(recent)
        avg_wick_pct = _avg([
            ((c["high"] - max(c["open"], c["close"])) + (min(c["open"], c["close"]) - c["low"])) / max(c["high"] - c["low"], 1e-12)
            for c in recent
        ])

        volatility_state = "NORMAL"
        if range_ratio >= 1.35:
            volatility_state = "EXPANDING"
        elif range_ratio <= 0.75:
            volatility_state = "COMPRESSING"

        volume_state = "NORMAL"
        if volume_ratio >= 1.35:
            volume_state = "EXPANDING"
        elif volume_ratio <= 0.70:
            volume_state = "DRYING"

        trend_state = "RANGING"
        if abs(slope_pct) >= 0.015 and alternation_ratio <= 0.50:
            trend_state = "TRENDING"
        elif alternation_ratio >= 0.65 or overlap >= 0.90:
            trend_state = "CHOPPY"

        candle_state = "CONTINUATION"
        bullish_extreme = close_location >= 0.72
        bearish_extreme = close_location <= 0.28
        if body_pct >= 0.62 and (bullish_extreme or bearish_extreme) and range_ratio >= 1.10:
            candle_state = "IMPULSE"
        elif wick_pct >= 0.55 and (bullish_extreme or bearish_extreme):
            candle_state = "ABSORPTION"
        elif wick_pct >= 0.55:
            candle_state = "REVERSAL"
        elif volatility_state == "COMPRESSING" and body_pct <= 0.45:
            candle_state = "COMPRESSION"

        risk_texture = "CLEAN"
        if volume_state == "DRYING":
            risk_texture = "THIN"
        if risk_texture != "THIN" and avg_wick_pct >= 0.55:
            risk_texture = "WICKY"
        if risk_texture != "THIN" and (trend_state == "CHOPPY" or (overlap >= 0.90 and alternation_ratio >= 0.45)):
            risk_texture = "NOISY"

        bias = "NEUTRAL"
        if trend_state == "TRENDING" or candle_state in {"IMPULSE", "ABSORPTION", "REVERSAL"}:
            if slope > 0 or close_location >= 0.72:
                bias = "LONG"
            elif slope < 0 or close_location <= 0.28:
                bias = "SHORT"

        events: list[str] = []
        if volatility_state == "EXPANDING":
            events.append("VOLATILITY_EXPANDING")
        elif volatility_state == "COMPRESSING":
            events.append("VOLATILITY_COMPRESSING")
        if volume_state == "EXPANDING":
            events.append("VOLUME_EXPANDING")
        elif volume_state == "DRYING":
            events.append("VOLUME_DRYING")
        if candle_state != "UNKNOWN":
            events.append(candle_state)
        if risk_texture in {"NOISY", "WICKY", "THIN"}:
            events.append(f"RISK_{risk_texture}")

        score = 55
        if risk_texture == "CLEAN" and volatility_state == "EXPANDING" and volume_state == "EXPANDING" and bias != "NEUTRAL":
            score = 86
        elif risk_texture == "CLEAN" and trend_state == "TRENDING" and candle_state in {"CONTINUATION", "IMPULSE"}:
            score = 76
        elif candle_state in {"ABSORPTION", "REVERSAL"} and bias != "NEUTRAL":
            score = 70
        elif trend_state in {"CHOPPY", "RANGING"} or risk_texture in {"NOISY", "WICKY", "THIN"}:
            score = 45
        if volatility_state == "COMPRESSING":
            score = min(score, 55)

        if side_norm in {"LONG", "SHORT"}:
            if bias == side_norm and bias != "NEUTRAL":
                events.append("SIDE_ALIGNED")
                score = min(95, score + 7)
            elif bias != "NEUTRAL":
                events.append("SIDE_CONFLICT")
                score = max(0, score - 12)
            else:
                events.append("SIDE_NEUTRAL")

        result = _empty_result(status="OK", timeframe=timeframe, side=side_norm, bias=bias, score=score)
        result["dna"] = {
            "volatility_state": volatility_state,
            "volume_state": volume_state,
            "trend_state": trend_state,
            "candle_state": candle_state,
            "risk_texture": risk_texture,
        }
        result["events"] = events
        result["explanation"] = (
            f"Market DNA: volatility={volatility_state}, volume={volume_state}, "
            f"trend={trend_state}, candle={candle_state}, texture={risk_texture}, bias={bias}. "
            f"Range ratio={range_ratio:.2f}, volume ratio={volume_ratio:.2f}, "
            f"body={body_pct:.2f}, wick={wick_pct:.2f}, close_location={close_location:.2f}."
        )
        if side_norm in {"LONG", "SHORT"}:
            if "SIDE_ALIGNED" in events:
                result["explanation"] += f" DNA supports {side_norm}."
            elif "SIDE_CONFLICT" in events:
                result["explanation"] += f" DNA conflicts with {side_norm}."
            else:
                result["explanation"] += f" DNA is neutral for {side_norm}."
        return result
    except Exception as exc:
        result = _empty_result(status="ERROR", timeframe=timeframe, side=side)
        result["errors"].append(f"{type(exc).__name__}: {exc}")
        result["explanation"] = "Market DNA analysis failed safely."
        return result

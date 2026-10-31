"""Pure advisory market-structure detector for Novo intelligence.

This module is intentionally standalone. It does not import runtime trading,
execution, risk, sizing, registry, exchange, Telegram, or scheduler modules.
"""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any


_MIN_CANDLES = 6


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
    structure: str = "INSUFFICIENT_DATA",
    bias: str = "NEUTRAL",
    score: int = 0,
) -> dict[str, Any]:
    return {
        "status": status,
        "timeframe": None if timeframe is None else str(timeframe),
        "side": None if side is None else str(side).upper(),
        "structure": structure,
        "bias": bias,
        "score": int(max(0, min(100, score))),
        "swings": {
            "last_high": None,
            "last_low": None,
            "previous_high": None,
            "previous_low": None,
        },
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
        high = _num(candle, "high", "h")
        low = _num(candle, "low", "l")
        close = _num(candle, "close", "c")
        open_ = _num(candle, "open", "o")
        volume = _num(candle, "volume", "v")
        timestamp = _num(candle, "timestamp", "ts")
        if high is None or low is None or close is None:
            continue
        if high < low:
            continue
        row = {"high": high, "low": low, "close": close}
        if open_ is not None:
            row["open"] = open_
        if volume is not None:
            row["volume"] = volume
        if timestamp is not None:
            row["timestamp"] = timestamp
        normalized.append(row)
    return normalized


def _pivot_swings(candles: list[dict[str, float]], window: int = 1) -> tuple[list[tuple[int, float]], list[tuple[int, float]]]:
    highs: list[tuple[int, float]] = []
    lows: list[tuple[int, float]] = []
    count = len(candles)
    for idx in range(window, count - window):
        high = candles[idx]["high"]
        low = candles[idx]["low"]
        left = candles[idx - window : idx]
        right = candles[idx + 1 : idx + 1 + window]
        if all(high >= c["high"] for c in left) and all(high > c["high"] for c in right):
            highs.append((idx, high))
        if all(low <= c["low"] for c in left) and all(low < c["low"] for c in right):
            lows.append((idx, low))
    return highs, lows


def _fallback_swings(candles: list[dict[str, float]]) -> tuple[float, float, float, float]:
    split = max(2, len(candles) // 2)
    earlier = candles[:split]
    later = candles[split:]
    previous_high = max(c["high"] for c in earlier)
    previous_low = min(c["low"] for c in earlier)
    last_high = max(c["high"] for c in later)
    last_low = min(c["low"] for c in later)
    return previous_high, previous_low, last_high, last_low


def _latest_swings(candles: list[dict[str, float]]) -> tuple[float, float, float, float, list[str]]:
    pivot_highs, pivot_lows = _pivot_swings(candles)
    warnings: list[str] = []
    if len(pivot_highs) >= 2 and len(pivot_lows) >= 2:
        previous_high = pivot_highs[-2][1]
        last_high = pivot_highs[-1][1]
        previous_low = pivot_lows[-2][1]
        last_low = pivot_lows[-1][1]
        return previous_high, previous_low, last_high, last_low, warnings
    warnings.append("Using range fallback because local pivots are sparse.")
    previous_high, previous_low, last_high, last_low = _fallback_swings(candles)
    return previous_high, previous_low, last_high, last_low, warnings


def _classify_side(side: Any) -> str | None:
    if side is None:
        return None
    value = str(side).strip().upper()
    if value in {"LONG", "BUY", "BULL", "BULLISH"}:
        return "LONG"
    if value in {"SHORT", "SELL", "BEAR", "BEARISH"}:
        return "SHORT"
    return value or None


def analyze_market_structure(candles, side=None, timeframe=None, settings=None) -> dict:
    """Analyze market structure from candle dictionaries without side effects."""
    try:
        min_candles = int(_safe_get_setting(settings, "novo_market_structure_min_candles", _MIN_CANDLES) or _MIN_CANDLES)
        normalized = _normalize_candles(candles)
        side_norm = _classify_side(side)

        if len(normalized) < max(4, min_candles):
            result = _empty_result(status="INSUFFICIENT_DATA", timeframe=timeframe, side=side_norm)
            result["explanation"] = f"Need at least {max(4, min_candles)} valid candles; received {len(normalized)}."
            return result

        previous_high, previous_low, last_high, last_low, warnings = _latest_swings(normalized)
        last_close = normalized[-1]["close"]

        events: list[str] = []
        if last_high > previous_high:
            events.append("HH")
        elif last_high < previous_high:
            events.append("LH")

        if last_low > previous_low:
            events.append("HL")
        elif last_low < previous_low:
            events.append("LL")

        bullish_structure = "HH" in events and "HL" in events
        bearish_structure = "LH" in events and "LL" in events
        bullish_bos = last_close > previous_high
        bearish_bos = last_close < previous_low

        bias = "NEUTRAL"
        structure = "RANGE_BOUND"
        score = 50

        if bullish_structure:
            bias = "LONG"
            structure = "TREND_CONTINUATION"
            score = 76
        elif bearish_structure:
            bias = "SHORT"
            structure = "TREND_CONTINUATION"
            score = 76

        if bullish_bos:
            events.append("BOS_BULLISH")
            structure = "BOS_BULLISH"
            bias = "LONG"
            score = 90 if bullish_structure else 84
        elif bearish_bos:
            events.append("BOS_BEARISH")
            structure = "BOS_BEARISH"
            bias = "SHORT"
            score = 90 if bearish_structure else 84

        if bullish_structure and previous_high < last_high and previous_low < last_low and last_close > previous_high:
            events.append("CHOCH_BULLISH")
        elif bearish_structure and previous_high > last_high and previous_low > last_low and last_close < previous_low:
            events.append("CHOCH_BEARISH")

        if structure == "RANGE_BOUND":
            score = 50 if events else 40
        if side_norm in {"LONG", "SHORT"}:
            if bias == side_norm and bias != "NEUTRAL":
                events.append("SIDE_ALIGNED")
                score = min(100, score + 5)
            elif bias != "NEUTRAL":
                events.append("SIDE_CONFLICT")
                score = max(0, score - 10)

        if side_norm == "LONG" and bias == "SHORT":
            structure = "TREND_FAILURE" if structure == "BOS_BEARISH" else structure
        elif side_norm == "SHORT" and bias == "LONG":
            structure = "TREND_FAILURE" if structure == "BOS_BULLISH" else structure

        explanation_parts = [
            f"Structure {structure} with {bias} bias.",
            f"Highs {previous_high:.8g}->{last_high:.8g}; lows {previous_low:.8g}->{last_low:.8g}; close {last_close:.8g}.",
        ]
        if side_norm in {"LONG", "SHORT"}:
            if "SIDE_ALIGNED" in events:
                explanation_parts.append(f"Structure supports {side_norm}.")
            elif "SIDE_CONFLICT" in events:
                explanation_parts.append(f"Structure conflicts with {side_norm}.")
            else:
                explanation_parts.append(f"Structure is neutral for {side_norm}.")

        result = _empty_result(status="OK", timeframe=timeframe, side=side_norm, structure=structure, bias=bias, score=score)
        result["swings"] = {
            "last_high": float(last_high),
            "last_low": float(last_low),
            "previous_high": float(previous_high),
            "previous_low": float(previous_low),
        }
        result["events"] = events
        result["explanation"] = " ".join(explanation_parts)
        result["warnings"] = warnings
        return result
    except Exception as exc:
        result = _empty_result(status="ERROR", timeframe=timeframe, side=side)
        result["errors"].append(f"{type(exc).__name__}: {exc}")
        result["explanation"] = "Market structure analysis failed safely."
        return result

"""Pure advisory liquidity sweep detector for Novo intelligence."""
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
    sweep_state: str = "UNKNOWN",
    score: int = 0,
) -> dict[str, Any]:
    return {
        "status": status,
        "timeframe": None if timeframe is None else str(timeframe),
        "side": None if side is None else str(side).upper(),
        "bias": bias,
        "sweep_state": sweep_state,
        "score": int(max(0, min(100, score))),
        "swept_level": {
            "type": None,
            "price": None,
            "distance_pct": None,
        },
        "levels": {
            "previous_high": None,
            "previous_low": None,
            "equal_high": None,
            "equal_low": None,
            "session_high": None,
            "session_low": None,
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


def _level_value(levels: Any, key: str) -> float | None:
    if not isinstance(levels, Mapping) or key not in levels:
        return None
    try:
        value = float(levels[key])
    except Exception:
        return None
    return value if value == value else None


def _detect_equal_level(values: list[float], tolerance_pct: float) -> float | None:
    if len(values) < 3:
        return None
    for idx, value in enumerate(values):
        matches = [other for j, other in enumerate(values) if j != idx and abs(other - value) / max(abs(value), 1e-12) * 100 <= tolerance_pct]
        if len(matches) >= 1:
            cluster = [value, *matches]
            return sum(cluster) / len(cluster)
    return None


def _distance_pct(price: float, level: float) -> float:
    return abs(price - level) / max(abs(level), 1e-12) * 100


def _candidate_levels(candles: list[dict[str, float]], provided: Any, tolerance_pct: float) -> dict[str, float | None]:
    prior = candles[:-1]
    recent = prior[-8:] if len(prior) >= 8 else prior
    highs = [c["high"] for c in recent]
    lows = [c["low"] for c in recent]
    return {
        "previous_high": _level_value(provided, "previous_high") if _level_value(provided, "previous_high") is not None else (max(highs) if highs else None),
        "previous_low": _level_value(provided, "previous_low") if _level_value(provided, "previous_low") is not None else (min(lows) if lows else None),
        "equal_high": _level_value(provided, "equal_high") if _level_value(provided, "equal_high") is not None else _detect_equal_level(highs, tolerance_pct),
        "equal_low": _level_value(provided, "equal_low") if _level_value(provided, "equal_low") is not None else _detect_equal_level(lows, tolerance_pct),
        "session_high": _level_value(provided, "session_high"),
        "session_low": _level_value(provided, "session_low"),
    }


def _best_high_level(levels: dict[str, float | None], current_high: float, current_close: float) -> tuple[str | None, float | None]:
    candidates = [
        (name, price)
        for name, price in levels.items()
        if name.endswith("high") and price is not None and current_high > price
    ]
    if not candidates:
        return None, None
    return min(candidates, key=lambda item: abs(current_close - float(item[1])))


def _best_low_level(levels: dict[str, float | None], current_low: float, current_close: float) -> tuple[str | None, float | None]:
    candidates = [
        (name, price)
        for name, price in levels.items()
        if name.endswith("low") and price is not None and current_low < price
    ]
    if not candidates:
        return None, None
    return min(candidates, key=lambda item: abs(current_close - float(item[1])))


def analyze_liquidity_sweep(candles, side=None, timeframe=None, levels=None, settings=None) -> dict:
    """Analyze candle/level liquidity sweeps without side effects."""
    try:
        min_candles = int(_safe_get_setting(settings, "novo_liquidity_sweep_min_candles", _MIN_CANDLES) or _MIN_CANDLES)
        tolerance_pct = float(_safe_get_setting(settings, "novo_equal_level_tolerance_pct", 0.12) or 0.12)
        normalized = _normalize_candles(candles)
        side_norm = _classify_side(side)
        required = max(4, min_candles)
        if len(normalized) < required:
            result = _empty_result(status="INSUFFICIENT_DATA", timeframe=timeframe, side=side_norm)
            result["explanation"] = f"Need at least {required} valid candles; received {len(normalized)}."
            return result

        current = normalized[-1]
        derived_levels = _candidate_levels(normalized, levels, tolerance_pct)
        events: list[str] = []
        if derived_levels.get("equal_high") is not None:
            events.append("EQUAL_HIGHS")
        if derived_levels.get("equal_low") is not None:
            events.append("EQUAL_LOWS")

        high_type, high_level = _best_high_level(derived_levels, current["high"], current["close"])
        low_type, low_level = _best_low_level(derived_levels, current["low"], current["close"])

        bias = "NEUTRAL"
        sweep_state = "NO_SWEEP"
        score = 35
        swept_type: str | None = None
        swept_price: float | None = None

        if low_type and low_level is not None and current["close"] > low_level:
            bias = "LONG"
            sweep_state = "SWEEP_LOW_RECLAIM"
            score = 82
            swept_type, swept_price = low_type, low_level
            events.append("SWEEP_LOW_RECLAIM")
        elif high_type and high_level is not None and current["close"] < high_level:
            bias = "SHORT"
            sweep_state = "SWEEP_HIGH_REJECT"
            score = 82
            swept_type, swept_price = high_type, high_level
            events.append("SWEEP_HIGH_REJECT")
        elif any(price is not None and current["close"] > price for name, price in derived_levels.items() if name.endswith("high")):
            high_breaks = [(name, price) for name, price in derived_levels.items() if name.endswith("high") and price is not None and current["close"] > price]
            swept_type, swept_price = max(high_breaks, key=lambda item: float(item[1]))
            bias = "LONG"
            sweep_state = "BREAKOUT_HOLD"
            score = 68
            events.append("BREAKOUT_HOLD")
        elif any(price is not None and current["close"] < price for name, price in derived_levels.items() if name.endswith("low")):
            low_breaks = [(name, price) for name, price in derived_levels.items() if name.endswith("low") and price is not None and current["close"] < price]
            swept_type, swept_price = min(low_breaks, key=lambda item: float(item[1]))
            bias = "SHORT"
            sweep_state = "BREAKDOWN_HOLD"
            score = 68
            events.append("BREAKDOWN_HOLD")
        elif "EQUAL_HIGHS" in events:
            sweep_state = "EQUAL_HIGHS"
            score = 55
        elif "EQUAL_LOWS" in events:
            sweep_state = "EQUAL_LOWS"
            score = 55
        else:
            events.append("NO_SWEEP")

        volumes = [c.get("volume", 0.0) for c in normalized[:-1] if "volume" in c]
        latest_volume = current.get("volume")
        if latest_volume is not None and volumes:
            avg_volume = _avg(volumes[-8:] if len(volumes) >= 8 else volumes)
            if avg_volume > 0 and latest_volume >= avg_volume * 1.35:
                events.append("VOLUME_CONFIRMATION")
                if bias != "NEUTRAL":
                    score = min(92, score + 5)

        if side_norm in {"LONG", "SHORT"}:
            if bias == side_norm and bias != "NEUTRAL":
                events.append("SIDE_ALIGNED")
                score = min(92, score + 5)
            elif bias != "NEUTRAL":
                events.append("SIDE_CONFLICT")
                score = max(0, score - 12)
            else:
                events.append("SIDE_NEUTRAL")

        result = _empty_result(status="OK", timeframe=timeframe, side=side_norm, bias=bias, sweep_state=sweep_state, score=score)
        result["levels"] = {key: (float(value) if value is not None else None) for key, value in derived_levels.items()}
        result["swept_level"] = {
            "type": swept_type,
            "price": float(swept_price) if swept_price is not None else None,
            "distance_pct": _distance_pct(current["close"], swept_price) if swept_price is not None else None,
        }
        result["events"] = events
        result["explanation"] = f"Liquidity state {sweep_state} with {bias} bias."
        if swept_type and swept_price is not None:
            result["explanation"] += f" Swept {swept_type} at {swept_price:.8g}."
        if side_norm in {"LONG", "SHORT"}:
            if "SIDE_ALIGNED" in events:
                result["explanation"] += f" Liquidity context supports {side_norm}."
            elif "SIDE_CONFLICT" in events:
                result["explanation"] += f" Liquidity context conflicts with {side_norm}."
            else:
                result["explanation"] += f" Liquidity context is neutral for {side_norm}."
        return result
    except Exception as exc:
        result = _empty_result(status="ERROR", timeframe=timeframe, side=side)
        result["errors"].append(f"{type(exc).__name__}: {exc}")
        result["explanation"] = "Liquidity sweep analysis failed safely."
        return result


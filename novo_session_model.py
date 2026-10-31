"""Pure advisory session/manipulation context detector for Novo intelligence."""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
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
    active_session: str = "UNKNOWN",
    bias: str = "NEUTRAL",
    session_state: str = "UNKNOWN",
    score: int = 0,
) -> dict[str, Any]:
    return {
        "status": status,
        "timeframe": None if timeframe is None else str(timeframe),
        "side": None if side is None else str(side).upper(),
        "active_session": active_session,
        "bias": bias,
        "session_state": session_state,
        "score": int(max(0, min(100, score))),
        "levels": {
            "asia_high": None,
            "asia_low": None,
            "london_high": None,
            "london_low": None,
            "ny_high": None,
            "ny_low": None,
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


def _classify_side(side: Any) -> str | None:
    if side is None:
        return None
    value = str(side).strip().upper()
    if value in {"LONG", "BUY", "BULL", "BULLISH"}:
        return "LONG"
    if value in {"SHORT", "SELL", "BEAR", "BEARISH"}:
        return "SHORT"
    return value or None


def _parse_now(now: Any) -> datetime:
    if now is None:
        return datetime.now(timezone.utc)
    if isinstance(now, datetime):
        return now.astimezone(timezone.utc) if now.tzinfo else now.replace(tzinfo=timezone.utc)
    if isinstance(now, str):
        text = now.strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        return datetime.fromisoformat(text).astimezone(timezone.utc)
    return datetime.now(timezone.utc)


def _active_session(now: Any) -> str:
    hour = _parse_now(now).hour
    if 0 <= hour < 7:
        return "ASIA"
    if 7 <= hour < 13:
        return "LONDON"
    if 13 <= hour < 21:
        return "NEW_YORK"
    return "OFF_HOURS"


def _level_value(levels: Any, key: str) -> float | None:
    if not isinstance(levels, Mapping) or key not in levels:
        return None
    try:
        value = float(levels[key])
    except Exception:
        return None
    return value if value == value else None


def _derive_levels(candles: list[dict[str, float]], session_levels: Any) -> dict[str, float | None]:
    prior = candles[:-1]
    recent = prior[-8:] if len(prior) >= 8 else prior
    highs = [c["high"] for c in recent]
    lows = [c["low"] for c in recent]
    fallback_high = max(highs) if highs else None
    fallback_low = min(lows) if lows else None
    return {
        "asia_high": _level_value(session_levels, "asia_high") if _level_value(session_levels, "asia_high") is not None else fallback_high,
        "asia_low": _level_value(session_levels, "asia_low") if _level_value(session_levels, "asia_low") is not None else fallback_low,
        "london_high": _level_value(session_levels, "london_high"),
        "london_low": _level_value(session_levels, "london_low"),
        "ny_high": _level_value(session_levels, "ny_high"),
        "ny_low": _level_value(session_levels, "ny_low"),
    }


def _add_side_event(events: list[str], side: str | None, bias: str, score: int) -> tuple[int, str | None]:
    if side not in {"LONG", "SHORT"}:
        return score, None
    if bias == side and bias != "NEUTRAL":
        events.append("SIDE_ALIGNED")
        return min(92, score + 7), "aligned"
    if bias != "NEUTRAL":
        events.append("SIDE_CONFLICT")
        return max(0, score - 12), "conflict"
    events.append("SIDE_NEUTRAL")
    return score, "neutral"


def analyze_session_model(candles, side=None, timeframe=None, now=None, session_levels=None, settings=None) -> dict:
    """Analyze session/manipulation context without side effects."""
    try:
        min_candles = int(_safe_get_setting(settings, "novo_session_model_min_candles", _MIN_CANDLES) or _MIN_CANDLES)
        normalized = _normalize_candles(candles)
        side_norm = _classify_side(side)
        active = _active_session(now)
        required = max(4, min_candles)
        if len(normalized) < required:
            result = _empty_result(status="INSUFFICIENT_DATA", timeframe=timeframe, side=side_norm, active_session=active)
            result["explanation"] = f"Need at least {required} valid candles; received {len(normalized)}."
            return result

        current = normalized[-1]
        levels = _derive_levels(normalized, session_levels)
        events: list[str] = []
        bias = "NEUTRAL"
        session_state = "RANGE_BOUND"
        score = 42

        asia_high = levels.get("asia_high")
        asia_low = levels.get("asia_low")
        london_high = levels.get("london_high")
        london_low = levels.get("london_low")

        active_high = None
        active_low = None
        if active == "ASIA":
            active_high, active_low = asia_high, asia_low
        elif active == "LONDON":
            active_high = london_high if london_high is not None else asia_high
            active_low = london_low if london_low is not None else asia_low
        elif active == "NEW_YORK":
            active_high = london_high if london_high is not None else asia_high
            active_low = london_low if london_low is not None else asia_low
        else:
            active_high, active_low = asia_high, asia_low

        if active == "ASIA" and asia_high is not None and asia_low is not None and asia_low <= current["close"] <= asia_high:
            session_state = "ASIA_RANGE"
            score = 40
            events.append("ASIA_RANGE")
        elif active == "LONDON" and asia_low is not None and current["low"] < asia_low and current["close"] > asia_low:
            bias = "LONG"
            session_state = "LONDON_SWEEP_LOW"
            score = 82
            events.append("LONDON_SWEEP_LOW")
        elif active == "LONDON" and asia_high is not None and current["high"] > asia_high and current["close"] < asia_high:
            bias = "SHORT"
            session_state = "LONDON_SWEEP_HIGH"
            score = 82
            events.append("LONDON_SWEEP_HIGH")
        elif active == "NEW_YORK" and active_low is not None and current["low"] < active_low and current["close"] > active_low:
            bias = "LONG"
            session_state = "NY_SWEEP_LOW"
            score = 82
            events.append("NY_SWEEP_LOW")
        elif active == "NEW_YORK" and active_high is not None and current["high"] > active_high and current["close"] < active_high:
            bias = "SHORT"
            session_state = "NY_SWEEP_HIGH"
            score = 82
            events.append("NY_SWEEP_HIGH")
        elif active_high is not None and current["close"] > active_high:
            bias = "LONG"
            session_state = "SESSION_EXPANSION_UP"
            score = 68
            events.append("SESSION_EXPANSION_UP")
        elif active_low is not None and current["close"] < active_low:
            bias = "SHORT"
            session_state = "SESSION_EXPANSION_DOWN"
            score = 68
            events.append("SESSION_EXPANSION_DOWN")
        else:
            events.append("RANGE_BOUND")

        score, side_state = _add_side_event(events, side_norm, bias, score)

        result = _empty_result(
            status="OK",
            timeframe=timeframe,
            side=side_norm,
            active_session=active,
            bias=bias,
            session_state=session_state,
            score=score,
        )
        result["levels"] = {key: (float(value) if value is not None else None) for key, value in levels.items()}
        result["events"] = events
        result["explanation"] = f"Session {active}: {session_state} with {bias} bias."
        if side_norm in {"LONG", "SHORT"}:
            if side_state == "aligned":
                result["explanation"] += f" Session context supports {side_norm}."
            elif side_state == "conflict":
                result["explanation"] += f" Session context conflicts with {side_norm}."
            else:
                result["explanation"] += f" Session context is neutral for {side_norm}."
        return result
    except Exception as exc:
        result = _empty_result(status="ERROR", timeframe=timeframe, side=side)
        result["errors"].append(f"{type(exc).__name__}: {exc}")
        result["explanation"] = "Session model analysis failed safely."
        return result


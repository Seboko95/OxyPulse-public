"""Alert-only Gem Hunter advisory scanner.

This module is intentionally standalone and side-effect free. It ranks unusual
opportunity candidates for operator awareness only; it has no execution,
admission, sizing, risk, registry, or Telegram authority.
"""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any


_MIN_CANDLES = 10


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


def _base_result(status: str, symbol: Any = None) -> dict[str, Any]:
    tier = "COLLECTING" if status == "INSUFFICIENT_DATA" else "IGNORE"
    return {
        "status": status,
        "symbol": None if symbol is None else str(symbol),
        "alert_only": True,
        "gem_score": 0,
        "gem_tier": tier,
        "direction": "NEUTRAL",
        "signals": {
            "volume_expansion": 0,
            "volatility_expansion": 0,
            "trend_acceleration": 0,
            "compression_breakout": 0,
            "liquidity_reclaim": 0,
            "structure_break": 0,
        },
        "events": [],
        "explanation": "",
        "warnings": [],
        "errors": [],
    }


def _avg(values: list[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def _clamp_score(value: float) -> int:
    return int(max(0, min(100, round(value))))


def _tier(score: int, has_data: bool = True) -> str:
    if not has_data:
        return "COLLECTING"
    if score >= 90:
        return "ELITE"
    if score >= 85:
        return "PRIORITY"
    if score >= 80:
        return "WATCHLIST"
    return "IGNORE" if score > 0 else "COLLECTING"


def _context_bonus(market_context: Any, events: list[str], warnings: list[str]) -> int:
    if not isinstance(market_context, Mapping):
        return 0
    bonus = 0
    try:
        social_score = market_context.get("social_score")
        if social_score is not None:
            social = max(0.0, min(100.0, float(social_score)))
            if social >= 75:
                bonus += 3
                events.append("SOCIAL_CONTEXT_BONUS")
        narrative = str(market_context.get("narrative") or "").strip()
        if narrative:
            bonus += 2
            events.append("NARRATIVE_CONTEXT_BONUS")
        listing_age = market_context.get("listing_age_days")
        if listing_age is not None and float(listing_age) <= 30:
            bonus += 2
            events.append("NEW_LISTING_CONTEXT")
    except Exception:
        warnings.append("Market context could not be fully evaluated.")
    return min(7, bonus)


def analyze_gem_candidate(symbol=None, candles=None, market_context=None, settings=None) -> dict:
    """Rank an unusual opportunity candidate for alert-only operator review."""
    try:
        enabled = bool(_safe_get_setting(settings, "novo_gem_hunter_enabled", False))
        result = _base_result("DISABLED" if not enabled else "INSUFFICIENT_DATA", symbol)
        if not enabled:
            result["gem_tier"] = "COLLECTING"
            result["explanation"] = "Gem Hunter is disabled."
            return result

        alert_only = bool(_safe_get_setting(settings, "gem_hunter_alert_only", True))
        if not alert_only:
            result["warnings"].append("gem_hunter_alert_only is false; execution output remains unavailable.")

        normalized = _normalize_candles(candles)
        min_candles = int(_safe_get_setting(settings, "gem_hunter_min_candles", _MIN_CANDLES) or _MIN_CANDLES)
        required = max(6, min_candles)
        if len(normalized) < required:
            result["status"] = "INSUFFICIENT_DATA"
            result["gem_tier"] = "COLLECTING"
            result["explanation"] = f"Need at least {required} valid candles; received {len(normalized)}."
            return result

        ranges = [max(c["high"] - c["low"], 0.0) for c in normalized]
        closes = [c["close"] for c in normalized]
        volumes = [c.get("volume", 0.0) for c in normalized]
        recent_n = max(3, min(5, len(normalized) // 3))
        prior = normalized[:-recent_n] or normalized
        recent = normalized[-recent_n:]
        last = normalized[-1]
        prior_ranges = ranges[:-recent_n] or ranges
        recent_ranges = ranges[-recent_n:]
        prior_volumes = volumes[:-recent_n]
        recent_volumes = volumes[-recent_n:]

        avg_prior_range = _avg(prior_ranges)
        avg_recent_range = _avg(recent_ranges)
        range_ratio = avg_recent_range / avg_prior_range if avg_prior_range > 0 else 1.0
        avg_prior_volume = _avg(prior_volumes)
        avg_recent_volume = _avg(recent_volumes)
        volume_ratio = avg_recent_volume / avg_prior_volume if avg_prior_volume > 0 else 1.0

        prior_high = max(c["high"] for c in prior)
        prior_low = min(c["low"] for c in prior)
        recent_closes = closes[-recent_n:]
        prior_close = closes[-recent_n - 1] if len(closes) > recent_n else closes[0]
        slope = recent_closes[-1] - recent_closes[0]
        slope_pct = slope / max(abs(recent_closes[0]), 1e-12)
        last_move_pct = (last["close"] - prior_close) / max(abs(prior_close), 1e-12)

        compression_window = normalized[-min(8, len(normalized)):-1]
        compression_ranges = [max(c["high"] - c["low"], 0.0) for c in compression_window]
        compression_avg = _avg(compression_ranges)
        tight_before_break = compression_avg <= (avg_prior_range * 0.75 if avg_prior_range > 0 else compression_avg)

        last_range = max(last["high"] - last["low"], 0.0)
        close_location = (last["close"] - last["low"]) / last_range if last_range > 0 else 0.5
        swept_low_reclaim = last["low"] < prior_low and last["close"] > prior_low
        swept_high_reject = last["high"] > prior_high and last["close"] < prior_high
        breaks_up = last["close"] > prior_high
        breaks_down = last["close"] < prior_low

        score = 0.0
        long_votes = 0
        short_votes = 0
        events: list[str] = []
        signals = result["signals"]

        if volume_ratio >= 1.6:
            signals["volume_expansion"] = min(100, int(round((volume_ratio - 1.0) * 45)))
            score += min(18, signals["volume_expansion"] * 0.18)
            events.append("VOLUME_EXPANSION")

        if range_ratio >= 1.35:
            signals["volatility_expansion"] = min(100, int(round((range_ratio - 1.0) * 55)))
            score += min(18, signals["volatility_expansion"] * 0.18)
            events.append("VOLATILITY_EXPANSION")

        if abs(slope_pct) >= 0.012 or abs(last_move_pct) >= 0.018:
            signals["trend_acceleration"] = min(100, int(round(max(abs(slope_pct), abs(last_move_pct)) * 1800)))
            score += min(20, signals["trend_acceleration"] * 0.20)
            events.append("TREND_ACCELERATION")
            if slope_pct > 0 or last_move_pct > 0:
                long_votes += 1
            if slope_pct < 0 or last_move_pct < 0:
                short_votes += 1

        if tight_before_break and (breaks_up or breaks_down):
            signals["compression_breakout"] = 85
            score += 18
            events.append("COMPRESSION_BREAKOUT")
            if breaks_up:
                long_votes += 1
            else:
                short_votes += 1

        if swept_low_reclaim or swept_high_reject:
            signals["liquidity_reclaim"] = 80
            score += 14
            events.append("LIQUIDITY_RECLAIM")
            if swept_low_reclaim:
                long_votes += 2
            if swept_high_reject:
                short_votes += 2

        if breaks_up or breaks_down:
            signals["structure_break"] = 90 if (volume_ratio >= 1.3 or range_ratio >= 1.2) else 70
            score += 16
            events.append("STRUCTURE_BREAK")
            if breaks_up:
                long_votes += 1
            if breaks_down:
                short_votes += 1

        if close_location >= 0.75 and (breaks_up or slope_pct > 0.012):
            long_votes += 1
        elif close_location <= 0.25 and (breaks_down or slope_pct < -0.012):
            short_votes += 1

        bonus = _context_bonus(market_context, events, result["warnings"])
        score += bonus
        score = _clamp_score(score)
        direction = "NEUTRAL"
        if long_votes > short_votes:
            direction = "LONG"
        elif short_votes > long_votes:
            direction = "SHORT"

        result.update({
            "status": "OK",
            "gem_score": score,
            "gem_tier": _tier(score),
            "direction": direction,
            "events": events,
            "explanation": (
                f"Alert-only Gem Hunter scan: score {score}, direction {direction}, "
                f"volume ratio {volume_ratio:.2f}, range ratio {range_ratio:.2f}."
            ),
        })
        return result
    except Exception as exc:
        result = _base_result("ERROR", symbol)
        result["errors"].append(str(exc))
        result["explanation"] = "Gem Hunter failed safely without execution output."
        return result

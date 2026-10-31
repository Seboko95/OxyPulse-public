"""Pure advisory Novo soft-enrichment suggestions.

The functions in this module return metadata only. They do not approve, reject,
block, place, prepare, or route trades.
"""
from __future__ import annotations

from collections.abc import Mapping
from copy import deepcopy
from typing import Any


_FLAG_KEYS = {
    "soft_confidence": "novo_soft_confidence_enabled",
    "exit_enrichment": "novo_exit_enrichment_enabled",
    "profit_lock_enrichment": "novo_profit_lock_enrichment_enabled",
    "fee_bleed_guard": "novo_fee_bleed_guard_enabled",
    "elite_size_boost": "novo_elite_size_boost_enabled",
}

_FORBIDDEN_OUTPUT_KEYS = {
    "order",
    "execute",
    "block",
    "reject",
    "approve",
    "admission_override",
    "trade_intent",
}


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


def _neutral_suggestions() -> dict[str, Any]:
    return {
        "tp1_multiplier": 1.0,
        "tp2_multiplier": 1.0,
        "trailing_activation_multiplier": 1.0,
        "trailing_distance_multiplier": 1.0,
        "break_even_delay_multiplier": 1.0,
        "profit_lock_multiplier": 1.0,
        "partial_min_notional_multiplier": 1.0,
        "execution_split_preference": "UNCHANGED",
        "size_multiplier": 1.0,
    }


def _base_result(status: str = "COLLECTING") -> dict[str, Any]:
    return {
        "status": status,
        "advisory_only": True,
        "no_hard_blocks": True,
        "quality_score": None,
        "quality_tier": "COLLECTING",
        "band": "COLLECTING",
        "suggestions": _neutral_suggestions(),
        "enabled_flags": {
            "soft_confidence": False,
            "exit_enrichment": False,
            "profit_lock_enrichment": False,
            "fee_bleed_guard": False,
            "elite_size_boost": False,
        },
        "events": [],
        "explanation": "",
        "warnings": [],
        "errors": [],
    }


def _enabled_flags(settings: Any) -> dict[str, bool]:
    return {
        name: bool(_safe_get_setting(settings, key, False))
        for name, key in _FLAG_KEYS.items()
    }


def _score(value: Any) -> int | None:
    try:
        score = float(value)
    except Exception:
        return None
    if score != score:
        return None
    return int(round(max(0.0, min(100.0, score))))


def _band_from_score(score: int | None) -> str:
    if score is None:
        return "COLLECTING"
    if score >= 90:
        return "ELITE"
    if score >= 80:
        return "STRONG"
    if score >= 70:
        return "GOOD"
    return "WEAK"


def _tier_from_novo(novo: Any, score: int | None) -> str:
    if not isinstance(novo, Mapping):
        return "COLLECTING"
    tier = str(novo.get("quality_tier", "") or "").strip().upper()
    if tier in {"ELITE", "STRONG", "GOOD", "WEAK", "COLLECTING"}:
        if score is None or tier == "COLLECTING":
            return tier
        return _band_from_score(score)
    return _band_from_score(score)


def _sanitize_result(result: dict[str, Any]) -> dict[str, Any]:
    for key in list(result.keys()):
        if key in _FORBIDDEN_OUTPUT_KEYS:
            result.pop(key, None)
    return result


def build_novo_enrichment(novo=None, side=None, settings=None, trade_context=None) -> dict:
    """Build future soft-enrichment suggestions from compact Novo metadata."""
    try:
        _ = deepcopy(trade_context) if isinstance(trade_context, Mapping) else trade_context
        flags = _enabled_flags(settings)
        result = _base_result("COLLECTING")
        result["enabled_flags"] = flags

        if not bool(_safe_get_setting(settings, "novo_no_hard_blocks", True)):
            result["warnings"].append("novo_no_hard_blocks is false; suggestions remain advisory/no-block.")
        if not bool(_safe_get_setting(settings, "novo_trade_quality_advisory_only", True)):
            result["warnings"].append("novo_trade_quality_advisory_only is false; suggestions remain advisory-only.")

        if not any(flags.values()):
            result["status"] = "DISABLED"
            result["explanation"] = "Novo soft enrichment is disabled; all suggestions are neutral."
            return _sanitize_result(result)

        if not isinstance(novo, Mapping):
            result["status"] = "COLLECTING"
            result["explanation"] = "Novo quality metadata is missing or invalid."
            return _sanitize_result(result)

        novo_copy = deepcopy(novo)
        score = _score(novo_copy.get("quality_score"))
        band = _band_from_score(score)
        tier = _tier_from_novo(novo_copy, score)
        result["quality_score"] = score
        result["quality_tier"] = tier
        result["band"] = band

        if score is None:
            result["status"] = "COLLECTING"
            result["explanation"] = "Novo quality score is not available yet."
            return _sanitize_result(result)

        suggestions = result["suggestions"]
        events = result["events"]

        if flags["soft_confidence"]:
            events.append("SOFT_CONFIDENCE_OBSERVED")

        if flags["exit_enrichment"]:
            if band == "ELITE":
                suggestions["tp2_multiplier"] = 1.12
                suggestions["trailing_activation_multiplier"] = 1.10
                suggestions["trailing_distance_multiplier"] = 1.08
                suggestions["break_even_delay_multiplier"] = 1.10
                suggestions["execution_split_preference"] = "LIMIT_PREFERRED"
                events.append("ELITE_EXIT_PATIENCE_SUGGESTED")
            elif band == "STRONG":
                suggestions["tp2_multiplier"] = 1.06
                suggestions["trailing_activation_multiplier"] = 1.05
                suggestions["trailing_distance_multiplier"] = 1.04
                suggestions["break_even_delay_multiplier"] = 1.05
                events.append("STRONG_EXIT_PATIENCE_SUGGESTED")

        if flags["profit_lock_enrichment"]:
            if band == "ELITE":
                suggestions["profit_lock_multiplier"] = 1.08
                events.append("ELITE_PROFIT_LOCK_AFTER_CONFIRMATION")
            elif band == "STRONG":
                suggestions["profit_lock_multiplier"] = 1.04
                events.append("STRONG_PROFIT_LOCK_AFTER_CONFIRMATION")

        if flags["fee_bleed_guard"]:
            if band == "ELITE":
                suggestions["partial_min_notional_multiplier"] = 1.20
            elif band == "STRONG":
                suggestions["partial_min_notional_multiplier"] = 1.15
            elif band == "GOOD":
                suggestions["partial_min_notional_multiplier"] = 1.10
            else:
                suggestions["partial_min_notional_multiplier"] = 1.05
            events.append("FEE_BLEED_PARTIAL_ECONOMICS_GUARD")

        if flags["elite_size_boost"] and band == "ELITE":
            cap = _safe_get_setting(settings, "novo_elite_size_boost_max", 1.12)
            try:
                cap_f = float(cap)
            except Exception:
                cap_f = 1.12
            suggestions["size_multiplier"] = round(max(1.0, min(cap_f, 1.12)), 4)
            if suggestions["size_multiplier"] > 1.0:
                events.append("ELITE_SIZE_BOOST_SUGGESTED")

        result["status"] = "OK"
        result["explanation"] = (
            f"Novo soft enrichment suggestions for {band} quality "
            f"remain advisory-only and do not change admission or execution."
        )
        return _sanitize_result(result)
    except Exception as exc:
        result = _base_result("ERROR")
        result["errors"].append(str(exc))
        result["explanation"] = "Novo soft enrichment failed safely with neutral suggestions."
        return _sanitize_result(result)

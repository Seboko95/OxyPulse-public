"""Pure advisory trade-quality aggregator for Novo intelligence."""
from __future__ import annotations

from collections.abc import Mapping
from copy import deepcopy
from typing import Any


_MODULE_KEYS = (
    "market_dna",
    "market_structure",
    "bias_4h",
    "crt",
    "liquidity_sweep",
    "session_model",
)

_WEIGHTS = {
    "existing_confidence": 0.20,
    "consensus": 0.10,
    "market_dna": 0.15,
    "market_structure": 0.15,
    "bias_4h": 0.15,
    "crt": 0.10,
    "liquidity_sweep": 0.10,
    "session_model": 0.05,
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


def _base_result(symbol: Any = None, side: Any = None, engine: Any = None) -> dict[str, Any]:
    return {
        "status": "INSUFFICIENT_DATA",
        "symbol": None if symbol is None else str(symbol),
        "side": _normalize_side(side),
        "engine": None if engine is None else str(engine),
        "advisory_only": True,
        "quality_score": 0,
        "quality_tier": "COLLECTING",
        "alignment": {
            "aligned": 0,
            "conflicted": 0,
            "neutral": 0,
            "unknown": 0,
        },
        "components": {
            "existing_confidence": None,
            "consensus": None,
            "market_dna": None,
            "market_structure": None,
            "bias_4h": None,
            "crt": None,
            "liquidity_sweep": None,
            "session_model": None,
        },
        "events": [],
        "explanation": "",
        "warnings": [],
        "errors": [],
    }


def _normalize_side(side: Any) -> str | None:
    if side is None:
        return None
    value = str(side).strip().upper()
    if value in {"LONG", "BUY", "BULL", "BULLISH"}:
        return "LONG"
    if value in {"SHORT", "SELL", "BEAR", "BEARISH"}:
        return "SHORT"
    return value or None


def _as_score(value: Any) -> int | None:
    try:
        score = float(value)
    except Exception:
        return None
    if score != score:
        return None
    return int(round(max(0.0, min(100.0, score))))


def _confidence_score(existing_confidence: Any) -> int | None:
    try:
        value = float(existing_confidence)
    except Exception:
        return None
    if value != value:
        return None
    if 0.0 <= value <= 1.0:
        return _as_score(value * 100.0)
    if 0.0 <= value <= 10.0:
        return _as_score(value * 10.0)
    return _as_score(value)


def _consensus_score(consensus: Any) -> int | None:
    if consensus is None:
        return None
    if isinstance(consensus, Mapping):
        for key in ("score", "quality_score", "agreement_score", "confidence"):
            if key in consensus:
                found = _as_score(consensus[key])
                if found is not None:
                    return found
        count = consensus.get("agreement_count", consensus.get("count"))
    else:
        for key in ("score", "quality_score", "agreement_score", "confidence"):
            if hasattr(consensus, key):
                found = _as_score(getattr(consensus, key))
                if found is not None:
                    return found
        count = getattr(consensus, "agreement_count", getattr(consensus, "count", None))
    try:
        value = int(count)
    except Exception:
        return None
    return _as_score(min(100, max(0, value) * 20))


def _module_result(modules: Any, key: str) -> Any:
    if not isinstance(modules, Mapping):
        return None
    return modules.get(key)


def _module_score(module_output: Any) -> int | None:
    if not isinstance(module_output, Mapping):
        return None
    status = str(module_output.get("status", "OK") or "OK").upper()
    if status not in {"OK", "COLLECTING"}:
        return None
    return _as_score(module_output.get("score"))


def _module_bias(module_output: Any) -> str | None:
    if not isinstance(module_output, Mapping):
        return None
    return _normalize_side(module_output.get("bias"))


def _module_events(module_output: Any) -> list[str]:
    if not isinstance(module_output, Mapping):
        return []
    events = module_output.get("events", [])
    if not isinstance(events, list):
        return []
    return [str(event) for event in events if event is not None]


def _quality_tier(score: int, valid_components: int) -> str:
    if valid_components <= 0:
        return "COLLECTING"
    if score >= 90:
        return "ELITE"
    if score >= 80:
        return "STRONG"
    if score >= 70:
        return "GOOD"
    return "WEAK"


def _alignment_count(alignment: dict[str, int], side: str | None, bias: str | None) -> None:
    if side not in {"LONG", "SHORT"} or bias is None:
        alignment["unknown"] += 1
    elif bias == side:
        alignment["aligned"] += 1
    elif bias in {"LONG", "SHORT"}:
        alignment["conflicted"] += 1
    elif bias == "NEUTRAL":
        alignment["neutral"] += 1
    else:
        alignment["unknown"] += 1


def calculate_trade_quality(
    symbol=None,
    side=None,
    engine=None,
    existing_confidence=None,
    consensus=None,
    modules=None,
    settings=None,
) -> dict:
    """Combine advisory module outputs into one advisory-only quality score."""
    _ = _safe_get_setting(settings, "novo_trade_quality_enabled", True)
    try:
        modules_copy = deepcopy(modules) if isinstance(modules, Mapping) else modules
        result = _base_result(symbol, side, engine)
        side_norm = result["side"]
        components = result["components"]
        weighted_sum = 0.0
        weight_total = 0.0
        valid_components = 0

        conf_score = _confidence_score(existing_confidence)
        if conf_score is not None:
            components["existing_confidence"] = conf_score
            weighted_sum += conf_score * _WEIGHTS["existing_confidence"]
            weight_total += _WEIGHTS["existing_confidence"]
            valid_components += 1
            result["events"].append("EXISTING_CONFIDENCE_INCLUDED")

        cons_score = _consensus_score(consensus)
        if cons_score is not None:
            components["consensus"] = cons_score
            weighted_sum += cons_score * _WEIGHTS["consensus"]
            weight_total += _WEIGHTS["consensus"]
            valid_components += 1
            result["events"].append("CONSENSUS_INCLUDED")

        for key in _MODULE_KEYS:
            module_output = _module_result(modules_copy, key)
            score = _module_score(module_output)
            if score is not None:
                components[key] = score
                weighted_sum += score * _WEIGHTS[key]
                weight_total += _WEIGHTS[key]
                valid_components += 1
                result["events"].append(f"{key.upper()}_INCLUDED")
                result["events"].extend(_module_events(module_output))
            else:
                if module_output is None:
                    result["warnings"].append(f"{key} missing.")
                else:
                    result["warnings"].append(f"{key} unavailable or malformed.")
            _alignment_count(result["alignment"], side_norm, _module_bias(module_output))

        if valid_components <= 0 or weight_total <= 0.0:
            result["status"] = "INSUFFICIENT_DATA"
            result["quality_score"] = 0
            result["quality_tier"] = "COLLECTING"
            result["explanation"] = "No valid quality components available yet."
            result["advisory_only"] = True
            return result

        score = weighted_sum / weight_total
        aligned = int(result["alignment"]["aligned"])
        conflicted = int(result["alignment"]["conflicted"])
        if aligned >= 3 and conflicted == 0:
            score += min(8, aligned * 2)
            result["events"].append("ALIGNMENT_BOOST")
        if conflicted >= 2:
            score -= min(15, conflicted * 5)
            result["events"].append("CONFLICT_PENALTY")

        final_score = int(round(max(0.0, min(100.0, score))))
        result["status"] = "OK"
        result["quality_score"] = final_score
        result["quality_tier"] = _quality_tier(final_score, valid_components)
        result["explanation"] = (
            f"Novo trade quality {final_score}/100 ({result['quality_tier']}) from "
            f"{valid_components} valid components. Alignment: "
            f"{aligned} aligned, {conflicted} conflicted, "
            f"{result['alignment']['neutral']} neutral, {result['alignment']['unknown']} unknown."
        )
        result["advisory_only"] = True
        return result
    except Exception as exc:
        result = _base_result(symbol, side, engine)
        result["status"] = "ERROR"
        result["errors"].append(f"{type(exc).__name__}: {exc}")
        result["explanation"] = "Trade quality calculation failed safely."
        result["advisory_only"] = True
        return result


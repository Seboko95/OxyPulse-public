"""Advisory-only facade for Novo-style intelligence.

This module only coordinates pure advisory modules. It writes nothing, sends
nothing, and cannot place or prepare orders.
"""
from __future__ import annotations

from collections.abc import Mapping
from copy import deepcopy
from typing import Any

import novo_bias_4h
import novo_crt_engine
import novo_liquidity_sweep
import novo_market_dna
import novo_market_structure
import novo_session_model
import novo_trade_quality


_MODULE_NAMES = (
    "market_dna",
    "market_structure",
    "bias_4h",
    "crt",
    "liquidity_sweep",
    "session_model",
)

_MODULE_FLAGS = {
    "market_dna": "novo_market_dna_enabled",
    "market_structure": "novo_market_structure_enabled",
    "bias_4h": "novo_4h_bias_enabled",
    "crt": "novo_crt_enabled",
    "liquidity_sweep": "novo_liquidity_sweep_enabled",
    "session_model": "novo_session_model_enabled",
}


def _safe_get_setting(settings: Any, key: str, default: Any = None) -> Any:
    """Read a setting from a dict-like or settings_store-like object safely."""
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


def _module_pending_snapshot() -> dict[str, dict[str, str]]:
    return {name: {"status": "PENDING"} for name in _MODULE_NAMES}


def _base_snapshot(
    symbol: Any,
    side: Any,
    engine: Any = None,
    *,
    enabled: bool,
) -> dict[str, Any]:
    return {
        "enabled": bool(enabled),
        "symbol": "" if symbol is None else str(symbol),
        "side": "" if side is None else str(side).upper(),
        "engine": None if engine is None else str(engine),
        "advisory_only": True,
        "quality_score": None,
        "quality_tier": "COLLECTING",
        "modules": _module_pending_snapshot(),
        "explanations": [],
        "warnings": [],
        "errors": [],
    }


def _disabled_snapshot(symbol: Any, side: Any, engine: Any = None) -> dict[str, Any]:
    snap = _base_snapshot(symbol, side, engine, enabled=False)
    snap["quality_tier"] = "DISABLED"
    snap["explanations"].append("Novo intelligence disabled.")
    return snap


def _quality_tier(score: Any) -> str:
    try:
        value = float(score)
    except Exception:
        return "COLLECTING"
    if value >= 85.0:
        return "ELITE"
    if value >= 70.0:
        return "STRONG"
    if value >= 55.0:
        return "ACCEPTABLE"
    if value >= 40.0:
        return "WEAK"
    return "POOR"


def _module_disabled() -> dict[str, str]:
    return {"status": "DISABLED"}


def _module_error(exc: Exception) -> dict[str, Any]:
    return {
        "status": "ERROR",
        "errors": [f"{type(exc).__name__}: {exc}"],
        "warnings": [],
        "events": [],
        "explanation": "Module failed safely.",
    }


def _market_data_missing(market_data: Any) -> bool:
    if market_data is None:
        return True
    if isinstance(market_data, Mapping):
        return len(market_data) == 0
    try:
        return len(market_data) == 0
    except Exception:
        return False


def _market_mapping(market_data: Any) -> Mapping[str, Any]:
    return market_data if isinstance(market_data, Mapping) else {}


def _first_present(data: Mapping[str, Any], keys: tuple[str, ...]) -> Any:
    for key in keys:
        value = data.get(key)
        if value:
            return value
    return None


def _best_lower_mid_candles(data: Mapping[str, Any]) -> Any:
    return _first_present(data, ("candles_15m", "15m", "candles_1h", "1h", "candles", "5m", "1m"))


def _generic_candles(data: Mapping[str, Any]) -> Any:
    return _first_present(data, ("candles", "candles_15m", "15m", "candles_1h", "1h", "5m", "1m"))


def _candles_4h(data: Mapping[str, Any]) -> Any:
    return _first_present(data, ("candles_4h", "4h"))


def _module_enabled(settings: Any, module_name: str) -> bool:
    return bool(_safe_get_setting(settings, _MODULE_FLAGS[module_name], True))


def _guarded_module_call(module_name: str, facade_settings: Any, func, *args, **kwargs) -> dict[str, Any]:
    if not _module_enabled(facade_settings, module_name):
        return _module_disabled()
    try:
        result = func(*args, **kwargs)
        if isinstance(result, Mapping):
            return dict(result)
        return {
            "status": "ERROR",
            "errors": [f"{module_name} returned non-mapping result."],
            "warnings": [],
            "events": [],
            "explanation": "Module failed safely.",
        }
    except Exception as exc:
        return _module_error(exc)


def _collect_module_messages(modules: Mapping[str, Any]) -> tuple[list[str], list[str], list[str]]:
    explanations: list[str] = []
    warnings: list[str] = []
    errors: list[str] = []
    for name, result in modules.items():
        if not isinstance(result, Mapping):
            warnings.append(f"{name} returned unusable output.")
            continue
        explanation = result.get("explanation")
        if explanation:
            explanations.append(f"{name}: {explanation}")
        for warning in result.get("warnings", []) or []:
            warnings.append(f"{name}: {warning}")
        for error in result.get("errors", []) or []:
            errors.append(f"{name}: {error}")
    return explanations, warnings, errors


def analyze_candidate(
    symbol,
    side,
    engine=None,
    market_data=None,
    confidence=None,
    consensus=None,
    regime=None,
    admission=None,
    rejection_reason=None,
    settings=None,
) -> dict:
    """Return advisory context for a candidate without affecting live decisions."""
    try:
        enabled = bool(_safe_get_setting(settings, "novo_intelligence_enabled", False))
        if not enabled:
            return _disabled_snapshot(symbol, side, engine)

        snap = _base_snapshot(symbol, side, engine, enabled=True)

        if _market_data_missing(market_data):
            snap["warnings"].append("Market data unavailable; Novo modules collecting.")

        if confidence is not None:
            try:
                snap["explanations"].append(f"Input confidence observed: {float(confidence):.2f}.")
            except Exception:
                snap["warnings"].append("Input confidence was not numeric.")

        if consensus is not None:
            snap["explanations"].append("Consensus context observed.")
        if regime is not None:
            snap["explanations"].append(f"Regime context observed: {regime}.")
        if admission is not None:
            snap["explanations"].append("Admission context observed without modification.")
        if rejection_reason:
            snap["explanations"].append(f"Rejection context observed: {rejection_reason}.")

        # Snapshot caller-provided market data defensively to prove we never mutate it.
        if isinstance(market_data, Mapping):
            try:
                snap["market_data_keys"] = sorted(str(k) for k in deepcopy(dict(market_data)).keys())
            except Exception:
                snap["warnings"].append("Market data keys unavailable.")

        data = _market_mapping(market_data)
        lower_mid = _best_lower_mid_candles(data)
        generic = _generic_candles(data)
        candles_4h = _candles_4h(data)
        levels = deepcopy(data.get("levels", {})) if isinstance(data.get("levels", {}), Mapping) else None
        session_levels = deepcopy(data.get("session_levels", {})) if isinstance(data.get("session_levels", {}), Mapping) else None

        snap["modules"]["market_dna"] = _guarded_module_call(
            "market_dna",
            settings,
            novo_market_dna.analyze_market_dna,
            lower_mid,
            side=side,
            timeframe="15m",
            settings=settings,
        )
        snap["modules"]["market_structure"] = _guarded_module_call(
            "market_structure",
            settings,
            novo_market_structure.analyze_market_structure,
            _first_present(data, ("candles_15m", "15m", "candles")) or generic,
            side=side,
            timeframe="15m",
            settings=settings,
        )
        snap["modules"]["bias_4h"] = _guarded_module_call(
            "bias_4h",
            settings,
            novo_bias_4h.analyze_4h_bias,
            candles_4h,
            side=side,
            settings=settings,
        )
        snap["modules"]["crt"] = _guarded_module_call(
            "crt",
            settings,
            novo_crt_engine.analyze_crt,
            lower_mid,
            side=side,
            timeframe="15m",
            settings=settings,
        )
        snap["modules"]["liquidity_sweep"] = _guarded_module_call(
            "liquidity_sweep",
            settings,
            novo_liquidity_sweep.analyze_liquidity_sweep,
            lower_mid,
            side=side,
            timeframe="15m",
            levels=levels,
            settings=settings,
        )
        snap["modules"]["session_model"] = _guarded_module_call(
            "session_model",
            settings,
            novo_session_model.analyze_session_model,
            lower_mid,
            side=side,
            timeframe="15m",
            session_levels=session_levels,
            settings=settings,
        )

        mod_explanations, mod_warnings, mod_errors = _collect_module_messages(snap["modules"])
        snap["explanations"].extend(mod_explanations)
        snap["warnings"].extend(mod_warnings)
        snap["errors"].extend(mod_errors)

        if bool(_safe_get_setting(settings, "novo_trade_quality_enabled", True)):
            quality = novo_trade_quality.calculate_trade_quality(
                symbol=symbol,
                side=side,
                engine=engine,
                existing_confidence=confidence,
                consensus=consensus,
                modules=snap["modules"],
                settings=settings,
            )
            snap["trade_quality"] = quality
            snap["quality_score"] = quality.get("quality_score") if isinstance(quality, Mapping) else None
            snap["quality_tier"] = str(quality.get("quality_tier", "COLLECTING")) if isinstance(quality, Mapping) else "COLLECTING"
            if isinstance(quality, Mapping) and quality.get("explanation"):
                snap["explanations"].append(f"trade_quality: {quality.get('explanation')}")
            if isinstance(quality, Mapping):
                snap["warnings"].extend(f"trade_quality: {w}" for w in (quality.get("warnings", []) or []))
                snap["errors"].extend(f"trade_quality: {e}" for e in (quality.get("errors", []) or []))
        else:
            snap["trade_quality"] = _module_disabled()
            snap["quality_score"] = None
            snap["quality_tier"] = _quality_tier(snap.get("quality_score"))

        snap["advisory_only"] = True
        return snap
    except Exception as exc:
        snap = _base_snapshot(symbol, side, engine, enabled=False)
        snap["quality_tier"] = "COLLECTING"
        snap["errors"].append(f"{type(exc).__name__}: {exc}")
        snap["advisory_only"] = True
        return snap

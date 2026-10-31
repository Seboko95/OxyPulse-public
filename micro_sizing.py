"""Adaptive micro-account sizing helpers for OxyPulse.

Pure sizing math and lightweight runtime diagnostics.  Trading decisions remain
with the caller; these helpers only make floor, buffer, and deployment math
consistent across preview, deployment, and execution preflight.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
import logging
import time
from typing import Any

logger = logging.getLogger("OxyPulse.MicroSizing")


@dataclass
class FloorAwareSizeResult:
    requested_size_usdt: float
    final_size_usdt: float
    skipped: bool = False
    reason: str = ""
    floor_preserved: bool = False
    required_margin_usdt: float = 0.0
    effective_floor_usdt: float = 0.0
    reductions_stacked: bool = False
    reductions: list[str] = field(default_factory=list)


_LAST_DIAGNOSTICS: dict[str, Any] = {}


def _get(settings: Any, key: str, default: Any = None) -> Any:
    try:
        getter = getattr(settings, "get_val", None)
        if callable(getter):
            return getter(key, default)
        if isinstance(settings, dict):
            return settings.get(key, default)
    except Exception:
        return default
    return default


def _float(settings: Any, key: str, default: float) -> float:
    try:
        return float(_get(settings, key, default) or default)
    except Exception:
        return float(default)


def _bool(settings: Any, key: str, default: bool) -> bool:
    try:
        return bool(_get(settings, key, default))
    except Exception:
        return bool(default)


def is_micro_sizing_active(balance_usdt: float | None, settings: Any) -> bool:
    if not _bool(settings, "micro_sizing_enabled", False):
        return False
    try:
        balance = float(balance_usdt or 0.0)
    except Exception:
        return False
    micro_max = _float(settings, "v6_micro_max_balance", 100.0)
    return balance > 0.0 and balance < micro_max


def account_floor_usdt(balance_usdt: float | None, settings: Any) -> float:
    """Return the bot-level executable notional floor for the current balance."""
    base_floor = max(_float(settings, "min_order_usdt", 100.0), 1.0)
    v6_floor = max(_float(settings, "v6_min_position_size_usdt", 25.0), 1.0)
    if balance_usdt is None or not _bool(settings, "adaptive_min_order_floor_enabled", True):
        return base_floor

    balance = max(float(balance_usdt or 0.0), 0.0)
    micro_max = _float(settings, "v6_micro_max_balance", 100.0)
    growth_max = _float(settings, "v6_growth_max_balance", 1000.0)

    if is_micro_sizing_active(balance, settings):
        micro_floor = max(_float(settings, "micro_min_order_usdt_floor", 5.0), 1.0)
        profile_floor = max(_float(settings, "min_order_usdt_micro", v6_floor), 1.0)
        if balance < 10.0:
            return round(min(base_floor, profile_floor, micro_floor), 2)
        if balance < 25.0:
            # Smoothly transition from the tiny executable floor toward the
            # operator's configured micro floor instead of jumping at $10.
            blend = min(max((balance - 10.0) / 15.0, 0.0), 1.0)
            blended = micro_floor + (profile_floor - micro_floor) * blend
            return round(max(min(base_floor, profile_floor, blended), 1.0), 2)
        return round(max(min(base_floor, profile_floor), 1.0), 2)

    if balance < growth_max:
        profile_floor = _float(settings, "min_order_usdt_growth", max(v6_floor, 50.0))
    else:
        profile_floor = _float(settings, "min_order_usdt_pro", base_floor)
    return round(max(min(base_floor, profile_floor), 1.0), 2)


def margin_buffer_usdt(balance_usdt: float | None, settings: Any) -> float:
    legacy = max(_float(settings, "min_free_balance_buffer_usdt", 0.0), 0.0)
    balance = max(float(balance_usdt or 0.0), 0.0)
    if not is_micro_sizing_active(balance, settings):
        return round(legacy, 2)
    pct = max(_float(settings, "micro_margin_buffer_pct_balance", 12.0), 0.0)
    floor = max(_float(settings, "micro_margin_buffer_min_usdt", 0.50), 0.0)
    cap = max(_float(settings, "micro_margin_buffer_max_usdt", legacy or 5.0), floor)
    adaptive = max(balance * pct / 100.0, floor)
    return round(min(adaptive, cap, max(balance - 0.01, 0.0)), 2)


def usable_margin_usdt(balance_usdt: float | None, settings: Any) -> float:
    balance = max(float(balance_usdt or 0.0), 0.0)
    return round(max(balance - margin_buffer_usdt(balance, settings), 0.0), 2)


def max_notional_for_margin(balance_usdt: float | None, leverage: int, settings: Any, cushion: float = 1.05) -> float:
    lev = max(int(leverage or 1), 1)
    return round(max(usable_margin_usdt(balance_usdt, settings), 0.0) * lev / max(float(cushion or 1.0), 1.0), 2)


def deployment_position_ceiling(balance_usdt: float | None, settings: Any) -> int | None:
    if not is_micro_sizing_active(balance_usdt, settings):
        return None
    balance = max(float(balance_usdt or 0.0), 0.0)
    if balance < 10.0:
        return 1
    if balance < 25.0:
        return 2
    if balance < 50.0:
        return 3
    return None


def deployment_slot_floor_usdt(
    balance_usdt: float | None,
    *,
    settings: Any,
    account_floor: float | None = None,
    target_min: float | None = None,
) -> float:
    floor = float(account_floor if account_floor is not None else account_floor_usdt(balance_usdt, settings))
    if not is_micro_sizing_active(balance_usdt, settings):
        dyn = float(target_min if target_min is not None else _float(settings, "dynamic_target_min_usdt", 50.0))
        return round(max(floor, dyn, 1.0), 2)
    balance = max(float(balance_usdt or 0.0), 0.0)
    dyn = max(float(target_min if target_min is not None else _float(settings, "dynamic_target_min_usdt", 50.0)), 1.0)
    if balance < 25.0:
        return round(max(floor, 1.0), 2)
    if balance < 50.0:
        return round(max(floor, min(dyn * 0.5, floor * 1.5), 1.0), 2)
    return round(max(floor, min(dyn, floor * 3.0), 1.0), 2)


def target_band_usdt(balance_usdt: float | None, settings: Any, cap_mult: float = 1.0) -> tuple[float, float]:
    balance = max(float(balance_usdt or 0.0), 0.0)
    floor = account_floor_usdt(balance, settings)
    if is_micro_sizing_active(balance, settings):
        min_pct = max(_float(settings, "micro_target_min_pct_balance", 300.0), 0.0)
        max_pct = max(_float(settings, "micro_target_max_pct_balance", 900.0), min_pct)
        target_min = max(floor, balance * min_pct / 100.0)
        target_max = max(target_min, balance * max_pct / 100.0)
        return round(target_min * max(float(cap_mult or 1.0), 0.1), 2), round(target_max * max(float(cap_mult or 1.0), 0.1), 2)
    dyn_min = _float(settings, "dynamic_target_min_usdt", 50.0)
    dyn_max = _float(settings, "dynamic_target_max_usdt", 100.0)
    band_mult = max(float(cap_mult or 1.0), 0.1)
    return round(max(min(dyn_min * band_mult, dyn_max * band_mult), floor), 2), round(max(dyn_max * band_mult, floor), 2)


def floor_margin_fits(
    floor_usdt: float,
    *,
    balance_usdt: float | None,
    leverage: int,
    settings: Any,
    cushion: float = 1.05,
) -> bool:
    required = float(floor_usdt or 0.0) / float(max(int(leverage or 1), 1))
    return required * max(float(cushion or 1.0), 1.0) <= usable_margin_usdt(balance_usdt, settings) + 1e-9


def floor_aware_reduce(
    size_usdt: float,
    multiplier: float,
    *,
    balance_usdt: float | None,
    leverage: int,
    settings: Any,
    exchange_floor_usdt: float = 0.0,
    reason: str = "",
    prior_reductions: list[str] | None = None,
) -> FloorAwareSizeResult:
    size = max(float(size_usdt or 0.0), 0.0)
    mult = max(float(multiplier or 0.0), 0.0)
    reduced = round(size * mult, 2)
    floor = max(account_floor_usdt(balance_usdt, settings), float(exchange_floor_usdt or 0.0), 1.0)
    reductions = list(prior_reductions or [])
    if mult < 0.999:
        reductions.append(str(reason or f"x{mult:.2f}"))
    required = floor / float(max(int(leverage or 1), 1))
    if reduced + 0.01 >= floor:
        return FloorAwareSizeResult(size, reduced, False, reason, False, required, floor, len(reductions) > 1, reductions)
    if size >= floor and floor_margin_fits(floor, balance_usdt=balance_usdt, leverage=leverage, settings=settings):
        return FloorAwareSizeResult(size, round(floor, 2), False, f"{reason}|floor_preserved", True, required, floor, len(reductions) > 1, reductions)
    return FloorAwareSizeResult(size, reduced, True, f"{reason or 'reduced'}_below_floor", False, required, floor, len(reductions) > 1, reductions)


def net_expectancy_cost_multipliers(balance_usdt: float | None, settings: Any) -> tuple[float, float]:
    min_mult = _float(settings, "net_expectancy_min_cost_mult", 1.25)
    weak_mult = _float(settings, "net_expectancy_weak_cost_mult", 2.0)
    if not is_micro_sizing_active(balance_usdt, settings):
        return min_mult, weak_mult
    balance = max(float(balance_usdt or 0.0), 0.0)
    if balance < 10.0:
        factor = 0.60
    elif balance < 25.0:
        factor = 0.72
    elif balance < 50.0:
        factor = 0.85
    else:
        factor = 0.95
    return round(max(min_mult * factor, 0.85), 3), round(max(weak_mult * factor, 1.15), 3)


def record_sizing_diagnostics(**kwargs: Any) -> None:
    global _LAST_DIAGNOSTICS
    data = dict(kwargs)
    data["ts"] = time.time()
    _LAST_DIAGNOSTICS = data
    try:
        reason = str(data.get("block_reason") or data.get("reason") or "")
        if reason:
            logger.info(
                "[MicroSizing] %s final=$%.2f floor=$%.2f margin=$%.2f max_pos=%s reductions_stacked=%s",
                reason,
                float(data.get("final_executable_size_usdt", 0.0) or 0.0),
                float(data.get("effective_account_floor_usdt", 0.0) or 0.0),
                float(data.get("final_margin_requirement_usdt", 0.0) or 0.0),
                data.get("effective_max_positions", "?"),
                bool(data.get("reductions_stacked", False)),
            )
    except Exception:
        pass


def get_last_sizing_diagnostics() -> dict[str, Any]:
    return dict(_LAST_DIAGNOSTICS)


def diagnostics_snapshot(
    *,
    balance_usdt: float | None,
    settings: Any,
    leverage: int | None = None,
    exchange_floor_usdt: float = 0.0,
    final_size_usdt: float = 0.0,
    block_reason: str = "",
    reductions_stacked: bool = False,
    effective_max_positions: int | None = None,
) -> dict[str, Any]:
    lev = max(int(leverage or _float(settings, "leverage_default", 10.0) or 10), 1)
    account_floor = account_floor_usdt(balance_usdt, settings)
    exchange_floor = max(float(exchange_floor_usdt or 0.0), 0.0)
    floor = max(account_floor, exchange_floor, 1.0)
    final_size = max(float(final_size_usdt or 0.0), 0.0)
    snap = {
        "balance_usdt": round(float(balance_usdt or 0.0), 2),
        "micro_sizing_active": is_micro_sizing_active(balance_usdt, settings),
        "effective_account_floor_usdt": round(account_floor, 2),
        "exchange_symbol_floor_usdt": round(exchange_floor, 2),
        "effective_floor_usdt": round(floor, 2),
        "effective_margin_buffer_usdt": margin_buffer_usdt(balance_usdt, settings),
        "usable_margin_usdt": usable_margin_usdt(balance_usdt, settings),
        "final_executable_size_usdt": round(final_size, 2),
        "final_margin_requirement_usdt": round(final_size / float(lev), 4) if final_size > 0 else 0.0,
        "block_reason": str(block_reason or ""),
        "reductions_stacked": bool(reductions_stacked),
        "effective_max_positions": effective_max_positions,
        "leverage": lev,
    }
    return snap


def as_plain_dict(result: FloorAwareSizeResult) -> dict[str, Any]:
    return asdict(result)

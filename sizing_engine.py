"""
OxyPulse V6 / V6.1 - sizing_engine.py
Capital-profile-aware dynamic position sizing.

The sizing engine computes a tradeable notional window, then:
  - applies confidence / capital / volatility / drawdown modifiers
  - scales small-account trades toward a target band when room exists
  - clamps outsized trades back into the configured live band

All side effects remain with the caller.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field

import settings_store as _ss
import micro_sizing as _micro_sizing

logger = logging.getLogger("OxyPulse.V6.Sizing")

_HIGH_VOL_RATIO = 0.04
_LOW_VOL_RATIO = 0.01


def _normalize_mode_token(value: str | None) -> str:
    return str(value or "").strip().lower().replace("-", "_").replace(" ", "_")


def _floor_preserve_allowed(capital_profile=None) -> bool:
    if not bool(_ss.get_val("risk_floor_preserve_enabled", True)):
        return False

    raw_scope = str(_ss.get_val("risk_floor_preserve_modes", "") or "")
    allowed = {
        _normalize_mode_token(token)
        for token in raw_scope.split(",")
        if str(token).strip()
    }
    if not allowed:
        return True

    preset = _normalize_mode_token(_ss.get_val("trading_mode_preset", ""))
    deployment = _normalize_mode_token(
        getattr(
            capital_profile,
            "deployment_effective_mode",
            getattr(capital_profile, "deployment_mode", _ss.get_val("deployment_mode", "")),
        )
    )
    # Preset is the operator's primary intent. If a preset is set, it must be explicitly allowed.
    if preset:
        return preset in allowed
    if deployment:
        return deployment in allowed
    return True


def _floor_margin_fits(floor: float, leverage: int, balance: float) -> bool:
    """Return True when the executable notional floor can fit available margin."""
    if not _micro_sizing.is_micro_sizing_active(balance, _ss):
        if floor <= 0 or leverage <= 0 or balance <= 0:
            return False
        buffer_usdt = float(_ss.get_val("min_free_balance_buffer_usdt", 0.0) or 0.0)
        usable_balance = max(float(balance or 0.0) - max(buffer_usdt, 0.0), 0.0)
        required_margin = float(floor) / float(leverage)
        return required_margin <= max(usable_balance, float(balance or 0.0)) + 1e-9
    return _micro_sizing.floor_margin_fits(
        float(floor or 0.0),
        balance_usdt=balance,
        leverage=int(leverage or 1),
        settings=_ss,
    )


@dataclass
class SizingResult:
    requested_size_usdt: float
    final_size_usdt: float
    skipped: bool = False
    reason: str = ""
    modifiers: dict = field(default_factory=dict)


def compute_size(
    base_size: float,
    confidence_score: float,
    atr_val: float,
    entry_px: float,
    leverage: int,
    balance: float,
    dd_size_multiplier: float = 1.0,
    capital_profile=None,
    intent_size_multiplier: float = 1.0,
    effective_cap_usdt: float | None = None,
    minimum_exec_usdt: float | None = None,
) -> SizingResult:
    if minimum_exec_usdt is not None:
        floor = max(float(minimum_exec_usdt or 0.0), 0.0)
    else:
        try:
            floor = float(_ss.get_effective_min_order_usdt(balance))  # type: ignore[attr-defined]
        except Exception:
            floor = float(_ss.get_val("v6_min_position_size_usdt", 25.0))
    cap = float(_ss.get_val("v6_max_position_size_usdt", 250.0))
    conf_mult_scale = float(_ss.get_val("v6_confidence_size_multiplier", 1.0))
    vol_penalty_scale = float(_ss.get_val("v6_volatility_size_penalty", 1.0))
    dd_penalty_scale = float(_ss.get_val("v6_drawdown_size_penalty", 1.0))
    max_margin_pct = float(_ss.get_val("v6_max_position_pct_of_balance", 0.03))
    target_min_usdt = float(_ss.get_val("dynamic_target_min_usdt", 50.0) or 50.0)
    target_max_usdt = float(_ss.get_val("dynamic_target_max_usdt", 100.0) or 100.0)

    modifiers: dict = {"base": round(float(base_size or 0.0), 2)}
    deployment_target_usdt = 0.0

    if capital_profile is not None:
        cap_mult = float(getattr(capital_profile, "size_multiplier", 1.0) or 1.0)
        if balance > 0 and float(getattr(capital_profile, "max_exposure_pct", 0.0) or 0.0) > 0:
            exposure_cap = balance * (float(capital_profile.max_exposure_pct) / 100.0)
            cap = min(cap, exposure_cap)
        deployment_target_usdt = float(
            getattr(capital_profile, "deployment_per_position_target_usdt", 0.0) or 0.0
        )
        if deployment_target_usdt > 0:
            cap = max(cap, deployment_target_usdt)
            modifiers["deployment_target_usdt"] = round(deployment_target_usdt, 2)
            modifiers["deployment_mode"] = str(
                getattr(capital_profile, "deployment_effective_mode", getattr(capital_profile, "deployment_mode", "Guarded"))
            )
    else:
        cap_mult = 1.0
    modifiers["capital_multiplier"] = round(cap_mult, 3)

    conf_fraction = min(float(confidence_score or 0.0) / 3.0, 1.0) if confidence_score > 0 else 0.0
    confidence_multiplier = 1.0 + (conf_fraction * 0.5 * conf_mult_scale)
    modifiers["confidence_multiplier"] = round(confidence_multiplier, 3)

    vol_mult = 1.0
    if atr_val > 0 and entry_px > 0 and vol_penalty_scale > 0:
        vol_ratio = atr_val / entry_px
        if vol_ratio >= _HIGH_VOL_RATIO:
            vol_mult = max(
                0.6,
                1.0 - (vol_ratio - _LOW_VOL_RATIO) / (_HIGH_VOL_RATIO - _LOW_VOL_RATIO) * 0.4,
            )
        elif vol_ratio > _LOW_VOL_RATIO:
            vol_mult = 1.0 - ((vol_ratio - _LOW_VOL_RATIO) / (_HIGH_VOL_RATIO - _LOW_VOL_RATIO)) * 0.4
        vol_mult = 1.0 - (1.0 - vol_mult) * vol_penalty_scale
        vol_mult = max(vol_mult, 0.5)
    modifiers["volatility_multiplier"] = round(vol_mult, 3)

    effective_dd = 1.0 - (1.0 - float(dd_size_multiplier or 1.0)) * dd_penalty_scale
    effective_dd = max(effective_dd, 0.1)
    modifiers["drawdown_multiplier"] = round(effective_dd, 3)

    intent_multiplier = max(float(intent_size_multiplier or 1.0), 0.0)
    modifiers["intent_multiplier"] = round(intent_multiplier, 3)

    adjusted = float(base_size or 0.0) * cap_mult * confidence_multiplier * vol_mult * effective_dd * intent_multiplier

    micro_active = _micro_sizing.is_micro_sizing_active(balance, _ss)
    max_by_balance = cap
    if leverage > 0 and balance > 0:
        max_by_balance = balance * max_margin_pct * leverage
        margin_room = _micro_sizing.max_notional_for_margin(balance, leverage, _ss)
        if micro_active:
            if deployment_target_usdt > 0:
                max_by_balance = max(max_by_balance, min(deployment_target_usdt, margin_room))
            max_by_balance = min(max_by_balance, margin_room)
            modifiers["micro_sizing_active"] = True
            modifiers["usable_margin_usdt"] = round(_micro_sizing.usable_margin_usdt(balance, _ss), 2)
        elif margin_room > 0:
            max_by_balance = min(max_by_balance, max(margin_room, floor))
        if deployment_target_usdt > 0 and not micro_active:
            max_by_balance = max(max_by_balance, deployment_target_usdt)
            modifiers["deployment_margin_override"] = True
        if adjusted > max_by_balance:
            adjusted = max_by_balance
            modifiers["balance_cap_applied"] = True

    if effective_cap_usdt is not None and float(effective_cap_usdt or 0.0) > 0:
        cap = min(cap, float(effective_cap_usdt))
        max_by_balance = min(max_by_balance, float(effective_cap_usdt))
        modifiers["effective_cap_usdt"] = round(float(effective_cap_usdt), 2)

    if _ss.get_val("dynamic_sizing_enabled", _ss.get_val("v6_dynamic_sizing_enabled", True)) or deployment_target_usdt > 0:
        band_multiplier = max(cap_mult * intent_multiplier, 0.1)
        if micro_active:
            effective_target_min, effective_target_max = _micro_sizing.target_band_usdt(
                balance,
                _ss,
                cap_mult=band_multiplier,
            )
            effective_target_max = min(effective_target_max, max(max_by_balance, floor))
        elif deployment_target_usdt > 0:
            effective_target_min = max(floor, deployment_target_usdt * 0.85)
            effective_target_max = max(effective_target_min, deployment_target_usdt)
        else:
            effective_target_min = max(min(target_min_usdt * band_multiplier, target_max_usdt * band_multiplier), floor)
            effective_target_max = max(target_max_usdt * band_multiplier, effective_target_min)
        modifiers["target_min_usdt"] = round(effective_target_min, 2)
        modifiers["target_max_usdt"] = round(effective_target_max, 2)
        upward_cap = min(cap, max_by_balance)
        if adjusted < effective_target_min <= upward_cap:
            adjusted = effective_target_min
            modifiers["scaled_to_target_min"] = True
        if adjusted > effective_target_max:
            adjusted = effective_target_max
            modifiers["clamped_to_target_max"] = True

    max_executable_usdt = min(cap, max_by_balance)
    pre_floor_size = min(adjusted, max_executable_usdt)
    modifiers["exec_floor"] = round(floor, 2)
    modifiers["pre_floor_size_usdt"] = round(pre_floor_size, 2)
    modifiers["max_executable_usdt"] = round(max_executable_usdt, 2)
    modifiers["floor_preserve_allowed"] = bool(_floor_preserve_allowed(capital_profile))

    active_preset = _normalize_mode_token(_ss.get_val("trading_mode_preset", ""))
    active_deployment = _normalize_mode_token(
        getattr(
            capital_profile,
            "deployment_effective_mode",
            getattr(capital_profile, "deployment_mode", ""),
        )
    )
    floor_cap_override_allowed = bool(active_preset or active_deployment)
    floor_preserve_can_fit = bool(
        floor_cap_override_allowed
        and modifiers["floor_preserve_allowed"]
        and _floor_margin_fits(floor, leverage, balance)
    )
    modifiers["floor_preserve_can_fit"] = floor_preserve_can_fit

    if max_executable_usdt + 0.01 < floor:
        if floor_preserve_can_fit:
            final = round(floor, 2)
            modifiers["floor_preserved"] = True
            modifiers["floor_cap_override"] = True
            modifiers["required_margin_usdt"] = round(float(floor) / float(max(leverage, 1)), 4)
            modifiers["final"] = final
            reason = (
                f"base ${float(base_size or 0.0):.2f} preserved to executable floor "
                f"${floor:.2f}; margin fits at {int(leverage)}x despite cap ${max_executable_usdt:.2f}"
            )
            logger.debug("[V6.Sizing] %s", reason)
            return SizingResult(
                requested_size_usdt=float(base_size or 0.0),
                final_size_usdt=final,
                skipped=False,
                reason=reason,
                modifiers=modifiers,
            )
        reason = f"max executable ${max_executable_usdt:.2f} below floor ${floor:.2f}"
        logger.debug("[V6.Sizing] %s", reason)
        return SizingResult(
            requested_size_usdt=float(base_size or 0.0),
            final_size_usdt=round(pre_floor_size, 2),
            skipped=True,
            reason=reason,
            modifiers=modifiers,
        )

    if pre_floor_size + 0.01 < floor:
        if modifiers["floor_preserve_allowed"]:
            final = round(floor, 2)
            modifiers["floor_preserved"] = True
            modifiers["final"] = final
        else:
            reason = f"reduced ${pre_floor_size:.2f} below floor ${floor:.2f}"
            logger.debug("[V6.Sizing] %s", reason)
            return SizingResult(
                requested_size_usdt=float(base_size or 0.0),
                final_size_usdt=round(pre_floor_size, 2),
                skipped=True,
                reason=reason,
                modifiers=modifiers,
            )
    else:
        final = round(pre_floor_size, 2)
        modifiers["final"] = final

    scale_tags: list[str] = []
    if modifiers.get("scaled_to_target_min"):
        scale_tags.append(f"scaled_to_min ${float(modifiers['target_min_usdt']):.2f}")
    if modifiers.get("clamped_to_target_max"):
        scale_tags.append(f"clamped_to_max ${float(modifiers['target_max_usdt']):.2f}")
    if modifiers.get("balance_cap_applied"):
        scale_tags.append("margin_cap")
    if modifiers.get("effective_cap_usdt"):
        scale_tags.append(f"available_cap ${float(modifiers['effective_cap_usdt']):.2f}")
    if modifiers.get("floor_preserved"):
        scale_tags.append(f"floor_preserved ${floor:.2f}")
    scale_suffix = f" | {'; '.join(scale_tags)}" if scale_tags else ""

    reason = (
        f"base ${float(base_size or 0.0):.2f} x cap_modex{cap_mult:.2f} "
        f"confx{confidence_multiplier:.2f} volx{vol_mult:.2f} "
        f"ddx{effective_dd:.2f} intentx{intent_multiplier:.2f} = ${final:.2f}"
        f"{scale_suffix}"
    )
    logger.debug("[V6.Sizing] %s", reason)

    return SizingResult(
        requested_size_usdt=float(base_size or 0.0),
        final_size_usdt=round(final, 2),
        skipped=False,
        reason=reason,
        modifiers=modifiers,
    )

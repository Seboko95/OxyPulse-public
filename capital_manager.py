"""
OxyPulse V6.1 - capital_manager.py
Capital profile and deployment-mode resolution.

This module is read-only. It resolves the active capital profile plus the
operator-facing deployment mode into one runtime profile object that the
sizing, exposure, and Telegram layers can consume consistently.
"""
from __future__ import annotations

import logging
import time as _time
from collections import deque
from dataclasses import dataclass

import settings_store as _ss
import micro_sizing as _micro_sizing

logger = logging.getLogger("OxyPulse.V6.CapitalManager")

MODE_MICRO = "MICRO"
MODE_GROWTH = "GROWTH"
MODE_PRO = "PRO"
MODE_AUTO = "AUTO"

DEPLOYMENT_GUARDED = "Guarded"
DEPLOYMENT_BALANCED = "Balanced"
DEPLOYMENT_MAX_DEPLOY = "Max Deploy"
DEPLOYMENT_FULL_DEPLOYMENT = "Full Deployment"
DEPLOYMENT_SINGLE_SHOT = "Single Shot"
DEPLOYMENT_SWARM = "Swarm"
DEPLOYMENT_RECOVERY = "Recovery"
DEPLOYMENT_AI_ADAPTIVE = "AI Adaptive"

CAPITAL_PROFILE_CHOICES = (MODE_AUTO, MODE_MICRO, MODE_GROWTH, MODE_PRO)
DEPLOYMENT_MODE_CHOICES = (
    DEPLOYMENT_GUARDED,
    DEPLOYMENT_BALANCED,
    DEPLOYMENT_MAX_DEPLOY,
    DEPLOYMENT_FULL_DEPLOYMENT,
    DEPLOYMENT_SINGLE_SHOT,
    DEPLOYMENT_SWARM,
    DEPLOYMENT_RECOVERY,
    DEPLOYMENT_AI_ADAPTIVE,
)

_DEPLOYMENT_PRESETS: dict[str, dict] = {
    DEPLOYMENT_GUARDED: {
        "target_utilization_pct": 15.0,
        "max_positions": 4,
        "respect_exposure_cap": True,
        "respect_headroom": True,
        "total_exposure_limit_pct": 300.0,
        "same_side_limit_pct": 210.0,
        "description": "Guarded keeps the current conservative posture and spreads deployment across a few positions.",
    },
    DEPLOYMENT_BALANCED: {
        "target_utilization_pct": 25.0,
        "max_positions": 3,
        "respect_exposure_cap": True,
        "respect_headroom": True,
        "total_exposure_limit_pct": 450.0,
        "same_side_limit_pct": 320.0,
        "description": "Balanced deploys more capital while still keeping portfolio controls visible and active.",
    },
    DEPLOYMENT_MAX_DEPLOY: {
        "target_utilization_pct": 90.0,
        "max_positions": 2,
        "respect_exposure_cap": True,
        "respect_headroom": True,
        "total_exposure_limit_pct": 1500.0,
        "same_side_limit_pct": 1200.0,
        "description": "Max Deploy concentrates capital into one or two ideas and can use most usable account capital.",
    },
    DEPLOYMENT_FULL_DEPLOYMENT: {
        "target_utilization_pct": 92.0,
        # Dynamic: resolved at runtime from deployable notional + minimum viable slot size.
        "max_positions": 8,
        "respect_exposure_cap": True,
        "respect_headroom": True,
        "total_exposure_limit_pct": 1800.0,
        "same_side_limit_pct": 1400.0,
        "description": "Full Deployment uses most deployable capital while spreading across multiple quality setups (dynamic slots).",
    },
    DEPLOYMENT_SINGLE_SHOT: {
        "target_utilization_pct": 92.0,
        "max_positions": 1,
        "respect_exposure_cap": True,
        "respect_headroom": True,
        "total_exposure_limit_pct": 1600.0,
        "same_side_limit_pct": 1600.0,
        "description": "Single Shot concentrates most deployable capital into the strongest trade only.",
    },
    DEPLOYMENT_SWARM: {
        "target_utilization_pct": 35.0,
        "max_positions": 5,
        "respect_exposure_cap": True,
        "respect_headroom": True,
        "total_exposure_limit_pct": 700.0,
        "same_side_limit_pct": 350.0,
        "description": "Swarm spreads deployable capital across more names with smaller per-trade sizing.",
    },
    DEPLOYMENT_RECOVERY: {
        "target_utilization_pct": 8.0,
        "max_positions": 2,
        "respect_exposure_cap": True,
        "respect_headroom": True,
        "total_exposure_limit_pct": 100.0,
        "same_side_limit_pct": 60.0,
        "description": "Recovery reduces deployment while win rate or drawdown conditions are weak.",
    },
    DEPLOYMENT_AI_ADAPTIVE: {
        "target_utilization_pct": 20.0,
        "max_positions": 3,
        "respect_exposure_cap": True,
        "respect_headroom": True,
        "total_exposure_limit_pct": 350.0,
        "same_side_limit_pct": 250.0,
        "description": "AI Adaptive adjusts deployment from win rate, drawdown, regime, and recent performance.",
    },
}


@dataclass
class CapitalProfile:
    mode: str
    balance: float
    risk_multiplier: float
    size_multiplier: float
    max_trades_per_hour: int
    max_exposure_pct: float
    aggressiveness_score: float
    confidence_threshold: float
    drawdown_tolerance_pct: float
    description: str
    profile_setting: str = MODE_AUTO
    deployment_mode: str = DEPLOYMENT_GUARDED
    deployment_effective_mode: str = DEPLOYMENT_GUARDED
    deployment_requested_utilization_pct: float = 0.0
    deployment_target_utilization_pct: float = 0.0
    deployment_max_positions: int = 0
    deployment_respect_exposure_cap: bool = True
    deployment_respect_headroom: bool = True
    deployment_same_side_limit_pct: float = 0.0
    deployment_total_exposure_limit_pct: float = 0.0
    deployment_per_position_target_usdt: float = 0.0
    deployment_total_deployable_usdt: float = 0.0
    deployment_reference_leverage: int = 1
    deployment_description: str = ""


def _normalize_profile_setting(value: str | None) -> str:
    active = str(value or MODE_AUTO).strip().upper()
    return active if active in CAPITAL_PROFILE_CHOICES else MODE_AUTO


def _normalize_deployment_mode(value: str | None) -> str:
    raw = str(value or DEPLOYMENT_GUARDED).strip().lower()
    for mode in DEPLOYMENT_MODE_CHOICES:
        if raw == mode.lower():
            return mode
    aliases = {
        "max_deploy": DEPLOYMENT_MAX_DEPLOY,
        "full_deployment": DEPLOYMENT_FULL_DEPLOYMENT,
        "full deploy": DEPLOYMENT_FULL_DEPLOYMENT,
        "single_shot": DEPLOYMENT_SINGLE_SHOT,
        "ai_adaptive": DEPLOYMENT_AI_ADAPTIVE,
    }
    return aliases.get(raw, DEPLOYMENT_GUARDED)


def _resolve_profile_mode(balance: float) -> tuple[str, str]:
    profile_setting = _normalize_profile_setting(_ss.get_val("capital_profile", MODE_AUTO))
    if profile_setting in {MODE_MICRO, MODE_GROWTH, MODE_PRO}:
        return profile_setting, profile_setting

    micro_max = float(_ss.get_val("v6_micro_max_balance", 100.0) or 100.0)
    growth_max = float(_ss.get_val("v6_growth_max_balance", 1000.0) or 1000.0)
    if balance < micro_max:
        return MODE_MICRO, MODE_AUTO
    if balance < growth_max:
        return MODE_GROWTH, MODE_AUTO
    return MODE_PRO, MODE_AUTO


def _resolve_ai_deployment_mode(balance: float) -> str:
    intent_mode = "NORMAL"
    try:
        import trade_intent as _intent

        intent_mode = str(_intent.get_trade_intent(balance=balance).mode or "NORMAL").upper()
    except Exception:
        intent_mode = "NORMAL"

    regime = ""
    try:
        import regime_detector as _regime

        regime = str(_regime.get_regime() or "").upper()
    except Exception:
        regime = ""

    if intent_mode in {"STOPPED", "DEFENSIVE"}:
        return DEPLOYMENT_RECOVERY
    if intent_mode == "AGGRESSIVE" and regime == "TRENDING":
        return DEPLOYMENT_MAX_DEPLOY
    if intent_mode == "AGGRESSIVE":
        return DEPLOYMENT_BALANCED
    if regime == "VOLATILE":
        return DEPLOYMENT_GUARDED
    if regime == "TRENDING":
        return DEPLOYMENT_BALANCED
    return DEPLOYMENT_GUARDED


def _base_profile(balance: float, mode: str, profile_setting: str) -> CapitalProfile:
    micro_max = float(_ss.get_val("v6_micro_max_balance", 100.0) or 100.0)
    growth_max = float(_ss.get_val("v6_growth_max_balance", 1000.0) or 1000.0)

    if mode == MODE_MICRO:
        return CapitalProfile(
            mode=MODE_MICRO,
            balance=balance,
            risk_multiplier=float(_ss.get_val("v6_micro_risk_multiplier", 1.4)),
            size_multiplier=float(_ss.get_val("v6_micro_size_multiplier", 1.3)),
            max_trades_per_hour=int(_ss.get_val("v6_micro_max_trades_per_hour", 8)),
            max_exposure_pct=float(_ss.get_val("v6_micro_max_exposure_pct", 130.0)),
            aggressiveness_score=0.80,
            confidence_threshold=float(_ss.get_val("v6_micro_confidence_threshold", 1.5)),
            drawdown_tolerance_pct=float(_ss.get_val("v6_micro_drawdown_tolerance_pct", 15.0)),
            description=f"MICRO: aggressive growth (balance ${balance:.2f} < ${micro_max:.0f})",
            profile_setting=profile_setting,
        )
    if mode == MODE_GROWTH:
        return CapitalProfile(
            mode=MODE_GROWTH,
            balance=balance,
            risk_multiplier=float(_ss.get_val("v6_growth_risk_multiplier", 1.0)),
            size_multiplier=float(_ss.get_val("v6_growth_size_multiplier", 1.0)),
            max_trades_per_hour=int(_ss.get_val("v6_growth_max_trades_per_hour", 5)),
            max_exposure_pct=float(_ss.get_val("v6_growth_max_exposure_pct", 60.0)),
            aggressiveness_score=0.50,
            confidence_threshold=float(_ss.get_val("v6_growth_confidence_threshold", 1.8)),
            drawdown_tolerance_pct=float(_ss.get_val("v6_growth_drawdown_tolerance_pct", 10.0)),
            description=f"GROWTH: balanced (balance ${balance:.2f})",
            profile_setting=profile_setting,
        )
    return CapitalProfile(
        mode=MODE_PRO,
        balance=balance,
        risk_multiplier=float(_ss.get_val("v6_pro_risk_multiplier", 0.75)),
        size_multiplier=float(_ss.get_val("v6_pro_size_multiplier", 0.85)),
        max_trades_per_hour=int(_ss.get_val("v6_pro_max_trades_per_hour", 3)),
        max_exposure_pct=float(_ss.get_val("v6_pro_max_exposure_pct", 40.0)),
        aggressiveness_score=0.20,
        confidence_threshold=float(_ss.get_val("v6_pro_confidence_threshold", 2.2)),
        drawdown_tolerance_pct=float(_ss.get_val("v6_pro_drawdown_tolerance_pct", 5.0)),
        description=f"PRO: preservation (balance ${balance:.2f} >= ${growth_max:.0f})",
        profile_setting=profile_setting,
    )


def _deployment_policy(profile: CapitalProfile, deployment_mode: str) -> dict:
    effective_mode = deployment_mode
    if deployment_mode == DEPLOYMENT_AI_ADAPTIVE:
        effective_mode = _resolve_ai_deployment_mode(profile.balance)

    preset = dict(_DEPLOYMENT_PRESETS.get(effective_mode, _DEPLOYMENT_PRESETS[DEPLOYMENT_GUARDED]))
    requested_util = float(
        _ss.get_val("deployment_target_utilization_pct", preset["target_utilization_pct"])
        or preset["target_utilization_pct"]
    )
    requested_util = max(min(requested_util, 95.0), 5.0)
    override_max_positions = int(_ss.get_val("deployment_max_positions", 0) or 0)
    requested_max_positions = max(override_max_positions if override_max_positions > 0 else int(preset["max_positions"]), 1)
    max_positions = requested_max_positions

    # Deployment posture may reshape allocation, but it may not disable hard
    # exposure or capital-headroom safety controls.
    respect_exposure_cap = True
    respect_headroom = True

    reference_leverage = max(
        min(
            int(_ss.get_val("leverage_default", 10)),
            int(_ss.get_val("leverage_max", _ss.get_val("leverage_default", 10))),
        ),
        int(_ss.get_val("leverage_min", 5)),
    )
    derived_total_limit_pct = requested_util * float(reference_leverage)
    total_limit_override = float(_ss.get_val("deployment_total_exposure_limit_pct", 0.0) or 0.0)
    same_side_override = float(_ss.get_val("deployment_same_side_limit_pct", 0.0) or 0.0)

    if respect_exposure_cap:
        total_limit_pct = float(profile.max_exposure_pct)
    else:
        total_limit_pct = total_limit_override if total_limit_override > 0 else min(
            derived_total_limit_pct,
            float(preset["total_exposure_limit_pct"]),
        )

    if respect_headroom:
        default_same_side = float(
            _ss.get_val(
                f"portfolio_same_direction_pct_{profile.mode.lower()}",
                profile.max_exposure_pct,
            ) or profile.max_exposure_pct
        )
        same_side_limit_pct = min(default_same_side, total_limit_pct)
    else:
        same_side_limit_pct = same_side_override if same_side_override > 0 else min(
            float(preset["same_side_limit_pct"]),
            total_limit_pct,
        )

    deployable_total_usdt = 0.0
    if profile.balance > 0 and reference_leverage > 0:
        deployable_total_usdt = profile.balance * requested_util / 100.0 * float(reference_leverage)
        deployable_total_usdt = min(
            deployable_total_usdt,
            profile.balance * total_limit_pct / 100.0,
        )
    try:
        min_order = float(_ss.get_effective_min_order_usdt(profile.balance) or 0.0)
    except Exception:
        min_order = 0.0
    dyn_min = float(_ss.get_val("dynamic_target_min_usdt", 50.0) or 50.0)
    min_slot = _micro_sizing.deployment_slot_floor_usdt(
        profile.balance,
        settings=_ss,
        account_floor=min_order,
        target_min=dyn_min,
    )
    micro_ceiling = _micro_sizing.deployment_position_ceiling(profile.balance, settings=_ss)
    if micro_ceiling is not None and max_positions > micro_ceiling and deployable_total_usdt > 0:
        viable_slots = max(min(int(deployable_total_usdt // max(min_slot, 1.0)), micro_ceiling), 1)
        if viable_slots < max_positions:
            logger.warning(
                "[V6.Capital] deployment max positions %d reduced to %d for micro executable floor $%.2f",
                max_positions,
                viable_slots,
                min_slot,
            )
        max_positions = viable_slots

    # Full Deployment must not split a MICRO account into more slots than can
    # produce executable positions. Operator overrides remain respected unless
    # they would create hidden below-minimum per-slot sizing.
    if effective_mode == DEPLOYMENT_FULL_DEPLOYMENT and deployable_total_usdt > 0:
        dynamic_slots = int(deployable_total_usdt // min_slot) if min_slot > 0 else int(preset["max_positions"])
        # Avoid chaos: cap by balance tier so small accounts don't explode into dozens of tiny positions.
        if micro_ceiling is not None:
            ceiling = micro_ceiling
        elif profile.balance < 75:
            ceiling = 4
        elif profile.balance < 150:
            ceiling = 6
        elif profile.balance < 400:
            ceiling = 8
        elif profile.balance < 1000:
            ceiling = 10
        else:
            ceiling = 12
        viable_slots = max(min(dynamic_slots, ceiling), 1)
        if override_max_positions > 0:
            requested_slot_usdt = deployable_total_usdt / max(requested_max_positions, 1)
            requested_viable = max(int(deployable_total_usdt // max(min_slot, 1.0)), 1)
            max_positions = min(requested_max_positions, requested_viable, ceiling)
            if max_positions < requested_max_positions:
                logger.warning(
                    "[V6.Capital] deployment_max_positions %d reduced to %d; deployable $%.2f cannot support more executable $%.2f slots",
                    requested_max_positions,
                    max_positions,
                    deployable_total_usdt,
                    min_slot,
                )
        else:
            if micro_ceiling is not None:
                max_positions = viable_slots
            else:
                max_positions = max(viable_slots, 4 if deployable_total_usdt >= min_slot * 4 else viable_slots)

    per_position_target_usdt = deployable_total_usdt / max_positions if max_positions > 0 else deployable_total_usdt

    description = str(preset["description"])
    if deployment_mode == DEPLOYMENT_AI_ADAPTIVE:
        description = f"{description} Effective mode now {effective_mode}."

    return {
        "deployment_mode": deployment_mode,
        "deployment_effective_mode": effective_mode,
        "requested_utilization_pct": requested_util,
        "target_utilization_pct": requested_util,
        "max_positions": max_positions,
        "respect_exposure_cap": respect_exposure_cap,
        "respect_headroom": respect_headroom,
        "same_side_limit_pct": round(max(same_side_limit_pct, 0.0), 2),
        "total_exposure_limit_pct": round(max(total_limit_pct, 0.0), 2),
        "per_position_target_usdt": round(max(per_position_target_usdt, 0.0), 2),
        "total_deployable_usdt": round(max(deployable_total_usdt, 0.0), 2),
        "reference_leverage": int(reference_leverage),
        "description": description,
    }


def get_profile_for_mode(balance: float, deployment_mode: str) -> CapitalProfile:
    """Resolve a profile for diagnostics without mutating runtime settings."""
    mode, profile_setting = _resolve_profile_mode(balance)
    profile = _base_profile(balance, mode, profile_setting)

    deployment_mode = _normalize_deployment_mode(deployment_mode)
    policy = _deployment_policy(profile, deployment_mode)
    resolved = CapitalProfile(
        **{
            **profile.__dict__,
            "deployment_mode": policy["deployment_mode"],
            "deployment_effective_mode": policy["deployment_effective_mode"],
            "deployment_requested_utilization_pct": policy["requested_utilization_pct"],
            "deployment_target_utilization_pct": policy["target_utilization_pct"],
            "deployment_max_positions": policy["max_positions"],
            "deployment_respect_exposure_cap": policy["respect_exposure_cap"],
            "deployment_respect_headroom": policy["respect_headroom"],
            "deployment_same_side_limit_pct": policy["same_side_limit_pct"],
            "deployment_total_exposure_limit_pct": policy["total_exposure_limit_pct"],
            "deployment_per_position_target_usdt": policy["per_position_target_usdt"],
            "deployment_total_deployable_usdt": policy["total_deployable_usdt"],
            "deployment_reference_leverage": policy["reference_leverage"],
            "deployment_description": policy["description"],
            "max_exposure_pct": policy["total_exposure_limit_pct"],
            "description": (
                f"{profile.description} | deployment {deployment_mode}"
                f" -> ${policy['per_position_target_usdt']:.2f} per slot"
            ),
        }
    )
    logger.debug("[V6.Capital] %s", resolved.description)
    return resolved


def get_profile(balance: float) -> CapitalProfile:
    return get_profile_for_mode(
        balance,
        _ss.get_val("deployment_mode", DEPLOYMENT_GUARDED),
    )


_trade_timestamps: deque = deque()


def record_trade_entry() -> None:
    _trade_timestamps.append(_time.monotonic())
    cutoff_24h = _time.monotonic() - 86400
    while _trade_timestamps and _trade_timestamps[0] < cutoff_24h:
        _trade_timestamps.popleft()


def reset_frequency_state() -> None:
    """Clear internal trade-frequency buckets (NEW decisions only)."""
    _trade_timestamps.clear()


def check_frequency(profile: CapitalProfile) -> tuple[bool, str]:
    if not _ss.get_val("v6_trade_frequency_enabled", True):
        return True, "frequency control disabled"

    now = _time.monotonic()
    cutoff_hour = now - 3600
    cutoff_day = now - 86400
    trades_last_hour = sum(1 for ts in _trade_timestamps if ts >= cutoff_hour)
    trades_last_day = sum(1 for ts in _trade_timestamps if ts >= cutoff_day)

    max_per_hour = profile.max_trades_per_hour
    deployment_max = max(int(getattr(profile, "deployment_max_positions", 0) or 0), 0)
    if deployment_max > 0:
        max_per_hour = min(max_per_hour, max(deployment_max * 2, 1))
    max_per_day = int(_ss.get_val("v6_max_trades_per_day", 30))

    if trades_last_hour >= max_per_hour:
        return False, f"{mode_label(profile.mode)} hourly cap reached ({trades_last_hour}/{max_per_hour})"
    return True, "ok"


def mode_label(mode: str) -> str:
    return {"MICRO": "MICRO", "GROWTH": "GROWTH", "PRO": "PRO"}.get(mode, mode)

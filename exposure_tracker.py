"""
OxyPulse V6.1 — exposure_tracker.py
Real-time long/short exposure and correlation control.

Reads live position data from position_registry and computes:
  - total long notional (USDT)
  - total short notional (USDT)
  - net exposure (long − short)
  - per-coin exposure
  - BTC-proxy correlation group exposure

Used by the V6.1 pipeline to:
  1. Block trades if one direction is already over-exposed
  2. Block trades if too many correlated coins are already open
  3. Feed the AI gatekeeper with net exposure context

No state is stored here — all data is read fresh from position_registry each call.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field

import settings_store as _ss
import micro_sizing as _micro_sizing

logger = logging.getLogger("OxyPulse.V6.Exposure")

EXPOSURE_MODE_STRICT = "Strict"
EXPOSURE_MODE_ADAPTIVE = "Adaptive"
EXPOSURE_MODE_RELAXED = "Relaxed"
EXPOSURE_MODE_OVERRIDE = "Override"
EXPOSURE_MODE_CHOICES = (
    EXPOSURE_MODE_STRICT,
    EXPOSURE_MODE_ADAPTIVE,
    EXPOSURE_MODE_RELAXED,
    EXPOSURE_MODE_OVERRIDE,
)

# Correlation groups — coins in the same group behave similarly.
# Extend as needed; keys are group names, values are sets of uppercase tickers.
_CORRELATION_GROUPS: dict[str, set[str]] = {
    "BTC_LAYER1":    {"BTC", "ETH", "BNB", "SOL", "AVAX", "MATIC", "DOT", "ADA"},
    "DEFI":          {"UNI", "AAVE", "COMP", "CRV", "MKR", "SNX", "LDO", "GMX"},
    "MEME":          {"PEPE", "DOGE", "SHIB", "FLOKI", "BONK", "WIF", "NEIRO"},
    "L2":            {"ARB", "OP", "STRK", "MANTA", "BLAST", "ZK"},
    "AI_INFRA":      {"FET", "AGIX", "NEAR", "TAO", "RNDR", "WLD"},
    "GAMING":        {"AXS", "IMX", "SAND", "MANA", "GMT", "GALA"},
}


def _get_coin_group(coin: str) -> str | None:
    """Return the correlation group name for a coin, or None."""
    uc = coin.upper().replace("-USDT-SWAP", "").replace("USDT", "")
    for group, members in _CORRELATION_GROUPS.items():
        if uc in members:
            return group
    return None


@dataclass
class ExposureState:
    """Snapshot of current portfolio exposure."""
    long_usdt: float = 0.0
    short_usdt: float = 0.0
    net_usdt: float = 0.0           # long − short
    total_usdt: float = 0.0         # long + short
    long_count: int = 0
    short_count: int = 0
    by_coin: dict = field(default_factory=dict)          # coin → (direction, usdt)
    by_group: dict = field(default_factory=dict)         # group → {long: n, short: n}
    balance: float = 0.0
    net_exposure_pct: float = 0.0   # net / balance × 100


@dataclass(frozen=True)
class ExposurePolicy:
    mode: str
    total_exposure_pct: float
    same_side_pct: float
    max_positions: int
    correlation_limit: int
    description: str
    warning: str = ""
    override: bool = False


def _normalize_exposure_mode(value: str | None) -> str:
    raw = str(value or EXPOSURE_MODE_STRICT).strip().lower()
    aliases = {
        "strict": EXPOSURE_MODE_STRICT,
        "adaptive": EXPOSURE_MODE_ADAPTIVE,
        "relaxed": EXPOSURE_MODE_RELAXED,
        "override": EXPOSURE_MODE_OVERRIDE,
    }
    return aliases.get(raw, EXPOSURE_MODE_STRICT)


def _profile_context(balance: float, capital_profile=None):
    if capital_profile is not None:
        return capital_profile
    try:
        from capital_manager import get_profile as _get_profile

        return _get_profile(balance)
    except Exception:
        return None


def _base_policy(balance: float, capital_profile=None) -> tuple[float, float, int, int, bool]:
    profile = _profile_context(balance, capital_profile)
    fixed_global = max(int(_ss.get_val("max_open_positions", 10) or 10), 1)
    corr_limit = max(int(_ss.get_val("correlation_max_same_group", 2) or 2), 1)
    if profile is None:
        return 60.0, 60.0, fixed_global, corr_limit, False

    def _norm_slot_mode(value: str | None) -> str:
        raw = str(value or "fixed").strip().lower()
        return raw if raw in {"fixed", "auto", "unlimited"} else "fixed"

    def _unlimited_global_cap(balance_usdt: float, prof) -> int:
        deployable = float(getattr(prof, "deployment_total_deployable_usdt", 0.0) or 0.0)
        if deployable <= 0:
            return fixed_global
        min_order = float(_ss.get_effective_min_order_usdt(balance_usdt) or 0.0)
        dyn_min = float(_ss.get_val("dynamic_target_min_usdt", 50.0) or 50.0)
        min_slot = _micro_sizing.deployment_slot_floor_usdt(
            balance_usdt,
            settings=_ss,
            account_floor=min_order,
            target_min=dyn_min,
        )
        slots = int(deployable // min_slot) if min_slot > 0 else fixed_global
        micro_ceiling = _micro_sizing.deployment_position_ceiling(balance_usdt, settings=_ss)
        if micro_ceiling is None:
            slots = max(slots, 4)
        if micro_ceiling is not None:
            ceiling = micro_ceiling
        elif balance_usdt < 150:
            ceiling = 10
        elif balance_usdt < 400:
            ceiling = 15
        elif balance_usdt < 1000:
            ceiling = 20
        else:
            ceiling = 30
        return max(min(slots, ceiling), 1 if micro_ceiling is not None else 4)

    slot_mode = _norm_slot_mode(_ss.get_val("global_slot_mode", "fixed"))
    deployment_max = int(getattr(profile, "deployment_max_positions", 0) or 0)
    if slot_mode == "fixed":
        global_max = fixed_global
    elif slot_mode == "auto":
        global_max = deployment_max if deployment_max > 0 else fixed_global
    else:
        global_max = _unlimited_global_cap(balance, profile)

    mode_key = str(getattr(profile, "mode", "GROWTH") or "GROWTH").lower()
    total_pct = float(getattr(profile, "max_exposure_pct", 60.0) or 60.0)
    deployment_total = float(getattr(profile, "deployment_total_exposure_limit_pct", 0.0) or 0.0)
    if not bool(getattr(profile, "deployment_respect_exposure_cap", True)) and deployment_total > 0:
        total_pct = max(total_pct, deployment_total)

    same_side_pct = float(_ss.get_val(f"portfolio_same_direction_pct_{mode_key}", total_pct) or total_pct)
    deployment_same = float(getattr(profile, "deployment_same_side_limit_pct", 0.0) or 0.0)
    if not bool(getattr(profile, "deployment_respect_headroom", True)) and deployment_same > 0:
        same_side_pct = max(same_side_pct, deployment_same)
    same_side_pct = min(same_side_pct, total_pct) if total_pct > 0 else same_side_pct

    # Fixed mode is a hard cap. Auto/unlimited are already computed; they should not be re-minned
    # back down to a small static value.
    max_positions = max(global_max, 1)
    concentrated = str(
        getattr(profile, "deployment_effective_mode", getattr(profile, "deployment_mode", "Guarded")) or "Guarded"
    ).strip().lower() in {"max deploy", "single shot", "recovery"}
    return total_pct, same_side_pct, max_positions, corr_limit, concentrated


def get_exposure_policy(
    balance: float,
    capital_profile=None,
    candidate_quality: float = 0.0,
) -> ExposurePolicy:
    mode = _normalize_exposure_mode(_ss.get_val("exposure_control_mode", EXPOSURE_MODE_STRICT))
    total_pct, same_side_pct, max_positions, corr_limit, concentrated = _base_policy(balance, capital_profile)
    open_count = len(get_exposure(balance).by_coin)

    dd_level = 0
    try:
        import drawdown_guard as _ddg

        dd_level = int(_ddg.get_drawdown_state(capital_profile=capital_profile).level or 0)
    except Exception:
        dd_level = 0

    intent_mode = "NORMAL"
    try:
        import trade_intent as _intent

        intent_mode = str(_intent.get_trade_intent(balance=balance).mode or "NORMAL").upper()
    except Exception:
        intent_mode = "NORMAL"

    quality = max(float(candidate_quality or 0.0), 0.0)

    if mode == EXPOSURE_MODE_STRICT:
        return ExposurePolicy(
            mode=mode,
            total_exposure_pct=round(max(total_pct, 40.0), 2),
            same_side_pct=round(max(min(same_side_pct, total_pct), 20.0), 2),
            max_positions=max(max_positions, 1),
            correlation_limit=max(corr_limit, 1),
            description="Strict preserves the current conservative exposure, same-side, and correlation limits for new entries.",
        )

    if mode == EXPOSURE_MODE_RELAXED:
        total_pct = max(total_pct * 1.20, total_pct + 20.0)
        same_side_pct = max(same_side_pct * 1.15, same_side_pct + 15.0)
        if not concentrated:
            max_positions += 1
        corr_limit += 1
        return ExposurePolicy(
            mode=mode,
            total_exposure_pct=round(total_pct, 2),
            same_side_pct=round(min(same_side_pct, total_pct), 2),
            max_positions=max(max_positions, 1),
            correlation_limit=max(corr_limit, 1),
            description="Relaxed raises exposure and correlation tolerance for stronger capital use while keeping hard execution and stop protections intact.",
        )

    if mode == EXPOSURE_MODE_OVERRIDE:
        total_pct = max(total_pct * 1.55, total_pct + 60.0)
        same_side_pct = max(same_side_pct * 1.45, same_side_pct + 45.0)
        if not concentrated:
            max_positions += 2
        corr_limit += 2
        return ExposurePolicy(
            mode=mode,
            total_exposure_pct=round(total_pct, 2),
            same_side_pct=round(min(same_side_pct, total_pct), 2),
            max_positions=max(max_positions, 1),
            correlation_limit=max(corr_limit, 1),
            description="Override heavily relaxes exposure and correlation gating for new entries. This is intentionally high risk.",
            warning="High risk override",
            override=True,
        )

    quality_bonus = 0.0
    headroom_bonus = 0.0
    slot_bonus = 0
    corr_bonus = 0
    reasons: list[str] = []
    if dd_level >= 2 or intent_mode in {"DEFENSIVE", "STOPPED"}:
        total_pct = max(total_pct * 0.90, 40.0)
        same_side_pct = max(same_side_pct * 0.90, 20.0)
        if not concentrated:
            max_positions = max(max_positions - 1, 1)
        reasons.append("drawdown-aware clamp")
    else:
        if quality >= 55.0:
            quality_bonus = min((quality - 55.0) * 0.35, 24.0)
            headroom_bonus = min((quality - 55.0) * 0.25, 18.0)
            if quality >= 72.0:
                corr_bonus = 1
            reasons.append("quality uplift")
        if 0 < balance <= float(_ss.get_val("v6_micro_max_balance", 100.0) or 100.0) and open_count <= 2:
            total_pct += 15.0
            same_side_pct += 15.0
            reasons.append("small-balance flexibility")
        if not concentrated and quality >= 68.0 and intent_mode == "AGGRESSIVE":
            slot_bonus = 1
            reasons.append("aggressive intent slot uplift")
        total_pct += quality_bonus
        same_side_pct += headroom_bonus
    return ExposurePolicy(
        mode=mode,
        total_exposure_pct=round(total_pct, 2),
        same_side_pct=round(min(same_side_pct, total_pct), 2),
        max_positions=max(max_positions + slot_bonus, 1),
        correlation_limit=max(corr_limit + corr_bonus, 1),
        description="Adaptive shifts exposure tolerance with drawdown state, trade quality, balance, and current deployment posture."
        + (f" ({', '.join(reasons)})" if reasons else ""),
    )


def get_exposure(balance: float = 1.0) -> ExposureState:
    """Compute current portfolio exposure from position_registry.

    Safe: returns zero ExposureState if registry is unavailable.
    """
    state = ExposureState(balance=balance)
    try:
        from position_registry import get_all_details as _get_all
        positions = _get_all()
    except Exception:
        return state

    for coin, info in positions.items():
        direction = str(info.get("direction", "")).lower()
        size_usdt = float(info.get("size_usdt", 0.0))
        if size_usdt <= 0:
            continue

        state.by_coin[coin] = (direction, size_usdt)

        if direction == "long":
            state.long_usdt  += size_usdt
            state.long_count += 1
        elif direction == "short":
            state.short_usdt  += size_usdt
            state.short_count += 1

        group = _get_coin_group(coin)
        if group:
            g = state.by_group.setdefault(group, {"long": 0, "short": 0})
            if direction in g:
                g[direction] += 1

    state.net_usdt   = state.long_usdt - state.short_usdt
    state.total_usdt = state.long_usdt + state.short_usdt
    if balance > 0:
        state.net_exposure_pct = abs(state.net_usdt) / balance * 100

    return state


def get_available_exposure_usdt(
    balance: float,
    max_exposure_pct: float,
    *,
    capital_profile=None,
    candidate_quality: float = 0.0,
) -> float:
    if balance <= 0:
        return 0.0
    exp = get_exposure(balance)
    policy = get_exposure_policy(balance, capital_profile=capital_profile, candidate_quality=candidate_quality)
    effective_pct = max(float(max_exposure_pct or 0.0), float(policy.total_exposure_pct or 0.0))
    cap_usdt = balance * effective_pct / 100.0
    return round(max(cap_usdt - exp.total_usdt, 0.0), 4)


def check_exposure(
    coin: str,
    direction: str,
    size_usdt: float,
    balance: float,
    max_exposure_pct: float,
    *,
    capital_profile=None,
    candidate_quality: float = 0.0,
    debug_override_exposure_cap: str = "normal",      # normal | relaxed | unlimited
    debug_override_correlation_cap: str = "normal",   # normal | relaxed | unlimited
    debug_override_same_direction_cap: str = "normal" # normal | relaxed | unlimited
) -> tuple[bool, str]:
    """Check if adding this trade would breach exposure limits.

    Args:
        coin:              Coin to trade.
        direction:         "long" | "short".
        size_usdt:         Notional size of the proposed trade.
        balance:           Current account balance.
        max_exposure_pct:  Max total notional allowed as % of balance.

    Returns:
        (allowed: bool, reason: str)
    """
    if not _ss.get_val("v6_correlation_control_enabled", True):
        return True, "correlation control disabled"

    exp = get_exposure(balance)
    policy = get_exposure_policy(balance, capital_profile=capital_profile, candidate_quality=candidate_quality)
    max_exposure_pct = max(float(max_exposure_pct or 0.0), float(policy.total_exposure_pct or 0.0))

    # Debug overrides are applied by the caller (category_engine) for NEW decisions only.
    lvl_exp = str(debug_override_exposure_cap or "normal").strip().lower()
    lvl_corr = str(debug_override_correlation_cap or "normal").strip().lower()
    lvl_dir = str(debug_override_same_direction_cap or "normal").strip().lower()
    if lvl_exp not in {"normal", "relaxed", "unlimited"}:
        lvl_exp = "normal"
    if lvl_corr not in {"normal", "relaxed", "unlimited"}:
        lvl_corr = "normal"
    if lvl_dir not in {"normal", "relaxed", "unlimited"}:
        lvl_dir = "normal"

    if lvl_exp != "normal":
        if lvl_exp == "relaxed":
            max_exposure_pct = min(max_exposure_pct + max(10.0, max_exposure_pct * 0.25), 180.0)
        else:  # unlimited (bounded by other safety gates)
            max_exposure_pct = min(max_exposure_pct * 2.0, 220.0)

    # ── 1. Single-direction cap ───────────────────────────────────────────────
    max_one_dir = balance * float(policy.same_side_pct or 0.0) / 100.0 if balance > 0 else 0.0
    static_one_dir = float(_ss.get_val("v6_max_single_direction_usdt", 0.0) or 0.0)
    if static_one_dir > 0:
        max_one_dir = min(max_one_dir, static_one_dir) if max_one_dir > 0 else static_one_dir
    if lvl_dir != "normal" and max_one_dir > 0:
        if lvl_dir == "relaxed":
            max_one_dir *= 1.25
        else:  # unlimited (bounded by total exposure + other caps)
            max_one_dir *= 1.75
    if direction == "long":
        projected = exp.long_usdt + size_usdt
        if projected > max_one_dir:
            available = max(max_one_dir - exp.long_usdt, 0.0)
            return False, (
                f"long exposure {projected:.0f} > cap {max_one_dir:.0f} USDT [{policy.mode}] | "
                f"required=${size_usdt:.2f} available=${available:.2f} | "
                f"suggested=reduce size or lift single-direction cap"
            )
    elif direction == "short":
        projected = exp.short_usdt + size_usdt
        if projected > max_one_dir:
            available = max(max_one_dir - exp.short_usdt, 0.0)
            return False, (
                f"short exposure {projected:.0f} > cap {max_one_dir:.0f} USDT [{policy.mode}] | "
                f"required=${size_usdt:.2f} available=${available:.2f} | "
                f"suggested=reduce size or lift single-direction cap"
            )

    # ── 2. Total notional cap ─────────────────────────────────────────────────
    if balance > 0:
        projected_total = exp.total_usdt + size_usdt
        if projected_total / balance * 100 > max_exposure_pct:
            available = max(balance * max_exposure_pct / 100.0 - exp.total_usdt, 0.0)
            return False, (
                f"total exposure {projected_total:.0f} USDT "
                f"({projected_total/balance*100:.0f}% > {max_exposure_pct:.0f}%) [{policy.mode}] | "
                f"required=${size_usdt:.2f} available=${available:.2f} | "
                f"suggested=increase exposure cap or use smaller size"
            )

    # ── 3. Correlation group cap ───────────────────────────────────────────────
    if _ss.get_val("correlation_filter_enabled", True):
        max_corr = max(int(policy.correlation_limit or 0), int(_ss.get_val("correlation_max_same_group", 2) or 2))
        if lvl_corr != "normal":
            if lvl_corr == "relaxed":
                max_corr += 1
            else:  # unlimited (bounded)
                max_corr += 3
        group = _get_coin_group(coin)
        if group and group in exp.by_group:
            group_count = exp.by_group[group].get(direction, 0)
            if group_count >= max_corr:
                return False, (
                    f"correlation cap: already {group_count} {direction} trades in group '{group}' [{policy.mode}] | "
                    f"suggested=wait for correlated exposure to clear or disable the filter"
                )

    return True, "ok"


def exposure_summary(state: ExposureState) -> dict:
    """Return a compact dict for logging/telemetry."""
    return {
        "long_usdt": round(state.long_usdt, 2),
        "short_usdt": round(state.short_usdt, 2),
        "net_pct": round(state.net_exposure_pct, 1),
        "total_usdt": round(state.total_usdt, 2),
        "positions": len(state.by_coin),
    }

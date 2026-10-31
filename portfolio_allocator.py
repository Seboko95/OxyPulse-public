from __future__ import annotations

from dataclasses import dataclass

import settings_store as _s
from capital_manager import get_profile
import position_registry as _pr


@dataclass(frozen=True)
class LaneStatus:
    lane: str
    mode: str
    used_usdt: float
    used_pct: float
    budget_pct: float
    remaining_pct: float
    positions: int


_LANE_LABELS = {
    "trend": "Trend",
    "breakout": "Breakout",
    "structure": "Structure",
    "mean_reversion": "Mean Rev",
    "order_flow": "Order Flow",
    "selector": "Selector",
    "protector": "Protector",
}


def lane_label(lane: str) -> str:
    return _LANE_LABELS.get(lane, lane.replace("_", " ").title())


def _mode(balance: float, capital_profile=None) -> str:
    if capital_profile is not None and getattr(capital_profile, "mode", ""):
        return str(capital_profile.mode).upper()
    return str(get_profile(balance).mode).upper()


def _lane_soft_accept_bonus(mode: str, session: str | None = None) -> float:
    mode_lc = str(mode or "").lower()
    if mode_lc != "micro":
        return 0.0
    bonus = float(_s.get_val("soft_accept_lane_budget_bonus_micro", 10.0) or 10.0)
    if str(session or "").upper() == "OFF":
        bonus += float(_s.get_val("soft_accept_lane_budget_bonus_micro_off", 5.0) or 5.0)
    return max(bonus, 0.0)


def get_lane_budget_pct(
    lane: str,
    balance: float,
    candidate_size_usdt: float = 0.0,
    capital_profile=None,
    soft_accept: bool = False,
    session: str | None = None,
) -> float:
    mode = _mode(balance, capital_profile).lower()
    key = f"lane_budget_{mode}_{lane}_pct"
    default = 100.0
    budget_pct = float(_s.get_val(key, default) or default)
    if balance > 0 and candidate_size_usdt > 0:
        # MICRO accounts often need one full executable trade that already
        # consumes most of the exposure budget. Allow that first viable trade,
        # but still block the lane from stacking beyond it.
        budget_pct = max(budget_pct, (candidate_size_usdt / balance) * 100.0)
    if soft_accept:
        budget_pct += _lane_soft_accept_bonus(mode, session=session)
        if mode == "micro":
            budget_pct = min(budget_pct, 95.0)
    return max(budget_pct, 0.0)


def get_same_direction_cap_pct(balance: float, capital_profile=None) -> float:
    mode = _mode(balance, capital_profile).lower()
    profile = capital_profile or get_profile(balance)
    default = float(getattr(profile, "max_exposure_pct", 60.0) or 60.0)
    key = f"portfolio_same_direction_pct_{mode}"
    cap_pct = float(_s.get_val(key, default) or default)
    deployment_limit = float(getattr(profile, "deployment_same_side_limit_pct", 0.0) or 0.0)
    respect_headroom = bool(getattr(profile, "deployment_respect_headroom", True))
    if not respect_headroom and deployment_limit > 0:
        cap_pct = max(min(deployment_limit, max(default, deployment_limit)), 0.0)
    else:
        cap_pct = max(min(cap_pct, default), 0.0)
    try:
        from exposure_tracker import get_exposure_policy as _get_exposure_policy

        policy = _get_exposure_policy(balance, capital_profile=capital_profile)
        return max(min(float(policy.same_side_pct or cap_pct), float(policy.total_exposure_pct or cap_pct)), 0.0)
    except Exception:
        return cap_pct


def get_same_direction_headroom_usdt(
    direction: str,
    balance: float,
    capital_profile=None,
) -> float:
    if balance <= 0:
        return 0.0
    direction = str(direction or "").lower()
    current_usdt = 0.0
    for trade in _pr._registry.values():
        if str(trade.get("direction", "") or "").lower() != direction:
            continue
        current_usdt += float(trade.get("size_usdt", 0.0) or 0.0)
    cap_pct = get_same_direction_cap_pct(balance, capital_profile=capital_profile)
    cap_usdt = balance * cap_pct / 100.0
    return round(max(cap_usdt - current_usdt, 0.0), 4)


def check_same_direction_budget(
    direction: str,
    candidate_size_usdt: float,
    balance: float,
    capital_profile=None,
    candidate_quality: float = 0.0,
    debug_override_level: str = "normal",  # normal | relaxed | unlimited
) -> tuple[bool, str]:
    if not _s.get_val("same_direction_headroom_enabled", True):
        return True, "same-direction headroom disabled"
    if balance <= 0:
        return True, "portfolio budget skipped"

    direction = str(direction or "").lower()
    current_usdt = 0.0
    for trade in _pr._registry.values():
        if str(trade.get("direction", "") or "").lower() != direction:
            continue
        current_usdt += float(trade.get("size_usdt", 0.0) or 0.0)

    cap_pct = get_same_direction_cap_pct(balance, capital_profile=capital_profile)
    cap_usdt = balance * cap_pct / 100.0
    projected_usdt = current_usdt + max(float(candidate_size_usdt or 0.0), 0.0)
    try:
        from exposure_tracker import get_exposure_policy as _get_exposure_policy

        policy = _get_exposure_policy(balance, capital_profile=capital_profile, candidate_quality=candidate_quality)
        cap_pct = float(policy.same_side_pct or cap_pct)
        cap_usdt = balance * cap_pct / 100.0
    except Exception:
        policy = None

    # Debug override: adjust headroom cap for NEW decisions only (category_engine passes this).
    lvl = str(debug_override_level or "normal").strip().lower()
    if lvl not in {"normal", "relaxed", "unlimited"}:
        lvl = "normal"
    if lvl != "normal":
        if lvl == "relaxed":
            cap_pct = min(cap_pct + max(5.0, cap_pct * 0.15), 95.0)
        else:  # unlimited (bounded by other safety gates)
            cap_pct = 99.0
        cap_usdt = balance * cap_pct / 100.0
    if projected_usdt > cap_usdt + 0.01:
        remaining_usdt = max(cap_usdt - current_usdt, 0.0)
        executable_floor = _s.get_effective_min_order_usdt(balance)
        mode = _mode(balance, capital_profile)
        mode_suffix = f" [{policy.mode}]" if policy is not None else ""
        if remaining_usdt < executable_floor:
            return (
                False,
                f"portfolio {direction} headroom ${remaining_usdt:.2f} < executable floor "
                f"${executable_floor:.2f} ({cap_pct:.0f}% {mode}{mode_suffix}) | required=${candidate_size_usdt:.2f} "
                f"available=${remaining_usdt:.2f} | suggested=increase {mode} headroom cap or reduce size",
            )
        return (
            False,
            f"portfolio {direction} projected ${projected_usdt:.2f} > ${cap_usdt:.2f} cap "
            f"({cap_pct:.0f}% {mode}{mode_suffix}) | required=${candidate_size_usdt:.2f} available=${remaining_usdt:.2f} "
            f"| suggested=raise {mode} headroom cap or allow smaller size",
        )
    return True, "ok"


def summarize_lane_usage(balance: float, capital_profile=None) -> list[LaneStatus]:
    if balance <= 0:
        return []

    mode = _mode(balance, capital_profile)
    usage: dict[str, dict] = {}
    for trade in _pr._registry.values():
        engine = str(trade.get("engine", "") or "")
        lane = _s.get_engine_lane(engine)
        info = usage.setdefault(lane, {"used_usdt": 0.0, "positions": 0})
        info["used_usdt"] += float(trade.get("size_usdt", 0.0) or 0.0)
        info["positions"] += 1

    statuses: list[LaneStatus] = []
    for lane in sorted(usage.keys()):
        used_usdt = float(usage.get(lane, {}).get("used_usdt", 0.0) or 0.0)
        used_pct = (used_usdt / balance * 100.0) if balance > 0 else 0.0
        budget_pct = get_lane_budget_pct(lane, balance, capital_profile=capital_profile)
        statuses.append(
            LaneStatus(
                lane=lane,
                mode=mode,
                used_usdt=round(used_usdt, 4),
                used_pct=round(used_pct, 4),
                budget_pct=round(budget_pct, 4),
                remaining_pct=round(max(budget_pct - used_pct, 0.0), 4),
                positions=int(usage.get(lane, {}).get("positions", 0) or 0),
            )
        )
    return sorted(statuses, key=lambda item: item.used_pct, reverse=True)


def get_lane_available_usdt(
    engine_name: str,
    balance: float,
    capital_profile=None,
    soft_accept: bool = False,
    session: str | None = None,
) -> float:
    if balance <= 0:
        return 0.0
    lane = _s.get_engine_lane(engine_name)
    used_usdt = 0.0
    for trade in _pr._registry.values():
        engine = str(trade.get("engine", "") or "")
        if _s.get_engine_lane(engine) != lane:
            continue
        used_usdt += float(trade.get("size_usdt", 0.0) or 0.0)
    budget_pct = get_lane_budget_pct(
        lane,
        balance,
        capital_profile=capital_profile,
        soft_accept=soft_accept,
        session=session,
    )
    budget_usdt = balance * budget_pct / 100.0
    return round(max(budget_usdt - used_usdt, 0.0), 4)


def check_lane_budget(
    engine_name: str,
    candidate_size_usdt: float,
    balance: float,
    capital_profile=None,
    soft_accept: bool = False,
    session: str | None = None,
    debug_override_level: str = "normal",  # normal | relaxed | unlimited
) -> tuple[bool, str]:
    if not _s.get_val("lane_budget_enabled", True):
        return True, "lane budgets disabled"
    if balance <= 0 or candidate_size_usdt <= 0:
        return True, "lane budget skipped"

    lane = _s.get_engine_lane(engine_name)
    mode = _mode(balance, capital_profile)
    used_usdt = 0.0
    for trade in _pr._registry.values():
        engine = str(trade.get("engine", "") or "")
        if _s.get_engine_lane(engine) != lane:
            continue
        used_usdt += float(trade.get("size_usdt", 0.0) or 0.0)

    used_pct = (used_usdt / balance) * 100.0
    projected_pct = ((used_usdt + candidate_size_usdt) / balance) * 100.0
    budget_pct = get_lane_budget_pct(
        lane,
        balance,
        candidate_size_usdt=candidate_size_usdt,
        capital_profile=capital_profile,
        soft_accept=soft_accept,
        session=session,
    )

    lvl = str(debug_override_level or "normal").strip().lower()
    if lvl not in {"normal", "relaxed", "unlimited"}:
        lvl = "normal"
    if lvl != "normal":
        if lvl == "relaxed":
            budget_pct = min(budget_pct * 1.25, 95.0)
        else:  # unlimited (bounded by other safety gates)
            budget_pct = 99.0
    if projected_pct > budget_pct + 0.01:
        remaining_usdt = max(balance * budget_pct / 100.0 - used_usdt, 0.0)
        return (
            False,
            f"{lane_label(lane)} lane {projected_pct:.1f}% > {budget_pct:.1f}% budget in {mode} "
            f"({used_pct:.1f}% used) | required=${candidate_size_usdt:.2f} available=${remaining_usdt:.2f} "
            f"| suggested=raise {lane_label(lane)} lane budget or shrink candidate size",
        )
    return True, "ok"

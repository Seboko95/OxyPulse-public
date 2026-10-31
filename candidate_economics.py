"""Pure, size-aware economics and inverse-capital calculations for candidates."""
from __future__ import annotations

import math
from dataclasses import asdict, dataclass
from typing import Any, Callable

from profitability_control_plane import evaluate_candidate


def _ceil_step(value: float, step: float) -> float:
    if step <= 0:
        return value
    return math.ceil((value / step) - 1e-12) * step


def resolve_exit_management_mode(
    *,
    full_exit_viable: bool,
    quantity_partial_viable: bool,
    final_notional_usdt: float,
    partial_exit_floor_usdt: float = 0.0,
) -> tuple[bool, str, str]:
    """Resolve partial exits without rejecting an otherwise viable full close."""
    if not full_exit_viable:
        return False, "full_position_only", "full_exit_not_viable"
    if not quantity_partial_viable:
        return False, "full_position_only", "partial_contract_quantity_not_viable"
    floor = max(float(partial_exit_floor_usdt or 0.0), 0.0)
    if floor and float(final_notional_usdt or 0.0) + 1e-9 < floor:
        return False, "full_position_only", "partial_exit_economic_floor"
    return True, "partial", "partial_exit_viable"


@dataclass(frozen=True)
class FloorDecomposition:
    exchange_min_notional_usdt: float
    configured_candidate_floor_usdt: float
    full_position_floor_usdt: float
    partial_exit_floor_usdt: float
    partial_exit_contracts: float
    contract_notional_usdt: float
    entry_fee_rate: float
    exit_fee_rate: float
    spread_pct: float
    slippage_pct: float
    funding_pct: float
    minimum_net_profit_usdt: float
    minimum_profit_cost_multiple: float
    exit_legs: int
    partial_exits_viable: bool
    binding_constraint: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def decompose_floor(
    *,
    instrument: Any,
    price: float,
    configured_candidate_floor_usdt: float,
    fee_rate: float,
    target_move_pct: float,
    spread_pct: float = 0.0,
    slippage_pct: float = 0.0,
    funding_pct: float = 0.0,
    minimum_net_profit_usdt: float = 0.0,
    minimum_profit_cost_multiple: float = 1.0,
    exit_legs: int = 3,
) -> FloorDecomposition:
    contract_notional = float(instrument.contract_notional(price))
    min_contracts = max(float(instrument.min_sz), float(instrument.lot_sz))
    exchange_floor = min_contracts * contract_notional
    full_floor = max(exchange_floor, float(configured_candidate_floor_usdt or 0.0))
    leg_contracts = _ceil_step(min_contracts, float(instrument.lot_sz))
    partial_contracts = _ceil_step(leg_contracts * max(int(exit_legs), 1), float(instrument.lot_sz))
    quantity_partial_floor = partial_contracts * contract_notional

    target_fraction = max(float(target_move_pct or 0.0), 0.0) / 100.0
    variable_cost_fraction = (
        abs(float(fee_rate or 0.0)) * 2.0
        + max(float(spread_pct or 0.0), 0.0) / 100.0
        + max(float(slippage_pct or 0.0), 0.0) / 100.0
        + max(float(funding_pct or 0.0), 0.0) / 100.0
    )
    required_cost_fraction = variable_cost_fraction * max(float(minimum_profit_cost_multiple or 0.0), 0.0)
    net_fraction = target_fraction - variable_cost_fraction - required_cost_fraction
    profit_floor = 0.0
    if minimum_net_profit_usdt > 0.0:
        profit_floor = math.inf if net_fraction <= 0.0 else float(minimum_net_profit_usdt) / net_fraction
    partial_floor = max(full_floor, quantity_partial_floor, profit_floor)
    partial_viable = math.isfinite(partial_floor)

    components = {
        "exchange_min_notional": exchange_floor,
        "configured_candidate_floor": float(configured_candidate_floor_usdt or 0.0),
        "partial_exit_contract_quantity": quantity_partial_floor,
        "minimum_net_profit": profit_floor,
    }
    binding = max(components, key=lambda key: components[key]) if partial_viable else "non_positive_unit_economics"
    return FloorDecomposition(
        exchange_min_notional_usdt=round(exchange_floor, 8),
        configured_candidate_floor_usdt=round(float(configured_candidate_floor_usdt or 0.0), 8),
        full_position_floor_usdt=round(full_floor, 8),
        partial_exit_floor_usdt=round(partial_floor, 8) if partial_viable else math.inf,
        partial_exit_contracts=round(partial_contracts, 8),
        contract_notional_usdt=round(contract_notional, 8),
        entry_fee_rate=abs(float(fee_rate or 0.0)),
        exit_fee_rate=abs(float(fee_rate or 0.0)),
        spread_pct=float(spread_pct or 0.0),
        slippage_pct=float(slippage_pct or 0.0),
        funding_pct=float(funding_pct or 0.0),
        minimum_net_profit_usdt=float(minimum_net_profit_usdt or 0.0),
        minimum_profit_cost_multiple=float(minimum_profit_cost_multiple or 0.0),
        exit_legs=max(int(exit_legs), 1),
        partial_exits_viable=partial_viable,
        binding_constraint=binding,
    )


def evaluate_final_size(
    *,
    desired_size_usdt: float,
    final_size_usdt: float,
    target_move_pct: float,
    stop_move_pct: float,
    fee_rate: float,
    spread_pct: float,
    slippage_pct: float,
    funding_pct: float = 0.0,
    impact_pct: float = 0.0,
    engine: str = "",
    direction: str = "",
    regime: str = "",
    contributors=(),
    history=(),
    min_cost_multiple: float = 2.0,
    instrument: Any | None = None,
    entry_price: float = 0.0,
    exit_legs: int = 3,
) -> dict[str, Any]:
    common = dict(
        target_move_pct=target_move_pct,
        stop_move_pct=stop_move_pct,
        fee_rate=fee_rate,
        spread_pct=spread_pct,
        slippage_pct=slippage_pct,
        funding_pct=funding_pct,
        impact_pct=impact_pct,
        engine=engine,
        direction=direction,
        regime=regime,
        contributors=contributors,
        history=history,
        min_cost_multiple=min_cost_multiple,
    )
    desired = evaluate_candidate(size_usdt=desired_size_usdt, **common)
    final = evaluate_candidate(size_usdt=final_size_usdt, **common)
    contracts = 0.0
    partial_exits_viable = True
    full_exit_viable = final_size_usdt > 0.0
    if instrument is not None and entry_price > 0.0:
        contracts = instrument.notional_to_contracts(final_size_usdt, entry_price)
        full_exit_viable = instrument.quantity_feasible(contracts, "market")[0]
        leg = instrument.round_contracts_down(contracts / max(int(exit_legs), 1))
        partial_exits_viable = full_exit_viable and instrument.quantity_feasible(leg, "market")[0]
    positive_final_net = float(final.expected_net_usdt) > 0.0
    allowed = bool(final.allowed and full_exit_viable and positive_final_net)
    reason = final.reason
    if not full_exit_viable:
        reason = "final_quantity_not_exit_feasible"
    elif not positive_final_net:
        reason = "non_positive_final_net_expectancy"
    return {
        "desired_size_usdt": round(float(desired_size_usdt or 0.0), 8),
        "final_size_usdt": round(float(final_size_usdt or 0.0), 8),
        "desired": desired.to_dict(),
        "final": final.to_dict(),
        "fees_recomputed": True,
        "slippage_recomputed": True,
        "expectancy_recomputed": True,
        "exit_plan_recomputed": True,
        "approved_contracts": round(float(contracts or 0.0), 8),
        "full_exit_viable": bool(full_exit_viable),
        "partial_exits_viable": bool(partial_exits_viable),
        "exit_management_mode": "partial" if partial_exits_viable else "full_position_only",
        "positive_final_net": positive_final_net,
        "allowed": allowed,
        "reason": reason,
    }


def solve_minimum_equity(
    evaluator: Callable[[float], dict[str, Any]],
    *,
    lower: float = 0.01,
    upper: float = 10_000.0,
    tolerance: float = 0.0001,
) -> dict[str, Any]:
    """Find the first viable equity using monotone binary search plus a final scan."""
    lo = max(float(lower), 0.0)
    hi = max(float(upper), lo + tolerance)
    high_result = evaluator(hi)
    if not bool(high_result.get("viable")):
        return {"solved": False, "minimum_equity_usdt": math.inf, "binding_constraint": high_result.get("binding_constraint", "not_viable_at_upper_bound")}
    for _ in range(80):
        if hi - lo <= tolerance:
            break
        mid = (lo + hi) / 2.0
        if bool(evaluator(mid).get("viable")):
            hi = mid
        else:
            lo = mid
    result = evaluator(hi)
    return {
        "solved": True,
        "minimum_equity_usdt": round(hi, 6),
        "minimum_available_margin_usdt": round(float(result.get("minimum_available_margin_usdt", 0.0) or 0.0), 6),
        "minimum_safe_funded_notional_usdt": round(float(result.get("final_size_usdt", 0.0) or 0.0), 6),
        "minimum_executable_contract_quantity": round(float(result.get("contracts", 0.0) or 0.0), 8),
        "minimum_economically_valid_notional_usdt": round(float(result.get("economic_floor_usdt", 0.0) or 0.0), 6),
        "binding_constraint": str(result.get("binding_constraint", "none")),
    }

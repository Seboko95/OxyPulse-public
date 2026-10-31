"""Pure capital-efficiency diagnostics for a sized trading candidate."""
from __future__ import annotations

from typing import Any


def _ratio(numerator: float, denominator: float) -> float | None:
    if denominator <= 0.0:
        return None
    return round(float(numerator) / float(denominator), 8)


def build_snapshot(
    *,
    equity_usdt: float,
    desired_notional_usdt: float,
    final_notional_usdt: float,
    stop_dist_pct: float,
    leverage: int,
    expected_gross_usdt: float = 0.0,
    expected_cost_usdt: float = 0.0,
    expected_net_usdt: float = 0.0,
    binding_constraint: str = "",
    exit_management_mode: str = "",
    position_cap_pct_equity: float = 0.0,
) -> dict[str, Any]:
    """Return dimensionally explicit candidate economics without mutating state."""
    equity = max(float(equity_usdt or 0.0), 0.0)
    desired = max(float(desired_notional_usdt or 0.0), 0.0)
    final = max(float(final_notional_usdt or 0.0), 0.0)
    stop_fraction = max(float(stop_dist_pct or 0.0), 0.0) / 100.0
    lev = max(int(leverage or 1), 1)
    margin = final / lev
    risk = final * stop_fraction
    gross = float(expected_gross_usdt or 0.0)
    costs = max(float(expected_cost_usdt or 0.0), 0.0)
    net = float(expected_net_usdt or 0.0)
    cap_pct = max(float(position_cap_pct_equity or 0.0), 0.0)
    cap_min_equity = final / (cap_pct / 100.0) if final > 0 and cap_pct > 0 else None
    return {
        "equity_usdt": round(equity, 8),
        "desired_notional_usdt": round(desired, 8),
        "position_notional_usdt": round(final, 8),
        "isolated_margin_required_usdt": round(margin, 8),
        "risk_at_stop_usdt": round(risk, 8),
        "risk_pct_equity": round(risk / equity * 100.0, 8) if equity > 0 else None,
        "margin_pct_equity": round(margin / equity * 100.0, 8) if equity > 0 else None,
        "notional_pct_equity": round(final / equity * 100.0, 8) if equity > 0 else None,
        "position_cap_pct_equity": round(cap_pct, 8),
        "position_cap_basis": "notional_pct_equity",
        "minimum_equity_for_position_cap_usdt": round(cap_min_equity, 8) if cap_min_equity is not None else None,
        "expected_gross_usdt": round(gross, 8),
        "expected_cost_usdt": round(costs, 8),
        "expected_net_usdt": round(net, 8),
        "expected_net_edge_bps": round(net / final * 10_000.0, 8) if final > 0 else None,
        "cost_to_gross_edge_ratio": _ratio(costs, gross),
        "cost_to_expected_profit_ratio": _ratio(costs, net),
        "cost_to_risk_ratio": _ratio(costs, risk),
        "expected_net_return_on_risk": _ratio(net, risk),
        "expected_net_return_on_margin": _ratio(net, margin),
        "binding_constraint": str(binding_constraint or "none"),
        "exit_management_mode": str(exit_management_mode or "unspecified"),
    }

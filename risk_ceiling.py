"""
risk_ceiling.py

Small, testable helper for the dynamic realized-risk ceiling used to reduce
oversized SL realizations without starving trade flow.
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class RiskCeilingResult:
    size_usdt: float
    stop_loss: float
    meta: dict


def apply_realized_risk_ceiling(
    *,
    entry_px: float,
    stop_px: float,
    size_usdt: float,
    balance_usdt: float,
    min_exec_usdt: float,
    side: str,
    settings,
) -> RiskCeilingResult:
    """
    Enforce a USDT risk cap on the expected loss at the current stop distance.

    Prefers reducing size. If required size would fall below executable minimum,
    can optionally tighten SL (never widens) to satisfy the cap while keeping
    size executable.
    """
    if not bool(settings.get_val("realized_risk_ceiling_enabled", True)):
        return RiskCeilingResult(float(size_usdt or 0.0), float(stop_px or 0.0), {})

    entry = float(entry_px or 0.0)
    stop = float(stop_px or 0.0)
    size = float(size_usdt or 0.0)
    bal = float(balance_usdt or 0.0)
    min_exec = float(min_exec_usdt or 0.0)
    if entry <= 0 or stop <= 0 or size <= 0:
        return RiskCeilingResult(size, stop, {})

    sl_dist_pct = abs((stop - entry) / entry) * 100.0
    if sl_dist_pct <= 0:
        return RiskCeilingResult(size, stop, {})

    cap_pct = float(settings.get_val("realized_risk_cap_pct_balance", 1.0) or 1.0)
    cap = max(bal * cap_pct / 100.0, 0.0)
    cap = max(cap, float(settings.get_val("realized_risk_cap_min_usdt", 1.0) or 1.0))
    cap = min(cap, float(settings.get_val("realized_risk_cap_max_usdt", 3.0) or 3.0))

    expected_loss = size * (sl_dist_pct / 100.0)
    action = "none"
    size_before = size
    stop_before = stop

    if expected_loss > cap + 1e-9:
        required_size = (cap * 100.0 / sl_dist_pct) if sl_dist_pct > 0 else size
        if min_exec <= 0 or required_size >= min_exec:
            size = max(required_size, min_exec) if min_exec > 0 else required_size
            action = "size_reduced"
        elif bool(settings.get_val("realized_risk_ceiling_tighten_sl_if_needed", True)):
            allowed_pct = max((cap / size) * 100.0, 0.0)
            min_sl_pct = float(settings.get_val("min_sl_dist_pct", 1.0) or 1.0)
            if allowed_pct >= min_sl_pct:
                if str(side or "").lower() == "buy":
                    stop = round(entry * (1.0 - allowed_pct / 100.0), 8)
                else:
                    stop = round(entry * (1.0 + allowed_pct / 100.0), 8)
                action = "sl_tightened"

    meta = {
        "expected_risk_usdt": round(float(expected_loss), 6),
        "risk_cap_usdt": round(float(cap), 6),
        "sl_dist_pct": round(float(sl_dist_pct), 4),
        "risk_ceiling_action": action,
        "size_before": round(float(size_before), 4),
        "size_after": round(float(size), 4),
        "sl_before": round(float(stop_before), 8),
        "sl_after": round(float(stop), 8),
    }
    return RiskCeilingResult(size, stop, meta)


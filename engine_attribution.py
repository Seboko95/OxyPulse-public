"""Non-double-counting engine contribution metadata."""
from __future__ import annotations

from typing import Any, Iterable


def build_attribution(
    *,
    primary_engine: str,
    confirming_engines: Iterable[str] = (),
    role_contributions: dict[str, Any] | None = None,
    baseline_probability: float | None = None,
    final_probability: float | None = None,
    baseline_uncertainty: float | None = None,
    final_uncertainty: float | None = None,
    ranking_delta: float = 0.0,
    size_multiplier: float = 1.0,
) -> dict[str, Any]:
    primary = str(primary_engine or "UNKNOWN").upper()
    roles = dict(role_contributions or {})
    names = []
    for value in confirming_engines:
        name = str(value or "").upper()
        if name and name != primary and name not in names:
            names.append(name)
    for value in roles:
        name = str(value or "").upper()
        if name and name != primary and name not in names:
            names.append(name)
    p_delta = None if baseline_probability is None or final_probability is None else float(final_probability) - float(baseline_probability)
    u_delta = None if baseline_uncertainty is None or final_uncertainty is None else float(baseline_uncertainty) - float(final_uncertainty)
    metadata = []
    for name in names:
        role_row = roles.get(name, roles.get(name.lower(), {}))
        role = str(role_row.get("role", "contributor") if isinstance(role_row, dict) else "contributor")
        metadata.append({
            "engine": name,
            "role": role,
            "contribution_type": "confirmation",
            "probability_improvement": p_delta,
            "uncertainty_reduction": u_delta,
            "ranking_improvement": float(ranking_delta or 0.0),
            "size_multiplier_effect": float(size_multiplier or 1.0),
            "attributed_pnl_usdt": 0.0,
        })
    return {
        "primary_engine": primary,
        "contributing_engines": names,
        "contributing_roles": [row["role"] for row in metadata],
        "contribution_type": "primary_plus_confirmations" if names else "primary_only",
        "contribution_metadata": metadata,
    }


def close_attribution(candidate: dict[str, Any], pnl_usdt: float) -> dict[str, Any]:
    result = dict(candidate or {})
    result.setdefault("primary_engine", str(result.get("engine", "UNKNOWN") or "UNKNOWN").upper())
    result.setdefault("contributing_engines", list(result.get("confirming_engines", []) or []))
    result.setdefault("contribution_metadata", [])
    result["candidate_total_pnl_usdt"] = float(pnl_usdt or 0.0)
    result["primary_engine_pnl_usdt"] = float(pnl_usdt or 0.0)
    result["contributor_pnl_total_usdt"] = 0.0
    return result

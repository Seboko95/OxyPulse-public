"""Calibrated, cost-aware candidate economics for OxyPulse.

The control plane deliberately keeps probability estimation separate from raw
engine confidence. Sparse observations are shrunk toward conservative priors;
provisional and malformed records are ignored.
"""
from __future__ import annotations

import json
import math
import os
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Iterable


OUTCOMES = ("tp1", "tp2", "trailing", "stop", "invalidation", "expiry")
DEFAULT_PRIORS = {
    "tp1": 0.16,
    "tp2": 0.12,
    "trailing": 0.18,
    "stop": 0.34,
    "invalidation": 0.10,
    "expiry": 0.10,
}
_HISTORY_CACHE: dict[str, Any] = {"path": "", "mtime": None, "rows": []}


def _f(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _classify(row: dict[str, Any]) -> str:
    reason = str(row.get("exit_reason", "") or "").lower()
    if "tp2" in reason:
        return "tp2"
    if "tp1" in reason:
        return "tp1"
    if "trail" in reason or "profit_lock" in reason:
        return "trailing"
    if "sl" in reason or "stop" in reason:
        return "stop"
    if "invalid" in reason or "setup_failure" in reason:
        return "invalidation"
    return "expiry"


@dataclass(frozen=True)
class ProbabilityEstimate:
    probabilities: dict[str, float]
    sample_size: int
    effective_sample_size: float
    uncertainty: float
    source: str
    live_sample_size: int = 0
    paper_sample_size: int = 0
    unknown_sample_size: int = 0
    prior_strength: float = 24.0
    conditioning: tuple[str, ...] = ("engine", "direction", "regime")

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class EconomicDecision:
    allowed: bool
    action: str
    reason: str
    expected_gross_usdt: float
    expected_net_usdt: float
    required_net_usdt: float
    estimated_cost_usdt: float
    safety_margin_usdt: float
    size_multiplier: float
    diversity_factor: float
    size_usdt: float
    risk_at_stop_usdt: float
    probability: ProbabilityEstimate
    costs: dict[str, float]
    classification: str
    evidence_quality: str
    expected_upside_usdt: float
    expected_downside_usdt: float
    model_uncertainty_usdt: float
    execution_uncertainty_usdt: float
    outcome_decomposition: dict[str, dict[str, float]]
    execution_route: str

    def to_dict(self) -> dict[str, Any]:
        row = asdict(self)
        row["probability"] = self.probability.to_dict()
        size = float(self.size_usdt or 0.0)
        risk = float(self.risk_at_stop_usdt or 0.0)
        gross = float(self.expected_gross_usdt or 0.0)
        net = float(self.expected_net_usdt or 0.0)
        cost = float(self.estimated_cost_usdt or 0.0)
        row.update(
            {
                "expected_net_edge_bps": round(net / size * 10_000.0, 8) if size > 0 else None,
                "cost_to_gross_edge_ratio": round(cost / gross, 8) if gross > 0 else None,
                "cost_to_expected_profit_ratio": round(cost / net, 8) if net > 0 else None,
                "cost_to_risk_ratio": round(cost / risk, 8) if risk > 0 else None,
                "expected_net_return_on_risk": round(net / risk, 8) if risk > 0 else None,
            }
        )
        return row


def load_closed_history(path: Path | str = "data/category_stats.json") -> list[dict[str, Any]]:
    source_path = Path(path)
    try:
        mtime = source_path.stat().st_mtime_ns
    except OSError:
        mtime = None
    if _HISTORY_CACHE["path"] == str(source_path) and _HISTORY_CACHE["mtime"] == mtime:
        return list(_HISTORY_CACHE["rows"])
    try:
        payload = json.loads(source_path.read_text(encoding="utf-8"))
    except Exception:
        return []
    rows: list[dict[str, Any]] = []
    source = payload.get("engines", payload)
    if isinstance(source, dict):
        for engine, bucket in source.items():
            trades = bucket.get("trades", []) if isinstance(bucket, dict) else []
            for trade in trades:
                if isinstance(trade, dict) and trade.get("closed_at"):
                    rows.append({"engine": engine, **trade})
    _HISTORY_CACHE.update({"path": str(source_path), "mtime": mtime, "rows": list(rows)})
    return rows


def calibrate_probabilities(
    *,
    engine: str,
    direction: str,
    regime: str = "",
    history: Iterable[dict[str, Any]] = (),
    prior_strength: float = 24.0,
    production_calibration: bool = False,
) -> ProbabilityEstimate:
    engine_u = str(engine or "").upper()
    direction_n = str(direction or "").lower()
    regime_u = str(regime or "").upper()
    weighted_counts = {name: 0.0 for name in OUTCOMES}
    matched = 0
    effective = 0.0
    live_count = 0
    paper_count = 0
    unknown_count = 0
    for row in history:
        if not isinstance(row, dict) or not row.get("closed_at"):
            continue
        environment = str(row.get("environment", row.get("trading_mode", row.get("mode", ""))) or "").lower()
        exchange_final = row.get("exchange_final") is True or str(row.get("pnl_status", "")).upper() == "EXCHANGE_FINAL"
        eligible = row.get("eligible_for_production_learning") is True or (environment == "live" and exchange_final)
        if environment in {"paper", "simulation", "simulated", "backtest"}:
            paper_count += 1
        elif eligible or environment == "live":
            live_count += 1
        else:
            unknown_count += 1
        if production_calibration and not eligible:
            continue
        row_engine = str(row.get("engine", "") or "").upper()
        row_direction = str(row.get("direction", row.get("side", "")) or "").lower()
        row_regime = str(row.get("regime_at_entry", row.get("regime", "")) or "").upper()
        weight = 0.20
        if row_engine == engine_u:
            weight += 0.45
        if row_direction == direction_n:
            weight += 0.20
        if regime_u and row_regime == regime_u:
            weight += 0.15
        weighted_counts[_classify(row)] += weight
        effective += weight
        matched += 1
    strength = max(float(prior_strength or 0.0), 1.0)
    denom = strength + effective
    probs = {
        name: ((DEFAULT_PRIORS[name] * strength) + weighted_counts[name]) / denom
        for name in OUTCOMES
    }
    total = sum(probs.values()) or 1.0
    probs = {name: value / total for name, value in probs.items()}
    max_variance = max((p * (1.0 - p)) / (denom + 1.0) for p in probs.values())
    uncertainty = min(max(math.sqrt(max_variance) * 2.0, 0.03), 0.35)
    return ProbabilityEstimate(
        probabilities={k: round(v, 8) for k, v in probs.items()},
        sample_size=matched,
        effective_sample_size=round(effective, 4),
        uncertainty=round(uncertainty, 6),
        source="shrunk_closed_trade_history" if matched else "conservative_prior",
        live_sample_size=live_count,
        paper_sample_size=paper_count,
        unknown_sample_size=unknown_count,
        prior_strength=round(strength, 4),
    )


def classify_expectancy(
    *,
    expected_gross_usdt: float,
    expected_net_usdt: float,
    required_net_usdt: float,
    model_uncertainty_usdt: float,
) -> str:
    """Classify where candidate economics fail without changing admission policy."""
    gross = float(expected_gross_usdt or 0.0)
    net = float(expected_net_usdt or 0.0)
    if gross <= 0.0:
        return "NEGATIVE_GROSS_EDGE"
    if net <= 0.0:
        return "POSITIVE_GROSS_EDGE_COST_DESTROYED"
    if net < float(required_net_usdt or 0.0) and model_uncertainty_usdt > 0.0:
        return "POSITIVE_GROSS_EDGE_UNCERTAINTY_DESTROYED"
    return "POSITIVE_EXPECTANCY"


def diversity_adjustment(
    contributors: Iterable[str] = (),
    feature_families: dict[str, str] | None = None,
) -> float:
    names = [str(name or "").upper() for name in contributors if str(name or "").strip()]
    if not names:
        return 1.0
    families = feature_families or {}
    distinct = {str(families.get(name, name)).lower() for name in names}
    raw_count = len(set(names))
    independent_count = len(distinct)
    redundancy = max(raw_count - independent_count, 0)
    return round(max(0.70, min(1.15, 1.0 + 0.05 * (independent_count - 1) - 0.08 * redundancy)), 4)


def evaluate_candidate(
    *,
    size_usdt: float,
    target_move_pct: float,
    stop_move_pct: float,
    fee_rate: float = 0.0,
    entry_fee_rate: float | None = None,
    exit_fee_rate: float | None = None,
    spread_pct: float = 0.0,
    slippage_pct: float = 0.0,
    entry_slippage_pct: float | None = None,
    exit_slippage_pct: float | None = None,
    funding_pct: float = 0.0,
    impact_pct: float = 0.0,
    engine: str = "",
    direction: str = "",
    regime: str = "",
    contributors: Iterable[str] = (),
    feature_families: dict[str, str] | None = None,
    history: Iterable[dict[str, Any]] = (),
    min_cost_multiple: float = 2.0,
    soft_size_multiplier: float = 0.75,
    execution_route: str = "taker_taker",
    execution_uncertainty_pct: float = 0.0,
    funding_applicable: bool = True,
    production_calibration: bool | None = None,
) -> EconomicDecision:
    size = max(_f(size_usdt), 0.0)
    if production_calibration is None:
        production_calibration = os.getenv("PAPER_TRADING", "true").strip().lower() == "false"
    estimate = calibrate_probabilities(
        engine=engine,
        direction=direction,
        regime=regime,
        history=history,
        production_calibration=bool(production_calibration),
    )
    diversity = diversity_adjustment(contributors, feature_families)
    p = dict(estimate.probabilities)
    positive = p["tp1"] + p["tp2"] + p["trailing"]
    adjusted_positive = min(max(positive * diversity, 0.0), 0.90)
    if positive > 0.0:
        scale = adjusted_positive / positive
        for name in ("tp1", "tp2", "trailing"):
            p[name] *= scale
        negative_total = p["stop"] + p["invalidation"] + p["expiry"]
        remaining = 1.0 - adjusted_positive
        if negative_total > 0.0:
            for name in ("stop", "invalidation", "expiry"):
                p[name] = p[name] / negative_total * remaining

    target = max(_f(target_move_pct), 0.0) / 100.0
    stop = max(_f(stop_move_pct), 0.0) / 100.0
    outcome_returns = {
        "tp1": target * 0.50,
        "tp2": target,
        "trailing": target * 0.65,
        "stop": -stop,
        "invalidation": -stop * 0.50,
        "expiry": -stop * 0.10,
    }
    outcome_decomposition: dict[str, dict[str, float]] = {}
    for name in OUTCOMES:
        pnl = size * outcome_returns[name]
        outcome_decomposition[name] = {
            "probability": round(p[name], 8),
            "return_pct": round(outcome_returns[name] * 100.0, 8),
            "outcome_pnl_usdt": round(pnl, 8),
            "expected_contribution_usdt": round(p[name] * pnl, 8),
        }
    upside = sum(v["expected_contribution_usdt"] for k, v in outcome_decomposition.items() if k in {"tp1", "tp2", "trailing"})
    downside = -sum(v["expected_contribution_usdt"] for k, v in outcome_decomposition.items() if k in {"stop", "invalidation", "expiry"})
    gross = upside - downside
    entry_rate = max(_f(entry_fee_rate, _f(fee_rate)), 0.0)
    exit_rate = max(_f(exit_fee_rate, _f(fee_rate)), 0.0)
    entry_slip = max(_f(entry_slippage_pct, _f(slippage_pct)), 0.0)
    # Legacy slippage_pct represented the complete route, not a per-leg value.
    exit_slip = max(_f(exit_slippage_pct, 0.0), 0.0)
    costs = {
        "entry_fee": size * entry_rate,
        "exit_fee": size * exit_rate,
        "spread": size * max(_f(spread_pct), 0.0) / 100.0,
        "entry_slippage": size * entry_slip / 100.0,
        "exit_slippage": size * exit_slip / 100.0,
        "funding": size * max(_f(funding_pct), 0.0) / 100.0 if funding_applicable else 0.0,
        "market_impact": size * max(_f(impact_pct), 0.0) / 100.0,
    }
    costs["total"] = sum(costs.values())
    model_uncertainty = size * max(target, stop) * estimate.uncertainty
    execution_uncertainty = size * max(_f(execution_uncertainty_pct), 0.0) / 100.0
    safety = model_uncertainty + execution_uncertainty
    net = gross - costs["total"]
    required = costs["total"] * max(_f(min_cost_multiple, 2.0), 0.0) + safety
    classification = classify_expectancy(
        expected_gross_usdt=gross,
        expected_net_usdt=net,
        required_net_usdt=required,
        model_uncertainty_usdt=model_uncertainty,
    )
    if size <= 0.0:
        action, reason, allowed, multiplier = "shadow", "no_executable_size", False, 0.0
    elif net >= required:
        action, reason, allowed, multiplier = "accept", "positive_net_expectancy", True, 1.0
    elif net > 0.0:
        action, reason, allowed, multiplier = "reduce", "positive_but_below_safety_margin", True, max(min(_f(soft_size_multiplier, 0.75), 1.0), 0.05)
    elif estimate.effective_sample_size < 12.0:
        action, reason, allowed, multiplier = "reduce", "insufficient_calibration_sample", True, max(min(_f(soft_size_multiplier, 0.75), 0.75), 0.25)
    else:
        action, reason, allowed, multiplier = "queue", "non_positive_net_expectancy", False, 0.0
    return EconomicDecision(
        allowed=allowed,
        action=action,
        reason=reason,
        expected_gross_usdt=round(gross, 6),
        expected_net_usdt=round(net, 6),
        required_net_usdt=round(required, 6),
        estimated_cost_usdt=round(costs["total"], 6),
        safety_margin_usdt=round(safety, 6),
        size_multiplier=round(multiplier, 4),
        diversity_factor=diversity,
        size_usdt=round(size, 8),
        risk_at_stop_usdt=round(size * stop, 8),
        probability=estimate,
        costs={key: round(value, 6) for key, value in costs.items()},
        classification=classification,
        evidence_quality=("INSUFFICIENT_EVIDENCE" if estimate.effective_sample_size < 12.0 else "CALIBRATED"),
        expected_upside_usdt=round(upside, 6),
        expected_downside_usdt=round(downside, 6),
        model_uncertainty_usdt=round(model_uncertainty, 6),
        execution_uncertainty_usdt=round(execution_uncertainty, 6),
        outcome_decomposition=outcome_decomposition,
        execution_route=str(execution_route or "taker_taker"),
    )

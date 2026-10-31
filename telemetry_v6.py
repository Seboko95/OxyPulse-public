"""
OxyPulse V6 / V6.1 — telemetry_v6.py
Structured decision logging for V6 + V6.1 pipeline components.

V6.1 additions:
  - log_capital_profile: logs current mode + key parameters
  - log_exposure: logs net exposure state
  - log_market_filter: logs market quality gate outcome
  - log_frequency_check: logs trade frequency gate
  - Updated log_v6_summary to include capital mode

Format: [V6.Tel] {component} | {engine} {coin} | {json_payload}

Controlled by v6_telemetry_enabled (default ON — safe, no side effects).
"""
from __future__ import annotations

import json
import logging

import settings_store as _ss

logger = logging.getLogger("OxyPulse.V6.Telemetry")


def _enabled() -> bool:
    return bool(_ss.get_val("v6_telemetry_enabled", True))


def _j(data: dict) -> str:
    return json.dumps(data, separators=(",", ":"), default=str)


# ── V6 base ───────────────────────────────────────────────────────────────────

def log_confidence(engine: str, coin: str, result) -> None:
    if not _enabled():
        return
    payload = {
        "direction":    result.direction,
        "raw_votes":    result.raw_votes,
        "weight":       result.engine_weight,
        "score":        round(result.confidence_score, 2),
        "threshold":    result.min_threshold,
        "approved":     result.approved,
        # V6.1 factors
        "regime_mult":  getattr(result, "regime_mult",  1.0),
        "perf_mult":    getattr(result, "perf_mult",    1.0),
        "vol_adj":      getattr(result, "vol_adj",      1.0),
        "mtf_boost":    getattr(result, "mtf_boost",    0.0),
        "opp_penalty":  getattr(result, "opposition_penalty", 0.0),
    }
    level = logging.INFO if not result.approved else logging.DEBUG
    logger.log(level, "[V6.Tel] Confidence | %s %s | %s", engine, coin, _j(payload))


def log_sizing(engine: str, coin: str, result) -> None:
    if not _enabled():
        return
    payload = {
        "requested": result.requested_size_usdt,
        "final":     result.final_size_usdt,
        "skipped":   result.skipped,
        "modifiers": result.modifiers,
    }
    level = logging.INFO if result.skipped else logging.DEBUG
    logger.log(level, "[V6.Tel] Sizing | %s %s | %s", engine, coin, _j(payload))


def log_ai_gate(engine: str, coin: str, result) -> None:
    if not _enabled():
        return
    payload = {
        "decision":   result.decision,
        "size_mult":  result.size_multiplier,
        "reason":     result.reason,
        "fallback":   result.fallback_used,
        "timed_out":  result.timed_out,
    }
    level = logging.INFO if result.decision in ("reject",) else logging.DEBUG
    logger.log(level, "[V6.Tel] AIGate | %s %s | %s", engine, coin, _j(payload))


def log_drawdown(engine: str, state) -> None:
    if not _enabled() or state.level == 0:
        return
    payload = {
        "level":       state.level,
        "label":       state.label,
        "size_mult":   state.size_multiplier,
        "allow":       state.allow_new_trades,
        "daily_dd":    round(state.daily_dd_pct,  2),
        "weekly_dd":   round(state.weekly_dd_pct, 2),
        "consec":      state.consec_losses,
        "tolerance":   getattr(state, "tolerance_pct", "?"),
    }
    logger.info("[V6.Tel] Drawdown | %s | %s", engine, _j(payload))


def log_execution(engine: str, coin: str, plan) -> None:
    if not _enabled():
        return
    payload = {
        "mode":     plan.mode,
        "chunks":   len(plan.chunks),
        "fallback": plan.fallback_mode,
        "notes":    plan.notes,
    }
    logger.debug("[V6.Tel] Execution | %s %s | %s", engine, coin, _j(payload))


def log_weight_update(engine: str, old_weight: float, new_weight: float,
                      win_rate: float, num_trades: int) -> None:
    if not _enabled():
        return
    payload = {
        "old_weight": round(old_weight, 3),
        "new_weight": round(new_weight, 3),
        "win_rate":   round(win_rate, 3),
        "num_trades": num_trades,
    }
    logger.info("[V6.Tel] WeightUpdate | %s | %s", engine, _j(payload))


# ── V6.1 additions ────────────────────────────────────────────────────────────

def log_capital_profile(engine: str, profile) -> None:
    """Log the current capital profile for a trade decision."""
    if not _enabled():
        return
    payload = {
        "mode":          profile.mode,
        "balance":       round(profile.balance, 2),
        "size_mult":     profile.size_multiplier,
        "risk_mult":     profile.risk_multiplier,
        "conf_thresh":   profile.confidence_threshold,
        "max_trades_h":  profile.max_trades_per_hour,
        "dd_tolerance":  profile.drawdown_tolerance_pct,
        "aggression":    profile.aggressiveness_score,
    }
    logger.debug("[V6.Tel] CapitalProfile | %s | %s", engine, _j(payload))


def log_exposure(engine: str, coin: str, state, allowed: bool, reason: str) -> None:
    """Log exposure check result."""
    if not _enabled():
        return
    from exposure_tracker import exposure_summary
    payload = {**exposure_summary(state), "allowed": allowed, "reason": reason}
    level = logging.INFO if not allowed else logging.DEBUG
    logger.log(level, "[V6.Tel] Exposure | %s %s | %s", engine, coin, _j(payload))


def log_market_filter(engine: str, coin: str, result) -> None:
    """Log market quality filter outcome."""
    if not _enabled():
        return
    payload = {
        "allowed": result.allowed,
        "reason":  result.reason,
        "checks":  result.checks,
    }
    level = logging.INFO if not result.allowed else logging.DEBUG
    logger.log(level, "[V6.Tel] MarketFilter | %s %s | %s", engine, coin, _j(payload))


def log_frequency_check(engine: str, allowed: bool, reason: str) -> None:
    """Log trade frequency gate outcome."""
    if not _enabled() or allowed:
        return
    logger.info("[V6.Tel] FreqCheck | %s | allowed=%s reason=%s", engine, allowed, reason)


def log_v6_summary(
    engine: str,
    coin: str,
    direction: str,
    conf=None,
    sz=None,
    gate=None,
    dd=None,
    plan=None,
    final_action: str = "trade",
    capital_mode: str = "",
) -> None:
    """Consolidated single-line V6.1 pipeline summary."""
    if not _enabled():
        return
    payload: dict = {"direction": direction, "action": final_action}
    if capital_mode:
        payload["mode"] = capital_mode
    if conf:
        payload["conf"] = round(conf.confidence_score, 2)
        payload["conf_ok"] = conf.approved
    if sz:
        payload["size"] = sz.final_size_usdt
    if gate:
        payload["gate"] = gate.decision
        payload["gate_mult"] = gate.size_multiplier
    if dd:
        payload["dd_level"] = dd.level
        payload["dd_mult"] = dd.size_multiplier
    if plan:
        payload["exec"] = plan.mode
    logger.info("[V6.Tel] Summary | %s %s | %s", engine, coin, _j(payload))

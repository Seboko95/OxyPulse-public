"""
OxyPulse — exit_manager.py  v5
Single exit authority for all 12 category engines.

v4 fixes:
  1. Per-trade leverage in ExitPlan — PnL now calculated correctly for every
     trade, not from the global market_mode lever value.
  2. Exchange sync loop (_sync_with_exchange every 30s) — detects when OKX
     protection orders closed the position, records PnL, and cleans up
     internal state without placing a redundant close order.
  3. _execute_exit checks position still exists on exchange before closing —
     prevents the "position already closed" error and double-PnL recording.
  4. _notify_engine calls notify_closed_by_exchange() for algo-closed trades
     so category_tracker wins/losses are always updated.
  5. Leverage stored and used consistently through all close paths.
  6. Explicit per-position TP ownership prevents exchange full-TP and local
     staged TP/runner management from competing.
"""
from __future__ import annotations
import asyncio
import hashlib
import json
import logging
import os
import time
from datetime import datetime, timezone
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import settings_store as _s
import performance_tracker as _perf
import sync_health as _sync_health
import lifecycle_analytics as _la
import paper_validation as _paper_validation
import exit_profiles as _exit_profiles
from full_close_economics import (
    ECONOMIC_EVIDENCE_ESTIMATED_CONSERVATIVE,
    ECONOMIC_EVIDENCE_COMPLETE,
    ECONOMIC_EVIDENCE_INSUFFICIENT,
    OPTIONAL_PROFIT_CAPTURE,
    PositionLifecycleEconomics,
    build_position_lifecycle_economics,
    candidate_stop_is_economically_eligible,
    classify_full_close_reason,
    evaluate_optional_full_close,
    required_cost_relative_net,
)
from risk_manager import fee_cost_usdt
from market_data_stream import get_closes, get_volumes, latest_price
from notifier import (
    alert_take_profit,
    alert_stop_loss,
    alert_error,
    send_alert,
    alert_trade_closed,
)
from indicators import ema, macd
from protection_manager import (
    ProtectionManager,
    REPROTECTION_CONFIRMED,
    REPROTECTION_UNCHANGED,
    REPROTECTION_PENDING,
    REPROTECTION_UNKNOWN,
    REPROTECTION_FAILED,
    PROTECTION_SUBMIT_UNCERTAIN,
)
from tp_ownership import (
    LOCAL_STAGED,
    EXCHANGE_FULL,
    NO_PROFIT_TP,
    derive_tp_owner,
    normalize_persisted_owner,
    normalize_tp_mode,
    owner_flags,
    ownership_timestamp,
)

logger = logging.getLogger("OxyPulse.ExitManager")

# Partial-close lifecycle states.  A request/acknowledgement is not evidence
# that any contracts actually left the exchange position.
PARTIAL_REQUESTED = "PARTIAL_REQUESTED"
PARTIAL_SUBMISSION_ATTEMPTED = "PARTIAL_SUBMISSION_ATTEMPTED"
PARTIAL_ACKNOWLEDGED = "PARTIAL_ACKNOWLEDGED"
PARTIAL_PARTIALLY_FILLED = "PARTIAL_PARTIALLY_FILLED"
PARTIAL_FILLED = "PARTIAL_FILLED"
PARTIAL_UNCERTAIN = "PARTIAL_UNCERTAIN"
PARTIAL_REJECTED = "PARTIAL_REJECTED"
EXPOSURE_ACTIVE = "ACTIVE_POSITION"
EXPOSURE_UNKNOWN = "EXPOSURE_UNKNOWN"
EXCHANGE_CLOSED_AWAITING_RECONCILIATION = "EXCHANGE_CLOSED_AWAITING_RECONCILIATION"

_engine_registry: dict  = {}
_okx_client              = None
_monitor_task: Optional[asyncio.Task] = None
_sync_task:    Optional[asyncio.Task] = None
_protection_manager: Optional[ProtectionManager] = None

EXCHANGE_SYNC_INTERVAL = 30   # seconds between OKX position sync checks


def _resolve_position_lifecycle_id(
    coin: str,
    *,
    order_id: str = "",
    client_order_id: str = "",
    provided: str = "",
) -> str:
    """Resolve the plan identity from the same durable registry identity used by protection."""
    try:
        from position_registry import (
            derive_position_lifecycle_id,
            get_all_details,
            update as registry_update,
        )
        row = dict((get_all_details() or {}).get(str(coin or "").upper(), {}) or {})
        persisted = str(row.get("position_lifecycle_id", "") or "").strip()
        if persisted:
            return persisted
        identity = derive_position_lifecycle_id(
            row,
            order_id=order_id,
            client_order_id=client_order_id,
        )
        if identity:
            if row:
                try:
                    registry_update(
                        coin,
                        metadata={"position_lifecycle_id": identity},
                        require_persist=True,
                    )
                except Exception:
                    logger.exception("Could not persist derived position lifecycle identity for %s", coin)
            return identity
    except Exception:
        logger.debug("Position lifecycle registry lookup unavailable for %s", coin, exc_info=True)
    return str(provided or "").strip()

@dataclass
class ExitPlan:
    coin:            str
    direction:       str    # "long" | "short"
    engine_name:     str
    entry_price:     float
    size_usdt:       float   # current open notional
    atr_val:         float
    hard_sl:         float
    tp1_price:       float
    tp2_price:       float
    initial_size_usdt: float = 0.0
    leverage:        int   = 5      # ← per-trade leverage (v4 fix)
    requested_leverage: int = 0
    exchange_configured_leverage: int | None = None
    position_effective_leverage: int | None = None
    # Synthetic/paper plans may use their explicit plan leverage.  Live
    # restore paths must pass an explicit authoritative or unknown status.
    leverage_truth_status: str = "PAPER_LOCAL"
    leverage_truth_timestamp: str = ""
    leverage_truth_source: str = ""
    leverage_truth_mismatch: bool = False
    requested_vs_configured_mismatch: bool = False
    configured_vs_effective_mismatch: bool = False
    vote_score:      int   = 2
    created_at:      float = 0.0     # epoch timestamp when trade was opened
    tsl_armed:       bool  = False
    tsl_price:       float = 0.0
    peak_price:      float = 0.0
    sl_breach_time:  float = 0.0
    breakeven_moved:     bool  = False
    profit_lock_level:   int   = 0        # 0=none, 1=L1(15%→5%), 2=L2(25%→10%), 3=L3(40%→20%)
    close_notified:      bool  = False   # True once final/terminal close notification has been sent
    tp1_done:        bool  = False
    tp2_done:        bool  = False
    remaining_pct:   float = 100.0
    realized_pnl_usdt: float = 0.0
    last_protection_reason: str = ""
    desired_stop_price: float = 0.0
    local_effective_stop_price: float = 0.0
    exchange_confirmed_stop_price: float = 0.0
    exchange_protection_status: str = ""
    protection_intent: dict = field(default_factory=dict)
    protection_lifecycle: str = ""
    protection_cleanup_pending: bool = False
    exchange_protection_algo_ids: list[str] = field(default_factory=list)
    last_reprotection_attempt: str = ""
    last_reprotection_success: str = ""
    reprotection_reason: str = ""
    reprotection_error: str = ""
    reprotection_attempts: int = 0
    reprotection_evaluations: int = 0
    reprotection_read_recoveries: int = 0
    reprotection_transactions: int = 0
    reprotection_post_attempts: int = 0
    reprotection_confirmed: int = 0
    reprotection_rejected: int = 0
    reprotection_uncertain: int = 0
    last_reprotection_attempt_ts: float = 0.0
    order_id:        str   = ""
    entry_client_order_id: str = ""
    position_lifecycle_id: str = ""
    partial_mode:    bool  = True
    tp_owner:        str   = LOCAL_STAGED
    tp_mode_at_entry: str  = "reference_only"
    exchange_tp_algo_id: str = ""
    local_tp1_enabled: bool = True
    local_tp2_enabled: bool = True
    ownership_timestamp: str = ""
    tp_owner_timestamp: str = ""
    partials_enabled_at_entry: bool = True
    last_seen_on_exchange: str = ""
    last_successful_sync:  str = ""
    sync_status:           str = "UNCERTAIN"
    exchange_absence_confirmations: int = 0
    last_sync_error:       str = ""
    early_invalidation_breach_time: float = 0.0
    lifecycle_phase: str = "early_validation"
    mfe_roi: float = 0.0
    mae_roi: float = 0.0
    tp1_deferred: bool = False
    tp2_deferred: bool = False
    tp1_deferred_price: float = 0.0
    tp2_deferred_price: float = 0.0
    deferred_partial_pnl_usdt: float = 0.0
    runner_quality: float = 0.0
    runner_core_pct: float = 0.0
    exhaustion_score: float = 0.0
    exhaustion_reasons: str = ""
    economic_minimum: dict = field(default_factory=dict)
    runner_opportunity: dict = field(default_factory=dict)
    near_floor_tp_earned: bool = False
    near_floor_tp_ratio: float = 0.0
    near_floor_tp_grace_until: float = 0.0
    invalidation_diagnostics: dict = field(default_factory=dict)
    post_invalidation_telemetry: dict = field(default_factory=dict)
    macro_runner_trust_mult: float = 1.0
    expected_risk_usdt: float = 0.0
    risk_cap_usdt: float = 0.0
    risk_ceiling_action: str = ""
    paper_validation: dict = field(default_factory=dict)
    entry_order_ids: list[str] = field(default_factory=list)
    exit_order_ids: list[str] = field(default_factory=list)
    partial_exit_events: list[dict] = field(default_factory=list)
    partial_exit_state: dict = field(default_factory=dict)
    entry_contracts: float = 0.0
    confirmed_remaining_contracts: float = 0.0
    exchange_position_timestamp: str = ""
    exposure_truth_status: str = EXPOSURE_ACTIVE
    closure_handoff_at: str = ""
    partial_attribution_status: str = "NONE"
    partial_realized_pnl_usdt: float = 0.0
    partial_fees_usdt: float = 0.0
    entry_fee_usdt: float | None = None
    funding_usdt: float | None = None
    funding_status: str = "UNKNOWN"
    full_close_economics: dict = field(default_factory=dict)
    first_fill_at: str = ""
    last_entry_fill_at: str = ""
    first_exit_fill_at: str = ""
    last_exit_fill_at: str = ""
    exit_profile: dict = field(default_factory=dict)
    mfe_price: float = 0.0
    mae_price: float = 0.0
    mfe_gross_pnl_usdt: float = 0.0
    mae_gross_pnl_usdt: float = 0.0
    mfe_raw_move_pct: float = 0.0
    mae_raw_move_pct: float = 0.0
    mfe_observed_leverage: int = 0
    mae_observed_leverage: int = 0
    mfe_current_roi: float = 0.0
    mae_current_roi: float = 0.0
    exit_timeline: list[dict] = field(default_factory=list)
    trail_activated_at: str = ""
    trail_activation_net_pnl: float = 0.0
    trail_peak_net_pnl: float = 0.0
    trail_exit_net_pnl: float = 0.0
    profit_surrendered: float = 0.0


_plans: dict[str, ExitPlan] = {}
_partial_skip_log_state: dict[tuple[str, str], tuple[float, str]] = {}
_post_invalidation_watch: dict[str, dict] = {}
_novo_enrichment_outcome_seen: set[tuple[str, str, str, str, str]] = set()
_NOVO_MEMORY_PATH = Path("data/novo_trade_memory.jsonl")
_CONTINUATION_ENGINES = {
    "FUJIN",
    "HAYATE",
    "RYUJIN",
    "RAIJIN",
    "KAGUTSUCHI",
    "AMATERASU",
    "RYUU",
    "TENJIN",
}


def _paper_trading_enabled() -> bool:
    """Runtime paper-mode check used to keep exchange reconciliation out of paper runs."""
    env = str(os.getenv("PAPER_TRADING", "") or "").strip().lower()
    if env in {"true", "1", "yes", "on"}:
        return True
    try:
        import okx_client as _okx_mod
        return bool(getattr(_okx_mod, "PAPER_TRADING", False))
    except Exception:
        return False


def _opened_at_iso(plan: ExitPlan) -> str:
    try:
        ts = float(getattr(plan, "created_at", 0.0) or 0.0)
        if ts > 0:
            return datetime.fromtimestamp(ts, timezone.utc).isoformat()
    except Exception:
        pass
    return ""


def _paper_validation_close_context_from_plan(plan: ExitPlan, coin: str) -> dict:
    context = dict(getattr(plan, "paper_validation", {}) or {})
    if context:
        return context

    entry_px = float(getattr(plan, "entry_price", 0.0) or 0.0)
    stop_px = float(getattr(plan, "hard_sl", 0.0) or 0.0)
    sl_dist_pct = 0.0
    if entry_px > 0 and stop_px > 0:
        sl_dist_pct = abs(stop_px - entry_px) / entry_px * 100.0
    return {
        "entry_row_missing": True,
        "validation_excluded": True,
        "validation_exclusion_reason": "missing_entry_row_pre_current_validation",
        "admission_config_cohort": "legacy_pre_telemetry",
        "order_id": str(getattr(plan, "order_id", "") or ""),
        "entry_client_order_id": str(getattr(plan, "entry_client_order_id", "") or ""),
        "entry_clOrdId": str(getattr(plan, "entry_client_order_id", "") or ""),
        "engine": str(getattr(plan, "engine_name", "") or ""),
        "pair": str(coin or "").upper(),
        "side": "buy" if str(getattr(plan, "direction", "")).lower() == "long" else "sell",
        "entry_price": entry_px,
        "stop_price": stop_px,
        "sl_dist_pct": float(sl_dist_pct or 0.0),
        "risk_amount_usdt": float(getattr(plan, "expected_risk_usdt", 0.0) or 0.0),
        "raw_size_usdt": 0.0,
        "min_exec_usdt": 0.0,
        "cap_applied": False,
        "cap_source": "",
        "final_size_usdt": float(getattr(plan, "size_usdt", 0.0) or 0.0),
        "cap_at_admission": None,
        "current_rule_357_max_open_risk_pct": float(_s.get_val("rule_357_max_open_risk_pct", 0.0) or 0.0),
    }


def _record_exit_transition(plan: ExitPlan, event: str, **fields) -> None:
    """Persist only meaningful exit-state transitions, never market ticks."""
    event_name = str(event or "").upper()
    row = {"timestamp": datetime.now(timezone.utc).isoformat(), "event": event_name}
    row.update({str(key): value for key, value in fields.items()})
    prior = plan.exit_timeline[-1] if plan.exit_timeline else None
    if prior and prior.get("event") == event_name and all(prior.get(key) == value for key, value in fields.items()):
        return
    plan.exit_timeline.append(row)
    if len(plan.exit_timeline) > 100:
        del plan.exit_timeline[:-100]


def _neutral_novo_enrichment_suggestions() -> dict:
    return {
        "tp2_multiplier": 1.0,
        "trailing_activation_multiplier": 1.0,
        "break_even_delay_multiplier": 1.0,
        "profit_lock_multiplier": 1.0,
        "partial_min_notional_multiplier": 1.0,
        "execution_split_preference": "UNCHANGED",
        "size_multiplier": 1.0,
    }


def _registry_row_for_plan(coin: str) -> dict:
    try:
        from position_registry import get_all_details as _reg_details
        rows = _reg_details() or {}
        return dict(rows.get(str(coin or "").upper(), {}) or {})
    except Exception:
        return {}


def _local_mutation_authorized(coin: str) -> tuple[bool, str]:
    """Separate local admission visibility from order-mutation authority."""
    try:
        from position_registry import (
            exposure_status,
            EXPOSURE_UNKNOWN as _unknown,
            EXPOSURE_TERMINAL as _terminal,
            EXCHANGE_CLOSED_AWAITING_RECONCILIATION as _handoff,
        )
        row = _registry_row_for_plan(coin)
        if not row:
            return True, "no_local_registry_row_exchange_truth_required"
        # Real registry rows are normalized on load.  A fieldless dictionary
        # containing only protection-intent fields is not local exposure
        # evidence; legacy position rows still go through conservative
        # exposure classification.
        if "exposure_truth_status" not in row:
            return True, "no_local_exposure_classification"
        status = exposure_status(row)
        if status == _unknown:
            return False, "LOCAL_EXPOSURE_UNKNOWN"
        if status in {_terminal, _handoff}:
            return False, status
    except Exception:
        return False, "LOCAL_EXPOSURE_CLASSIFICATION_UNAVAILABLE"
    return True, "ACTIVE_POSITION"


def _confidence_stage_trace_for_plan(coin: str, plan: ExitPlan) -> dict:
    try:
        row = _registry_row_for_plan(coin)
        trace = row.get("confidence_stage_trace", {})
        if isinstance(trace, dict):
            return dict(trace)
    except Exception:
        pass
    return {}


def _latest_novo_candidate_for_plan(coin: str, plan: ExitPlan, limit: int = 500) -> dict:
    """Best-effort read-only fallback for future outcome attribution."""
    try:
        if not _NOVO_MEMORY_PATH.exists():
            return {}
        symbol = str(coin or "").upper()
        side = str(getattr(plan, "direction", "") or "").upper()
        engine = str(getattr(plan, "engine_name", "") or "")
        lines = _NOVO_MEMORY_PATH.read_text(encoding="utf-8", errors="ignore").splitlines()
        for line in reversed(lines[-max(int(limit or 500), 1):]):
            try:
                row = json.loads(line)
            except Exception:
                continue
            if not isinstance(row, dict):
                continue
            if str(row.get("record_type") or "") != "novo_candidate_observation":
                continue
            if str(row.get("symbol") or "").upper() != symbol:
                continue
            if side and str(row.get("side") or "").upper() != side:
                continue
            if engine and str(row.get("engine") or "") != engine:
                continue
            return dict(row)
    except Exception:
        return {}
    return {}


def _maybe_record_novo_enrichment_outcome(
    coin: str,
    exit_price: float,
    pnl_usdt: float,
    reason: str,
    plan: ExitPlan,
    *,
    fee_usdt: float,
    via_exchange: bool = False,
) -> None:
    if not bool(_s.get_val("novo_enrichment_outcome_telemetry_enabled", False)):
        return
    try:
        opened_at = _opened_at_iso(plan)
        closed_at = datetime.now(timezone.utc).isoformat()
        order_id = str(getattr(plan, "order_id", "") or "")
        dedupe_key = (
            str(coin or "").upper(),
            order_id,
            opened_at,
            str(reason or ""),
            "exchange" if via_exchange else "bot",
        )
        if dedupe_key in _novo_enrichment_outcome_seen:
            return
        trace = _confidence_stage_trace_for_plan(coin, plan)
        novo = trace.get("novo", {}) if isinstance(trace, dict) else {}
        enrichment = trace.get("novo_enrichment", {}) if isinstance(trace, dict) else {}
        if not isinstance(novo, dict):
            novo = {}
        if not isinstance(enrichment, dict):
            enrichment = {}
        fallback_candidate = {}
        if not novo or str(novo.get("quality_tier", "COLLECTING") or "COLLECTING").upper() == "COLLECTING":
            fallback_candidate = _latest_novo_candidate_for_plan(coin, plan)
            if fallback_candidate:
                novo = {
                    "quality_score": fallback_candidate.get("quality_score"),
                    "quality_tier": fallback_candidate.get("quality_tier", "COLLECTING"),
                    "modules": fallback_candidate.get("novo_modules", {}),
                }
                if not enrichment:
                    enrichment = fallback_candidate.get("novo_enrichment", {}) if isinstance(fallback_candidate.get("novo_enrichment"), dict) else {}
        suggestions = enrichment.get("suggestions", {})
        if not isinstance(suggestions, dict):
            suggestions = {}
        suggestions = {**_neutral_novo_enrichment_suggestions(), **suggestions}
        try:
            if opened_at:
                hold_time = max(datetime.fromisoformat(closed_at).timestamp() - datetime.fromisoformat(opened_at).timestamp(), 0.0)
            else:
                hold_time = max(time.time() - float(getattr(plan, "created_at", 0.0) or time.time()), 0.0)
        except Exception:
            hold_time = max(time.time() - float(getattr(plan, "created_at", 0.0) or time.time()), 0.0)
        fee_val = round(float(fee_usdt or 0.0), 6)
        pnl_val = round(float(pnl_usdt or 0.0), 6)
        record = {
            "record_type": "novo_enrichment_outcome",
            "dry_run": True,
            "symbol": str(coin or "").upper(),
            "side": str(getattr(plan, "direction", "") or "").upper(),
            "engine": str(getattr(plan, "engine_name", "") or ""),
            "order_id": order_id,
            "opened_at": opened_at,
            "closed_at": closed_at,
            "hold_time_seconds": round(float(hold_time or 0.0), 4),
            "quality_score": novo.get("quality_score"),
            "quality_tier": str(novo.get("quality_tier", "COLLECTING") or "COLLECTING"),
            "enrichment_band": str(enrichment.get("band", "COLLECTING") or "COLLECTING"),
            "novo_modules": novo.get("modules", {}) if isinstance(novo.get("modules", {}), dict) else {},
            "suggestions": suggestions,
            "position_id": str(getattr(plan, "order_id", "") or ""),
            "trace_id": str(fallback_candidate.get("candidate_id") or fallback_candidate.get("trace_id") or ""),
            "actual_pnl": pnl_val,
            "actual_fee": fee_val,
            "net_pnl": round(pnl_val - fee_val, 6),
            "mfe": _current_mfe_roi(plan),
            "mae": _current_mae_roi(plan),
            "exit_reason": str(reason or ""),
            "would_have_helped": "UNKNOWN",
            "would_have_helped_notes": "hypothetical only; no live behavior changed",
        }
        import novo_trade_memory as _novo_memory
        _novo_memory.append_memory(record)
        _novo_enrichment_outcome_seen.add(dedupe_key)
    except Exception as exc:
        logger.debug("Novo enrichment outcome telemetry skipped for %s: %s", coin, exc)


def _normalize_ops_mode() -> str:
    """
    Map runtime preset tokens to the final 5-mode vocabulary.

    trading_mode_preset values in settings_store are historical (safe/normal/aggressive/...).
    The final pass exposes: Safe, Balanced, Aggressive, Scalp, Adaptive.
    """
    raw = str(_s.get_val("trading_mode_preset", "") or "").strip().lower()
    if raw in {"", "custom"}:
        return "balanced"
    if raw == "normal":
        return "balanced"
    if raw == "ai":
        return "adaptive"
    if raw in {"safe", "balanced", "aggressive", "scalp", "adaptive"}:
        return raw
    return "balanced"


def _position_leverage(plan: ExitPlan) -> int:
    """Return the best authoritative lifecycle leverage available."""
    status = str(plan.leverage_truth_status or "UNKNOWN").upper()
    effective = int(plan.position_effective_leverage or 0)
    if effective > 0 and status in {
        "POSITION_EFFECTIVE",
        "EXCHANGE_POSITION",
        "AUTHORITATIVE",
        "CONFIRMED",
    }:
        return effective
    # Only explicitly local/synthetic plans may use their plan leverage.  A
    # configured leverage value is never a substitute for open-position truth.
    if status in {"PAPER_LOCAL", "TEST_STUB", "BACKTEST", "LEGACY_COMPAT"}:
        return max(int(plan.leverage or 1), 1)
    return 0


def _roi_pct(plan: ExitPlan, price: float) -> float:
    """
    Approx ROI% (leveraged) from entry to current price, independent of position size.
    LONG: (price-entry)/entry * lev * 100
    SHORT: (entry-price)/entry * lev * 100
    """
    entry = float(plan.entry_price or 0.0)
    if entry <= 0 or price <= 0:
        return 0.0
    lev = _position_leverage(plan)
    if lev <= 0:
        return 0.0
    move = (price - entry) / entry if plan.direction == "long" else (entry - price) / entry
    return float(move) * float(lev) * 100.0


def _raw_move_pct(plan: ExitPlan, price: float) -> float:
    """Return signed unleveraged price movement in percentage points."""
    entry = float(plan.entry_price or 0.0)
    price = float(price or 0.0)
    if entry <= 0 or price <= 0:
        return 0.0
    if str(plan.direction or "long").lower() == "long":
        return (price - entry) / entry * 100.0
    return (entry - price) / entry * 100.0


def _stored_excursion_raw(plan: ExitPlan, kind: str) -> float:
    """Recover canonical raw excursion, including legacy price-only records."""
    field = "mfe_raw_move_pct" if kind == "mfe" else "mae_raw_move_pct"
    raw = float(getattr(plan, field, 0.0) or 0.0)
    if abs(raw) > 1e-12:
        return raw
    price = float(getattr(plan, f"{kind}_price", 0.0) or 0.0)
    if price > 0 and abs(_raw_move_pct(plan, price)) > 1e-12:
        return _raw_move_pct(plan, price)
    # Legacy in-memory/unit plans may predate raw excursion persistence.  This
    # compatibility fallback is only used when no raw price evidence exists;
    # persisted modern plans always take the raw path above.
    observed_roi = float(getattr(plan, f"{kind}_roi", 0.0) or 0.0)
    observed_leverage = int(getattr(plan, f"{kind}_observed_leverage", 0) or 0) or _position_leverage(plan)
    return observed_roi / float(observed_leverage) if observed_leverage > 0 else 0.0


def _current_excursion_roi(plan: ExitPlan, kind: str) -> float:
    """Derive trigger-comparable excursion ROI from raw movement and live leverage."""
    leverage = _position_leverage(plan)
    if leverage <= 0:
        return 0.0
    return _stored_excursion_raw(plan, kind) * float(leverage)


def _current_mfe_roi(plan: ExitPlan) -> float:
    return _current_excursion_roi(plan, "mfe")


def _current_mae_roi(plan: ExitPlan) -> float:
    return _current_excursion_roi(plan, "mae")


def _is_swing_engine(engine_name: str) -> bool:
    return str(engine_name or "").strip().upper() in {"TENJIN"}


def _should_log_partial_skip(coin: str, stage: str, reason: str) -> bool:
    cooldown = float(_s.get_val("partial_skip_log_cooldown_sec", 60.0) or 60.0)
    if cooldown <= 0:
        return True
    key = (str(coin or "").upper(), str(stage or "").upper())
    now = time.monotonic()
    last_ts, last_reason = _partial_skip_log_state.get(key, (0.0, ""))
    if reason == last_reason and now - last_ts < cooldown:
        return False
    _partial_skip_log_state[key] = (now, reason)
    return True


def _structure_failure_signals(
    coin: str,
    closes: list,
    *,
    price: float,
    entry_price: float,
    is_long: bool,
) -> tuple[bool, bool, dict]:
    """
    Evidence-based structure failure signals for early invalidation.
    Returns: (structure_failed, momentum_flipped, evidence_dict)
    """
    evidence: dict = {}
    structure_failed = False
    momentum_flipped = False
    if not closes or len(closes) < 4 or price <= 0 or entry_price <= 0:
        return False, False, evidence

    # Price action micro-structure (fast failure sequence)
    try:
        if len(closes) >= 4:
            if is_long:
                if closes[-1] < closes[-2] < closes[-3] and closes[-1] < float(entry_price):
                    structure_failed = True
                    evidence["sequence_fail"] = "lower_lows_below_entry"
            else:
                if closes[-1] > closes[-2] > closes[-3] and closes[-1] > float(entry_price):
                    structure_failed = True
                    evidence["sequence_fail"] = "higher_highs_above_entry"
    except Exception:
        pass

    # EMA structure failure + slope failure
    try:
        e21 = ema(closes, 21)
        if e21 and len(e21) >= 2:
            ema21_now = float(e21[-1] or 0.0)
            ema21_prev = float(e21[-2] or 0.0)
            if ema21_now > 0:
                if is_long and price < ema21_now:
                    structure_failed = True
                    evidence["ema21_lost"] = True
                if (not is_long) and price > ema21_now:
                    structure_failed = True
                    evidence["ema21_lost"] = True
                if is_long and ema21_now < ema21_prev:
                    evidence["ema21_slope_fail"] = True
                    momentum_flipped = True
                if (not is_long) and ema21_now > ema21_prev:
                    evidence["ema21_slope_fail"] = True
                    momentum_flipped = True
    except Exception:
        pass

    # MACD momentum flip (1m/scan timeframe proxy)
    try:
        m = macd(closes)
        if m:
            _, _, hist = m
            if is_long and float(hist or 0.0) < 0:
                momentum_flipped = True
                evidence["macd_hist_against"] = True
            if (not is_long) and float(hist or 0.0) > 0:
                momentum_flipped = True
                evidence["macd_hist_against"] = True
    except Exception:
        pass

    # Momentum collapse (net move against over last few bars)
    try:
        if len(closes) >= 6 and closes[-6] and closes[-1]:
            frac = (float(closes[-1]) - float(closes[-6])) / float(closes[-6])
            if is_long and frac < -0.0018:
                momentum_flipped = True
                evidence["momentum_collapse"] = round(frac, 6)
            if (not is_long) and frac > 0.0018:
                momentum_flipped = True
                evidence["momentum_collapse"] = round(frac, 6)
    except Exception:
        pass

    return bool(structure_failed), bool(momentum_flipped), evidence


def _setting_float(key: str, default: float) -> float:
    value = _s.get_val(key, default)
    try:
        return float(default if value is None else value)
    except Exception:
        return float(default)


def _setting_bool(key: str, default: bool) -> bool:
    value = _s.get_val(key, default)
    return bool(default if value is None else value)


def _continuation_score(plan: ExitPlan, price: float, is_long: bool) -> tuple[float, str]:
    if not _s.get_val("continuation_runner_enabled", True):
        return 0.0, "disabled"

    score = 0.0
    reasons: list[str] = []
    engine_name = str(plan.engine_name or "").upper()
    roi = _roi_pct(plan, price)
    min_roi = float(_s.get_val("continuation_runner_min_roi", 2.5) or 2.5)

    if engine_name in _CONTINUATION_ENGINES:
        score += 1.0
        reasons.append("engine")
    try:
        trust = float(getattr(plan, "macro_runner_trust_mult", 1.0) or 1.0)
        if trust >= 1.08:
            score += 0.25
            reasons.append("macro+")
        elif trust <= 0.94:
            score -= 0.25
            reasons.append("macro-")
    except Exception:
        pass
    if int(plan.vote_score or 0) >= 3:
        score += 1.0
        reasons.append("vote")
    if roi >= min_roi:
        score += 1.0
        reasons.append("roi")

    closes = list(get_closes(plan.coin) or [])
    if len(closes) >= 12 and price > 0:
        short = sum(closes[-5:]) / 5.0
        medium = sum(closes[-12:]) / 12.0
        prior = closes[-6]
        aligned = short >= medium and price >= short and price >= prior if is_long else short <= medium and price <= short and price <= prior
        if aligned:
            score += 1.0
            reasons.append("trend")
        impulse = ((price - closes[-6]) / closes[-6]) if is_long else ((closes[-6] - price) / closes[-6])
        if closes[-6] > 0 and impulse >= 0.002:
            score += 0.5
            reasons.append("impulse")

    vols = list(get_volumes(plan.coin) or [])
    if len(vols) >= 12:
        recent = sum(vols[-3:]) / 3.0
        base = sum(vols[-12:-3]) / 9.0
        if base > 0 and recent >= base * 1.10:
            score += 0.5
            reasons.append("volume")

    return score, ",".join(reasons) or "none"


def _continuation_active(plan: ExitPlan, price: float, is_long: bool) -> tuple[bool, float, str]:
    score, reasons = _continuation_score(plan, price, is_long)
    threshold = float(_s.get_val("continuation_runner_score_threshold", 3.0) or 3.0)
    return score >= threshold, score, reasons


def _stage_reached(plan: ExitPlan, stage: str) -> bool:
    stage_s = str(stage or "").lower()
    if stage_s == "tp1":
        return bool(plan.tp1_done or plan.tp1_deferred)
    if stage_s == "tp2":
        return bool(plan.tp2_done or plan.tp2_deferred)
    return False


def _economic_min_remaining_notional(plan: ExitPlan) -> float:
    configured = float(_s.get_val("economic_min_runner_notional_usdt", 5.0) or 5.0)
    fee_floor = fee_cost_usdt(max(float(plan.size_usdt or 0.0), 0.0), int(plan.leverage or 1)) * float(
        _s.get_val("economic_min_fee_mult", 8.0) or 8.0
    )
    return round(max(configured, fee_floor), 6)


def _economic_partial_decision(
    plan: ExitPlan,
    *,
    close_pct: float,
    price: float,
    floor: float,
    continuation: bool,
) -> _la.EconomicMinimumDecision:
    pnl = _calc_pnl(plan, price, close_pct)
    return _la.evaluate_economic_minimum(
        current_notional=float(plan.size_usdt or 0.0),
        close_pct=float(close_pct or 0.0),
        pnl_usdt=pnl,
        floor_usdt=floor,
        min_remaining_notional=_economic_min_remaining_notional(plan),
        continuation=continuation,
    )


def _fee_bleed_guard_economics(
    plan: ExitPlan,
    *,
    stage: str,
    close_pct: float,
    price: float,
) -> dict:
    pct = max(float(close_pct or 0.0), 0.0)
    notional = round(float(plan.size_usdt or 0.0) * pct / 100.0, 8)
    gross_pnl = float(_calc_pnl(plan, price, pct) or 0.0)
    fee_est = float(fee_cost_usdt(notional, int(plan.leverage or 1)) or 0.0)
    net_pnl = gross_pnl - fee_est

    min_net_profit = float(
        _s.get_val(
            "fee_bleed_guard_min_net_profit_usdt",
            _s.get_val("min_net_take_profit_usdt", 0.10),
        )
        or 0.0
    )
    configured_min_notional = float(
        _s.get_val(
            "fee_bleed_guard_min_partial_notional_usdt",
            _s.get_val("novo_fee_bleed_min_partial_notional_usdt", 0.0),
        )
        or 0.0
    )
    min_notional = max(
        configured_min_notional,
        fee_est * float(_s.get_val("novo_fee_bleed_min_notional_fee_mult", 8.0) or 8.0),
    )
    fee_share = (fee_est / gross_pnl) if gross_pnl > 0 else 1.0
    max_fee_share = float(
        _s.get_val(
            "fee_bleed_guard_max_fee_share",
            _s.get_val("novo_fee_bleed_max_fee_share", 0.35),
        )
        or 0.35
    )
    failures = []
    if notional <= 0.0:
        failures.append("zero_notional")
    if notional + 1e-9 < min_notional:
        failures.append("notional_below_floor")
    if net_pnl <= 0.0:
        failures.append("net_non_positive")
    elif net_pnl < min_net_profit:
        failures.append("net_below_min_profit")
    if fee_share > max_fee_share:
        failures.append("fee_share_too_high")

    return {
        "enabled": True,
        "stage": str(stage or ""),
        "partial_notional": round(notional, 6),
        "notional": round(notional, 6),
        "gross_pnl": round(gross_pnl, 6),
        "fee_est": round(fee_est, 6),
        "expected_net_pnl": round(net_pnl, 6),
        "net_pnl": round(net_pnl, 6),
        "min_net_profit": round(min_net_profit, 6),
        "min_notional": round(min_notional, 6),
        "fee_share": round(fee_share, 6),
        "max_fee_share": round(max_fee_share, 6),
        "failures": failures,
        "allowed": not failures,
        "deferred": bool(failures),
    }


def _log_fee_bleed_guard_evaluation(coin: str, plan: ExitPlan, meta: dict, *, exit_reason: str, close_pct: float) -> None:
    try:
        logger.info(
            "FEE_BLEED_GUARD_EVALUATED %s side=%s engine=%s reason=%s pct=%.2f notional=$%.4f gross=$%.4f fee=$%.4f net=$%.4f fee_share=%.3f guard_evaluated=%s guard_deferred=%s why=%s",
            coin,
            getattr(plan, "direction", None),
            getattr(plan, "engine_name", None),
            exit_reason,
            float(close_pct or 0.0),
            float(meta.get("notional", meta.get("partial_notional", 0.0)) or 0.0),
            float(meta.get("gross_pnl", 0.0) or 0.0),
            float(meta.get("fee_est", 0.0) or 0.0),
            float(meta.get("expected_net_pnl", meta.get("net_pnl", 0.0)) or 0.0),
            float(meta.get("fee_share", 0.0) or 0.0),
            bool(meta.get("enabled", False)),
            bool(meta.get("deferred", False)),
            ",".join(meta.get("failures") or ["allowed"]),
        )
    except Exception:
        pass


def _novo_fee_bleed_guard_allows_partial(
    coin: str,
    plan: ExitPlan,
    *,
    stage: str,
    close_pct: float,
    price: float,
) -> tuple[bool, dict]:
    """Return False only for uneconomic optional partial exits when explicitly enabled."""
    try:
        if not bool(_s.get_val("novo_fee_bleed_guard_enabled", False)):
            return True, {"enabled": False}
        pct = max(float(close_pct or 0.0), 0.0)
        meta = _fee_bleed_guard_economics(plan, stage=stage, close_pct=pct, price=price)
        allowed = bool(meta.get("allowed", False))
        _log_fee_bleed_guard_evaluation(coin, plan, meta, exit_reason=str(stage or "partial"), close_pct=pct)
        if not allowed:
            reason = (
                f"{stage}:notional={float(meta.get('notional', 0.0)):.4f}:gross={float(meta.get('gross_pnl', 0.0)):.4f}:"
                f"fee={float(meta.get('fee_est', 0.0)):.4f}:net={float(meta.get('net_pnl', 0.0)):.4f}:"
                f"min={float(meta.get('min_notional', 0.0)):.4f}:fee_share={float(meta.get('fee_share', 0.0)):.3f}:"
                f"why={','.join(meta.get('failures') or [])}"
            )
            if _should_log_partial_skip(coin, f"novo_fee_bleed_{stage}", reason):
                logger.info(
                    "NOVO_FEE_BLEED_GUARD_DEFERRED_PARTIAL %s stage=%s pct=%.2f notional=$%.4f gross=$%.4f fee=$%.4f net=$%.4f min_notional=$%.4f min_net=$%.4f fee_share=%.3f why=%s",
                    coin,
                    stage,
                    pct,
                    float(meta.get("notional", 0.0) or 0.0),
                    float(meta.get("gross_pnl", 0.0) or 0.0),
                    float(meta.get("fee_est", 0.0) or 0.0),
                    float(meta.get("net_pnl", 0.0) or 0.0),
                    float(meta.get("min_notional", 0.0) or 0.0),
                    float(meta.get("min_net_profit", 0.0) or 0.0),
                    float(meta.get("fee_share", 0.0) or 0.0),
                    ",".join(meta.get("failures") or []),
                )
        elif bool(_s.get_val("fee_bleed_guard_trace_all_small_exits", True)):
            gross = float(meta.get("gross_pnl", 0.0) or 0.0)
            min_net = float(meta.get("min_net_profit", 0.0) or 0.0)
            if 0.0 < gross <= max(min_net * 3.0, 0.30):
                logger.info(
                    "FEE_BLEED_GUARD_ALLOWED_SMALL_EXIT %s stage=%s pct=%.2f notional=$%.4f gross=$%.4f fee=$%.4f net=$%.4f fee_share=%.3f",
                    coin,
                    stage,
                    pct,
                    float(meta.get("notional", 0.0) or 0.0),
                    gross,
                    float(meta.get("fee_est", 0.0) or 0.0),
                    float(meta.get("net_pnl", 0.0) or 0.0),
                    float(meta.get("fee_share", 0.0) or 0.0),
                )
        return allowed, meta
    except Exception as exc:
        logger.debug("Novo fee-bleed guard bypassed for %s: %s", coin, exc)
        return True, {"enabled": False, "error": str(exc)}


def _fee_bleed_trace_small_full_close(coin: str, plan: ExitPlan, *, price: float, reason: str, via_exchange: bool = False) -> None:
    """Trace small profitable full closes; never defers or changes close behavior."""
    try:
        if not bool(_s.get_val("novo_fee_bleed_guard_enabled", False)):
            return
        meta = _fee_bleed_guard_economics(plan, stage=reason, close_pct=100.0, price=price)
        gross = float(meta.get("gross_pnl", 0.0) or 0.0)
        if gross <= 0.0:
            return
        trace_all = bool(_s.get_val("fee_bleed_guard_trace_all_small_exits", True))
        small_or_fee_heavy = bool(meta.get("failures")) or gross <= max(float(meta.get("min_net_profit", 0.0) or 0.0) * 3.0, 0.30)
        if not trace_all and not small_or_fee_heavy:
            return
        meta = {
            **meta,
            "allowed": True,
            "deferred": False,
            "via_exchange": bool(via_exchange),
            "full_close_not_deferred": True,
        }
        _log_fee_bleed_guard_evaluation(coin, plan, meta, exit_reason=reason, close_pct=100.0)
        if small_or_fee_heavy:
            logger.info(
                "FEE_BLEED_GUARD_ALLOWED_SMALL_EXIT %s reason=%s via_exchange=%s notional=$%.4f gross=$%.4f fee=$%.4f net=$%.4f fee_share=%.3f why=full_close_not_deferred",
                coin,
                reason,
                bool(via_exchange),
                float(meta.get("notional", 0.0) or 0.0),
                gross,
                float(meta.get("fee_est", 0.0) or 0.0),
                float(meta.get("net_pnl", 0.0) or 0.0),
                float(meta.get("fee_share", 0.0) or 0.0),
            )
    except Exception as exc:
        logger.debug("Fee-bleed full-close trace skipped for %s: %s", coin, exc)


def _maybe_positive_mfe_profit_guard(coin: str, plan: ExitPlan, price: float, is_long: bool) -> bool:
    """Move SL forward after meaningful MFE; never closes or blocks a trade."""
    try:
        if not bool(_s.get_val("positive_mfe_profit_guard_enabled", False)):
            return False
        if not plan or price <= 0 or float(plan.entry_price or 0.0) <= 0 or float(plan.size_usdt or 0.0) <= 0:
            return False

        max_levels = int(float(_s.get_val("positive_mfe_guard_max_lock_levels", 1) or 1))
        if int(plan.profit_lock_level or 0) >= max_levels:
            return False

        created = float(getattr(plan, "created_at", 0.0) or 0.0)
        min_age = float(_s.get_val("positive_mfe_guard_min_age_sec", 30.0) or 30.0)
        if created > 0 and time.time() - created < min_age:
            return False

        mfe_trigger = float(_s.get_val("positive_mfe_guard_mfe_roi_trigger", 3.0) or 3.0)
        current_mfe_roi = _current_mfe_roi(plan)
        if current_mfe_roi + 1e-9 < mfe_trigger:
            return False

        total_profit = _total_trade_pnl_usdt(plan, price)
        if total_profit <= 0.0:
            return False

        entry = float(plan.entry_price or 0.0)
        lock_roi = float(_s.get_val("positive_mfe_guard_lock_roi_pct", 0.35) or 0.35)
        lock_frac = _roi_pct_to_price_frac(lock_roi, _position_leverage(plan))
        roi_lock_sl = entry * (1.0 + lock_frac) if is_long else entry * (1.0 - lock_frac)
        new_sl = roi_lock_sl

        tick_gap = max(abs(float(price)) * 0.000001, 0.00000001)
        if is_long:
            new_sl = min(new_sl, float(price) - tick_gap)
            if new_sl <= entry:
                return False
        else:
            new_sl = max(new_sl, float(price) + tick_gap)
            if new_sl >= entry:
                return False
        if not _improves_stop(plan, new_sl, is_long):
            return False
        economics, required_floor, eligible = _protection_candidate_economics(plan, price, new_sl)
        if not eligible:
            _record_protection_economic_defer(
                plan,
                stage="positive_mfe_profit_guard",
                economics=economics,
                required_floor=required_floor,
            )
            return False

        old_sl = float(plan.hard_sl or 0.0)
        old_reason = str(plan.last_protection_reason or "")
        _apply_stop_improvement(
            coin,
            plan,
            new_sl,
            is_long,
            "positive_mfe_profit_guard",
            (
                f"(engine={plan.engine_name or 'UNKNOWN'} side={plan.direction} "
                f"mfe={current_mfe_roi:.2f}% roi={_roi_pct(plan, price):.2f}% "
                f"pnl=${total_profit:.4f} candidate_net=${float(economics.candidate_stop_estimated_net_pnl or 0.0):.4f} "
                f"required=${float(required_floor or 0.0):.4f})"
            ),
        )
        if str(plan.last_protection_reason or "") != "positive_mfe_profit_guard" or float(plan.hard_sl or 0.0) == old_sl:
            plan.last_protection_reason = old_reason
            return False

        plan.profit_lock_level = max(int(plan.profit_lock_level or 0), 1)
        logger.info(
            "POSITIVE_MFE_PROFIT_GUARD_LOCKED %s side=%s engine=%s mfe=%.2f current_roi=%.2f unrealized_pnl=$%.4f old_sl=%.8f new_sl=%.8f fee_est=$%.4f",
            coin,
            plan.direction,
            plan.engine_name or "UNKNOWN",
            current_mfe_roi,
            _roi_pct(plan, price),
            total_profit,
            old_sl,
            float(plan.hard_sl or 0.0),
            float(economics.total_known_and_estimated_cost or 0.0),
        )
        return True
    except Exception as exc:
        logger.debug("Positive MFE profit guard bypassed for %s: %s", coin, exc)
        return False


def _maybe_positive_profit_lock_enrichment(coin: str, plan: ExitPlan, price: float, is_long: bool) -> bool:
    """Gradually improves stop protection after high-MFE milestones only."""
    try:
        if not bool(_s.get_val("positive_profit_lock_enrichment_enabled", False)):
            return False
        if not plan or price <= 0 or float(plan.entry_price or 0.0) <= 0 or float(plan.size_usdt or 0.0) <= 0:
            return False

        current_level = int(plan.profit_lock_level or 0)
        max_level = max(0, min(int(float(_s.get_val("profit_lock_enrichment_max_level", 3) or 3)), 3))
        if current_level >= max_level:
            return False

        mfe = _current_mfe_roi(plan)
        base_trigger = float(_s.get_val("profit_lock_enrichment_mfe_trigger_roi", 25.0) or 25.0)
        milestones = (
            (1, base_trigger, float(_s.get_val("profit_lock_enrichment_lock_pct_1", 0.20) or 0.20)),
            (2, base_trigger * 2.0, float(_s.get_val("profit_lock_enrichment_lock_pct_2", 0.35) or 0.35)),
            (3, base_trigger * 4.0, float(_s.get_val("profit_lock_enrichment_lock_pct_3", 0.50) or 0.50)),
        )

        target_level = 0
        lock_pct = 0.0
        trigger = 0.0
        for level, level_trigger, level_lock_pct in milestones:
            if level > max_level:
                continue
            if mfe + 1e-9 >= level_trigger and level > current_level:
                target_level = level
                lock_pct = max(0.0, min(float(level_lock_pct or 0.0), 0.95))
                trigger = level_trigger

        if target_level <= 0 or lock_pct <= 0.0:
            return False

        total_profit = _total_trade_pnl_usdt(plan, price)
        if total_profit <= 0.0:
            return False

        secure_total = total_profit * lock_pct
        secure_total = min(secure_total, total_profit)
        new_sl = _stop_price_for_total_profit(plan, secure_total, is_long)
        if not _improves_stop(plan, new_sl, is_long):
            return False
        economics, required_floor, eligible = _protection_candidate_economics(plan, price, new_sl)
        if not eligible:
            _record_protection_economic_defer(
                plan,
                stage=f"profit_lock_enrichment_l{target_level}",
                economics=economics,
                required_floor=required_floor,
            )
            return False

        old_sl = float(plan.hard_sl or 0.0)
        old_reason = str(plan.last_protection_reason or "")
        reason = f"profit_lock_enrichment_l{target_level}"
        _apply_stop_improvement(
            coin,
            plan,
            new_sl,
            is_long,
            reason,
            (
                f"(engine={plan.engine_name or 'UNKNOWN'} side={plan.direction} "
                f"mfe={mfe:.2f}% roi={_roi_pct(plan, price):.2f}% "
                f"profit=${total_profit:.4f} secure=${secure_total:.4f} lock_pct={lock_pct:.2f})"
            ),
        )
        if str(plan.last_protection_reason or "") != reason or float(plan.hard_sl or 0.0) == old_sl:
            plan.last_protection_reason = old_reason
            return False

        plan.profit_lock_level = max(current_level, target_level)
        logger.info(
            "PROFIT_LOCK_ENRICHMENT_L%d %s side=%s engine=%s mfe=%.2f current_roi=%.2f lock_pct=%.2f old_sl=%.8f new_sl=%.8f secure_profit=$%.4f",
            target_level,
            coin,
            plan.direction,
            plan.engine_name or "UNKNOWN",
            mfe,
            _roi_pct(plan, price),
            lock_pct,
            old_sl,
            float(plan.hard_sl or 0.0),
            secure_total,
        )
        return True
    except Exception as exc:
        logger.debug("Profit lock enrichment bypassed for %s: %s", coin, exc)
        return False


def _meaningful_profit_decision(
    plan: ExitPlan,
    *,
    price: float,
    continuation_score: float,
) -> _la.MeaningfulProfitDecision:
    return _la.meaningful_profit_protection(
        size_usdt=float(plan.size_usdt or 0.0),
        initial_size_usdt=float(plan.initial_size_usdt or plan.size_usdt or 0.0),
        profit_usdt=_total_trade_pnl_usdt(plan, price),
        estimated_fee_usdt=_estimated_total_fee_usdt(plan),
        # Partial economics remain separate from full-close/protection economics.
        min_profit_floor_usdt=_partial_profit_floor_usdt(plan, float(plan.size_usdt or 0.0)),
        runner_quality=float(plan.runner_quality or 0.0),
        continuation_score=float(continuation_score or 0.0),
        consensus_score=float(plan.vote_score or 0.0),
        candidate_quality=0.0,
        remaining_pct=float(plan.remaining_pct or 100.0),
        runner_min_remaining_pct=float(_s.get_val("micro_meaningful_profit_runner_floor_pct", 40.0) or 40.0),
        base_close_pct=float(_s.get_val("micro_meaningful_profit_close_pct", 35.0) or 35.0),
        max_close_pct=float(_s.get_val("micro_meaningful_profit_max_close_pct", 55.0) or 55.0),
    )


def _mark_deferred_milestone(
    coin: str,
    plan: ExitPlan,
    *,
    stage: str,
    price: float,
    roi: float,
    decision: _la.EconomicMinimumDecision,
    continuation_score: float,
    continuation_reason: str,
) -> None:
    stage_s = str(stage or "").lower()
    was_deferred = bool(plan.tp1_deferred if stage_s == "tp1" else plan.tp2_deferred)
    if stage_s == "tp1":
        plan.tp1_deferred = True
        if float(plan.tp1_deferred_price or 0.0) <= 0.0:
            plan.tp1_deferred_price = float(price or 0.0)
        plan.lifecycle_phase = "tp1_deferred"
    else:
        plan.tp2_deferred = True
        if float(plan.tp2_deferred_price or 0.0) <= 0.0:
            plan.tp2_deferred_price = float(price or 0.0)
        plan.lifecycle_phase = "tp2_deferred"
    plan.deferred_partial_pnl_usdt = round(max(float(plan.deferred_partial_pnl_usdt or 0.0), float(decision.pnl_usdt or 0.0)), 4)
    plan.economic_minimum = decision.to_dict()
    _maybe_mark_near_floor_tp_earned(plan, stage=stage_s, price=price, decision=decision, roi=roi)

    reason = f"deferred:{stage_s}:{decision.reason}:floor={decision.floor_usdt:.4f}:pct={float(decision.closed_notional or 0.0):.4f}"
    if was_deferred and not _should_log_partial_skip(coin, stage_s, reason):
        return
    logger.info(
        "MILESTONE %s deferred %s @ %.6f pnl=$%.4f floor=$%.4f roi=%.1f%% lev=%dx residual=%s continuation=%.1f/%s",
        stage_s.upper(),
        coin,
        price,
        decision.pnl_usdt,
        decision.floor_usdt,
        roi,
        plan.leverage,
        decision.residual_reason,
        continuation_score,
        continuation_reason,
    )


def _maybe_mark_near_floor_tp_earned(
    plan: ExitPlan,
    *,
    stage: str,
    price: float,
    decision: _la.EconomicMinimumDecision,
    roi: float,
) -> bool:
    if not _setting_bool("early_invalidation_near_tp_grace_enabled", True):
        return False
    floor = max(float(decision.floor_usdt or 0.0), 0.0)
    pnl = max(float(decision.pnl_usdt or 0.0), 0.0)
    if floor <= 0.0 or pnl <= 0.0:
        return False
    ratio = pnl / floor
    required = _setting_float("early_invalidation_near_tp_floor_ratio", 0.85)
    if ratio + 1e-9 < required:
        return False
    plan.near_floor_tp_earned = True
    plan.near_floor_tp_ratio = round(max(float(plan.near_floor_tp_ratio or 0.0), ratio), 4)
    grace = max(_setting_float("early_invalidation_near_tp_grace_sec", 90.0), 0.0)
    plan.near_floor_tp_grace_until = max(float(plan.near_floor_tp_grace_until or 0.0), time.time() + grace)
    plan.lifecycle_phase = f"{stage}_near_floor"
    plan.last_protection_reason = "near_floor_tp_earned"
    if roi > 0.0:
        plan.profit_lock_level = max(int(plan.profit_lock_level or 0), 1)
    plan.economic_minimum = {
        **dict(plan.economic_minimum or {}),
        "near_floor_tp_earned": True,
        "near_floor_tp_ratio": plan.near_floor_tp_ratio,
        "near_floor_stage": str(stage or ""),
        "near_floor_price": round(float(price or 0.0), 8),
    }
    return True


def _runner_preserved_close_pct(
    plan: ExitPlan,
    desired_pct: float,
    *,
    stage: str,
    continuation: bool = False,
) -> float:
    pct = min(max(float(desired_pct or 0.0), 0.0), 100.0)
    max_key = "partial_tp1_max_close_pct" if str(stage).lower() == "tp1" else "partial_tp2_max_close_pct"
    pct = min(pct, float(_s.get_val(max_key, pct) or pct))
    initial = float(plan.initial_size_usdt or plan.size_usdt or 0.0)
    current = float(plan.size_usdt or 0.0)
    if initial <= 0 or current <= 0:
        return pct
    _core_setting = _s.get_val("runner_core_min_remaining_pct", 45.0)
    core_min = float(45.0 if _core_setting is None else _core_setting)
    if continuation:
        _cont_core_setting = _s.get_val("continuation_runner_core_min_pct", 55.0)
        core_min = max(core_min, float(55.0 if _cont_core_setting is None else _cont_core_setting))
    max_close_for_core = max((1.0 - (core_min / 100.0) * initial / current) * 100.0, 0.0)
    return round(min(pct, max_close_for_core), 4)


def _roi_pct_to_price_frac(roi_pct: float, leverage: int) -> float:
    """Convert leveraged ROI% into a price-distance fraction (for SL/TSL placement)."""
    lev = max(int(leverage or 1), 1)
    return max(float(roi_pct or 0.0) / 100.0 / float(lev), 0.0)


def _milestone_thresholds(plan: ExitPlan) -> tuple[float, float, float, float, float]:
    """
    Milestone triggers (leveraged ROI%).
    Returns: (be_trigger, tp1_trigger, tp2_trigger, profit_lock_trigger, trailing_activation)
    """
    if _is_swing_engine(plan.engine_name):
        be = float(_s.get_val("swing_breakeven_trigger_pct", 3.2) or 3.2)
        tp1 = float(_s.get_val("swing_tp1_trigger_pct", 6.5) or 6.5)
        tp2 = float(_s.get_val("swing_tp2_trigger_pct", 12.0) or 12.0)
        pl = float(_s.get_val("swing_profit_lock_trigger_pct", 6.8) or 6.8)
        ta = float(_s.get_val("swing_trailing_activation_pct", 8.5) or 8.5)
        profile = dict(plan.exit_profile or _exit_profiles.profile_for_engine(plan.engine_name))
        return be * profile.get("be", 1.0), tp1 * profile.get("tp1", 1.0), tp2 * profile.get("tp2", 1.0), pl * profile.get("profit_lock", 1.0), ta * profile.get("trail_activation", 1.0)
    be = float(_s.get_val("breakeven_trigger_pct", 1.1) or 1.1)
    tp1 = float(_s.get_val("tp1_trigger_pct", 1.8) or 1.8)
    tp2 = float(_s.get_val("tp2_trigger_pct", 3.8) or 3.8)
    pl = float(_s.get_val("profit_lock_trigger_pct", 2.2) or 2.2)
    ta = float(_s.get_val("trailing_activation_pct", 3.0) or 3.0)
    profile = dict(plan.exit_profile or _exit_profiles.profile_for_engine(plan.engine_name))
    return be * profile.get("be", 1.0), tp1 * profile.get("tp1", 1.0), tp2 * profile.get("tp2", 1.0), pl * profile.get("profit_lock", 1.0), ta * profile.get("trail_activation", 1.0)


def _milestone_partials(mode: str) -> tuple[float, float]:
    # Legacy helper retained for older call sites. Do not clamp (operator values must be real).
    base_tp1 = float(_s.get_val("partial_tp1_close_pct", 40.0) or 40.0)
    base_tp2 = float(_s.get_val("partial_tp2_close_pct", 30.0) or 30.0)
    return float(base_tp1), float(base_tp2)


def _milestone_trail_pct(plan: ExitPlan, stage: int) -> float:
    """
    Percent trail distance for the runner stage (fraction, not percent).
    Stage:
      1 = baseline (pre-TP1)
      3 = post-TP1
      4 = post-TP2 (tightest)
    """
    if _is_swing_engine(plan.engine_name):
        base_roi = float(_s.get_val("swing_trailing_distance_pct", 3.2) or 3.2)
        if int(stage) >= 4:
            roi = base_roi * 0.75
        elif int(stage) >= 3:
            roi = base_roi * 0.85
        else:
            roi = base_roi
        profile = dict(plan.exit_profile or _exit_profiles.profile_for_engine(plan.engine_name))
        return _roi_pct_to_price_frac(roi * profile.get("trail_distance", 1.0), _position_leverage(plan))

    base_roi = float(_s.get_val("trailing_distance_pct", 1.1) or 1.1)
    after_tp1 = float(_s.get_val("trailing_after_tp1_pct", 0.85) or 0.85)
    after_tp2 = float(_s.get_val("trailing_after_tp2_pct", 0.60) or 0.60)
    if int(stage) >= 4:
        roi = after_tp2
    elif int(stage) >= 3:
        roi = after_tp1
    else:
        roi = base_roi
    profile = dict(plan.exit_profile or _exit_profiles.profile_for_engine(plan.engine_name))
    return _roi_pct_to_price_frac(roi * profile.get("trail_distance", 1.0), _position_leverage(plan))


def _update_lifecycle_telemetry(
    plan: ExitPlan,
    price: float,
    is_long: bool,
    *,
    continuation_score: float = 0.0,
    continuation_reason: str = "",
) -> None:
    roi = _roi_pct(plan, price)
    raw_move = _raw_move_pct(plan, price)
    previous_mfe_raw = _stored_excursion_raw(plan, "mfe")
    previous_mae_raw = _stored_excursion_raw(plan, "mae")
    leverage = _position_leverage(plan)
    if raw_move > previous_mfe_raw:
        plan.mfe_roi = round(roi, 4)
        plan.mfe_price = round(float(price or 0.0), 12)
        plan.mfe_raw_move_pct = round(raw_move, 6)
        plan.mfe_observed_leverage = leverage
        plan.mfe_gross_pnl_usdt = round(_calc_pnl_at_size(plan, price, plan.initial_size_usdt or plan.size_usdt), 8)
        if previous_mfe_raw == 0.0 or raw_move - previous_mfe_raw >= 0.01:
            _record_exit_transition(
                plan,
                "MFE_UPDATE",
                mfe_roi=plan.mfe_roi,
                mfe_price=plan.mfe_price,
                mfe_gross_pnl_usdt=plan.mfe_gross_pnl_usdt,
                mfe_raw_move_pct=plan.mfe_raw_move_pct,
                mfe_observed_leverage=plan.mfe_observed_leverage,
            )
    if raw_move < previous_mae_raw:
        plan.mae_roi = round(roi, 4)
        plan.mae_price = round(float(price or 0.0), 12)
        plan.mae_raw_move_pct = round(raw_move, 6)
        plan.mae_observed_leverage = leverage
        plan.mae_gross_pnl_usdt = round(_calc_pnl_at_size(plan, price, plan.initial_size_usdt or plan.size_usdt), 8)
        if previous_mae_raw == 0.0 or previous_mae_raw - raw_move >= 0.01:
            _record_exit_transition(
                plan,
                "MAE_UPDATE",
                mae_roi=plan.mae_roi,
                mae_price=plan.mae_price,
                mae_gross_pnl_usdt=plan.mae_gross_pnl_usdt,
                mae_raw_move_pct=plan.mae_raw_move_pct,
                mae_observed_leverage=plan.mae_observed_leverage,
            )
    plan.mfe_current_roi = round(_current_mfe_roi(plan), 4)
    plan.mae_current_roi = round(_current_mae_roi(plan), 4)
    if plan.tsl_armed:
        net_now = float(_total_trade_pnl_usdt(plan, price) or 0.0) - float(_estimated_total_fee_usdt(plan) or 0.0)
        plan.trail_peak_net_pnl = round(max(float(plan.trail_peak_net_pnl or 0.0), net_now), 8)
    plan.runner_quality = round(max(float(continuation_score or 0.0), 0.0), 4)
    plan.runner_core_pct = round(
        float(_s.get_val("continuation_runner_core_min_pct", 55.0) if continuation_score >= float(_s.get_val("continuation_runner_score_threshold", 3.0) or 3.0) else _s.get_val("runner_core_min_remaining_pct", 45.0)) or 0.0,
        4,
    )
    if plan.tp2_done or plan.tp2_deferred:
        plan.lifecycle_phase = "runner_core"
    elif plan.tp1_done or plan.tp1_deferred:
        plan.lifecycle_phase = "continuation" if continuation_score >= float(_s.get_val("continuation_runner_score_threshold", 3.0) or 3.0) else "runner_core"
    elif roi > 0:
        plan.lifecycle_phase = "early_validation"
    if float(plan.tp1_deferred_price or 0.0) > 0.0:
        plan.runner_opportunity = _la.update_runner_opportunity(
            plan.runner_opportunity,
            direction=plan.direction,
            stage="tp1",
            trim_pct=0.0 if not plan.tp1_done else max(100.0 - float(plan.remaining_pct or 100.0), 0.0),
            stage_price=float(plan.tp1_deferred_price or 0.0),
            current_price=price,
            initial_notional=float(plan.initial_size_usdt or 0.0),
        )
    if float(plan.tp2_deferred_price or 0.0) > 0.0:
        plan.runner_opportunity = _la.update_runner_opportunity(
            plan.runner_opportunity,
            direction=plan.direction,
            stage="tp2",
            trim_pct=0.0 if not plan.tp2_done else max(100.0 - float(plan.remaining_pct or 100.0), 0.0),
            stage_price=float(plan.tp2_deferred_price or 0.0),
            current_price=price,
            initial_notional=float(plan.initial_size_usdt or 0.0),
        )

    if plan.near_floor_tp_earned and price > 0:
        plan.runner_opportunity = _la.update_runner_opportunity(
            plan.runner_opportunity,
            direction=plan.direction,
            stage="near_floor",
            trim_pct=0.0,
            stage_price=float(plan.tp2_deferred_price or plan.tp1_deferred_price or plan.entry_price or price),
            current_price=price,
            initial_notional=float(plan.initial_size_usdt or 0.0),
        )

    closes = list(get_closes(plan.coin) or [])
    vols = list(get_volumes(plan.coin) or [])
    ex = _la.exhaustion_score(
        direction=plan.direction,
        closes=closes,
        volumes=vols,
        current_price=price,
        peak_price=float(plan.peak_price or 0.0),
        continuation_score=continuation_score,
        threshold=float(_s.get_val("exhaustion_tighten_score_threshold", 3.0) or 3.0),
    )
    plan.exhaustion_score = float(ex.score)
    plan.exhaustion_reasons = str(ex.reasons or "")


def _sync_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat()


def _mark_plan_sync_failure(reason: str) -> None:
    from position_registry import mark_sync_state as _reg_mark_sync
    if _plans:
        logger.warning("SYNC FAILURE - skipping closure. reason=%s", reason)
    _sync_health.mark_failure("exit_sync", reason)
    for coin, plan in _plans.items():
        plan.sync_status = "UNCERTAIN"
        plan.exchange_absence_confirmations = 0
        plan.last_sync_error = str(reason or "")
        _reg_mark_sync(coin, "UNCERTAIN", absence_confirmations=0, error_reason=reason)


def _mark_plan_seen(coin: str, plan: ExitPlan, sync_ts: str) -> None:
    from position_registry import mark_sync_state as _reg_mark_sync
    plan.sync_status = "OK"
    plan.last_successful_sync = sync_ts
    plan.last_seen_on_exchange = sync_ts
    plan.exchange_absence_confirmations = 0
    plan.last_sync_error = ""
    _reg_mark_sync(
        coin,
        "OK",
        success_ts=sync_ts,
        seen_ts=sync_ts,
        absence_confirmations=0,
        error_reason="",
    )


def _mark_plan_absent(coin: str, plan: ExitPlan, sync_ts: str) -> int:
    from position_registry import get_all_details as _reg_details, mark_sync_state as _reg_mark_sync
    details = _reg_details().get(coin, {})
    confirmations = int(details.get("exchange_absence_confirmations", plan.exchange_absence_confirmations) or 0) + 1
    plan.sync_status = "MISSING"
    plan.last_successful_sync = sync_ts
    plan.exchange_absence_confirmations = confirmations
    plan.last_sync_error = ""
    _reg_mark_sync(
        coin,
        "MISSING",
        success_ts=sync_ts,
        absence_confirmations=confirmations,
        error_reason="",
    )
    logger.warning(
        "UNCERTAIN STATE - awaiting confirmation %s absent_on_exchange confirmations=%d/2",
        coin,
        confirmations,
    )
    return confirmations


def register(
    coin:        str,
    direction:   str,
    entry_price: float,
    size_usdt:   float,
    atr_val:     float,
    hard_sl:     float,
    tp1_price:   float,
    tp2_price:   float,
    order_id:    str = "",
    client_order_id: str = "",
    position_lifecycle_id: str = "",
    engine_name: str = "UNKNOWN",
    vote_score:  int = 2,
    leverage:    int = 5,           # ← per-trade leverage parameter
    macro_runner_trust_mult: float = 1.0,
    expected_risk_usdt: float = 0.0,
    risk_cap_usdt: float = 0.0,
    risk_ceiling_action: str = "",
    paper_validation: dict | None = None,
    partial_mode_override: bool | None = None,
    partial_exit_state: dict | None = None,
    partial_exit_events: list[dict] | None = None,
    entry_contracts: float = 0.0,
    confirmed_remaining_contracts: float = 0.0,
    exchange_position_timestamp: str = "",
    exposure_truth_status: str = EXPOSURE_ACTIVE,
    closure_handoff_at: str = "",
    partial_attribution_status: str = "NONE",
    partial_realized_pnl_usdt: float = 0.0,
    partial_fees_usdt: float = 0.0,
    entry_fee_usdt: float | None = None,
    funding_usdt: float | None = None,
    funding_status: str = "UNKNOWN",
    tp_owner: str = "",
    tp_mode_at_entry: str = "",
    exchange_tp_algo_id: str = "",
    local_tp1_enabled: bool | None = None,
    local_tp2_enabled: bool | None = None,
    ownership_timestamp_value: str = "",
    tp_owner_timestamp_value: str = "",
    partials_enabled_at_entry: bool | None = None,
    requested_leverage: int | None = None,
    exchange_configured_leverage: int | None = None,
    position_effective_leverage: int | None = None,
    leverage_truth_status: str = "PAPER_LOCAL",
    leverage_truth_timestamp: str = "",
    leverage_truth_source: str = "",
    leverage_truth_mismatch: bool = False,
    requested_vs_configured_mismatch: bool = False,
    configured_vs_effective_mismatch: bool = False,
) -> ExitPlan:
    partial = (
        bool(_s.get_val("partial_exit_enabled", True))
        if partial_mode_override is None
        else bool(partial_mode_override)
    )
    mode_at_entry = normalize_tp_mode(tp_mode_at_entry or "reference_only")
    raw_owner = str(tp_owner or "").strip().upper()
    if raw_owner:
        owner = normalize_persisted_owner(raw_owner)
    else:
        owner = derive_tp_owner(mode_at_entry, partial_enabled=partial)
    owner_timestamp = str(
        tp_owner_timestamp_value or ownership_timestamp_value or ownership_timestamp()
    )
    default_tp1, default_tp2 = owner_flags(owner, partial_enabled=partial)
    resolved_lifecycle_id = _resolve_position_lifecycle_id(
        coin,
        order_id=order_id,
        client_order_id=client_order_id,
        provided=position_lifecycle_id,
    )
    opened_ts = time.time()
    opened_iso = datetime.fromtimestamp(opened_ts, timezone.utc).isoformat()
    plan = ExitPlan(
        coin=coin, direction=direction, engine_name=engine_name,
        entry_price=entry_price, size_usdt=size_usdt, atr_val=atr_val,
        hard_sl=hard_sl, tp1_price=tp1_price, tp2_price=tp2_price,
        initial_size_usdt=size_usdt,
        leverage=int(position_effective_leverage or exchange_configured_leverage or leverage or 1),
        requested_leverage=int(requested_leverage or leverage or 1),
        exchange_configured_leverage=(int(exchange_configured_leverage) if exchange_configured_leverage else None),
        position_effective_leverage=(int(position_effective_leverage) if position_effective_leverage else None),
        leverage_truth_status=str(leverage_truth_status or "PAPER_LOCAL"),
        leverage_truth_timestamp=str(leverage_truth_timestamp or ""),
        leverage_truth_source=str(leverage_truth_source or ""),
        leverage_truth_mismatch=bool(leverage_truth_mismatch),
        requested_vs_configured_mismatch=bool(requested_vs_configured_mismatch),
        configured_vs_effective_mismatch=bool(configured_vs_effective_mismatch),
        vote_score=vote_score,
        created_at=opened_ts,
        peak_price=entry_price, tsl_price=hard_sl,
        order_id=order_id,
        entry_client_order_id=str(client_order_id or ""),
        position_lifecycle_id=resolved_lifecycle_id,
        partial_mode=partial,
        tp_owner=owner,
        tp_mode_at_entry=mode_at_entry,
        exchange_tp_algo_id=str(exchange_tp_algo_id or ""),
        local_tp1_enabled=default_tp1 if local_tp1_enabled is None else bool(local_tp1_enabled),
        local_tp2_enabled=default_tp2 if local_tp2_enabled is None else bool(local_tp2_enabled),
        ownership_timestamp=owner_timestamp,
        tp_owner_timestamp=owner_timestamp,
        partials_enabled_at_entry=partial if partials_enabled_at_entry is None else bool(partials_enabled_at_entry),
        macro_runner_trust_mult=float(macro_runner_trust_mult or 1.0),
        expected_risk_usdt=float(expected_risk_usdt or 0.0),
        risk_cap_usdt=float(risk_cap_usdt or 0.0),
        risk_ceiling_action=str(risk_ceiling_action or ""),
        paper_validation=dict(paper_validation or {}),
        partial_exit_state=dict(partial_exit_state or {}),
        partial_exit_events=list(partial_exit_events or [])[-100:],
        entry_contracts=float(entry_contracts or 0.0),
        confirmed_remaining_contracts=float(confirmed_remaining_contracts or 0.0),
        exchange_position_timestamp=str(exchange_position_timestamp or ""),
        exposure_truth_status=str(exposure_truth_status or EXPOSURE_ACTIVE),
        closure_handoff_at=str(closure_handoff_at or ""),
        partial_attribution_status=str(partial_attribution_status or "NONE"),
        partial_realized_pnl_usdt=float(partial_realized_pnl_usdt or 0.0),
        partial_fees_usdt=float(partial_fees_usdt or 0.0),
        entry_fee_usdt=(None if entry_fee_usdt is None else float(entry_fee_usdt)),
        funding_usdt=(None if funding_usdt is None else float(funding_usdt)),
        funding_status=str(funding_status or "UNKNOWN"),
        entry_order_ids=[str(order_id)] if order_id else [],
        first_fill_at=str((paper_validation or {}).get("first_fill_at", "") or opened_iso),
        last_entry_fill_at=str((paper_validation or {}).get("last_entry_fill_at", "") or opened_iso),
        exit_profile=_exit_profiles.profile_for_engine(engine_name),
        mfe_price=float(entry_price or 0.0),
        mae_price=float(entry_price or 0.0),
        desired_stop_price=float(hard_sl or 0.0),
        local_effective_stop_price=float(hard_sl or 0.0),
        exchange_confirmed_stop_price=float(hard_sl or 0.0) if not _paper_trading_enabled() else 0.0,
        exchange_protection_status="STOP_CONFIRMED" if not _paper_trading_enabled() else "PAPER_LOCAL",
    )
    _record_exit_transition(
        plan,
        "ENTRY",
        entry_price=round(float(entry_price or 0.0), 12),
        contracts_or_notional=round(float(size_usdt or 0.0), 8),
    )
    _plans[coin] = plan
    logger.info(
        "ExitPlan [%s] %s %s entry=%.6f SL=%.6f TP1=%.6f TP2=%.6f lev=%dx",
        engine_name, coin, direction, entry_price, hard_sl,
        tp1_price, tp2_price, leverage,
    )
    return plan


def remove(coin: str) -> None:
    _plans.pop(coin, None)

def get_plan(coin: str) -> Optional[ExitPlan]:
    return _plans.get(coin)

def get_all_plans() -> dict:
    return _plans


async def start_monitor(okx_client, engine_registry: dict = None) -> None:
    global _monitor_task, _sync_task, _okx_client, _engine_registry, _protection_manager
    _okx_client = okx_client
    _protection_manager = ProtectionManager(okx_client, strict_intent_persistence=True)
    if engine_registry:
        _engine_registry = engine_registry
    if _monitor_task and not _monitor_task.done():
        return
    _monitor_task = asyncio.create_task(_monitor_loop(), name="exit_monitor")
    if _paper_trading_enabled():
        _sync_task = None
        logger.info("Exit manager monitor started in PAPER mode; exchange sync disabled.")
    else:
        _sync_task = asyncio.create_task(_exchange_sync_loop(), name="exit_sync")
        logger.info("Exit manager monitor and exchange sync started.")


async def stop_monitor() -> None:
    """Stop background monitor/sync tasks without mutating positions or orders."""
    global _monitor_task, _sync_task
    tasks = [t for t in (_monitor_task, _sync_task) if t and not t.done()]
    for task in tasks:
        task.cancel()
    for task in tasks:
        try:
            await task
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            logger.debug("Exit manager task shutdown: %s", exc)
    _monitor_task = None
    _sync_task = None
    logger.info("Exit manager monitor and exchange sync stopped.")


# ── Monitor loop (price-based SL/TP backup) ───────────────────────────────────

async def _current_exchange_contracts(coin: str, plan: ExitPlan) -> float | None:
    """Return the current exchange-confirmed remaining quantity.

    A dynamic protection mutation must never reuse the original notional after
    a partial fill/partial close. ``None`` means the read was not authoritative
    or the position was not present; callers fail closed and retain old stops.
    """
    if not _okx_client:
        return None
    try:
        positions = await _okx_client.get_positions()
    except Exception as exc:
        logger.warning("STOP_REPROTECTION position read unavailable %s: %s", coin, exc)
        return None
    target = str(coin or "").upper()
    wanted_side = "long" if str(plan.direction or "").lower() == "long" else "short"
    for row in list(positions or []):
        inst = str(row.get("instId", "") or "").upper()
        if not (inst == target or inst.startswith(f"{target}-")):
            continue
        pos_side = str(row.get("posSide", "") or "").lower()
        if pos_side and pos_side != wanted_side:
            continue
        try:
            contracts = abs(float(row.get("pos", row.get("availPos", 0.0)) or 0.0))
        except (TypeError, ValueError):
            return None
        return contracts if contracts > 0 else None
    return None


def _set_exchange_exposure_truth(
    coin: str,
    plan: ExitPlan,
    contracts: float,
    *,
    timestamp: str | None = None,
    attribution_status: str | None = None,
) -> None:
    """Update current exchange exposure without claiming economic attribution."""
    observed_at = str(timestamp or datetime.now(timezone.utc).isoformat())
    quantity = max(float(contracts or 0.0), 0.0)
    plan.confirmed_remaining_contracts = quantity
    plan.exchange_position_timestamp = observed_at
    if quantity > 0.0:
        if float(plan.initial_size_usdt or 0.0) <= 0.0:
            plan.initial_size_usdt = float(plan.size_usdt or 0.0)
        plan.exposure_truth_status = EXPOSURE_ACTIVE
        plan.closure_handoff_at = ""
        instrument = _partial_instrument(coin)
        if instrument is not None and plan.entry_price > 0:
            plan.size_usdt = round(instrument.contracts_to_notional(quantity, plan.entry_price), 8)
        _refresh_remaining_pct(plan)
        if attribution_status:
            plan.partial_attribution_status = str(attribution_status)
        return
    plan.size_usdt = 0.0
    plan.remaining_pct = 0.0
    plan.exposure_truth_status = EXCHANGE_CLOSED_AWAITING_RECONCILIATION
    plan.closure_handoff_at = observed_at
    plan.partial_attribution_status = str(attribution_status or "AWAITING_RECONCILIATION")


async def _reprotect_if_needed(coin: str, plan: ExitPlan) -> None:
    """Synchronize a locally improved stop to the exchange, fail closed."""
    if _paper_trading_enabled() or _protection_manager is None:
        return
    mutation_allowed, mutation_reason = _local_mutation_authorized(coin)
    if not mutation_allowed:
        logger.warning("STOP_REPROTECTION skipped %s: %s", coin, mutation_reason)
        return
    if plan.exposure_truth_status == EXCHANGE_CLOSED_AWAITING_RECONCILIATION:
        logger.info("STOP_REPROTECTION skipped %s: exchange closure handoff is pending.", coin)
        return
    desired = float(plan.desired_stop_price or plan.hard_sl or 0.0)
    confirmed = float(plan.exchange_confirmed_stop_price or 0.0)
    if desired <= 0 or (confirmed > 0 and desired == confirmed):
        return
    now = time.time()
    if now - float(plan.last_reprotection_attempt_ts or 0.0) < 5.0:
        return
    prior_status = str(plan.exchange_protection_status or "").upper()
    prior_intent = dict(plan.protection_intent or {})
    if (
        (prior_status == REPROTECTION_FAILED or str(plan.protection_lifecycle or "").upper() == REPROTECTION_FAILED)
        and str(prior_intent.get("reason", "")) == "dynamic_reprotection"
        and abs(float(prior_intent.get("desired_stop_price", 0.0) or 0.0) - desired) <= 1e-12
    ):
        # A definitive rejection is terminal for this logical transaction.
        # Do not re-enter read recovery or repost the same payload each cycle.
        return
    allow_mutation = prior_status not in {
        REPROTECTION_PENDING,
        REPROTECTION_UNKNOWN,
        REPROTECTION_FAILED,
    } and (
        int(plan.reprotection_attempts or 0) == 0
        or str(plan.protection_lifecycle or "").upper() == "PROTECTION_CONFIRMED_NOT_FOUND"
    )
    plan.last_reprotection_attempt_ts = now
    plan.last_reprotection_attempt = datetime.fromtimestamp(now, timezone.utc).isoformat()
    plan.reprotection_attempts = int(plan.reprotection_attempts or 0) + 1
    plan.reprotection_evaluations = int(plan.reprotection_evaluations or 0) + 1
    if prior_status in {REPROTECTION_PENDING, REPROTECTION_UNKNOWN}:
        plan.reprotection_read_recoveries = int(plan.reprotection_read_recoveries or 0) + 1
    if allow_mutation:
        plan.reprotection_transactions = int(plan.reprotection_transactions or 0) + 1
    plan.exchange_protection_status = "STOP_REPROTECTION_REQUESTED"
    _record_exit_transition(
        plan,
        "STOP_REPROTECTION_REQUESTED",
        desired_stop_price=desired,
        attempt=plan.reprotection_attempts,
    )
    contracts = await _current_exchange_contracts(coin, plan)
    if not contracts:
        plan.exchange_protection_status = REPROTECTION_UNKNOWN
        plan.reprotection_reason = "exchange_position_unavailable"
        plan.reprotection_error = "current_exchange_quantity_unavailable"
        _record_exit_transition(plan, "STOP_REPROTECTION_UNKNOWN", reason=plan.reprotection_error)
        return
    _set_exchange_exposure_truth(
        coin,
        plan,
        contracts,
        attribution_status=plan.partial_attribution_status or "AUTHORITATIVE_EXPOSURE",
    )
    _persist_exit_state(coin, plan)
    try:
        result = await _protection_manager.reprotect(
            coin=coin,
            direction=plan.direction,
            current_contracts=contracts,
            desired_stop_price=desired,
            take_profit=(
                float(plan.tp2_price or 0.0)
                if plan.tp_owner == EXCHANGE_FULL else 0.0
            ),
            client_order_id=str(plan.entry_client_order_id or plan.order_id or ""),
            position_lifecycle_id=str(plan.position_lifecycle_id or ""),
            paper=False,
            allow_mutation=allow_mutation,
            tp_owner=plan.tp_owner,
        )
    except Exception as exc:
        logger.exception("STOP_REPROTECTION failed unexpectedly for %s", coin)
        result = None
        plan.exchange_protection_status = REPROTECTION_UNKNOWN
        plan.reprotection_reason = "exception"
        plan.reprotection_error = type(exc).__name__
        _record_exit_transition(plan, "STOP_REPROTECTION_UNKNOWN", reason=plan.reprotection_error)
        return

    plan.reprotection_reason = str(result.reason or "")
    plan.reprotection_error = str(result.error or "")
    if result.confirmed_stop_price > 0:
        plan.exchange_confirmed_stop_price = float(result.confirmed_stop_price)
        plan.local_effective_stop_price = float(result.confirmed_stop_price)
    if result.sl_algo_id or result.tp_algo_ids:
        plan.exchange_protection_algo_ids = [
            value for value in [result.sl_algo_id, *list(result.tp_algo_ids or [])] if value
        ]
    plan.protection_lifecycle = str(result.intent_lifecycle or result.reason or "")
    plan.protection_cleanup_pending = bool(result.cleanup_pending)
    if bool(getattr(result, "mutation_attempted", False)):
        plan.reprotection_post_attempts = int(plan.reprotection_post_attempts or 0) + 1
    if result.reason in {REPROTECTION_CONFIRMED, REPROTECTION_UNCHANGED, "PROTECTION_CONFIRMED"}:
        plan.exchange_protection_status = "STOP_CONFIRMED"
        plan.last_reprotection_success = datetime.now(timezone.utc).isoformat()
        plan.reprotection_attempts = 0
        plan.reprotection_confirmed = int(plan.reprotection_confirmed or 0) + 1
        _record_exit_transition(
            plan,
            "STOP_REPROTECTION_CONFIRMED" if result.reason == REPROTECTION_CONFIRMED else "STOP_REPROTECTION_UNCHANGED",
            confirmed_stop_price=float(plan.exchange_confirmed_stop_price or 0.0),
            contracts=contracts,
        )
        return
    if result.reason == "PROTECTION_CONFIRMED_CLEANUP_PENDING":
        plan.exchange_protection_status = "PROTECTION_CONFIRMED_CLEANUP_PENDING"
        _record_exit_transition(plan, "STOP_REPROTECTION_CLEANUP_PENDING", reason=plan.reprotection_error or plan.reprotection_reason)
        return
    if result.reason == REPROTECTION_FAILED:
        plan.reprotection_rejected = int(plan.reprotection_rejected or 0) + 1
        if result.protection_state == "PROTECTED" and result.confirmed_stop_price > 0:
            # The attempted replacement is terminally rejected, but the old
            # exchange stop remains the authoritative live protection.
            plan.exchange_protection_status = "STOP_CONFIRMED"
            _record_exit_transition(
                plan, "STOP_REPROTECTION_REJECTED_OLD_PROTECTION_RETAINED",
                confirmed_stop_price=float(result.confirmed_stop_price),
                reason=plan.reprotection_error or plan.reprotection_reason,
            )
            return
        plan.exchange_protection_status = REPROTECTION_FAILED
    elif result.reason in {REPROTECTION_PENDING, REPROTECTION_UNKNOWN, PROTECTION_SUBMIT_UNCERTAIN}:
        plan.reprotection_uncertain = int(plan.reprotection_uncertain or 0) + 1
        plan.exchange_protection_status = str(result.reason or REPROTECTION_UNKNOWN)
    else:
        plan.exchange_protection_status = str(result.reason or REPROTECTION_UNKNOWN)
    event = {
        REPROTECTION_PENDING: "STOP_REPROTECTION_DEFERRED",
        REPROTECTION_UNKNOWN: "STOP_REPROTECTION_UNKNOWN",
        REPROTECTION_FAILED: "STOP_REPROTECTION_FAILED",
    }.get(result.reason, "STOP_REPROTECTION_UNKNOWN")
    _record_exit_transition(plan, event, reason=plan.reprotection_error or plan.reprotection_reason)


def _persist_exit_state(coin: str, plan: ExitPlan) -> None:
    """Save live exit state so trailing / lock state survives restart."""
    try:
        from position_registry import update as _reg_update_fn, _registry as _reg
        if coin not in _reg:
            return
        prev = _reg[coin]
        if not bool(prev.get("tp1_done", False)) and plan.tp1_done:
            _record_exit_transition(plan, "TP1_ACTIVATED", price=round(float(plan.tp1_deferred_price or plan.entry_price or 0.0), 12))
        if not bool(prev.get("tp2_done", False)) and plan.tp2_done:
            _record_exit_transition(plan, "TP2_ACTIVATED", price=round(float(plan.tp2_deferred_price or plan.entry_price or 0.0), 12))
        if not bool(prev.get("tsl_armed", False)) and plan.tsl_armed:
            _record_exit_transition(plan, "TRAILING_ARMED", stop_price=round(float(plan.tsl_price or 0.0), 12))
        previous_lock = int(prev.get("profit_lock_level", 0) or 0)
        if int(plan.profit_lock_level or 0) > previous_lock:
            _record_exit_transition(plan, "PROFIT_LOCK_ACTIVATED", level=int(plan.profit_lock_level or 0))
        if not bool(prev.get("breakeven_moved", False)) and plan.breakeven_moved:
            _record_exit_transition(plan, "BE_MOVED", stop_price=round(float(plan.hard_sl or 0.0), 12))
        if (float(prev.get("tsl_price", 0) or 0) != plan.tsl_price
                or float(prev.get("peak_price", 0) or 0) != plan.peak_price
                or bool(prev.get("tsl_armed", False)) != plan.tsl_armed
                or float(prev.get("hard_sl_live", 0) or 0) != plan.hard_sl
                or bool(prev.get("breakeven_moved", False)) != plan.breakeven_moved
                or int(prev.get("profit_lock_level", 0) or 0) != plan.profit_lock_level
                or float(prev.get("remaining_pct", 0) or 0) != plan.remaining_pct
                or float(prev.get("initial_size_usdt", 0) or 0) != plan.initial_size_usdt
                or float(prev.get("realized_pnl_usdt", 0) or 0) != plan.realized_pnl_usdt
                or bool(prev.get("tp1_done", False)) != plan.tp1_done
                or bool(prev.get("tp2_done", False)) != plan.tp2_done
                or bool(prev.get("tp1_deferred", False)) != plan.tp1_deferred
                or bool(prev.get("tp2_deferred", False)) != plan.tp2_deferred
                or str(prev.get("lifecycle_phase", "") or "") != plan.lifecycle_phase
                or float(prev.get("mfe_roi", 0) or 0) != plan.mfe_roi
                or float(prev.get("mae_roi", 0) or 0) != plan.mae_roi
                or float(prev.get("mfe_price", 0) or 0) != plan.mfe_price
                or float(prev.get("mae_price", 0) or 0) != plan.mae_price
                 or float(prev.get("mfe_gross_pnl_usdt", 0) or 0) != plan.mfe_gross_pnl_usdt
                 or float(prev.get("mae_gross_pnl_usdt", 0) or 0) != plan.mae_gross_pnl_usdt
                 or float(prev.get("mfe_raw_move_pct", 0) or 0) != plan.mfe_raw_move_pct
                 or float(prev.get("mae_raw_move_pct", 0) or 0) != plan.mae_raw_move_pct
                 or int(prev.get("mfe_observed_leverage", 0) or 0) != plan.mfe_observed_leverage
                 or int(prev.get("mae_observed_leverage", 0) or 0) != plan.mae_observed_leverage
                 or float(prev.get("mfe_current_roi", 0) or 0) != plan.mfe_current_roi
                 or float(prev.get("mae_current_roi", 0) or 0) != plan.mae_current_roi
                 or int(prev.get("requested_leverage", 0) or 0) != plan.requested_leverage
                 or prev.get("exchange_configured_leverage") != plan.exchange_configured_leverage
                 or prev.get("position_effective_leverage") != plan.position_effective_leverage
                 or str(prev.get("leverage_truth_status", "UNKNOWN") or "UNKNOWN") != plan.leverage_truth_status
                 or str(prev.get("leverage_truth_timestamp", "") or "") != plan.leverage_truth_timestamp
                 or str(prev.get("leverage_truth_source", "") or "") != plan.leverage_truth_source
                 or bool(prev.get("leverage_truth_mismatch", False)) != plan.leverage_truth_mismatch
                 or bool(prev.get("requested_vs_configured_mismatch", False)) != plan.requested_vs_configured_mismatch
                 or bool(prev.get("configured_vs_effective_mismatch", False)) != plan.configured_vs_effective_mismatch
                or float(prev.get("runner_quality", 0) or 0) != plan.runner_quality
                or float(prev.get("exhaustion_score", 0) or 0) != plan.exhaustion_score
                or bool(prev.get("near_floor_tp_earned", False)) != plan.near_floor_tp_earned
                or float(prev.get("near_floor_tp_ratio", 0) or 0) != plan.near_floor_tp_ratio
                or str(prev.get("last_protection_reason", "") or "") != plan.last_protection_reason
                or float(prev.get("desired_stop_price", 0.0) or 0.0) != plan.desired_stop_price
                or float(prev.get("local_effective_stop_price", 0.0) or 0.0) != plan.local_effective_stop_price
                or float(prev.get("exchange_confirmed_stop_price", 0.0) or 0.0) != plan.exchange_confirmed_stop_price
                or str(prev.get("exchange_protection_status", "") or "") != plan.exchange_protection_status
                or list(prev.get("exchange_protection_algo_ids", []) or []) != list(plan.exchange_protection_algo_ids or [])
                or str(prev.get("reprotection_reason", "") or "") != plan.reprotection_reason
                or str(prev.get("reprotection_error", "") or "") != plan.reprotection_error
                 or int(prev.get("reprotection_attempts", 0) or 0) != plan.reprotection_attempts
                 or int(prev.get("reprotection_evaluations", 0) or 0) != plan.reprotection_evaluations
                 or int(prev.get("reprotection_read_recoveries", 0) or 0) != plan.reprotection_read_recoveries
                 or int(prev.get("reprotection_transactions", 0) or 0) != plan.reprotection_transactions
                 or int(prev.get("reprotection_post_attempts", 0) or 0) != plan.reprotection_post_attempts
                 or int(prev.get("reprotection_confirmed", 0) or 0) != plan.reprotection_confirmed
                 or int(prev.get("reprotection_rejected", 0) or 0) != plan.reprotection_rejected
                 or int(prev.get("reprotection_uncertain", 0) or 0) != plan.reprotection_uncertain
                 or float(prev.get("size_usdt", 0) or 0) != plan.size_usdt
                 or str(prev.get("tp_owner", LOCAL_STAGED) or LOCAL_STAGED) != plan.tp_owner
                 or str(prev.get("tp_mode_at_entry", "") or "") != plan.tp_mode_at_entry
                 or str(prev.get("exchange_tp_algo_id", "") or "") != plan.exchange_tp_algo_id
                 or bool(prev.get("local_tp1_enabled", True)) != plan.local_tp1_enabled
                 or bool(prev.get("local_tp2_enabled", True)) != plan.local_tp2_enabled
                 or str(prev.get("ownership_timestamp", "") or "") != plan.ownership_timestamp
                 or str(prev.get("tp_owner_timestamp", prev.get("ownership_timestamp", "")) or "") != plan.tp_owner_timestamp
                 or bool(prev.get("partials_enabled_at_entry", plan.partials_enabled_at_entry)) != plan.partials_enabled_at_entry
                 or dict(prev.get("partial_exit_state", {}) or {}) != dict(plan.partial_exit_state or {})
                 or list(prev.get("partial_exit_events", []) or []) != list(plan.partial_exit_events or [])
                 or float(prev.get("entry_contracts", 0.0) or 0.0) != float(plan.entry_contracts or 0.0)
                 or float(prev.get("confirmed_remaining_contracts", 0.0) or 0.0) != float(plan.confirmed_remaining_contracts or 0.0)
                 or str(prev.get("exchange_position_timestamp", "") or "") != plan.exchange_position_timestamp
                 or str(prev.get("exposure_truth_status", EXPOSURE_ACTIVE) or EXPOSURE_ACTIVE) != plan.exposure_truth_status
                 or str(prev.get("closure_handoff_at", "") or "") != plan.closure_handoff_at
                 or str(prev.get("partial_attribution_status", "NONE") or "NONE") != plan.partial_attribution_status
                  or float(prev.get("partial_realized_pnl_usdt", 0.0) or 0.0) != float(plan.partial_realized_pnl_usdt or 0.0)
                  or float(prev.get("partial_fees_usdt", 0.0) or 0.0) != float(plan.partial_fees_usdt or 0.0)
                  or prev.get("entry_fee_usdt") != plan.entry_fee_usdt
                  or prev.get("funding_usdt") != plan.funding_usdt
                  or str(prev.get("funding_status", "UNKNOWN") or "UNKNOWN") != str(plan.funding_status or "UNKNOWN")
                 or dict(prev.get("full_close_economics", {}) or {}) != dict(plan.full_close_economics or {})):
            _reg_update_fn(coin, size_usdt=plan.size_usdt, stop_loss=plan.hard_sl, metadata={
                "tsl_price":  plan.tsl_price,
                "peak_price": plan.peak_price,
                "tsl_armed":  plan.tsl_armed,
                "hard_sl_live": plan.hard_sl,
                "breakeven_moved": plan.breakeven_moved,
                "profit_lock_level": plan.profit_lock_level,
                "remaining_pct": plan.remaining_pct,
                "initial_size_usdt": plan.initial_size_usdt,
                "realized_pnl_usdt": plan.realized_pnl_usdt,
                "tp1_done": plan.tp1_done,
                "tp2_done": plan.tp2_done,
                "tp1_deferred": plan.tp1_deferred,
                "tp2_deferred": plan.tp2_deferred,
                "tp1_deferred_price": plan.tp1_deferred_price,
                "tp2_deferred_price": plan.tp2_deferred_price,
                "deferred_partial_pnl_usdt": plan.deferred_partial_pnl_usdt,
                 "lifecycle_phase": plan.lifecycle_phase,
                 "position_lifecycle_id": str(plan.position_lifecycle_id or ""),
                "mfe_roi": plan.mfe_roi,
                "mae_roi": plan.mae_roi,
                "mfe_price": plan.mfe_price,
                "mae_price": plan.mae_price,
                "mfe_gross_pnl_usdt": plan.mfe_gross_pnl_usdt,
                 "mae_gross_pnl_usdt": plan.mae_gross_pnl_usdt,
                 "mfe_raw_move_pct": plan.mfe_raw_move_pct,
                 "mae_raw_move_pct": plan.mae_raw_move_pct,
                  "mfe_observed_leverage": plan.mfe_observed_leverage,
                  "mae_observed_leverage": plan.mae_observed_leverage,
                  "mfe_current_roi": plan.mfe_current_roi,
                  "mae_current_roi": plan.mae_current_roi,
                 "requested_leverage": plan.requested_leverage,
                 "exchange_configured_leverage": plan.exchange_configured_leverage,
                 "position_effective_leverage": plan.position_effective_leverage,
                 "leverage_truth_status": plan.leverage_truth_status,
                 "leverage_truth_timestamp": plan.leverage_truth_timestamp,
                 "leverage_truth_source": plan.leverage_truth_source,
                 "leverage_truth_mismatch": plan.leverage_truth_mismatch,
                 "requested_vs_configured_mismatch": plan.requested_vs_configured_mismatch,
                 "configured_vs_effective_mismatch": plan.configured_vs_effective_mismatch,
                "exit_timeline": list(plan.exit_timeline)[-100:],
                "runner_quality": plan.runner_quality,
                "runner_core_pct": plan.runner_core_pct,
                "exhaustion_score": plan.exhaustion_score,
                "exhaustion_reasons": plan.exhaustion_reasons,
                "economic_minimum": dict(plan.economic_minimum or {}),
                "runner_opportunity": dict(plan.runner_opportunity or {}),
                "near_floor_tp_earned": bool(plan.near_floor_tp_earned),
                "near_floor_tp_ratio": plan.near_floor_tp_ratio,
                "near_floor_tp_grace_until": plan.near_floor_tp_grace_until,
                "invalidation_diagnostics": dict(plan.invalidation_diagnostics or {}),
                "post_invalidation_telemetry": dict(plan.post_invalidation_telemetry or {}),
                "last_protection_reason": plan.last_protection_reason,
                "desired_stop_price": plan.desired_stop_price,
                "local_effective_stop_price": plan.local_effective_stop_price,
                "exchange_confirmed_stop_price": plan.exchange_confirmed_stop_price,
                "exchange_protection_status": plan.exchange_protection_status,
                "exchange_protection_algo_ids": list(plan.exchange_protection_algo_ids or []),
                "last_reprotection_attempt": plan.last_reprotection_attempt,
                "last_reprotection_success": plan.last_reprotection_success,
                "reprotection_reason": plan.reprotection_reason,
                "reprotection_error": plan.reprotection_error,
                 "reprotection_attempts": plan.reprotection_attempts,
                 "reprotection_evaluations": plan.reprotection_evaluations,
                 "reprotection_read_recoveries": plan.reprotection_read_recoveries,
                 "reprotection_transactions": plan.reprotection_transactions,
                 "reprotection_post_attempts": plan.reprotection_post_attempts,
                 "reprotection_confirmed": plan.reprotection_confirmed,
                 "reprotection_rejected": plan.reprotection_rejected,
                 "reprotection_uncertain": plan.reprotection_uncertain,
                "macro_runner_trust_mult": float(getattr(plan, "macro_runner_trust_mult", 1.0) or 1.0),
                "expected_risk_usdt": float(getattr(plan, "expected_risk_usdt", 0.0) or 0.0),
                "risk_cap_usdt": float(getattr(plan, "risk_cap_usdt", 0.0) or 0.0),
                 "risk_ceiling_action": str(getattr(plan, "risk_ceiling_action", "") or ""),
                 "tp_owner": plan.tp_owner,
                 "tp_mode_at_entry": plan.tp_mode_at_entry,
                 "exchange_tp_algo_id": plan.exchange_tp_algo_id,
                 "local_tp1_enabled": bool(plan.local_tp1_enabled),
                 "local_tp2_enabled": bool(plan.local_tp2_enabled),
                 "ownership_timestamp": plan.ownership_timestamp,
                 "tp_owner_timestamp": plan.tp_owner_timestamp,
                 "partials_enabled_at_entry": bool(plan.partials_enabled_at_entry),
                 "partial_exit_state": dict(plan.partial_exit_state or {}),
                 "partial_exit_events": list(plan.partial_exit_events or [])[-100:],
                 "entry_contracts": float(plan.entry_contracts or 0.0),
                 "confirmed_remaining_contracts": float(plan.confirmed_remaining_contracts or 0.0),
                 "exchange_position_timestamp": plan.exchange_position_timestamp,
                 "exposure_truth_status": plan.exposure_truth_status,
                 "closure_handoff_at": plan.closure_handoff_at,
                 "partial_attribution_status": plan.partial_attribution_status,
                  "partial_realized_pnl_usdt": float(plan.partial_realized_pnl_usdt or 0.0),
                  "partial_fees_usdt": float(plan.partial_fees_usdt or 0.0),
                  "entry_fee_usdt": plan.entry_fee_usdt,
                  "funding_usdt": plan.funding_usdt,
                  "funding_status": str(plan.funding_status or "UNKNOWN"),
                  "full_close_economics": dict(plan.full_close_economics or {}),
              })
    except Exception:
        pass  # non-critical — best-effort persistence


async def _monitor_loop() -> None:
    """
    Polls latest prices every 5 seconds and checks SL/TP conditions.
    Exchange-side stops remain crash-safe; profit-taking ownership is selected
    per position and local staged management never competes with EXCHANGE_FULL.
    The monitor handles: trailing SL, partial exits, breakeven moves.
    """
    while True:
        try:
            _update_post_invalidation_watches()
            for coin in list(_plans.keys()):
                try:
                    await _check_plan(coin)
                    plan = _plans.get(coin)
                    if plan:
                        _persist_exit_state(coin, plan)
                except Exception as e:
                    logger.error("Exit monitor _check_plan %s: %s", coin, e)
        except Exception as e:
            logger.error("Exit monitor loop: %s", e)
        await asyncio.sleep(5)


def _update_post_invalidation_watches() -> None:
    if not _post_invalidation_watch:
        return
    now = time.time()
    for coin, watch in list(_post_invalidation_watch.items()):
        if now > float(watch.get("expires_at", 0.0) or 0.0):
            _persist_post_invalidation_observation(watch)
            _post_invalidation_watch.pop(coin, None)
            continue
        price = latest_price(coin)
        if not price or price <= 0:
            continue
        direction = str(watch.get("direction", "") or "").lower()
        if direction == "short":
            watch["max_favorable_price"] = min(float(watch.get("max_favorable_price", price) or price), float(price))
            watch["max_adverse_price"] = max(float(watch.get("max_adverse_price", price) or price), float(price))
        else:
            watch["max_favorable_price"] = max(float(watch.get("max_favorable_price", price) or price), float(price))
            watch["max_adverse_price"] = min(float(watch.get("max_adverse_price", price) or price), float(price))
        watch["last_price"] = float(price)
        watch["samples"] = int(watch.get("samples", 0) or 0) + 1
        outcome = _la.evaluate_post_invalidation_outcome(
            direction=direction,
            entry_price=float(watch.get("entry_price", 0.0) or 0.0),
            invalidation_price=float(watch.get("invalidation_price", 0.0) or 0.0),
            current_price=float(price),
            max_favorable_price=float(watch.get("max_favorable_price", price) or price),
            max_adverse_price=float(watch.get("max_adverse_price", price) or price),
            size_usdt=float(watch.get("size_usdt", 0.0) or 0.0),
        ).to_dict()
        outcome["samples"] = int(watch.get("samples", 0) or 0)
        watch["outcome"] = outcome


def _persist_post_invalidation_observation(watch: dict) -> None:
    trade_id = str(watch.get("trade_id", "") or "")
    outcome = dict(watch.get("outcome", {}) or {})
    if not trade_id or not outcome:
        return
    try:
        from pnl_reconciler import reconciler as _pnl_reconciler

        _pnl_reconciler.update_record(
            trade_id,
            post_exit_continuation=dict(outcome),
            premature_exit_observed=str(outcome.get("classification", "")) == "false_positive",
            post_exit_observation_finalized_at=datetime.now(timezone.utc).isoformat(),
        )
    except Exception:
        logger.debug("Post-invalidation observation persistence failed for %s", trade_id, exc_info=True)


async def _check_plan(coin: str) -> None:
    plan = _plans.get(coin)
    if not plan:
        return
    mutation_allowed, mutation_reason = _local_mutation_authorized(coin)
    if not mutation_allowed:
        logger.warning("Exit plan %s skipped: local mutation is not authorized (%s)", coin, mutation_reason)
        return
    if plan.exposure_truth_status == EXCHANGE_CLOSED_AWAITING_RECONCILIATION:
        logger.debug("Exit plan %s is non-actionable pending exchange closure reconciliation.", coin)
        return
    price = latest_price(coin)
    if not price:
        return

    is_long = plan.direction == "long"
    if is_long:
        plan.peak_price = max(plan.peak_price, price)
    else:
        plan.peak_price = price if plan.peak_price == 0 else min(plan.peak_price, price)
    try:
        _cont_active, _cont_score, _cont_reason = _continuation_active(plan, price, is_long)
        _update_lifecycle_telemetry(plan, price, is_long, continuation_score=_cont_score, continuation_reason=_cont_reason)
    except Exception:
        pass

    if _paper_trading_enabled():
        await _check_paper_plan(coin, plan, price, is_long)
        return

    milestone_mode = bool(_s.get_val("secure_profit_mode_enabled", True))
    if milestone_mode:
        did_action = await _apply_milestone_profit_system(coin, plan, price, is_long)
        await _reprotect_if_needed(coin, plan)
        if did_action:
            _persist_exit_state(coin, plan)
            return
    else:
        if _s.get_val("profit_lock_enabled", True):
            _apply_legacy_profit_lock(coin, plan, price, is_long)

        if _s.get_val("tsl_enabled", False):
            await _update_tsl(plan, price, is_long)
            tsl_hit = plan.tsl_armed and (
                (is_long and price <= plan.tsl_price) or
                (not is_long and price >= plan.tsl_price)
            )
            if tsl_hit:
                if await _timeout_close(plan, coin, price, "tsl"):
                    return

    await _reprotect_if_needed(coin, plan)
    active_sl = float(plan.local_effective_stop_price or 0.0)
    if active_sl > 0:
        sl_hit = (is_long and price <= active_sl) or (not is_long and price >= active_sl)
        if sl_hit:
            if await _timeout_close(plan, coin, price, "sl"):
                return
        else:
            plan.sl_breach_time = 0.0
    else:
        plan.sl_breach_time = 0.0

    if not milestone_mode:
        # Legacy fixed-price partials (kept for backwards compatibility only).
        if plan.tp_owner != LOCAL_STAGED:
            # Ownership only suppresses local profit closes. Preserve the
            # existing non-TP safety exits for EXCHANGE_FULL and NO_PROFIT_TP.
            if await _maybe_stale_exit(coin, plan, price):
                return
            if await _maybe_early_invalidation_exit(coin, plan, price, is_long):
                return
            return
        if not plan.partial_mode:
            tp_hit = plan.tp1_price > 0 and (
                (is_long and price >= plan.tp1_price) or (not is_long and price <= plan.tp1_price)
            )
            if tp_hit:
                await _do_close(coin, price, "tp", plan)
            return

        tp1_close_pct = min(max(float(_s.get_val("partial_tp1_close_pct", 35.0) or 35.0), 0.0), 100.0)
        tp2_close_pct = min(max(float(_s.get_val("partial_tp2_close_pct", 25.0) or 25.0), 0.0), 100.0)

        if not plan.tp1_done:
            tp1_hit = plan.tp1_price > 0 and (
                (is_long and price >= plan.tp1_price) or (not is_long and price <= plan.tp1_price)
            )
            if tp1_hit:
                close_pct = min(tp1_close_pct, 100.0)
                pnl = _calc_pnl(plan, price, close_pct)
                closed_notional = round(plan.size_usdt * close_pct / 100.0, 8)
                partial = await _execute_partial(coin, price, close_pct, stage="legacy_tp1")
                partial = _normalize_partial_result(partial, plan, price, close_pct)
                if not partial:
                    return
                closed_notional = float(partial.get("closed_notional", closed_notional) or 0.0)
                pnl = float(partial.get("gross_pnl_usdt", pnl) or 0.0)
                fee_est = float(partial.get("fees_usdt", 0.0) or 0.0)
                actual_pct = float(partial.get("verified_filled_contracts", 0.0) or 0.0) / max(float(partial.get("requested_contracts", 0.0) or 0.0), 1e-12) * close_pct
                plan.tp1_done = bool(partial.get("milestone_complete", False))
                await alert_take_profit(
                    coin, plan.entry_price, price,
                    abs((plan.tp1_price - plan.entry_price) / plan.entry_price * 100),
                    closed_notional,
                    leverage=plan.leverage,
                    realized_pnl_usdt=pnl,
                    fee_est_usdt=fee_est,
                )
                logger.info(
                    "TP1 %s @ %.6f $%.4f (%.0f%% requested, %.0f%% filled) lev=%dx state=%s",
                    coin, float(partial.get("fill_price", price) or price), pnl, close_pct, actual_pct, plan.leverage, partial.get("state", ""),
                )
                await _reprotect_if_needed(coin, plan)
                _persist_exit_state(coin, plan)
                return

        if plan.tp1_done and not plan.tp2_done:
            tp2_hit = plan.tp2_price > 0 and (
                (is_long and price >= plan.tp2_price) or (not is_long and price <= plan.tp2_price)
            )
            if tp2_hit:
                close_pct = min(tp2_close_pct, 100.0)
                pnl = _calc_pnl(plan, price, close_pct)
                closed_notional = round(plan.size_usdt * close_pct / 100.0, 8)
                partial = await _execute_partial(coin, price, close_pct, stage="legacy_tp2")
                partial = _normalize_partial_result(partial, plan, price, close_pct)
                if not partial:
                    return
                closed_notional = float(partial.get("closed_notional", closed_notional) or 0.0)
                pnl = float(partial.get("gross_pnl_usdt", pnl) or 0.0)
                fee_est = float(partial.get("fees_usdt", 0.0) or 0.0)
                actual_pct = float(partial.get("verified_filled_contracts", 0.0) or 0.0) / max(float(partial.get("requested_contracts", 0.0) or 0.0), 1e-12) * close_pct
                plan.tp2_done = bool(partial.get("milestone_complete", False))
                await alert_take_profit(
                    coin,
                    plan.entry_price,
                    price,
                    abs((plan.tp2_price - plan.entry_price) / plan.entry_price * 100),
                    closed_notional,
                    leverage=plan.leverage,
                    realized_pnl_usdt=pnl,
                    fee_est_usdt=fee_est,
                )
                logger.info(
                    "TP2 %s @ %.6f $%.4f (%.0f%% of live size) lev=%dx",
                    coin, price, pnl, close_pct, plan.leverage,
                )
                await _reprotect_if_needed(coin, plan)
                _persist_exit_state(coin, plan)

        if plan.tp1_done and plan.size_usdt > 0 and plan.atr_val > 0:
            trail = plan.atr_val * float(_s.get_val("trailing_atr_mult", 2.0))
            if is_long:
                new_sl = price - trail
                if new_sl > plan.hard_sl:
                    plan.hard_sl = plan.tsl_price = round(new_sl, 8)
            else:
                new_sl = price + trail
                if new_sl < plan.hard_sl:
                    plan.hard_sl = plan.tsl_price = round(new_sl, 8)

    if await _maybe_stale_exit(coin, plan, price):
        return


async def _check_paper_plan(coin: str, plan: ExitPlan, price: float, is_long: bool) -> bool:
    """
    Paper-mode-native exit evaluation.

    Paper positions do not exist on OKX, so exchange absence is not a signal.
    Close only when local price polling reaches the plan's own SL/TP/trailing
    boundaries.
    """
    if not plan or price <= 0:
        return False

    if _s.get_val("tsl_enabled", True):
        await _update_tsl(plan, price, is_long)

    trailing_hit = bool(plan.tsl_armed and plan.tsl_price > 0 and (
        (is_long and price <= plan.tsl_price) or
        ((not is_long) and price >= plan.tsl_price)
    ))
    if trailing_hit:
        await _do_paper_close(coin, price, "trailing_stop_hit", plan)
        return True

    sl_hit = bool(plan.hard_sl > 0 and (
        (is_long and price <= plan.hard_sl) or
        ((not is_long) and price >= plan.hard_sl)
    ))
    if sl_hit:
        await _do_paper_close(coin, price, "sl_hit", plan)
        return True

    tp2_hit = bool(plan.tp2_price > 0 and (
        (is_long and price >= plan.tp2_price) or
        ((not is_long) and price <= plan.tp2_price)
    ))
    if tp2_hit and plan.tp_owner in {LOCAL_STAGED, EXCHANGE_FULL}:
        await _do_paper_close(coin, price, "tp2_hit", plan)
        return True

    tp1_hit = bool(plan.tp1_price > 0 and (
        (is_long and price >= plan.tp1_price) or
        ((not is_long) and price <= plan.tp1_price)
    ))
    if tp1_hit and not plan.tp1_done and plan.tp_owner == LOCAL_STAGED:
        plan.tp1_done = True
        plan.last_protection_reason = "tp1_hit"
        logger.info("PAPER TP1 HIT %s @ %.6f lev=%dx", coin, price, plan.leverage)
        if not bool(plan.partial_mode):
            await _do_paper_close(coin, price, "tp1_hit", plan)
            return True
        _persist_exit_state(coin, plan)

    return False


def _minimum_net_capture_fee_multiple() -> float:
    """Return the scale-aware cost multiple used by lifecycle gates."""
    return max(float(_s.get_val("minimum_net_capture_fee_multiple", 1.0) or 0.0), 0.0)


def _lifecycle_economics(
    plan: ExitPlan,
    *,
    current_price: float | None,
    candidate_stop_price: float | None = None,
) -> PositionLifecycleEconomics:
    """Build economics from the current remainder, never from initial size."""
    raw_contracts = getattr(plan, "confirmed_remaining_contracts", None)
    if raw_contracts is None:
        remaining_contracts = None
    elif float(raw_contracts or 0.0) > 0.0:
        remaining_contracts = float(raw_contracts)
    elif _paper_trading_enabled() and float(getattr(plan, "entry_contracts", 0.0) or 0.0) > 0.0:
        remaining_contracts = float(plan.entry_contracts)
    else:
        remaining_contracts = None

    current_notional = float(plan.size_usdt or 0.0) if float(plan.size_usdt or 0.0) > 0.0 else None
    entry_fee = getattr(plan, "entry_fee_usdt", None)
    entry_fee_evidence = "AUTHORITATIVE"
    if entry_fee is None:
        entry_fee = (plan.paper_validation or {}).get("entry_fee_usdt")
        if entry_fee is None:
            entry_fee = (plan.paper_validation or {}).get("entry_fee")

    funding = getattr(plan, "funding_usdt", None)
    if funding is None:
        funding = (plan.paper_validation or {}).get("funding_usdt")
    funding_status = str(getattr(plan, "funding_status", "UNKNOWN") or "UNKNOWN")
    if funding_status.strip().upper() == "UNKNOWN":
        funding_status = str((plan.paper_validation or {}).get("funding_status", "UNKNOWN") or "UNKNOWN")

    # Paper positions have no exchange contract snapshot.  Their simulated
    # notional and estimated round-trip fee are authoritative for simulation;
    # live positions never take this fallback and remain UNKNOWN without
    # exchange-confirmed quantity/evidence.
    if _paper_trading_enabled() and current_notional is not None:
        if remaining_contracts is None:
            remaining_contracts = 1.0
        if entry_fee is None:
            entry_fee = fee_cost_usdt(current_notional, int(plan.leverage or 1)) / 2.0
        if funding_status.strip().upper() == "UNKNOWN":
            funding_status = "SUCCESS_EMPTY"
            funding = 0.0

    # A known position with no fill-level entry fee can still be evaluated
    # using the configured fee schedule.  Mark this explicitly as a
    # conservative estimate; never turn an absent fee into an unlabelled zero.
    if entry_fee is None and current_notional is not None:
        entry_fee = fee_cost_usdt(current_notional, int(plan.leverage or 1)) / 2.0
        entry_fee_evidence = ECONOMIC_EVIDENCE_ESTIMATED_CONSERVATIVE

    partial_fees: float | None = 0.0
    if plan.tp1_done or plan.tp2_done or float(plan.partial_fees_usdt or 0.0) != 0.0:
        partial_fees = float(plan.partial_fees_usdt or 0.0)
        if str(plan.partial_attribution_status or "NONE").upper() in {"UNKNOWN", "UNCERTAIN"}:
            partial_fees = None

    final_exit_fee = None
    slippage_allowance = None
    if current_notional is not None:
        final_exit_fee = fee_cost_usdt(current_notional, int(plan.leverage or 1)) / 2.0
        slippage_pct = float(_s.get_val("net_expectancy_default_slippage_pct", 0.03) or 0.03)
        slippage_allowance = current_notional * max(slippage_pct, 0.0) / 100.0

    funding_evidence = "AUTHORITATIVE"
    # Funding is a cost component, not exposure truth.  During a temporary
    # read degradation, use a bounded adverse estimate so a clearly profitable
    # protection candidate is not suppressed indefinitely.  The estimate is
    # explicit and intentionally conservative: the larger of 0.05% of the
    # current remainder and one half of the scheduled round-trip fee.
    if (
        current_notional is not None
        and funding_status.strip().upper() == "UNKNOWN"
    ):
        funding = -max(
            current_notional * 0.0005,
            float(final_exit_fee or 0.0),
        )
        funding_status = "ESTIMATED_CONSERVATIVE"
        funding_evidence = ECONOMIC_EVIDENCE_ESTIMATED_CONSERVATIVE

    return build_position_lifecycle_economics(
        original_entry_contracts=(
            None if float(getattr(plan, "entry_contracts", 0.0) or 0.0) <= 0.0
            else float(plan.entry_contracts)
        ),
        current_remaining_contracts=remaining_contracts,
        current_remaining_notional=current_notional,
        entry_price=float(plan.entry_price or 0.0),
        side=str(plan.direction or "long"),
        current_price=current_price,
        candidate_stop_price=candidate_stop_price,
        already_realized_partial_gross=float(plan.partial_realized_pnl_usdt or plan.realized_pnl_usdt or 0.0),
        known_entry_fee=entry_fee,
        known_partial_exit_fees=partial_fees,
        estimated_remaining_exit_fee=final_exit_fee,
        known_funding=funding,
        funding_status=funding_status,
        slippage_allowance=slippage_allowance,
        entry_fee_evidence=entry_fee_evidence,
        funding_evidence=funding_evidence,
    )


def _protection_candidate_economics(
    plan: ExitPlan,
    price: float,
    candidate_stop_price: float | None,
) -> tuple[PositionLifecycleEconomics, float | None, bool]:
    economics = _lifecycle_economics(plan, current_price=price, candidate_stop_price=candidate_stop_price)
    multiple = _minimum_net_capture_fee_multiple()
    return economics, required_cost_relative_net(economics, multiple), candidate_stop_is_economically_eligible(economics, multiple)


def _record_protection_economic_defer(
    plan: ExitPlan,
    *,
    stage: str,
    economics: PositionLifecycleEconomics,
    required_floor: float | None,
) -> None:
    payload = {
        "stage": str(stage),
        "candidate_stop_price": economics.candidate_stop_price,
        "candidate_stop_estimated_net_pnl": economics.candidate_stop_estimated_net_pnl,
        "required_net_capture": required_floor,
        "economic_evidence_status": economics.economic_evidence_status,
        "evidence_class": economics.evidence_class,
        "unknown_fields": list(economics.unknown_fields),
    }
    plan.economic_minimum = {**dict(plan.economic_minimum or {}), "protection_economics": payload}
    event = (
        "INSUFFICIENT_ECONOMICS"
        if economics.economic_evidence_status == ECONOMIC_EVIDENCE_INSUFFICIENT
        else "PROTECTION_ECONOMICS_DEFERRED"
    )
    _record_exit_transition(plan, event, **payload)


def _fee_secure_stop_price(plan: ExitPlan, price: float, is_long: bool) -> float | None:
    """Return the price at which known costs are recovered on the remainder."""
    economics = _lifecycle_economics(plan, current_price=price)
    if economics.economic_evidence_status not in {
        ECONOMIC_EVIDENCE_COMPLETE,
        ECONOMIC_EVIDENCE_ESTIMATED_CONSERVATIVE,
    }:
        return None
    notional = float(economics.current_remaining_notional or 0.0)
    if notional <= 0.0 or economics.entry_price <= 0.0:
        return None
    entry_fee = abs(float(economics.known_entry_fee or 0.0))
    partial_fees = abs(float(economics.known_partial_exit_fees or 0.0))
    exit_fee = abs(float(economics.estimated_remaining_exit_fee or 0.0))
    slippage = abs(float(economics.slippage_allowance or 0.0))
    funding = 0.0 if economics.funding_status == "SUCCESS_EMPTY" else float(economics.known_funding or 0.0)
    required_remaining_gross = max(
        entry_fee + partial_fees + exit_fee + slippage - funding
        - float(economics.already_realized_partial_gross or 0.0),
        0.0,
    )
    move = required_remaining_gross / notional
    return round(economics.entry_price * (1.0 + move if is_long else 1.0 - move), 8)


def _optional_full_close_decision(coin: str, price: float, reason: str, plan: ExitPlan):
    """Evaluate and persist one optional full-close economics decision."""
    economics = _lifecycle_economics(plan, current_price=price)
    decision = evaluate_optional_full_close(
        reason=reason,
        economics=economics,
        fee_multiple=_minimum_net_capture_fee_multiple(),
    )
    telemetry = {**economics.to_dict(), **decision.to_dict()}
    telemetry.update({
        "coin": str(coin or "").upper(),
        "reason": str(reason or ""),
        "minimum_net_capture_fee_multiple": _minimum_net_capture_fee_multiple(),
    })
    prior = dict(plan.full_close_economics or {})
    signature = {
        key: telemetry.get(key)
        for key in (
            "status", "reason", "estimated_net_capture", "required_min_net_capture", "unknown_fields",
            "economic_evidence_status", "evidence_class",
            "candidate_stop_estimated_net_pnl", "total_known_and_estimated_cost",
        )
    }
    prior_signature = {
        key: prior.get(key)
        for key in (
            "status", "reason", "estimated_net_capture", "required_min_net_capture", "unknown_fields",
            "economic_evidence_status", "evidence_class",
            "candidate_stop_estimated_net_pnl", "total_known_and_estimated_cost",
        )
    }
    if signature != prior_signature:
        telemetry["evaluated_at"] = datetime.now(timezone.utc).isoformat()
        plan.full_close_economics = telemetry
        plan.economic_minimum = {
            **dict(plan.economic_minimum or {}),
            "full_close_net_capture": dict(telemetry),
        }
        _record_exit_transition(
            plan,
            "INSUFFICIENT_ECONOMICS" if economics.economic_evidence_status == ECONOMIC_EVIDENCE_INSUFFICIENT else "OPTIONAL_FULL_CLOSE_DECISION",
            reason=str(reason or ""),
            status=decision.status,
            estimated_net_capture=decision.estimated_net_capture,
            required_min_net_capture=decision.required_min_net_capture,
        )
        _persist_exit_state(coin, plan)
        logger.info(
            "OPTIONAL FULL CLOSE %s reason=%s status=%s gross=$%.6f net=%s floor=$%.6f defer=%s",
            coin,
            reason,
            decision.status,
            float(decision.gross_capture or 0.0),
            "unknown" if decision.estimated_net_capture is None else f"${decision.estimated_net_capture:.6f}",
            decision.required_min_net_capture,
            decision.should_defer,
        )
    else:
        plan.full_close_economics = prior
    return decision


async def _maybe_defer_optional_full_close(coin: str, price: float, reason: str, plan: ExitPlan) -> bool:
    """Defer only known micro-profit optional targets, never safety exits."""
    decision = _optional_full_close_decision(coin, price, reason, plan)
    if decision.should_defer:
        current = dict(plan.full_close_economics or {})
        signature = tuple(current.get(key) for key in (
            "status", "reason", "estimated_net_capture", "required_min_net_capture",
            "unknown_fields", "total_known_and_estimated_cost",
        ))
        prior_signature = tuple(current.get("last_defer_log_signature", ()))
        if signature != prior_signature:
            logger.info(
                "DEFER_OPTIONAL %s reason=%s net=%s floor=$%.6f",
                coin,
                reason,
                "unknown" if decision.estimated_net_capture is None else f"${decision.estimated_net_capture:.6f}",
                decision.required_min_net_capture,
            )
            plan.full_close_economics["last_defer_log_signature"] = list(signature)
        return True
    return False


async def _maybe_stale_exit(coin: str, plan: ExitPlan, price: float) -> bool:
    """
    Stale exit guard:
      - avoid killing good trades
      - only exit when flat/no follow-through
      - never touch protected trades
    """
    if not bool(_s.get_val("stale_trade_exit_enabled", True)):
        return False
    try:
        stale_mins = float(_s.get_val("stale_trade_mins", 18) or 18)
    except Exception:
        stale_mins = 18.0
    if stale_mins <= 0:
        return False
    if float(getattr(plan, "created_at", 0.0) or 0.0) <= 0:
        return False
    age_min = (time.time() - float(plan.created_at)) / 60.0
    if age_min < stale_mins:
        return False

    if bool(_s.get_val("stale_exit_block_if_tp1_hit", True)) and bool(plan.tp1_done):
        return False
    if bool(_s.get_val("stale_exit_block_if_profit_lock_active", True)) and int(plan.profit_lock_level or 0) >= 2:
        return False

    pnl = _calc_pnl(plan, price, 100.0)
    if bool(_s.get_val("stale_exit_require_nonpositive", True)) and pnl > 0:
        return False

    move_pct = abs((price - plan.entry_price) / plan.entry_price * 100.0) if plan.entry_price > 0 else 0.0
    if move_pct >= 1.5:
        return False

    logger.info(
        "Stale trade exit: %s open %.0fmin, move %.2f%%, pnl=$%.4f",
        coin,
        age_min,
        move_pct,
        pnl,
    )
    try:
        _s.bump_counter("metric_stale_exits", 1)
    except Exception:
        pass
    await _do_close(coin, price, "stale_exit", plan)
    return True


async def _maybe_early_invalidation_exit(coin: str, plan: ExitPlan, price: float, is_long: bool) -> bool:
    if not bool(_s.get_val("early_invalidation_enabled", True)):
        return False
    if plan.tp1_done or int(plan.profit_lock_level or 0) > 0:
        plan.early_invalidation_breach_time = 0.0
        try:
            if bool(plan.tp1_done):
                plan.invalidation_diagnostics = {
                    "should_invalidate": False,
                    "final_reason": "tp1_protected_skip",
                    "tp1_done": True,
                }
        except Exception:
            pass
        return False
    created = float(getattr(plan, "created_at", 0.0) or 0.0)
    if created <= 0 or price <= 0 or float(plan.entry_price or 0.0) <= 0:
        return False
    age_sec = time.time() - created
    if _setting_bool("adaptive_early_invalidation_enabled", False):
        try:
            did = await _maybe_adaptive_early_invalidation_exit(coin, plan, price, is_long, age_sec)
            if did:
                return True
        except Exception:
            pass

    min_age = float(_s.get_val("early_invalidation_min_age_sec", 45.0) or 45.0)
    max_age = float(_s.get_val("early_invalidation_max_age_min", 12.0) or 12.0) * 60.0
    if age_sec < min_age or age_sec > max_age:
        plan.early_invalidation_breach_time = 0.0
        return False

    roi = _roi_pct(plan, price)
    adverse_roi = float(_s.get_val("early_invalidation_roi_pct", 1.8) or 1.8)
    if roi > -adverse_roi:
        plan.early_invalidation_breach_time = 0.0
        return False

    closes = list(get_closes(coin) or [])
    structure_failed, momentum_flipped, evidence = _structure_failure_signals(
        coin,
        closes,
        price=float(price),
        entry_price=float(plan.entry_price),
        is_long=bool(is_long),
    )
    if not structure_failed and abs(roi) < adverse_roi * 1.35:
        return False

    now = time.time()
    _confirm_setting = _s.get_val("early_invalidation_confirm_secs", 8.0)
    confirm_secs = float(8.0 if _confirm_setting is None else _confirm_setting)
    if confirm_secs > 0 and plan.early_invalidation_breach_time <= 0:
        plan.early_invalidation_breach_time = now
        try:
            plan.invalidation_diagnostics = {
                "should_invalidate": False,
                "invalidation_score": 0.0,
                "failure_score": 0.0,
                "patience_score": 0.0,
                "hold_time": round(float(age_sec or 0.0), 4),
                "required_hold_time": float(min_age or 0.0),
                "final_reason": "confirm_window",
                "evidence": dict(evidence or {}),
            }
        except Exception:
            pass
        logger.info(
            "EARLY INVALIDATION watch %s roi=%.2f%% age=%.0fs price=%.8f entry=%.8f structure_failed=%s momentum=%s evidence=%s",
            coin, roi, age_sec, price, plan.entry_price, structure_failed, momentum_flipped, (evidence or {}),
        )
        return False
    if confirm_secs > 0 and now - float(plan.early_invalidation_breach_time or 0.0) < confirm_secs:
        return False

    expected_sl = float(getattr(plan, "hard_sl", 0.0) or 0.0)
    expected_sl_pnl = _calc_pnl(plan, expected_sl, 100.0) if expected_sl > 0 else 0.0
    current_pnl = _calc_pnl(plan, price, 100.0)
    avoided = 0.0
    try:
        if expected_sl_pnl < 0 and current_pnl < 0:
            avoided = max(abs(expected_sl_pnl) - abs(current_pnl), 0.0)
    except Exception:
        avoided = 0.0

    try:
        plan.invalidation_diagnostics = {
            "should_invalidate": True,
            "invalidation_score": 0.0,
            "failure_score": 0.0,
            "patience_score": 0.0,
            "hold_time": round(float(age_sec or 0.0), 4),
            "required_hold_time": float(min_age or 0.0),
            "final_reason": "simple_structure_failure",
            "evidence": dict(evidence or {}),
            "avoided_loss_estimate_usdt": round(float(avoided), 6),
            "expected_sl_loss_usdt": round(float(expected_sl_pnl), 6),
            "current_pnl_usdt": round(float(current_pnl), 6),
        }
    except Exception:
        pass

    logger.info(
        "EARLY INVALIDATION close %s roi=%.2f%% age=%.0fs pnl=$%.4f avoided≈$%.4f structure_failed=%s momentum=%s evidence=%s",
        coin, roi, age_sec, current_pnl, avoided, structure_failed, momentum_flipped, (evidence or {}),
    )
    try:
        _s.bump_counter("metric_early_invalidations", 1)
    except Exception:
        pass
    await _do_close(coin, price, "early_invalidation", plan)
    return True


def _regime_tags_for_invalidation(plan: ExitPlan) -> tuple[bool, bool, bool, str]:
    regime = str(getattr(plan, "regime", "") or getattr(plan, "market_regime", "") or "").upper()
    choppy = any(x in regime for x in ("CHOP", "RANG", "SIDEWAYS"))
    low_liq = any(x in regime for x in ("LOW_LIQ", "ILLIQUID", "THIN"))
    trend = any(x in regime for x in ("TREND", "VOLATILE", "MOMENTUM"))
    return choppy, low_liq, trend, regime or "unknown"


def _symbol_direction_recent_losses(coin: str, direction: str) -> int:
    try:
        window = int(float(_s.get_val("symbol_direction_memory_decay_hours", 24.0) or 24.0) * 60.0)
        return int(_perf.get_recent_directional_loss_count(coin, direction, max(window, 1)) or 0)
    except Exception:
        return 0


def _build_adaptive_invalidation_decision(
    coin: str,
    plan: ExitPlan,
    price: float,
    is_long: bool,
    age_sec: float,
) -> _la.InvalidationDecision:
    roi = _roi_pct(plan, price)
    closes = list(get_closes(coin) or [])
    vols = list(get_volumes(coin) or [])
    structure_failed = False
    momentum_flipped = False
    struct_evidence: dict = {}
    volume_failed = False
    rejected_entry = False
    if len(closes) >= 4:
        structure_failed, momentum_flipped, struct_evidence = _structure_failure_signals(
            coin,
            closes,
            price=float(price),
            entry_price=float(plan.entry_price),
            is_long=bool(is_long),
        )
        if is_long:
            rejected_entry = max(closes[-3:]) >= float(plan.entry_price) and closes[-1] < float(plan.entry_price)
        else:
            rejected_entry = min(closes[-3:]) <= float(plan.entry_price) and closes[-1] > float(plan.entry_price)
    if len(vols) >= 8:
        recent = sum(float(v) for v in vols[-3:]) / 3.0
        base = sum(float(v) for v in vols[-8:-3]) / 5.0
        volume_failed = bool(base > 0 and recent < base * 0.70)
    choppy, low_liq, trend, regime = _regime_tags_for_invalidation(plan)
    repeated_losses = _symbol_direction_recent_losses(coin, plan.direction)
    grace_remaining = max(float(plan.near_floor_tp_grace_until or 0.0) - time.time(), 0.0)
    patience_boost = 0.0
    try:
        mfe_floor = float(_s.get_val("winner_patience_mfe_roi_floor", _s.get_val("near_floor_tp_mfe_ratio", 0.65)) or 0.65)
        runner_floor = float(_s.get_val("winner_patience_runner_quality_floor", 3.0) or 3.0)
        consensus_floor = float(_s.get_val("winner_patience_consensus_floor", 4.0) or 4.0)
        macro_trust = float(plan.macro_runner_trust_mult or 1.0)
        if (
            _current_mfe_roi(plan) >= mfe_floor
            and (
                float(plan.runner_quality or 0.0) >= runner_floor
                or float(plan.vote_score or 0.0) >= consensus_floor
                or macro_trust >= 1.08
            )
        ):
            patience_boost = min(
                2.0,
                0.75
                + max(float(plan.runner_quality or 0.0) - runner_floor, 0.0) * 0.25
                + max(macro_trust - 1.0, 0.0) * 2.0,
            )
            trend = bool(trend or macro_trust >= 1.08)
    except Exception:
        patience_boost = 0.0
    decision = _la.assess_early_invalidation(
        roi=roi,
        mfe_roi=_current_mfe_roi(plan),
        mae_roi=_current_mae_roi(plan),
        age_sec=age_sec,
        base_hold_sec=_setting_float("early_invalidation_base_min_hold_sec", _s.get_val("early_invalidation_min_age_sec", 45.0) or 45.0),
        min_hold_floor_sec=_setting_float("early_invalidation_min_hold_floor_sec", 20.0),
        max_hold_cap_sec=_setting_float("early_invalidation_max_hold_cap_sec", (_s.get_val("early_invalidation_max_age_min", 12.0) or 12.0) * 60.0),
        failure_threshold=_setting_float("early_invalidation_failure_score_threshold", 2.5),
        patience_threshold=_setting_float("early_invalidation_patience_score_threshold", 1.5),
        near_tp_earned=bool(plan.near_floor_tp_earned),
        near_tp_grace_remaining_sec=grace_remaining,
        structure_failed=structure_failed,
        momentum_flipped=momentum_flipped,
        volume_failed=volume_failed,
        spread_worsened=False,
        rejected_entry_zone=rejected_entry,
        choppy_regime=choppy,
        low_liquidity_regime=low_liq,
        trend_regime=trend,
        continuation_quality=float(plan.runner_quality or 0.0) + patience_boost,
        consensus_score=float(plan.vote_score or 0.0),
        repeated_symbol_losses=repeated_losses,
        runner_quality=float(plan.runner_quality or 0.0) + patience_boost,
        chop_strictness_mult=_setting_float("early_invalidation_chop_strictness_mult", 1.25),
        trend_patience_mult=_setting_float("early_invalidation_trend_patience_mult", 1.35),
    )
    diag = decision.to_dict()
    diag.update({
        "MFE_before_invalidation": round(_current_mfe_roi(plan), 4),
        "MAE_before_invalidation": round(_current_mae_roi(plan), 4),
        "near_floor_tp_earned": bool(plan.near_floor_tp_earned),
        "near_floor_tp_ratio": round(float(plan.near_floor_tp_ratio or 0.0), 4),
        "symbol_direction_loss_state": {"losses": repeated_losses, "direction": plan.direction},
        "regime": regime,
        "confidence_consensus": float(plan.vote_score or 0.0),
        "winner_patience_boost": round(float(patience_boost or 0.0), 4),
        "structure_failure_evidence": dict(struct_evidence or {}),
    })
    plan.invalidation_diagnostics = diag
    return decision


async def _maybe_adaptive_early_invalidation_exit(coin: str, plan: ExitPlan, price: float, is_long: bool, age_sec: float) -> bool:
    max_age = _setting_float("early_invalidation_max_hold_cap_sec", (_s.get_val("early_invalidation_max_age_min", 12.0) or 12.0) * 60.0)
    if age_sec > max_age:
        plan.early_invalidation_breach_time = 0.0
        return False
    decision = _build_adaptive_invalidation_decision(coin, plan, price, is_long, age_sec)
    logger.info(
        "EARLY INVALIDATION adaptive %s score=%.2f failure=%.2f patience=%.2f hold=%.0fs req=%.0fs mfe=%.2f mae=%.2f near_tp=%s reason=%s",
        coin,
        decision.invalidation_score,
        decision.failure_score,
        decision.patience_score,
        decision.hold_time,
        decision.required_hold_time,
        _current_mfe_roi(plan),
        _current_mae_roi(plan),
        bool(plan.near_floor_tp_earned),
        decision.final_reason,
    )
    if not decision.should_invalidate:
        plan.early_invalidation_breach_time = 0.0
        return False
    now = time.time()
    confirm_secs = _setting_float("early_invalidation_confirm_secs", 8.0)
    if confirm_secs > 0 and plan.early_invalidation_breach_time <= 0:
        plan.early_invalidation_breach_time = now
        return False
    if confirm_secs > 0 and now - float(plan.early_invalidation_breach_time or 0.0) < confirm_secs:
        return False
    _post_invalidation_watch[coin] = {
        "coin": coin,
        "trade_id": str(plan.order_id or ""),
        "direction": plan.direction,
        "entry_price": float(plan.entry_price or 0.0),
        "invalidation_price": float(price or 0.0),
        "size_usdt": float(plan.size_usdt or 0.0),
        "opened_at": _opened_at_iso(plan),
        "invalidated_at": datetime.now(timezone.utc).isoformat(),
        "expires_at": time.time() + 900.0,
        "max_favorable_price": float(price or 0.0),
        "max_adverse_price": float(price or 0.0),
        "samples": 0,
        "diagnostics": dict(plan.invalidation_diagnostics or {}),
    }
    plan.post_invalidation_telemetry = dict(_post_invalidation_watch[coin])
    try:
        _s.bump_counter("metric_early_invalidations", 1)
    except Exception:
        pass
    await _do_close(coin, price, "early_invalidation", plan)
    return True


async def _apply_milestone_profit_system(coin: str, plan: ExitPlan, price: float, is_long: bool) -> bool:
    """
    Milestone-based trailing + partial exits (leveraged ROI% triggers).

    Returns True if it executed an action that should stop further processing this tick
    (typically a partial close or stale-exit close).
    """
    be_trig, tp1_trig, tp2_trig, lock_trig, trail_trig = _milestone_thresholds(plan)
    roi = _roi_pct(plan, price)
    total_profit = _total_trade_pnl_usdt(plan, price)

    _maybe_positive_mfe_profit_guard(coin, plan, price, is_long)
    _maybe_positive_profit_lock_enrichment(coin, plan, price, is_long)

    # Stage 1: break-even + buffer (ROI buffer converted to a price delta via leverage).
    if (
        _s.get_val("breakeven_enabled", True)
        and roi >= be_trig
        and int(plan.profit_lock_level or 0) < 1
    ):
        entry = float(plan.entry_price or 0.0)
        buf_roi = (
            float(_s.get_val("swing_breakeven_buffer_pct", 0.40) or 0.40)
            if _is_swing_engine(plan.engine_name)
            else float(_s.get_val("breakeven_buffer_pct", 0.18) or 0.18)
        )
        buf_frac = _roi_pct_to_price_frac(buf_roi, _position_leverage(plan))
        raw_be_sl = (entry * (1.0 + buf_frac) if is_long else entry * (1.0 - buf_frac))
        fee_secure_sl = _fee_secure_stop_price(plan, price, is_long)
        if fee_secure_sl and fee_secure_sl > 0:
            be_sl = max(raw_be_sl, fee_secure_sl) if is_long else min(raw_be_sl, fee_secure_sl)
        else:
            be_sl = raw_be_sl
        economics, required_floor, eligible = _protection_candidate_economics(plan, price, be_sl)
        if eligible:
            _apply_stop_improvement(
                coin,
                plan,
                be_sl,
                is_long,
                "milestone_be",
                f"(roi={roi:.1f}% profit=${total_profit:.4f} fee_secure={fee_secure_sl or 0.0:.8f} "
                f"candidate_net=${float(economics.candidate_stop_estimated_net_pnl or 0.0):.4f})",
            )
            if plan.last_protection_reason == "milestone_be":
                plan.breakeven_moved = True
                plan.profit_lock_level = max(int(plan.profit_lock_level or 0), 1)
                try:
                    _s.bump_counter("metric_be_hits", 1)
                except Exception:
                    pass
        else:
            _record_protection_economic_defer(plan, stage="breakeven", economics=economics, required_floor=required_floor)

    # Stage 2: profit lock (guarantee profit once reached; no giveback into loss).
    if (
        _s.get_val("profit_lock_enabled", True)
        and roi >= lock_trig
        and int(plan.profit_lock_level or 0) < 2
    ):
        entry = float(plan.entry_price or 0.0)
        lock_roi = (
            float(_s.get_val("swing_profit_lock_buffer_pct", 2.00) or 2.00)
            if _is_swing_engine(plan.engine_name)
            else float(_s.get_val("profit_lock_buffer_pct", 0.65) or 0.65)
        )
        lock_frac = _roi_pct_to_price_frac(lock_roi, _position_leverage(plan))
        lock_sl = (entry * (1.0 + lock_frac) if is_long else entry * (1.0 - lock_frac))
        fee_secure_sl = _fee_secure_stop_price(plan, price, is_long)
        if fee_secure_sl and fee_secure_sl > 0:
            lock_sl = max(lock_sl, fee_secure_sl) if is_long else min(lock_sl, fee_secure_sl)
        economics, required_floor, eligible = _protection_candidate_economics(plan, price, lock_sl)
        if eligible:
            _apply_stop_improvement(
                coin, plan, lock_sl, is_long, "milestone_lock",
                f"(roi={roi:.1f}% profit=${total_profit:.4f} candidate_net="
                f"${float(economics.candidate_stop_estimated_net_pnl or 0.0):.4f})",
            )
            if plan.last_protection_reason == "milestone_lock":
                plan.profit_lock_level = max(int(plan.profit_lock_level or 0), 2)
                try:
                    _s.bump_counter("metric_profit_lock_hits", 1)
                except Exception:
                    pass
        else:
            _record_protection_economic_defer(plan, stage="profit_lock", economics=economics, required_floor=required_floor)

    # Partials: per-engine swing profile overrides.
    if _is_swing_engine(plan.engine_name):
        tp1_close = float(_s.get_val("swing_tp1_close_pct", 25.0) or 25.0)
        tp2_close = float(_s.get_val("swing_tp2_close_pct", 25.0) or 25.0)
    else:
        tp1_close = float(_s.get_val("partial_tp1_close_pct", 40.0) or 40.0)
        tp2_close = float(_s.get_val("partial_tp2_close_pct", 30.0) or 30.0)
    min_net_tp = float(_s.get_val("min_net_take_profit_usdt", 0.16) or 0.16)

    did_partial = False

    # Profit-close mutations belong exclusively to LOCAL_STAGED.  EXCHANGE_FULL
    # may still observe prices and improve its stop, but must never submit a
    # competing TP1/TP2 close.  NO_PROFIT_TP keeps BE/trailing protection only.
    if (
        plan.tp_owner == LOCAL_STAGED
        and plan.local_tp1_enabled
        and plan.local_tp2_enabled
        and _s.get_val("partial_exit_enabled", True)
        and plan.partial_mode
        and plan.size_usdt > 0
    ):
        if (
            bool(_s.get_val("micro_meaningful_profit_enabled", True))
            and not plan.tp1_done
            and bool(plan.tp1_deferred)
            and str(plan.lifecycle_phase or "") != "meaningful_profit_secured"
        ):
            continuation, cont_score, cont_reason = _continuation_active(plan, price, is_long)
            quality_floor = float(_s.get_val("micro_meaningful_profit_quality_floor", 3.0) or 3.0)
            if (
                cont_score >= quality_floor
                or float(plan.runner_quality or 0.0) >= quality_floor
                or float(plan.vote_score or 0.0) >= 4.0
            ):
                _update_lifecycle_telemetry(plan, price, is_long, continuation_score=cont_score, continuation_reason=cont_reason)
                decision = _meaningful_profit_decision(plan, price=price, continuation_score=cont_score)
                plan.economic_minimum = {
                    **dict(plan.economic_minimum or {}),
                    "meaningful_profit": decision.to_dict(),
                }
                if decision.should_secure and decision.close_pct > 0.0:
                    floor = max(
                        min_net_tp,
                        decision.target_usdt * max(float(decision.close_pct or 0.0), 0.0) / 100.0,
                        _partial_profit_floor_usdt(plan, round(plan.size_usdt * decision.close_pct / 100.0, 8)),
                    )
                    econ = _economic_partial_decision(
                        plan,
                        close_pct=float(decision.close_pct or 0.0),
                        price=price,
                        floor=floor,
                        continuation=True,
                    )
                    plan.economic_minimum = {
                        **dict(plan.economic_minimum or {}),
                        "meaningful_profit_economic": econ.to_dict(),
                    }
                    guard_ok, guard_meta = _novo_fee_bleed_guard_allows_partial(
                        coin,
                        plan,
                        stage="meaningful_profit",
                        close_pct=float(decision.close_pct or 0.0),
                        price=price,
                    )
                    plan.economic_minimum = {
                        **dict(plan.economic_minimum or {}),
                        "novo_fee_bleed_guard": guard_meta,
                    }
                    partial = await _execute_partial(coin, price, decision.close_pct, stage="meaningful_profit") if econ.executable and guard_ok else PartialExecutionResult(ok=False)
                    partial = _normalize_partial_result(partial, plan, price, float(decision.close_pct or 0.0))
                    if partial:
                        closed_notional = float(partial.get("closed_notional", 0.0) or 0.0)
                        pnl = float(partial.get("gross_pnl_usdt", 0.0) or 0.0)
                        fee_est = float(partial.get("fees_usdt", 0.0) or 0.0)
                        plan.tp1_deferred = False
                        plan.tp1_deferred_price = float(price or 0.0)
                        plan.lifecycle_phase = "meaningful_profit_secured"
                        plan.profit_lock_level = max(int(plan.profit_lock_level or 0), 3)
                        plan.runner_opportunity = _la.update_runner_opportunity(
                            plan.runner_opportunity,
                            direction=plan.direction,
                            stage="runner",
                            trim_pct=float(partial.get("verified_filled_contracts", 0.0) or 0.0) / max(float(partial.get("requested_contracts", 0.0) or 0.0), 1e-12) * float(decision.close_pct or 0.0),
                            stage_price=float(partial.get("fill_price", price) or price),
                            current_price=price,
                            initial_notional=float(plan.initial_size_usdt or 0.0),
                        )
                        await alert_take_profit(
                            coin,
                            plan.entry_price,
                            float(partial.get("fill_price", price) or price),
                            abs((price - plan.entry_price) / plan.entry_price * 100) if plan.entry_price > 0 else 0.0,
                            closed_notional,
                            leverage=plan.leverage,
                            realized_pnl_usdt=pnl,
                            fee_est_usdt=fee_est,
                        )
                        logger.info(
                            "MEANINGFUL PROFIT SECURE %s @ %.6f $%.4f (verified %.1f%%) target=$%.4f remaining=%.2f%% continuation=%.1f/%s",
                            coin,
                            float(partial.get("fill_price", price) or price),
                            pnl,
                            float(partial.get("verified_filled_contracts", 0.0) or 0.0) / max(float(partial.get("requested_contracts", 0.0) or 0.0), 1e-12) * float(decision.close_pct or 0.0),
                            float(decision.target_usdt or 0.0),
                            float(plan.remaining_pct or 0.0),
                            cont_score,
                            cont_reason,
                        )
                        try:
                            _s.bump_counter("metric_meaningful_profit_secures", 1)
                        except Exception:
                            pass
                        return True
                    if econ.executable and guard_ok:
                        logger.warning(
                            "MEANINGFUL PROFIT partial failed %s @ %.6f pct=%.1f roi=%.1f%%",
                            coin,
                            price,
                            float(decision.close_pct or 0.0),
                            roi,
                        )

        if roi >= tp1_trig and not plan.tp1_done:
            continuation, cont_score, cont_reason = _continuation_active(plan, price, is_long)
            _update_lifecycle_telemetry(plan, price, is_long, continuation_score=cont_score, continuation_reason=cont_reason)
            close_pct = _runner_preserved_close_pct(plan, tp1_close, stage="tp1", continuation=continuation)
            floor = max(min_net_tp, _partial_profit_floor_usdt(plan, round(plan.size_usdt * close_pct / 100.0, 8) if close_pct > 0 else 0.0))
            econ = _economic_partial_decision(plan, close_pct=close_pct, price=price, floor=floor, continuation=continuation)
            closed_notional = econ.closed_notional
            pnl = econ.pnl_usdt
            plan.economic_minimum = econ.to_dict()
            guard_ok, guard_meta = _novo_fee_bleed_guard_allows_partial(
                coin,
                plan,
                stage="tp1",
                close_pct=close_pct,
                price=price,
            )
            plan.economic_minimum = {
                **dict(plan.economic_minimum or {}),
                "novo_fee_bleed_guard": guard_meta,
            }
            if not econ.executable:
                if bool(_s.get_val("fee_aware_partial_deferral_enabled", True)):
                    _mark_deferred_milestone(
                        coin,
                        plan,
                        stage="tp1",
                        price=price,
                        roi=roi,
                        decision=econ,
                        continuation_score=cont_score,
                        continuation_reason=cont_reason,
                    )
                else:
                    reason = f"pnl={pnl:.4f}|floor={floor:.4f}|pct={close_pct:.1f}"
                    if _should_log_partial_skip(coin, "tp1", reason):
                        logger.info(
                            "MILESTONE TP1 skipped %s @ %.6f pnl=$%.4f floor=$%.4f pct=%.1f roi=%.1f%% lev=%dx",
                            coin, price, pnl, floor, close_pct, roi, plan.leverage,
                        )
            elif not guard_ok:
                _mark_deferred_milestone(
                    coin,
                    plan,
                    stage="tp1",
                    price=price,
                    roi=roi,
                    decision=econ,
                    continuation_score=cont_score,
                    continuation_reason=cont_reason,
                )
            else:
                partial = _normalize_partial_result(
                    await _execute_partial(coin, price, close_pct, stage="tp1"), plan, price, close_pct,
                )
                if not partial:
                    # Acknowledgement without verified fill is not a successful milestone.
                    logger.warning(
                        "MILESTONE TP1 partial unresolved %s @ %.6f pct=%.1f roi=%.1f%% state=%s",
                        coin, price, close_pct, roi, partial.get("state", ""),
                    )
                else:
                    closed_notional = float(partial.get("closed_notional", 0.0) or 0.0)
                    pnl = float(partial.get("gross_pnl_usdt", 0.0) or 0.0)
                    fee_est = float(partial.get("fees_usdt", 0.0) or 0.0)
                    plan.tp1_done = bool(partial.get("milestone_complete", False))
                    if plan.tp1_done:
                        plan.tp1_deferred = False
                        plan.tp1_deferred_price = float(partial.get("fill_price", price) or price)
                        plan.lifecycle_phase = "runner_core"
                        plan.profit_lock_level = max(int(plan.profit_lock_level or 0), 3)
                    actual_pct = float(partial.get("verified_filled_contracts", 0.0) or 0.0) / max(float(partial.get("requested_contracts", 0.0) or 0.0), 1e-12) * close_pct
                    plan.runner_opportunity = _la.update_runner_opportunity(
                        plan.runner_opportunity,
                        direction=plan.direction,
                        stage="tp1",
                        trim_pct=actual_pct,
                        stage_price=float(partial.get("fill_price", price) or price),
                        current_price=price,
                        initial_notional=float(plan.initial_size_usdt or 0.0),
                    )
                    await alert_take_profit(
                        coin, plan.entry_price, float(partial.get("fill_price", price) or price),
                        abs((price - plan.entry_price) / plan.entry_price * 100) if plan.entry_price > 0 else 0.0,
                        closed_notional,
                        leverage=plan.leverage,
                        realized_pnl_usdt=pnl,
                        fee_est_usdt=fee_est,
                    )
                    logger.info(
                        "MILESTONE TP1 %s @ %.6f $%.4f (verified %.1f%%) roi=%.1f%% state=%s remaining=%.2f%%",
                        coin, float(partial.get("fill_price", price) or price), pnl, actual_pct, roi,
                        partial.get("state", ""), plan.remaining_pct,
                    )
                    did_partial = True
                    if plan.tp1_done:
                        try:
                            _s.bump_counter("metric_tp1_hits", 1)
                        except Exception:
                            pass

        elif roi >= tp2_trig and _stage_reached(plan, "tp1") and not plan.tp2_done and plan.size_usdt > 0:
            continuation, cont_score, cont_reason = _continuation_active(plan, price, is_long)
            _update_lifecycle_telemetry(plan, price, is_long, continuation_score=cont_score, continuation_reason=cont_reason)
            close_pct = _runner_preserved_close_pct(plan, tp2_close, stage="tp2", continuation=continuation)
            if continuation:
                close_pct = min(
                    close_pct,
                    close_pct * float(_s.get_val("continuation_runner_tp2_close_mult", 0.65) or 0.65),
                )
            floor = max(min_net_tp, _partial_profit_floor_usdt(plan, round(plan.size_usdt * close_pct / 100.0, 8) if close_pct > 0 else 0.0))
            econ = _economic_partial_decision(plan, close_pct=close_pct, price=price, floor=floor, continuation=continuation)
            closed_notional = econ.closed_notional
            pnl = econ.pnl_usdt
            plan.economic_minimum = econ.to_dict()
            guard_ok, guard_meta = _novo_fee_bleed_guard_allows_partial(
                coin,
                plan,
                stage="tp2",
                close_pct=close_pct,
                price=price,
            )
            plan.economic_minimum = {
                **dict(plan.economic_minimum or {}),
                "novo_fee_bleed_guard": guard_meta,
            }
            if not econ.executable:
                if bool(_s.get_val("fee_aware_partial_deferral_enabled", True)):
                    _mark_deferred_milestone(
                        coin,
                        plan,
                        stage="tp2",
                        price=price,
                        roi=roi,
                        decision=econ,
                        continuation_score=cont_score,
                        continuation_reason=cont_reason,
                    )
                else:
                    reason = f"pnl={pnl:.4f}|floor={floor:.4f}|pct={close_pct:.1f}|cont={cont_score:.1f}"
                    if _should_log_partial_skip(coin, "tp2", reason):
                        logger.info(
                            "MILESTONE TP2 skipped %s @ %.6f pnl=$%.4f floor=$%.4f pct=%.1f roi=%.1f%% lev=%dx continuation=%.1f/%s",
                            coin, price, pnl, floor, close_pct, roi, plan.leverage, cont_score, cont_reason,
                        )
            elif not guard_ok:
                _mark_deferred_milestone(
                    coin,
                    plan,
                    stage="tp2",
                    price=price,
                    roi=roi,
                    decision=econ,
                    continuation_score=cont_score,
                    continuation_reason=cont_reason,
                )
            else:
                partial = _normalize_partial_result(
                    await _execute_partial(coin, price, close_pct, stage="tp2"), plan, price, close_pct,
                )
                if not partial:
                    logger.warning(
                        "MILESTONE TP2 partial unresolved %s @ %.6f pct=%.1f roi=%.1f%% state=%s",
                        coin, price, close_pct, roi, partial.get("state", ""),
                    )
                else:
                    closed_notional = float(partial.get("closed_notional", 0.0) or 0.0)
                    pnl = float(partial.get("gross_pnl_usdt", 0.0) or 0.0)
                    fee_est = float(partial.get("fees_usdt", 0.0) or 0.0)
                    plan.tp2_done = bool(partial.get("milestone_complete", False))
                    if plan.tp2_done:
                        plan.tp2_deferred = False
                        plan.tp2_deferred_price = float(partial.get("fill_price", price) or price)
                        plan.lifecycle_phase = "runner_core"
                        plan.profit_lock_level = max(int(plan.profit_lock_level or 0), 4)
                    actual_pct = float(partial.get("verified_filled_contracts", 0.0) or 0.0) / max(float(partial.get("requested_contracts", 0.0) or 0.0), 1e-12) * close_pct
                    plan.runner_opportunity = _la.update_runner_opportunity(
                        plan.runner_opportunity,
                        direction=plan.direction,
                        stage="tp2",
                        trim_pct=actual_pct,
                        stage_price=float(partial.get("fill_price", price) or price),
                        current_price=price,
                        initial_notional=float(plan.initial_size_usdt or 0.0),
                    )
                    await alert_take_profit(
                        coin,
                        plan.entry_price,
                        float(partial.get("fill_price", price) or price),
                        abs((price - plan.entry_price) / plan.entry_price * 100) if plan.entry_price > 0 else 0.0,
                        closed_notional,
                        leverage=plan.leverage,
                        realized_pnl_usdt=pnl,
                        fee_est_usdt=fee_est,
                    )
                    logger.info(
                        "MILESTONE TP2 %s @ %.6f $%.4f (verified %.1f%%) roi=%.1f%% state=%s remaining=%.2f%%",
                        coin, float(partial.get("fill_price", price) or price), pnl, actual_pct, roi,
                        partial.get("state", ""), plan.remaining_pct,
                    )
                    did_partial = True
                    if plan.tp2_done:
                        try:
                            _s.bump_counter("metric_tp2_hits", 1)
                        except Exception:
                            pass

    # Runner trailing: only tighten, anchored to peak_price so it never loosens.
    if (
        _s.get_val("tsl_enabled", True)
        and plan.size_usdt > 0
        and roi >= trail_trig
        and plan.peak_price > 0
    ):
        stage = 4 if _stage_reached(plan, "tp2") else (3 if _stage_reached(plan, "tp1") else max(int(plan.profit_lock_level or 1), 1))
        pct_trail = _milestone_trail_pct(plan, stage)
        continuation, cont_score, cont_reason = _continuation_active(plan, price, is_long)
        _update_lifecycle_telemetry(plan, price, is_long, continuation_score=cont_score, continuation_reason=cont_reason)
        if continuation:
            pct_trail *= float(_s.get_val("continuation_runner_trail_mult", 1.35) or 1.35)
        if bool(_s.get_val("exhaustion_assisted_tightening_enabled", False)):
            ex_threshold = float(_s.get_val("exhaustion_tighten_score_threshold", 3.0) or 3.0)
            if float(plan.exhaustion_score or 0.0) >= ex_threshold:
                pct_trail *= float(_s.get_val("exhaustion_tighten_trail_mult", 0.72) or 0.72)
        dist = _trail_distance(plan, float(plan.peak_price), pct_trail)
        if dist > 0:
            new_sl = (float(plan.peak_price) - dist) if is_long else (float(plan.peak_price) + dist)
            extra = (
                f"(roi={roi:.1f}% continuation={cont_score:.1f}/{cont_reason} exhaustion={plan.exhaustion_score:.1f}/{plan.exhaustion_reasons})"
                if continuation
                else f"(roi={roi:.1f}% exhaustion={plan.exhaustion_score:.1f}/{plan.exhaustion_reasons})"
            )
            old_sl = float(plan.hard_sl or 0.0)
            economics, required_floor, eligible = _protection_candidate_economics(plan, price, new_sl)
            if eligible:
                _apply_stop_improvement(coin, plan, new_sl, is_long, f"trail_s{stage}", extra)
            else:
                _record_protection_economic_defer(plan, stage=f"trail_s{stage}", economics=economics, required_floor=required_floor)
            if float(plan.hard_sl or 0.0) != old_sl:
                base_price = float(plan.tp2_deferred_price or plan.tp1_deferred_price or price or 0.0)
                plan.runner_opportunity = _la.update_runner_opportunity(
                    plan.runner_opportunity,
                    direction=plan.direction,
                    stage="trail",
                    trim_pct=max(100.0 - float(plan.remaining_pct or 100.0), 0.0),
                    stage_price=base_price,
                    current_price=price,
                    initial_notional=float(plan.initial_size_usdt or 0.0),
                )

    if await _maybe_stale_exit(coin, plan, price):
        return True

    if await _maybe_early_invalidation_exit(coin, plan, price, is_long):
        return True

    return did_partial

# ── Exchange sync loop (detects algo TP/SL closes) ────────────────────────────

async def _exchange_sync_loop() -> None:
    """
    Every EXCHANGE_SYNC_INTERVAL seconds: compare active exit plans against
    real OKX positions. Any plan whose coin is no longer open on OKX was
    closed by an exchange algo order (TP/SL). Record PnL and clean up.
    """
    await asyncio.sleep(60)   # wait for boot to complete
    while True:
        try:
            await _do_exchange_sync()
        except Exception as e:
            logger.debug("Exchange sync error: %s", e)
        await asyncio.sleep(EXCHANGE_SYNC_INTERVAL)


async def _do_exchange_sync() -> None:
    if _paper_trading_enabled():
        logger.debug("Exchange sync skipped in PAPER mode.")
        return
    if not _okx_client or not _plans:
        return

    status = await _okx_client.get_live_open_coins_status()
    if not bool(status.get("ok", False)):
        _mark_plan_sync_failure(str(status.get("reason", "get_live_open_coins_failed") or "get_live_open_coins_failed"))
        return

    _sync_health.mark_success("exit_sync")
    live_coins = set(status.get("coins", set()) or set())
    sync_ts = _sync_timestamp()

    for coin in list(_plans.keys()):
        plan = _plans.get(coin)
        if not plan:
            continue
        if plan.exposure_truth_status == EXCHANGE_CLOSED_AWAITING_RECONCILIATION:
            logger.warning("EXCHANGE CLOSED HANDOFF %s remains reconciliation-owned; no local close or protection mutation.", coin)
            continue
        if coin in live_coins:
            _mark_plan_seen(coin, plan, sync_ts)
            continue
        confirmations = _mark_plan_absent(coin, plan, sync_ts)
        if confirmations < 2:
            continue
        logger.info(
            "CONFIRMED CLOSE - safe to notify %s confirmed_absence_syncs=%d",
            coin,
            confirmations,
        )
        await _handle_exchange_closed(coin, plan)


async def _handle_exchange_closed(coin: str, plan: ExitPlan) -> None:
    """
    The exchange closed this position (TP/SL algo order fired).
    Determine actual exit price and PnL, record everything.
    """
    if _paper_trading_enabled():
        logger.warning("Ignoring exchange-closed reconciliation for %s in PAPER mode.", coin)
        return
    if plan.exposure_truth_status == EXCHANGE_CLOSED_AWAITING_RECONCILIATION:
        logger.warning("Holding %s in exchange closure handoff; partial economics are not yet authoritative.", coin)
        return
    # Fetch actual fill price from OKX trade history.
    # latest_price() reflects the market at sync time (up to 30s after the fill)
    # which is inaccurate. The fills endpoint returns the real execution price.
    price = None
    if _okx_client:
        try:
            price = await _okx_client.get_last_fill_price(coin)
        except Exception as e:
            logger.debug("_handle_exchange_closed: fill price fetch failed for %s: %s", coin, e)
    # Fall back to latest market price if OKX fill fetch fails
    if not price or price <= 0:
        price = latest_price(coin)
    if not price or price <= 0:
        price = plan.entry_price

    is_long   = plan.direction == "long"
    pnl       = _calc_pnl(plan, price, 100.0)

    # Determine TP vs SL by actual pnl direction (not vs tp1 price, which
    # could mis-label a profitable early close as sl_exchange).
    if is_long:
        reason = "tp_exchange" if price > plan.entry_price else "sl_exchange"
    else:
        reason = "tp_exchange" if price < plan.entry_price else "sl_exchange"

    logger.info(
        "Exchange algo closed %s @ %.6f pnl=$%.4f (%s) lev=%dx",
        coin, price, pnl, reason, plan.leverage,
    )
    _fee_bleed_trace_small_full_close(coin, plan, price=price, reason=reason, via_exchange=True)

    if not plan.close_notified:
        plan.close_notified = True
        await alert_trade_closed(
            engine=plan.engine_name,
            coin=coin,
            direction=plan.direction,
            entry_price=plan.entry_price,
            exit_price=price,
            size_usdt=plan.size_usdt,
            pnl_usdt=pnl,
            exit_reason=reason,
            leverage=plan.leverage,
            via_exchange=True,
        )

    # Notify engine — _record_close handles both cat_record + _perf.record_trade
    _notify_engine(coin, price, pnl, reason, plan, via_exchange=True)

    # Clean up exit plan
    remove(coin)


# ── Timeout / close helpers ───────────────────────────────────────────────────

async def _update_tsl(plan: ExitPlan, price: float, is_long: bool) -> None:
    act = float(_s.get_val("tsl_activation_pct", 0.5)) / 100
    trail_pct = float(_s.get_val("tsl_trail_pct", 1.5)) / 100
    pct_ready = (
        (is_long and price >= plan.entry_price * (1 + act)) or
        (not is_long and price <= plan.entry_price * (1 - act))
    )
    if not pct_ready and not plan.tsl_armed:
        return

    trail_dist = _trail_distance(plan, plan.peak_price or price, trail_pct)
    if trail_dist <= 0:
        return
    new = (plan.peak_price or price) - trail_dist if is_long else (plan.peak_price or price) + trail_dist
    economics, required_floor, eligible = _protection_candidate_economics(plan, price, new)
    if not eligible:
        _record_protection_economic_defer(plan, stage="legacy_trailing", economics=economics, required_floor=required_floor)
        return

    if not plan.tsl_armed:
        plan.tsl_armed = True
        plan.trail_activated_at = datetime.now(timezone.utc).isoformat()
        plan.trail_activation_net_pnl = round(float(economics.current_estimated_net_pnl or 0.0), 8)
        plan.trail_peak_net_pnl = max(plan.trail_peak_net_pnl, plan.trail_activation_net_pnl)
        logger.info(
            "TRAIL ARM %s: net=$%.4f candidate_net=$%.4f required=$%.4f hard_sl=%.8f",
            plan.coin,
            float(economics.current_estimated_net_pnl or 0.0),
            float(economics.candidate_stop_estimated_net_pnl or 0.0),
            float(required_floor or 0.0),
            plan.hard_sl,
        )
    if is_long:
        if new > plan.tsl_price:
            plan.tsl_price = round(new, 8)
            if new > plan.hard_sl:
                plan.hard_sl = plan.tsl_price
    else:
        if plan.tsl_price == 0 or new < plan.tsl_price:
            plan.tsl_price = round(new, 8)
            if plan.hard_sl == 0 or new < plan.hard_sl:
                plan.hard_sl = plan.tsl_price


async def _timeout_close(plan: ExitPlan, coin: str, price: float, reason: str) -> bool:
    enabled = _s.get_val("sl_timeout_enabled", True)
    if reason == "sl":
        secs = float(_s.get_val("internal_sl_confirm_secs", _s.get_val("sl_timeout_secs", 8)))
    else:
        secs = float(_s.get_val("sl_timeout_secs", 8))
    if not enabled or secs <= 0:
        return bool(await _do_close(coin, price, reason, plan))
    now = time.time()
    if plan.sl_breach_time == 0.0:
        plan.sl_breach_time = now
        logger.info("[%s] SL breach @ %.6f - guard %ds", coin, price, secs)
        return False
    if now - plan.sl_breach_time >= secs:
        return bool(await _do_close(coin, price, reason, plan))
    return False


async def _do_paper_close(coin: str, price: float, reason: str, plan: ExitPlan) -> bool:
    """Close a paper position locally without exchange close/reconciliation calls."""
    if classify_full_close_reason(reason) == OPTIONAL_PROFIT_CAPTURE:
        if await _maybe_defer_optional_full_close(coin, price, reason, plan):
            return False
    fill_at = datetime.now(timezone.utc).isoformat()
    plan.first_exit_fill_at = plan.first_exit_fill_at or fill_at
    plan.last_exit_fill_at = fill_at
    pnl = _calc_pnl(plan, price, 100.0)
    if "trail" in str(reason).lower():
        plan.trail_exit_net_pnl = round(float(pnl) - float(_estimated_total_fee_usdt(plan) or 0.0), 8)
        plan.profit_surrendered = round(max(float(plan.trail_peak_net_pnl or 0.0) - plan.trail_exit_net_pnl, 0.0), 8)
    logger.info(
        "Paper position closed %s @ %.6f pnl=$%.4f (%s) lev=%dx",
        coin, price, pnl, reason, plan.leverage,
    )
    _fee_bleed_trace_small_full_close(coin, plan, price=price, reason=reason, via_exchange=False)
    try:
        if not plan.close_notified:
            plan.close_notified = True
            await alert_trade_closed(
                engine=plan.engine_name,
                coin=coin,
                direction=plan.direction,
                entry_price=plan.entry_price,
                exit_price=price,
                size_usdt=plan.size_usdt,
                pnl_usdt=pnl,
                exit_reason=reason,
                leverage=plan.leverage,
                via_exchange=False,
            )
    except Exception as exc:
        logger.warning("Paper close notification failed for %s reason=%s: %s", coin, reason, exc)
    try:
        _notify_engine(coin, price, pnl, reason, plan, via_exchange=False)
    except Exception as exc:
        logger.warning("Paper close accounting failed for %s reason=%s: %s", coin, reason, exc)
    finally:
        remove(coin)
    return True


async def _do_close(coin: str, price: float, reason: str, plan: ExitPlan) -> bool:
    reason = _close_reason_for_stop_hit(plan, price, reason)
    if classify_full_close_reason(reason) == OPTIONAL_PROFIT_CAPTURE:
        if await _maybe_defer_optional_full_close(coin, price, reason, plan):
            return False
    pnl = _calc_pnl(plan, price, 100.0)
    if "trail" in str(reason).lower():
        plan.trail_exit_net_pnl = round(float(pnl) - float(_estimated_total_fee_usdt(plan) or 0.0), 8)
        plan.profit_surrendered = round(max(float(plan.trail_peak_net_pnl or 0.0) - plan.trail_exit_net_pnl, 0.0), 8)
    if not await _execute_exit(coin, price, reason, plan):
        logger.warning("EXIT DEFERRED %s reason=%s - close not confirmed", coin, reason)
        return False
    if plan.exposure_truth_status == EXCHANGE_CLOSED_AWAITING_RECONCILIATION:
        logger.info("EXIT HANDOFF %s reason=%s - awaiting exchange economic reconciliation", coin, reason)
        return True
    _fee_bleed_trace_small_full_close(coin, plan, price=price, reason=reason, via_exchange=False)

    if not plan.close_notified:
        plan.close_notified = True
        await alert_trade_closed(
            engine=plan.engine_name,
            coin=coin,
            direction=plan.direction,
            entry_price=plan.entry_price,
            exit_price=price,
            size_usdt=plan.size_usdt,
            pnl_usdt=pnl,
            exit_reason=reason,
            leverage=plan.leverage,
            via_exchange=False,
        )
    _notify_engine(coin, price, pnl, reason, plan, via_exchange=False)
    remove(coin)
    return True


def _notify_engine(
    coin: str, exit_price: float, pnl_usdt: float,
    reason: str, plan: ExitPlan,
    via_exchange: bool = False,
) -> None:
    eng_name = plan.engine_name
    _record_exit_transition(
        plan,
        "FINAL_EXIT",
        reason=str(reason or ""),
        exit_price=round(float(exit_price or 0.0), 12),
        realized_pnl_usdt=round(float(pnl_usdt or 0.0), 8),
    )
    close_context = {
        "entry_order_ids": list(plan.entry_order_ids),
        "entry_client_order_id": str(plan.entry_client_order_id or ""),
        "entry_clOrdId": str(plan.entry_client_order_id or ""),
        "exit_order_ids": list(plan.exit_order_ids),
        "exit_order_id": str(plan.exit_order_ids[-1] if plan.exit_order_ids else ""),
        "first_fill_at": str(plan.first_fill_at or ""),
        "last_entry_fill_at": str(plan.last_entry_fill_at or ""),
        "first_exit_fill_at": str(plan.first_exit_fill_at or ""),
        "last_exit_fill_at": str(plan.last_exit_fill_at or ""),
        "closed_at": str(plan.last_exit_fill_at or ""),
        "partial_exit_events": list(plan.partial_exit_events),
        "partial_exit_state": dict(plan.partial_exit_state or {}),
        "tp_owner": str(plan.tp_owner or LOCAL_STAGED),
        "tp_mode_at_entry": str(plan.tp_mode_at_entry or ""),
        "exchange_tp_algo_id": str(plan.exchange_tp_algo_id or ""),
        "local_tp1_enabled": bool(plan.local_tp1_enabled),
        "local_tp2_enabled": bool(plan.local_tp2_enabled),
        "ownership_timestamp": str(plan.ownership_timestamp or ""),
        "tp_owner_timestamp": str(plan.tp_owner_timestamp or plan.ownership_timestamp or ""),
        "partials_enabled_at_entry": bool(plan.partials_enabled_at_entry),
        "entry_contracts": float(plan.entry_contracts or 0.0),
        "confirmed_remaining_contracts": float(plan.confirmed_remaining_contracts or 0.0),
        "exchange_position_timestamp": str(plan.exchange_position_timestamp or ""),
        "exposure_truth_status": str(plan.exposure_truth_status or EXPOSURE_ACTIVE),
        "closure_handoff_at": str(plan.closure_handoff_at or ""),
        "partial_attribution_status": str(plan.partial_attribution_status or "NONE"),
        "partial_realized_pnl_usdt": float(plan.partial_realized_pnl_usdt or 0.0),
        "partial_fees_usdt": float(plan.partial_fees_usdt or 0.0),
        "entry_fee_usdt": plan.entry_fee_usdt,
        "funding_usdt": plan.funding_usdt,
        "funding_status": str(plan.funding_status or "UNKNOWN"),
        "full_close_economics": dict(plan.full_close_economics or {}),
        "exit_timeline": list(plan.exit_timeline)[-100:],
        "mfe_price": float(plan.mfe_price or 0.0),
        "mae_price": float(plan.mae_price or 0.0),
        "mfe_gross_pnl_usdt": float(plan.mfe_gross_pnl_usdt or 0.0),
        "mae_gross_pnl_usdt": float(plan.mae_gross_pnl_usdt or 0.0),
        "oxypulse_reported_gross_pnl": round(float(plan.realized_pnl_usdt or 0.0) + float(pnl_usdt or 0.0), 8),
        "oxypulse_reported_final_pnl": round(
            float(plan.realized_pnl_usdt or 0.0)
            + float(pnl_usdt or 0.0)
            - float(_estimated_total_fee_usdt(plan) or 0.0),
            8,
        ),
         "mfe_roi": _current_mfe_roi(plan),
         "mae_roi": _current_mae_roi(plan),
         "mfe_observed_roi": float(plan.mfe_roi or 0.0),
         "mae_observed_roi": float(plan.mae_roi or 0.0),
         "mfe_raw_move_pct": float(plan.mfe_raw_move_pct or 0.0),
         "mae_raw_move_pct": float(plan.mae_raw_move_pct or 0.0),
         "mfe_observed_leverage": int(plan.mfe_observed_leverage or 0),
         "mae_observed_leverage": int(plan.mae_observed_leverage or 0),
        "exit_profile": dict(plan.exit_profile or {}),
        "trail_semantic_reason": _trail_semantic_reason(plan, pnl_usdt, reason),
        "trail_activated_at": str(plan.trail_activated_at or ""),
        "trail_activation_net_pnl": float(plan.trail_activation_net_pnl or 0.0),
        "trail_peak_net_pnl": float(plan.trail_peak_net_pnl or 0.0),
        "trail_exit_net_pnl": float(plan.trail_exit_net_pnl or 0.0),
        "profit_surrendered": float(plan.profit_surrendered or 0.0),
        "post_invalidation_telemetry": dict(plan.post_invalidation_telemetry or {}),
    }
    eng = _engine_registry.get(eng_name) if eng_name else None
    handled = False
    if eng is not None:
        try:
            if via_exchange and hasattr(eng, "notify_closed_by_exchange"):
                handled = bool(eng.notify_closed_by_exchange(coin, exit_price, pnl_usdt, reason, close_context=close_context))
            elif not via_exchange and hasattr(eng, "notify_closed_by_exit_manager"):
                handled = bool(eng.notify_closed_by_exit_manager(coin, exit_price, pnl_usdt, reason, close_context=close_context))
        except Exception as exc:
            logger.warning("_notify_engine: engine %s handler failed for %s: %s", eng_name, coin, exc)
            handled = False

    fee_usdt = fee_cost_usdt(plan.size_usdt, plan.leverage)
    _maybe_record_novo_enrichment_outcome(
        coin,
        exit_price,
        pnl_usdt,
        reason,
        plan,
        fee_usdt=fee_usdt,
        via_exchange=via_exchange,
    )
    if not handled:
        _paper_validation.record_close(
            context={**_paper_validation_close_context_from_plan(plan, coin), **close_context},
            order_id=str(getattr(plan, "order_id", "") or ""),
            pair=str(coin or "").upper(),
            exit_price=float(exit_price or 0.0),
            pnl_usdt=float(pnl_usdt or 0.0),
            fee_usdt=float(fee_usdt or 0.0),
            exit_reason=str(reason or ""),
        )
        _perf.record_trade(
            coin, plan.direction, plan.entry_price, exit_price,
            plan.size_usdt, pnl_usdt, reason,
            fee_usdt=fee_usdt,
            opened_at=_opened_at_iso(plan),
        )
        try:
            from category_tracker import ENGINE_NAMES as _CAT_ENGINES, record_trade as _cat_record
            if eng_name in _CAT_ENGINES:
                _cat_record(
                    engine=eng_name,
                    coin=coin,
                    direction=plan.direction,
                    entry=plan.entry_price,
                    exit_price=exit_price,
                    size_usdt=plan.size_usdt,
                    pnl_usdt=pnl_usdt,
                    vote_score=int(plan.vote_score or 2),
                    exit_reason=reason,
                    fee_usdt=fee_usdt,
                    opened_at=_opened_at_iso(plan),
                )
        except Exception as exc:
            logger.warning("_notify_engine: category fallback failed for %s [%s]: %s", coin, eng_name, exc)
        if not _paper_trading_enabled():
            try:
                from pnl_reconciler import reconciler as _pnl_reconciler

                fee_usdt = fee_cost_usdt(plan.size_usdt, plan.leverage)
                _pnl_reconciler.record_provisional(
                    str(plan.order_id or f"{eng_name}-{coin}-{_opened_at_iso(plan)}"),
                    coin=coin,
                    engine=eng_name,
                    direction=plan.direction,
                    gross_price_pnl=float(close_context.get("oxypulse_reported_gross_pnl", pnl_usdt) or 0.0),
                    entry_fee=-abs(float(fee_usdt or 0.0) / 2.0),
                    exit_fee=-abs(float(fee_usdt or 0.0) / 2.0),
                    local_estimated_net_pnl=float(close_context.get("oxypulse_reported_final_pnl", pnl_usdt) or 0.0),
                    oxypulse_reported_final_pnl=float(close_context.get("oxypulse_reported_final_pnl", pnl_usdt) or 0.0),
                    oxypulse_reported_gross_pnl=float(close_context.get("oxypulse_reported_gross_pnl", pnl_usdt) or 0.0),
                    entry_notional_usdt=float(plan.initial_size_usdt or plan.size_usdt or 0.0),
                    mfe_gross_pnl_usdt=float(plan.mfe_gross_pnl_usdt or 0.0),
                    mae_gross_pnl_usdt=float(plan.mae_gross_pnl_usdt or 0.0),
                    mfe_price=float(plan.mfe_price or 0.0),
                    mae_price=float(plan.mae_price or 0.0),
                     mfe_roi=_current_mfe_roi(plan),
                     mae_roi=_current_mae_roi(plan),
                    exit_timeline=list(plan.exit_timeline or []),
                    partial_exit_events=list(plan.partial_exit_events or []),
                    trail_semantic_reason=str(plan.last_protection_reason or ""),
                    trail_activated_at=str(plan.trail_activated_at or ""),
                    trail_activation_net_pnl=float(plan.trail_activation_net_pnl or 0.0),
                    trail_peak_net_pnl=float(plan.trail_peak_net_pnl or 0.0),
                    trail_exit_net_pnl=float(plan.trail_exit_net_pnl or 0.0),
                    profit_surrendered=float(plan.profit_surrendered or 0.0),
                    profit_lock_level=int(plan.profit_lock_level or 0),
                    breakeven_moved=bool(plan.breakeven_moved),
                    tp1_done=bool(plan.tp1_done),
                    tp2_done=bool(plan.tp2_done),
                    post_invalidation_telemetry=dict(plan.post_invalidation_telemetry or {}),
                    exit_reason=str(reason or ""),
                    entry_order_id=str(plan.order_id or ""),
                    entry_order_ids=list(plan.entry_order_ids or []),
                    entry_client_order_id=str(plan.entry_client_order_id or ""),
                    entry_clOrdId=str(plan.entry_client_order_id or ""),
                    exit_order_id=str(plan.exit_order_ids[-1] if plan.exit_order_ids else ""),
                    exit_order_ids=list(plan.exit_order_ids or []),
                    position_id=str(plan.order_id or ""),
                    first_fill_at=str(plan.first_fill_at or ""),
                    last_entry_fill_at=str(plan.last_entry_fill_at or ""),
                    first_exit_fill_at=str(plan.first_exit_fill_at or ""),
                    last_exit_fill_at=str(plan.last_exit_fill_at or ""),
                    inst_id=f"{str(coin or '').upper()}-USDT-SWAP",
                    execution_environment="production",
                )
            except Exception as exc:
                logger.warning("_notify_engine: live fallback PnL reconciliation failed for %s: %s", coin, exc)
        logger.warning("_notify_engine: engine %s not managing %s - direct fallback record used", eng_name, coin)


async def _execute_exit(coin: str, price: float, reason: str, plan: ExitPlan) -> bool:
    """
    Place a close order on the exchange.
    Skips if position already gone (exchange TP/SL already fired).
    """
    if not _okx_client:
        return False
    mutation_allowed, mutation_reason = _local_mutation_authorized(coin)
    if not mutation_allowed:
        logger.warning("_execute_exit skipped %s: %s; no close POST.", coin, mutation_reason)
        return False

    snapshot = await _partial_position_snapshot(coin, plan)
    if snapshot is None:
        logger.warning("_execute_exit: current exchange quantity unavailable for %s; no close POST.", coin)
        plan.exposure_truth_status = EXPOSURE_UNKNOWN
        plan.partial_attribution_status = "EXPOSURE_UNKNOWN"
        _partial_persist(coin, plan, "final_close", strict=True)
        return False
    current_contracts = float(snapshot.get("contracts", 0.0) or 0.0)
    if not bool(snapshot.get("present", False)) or current_contracts <= 0.0:
        _set_exchange_exposure_truth(
            coin,
            plan,
            0.0,
            attribution_status="AWAITING_RECONCILIATION",
        )
        if not _partial_persist(coin, plan, "final_close", strict=True):
            logger.error("_execute_exit: closure handoff for %s could not be durably persisted; no POST.", coin)
            return False
        logger.warning("_execute_exit: %s is confirmed closed; durable reconciliation handoff created, no POST.", coin)
        return True

    _set_exchange_exposure_truth(
        coin,
        plan,
        current_contracts,
        attribution_status=plan.partial_attribution_status or "AUTHORITATIVE_EXPOSURE",
    )
    if not _partial_persist(coin, plan, "final_close", strict=True):
        logger.error("_execute_exit: could not persist fresh close exposure for %s; no close POST.", coin)
        return False

    pos_side = "long" if plan.direction == "long" else "short"
    size = plan.size_usdt
    try:
        from execution_router import router_for
        close_kwargs = {"partial": True, "contracts": current_contracts}
        result = await router_for(_okx_client).close(coin, pos_side, size, **close_kwargs)
        if isinstance(result, dict):
            order_id = str(result.get("ordId", "") or "")
            fill_at = str(result.get("fillTime", result.get("uTime", "")) or datetime.now(timezone.utc).isoformat())
            if order_id and order_id not in plan.exit_order_ids:
                plan.exit_order_ids.append(order_id)
            plan.first_exit_fill_at = plan.first_exit_fill_at or fill_at
            plan.last_exit_fill_at = fill_at
        if result.get("exchange_closed"):
            logger.info(
                "_execute_exit: %s returned exchange_closed - position was already closed.",
                coin,
            )
        return True
    except Exception as e:
        logger.error("Exit order %s: %s", coin, e)
        await alert_error(f"exit_{coin}_{reason}", str(e))
        return False


class PartialExecutionResult(dict):
    """Structured partial-close result that remains truthy only when fill data is verified."""

    def __bool__(self) -> bool:
        return bool(self.get("ok", False))


def _normalize_partial_result(result, plan: ExitPlan, price: float, pct: float) -> PartialExecutionResult:
    """Keep older test/integration stubs compatible without weakening live accounting."""
    if isinstance(result, dict):
        return PartialExecutionResult(result)
    if not result:
        return PartialExecutionResult(ok=False, state=PARTIAL_REJECTED)
    closed_notional = float(plan.size_usdt or 0.0) * float(pct or 0.0) / 100.0
    pnl = _calc_pnl(plan, price, pct)
    fee = fee_cost_usdt(closed_notional, plan.leverage)
    plan.realized_pnl_usdt = round(float(plan.realized_pnl_usdt or 0.0) + pnl, 8)
    plan.size_usdt = round(max(float(plan.size_usdt or 0.0) - closed_notional, 0.0), 8)
    _refresh_remaining_pct(plan)
    return PartialExecutionResult(
        ok=True,
        state=PARTIAL_FILLED,
        milestone_complete=True,
        requested_contracts=1.0,
        verified_filled_contracts=1.0,
        closed_notional=closed_notional,
        fill_price=float(price or 0.0),
        gross_pnl_usdt=pnl,
        fees_usdt=fee,
        fees_known=False,
    )


def _partial_state(plan: ExitPlan, stage: str) -> dict:
    return dict((plan.partial_exit_state or {}).get(str(stage or "partial"), {}) or {})


def _partial_instrument(coin: str):
    try:
        from instrument_registry import registry as _instrument_registry
        return _instrument_registry.require(coin)
    except Exception:
        return None


def _partial_contracts_equal(left: float, right: float, instrument=None) -> bool:
    tolerance = max(float(getattr(instrument, "lot_sz", 0.0) or 0.0) / 2.0, 1e-8)
    return abs(float(left or 0.0) - float(right or 0.0)) <= tolerance


async def _partial_position_snapshot(coin: str, plan: ExitPlan) -> dict | None:
    """Read current exchange quantity; None means the read was not authoritative."""
    if not _okx_client or not hasattr(_okx_client, "get_positions"):
        return None
    try:
        positions = await _okx_client.get_positions()
    except Exception as exc:
        logger.warning("Partial close position read unavailable %s: %s", coin, exc)
        return None
    target = str(coin or "").upper()
    wanted_side = "long" if str(plan.direction or "").lower() == "long" else "short"
    for row in list(positions or []):
        inst = str(row.get("instId", "") or "").upper()
        if not (inst == target or inst.startswith(f"{target}-")):
            continue
        pos_side = str(row.get("posSide", "") or "").lower()
        if pos_side and pos_side not in {wanted_side, "net"}:
            continue
        try:
            contracts = abs(float(row.get("pos", row.get("availPos", 0.0)) or 0.0))
        except (TypeError, ValueError):
            return None
        return {"ok": True, "present": contracts > 0.0, "contracts": contracts, "row": dict(row)}
    return {"ok": True, "present": False, "contracts": 0.0, "row": {}}


def _partial_fill_quantity(row: dict) -> float:
    for key in ("fillSz", "fill_size", "accFillSz", "filled_contracts", "filledQty"):
        value = row.get(key)
        if value not in {None, ""}:
            try:
                quantity = abs(float(value))
            except (TypeError, ValueError):
                continue
            if quantity > 0.0:
                return quantity
    return 0.0


def _partial_fill_price(row: dict) -> float:
    for key in ("fillPx", "fill_price", "avgPx", "average_fill_price", "price"):
        value = row.get(key)
        if value not in {None, ""}:
            try:
                price = float(value)
            except (TypeError, ValueError):
                continue
            if price > 0.0:
                return price
    return 0.0


def _partial_fill_identity(row: dict) -> str:
    for key in ("fillId", "tradeId", "billId", "execId"):
        if row.get(key) not in {None, ""}:
            return f"{key}:{row.get(key)}"
    return json.dumps(dict(row), sort_keys=True, default=str)


def _partial_deduplicate_fills(rows: list[dict]) -> list[dict]:
    seen: set[str] = set()
    result: list[dict] = []
    for row in rows:
        identity = _partial_fill_identity(row)
        if identity in seen:
            continue
        seen.add(identity)
        result.append(dict(row))
    return result


def _partial_gross_pnl(plan: ExitPlan, contracts: float, exit_price: float, instrument=None) -> float:
    entry = float(plan.entry_price or 0.0)
    exit_px = float(exit_price or 0.0)
    qty = float(contracts or 0.0)
    if entry <= 0.0 or exit_px <= 0.0 or qty <= 0.0:
        return 0.0
    if instrument is not None:
        entry_notional = instrument.contracts_to_notional(qty, entry)
        exit_notional = instrument.contracts_to_notional(qty, exit_px)
        return round((exit_notional - entry_notional) if plan.direction == "long" else (entry_notional - exit_notional), 8)
    move = ((exit_px - entry) / entry) if plan.direction == "long" else ((entry - exit_px) / entry)
    closed_notional = float(plan.size_usdt or 0.0) * (qty / max(float(plan.entry_contracts or qty), qty, 1e-12))
    return round(closed_notional * move, 8)


def _partial_persist(coin: str, plan: ExitPlan, stage: str, *, strict: bool = False) -> bool:
    """Persist close identity/state before mutation and after every read-only resolution."""
    if _paper_trading_enabled() and not strict:
        return True
    try:
        from position_registry import update as _registry_update, get_all_details as _registry_details
        if str(coin or "").upper() not in _registry_details():
            return False
        _registry_update(
            coin,
            size_usdt=float(plan.size_usdt or 0.0),
            metadata={
                "partial_exit_state": dict(plan.partial_exit_state or {}),
                "partial_exit_events": list(plan.partial_exit_events or [])[-100:],
                "entry_contracts": float(plan.entry_contracts or 0.0),
                "confirmed_remaining_contracts": float(plan.confirmed_remaining_contracts or 0.0),
                "exchange_position_timestamp": plan.exchange_position_timestamp,
                "exposure_truth_status": plan.exposure_truth_status,
                "closure_handoff_at": plan.closure_handoff_at,
                "partial_attribution_status": plan.partial_attribution_status,
                "partial_realized_pnl_usdt": float(plan.partial_realized_pnl_usdt or 0.0),
                "partial_fees_usdt": float(plan.partial_fees_usdt or 0.0),
                "realized_pnl_usdt": float(plan.realized_pnl_usdt or 0.0),
            },
            require_persist=bool(strict),
        )
        return True
    except Exception as exc:
        logger.error("Partial lifecycle persistence failed %s stage=%s strict=%s: %s", coin, stage, strict, exc)
        return False


def _partial_economic_event_id(coin: str, plan: ExitPlan, stage: str, event: dict) -> str:
    existing = str(event.get("economic_event_id", "") or "").strip()
    if existing:
        return existing
    fills = ",".join(sorted(str(value) for value in list(event.get("fill_ids", []) or []) if str(value)))
    basis = "|".join([
        str(coin or "").upper(),
        str(stage or ""),
        str(event.get("order_id", "") or ""),
        str(event.get("client_order_id", "") or ""),
        fills,
        f"{float(event.get('verified_filled_contracts', 0.0) or 0.0):.12f}",
        f"{float(event.get('avg_fill_price', 0.0) or 0.0):.12f}",
    ])
    return "partial-economic:" + hashlib.sha256(basis.encode("utf-8")).hexdigest()[:32]


def _record_closure_handoff_reconciliation(coin: str, plan: ExitPlan) -> None:
    """Create the durable PnL-reconciler handoff without inventing local PnL."""
    if _paper_trading_enabled():
        return
    try:
        from pnl_reconciler import reconciler as _pnl_reconciler

        exit_ids = list(plan.exit_order_ids or [])
        for state in (plan.partial_exit_state or {}).values():
            order_id = str(state.get("order_id", "") or "")
            if order_id and order_id not in exit_ids:
                exit_ids.append(order_id)
        trade_id = str(plan.order_id or plan.entry_client_order_id or f"{plan.engine_name}-{coin}")
        _pnl_reconciler.record_provisional(
            trade_id,
            coin=str(coin or "").upper(),
            engine=plan.engine_name,
            direction=plan.direction,
            entry_order_ids=list(plan.entry_order_ids or []),
            entry_order_id=str(plan.order_id or ""),
            entry_client_order_id=str(plan.entry_client_order_id or ""),
            exit_order_ids=exit_ids,
            exit_order_id=str(exit_ids[-1] if exit_ids else ""),
            entry_notional_usdt=float(plan.initial_size_usdt or plan.size_usdt or 0.0),
            gross_price_pnl=0.0,
            local_estimated_net_pnl=None,
            closure_handoff=True,
            exposure_truth_status=EXCHANGE_CLOSED_AWAITING_RECONCILIATION,
            partial_exit_state=dict(plan.partial_exit_state or {}),
            partial_attribution_status=str(plan.partial_attribution_status or "AWAITING_RECONCILIATION"),
        )
    except Exception as exc:
        logger.error("Closure handoff PnL reconciliation setup failed %s: %s", coin, exc)


def _record_partial_performance_once(coin: str, plan: ExitPlan, stage: str) -> None:
    """Record verified partial economics once, including after restart recovery."""
    state = _partial_state(plan, stage)
    if bool(state.get("performance_recorded", False)):
        return
    event = next(
        (
            row for row in reversed(list(plan.partial_exit_events or []))
            if str(row.get("stage", "")) == str(stage)
            and str(row.get("order_id", "")) == str(state.get("order_id", ""))
        ),
        None,
    )
    if not event:
        return
    try:
        event_id = _partial_economic_event_id(coin, plan, stage, event)
        event["economic_event_id"] = event_id
        state["economic_event_id"] = event_id
        _perf.record_trade(
            coin,
            plan.direction,
            plan.entry_price,
            float(event.get("avg_fill_price", plan.entry_price) or plan.entry_price),
            float(event.get("closed_notional", 0.0) or 0.0),
            float(event.get("gross_pnl_usdt", 0.0) or 0.0),
            f"partial_{stage}",
            fee_usdt=float(event.get("fees_usdt", 0.0) or 0.0),
            opened_at=_opened_at_iso(plan),
            closed_at=str(event.get("timestamp", "") or event.get("last_reconciliation_time", "") or ""),
            event_id=event_id,
        )
        state["performance_recorded"] = True
        plan.partial_exit_state[stage] = state
        _partial_persist(coin, plan, stage)
    except Exception as exc:
        logger.warning("Partial performance persistence deferred %s stage=%s: %s", coin, stage, exc)


async def _resolve_partial_readonly(
    coin: str,
    price: float,
    pct: float,
    stage: str,
    plan: ExitPlan,
    state: dict,
    response: dict,
    before_contracts: float,
    requested_contracts: float,
) -> PartialExecutionResult:
    """Resolve one already-attempted close without ever issuing another POST."""
    order_id = str(response.get("ordId", state.get("order_id", "")) or "")
    client_order_id = str(
        response.get("clOrdId", response.get("client_order_id", state.get("client_order_id", ""))) or ""
    )
    order = dict(response or {})
    direct_response_qty = _partial_fill_quantity(response)
    detail_ok = False
    if _okx_client and hasattr(_okx_client, "get_order_status") and (order_id or client_order_id):
        try:
            detail = await _okx_client.get_order_status(coin, order_id=order_id, client_order_id=client_order_id)
            if isinstance(detail, dict):
                if detail.get("classification") == "QUERY_FAILED" or detail.get("ok") is False:
                    detail_ok = False
                if detail.get("found") and detail.get("order"):
                    order.update(dict(detail.get("order") or {}))
                    order_id = str(order.get("ordId", "") or order_id)
                    client_order_id = str(order.get("clOrdId", "") or client_order_id)
                    detail_ok = True
        except Exception:
            detail_ok = False

    fills: list[dict] = []
    fills_ok = bool(order_id or client_order_id) and hasattr(_okx_client, "get_fills_status")
    if _okx_client and hasattr(_okx_client, "get_fills_status") and (order_id or client_order_id):
        try:
            coin_inst = f"{str(coin).upper()}-USDT-SWAP"
            evidence = await _okx_client.get_fills_status(inst_id=coin_inst, order_id=order_id)
            if isinstance(evidence, dict):
                if evidence.get("status") == "QUERY_FAILED" or evidence.get("ok") is False:
                    fills_ok = False
                fills = list(evidence.get("fills", []) or [])
        except TypeError:
            try:
                evidence = await _okx_client.get_fills_status(coin_inst, order_id=order_id)
                if isinstance(evidence, dict):
                    if evidence.get("status") == "QUERY_FAILED" or evidence.get("ok") is False:
                        fills_ok = False
                    fills = list(evidence.get("fills", []) or [])
            except Exception:
                fills_ok = False
        except Exception:
            fills_ok = False
    fills = _partial_deduplicate_fills(fills)
    if order_id:
        fills = [row for row in fills if not row.get("ordId") or str(row.get("ordId")) == order_id]

    if _paper_trading_enabled():
        after = {
            "ok": True,
            "present": True,
            "contracts": max(float(before_contracts or 0.0) - float(requested_contracts or 0.0), 0.0),
            "row": {},
        }
    else:
        after = await _partial_position_snapshot(coin, plan)
    if after is None:
        state["lifecycle"] = PARTIAL_UNCERTAIN
        state["recovery_state"] = "POSITION_READ_FAILED"
        plan.partial_exit_state[stage] = state
        _partial_persist(coin, plan, stage, strict=not _paper_trading_enabled())
        return PartialExecutionResult(
            ok=False, uncertain=True, state=PARTIAL_UNCERTAIN, order_id=order_id,
            recovery_state="POSITION_READ_FAILED",
        )

    instrument = _partial_instrument(coin)
    fill_qty = sum(_partial_fill_quantity(row) for row in fills)
    detail_qty = _partial_fill_quantity(order)
    if fill_qty <= 0.0:
        fill_qty = detail_qty or direct_response_qty
    after_contracts = float(after.get("contracts", 0.0) or 0.0)
    if not _paper_trading_enabled():
        _set_exchange_exposure_truth(
            coin,
            plan,
            after_contracts,
            attribution_status="UNCERTAIN" if after_contracts > 0.0 else "AWAITING_RECONCILIATION",
        )
    if not _paper_trading_enabled() and after_contracts <= 0.0:
        state.update({
            "lifecycle": PARTIAL_UNCERTAIN,
            "recovery_state": "EXCHANGE_CLOSED_AWAITING_RECONCILIATION",
            "economics_status": "AWAITING_RECONCILIATION",
            "fill_attribution_status": "UNCERTAIN",
            "fill_quantity": fill_qty,
            "position_delta": max(float(before_contracts or 0.0), 0.0),
            "remaining_exchange_contracts": 0.0,
            "order_id": order_id,
            "client_order_id": client_order_id,
            "last_reconciliation_time": datetime.now(timezone.utc).isoformat(),
        })
        plan.partial_attribution_status = "AWAITING_RECONCILIATION"
        if order_id and order_id not in plan.exit_order_ids:
            plan.exit_order_ids.append(order_id)
        plan.partial_exit_state[stage] = state
        _partial_persist(coin, plan, stage, strict=True)
        _record_closure_handoff_reconciliation(coin, plan)
        logger.warning(
            "Partial close %s confirmed zero exchange exposure; economics remain uncertain and local action is halted.",
            coin,
        )
        return PartialExecutionResult(
            ok=False,
            uncertain=True,
            state=PARTIAL_UNCERTAIN,
            order_id=order_id,
            recovery_state="EXCHANGE_CLOSED_AWAITING_RECONCILIATION",
            fill_quantity=fill_qty,
            position_delta=max(float(before_contracts or 0.0), 0.0),
            remaining_contracts=0.0,
        )
    delta_qty = max(float(before_contracts or 0.0) - after_contracts, 0.0)
    order_state = str(order.get("state", "") or "").lower()
    if fill_qty > 0.0 and not _partial_contracts_equal(fill_qty, delta_qty, instrument):
        state["lifecycle"] = PARTIAL_UNCERTAIN
        state["recovery_state"] = "FILL_POSITION_DELTA_MISMATCH"
        state["fill_quantity"] = fill_qty
        state["position_delta"] = delta_qty
        state["remaining_exchange_contracts"] = after_contracts
        state["order_id"] = order_id
        state["client_order_id"] = client_order_id
        plan.partial_exit_state[stage] = state
        _partial_persist(coin, plan, stage, strict=not _paper_trading_enabled())
        return PartialExecutionResult(
            ok=False, uncertain=True, state=PARTIAL_UNCERTAIN, order_id=order_id,
            recovery_state="FILL_POSITION_DELTA_MISMATCH",
            fill_quantity=fill_qty, position_delta=delta_qty,
        )

    # A position delta by itself can corroborate a fill only when it matches
    # the requested close. A larger delta indicates a concurrent protection or
    # full-close race and must not be booked as this partial intent.
    if fill_qty <= 0.0 and delta_qty > 0.0 and not (
        detail_ok
        and order_state in {"filled", "partially_filled", "effective"}
        and _partial_contracts_equal(delta_qty, requested_contracts, instrument)
    ):
        state["lifecycle"] = PARTIAL_UNCERTAIN
        state["recovery_state"] = "POSITION_DELTA_EXCEEDS_REQUEST"
        state["position_delta"] = delta_qty
        state["remaining_exchange_contracts"] = after_contracts
        state["order_id"] = order_id
        state["client_order_id"] = client_order_id
        plan.partial_exit_state[stage] = state
        _partial_persist(coin, plan, stage)
        return PartialExecutionResult(
            ok=False, uncertain=True, state=PARTIAL_UNCERTAIN, order_id=order_id,
            recovery_state="POSITION_DELTA_EXCEEDS_REQUEST", position_delta=delta_qty,
        )

    verified_closed = fill_qty if fill_qty > 0.0 else delta_qty
    zero_fill_rejected = verified_closed <= 0.0 and (
        order_state in {"rejected", "canceled", "cancelled", "failed"}
        or (
            order_state not in {"live", "partially_filled", "effective", "new", "open", "pending", "submitted"}
            and (detail_ok and fills_ok)
            and _partial_contracts_equal(after_contracts, before_contracts, instrument)
        )
    )
    if zero_fill_rejected:
        state["lifecycle"] = PARTIAL_REJECTED
        state["recovery_state"] = "CONFIRMED_NO_FILL"
        plan.partial_exit_state[stage] = state
        _partial_persist(coin, plan, stage)
        return PartialExecutionResult(ok=False, state=PARTIAL_REJECTED, order_id=order_id, fill_quantity=0.0)

    if verified_closed <= 0.0 or not (detail_ok or fills or direct_response_qty > 0.0 or detail_qty > 0.0):
        state["lifecycle"] = PARTIAL_UNCERTAIN
        state["recovery_state"] = "FILL_EVIDENCE_INCOMPLETE"
        plan.partial_exit_state[stage] = state
        _partial_persist(coin, plan, stage)
        return PartialExecutionResult(
            ok=False, uncertain=True, state=PARTIAL_UNCERTAIN, order_id=order_id,
            recovery_state="FILL_EVIDENCE_INCOMPLETE",
        )

    fill_price = 0.0
    weighted_qty = 0.0
    for row in fills:
        quantity = _partial_fill_quantity(row)
        row_price = _partial_fill_price(row)
        if quantity > 0.0 and row_price > 0.0:
            fill_price += quantity * row_price
            weighted_qty += quantity
    fill_price = (fill_price / weighted_qty) if weighted_qty > 0.0 else _partial_fill_price(order)
    if fill_price <= 0.0:
        fill_price = float(price or 0.0)
    if fill_price <= 0.0:
        state["lifecycle"] = PARTIAL_UNCERTAIN
        state["recovery_state"] = "FILL_PRICE_MISSING"
        plan.partial_exit_state[stage] = state
        _partial_persist(coin, plan, stage)
        return PartialExecutionResult(ok=False, uncertain=True, state=PARTIAL_UNCERTAIN, order_id=order_id)

    fees_known = bool(fills) and all(row.get("fee") not in {None, ""} for row in fills)
    fees = sum(max(-float(row.get("fee", 0.0) or 0.0), 0.0) for row in fills) if fees_known else 0.0
    gross = _partial_gross_pnl(plan, verified_closed, fill_price, instrument)
    remaining = after_contracts
    complete = _partial_contracts_equal(verified_closed, requested_contracts, instrument) or verified_closed >= before_contracts - max(float(getattr(instrument, "lot_sz", 0.0) or 0.0) / 2.0, 1e-8)
    lifecycle = PARTIAL_FILLED if complete else PARTIAL_PARTIALLY_FILLED
    fill_at = str(order.get("fillTime", order.get("uTime", "")) or datetime.now(timezone.utc).isoformat())
    state.update({
        "lifecycle": lifecycle,
        "recovery_state": "VERIFIED_EXCHANGE_FILL",
        "order_id": order_id,
        "client_order_id": client_order_id,
        "requested_close_contracts": float(requested_contracts or 0.0),
        "verified_filled_contracts": float(verified_closed),
        "remaining_exchange_contracts": float(remaining),
        "average_fill_price": float(fill_price),
        "fees_usdt": float(fees),
        "fees_known": bool(fees_known),
        "last_reconciliation_time": datetime.now(timezone.utc).isoformat(),
    })
    plan.partial_exit_state[stage] = state
    if order_id and order_id not in plan.exit_order_ids:
        plan.exit_order_ids.append(order_id)
    plan.first_exit_fill_at = plan.first_exit_fill_at or fill_at
    plan.last_exit_fill_at = fill_at
    if plan.entry_contracts <= 0.0:
        plan.entry_contracts = float(before_contracts or 0.0)
    plan.confirmed_remaining_contracts = float(remaining)
    if float(plan.initial_size_usdt or 0.0) <= 0.0:
        plan.initial_size_usdt = float(plan.size_usdt or 0.0)
    closed_notional = (
        instrument.contracts_to_notional(verified_closed, plan.entry_price)
        if instrument is not None else float(plan.size_usdt or 0.0) * (verified_closed / max(before_contracts, 1e-12))
    )
    if instrument is not None:
        plan.size_usdt = round(instrument.contracts_to_notional(remaining, plan.entry_price), 8)
    else:
        plan.size_usdt = round(max(float(plan.size_usdt or 0.0) - closed_notional, 0.0), 8)
    _refresh_remaining_pct(plan)
    plan.realized_pnl_usdt = round(float(plan.realized_pnl_usdt or 0.0) + gross, 8)
    plan.partial_realized_pnl_usdt = round(float(plan.partial_realized_pnl_usdt or 0.0) + gross, 8)
    plan.partial_fees_usdt = round(float(plan.partial_fees_usdt or 0.0) + fees, 8)
    event = {
        "stage": str(stage),
        "order_id": order_id,
        "client_order_id": client_order_id,
        "requested_close_contracts": float(requested_contracts or 0.0),
        "verified_filled_contracts": float(verified_closed),
        "remaining_exchange_contracts": float(remaining),
        "partial_state": lifecycle,
        "fill_count": len(fills),
        "fill_ids": [_partial_fill_identity(row) for row in fills],
        "avg_fill_price": float(fill_price),
        "closed_notional": float(closed_notional),
        "fees_usdt": float(fees),
        "fees_known": bool(fees_known),
        "gross_pnl_usdt": float(gross),
        "net_pnl_usdt": float(gross - fees) if fees_known else None,
        "verification_source": "fills+position_delta" if fills and delta_qty > 0.0 else ("fills" if fills else "order+position_delta"),
        "last_reconciliation_time": datetime.now(timezone.utc).isoformat(),
        "timestamp": fill_at,
    }
    if not any(str(row.get("order_id", "")) == order_id and str(row.get("stage", "")) == str(stage) for row in plan.partial_exit_events):
        plan.partial_exit_events.append(event)
    _partial_persist(coin, plan, stage)
    _record_partial_performance_once(coin, plan, stage)
    return PartialExecutionResult(
        ok=True,
        state=lifecycle,
        milestone_complete=complete,
        order_id=order_id,
        client_order_id=client_order_id,
        requested_contracts=float(requested_contracts or 0.0),
        verified_filled_contracts=float(verified_closed),
        remaining_contracts=float(remaining),
        closed_notional=float(closed_notional),
        fill_price=float(fill_price),
        gross_pnl_usdt=float(gross),
        fees_usdt=float(fees),
        fees_known=bool(fees_known),
        net_pnl_usdt=float(gross - fees) if fees_known else None,
        fill_count=len(fills),
        verification_source=event["verification_source"],
    )


async def _execute_partial(coin: str, price: float, pct: float, *, stage: str = "partial") -> PartialExecutionResult:
    plan = _plans.get(coin)
    if not plan or not _okx_client:
        return PartialExecutionResult(ok=False, state=PARTIAL_REJECTED, recovery_state="plan_or_client_missing")
    stage = str(stage or "partial")
    if plan.exposure_truth_status == EXCHANGE_CLOSED_AWAITING_RECONCILIATION:
        return PartialExecutionResult(
            ok=False,
            uncertain=True,
            state=PARTIAL_UNCERTAIN,
            recovery_state="EXCHANGE_CLOSED_AWAITING_RECONCILIATION",
            remaining_contracts=0.0,
        )
    pct = max(float(pct or 0.0), 0.0)
    if pct <= 0.0 or float(plan.size_usdt or 0.0) <= 0.0:
        return PartialExecutionResult(ok=False, state=PARTIAL_REJECTED, recovery_state="invalid_requested_percentage")
    previous = _partial_state(plan, stage)
    previous_lifecycle = str(previous.get("lifecycle", "") or "")
    if previous_lifecycle in {PARTIAL_SUBMISSION_ATTEMPTED, PARTIAL_ACKNOWLEDGED, PARTIAL_UNCERTAIN}:
        return await _resolve_partial_readonly(
            coin, price, pct, stage, plan, previous,
            dict(previous.get("response", {}) or {}),
            float(previous.get("before_contracts", 0.0) or 0.0),
            float(previous.get("requested_close_contracts", 0.0) or 0.0),
        )
    if previous_lifecycle in {PARTIAL_PARTIALLY_FILLED, PARTIAL_FILLED, PARTIAL_REJECTED}:
        if previous_lifecycle in {PARTIAL_PARTIALLY_FILLED, PARTIAL_FILLED}:
            _record_partial_performance_once(coin, plan, stage)
        return PartialExecutionResult(
            ok=False,
            state=previous_lifecycle,
            recovery_state="already_resolved_no_replay",
            verified_filled_contracts=float(previous.get("verified_filled_contracts", 0.0) or 0.0),
            remaining_contracts=float(previous.get("remaining_exchange_contracts", plan.confirmed_remaining_contracts) or 0.0),
        )

    # Existing durable partial intents may be resolved read-only even while
    # local exposure is unresolved.  Only a new mutation requires an
    # explicitly active local row.
    mutation_allowed, mutation_reason = _local_mutation_authorized(coin)
    if not mutation_allowed:
        return PartialExecutionResult(
            ok=False,
            uncertain=True,
            state=PARTIAL_UNCERTAIN,
            recovery_state=f"{mutation_reason}:no_mutation",
        )

    instrument = _partial_instrument(coin)
    if _paper_trading_enabled():
        before_contracts = float(plan.confirmed_remaining_contracts or plan.entry_contracts or plan.size_usdt or 0.0)
    else:
        snapshot = await _partial_position_snapshot(coin, plan)
        if snapshot is None or not bool(snapshot.get("present", False)):
            return PartialExecutionResult(ok=False, state=PARTIAL_UNCERTAIN, recovery_state="position_unavailable_before_post")
        before_contracts = float(snapshot.get("contracts", 0.0) or 0.0)
        if instrument is None:
            return PartialExecutionResult(ok=False, state=PARTIAL_REJECTED, recovery_state="instrument_metadata_missing")
        _set_exchange_exposure_truth(
            coin,
            plan,
            before_contracts,
            attribution_status="PENDING",
        )
    if before_contracts <= 0.0:
        return PartialExecutionResult(ok=False, state=PARTIAL_REJECTED, recovery_state="zero_position_before_post")
    if plan.entry_contracts <= 0.0:
        plan.entry_contracts = before_contracts
    requested_notional = float(plan.size_usdt or 0.0) * pct / 100.0
    requested_contracts = before_contracts * pct / 100.0 if _paper_trading_enabled() else instrument.notional_to_contracts(requested_notional, float(price or 0.0))
    if instrument is not None:
        requested_contracts = instrument.round_contracts_down(min(requested_contracts, before_contracts))
    else:
        requested_contracts = min(requested_contracts, before_contracts)
    if requested_contracts <= 0.0:
        return PartialExecutionResult(ok=False, state=PARTIAL_REJECTED, recovery_state="zero_requested_contracts")

    intent_id = str(previous.get("intent_id", "") or f"PARTIAL-{str(plan.order_id or plan.entry_client_order_id or coin).upper()}-{stage.upper()}")
    try:
        from execution_router import deterministic_client_order_id
        deterministic_clordid = deterministic_client_order_id(intent_id, "C")
    except Exception:
        deterministic_clordid = intent_id[:32]
    state = {
        **previous,
        "intent_id": intent_id,
        "stage": stage,
        "requested_close_pct": pct,
        "requested_close_contracts": float(requested_contracts),
        "before_contracts": float(before_contracts),
        "lifecycle": PARTIAL_REQUESTED,
        "mutation_attempted": False,
        "client_order_id": str(previous.get("client_order_id", "") or deterministic_clordid),
        "created_at": previous.get("created_at", datetime.now(timezone.utc).isoformat()),
    }
    plan.partial_exit_state[stage] = state
    if not _partial_persist(coin, plan, stage, strict=not _paper_trading_enabled()):
        return PartialExecutionResult(ok=False, state=PARTIAL_REJECTED, recovery_state="intent_persistence_failed_before_post")
    state["lifecycle"] = PARTIAL_SUBMISSION_ATTEMPTED
    state["mutation_attempted"] = True
    plan.partial_exit_state[stage] = state
    if not _partial_persist(coin, plan, stage, strict=not _paper_trading_enabled()):
        state["lifecycle"] = PARTIAL_UNCERTAIN
        state["recovery_state"] = "attempt_persistence_failed_before_post"
        plan.partial_exit_state[stage] = state
        return PartialExecutionResult(ok=False, state=PARTIAL_UNCERTAIN, recovery_state="attempt_persistence_failed_before_post")

    pos_side = "long" if plan.direction == "long" else "short"
    try:
        if _paper_trading_enabled():
            # Paper mode is a local simulation boundary. Do not route it
            # through the exchange-truth operator-close path or imply that a
            # transport POST occurred; the synthetic identity is only for
            # deterministic local lifecycle attribution.
            response = {
                "status": "PAPER_LOCAL",
                "ordId": f"PAPER-{intent_id}",
                "clOrdId": str(previous.get("client_order_id", "") or deterministic_clordid),
                "state": "filled",
                "fillSz": float(requested_contracts),
                "fillPx": float(price or 0.0),
                "post_attempted": False,
            }
        else:
            from execution_router import router_for
            response = await router_for(_okx_client).close(
                coin,
                pos_side,
                requested_notional,
                contracts=requested_contracts,
                partial=True,
                intent_id=intent_id,
            )
        if not isinstance(response, dict):
            response = {}
        nested_response = response.get("response")
        if isinstance(nested_response, dict):
            lifecycle_fields = {
                key: response[key]
                for key in (
                    "status", "intent_id", "client_order_id", "exchange_order_id",
                    "reason", "post_count", "remaining_contracts",
                )
                if key in response
            }
            response = {**response, **nested_response}
            response.update(lifecycle_fields)
        state["lifecycle"] = PARTIAL_ACKNOWLEDGED
        state["response"] = dict(response)
        state["order_id"] = str(
            response.get("ordId", response.get("exchange_order_id", "")) or ""
        )
        state["client_order_id"] = str(
            response.get("clOrdId", response.get("client_order_id", "")) or ""
        )
        plan.partial_exit_state[stage] = state
        _partial_persist(coin, plan, stage)
    except Exception as exc:
        state["lifecycle"] = PARTIAL_UNCERTAIN
        state["recovery_state"] = f"post_exception:{type(exc).__name__}"
        plan.partial_exit_state[stage] = state
        _partial_persist(coin, plan, stage)
        logger.error("Partial exit %s POST outcome uncertain: %s", coin, exc)
        return await _resolve_partial_readonly(
            coin, price, pct, stage, plan, state, {}, before_contracts, requested_contracts,
        )
    if _paper_trading_enabled():
        # Paper mode has a deterministic local fill; it is intentionally
        # isolated from exchange-readback requirements.
        response.setdefault("ordId", state.get("client_order_id", intent_id))
        response.setdefault("clOrdId", state.get("client_order_id", ""))
        response.setdefault("fillSz", requested_contracts)
        response.setdefault("fillPx", price)
        response.setdefault("fee", 0.0)
        state["response"] = response
    return await _resolve_partial_readonly(
        coin, price, pct, stage, plan, state, response, before_contracts, requested_contracts,
    )


def _calc_pnl(plan: ExitPlan, exit_price: float, pct: float) -> float:
    """
    Calculate PnL using per-trade leverage stored in the ExitPlan.
    Formula: notional x move_pct.
    """
    size = plan.size_usdt * pct / 100
    return round(_calc_pnl_at_size(plan, exit_price, size), 4)


def _calc_pnl_at_size(plan: ExitPlan, exit_price: float, size_usdt: float) -> float:
    entry = float(plan.entry_price or 0.0)
    if entry <= 0 or float(exit_price or 0.0) <= 0:
        return 0.0
    move = ((float(exit_price) - entry) / entry
            if plan.direction == "long"
            else (entry - float(exit_price)) / entry)
    return float(size_usdt or 0.0) * move


def _refresh_remaining_pct(plan: ExitPlan) -> None:
    base = float(plan.initial_size_usdt or 0.0)
    if base <= 0:
        plan.remaining_pct = 0.0
        return
    plan.remaining_pct = round(max(min((plan.size_usdt / base) * 100.0, 100.0), 0.0), 4)


def _estimated_total_fee_usdt(plan: ExitPlan) -> float:
    base_size = float(plan.initial_size_usdt or plan.size_usdt or 0.0)
    return max(fee_cost_usdt(base_size, plan.leverage), 0.0)


def _total_trade_pnl_usdt(plan: ExitPlan, price: float) -> float:
    return round(float(plan.realized_pnl_usdt or 0.0) + _calc_pnl(plan, price, 100.0), 4)


def _partial_profit_floor_usdt(plan: ExitPlan, close_notional: float) -> float:
    configured_floor = float(_s.get_val("min_net_take_profit_usdt", 0.05) or 0.05)
    fee_floor = fee_cost_usdt(max(float(close_notional or 0.0), 0.0), int(plan.leverage or 1)) * 1.50
    return round(max(configured_floor, fee_floor), 6)


def _full_profit_floor_usdt(plan: ExitPlan) -> float:
    """Compatibility name for the current-remainder cost-relative floor.

    ``min_full_close_profit_usdt`` is intentionally not consulted here.  The
    old setting remains an informational/historical control; all lifecycle
    gates use current costs multiplied by the configured fee multiple.
    """
    current_size = float(plan.size_usdt or 0.0)
    if current_size <= 0.0:
        return 0.0
    return round(
        fee_cost_usdt(current_size, int(plan.leverage or 1))
        * _minimum_net_capture_fee_multiple(),
        6,
    )


def _stop_price_for_total_profit(plan: ExitPlan, total_profit_usdt: float, is_long: bool) -> float | None:
    remaining_size = float(plan.size_usdt or 0.0)
    entry = float(plan.entry_price or 0.0)
    if remaining_size <= 0 or entry <= 0:
        return None
    required_from_open = max(float(total_profit_usdt or 0.0) - float(plan.realized_pnl_usdt or 0.0), 0.0)
    move_frac = required_from_open / remaining_size
    return round(entry * (1 + move_frac if is_long else 1 - move_frac), 8)


def _trail_distance(plan: ExitPlan, reference_price: float, pct_trail: float) -> float:
    pct_dist = max(float(reference_price or 0.0) * float(pct_trail or 0.0), 0.0)
    atr_dist = max(float(plan.atr_val or 0.0) * float(_s.get_val("trail_distance_atr_mult", 2.5) or 2.5), 0.0)
    mode = str(_s.get_val("trail_distance_mode", "hybrid") or "hybrid").lower()
    if mode == "atr" and atr_dist > 0:
        return atr_dist
    if mode == "percent" or atr_dist <= 0:
        return pct_dist
    return max(pct_dist, atr_dist)


def _improves_stop(plan: ExitPlan, new_sl: float | None, is_long: bool) -> bool:
    if new_sl is None or new_sl <= 0:
        return False
    return (is_long and new_sl > plan.hard_sl) or (not is_long and (plan.hard_sl == 0 or new_sl < plan.hard_sl))


def _apply_stop_improvement(coin: str, plan: ExitPlan, new_sl: float | None, is_long: bool, reason: str, extra: str = "") -> None:
    if not _improves_stop(plan, new_sl, is_long):
        return
    plan.hard_sl = round(float(new_sl), 8)
    plan.desired_stop_price = plan.hard_sl
    plan.local_effective_stop_price = float(plan.exchange_confirmed_stop_price or 0.0)
    plan.tsl_price = plan.hard_sl
    plan.tsl_armed = True
    plan.last_protection_reason = reason
    if not _paper_trading_enabled():
        plan.exchange_protection_status = "STOP_PROPOSED"
    _record_exit_transition(
        plan,
        "STOP_IMPROVEMENT_PROPOSED",
        desired_stop_price=round(float(plan.desired_stop_price), 12),
        reason=str(reason or ""),
    )
    _record_exit_transition(
        plan,
        "TRAILING_MOVED" if "trail" in str(reason).lower() or "tsl" in str(reason).lower() else "STOP_MOVED",
        reason=str(reason or ""),
        stop_price=round(float(plan.hard_sl or 0.0), 12),
    )
    logger.info("%s %s: SL -> %.8f%s", reason.upper(), coin, plan.hard_sl, f" {extra}" if extra else "")


def _apply_legacy_profit_lock(coin: str, plan: ExitPlan, price: float, is_long: bool) -> None:
    _lev = _position_leverage(plan)
    if _lev <= 0:
        _record_exit_transition(plan, "PROFIT_LOCK_DEFERRED", reason="leverage_unknown")
        return
    lock_levels = [
        (1, float(_s.get_val("profit_lock_l1_trigger_pct", 15.0)), float(_s.get_val("profit_lock_l1_floor_pct", 5.0))),
        (2, float(_s.get_val("profit_lock_l2_trigger_pct", 25.0)), float(_s.get_val("profit_lock_l2_floor_pct", 10.0))),
        (3, float(_s.get_val("profit_lock_l3_trigger_pct", 40.0)), float(_s.get_val("profit_lock_l3_floor_pct", 20.0))),
    ]
    for lvl, trig_pct, floor_pct in lock_levels:
        if plan.profit_lock_level >= lvl:
            continue
        trigger = trig_pct / 100.0 / _lev
        floor = floor_pct / 100.0 / _lev
        hit = (
            (is_long and price >= plan.entry_price * (1 + trigger)) or
            (not is_long and price <= plan.entry_price * (1 - trigger))
        )
        if not hit:
            continue
        new_sl = round(plan.entry_price * (1 + floor if is_long else 1 - floor), 8)
        economics, required_floor, eligible = _protection_candidate_economics(plan, price, new_sl)
        if eligible:
            _apply_stop_improvement(
                coin,
                plan,
                new_sl,
                is_long,
                f"profit_lock_l{lvl}",
                f"(legacy lock +{floor_pct:.1f}% x {_lev}x candidate_net="
                f"${float(economics.candidate_stop_estimated_net_pnl or 0.0):.6f})",
            )
            if plan.last_protection_reason == f"profit_lock_l{lvl}":
                plan.profit_lock_level = lvl
        else:
            _record_protection_economic_defer(
                plan,
                stage=f"legacy_profit_lock_l{lvl}",
                economics=economics,
                required_floor=required_floor,
            )


def _apply_secure_profit_model(coin: str, plan: ExitPlan, price: float, is_long: bool) -> None:
    """Legacy compatibility path using candidate-stop economics only."""
    if _s.get_val("breakeven_enabled", True) and not plan.breakeven_moved:
        new_sl = _fee_secure_stop_price(plan, price, is_long)
        economics, required_floor, eligible = _protection_candidate_economics(plan, price, new_sl)
        if eligible:
            _apply_stop_improvement(coin, plan, new_sl, is_long, "breakeven", "(candidate-stop economics)")
            if plan.last_protection_reason == "breakeven":
                plan.breakeven_moved = True
        elif new_sl:
            _record_protection_economic_defer(plan, stage="legacy_breakeven", economics=economics, required_floor=required_floor)

    if not _s.get_val("profit_lock_enabled", True):
        return
    lock_roi = float(_s.get_val("profit_lock_buffer_pct", 0.65) or 0.65)
    lock_frac = _roi_pct_to_price_frac(lock_roi, _position_leverage(plan))
    new_sl = plan.entry_price * (1.0 + lock_frac if is_long else 1.0 - lock_frac)
    economics, required_floor, eligible = _protection_candidate_economics(plan, price, new_sl)
    if eligible:
        _apply_stop_improvement(coin, plan, new_sl, is_long, "profit_lock_l1", "(candidate-stop economics)")
        if plan.last_protection_reason == "profit_lock_l1":
            plan.profit_lock_level = max(plan.profit_lock_level, 1)
    else:
        _record_protection_economic_defer(plan, stage="legacy_profit_lock", economics=economics, required_floor=required_floor)


def _close_reason_for_stop_hit(plan: ExitPlan, price: float, base_reason: str) -> str:
    if base_reason == "tsl":
        return "trail_stop"
    if base_reason != "sl":
        return base_reason
    pnl = _calc_pnl(plan, price, 100.0)
    if pnl >= 0 and str(plan.last_protection_reason).startswith("profit_lock"):
        return "profit_lock"
    if pnl >= 0 and plan.breakeven_moved:
        return "breakeven_stop"
    return "sl"


def _trail_semantic_reason(plan: ExitPlan, pnl_usdt: float, base_reason: str) -> str:
    if "trail" not in str(base_reason or "").lower() and str(base_reason or "").lower() != "tsl":
        return ""
    if plan.tp2_done:
        return "TRAIL_AFTER_TP2"
    if plan.tp1_done:
        return "TRAIL_AFTER_TP1"
    net_exit = float(pnl_usdt or 0.0) - float(_estimated_total_fee_usdt(plan) or 0.0)
    if float(plan.trail_activation_net_pnl or 0.0) > 0 and net_exit >= 0:
        return "TRAIL_PROFIT"
    return "TRAIL_RISK"

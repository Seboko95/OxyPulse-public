"""
OxyPulse – trading_engine.py  v6
Multi-engine orchestrator. One CategoryEngine per strategy engine.
Provides the `engine` singleton consumed by bot.py.
"""
from __future__ import annotations
import asyncio
import math
import json
import logging
import os
import time
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from category_engine import CategoryEngine
from category_tracker import (
    ENGINE_NAMES,
    ENGINE_EMOJI,
    get_combined_stats,
    get_engine_guard_status,
    get_engine_recent_performance,
    record_trade as category_record,
)
from coin_universe import get_futures_universe
from market_data_stream import (
    refresh_funding_batch,
    latest_price,
    market_readiness_snapshot,
)
from position_registry import (
    count as registry_count,
    get_active_details as registry_active_details,
    get_restorable_details as registry_restorable_details,
    get_nonterminal_details as registry_nonterminal_details,
    get_all_details as registry_all_details,
    get_unknown_details as registry_unknown_details,
    exposure_status as registry_exposure_status,
    is_unresolved_exposure,
    is_actionable_position,
    mark_sync_state as registry_mark_sync_state,
    register as registry_register,
    release as registry_release,
    release_uncommitted_pending_for_engine as registry_release_uncommitted_pending,
    update as registry_update,
    derive_exchange_lifecycle_anchor,
    derive_exchange_adopted_lifecycle_id,
    resolve_position_lifecycle,
)
from ws_stream import stream as _ws_stream
import okx_client as _okx_mod
from okx_client import OKXClient
import market_mode as mm
import exit_manager as _em
import performance_tracker as _perf
import settings_store as _s
import sync_health as _sync_health
import regime_detector as _regime
import production_research_funnel as _research_funnel
import latency_telemetry as _latency
from risk_manager import fee_cost_usdt
from strategy_categories import clear_cache as clear_strategy_cache

logger = logging.getLogger("OxyPulse.Engine")


def _positive_net_shadow_protected(engine_name: str, current_mode: str, perf: dict, guard: dict | None = None) -> tuple[bool, str]:
    if str(current_mode or "").lower() != "probation":
        return False, ""
    last10 = float((perf or {}).get("last10_net", 0.0) or 0.0)
    last30 = float((perf or {}).get("last30_net", 0.0) or 0.0)
    if last30 <= 0:
        return False, ""
    if last10 < 0 and last30 < 0:
        return False, ""
    return True, f"positive last30 net ${last30:+.2f}; keeping contribution rights active"


def _positive_net_shadow_rehab_candidate(engine_name: str, current_mode: str, perf: dict) -> tuple[bool, str]:
    if str(current_mode or "").lower() != "shadow":
        return False, ""
    if not bool(_s.get_val("positive_net_shadow_rehab_enabled", True)):
        return False, ""
    allowed = {
        str(item or "").strip().upper()
        for item in str(_s.get_val("positive_net_shadow_rehab_engines", "RONIN,RYUJIN") or "").split(",")
        if str(item or "").strip()
    }
    name = str(engine_name or "").upper()
    if name not in allowed:
        return False, ""
    last10 = float((perf or {}).get("last10_net", 0.0) or 0.0)
    last30 = float((perf or {}).get("last30_net", 0.0) or 0.0)
    if last30 <= 0:
        return False, ""
    if last10 < 0 and last30 < 0:
        return False, ""
    return True, f"positive last30 net ${last30:+.2f}; restoring probation contribution rights"

SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL_FUTURES", "60"))
SYNC_INTERVAL_SEC = 180
REFRESH_15M_SEC = 900
REFRESH_1H_SEC = 3600
REFRESH_4H_SEC = 14400
REFRESH_1D_SEC = 86400
REFRESH_FUNDING_SEC = 60
ENGINE_GUARD_INTERVAL_SEC = 900
ENGINE_NOTICE_REMINDER_SEC = 6 * 3600

engine_notice_state: dict[str, dict] = {}

_RECOVERY_MANAGED = "managed"
_RECOVERY_QUARANTINED = "quarantined"
_RECOVERY_LEGACY = "legacy"
_POLICY_TAG = "2026-04-02"


def _pnl_reconciliation_context(details: dict) -> dict:
    keys = (
        "trade_id", "entry_order_id", "entry_order_ids", "entry_client_order_id", "entry_clOrdId",
        "exit_order_id", "exit_order_ids", "position_id", "pos_id", "first_fill_at",
        "last_entry_fill_at", "first_exit_fill_at", "last_exit_fill_at",
    )
    return {key: details.get(key) for key in keys if key in details}


def _engine_notice_hash(
    *,
    engine_name: str,
    mode: str,
    protection_state: str,
    reason: str,
    guard_note: str = "",
    role: str = "",
) -> str:
    return "|".join(
        [
            str(engine_name or "").upper(),
            str(mode or "").lower(),
            str(protection_state or "").lower(),
            str(role or "").lower(),
            str(reason or "").strip(),
            str(guard_note or "").strip(),
        ]
    )


def _should_send_engine_notice(
    engine_name: str,
    *,
    mode: str,
    protection_state: str,
    reason: str,
    guard_note: str = "",
    role: str = "",
    reminder_sec: float | None = None,
) -> bool:
    """Deduplicate engine guard notices while preserving state-change alerts."""
    now = time.time()
    reminder = float(reminder_sec if reminder_sec is not None else ENGINE_NOTICE_REMINDER_SEC)
    state_hash = _engine_notice_hash(
        engine_name=engine_name,
        mode=mode,
        protection_state=protection_state,
        reason=reason,
        guard_note=guard_note,
        role=role,
    )
    key = str(engine_name or "").upper()
    prev = engine_notice_state.get(key, {}) or {}
    changed = (
        prev.get("state_hash") != state_hash
        or prev.get("last_mode") != str(mode or "").lower()
        or prev.get("last_reason") != str(reason or "")
        or prev.get("last_protection_state") != str(protection_state or "").lower()
        or prev.get("last_role") != str(role or "").lower()
    )
    elapsed = now - float(prev.get("last_sent_ts", 0.0) or 0.0)
    if changed or elapsed >= reminder:
        engine_notice_state[key] = {
            "state_hash": state_hash,
            "last_mode": str(mode or "").lower(),
            "last_reason": str(reason or ""),
            "last_sent_ts": now,
            "last_protection_state": str(protection_state or "").lower(),
            "last_role": str(role or "").lower(),
        }
        return True
    return False


# ── Multi-engine orchestrator ─────────────────────────────────────────────────

class MultiEngine:
    def __init__(self):
        self.engines: dict[str, CategoryEngine] = {
            name: CategoryEngine(name) for name in ENGINE_NAMES
        }

        self.running = False
        self.research_only = False
        self._scan_task: Optional[asyncio.Task] = None
        self._position_sync_task: Optional[asyncio.Task] = None
        self._funding_refresh_task: Optional[asyncio.Task] = None
        self._market_refresh_ts = {
            "15m": 0.0,
            "1h": 0.0,
            "4h": 0.0,
            "1d": 0.0,
            "funding": 0.0,
        }
        self._last_sync_ts: float = 0.0
        self._sync_failure_count: int = 0
        self._last_sync_error: str = ""
        self._last_engine_guard_ts: float = 0.0
        self._last_balance_usdt: float = 0.0
        self._last_scan_report: dict = {
            "started_at": "",
            "finished_at": "",
            "duration_sec": 0.0,
            "universe_count": 0,
            "active_engine_count": 0,
            "engine_timeouts": [],
            "engine_errors": [],
            "last_error": "",
        }
        # Drawdown recovery tracking
        self._peak_balance: float = 0.0
        self._drawdown_active: bool = False
        self._drawdown_notified: bool = False
        # Drawdown recovery temporarily reduces max_open_positions; track original so we can restore it.
        self._drawdown_prev_max_open_positions: Optional[int] = None
        self._drawdown_reduced_max_open_positions: Optional[int] = None
        # Daily recap tracking
        self._last_recap_day: str = ""
        self._scan_cycle_count: int = 0
        self._last_preflight: dict = {}
        self._position_sync_worker_health: dict = {
            "expected_cadence_sec": SYNC_INTERVAL_SEC,
            "last_attempt": "",
            "last_success": "",
            "last_failure": "",
            "consecutive_failures": 0,
            "latest_duration_sec": 0.0,
            "task_alive": False,
        }

    # ── Read-only properties ─────────────────────────────────────────────────

    def running_count(self) -> int:
        return sum(1 for e in self.engines.values() if e.running)

    def total_positions(self) -> int:
        return registry_count()

    def all_open_positions(self) -> list[dict]:
        positions: dict[str, dict] = {}
        for name, eng in self.engines.items():
            for coin, trade in eng.open_positions().items():
                positions[coin] = {**trade, "coin": coin, "engine": eng.name}
        for coin, trade in registry_restorable_details().items():
            if coin in positions:
                continue
            direction = str(trade.get("direction", "long")).lower()
            positions[coin] = {
                "coin": coin,
                "engine": trade.get("engine", "RECOVERED"),
                "side": "buy" if direction == "long" else "sell",
                "entry": float(trade.get("entry", 0.0) or 0.0),
                "size": float(trade.get("size_usdt", 0.0) or 0.0),
                "leverage": int(trade.get("leverage", _s.get_val("leverage_default", 10)) or _s.get_val("leverage_default", 10)),
                "stop_loss": float(trade.get("stop_loss", 0.0) or 0.0),
                "take_profit": float(trade.get("take_profit", 0.0) or 0.0),
                "order_id": str(trade.get("order_id", "") or ""),
                "recovered": True,
            }
        return list(positions.values())

    def status_snapshot(self) -> dict:
        combined = get_combined_stats()
        ws_stats = _ws_stream.stats()
        return {
            "running":         self.running,
            "running_count":   self.running_count(),
            "total_positions": self.total_positions(),
            "ws_active":       _ws_stream.is_available(),
            "ws_market_data_ready": bool(ws_stats.get("market_data_ready", False)),
            "sync_health":     self.get_sync_health_snapshot(),
            "today_pnl":       combined.get("today_pnl", 0.0),
            "daily_pnl":       combined.get("today_pnl", 0.0),
            "today_wr":        combined.get("today_wr", 0.0),
        }

    def trade_flow_snapshot(self) -> dict:
        report = dict(self._last_scan_report or {})
        try:
            import rejection_log as _rjlog
            import decision_trace as _dtrace

            report["top_blockers"] = _rjlog.get_summary()
            report["decision_summary"] = _dtrace.get_summary()
        except Exception:
            report.setdefault("top_blockers", {})
            report.setdefault("decision_summary", {})
        try:
            report["open_positions"] = registry_count()
        except Exception:
            report["open_positions"] = 0
        report["running"] = bool(self.running)
        return report

    def _recovery_mode(self, details: dict) -> str:
        mode = str(details.get("recovery_mode", "") or "").strip().lower()
        if mode in (_RECOVERY_MANAGED, _RECOVERY_QUARANTINED, _RECOVERY_LEGACY):
            return mode
        return _RECOVERY_LEGACY

    def _source_engine(self, details: dict) -> str:
        source = str(details.get("source_engine", "") or details.get("engine", "") or "RECOVERED").upper()
        return source or "RECOVERED"

    def _sync_timestamp(self) -> str:
        return datetime.now(timezone.utc).isoformat()

    def _absence_confirmations(self, details: dict) -> int:
        return int(details.get("exchange_absence_confirmations", 0) or 0)

    def _mark_sync_failure(self, reason: str) -> None:
        registry_map = registry_nonterminal_details()
        if registry_map:
            logger.warning("SYNC FAILURE - skipping closure. reason=%s", reason)
        self._sync_failure_count += 1
        self._last_sync_error = str(reason or "")
        _sync_health.mark_failure("positions_sync", reason)
        for coin in registry_map:
            registry_mark_sync_state(coin, "UNCERTAIN", absence_confirmations=0, error_reason=reason)
        for coin, plan in _em.get_all_plans().items():
            plan.sync_status = "UNCERTAIN"
            plan.exchange_absence_confirmations = 0
            plan.last_sync_error = str(reason or "")

    def _mark_seen_on_exchange(self, coin: str, sync_ts: str) -> None:
        registry_mark_sync_state(
            coin,
            "OK",
            success_ts=sync_ts,
            seen_ts=sync_ts,
            absence_confirmations=0,
            error_reason="",
        )
        plan = _em.get_plan(coin)
        if plan is not None:
            plan.sync_status = "OK"
            plan.last_successful_sync = sync_ts
            plan.last_seen_on_exchange = sync_ts
            plan.exchange_absence_confirmations = 0
            plan.last_sync_error = ""

    def _mark_absent_on_exchange(self, coin: str, details: dict, sync_ts: str) -> int:
        confirmations = self._absence_confirmations(details) + 1
        registry_mark_sync_state(
            coin,
            "MISSING",
            success_ts=sync_ts,
            absence_confirmations=confirmations,
            error_reason="",
        )
        plan = _em.get_plan(coin)
        if plan is not None:
            plan.sync_status = "MISSING"
            plan.last_successful_sync = sync_ts
            plan.exchange_absence_confirmations = confirmations
            plan.last_sync_error = ""
        logger.warning(
            "UNCERTAIN STATE - awaiting confirmation %s absent_on_exchange confirmations=%d/2",
            coin,
            confirmations,
        )
        return confirmations

    def get_sync_health_snapshot(self) -> dict:
        snap = _sync_health.get_snapshot()
        worker = dict(self._position_sync_worker_health)
        position_sync_task = getattr(self, "_position_sync_task", None)
        worker["task_alive"] = bool(
            position_sync_task is not None
            and not position_sync_task.done()
            and self.running
        )
        snap = dict(snap)
        snap["position_sync_worker"] = worker
        if self.running and not _ws_stream.is_available() and str(snap.get("status", "OK")).upper() == "OK":
            snap["status"] = "DEGRADED"
            snap["status_emoji"] = "🟡"
            snap["reason"] = str(snap.get("reason", "") or "ws_unavailable")
        return snap

    def get_sync_health_label(self) -> str:
        return _sync_health.get_status_label(running=self.running, ws_active=_ws_stream.is_available())

    async def _position_sync_loop(self) -> None:
        """Run exchange reconciliation independently of scan completion."""
        self._position_sync_worker_health["task_alive"] = True
        logger.info("Position reconciliation worker started (interval=%ds).", SYNC_INTERVAL_SEC)
        try:
            while self.running:
                await asyncio.sleep(max(float(SYNC_INTERVAL_SEC), 0.1))
                if not self.running:
                    break
                started = time.monotonic()
                self._position_sync_worker_health["last_attempt"] = self._sync_timestamp()
                try:
                    summary = await self.sync_positions(repair=True)
                    failed = bool(self._last_sync_error) or "_uncertain_" in str(summary) or str(summary).startswith("❌")
                    duration = time.monotonic() - started
                    self._position_sync_worker_health["latest_duration_sec"] = round(duration, 6)
                    if failed:
                        self._position_sync_worker_health["last_failure"] = self._sync_timestamp()
                        self._position_sync_worker_health["consecutive_failures"] = int(
                            self._position_sync_worker_health.get("consecutive_failures", 0) or 0
                        ) + 1
                    else:
                        self._position_sync_worker_health["last_success"] = self._sync_timestamp()
                        self._position_sync_worker_health["consecutive_failures"] = 0
                    logger.info(
                        "Position reconciliation worker complete: failed=%s duration=%.3fs",
                        failed,
                        duration,
                    )
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    self._position_sync_worker_health["latest_duration_sec"] = round(time.monotonic() - started, 6)
                    self._position_sync_worker_health["last_failure"] = self._sync_timestamp()
                    self._position_sync_worker_health["consecutive_failures"] = int(
                        self._position_sync_worker_health.get("consecutive_failures", 0) or 0
                    ) + 1
                    logger.exception("Position reconciliation worker failed: %s", exc)
        except asyncio.CancelledError:
            raise
        finally:
            self._position_sync_worker_health["task_alive"] = False
            logger.info("Position reconciliation worker stopped.")

    def _infer_exchange_only_origin(self, coin: str) -> tuple[str, dict]:
        for name, eng in self.engines.items():
            try:
                open_positions = eng.open_positions()
            except Exception:
                continue
            if coin in open_positions:
                details = dict(open_positions.get(coin, {}) or {})
                return name, details
        plan = _em.get_plan(coin)
        if plan is not None:
            return plan.engine_name or "RECOVERED", {
                "direction": plan.direction,
                "entry": plan.entry_price,
                "size_usdt": plan.size_usdt,
                "leverage": plan.leverage,
                "stop_loss": plan.hard_sl,
                "take_profit": plan.tp1_price,
                "tp2_price": plan.tp2_price,
                "vote_score": plan.vote_score,
                "order_id": plan.order_id,
            }
        return "RECOVERED", {}

    def _restore_target(self, engine_name: str):
        eng = self.engines.get(engine_name)
        if not eng or not hasattr(eng, "restore_position"):
            return None
        return eng

    def _can_restore_managed_position(self, engine_name: str, details: dict) -> bool:
        eng = self._restore_target(engine_name)
        if eng is None:
            return False
        if self._recovery_mode(details) != _RECOVERY_MANAGED:
            return False
        try:
            return bool(eng.is_enabled())
        except Exception:
            return False

    async def _register_exit_plan(self, coin: str, engine_name: str, details: dict, live: dict | None = None) -> None:
        existing_plan = _em.get_plan(coin)
        if existing_plan is not None:
            if live is not None:
                live_leverage = int((live or {}).get("leverage", 0) or 0)
                existing_plan.position_effective_leverage = live_leverage or None
                existing_plan.leverage_truth_status = "POSITION_EFFECTIVE" if live_leverage else "LEVERAGE_UNKNOWN"
                existing_plan.leverage_truth_timestamp = str(
                    details.get("exchange_position_timestamp", "") or ""
                )
                existing_plan.leverage_truth_source = "okx.position" if live_leverage else ""
                configured = int(details.get("exchange_configured_leverage", 0) or 0)
                existing_plan.requested_vs_configured_mismatch = bool(
                    details.get("requested_vs_configured_mismatch", details.get("leverage_truth_mismatch", False))
                )
                existing_plan.configured_vs_effective_mismatch = bool(
                    configured and live_leverage and configured != live_leverage
                )
                existing_plan.leverage_truth_mismatch = existing_plan.requested_vs_configured_mismatch
                existing_plan.mfe_current_roi = _em._current_mfe_roi(existing_plan)
                existing_plan.mae_current_roi = _em._current_mae_roi(existing_plan)
            return
        if registry_exposure_status(details) != "ACTIVE_POSITION":
            logger.warning(
                "Skipping ExitPlan restore for %s: local exposure status=%s",
                coin,
                registry_exposure_status(details),
            )
            return
        live = live or {}
        restored_direction = str(details.get("direction", live.get("direction", "long")) or live.get("direction", "long")).lower()
        restored_entry = float(details.get("entry", live.get("entry", 0.0)) or live.get("entry", 0.0) or 0.0)
        restored_size = float(details.get("size_usdt", live.get("size_usdt", 0.0)) or live.get("size_usdt", 0.0) or 0.0)
        requested_leverage = int(details.get("requested_leverage", details.get("leverage", 0)) or details.get("leverage", 0) or 0)
        exchange_configured_leverage = int(details.get("exchange_configured_leverage", 0) or 0) or None
        live_leverage = int((live or {}).get("leverage", 0) or 0)
        position_effective_leverage = live_leverage or None
        leverage_truth_status = "POSITION_EFFECTIVE" if position_effective_leverage else "LEVERAGE_UNKNOWN"
        restored_leverage = int(position_effective_leverage or 1)
        # Use persisted hard_sl_live if available (updated by trailing stop)
        restored_sl = float(details.get("hard_sl_live", details.get("stop_loss", 0.0)) or details.get("stop_loss", 0.0) or 0.0)
        from tp_ownership import normalize_persisted_owner, normalize_tp_mode, resolve_legacy_owner
        tp_mode_at_entry = normalize_tp_mode(
            details.get("tp_mode_at_entry", details.get("tp_mode", "hybrid"))
        )
        raw_tp_owner = str(details.get("tp_owner", "") or "").strip().upper()
        if raw_tp_owner:
            tp_owner = normalize_persisted_owner(raw_tp_owner)
        else:
            tp_owner = resolve_legacy_owner(
                details,
                mode_at_entry=tp_mode_at_entry,
                partial_enabled=bool(details.get("partial_exit_enabled", True)),
            )
        plan = _em.register(
            coin=coin,
            direction=restored_direction,
            entry_price=restored_entry,
            size_usdt=restored_size,
            atr_val=float(details.get("atr", 0.0) or 0.0),
            hard_sl=restored_sl,
            tp1_price=float(details.get("take_profit", 0.0) or 0.0),
            tp2_price=float(details.get("tp2_price", details.get("take_profit", 0.0)) or 0.0),
            order_id=str(details.get("order_id", "") or ""),
            client_order_id=str(details.get("entry_client_order_id", details.get("entry_clOrdId", "")) or ""),
            position_lifecycle_id=str(details.get("position_lifecycle_id", "") or ""),
            engine_name=engine_name,
            vote_score=int(float(details.get("vote_score", 2) or 2)),
            leverage=restored_leverage,
            requested_leverage=requested_leverage or restored_leverage,
            exchange_configured_leverage=exchange_configured_leverage,
            position_effective_leverage=position_effective_leverage,
            leverage_truth_status=leverage_truth_status,
            leverage_truth_timestamp=str(details.get("leverage_truth_timestamp", "") or ""),
            leverage_truth_source=str(details.get("leverage_truth_source", "") or ""),
            leverage_truth_mismatch=bool(details.get("leverage_truth_mismatch", False)),
            requested_vs_configured_mismatch=bool(
                details.get("requested_vs_configured_mismatch", details.get("leverage_truth_mismatch", False))
            ),
            configured_vs_effective_mismatch=bool(details.get("configured_vs_effective_mismatch", False)),
            tp_owner=tp_owner,
            tp_mode_at_entry=tp_mode_at_entry,
            exchange_tp_algo_id=str(details.get("exchange_tp_algo_id", "") or ""),
            local_tp1_enabled=details.get("local_tp1_enabled"),
            local_tp2_enabled=details.get("local_tp2_enabled"),
            ownership_timestamp_value=str(details.get("ownership_timestamp", "") or ""),
            tp_owner_timestamp_value=str(details.get("tp_owner_timestamp", details.get("ownership_timestamp", "")) or ""),
            partials_enabled_at_entry=details.get("partials_enabled_at_entry", details.get("partial_exit_enabled")),
        )
        # FINAL PASS: dynamic exits do not require a fixed TP on entry/recovery.
        if restored_entry <= 0 or restored_sl <= 0:
            logger.warning(
                "KANNON GUARD %s incomplete recovery context entry=%.6f sl=%.6f source=%s",
                coin,
                restored_entry,
                restored_sl,
                engine_name,
            )
        # Restore trailing stop state from registry (survives restart)
        if plan:
            saved_tsl = float(details.get("tsl_price", 0) or 0)
            saved_peak = float(details.get("peak_price", 0) or 0)
            saved_armed = bool(details.get("tsl_armed", False))
            plan.initial_size_usdt = float(details.get("initial_size_usdt", restored_size) or restored_size)
            plan.remaining_pct = float(details.get("remaining_pct", 100.0) or 100.0)
            plan.realized_pnl_usdt = float(details.get("realized_pnl_usdt", 0.0) or 0.0)
            plan.tp1_done = bool(details.get("tp1_done", False))
            plan.tp2_done = bool(details.get("tp2_done", False))
            plan.tp1_deferred = bool(details.get("tp1_deferred", False))
            plan.tp2_deferred = bool(details.get("tp2_deferred", False))
            plan.tp1_deferred_price = float(details.get("tp1_deferred_price", 0.0) or 0.0)
            plan.tp2_deferred_price = float(details.get("tp2_deferred_price", 0.0) or 0.0)
            plan.deferred_partial_pnl_usdt = float(details.get("deferred_partial_pnl_usdt", 0.0) or 0.0)
            plan.lifecycle_phase = str(details.get("lifecycle_phase", "early_validation") or "early_validation")
            plan.mfe_roi = float(details.get("mfe_roi", 0.0) or 0.0)
            plan.mae_roi = float(details.get("mae_roi", 0.0) or 0.0)
            plan.mfe_price = float(details.get("mfe_price", restored_entry) or restored_entry)
            plan.mae_price = float(details.get("mae_price", restored_entry) or restored_entry)
            plan.mfe_gross_pnl_usdt = float(details.get("mfe_gross_pnl_usdt", 0.0) or 0.0)
            plan.mae_gross_pnl_usdt = float(details.get("mae_gross_pnl_usdt", 0.0) or 0.0)
            plan.mfe_raw_move_pct = float(details.get("mfe_raw_move_pct", 0.0) or 0.0)
            plan.mae_raw_move_pct = float(details.get("mae_raw_move_pct", 0.0) or 0.0)
            plan.mfe_observed_leverage = int(details.get("mfe_observed_leverage", 0) or 0)
            plan.mae_observed_leverage = int(details.get("mae_observed_leverage", 0) or 0)
            plan.mfe_current_roi = _em._current_mfe_roi(plan)
            plan.mae_current_roi = _em._current_mae_roi(plan)
            plan.exit_timeline = list(details.get("exit_timeline", []) or [])[-100:]
            plan.runner_quality = float(details.get("runner_quality", 0.0) or 0.0)
            plan.runner_core_pct = float(details.get("runner_core_pct", 0.0) or 0.0)
            plan.exhaustion_score = float(details.get("exhaustion_score", 0.0) or 0.0)
            plan.exhaustion_reasons = str(details.get("exhaustion_reasons", "") or "")
            plan.economic_minimum = dict(details.get("economic_minimum", {}) or {})
            plan.runner_opportunity = dict(details.get("runner_opportunity", {}) or {})
            plan.breakeven_moved = bool(details.get("breakeven_moved", False))
            plan.profit_lock_level = int(details.get("profit_lock_level", 0) or 0)
            plan.last_protection_reason = str(details.get("last_protection_reason", "") or "")
            plan.desired_stop_price = float(details.get("desired_stop_price", restored_sl) or restored_sl)
            plan.local_effective_stop_price = float(details.get("local_effective_stop_price", 0.0) or 0.0)
            plan.exchange_confirmed_stop_price = float(details.get("exchange_confirmed_stop_price", 0.0) or 0.0)
            plan.exchange_protection_status = str(details.get("exchange_protection_status", "") or "")
            plan.protection_intent = dict(details.get("protection_intent", {}) or {})
            plan.protection_lifecycle = str(details.get("protection_lifecycle", "") or "")
            plan.protection_cleanup_pending = bool(details.get("protection_cleanup_pending", False))
            plan.exchange_protection_algo_ids = list(details.get("exchange_protection_algo_ids", []) or [])
            persisted_owner = normalize_persisted_owner(details.get("tp_owner", ""))
            if persisted_owner:
                plan.tp_owner = persisted_owner
            plan.tp_mode_at_entry = str(details.get("tp_mode_at_entry", plan.tp_mode_at_entry) or plan.tp_mode_at_entry).lower()
            plan.exchange_tp_algo_id = str(details.get("exchange_tp_algo_id", "") or "")
            plan.local_tp1_enabled = bool(details.get("local_tp1_enabled", plan.local_tp1_enabled))
            plan.local_tp2_enabled = bool(details.get("local_tp2_enabled", plan.local_tp2_enabled))
            plan.ownership_timestamp = str(details.get("ownership_timestamp", plan.ownership_timestamp) or plan.ownership_timestamp)
            plan.tp_owner_timestamp = str(details.get("tp_owner_timestamp", plan.ownership_timestamp) or plan.ownership_timestamp)
            plan.partials_enabled_at_entry = bool(details.get("partials_enabled_at_entry", plan.partials_enabled_at_entry))
            plan.last_reprotection_attempt = str(details.get("last_reprotection_attempt", "") or "")
            plan.last_reprotection_success = str(details.get("last_reprotection_success", "") or "")
            plan.reprotection_reason = str(details.get("reprotection_reason", "") or "")
            plan.reprotection_error = str(details.get("reprotection_error", "") or "")
            plan.reprotection_attempts = int(details.get("reprotection_attempts", 0) or 0)
            plan.reprotection_evaluations = int(details.get("reprotection_evaluations", 0) or 0)
            plan.reprotection_read_recoveries = int(details.get("reprotection_read_recoveries", 0) or 0)
            plan.reprotection_transactions = int(details.get("reprotection_transactions", 0) or 0)
            plan.reprotection_post_attempts = int(details.get("reprotection_post_attempts", 0) or 0)
            plan.reprotection_confirmed = int(details.get("reprotection_confirmed", 0) or 0)
            plan.reprotection_rejected = int(details.get("reprotection_rejected", 0) or 0)
            plan.reprotection_uncertain = int(details.get("reprotection_uncertain", 0) or 0)
            if plan.exchange_confirmed_stop_price <= 0 and plan.exchange_protection_status == "STOP_CONFIRMED":
                plan.exchange_confirmed_stop_price = restored_sl
            if plan.local_effective_stop_price <= 0 and plan.exchange_confirmed_stop_price > 0:
                plan.local_effective_stop_price = plan.exchange_confirmed_stop_price
            if not plan.exchange_protection_status:
                plan.exchange_protection_status = (
                    "PAPER_LOCAL" if os.getenv("PAPER_TRADING", "true").strip().lower() == "true"
                    else "PROTECTION_STATE_UNKNOWN"
                )
            plan.last_seen_on_exchange = str(details.get("last_seen_on_exchange", "") or "")
            plan.last_successful_sync = str(details.get("last_successful_sync", "") or "")
            plan.sync_status = str(details.get("sync_status", "UNCERTAIN") or "UNCERTAIN").upper()
            plan.exchange_absence_confirmations = int(details.get("exchange_absence_confirmations", 0) or 0)
            plan.last_sync_error = str(details.get("last_sync_error", "") or "")
            plan.partial_exit_state = dict(details.get("partial_exit_state", {}) or {})
            plan.partial_exit_events = list(details.get("partial_exit_events", []) or [])[-100:]
            plan.entry_contracts = float(details.get("entry_contracts", 0.0) or 0.0)
            plan.confirmed_remaining_contracts = float(details.get("confirmed_remaining_contracts", 0.0) or 0.0)
            plan.exchange_position_timestamp = str(details.get("exchange_position_timestamp", "") or "")
            plan.exposure_truth_status = str(details.get("exposure_truth_status", "ACTIVE_POSITION") or "ACTIVE_POSITION")
            plan.closure_handoff_at = str(details.get("closure_handoff_at", "") or "")
            plan.partial_attribution_status = str(details.get("partial_attribution_status", "NONE") or "NONE")
            plan.partial_realized_pnl_usdt = float(details.get("partial_realized_pnl_usdt", 0.0) or 0.0)
            plan.partial_fees_usdt = float(details.get("partial_fees_usdt", 0.0) or 0.0)
            if saved_tsl > 0:
                plan.tsl_price = saved_tsl
            if saved_peak > 0:
                plan.peak_price = saved_peak
            if saved_armed:
                plan.tsl_armed = True
            if saved_tsl > 0 or saved_peak > 0:
                logger.info("Restored TSL state for %s: armed=%s tsl=%.6f peak=%.6f",
                           coin, plan.tsl_armed, plan.tsl_price, plan.peak_price)

    def _record_direct_close(
        self,
        engine_name: str,
        coin: str,
        direction: str,
        entry: float,
        exit_px: float,
        size: float,
        pnl_usdt: float,
        reason: str,
        vote_score: float = 2.0,
        opened_at: str = "",
        metadata: dict | None = None,
        reconciliation: dict | None = None,
    ) -> None:
        fee_usdt = fee_cost_usdt(float(size or 0.0), int(_s.get_val("leverage_default", 10) or 10))
        if size > 0 and entry > 0:
            _perf.record_trade(
                coin, direction, entry, exit_px, size, pnl_usdt, reason,
                fee_usdt=fee_usdt, opened_at=opened_at,
            )
        if engine_name in ENGINE_NAMES and size > 0 and entry > 0:
            category_record(
                engine=engine_name,
                coin=coin,
                direction=direction,
                entry=entry,
                exit_price=exit_px,
                size_usdt=size,
                pnl_usdt=pnl_usdt,
                vote_score=int(vote_score or 2),
                exit_reason=reason,
                fee_usdt=fee_usdt,
                opened_at=opened_at,
                metadata=metadata or {},
            )
        if os.getenv("PAPER_TRADING", "true").strip().lower() != "true":
            try:
                from pnl_reconciler import reconciler as _pnl_reconciler

                context = dict(reconciliation or {})
                entry_order_ids = [str(value) for value in (context.get("entry_order_ids", []) or []) if value]
                entry_order_id = str(context.get("entry_order_id", "") or (entry_order_ids[0] if entry_order_ids else ""))
                entry_client_order_id = str(
                    context.get("entry_client_order_id", context.get("entry_clOrdId", "")) or ""
                )
                exit_order_ids = [str(value) for value in (context.get("exit_order_ids", []) or []) if value]
                _pnl_reconciler.record_provisional(
                    str(context.get("trade_id", "") or entry_order_id or f"{engine_name}-{coin}-{opened_at}"),
                    coin=coin,
                    engine=engine_name,
                    direction=direction,
                    gross_price_pnl=float(pnl_usdt or 0.0),
                    entry_fee=-abs(float(fee_usdt or 0.0) / 2.0),
                    exit_fee=-abs(float(fee_usdt or 0.0) / 2.0),
                    local_estimated_net_pnl=float(pnl_usdt or 0.0) - float(fee_usdt or 0.0),
                    oxypulse_reported_final_pnl=float(pnl_usdt or 0.0) - float(fee_usdt or 0.0),
                    oxypulse_reported_gross_pnl=float(pnl_usdt or 0.0),
                    entry_notional_usdt=float(size or 0.0),
                    exit_reason=str(reason or ""),
                    entry_order_id=entry_order_id,
                    entry_order_ids=entry_order_ids or ([entry_order_id] if entry_order_id else []),
                    entry_client_order_id=entry_client_order_id,
                    entry_clOrdId=entry_client_order_id,
                    exit_order_ids=exit_order_ids,
                    exit_order_id=str(context.get("exit_order_id", "") or (exit_order_ids[-1] if exit_order_ids else "")),
                    position_id=str(context.get("position_id", context.get("pos_id", "")) or ""),
                    first_fill_at=str(context.get("first_fill_at", opened_at) or opened_at),
                    last_entry_fill_at=str(context.get("last_entry_fill_at", opened_at) or opened_at),
                    first_exit_fill_at=str(context.get("first_exit_fill_at", "") or ""),
                    last_exit_fill_at=str(context.get("last_exit_fill_at", "") or ""),
                    execution_environment="production",
                )
            except Exception as exc:
                logger.error("[%s] direct-close PnL reconciliation storage failed for %s: %s", engine_name, coin, exc)

    async def _apply_engine_guardrails(self) -> None:
        """Demotion guard: live → probation → shadow (not straight disable).
        Demotions are visible in Telegram and persisted in config.
        """
        if not _s.get_val("engine_guard_enabled", True):
            return
        roll_window = int(_s.get_val("engine_guard_roll_window", 12))
        min_trades = int(_s.get_val("engine_guard_min_trades", 6))
        wr_floor = float(_s.get_val("engine_guard_wr_floor", 35.0))
        loss_streak_limit = int(_s.get_val("engine_guard_loss_streak", 4))
        today_pnl_floor = float(_s.get_val("engine_guard_today_pnl_floor", -3.0))

        from notifier import send_alert

        for name, eng in self.engines.items():
            current_mode = _s.get_engine_mode(name)
            if current_mode == "shadow":
                try:
                    recent_perf = get_engine_recent_performance(name, 10, 30)
                except Exception:
                    recent_perf = {}
                rehab_ok, rehab_reason = _positive_net_shadow_rehab_candidate(name, current_mode, recent_perf)
                if rehab_ok:
                    _s.set_engine_mode(name, "probation", actor="engine_guard", reason=rehab_reason)
                    try:
                        if not getattr(eng, "running", False):
                            await eng.start()
                    except Exception:
                        logger.exception("Engine rehab restart failed: %s", name)
                    logger.warning("Engine rehab restored %s: shadow -> probation (%s)", name, rehab_reason)
                    try:
                        await send_alert(
                            f"🛡️ *Engine Rehab* — `{name}`\n"
                            f"  SHADOW → PROBATION\n"
                            f"  Reason: {rehab_reason}"
                        )
                    except Exception:
                        logger.debug("Engine rehab alert failed for %s", name)
                    continue
            if current_mode not in ("live", "probation"):
                continue  # only demote engines that are actually trading
            guard = get_engine_guard_status(name, roll_window)
            reasons: list[str] = []
            roll_bad = guard["roll_trades"] >= min_trades and guard["roll_win_rate"] < wr_floor
            pnl_bad = guard["trade_count"] >= min_trades and guard["today_pnl"] <= today_pnl_floor
            if roll_bad and pnl_bad:
                reasons.append(
                    f"rolling WR {guard['roll_win_rate']:.1f}% on {guard['roll_trades']} trades "
                    f"with today PnL ${guard['today_pnl']:+.2f}"
                )
            if guard["loss_streak"] >= loss_streak_limit:
                reasons.append(f"loss streak {guard['loss_streak']}")
            if not reasons:
                continue

            # Demotion: live → probation → shadow (not straight disable)
            if current_mode == "live":
                new_mode = "probation"
            else:  # probation → shadow
                try:
                    recent_perf = get_engine_recent_performance(name, 10, 30)
                except Exception:
                    recent_perf = {}
                protected, protection_reason = _positive_net_shadow_protected(name, current_mode, recent_perf, guard)
                if protected:
                    logger.warning("Engine guard protected %s from shadow: %s (%s)", name, protection_reason, "; ".join(reasons))
                    guard_note = "; ".join(reasons)
                    if not _should_send_engine_notice(
                        name,
                        mode=current_mode,
                        protection_state="protected",
                        reason=protection_reason,
                        guard_note=guard_note,
                        role=_s.get_engine_role(name),
                    ):
                        continue
                    try:
                        await send_alert(
                            f"🛡️ *Engine Protected* — `{name}`\n"
                            f"  Remains PROBATION\n"
                            f"  Reason: {protection_reason}\n"
                            f"  Guard note: {'; '.join(reasons)}"
                        )
                    except Exception:
                        logger.debug("Engine guard protection alert failed for %s", name)
                    continue
                new_mode = "shadow"

            _s.set_engine_mode(name, new_mode, actor="engine_guard",
                               reason="; ".join(reasons))
            # If demoted to shadow, stop the engine from placing orders
            if new_mode == "shadow":
                try:
                    if getattr(eng, "running", False):
                        await eng.stop()
                except Exception:
                    logger.exception("Engine guard stop failed: %s", name)

            logger.warning("Engine guard demoted %s: %s → %s (%s)",
                           name, current_mode, new_mode, "; ".join(reasons))
            try:
                mode_emoji = {"probation": "⚠️", "shadow": "👻"}.get(new_mode, "🛑")
                await send_alert(
                    f"{mode_emoji} *Engine Demoted* — `{name}`\n"
                    f"  {current_mode.upper()} → {new_mode.upper()}\n"
                    f"  Reason: {'; '.join(reasons)}\n"
                    f"  {'Shadow mode: signals recorded, no orders.' if new_mode == 'shadow' else 'Probation: max 1 position, tighter thresholds.'}"
                )
            except Exception:
                logger.debug("Engine guard alert failed for %s", name)

    # ── Growth Phase Auto-Promotion ────────────────────────────────────────
    _growth_phase_checked = 0  # last phase applied (1, 2, or 3)

    async def _check_growth_phase(self, balance: float) -> None:
        """Auto-adjust bot posture based on balance growth phases."""
        p2_bal = float(_s.get_val("growth_phase_2_balance", 200.0))
        p3_bal = float(_s.get_val("growth_phase_3_balance", 500.0))

        if balance >= p3_bal and self._growth_phase_checked < 3:
            self._growth_phase_checked = 3
            await self._apply_growth_phase_3(balance)
        elif balance >= p2_bal and self._growth_phase_checked < 2:
            self._growth_phase_checked = 2
            await self._apply_growth_phase_2(balance)
        elif self._growth_phase_checked == 0:
            self._growth_phase_checked = 1  # Phase 1 is the default state

    async def _apply_growth_phase_2(self, balance: float) -> None:
        """Phase 2 ($200+): widen deployment and slot budget without reviving retired engines."""
        from notifier import send_alert
        # Increase max open positions
        if int(_s.get_val("max_open_positions", 6)) < 8:
            _s.set_val("max_open_positions", 8, actor="growth_phase",
                       reason=f"Phase 2 bump at ${balance:.2f}")
        try:
            await send_alert(
                f"🚀 *Growth Phase 2 Activated*\n"
                f"  Balance: `${balance:.2f}`\n"
                f"  Max positions: 8\n"
                f"  Deployment posture widened"
            )
        except Exception:
            pass
        logger.info("Growth Phase 2 activated at $%.2f", balance)

    async def _apply_growth_phase_3(self, balance: float) -> None:
        """Phase 3 ($500+): Increase max_open to 20, bump sizing."""
        from notifier import send_alert
        # Increase max open positions
        if int(_s.get_val("max_open_positions", 6)) < 20:
            _s.set_val("max_open_positions", 20, actor="growth_phase",
                       reason=f"Phase 3 bump at ${balance:.2f}")
        # Increase same-direction cap
        if int(_s.get_val("max_same_direction_positions", 4)) < 10:
            _s.set_val("max_same_direction_positions", 10, actor="growth_phase",
                       reason=f"Phase 3 bump at ${balance:.2f}")
        try:
            await send_alert(
                f"🏆 *Growth Phase 3 Activated*\n"
                f"  Balance: `${balance:.2f}`\n"
                f"  Max positions: 20\n"
                f"  Same-direction cap: 10"
            )
        except Exception:
            pass
        logger.info("Growth Phase 3 activated at $%.2f", balance)

    # ── Drawdown Recovery Mode ───────────────────────────────────────────────

    async def _check_drawdown_recovery(self, balance: float) -> None:
        """Auto-tighten risk when balance drops significantly from peak."""
        if not _s.get_val("drawdown_recovery_enabled", True):
            if self._drawdown_active:
                self._drawdown_active = False
                self._drawdown_notified = False
                # Clear restore tracking so we don't overwrite operator settings later.
                self._drawdown_prev_max_open_positions = None
                self._drawdown_reduced_max_open_positions = None
            return
        # Track peak
        if balance > self._peak_balance:
            self._peak_balance = balance
        if self._peak_balance <= 0:
            return
        dd_pct = (self._peak_balance - balance) / self._peak_balance * 100
        trigger = float(_s.get_val("drawdown_recovery_trigger_pct", 10.0))
        exit_at = float(_s.get_val("drawdown_recovery_exit_pct", 5.0))

        if not self._drawdown_active and dd_pct >= trigger:
            self._drawdown_active = True
            # Halve position sizes, require 3/3 votes
            _s.set_val("size_multiplier_votes_2", 0.0, actor="drawdown_recovery",
                       reason=f"DD {dd_pct:.1f}% — block 2/3 vote trades")
            # Reduce max positions temporarily (store original so it can be restored on recovery end).
            try:
                _prev_max = int(_s.get_val("max_open_positions", 10) or 10)
            except Exception:
                _prev_max = 10
            _reduced_max = max(3, int(_prev_max) // 2)
            self._drawdown_prev_max_open_positions = _prev_max
            self._drawdown_reduced_max_open_positions = _reduced_max
            _s.set_val("max_open_positions",
                       max(3, int(_s.get_val("max_open_positions", 10)) // 2),
                       actor="drawdown_recovery",
                       reason=f"DD {dd_pct:.1f}% — halve max positions")
            if not self._drawdown_notified:
                self._drawdown_notified = True
                from notifier import send_alert
                try:
                    await send_alert(
                        f"🛡️ *Drawdown Recovery Activated*\n"
                        f"  Peak: `${self._peak_balance:.2f}`\n"
                        f"  Now:  `${balance:.2f}` ({dd_pct:.1f}% DD)\n"
                        f"  2/3-vote trades blocked\n"
                        f"  Max positions halved"
                    )
                except Exception:
                    pass
            logger.warning("Drawdown recovery ON: peak=$%.2f now=$%.2f (%.1f%%)", self._peak_balance, balance, dd_pct)
        elif self._drawdown_active and dd_pct <= exit_at:
            self._drawdown_active = False
            self._drawdown_notified = False
            # Restore settings
            _s.set_val("size_multiplier_votes_2",
                       float(_s.DEFAULTS.get("size_multiplier_votes_2", 0.4)),
                       actor="drawdown_recovery",
                       reason=f"DD recovered to {dd_pct:.1f}% — restore sizing")
            from notifier import send_alert
            try:
                await send_alert(
                    f"✅ *Drawdown Recovery Deactivated*\n"
                    f"  Peak: `${self._peak_balance:.2f}`\n"
                    f"  Now:  `${balance:.2f}` ({dd_pct:.1f}% from peak)\n"
                    f"  Normal sizing restored"
                )
            except Exception:
                pass
            # Restore max_open_positions only if it still matches the reduced value we set.
            # If the operator adjusted it manually during recovery, do not overwrite.
            try:
                _cur_max = int(_s.get_val("max_open_positions", 10) or 10)
            except Exception:
                _cur_max = 10
            if (
                self._drawdown_prev_max_open_positions is not None
                and self._drawdown_reduced_max_open_positions is not None
                and _cur_max == int(self._drawdown_reduced_max_open_positions)
            ):
                _s.set_val(
                    "max_open_positions",
                    int(self._drawdown_prev_max_open_positions),
                    actor="drawdown_recovery",
                    reason=f"DD recovered to {dd_pct:.1f}% - restore max positions",
                )
            self._drawdown_prev_max_open_positions = None
            self._drawdown_reduced_max_open_positions = None
            logger.info("Drawdown recovery OFF: recovered to %.1f%% from peak", dd_pct)

    @property
    def drawdown_active(self) -> bool:
        return self._drawdown_active

    # ── Daily Recap ──────────────────────────────────────────────────────────

    async def _send_daily_recap(self, balance: float) -> None:
        """Send daily recap at midnight UTC."""
        from datetime import datetime, timezone
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        if today == self._last_recap_day:
            return
        if not _s.get_val("daily_recap_enabled", True):
            self._last_recap_day = today
            return
        # Only send if we've been running (have a peak balance)
        if self._peak_balance <= 0:
            self._last_recap_day = today
            return
        self._last_recap_day = today
        from category_tracker import get_daily_report_snapshot
        snapshot = get_daily_report_snapshot(open_positions=registry_count())
        tw = snapshot.get("wins", 0)
        tl = snapshot.get("losses", 0)
        tp = snapshot.get("net_pnl_usdt", 0.0)
        twr = snapshot.get("win_rate", 0.0)
        total = tw + tl
        dd_emoji = "🛡️ DD Recovery" if self._drawdown_active else "✅ Normal"
        from notifier import send_alert
        try:
            await send_alert(
                f"📋 *Daily Recap — {today}*\n\n"
                f"  Trades:  `{total}` ({tw}W / {tl}L)\n"
                f"  Win Rate: `{twr}%`\n"
                f"  Net PnL: `${tp:+.4f}`\n"
                f"  Fees:    `${float(snapshot.get('fee_usdt', 0.0) or 0.0):+.6f}`\n"
                f"  Open:    `{int(snapshot.get('open_positions') or 0)}`\n"
                f"  EI/TP:   `{int(snapshot.get('early_invalidation_count') or 0)}` / `{int(snapshot.get('tp_hit_count') or 0)}`\n"
                f"  Balance: `${balance:.2f}`\n"
                f"  Peak:    `${self._peak_balance:.2f}`\n"
                f"  Mode:    {dd_emoji}"
            )
        except Exception:
            pass
        logger.info("Daily recap sent for %s: %dW/%dL $%.4f", today, tw, tl, tp)

    # ── Start / Stop ─────────────────────────────────────────────────────────

    async def start_all(self) -> str:
        if self.running:
            return "⚡ Bot already running."
        shared_okx = OKXClient()
        preflight = await shared_okx.auth_preflight()
        self._last_preflight = dict(preflight or {})
        if not bool(preflight.get("ok", False)):
            reason = str(preflight.get("reason", "private auth preflight failed") or "private auth preflight failed")
            logger.error("OKX private auth preflight failed before engine start: %s", reason)
            if not bool(getattr(_okx_mod, "PAPER_TRADING", False)):
                from live_state import state as _startup_live_state
                try:
                    _startup_live_state.record_preflight(False, reason)
                except Exception:
                    logger.exception("Could not persist failed startup preflight state")
                return f"❌ Refusing to start live trading: OKX private auth preflight failed: {reason}"
            logger.warning("Continuing paper-mode engine start despite OKX private auth preflight failure: %s", reason)
        else:
            logger.info("OKX private auth preflight passed before engine start.")
        if not bool(getattr(_okx_mod, "PAPER_TRADING", False)) and str(mm.td_mode()).lower() != "isolated":
            logger.critical("Refusing live engine start: margin mode must be isolated, got %s", mm.td_mode())
            return f"❌ Refusing to start live trading: margin mode must be isolated, got `{mm.td_mode()}`."

        from instrument_registry import registry as _instrument_registry
        try:
            if "PYTEST_CURRENT_TEST" not in os.environ:
                await _instrument_registry.refresh(shared_okx, force=False)
        except Exception as exc:
            logger.error("Instrument metadata startup refresh failed: %s", exc)
            if not bool(getattr(_okx_mod, "PAPER_TRADING", False)):
                return f"Refusing to start live trading: instrument metadata unavailable: {exc}"
            logger.warning("Paper scanning may start, but submissions remain blocked until metadata refresh succeeds.")

        if not bool(getattr(_okx_mod, "PAPER_TRADING", False)):
            from live_preflight import run_preflight as _run_live_preflight
            from live_state import state as _live_state
            from controlled_lifecycle import gate as _controlled_lifecycle

            # Resolve durable uncertain submissions before the final preflight
            # evaluates no_unresolved_orders.  This is read-only exchange
            # reconciliation and never re-enters the submission path.
            try:
                from execution_router import router_for

                recovered_intents = await router_for(shared_okx).recover_unresolved()
                if recovered_intents:
                    logger.warning(
                        "Bootstrap uncertain recovery resolved %d intent(s): %s",
                        len(recovered_intents),
                        ", ".join(
                            f"{item.intent_id}:{item.state}"
                            for item in recovered_intents
                        ),
                    )
            except Exception:
                # The final preflight remains the safety authority.  If
                # recovery fails, unresolved intents stay durable and the
                # no_unresolved_orders check must fail closed.
                logger.exception("Bootstrap uncertain submission recovery failed")

            full_preflight = await _run_live_preflight(
                shared_okx,
                refresh_metadata=False,
                persist_state=False,
            )
            self._last_preflight = dict(full_preflight or {})
            stage1_ready = bool(
                not full_preflight.get("ok")
                and (full_preflight.get("readiness") or {}).get("ready_for_funding")
                and (full_preflight.get("readiness") or {}).get("structural_ready")
            )
            if not bool(full_preflight.get("ok")) and not stage1_ready:
                try:
                    _live_state.record_preflight(False, str(full_preflight.get("reason", "preflight_failed")))
                except Exception:
                    logger.exception("Could not persist failed production preflight state")
                    return "Refusing to start live trading: production preflight state could not be persisted."
                return "Refusing to start live trading: full production preflight failed."
            if stage1_ready:
                try:
                    _live_state.record_preflight(True, "ready_for_funding_unarmed")
                except Exception:
                    logger.exception("Could not persist structural readiness state")
                    return "Refusing to start live trading: structural readiness state could not be persisted."
                logger.warning(
                    "PRODUCTION STRUCTURAL READINESS PASSED | account funding required; runtime remains disarmed"
                )

            mutation_ready = not stage1_ready
            readiness = getattr(shared_okx, "mutation_transport_ready", None)
            if not stage1_ready and callable(readiness):
                mutation_ready, readiness_reason = await readiness()
                self._last_preflight["mutation_readiness"] = {
                    "ok": bool(mutation_ready),
                    "reason": str(readiness_reason or ""),
                }
                if not mutation_ready:
                    try:
                        _live_state.record_preflight(False, str(readiness_reason or "mutation_transport_not_ready"))
                    except Exception:
                        logger.exception("Could not persist mutation-readiness failure state")
                        return "Refusing to start live trading: readiness state could not be persisted."
                    logger.error(
                        "Mutation transport readiness failed after production preflight: %s",
                        readiness_reason,
                    )

            operator_live_start = os.environ.pop("OXYPULSE_OPERATOR_LIVE_ARM_START", "false").strip().lower() in {
                "1", "true", "yes", "on",
            }
            if mutation_ready and _controlled_lifecycle.can_auto_arm():
                armed_now, arm_result = _live_state.arm(full_preflight)
                if not armed_now:
                    return f"Controlled lifecycle authorization could not arm: {arm_result}."
                logger.warning("CONTROLLED LIFECYCLE ARMED | maximum_new_positions=1")
            elif mutation_ready and operator_live_start:
                _controlled_lifecycle.retire("normal_production_operator_authorized")
                armed_now, arm_result = _live_state.arm(full_preflight)
                if not armed_now:
                    return f"Explicit production startup authorization could not arm: {arm_result}."
                logger.warning(
                    "NORMAL PRODUCTION OPERATOR ARM ACCEPTED | one-time startup authorization consumed"
                )
            armed, arm_reason = _live_state.can_submit_live()
            if not mutation_ready:
                self.research_only = True
                os.environ["OXYPULSE_PRODUCTION_RESEARCH_ONLY"] = "true"
                logger.warning(
                    "PRODUCTION RESEARCH RUNTIME LATCHED | mutation transport readiness unavailable; live submission disabled"
                )
            elif not armed:
                live_snapshot = _live_state.snapshot()
                if str(live_snapshot.get("state", "")) == "LIVE_READY_UNARMED" and not bool(live_snapshot.get("armed")):
                    try:
                        persisted = _live_state.record_preflight(True, "startup_preflight_passed_unarmed")
                        persisted = persisted or _live_state.snapshot()
                    except Exception:
                        logger.exception("Could not persist successful unarmed readiness state")
                        return "Refusing to start live trading: successful readiness state could not be persisted."
                    if str(persisted.get("state", "")) != "LIVE_READY_UNARMED" or bool(persisted.get("armed")):
                        logger.critical("Successful unarmed startup did not persist LIVE_READY_UNARMED")
                        return "Refusing to start live trading: persisted readiness state is unsafe."
                    self.research_only = True
                    os.environ["OXYPULSE_PRODUCTION_RESEARCH_ONLY"] = "true"
                    logger.warning(
                        "PRODUCTION RESEARCH RUNTIME LATCHED | unarmed scanning enabled; reservation, payload construction, and submission disabled"
                    )
                else:
                    return f"Live infrastructure is ready but unarmed: {arm_reason}. Explicit operator arming is required."
            else:
                self.research_only = False
                os.environ.pop("OXYPULSE_PRODUCTION_RESEARCH_ONLY", None)

        # Resolve the shared BTC regime before the first scan can attribute
        # engine inactivity. A failed/insufficient fetch remains UNKNOWN and
        # is persisted as warm-up evidence; it never fabricates a regime.
        await _regime.update_regime("BTC")

        self.running = True
        if "PYTEST_CURRENT_TEST" not in os.environ:
            await _instrument_registry.start_periodic_refresh(shared_okx)
        if "PYTEST_CURRENT_TEST" not in os.environ:
            from capital_scheduler import scheduler as _capital_scheduler
            _capital_scheduler.configure(shared_okx, self.engines)
            await _capital_scheduler.start()
            from pnl_reconciler import reconciler as _pnl_reconciler
            await _pnl_reconciler.start_retry_worker(shared_okx)
        try:
            await _em.start_monitor(shared_okx, self.engines)
        except Exception as e:
            logger.exception("Exit monitor start failed")
            self.running = False
            return f"âŒ Failed to start exit monitor: {e}"

        for name, eng in self.engines.items():
            try:
                await eng.start()
            except Exception:
                logger.exception("Engine start failed: %s", name)

        # Start the long-lived transport and its owned bootstrap/recovery
        # coordinator. Subscription/history/continuity readiness is reported
        # separately; scan consumers never initiate candle REST work.
        try:
            universe = await get_futures_universe()
            transport_started = await _ws_stream.start(universe)
            self._last_scan_report["ws_transport_started"] = bool(transport_started)
            self._last_scan_report["ws_market_data_ready"] = _ws_stream.market_data_ready_for(
                set(universe), tuple(_ws_stream._timeframes),
            )
        except Exception as e:
            logger.warning("WS stream start failed: %s", e)

        # Launch the main scan loop after WS ownership is established. The
        # coordinator maintains history independently of scan reads.
        if self._scan_task is None or self._scan_task.done():
            self._scan_task = asyncio.create_task(self._scan_loop())

        sync_summary = await self.sync_positions(repair=True, startup=True)
        position_sync_task = getattr(self, "_position_sync_task", None)
        if position_sync_task is None or position_sync_task.done():
            self._position_sync_task = asyncio.create_task(
                self._position_sync_loop(),
                name="oxypulse-position-reconciliation",
            )
        count = self.running_count()
        runtime_label = "PRODUCTION_RESEARCH_ONLY" if self.research_only else "EXECUTION_ELIGIBLE"
        return (
            f"✅ *{count}/{len(ENGINE_NAMES)} engines started.*\n"
            f"Runtime: `{runtime_label}`\n"
            f"{sync_summary}"
        )

    async def stop_all(self) -> str:
        self.running = False
        if self.research_only:
            os.environ.pop("OXYPULSE_PRODUCTION_RESEARCH_ONLY", None)
        self.research_only = False

        for name, eng in self.engines.items():
            try:
                await eng.stop()
            except Exception:
                logger.exception("Engine stop failed: %s", name)

        if self._scan_task and not self._scan_task.done():
            self._scan_task.cancel()
            try:
                await self._scan_task
            except asyncio.CancelledError:
                pass
        self._scan_task = None

        position_sync_task = getattr(self, "_position_sync_task", None)
        if position_sync_task and not position_sync_task.done():
            position_sync_task.cancel()
            try:
                await position_sync_task
            except asyncio.CancelledError:
                pass
        self._position_sync_task = None

        if self._funding_refresh_task and not self._funding_refresh_task.done():
            self._funding_refresh_task.cancel()
            try:
                await self._funding_refresh_task
            except asyncio.CancelledError:
                pass
        self._funding_refresh_task = None

        try:
            await _ws_stream.stop()
        except Exception as e:
            logger.warning("WS stream stop: %s", e)

        try:
            from instrument_registry import registry as _instrument_registry
            await _instrument_registry.stop_periodic_refresh()
        except Exception as e:
            logger.warning("Instrument metadata refresh stop: %s", e)

        try:
            from capital_scheduler import scheduler as _capital_scheduler
            await _capital_scheduler.stop()
        except Exception as e:
            logger.warning("Capital scheduler stop: %s", e)

        try:
            from pnl_reconciler import reconciler as _pnl_reconciler
            await _pnl_reconciler.stop_retry_worker()
        except Exception as e:
            logger.warning("PnL reconciliation worker stop: %s", e)

        return "🛑 *All engines stopped.*"

    async def start_engine(self, name: str) -> str:
        name = name.upper()
        eng  = self.engines.get(name)
        if not eng:
            return f"❌ Unknown engine: {name}"
        return await eng.start()

    async def stop_engine(self, name: str) -> str:
        name = name.upper()
        eng  = self.engines.get(name)
        if not eng:
            return f"❌ Unknown engine: {name}"
        return await eng.stop()

    async def close_engine_positions(self, name: str) -> str:
        name = name.upper()
        eng  = self.engines.get(name)
        if not eng:
            return f"⚠️ No positions to close for {name}."
        results = [await self.close_coin(coin) for coin in list(eng.open_positions())]
        closed = sum(1 for result in results if "CLOSE_FILLED" in str(result))
        return (
            f"{eng.emoji} *{name}* closed {closed} position(s). "
            f"Operator results: `{'; '.join(results) or 'none'}`"
        )

    async def close_coin(self, coin: str) -> str:
        coin = coin.upper()
        local_engine = None
        local_trade = None
        for name, eng in self.engines.items():
            trade = eng.open_positions().get(coin)
            if trade is not None:
                local_engine, local_trade = eng, dict(trade)
                break
        registry = registry_all_details().get(coin)
        requested_side = ""
        if local_trade:
            requested_side = "long" if str(local_trade.get("side", "buy")).lower() == "buy" else "short"
        elif registry:
            requested_side = str(registry.get("direction", "") or "").lower()
        from execution_router import router_for
        try:
            close_client = OKXClient()
            # Keep lightweight legacy control/test adapters compatible. The
            # production OKXClient always exposes the canonical pipeline.
            if not callable(getattr(close_client, "close_futures_position", None)) and registry and local_engine is None:
                if is_unresolved_exposure(registry):
                    reader = getattr(close_client, "get_positions_status", None)
                    try:
                        try:
                            resolution = await reader(force_refresh=True, priority="P0")
                        except TypeError:
                            try:
                                resolution = await reader(force_refresh=True)
                            except TypeError:
                                resolution = await reader()
                    except Exception as exc:
                        return f"CLOSE_FAILED_BEFORE_SUBMISSION {coin}: exchange truth unavailable ({exc}); No close submitted."
                    live = next(
                        (
                            dict(row) for row in list(resolution.get("positions", []) or [])
                            if str(row.get("instId", "")).upper() in {coin, f"{coin}-USDT-SWAP"}
                            and abs(float(row.get("pos", row.get("availPos", 0.0)) or 0.0)) > 0.0
                        ), None,
                    ) if bool(resolution.get("ok", False)) else None
                    if live is None:
                        registry_update(
                            coin,
                            size_usdt=0.0,
                            metadata={
                                "exposure_truth_status": "EXCHANGE_CLOSED_AWAITING_RECONCILIATION",
                                "confirmed_remaining_contracts": 0.0,
                                "closure_handoff_at": datetime.now(timezone.utc).isoformat(),
                                "sync_status": "MISSING",
                            },
                        )
                        return f"CLOSE_ALREADY_ZERO {coin} — No close submitted; exchange position is zero."
                    now_iso = datetime.now(timezone.utc).isoformat()
                    effective_leverage = int(live.get("leverage", live.get("lever", 0)) or 0)
                    registry_update(
                        coin,
                        metadata={
                            "exposure_truth_status": "ACTIVE_POSITION",
                            "confirmed_remaining_contracts": abs(float(live.get("pos", live.get("availPos", 0.0)) or 0.0)),
                            "position_effective_leverage": effective_leverage or None,
                            "leverage_truth_status": "POSITION_EFFECTIVE" if effective_leverage else "LEVERAGE_UNKNOWN",
                            "leverage_truth_timestamp": now_iso,
                            "leverage_truth_source": "okx.position",
                            "resolved_from_local_unknown": True,
                        },
                    )
                    registry = registry_all_details().get(coin, registry)
                engine_name = self._source_engine(registry)
                fallback_engine = self._restore_target(engine_name)
                if fallback_engine and hasattr(fallback_engine, "restore_position") and hasattr(fallback_engine, "close_position"):
                    fallback_engine.restore_position(coin, registry)
                    legacy = await fallback_engine.close_position(coin, "manual")
                    if legacy.get("ok"):
                        return f"CLOSE_FILLED {coin} | Closed {coin}"
            result = await router_for(close_client).operator_close(
                coin, requested_pos_side=requested_side, source="operator/telegram",
            )
        except Exception as exc:
            logger.exception("Operator close pipeline failed for %s", coin)
            return f"CLOSE_FAILED_BEFORE_SUBMISSION {coin}: {exc}"

        # Operator lifecycle resolution may adopt an exchange-only position
        # into the registry. Refresh the row before terminal cleanup so that
        # an adopted lifecycle cannot remain active after a confirmed close.
        registry = registry_all_details().get(coin, registry)
        status = str(result.get("status", "CLOSE_FAILED_BEFORE_SUBMISSION"))
        if status == "CLOSE_ALREADY_ZERO":
            if local_engine is not None:
                local_engine._open.pop(coin, None)
            if registry and is_unresolved_exposure(registry):
                registry_update(
                    coin, size_usdt=0.0,
                    metadata={
                        "exposure_truth_status": "EXCHANGE_CLOSED_AWAITING_RECONCILIATION",
                        "confirmed_remaining_contracts": 0.0,
                        "closure_handoff_at": datetime.now(timezone.utc).isoformat(),
                    },
                )
            elif registry and is_actionable_position(registry):
                registry_release(coin, authoritative_position_zero=True)
            _em.remove(coin)
            return f"CLOSE_ALREADY_ZERO {coin} — No close submitted; exchange position is zero."
        if status == "CLOSE_FILLED":
            response = dict(result.get("response", {}) or {})
            exit_px = float(response.get("fillPx", 0.0) or (local_trade or {}).get("entry", 0.0) or 0.0)
            if local_engine is not None and local_trade is not None:
                entry = float(local_trade.get("entry", 0.0) or 0.0)
                size = float(local_trade.get("size", 0.0) or 0.0)
                pnl_pct = ((exit_px - entry) / entry * 100) if requested_side == "long" and entry > 0 else ((entry - exit_px) / entry * 100) if entry > 0 else 0.0
                local_engine._record_close(coin, local_trade, exit_px, round(size * (pnl_pct / 100), 4), "manual")
            elif registry:
                entry = float(registry.get("entry", 0.0) or 0.0)
                size = float(registry.get("size_usdt", 0.0) or 0.0)
                pnl_pct = ((exit_px - entry) / entry * 100) if requested_side == "long" and entry > 0 else ((entry - exit_px) / entry * 100) if entry > 0 else 0.0
                self._record_direct_close(
                    engine_name=self._source_engine(registry), coin=coin,
                    direction=requested_side or "unknown", entry=entry, exit_px=exit_px,
                    size=size, pnl_usdt=round(size * (pnl_pct / 100), 4),
                    reason="manual_operator", vote_score=float(registry.get("vote_score", 2) or 2),
                    opened_at=str(registry.get("opened_at", "") or ""),
                    reconciliation=_pnl_reconciliation_context(registry),
                )
                registry_release(coin, authoritative_position_zero=True)
            return f"CLOSE_FILLED {coin} | Closed {coin} intent={result.get('intent_id', '')}"
        if status == "CLOSE_SUBMITTED":
            return f"CLOSE_SUBMITTED {coin} intent={result.get('intent_id', '')} remaining={result.get('remaining_contracts', 0)}"
        return f"{status} {coin}: {result.get('reason', 'operator close not completed')}"

    async def close_all_positions(self) -> str:
        coins = set()
        for eng in self.engines.values():
            coins.update(eng.open_positions().keys())
        coins.update(
            coin for coin, row in registry_all_details().items()
            if is_actionable_position(row)
        )
        try:
            exchange_client = OKXClient()
            try:
                exchange_truth = await exchange_client.get_positions_status(force_refresh=True, priority="P0")
            except TypeError:
                exchange_truth = await exchange_client.get_positions_status(force_refresh=True)
            if not bool(exchange_truth.get("ok", False)):
                return f"CLOSE_FAILED_BEFORE_SUBMISSION: exchange position truth unavailable ({exchange_truth.get('reason', 'unknown')}); no closes submitted."
            for row in list(exchange_truth.get("positions", []) or []):
                inst_id = str(row.get("instId", "") or "").upper()
                try:
                    quantity = abs(float(row.get("pos", row.get("availPos", row.get("posSize", 0.0))) or 0.0))
                except Exception:
                    quantity = 0.0
                if quantity > 0 and inst_id.endswith("-USDT-SWAP"):
                    coins.add(inst_id.removesuffix("-USDT-SWAP"))
        except Exception as exc:
            return f"CLOSE_FAILED_BEFORE_SUBMISSION: exchange position truth unavailable ({exc}); no closes submitted."
        results = [await self.close_coin(coin) for coin in sorted(coins)]
        total = sum(1 for result in results if "CLOSE_FILLED" in str(result) or "CLOSE_ALREADY_ZERO" in str(result))
        unresolved = sorted(registry_unknown_details())
        suffix = (
            f" Unresolved local exposure skipped: `{','.join(unresolved)}`."
            if unresolved else ""
        )
        return f"🛑 Operator close results: `{'; '.join(results) or 'no local positions'}`. Completed: `{total}`.{suffix}"

    async def _estimate_exchange_notional(self, okx: OKXClient, coin: str, position: dict) -> float:
        notional = abs(float(position.get("notionalUsd", 0) or position.get("notionalUSD", 0) or 0))
        if notional > 0:
            return notional
        avg_px = float(position.get("avgPx", 0) or position.get("markPx", 0) or 0)
        pos_sz = abs(float(position.get("pos", 0) or 0))
        if avg_px <= 0 or pos_sz <= 0:
            return 0.0
        try:
            ct_val = await okx.fetch_contract_value(coin)
        except Exception:
            return 0.0
        if not math.isfinite(float(ct_val)) or float(ct_val) <= 0:
            return 0.0
        return pos_sz * ct_val * avg_px

    async def sync_positions(self, repair: bool = True, startup: bool = False) -> str:
        """Reconcile local open positions against the OKX exchange."""
        async def fresh_positions(client):
            reader = client.get_positions_status
            try:
                return await reader(force_refresh=True, priority="P0")
            except TypeError:
                return await reader()

        try:
            phase = "Startup" if startup else "Sync"
            if os.getenv("PAPER_TRADING", "false").lower() == "true" or not os.getenv("OKX_API_KEY", ""):
                self._sync_failure_count = 0
                self._last_sync_error = ""
                _sync_health.mark_success("positions_sync")
                registry_map = registry_restorable_details()
                rehydrated = quarantined = 0
                for coin, details in registry_map.items():
                    engine_name = self._source_engine(details)
                    can_restore = self._can_restore_managed_position(engine_name, details)
                    eng = self._restore_target(engine_name)
                    disabled_engine = bool(eng) and not bool(eng.is_enabled())
                    metadata = {
                        "source_engine": engine_name,
                        "managed_by_engine": can_restore,
                        "policy_tag": str(details.get("policy_tag", _POLICY_TAG) or _POLICY_TAG),
                    }
                    if can_restore:
                        metadata["recovery_mode"] = _RECOVERY_MANAGED
                        eng = self._restore_target(engine_name)
                        if eng and coin not in eng.open_positions() and eng.restore_position(coin, details):
                            rehydrated += 1
                    else:
                        metadata["recovery_mode"] = _RECOVERY_QUARANTINED
                        metadata["quarantine_reason"] = "disabled_engine" if disabled_engine else "legacy_recovery"
                        quarantined += 1
                    registry_update(coin, metadata=metadata)
                self._last_sync_ts = time.monotonic()
                return (
                    f"🔄 *{phase}*\n\n"
                    f"  Mode: `paper/local`\n"
                    f"  Registry: `{len(registry_map)}`\n"
                    f"  Rehydrated: `{rehydrated}`\n"
                    f"  Quarantined: `{quarantined}`\n"
                    f"  Exchange reconciliation: `_skipped_`"
                )

            okx = OKXClient()
            pos_status = await fresh_positions(okx)
            if not bool(pos_status.get("ok", False)):
                reason = str(pos_status.get("reason", "positions_unavailable") or "positions_unavailable")
                self._mark_sync_failure(reason)
                return (
                    f"⚠️ *{phase}*\n\n"
                    f"  Exchange sync: `_uncertain_`\n"
                    f"  Reason: `{reason}`\n"
                    f"  Registry retained: `{len(registry_all_details())}`\n"
                    "  Closures skipped: `yes`"
                )

            # Reconcile any prior mutation uncertainty using read-only order,
            # fill, history, and position evidence before normal position
            # reconciliation. This path never reposts an order.
            try:
                from execution_router import router_for
                recovered_intents = await router_for(okx).recover_unresolved()
                if recovered_intents:
                    logger.warning(
                        "Uncertain submission recovery reconciled %d intent(s): %s",
                        len(recovered_intents),
                        ", ".join(
                            f"{item.intent_id}:{item.state}"
                            for item in recovered_intents
                        ),
                    )
                    pos_status = await fresh_positions(okx)
                    if not bool(pos_status.get("ok", False)):
                        reason = str(pos_status.get("reason", "positions_unavailable") or "positions_unavailable")
                        self._mark_sync_failure(reason)
                        return (
                            f"⚠️ *{phase}*\n\n"
                            f"  Exchange sync: `_uncertain_`\n"
                            f"  Reason: `{reason}`\n"
                            f"  Registry retained: `{len(registry_all_details())}`\n"
                            "  Closures skipped: `yes`"
                        )
            except Exception:
                logger.exception("Uncertain submission recovery failed")

            self._sync_failure_count = 0
            self._last_sync_error = ""
            _sync_health.mark_success("positions_sync")
            sync_ts = self._sync_timestamp()
            raw = list(pos_status.get("positions", []) or [])
            exchange_map: dict[str, dict] = {}
            for pos in raw:
                inst = pos.get("instId", "")
                if "-USDT-SWAP" not in inst:
                    continue
                if float(pos.get("pos", 0) or 0) == 0:
                    continue
                coin = inst.replace("-USDT-SWAP", "")
                direction = str(pos.get("posSide", "") or "").lower()
                if direction not in ("long", "short"):
                    direction = "long" if float(pos.get("pos", 0) or 0) > 0 else "short"
                exchange_map[coin] = {
                    "direction": direction,
                    "entry": float(pos.get("avgPx", 0) or pos.get("markPx", 0) or 0),
                    "leverage": int(float(pos.get("lever", 0) or _s.get_val("leverage_default", 10))),
                    "size_usdt": await self._estimate_exchange_notional(okx, coin, pos),
                    "contracts": abs(float(pos.get("pos", pos.get("availPos", 0.0)) or 0.0)),
                    "position": dict(pos),
                }

            registry_map = registry_nonterminal_details()
            exchange_coins = set(exchange_map.keys())
            registry_coins = set(registry_map.keys())
            rehydrated = released = recovered = mismatched = quarantined = 0

            for coin in sorted(exchange_coins & registry_coins):
                live = exchange_map[coin]
                details = registry_map[coin]
                lifecycle = resolve_position_lifecycle(details, live.get("position", {}))
                lifecycle_status = str(lifecycle.get("status", "UNRESOLVED") or "UNRESOLVED")
                if lifecycle_status == "UNRESOLVED":
                    registry_update(
                        coin,
                        metadata={
                            "source_engine": engine_name,
                            "exposure_truth_status": "EXPOSURE_UNKNOWN",
                            "lifecycle_resolution_status": lifecycle_status,
                            "lifecycle_resolution_reason": str(lifecycle.get("reason", "") or ""),
                            "confirmed_remaining_contracts": float(live.get("contracts", 0.0) or 0.0),
                            "exchange_position_timestamp": sync_ts,
                        },
                    )
                    quarantined += 1
                    logger.warning(
                        "Lifecycle resolution unresolved for %s; active restore/protection skipped",
                        coin,
                    )
                    continue
                engine_name = self._source_engine(details)
                can_restore = self._can_restore_managed_position(engine_name, details)
                eng = self._restore_target(engine_name)
                disabled_engine = bool(eng) and not bool(eng.is_enabled())
                metadata = {
                    "source_engine": engine_name,
                    "managed_by_engine": can_restore,
                    "policy_tag": str(details.get("policy_tag", _POLICY_TAG) or _POLICY_TAG),
                    "recovery_mode": _RECOVERY_MANAGED if can_restore else _RECOVERY_QUARANTINED,
                    # A fresh successful exchange read is authoritative for
                    # current exposure and resolves a local unknown row.
                    "exposure_truth_status": "ACTIVE_POSITION",
                    "confirmed_remaining_contracts": float(live.get("contracts", 0.0) or 0.0),
                    "exchange_position_timestamp": sync_ts,
                    "position_lifecycle_id": str(lifecycle.get("position_lifecycle_id", "") or ""),
                    "exchange_lifecycle_anchor": str(lifecycle.get("exchange_lifecycle_anchor", "") or ""),
                    "lifecycle_resolution_status": lifecycle_status,
                    "previous_position_lifecycle_id": str(lifecycle.get("previous_position_lifecycle_id", "") or ""),
                }
                if not can_restore:
                    metadata["quarantine_reason"] = "disabled_engine" if disabled_engine else "legacy_recovery"
                if details.get("direction") != live["direction"]:
                    mismatched += 1
                    logger.warning(
                        "Sync direction mismatch for %s: registry=%s exchange=%s",
                        coin, details.get("direction"), live["direction"],
                    )
                registry_update(
                    coin,
                    size_usdt=float(details.get("size_usdt", 0) or live["size_usdt"]),
                    entry=float(details.get("entry", 0) or live["entry"]),
                    leverage=int(live.get("leverage", 0) or details.get("leverage", 0) or _s.get_val("leverage_default", 10)),
                    metadata=metadata,
                )
                self._mark_seen_on_exchange(coin, sync_ts)
                restored = registry_all_details()[coin]
                if can_restore and eng and coin not in eng.open_positions():
                    if eng.restore_position(coin, restored):
                        rehydrated += 1
                else:
                    quarantined += 1
                await self._register_exit_plan(coin, engine_name, restored, live)

            for coin in sorted(registry_coins - exchange_coins):
                details = registry_map[coin]
                if registry_exposure_status(details) == "EXCHANGE_CLOSED_AWAITING_RECONCILIATION":
                    continue
                confirmations = self._mark_absent_on_exchange(coin, details, sync_ts)
                if confirmations < 2:
                    continue
                logger.info(
                    "CONFIRMED CLOSE - safe to notify %s confirmed_absence_syncs=%d",
                    coin,
                    confirmations,
                )
                engine_name = self._source_engine(details)
                if is_unresolved_exposure(details):
                    registry_update(
                        coin,
                        size_usdt=0.0,
                        metadata={
                            "exposure_truth_status": "EXCHANGE_CLOSED_AWAITING_RECONCILIATION",
                            "confirmed_remaining_contracts": 0.0,
                            "closure_handoff_at": sync_ts,
                            "last_successful_sync": sync_ts,
                            "sync_status": "MISSING",
                        },
                    )
                    _em.remove(coin)
                    released += 1
                    logger.warning(
                        "Unresolved local exposure %s confirmed absent; preserved handoff without local close/PnL.",
                        coin,
                    )
                    continue
                eng = self._restore_target(engine_name)
                can_restore = repair and self._can_restore_managed_position(engine_name, details)
                entry = float(details.get("entry", 0.0) or 0.0)
                size = float(details.get("size_usdt", 0.0) or 0.0)
                direction = str(details.get("direction", "long")).lower()
                exit_px = await okx.get_last_fill_price(coin)
                if not exit_px or exit_px <= 0:
                    exit_px = latest_price(coin) or entry
                pnl_pct = 0.0
                if entry > 0 and size > 0:
                    pnl_pct = ((exit_px - entry) / entry * 100) if direction == "long" else ((entry - exit_px) / entry * 100)
                pnl_usdt = round(size * (pnl_pct / 100), 4) if size > 0 else 0.0
                if repair and can_restore and eng:
                    if coin not in eng.open_positions():
                        eng.restore_position(coin, details)
                    handled = eng.notify_closed_by_exchange(coin, exit_px, pnl_usdt, "sync_confirmed_close")
                    if not handled:
                        self._record_direct_close(
                            engine_name=engine_name,
                            coin=coin,
                            direction=direction,
                            entry=entry,
                            exit_px=exit_px,
                            size=size,
                            pnl_usdt=pnl_usdt,
                            reason="sync_confirmed_close",
                            vote_score=float(details.get("vote_score", 2) or 2),
                            opened_at=str(details.get("opened_at", "") or ""),
                            metadata={
                                k: details.get(k)
                                for k in ("position_strength", "trade_class", "preferred_exit_style", "contributor_roles")
                                if k in details
                            },
                            reconciliation=_pnl_reconciliation_context(details),
                        )
                        registry_release(coin, authoritative_position_zero=True)
                else:
                    self._record_direct_close(
                        engine_name=engine_name,
                        coin=coin,
                        direction=direction,
                        entry=entry,
                        exit_px=exit_px,
                        size=size,
                        pnl_usdt=pnl_usdt,
                        reason="sync_confirmed_close",
                        vote_score=float(details.get("vote_score", 2) or 2),
                        opened_at=str(details.get("opened_at", "") or ""),
                        metadata={
                            k: details.get(k)
                            for k in ("position_strength", "trade_class", "preferred_exit_style", "contributor_roles")
                            if k in details
                        },
                        reconciliation=_pnl_reconciliation_context(details),
                    )
                    registry_release(coin, authoritative_position_zero=True)
                _em.remove(coin)
                released += 1

            for coin in sorted(exchange_coins - registry_coins):
                live = exchange_map[coin]
                engine_name, hints = self._infer_exchange_only_origin(coin)
                exchange_anchor = derive_exchange_lifecycle_anchor(live.get("position", {}))
                adopted_lifecycle = derive_exchange_adopted_lifecycle_id(exchange_anchor)
                if not exchange_anchor or not adopted_lifecycle:
                    registry_register(
                        coin=coin,
                        direction=str(live.get("direction", "long") or "long"),
                        engine=engine_name,
                        size_usdt=float(live.get("size_usdt", 0.0) or 0.0),
                        entry=float(live.get("entry", 0.0) or 0.0),
                        order_id="",
                        metadata={
                            "source_engine": engine_name,
                            "exposure_truth_status": "EXPOSURE_UNKNOWN",
                            "lifecycle_resolution_status": "UNRESOLVED",
                            "lifecycle_resolution_reason": "exchange_lifecycle_anchor_unavailable",
                            "confirmed_remaining_contracts": float(live.get("contracts", 0.0) or 0.0),
                            "recovery_mode": _RECOVERY_QUARANTINED,
                            "sync_status": "OK",
                            "last_seen_on_exchange": sync_ts,
                            "last_successful_sync": sync_ts,
                        },
                    )
                    quarantined += 1
                    continue
                merged_direction = str(hints.get("direction", live["direction"]) or live["direction"]).lower()
                merged_entry = float(hints.get("entry", live["entry"]) or live["entry"] or 0.0)
                merged_size = max(float(hints.get("size_usdt", live["size_usdt"]) or live["size_usdt"] or 0.0), 1.0)
                merged_leverage = int(hints.get("leverage", live["leverage"]) or live["leverage"])
                eng = self._restore_target(engine_name)
                can_restore = repair and self._can_restore_managed_position(engine_name, {
                    "source_engine": engine_name,
                    "recovery_mode": _RECOVERY_MANAGED,
                })
                if repair:
                    registry_register(
                        coin=coin,
                        direction=merged_direction,
                        engine=engine_name,
                        size_usdt=merged_size,
                        entry=merged_entry,
                        leverage=merged_leverage,
                        stop_loss=float(hints.get("stop_loss", 0.0) or 0.0),
                        take_profit=float(hints.get("take_profit", 0.0) or 0.0),
                        order_id=str(hints.get("order_id", "") or "RECOVERED"),
                        metadata={
                            "position_lifecycle_id": adopted_lifecycle,
                            "exchange_lifecycle_anchor": exchange_anchor,
                            "lifecycle_resolution_status": "NEW_LIFECYCLE",
                            "entry_lifecycle": "EXCHANGE_ADOPTED",
                            "source_engine": engine_name,
                            "managed_by_engine": bool(can_restore),
                            "recovery_mode": _RECOVERY_MANAGED if can_restore else _RECOVERY_QUARANTINED,
                            "quarantine_reason": "" if can_restore else "exchange_only",
                            "policy_tag": _POLICY_TAG,
                            "sync_status": "OK",
                            "last_seen_on_exchange": sync_ts,
                            "last_successful_sync": sync_ts,
                            "exchange_absence_confirmations": 0,
                            "last_sync_error": "",
                            "vote_score": int(float(hints.get("vote_score", 2) or 2)),
                            "tp2_price": float(hints.get("tp2_price", hints.get("take_profit", 0.0)) or 0.0),
                        },
                    )
                    restored = registry_all_details().get(coin, {})
                    if can_restore and eng and coin not in eng.open_positions():
                        if eng.restore_position(coin, restored):
                            rehydrated += 1
                    else:
                        quarantined += 1
                    await self._register_exit_plan(coin, engine_name, restored, live)
                recovered += 1
                logger.warning(
                    "RECOVERY - restored position from exchange %s direction=%s entry=%.6f size=$%.2f lev=%dx source=%s mode=%s",
                    coin,
                    merged_direction,
                    merged_entry,
                    merged_size,
                    merged_leverage,
                    engine_name,
                    _RECOVERY_MANAGED if can_restore else _RECOVERY_QUARANTINED,
                )

            self._last_sync_ts = time.monotonic()
            return (
                f"🔄 *{phase}*\n\n"
                f"  Exchange: `{len(exchange_coins)}`\n"
                f"  Registry: `{len(registry_all_details())}`\n"
                f"  Rehydrated: `{rehydrated}`\n"
                f"  Quarantined: `{quarantined}`\n"
                f"  Recovered: `{recovered}`\n"
                f"  Released: `{released}`\n"
                f"  Mismatched: `{mismatched}`"
            )
        except Exception as e:
            logger.exception("sync_positions failed")
            self._mark_sync_failure(str(e))
            return f"❌ Sync failed: {e}"

    async def send_daily_summary(self) -> None:
        """Send daily P&L summary via Telegram notifier."""
        try:
            from notifier import alert_daily_summary
            from okx_client import OKXClient
            from category_tracker import get_daily_report_snapshot
            okx     = OKXClient()
            balance = await okx.get_usdt_balance()
            snapshot = get_daily_report_snapshot(open_positions=registry_count())
            total   = int(snapshot["total_trades"])
            pnl     = float(snapshot["net_pnl_usdt"])
            pct     = (pnl / balance * 100) if balance > 0 else 0.0
            await alert_daily_summary(
                total   = total,
                wins    = int(snapshot["wins"]),
                losses  = int(snapshot["losses"]),
                pnl     = pnl,
                balance = balance,
                pct     = pct,
                fee_usdt = float(snapshot["fee_usdt"]),
                gross_pnl = float(snapshot["gross_pnl_usdt"]),
                open_positions = int(snapshot.get("open_positions") or 0),
                exit_counts = snapshot.get("exit_counts") or {},
                engine_stats = snapshot.get("engine_stats") or {},
                report_date = str(snapshot.get("date") or ""),
                timezone_label = str(snapshot.get("timezone") or "UTC"),
            )
            logger.info("Daily summary sent.")
        except Exception as e:
            logger.error("send_daily_summary: %s", e)

    # ── Main scan loop ────────────────────────────────────────────────────────

    async def _scan_loop(self) -> None:
        logger.info("Scan loop started (interval=%ds).", SCAN_INTERVAL)
        while self.running:
            try:
                await self._scan_cycle()
            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("Scan cycle error")
            await asyncio.sleep(SCAN_INTERVAL)
        logger.info("Scan loop stopped.")

    async def _refresh_funding_background(self, universe: list[str], okx: OKXClient) -> None:
        started = time.monotonic()
        try:
            result = await refresh_funding_batch(
                universe,
                okx,
                max_symbols=24,
                concurrency=2,
                request_timeout_sec=6.0,
            )
            logger.info(
                "Background funding refresh complete: requested=%d ok=%d timeout=%d errors=%d duration=%.2fs",
                int(result.get("requested", 0) or 0),
                int(result.get("ok", 0) or 0),
                int(result.get("timeout", 0) or 0),
                int(result.get("error", 0) or 0),
                time.monotonic() - started,
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Background funding refresh failed")

    def _persist_scan_latency_report(self, report: dict) -> None:
        """Observational latency persistence for steady-state audits. Non-blocking best-effort."""
        path = Path("data/scan_latency_history.jsonl")
        if "PYTEST_CURRENT_TEST" in os.environ and path == Path("data/scan_latency_history.jsonl"):
            return
        path.parent.mkdir(parents=True, exist_ok=True)
        engine_durations = dict(report.get("engine_durations_sec", {}) or {})
        durations = [float(v) for v in engine_durations.values() if float(v) > 0]
        durations_sorted = sorted(durations)
        p95 = 0.0
        median = 0.0
        if durations_sorted:
            median = durations_sorted[len(durations_sorted) // 2]
            p95 = durations_sorted[min(len(durations_sorted) - 1, max(int(len(durations_sorted) * 0.95) - 1, 0))]
        phase_durations = dict(report.get("phase_durations_sec", {}) or {})
        additive_phases = {
            key: value for key, value in phase_durations.items() if key != "market_refresh"
        }
        aggregate_phases = {}
        if "market_refresh" in phase_durations:
            aggregate_phases["market_refresh"] = phase_durations["market_refresh"]
        row = {
            "schema_version": 2,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "cycle_number": report.get("cycle_number"),
            "scan_status": report.get("scan_status", "completed"),
            "duration_sec": report.get("duration_sec"),
            "universe_count": report.get("universe_count"),
            "active_engine_count": report.get("active_engine_count"),
            "additive_phase_durations_sec": additive_phases,
            "aggregate_phase_durations_sec": aggregate_phases,
            "phase_timing_metadata": {
                "additive_fields": sorted(additive_phases),
                "aggregate_fields": sorted(aggregate_phases),
                "overlaps": {"market_refresh": ["htf_refresh", "market_refresh_1m"]},
            },
            "engine_timeouts": list(report.get("engine_timeouts", []) or []),
            "engine_errors": list(report.get("engine_errors", []) or []),
            "engine_durations_sec": engine_durations,
            "engine_duration_stats": {
                "count": len(durations_sorted),
                "median_sec": round(median, 3),
                "p95_sec": round(p95, 3),
                "max_sec": round(max(durations_sorted), 3) if durations_sorted else 0.0,
            },
            "latency_metrics": dict(report.get("latency_metrics", {}) or {}),
        }
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(row, sort_keys=True, separators=(",", ":"), default=str) + "\n")
        latest = Path("data/scan_latency_latest.json")
        latest.write_text(json.dumps(row, indent=2, ensure_ascii=True, default=str), encoding="utf-8")

    async def _run_engine_scan_with_timeout(self, name: str, eng: CategoryEngine, universe: list, balance: float) -> dict:
        timeout_sec = max(float(_s.get_val("engine_scan_timeout_sec", 180.0) or 180.0), 30.0)
        started = time.monotonic()
        try:
            await asyncio.wait_for(eng.evaluate_all(universe, balance), timeout=timeout_sec)
            return {"engine": name, "ok": True, "duration_sec": round(time.monotonic() - started, 3)}
        except asyncio.TimeoutError:
            logger.error("Engine scan timeout: %s exceeded %.1fs; continuing next scan cycle.", name, timeout_sec)
            protected_coins: set[str] = set()
            try:
                from capital_reservations import book as reservation_book
                from execution_supervisor import supervisor as execution_supervisor
                from order_state_store import store as order_store

                for reservation in reservation_book.snapshot():
                    metadata = dict(reservation.get("metadata", {}) or {})
                    if str(metadata.get("engine", "") or "").upper() == str(name or "").upper():
                        coin = str(metadata.get("coin", "") or "").upper()
                        if coin:
                            protected_coins.add(coin)
                for intent in order_store.unresolved():
                    if str(intent.get("engine", "") or "").upper() == str(name or "").upper():
                        coin = str(intent.get("coin", "") or "").upper()
                        if coin:
                            protected_coins.add(coin)
                protected_coins.update(execution_supervisor.active_coins(name))
                released = await registry_release_uncommitted_pending(
                    name,
                    protected_coins=protected_coins,
                )
                if released:
                    logger.warning("Engine timeout cleanup released uncommitted registry state: %s", ", ".join(released))
                if protected_coins:
                    logger.critical(
                        "Engine timeout retained submission-related state for reconciliation: %s",
                        ", ".join(sorted(protected_coins)),
                    )
            except Exception:
                logger.exception("Engine timeout cleanup failed: %s", name)
            duration = round(time.monotonic() - started, 3)
            _latency.record_engine_timeout(name, duration)
            return {"engine": name, "ok": False, "timeout": True, "duration_sec": duration}
        except Exception as exc:
            logger.exception("Engine task failed: %s", name)
            duration = round(time.monotonic() - started, 3)
            _latency.record_engine_summary(name, 0, duration, error=str(exc))
            return {"engine": name, "ok": False, "error": str(exc), "duration_sec": duration}

    async def _scan_cycle(self) -> None:
        collector = _latency.ScanLatencyTelemetry(self._scan_cycle_count + 1)
        token = _latency.activate(collector)
        status = "failed"
        try:
            await self._scan_cycle_impl()
            report = self._last_scan_report or {}
            if report.get("last_error") or report.get("engine_errors"):
                status = "failed"
            elif report.get("engine_timeouts"):
                status = "completed_with_engine_timeouts"
            else:
                status = "completed"
        except asyncio.CancelledError:
            self._last_scan_report["last_error"] = self._last_scan_report.get("last_error") or "cancelled"
            raise
        except Exception as exc:
            self._last_scan_report["last_error"] = self._last_scan_report.get("last_error") or f"scan: {exc}"
            logger.exception("Scan cycle failed")
            raise
        finally:
            try:
                report = self._last_scan_report or {}
                report["scan_status"] = status
                report["finished_at"] = datetime.now(timezone.utc).isoformat()
                report["duration_sec"] = round(time.monotonic() - collector.started_monotonic, 3)
                try:
                    report["latency_metrics"] = collector.snapshot(
                        status=status,
                        duration_sec=report["duration_sec"],
                        phase_durations_sec=report.get("phase_durations_sec", {}),
                    )
                except Exception:
                    logger.debug("Scan latency snapshot failed", exc_info=True)
                    report["latency_metrics"] = {
                        "schema_version": _latency.SCHEMA_VERSION,
                        "scan_status": status,
                        "snapshot_failed": True,
                    }
                try:
                    self._persist_scan_latency_report(report)
                except Exception:
                    logger.debug("Scan latency persistence failed", exc_info=True)
            except Exception:
                logger.debug("Scan latency finalization failed", exc_info=True)
            finally:
                try:
                    _latency.reset(token)
                except Exception:
                    logger.debug("Scan latency context reset failed", exc_info=True)

    async def _scan_cycle_impl(self) -> None:
        scan_started = time.monotonic()
        self._scan_cycle_count += 1
        scan_started_iso = datetime.now(timezone.utc).isoformat()
        self._last_scan_report = {
            "started_at": scan_started_iso,
            "finished_at": "",
            "duration_sec": 0.0,
            "universe_count": 0,
            "active_engine_count": 0,
            "engine_timeouts": [],
            "engine_errors": [],
            "engine_durations_sec": {},
            "last_error": "",
            "cycle_number": self._scan_cycle_count,
            "phase_durations_sec": {},
        }
        def _record_phase(name: str, started: float) -> None:
            duration = round(time.monotonic() - started, 3)
            self._last_scan_report["phase_durations_sec"][name] = duration
            _latency.record_phase(name, duration)

        universe_started = time.monotonic()
        try:
            universe = await get_futures_universe()
        except Exception as exc:
            _record_phase("universe", universe_started)
            logger.warning("Universe refresh failed: %s", exc)
            self._last_scan_report["last_error"] = f"universe: {exc}"
            return
        if not universe:
            _record_phase("universe", universe_started)
            self._last_scan_report["last_error"] = "empty_universe"
            return
        self._last_scan_report["universe_count"] = len(universe)
        _record_phase("universe", universe_started)

        clear_strategy_cache()

        okx = OKXClient()
        now = time.monotonic()
        if (
            now - self._market_refresh_ts["funding"] >= REFRESH_FUNDING_SEC
            and (self._funding_refresh_task is None or self._funding_refresh_task.done())
        ):
            self._funding_refresh_task = asyncio.create_task(
                self._refresh_funding_background(universe, okx),
                name="oxypulse-funding-refresh",
                context=_latency.detached_context(),
            )
            self._market_refresh_ts["funding"] = now
        # Candle acquisition is owned by the WS market-data coordinator. A
        # scan consumes its published cache and never starts bootstrap,
        # repair, retry, or fallback network work.
        for phase in ("market_refresh_1m", "htf_refresh", "market_refresh"):
            self._last_scan_report["phase_durations_sec"][phase] = 0.0
            _latency.record_phase(phase, 0.0)

        regime_started = time.monotonic()
        await _regime.update_regime("BTC")
        _record_phase("regime", regime_started)
        scan_id = f"production-scan-{int(time.time() * 1000)}"
        readiness_started = time.monotonic()
        readiness = await asyncio.to_thread(market_readiness_snapshot, universe)
        self._last_scan_report["ws_market_data_ready"] = _ws_stream.market_data_ready_for(
            set(universe), tuple(_ws_stream._timeframes),
        )
        _record_phase("readiness", readiness_started)
        research_begin_started = time.monotonic()
        await asyncio.to_thread(
            _research_funnel.begin_scan,
            scan_id,
            universe,
            readiness,
            asdict(_regime.get_state()),
        )
        _record_phase("research_begin", research_begin_started)

        # Re-subscribe WS if universe changed
        ws_started = time.monotonic()
        try:
            await _ws_stream.update_coins(universe)
        except Exception:
            pass
        _record_phase("ws_update", ws_started)
        self._last_scan_report["ws_market_data"] = _ws_stream.stats()

        # Position reconciliation is owned by the independent worker. Keep a
        # zero scan phase so scan latency cannot be mistaken for sync latency.
        _latency.record_phase("position_sync", 0.0)

        if now - self._last_engine_guard_ts >= ENGINE_GUARD_INTERVAL_SEC:
            guard_started = time.monotonic()
            await self._apply_engine_guardrails()
            _record_phase("engine_guardrails", guard_started)
            self._last_engine_guard_ts = now
        else:
            _latency.record_phase("engine_guardrails", 0.0)

        balance_started = time.monotonic()
        try:
            balance = await okx.get_usdt_balance()
            self._last_balance_usdt = balance
        except Exception as exc:
            _record_phase("balance_refresh", balance_started)
            logger.warning("Balance refresh failed: %s", exc)
            return
        _record_phase("balance_refresh", balance_started)
        if balance <= 0:
            self._last_scan_report["last_error"] = "non_positive_balance"
            _research_funnel.complete_scan(blocker="non_positive_balance")
            return

        # ── Growth phase auto-promotion ──────────────────────────────────────
        # Checks balance against phase thresholds and auto-enables engines/settings
        misc_started = time.monotonic()
        if _s.get_val("growth_phase_auto", True):
            await self._check_growth_phase(balance)

        # ── Drawdown recovery mode ───────────────────────────────────────────
        await self._check_drawdown_recovery(balance)

        # ── Daily recap (midnight UTC) ───────────────────────────────────────
        await self._send_daily_recap(balance)
        _record_phase("pre_engine_misc", misc_started)

        # Run every active, enabled CategoryEngine concurrently
        tasks = []
        active_names = []
        for name, eng in self.engines.items():
            if eng.running and eng.is_enabled():
                active_names.append(name)
                tasks.append(self._run_engine_scan_with_timeout(name, eng, universe, balance))

        self._last_scan_report["active_engine_count"] = len(active_names)
        engine_durations: dict[str, float] = {}
        if tasks:
            engine_scan_started = time.monotonic()
            task_results = await asyncio.gather(*tasks, return_exceptions=True)
            timeouts: list[str] = []
            errors: list[str] = []
            for name, result in zip(active_names, task_results):
                if isinstance(result, Exception):
                    logger.error("Engine task wrapper failed: %s -> %s", name, result)
                    errors.append(f"{name}: {result}")
                elif isinstance(result, dict):
                    duration = float(result.get("duration_sec", 0.0) or 0.0)
                    engine_durations[str(result.get("engine") or name)] = duration
                    if result.get("timeout"):
                        timeouts.append(str(result.get("engine") or name))
                    if result.get("error"):
                        errors.append(f"{result.get('engine') or name}: {result.get('error')}")
            self._last_scan_report["engine_timeouts"] = timeouts
            self._last_scan_report["engine_errors"] = errors
            self._last_scan_report["engine_durations_sec"] = engine_durations
            _record_phase("engine_evaluation", engine_scan_started)
        else:
            _latency.record_phase("engine_evaluation", 0.0)
        self._last_scan_report["finished_at"] = datetime.now(timezone.utc).isoformat()
        self._last_scan_report["duration_sec"] = round(time.monotonic() - scan_started, 3)
        logger.info(
            "Scan cycle complete: universe=%d engines=%d duration=%.2fs timeouts=%d errors=%d phases=%s",
            int(self._last_scan_report.get("universe_count", 0) or 0),
            int(self._last_scan_report.get("active_engine_count", 0) or 0),
            float(self._last_scan_report.get("duration_sec", 0.0) or 0.0),
            len(self._last_scan_report.get("engine_timeouts", []) or []),
            len(self._last_scan_report.get("engine_errors", []) or []),
            self._last_scan_report.get("phase_durations_sec", {}),
        )
        self._last_scan_report["scan_status"] = (
            "completed_with_engine_timeouts" if self._last_scan_report.get("engine_timeouts") else "completed"
        )
        research_completion_started = time.monotonic()
        await asyncio.to_thread(
            _research_funnel.complete_scan,
            duration_sec=self._last_scan_report["duration_sec"],
            engine_timeouts=self._last_scan_report.get("engine_timeouts", []),
            engine_errors=self._last_scan_report.get("engine_errors", []),
            websocket=_ws_stream.stats(),
        )
        _record_phase("research_completion", research_completion_started)


# ── Singleton exposed to bot.py ───────────────────────────────────────────────
engine = MultiEngine()

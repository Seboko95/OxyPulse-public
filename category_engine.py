"""
OxyPulse — category_engine.py  v5
7 engines including SHOGUN (CRT+SMC+ICT).
Shared OKXClient singleton, indicator cache coin= param, 4H highs/lows for SHOGUN.
"""
from __future__ import annotations
import asyncio, inspect, json, logging, os, time
import httpx
from rest_rate_governor import process_rest_rate_governor, run_with_rate_admission
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
from okx_client import OKXClient, OKXOrderError, risk
from risk_manager import fee_cost_usdt
from position_registry import (
    can_open_and_reserve, release as reg_close,
    release_pre_submit_reservation as reg_release_pre_submit,
    _registry,
    engine_count as _reg_engine_count, count as _reg_count,
    update as _reg_update,
    confirm as reg_confirm,
    direction_count as _reg_direction_count,
    direction_notional as _reg_direction_notional,
    get_all_details as _reg_details,
    is_mutation_actionable_position as _reg_mutation_actionable,
)
from category_tracker import (
    record_trade as cat_record,
    ENGINE_EMOJI,
    engine_log_name,
    get_engine_recent_performance,
)
from market_data_stream import (
    get_closes, get_highs, get_lows, get_volumes,
    get_4h_closes, get_4h_highs, get_4h_lows,
    get_15m_closes, get_15m_highs, get_15m_lows, get_15m_volumes,
    get_1h_closes, get_1h_highs, get_1h_lows,
    get_1d_closes, get_1d_highs, get_1d_lows,
    candle_count, latest_price, execution_price_snapshot,
    timeframe_snapshot,
    get_cached_funding_rate, funding_is_fresh,
)
from strategy_categories import evaluate
from indicators import (
    atr as calc_atr, adx as calc_adx, ema as calc_ema, rsi as calc_rsi,
    macd as calc_macd, bollinger_bands as calc_bollinger_bands,
    bollinger_band_width as calc_bollinger_band_width, vwap as calc_vwap,
    hurst_exponent as calc_hurst,
)
import settings_store as _s
import exit_manager as _em
import regime_detector as _regime
import sl_tp_engine as _sl_tp
from coin_universe import (
    get_volume, MIN_VOLUME_USD, get_top20_mcap, get_universe_preference, rank_tradeable_candidates,
)
import performance_tracker as _perf
import engine_attribution as _engine_attribution
import execution_admission as _admission
import trade_intent as _trade_intent
import signal_consensus as _consensus
import profit_targeting as _profit_targets
import weekly_cap as _wcap
import decision_trace as _dtrace
import lifecycle_analytics as _life
import profitability_control_plane as _profitability
import micro_sizing as _micro_sizing
import novo_intelligence as _novo_intelligence
import novo_soft_enrichment as _novo_soft_enrichment
import novo_trade_memory as _novo_trade_memory
import paper_validation as _paper_validation
from notifier import alert_trade_executed, alert_error, send_alert
from notifier import alert_trade_closed
import rejection_log as _rjlog
import production_research_funnel as _research_funnel
from execution_supervisor import supervisor as _execution_supervisor
import latency_telemetry as _latency

logger = logging.getLogger("OxyPulse.CategoryEngine")


def _signal_age_inputs(coin: str) -> dict:
    """Read completed market snapshot timestamps for observational telemetry."""
    snapshots = {
        timeframe: timeframe_snapshot(coin, timeframe, 1)
        for timeframe in ("1m", "15m", "1h", "4h")
    }
    ws_generation = max(
        int(snapshots[timeframe].get("connection_generation", 0) or 0)
        for timeframe in snapshots
    )
    sources = [str(snapshots[timeframe].get("source", "") or "") for timeframe in snapshots]
    gap_states = [str(snapshots[timeframe].get("refresh_status", "") or "") for timeframe in snapshots]
    htf_snapshots = [snapshots[timeframe] for timeframe in ("15m", "1h", "4h")]
    htf_transport = [float(row.get("transport_age_sec") or 0.0) for row in htf_snapshots if row.get("transport_age_sec") is not None]
    htf_bar = [float(row.get("bar_age_sec") or 0.0) for row in htf_snapshots if row.get("bar_age_sec") is not None]
    htf_received = [float(row.get("observed_at") or 0.0) for row in htf_snapshots if float(row.get("observed_at") or 0.0) > 0.0]
    htf_bar_open = [float(row.get("bar_open_ts") or 0.0) for row in htf_snapshots if float(row.get("bar_open_ts") or 0.0) > 0.0]
    ticker = execution_price_snapshot(coin)
    return {
        "ticker_observed_at": ticker.get("last_update_received_at") or ticker.get("latest_timestamp"),
        "ticker_transport_age": ticker.get("transport_age_sec"),
        # Compatibility observed_at fields now mean local transport evidence;
        # bar opening time is retained separately below.
        "one_minute_observed_at": snapshots["1m"].get("observed_at") or snapshots["1m"].get("received_at"),
        "one_minute_bar_open_ts": snapshots["1m"].get("bar_open_ts"),
        "one_minute_bar_age": snapshots["1m"].get("bar_age_sec"),
        "one_minute_transport_age": snapshots["1m"].get("transport_age_sec"),
        "one_minute_confirmed_complete": snapshots["1m"].get("confirmed_history_complete"),
        "relevant_htf_observed_at": min(htf_received) if htf_received else None,
        "relevant_htf_bar_open_ts": min(htf_bar_open) if htf_bar_open else None,
        "relevant_htf_bar_age": max(htf_bar) if htf_bar else None,
        "relevant_htf_transport_age": max(htf_transport) if htf_transport else None,
        "relevant_htf_confirmed_complete": all(
            bool(row.get("confirmed_history_complete")) for row in htf_snapshots
        ),
        "ws_connection_generation": ws_generation,
        "rest_fallback_used": any(source and "websocket" not in source for source in sources),
        "gap_repair_state": next((state for state in gap_states if state in {"gap_detected", "disconnected", "failed"}), ""),
    }


async def _timed_engine_dependency(engine: str, coin: str, operation: str, data_type: str, awaitable):
    started = time.monotonic()
    outcome = "ok"
    try:
        return await awaitable
    except BaseException as exc:
        outcome = "cancelled" if isinstance(exc, asyncio.CancelledError) else "error"
        raise
    finally:
        _latency.record_engine_dependency(
            engine=engine,
            instrument=coin,
            operation=operation,
            data_type=data_type,
            duration_sec=time.monotonic() - started,
            outcome=outcome,
        )
async def _candidate_preview_ticker(okx: OKXClient, coin: str, *, engine_name: str = "") -> dict:
    """Use the streaming price for preview work; REST remains authoritative at submission."""
    snapshot = execution_price_snapshot(
        coin,
        float(_s.get_val("execution_market_data_max_age_sec", 90.0) or 90.0),
    )
    if bool(snapshot.get("fresh")) and float(snapshot.get("price", 0.0) or 0.0) > 0.0:
        _latency.record_ticker(engine_name, coin, "ws", 0.0, success=True)
        return {
            "last": float(snapshot["price"]),
            "bid": 0.0,
            "ask": 0.0,
            "source": "ws_1m",
            "timestamp": float(snapshot.get("latest_timestamp", 0.0) or 0.0),
        }
    fallback_started = time.monotonic()
    try:
        ticker = dict(await okx.get_ticker(coin) or {})
    except Exception:
        _latency.record_ticker(
            engine_name,
            coin,
            "rest",
            time.monotonic() - fallback_started,
            success=False,
        )
        raise
    _latency.record_ticker(
        engine_name,
        coin,
        "rest",
        time.monotonic() - fallback_started,
        success=True,
    )
    ticker.setdefault("source", "okx_rest")
    return ticker

# Slot replacement anti-churn guard (monotonic time of last successful replacement).
_last_slot_rotation_ts: float = 0.0
_symbol_direction_cooldowns: dict[tuple[str, str], float] = {}
_SILENT_SIGNAL_REASONS: tuple[str, ...] = (
    "missing_data",
    "insufficient_candles",
    "invalid_direction",
    "raw_hold_return",
    "engine_returned_hold",
    "no_votes",
    "votes_below_floor",
    "near_vote_floor",
    "vote_rescue_eligible",
    "vote_rescue_success",
    "selector_contribution_only",
    "shadow_contribution_only",
    "role_not_opener",
    "low_confidence",
    "low_quality",
    "admission_confidence_low",
    "regime_block",
    "market_mode_block",
    "spread_liquidity_block",
    "volatility_block",
    "execution_admission_block",
    "capital_block",
    "sizing_floor_block",
    "duplicate_position",
    "cooldown_block",
    "weekly_cap_block",
    "runtime_lock_block",
    "unknown_exception",
)
_silent_signal_diagnostics: dict = {
    "started_at": time.time(),
    "total": 0,
    "reasons": {reason: 0 for reason in _SILENT_SIGNAL_REASONS},
    "by_engine": {},
    "recent": [],
}
_SIGNAL_FUNNEL_COUNTER_PATH = Path("data/signal_funnel_counters.json")


def _load_signal_funnel_counters() -> None:
    try:
        if not _SIGNAL_FUNNEL_COUNTER_PATH.exists():
            return
        payload = json.loads(_SIGNAL_FUNNEL_COUNTER_PATH.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            return
        _silent_signal_diagnostics["started_at"] = float(payload.get("started_at", time.time()) or time.time())
        _silent_signal_diagnostics["total"] = int(payload.get("total", 0) or 0)
        reasons = {key: int((payload.get("reasons", {}) or {}).get(key, 0) or 0) for key in _SILENT_SIGNAL_REASONS}
        reasons["vote_rescue_success"] = min(reasons["vote_rescue_success"], reasons["vote_rescue_eligible"])
        _silent_signal_diagnostics["reasons"] = reasons
        _silent_signal_diagnostics["by_engine"] = dict(payload.get("by_engine", {}) or {})
        _silent_signal_diagnostics["recent"] = list(payload.get("recent", []) or [])[-100:]
    except Exception:
        logger.warning("Signal funnel counter load failed", exc_info=True)


def _save_signal_funnel_counters() -> None:
    if "PYTEST_CURRENT_TEST" in os.environ and _SIGNAL_FUNNEL_COUNTER_PATH == Path("data/signal_funnel_counters.json"):
        return
    started = time.monotonic()
    success = False
    try:
        _SIGNAL_FUNNEL_COUNTER_PATH.parent.mkdir(parents=True, exist_ok=True)
        _SIGNAL_FUNNEL_COUNTER_PATH.write_text(json.dumps(_silent_signal_diagnostics, indent=2), encoding="utf-8")
        success = True
    except Exception:
        logger.warning("Signal funnel counter save failed", exc_info=True)
    finally:
        _latency.record_signal_write(time.monotonic() - started, success=success)
_SPREAD_LIQUIDITY_BLOCK_MAX = 300
_spread_liquidity_block_diagnostics: list[dict] = []


def _diag_float(value):
    try:
        if value is None:
            return None
        return float(value)
    except Exception:
        return None


def _diag_ticker_value(ticker, *keys):
    if not isinstance(ticker, dict):
        return None
    for key in keys:
        value = _diag_float(ticker.get(key))
        if value is not None:
            return value
    return None


def _diag_spread_pct(ticker):
    bid = _diag_ticker_value(ticker, "bid", "bidPx", "bestBid")
    ask = _diag_ticker_value(ticker, "ask", "askPx", "bestAsk")
    last = _diag_ticker_value(ticker, "last", "lastPx", "close")
    try:
        price = last or ((bid + ask) / 2.0 if bid and ask else None)
        if bid is None or ask is None or not price or price <= 0:
            return None
        return ((ask - bid) / price) * 100.0
    except Exception:
        return None


def _diag_spread_liquidity_sub_reason(detail_reason=None, ticker=None, volume_24h=None, volume_threshold=None):
    try:
        text = str(detail_reason or "").lower()
        if "atr" in text or "spike" in text:
            return "atr_spike_filter"
        if "spread" in text:
            return "spread_too_wide"
        if "volume" in text or "vol" in text:
            return "volume_too_low"
        if isinstance(ticker, dict) and not any(_diag_ticker_value(ticker, key) is not None for key in ("bid", "bidPx", "ask", "askPx", "last", "lastPx")):
            return "ticker_missing"
        vol = _diag_float(volume_24h)
        threshold = _diag_float(volume_threshold)
        if vol is not None and threshold is not None and 0 < vol < threshold:
            return "volume_too_low"
    except Exception:
        pass
    return "unknown"


def _record_spread_liquidity_block(
    *,
    symbol=None,
    engine=None,
    side=None,
    sub_reason=None,
    ticker=None,
    detail_reason=None,
    spread_threshold=None,
    volume_24h=None,
    volume_threshold=None,
    liquidity_score=None,
    depth=None,
    data_age_sec=None,
    market_regime=None,
    role=None,
):
    """In-memory diagnostics only; must never influence candidate flow."""
    try:
        bid = _diag_ticker_value(ticker, "bid", "bidPx", "bestBid")
        ask = _diag_ticker_value(ticker, "ask", "askPx", "bestAsk")
        last = _diag_ticker_value(ticker, "last", "lastPx", "close")
        vol = _diag_float(volume_24h)
        if vol is None:
            vol = _diag_ticker_value(ticker, "volCcy24h", "vol24h")
        record = {
            "timestamp": time.time(),
            "symbol": str(symbol or "").upper() or None,
            "engine": str(engine or "").upper() or None,
            "side": str(side or "").upper() or None,
            "reason": "spread_liquidity_block",
            "sub_reason": str(sub_reason or _diag_spread_liquidity_sub_reason(detail_reason, ticker, vol, volume_threshold)),
            "bid": bid,
            "ask": ask,
            "last": last,
            "spread_pct": _diag_spread_pct(ticker),
            "spread_threshold": _diag_float(spread_threshold),
            "volume_24h": vol,
            "volume_threshold": _diag_float(volume_threshold),
            "liquidity_score": _diag_float(liquidity_score),
            "depth": _diag_float(depth),
            "data_age_sec": _diag_float(data_age_sec),
            "market_regime": str(market_regime or "").upper() or None,
            "role": str(role or "").lower() or None,
        }
        _spread_liquidity_block_diagnostics.append(record)
        if len(_spread_liquidity_block_diagnostics) > _SPREAD_LIQUIDITY_BLOCK_MAX:
            del _spread_liquidity_block_diagnostics[:-_SPREAD_LIQUIDITY_BLOCK_MAX]
    except Exception:
        return None
    return None


def get_spread_liquidity_block_diagnostics(limit=50):
    try:
        n = int(limit if limit is not None else 50)
        if n < 0:
            n = 0
        n = min(n, _SPREAD_LIQUIDITY_BLOCK_MAX)
        records = list(_spread_liquidity_block_diagnostics[-n:]) if n else []
        return {
            "status": "OK",
            "total_recent": len(_spread_liquidity_block_diagnostics),
            "limit": n,
            "records": json.loads(json.dumps(records)),
        }
    except Exception:
        return {"status": "ERROR", "total_recent": 0, "limit": 0, "records": []}


def _bump_silent_signal_counter(reason, engine=None, symbol=None, role=None):
    """In-memory diagnostics only; must never influence candidate flow."""
    try:
        reason_key = str(reason or "unknown_exception").strip().lower()
        if reason_key not in _silent_signal_diagnostics["reasons"]:
            reason_key = "unknown_exception"
        engine_key = str(engine or "UNKNOWN").strip().upper() or "UNKNOWN"
        role_key = str(role or "").strip().lower()
        symbol_key = str(symbol or "").strip().upper()
        diag = _silent_signal_diagnostics
        diag["total"] = int(diag.get("total", 0) or 0) + 1
        diag["reasons"][reason_key] = int(diag["reasons"].get(reason_key, 0) or 0) + 1
        by_engine = diag.setdefault("by_engine", {})
        row = by_engine.setdefault(
            engine_key,
            {
                "engine": engine_key,
                "role": role_key,
                "total": 0,
                "reasons": {key: 0 for key in _SILENT_SIGNAL_REASONS},
            },
        )
        if role_key and not row.get("role"):
            row["role"] = role_key
        row["total"] = int(row.get("total", 0) or 0) + 1
        row_reasons = row.setdefault("reasons", {key: 0 for key in _SILENT_SIGNAL_REASONS})
        row_reasons[reason_key] = int(row_reasons.get(reason_key, 0) or 0) + 1
        recent = diag.setdefault("recent", [])
        recent.append(
            {
                "ts": time.time(),
                "reason": reason_key,
                "engine": engine_key,
                "symbol": symbol_key,
                "role": role_key,
            }
        )
        if len(recent) > 100:
            del recent[:-100]
        _save_signal_funnel_counters()
    except Exception:
        return None
    return None


def get_silent_signal_diagnostics():
    try:
        return json.loads(json.dumps(_silent_signal_diagnostics))
    except Exception:
        return {
            "started_at": 0.0,
            "total": 0,
            "reasons": {reason: 0 for reason in _SILENT_SIGNAL_REASONS},
            "by_engine": {},
            "recent": [],
        }


def reset_silent_signal_diagnostics():
    try:
        _silent_signal_diagnostics["started_at"] = time.time()
        _silent_signal_diagnostics["total"] = 0
        _silent_signal_diagnostics["reasons"] = {reason: 0 for reason in _SILENT_SIGNAL_REASONS}
        _silent_signal_diagnostics["by_engine"] = {}
        _silent_signal_diagnostics["recent"] = []
        _save_signal_funnel_counters()
    except Exception:
        pass


_load_signal_funnel_counters()


def _dbg_snap() -> dict:
    try:
        return _s.get_debug_override_snapshot() or {}
    except Exception:
        return {"mode": "off", "levels": {}, "active": {}}


def _symbol_direction_key(coin: str, direction: str) -> tuple[str, str]:
    return (str(coin or "").upper(), str(direction or "").lower())


def _symbol_direction_cooldown_remaining(coin: str, direction: str) -> float:
    key = _symbol_direction_key(coin, direction)
    until = float(_symbol_direction_cooldowns.get(key, 0.0) or 0.0)
    now = time.time()
    if until <= now:
        _symbol_direction_cooldowns.pop(key, None)
        return 0.0
    return until - now


def _symbol_direction_rehab_state(
    coin: str,
    direction: str,
    *,
    candidate_quality: float = 0.0,
    consensus_count: int = 0,
    selector_bonus: float = 0.0,
) -> _life.SymbolDirectionCooling:
    window_mins = int(float(_s.get_val("symbol_direction_memory_decay_hours", 24.0) or 24.0) * 60.0)
    losses = int(_perf.get_recent_directional_loss_count(coin, direction, max(window_mins, 1)) or 0)
    return _life.symbol_direction_cooling_state(
        loss_count=losses,
        threshold=int(float(_s.get_val("symbol_direction_loss_threshold", 3) or 3)),
        cooldown_minutes=float(_s.get_val("symbol_direction_cooldown_minutes", 0.0) or 0.0),
        confidence_bump=float(_s.get_val("symbol_direction_loss_conf_bump", 0.60) or 0.60),
        size_multiplier=float(_s.get_val("symbol_direction_loss_size_mult", 0.85) or 0.85),
        mild_threshold=int(float(_s.get_val("symbol_direction_mild_loss_threshold", 2) or 2)),
        strong_threshold=int(float(_s.get_val("symbol_direction_confirm_loss_threshold", _s.get_val("symbol_direction_loss_threshold", 3)) or 3)),
        cooldown_threshold=int(float(_s.get_val("symbol_direction_cooldown_loss_threshold", 5) or 5)),
        mild_confidence_bump=float(_s.get_val("symbol_direction_mild_conf_bump", 0.25) or 0.25),
        mild_size_multiplier=float(_s.get_val("symbol_direction_mild_size_mult", 0.90) or 0.90),
        min_consensus=int(float(_s.get_val("symbol_direction_min_consensus", 2) or 2)),
        quality_floor=float(_s.get_val("symbol_direction_quality_override", 82.0) or 82.0),
        candidate_quality=float(candidate_quality or 0.0),
        consensus_count=int(consensus_count or 0),
        selector_bonus=float(selector_bonus or 0.0),
    )


def _rate_limit_degraded_flow(okx_client) -> tuple[bool, str]:
    try:
        if not hasattr(okx_client, "get_rate_limit_snapshot"):
            return False, ""
        snap = okx_client.get_rate_limit_snapshot() or {}
        warnings = [str(x) for x in (snap.get("stale_warnings", []) or [])]
        pressure = list(snap.get("endpoint_pressure", []) or [])
        taker_warn = any("taker" in w.lower() or "rubik" in w.lower() for w in warnings)
        taker_backoff = any(
            "taker-volume" in str(row.get("endpoint", ""))
            and bool(row.get("backoff_active"))
            for row in pressure
            if isinstance(row, dict)
        )
        degraded = bool(taker_warn or taker_backoff)
        return degraded, ("; ".join(warnings[:2]) or "rubik_taker_backoff" if degraded else "")
    except Exception:
        return False, ""


def _mark_symbol_direction_loss(coin: str, direction: str) -> None:
    if not bool(_s.get_val("symbol_direction_loss_cooldown_enabled", False)):
        return
    try:
        state = _symbol_direction_rehab_state(coin, direction)
        if state.active and state.cooldown_minutes > 0:
            _symbol_direction_cooldowns[_symbol_direction_key(coin, direction)] = time.time() + state.cooldown_minutes * 60.0
            logger.info(
                "SYMBOL-DIRECTION COOLDOWN %s %s losses=%d stage=%s cooldown=%.1fm",
                coin,
                str(direction or "").upper(),
                state.loss_count,
                state.stage,
                state.cooldown_minutes,
            )
    except Exception:
        return


def _dbg_level(levels: dict, family: str) -> str:
    raw = str((levels or {}).get(family, "normal") or "normal").strip().lower()
    return raw if raw in {"normal", "relaxed", "unlimited"} else "normal"


def _dbg_mode(mode: str | None) -> str:
    raw = str(mode or "off").strip().lower()
    return raw if raw in {"off", "probe", "live_override"} else "off"


def _dbg_active_label(active: dict) -> str:
    if not active:
        return ""
    return ", ".join(f"{k}={str(v).upper()}" for k, v in sorted(active.items()))


def _dbg_adjust_global_cap(base_cap: int, level: str) -> int:
    cap = max(int(base_cap or 0), 1)
    lvl = str(level or "normal").strip().lower()
    if lvl == "relaxed":
        bump = max(1, int(round(cap * 0.25)))
        return max(min(cap + bump, 30), 1)
    if lvl == "unlimited":
        # Bounded "unlimited": never infinite.
        return max(min(max(cap * 3, cap + 5), 30), 1)
    return cap


def _dbg_adjust_engine_cap(base_cap: int, level: str, *, engine_name: str = "") -> int:
    cap = max(int(base_cap or 0), 1)
    lvl = str(level or "normal").strip().lower()
    if lvl == "relaxed":
        cap = cap + 1
    elif lvl == "unlimited":
        cap = max(cap + 2, cap * 2)
    # Bounded: diagnostic headroom only.
    cap = min(cap, 6)
    if str(engine_name or "").upper() == "HACHIMAN":
        cap = max(cap, 3)
    return max(cap, 1)


def _dbg_adjust_portfolio_dir_cap(base_cap: int, level: str) -> int:
    cap = max(int(base_cap or 0), 1)
    lvl = str(level or "normal").strip().lower()
    if lvl == "relaxed":
        return min(cap + 2, 12)
    if lvl == "unlimited":
        return min(max(cap * 3, cap + 4), 16)
    return cap


def _dbg_registry_meta() -> dict:
    snap = _dbg_snap()
    if _dbg_mode(snap.get("mode")) != "live_override":
        return {}
    active = snap.get("active", {}) or {}
    if not active:
        return {}
    return {
        "debug_override_mode": "live_override",
        "debug_override_active": dict(active),
        "debug_override_label": _dbg_active_label(active),
    }


def _dbg_record_accept(
    engine: str,
    coin: str,
    direction: str,
    conf_val: float,
    *,
    size_usdt: float = 0.0,
    candidate_quality: float = 0.0,
    regime: str = "",
    slot_state: str = "",
    exposure_state: str = "",
    size_decision: str = "",
    reason: str = "admitted",
) -> None:
    snap = _dbg_snap()
    if _dbg_mode(snap.get("mode")) != "live_override":
        active = {}
    else:
        active = snap.get("active", {}) or {}
    try:
        _record_decision_trace(
            engine=engine,
            coin=coin,
            direction=direction,
            confidence=float(conf_val or 0.0),
            raw_score=float(conf_val or 0.0),
            decision="admitted",
            reason=reason,
            size_usdt=size_usdt,
            candidate_quality=candidate_quality,
            regime=regime,
            slot_state=slot_state,
            exposure_state=exposure_state,
            size_decision=size_decision,
            notes=_dbg_active_label(active) if active else "",
            extra={"live_override": bool(active)},
        )
        if active:
            _rjlog.record(
                engine,
                coin,
                direction,
                float(conf_val or 0.0),
                "debug_override_accept",
                f"accepted under LIVE override ({_dbg_active_label(active)})",
            )
    except Exception:
        pass


def _simulate_blockers_under_levels(
    engine_name: str,
    coin: str,
    direction: str,
    *,
    balance: float,
    capital_profile=None,
    candidate_quality: float = 0.0,
    confidence: float = 0.0,
    intent=None,
    candidate_size_usdt: float = 0.0,
    levels: dict | None = None,
) -> list[str]:
    """Best-effort blocker-family simulation used by probe mode (no exchange calls)."""
    lv = levels or {}
    blockers: set[str] = set()

    # Weekly cap (adaptive): only counts as a blocker when the decision is BLOCK.
    try:
        _wk_res = _wcap.evaluate(
            balance=float(balance or 0.0),
            confidence=float(confidence or 0.0),
            candidate_quality=float(candidate_quality or 0.0),
            engine_name=str(engine_name or ""),
            override_level=_dbg_level(lv, "weekly_cap"),
            mutate_state=False,
        )
        if str(_wk_res.decision or "").upper() == "BLOCK":
            blockers.add("weekly_cap")
    except Exception:
        pass

    # Engine cap
    if _per_engine_cap_enabled():
        base_cap = get_effective_engine_cap(str(engine_name or ""), balance=balance, capital_profile=capital_profile)
        cap = _dbg_adjust_engine_cap(base_cap, _dbg_level(lv, "engine_cap"), engine_name=str(engine_name or ""))
        if _reg_engine_count(str(engine_name or "")) >= int(cap or 1):
            blockers.add("engine_cap")

    # Correlation cap
    corr_ok, _corr_reason = _correlation_check(
        coin,
        direction,
        balance=balance,
        capital_profile=capital_profile,
        candidate_quality=candidate_quality,
        debug_override_level=_dbg_level(lv, "correlation_cap"),
    )
    if not corr_ok:
        blockers.add("correlation_cap")

    # Global cap (simple cap check; does not attempt replacement rotation simulation)
    base_global, _meta = _effective_global_slot_limit(
        balance,
        capital_profile=capital_profile,
        candidate_quality=candidate_quality,
        intent=intent,
    )
    global_cap = _dbg_adjust_global_cap(base_global, _dbg_level(lv, "global_cap"))
    if _reg_count() >= int(global_cap or 1):
        blockers.add("global_cap")

    # Portfolio direction count cap
    same_dir_limit = int(_s.get_val("max_same_direction_positions", 4) or 4)
    same_dir_limit = _dbg_adjust_portfolio_dir_cap(same_dir_limit, _dbg_level(lv, "portfolio_direction_cap"))
    if same_dir_limit > 0 and _reg_direction_count(str(direction or "").lower()) >= same_dir_limit:
        blockers.add("portfolio_direction_cap")

    # Same-direction budget (headroom)
    try:
        from portfolio_allocator import check_same_direction_budget as _check_dir_budget
        ok_dir, _r = _check_dir_budget(
            str(direction or "").lower(),
            float(candidate_size_usdt or 0.0),
            float(balance or 0.0),
            capital_profile=capital_profile,
            candidate_quality=candidate_quality,
            debug_override_level=_dbg_level(lv, "same_direction_cap"),
        )
        if not ok_dir:
            blockers.add("same_direction_cap")
    except Exception:
        pass

    # Lane budget
    try:
        from portfolio_allocator import check_lane_budget as _check_lane_budget
        ok_lane, _r = _check_lane_budget(
            str(engine_name or ""),
            float(candidate_size_usdt or 0.0),
            float(balance or 0.0),
            capital_profile=capital_profile,
            soft_accept=False,
            session=None,
            debug_override_level=_dbg_level(lv, "lane_budget"),
        )
        if not ok_lane:
            blockers.add("lane_budget")
    except Exception:
        pass

    # Exposure / correlation / same-side caps (notional governance)
    try:
        from exposure_tracker import check_exposure as _check_exposure
        max_exp_pct = capital_profile.max_exposure_pct if capital_profile else 60.0
        ok_exp, exp_reason = _check_exposure(
            coin=str(coin or ""),
            direction=str(direction or "").lower(),
            size_usdt=float(candidate_size_usdt or 0.0),
            balance=float(balance or 0.0),
            max_exposure_pct=float(max_exp_pct),
            capital_profile=capital_profile,
            candidate_quality=candidate_quality,
            debug_override_exposure_cap=_dbg_level(lv, "exposure_cap"),
            debug_override_correlation_cap=_dbg_level(lv, "correlation_cap"),
            debug_override_same_direction_cap=_dbg_level(lv, "same_direction_cap"),
        )
        if not ok_exp:
            low = str(exp_reason or "").lower()
            if "correlation cap" in low:
                blockers.add("correlation_cap")
            elif "long exposure" in low or "short exposure" in low:
                blockers.add("same_direction_cap")
            else:
                blockers.add("exposure_cap")
    except Exception:
        pass

    # Stable ordering for display.
    order = [
        "weekly_cap",
        "engine_cap",
        "global_cap",
        "exposure_cap",
        "same_direction_cap",
        "portfolio_direction_cap",
        "correlation_cap",
        "lane_budget",
    ]
    return [b for b in order if b in blockers]


def _probe_suffix(
    engine_name: str,
    coin: str,
    direction: str,
    *,
    balance: float,
    capital_profile=None,
    candidate_quality: float = 0.0,
    confidence: float = 0.0,
    intent=None,
    candidate_size_usdt: float = 0.0,
) -> str:
    """Probe-only: annotate a rejection with "would pass if" under selected overrides."""
    snap = _dbg_snap()
    mode = _dbg_mode(snap.get("mode"))
    if mode != "probe":
        return ""
    active = snap.get("active", {}) or {}
    if not active:
        return ""

    base_levels = {k: "normal" for k in (snap.get("levels", {}) or {}).keys()}
    # In case levels dict is missing keys, still cover known families.
    for k in (
        "global_cap",
        "engine_cap",
        "weekly_cap",
        "exposure_cap",
        "correlation_cap",
        "same_direction_cap",
        "portfolio_direction_cap",
        "lane_budget",
    ):
        base_levels.setdefault(k, "normal")

    baseline = _simulate_blockers_under_levels(
        engine_name,
        coin,
        direction,
        balance=balance,
        capital_profile=capital_profile,
        candidate_quality=candidate_quality,
        confidence=confidence,
        intent=intent,
        candidate_size_usdt=candidate_size_usdt,
        levels=base_levels,
    )
    with_overrides = _simulate_blockers_under_levels(
        engine_name,
        coin,
        direction,
        balance=balance,
        capital_profile=capital_profile,
        candidate_quality=candidate_quality,
        confidence=confidence,
        intent=intent,
        candidate_size_usdt=candidate_size_usdt,
        levels=(snap.get("levels", {}) or {}),
    )

    cleared = [b for b in baseline if b not in with_overrides]
    still = list(with_overrides)

    selected = _dbg_active_label(active)
    cleared_s = ",".join(cleared) if cleared else "—"
    still_s = ",".join(still) if still else "—"
    return f" | PROBE selected[{selected}] cleared[{cleared_s}] still_blocked[{still_s}]"


def _ef_tag(reason: str) -> str:
    """Map an entry_filter reason string to a canonical rejection tag."""
    r = reason.lower()
    if "blackswan" in r:                  return "blackswan"
    if "weekly" in r:                     return "weekly_cap"
    if "daily" in r:                      return "daily_cap"
    if "3-5-7" in r or "open risk" in r: return "open_risk"
    if "cooldown" in r:                   return "cooldown"
    if "session" in r or "weekend" in r: return "entry_filter"
    if "htf" in r:                        return "entry_filter"
    if "funding" in r:                    return "entry_filter"
    if "oi " in r or "open interest" in r: return "entry_filter"
    return "entry_filter"


def _is_soft_ef_reason(reason: str) -> bool:
    """Return True if an entry_filter failure is soft (eligible for soft-accept override).

    HARD reasons (blackswan, weekly cap, 3-5-7 open risk) → never bypass.
    SOFT reasons (HTF, funding, OI min, session) → allow if confidence is high enough.
    """
    import entry_filter as _ef
    return _ef.is_soft_reason(reason)


def _rej(engine: str, coin: str, direction: str,
         conf,          # float | ConfidenceResult | None
         tag: str, detail: str,
         *,
         balance: float | None = None,
         candidate_quality: float = 0.0,
         regime: str = "",
         size_usdt: float = 0.0,
         slot_state: str = "",
         exposure_state: str = "",
         size_decision: str = "",
         extra: dict | None = None) -> None:
    """Record a structured rejection and emit one clean INFO log line."""
    c = float(conf.confidence_score if hasattr(conf, "confidence_score") else (conf or 0.0))
    _research_funnel.record(
        "pre_candidate", engine=engine, symbol=coin, reason=str(tag or "rejected"),
        direction=str(direction or "").upper(), confidence=c, detail=str(detail or ""),
    )
    meta = _rjlog.record(engine, coin, direction, c, tag, detail)
    _record_decision_trace(
        engine=engine,
        coin=coin,
        direction=direction,
        confidence=c,
        raw_score=c,
        decision="skipped",
        reason=f"{tag}: {str(detail or '').replace(chr(10), ' ').strip()}",
        balance=balance,
        size_usdt=size_usdt,
        candidate_quality=candidate_quality,
        regime=regime,
        slot_state=slot_state,
        exposure_state=exposure_state,
        size_decision=size_decision,
        notes=tag,
        extra=extra,
    )
    if meta.get("suppressed_log"):
        return
    _prefix = "BLOCKED"
    if tag == "lane_budget":
        _prefix = "BUDGET BLOCK"
    elif tag == "headroom":
        _prefix = "HEADROOM BLOCK"
    logger.info(
        "%s [%s] %s %s conf=%.2f | %s: %s",
        _prefix,
        engine_log_name(engine),
        str(coin or "").upper(),
        str(direction or "").upper(),
        c,
        tag,
        str(detail or "").replace("\n", " ").strip(),
    )


def _release(engine: str, coin: str, direction: str, conf, reason: str) -> None:
    c = float(conf.confidence_score if hasattr(conf, "confidence_score") else (conf or 0.0))
    # A generic candidate cleanup may only remove a reservation that is
    # explicitly proven never to have crossed the exchange mutation boundary.
    # Submission-attempted/uncertain rows must remain visible for recovery.
    reg_release_pre_submit(coin)
    logger.info(
        "RELEASE [%s] %s %s conf=%.2f | reason=%s",
        engine_log_name(engine),
        str(coin or "").upper(),
        str(direction or "").upper(),
        c,
        str(reason or "unspecified").replace("\n", " ").strip(),
    )


def _participation_cap_window(
    engine_name: str,
    direction: str,
    balance: float,
    capital_profile=None,
    *,
    soft_accept: bool = False,
    session: str | None = None,
    include_lane: bool = True,
) -> tuple[float | None, list[str]]:
    if not bool(_s.get_val("headroom_scaling_enabled", True)):
        return None, []
    if balance <= 0:
        return None, []

    caps: list[float] = []
    details: list[str] = []

    from portfolio_allocator import (
        get_lane_available_usdt as _get_lane_available_usdt,
        get_same_direction_headroom_usdt as _get_same_direction_headroom_usdt,
    )
    if bool(_s.get_val("same_direction_headroom_enabled", True)):
        headroom = float(
            _get_same_direction_headroom_usdt(
                direction,
                balance,
                capital_profile=capital_profile,
            )
            or 0.0
        )
        caps.append(headroom)
        details.append(f"headroom ${headroom:.2f}")

    if include_lane and bool(_s.get_val("lane_budget_enabled", True)):
        lane_avail = float(
            _get_lane_available_usdt(
                engine_name,
                balance,
                capital_profile=capital_profile,
                soft_accept=soft_accept,
                session=session,
            )
            or 0.0
        )
        caps.append(lane_avail)
        details.append(f"lane ${lane_avail:.2f}")

    if bool(_s.get_val("v6_enabled", False)) and bool(_s.get_val("v6_correlation_control_enabled", True)):
        from exposure_tracker import get_available_exposure_usdt as _get_available_exposure_usdt

        max_exp_pct = float(getattr(capital_profile, "max_exposure_pct", 60.0) or 60.0)
        exposure_avail = float(_get_available_exposure_usdt(balance, max_exp_pct) or 0.0)
        caps.append(exposure_avail)
        details.append(f"exposure ${exposure_avail:.2f}")

    if not caps:
        return None, details
    return max(min(caps), 0.0), details


def _apply_participation_scaling(
    engine_name: str,
    direction: str,
    requested_size: float,
    balance: float,
    capital_profile=None,
    *,
    soft_accept: bool = False,
    session: str | None = None,
    include_lane: bool = True,
) -> tuple[float, str]:
    requested = max(float(requested_size or 0.0), 0.0)
    available, details = _participation_cap_window(
        engine_name,
        direction,
        balance,
        capital_profile=capital_profile,
        soft_accept=soft_accept,
        session=session,
        include_lane=include_lane,
    )
    if available is None or requested <= 0 or requested <= available + 0.01:
        return requested, ""
    floor = _effective_exec_floor(balance)
    if available >= floor:
        return round(available, 2), f"participation scale ${requested:.2f} -> ${available:.2f} | {' | '.join(details)}"
    return requested, ""


def _format_trade_explanation(
    *,
    intent,
    consensus,
    confidence: float,
    quality_score: float,
    base_size: float,
    final_size: float,
    leverage: int,
    size_reason: str,
    margin_used: float,
    tp_reason: str,
    entry_reason: str,
) -> str:
    lines = [
        "*Trade Explanation*",
        f"Intent: `{intent.mode}` - {intent.summary}",
        f"Engines: `{consensus.agreement_count}` aligned - {consensus.summary}",
        f"Confidence: `{confidence:.2f}`",
        f"Quality: `{quality_score:.1f}`",
        f"Sizing: base `${base_size:.2f}` -> live `${final_size:.2f}` | margin `~${margin_used:.2f}` @ `{leverage}x`",
        f"Size Logic: `{size_reason}`",
        f"TP Logic: `{tp_reason}`",
        f"Entry: `{entry_reason}`",
    ]
    return "\n".join(lines)


def _effective_exec_floor(balance: float, coin: str = "", entry_price: float = 0.0) -> float:
    """Shared placement floor, using contract geometry when available."""
    import executable_minimum as _exec_min

    instrument_min = 0.0
    contract_aware = bool(coin and entry_price and _s.get_val("contract_aware_execution_floor_enabled", False))
    if contract_aware:
        try:
            from instrument_registry import registry as _instrument_registry
            instrument = _instrument_registry.get(coin, require_fresh=True)
            if instrument is not None:
                instrument_min = float(instrument.min_sz) * float(instrument.contract_notional(float(entry_price)))
        except Exception:
            contract_aware = False

    return float(
        _exec_min.placement_min_notional_usdt(
            balance,
            instrument_min_notional_usdt=instrument_min,
            settings=_s,
            absolute_floor_usdt=float(_s.get_val("adaptive_min_economic_notional_usdt", 1.0) or 1.0),
            contract_aware=contract_aware,
        )
    )


def _candidate_exec_floor(coin: str, entry_price: float, balance: float) -> float:
    """Shared placement floor including instrument min notional when known."""
    import executable_minimum as _exec_min

    configured = max(float(_s.get_val("adaptive_min_economic_notional_usdt", 1.0) or 1.0), 0.0)
    exchange = 0.0
    try:
        from instrument_registry import registry as _instrument_registry

        instrument = _instrument_registry.get(coin, require_fresh=True)
        if instrument is not None and float(entry_price or 0.0) > 0.0:
            exchange = float(instrument.min_sz) * float(instrument.contract_notional(float(entry_price)))
    except Exception:
        exchange = 0.0
    return float(
        _exec_min.placement_min_notional_usdt(
            balance,
            instrument_min_notional_usdt=exchange,
            settings=_s,
            absolute_floor_usdt=configured if configured > 0 else 1.0,
            contract_aware=bool(_s.get_val("contract_aware_execution_floor_enabled", False) and exchange > 0),
        )
    )


def _candidate_quality_snapshot(
    engine_name: str,
    coin: str,
    direction: str,
    balance: float,
    votes: float,
    confidence_score: float,
):
    preference = get_universe_preference(coin, balance_usdt=balance)
    volume_quality = float(preference.get("volume_quality", 0.0) or 0.0)
    spread_quality = float(preference.get("spread_quality", 0.0) or 0.0)
    market_cap_bonus = float(preference.get("market_cap_bonus", 0.0) or 0.0)
    selection_penalty = float(preference.get("selection_penalty", 0.0) or 0.0)
    consensus = _consensus.get_snapshot(coin, str(direction or "").upper())
    engine_weight = float(_s.get_val(f"v6_weight_{str(engine_name or '').lower()}", 1.0) or 1.0)
    selector_bonus = 0.0
    if bool(_s.get_val("hachiman_selector_enabled", True)):
        volume_floor = float(_s.get_val("hachiman_volume_quality_min", 1.2) or 1.2)
        spread_floor = float(_s.get_val("hachiman_spread_quality_min", 0.15) or 0.15)
        if volume_quality >= volume_floor and spread_quality >= spread_floor:
            selector_bonus += float(_s.get_val("hachiman_selection_bonus", 6.0) or 6.0)
            if coin.upper() in set(get_top20_mcap()):
                selector_bonus += float(_s.get_val("hachiman_top_bias_bonus", 3.0) or 3.0)

    base_quality_score = (
        float(confidence_score or 0.0) * 18.0
        + float(votes or 0.0) * 10.0
        + min(volume_quality, 8.0) * 4.0
        + spread_quality * 10.0
        + market_cap_bonus
        + float(consensus.agreement_bonus or 0.0)
        + max(engine_weight - 1.0, 0.0) * 8.0
        + selector_bonus
        - selection_penalty
    )

    # Winrate optimization layer:
    # ranking/ordering improvements only; avoid new broad hard blocks.
    winrate_enabled = bool(_s.get_val("winrate_optimization_enabled", False))
    winrate_profile = str(_s.get_val("winrate_profile", "balanced") or "balanced").strip().lower()
    winrate_profile = winrate_profile if winrate_profile in {"balanced", "strict", "adaptive"} else "balanced"

    regime = ""
    try:
        regime = str(_regime.get_regime() if _regime else "") or ""
    except Exception:
        regime = ""
    lane = ""
    try:
        lane = str(getattr(_s, "ENGINE_LANES", {}).get(str(engine_name or "").upper(), "") or "")
    except Exception:
        lane = ""

    regime_bonus = 0.0
    confluence_bonus = 0.0
    strictness_penalty = 0.0
    if winrate_enabled:
        # Regime-aware cooperation: reward engines that match the current regime,
        # apply only small penalties (no blanket freeze).
        reg = regime.upper().strip()
        if reg == "TRENDING":
            if lane in {"trend", "breakout", "order_flow"}:
                regime_bonus += 3.0
            elif lane == "structure":
                regime_bonus += 1.0
            elif lane == "mean_reversion":
                regime_bonus -= 2.0
        elif reg == "RANGING":
            if lane in {"mean_reversion", "structure"}:
                regime_bonus += 3.0
            elif lane in {"trend", "breakout"}:
                regime_bonus -= 1.0
        elif reg == "VOLATILE":
            if lane == "order_flow":
                regime_bonus += 2.0
            elif lane == "breakout":
                regime_bonus += 1.0
            else:
                regime_bonus -= 1.0

        # Confluence strength: reward premium families when multiple engines agree.
        engines = {str(e or "").upper() for e in list(consensus.engines or [])}
        if "YATAGARASU" in engines:
            confluence_bonus += 4.0
        if "KAGUTSUCHI" in engines:
            confluence_bonus += 3.0
        if "RYUJIN" in engines:
            confluence_bonus += 2.0
        if "AMATERASU" in engines:
            confluence_bonus += 2.0
        if int(consensus.agreement_count or 0) >= 3:
            confluence_bonus += 2.0
        if int(consensus.agreement_count or 0) >= 4:
            confluence_bonus += 2.0

        # Strictness is a penalty, not a hard block. Only meaningful when market quality is weak.
        if winrate_profile == "strict":
            if volume_quality < 1.0:
                strictness_penalty += 2.0
            if spread_quality < 0.15:
                strictness_penalty += 2.0
            if selection_penalty >= 1.0:
                strictness_penalty += 2.0
        elif winrate_profile == "adaptive":
            if reg in {"VOLATILE"} and spread_quality < 0.15:
                strictness_penalty += 1.5
            if reg in {"VOLATILE"} and volume_quality < 1.0:
                strictness_penalty += 1.0

    quality_score = base_quality_score + regime_bonus + confluence_bonus - strictness_penalty
    advisory_penalty = 0.0
    if bool(_s.get_val("genbu_advisory_enabled", True)):
        quality_floor = float(_s.get_val("genbu_quality_floor", 55.0) or 55.0)
        weak_market_quality = (
            volume_quality < 0.50
            or spread_quality < 0.10
            or selection_penalty >= 1.0
        )
        if quality_score < quality_floor or weak_market_quality:
            advisory_penalty = float(_s.get_val("genbu_soft_penalty", 6.0) or 6.0)
            quality_score -= advisory_penalty

    return {
        "quality_score": round(quality_score, 3),
        "quality_score_base": round(base_quality_score, 3),
        "quality_score_winrate_adj": round((regime_bonus + confluence_bonus - strictness_penalty), 3),
        "quality_regime_bonus": round(regime_bonus, 3),
        "quality_confluence_bonus": round(confluence_bonus, 3),
        "quality_strictness_penalty": round(strictness_penalty, 3),
        "winrate_enabled": bool(winrate_enabled),
        "winrate_profile": winrate_profile,
        "regime": regime,
        "lane": lane,
        "volume_quality": round(volume_quality, 3),
        "spread_quality": round(spread_quality, 3),
        "market_cap_bonus": round(market_cap_bonus, 3),
        "selection_penalty": round(selection_penalty, 3),
        "selector_bonus": round(selector_bonus, 3),
        "advisory_penalty": round(advisory_penalty, 3),
        "consensus_bonus": round(float(consensus.agreement_bonus or 0.0), 3),
        "consensus_count": int(consensus.agreement_count or 0),
        "engine_weight": round(engine_weight, 3),
        "consensus": consensus,
    }


def _engine_role(engine_name: str) -> str:
    try:
        return _s.get_engine_role(engine_name)
    except Exception:
        return "hunter"


def _role_weight(role: str) -> float:
    try:
        return _s.get_engine_role_weight(role=role)
    except Exception:
        return 1.0


def _engine_perf(engine_name: str) -> dict:
    try:
        return get_engine_recent_performance(engine_name, 10, 30)
    except Exception:
        return {
            "last10_net": 0.0,
            "last30_net": 0.0,
            "last10_wr": 0.0,
            "last30_wr": 0.0,
            "loss_streak": 0,
            "fee_to_profit_ratio": 0.0,
            "duplicate_entry_rate": 0.0,
        }


def _role_exit_hints(engine_name: str, role: str) -> dict:
    lane = _s.get_engine_lane(engine_name)
    name = str(engine_name or "").upper()
    if name in {"RONIN", "AMATERASU", "HAYATE"} or lane in {"trend", "breakout"}:
        trade_class = "breakout" if lane == "breakout" else "trend"
        exit_style = "runner"
    elif name in {"KAGUTSUCHI", "FUJIN", "RYUJIN"} or lane == "order_flow":
        trade_class = "scalp"
        exit_style = "tp1_lock"
    elif lane == "mean_reversion":
        trade_class = "mean_reversion"
        exit_style = "quick_lock"
    else:
        trade_class = "squeeze" if role == "specialist" else "trend"
        exit_style = "runner" if role == "specialist" else "tp1_lock"
    return {
        "trade_class": trade_class,
        "preferred_exit_style": exit_style,
    }


def _position_strength_context(
    engine_name: str,
    coin: str,
    direction: str,
    quality_snapshot: dict,
    confidence: float,
) -> dict:
    role = _engine_role(engine_name)
    perf = _engine_perf(engine_name)
    consensus = quality_snapshot.get("consensus") or _consensus.get_snapshot(coin, str(direction or "").upper())
    consensus_count = int(getattr(consensus, "agreement_count", 0) or quality_snapshot.get("consensus_count", 0) or 0)
    engines = [str(e or "").upper() for e in list(getattr(consensus, "engines", []) or []) if e]
    if str(engine_name or "").upper() not in engines:
        engines.append(str(engine_name or "").upper())
    roles = {eng: _engine_role(eng) for eng in engines}
    role_values = set(roles.values())
    selector_score = float(quality_snapshot.get("selector_bonus", 0.0) or 0.0)
    booster_score = 0.0
    if role == "booster":
        booster_score = max(float(quality_snapshot.get("quality_score", 0.0) or 0.0) * _role_weight(role) / 10.0, 0.0)
    role_component = _role_weight(role)
    net_component = max(min(float(perf.get("last30_net", 0.0) or 0.0) / 2.0, 0.65), -0.45)
    fee_penalty = min(max(float(perf.get("fee_to_profit_ratio", 0.0) or 0.0), 0.0) * 0.20, 0.35)
    noise_penalty = min(max(float(perf.get("duplicate_entry_rate", 0.0) or 0.0), 0.0) / 100.0 * 0.25, 0.25)
    regime_bonus = max(float(quality_snapshot.get("quality_regime_bonus", 0.0) or 0.0), -2.0) / 10.0
    quality_score = float(quality_snapshot.get("quality_score", 0.0) or 0.0)
    strength = (
        quality_score / 25.0
        + max(consensus_count - 1, 0) * 0.75
        + role_component
        + net_component
        + selector_score / 25.0
        + booster_score / 10.0
        + regime_bonus
        - fee_penalty
        - noise_penalty
    )
    strength = round(max(strength, 0.0), 3)
    positive_quality = (
        float(perf.get("last30_net", 0.0) or 0.0) > 0
        and quality_score >= float(_s.get_val("role_positive_engine_quality", 68.0) or 68.0)
    )
    role_consensus = (
        consensus_count >= 2
        or ("hunter" in role_values and "confirmer" in role_values)
        or ("selector" in role_values and "confirmer" in role_values)
        or positive_quality
        or (role == "specialist" and quality_score >= float(_s.get_val("role_specialist_quality", 72.0) or 72.0))
    )
    return {
        "engine_role": role,
        "role_weight": round(role_component, 3),
        "role_consensus": bool(role_consensus),
        "consensus_count": max(consensus_count, 1),
        "consensus_engines": engines,
        "contributor_roles": sorted(role_values or {role}),
        "selector_score": round(selector_score, 3),
        "booster_score": round(booster_score, 3),
        "position_strength": strength,
        "performance": perf,
        "fee_pressure": round(float(perf.get("fee_to_profit_ratio", 0.0) or 0.0), 4),
        "duplicate_rate": round(float(perf.get("duplicate_entry_rate", 0.0) or 0.0), 1),
        **_role_exit_hints(engine_name, role),
    }


def _role_quality_adjustment(role_ctx: dict) -> float:
    """Small priority adjustment only; never a standalone blocker."""
    strength = float(role_ctx.get("position_strength", 0.0) or 0.0)
    fee_pressure = float(role_ctx.get("fee_pressure", 0.0) or 0.0)
    perf = role_ctx.get("performance", {}) or {}
    last10 = float(perf.get("last10_net", 0.0) or 0.0)
    last30 = float(perf.get("last30_net", 0.0) or 0.0)
    boost = min(strength * 0.75, 6.0)
    if fee_pressure > 0.55:
        boost -= min((fee_pressure - 0.55) * 4.0, 3.0)
    if last10 < 0 and last30 < 0:
        boost -= 2.0
    return round(boost, 3)


def _role_consensus_confidence_lift(role_ctx: dict, quality_snapshot: dict, base_confidence: float) -> tuple[float, str]:
    """Admission-only confidence lift for genuinely strong multi-engine agreement."""
    if not bool(_s.get_val("role_consensus_conf_lift_enabled", True)):
        return 0.0, ""
    consensus_count = int(role_ctx.get("consensus_count", 0) or quality_snapshot.get("consensus_count", 0) or 0)
    strength = float(role_ctx.get("position_strength", 0.0) or 0.0)
    quality = float(quality_snapshot.get("quality_score", 0.0) or 0.0)
    min_count = int(float(_s.get_val("role_consensus_conf_lift_min_count", 3) or 3))
    min_strength = float(_s.get_val("role_consensus_conf_lift_min_strength", 5.5) or 5.5)
    min_quality = float(_s.get_val("role_consensus_conf_lift_min_quality", 65.0) or 65.0)
    if consensus_count < min_count or strength < min_strength or quality < min_quality:
        return 0.0, ""
    roles = set(str(r or "").lower() for r in list(role_ctx.get("contributor_roles", []) or []))
    role_diversity = len([r for r in roles if r])
    if role_diversity < 2 and consensus_count < (min_count + 1):
        return 0.0, ""
    max_lift = max(float(_s.get_val("role_consensus_conf_lift_max", 1.25) or 1.25), 0.0)
    count_lift = max(consensus_count - min_count + 1, 0) * 0.25
    strength_lift = max(strength - min_strength, 0.0) * 0.12
    quality_lift = max(quality - min_quality, 0.0) / 100.0
    lift = min(max_lift, count_lift + strength_lift + quality_lift)
    if lift <= 0:
        return 0.0, ""
    return round(lift, 3), (
        f"role_consensus_lift +{lift:.2f} "
        f"count={consensus_count} strength={strength:.2f} quality={quality:.1f}"
    )


def _confidence_stage_trace(
    *,
    base_confidence: float,
    consensus_lift: float = 0.0,
    quality_snapshot: dict | None = None,
    degraded_data: bool = False,
    recovery_deficit: float = 0.0,
    admission_confidence: float | None = None,
) -> dict:
    quality_snapshot = dict(quality_snapshot or {})
    regime_modifier = float(quality_snapshot.get("quality_regime_bonus", 0.0) or 0.0) / 10.0
    degraded_modifier = -abs(float(_s.get_val("weekly_cap_degraded_conf_grace", 0.0) or 0.0)) if degraded_data else 0.0
    return _life.build_confidence_stage_trace(
        base_confidence=float(base_confidence or 0.0),
        consensus_lift=float(consensus_lift or 0.0),
        regime_modifier=regime_modifier,
        symbol_memory_modifier=0.0,
        degraded_data_modifier=degraded_modifier,
        recovery_adjustment=-abs(float(recovery_deficit or 0.0)),
        admission_confidence=admission_confidence,
    )


def _role_independent_open_allowed(role_ctx: dict, quality_score: float) -> tuple[bool, str]:
    role = str(role_ctx.get("engine_role", "hunter") or "hunter")
    consensus = bool(role_ctx.get("role_consensus", False))
    perf = role_ctx.get("performance", {}) or {}
    last10 = float(perf.get("last10_net", 0.0) or 0.0)
    last30 = float(perf.get("last30_net", 0.0) or 0.0)
    quality_score = float(quality_score or 0.0)

    if role == "shadow_advisor":
        return False, "shadow advisor contribution only"
    if role == "selector":
        if bool(_s.get_val("selector_independent_open_enabled", False)):
            return True, "selector independent opens enabled"
        return False, "selector contribution only"
    if role == "booster":
        if consensus and (last30 > 0 or quality_score >= float(_s.get_val("role_positive_engine_quality", 68.0) or 68.0)):
            return True, "booster consensus open"
        return False, "booster contribution only"
    if role == "confirmer":
        if consensus:
            return True, "confirmer consensus open"
        if quality_score >= float(_s.get_val("role_confirmer_solo_quality", 78.0) or 78.0):
            return True, "confirmer high-quality solo open"
        return False, "weak solo confirmer contribution only"
    if role == "specialist":
        if quality_score >= float(_s.get_val("role_specialist_quality", 72.0) or 72.0):
            return True, "specialist threshold met"
        return False, "specialist threshold not met"
    if last10 < 0 and last30 < 0:
        return True, "hunter allowed with negative-window priority reduction"
    return True, "hunter open allowed"


def _role_can_contribute_below_open_votes(engine_name: str) -> bool:
    role = _engine_role(engine_name)
    return role in {"confirmer", "selector", "booster", "specialist", "shadow_advisor"}


def _role_contribution_min_votes(engine_name: str, default_min_votes: float | None = None) -> float:
    role = _engine_role(engine_name)
    role_key = f"role_{role}_contribution_min_votes"
    base = float(
        _s.get_val(
            role_key,
            _s.get_val("role_contribution_min_votes", default_min_votes if default_min_votes is not None else 2.0),
        )
        or 0.0
    )
    if default_min_votes is not None:
        return min(float(default_min_votes), base)
    return base


_NOVO_TIER_RANK = {
    "COLLECTING": 0,
    "WEAK": 1,
    "GOOD": 2,
    "STRONG": 3,
    "ELITE": 4,
}


def _csv_setting_values(value: str | None) -> set[str]:
    return {
        str(part or "").strip().lower()
        for part in str(value or "").split(",")
        if str(part or "").strip()
    }


def _novo_tier_rank(tier: str | None) -> int:
    return int(_NOVO_TIER_RANK.get(str(tier or "").strip().upper(), 0))


def _novo_supports_direction(novo_result: dict | None, direction: str, min_tier: str) -> tuple[bool, str]:
    if not isinstance(novo_result, dict):
        return False, ""
    tier = str(novo_result.get("quality_tier", "COLLECTING") or "COLLECTING").upper()
    if _novo_tier_rank(tier) < _novo_tier_rank(min_tier):
        return False, tier

    wanted = "LONG" if str(direction or "").upper() == "LONG" else "SHORT"
    aligned = 0
    conflicted = 0
    for module in (novo_result.get("modules") or {}).values():
        if not isinstance(module, dict):
            continue
        bias = str(module.get("bias", "") or "").upper()
        if bias == wanted:
            aligned += 1
        elif bias in {"LONG", "SHORT"}:
            conflicted += 1
    if aligned > 0 and aligned >= conflicted:
        return True, tier
    return False, tier


def _evaluate_vote_rescue(
    *,
    engine_name: str,
    coin: str,
    direction: str,
    votes: float,
    min_votes: float,
    quality_snapshot: dict,
    role_ctx: dict,
    admission_confidence: float,
    novo_result: dict | None = None,
) -> dict:
    gap = max(float(min_votes or 0.0) - float(votes or 0.0), 0.0)
    base = {
        "allowed": False,
        "reason": "LOW_VOTE_FLOOR",
        "engine": str(engine_name or ""),
        "symbol": str(coin or "").upper(),
        "side": str(direction or "").upper(),
        "votes": float(votes or 0.0),
        "min_votes": float(min_votes or 0.0),
        "vote_gap": round(gap, 4),
        "quality": float((quality_snapshot or {}).get("quality_score", 0.0) or 0.0),
        "admission_confidence": float(admission_confidence or 0.0),
        "support_source": "",
        "novo_tier": "",
    }
    if not bool(_s.get_val("vote_rescue_enabled", False)):
        return base
    if str(direction or "").upper() == "HOLD":
        return {**base, "reason": "LOW_VOTE_FLOOR"}

    max_gap = max(float(_s.get_val("vote_rescue_max_vote_gap", 1.0) or 1.0), 0.0)
    if gap <= 0 or gap > max_gap or gap > 1.0:
        return {**base, "reason": "VOTE_RESCUE_GAP_TOO_LARGE"}

    role = str((role_ctx or {}).get("engine_role", _engine_role(engine_name)) or "hunter").lower()
    allowed_roles = _csv_setting_values(
        _s.get_val("vote_rescue_live_opener_roles", "hunter,specialist,confirmer,booster")
    )
    if role not in allowed_roles or role in {"selector", "shadow_advisor"}:
        return {**base, "reason": "VOTE_RESCUE_ROLE_NOT_OPENER"}

    role_open_allowed, role_reason = _role_independent_open_allowed(role_ctx or {}, base["quality"])
    if not role_open_allowed:
        return {**base, "reason": "VOTE_RESCUE_ROLE_NOT_OPENER", "role_reason": role_reason}

    min_quality = float(_s.get_val("vote_rescue_min_quality", 62.0) or 62.0)
    if base["quality"] < min_quality:
        return {**base, "reason": "VOTE_RESCUE_LOW_QUALITY"}

    min_adm = float(_s.get_val("vote_rescue_min_admission_confidence", 2.0) or 2.0)
    if base["admission_confidence"] < min_adm:
        return {**base, "reason": "VOTE_RESCUE_LOW_CONFIDENCE"}

    support_sources: list[str] = []
    if int((role_ctx or {}).get("consensus_count", 0) or 0) >= 2:
        support_sources.append("consensus_count")
    if float((role_ctx or {}).get("selector_score", 0.0) or 0.0) > 0:
        support_sources.append("selector_support")
    if bool((role_ctx or {}).get("role_consensus", False)):
        support_sources.append("role_consensus")
    novo_ok, novo_tier = _novo_supports_direction(
        novo_result,
        direction,
        str(_s.get_val("vote_rescue_novo_min_tier", "GOOD") or "GOOD"),
    )
    if novo_tier:
        base["novo_tier"] = novo_tier
    if novo_ok:
        support_sources.append(f"novo_{novo_tier.lower()}")

    if bool(_s.get_val("vote_rescue_require_support", True)) and not support_sources:
        return {**base, "reason": "VOTE_RESCUE_NO_SUPPORT", "novo_tier": base["novo_tier"]}

    return {
        **base,
        "allowed": True,
        "reason": "near_floor_supported",
        "support_source": ",".join(support_sources) if support_sources else "support_not_required",
        "novo_tier": base["novo_tier"],
    }


def _role_registry_metadata(role_ctx: dict, engine_name: str, now_ts: float | None = None) -> dict:
    now_ts = float(now_ts or time.time())
    role = str(role_ctx.get("engine_role", "hunter") or "hunter")
    role_contributions = {
        str(engine_name or "").upper(): {
            "role": role,
            "weight": float(role_ctx.get("role_weight", 1.0) or 1.0),
            "position_strength": float(role_ctx.get("position_strength", 0.0) or 0.0),
            "ts": now_ts,
        }
    }
    return {
        "engine_role": role,
        "contributor_roles": list(role_ctx.get("contributor_roles", [role]) or [role]),
        "role_contributions": role_contributions,
        "confirming_engines": [
            e for e in list(role_ctx.get("consensus_engines", []) or [])
            if str(e or "").upper() != str(engine_name or "").upper()
        ],
        "consensus_count": int(role_ctx.get("consensus_count", 1) or 1),
        "selector_score": float(role_ctx.get("selector_score", 0.0) or 0.0),
        "booster_score": float(role_ctx.get("booster_score", 0.0) or 0.0),
        "position_strength": float(role_ctx.get("position_strength", 0.0) or 0.0),
        "fee_pressure": float(role_ctx.get("fee_pressure", 0.0) or 0.0),
        "duplicate_entry_rate": float(role_ctx.get("duplicate_rate", 0.0) or 0.0),
        "role_consensus": bool(role_ctx.get("role_consensus", False)),
        "trade_class": str(role_ctx.get("trade_class", "") or ""),
        "preferred_exit_style": str(role_ctx.get("preferred_exit_style", "") or ""),
        "last_signal_ts": now_ts,
    }


def _within_duplicate_merge_window(existing: dict | None, now_ts: float | None = None) -> bool:
    if not existing:
        return False
    now_ts = float(now_ts or time.time())
    window = max(float(_s.get_val("duplicate_merge_window_sec", 90) or 90), 1.0)
    last_ts = 0.0
    for key in ("last_signal_ts", "reserved_at", "opened_ts"):
        try:
            last_ts = float((existing or {}).get(key, 0.0) or 0.0)
        except Exception:
            last_ts = 0.0
        if last_ts > 0:
            break
    return bool(last_ts > 0 and (now_ts - last_ts) <= window)


def _effective_global_slot_limit(
    balance: float,
    capital_profile=None,
    *,
    candidate_quality: float = 0.0,
    intent=None,
) -> tuple[int, dict]:
    def _norm_slot_mode(value: str | None) -> str:
        raw = str(value or "fixed").strip().lower()
        if raw in {"fixed", "auto"}:
            return raw
        return "fixed"

    def _unlimited_global_cap(balance_usdt: float, profile) -> int:
        """SMART dynamic cap: deployable/min_slot with balance-tier ceilings (never infinite)."""
        fixed_fallback = max(int(_s.get_val("max_open_positions", 10) or 10), 1)
        if profile is None:
            return fixed_fallback
        deployable = float(getattr(profile, "deployment_total_deployable_usdt", 0.0) or 0.0)
        if deployable <= 0:
            return fixed_fallback
        try:
            from settings_store import get_effective_min_order_usdt as _min_order
            min_order = float(_min_order(balance_usdt) or 0.0)
        except Exception:
            min_order = 0.0
        dyn_min = float(_s.get_val("dynamic_target_min_usdt", 50.0) or 50.0)
        min_slot = _micro_sizing.deployment_slot_floor_usdt(
            balance_usdt,
            settings=_s,
            account_floor=min_order,
            target_min=dyn_min,
        )
        slots = int(deployable // min_slot) if min_slot > 0 else fixed_fallback
        micro_ceiling = _micro_sizing.deployment_position_ceiling(balance_usdt, settings=_s)
        if micro_ceiling is None:
            slots = max(slots, 4)
        # Balance-tier ceiling to prevent micro-slot spam.
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

    global_slot_mode = _norm_slot_mode(_s.get_val("global_slot_mode", "fixed"))
    fixed_global = max(int(_s.get_val("max_open_positions", 10) or 10), 1)
    deployment_max = int(getattr(capital_profile, "deployment_max_positions", 0) or 0)
    if global_slot_mode == "fixed":
        global_max = fixed_global
    elif global_slot_mode == "auto":
        global_max = deployment_max if deployment_max > 0 else fixed_global
    else:
        global_max = fixed_global

    base_limit = global_max
    policy_limit = base_limit
    policy_mode = "Adaptive"
    try:
        from exposure_tracker import get_exposure_policy as _get_exposure_policy

        policy = _get_exposure_policy(balance, capital_profile=capital_profile, candidate_quality=candidate_quality)
        # Policy may recommend fewer slots for safety; it must not raise above the global slot cap.
        policy_raw = int(policy.max_positions or base_limit)
        policy_limit = max(min(policy_raw, base_limit), 1)
        policy_mode = str(policy.mode or policy_mode)
    except Exception:
        policy = None
    intent_mult = max(float(getattr(intent, "max_positions_multiplier", 1.0) or 1.0), 0.0)
    intent_limit = max(int(base_limit * intent_mult), 0) or base_limit
    effective_limit = min(max(base_limit, 1), max(policy_limit, 1), max(intent_limit, 1))
    return effective_limit, {
        "global_slot_mode": global_slot_mode,
        "global_max": global_max,
        "fixed_global": fixed_global,
        "deployment_max": deployment_max,
        "base_limit": base_limit,
        "policy_limit": policy_limit,
        "intent_limit": intent_limit,
        "policy_mode": policy_mode,
        "policy": policy,
    }


def get_effective_global_slot_limit(
    balance: float,
    *,
    capital_profile=None,
    candidate_quality: float = 0.0,
    intent=None,
) -> tuple[int, dict]:
    """Public wrapper for diagnostics/tests (hot, read-only)."""
    return _effective_global_slot_limit(
        balance,
        capital_profile=capital_profile,
        candidate_quality=candidate_quality,
        intent=intent,
    )


def get_effective_engine_cap(
    engine_name: str,
    *,
    balance: float,
    capital_profile=None,
) -> int:
    """Mode-aware engine cap (hot-adjustable, affects only NEW admissions)."""
    name = str(engine_name or "").upper().strip()
    if not name:
        return 1

    if _s.get_engine_mode(name) == "probation":
        return 1

    # Global slot cap (used to bound "inherit/unlimited" per-engine behavior).
    global_cap, _meta = get_effective_global_slot_limit(balance, capital_profile=capital_profile)
    global_cap = max(int(global_cap or 0), 1)

    _raw_max = _s.get_val(f"engine_{name.lower()}_max_pos", 3)
    try:
        max_pos_setting = int(_raw_max) if _raw_max is not None else 3
    except Exception:
        max_pos_setting = 3
    raw_mode = str(
        _s.get_val(f"engine_{name.lower()}_slot_mode", "") or _s.get_val("engine_slot_mode", "fixed")
    ).strip().lower()

    # Primary semantics come from max_pos:
    #   >0 explicit cap, 0 inherit, <0 unlimited (still bounded by global).
    if max_pos_setting > 0:
        return max(min(max_pos_setting, global_cap), 1)
    if max_pos_setting == 0:
        mode = "inherit"
    elif max_pos_setting < 0:
        mode = "unlimited"
    else:
        mode = raw_mode if raw_mode in {"fixed", "inherit", "unlimited"} else "fixed"

    # Fixed: use the configured cap (but never exceed global).
    if mode == "fixed":
        cap = max(max_pos_setting, 1)
        # Selector must not be stuck at 1/1 when the portfolio can support multiple quality picks.
        if name == "HACHIMAN":
            if global_cap >= 4:
                cap = max(cap, 2)
            if global_cap >= 9:
                cap = max(cap, 3)
        return max(min(cap, global_cap), 1)

    # Inherit/unlimited: allow multiple positions, but still bounded by global and a share heuristic.
    share = 0.35 if mode == "inherit" else 0.60
    cap = int(round(global_cap * share))
    cap = max(cap, 1)

    # Special handling: selector should not be stuck at 1/1 when slots are available.
    if name == "HACHIMAN":
        if global_cap >= 4:
            cap = max(cap, 2)
        if global_cap >= 9:
            cap = max(cap, 3)

    # If operator still configured a positive baseline, respect it as a floor in these modes.
    if max_pos_setting > 0:
        cap = max(cap, max_pos_setting)

    return max(min(cap, global_cap), 1)


def _trade_quality_from_open(trade: dict) -> float:
    stored = float(trade.get("quality_score", 0.0) or 0.0)
    if stored > 0:
        return stored
    conf = float(trade.get("confidence_score", 0.0) or 0.0)
    if conf > 0:
        return conf * 18.0
    return float(trade.get("vote_score", 0.0) or 0.0) * 10.0


def _trade_age_mins(trade: dict) -> float:
    opened_at = str(trade.get("opened_at", "") or "").strip()
    if not opened_at:
        return 0.0
    try:
        return max((datetime.now(timezone.utc) - datetime.fromisoformat(opened_at)).total_seconds() / 60.0, 0.0)
    except Exception:
        return 0.0


def _live_registry_view() -> dict:
    try:
        import position_registry as _pr
        current = getattr(_pr, "_registry", None)
        if isinstance(current, dict):
            return current
    except Exception:
        pass
    return _registry


def _is_trade_profit_protected(trade: dict) -> bool:
    entry = float(trade.get("entry", 0.0) or 0.0)
    stop = float(trade.get("stop_loss", 0.0) or 0.0)
    direction = str(trade.get("direction", "") or "").lower()
    if entry <= 0 or stop <= 0:
        return False
    if direction == "long":
        return stop >= entry
    if direction == "short":
        return stop <= entry
    return False


def _is_trade_stagnant(coin: str, trade: dict) -> bool:
    entry = float(trade.get("entry", 0.0) or 0.0)
    if entry <= 0:
        return True
    live_px = float(latest_price(coin) or 0.0)
    if live_px <= 0:
        closes = get_closes(coin)
        live_px = float(closes[-1] if closes else entry)
    band_pct = float(_s.get_val("slot_rotation_stagnation_band_pct", 0.35) or 0.35)
    drift_pct = abs((live_px - entry) / entry) * 100.0 if entry > 0 else 0.0
    return drift_pct <= band_pct


def _slot_rotation_candidate(
    candidate_quality: float,
    balance: float,
    candidate_confidence: float = 0.0,
    capital_profile=None,
    intent=None,
) -> dict | None:
    if not bool(_s.get_val("replace_weakest_enabled", True)):
        return None
    if not bool(_s.get_val("slot_rotation_enabled", True)):
        return None
    # Anti-churn: avoid rapid replacement loops when many candidates arrive at once.
    try:
        cooldown = float(_s.get_val("slot_rotation_cooldown_secs", 240.0) or 240.0)
    except Exception:
        cooldown = 240.0
    now = time.monotonic()
    global _last_slot_rotation_ts
    if cooldown > 0 and _last_slot_rotation_ts and (now - float(_last_slot_rotation_ts)) < cooldown:
        return None
    if candidate_quality <= 0:
        return None
    if str(_s.get_val("exposure_control_mode", "Strict") or "Strict").strip().lower() == "strict":
        return None
    if str(getattr(intent, "mode", "") or "").upper() in {"DEFENSIVE", "STOPPED"}:
        return None

    min_age_mins = float(_s.get_val("slot_rotation_min_age_mins", 45.0) or 45.0)
    quality_gap = float(_s.get_val("slot_rotation_quality_gap", 12.0) or 12.0)
    conf_margin = float(_s.get_val("slot_upgrade_margin_score", 0.85) or 0.85)
    block_if_profit_locked = bool(_s.get_val("replace_block_if_profit_locked", True))
    block_if_roi_above = float(_s.get_val("replace_block_if_roi_pct_above", 2.50) or 2.50)

    # Winrate optimization: make replacements more conservative in strict/adaptive profiles.
    if bool(_s.get_val("winrate_optimization_enabled", False)):
        wr_profile = str(_s.get_val("winrate_profile", "balanced") or "balanced").strip().lower()
        wr_profile = wr_profile if wr_profile in {"balanced", "strict", "adaptive"} else "balanced"
        if wr_profile == "strict":
            min_age_mins *= 1.20
            quality_gap *= 1.35
        elif wr_profile == "adaptive":
            quality_gap *= 1.15

    weakest: dict | None = None

    for coin, trade in _live_registry_view().items():
        engine_name = str(trade.get("engine", "") or "").upper()
        if not engine_name:
            continue
        if bool(trade.get("lane_override_active", False)):
            continue
        age_mins = _trade_age_mins(trade)
        if age_mins < min_age_mins:
            continue
        if block_if_profit_locked and _is_trade_profit_protected(trade):
            continue
        if block_if_roi_above > 0:
            entry = float(trade.get("entry", 0.0) or 0.0)
            lev = max(int(trade.get("leverage", 1) or 1), 1)
            direction = str(trade.get("direction", "") or "").lower()
            if entry > 0 and direction in {"long", "short"}:
                live_px = float(latest_price(coin) or 0.0)
                if live_px <= 0:
                    closes = get_closes(coin)
                    live_px = float(closes[-1] if closes else entry)
                if live_px > 0:
                    move = (live_px - entry) / entry if direction == "long" else (entry - live_px) / entry
                    roi_pct = float(move) * float(lev) * 100.0
                    if roi_pct >= block_if_roi_above:
                        continue
        if not _is_trade_stagnant(coin, trade) and age_mins < min_age_mins * 2.0:
            continue
        trade_quality = _trade_quality_from_open(trade)
        candidate = {
            "coin": coin.upper(),
            "engine": engine_name,
            "quality": round(trade_quality, 3),
            "age_mins": round(age_mins, 1),
            "trade": trade,
        }
        if weakest is None or trade_quality < weakest["quality"] or (
            trade_quality == weakest["quality"] and age_mins > weakest["age_mins"]
        ):
            weakest = candidate

    if weakest is None:
        return None
    # Margin gate: require meaningful confidence improvement (avoid churn on marginal score gaps).
    victim_conf = float(weakest.get("trade", {}).get("confidence_score", 0.0) or 0.0)
    if float(candidate_confidence or 0.0) > 0 and victim_conf > 0 and float(conf_margin or 0.0) > 0:
        required = victim_conf + float(conf_margin)
        if float(candidate_confidence) < required:
            weakest["required_confidence"] = round(required, 3)
            return weakest
    if candidate_quality < weakest["quality"] + quality_gap:
        weakest["required_quality"] = round(weakest["quality"] + quality_gap, 3)
        return weakest
    weakest["required_quality"] = round(weakest["quality"] + quality_gap, 3)
    weakest["replacement_ok"] = True
    return weakest


async def _execute_slot_rotation(
    *,
    rotation: dict,
    new_engine: str,
    new_coin: str,
    new_direction: str,
    candidate_quality: float,
) -> tuple[bool, str]:
    victim_coin = str(rotation.get("coin", "") or "").upper()
    victim_engine = str(rotation.get("engine", "") or "").upper()
    if not victim_coin or not victim_engine:
        return False, "rotation candidate incomplete"
    try:
        from trading_engine import engine as _engine_manager

        target = getattr(_engine_manager, "engines", {}).get(victim_engine)
        if target is None or not hasattr(target, "open_positions") or victim_coin not in target.open_positions():
            return False, f"{victim_coin} no longer managed by {victim_engine}"
        result = await target.close_position(victim_coin, "slot_rotation")
        if not bool(result.get("ok", False)):
            return False, str(result.get("reason") or "slot rotation close failed")
        global _last_slot_rotation_ts
        _last_slot_rotation_ts = time.monotonic()
        logger.info(
            "SLOT ROTATION [%s] %s %s q=%.2f replaced %s [%s] q=%.2f age=%.1fm",
            engine_log_name(new_engine),
            new_coin,
            str(new_direction or "").upper(),
            float(candidate_quality or 0.0),
            victim_coin,
            victim_engine,
            float(rotation.get("quality", 0.0) or 0.0),
            float(rotation.get("age_mins", 0.0) or 0.0),
        )
        try:
            _s.bump_counter("metric_replacements", 1)
        except Exception:
            pass
        _record_decision_trace(
            engine=new_engine,
            coin=new_coin,
            direction=new_direction,
            confidence=float(rotation.get("quality", 0.0) or 0.0),
            raw_score=float(candidate_quality or 0.0),
            decision="replaced_weakest_trade",
            reason=f"replaced {victim_coin} [{victim_engine}]",
            candidate_quality=float(candidate_quality or 0.0),
            notes=f"victim_quality={float(rotation.get('quality', 0.0) or 0.0):.2f}",
            extra={"victim_coin": victim_coin, "victim_engine": victim_engine},
        )
        return True, f"replaced {victim_coin} [{victim_engine}]"
    except Exception as exc:
        logger.warning(
            "SLOT ROTATION FAIL [%s] %s could not replace %s [%s]: %s",
            engine_log_name(new_engine),
            new_coin,
            victim_coin,
            victim_engine,
            exc,
        )
        return False, str(exc)


def _size_mode_base_notional(balance: float, engine_name: str) -> tuple[float, str]:
    """Return the base notional used for NEW trades, respecting operator size mode."""
    bal = max(float(balance or 0.0), 0.0)
    base_notional = float(_s.get_val("position_size_usdt", 100.0) or 100.0)
    balance_pct = float(_s.get_val("position_size_pct", 0.05) or 0.05)
    risk_pct = float(_s.get_val("risk_pct", 1.25) or 1.25)
    leverage = max(int(_s.get_val("leverage_default", 10) or 10), 1)
    balance_notional = bal * balance_pct if bal > 0 else base_notional
    risk_notional = bal * (risk_pct / 100.0) * leverage if bal > 0 else base_notional
    mode = str(_s.get_val("size_mode", "blended") or "blended").strip().lower()
    if mode == "notional":
        raw = base_notional
    elif mode == "balance":
        raw = balance_notional
    elif mode == "risk":
        raw = max(risk_notional, base_notional)
    else:
        if _micro_sizing.is_micro_sizing_active(bal, _s):
            raw = max(base_notional, balance_notional, risk_notional)
        else:
            raw = max(base_notional, balance_notional)
    cap = float(_s.get_val("position_size_cap_usdt", 250.0) or 250.0)
    return round(min(max(raw, 0.0), cap), 2), mode


def _record_decision_trace(
    *,
    engine: str,
    coin: str,
    direction: str,
    confidence: float,
    decision: str,
    reason: str,
    raw_score: float = 0.0,
    balance: float | None = None,
    size_usdt: float = 0.0,
    candidate_quality: float = 0.0,
    regime: str = "",
    slot_state: str = "",
    exposure_state: str = "",
    size_decision: str = "",
    hachiman_influence: float = 0.0,
    selector_influence: float = 0.0,
    notes: str = "",
    extra: dict | None = None,
) -> None:
    try:
        _dtrace.record({
            "symbol": str(coin or "").upper(),
            "side": str(direction or "").upper(),
            "engine": str(engine or "").upper(),
            "raw_score": float(raw_score or 0.0),
            "confidence": float(confidence or 0.0),
            "hachiman_influence": float(hachiman_influence or 0.0),
            "selector_influence": float(selector_influence or 0.0),
            "regime": str(regime or "").upper(),
            "slot_state": str(slot_state or ""),
            "exposure_state": str(exposure_state or ""),
            "size_usdt": float(size_usdt or 0.0),
            "size_decision": str(size_decision or ""),
            "final_decision": str(decision or "").lower(),
            "reasons": str(reason or ""),
            "candidate_quality": float(candidate_quality or 0.0),
            "notes": str(notes or ""),
            "extra": dict(extra or {}),
        })
    except Exception:
        pass


def _novo_candles_from_series(closes=None, highs=None, lows=None, volumes=None, limit: int = 120) -> list[dict]:
    """Build read-only candle dicts for advisory-only Novo analysis."""
    try:
        c = list(closes or [])
        h = list(highs or [])
        l = list(lows or [])
        v = list(volumes or [])
        n = min(len(c), len(h), len(l))
        if n <= 0:
            return []
        start = max(0, n - max(int(limit or 120), 1))
        out: list[dict] = []
        prev_close = None
        for idx in range(start, n):
            close = float(c[idx])
            high = float(h[idx])
            low = float(l[idx])
            open_ = float(prev_close if prev_close is not None else close)
            row = {"open": open_, "high": high, "low": low, "close": close}
            if idx < len(v):
                try:
                    row["volume"] = float(v[idx])
                except Exception:
                    pass
            out.append(row)
            prev_close = close
        return out
    except Exception:
        return []


def _novo_compact_snapshot(novo_result: dict | None) -> dict:
    modules = {}
    try:
        for name in ("market_dna", "market_structure", "bias_4h", "crt", "liquidity_sweep", "session_model"):
            mod = (novo_result or {}).get("modules", {}).get(name, {})
            modules[name] = str(mod.get("status", "UNKNOWN") if isinstance(mod, dict) else "UNKNOWN")
        score = (novo_result or {}).get("quality_score")
        try:
            score = int(score) if score is not None else None
        except Exception:
            score = None
        return {
            "enabled": bool((novo_result or {}).get("enabled", False)),
            "advisory_only": True,
            "quality_score": score,
            "quality_tier": str((novo_result or {}).get("quality_tier", "COLLECTING") or "COLLECTING"),
            "modules": modules,
        }
    except Exception:
        return {
            "enabled": False,
            "advisory_only": True,
            "quality_score": None,
            "quality_tier": "ERROR",
            "modules": {name: "ERROR" for name in ("market_dna", "market_structure", "bias_4h", "crt", "liquidity_sweep", "session_model")},
        }


def _novo_enrichment_compact_snapshot(enrichment_result: dict | None) -> dict:
    try:
        data = dict(enrichment_result or {})
        suggestions_src = data.get("suggestions", {}) if isinstance(data.get("suggestions", {}), dict) else {}
        suggestions = {
            "tp2_multiplier": float(suggestions_src.get("tp2_multiplier", 1.0) or 1.0),
            "trailing_activation_multiplier": float(suggestions_src.get("trailing_activation_multiplier", 1.0) or 1.0),
            "break_even_delay_multiplier": float(suggestions_src.get("break_even_delay_multiplier", 1.0) or 1.0),
            "profit_lock_multiplier": float(suggestions_src.get("profit_lock_multiplier", 1.0) or 1.0),
            "partial_min_notional_multiplier": float(suggestions_src.get("partial_min_notional_multiplier", 1.0) or 1.0),
            "execution_split_preference": str(suggestions_src.get("execution_split_preference", "UNCHANGED") or "UNCHANGED"),
            "size_multiplier": float(suggestions_src.get("size_multiplier", 1.0) or 1.0),
        }
        flags = data.get("enabled_flags", {}) if isinstance(data.get("enabled_flags", {}), dict) else {}
        return {
            "status": str(data.get("status", "COLLECTING") or "COLLECTING"),
            "band": str(data.get("band", "COLLECTING") or "COLLECTING"),
            "advisory_only": True,
            "no_hard_blocks": True,
            "enabled_flags": {str(k): bool(v) for k, v in flags.items()},
            "suggestions": suggestions,
        }
    except Exception:
        return {
            "status": "ERROR",
            "band": "COLLECTING",
            "advisory_only": True,
            "no_hard_blocks": True,
            "enabled_flags": {},
            "suggestions": {
                "tp2_multiplier": 1.0,
                "trailing_activation_multiplier": 1.0,
                "break_even_delay_multiplier": 1.0,
                "profit_lock_multiplier": 1.0,
                "partial_min_notional_multiplier": 1.0,
                "execution_split_preference": "UNCHANGED",
                "size_multiplier": 1.0,
            },
        }


def _novo_candidate_observation_record(
    *,
    coin: str,
    direction: str,
    engine: str,
    signal_confidence: float,
    admission_confidence: float,
    candidate_quality: float,
    novo_result: dict | None,
    novo_compact: dict | None,
    novo_enrichment_compact: dict | None,
    admission_result: str = "PENDING",
    rejection_reason=None,
) -> dict:
    """Build append-only Novo candidate telemetry without trading authority."""
    try:
        score = (novo_result or {}).get("quality_score")
        try:
            score = int(score) if score is not None else None
        except Exception:
            score = None
        modules = {}
        raw_modules = (novo_result or {}).get("modules", {})
        if isinstance(raw_modules, dict):
            for key, value in raw_modules.items():
                if isinstance(value, dict):
                    modules[str(key)] = {
                        "status": str(value.get("status", "UNKNOWN") or "UNKNOWN"),
                        "score": value.get("score"),
                        "bias": value.get("bias"),
                    }
                else:
                    modules[str(key)] = {"status": "UNKNOWN"}
        if not modules:
            compact_modules = (novo_compact or {}).get("modules", {})
            if isinstance(compact_modules, dict):
                modules = {str(k): {"status": str(v or "UNKNOWN")} for k, v in compact_modules.items()}
        return {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "record_type": "novo_candidate_observation",
            "dry_run": True,
            "symbol": str(coin or "").upper(),
            "side": str(direction or "").upper(),
            "engine": str(engine or ""),
            "confidence": float(signal_confidence or 0.0),
            "admission_confidence": float(admission_confidence or 0.0),
            "candidate_quality": float(candidate_quality or 0.0),
            "quality_score": score,
            "quality_tier": str((novo_result or {}).get("quality_tier", "COLLECTING") or "COLLECTING"),
            "novo_modules": modules,
            "novo_enrichment": dict(novo_enrichment_compact or {}),
            "advisory_only": True,
            "no_hard_blocks": True,
            "admission_result": str(admission_result or "PENDING").upper(),
            "rejection_reason": rejection_reason,
        }
    except Exception:
        return {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "record_type": "novo_candidate_observation",
            "dry_run": True,
            "symbol": str(coin or "").upper(),
            "side": str(direction or "").upper(),
            "engine": str(engine or ""),
            "confidence": float(signal_confidence or 0.0),
            "admission_confidence": float(admission_confidence or 0.0),
            "candidate_quality": float(candidate_quality or 0.0),
            "quality_score": None,
            "quality_tier": "COLLECTING",
            "novo_modules": {},
            "novo_enrichment": {},
            "advisory_only": True,
            "no_hard_blocks": True,
            "admission_result": str(admission_result or "PENDING").upper(),
            "rejection_reason": rejection_reason,
        }


def _maybe_append_novo_candidate_observation(
    *,
    coin: str,
    direction: str,
    engine: str,
    signal_confidence: float,
    admission_confidence: float,
    candidate_quality: float,
    novo_result: dict | None,
    novo_compact: dict | None,
    novo_enrichment_compact: dict | None,
) -> bool:
    """Append durable Novo candidate evidence when memory is explicitly enabled."""
    if not bool(_s.get_val("novo_ai_trade_memory_enabled", False)):
        return False
    try:
        record = _novo_candidate_observation_record(
            coin=coin,
            direction=direction,
            engine=engine,
            signal_confidence=signal_confidence,
            admission_confidence=admission_confidence,
            candidate_quality=candidate_quality,
            novo_result=novo_result,
            novo_compact=novo_compact,
            novo_enrichment_compact=novo_enrichment_compact,
            admission_result="PENDING",
            rejection_reason=None,
        )
        _novo_trade_memory.append_memory(record)
        return True
    except Exception as exc:
        logger.debug("[%s] Novo candidate memory append skipped for %s %s: %s", engine, coin, direction, exc)
        return False


def _apply_soft_accept_size(base_size: float, balance: float, mult: float) -> float:
    """Reduce size for soft-accepts, but never below the executable floor."""
    size = round(float(base_size or 0.0), 2)
    if mult >= 1.0:
        return size
    result = _micro_sizing.floor_aware_reduce(
        size,
        float(mult or 1.0),
        balance_usdt=balance,
        leverage=max(int(_s.get_val("leverage_default", 10) or 10), 1),
        settings=_s,
        reason="soft_accept",
    )
    return round(float(result.final_size_usdt), 2)


def _risk_sized_notional_from_stop(
    *,
    engine_name: str,
    coin: str,
    side: str,
    entry_px: float,
    stop_px: float,
    balance: float,
    current_size_usdt: float,
    base_size_usdt: float,
    min_exec_usdt: float,
    max_size_usdt: float | None = None,
    max_size_source: str = "",
) -> dict:
    """Derive final notional from risk budget and actual stop distance."""
    if not bool(_s.get_val("risk_sizing_enabled", True)):
        return {
            "skip": False,
            "size_usdt": round(float(current_size_usdt or 0.0), 2),
            "reason": "risk_sizing_disabled",
            "meta": {"risk_sizing_enabled": False},
        }

    entry = float(entry_px or 0.0)
    stop = float(stop_px or 0.0)
    balance_val = max(float(balance or 0.0), 0.0)
    current_size = max(float(current_size_usdt or 0.0), 0.0)
    base_size = max(float(base_size_usdt or 0.0), float(min_exec_usdt or 0.0), 1e-9)
    min_exec = max(float(min_exec_usdt or 0.0), 0.0)
    if entry <= 0 or stop <= 0 or balance_val <= 0:
        return {
            "skip": True,
            "size_usdt": 0.0,
            "reason": "risk_sizing_invalid_inputs",
            "meta": {
                "risk_sizing_enabled": True,
                "entry": entry,
                "stop": stop,
                "balance": balance_val,
            },
        }

    stop_dist_pct = abs(entry - stop) / entry * 100.0
    if stop_dist_pct <= 0:
        return {
            "skip": True,
            "size_usdt": 0.0,
            "reason": "risk_sizing_zero_stop_distance",
            "meta": {"risk_sizing_enabled": True, "sl_dist_pct": stop_dist_pct},
        }

    risk_pct = max(float(_s.get_val("risk_pct", 1.0) or 1.0), 0.0)
    risk_multiplier = max(current_size / base_size, 0.0)
    risk_amount = balance_val * (risk_pct / 100.0) * risk_multiplier
    raw_size = risk_amount / (stop_dist_pct / 100.0)
    cap = float(max_size_usdt) if max_size_usdt is not None and float(max_size_usdt or 0.0) > 0 else 0.0
    capped_size = min(raw_size, cap) if cap > 0 else raw_size
    final_size = round(max(capped_size, 0.0), 2)
    policy = str(_s.get_val("risk_sizing_min_exec_policy", "skip") or "skip").strip().lower()
    meta = {
        "risk_sizing_enabled": True,
        "risk_pct": round(risk_pct, 6),
        "risk_multiplier": round(risk_multiplier, 6),
        "risk_amount_usdt": round(risk_amount, 6),
        "sl_dist_pct": round(stop_dist_pct, 6),
        "raw_size_usdt": round(raw_size, 6),
        "size_before_risk": round(current_size, 6),
        "base_size_usdt": round(base_size, 6),
        "min_exec_usdt": round(min_exec, 6),
        "max_size_usdt": round(cap, 6),
        "cap_applied": bool(cap > 0 and raw_size > cap),
        "cap_source": str(max_size_source or "max_size_usdt" if cap > 0 else ""),
        "policy": policy,
    }
    if final_size + 0.01 < min_exec:
        return {
            "skip": True,
            "size_usdt": final_size,
            "reason": (
                f"risk_sizing_below_min size=${final_size:.2f} < min=${min_exec:.2f} "
                f"risk=${risk_amount:.4f} sl={stop_dist_pct:.2f}%"
            ),
            "meta": meta,
        }
    return {
        "skip": False,
        "size_usdt": final_size,
        "reason": (
            f"risk_sized ${current_size:.2f}->${final_size:.2f} "
            f"risk=${risk_amount:.4f} sl={stop_dist_pct:.2f}%"
        ),
        "meta": meta,
    }


async def _adaptive_capital_allocation(
    *,
    coin: str,
    balance: float,
    size_usdt: float,
    entry_px: float,
    stop_px: float,
    leverage: int,
    risk_meta: dict | None = None,
    participation_cap_usdt: float | None = None,
    okx=None,
    side: str = "",
) -> dict:
    """Apply funded-capacity allocation after strategy stop/risk sizing."""
    try:
        from capital_allocator import allocate_entry
        from capital_reservations import book as _reservation_book
        from instrument_registry import registry as _instrument_registry

        instrument = _instrument_registry.get(coin, require_fresh=True)
        if instrument is None:
            return {
                "skip": True,
                "size_usdt": 0.0,
                "reason": "missing_or_stale_instrument_metadata",
                "meta": {
                    "capital_allocator_enabled": True,
                    "capital_allocator_allowed": False,
                    "allocator_rejection_class": "stale_metadata",
                },
            }

        requested_leverage = int(leverage or _s.get_val("leverage_default", 10) or 10)
        executable_leverage = requested_leverage
        leverage_truth = {
            "requested_leverage": requested_leverage,
            "exchange_configured_leverage": requested_leverage,
            "leverage_truth_status": "PAPER_LOCAL",
            "leverage_truth_timestamp": "",
            "leverage_truth_source": "paper",
            "leverage_truth_mismatch": False,
        }
        if os.getenv("PAPER_TRADING", "true").strip().lower() != "true" and okx is not None:
            reader = getattr(okx, "get_authoritative_leverage", None)
            if callable(reader):
                try:
                    leverage_truth = dict(await reader(
                        coin,
                        "long" if str(side or (risk_meta or {}).get("side", "")).lower() in {"buy", "long"} else "short",
                        requested_leverage=requested_leverage,
                        margin_mode="isolated",
                    ) or {})
                    executable_leverage = int(float(leverage_truth.get("exchange_configured_leverage", 0) or 0))
                    if executable_leverage <= 0:
                        raise RuntimeError("missing_exchange_configured_leverage")
                except Exception as exc:
                    return {
                        "skip": True,
                        "size_usdt": 0.0,
                        "reason": f"leverage_truth_unavailable:{exc}",
                        "meta": {
                            "capital_allocator_enabled": True,
                            "capital_allocator_allowed": False,
                            "allocator_rejection_class": "exchange_configuration_failure",
                            "requested_leverage": requested_leverage,
                        },
                    }
            elif okx.__class__.__module__ == "okx_client":
                return {
                    "skip": True,
                    "size_usdt": 0.0,
                    "reason": "leverage_truth_unavailable:reader_missing",
                    "meta": {
                        "capital_allocator_enabled": True,
                        "capital_allocator_allowed": False,
                        "allocator_rejection_class": "exchange_configuration_failure",
                        "requested_leverage": requested_leverage,
                    },
                }
        decision = allocate_entry(
            balance_usdt=float(balance or 0.0),
            requested_size_usdt=float(size_usdt or 0.0),
            entry_price=float(entry_px or 0.0),
            stop_price=float(stop_px or 0.0),
            leverage=executable_leverage,
            open_positions={
                key: value for key, value in _registry.items()
                if str(key).upper() != str(coin).upper()
                and not bool(value.get("pending_reservation", False))
            },
            metadata=dict(risk_meta or {}),
            instrument_metadata=instrument,
            currently_reserved_margin_usdt=_reservation_book.total_margin(),
            deployment_mode=str(_s.get_val("deployment_mode", "Guarded") or "Guarded"),
            contract_aware_minimum=bool(_s.get_val("contract_aware_execution_floor_enabled", False)),
            participation_cap_usdt=participation_cap_usdt,
        )
        decision_meta = decision.metadata()
        decision_meta.update(leverage_truth)
        return {
            "skip": not bool(decision.allowed),
            "size_usdt": float(decision.final_size_usdt or 0.0),
            "reason": decision.reason,
            "meta": decision_meta,
        }
    except Exception as exc:
        logger.error("Adaptive capital allocator failed closed: %s", exc)
        return {
            "skip": True,
            "size_usdt": 0.0,
            "reason": "allocator_exception_fail_closed",
            "meta": {
                "capital_allocator_enabled": True,
                "capital_allocator_allowed": False,
                "capital_allocator_reason": f"exception:{exc}",
                "allocator_rejection_class": "exchange_configuration_failure",
            },
        }


def _final_candidate_economics(
    *,
    coin: str,
    engine_name: str,
    direction: str,
    entry_px: float,
    stop_px: float,
    tp2_px: float,
    desired_size_usdt: float,
    final_size_usdt: float,
    balance_usdt: float,
    leverage: int,
    ticker: dict,
) -> dict:
    """Recompute fees, execution costs, expectancy, and exit feasibility at final size."""
    from candidate_economics import decompose_floor, evaluate_final_size, resolve_exit_management_mode
    from capital_efficiency import build_snapshot
    from instrument_registry import registry as _instrument_registry
    from live_fee_schedule import current as _current_fee

    instrument = _instrument_registry.require(coin)
    entry = float(entry_px or 0.0)
    stop_move_pct = abs(entry - float(stop_px or 0.0)) / entry * 100.0 if entry > 0 else 0.0
    target_move_pct = abs(float(tp2_px or 0.0) - entry) / entry * 100.0 if entry > 0 else 0.0
    bid = float((ticker or {}).get("bidPx", 0.0) or 0.0)
    ask = float((ticker or {}).get("askPx", 0.0) or 0.0)
    spread_pct = ((ask - bid) / entry * 100.0) if entry > 0 and ask > 0 and bid > 0 else 0.0
    fee_rate, fee_schedule = _current_fee(execution_type="market")
    min_cost_multiple = _micro_sizing.net_expectancy_cost_multipliers(balance_usdt, _s)[0]
    floor = decompose_floor(
        instrument=instrument,
        price=entry,
        configured_candidate_floor_usdt=float(_s.get_val("adaptive_min_economic_notional_usdt", 1.0) or 1.0),
        fee_rate=fee_rate,
        target_move_pct=target_move_pct,
        spread_pct=spread_pct,
        slippage_pct=float(_s.get_val("net_expectancy_default_slippage_pct", 0.03) or 0.03),
        minimum_net_profit_usdt=float(_s.get_val("min_net_take_profit_usdt", 0.16) or 0.16),
        minimum_profit_cost_multiple=min_cost_multiple,
        exit_legs=3,
    )
    result = evaluate_final_size(
        desired_size_usdt=float(desired_size_usdt or final_size_usdt or 0.0),
        final_size_usdt=float(final_size_usdt or 0.0),
        target_move_pct=target_move_pct,
        stop_move_pct=stop_move_pct,
        fee_rate=fee_rate,
        spread_pct=spread_pct,
        slippage_pct=float(_s.get_val("net_expectancy_default_slippage_pct", 0.03) or 0.03),
        funding_pct=0.0,
        engine=engine_name,
        direction=direction,
        regime=str(_regime.get_regime() if _regime else ""),
        history=_profitability.load_closed_history(),
        min_cost_multiple=min_cost_multiple,
        instrument=instrument,
        entry_price=entry,
        exit_legs=3,
    )
    partial_viable, exit_mode, exit_reason = resolve_exit_management_mode(
        full_exit_viable=bool(result.get("full_exit_viable")),
        quantity_partial_viable=bool(result.get("partial_exits_viable")),
        final_notional_usdt=float(final_size_usdt or 0.0),
        partial_exit_floor_usdt=float(floor.partial_exit_floor_usdt or 0.0),
    )
    result["partial_exits_viable"] = partial_viable
    result["exit_management_mode"] = exit_mode
    result["exit_management_reason"] = exit_reason
    result["floor_decomposition"] = floor.to_dict()
    result["fee_schedule"] = {
        "maker": fee_schedule.maker,
        "taker": fee_schedule.taker,
        "fetched_at": fee_schedule.fetched_at,
        "source": fee_schedule.source,
        "expected_execution_type": "market",
    }
    final_economic = dict(result.get("final") or {})
    result["capital_efficiency"] = build_snapshot(
        equity_usdt=float(balance_usdt or 0.0),
        desired_notional_usdt=float(desired_size_usdt or 0.0),
        final_notional_usdt=float(final_size_usdt or 0.0),
        stop_dist_pct=stop_move_pct,
        leverage=int(leverage or 1),
        expected_gross_usdt=float(final_economic.get("expected_gross_usdt", 0.0) or 0.0),
        expected_cost_usdt=float(final_economic.get("estimated_cost_usdt", 0.0) or 0.0),
        expected_net_usdt=float(final_economic.get("expected_net_usdt", 0.0) or 0.0),
        binding_constraint="final_candidate_economics",
        exit_management_mode=exit_mode,
        position_cap_pct_equity=float(_s.get_val("position_size_cap_pct_balance", 100.0) or 100.0),
    )
    return result


async def _execute_central_entry(
    *,
    okx,
    engine_name: str,
    coin: str,
    side: str,
    size_usdt: float,
    entry_px: float,
    stop_px: float,
    take_profit: float,
    tp1_price: float = 0.0,
    tp2_price: float = 0.0,
    atr_value: float = 0.0,
    leverage: int,
    allocator_meta: dict,
    execution_plan=None,
    confidence: float = 0.0,
    score: float = 0.0,
    tp_owner: str = "",
    tp_mode_at_entry: str = "",
) -> dict | None:
    """Reserve capital and submit through the sole execution mutation gateway."""
    from capital_reservations import book as reservation_book
    from execution_router import ExecutionRequest, new_intent_id, router_for
    from instrument_registry import registry as instrument_registry
    from opportunity_queue import Opportunity, candidate_id, queue as opportunity_queue

    now = time.time()
    direction = "long" if side == "buy" else "short"
    cid = candidate_id(coin, direction, engine_name, now)
    intent_id = new_intent_id(cid, engine_name, coin, direction)
    instrument = instrument_registry.require(coin)
    requested_leverage = int(leverage or 1)
    exchange_configured_leverage = requested_leverage
    leverage_truth_status = "PAPER_LOCAL"
    leverage_truth_timestamp = ""
    leverage_truth_source = "paper"
    leverage_truth_mismatch = False
    requested_vs_configured_mismatch = False
    configured_vs_effective_mismatch = False
    if os.getenv("PAPER_TRADING", "true").strip().lower() != "true":
        truth_reader = getattr(okx, "get_authoritative_leverage", None)
        if callable(truth_reader):
            try:
                truth = dict(await truth_reader(
                    coin,
                    direction,
                    requested_leverage=requested_leverage,
                    margin_mode="isolated",
                ) or {})
                exchange_configured_leverage = int(float(
                    truth.get("exchange_configured_leverage", 0) or 0
                ))
                if exchange_configured_leverage <= 0:
                    raise RuntimeError("missing_exchange_configured_leverage")
                leverage_truth_status = str(truth.get("status", "EXCHANGE_CONFIGURED") or "EXCHANGE_CONFIGURED")
                leverage_truth_timestamp = str(truth.get("timestamp", "") or "")
                leverage_truth_source = str(truth.get("source", "") or "")
                requested_vs_configured_mismatch = bool(
                    truth.get("requested_vs_configured_mismatch", exchange_configured_leverage != requested_leverage)
                )
                configured_vs_effective_mismatch = bool(truth.get("configured_vs_effective_mismatch", False))
                leverage_truth_mismatch = requested_vs_configured_mismatch
            except Exception as exc:
                okx._set_last_order_skip_reason(f"leverage_truth_unavailable:{exc}")
                return None
        elif okx.__class__.__module__ == "okx_client":
            okx._set_last_order_skip_reason("leverage_truth_unavailable:reader_missing")
            return None
        else:
            leverage_truth_status = "TEST_STUB"
            leverage_truth_source = "test"
    executable_leverage = max(int(exchange_configured_leverage), 1)
    contracts = float(allocator_meta.get("allocator_approved_contracts", 0.0) or 0.0)
    if contracts <= 0:
        contracts = instrument.notional_to_contracts(float(size_usdt), float(entry_px))
    approved_notional = instrument.contracts_to_notional(contracts, float(entry_px))
    estimated_margin = instrument.required_margin(contracts, float(entry_px), executable_leverage)
    approved_risk = float(allocator_meta.get("allocator_approved_risk", 0.0) or 0.0)
    deployable = float(allocator_meta.get("allocator_deployable_margin", 0.0) or 0.0)
    deployed = float(allocator_meta.get("allocator_currently_deployed_margin", 0.0) or 0.0)
    max_reservable = max(deployable - deployed, 0.0)
    reserved, reserve_reason, reservation_id = await reservation_book.reserve(
        intent_id=intent_id,
        candidate_id=cid,
        margin_usdt=estimated_margin,
        risk_usdt=approved_risk,
        max_total_margin_usdt=max_reservable,
        ttl_secs=float(_s.get_val("capital_reservation_ttl_secs", 120.0) or 120.0),
        metadata={
            "coin": coin, "engine": engine_name, "direction": direction,
            "requested_leverage": requested_leverage,
            "exchange_configured_leverage": executable_leverage,
            "leverage_truth_status": leverage_truth_status,
            "leverage_truth_timestamp": leverage_truth_timestamp,
            "requested_vs_configured_mismatch": requested_vs_configured_mismatch,
            "configured_vs_effective_mismatch": configured_vs_effective_mismatch,
        },
    )
    if not reserved:
        expiry = now + float(_s.get_val("opportunity_queue_ttl_secs", 300.0) or 300.0)
        opportunity_queue.enqueue(Opportunity(
            candidate_id=cid,
            pair=coin.upper(),
            direction=direction,
            engine=engine_name,
            signal_timestamp=now,
            expiry_timestamp=expiry,
            original_score=float(score),
            current_score=float(score),
            confidence=float(confidence),
            intended_entry=float(entry_px),
            stop=float(stop_px),
            tp_structure={"tp2": float(take_profit)},
            risk_requirement=approved_risk,
            estimated_margin_requirement=estimated_margin,
            instrument_eligibility="eligible",
            deployment_mode=str(_s.get_val("deployment_mode", "Guarded") or "Guarded"),
            correlation_group="",
            queued_reason=reserve_reason,
            market_geometry_timestamp=now,
            payload={
                "intent_id": intent_id,
                "approved_contracts": contracts,
                "approved_notional_usdt": approved_notional,
                "leverage": executable_leverage,
                "entry_price": float(entry_px),
                "stop_price": float(stop_px),
                "tp1_price": float(tp1_price),
                "tp2_price": float(tp2_price),
                "exchange_take_profit": float(take_profit),
                "tp_owner": str(tp_owner or ""),
                "tp_mode_at_entry": str(tp_mode_at_entry or ""),
                "local_tp1_enabled": str(tp_owner or "").upper() == "LOCAL_STAGED",
                "local_tp2_enabled": str(tp_owner or "").upper() == "LOCAL_STAGED",
                "partials_enabled_at_entry": bool(_s.get_val("partial_exit_enabled", True)),
                "tp_owner_timestamp": datetime.now(timezone.utc).isoformat() if tp_owner else "",
                "atr_value": float(atr_value),
                "order_type": str(getattr(execution_plan, "mode", "market") or "market"),
            },
        ))
        okx._set_last_order_skip_reason(f"queued:{reserve_reason}")
        return None
    mode = str(getattr(execution_plan, "mode", "market") or "market").lower()
    if mode == "blocked":
        okx._set_last_order_skip_reason(
            f"execution_incompatibility:{str(getattr(execution_plan, 'blocked_reason', '') or 'uneconomic_execution')}"
        )
        await reservation_book.release(reservation_id)
        return None
    order_type = mode if mode in {"limit", "staged"} else "market"
    order_price = float(entry_px)
    if order_type == "limit":
        chunks = list(getattr(execution_plan, "chunks", []) or [])
        offset = float(chunks[0].get("price_offset_pct", 0.0) or 0.0) if chunks else 0.0
        raw_price = order_price * (1.0 + offset)
        order_price = instrument.round_price(raw_price, side=direction, purpose="entry")
    _instrument_min_notional = 0.0
    try:
        _min_sz = float(getattr(instrument, "min_sz", 0.0) or 0.0)
        _contract_notional = getattr(instrument, "contract_notional", None)
        if _min_sz > 0.0 and callable(_contract_notional):
            _instrument_min_notional = _min_sz * float(_contract_notional(float(entry_px)))
    except Exception:
        _instrument_min_notional = 0.0

    request = ExecutionRequest(
        intent_id=intent_id,
        candidate_id=cid,
        reservation_id=reservation_id,
        mode=str(_s.get_val("deployment_mode", "Guarded") or "Guarded"),
        engine=engine_name,
        coin=coin,
        direction=direction,
        requested_contracts=contracts,
        approved_contracts=contracts,
        approved_notional_usdt=approved_notional,
        order_type=order_type,
        price=order_price,
        stop_price=instrument.round_price(stop_px, side=direction, purpose="stop"),
        take_profit=instrument.round_price(take_profit, side=direction, purpose="take_profit") if take_profit > 0 else 0.0,
        margin_mode="isolated",
        leverage=executable_leverage,
        requested_leverage=requested_leverage,
        exchange_configured_leverage=executable_leverage,
        leverage_truth_status=leverage_truth_status,
        leverage_truth_timestamp=leverage_truth_timestamp,
        requested_vs_configured_mismatch=requested_vs_configured_mismatch,
        configured_vs_effective_mismatch=configured_vs_effective_mismatch,
        timeout_secs=float(_s.get_val("execution_order_timeout_secs", 15.0) or 15.0),
        retry_count=int(_s.get_val("execution_retry_count", 1) or 1),
        fallback_mode=str(getattr(execution_plan, "fallback_mode", "market") or "market"),
        metadata={
            **dict(allocator_meta or {}),
            "contract_aware_minimum": bool(_s.get_val("contract_aware_execution_floor_enabled", False)),
            "instrument_min_notional_usdt": _instrument_min_notional,
            "execution_chunks": list(getattr(execution_plan, "chunks", []) or []),
            "signal_timestamp": now,
            "candidate_timestamp": now,
            "market_geometry_timestamp": now,
            "risk_budget_usdt": approved_risk,
            "required_margin_usdt": estimated_margin,
            "requested_leverage": requested_leverage,
            "exchange_configured_leverage": executable_leverage,
            "leverage_truth_status": leverage_truth_status,
            "leverage_truth_timestamp": leverage_truth_timestamp,
            "leverage_truth_source": leverage_truth_source,
            "leverage_truth_mismatch": leverage_truth_mismatch,
            "requested_vs_configured_mismatch": requested_vs_configured_mismatch,
            "configured_vs_effective_mismatch": configured_vs_effective_mismatch,
            "tp_owner": str(tp_owner or ""),
            "tp_mode_at_entry": str(tp_mode_at_entry or ""),
            "local_tp1_enabled": str(tp_owner or "").upper() == "LOCAL_STAGED",
            "local_tp2_enabled": str(tp_owner or "").upper() == "LOCAL_STAGED",
            "ownership_timestamp": datetime.now(timezone.utc).isoformat() if tp_owner else "",
            "tp_owner_timestamp": datetime.now(timezone.utc).isoformat() if tp_owner else "",
            "partials_enabled_at_entry": bool(_s.get_val("partial_exit_enabled", True)),
        },
    )
    handoff_started = time.monotonic()
    try:
        execution = await router_for(okx).submit(request)
    finally:
        _latency.record_execution_handoff(time.monotonic() - handoff_started)
    if not execution.ok:
        okx._set_last_order_skip_reason(execution.reason)
        return None
    try:
        import candidate_expectancy_telemetry as _expectancy_telemetry

        _expectancy_telemetry.record_execution(
            candidate_id=cid,
            opportunity_id=cid,
            symbol=coin,
            direction=direction,
            engine=engine_name,
            intent_id=intent_id,
            reservation_id=reservation_id,
            order_id=execution.order_id,
            filled_contracts=execution.filled_contracts,
            average_fill_price=execution.average_fill_price,
            protection=execution.protection,
        )
    except Exception:
        logger.exception("Production execution telemetry failed for %s %s", engine_name, coin)
    opportunity_queue.remove(cid, "funded")
    return execution.legacy_response()


def _stop_geometry_atr(coin: str, fallback_atr: float | None = None) -> float:
    """Use 1H ATR for SL/TP geometry while leaving entry/execution ATR untouched."""
    try:
        highs_1h = get_1h_highs(coin)
        lows_1h = get_1h_lows(coin)
        closes_1h = get_1h_closes(coin)
        if highs_1h and lows_1h and closes_1h and len(closes_1h) >= 15:
            atr_1h = calc_atr(highs_1h, lows_1h, closes_1h, 14)
            if atr_1h and atr_1h > 0:
                return float(atr_1h)
    except Exception:
        logger.debug("1H stop ATR unavailable for %s", coin, exc_info=True)
    return float(fallback_atr or 0.0)


def _preview_candidate_geometry(
    *,
    engine: str,
    coin: str,
    direction: str,
    entry_price: float,
    closes: list[float],
    highs: list[float],
    lows: list[float],
    volumes: list[float],
    fallback_atr: float,
) -> dict:
    """Build the early economic preview from the same base geometry as execution."""
    entry = float(entry_price or 0.0)
    if entry <= 0.0:
        return {"valid": False, "reason": "invalid_entry_price"}
    stop_atr = _stop_geometry_atr(coin, fallback_atr)
    leverage = _requested_leverage_for_engine(engine, fallback_atr, entry)
    leverage = min(leverage, int(_s.get_val("leverage_max", 15)))
    leverage = max(leverage, int(_s.get_val("leverage_min", 5)))
    side = "buy" if str(direction or "").lower() in {"long", "buy"} else "sell"
    stop, tp1, tp2 = _sl_tp.compute(
        engine=engine,
        side=side,
        entry=entry,
        closes=closes,
        highs=highs,
        lows=lows,
        atr_val=stop_atr,
        coin=coin,
        leverage=leverage,
        volumes=volumes or [],
    )
    stop_pct = abs(entry - float(stop or 0.0)) / entry * 100.0
    tp1_pct = abs(float(tp1 or 0.0) - entry) / entry * 100.0
    tp2_pct = abs(float(tp2 or 0.0) - entry) / entry * 100.0
    return {
        "valid": bool(stop_pct > 0.0 and tp2_pct > 0.0),
        "source": "production_sl_tp_base_geometry_preview",
        "entry_price": entry,
        "stop_price": float(stop or 0.0),
        "tp1_price": float(tp1 or 0.0),
        "tp2_price": float(tp2 or 0.0),
        "stop_move_pct": stop_pct,
        "tp1_move_pct": tp1_pct,
        "target_move_pct": tp2_pct,
        "atr_value": float(stop_atr or 0.0),
        "atr_timeframe": "1H" if stop_atr else "fallback",
        "leverage": leverage,
    }


def _production_research_only() -> bool:
    """True when production data is allowed but exchange mutation is unarmed."""
    if os.getenv("OXYPULSE_PRODUCTION_RESEARCH_ONLY", "false").strip().lower() == "true":
        return True
    if os.getenv("PAPER_TRADING", "true").strip().lower() == "true":
        return False
    try:
        from live_state import state as _live_state

        snapshot = _live_state.snapshot()
        return not bool(snapshot.get("armed")) or str(snapshot.get("state")) != "LIVE_ARMED"
    except Exception:
        return True


def _candidate_feature_snapshot(
    *,
    coin: str,
    direction: str,
    engine_name: str,
    entry_price: float,
    ticker: dict,
    closes: list[float],
    highs: list[float],
    lows: list[float],
    volumes: list[float],
    closes_15m: list[float] | None,
    closes_1h: list[float] | None,
    closes_4h: list[float] | None,
    funding: float,
    oi_series: list[float],
    taker_buy_volume: float,
    taker_sell_volume: float,
    geometry: dict,
    quality_snapshot: dict,
    role_context: dict,
    desired_notional: float,
    balance: float,
) -> dict:
    """Capture replayable fields already available in the candidate pipeline."""
    entry = float(entry_price or 0.0)
    bid = float((ticker or {}).get("bid", (ticker or {}).get("bidPx", 0.0)) or 0.0)
    ask = float((ticker or {}).get("ask", (ticker or {}).get("askPx", 0.0)) or 0.0)
    mark = float((ticker or {}).get("mark", (ticker or {}).get("markPx", entry)) or entry)
    spread_bps = ((ask - bid) / entry * 10_000.0) if entry > 0 and ask > 0 and bid > 0 else 0.0
    executable_entry = ask if str(direction).upper() == "LONG" and ask > 0 else bid if str(direction).upper() == "SHORT" and bid > 0 else entry
    atr_value = float(geometry.get("atr_value", 0.0) or 0.0)
    atr_pct = atr_value / entry * 100.0 if entry > 0 else 0.0
    adx_value = calc_adx(highs, lows, closes, 14)
    rsi_value = calc_rsi(closes, 14)
    ema9 = calc_ema(closes, 9)
    ema21 = calc_ema(closes, 21)
    ema55 = calc_ema(closes, 55)
    macd_value = calc_macd(closes)
    bands = calc_bollinger_bands(closes)
    vwap_value = calc_vwap(highs, lows, closes, volumes) if len(highs) == len(lows) == len(closes) == len(volumes) else None
    average_volume = sum(volumes[-20:]) / max(len(volumes[-20:]), 1) if volumes else 0.0
    relative_volume = float(volumes[-1]) / average_volume if volumes and average_volume > 0 else None
    order_flow_total = float(taker_buy_volume or 0.0) + float(taker_sell_volume or 0.0)
    order_flow = ((float(taker_buy_volume or 0.0) - float(taker_sell_volume or 0.0)) / order_flow_total) if order_flow_total > 0 else None
    stop_pct = float(geometry.get("stop_move_pct", 0.0) or 0.0)
    leverage = max(int(geometry.get("leverage", _s.get_val("leverage_default", 10)) or 10), 1)
    roles = dict((role_context or {}).get("engine_roles", {}) or {})
    if not roles:
        roles = {
            str(name).upper(): _engine_role(str(name))
            for name in list((role_context or {}).get("consensus_engines", []) or [])
        }
    exit_profile = str((role_context or {}).get("preferred_exit_style", "") or "")
    try:
        from instrument_registry import registry as _instrument_registry

        instrument = _instrument_registry.get(coin, require_fresh=True)
        contracts = instrument.notional_to_contracts(float(desired_notional or 0.0), entry) if instrument and entry > 0 else 0.0
        partial_contracts = instrument.round_contracts_down(contracts / 3.0) if instrument else 0.0
        partial_viable = bool(instrument and instrument.quantity_feasible(partial_contracts, "market")[0])
        if not partial_viable:
            exit_profile = "full_position_only"
    except Exception:
        partial_viable = False
        if float(desired_notional or 0.0) <= float(_s.get_val("adaptive_min_economic_notional_usdt", 1.0) or 1.0) * 3.0:
            exit_profile = "full_position_only"
    legacy_cap_shadow = _legacy_position_cap_shadow(
        coin=coin,
        engine_name=engine_name,
        direction=direction,
        entry_price=entry,
        stop_price=float(geometry.get("stop_price", 0.0) or 0.0),
        stop_distance_pct=stop_pct,
        desired_notional=float(desired_notional or 0.0),
        balance=float(balance or 0.0),
        leverage=leverage,
    )
    return {
        "BTC_regime": quality_snapshot.get("btc_regime"),
        "session": _soft_accept_session_name(),
        "timeframe_context": {
            "entry": "1m/5m cached stream",
            "stop_geometry": "1H",
            "bars_entry": len(closes),
            "bars_15m": len(closes_15m or []),
            "bars_1h": len(closes_1h or []),
            "bars_4h": len(closes_4h or []),
        },
        "entry_reference_price": entry,
        "entry_executable_price": executable_entry,
        "mark_price": mark,
        "bid": bid or None,
        "ask": ask or None,
        "spread_bps": round(spread_bps, 6),
        "ATR": atr_value or None,
        "ATR_pct": round(atr_pct, 8),
        "ADX": adx_value,
        "RSI": rsi_value,
        "EMA_state": {
            "ema9": ema9[-1] if ema9 else None,
            "ema21": ema21[-1] if ema21 else None,
            "ema55": ema55[-1] if ema55 else None,
            "alignment": "bullish" if ema9 and ema21 and ema55 and ema9[-1] > ema21[-1] > ema55[-1] else "bearish" if ema9 and ema21 and ema55 and ema9[-1] < ema21[-1] < ema55[-1] else "mixed",
        },
        "MACD_state": {"line": macd_value[0], "signal": macd_value[1], "histogram": macd_value[2]} if macd_value else None,
        "BB_state": {"upper": bands[0], "middle": bands[1], "lower": bands[2], "width": calc_bollinger_band_width(closes)} if bands else None,
        "VWAP": vwap_value,
        "VWAP_relationship": "above" if vwap_value and entry > vwap_value else "below" if vwap_value else None,
        "Hurst": calc_hurst(closes),
        "OI": float(oi_series[-1]) if oi_series else None,
        "funding": float(funding or 0.0),
        "volume": float(volumes[-1]) if volumes else None,
        "relative_volume": relative_volume,
        "order_flow": order_flow,
        "liquidity_metrics": {
            "volume_quality": quality_snapshot.get("volume_quality"),
            "spread_quality": quality_snapshot.get("spread_quality"),
            "market_cap_bonus": quality_snapshot.get("market_cap_bonus"),
        },
        "liquidity_bucket": "high" if float(quality_snapshot.get("volume_quality", 0.0) or 0.0) >= 1.5 and spread_bps <= 10 else "low" if float(quality_snapshot.get("volume_quality", 0.0) or 0.0) < 0.5 or spread_bps > 30 else "medium",
        "volatility_bucket": "high" if atr_pct >= 1.0 else "low" if atr_pct < 0.35 else "medium",
        "planned_entry": entry,
        "planned_stop": float(geometry.get("stop_price", 0.0) or 0.0),
        "stop_distance_pct": stop_pct,
        "tp1": float(geometry.get("tp1_price", 0.0) or 0.0),
        "tp2": float(geometry.get("tp2_price", 0.0) or 0.0),
        "trail_activation": float(_s.get_val("trail_activation_profit_usd", 0.90) or 0.90),
        "trail_distance": {"mode": str(_s.get_val("trail_distance_mode", "hybrid")), "atr_mult": float(_s.get_val("trail_distance_atr_mult", 2.5) or 2.5)},
        "exit_profile": exit_profile,
        "partial_exits_viable": bool(partial_viable),
        "desired_notional": float(desired_notional or 0.0),
        "final_notional": float(desired_notional or 0.0),
        "risk_at_stop": float(desired_notional or 0.0) * stop_pct / 100.0,
        "isolated_margin": float(desired_notional or 0.0) / leverage,
        "legacy_40pct_cap_shadow": legacy_cap_shadow,
        "engine_family": str(quality_snapshot.get("lane", "") or _s.get_engine_lane(engine_name)),
        "engine_roles": roles,
        "contributor_families": {
            str(name).upper(): str(_s.get_engine_lane(str(name)) or "unknown")
            for name in list((role_context or {}).get("consensus_engines", []) or [])
        },
        "vote_rescue": dict(quality_snapshot.get("vote_rescue", {}) or {}),
    }


def _legacy_position_cap_shadow(
    *,
    coin: str,
    engine_name: str,
    direction: str,
    entry_price: float,
    stop_price: float,
    stop_distance_pct: float,
    desired_notional: float,
    balance: float,
    leverage: int,
) -> dict:
    """Compare normal allocation with a read-only allocation that ignores only the legacy equity-percent cap."""
    result = {
        "research_only": True,
        "applied_to_admission": False,
        "configured_cap_pct_balance": float(_s.get_val("position_size_cap_pct_balance", 100.0) or 100.0),
        "available": False,
    }
    if entry_price <= 0 or stop_price <= 0 or stop_distance_pct <= 0 or balance <= 0:
        result["reason"] = "invalid_shadow_inputs"
        return result
    try:
        from capital_allocator import allocate_entry
        from capital_reservations import book as _reservation_book
        from instrument_registry import registry as _instrument_registry

        base_size, _ = _size_mode_base_notional(balance, engine_name)
        uncapped = _risk_sized_notional_from_stop(
            engine_name=engine_name,
            coin=coin,
            side="buy" if str(direction).upper() == "LONG" else "sell",
            entry_px=entry_price,
            stop_px=stop_price,
            balance=balance,
            current_size_usdt=desired_notional,
            base_size_usdt=base_size,
            min_exec_usdt=_candidate_exec_floor(coin, entry_price, balance),
            max_size_usdt=None,
        )
        risk_meta = dict(uncapped.get("meta", {}) or {})
        requested = float(risk_meta.get("raw_size_usdt", uncapped.get("size_usdt", 0.0)) or 0.0)
        instrument = _instrument_registry.get(coin, require_fresh=True)
        open_positions = {
            key: value for key, value in _registry.items()
            if str(key).upper() != str(coin).upper()
            and not bool(value.get("pending_reservation", False))
        }
        common = dict(
            balance_usdt=balance,
            requested_size_usdt=requested,
            entry_price=entry_price,
            stop_price=stop_price,
            leverage=leverage,
            open_positions=open_positions,
            metadata={"risk_sizing": risk_meta, "sl_dist_pct": stop_distance_pct},
            instrument_metadata=instrument,
            currently_reserved_margin_usdt=_reservation_book.total_margin(),
            deployment_mode=str(_s.get_val("deployment_mode", "Guarded") or "Guarded"),
        )
        normal = allocate_entry(**common)
        shadow = allocate_entry(**common, ignore_position_size_pct_cap=True)
        normal_meta = normal.metadata()
        shadow_meta = shadow.metadata()
        result.update({
            "available": True,
            "risk_budget_usdt": float(risk_meta.get("risk_amount_usdt", 0.0) or 0.0),
            "risk_sized_notional_usdt": requested,
            "normal": normal_meta,
            "without_pct_cap": shadow_meta,
            "notional_delta_usdt": round(float(shadow.final_size_usdt or 0.0) - float(normal.final_size_usdt or 0.0), 6),
            "margin_delta_usdt": round(float(shadow.required_margin_usdt or 0.0) - float(normal.required_margin_usdt or 0.0), 6),
            "risk_delta_usdt": round(float(shadow.approved_risk or 0.0) - float(normal.approved_risk or 0.0), 6),
        })
    except Exception as exc:
        result["reason"] = f"shadow_unavailable:{type(exc).__name__}"
    return result


def _paper_validation_payload(
    *,
    engine_name: str,
    coin: str,
    side: str,
    entry_px: float,
    stop_px: float,
    final_size_usdt: float,
    risk_meta: dict,
    order_id: str = "",
    exit_reason: str = "",
    pnl_usdt: float | None = None,
) -> dict:
    opened_at = datetime.now(timezone.utc).isoformat()
    allocator_meta = dict((risk_meta or {}).get("capital_allocator", {}) or {})
    final_economics = dict((risk_meta or {}).get("final_candidate_economics", {}) or {})
    capital_efficiency = dict(final_economics.get("capital_efficiency", {}) or {})
    payload = {
        "order_id": str(order_id or ""),
        "engine": str(engine_name or ""),
        "pair": str(coin or "").upper(),
        "side": str(side or ""),
        "entry_price": float(entry_px or 0.0),
        "stop_price": float(stop_px or 0.0),
        "sl_dist_pct": float((risk_meta or {}).get("sl_dist_pct", 0.0) or 0.0),
        "risk_amount_usdt": float((risk_meta or {}).get("risk_amount_usdt", 0.0) or 0.0),
        "raw_size_usdt": float((risk_meta or {}).get("raw_size_usdt", 0.0) or 0.0),
        "min_exec_usdt": float((risk_meta or {}).get("min_exec_usdt", 0.0) or 0.0),
        "cap_applied": bool((risk_meta or {}).get("cap_applied", False)),
        "cap_source": str((risk_meta or {}).get("cap_source", "") or ""),
        "final_size_usdt": float(final_size_usdt or 0.0),
        "position_notional_usdt": float(
            allocator_meta.get("allocator_position_notional_usdt", final_size_usdt) or 0.0
        ),
        "isolated_margin_required_usdt": float(
            allocator_meta.get("allocator_isolated_margin_required_usdt", 0.0) or 0.0
        ),
        "risk_at_stop_usdt": float(
            allocator_meta.get("allocator_risk_at_stop_usdt", 0.0) or 0.0
        ),
        "risk_pct_equity": allocator_meta.get("allocator_risk_pct_equity"),
        "margin_pct_equity": allocator_meta.get("allocator_margin_pct_equity"),
        "notional_pct_equity": allocator_meta.get("allocator_notional_pct_equity"),
        "position_cap_basis": str(
            allocator_meta.get("allocator_position_cap_basis", "notional_pct_equity") or "notional_pct_equity"
        ),
        "capital_efficiency": capital_efficiency,
        "lifecycle_id": str(order_id or (risk_meta or {}).get("candidate_id", "") or ""),
        "candidate_id": str((risk_meta or {}).get("candidate_id", "") or ""),
        "opened_at": opened_at,
        "first_fill_at": opened_at,
        "last_entry_fill_at": opened_at,
    }
    if exit_reason:
        payload["exit_reason"] = str(exit_reason)
    if pnl_usdt is not None:
        payload["pnl_usdt"] = float(pnl_usdt or 0.0)
    return payload


def _paper_validation_close_context_from_trade(trade: dict, coin: str) -> dict:
    context = dict((trade or {}).get("paper_validation", {}) or {})
    if context:
        for key in ("first_fill_at", "last_entry_fill_at", "first_exit_fill_at", "last_exit_fill_at", "closed_at"):
            value = (trade or {}).get(key)
            if value:
                context[key] = value
        if context.get("last_exit_fill_at"):
            context["closed_at"] = context["last_exit_fill_at"]
        return context

    risk_meta = dict((trade or {}).get("risk_sizing", {}) or {})
    entry_px = float((trade or {}).get("entry", (trade or {}).get("entry_price", 0.0)) or 0.0)
    stop_px = float(
        (trade or {}).get(
            "stop_loss",
            (trade or {}).get("hard_sl", (trade or {}).get("stop_price", 0.0)),
        )
        or 0.0
    )
    sl_dist_pct = float((trade or {}).get("sl_dist_pct", risk_meta.get("sl_dist_pct", 0.0)) or 0.0)
    if sl_dist_pct <= 0 and entry_px > 0 and stop_px > 0:
        sl_dist_pct = abs(stop_px - entry_px) / entry_px * 100.0

    return {
        "entry_row_missing": True,
        "validation_excluded": True,
        "validation_exclusion_reason": "missing_entry_row_pre_current_validation",
        "admission_config_cohort": "legacy_pre_telemetry",
        "order_id": str((trade or {}).get("order_id", "") or ""),
        "engine": str((trade or {}).get("engine", (trade or {}).get("source_engine", "")) or ""),
        "pair": str(coin or "").upper(),
        "side": str((trade or {}).get("side", "") or ""),
        "entry_price": entry_px,
        "stop_price": stop_px,
        "sl_dist_pct": float(sl_dist_pct or 0.0),
        "risk_amount_usdt": float(risk_meta.get("risk_amount_usdt", (trade or {}).get("expected_risk_usdt", 0.0)) or 0.0),
        "raw_size_usdt": float(risk_meta.get("raw_size_usdt", 0.0) or 0.0),
        "min_exec_usdt": float(risk_meta.get("min_exec_usdt", 0.0) or 0.0),
        "cap_applied": bool(risk_meta.get("cap_applied", False)),
        "cap_source": str(risk_meta.get("cap_source", "") or ""),
        "final_size_usdt": float((trade or {}).get("size", (trade or {}).get("size_usdt", 0.0)) or 0.0),
        "cap_at_admission": (trade or {}).get("cap_at_admission", (trade or {}).get("rule_357_max_open_risk_pct")),
        "current_rule_357_max_open_risk_pct": float(_s.get_val("rule_357_max_open_risk_pct", 0.0) or 0.0),
    }


def _requested_leverage_for_engine(engine_name: str, atr_val: float, entry_px: float) -> int:
    if engine_name == "TENJIN":
        lev = int(_s.get_val("tenjin_leverage", 8))
    elif engine_name == "AMATERASU":
        lev = int(_s.get_val("amaterasu_leverage", 15))
    elif engine_name == "TITAN":
        lev = int(_s.get_val("titan_leverage", 15))
    elif engine_name == "RONIN":
        lev = int(_s.get_val("ronin_leverage", 15))
    elif engine_name == "OROCHI":
        lev = int(_s.get_val("orochi_leverage", 15))
    else:
        lev = int(_s.get_val("leverage_default", 10))
    if _s.get_val("vol_leverage_enabled", True) and engine_name not in ("TENJIN", "AMATERASU", "TITAN", "RONIN", "OROCHI"):
        lev = risk.get_vol_adjusted_leverage(atr_val, entry_px, lev)
    lev = min(lev, int(_s.get_val("leverage_max", 15)))
    lev = max(lev, int(_s.get_val("leverage_min", 5)))
    return lev


def _vote_scaled_size_for_engine(engine_name: str, base_size: float, votes: float) -> float:
    if engine_name == "TITAN":
        if votes >= 3:
            return base_size * float(_s.get_val("titan_size_mult_3votes", 2.0))
        return base_size * float(_s.get_val("titan_size_mult_2votes", 1.2))
    if engine_name == "IZANAGI":
        if votes >= 3:
            return base_size * float(_s.get_val("izanagi_size_mult_3votes", 0.9))
        return base_size * float(_s.get_val("izanagi_size_mult_2votes", 0.6))
    if engine_name == "RONIN":
        if votes >= 3:
            return base_size * float(_s.get_val("ronin_size_mult_3votes", 0.9))
        return base_size * float(_s.get_val("ronin_size_mult_2votes", 0.55))
    if engine_name == "OROCHI":
        if votes >= 3:
            return base_size * float(_s.get_val("orochi_size_mult_3votes", 0.9))
        return base_size * float(_s.get_val("orochi_size_mult_2votes", 0.6))
    if votes >= 3.0:
        return base_size * float(_s.get_val("size_multiplier_votes_3", 1.5))
    if votes >= 2.5:
        return base_size * 1.0
    return base_size * float(_s.get_val("size_multiplier_votes_2", 0.7))


def _candidate_wait_dependencies(
    *,
    engine_name: str,
    direction: str,
    balance: float,
    spread_quality: float,
    signal_score: float,
    capital_profile,
    soft_accept_reason: str,
    soft_accept_session: str,
) -> tuple[dict, float, dict]:
    """Build stable wait-state dependencies after calculating the real cap."""
    from candidate_economic_state import store as candidate_state

    cap_usdt, cap_details = _participation_cap_window(
        engine_name,
        direction,
        balance,
        capital_profile=capital_profile,
        soft_accept=bool(soft_accept_reason),
        session=soft_accept_session,
    )
    spread_pct = max((0.75 - float(spread_quality or 0.0)) * 0.04, 0.0)
    dependencies = candidate_state.dependencies(
        equity=balance,
        spread_pct=spread_pct,
        signal_score=signal_score,
        risk_capacity_usdt=float(cap_usdt or 0.0),
        execution_floor_policy=(
            "contract_aware"
            if bool(_s.get_val("contract_aware_execution_floor_enabled", False))
            else "account_floor"
        ),
    )
    return dependencies, float(cap_usdt or 0.0), dict(cap_details or {})


def _preview_trade_execution(
    *,
    coin: str = "",
    engine_name: str,
    side: str,
    balance: float,
    votes: float,
    closes: list[float],
    highs: list[float],
    lows: list[float],
    entry_px: float,
    v6_conf=None,
    v6_dd=None,
    v6_gate=None,
    v6_profile=None,
    soft_accept_mult: float = 1.0,
    soft_accept_reason: str = "",
    soft_accept_mode: str = "",
    soft_accept_session: str = "",
    weekly_cap_mult: float = 1.0,
    weekly_cap_reason: str = "",
    weekly_cap_decision: str = "ACCEPT",
    candidate_quality: float = 0.0,
) -> dict:
    direction = "long" if side == "buy" else "short"
    atr_val = calc_atr(highs, lows, closes, 14) or 0.0
    live_entry = float(entry_px or (closes[-1] if closes else 0.0) or 0.0)
    lev = _requested_leverage_for_engine(engine_name, atr_val, live_entry)
    intent = _trade_intent.get_trade_intent(
        balance=balance,
        regime=str(_regime.get_regime() if _regime else ""),
    )
    participation_cap_usdt, participation_details = _participation_cap_window(
        engine_name,
        direction,
        balance,
        capital_profile=v6_profile,
        soft_accept=bool(soft_accept_reason),
        session=soft_accept_session,
    )

    size_mult = 0.85 if engine_name == "SHOGUN" else 1.0
    cfg_floor, size_mode = _size_mode_base_notional(balance, engine_name)
    pct_size = balance * float(_s.get_val("position_size_pct", 0.05))
    cfg_cap_pct = float(_s.get_val("position_size_cap_pct_balance", 100.0))
    cfg_cap_usdt = float(_s.get_val("position_size_cap_usdt", 250.0))
    raw_size = cfg_floor
    base_size = raw_size * size_mult

    projected_size = round(_vote_scaled_size_for_engine(engine_name, base_size, float(votes or 0.0)), 2)
    exec_floor = _candidate_exec_floor(coin, live_entry, balance)
    max_size = min(balance * (cfg_cap_pct / 100), cfg_cap_usdt) if balance > 0 else cfg_cap_usdt

    def _record_micro_diag(final_size: float, block_reason: str = "", modifiers: dict | None = None) -> None:
        try:
            _micro_sizing.record_sizing_diagnostics(
                **_micro_sizing.diagnostics_snapshot(
                    balance_usdt=balance,
                    settings=_s,
                    leverage=lev,
                    exchange_floor_usdt=exec_floor,
                    final_size_usdt=float(final_size or 0.0),
                    block_reason=str(block_reason or ""),
                    reductions_stacked=bool((modifiers or {}).get("reductions_stacked", False)),
                    effective_max_positions=int(getattr(v6_profile, "deployment_max_positions", 0) or 0) if v6_profile is not None else None,
                ),
                size_mode=size_mode,
                projected_size_usdt=round(projected_size, 2),
                base_size_usdt=round(base_size, 2),
                engine=str(engine_name or ""),
                direction=direction,
            )
        except Exception:
            pass

    performance_factor = float(_perf.get_size_multiplier() or 1.0)
    reduced_size = round(projected_size * performance_factor, 2)
    if not (_s.get_val("v6_enabled", False) and _s.get_val("dynamic_sizing_enabled", _s.get_val("v6_dynamic_sizing_enabled", False))):
        reduced_size = round(reduced_size * max(float(intent.size_multiplier or 1.0), 0.0), 2)
    reduced_size = max(min(reduced_size, max_size), 0.0)

    gate_mult = float(v6_gate.size_multiplier if v6_gate is not None and hasattr(v6_gate, "size_multiplier") else 1.0)
    drawdown_mult = float(v6_dd.size_multiplier if v6_dd is not None and hasattr(v6_dd, "size_multiplier") else 1.0)
    size = reduced_size
    size_reason = f"mode {size_mode} projected ${projected_size:.2f} -> risk ${reduced_size:.2f} | perf x{performance_factor:.2f} dd x{drawdown_mult:.2f} gate x{gate_mult:.2f} intent {intent.mode}"
    sizing_modifiers: dict = {}
    if _s.get_val("v6_enabled", False) and _s.get_val("dynamic_sizing_enabled", _s.get_val("v6_dynamic_sizing_enabled", False)):
        from sizing_engine import compute_size as _v6_size

        conf_score = v6_conf.confidence_score if v6_conf is not None and hasattr(v6_conf, "confidence_score") else float(votes or 0.0)
        size_result = _v6_size(
            base_size=size * gate_mult,
            confidence_score=conf_score,
            atr_val=atr_val,
            entry_px=live_entry,
            leverage=lev,
            balance=balance,
            dd_size_multiplier=drawdown_mult,
            capital_profile=v6_profile,
            intent_size_multiplier=float(intent.size_multiplier or 1.0),
            effective_cap_usdt=participation_cap_usdt,
            minimum_exec_usdt=exec_floor,
        )
        sizing_modifiers = dict(size_result.modifiers or {})
        reduced_live = float(sizing_modifiers.get("pre_floor_size_usdt", size_result.final_size_usdt) or size_result.final_size_usdt)
        size = float(size_result.final_size_usdt)
        size_reason = (
            f"projected ${projected_size:.2f} -> risk ${reduced_live:.2f} -> live ${size:.2f} "
            f"| perf x{performance_factor:.2f} dd x{drawdown_mult:.2f} gate x{gate_mult:.2f} intent {intent.mode}"
        )
        if size_result.reason:
            size_reason = f"{size_reason} | {size_result.reason}"
        if size_result.skipped:
            _reason = (
                f"reduced_size_below_min projected=${projected_size:.2f} "
                f"reduced=${reduced_live:.2f} min=${exec_floor:.2f}"
            )
            _record_micro_diag(size, _reason, sizing_modifiers)
            return {
                "skip": True,
                "reject_tag": "sizing",
                "reject_reason": _reason,
                "projected_size_usdt": projected_size,
                "reduced_size_usdt": round(reduced_live, 2),
                "size_usdt": round(size, 2),
                "exec_floor": round(exec_floor, 2),
                "performance_factor": round(performance_factor, 2),
                "drawdown_factor": round(drawdown_mult, 2),
                "gate_factor": round(gate_mult, 2),
                "floor_preserved": bool(sizing_modifiers.get("floor_preserved")),
                "intent": intent,
                "entry_px": round(live_entry, 8),
                "atr_val": round(float(atr_val or 0.0), 8),
                "leverage": lev,
                "tier": risk.get_tier(balance),
                "size_reason": size_reason,
                "sizing_modifiers": sizing_modifiers,
                "participation_cap_usdt": round(float(participation_cap_usdt or 0.0), 2),
                "participation_details": list(participation_details),
                "base_size_usdt": round(base_size, 2),
                "weekly_cap_decision": str(weekly_cap_decision or ""),
                "weekly_cap_mult": round(float(weekly_cap_mult or 1.0), 4),
                "weekly_cap_reason": str(weekly_cap_reason or ""),
                "candidate_quality": round(float(candidate_quality or 0.0), 3),
            }
    else:
        try:
            from sizing_engine import _floor_preserve_allowed as _floor_ok
        except Exception:
            _floor_ok = lambda *_args, **_kwargs: bool(_s.get_val("risk_floor_preserve_enabled", True))

        sizing_modifiers = {
            "pre_floor_size_usdt": round(size, 2),
            "exec_floor": round(exec_floor, 2),
            "max_executable_usdt": round(max_size, 2),
            "floor_preserve_allowed": bool(_floor_ok(v6_profile)),
        }
        if max_size + 0.01 < exec_floor:
            _reason = (
                f"reduced_size_below_min projected=${projected_size:.2f} "
                f"reduced=${size:.2f} min=${exec_floor:.2f}"
            )
            _record_micro_diag(size, _reason, sizing_modifiers)
            return {
                "skip": True,
                "reject_tag": "sizing",
                "reject_reason": _reason,
                "projected_size_usdt": projected_size,
                "reduced_size_usdt": round(size, 2),
                "size_usdt": round(size, 2),
                "exec_floor": round(exec_floor, 2),
                "performance_factor": round(performance_factor, 2),
                "drawdown_factor": round(drawdown_mult, 2),
                "gate_factor": round(gate_mult, 2),
                "floor_preserved": False,
                "intent": intent,
                "entry_px": round(live_entry, 8),
                "atr_val": round(float(atr_val or 0.0), 8),
                "leverage": lev,
                "tier": risk.get_tier(balance),
                "size_reason": size_reason,
                "sizing_modifiers": sizing_modifiers,
                "participation_cap_usdt": round(float(participation_cap_usdt or 0.0), 2),
                "participation_details": list(participation_details),
                "base_size_usdt": round(base_size, 2),
                "weekly_cap_decision": str(weekly_cap_decision or ""),
                "weekly_cap_mult": round(float(weekly_cap_mult or 1.0), 4),
                "weekly_cap_reason": str(weekly_cap_reason or ""),
                "candidate_quality": round(float(candidate_quality or 0.0), 3),
            }
        if size + 0.01 < exec_floor:
            if sizing_modifiers["floor_preserve_allowed"]:
                size = round(exec_floor, 2)
                sizing_modifiers["floor_preserved"] = True
                size_reason = f"{size_reason} | floor_preserved ${exec_floor:.2f}"
            else:
                _reason = (
                    f"reduced_size_below_min projected=${projected_size:.2f} "
                    f"reduced=${size:.2f} min=${exec_floor:.2f}"
                )
                _record_micro_diag(size, _reason, sizing_modifiers)
                return {
                    "skip": True,
                    "reject_tag": "sizing",
                    "reject_reason": _reason,
                    "projected_size_usdt": projected_size,
                    "reduced_size_usdt": round(size, 2),
                    "size_usdt": round(size, 2),
                    "exec_floor": round(exec_floor, 2),
                    "performance_factor": round(performance_factor, 2),
                    "drawdown_factor": round(drawdown_mult, 2),
                    "gate_factor": round(gate_mult, 2),
                    "floor_preserved": False,
                    "intent": intent,
                    "entry_px": round(live_entry, 8),
                    "atr_val": round(float(atr_val or 0.0), 8),
                    "leverage": lev,
                    "tier": risk.get_tier(balance),
                    "size_reason": size_reason,
                    "sizing_modifiers": sizing_modifiers,
                    "participation_cap_usdt": round(float(participation_cap_usdt or 0.0), 2),
                    "participation_details": list(participation_details),
                    "base_size_usdt": round(base_size, 2),
                    "weekly_cap_decision": str(weekly_cap_decision or ""),
                    "weekly_cap_mult": round(float(weekly_cap_mult or 1.0), 4),
                    "weekly_cap_reason": str(weekly_cap_reason or ""),
                    "candidate_quality": round(float(candidate_quality or 0.0), 3),
                }

    if soft_accept_mult < 1.0:
        adjusted_soft = _apply_soft_accept_size(size, balance, soft_accept_mult)
        if abs(adjusted_soft - size) > 0.009:
            size_reason = f"{size_reason} | soft_accept x{soft_accept_mult:.2f} -> ${adjusted_soft:.2f}"
        size = adjusted_soft

    if weekly_cap_mult < 1.0:
        adjusted_wk = _apply_soft_accept_size(size, balance, weekly_cap_mult)
        if abs(adjusted_wk - size) > 0.009:
            _wk_note = str(weekly_cap_reason or "weekly_cap").strip()
            size_reason = f"{size_reason} | weekly_cap x{weekly_cap_mult:.2f} -> ${adjusted_wk:.2f} ({_wk_note})"
        size = adjusted_wk

    if (
        participation_cap_usdt is not None
        and participation_cap_usdt > 0
        and size > participation_cap_usdt + 0.01
        and participation_cap_usdt >= exec_floor
    ):
        size_reason = f"{size_reason} | participation cap ${participation_cap_usdt:.2f}"
        size = round(float(participation_cap_usdt), 2)

    if size + 0.01 < exec_floor:
        _reason = (
            f"reduced_size_below_min projected=${projected_size:.2f} "
            f"reduced=${size:.2f} min=${exec_floor:.2f}"
        )
        _record_micro_diag(size, _reason, sizing_modifiers)
        return {
            "skip": True,
            "reject_tag": "sizing",
            "reject_reason": _reason,
            "projected_size_usdt": projected_size,
            "reduced_size_usdt": round(size, 2),
            "size_usdt": round(size, 2),
            "exec_floor": round(exec_floor, 2),
            "performance_factor": round(performance_factor, 2),
            "drawdown_factor": round(drawdown_mult, 2),
            "gate_factor": round(gate_mult, 2),
            "floor_preserved": False,
            "intent": intent,
            "entry_px": round(live_entry, 8),
            "atr_val": round(float(atr_val or 0.0), 8),
            "leverage": lev,
            "tier": risk.get_tier(balance),
            "size_reason": size_reason,
            "sizing_modifiers": sizing_modifiers,
            "participation_cap_usdt": round(float(participation_cap_usdt or 0.0), 2),
            "participation_details": list(participation_details),
            "base_size_usdt": round(base_size, 2),
            "weekly_cap_decision": str(weekly_cap_decision or ""),
            "weekly_cap_mult": round(float(weekly_cap_mult or 1.0), 4),
            "weekly_cap_reason": str(weekly_cap_reason or ""),
            "candidate_quality": round(float(candidate_quality or 0.0), 3),
        }

    _record_micro_diag(size, "", sizing_modifiers)
    return {
        "skip": False,
        "size_usdt": round(float(size or 0.0), 2),
        "projected_size_usdt": round(projected_size, 2),
        "reduced_size_usdt": round(float(sizing_modifiers.get("pre_floor_size_usdt", reduced_size) or reduced_size), 2),
        "exec_floor": round(exec_floor, 2),
        "performance_factor": round(performance_factor, 2),
        "drawdown_factor": round(drawdown_mult, 2),
        "gate_factor": round(gate_mult, 2),
        "floor_preserved": bool(sizing_modifiers.get("floor_preserved")),
        "intent": intent,
        "entry_px": round(live_entry, 8),
        "atr_val": round(float(atr_val or 0.0), 8),
        "leverage": lev,
        "tier": risk.get_tier(balance),
        "size_reason": size_reason,
        "sizing_modifiers": sizing_modifiers,
        "participation_cap_usdt": round(float(participation_cap_usdt or 0.0), 2),
        "participation_details": list(participation_details),
        "base_size_usdt": round(base_size, 2),
        "weekly_cap_decision": str(weekly_cap_decision or ""),
        "weekly_cap_mult": round(float(weekly_cap_mult or 1.0), 4),
        "weekly_cap_reason": str(weekly_cap_reason or ""),
        "candidate_quality": round(float(candidate_quality or 0.0), 3),
    }


def _projected_risk_reserve_metadata(size_preview: dict | None) -> dict:
    if not size_preview:
        return {}
    entry_px = float(size_preview.get("entry_px", 0.0) or 0.0)
    atr_val = float(size_preview.get("atr_val", 0.0) or 0.0)
    sl_dist_pct = 0.0
    if entry_px > 0 and atr_val > 0:
        sl_dist_pct = (atr_val * float(_s.get_val("atr_sl_multiplier", 2.3) or 2.3)) / entry_px * 100.0
    min_sl_pct = float(_s.get_val("min_sl_dist_pct", 1.0) or 1.0)
    max_sl_pct = float(_s.get_val("max_sl_dist_pct", 3.0) or 3.0)
    if sl_dist_pct <= 0:
        sl_dist_pct = min_sl_pct
    sl_dist_pct = min(max(sl_dist_pct, min_sl_pct), max_sl_pct)
    return {
        "entry_px": round(entry_px, 8),
        "atr_val": round(atr_val, 8),
        "sl_dist_pct": round(sl_dist_pct, 6),
        "risk_sizing": {
            "sl_dist_pct": round(sl_dist_pct, 6),
            "size_usdt": round(float(size_preview.get("size_usdt", 0.0) or 0.0), 6),
        },
    }


def _capital_mode_name(balance: float, capital_profile=None) -> str:
    if capital_profile is not None and getattr(capital_profile, "mode", ""):
        return str(capital_profile.mode).upper()
    micro_max = float(_s.get_val("v6_micro_max_balance", 100.0) or 100.0)
    growth_max = float(_s.get_val("v6_growth_max_balance", 1000.0) or 1000.0)
    if balance < micro_max:
        return "MICRO"
    if balance >= growth_max:
        return "PRO"
    return "GROWTH"


def _soft_accept_session_name() -> str:
    try:
        from session_context import get_session as _get_session
        return str(_get_session() or "OFF").upper()
    except Exception:
        return "OFF"


def _soft_accept_min_confidence(mode: str, session_name: str) -> float:
    mode_lc = str(mode or "growth").lower()
    threshold = float(
        _s.get_val(
            f"soft_accept_min_confidence_{mode_lc}",
            _s.get_val("soft_accept_min_confidence", 3.5),
        )
    )
    if mode_lc == "micro" and str(session_name or "").upper() == "OFF":
        threshold -= float(_s.get_val("soft_accept_off_session_confidence_boost_micro", 0.2) or 0.2)
    return max(threshold, 0.0)


_ACTIVE_LANE_OVERRIDE_SESSIONS = {"LONDON", "NY", "NEW_YORK"}


def _lane_override_enabled_for_mode(mode: str) -> bool:
    return str(mode or "").upper() == "MICRO" and bool(_s.get_val("lane_override_enabled", True))


def _confirm_path_enabled() -> bool:
    return bool(_s.get_val("engine_autonomy_enabled", True)) and bool(_s.get_val("confirm_path_enabled", True))


def _per_engine_cap_enabled() -> bool:
    return bool(_s.get_val("per_engine_cap_enabled", True))


def _engine_weight_adaptation_enabled() -> bool:
    return bool(_s.get_val("engine_weight_adaptation_enabled", False))


def _lane_override_session_is_active(session_name: str) -> bool:
    return str(session_name or "").upper() in _ACTIVE_LANE_OVERRIDE_SESSIONS


def _lane_override_min_confidence(session_name: str) -> float:
    if _lane_override_session_is_active(session_name):
        return float(_s.get_val("lane_override_active_session_min_confidence", 2.3) or 2.3)
    return float(_s.get_val("lane_override_min_confidence", 2.7) or 2.7)


def _lane_override_confidence(conf_obj, votes) -> float:
    return float(
        conf_obj.confidence_score
        if conf_obj is not None and hasattr(conf_obj, "confidence_score")
        else (votes or 0.0)
    )


def _lane_override_slot_in_use() -> bool:
    for trade in _registry.values():
        if bool(trade.get("lane_override_active", False)):
            return True
    return False


def _lane_override_precheck(balance: float, capital_profile, conf_score: float, session_name: str) -> tuple[bool, str]:
    mode = _capital_mode_name(balance, capital_profile)
    if not _lane_override_enabled_for_mode(mode):
        return False, f"lane_override_disabled_for_mode:{mode}"
    if _lane_override_slot_in_use():
        return False, "override_slot_in_use"
    if _reg_count() > 0:
        return False, f"account_not_flat:{_reg_count()} active positions"
    threshold = _lane_override_min_confidence(session_name)
    if conf_score < threshold:
        return False, f"confidence {conf_score:.2f} < {threshold:.2f}"
    return True, "ok"


def _lane_override_rank_key(candidate: dict) -> tuple[float, float, str]:
    conf = float(candidate.get("confidence", 0.0) or 0.0)
    votes = float(candidate.get("votes", 0.0) or 0.0)
    coin = str(candidate.get("coin", "") or "").upper()
    # When winrate optimization is enabled, prefer the higher-quality candidate when
    # multiple lane-override candidates compete in the same cycle.
    if bool(_s.get_val("winrate_optimization_enabled", False)):
        q = float(candidate.get("quality_score", 0.0) or 0.0)
        return (-q, -conf, -votes, coin)
    return (-conf, -votes, coin)


def _log_lane_override_pick(engine: str, coin: str, direction: str, conf: float) -> None:
    logger.info(
        "LANE OVERRIDE PICK [%s] %s %s conf=%.2f",
        engine_log_name(engine),
        str(coin or "").upper(),
        str(direction or "").upper(),
        float(conf or 0.0),
    )


def _log_lane_override_block(engine: str, coin: str, direction: str, conf: float, reason: str) -> None:
    meta = _rjlog.record(engine, coin, direction, float(conf or 0.0), "lane_budget", reason)
    if meta.get("suppressed_log"):
        return
    logger.info(
        "LANE OVERRIDE BLOCK [%s] %s %s conf=%.2f | reason=%s",
        engine_log_name(engine),
        str(coin or "").upper(),
        str(direction or "").upper(),
        float(conf or 0.0),
        str(reason or "unspecified").replace("\n", " ").strip(),
    )


MIN_CANDLES = 50

_shared_okx: Optional[OKXClient] = None
# ── Shared HTTP client for TSUNAMI / RAIJUU public-endpoint fetches ──────────
_engine_http: httpx.AsyncClient = httpx.AsyncClient(
    timeout=httpx.Timeout(10.0, connect=5.0),
    limits=httpx.Limits(max_connections=10, max_keepalive_connections=5),
)
_engine_http_gate = asyncio.Semaphore(10)

# ── Correlation groups ───────────────────────────────────────────────────────
# Coins within the same group move together. The correlation filter blocks
# opening a new position when too many correlated coins are already open
# in the same direction.
_CORRELATION_GROUPS: dict[str, list[str]] = {
    "MEME":    ["DOGE", "SHIB", "PEPE", "FLOKI", "BONK", "WIF", "MEME", "TURBO", "BRETT", "NEIRO"],
    "BTC_ECO": ["BTC", "STX", "ORDI", "SATS", "RUNE"],
    "ETH_ECO": ["ETH", "ARB", "OP", "STRK", "MANTA", "BLAST", "ZK"],
    "L1_ALT":  ["SOL", "AVAX", "SUI", "SEI", "APT", "TIA", "INJ", "FTM", "NEAR", "DOT", "ADA", "ATOM"],
    "AI":      ["FET", "RNDR", "TAO", "WLD", "ARKM", "AGIX"],
    "DEFI":    ["UNI", "AAVE", "MKR", "CRV", "DYDX", "SUSHI", "COMP", "SNX"],
    "GAMING":  ["IMX", "GALA", "AXS", "SAND", "MANA", "PIXEL", "PORTAL"],
}
# Pre-compute coin→group lookup
_COIN_TO_GROUP: dict[str, str] = {}
for _grp, _coins in _CORRELATION_GROUPS.items():
    for _c in _coins:
        _COIN_TO_GROUP[_c] = _grp


def _correlation_check(
    coin: str,
    direction: str,
    *,
    balance: float = 0.0,
    capital_profile=None,
    candidate_quality: float = 0.0,
    debug_override_level: str = "normal",  # normal | relaxed | unlimited
) -> tuple[bool, str]:
    """Return (allowed, reason). Checks if too many correlated coins are already open."""
    if not _s.get_val("correlation_filter_enabled", True):
        return True, ""
    raw_coin = coin.replace("-USDT-SWAP", "").replace("USDT", "")
    group = _COIN_TO_GROUP.get(raw_coin)
    if not group:
        return True, ""  # coin not in any group
    max_same = int(_s.get_val("correlation_max_same_group", 2))
    policy_suffix = ""
    try:
        from exposure_tracker import get_exposure_policy as _get_exposure_policy

        policy = _get_exposure_policy(balance, capital_profile=capital_profile, candidate_quality=candidate_quality)
        max_same = max(int(policy.correlation_limit or max_same), 1)
        policy_suffix = f" [{policy.mode}]"
    except Exception:
        policy_suffix = ""

    lvl = str(debug_override_level or "normal").strip().lower()
    if lvl not in {"normal", "relaxed", "unlimited"}:
        lvl = "normal"
    if lvl != "normal":
        if lvl == "relaxed":
            max_same += 1
        else:  # unlimited (bounded dynamic, not infinite)
            max_same += 3
    # Count open positions in same group + same direction
    count = 0
    for open_coin, details in _registry.items():
        open_raw = open_coin.replace("-USDT-SWAP", "").replace("USDT", "")
        if _COIN_TO_GROUP.get(open_raw) == group:
            open_dir = str(details.get("direction", "")).upper()
            if open_dir == direction.upper():
                count += 1
    if count >= max_same:
        return False, f"correlation:{group}({count}/{max_same} {direction}){policy_suffix}"
    return True, ""


# ── TSUNAMI liquidation cache ────────────────────────────────────────────────
# {coin: {"long_usd": float, "short_usd": float, "ts": float}}
_tsunami_cache: dict[str, dict] = {}
_TSUNAMI_FETCH_COOLDOWN = 60   # seconds between API calls per coin

async def _fetch_tsunami(coin: str) -> tuple[float, float]:
    """Return (long_liq_usd, short_liq_usd) liquidated in last 5 minutes."""
    try:
        now    = time.time()
        cached = _tsunami_cache.get(coin, {})
        if cached and now - cached.get("ts", 0) < _TSUNAMI_FETCH_COOLDOWN:
            return cached["long_usd"], cached["short_usd"]

        inst_id = f"{coin}-USDT-SWAP"
        resp, rate_bucket = await run_with_rate_admission(
            "GET", "/api/v5/public/liquidation-orders",
            params={"instType": "SWAP", "instId": inst_id, "state": "filled"}, priority="P2",
            pool_gate=_engine_http_gate,
            send=lambda: _engine_http.get(
                "https://www.okx.com/api/v5/public/liquidation-orders",
                params={"instType": "SWAP", "instId": inst_id, "state": "filled"},
            ),
        )
        if resp.status_code == 429:
            process_rest_rate_governor.record_http_429(rate_bucket)
        resp.raise_for_status()
        payload = resp.json()
        if str(payload.get("code", "")) == "50011":
            process_rest_rate_governor.record_business_50011(rate_bucket)
        data = payload.get("data", [])

        five_min_ago_ms = (now - 300) * 1000
        long_liq_usd = short_liq_usd = 0.0
        for item in data:
            for detail in item.get("details", []):
                ts = float(detail.get("ts", 0))
                if ts < five_min_ago_ms:
                    continue
                sz    = float(detail.get("sz",   0))
                bk_px = float(detail.get("bkPx", 0))
                usd   = sz * bk_px
                side  = detail.get("side", "")
                if side == "sell":       # long position force-closed → selling
                    long_liq_usd  += usd
                elif side == "buy":      # short position force-closed → buying
                    short_liq_usd += usd

        _tsunami_cache[coin] = {"long_usd": long_liq_usd, "short_usd": short_liq_usd, "ts": now}
        return long_liq_usd, short_liq_usd
    except Exception as e:
        logger.debug("[TSUNAMI] liquidation fetch %s: %s", coin, e)
        cached = _tsunami_cache.get(coin, {})
        return cached.get("long_usd", 0.0), cached.get("short_usd", 0.0)


# ── RAIJUU OI cache ──────────────────────────────────────────────────────────
# {coin: {"series": [float,...], "ts": float}}
_raijuu_cache: dict[str, dict] = {}
_RAIJUU_FETCH_COOLDOWN = 120   # seconds between API calls per coin

async def _fetch_raijuu_oi(coin: str) -> list[float]:
    """Return OI USD series (oldest-first, last 3 x 5m bars ~= 15 min)."""
    try:
        now    = time.time()
        cached = _raijuu_cache.get(coin, {})
        if cached and now - cached.get("ts", 0) < _RAIJUU_FETCH_COOLDOWN:
            return cached.get("series", [])

        resp, rate_bucket = await run_with_rate_admission(
            "GET", "/api/v5/rubik/stat/contracts/open-interest-volume",
            params={"ccy": coin, "period": "5m"}, priority="P2",
            pool_gate=_engine_http_gate,
            send=lambda: _engine_http.get(
                "https://www.okx.com/api/v5/rubik/stat/contracts/open-interest-volume",
                params={"ccy": coin, "period": "5m"},
            ),
        )
        if resp.status_code == 429:
            process_rest_rate_governor.record_http_429(rate_bucket)
        resp.raise_for_status()
        payload = resp.json()
        if str(payload.get("code", "")) == "50011":
            process_rest_rate_governor.record_business_50011(rate_bucket)
        rows = payload.get("data", [])

        series: list[float] = []
        for row in rows:
            # OKX returns [ts, oiCcy, volCcy24h] or [ts, oi, vol] — try index 1
            try:
                series.append(float(row[1]))
            except (IndexError, ValueError):
                pass

        # Oldest-first; take last 3 data points (~15 min)
        series = series[-3:] if len(series) >= 3 else series
        _raijuu_cache[coin] = {"series": series, "ts": now}
        return series
    except Exception as e:
        logger.debug("[RAIJUU] OI fetch %s: %s", coin, e)
        return _raijuu_cache.get(coin, {}).get("series", [])



# Global margin cooldown: when any engine hits "insufficient USDT balance",
# ALL engines pause new trade attempts for this many seconds.
# Prevents hundreds of identical errors spamming the log every ~80s.
_MARGIN_COOLDOWN_SECS = 300          # 5 minutes
_margin_cooldown_until: float = 0.0  # epoch timestamp

def _set_margin_cooldown() -> None:
    global _margin_cooldown_until
    _margin_cooldown_until = time.time() + _MARGIN_COOLDOWN_SECS
    logger.warning(
        "Margin exhausted — global cooldown active for %ds (until all engines pause new trades).",
        _MARGIN_COOLDOWN_SECS,
    )

def _is_margin_cooled_down() -> bool:
    return time.time() < _margin_cooldown_until


# ── Global per-coin loss tracker ──────────────────────────────────────────────
# Prevents ANY engine from re-entering a coin that has hit consecutive SL losses.
# After _COIN_LOSS_LIMIT losses within _COIN_LOSS_WINDOW seconds, the coin is
# globally blocked for _COIN_BLOCK_SECS across all engines.
_COIN_LOSS_LIMIT  = 2         # FIX 2026-03-27: lowered 3→2; PIPPIN hit SL twice without triggering block. 2 losses within window now triggers 2h global block.
_COIN_LOSS_WINDOW = 7200      # only count losses within last 2 hours
_COIN_BLOCK_SECS  = 7200      # block duration: 2 hours
_COIN_GUARD_PATH = Path("data/coin_guard_state.json")

_global_coin_losses: dict[str, list] = {}   # coin → [loss_timestamps]
_global_coin_block:  dict[str, float] = {}  # coin → blocked_until timestamp


# ── Single-SL global cooldown ──────────────────────────────────────────
# After ANY single SL hit on a coin, block ALL engines from re-entering
# that coin for 30 minutes.  This is separate from the 2-loss/2h block
# above — it fires on the FIRST loss, not after repeated losses.
_SINGLE_SL_COOLDOWN_SECS = 1800   # 30 minutes
_single_sl_cooldown: dict[str, float] = {}   # coin → cooldown_until


def _prune_coin_guard_state(now: float | None = None) -> None:
    now = time.time() if now is None else now
    for coin, until in list(_single_sl_cooldown.items()):
        if now >= float(until or 0):
            _single_sl_cooldown.pop(coin, None)
    for coin, losses in list(_global_coin_losses.items()):
        kept = [float(ts) for ts in losses if now - float(ts) < _COIN_LOSS_WINDOW]
        if kept:
            _global_coin_losses[coin] = kept
        else:
            _global_coin_losses.pop(coin, None)
    for coin, until in list(_global_coin_block.items()):
        if now >= float(until or 0):
            _global_coin_block.pop(coin, None)
            _global_coin_losses.pop(coin, None)


def _save_coin_guard_state() -> None:
    try:
        _prune_coin_guard_state()
        _COIN_GUARD_PATH.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "single_sl_cooldown": _single_sl_cooldown,
            "global_coin_losses": _global_coin_losses,
            "global_coin_block": _global_coin_block,
        }
        _COIN_GUARD_PATH.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    except Exception as e:
        logger.debug("Coin guard state save failed: %s", e)


def _load_coin_guard_state() -> None:
    global _single_sl_cooldown, _global_coin_losses, _global_coin_block
    if not _COIN_GUARD_PATH.exists():
        return
    try:
        raw = json.loads(_COIN_GUARD_PATH.read_text(encoding="utf-8"))
        _single_sl_cooldown = {
            str(coin).upper(): float(until)
            for coin, until in dict(raw.get("single_sl_cooldown", {})).items()
        }
        _global_coin_losses = {
            str(coin).upper(): [float(ts) for ts in losses]
            for coin, losses in dict(raw.get("global_coin_losses", {})).items()
        }
        _global_coin_block = {
            str(coin).upper(): float(until)
            for coin, until in dict(raw.get("global_coin_block", {})).items()
        }
        _prune_coin_guard_state()
        _save_coin_guard_state()
    except Exception as e:
        logger.warning("Coin guard state load failed: %s", e)


def _set_single_sl_cooldown(coin: str) -> None:
    """Set a 30-min global cooldown on a coin after a single SL."""
    _single_sl_cooldown[coin] = time.time() + _SINGLE_SL_COOLDOWN_SECS
    _save_coin_guard_state()
    logger.info("SingleSLCooldown: %s blocked for %dm after SL hit.",
                coin, _SINGLE_SL_COOLDOWN_SECS // 60)


def _is_single_sl_cooled(coin: str) -> bool:
    """Returns True if this coin is in single-SL cooldown."""
    until = _single_sl_cooldown.get(coin, 0.0)
    if time.time() < until:
        return True
    if coin in _single_sl_cooldown:
        del _single_sl_cooldown[coin]
        _save_coin_guard_state()
    return False

def _record_coin_loss(coin: str) -> None:
    """Track a loss on a coin. Block it globally if limit is reached."""
    now    = time.time()
    losses = _global_coin_losses.setdefault(coin, [])
    losses.append(now)
    # Prune losses outside the rolling window
    losses[:] = [t for t in losses if now - t < _COIN_LOSS_WINDOW]
    if len(losses) >= _COIN_LOSS_LIMIT:
        _global_coin_block[coin] = now + _COIN_BLOCK_SECS
        logger.warning(
            "GlobalCoinBlock: %s blocked for %dh after %d SL losses within %.0fmin.",
            coin, _COIN_BLOCK_SECS // 3600, len(losses), _COIN_LOSS_WINDOW / 60,
        )
    _save_coin_guard_state()


def _is_coin_globally_blocked(coin: str) -> bool:
    """Returns True if this coin is globally blocked across all engines."""
    blocked_until = _global_coin_block.get(coin, 0.0)
    if time.time() < blocked_until:
        return True
    # Block expired — clean up
    if coin in _global_coin_block:
        del _global_coin_block[coin]
        _global_coin_losses.pop(coin, None)
        _save_coin_guard_state()
        logger.info("GlobalCoinBlock: %s unblocked.", coin)
    return False


def _clear_coin_loss_track(coin: str) -> None:
    """Clear loss history on a winning trade — coin is rehabilitated."""
    _global_coin_losses.pop(coin, None)
    _global_coin_block.pop(coin, None)
    _single_sl_cooldown.pop(coin, None)
    _save_coin_guard_state()


_load_coin_guard_state()


def get_shared_okx() -> OKXClient:
    global _shared_okx
    if _shared_okx is None:
        _shared_okx = OKXClient()
        logger.info("Shared OKXClient singleton created for all engines.")
    return _shared_okx


class CategoryEngine:
    def __init__(self, name: str) -> None:
        self.name    = name.upper()
        self.emoji   = ENGINE_EMOJI.get(self.name, "🤖")
        self.okx     = get_shared_okx()
        self.running = False
        self._task: Optional[asyncio.Task] = None
        self._entry_lock = asyncio.Lock()
        self._open:      dict[str, dict] = {}
        self._cooldowns: dict[str, float] = {}
        # Shadow tracking: record hypothetical signals without placing orders
        self._shadow_signals: list[dict] = []
        # Anti-churn: track last failed trade per coin to prevent same-coin re-entry
        self._failed_coins: dict[str, float] = {}  # coin → timestamp of last failure
        self._FAILED_COIN_COOLDOWN = 3600  # 1 hour cooldown after failed trade on same coin
        # Dynamic cooldown: tracks whether last trade was a win or loss
        self._last_trade_was_win: bool = False
        logger.info("Engine %s %s initialised.", self.emoji, self.name)

    def is_enabled(self) -> bool:
        """True if this engine is enabled in any non-disabled mode."""
        return _s.is_engine_enabled(self.name)

    def is_trading_mode(self) -> bool:
        """True if this engine should place real orders (live or probation)."""
        return _s.is_engine_live_or_probation(self.name)

    def is_shadow_mode(self) -> bool:
        """True if this engine should only record hypothetical signals."""
        return _s.is_engine_shadow(self.name)

    def get_mode(self) -> str:
        return _s.get_engine_mode(self.name)

    def open_positions(self) -> dict: return dict(self._open)
    def trade_count(self) -> int:    return len(self._open)

    def _candidate_limit(self) -> int:
        if self.name == "TENSEI":
            return max(int(_s.get_val("tensei_reentry_limit", 24) or 24), 1)
        legacy = int(_s.get_val("full_universe_candidate_limit", 0) or 0)
        if legacy > 0:
            return max(legacy, 1)
        key = f"engine_candidate_limit_{self.name.lower()}"
        specific = int(_s.get_val(key, 0) or 0)
        if specific > 0:
            return max(specific, 1)
        default = int(_s.get_val("engine_candidate_limit_default", 18) or 18)
        return max(default, 0)

    def _is_major_lane_trade(self, coin: str) -> bool:
        if self.name in ("TITAN", "RONIN", "IZANAGI", "OROCHI", "HACHIMAN", "HAYATE", "KAGUTSUCHI", "YATAGARASU", "RYUJIN", "IBUKI", "ONRYO", "TENSEI"):
            return True
        return coin.upper() in set(get_top20_mcap())

    def _candidate_pool(self, universe: list, balance: float, include_open: bool = False) -> list[str]:
        if self.name == "TENSEI":
            if not bool(_s.get_val("safe_reentry_enabled", True)):
                return []
            recent = set(
                _perf.get_recent_closed_coins(
                    int(_s.get_val("tensei_reentry_window_mins", 240) or 240),
                    limit=self._candidate_limit(),
                )
            )
            candidates = [c for c in universe if c in recent]
        else:
            candidates = list(universe)

        if not include_open:
            candidates = [c for c in candidates if c not in self._open]
        candidates = rank_tradeable_candidates(candidates, balance_usdt=balance)
        limit = self._candidate_limit()
        if limit > 0:
            candidates = candidates[:limit]
        return candidates

    def _portfolio_gate(
        self,
        coin: str,
        direction: str,
        balance: float,
        candidate_size_usdt: float = 0.0,
        capital_profile=None,
        candidate_quality: float = 0.0,
    ) -> tuple[bool, str]:
        direction = direction.lower()
        same_dir_limit = int(_s.get_val("max_same_direction_positions", 4))
        try:
            _dbg = _dbg_snap()
            if _dbg_mode(_dbg.get("mode")) == "live_override":
                same_dir_limit = _dbg_adjust_portfolio_dir_cap(
                    same_dir_limit,
                    _dbg_level(_dbg.get("levels", {}) or {}, "portfolio_direction_cap"),
                )
        except Exception:
            pass
        if same_dir_limit > 0 and _reg_direction_count(direction) >= same_dir_limit:
            return False, f"portfolio direction cap hit for {direction}"

        major_reserved = int(_s.get_val("major_lane_reserved_slots", 0))
        global_max = int(_s.get_val("max_open_positions", 6))
        if (
            major_reserved > 0
            and not self._is_major_lane_trade(coin)
            and _reg_count() >= max(global_max - major_reserved, 0)
        ):
            return False, "major lane reserved slots held back for top-tier setups"

        from portfolio_allocator import check_same_direction_budget as _check_same_direction_budget
        _dbg_dir_level = "normal"
        try:
            _dbg = _dbg_snap()
            if _dbg_mode(_dbg.get("mode")) == "live_override":
                _dbg_dir_level = _dbg_level(_dbg.get("levels", {}) or {}, "same_direction_cap")
        except Exception:
            _dbg_dir_level = "normal"
        _dir_ok, _dir_reason = _check_same_direction_budget(
            direction,
            candidate_size_usdt=candidate_size_usdt,
            balance=balance,
            capital_profile=capital_profile,
            candidate_quality=candidate_quality,
            debug_override_level=_dbg_dir_level,
        )
        if not _dir_ok:
            return False, _dir_reason

        return True, "OK"

    async def _recheck_live_entry_constraints(
        self,
        coin: str,
        direction: str,
        balance: float,
        candidate_size_usdt: float,
        votes,
        conf_obj=None,
        capital_profile=None,
        candidate_quality: float = 0.0,
        soft_accept_active: bool = False,
        soft_accept_reason: str = "",
        soft_accept_mode: str = "",
        soft_accept_session: str = "",
        lane_override: bool = False,
    ) -> tuple[str, str]:
        direction_lc = str(direction or "").lower()
        _dbg = _dbg_snap()
        _dbg_mode_name = _dbg_mode(_dbg.get("mode"))
        _dbg_levels = _dbg.get("levels", {}) or {}
        _dbg_live = (_dbg_mode_name == "live_override")
        existing = _registry.get(coin.upper(), {})
        existing_dir = str(existing.get("direction", "") or "").lower()
        existing_engine = str(existing.get("engine", "") or "")

        if (
            _confirm_path_enabled()
            and existing
            and existing_dir == direction_lc
            and existing_engine.upper() != self.name.upper()
            and _within_duplicate_merge_window(existing)
        ):
            conf_val = float(
                conf_obj.confidence_score
                if conf_obj is not None and hasattr(conf_obj, "confidence_score")
                else votes
            )
            confirm_meta = await reg_confirm(
                coin,
                self.name,
                metadata={
                    "last_confirm_direction": direction_lc,
                    "last_confirm_reason": "same_coin_same_direction",
                    "last_confirm_confidence": conf_val,
                    "last_confirm_engine": self.name,
                },
            )
            if confirm_meta:
                logger.info(
                    "CONFIRM [%s] %s %s conf=%.2f | existing by %s confirmations=%d",
                    self.name,
                    coin,
                    direction.upper(),
                    conf_val,
                    confirm_meta.get("engine") or existing_engine,
                    int(confirm_meta.get("confirmation_count", 0) or 0),
                )
                return "confirm", "confirmed"

        if _per_engine_cap_enabled():
            engine_max = get_effective_engine_cap(
                self.name,
                balance=balance,
                capital_profile=capital_profile,
            )
            if _dbg_live:
                engine_max = _dbg_adjust_engine_cap(
                    int(engine_max or 1),
                    _dbg_level(_dbg_levels, "engine_cap"),
                    engine_name=self.name,
                )
            if _reg_engine_count(self.name) >= engine_max:
                return "engine_cap", f"{_reg_engine_count(self.name)}/{engine_max} positions open for {self.name}"

        if not lane_override:
            corr_ok, corr_reason = _correlation_check(
                coin,
                direction,
                balance=balance,
                capital_profile=capital_profile,
                candidate_quality=candidate_quality,
                debug_override_level=_dbg_level(_dbg_levels, "correlation_cap") if _dbg_live else "normal",
            )
            if not corr_ok:
                return "correlation", corr_reason

        effective_limit, slot_meta = _effective_global_slot_limit(
            balance,
            capital_profile=capital_profile,
            candidate_quality=candidate_quality,
        )
        if _dbg_live:
            effective_limit = _dbg_adjust_global_cap(int(effective_limit or 1), _dbg_level(_dbg_levels, "global_cap"))
        if _reg_count() >= effective_limit:
            return (
                "global_cap",
                f"max positions {_reg_count()}/{effective_limit} reached [{slot_meta.get('policy_mode', 'Adaptive')}]",
            )

        if not lane_override:
            from portfolio_allocator import check_lane_budget as _check_lane_budget
            lane_ok, lane_reason = _check_lane_budget(
                self.name,
                candidate_size_usdt,
                balance,
                capital_profile=capital_profile,
                soft_accept=soft_accept_active,
                session=soft_accept_session,
                debug_override_level=_dbg_level(_dbg_levels, "lane_budget") if _dbg_live else "normal",
            )
            if not lane_ok:
                if soft_accept_active:
                    lane_reason = (
                        f"{lane_reason} | soft_accept[{soft_accept_mode}/{soft_accept_session}] "
                        f"size ${candidate_size_usdt:.2f} via {soft_accept_reason}"
                    )
                return "lane_budget", lane_reason

        allowed_portfolio, reason_portfolio = self._portfolio_gate(
            coin,
            direction,
            balance,
            candidate_size_usdt=candidate_size_usdt,
            capital_profile=capital_profile,
            candidate_quality=candidate_quality,
        )
        if not allowed_portfolio:
            portfolio_tag = "headroom" if "headroom" in reason_portfolio.lower() else "portfolio"
            if soft_accept_active:
                reason_portfolio = (
                    f"{reason_portfolio} | soft_accept[{soft_accept_mode}/{soft_accept_session}] "
                    f"size ${candidate_size_usdt:.2f} via {soft_accept_reason}"
                )
            return portfolio_tag, reason_portfolio

        if _s.get_val("v6_enabled", False) and _s.get_val("v6_correlation_control_enabled", True):
            from exposure_tracker import check_exposure as _check_exposure
            max_exp_pct = capital_profile.max_exposure_pct if capital_profile else 60.0
            exp_ok, exp_reason = _check_exposure(
                coin=coin,
                direction=direction_lc,
                size_usdt=candidate_size_usdt,
                balance=balance,
                max_exposure_pct=max_exp_pct,
                capital_profile=capital_profile,
                candidate_quality=candidate_quality,
                debug_override_exposure_cap=_dbg_level(_dbg_levels, "exposure_cap") if _dbg_live else "normal",
                debug_override_correlation_cap=_dbg_level(_dbg_levels, "correlation_cap") if _dbg_live else "normal",
                debug_override_same_direction_cap=_dbg_level(_dbg_levels, "same_direction_cap") if _dbg_live else "normal",
            )
            if not exp_ok:
                if soft_accept_active:
                    exp_reason = (
                        f"{exp_reason} | soft_accept[{soft_accept_mode}/{soft_accept_session}] "
                        f"size ${candidate_size_usdt:.2f} via {soft_accept_reason}"
                    )
                return "exposure", exp_reason

        return "", "ok"

    async def start(self) -> str:
        if self.running:       return f"{self.emoji} {self.name} already running."
        mode = self.get_mode()
        if mode == "disabled" and not self.is_enabled():
            return f"{self.emoji} {self.name} disabled in settings."
        self.running = True
        mode_label = {"live": "LIVE", "probation": "PROBATION", "shadow": "SHADOW"}.get(mode, mode.upper())
        logger.info("%s %s started [%s].", self.emoji, self.name, mode_label)
        return f"{self.emoji} *{self.name}* engine started `[{mode_label}]`."

    async def stop(self) -> str:
        self.running = False
        if self._task and not self._task.done():
            self._task.cancel()
            try: await self._task
            except asyncio.CancelledError: pass
        self._task = None
        return f"{self.emoji} *{self.name}* engine stopped."

    async def close_all(self) -> list[dict]:
        return [await self.close_position(c, "manual_stop") for c in list(self._open)]

    async def close_position(self, coin: str, reason: str = "manual") -> dict:
        coin = coin.upper()
        registry_row = (_reg_details() or {}).get(coin)
        if registry_row and not _reg_mutation_actionable(registry_row):
            return {"ok": False, "reason": "local_exposure_not_mutation_actionable"}
        if coin not in self._open:
            return {"ok": False, "reason": f"{coin} not in {self.name}"}
        trade = self._open[coin]
        pos_side = "long" if trade["side"] == "buy" else "short"
        size = float(trade["size"]); entry = float(trade["entry"]); lev = int(trade.get("leverage", 5))
        try:
            from execution_router import router_for
            result = await router_for(self.okx).close(coin, pos_side, size)
            mutation_outcome = str(result.get("mutation_outcome", "") or "")
            if mutation_outcome and mutation_outcome != "SUCCESS":
                return {"ok": False, "reason": f"close_mutation_{mutation_outcome.lower()}"}
            raw_f   = result.get("fillPx", 0)
            exit_px = float(raw_f) if raw_f and float(raw_f) > 0 else entry
            pnl_pct = ((exit_px-entry)/entry*100) if trade["side"]=="buy" else ((entry-exit_px)/entry*100)
            # FIX: size_usdt is already NOTIONAL (leveraged) — do NOT multiply by lev again.
            # Old code: size*(pnl_pct/100)*lev caused 10-20x inflated PnL display.
            pnl     = round(size*(pnl_pct/100), 4)
            self._record_close(coin, trade, exit_px, pnl, reason)
            await alert_trade_closed(
                engine=self.name,
                coin=coin,
                direction=pos_side,
                entry_price=entry,
                exit_price=exit_px,
                size_usdt=size,
                pnl_usdt=pnl,
                exit_reason=reason,
                leverage=lev,
            )
            logger.info("[%s] CLOSE %s @ %.6f pnl=$%.4f lev=%dx (%s)", self.name, coin, exit_px, pnl, lev, reason)
            return {"ok": True, "coin": coin, "pnl": pnl, "exit_price": exit_px}
        except Exception as e:
            logger.error("[%s] close_position %s: %s", self.name, coin, e)
            return {"ok": False, "reason": str(e)}

    def _record_close(self, coin, trade, exit_px, pnl_usdt, reason) -> None:
        pos_side = "long" if trade["side"] == "buy" else "short"
        fee_usdt = fee_cost_usdt(float(trade["size"]), int(trade.get("leverage", 5) or 5))
        opened_at = str(trade.get("opened_at", "") or "")
        closed_at = str(trade.get("last_exit_fill_at", trade.get("closed_at", "")) or "")
        trade_meta = {
            k: trade.get(k)
            for k in (
                "position_strength",
                "trade_class",
                "preferred_exit_style",
                "contributor_roles",
                "role_quality_adjustment",
            )
            if k in trade
        }
        trade_meta.update(_engine_attribution.close_attribution(trade, float(pnl_usdt or 0.0)))
        entry_px = float(trade["entry"])
        size_usdt = float(trade["size"])
        try:
            reg_close(coin)
        except Exception as exc:
            logger.error("[%s] close cleanup registry release failed for %s: %s", self.name, coin, exc)
        try:
            _em.remove(coin)
        except Exception as exc:
            logger.error("[%s] close cleanup exit plan remove failed for %s: %s", self.name, coin, exc)
        self._cooldowns[coin] = time.time()
        self._open.pop(coin, None)
        # ── Global per-coin loss tracking ─────────────────────────────────────
        _is_sl_exit = any(r in reason for r in ("sl", "tsl"))
        if pnl_usdt < 0 or _is_sl_exit:
            _set_single_sl_cooldown(coin)   # Tier-1: 30m global cooldown on any SL
            _record_coin_loss(coin)
            _mark_symbol_direction_loss(coin, pos_side)
            # Anti-churn: record failed coin for this engine specifically
            self._failed_coins[coin] = time.time()
            self._last_trade_was_win = False
        else:
            _clear_coin_loss_track(coin)
            self._failed_coins.pop(coin, None)
            self._last_trade_was_win = True
        try:
            cat_record(engine=self.name, coin=coin, direction=pos_side,
                       entry=entry_px, exit_price=exit_px,
                       size_usdt=size_usdt, pnl_usdt=pnl_usdt,
                       vote_score=int(trade.get("vote_score",2)), exit_reason=reason,
                       fee_usdt=fee_usdt, opened_at=opened_at, closed_at=closed_at, metadata=trade_meta)
                       
        except Exception as exc:
            logger.error("[%s] category close accounting failed for %s: %s", self.name, coin, exc)
        try:
            _perf.record_trade(coin, pos_side, entry_px, exit_px,
                               size_usdt, pnl_usdt, reason,
                               fee_usdt=fee_usdt, opened_at=opened_at, closed_at=closed_at)
        except Exception as exc:
            logger.error("[%s] performance close accounting failed for %s: %s", self.name, coin, exc)
        try:
            _paper_validation.record_close(
                context=_paper_validation_close_context_from_trade(trade, coin),
                order_id=str(trade.get("order_id", "") or ""),
                pair=str(coin or "").upper(),
                exit_price=float(exit_px or 0.0),
                pnl_usdt=float(pnl_usdt or 0.0),
                fee_usdt=float(fee_usdt or 0.0),
                exit_reason=str(reason or ""),
            )
        except Exception as exc:
            logger.error("[%s] paper validation close telemetry failed for %s: %s", self.name, coin, exc)
        if os.getenv("PAPER_TRADING", "true").strip().lower() != "true":
            try:
                from pnl_reconciler import reconciler as _pnl_reconciler
                trade_id = str(trade.get("order_id", "") or f"{self.name}-{coin}-{opened_at}")
                _pnl_reconciler.record_provisional(
                    trade_id,
                    coin=coin,
                    engine=self.name,
                    direction=pos_side,
                    gross_price_pnl=float(trade.get("oxypulse_reported_gross_pnl", pnl_usdt) or 0.0),
                    entry_fee=-abs(float(fee_usdt or 0.0) / 2.0),
                    exit_fee=-abs(float(fee_usdt or 0.0) / 2.0),
                    local_estimated_net_pnl=float(trade.get("oxypulse_reported_final_pnl", float(pnl_usdt or 0.0) - float(fee_usdt or 0.0)) or 0.0),
                    oxypulse_reported_final_pnl=float(trade.get("oxypulse_reported_final_pnl", float(pnl_usdt or 0.0) - float(fee_usdt or 0.0)) or 0.0),
                    oxypulse_reported_gross_pnl=float(trade.get("oxypulse_reported_gross_pnl", pnl_usdt) or 0.0),
                    entry_notional_usdt=float(trade.get("initial_size_usdt", trade.get("size", 0.0)) or 0.0),
                    mfe_gross_pnl_usdt=float(trade.get("mfe_gross_pnl_usdt", 0.0) or 0.0),
                    mae_gross_pnl_usdt=float(trade.get("mae_gross_pnl_usdt", 0.0) or 0.0),
                    mfe_price=float(trade.get("mfe_price", 0.0) or 0.0),
                    mae_price=float(trade.get("mae_price", 0.0) or 0.0),
                    mfe_roi=float(trade.get("mfe_roi", 0.0) or 0.0),
                    mae_roi=float(trade.get("mae_roi", 0.0) or 0.0),
                    exit_timeline=list(trade.get("exit_timeline", []) or []),
                    partial_exit_events=list(trade.get("partial_exit_events", []) or []),
                    trail_semantic_reason=str(trade.get("trail_semantic_reason", "") or ""),
                    trail_activated_at=str(trade.get("trail_activated_at", "") or ""),
                    trail_activation_net_pnl=float(trade.get("trail_activation_net_pnl", 0.0) or 0.0),
                    trail_peak_net_pnl=float(trade.get("trail_peak_net_pnl", 0.0) or 0.0),
                    trail_exit_net_pnl=float(trade.get("trail_exit_net_pnl", 0.0) or 0.0),
                    profit_surrendered=float(trade.get("profit_surrendered", 0.0) or 0.0),
                    profit_lock_level=int(trade.get("profit_lock_level", 0) or 0),
                    breakeven_moved=bool(trade.get("breakeven_moved", False)),
                    tp1_done=bool(trade.get("tp1_done", False)),
                    tp2_done=bool(trade.get("tp2_done", False)),
                    post_invalidation_telemetry=dict(trade.get("post_invalidation_telemetry", {}) or {}),
                    exit_reason=str(reason or ""),
                    entry_order_id=str(trade.get("order_id", "") or ""),
                    entry_client_order_id=str(trade.get("entry_client_order_id", trade.get("entry_clOrdId", "")) or ""),
                    entry_clOrdId=str(trade.get("entry_client_order_id", trade.get("entry_clOrdId", "")) or ""),
                    entry_order_ids=list(trade.get("entry_order_ids", []) or [str(trade.get("order_id", "") or "")]),
                    exit_order_id=str(trade.get("exit_order_id", "") or ""),
                    exit_order_ids=list(trade.get("exit_order_ids", []) or []),
                    position_id=str(trade.get("position_id", trade.get("pos_id", "")) or ""),
                    first_fill_at=str(trade.get("first_fill_at", "") or ""),
                    last_entry_fill_at=str(trade.get("last_entry_fill_at", "") or ""),
                    first_exit_fill_at=str(trade.get("first_exit_fill_at", "") or ""),
                    last_exit_fill_at=str(trade.get("last_exit_fill_at", "") or ""),
                    inst_id=f"{str(coin or '').upper()}-USDT-SWAP",
                )
            except Exception as exc:
                logger.error("[%s] live provisional PnL storage failed for %s: %s", self.name, coin, exc)
        # ── V6: Adaptive engine weight update ────────────────────────────────
        if _s.get_val("v6_enabled", False) and _engine_weight_adaptation_enabled():
            try:
                from engine_weights import maybe_update_from_tracker as _v6_update_weight
                _v6_update_weight(self.name)
            except Exception:
                pass   # weight update failure must never affect trade close
        # ─────────────────────────────────────────────────────────────────────

    def notify_closed_by_exit_manager(self, coin, exit_price, pnl_usdt, exit_reason="tp", close_context: dict | None = None) -> bool:
        coin = coin.upper(); trade = self._open.get(coin)
        if not trade:
            return False
        if close_context:
            trade.update(dict(close_context))
            trade["paper_validation"] = {**dict(trade.get("paper_validation", {}) or {}), **dict(close_context)}
        self._record_close(coin, trade, exit_price, pnl_usdt, exit_reason)
        logger.info("[%s] ExitManager closed %s pnl=$%.4f (%s)", self.name, coin, pnl_usdt, exit_reason)
        return True

    def notify_closed_by_exchange(self, coin, exit_price, pnl_usdt, exit_reason="exchange_algo", close_context: dict | None = None) -> bool:
        coin = coin.upper(); trade = self._open.get(coin)
        if not trade:
            return False
        if close_context:
            trade.update(dict(close_context))
            trade["paper_validation"] = {**dict(trade.get("paper_validation", {}) or {}), **dict(close_context)}
        self._record_close(coin, trade, exit_price, pnl_usdt, exit_reason)
        if exit_reason in {"sync_exchange_closed", "sync_confirmed_close"}:
            try:
                asyncio.create_task(alert_trade_closed(
                    engine=self.name,
                    coin=coin,
                    direction="long" if trade.get("side") == "buy" else "short",
                    entry_price=float(trade.get("entry", 0.0) or 0.0),
                    exit_price=exit_price,
                    size_usdt=float(trade.get("size", 0.0) or 0.0),
                    pnl_usdt=pnl_usdt,
                    exit_reason=exit_reason,
                    leverage=int(trade.get("leverage", 5) or 5),
                    via_exchange=True,
                ))
            except Exception:
                pass
        logger.info("[%s] Exchange algo closed %s @ %.6f pnl=$%.4f", self.name, coin, exit_price, pnl_usdt)
        return True

    def restore_position(self, coin: str, details: dict) -> bool:
        coin = coin.upper()
        direction = str(details.get("direction", "long")).lower()
        side = "buy" if direction == "long" else "sell"
        entry = float(details.get("entry", 0.0) or 0.0)
        size = float(details.get("size_usdt", 0.0) or 0.0)
        from position_registry import exposure_status as _registry_exposure_status
        exposure_status = _registry_exposure_status(details)
        if exposure_status.upper() == "EXPOSURE_UNKNOWN":
            logger.warning("[%s] Skipping speculative restore for unresolved exposure %s", self.name, coin)
            return False
        if exposure_status.upper() == "NON_ACTIONABLE_TERMINAL":
            logger.info("[%s] Skipping terminal historical restore for %s", self.name, coin)
            return False
        if exposure_status.upper() not in {"ACTIVE_POSITION", "EXCHANGE_CLOSED_AWAITING_RECONCILIATION"}:
            logger.warning("[%s] Skipping restore for non-actionable exposure status=%s coin=%s", self.name, exposure_status, coin)
            return False
        closed_handoff = exposure_status == "EXCHANGE_CLOSED_AWAITING_RECONCILIATION"
        if entry <= 0 or (size <= 0 and not closed_handoff):
            logger.warning("[%s] Skipping invalid restore for %s (entry=%.6f size=%.2f)", self.name, coin, entry, size)
            return False
        leverage = int(details.get("leverage", _s.get_val("leverage_default", 10)) or _s.get_val("leverage_default", 10))
        effective_leverage = int(details.get("position_effective_leverage", 0) or 0)
        if effective_leverage > 0:
            leverage_truth_status = "POSITION_EFFECTIVE"
        elif os.getenv("PAPER_TRADING", "true").strip().lower() == "true":
            leverage_truth_status = "PAPER_LOCAL"
        else:
            # Legacy live rows without position-effective evidence are
            # deliberately non-authoritative until exchange truth is read.
            leverage_truth_status = "LEVERAGE_UNKNOWN"
        stop_loss = float(details.get("stop_loss", 0.0) or 0.0)
        take_profit = float(details.get("take_profit", 0.0) or 0.0)
        tp2_price = float(details.get("tp2_price", details.get("tp2", take_profit)) or take_profit or 0.0)
        atr_val = float(details.get("atr", 0.0) or 0.0)
        order_id = str(details.get("order_id", "") or "")
        from tp_ownership import (
            normalize_persisted_owner,
            normalize_tp_mode,
            resolve_legacy_owner,
        )
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
        self._open[coin] = {
            "side": side,
            "entry": entry,
            "stop_loss": stop_loss,
            "take_profit": take_profit,
            "tp2": tp2_price,
            "size": size,
            "leverage": leverage,
            "atr": atr_val,
            "vote_score": float(details.get("vote_score", 2) or 2),
            "order_id": order_id,
            "engine": self.name,
            "requested_coin": details.get("requested_coin", coin),
            "recovery_mode": str(details.get("recovery_mode", "managed") or "managed"),
            "opened_at": str(details.get("opened_at", "") or ""),
            "expected_risk_usdt": float(details.get("expected_risk_usdt", 0.0) or 0.0),
            "risk_cap_usdt": float(details.get("risk_cap_usdt", 0.0) or 0.0),
            "risk_ceiling_action": str(details.get("risk_ceiling_action", "") or ""),
            "risk_sizing": dict(details.get("risk_sizing", {}) or {}),
            "tp_owner": tp_owner,
            "tp_mode_at_entry": tp_mode_at_entry,
            "partials_enabled_at_entry": details.get("partials_enabled_at_entry", details.get("partial_exit_enabled")),
            "restored": True,
        }
        _em.register(
            coin,
            direction,
            entry,
            size,
            atr_val,
            stop_loss,
            take_profit,
            tp2_price,
            order_id,
            client_order_id=str(details.get("entry_client_order_id", details.get("entry_clOrdId", "")) or ""),
            position_lifecycle_id=str(details.get("position_lifecycle_id", "") or ""),
            engine_name=self.name,
            vote_score=float(details.get("vote_score", 2) or 2),
            leverage=leverage,
            requested_leverage=(
                int(details.get("requested_leverage", leverage) or leverage)
            ),
            exchange_configured_leverage=(
                int(details.get("exchange_configured_leverage", 0) or 0) or None
            ),
            position_effective_leverage=(
                int(details.get("position_effective_leverage", 0) or 0) or None
            ),
            leverage_truth_status=leverage_truth_status,
            leverage_truth_timestamp=str(details.get("leverage_truth_timestamp", "") or ""),
            leverage_truth_source=str(details.get("leverage_truth_source", "") or ""),
            leverage_truth_mismatch=bool(details.get("leverage_truth_mismatch", False)),
            requested_vs_configured_mismatch=bool(details.get("requested_vs_configured_mismatch", details.get("leverage_truth_mismatch", False))),
            configured_vs_effective_mismatch=bool(details.get("configured_vs_effective_mismatch", False)),
            macro_runner_trust_mult=float(details.get("macro_runner_trust_mult", 1.0) or 1.0),
            expected_risk_usdt=float(details.get("expected_risk_usdt", 0.0) or 0.0),
            risk_cap_usdt=float(details.get("risk_cap_usdt", 0.0) or 0.0),
            risk_ceiling_action=str(details.get("risk_ceiling_action", "") or ""),
            paper_validation=dict(details.get("paper_validation", {}) or {}),
            partial_exit_state=dict(details.get("partial_exit_state", {}) or {}),
            partial_exit_events=list(details.get("partial_exit_events", []) or [])[-100:],
            entry_contracts=float(details.get("entry_contracts", 0.0) or 0.0),
            confirmed_remaining_contracts=float(details.get("confirmed_remaining_contracts", 0.0) or 0.0),
            exchange_position_timestamp=str(details.get("exchange_position_timestamp", "") or ""),
            exposure_truth_status=str(details.get("exposure_truth_status", "ACTIVE_POSITION") or "ACTIVE_POSITION"),
            closure_handoff_at=str(details.get("closure_handoff_at", "") or ""),
            partial_attribution_status=str(details.get("partial_attribution_status", "NONE") or "NONE"),
            partial_realized_pnl_usdt=float(details.get("partial_realized_pnl_usdt", 0.0) or 0.0),
            partial_fees_usdt=float(details.get("partial_fees_usdt", 0.0) or 0.0),
            entry_fee_usdt=(
                None if details.get("entry_fee_usdt") in {None, ""}
                else float(details.get("entry_fee_usdt") or 0.0)
            ),
            funding_usdt=(
                None if details.get("funding_usdt") in {None, ""}
                else float(details.get("funding_usdt") or 0.0)
            ),
            funding_status=str(details.get("funding_status", "UNKNOWN") or "UNKNOWN"),
            partial_mode_override=(
                str(details.get("exit_management_mode", "partial") or "partial") == "partial"
            ),
            tp_owner=tp_owner,
            tp_mode_at_entry=tp_mode_at_entry,
            exchange_tp_algo_id=str(details.get("exchange_tp_algo_id", "") or ""),
            local_tp1_enabled=details.get("local_tp1_enabled"),
            local_tp2_enabled=details.get("local_tp2_enabled"),
            ownership_timestamp_value=str(details.get("ownership_timestamp", "") or ""),
            tp_owner_timestamp_value=str(details.get("tp_owner_timestamp", details.get("ownership_timestamp", "")) or ""),
            partials_enabled_at_entry=details.get("partials_enabled_at_entry", details.get("partial_exit_enabled")),
        )
        restored_plan = _em.get_plan(coin)
        if restored_plan is not None:
            restored_plan.mfe_price = float(details.get("mfe_price", entry) or entry)
            restored_plan.mae_price = float(details.get("mae_price", entry) or entry)
            restored_plan.mfe_gross_pnl_usdt = float(details.get("mfe_gross_pnl_usdt", 0.0) or 0.0)
            restored_plan.mae_gross_pnl_usdt = float(details.get("mae_gross_pnl_usdt", 0.0) or 0.0)
            restored_plan.mfe_raw_move_pct = float(details.get("mfe_raw_move_pct", 0.0) or 0.0)
            restored_plan.mae_raw_move_pct = float(details.get("mae_raw_move_pct", 0.0) or 0.0)
            restored_plan.mfe_observed_leverage = int(details.get("mfe_observed_leverage", 0) or 0)
            restored_plan.mae_observed_leverage = int(details.get("mae_observed_leverage", 0) or 0)
            restored_plan.mfe_roi = float(details.get("mfe_roi", 0.0) or 0.0)
            restored_plan.mae_roi = float(details.get("mae_roi", 0.0) or 0.0)
            restored_plan.mfe_current_roi = _em._current_mfe_roi(restored_plan)
            restored_plan.mae_current_roi = _em._current_mae_roi(restored_plan)
        logger.info(
            "[%s] Restored %s from registry sync entry=%.6f size=$%.2f lev=%dx TP1=%.6f TP2=%.6f",
            self.name, coin, entry, size, leverage, take_profit, tp2_price,
        )
        return True

    async def evaluate_all(self, universe: list, balance: float) -> None:
        if not self.running or not self.is_enabled(): return
        # Shadow engines still evaluate but never place orders
        if self.is_shadow_mode():
            await self._evaluate_shadow(universe, balance)
            return
        if _is_margin_cooled_down(): return
        limit_hit, _ = risk.is_daily_limit_hit(balance)
        if limit_hit: return
        _perf.update_equity_peak(balance)
        candidates = self._candidate_pool(universe, balance, include_open=False)
        if not candidates:
            logger.debug("[%s] No candidates after filtering.", self.name)
            return
        _scan_started = time.monotonic()
        _raw_universe_count = len(universe or [])
        if bool(_s.get_val("candidate_quality_summary_enabled", True)):
            logger.info(
                "ENGINE SCAN [%s] ranked candidates=%d/%d limit=%d",
                engine_log_name(self.name),
                len(candidates),
                _raw_universe_count,
                self._candidate_limit(),
            )
        else:
            logger.debug("[%s] Evaluating %d unified-universe candidates.", self.name, len(candidates))
        if self.name == "ENMA":
            _enma_started = time.monotonic()
            await self._evaluate_enma_cycle(candidates, balance)
            _latency.record_engine_summary(
                self.name,
                len(candidates),
                time.monotonic() - _enma_started,
            )
            return
        lane_override_candidates: list[dict] = []
        eval_errors = 0
        batch_size = max(
            1,
            min(int(_s.get_val("engine_eval_cooperative_batch_size", 4) or 4), 30),
        )
        slow_symbol_samples: list[tuple[float, str]] = []

        async def _timed_eval(coin: str):
            started = time.monotonic()
            try:
                result = await self._evaluate_coin(coin, balance)
            except Exception as exc:
                return coin, exc, time.monotonic() - started
            return coin, result, time.monotonic() - started

        for offset in range(0, len(candidates), batch_size):
            batch = candidates[offset: offset + batch_size]
            batch_number = (offset // batch_size) + 1
            batch_started = time.monotonic()
            batch_results = await asyncio.gather(
                *[_timed_eval(c) for c in batch],
                return_exceptions=True,
            )
            batch_elapsed = time.monotonic() - batch_started
            batch_slowest_symbol = ""
            batch_slowest_duration = 0.0
            for item in batch_results:
                if isinstance(item, Exception):
                    eval_errors += 1
                    logger.error("[%s] evaluate_all batch wrapper: %s", self.name, item)
                    continue
                coin, result, elapsed = item
                _latency.record_symbol(self.name, coin, elapsed)
                if float(elapsed) >= batch_slowest_duration:
                    batch_slowest_symbol = str(coin)
                    batch_slowest_duration = float(elapsed)
                if elapsed >= 0.05:
                    slow_symbol_samples.append((elapsed, str(coin)))
                if isinstance(result, Exception):
                    eval_errors += 1
                    logger.error("[%s] evaluate_all batch %s: %s", self.name, coin, result)
                elif isinstance(result, dict) and result.get("lane_override_candidate"):
                    lane_override_candidates.append(result["lane_override_candidate"])
            _latency.record_batch(
                self.name,
                batch_number,
                len(batch),
                batch_elapsed,
                batch_slowest_symbol,
                batch_slowest_duration,
            )
            if offset + batch_size < len(candidates):
                await asyncio.sleep(0)
        if slow_symbol_samples:
            slow_symbol_samples.sort(reverse=True)
            top = slow_symbol_samples[:5]
            logger.info(
                "ENGINE SLOW SYMBOLS [%s] %s",
                engine_log_name(self.name),
                ", ".join(f"{coin}={elapsed:.3f}s" for elapsed, coin in top),
            )
            try:
                self._last_slow_symbols = [
                    {"symbol": coin, "duration_sec": round(elapsed, 3), "engine": self.name}
                    for elapsed, coin in top
                ]
            except Exception:
                pass
        if lane_override_candidates:
            await self._process_lane_override_candidates(lane_override_candidates)
        _latency.record_engine_summary(
            self.name,
            len(candidates),
            time.monotonic() - _scan_started,
            error=f"evaluation_errors={eval_errors}" if eval_errors else "",
        )
        if bool(_s.get_val("candidate_quality_summary_enabled", True)):
            logger.info(
                "ENGINE SCAN SUMMARY [%s] evaluated=%d lane_override=%d errors=%d elapsed=%.2fs",
                engine_log_name(self.name),
                len(candidates),
                len(lane_override_candidates),
                eval_errors,
                time.monotonic() - _scan_started,
            )

    async def _process_lane_override_candidates(self, candidates: list[dict]) -> None:
        if not candidates:
            return
        ranked = sorted(candidates, key=_lane_override_rank_key)
        best = ranked[0]
        best_coin = str(best.get("coin", "") or "").upper()
        best_conf = float(best.get("confidence", 0.0) or 0.0)
        for candidate in ranked[1:]:
            _log_lane_override_block(
                self.name,
                str(candidate.get("coin", "") or ""),
                str(candidate.get("direction", "") or ""),
                float(candidate.get("confidence", 0.0) or 0.0),
                f"not_top_ranked_in_cycle: best={best_coin} conf={best_conf:.2f}",
            )
        await self._execute_lane_override_candidate(best)

    async def _execute_lane_override_candidate(self, candidate: dict) -> None:
        coin = str(candidate.get("coin", "") or "").upper()
        direction = str(candidate.get("direction", "") or "").upper()
        side = str(candidate.get("side", "") or "").lower()
        balance = float(candidate.get("balance", 0.0) or 0.0)
        votes = float(candidate.get("votes", 0.0) or 0.0)
        conf = float(candidate.get("confidence", 0.0) or 0.0)
        reserved_size_usdt = float(candidate.get("reserved_size_usdt", 0.0) or 0.0)
        candidate_quality = float(candidate.get("quality_score", 0.0) or 0.0)
        soft_accept_reason = str(candidate.get("soft_accept_reason", "") or "")
        soft_accept_mode = str(candidate.get("soft_accept_mode", "") or "")
        soft_accept_session = str(candidate.get("soft_accept_session", "") or "")

        async with self._entry_lock:
            override_ok, override_reason = _lane_override_precheck(
                balance,
                candidate.get("v6_profile"),
                conf,
                soft_accept_session,
            )
            if not override_ok:
                _log_lane_override_block(self.name, coin, direction, conf, override_reason)
                return

            live_tag, live_reason = await self._recheck_live_entry_constraints(
                coin,
                direction,
                balance,
                reserved_size_usdt,
                votes,
                conf_obj=candidate.get("v6_conf"),
                capital_profile=candidate.get("v6_profile"),
                candidate_quality=candidate_quality,
                soft_accept_active=bool(soft_accept_reason),
                soft_accept_reason=soft_accept_reason,
                soft_accept_mode=soft_accept_mode,
                soft_accept_session=soft_accept_session,
                lane_override=True,
            )
            if live_tag == "confirm":
                _log_lane_override_block(self.name, coin, direction, conf, f"confirm_only: {live_reason}")
                return
            if live_tag:
                _log_lane_override_block(self.name, coin, direction, conf, f"{live_tag}: {live_reason}")
                return

            allowed, reason = await can_open_and_reserve(
                coin,
                direction.lower(),
                self.name,
                reserved_size_usdt,
                metadata={
                    "pending_reservation": True,
                    "reserved_notional_usdt": round(reserved_size_usdt, 4),
                    "reserved_at": time.time(),
                    "lane_override_active": True,
                    "lane_override_reason": "lane_budget",
                    "lane_override_confidence": round(conf, 4),
                    **_projected_risk_reserve_metadata(candidate.get("size_preview")),
                },
                balance_usdt=float(balance or 0.0),
            )
            if not allowed:
                _log_lane_override_block(self.name, coin, direction, conf, f"reserve: {reason}")
                return

            _log_lane_override_pick(self.name, coin, direction, conf)

            _handoff_key = f"{self.name}:{coin}:{direction.lower()}"
            _handoff_ok = _execution_supervisor.handoff(
                _handoff_key,
                self._place_trade(
                coin,
                side,
                balance,
                votes,
                candidate.get("closes") or [],
                candidate.get("highs") or [],
                candidate.get("lows") or [],
                candidate.get("volumes") or [],
                _v6_conf=candidate.get("v6_conf"),
                _v6_dd=candidate.get("v6_dd"),
                _v6_gate=candidate.get("v6_gate"),
                _v6_profile=candidate.get("v6_profile"),
                _soft_accept_mult=float(candidate.get("soft_accept_mult", 1.0) or 1.0),
                _soft_accept_reason=soft_accept_reason,
                _soft_accept_mode=soft_accept_mode,
                _soft_accept_session=soft_accept_session,
                _weekly_cap_mult=float(candidate.get("weekly_cap_mult", 1.0) or 1.0),
                _weekly_cap_reason=str(candidate.get("weekly_cap_reason", "") or ""),
                _weekly_cap_decision=str(candidate.get("weekly_cap_decision", "ACCEPT") or "ACCEPT"),
                _candidate_quality=float(candidate.get("quality_score", 0.0) or 0.0),
                _lane_override=True,
                _size_preview=candidate.get("size_preview"),
                ),
                engine=self.name,
                coin=coin,
                direction=direction.lower(),
                lane_override=True,
            )
            if not _handoff_ok:
                reg_release_pre_submit(coin)
                _log_lane_override_block(self.name, coin, direction, conf, "execution_handoff_duplicate")
            else:
                # Start the independent worker immediately without coupling
                # its remaining lifetime to this engine scan coroutine.
                await asyncio.sleep(0)
                await asyncio.sleep(0)

    async def _preview_enma_candidate(self, coin: str, balance: float = 0.0) -> dict | None:
        if candle_count(coin) < MIN_CANDLES:
            return None
        closes = get_closes(coin)
        highs = get_highs(coin)
        lows = get_lows(coin)
        volumes = get_volumes(coin)
        if not closes or len(closes) < MIN_CANDLES:
            return None

        closes_15m = get_15m_closes(coin)
        closes_1h = get_1h_closes(coin)
        direction, votes = evaluate(
            "ENMA",
            closes,
            highs,
            lows,
            volumes,
            closes_15m=closes_15m,
            closes_1h=closes_1h,
            coin=coin,
        )
        if direction == "HOLD":
            return None

        min_votes = float(_s.get_val("enma_min_votes", _s.get_val("min_vote_score", 3.0)))
        if float(votes or 0.0) < min_votes:
            return None
        if _rjlog.should_skip_candidate(self.name, coin, direction):
            return None

        preference = get_universe_preference(coin, balance_usdt=balance)
        volume_quality = float(preference.get("volume_quality", 0.0) or 0.0)
        spread_pct = float(preference.get("spread_pct", 0.0) or 0.0)
        spread_quality = float(preference.get("spread_quality", 0.0) or 0.0)
        penalty = float(preference.get("selection_penalty", 0.0) or 0.0)
        market_cap_bonus = float(preference.get("market_cap_bonus", 0.0) or 0.0)
        _consensus.record_signal("ENMA", coin, direction, float(votes or 0.0))
        consensus = _consensus.get_snapshot(coin, direction)
        consensus_bonus = float(consensus.agreement_bonus or 0.0)
        impulse_pct = 0.0
        if len(closes) >= 6 and closes[-6] > 0:
            impulse_pct = abs((closes[-1] - closes[-6]) / closes[-6]) * 100.0
        score = (
            float(votes or 0.0) * 100.0
            + min(volume_quality, 8.0) * 6.0
            + spread_quality * 12.0
            + market_cap_bonus
            + consensus_bonus
            + impulse_pct
            - penalty
        )
        return {
            "coin": coin.upper(),
            "direction": str(direction or "").upper(),
            "votes": float(votes or 0.0),
            "score": round(score, 4),
            "volume_quality": round(volume_quality, 3),
            "spread_pct": round(spread_pct, 4),
            "spread_quality": round(spread_quality, 3),
            "selection_penalty": round(penalty, 3),
            "market_cap_bonus": round(market_cap_bonus, 3),
            "consensus_bonus": round(consensus_bonus, 3),
            "consensus_count": int(consensus.agreement_count or 0),
            "impulse_pct": round(impulse_pct, 3),
        }

    async def _evaluate_enma_cycle(self, candidates: list[str], balance: float) -> None:
        previews = []
        for coin in candidates[: max(int(_s.get_val("enma_top_pick_limit", 1) or 1) * 6, 6)]:
            try:
                preview = await self._preview_enma_candidate(coin, balance)
            except Exception as exc:
                logger.debug("[ENMA] preview %s failed: %s", coin, exc)
                preview = None
            if preview:
                previews.append(preview)

        if not previews:
            return

        ranked = sorted(
            previews,
            key=lambda item: (
                -float(item.get("score", 0.0) or 0.0),
                -float(item.get("votes", 0.0) or 0.0),
                str(item.get("coin", "") or "").upper(),
            ),
        )
        best = ranked[0]
        logger.info(
            "ENMA PICK [%s] %s %s conf=%.2f | score=%.2f vol=%.2fx spread=%.3f%% cap=%.1f penalty=%.1f impulse=%.2f%%",
            engine_log_name(self.name),
            best["coin"],
            best["direction"],
            best["votes"],
            best["score"],
            best["volume_quality"],
            best["spread_pct"],
            best["market_cap_bonus"],
            best["selection_penalty"],
            best["impulse_pct"],
        )
        for alt in ranked[1:4]:
            logger.info(
                "ENMA BLOCK [%s] %s %s conf=%.2f | reason=deprioritized_by_cluster_rank: best=%s score=%.2f penalty=%.1f",
                engine_log_name(self.name),
                alt["coin"],
                alt["direction"],
                alt["votes"],
                best["coin"],
                best["score"],
                alt["selection_penalty"],
            )
        await self._evaluate_coin(best["coin"], balance)

    async def _evaluate_shadow(self, universe: list, balance: float) -> None:
        """Shadow mode: evaluate coins and record hypothetical signals without placing orders."""
        candidates = self._candidate_pool(universe, balance, include_open=True)

        for coin in candidates:
            try:
                if candle_count(coin) < MIN_CANDLES:
                    continue
                closes = get_closes(coin); highs = get_highs(coin)
                lows = get_lows(coin); volumes = get_volumes(coin)
                if not closes or len(closes) < MIN_CANDLES:
                    continue
                try:
                    import shadow_rehab_tracker as _shadow_rehab
                    _shadow_rehab.update_market_price(
                        coin,
                        float(closes[-1]),
                        high=float(highs[-1]) if highs else None,
                        low=float(lows[-1]) if lows else None,
                    )
                except Exception:
                    pass

                closes_4h = get_4h_closes(coin) if self.name in ("RYUU","SHOGUN","TENJIN","IZANAGI","RONIN","OROCHI","HAYATE") else None
                closes_15m = get_15m_closes(coin) if self.name in ("AMATERASU", "IZANAGI", "OROCHI", "ENMA", "HAYATE", "HACHIMAN", "YATAGARASU") else None
                closes_1h = get_1h_closes(coin) if self.name in ("AMATERASU", "SUIJIN", "ENMA", "HAYATE", "HACHIMAN", "YATAGARASU") else None
                funding = get_cached_funding_rate(coin) if funding_is_fresh(coin) else 0.0
                taker_buy_vol = taker_sell_vol = 0.0
                if self.name in ("FUJIN", "KAGUTSUCHI", "YATAGARASU", "RYUJIN"):
                    try:
                        tv = await self.okx.get_taker_volume(coin)
                        taker_buy_vol = float(tv.get("buy", 0.0))
                        taker_sell_vol = float(tv.get("sell", 0.0))
                    except Exception:
                        pass
                raijuu_oi_series: list[float] = []
                if self.name in ("RAIJUU", "ONRYO"):
                    try:
                        raijuu_oi_series = await _fetch_raijuu_oi(coin)
                    except Exception:
                        pass

                if self.name == "HAYATE" and (
                    len(closes_4h or []) < 8
                    or len(closes_15m or []) < 8
                    or len(closes_1h or []) < 8
                ):
                    continue
                if self.name == "HACHIMAN" and (len(closes_15m or []) < 55 or len(closes_1h or []) < 55):
                    continue
                if self.name == "YATAGARASU" and len(closes_15m or []) < 80:
                    continue
                if self.name == "ONRYO" and (
                    not funding_is_fresh(coin)
                    or len(raijuu_oi_series or []) < 6
                ):
                    continue

                direction, votes = evaluate(
                    self.name, closes, highs, lows, volumes,
                    closes_4h=closes_4h, closes_15m=closes_15m,
                    closes_1h=closes_1h, funding_rate=funding,
                    taker_buy_vol=taker_buy_vol, taker_sell_vol=taker_sell_vol,
                    raijuu_oi_series=raijuu_oi_series,
                    coin=coin,
                )
                if direction == "HOLD":
                    continue

                _engine_min_votes = _s.get_val(f"{self.name.lower()}_min_votes", None)
                min_votes = float(_engine_min_votes) if _engine_min_votes is not None else float(_s.get_val("min_vote_score", 3.0))
                if votes < min_votes:
                    skip_reason = f"votes {votes:.1f} < {min_votes:.1f}"
                else:
                    skip_reason = "shadow_mode"

                contribution_recorded = False
                if votes >= _role_contribution_min_votes(self.name, min_votes):
                    try:
                        _consensus.record_signal(self.name, coin, direction, float(votes or 0.0))
                        _shadow_quality = _candidate_quality_snapshot(
                            self.name,
                            coin,
                            direction,
                            balance,
                            float(votes or 0.0),
                            float(votes or 0.0),
                        )
                        _shadow_ctx = _position_strength_context(
                            self.name,
                            coin,
                            direction,
                            _shadow_quality,
                            float(votes or 0.0),
                        )
                        contribution_recorded = True
                        _record_decision_trace(
                            engine=self.name,
                            coin=coin,
                            direction=direction,
                            confidence=float(votes or 0.0),
                            decision="shadow_contribution",
                            reason=skip_reason,
                            raw_score=float(votes or 0.0),
                            balance=balance,
                            candidate_quality=float(_shadow_quality.get("quality_score", 0.0) or 0.0),
                            regime=str(_shadow_quality.get("regime", "") or ""),
                            selector_influence=float(_shadow_ctx.get("selector_score", 0.0) or 0.0),
                            notes=(
                                f"role={_shadow_ctx.get('engine_role')} "
                                f"consensus={int(_shadow_ctx.get('consensus_count', 1) or 1)} "
                                f"strength={float(_shadow_ctx.get('position_strength', 0.0) or 0.0):.2f}"
                            ),
                            extra={"role_context": {k: v for k, v in _shadow_ctx.items() if k != "performance"}},
                        )
                        if bool(_s.get_val("shadow_rehab_enabled", True)):
                            import shadow_rehab_tracker as _shadow_rehab
                            if self.name == "IZANAGI":
                                _shadow_rehab.record_izanagi_contribution(
                                    symbol=coin,
                                    side=direction,
                                    entry_price=float(closes[-1] if closes else 0.0),
                                    confidence=float(votes or 0.0),
                                    quality=float(_shadow_quality.get("quality_score", 0.0) or 0.0),
                                    advisor_strength=float(_shadow_ctx.get("position_strength", 0.0) or 0.0),
                                    consensus_count=int(_shadow_ctx.get("consensus_count", 0) or 0),
                                    role_context={k: v for k, v in _shadow_ctx.items() if k != "performance"},
                                )
                            else:
                                _shadow_rehab.record_shadow_signal(
                                    engine=self.name,
                                    symbol=coin,
                                    side=direction,
                                    entry_price=float(closes[-1] if closes else 0.0),
                                    confidence=float(votes or 0.0),
                                    quality=float(_shadow_quality.get("quality_score", 0.0) or 0.0),
                                    advisor_strength=float(_shadow_ctx.get("position_strength", 0.0) or 0.0),
                                    consensus_count=int(_shadow_ctx.get("consensus_count", 0) or 0),
                                    role_context={k: v for k, v in _shadow_ctx.items() if k != "performance"},
                                )
                    except Exception:
                        logger.debug("[%s] shadow contribution trace failed for %s", self.name, coin)

                # Record shadow signal
                shadow_rec = {
                    "ts": time.time(),
                    "coin": coin,
                    "direction": direction,
                    "engine": self.name,
                    "votes": votes,
                    "skip_reason": skip_reason,
                    "contribution_recorded": contribution_recorded,
                    "price": closes[-1] if closes else 0.0,
                }
                self._shadow_signals.append(shadow_rec)
                # Keep only last 200 shadow signals
                if len(self._shadow_signals) > 200:
                    self._shadow_signals = self._shadow_signals[-200:]

                # Update shadow stats in settings
                if skip_reason == "shadow_mode":
                    _shadow_trades_key = f"engine_{self.name.lower()}_shadow_trades"
                    _s.bump_counter(_shadow_trades_key, 1)

                logger.debug("[%s] SHADOW signal: %s %s votes=%.1f (%s)",
                             self.name, direction, coin, votes, skip_reason)
            except Exception:
                logger.debug("[%s] shadow eval %s failed", self.name, coin)

    def get_shadow_signals(self, n: int = 20) -> list[dict]:
        """Return last N shadow signals for this engine."""
        return list(self._shadow_signals[-n:])

    def status_dict(self) -> dict:
        """Return engine status for Telegram display."""
        from category_tracker import get_engine_stats
        stats = get_engine_stats(self.name)
        return {
            "name": self.name,
            "emoji": self.emoji,
            "running": self.running,
            "enabled": self.is_enabled(),
            "mode": self.get_mode(),
            "positions": len(self._open),
            "today_wins": stats.get("today_wins", 0),
            "today_losses": stats.get("today_losses", 0),
            "today_pnl": stats.get("today_pnl", 0.0),
            "shadow_signals": len(self._shadow_signals),
        }

    async def _evaluate_coin(self, coin: str, balance: float) -> None:
        engine_started_at = time.time()
        try:
            _research_funnel.record("context_offered", engine=self.name, symbol=coin)
            # ── Regime gating (lane-based) ────────────────────────────────────
            # Each engine belongs to a lane. The lane defines which regimes are
            # allowed. This replaces the old hardcoded name lists.
            _cur_regime = _regime.get_regime()
            if _cur_regime == _regime.DEAD:
                # No liquidity — all engines pause.
                _bump_silent_signal_counter("market_mode_block", self.name, coin, _s.get_engine_role(self.name))
                _research_funnel.record("upstream_block", engine=self.name, symbol=coin, reason="btc_regime_dead")
                return
            _allowed_regimes = _s.get_lane_allowed_regimes(self.name)
            if _cur_regime not in _allowed_regimes:
                _bump_silent_signal_counter("regime_block", self.name, coin, _s.get_engine_role(self.name))
                _reason = "btc_regime_unknown" if _cur_regime == _regime.UNKNOWN else f"lane_regime_restricted:{_cur_regime}"
                _research_funnel.record("upstream_block", engine=self.name, symbol=coin, reason=_reason)
                return

            # ── Anti-churn: no same-coin re-entry after failed trade ──────
            _fail_ts = self._failed_coins.get(coin, 0.0)
            if _fail_ts and time.time() - _fail_ts < self._FAILED_COIN_COOLDOWN:
                _bump_silent_signal_counter("cooldown_block", self.name, coin, _s.get_engine_role(self.name))
                return
            # Clean up expired entries
            if _fail_ts and time.time() - _fail_ts >= self._FAILED_COIN_COOLDOWN:
                self._failed_coins.pop(coin, None)
            # ── end regime gating ───────────────────────────────────────────────

            # ── Global per-coin SL loss block ────────────────────────────────────
            # Prevents any engine from re-entering a coin that has been stopped
            # out repeatedly. Clears automatically after _COIN_BLOCK_SECS (2h).
            if _is_coin_globally_blocked(coin):
                _bump_silent_signal_counter("cooldown_block", self.name, coin, _s.get_engine_role(self.name))
                return
            # ── Single-SL cooldown check (Tier-1) ───────────────────────────
            if _is_single_sl_cooled(coin):
                _bump_silent_signal_counter("cooldown_block", self.name, coin, _s.get_engine_role(self.name))
                return
            cooled_down, _cool_reason = _admission.is_symbol_cooled_down(coin)
            if cooled_down:
                _bump_silent_signal_counter("cooldown_block", self.name, coin, _s.get_engine_role(self.name))
                return
            # ── end single-SL cooldown ───────────────────────────────────
            # ── end global coin block ────────────────────────────────────────────

            _coin_volume_24h = get_volume(coin)
            if _coin_volume_24h > 0 and _coin_volume_24h < MIN_VOLUME_USD:
                _role = _s.get_engine_role(self.name)
                _bump_silent_signal_counter("spread_liquidity_block", self.name, coin, _role)
                _record_spread_liquidity_block(
                    symbol=coin,
                    engine=self.name,
                    side=None,
                    sub_reason="volume_too_low",
                    volume_24h=_coin_volume_24h,
                    volume_threshold=MIN_VOLUME_USD,
                    market_regime=_cur_regime,
                    role=_role,
                )
                return
            if candle_count(coin) < MIN_CANDLES:
                _bump_silent_signal_counter("insufficient_candles", self.name, coin, _s.get_engine_role(self.name))
                _research_funnel.record("upstream_block", engine=self.name, symbol=coin, reason="insufficient_1m_samples")
                return
            if _s.get_val("cooldown_enabled", True):
                base_cool = float(_s.get_val("cooldown_mins", 10))
                if _s.get_val("dynamic_cooldown_enabled", True):
                    # After win: halve cooldown (conditions were good, stay active)
                    # After loss: double cooldown (wait for better setup)
                    if self._last_trade_was_win:
                        cool_mins = base_cool * 0.5
                    else:
                        cool_mins = base_cool * 2.0
                else:
                    cool_mins = base_cool
                if time.time() - self._cooldowns.get(coin, 0) < cool_mins * 60:
                    _bump_silent_signal_counter("cooldown_block", self.name, coin, _s.get_engine_role(self.name))
                    return

            closes = get_closes(coin); highs = get_highs(coin)
            lows   = get_lows(coin);   volumes = get_volumes(coin)
            if not closes or len(closes) < MIN_CANDLES:
                _bump_silent_signal_counter("insufficient_candles", self.name, coin, _s.get_engine_role(self.name))
                _research_funnel.record("upstream_block", engine=self.name, symbol=coin, reason="insufficient_1m_samples")
                return
            try:
                import candidate_expectancy_telemetry as _expectancy_telemetry

                _expectancy_telemetry.observe_price(coin, float(closes[-1]))
            except Exception:
                logger.debug("Counterfactual price observation failed for %s", coin, exc_info=True)
            try:
                import shadow_rehab_tracker as _shadow_rehab
                _shadow_rehab.update_market_price(
                    coin,
                    float(closes[-1]),
                    high=float(highs[-1]) if highs else None,
                    low=float(lows[-1]) if lows else None,
                )
            except Exception:
                pass

            closes_4h  = get_4h_closes(coin) if self.name in ("RYUU","SHOGUN","TENJIN","IZANAGI","RONIN","OROCHI","HAYATE") else None
            highs_4h   = get_4h_highs(coin)  if self.name in ("SHOGUN","TENJIN","IZANAGI") else None
            lows_4h    = get_4h_lows(coin)   if self.name in ("SHOGUN","TENJIN","IZANAGI") else None
            # 15m + 1H data for AMATERASU; IZANAGI uses 15m AI confirmation.
            closes_15m = get_15m_closes(coin) if self.name in ("AMATERASU", "IZANAGI", "OROCHI", "ENMA", "HAYATE", "HACHIMAN", "YATAGARASU") else None
            highs_15m  = get_15m_highs(coin)  if self.name in ("IZANAGI", "OROCHI", "YATAGARASU") else None
            lows_15m   = get_15m_lows(coin)   if self.name in ("IZANAGI", "OROCHI", "YATAGARASU") else None
            volumes_15m = get_15m_volumes(coin) if self.name in ("IZANAGI", "OROCHI", "YATAGARASU") else None
            closes_1h  = get_1h_closes(coin)  if self.name in ("AMATERASU", "SUIJIN", "ENMA", "HAYATE", "HACHIMAN", "YATAGARASU") else None
            highs_1h   = get_1h_highs(coin)   if self.name in ("AMATERASU", "SUIJIN", "YATAGARASU") else None
            lows_1h    = get_1h_lows(coin)    if self.name in ("AMATERASU", "SUIJIN", "YATAGARASU") else None
            # Daily candles for IZANAGI (Elliott Wave needs multi-day history)
            closes_1d  = get_1d_closes(coin)  if self.name == "IZANAGI" else None
            highs_1d   = get_1d_highs(coin)   if self.name == "IZANAGI" else None
            lows_1d    = get_1d_lows(coin)    if self.name == "IZANAGI" else None
            funding    = get_cached_funding_rate(coin) if funding_is_fresh(coin) else 0.0

            if self.name in ("RYUU", "SHOGUN", "TENJIN") and len(closes_4h or []) < 60:
                _research_funnel.record("upstream_block", engine=self.name, symbol=coin, reason="missing_4h_context")
                logger.debug("[%s] %s skipped — missing 4H context.", self.name, coin)
                _bump_silent_signal_counter("insufficient_candles", self.name, coin, _s.get_engine_role(self.name))
                return
            if self.name == "RONIN" and len(closes_4h or []) < 205:
                _research_funnel.record("upstream_block", engine=self.name, symbol=coin, reason="missing_4h_trend_context")
                logger.debug("[%s] %s skipped - missing 4H trend context.", self.name, coin)
                _bump_silent_signal_counter("insufficient_candles", self.name, coin, _s.get_engine_role(self.name))
                return
            if self.name == "OROCHI" and (
                len(closes_4h or []) < 205
                or len(closes_15m or []) < 80
                or len(highs_15m or []) < 80
                or len(lows_15m or []) < 80
                or len(volumes_15m or []) < 80
            ):
                _research_funnel.record("upstream_block", engine=self.name, symbol=coin, reason="missing_4h_15m_context")
                logger.debug("[%s] %s skipped — missing 4H/15m continuation context.", self.name, coin)
                _bump_silent_signal_counter("insufficient_candles", self.name, coin, _s.get_engine_role(self.name))
                return
            if self.name == "IZANAGI" and (
                len(closes_4h or []) < 60
                or len(closes_1d or []) < 120
                or len(closes_15m or []) < 120
                or len(highs_15m or []) < 120
                or len(lows_15m or []) < 120
                or len(volumes_15m or []) < 120
            ):
                _research_funnel.record("upstream_block", engine=self.name, symbol=coin, reason="missing_4h_1d_15m_context")
                logger.debug("[%s] %s skipped — missing 4H/1D context.", self.name, coin)
                _bump_silent_signal_counter("insufficient_candles", self.name, coin, _s.get_engine_role(self.name))
                return
            if self.name == "AMATERASU" and (len(closes_15m or []) < 60 or len(closes_1h or []) < 60):
                _research_funnel.record("upstream_block", engine=self.name, symbol=coin, reason="missing_15m_1h_context")
                logger.debug("[%s] %s skipped — missing 15m/1H context.", self.name, coin)
                _bump_silent_signal_counter("insufficient_candles", self.name, coin, _s.get_engine_role(self.name))
                return
                if self.name == "HAYATE" and (
                    len(closes_4h or []) < 8
                    or len(closes_15m or []) < 8
                    or len(closes_1h or []) < 8
                ):
                    logger.debug("[%s] %s skipped — missing relative-strength timeframe context.", self.name, coin)
                    return
                if self.name == "HACHIMAN" and (len(closes_15m or []) < 55 or len(closes_1h or []) < 55):
                    logger.debug("[%s] %s skipped — missing selector timeframe context.", self.name, coin)
                    return
                if self.name == "YATAGARASU" and (
                    len(closes_15m or []) < 80
                    or len(highs_15m or []) < 80
                    or len(lows_15m or []) < 80
                    or len(volumes_15m or []) < 80
                    or len(closes_1h or []) < 60
                ):
                    logger.debug("[%s] %s skipped — missing confluence timeframe context.", self.name, coin)
                    return
                if self.name == "SUIJIN" and len(closes_1h or []) < 60:
                    logger.debug("[%s] %s skipped — missing 1H context.", self.name, coin)
                    return
            if self.name in ("SUSANOO", "ONRYO") and not funding_is_fresh(coin):
                logger.debug("[%s] %s skipped — funding cache is stale.", self.name, coin)
                _bump_silent_signal_counter("missing_data", self.name, coin, _s.get_engine_role(self.name))
                _research_funnel.record("upstream_block", engine=self.name, symbol=coin, reason="funding_context_stale")
                return

            # TSUNAMI: fetch liquidation cascade data (cached, 60s cooldown)
            tsunami_long_usd = tsunami_short_usd = 0.0
            if self.name == "TSUNAMI" and _s.is_engine_enabled("TSUNAMI"):
                try:
                    tsunami_long_usd, tsunami_short_usd = await _timed_engine_dependency(
                        self.name, coin, "_fetch_tsunami", "liquidation_cascade", _fetch_tsunami(coin)
                    )
                except Exception:
                    pass

            # RAIJUU: fetch open interest history (cached, 120s cooldown)
            raijuu_oi_series: list[float] = []
            if self.name in ("RAIJUU", "ONRYO") and (
                self.name != "RAIJUU" or _s.is_engine_enabled("RAIJUU")
            ):
                try:
                    raijuu_oi_series = await _timed_engine_dependency(
                        self.name, coin, "_fetch_raijuu_oi", "open_interest_history", _fetch_raijuu_oi(coin)
                    )
                except Exception:
                    pass

            # FUJIN: fetch OKX taker buy/sell volume (1H period)
            taker_buy_vol = taker_sell_vol = 0.0
            if self.name in ("FUJIN", "KAGUTSUCHI", "YATAGARASU", "RYUJIN"):
                try:
                    tv = await _timed_engine_dependency(
                        self.name, coin, "get_taker_volume", "taker_volume", self.okx.get_taker_volume(coin)
                    )
                    taker_buy_vol  = float(tv.get("buy",  0.0))
                    taker_sell_vol = float(tv.get("sell", 0.0))
                except Exception:
                    pass

            if self.name == "ONRYO" and len(raijuu_oi_series or []) < 6:
                logger.debug("[%s] %s skipped — missing OI context.", self.name, coin)
                _bump_silent_signal_counter("missing_data", self.name, coin, _s.get_engine_role(self.name))
                return

            # SHOGUN: fetch OKX retail long/short ratio for contrarian boost
            long_short_ratio = 0.0
            if self.name == "SHOGUN":
                try:
                    long_short_ratio = await _timed_engine_dependency(
                        self.name, coin, "get_long_short_ratio", "long_short_ratio",
                        self.okx.get_long_short_ratio(coin),
                    )
                except Exception:
                    pass

            # ── RYUU / RAIJIN: ADX > 28 gate in TRENDING regime ──────────
            # Even inside a trending regime, RYUU and RAIJIN only fire when
            # the coin's own ADX is strong enough (>28) to confirm the trend
            # is real for this specific asset.
            if (_cur_regime == _regime.TRENDING
                    and self.name in ("RYUU", "RAIJIN")
                    and len(highs) >= 15 and len(lows) >= 15):
                _adx_val = calc_adx(highs, lows, closes, 14) or 0.0
                if _adx_val <= 28:
                    _bump_silent_signal_counter("volatility_block", self.name, coin, _s.get_engine_role(self.name))
                    return   # ADX too weak — trend not confirmed for this coin

            # Yield to the event loop before CPU-heavy indicator calculations.
            # Prevents the 14-engine × 60-coin evaluation from starving the WS
            # heartbeat task, Telegram handler, and exchange sync loop.
            await asyncio.sleep(0)

            # Ensure new engine data vars are always defined
            if "tsunami_long_usd"  not in locals(): tsunami_long_usd  = 0.0
            if "tsunami_short_usd" not in locals(): tsunami_short_usd = 0.0
            if "raijuu_oi_series"  not in locals(): raijuu_oi_series  = []
            _research_funnel.record("engine_evaluation", engine=self.name, symbol=coin)
            direction, votes = evaluate(
                self.name, closes, highs, lows, volumes,
                closes_4h=closes_4h, highs_4h=highs_4h, lows_4h=lows_4h,
                closes_15m=closes_15m, highs_15m=highs_15m, lows_15m=lows_15m, volumes_15m=volumes_15m,
                closes_1h=closes_1h,
                highs_1h=highs_1h, lows_1h=lows_1h,
                funding_rate=funding,
                long_short_ratio=long_short_ratio,
                taker_buy_vol=taker_buy_vol, taker_sell_vol=taker_sell_vol,
                coin=coin,
                closes_1d=closes_1d, highs_1d=highs_1d, lows_1d=lows_1d,
                tsunami_long_usd=tsunami_long_usd,
                tsunami_short_usd=tsunami_short_usd,
                raijuu_oi_series=raijuu_oi_series,
            )
            await asyncio.sleep(0)

            if direction != "HOLD":
                logger.info("[%s] Signal %s %s votes=%.1f", self.name, direction, coin, float(votes))
                signal_time = time.time()
                signal_age_inputs = {}
                try:
                    signal_age_inputs = _signal_age_inputs(coin)
                    _latency.record_signal_age(
                        instrument=coin,
                        engine=self.name,
                        decision_time=signal_time,
                        engine_started_at=engine_started_at,
                        engine_completed_at=signal_time,
                        **signal_age_inputs,
                    )
                except Exception:
                    logger.debug("Signal-age telemetry failed for %s %s", self.name, coin, exc_info=True)
                _research_funnel.record(
                    "raw_signal", engine=self.name, symbol=coin, direction=direction,
                    votes=float(votes), regime=str(_cur_regime),
                    decision_time=signal_time,
                    engine_started_at=engine_started_at,
                    engine_completed_at=signal_time,
                    **signal_age_inputs,
                )

            # ── MTF Confluence Filter ─────────────────────────────────────
            # Checks EMA9 vs EMA21 on 1m, 15m, and 1H timeframes.
            # If 2+ timeframes disagree with the trade direction → subtract 1.0
            # from vote score (reduces confidence but doesn't veto the trade).
            if (_s.get_val("mtf_confluence_enabled", True)
                    and direction != "HOLD" and votes > 0
                    and self.name not in ("AMATERASU",)):
                _disagree = 0
                for _tf_closes in [
                    closes,
                    get_15m_closes(coin) or [],
                    get_1h_closes(coin)  or [],
                ]:
                    if len(_tf_closes) < 22:
                        continue
                    _e9  = calc_ema(_tf_closes, 9)
                    _e21 = calc_ema(_tf_closes, 21)
                    if not _e9 or not _e21:
                        continue
                    _tf_dir = "LONG" if _e9[-1] > _e21[-1] else "SHORT"
                    if _tf_dir != direction:
                        _disagree += 1
                if _disagree >= 2:
                    votes = float(votes) - 1.0
                    logger.debug(
                        "[%s] MTF filter: %d/3 TFs disagree → votes %.1f→%.1f for %s %s",
                        self.name, _disagree, float(votes)+1.0, votes, direction, coin,
                    )
            _engine_min_votes = _s.get_val(f"{self.name.lower()}_min_votes", None)
            if _engine_min_votes is None:
                min_votes = float(_s.get_val("min_vote_score", 3.0))
            else:
                min_votes = float(_engine_min_votes)
            if direction == "HOLD":
                _bump_silent_signal_counter("engine_returned_hold", self.name, coin, _s.get_engine_role(self.name))
                _research_funnel.record("raw_hold", engine=self.name, symbol=coin, reason="engine_returned_hold")
                return
            if direction not in {"LONG", "SHORT"}:
                _bump_silent_signal_counter("invalid_direction", self.name, coin, _s.get_engine_role(self.name))
                return
            side = "buy" if direction == "LONG" else "sell"
            _low_vote_consensus_lift = False
            _vote_rescue_active = False
            _vote_rescue_meta: dict = {}
            if votes < min_votes:
                _research_funnel.record(
                    "pre_candidate", engine=self.name, symbol=coin, reason="votes_below_floor",
                    direction=direction, votes=float(votes), required_votes=float(min_votes),
                    gap=float(min_votes) - float(votes),
                )
                _vote_gap_for_diag = float(min_votes or 0.0) - float(votes or 0.0)
                if float(votes or 0.0) <= 0.0:
                    _bump_silent_signal_counter("no_votes", self.name, coin, _s.get_engine_role(self.name))
                _bump_silent_signal_counter("votes_below_floor", self.name, coin, _s.get_engine_role(self.name))
                if 0.0 < _vote_gap_for_diag <= float(_s.get_val("vote_rescue_max_vote_gap", 1.0) or 1.0):
                    _bump_silent_signal_counter("near_vote_floor", self.name, coin, _s.get_engine_role(self.name))
                if bool(_s.get_val("vote_rescue_enabled", False)):
                    try:
                        _bump_silent_signal_counter("vote_rescue_eligible", self.name, coin, _s.get_engine_role(self.name))
                        _rescue_quality = _candidate_quality_snapshot(
                            self.name,
                            coin,
                            direction,
                            balance,
                            float(votes or 0.0),
                            float(votes or 0.0),
                        )
                        _rescue_role_ctx = _position_strength_context(
                            self.name,
                            coin,
                            direction,
                            _rescue_quality,
                            float(votes or 0.0),
                        )
                        _rescue_lift, _rescue_lift_reason = _role_consensus_confidence_lift(
                            _rescue_role_ctx,
                            _rescue_quality,
                            float(votes or 0.0),
                        )
                        _rescue_admission_conf = round(float(votes or 0.0) + float(_rescue_lift or 0.0), 4)
                        _rescue_novo = None
                        if _s.get_val("novo_intelligence_enabled", False):
                            try:
                                _rescue_market_data = {
                                    "candles": _novo_candles_from_series(closes, highs, lows, volumes),
                                    "candles_15m": _novo_candles_from_series(closes_15m, highs_15m, lows_15m, volumes_15m),
                                    "candles_1h": _novo_candles_from_series(closes_1h, highs_1h, lows_1h, None),
                                    "candles_4h": _novo_candles_from_series(closes_4h, highs_4h, lows_4h, None),
                                }
                                _rescue_novo = _novo_intelligence.analyze_candidate(
                                    symbol=coin,
                                    side=direction,
                                    engine=self.name,
                                    confidence=float(votes or 0.0),
                                    consensus=_rescue_quality.get("consensus"),
                                    regime=_rescue_quality.get("regime", ""),
                                    admission={
                                        "admission_confidence": float(_rescue_admission_conf or 0.0),
                                        "candidate_quality": float(_rescue_quality.get("quality_score", 0.0) or 0.0),
                                    },
                                    rejection_reason=None,
                                    market_data=_rescue_market_data,
                                    settings=_s,
                                )
                            except Exception as _rescue_novo_exc:
                                logger.debug("[%s] Vote rescue Novo support skipped for %s %s: %s", self.name, coin, direction, _rescue_novo_exc)
                        _vote_rescue_meta = _evaluate_vote_rescue(
                            engine_name=self.name,
                            coin=coin,
                            direction=direction,
                            votes=float(votes or 0.0),
                            min_votes=float(min_votes or 0.0),
                            quality_snapshot=_rescue_quality,
                            role_ctx=_rescue_role_ctx,
                            admission_confidence=float(_rescue_admission_conf or 0.0),
                            novo_result=_rescue_novo,
                        )
                        _vote_rescue_meta["consensus_lift"] = float(_rescue_lift or 0.0)
                        _vote_rescue_meta["consensus_lift_reason"] = str(_rescue_lift_reason or "")
                        if bool(_vote_rescue_meta.get("allowed", False)):
                            _vote_rescue_active = True
                            _bump_silent_signal_counter("vote_rescue_success", self.name, coin, _s.get_engine_role(self.name))
                            logger.info(
                                "VOTE_RESCUE_OPEN_FLOOR [%s] %s %s votes=%.1f min=%.1f gap=%.1f q=%.2f adm=%.2f support=%s novo=%s",
                                engine_log_name(self.name),
                                coin,
                                direction,
                                float(votes or 0.0),
                                float(min_votes or 0.0),
                                float(_vote_rescue_meta.get("vote_gap", 0.0) or 0.0),
                                float(_vote_rescue_meta.get("quality", 0.0) or 0.0),
                                float(_vote_rescue_meta.get("admission_confidence", 0.0) or 0.0),
                                str(_vote_rescue_meta.get("support_source", "") or ""),
                                str(_vote_rescue_meta.get("novo_tier", "") or "n/a"),
                            )
                            _record_decision_trace(
                                engine=self.name,
                                coin=coin,
                                direction=direction,
                                confidence=float(votes or 0.0),
                                decision="vote_rescue",
                                reason="VOTE_RESCUE_OPEN_FLOOR",
                                raw_score=float(votes or 0.0),
                                balance=balance,
                                candidate_quality=float(_vote_rescue_meta.get("quality", 0.0) or 0.0),
                                regime=str(_rescue_quality.get("regime", "") or ""),
                                selector_influence=float(_rescue_role_ctx.get("selector_score", 0.0) or 0.0),
                                notes=f"gap={float(_vote_rescue_meta.get('vote_gap', 0.0) or 0.0):.2f} support={_vote_rescue_meta.get('support_source', '')}",
                                extra={"vote_rescue": dict(_vote_rescue_meta)},
                            )
                        else:
                            _record_decision_trace(
                                engine=self.name,
                                coin=coin,
                                direction=direction,
                                confidence=float(votes or 0.0),
                                decision="vote_rescue_rejected",
                                reason=str(_vote_rescue_meta.get("reason", "LOW_VOTE_FLOOR") or "LOW_VOTE_FLOOR"),
                                raw_score=float(votes or 0.0),
                                balance=balance,
                                candidate_quality=float(_vote_rescue_meta.get("quality", 0.0) or 0.0),
                                regime=str(_rescue_quality.get("regime", "") or ""),
                                selector_influence=float(_rescue_role_ctx.get("selector_score", 0.0) or 0.0),
                                notes=f"gap={float(_vote_rescue_meta.get('vote_gap', 0.0) or 0.0):.2f}",
                                extra={"vote_rescue": dict(_vote_rescue_meta)},
                            )
                            try:
                                _rjlog.record(
                                    self.name,
                                    coin,
                                    direction,
                                    float(votes or 0.0),
                                    str(_vote_rescue_meta.get("reason", "LOW_VOTE_FLOOR") or "LOW_VOTE_FLOOR").lower(),
                                    f"votes {float(votes or 0.0):.1f} < open floor {float(min_votes or 0.0):.1f}; gap={float(_vote_rescue_meta.get('vote_gap', 0.0) or 0.0):.2f}",
                                )
                            except Exception:
                                pass
                    except Exception as _vote_rescue_exc:
                        logger.debug("[%s] Vote rescue evaluation skipped for %s %s: %s", self.name, coin, direction, _vote_rescue_exc)
                _contrib_floor = _role_contribution_min_votes(self.name, min_votes)
                if _vote_rescue_active:
                    pass
                elif _role_can_contribute_below_open_votes(self.name) and votes >= _contrib_floor:
                    _consensus.record_signal(self.name, coin, direction, float(votes or 0.0))
                    _contrib_quality = _candidate_quality_snapshot(
                        self.name,
                        coin,
                        direction,
                        balance,
                        float(votes or 0.0),
                        float(votes or 0.0),
                    )
                    _contrib_ctx = _position_strength_context(
                        self.name,
                        coin,
                        direction,
                        _contrib_quality,
                        float(votes or 0.0),
                    )
                    _contrib_allowed, _contrib_reason = _role_independent_open_allowed(
                        _contrib_ctx,
                        float(_contrib_quality.get("quality_score", 0.0) or 0.0),
                    )
                    if _contrib_allowed and bool(_contrib_ctx.get("role_consensus", False)):
                        _low_vote_consensus_lift = True
                        logger.info(
                            "ROLE CONSENSUS LIFT [%s] %s %s votes=%.1f < %.1f role=%s consensus=%d strength=%.2f",
                            engine_log_name(self.name),
                            coin,
                            direction,
                            float(votes or 0.0),
                            float(min_votes or 0.0),
                            str(_contrib_ctx.get("engine_role", "")),
                            int(_contrib_ctx.get("consensus_count", 1) or 1),
                            float(_contrib_ctx.get("position_strength", 0.0) or 0.0),
                        )
                    else:
                        _contrib_reason_l = str(_contrib_reason or "").lower()
                        if "selector contribution only" in _contrib_reason_l:
                            _bump_silent_signal_counter("selector_contribution_only", self.name, coin, _s.get_engine_role(self.name))
                        elif "shadow advisor contribution only" in _contrib_reason_l:
                            _bump_silent_signal_counter("shadow_contribution_only", self.name, coin, _s.get_engine_role(self.name))
                        else:
                            _bump_silent_signal_counter("role_not_opener", self.name, coin, _s.get_engine_role(self.name))
                        _record_decision_trace(
                            engine=self.name,
                            coin=coin,
                            direction=direction,
                            confidence=float(votes or 0.0),
                            decision="role_contribution",
                            reason=f"contribution recorded; votes {float(votes):.1f} < open floor {float(min_votes):.1f}; {_contrib_reason}",
                            raw_score=float(votes or 0.0),
                            balance=balance,
                            candidate_quality=float(_contrib_quality.get("quality_score", 0.0) or 0.0),
                            regime=str(_contrib_quality.get("regime", "") or ""),
                            selector_influence=float(_contrib_ctx.get("selector_score", 0.0) or 0.0),
                            notes=(
                                f"role={_contrib_ctx.get('engine_role')} "
                                f"consensus={int(_contrib_ctx.get('consensus_count', 1) or 1)} "
                                f"strength={float(_contrib_ctx.get('position_strength', 0.0) or 0.0):.2f}"
                            ),
                            extra={"role_context": {k: v for k, v in _contrib_ctx.items() if k != "performance"}},
                        )
                        return
                else:
                    if not _vote_rescue_meta:
                        _record_decision_trace(
                            engine=self.name,
                            coin=coin,
                            direction=direction,
                            confidence=float(votes or 0.0),
                            decision="low_vote_floor",
                            reason="LOW_VOTE_FLOOR",
                            raw_score=float(votes or 0.0),
                            balance=balance,
                            notes=f"votes {float(votes or 0.0):.1f} < open floor {float(min_votes or 0.0):.1f}",
                            extra={
                                "vote_rescue": {
                                    "allowed": False,
                                    "reason": "LOW_VOTE_FLOOR",
                                    "votes": float(votes or 0.0),
                                    "min_votes": float(min_votes or 0.0),
                                    "vote_gap": round(float(min_votes or 0.0) - float(votes or 0.0), 4),
                                }
                            },
                        )
                    return

            # Repeated blocked candidates are cooled down briefly so the engine
            # does not waste cycles and log volume on the same failed path.
            _skip_state = _rjlog.should_skip_candidate(self.name, coin, direction)
            _skip_existing = _registry.get(coin.upper(), {})
            _skip_is_confirmable = (
                _confirm_path_enabled()
                and _skip_existing
                and str(_skip_existing.get("direction", "") or "").lower() == direction.lower()
                and str(_skip_existing.get("engine", "") or "").upper() != self.name.upper()
            )
            if _skip_state and not _skip_is_confirmable:
                _bump_silent_signal_counter("cooldown_block", self.name, coin, _s.get_engine_role(self.name))
                return

            # ── V6 / V6.1: Capital profile, confidence, drawdown, frequency ──
            # All V6.1 checks share the same _v6_* locals for downstream use.
            # Any import/runtime error silently skips the block (V5 fallback).
            _v6_conf    = None
            _v6_dd      = None
            _v6_gate    = None
            _v6_profile = None
            _exec_plan = None
            if _s.get_val("v6_enabled", False):
                try:
                    import telemetry_v6 as _t6

                    # ── V6.1: Capital profile ─────────────────────────────────
                    if _s.get_val("v6_capital_adaptive_enabled", True):
                        from capital_manager import get_profile as _v6_get_profile
                        from capital_manager import check_frequency as _v6_check_freq
                        _v6_profile = _v6_get_profile(balance)
                        _t6.log_capital_profile(self.name, _v6_profile)

                        # Frequency caps are diagnostics-only in V6.1 final:
                        # do not hard-block entries based on hourly/daily counts.
                        if _s.get_val("v6_trade_frequency_enabled", True):
                            _freq_profile = _v6_profile
                            try:
                                from dataclasses import replace as _dc_replace

                                _intent_preview = _trade_intent.get_trade_intent(balance=balance)
                                _freq_mult = max(float(_intent_preview.frequency_multiplier or 1.0), 0.0)
                                if _intent_preview.allow_new_trades and _freq_mult != 1.0:
                                    _scaled_max = max(int(round(_v6_profile.max_trades_per_hour * _freq_mult)), 1)
                                    _freq_profile = _dc_replace(_v6_profile, max_trades_per_hour=_scaled_max)
                            except Exception:
                                _freq_profile = _v6_profile
                            _freq_ok, _freq_reason = _v6_check_freq(_freq_profile)
                            _t6.log_frequency_check(self.name, _freq_ok, _freq_reason)
                            # Never block trade flow due to frequency caps; selection + slots govern throughput.

                    # ── V6 / V6.1: Drawdown guard (capital-profile-aware) ─────
                    from drawdown_guard import get_drawdown_state as _v6_dd_state
                    _v6_dd = _v6_dd_state(capital_profile=_v6_profile)
                    _t6.log_drawdown(self.name, _v6_dd)
                    if _s.get_val("v6_anti_drawdown_enabled", False) and not _v6_dd.allow_new_trades:
                        _bump_silent_signal_counter("runtime_lock_block", self.name, coin, _s.get_engine_role(self.name))
                        _rej(self.name, coin, direction, votes, "drawdown", _v6_dd.reason)
                        return

                    # ── V6 / V6.1: Confidence gate ───────────────────────────
                    # Determine MTF alignment for V6.1 multi-factor model
                    _mtf_aligned = False
                    try:
                        _tf_agree = 0
                        for _tf_c in [get_15m_closes(coin) or [], get_1h_closes(coin) or []]:
                            if len(_tf_c) >= 22:
                                _e9 = calc_ema(_tf_c, 9); _e21 = calc_ema(_tf_c, 21)
                                if _e9 and _e21 and ((_e9[-1] > _e21[-1]) == (direction == "LONG")):
                                    _tf_agree += 1
                        _mtf_aligned = _tf_agree >= 2
                    except Exception:
                        pass

                    _atr_ratio = None
                    try:
                        if closes and len(closes) >= 14:
                            _atr_v = calc_atr(highs, lows, closes, 14)
                            _last_px = closes[-1]
                            if _atr_v and _last_px > 0:
                                _atr_ratio = _atr_v / _last_px
                    except Exception:
                        pass

                    from confidence_engine import score_signal as _v6_score
                    _cur_regime = str(_regime.get_regime()) if _regime else None
                    _v6_conf = _v6_score(
                        engine_name=self.name,
                        direction=direction,
                        raw_votes=float(votes),
                        regime=_cur_regime,
                        atr_ratio=_atr_ratio,
                        mtf_aligned=_mtf_aligned,
                        capital_profile=_v6_profile,
                    )
                    _t6.log_confidence(self.name, coin, _v6_conf)
                    if _s.get_val("v6_weighted_confidence_enabled", False) and not _v6_conf.approved:
                        _bump_silent_signal_counter("low_confidence", self.name, coin, _s.get_engine_role(self.name))
                        _rej(self.name, coin, direction, _v6_conf, "low_confidence", _v6_conf.reason)
                        return

                except Exception:
                    logger.exception("[%s] V6/V6.1 pre-filter error for %s", self.name, coin)
            # ── end V6/V6.1 pre-filter ────────────────────────────────────────

            _intent = _trade_intent.get_trade_intent(
                balance=balance,
                regime=str(_regime.get_regime() if _regime else ""),
            )
            _macro_ctx = {}
            _macro_conf_mod = 0.0
            _macro_size_mult = 1.0
            _macro_runner_trust = 1.0
            if (
                str(self.name or "").upper() == "AMATERASU"
                and direction != "HOLD"
                and bool(_s.get_val("amaterasu_macro_layer_enabled", True))
            ):
                try:
                    from strategy_categories import amaterasu_macro_context as _am_macro_context
                    _macro_ctx = _am_macro_context(direction, closes_1d, coin=coin) or {}
                    _macro_conf_mod = float(_macro_ctx.get("conf_modifier", 0.0) or 0.0)
                    _macro_size_mult = float(_macro_ctx.get("size_multiplier", 1.0) or 1.0)
                    _macro_runner_trust = float(_macro_ctx.get("runner_trust_mult", 1.0) or 1.0)
                except Exception:
                    _macro_ctx = {}
                    _macro_conf_mod = 0.0
                    _macro_size_mult = 1.0
                    _macro_runner_trust = 1.0
            _signal_conf = float(
                _v6_conf.confidence_score
                if _v6_conf is not None and hasattr(_v6_conf, "confidence_score")
                else votes
            )
            if _macro_conf_mod:
                _signal_conf = max(_signal_conf + float(_macro_conf_mod), 0.0)
            _consensus.record_signal(self.name, coin, direction, _signal_conf)
            _quality_snapshot = _candidate_quality_snapshot(
                self.name,
                coin,
                direction,
                balance,
                float(votes or 0.0),
                _signal_conf,
            )
            if _macro_ctx:
                _quality_snapshot["macro_daily_alignment"] = str(_macro_ctx.get("daily_alignment", "") or "")
                _quality_snapshot["macro_weekly_alignment"] = str(_macro_ctx.get("weekly_alignment", "") or "")
                _quality_snapshot["macro_agreement_score"] = float(_macro_ctx.get("macro_agreement_score", 0) or 0)
                _quality_snapshot["macro_conflict"] = bool(_macro_ctx.get("macro_conflict", False))
                _quality_snapshot["macro_conf_modifier"] = float(_macro_conf_mod or 0.0)
                _quality_snapshot["macro_size_multiplier"] = float(_macro_size_mult or 1.0)
                _quality_snapshot["macro_runner_trust_mult"] = float(_macro_runner_trust or 1.0)
            _candidate_quality = float(_quality_snapshot.get("quality_score", 0.0) or 0.0)
            _role_ctx = _position_strength_context(
                self.name,
                coin,
                direction,
                _quality_snapshot,
                _signal_conf,
            )
            _role_adj = _role_quality_adjustment(_role_ctx)
            _candidate_quality = max(round(_candidate_quality + _role_adj, 3), 0.0)
            _quality_snapshot["quality_score"] = _candidate_quality
            _quality_snapshot["role_quality_adjustment"] = _role_adj
            _role_ctx = _position_strength_context(
                self.name,
                coin,
                direction,
                _quality_snapshot,
                _signal_conf,
            )
            _consensus_conf_lift, _consensus_conf_reason = _role_consensus_confidence_lift(
                _role_ctx,
                _quality_snapshot,
                _signal_conf,
            )
            _admission_conf = round(float(_signal_conf or 0.0) + float(_consensus_conf_lift or 0.0), 4)
            _conf_trace = _confidence_stage_trace(
                base_confidence=float(_signal_conf or 0.0),
                consensus_lift=float(_consensus_conf_lift or 0.0),
                quality_snapshot=_quality_snapshot,
                admission_confidence=float(_admission_conf or 0.0),
            )
            await asyncio.sleep(0)
            if _vote_rescue_active:
                _quality_snapshot["vote_rescue"] = dict(_vote_rescue_meta or {})
                _conf_trace["vote_rescue"] = dict(_vote_rescue_meta or {})
            if bool(_s.get_val("novo_intelligence_enabled", False)):
                try:
                    _novo_market_data = {
                        "candles": _novo_candles_from_series(closes, highs, lows, volumes),
                        "candles_15m": _novo_candles_from_series(closes_15m, highs_15m, lows_15m, volumes_15m),
                        "candles_1h": _novo_candles_from_series(closes_1h, highs_1h, lows_1h, None),
                        "candles_4h": _novo_candles_from_series(closes_4h, highs_4h, lows_4h, None),
                    }
                    _novo_result = _novo_intelligence.analyze_candidate(
                        symbol=coin,
                        side=direction,
                        engine=self.name,
                        confidence=float(_signal_conf or _admission_conf or 0.0),
                        consensus=_quality_snapshot.get("consensus"),
                        regime=_quality_snapshot.get("regime", ""),
                        admission={
                            "admission_confidence": float(_admission_conf or 0.0),
                            "candidate_quality": float(_candidate_quality or 0.0),
                        },
                        rejection_reason=None,
                        market_data=_novo_market_data,
                        settings=_s,
                    )
                    _quality_snapshot["novo"] = _novo_result
                    _novo_compact = _novo_compact_snapshot(_novo_result)
                    _conf_trace["novo"] = dict(_novo_compact)
                    _novo_enrichment_compact = {}
                    try:
                        _novo_enrichment = _novo_soft_enrichment.build_novo_enrichment(
                            novo=_novo_result,
                            side=direction,
                            settings=_s,
                            trade_context={
                                "symbol": coin,
                                "engine": self.name,
                                "admission_confidence": float(_admission_conf or 0.0),
                                "candidate_quality": float(_candidate_quality or 0.0),
                                "quality_snapshot": dict(_quality_snapshot or {}),
                            },
                        )
                        _quality_snapshot["novo_enrichment"] = _novo_enrichment
                        _novo_enrichment_compact = _novo_enrichment_compact_snapshot(_novo_enrichment)
                        _conf_trace["novo_enrichment"] = dict(_novo_enrichment_compact)
                    except Exception as _novo_enrich_exc:
                        logger.debug("[%s] Novo enrichment skipped for %s %s: %s", self.name, coin, direction, _novo_enrich_exc)
                    _maybe_append_novo_candidate_observation(
                        coin=coin,
                        direction=direction,
                        engine=self.name,
                        signal_confidence=float(_signal_conf or 0.0),
                        admission_confidence=float(_admission_conf or 0.0),
                        candidate_quality=float(_candidate_quality or 0.0),
                        novo_result=_novo_result,
                        novo_compact=_novo_compact,
                        novo_enrichment_compact=_novo_enrichment_compact,
                    )
                    _record_decision_trace(
                        engine=self.name,
                        coin=coin,
                        direction=direction,
                        confidence=float(_signal_conf or 0.0),
                        decision="novo_annotation",
                        reason="advisory_metadata_only",
                        raw_score=float(votes or 0.0),
                        balance=balance,
                        candidate_quality=float(_candidate_quality or 0.0),
                        regime=str(_quality_snapshot.get("regime", "") or ""),
                        extra={
                            "novo": dict(_novo_compact),
                            "novo_enrichment": dict(_novo_enrichment_compact),
                        },
                    )
                except Exception as _novo_exc:
                    logger.debug("[%s] Novo annotation skipped for %s %s: %s", self.name, coin, direction, _novo_exc)
            if _consensus_conf_lift > 0:
                logger.info(
                    "ROLE CONSENSUS CONF LIFT [%s] %s %s conf=%.2f->%.2f | %s",
                    engine_log_name(self.name),
                    coin,
                    str(direction or "").upper(),
                    float(_signal_conf or 0.0),
                    float(_admission_conf or 0.0),
                    _consensus_conf_reason,
                )
            if not _intent.allow_new_trades:
                _bump_silent_signal_counter("runtime_lock_block", self.name, coin, _s.get_engine_role(self.name))
                _rej(
                    self.name,
                    coin,
                    direction,
                    _v6_conf if _v6_conf else votes,
                    "intent",
                    f"trade intent {_intent.mode}: {_intent.summary}",
                )
                return

            # ── Per-engine position cap ────────────────────────────────────────
            # Prevents any single engine from monopolising all open slots.
            # Probation mode enforces max_pos=1 regardless of settings.
            _existing = _registry.get(coin.upper(), {})
            _existing_dir = str(_existing.get("direction", "") or "").lower()
            _existing_engine = str(_existing.get("engine", "") or "")
            if (
                _confirm_path_enabled()
                and _existing
                and _existing_dir == direction.lower()
                and _existing_engine.upper() != self.name.upper()
                and _within_duplicate_merge_window(_existing)
            ):
                _confirm_conf = float(
                    _v6_conf.confidence_score
                    if _v6_conf is not None and hasattr(_v6_conf, "confidence_score")
                    else votes
                )
                _confirm_meta = await reg_confirm(
                    coin,
                    self.name,
                    metadata={
                        "last_confirm_direction": direction.lower(),
                        "last_confirm_reason": "same_coin_same_direction",
                        "last_confirm_confidence": _confirm_conf,
                        "last_confirm_engine": self.name,
                        "admission_confidence": float(_admission_conf or _signal_conf or 0.0),
                        "confidence_stage_trace": dict(_conf_trace or {}),
                        **_role_registry_metadata(_role_ctx, self.name),
                    },
                )
                if _confirm_meta:
                    logger.info(
                        "CONFIRM [%s] %s %s conf=%.2f | existing by %s confirmations=%d",
                        engine_log_name(self.name),
                        coin,
                        str(direction or "").upper(),
                        _confirm_conf,
                        _confirm_meta.get("engine") or _existing_engine,
                        int(_confirm_meta.get("confirmation_count", 0) or 0),
                    )
                    _bump_silent_signal_counter("duplicate_position", self.name, coin, _s.get_engine_role(self.name))
                    return

            _role_can_open, _role_reason = _role_independent_open_allowed(_role_ctx, _candidate_quality)
            if not _role_can_open:
                _role_reason_l = str(_role_reason or "").lower()
                if "selector contribution only" in _role_reason_l:
                    _bump_silent_signal_counter("selector_contribution_only", self.name, coin, _s.get_engine_role(self.name))
                elif "shadow advisor contribution only" in _role_reason_l:
                    _bump_silent_signal_counter("shadow_contribution_only", self.name, coin, _s.get_engine_role(self.name))
                else:
                    _bump_silent_signal_counter("role_not_opener", self.name, coin, _s.get_engine_role(self.name))
                logger.info(
                    "ROLE CONTRIBUTION [%s] %s %s role=%s q=%.2f strength=%.2f | %s",
                    engine_log_name(self.name),
                    coin,
                    str(direction or "").upper(),
                    str(_role_ctx.get("engine_role", "")),
                    float(_candidate_quality or 0.0),
                    float(_role_ctx.get("position_strength", 0.0) or 0.0),
                    _role_reason,
                )
                _record_decision_trace(
                    engine=self.name,
                    coin=coin,
                    direction=direction,
                    confidence=float(_signal_conf or 0.0),
                    decision="role_contribution",
                    reason=_role_reason,
                    raw_score=float(votes or 0.0),
                    balance=balance,
                    candidate_quality=float(_candidate_quality or 0.0),
                    regime=str(_quality_snapshot.get("regime", "") or ""),
                    selector_influence=float(_role_ctx.get("selector_score", 0.0) or 0.0),
                    notes=f"role={_role_ctx.get('engine_role')} strength={float(_role_ctx.get('position_strength', 0.0) or 0.0):.2f}",
                    extra={"role_context": {k: v for k, v in _role_ctx.items() if k != "performance"}},
                )
                if self.name == "IZANAGI" and bool(_s.get_val("shadow_rehab_enabled", True)):
                    try:
                        import shadow_rehab_tracker as _shadow_rehab
                        _shadow_rehab.record_izanagi_contribution(
                            symbol=coin,
                            side=direction,
                            entry_price=float(closes[-1] if closes else 0.0),
                            confidence=float(_signal_conf or votes or 0.0),
                            quality=float(_candidate_quality or 0.0),
                            advisor_strength=float(_role_ctx.get("position_strength", 0.0) or 0.0),
                            consensus_count=int(_role_ctx.get("consensus_count", 0) or 0),
                            role_context={k: v for k, v in _role_ctx.items() if k != "performance"},
                        )
                    except Exception:
                        logger.debug("[%s] IZANAGI outcome attribution failed for %s", self.name, coin)
                return

            _dbg = _dbg_snap()
            _dbg_mode_name = _dbg_mode(_dbg.get("mode"))
            _dbg_levels = _dbg.get("levels", {}) or {}
            _dbg_live = (_dbg_mode_name == "live_override")

            if _per_engine_cap_enabled():
                _engine_max = get_effective_engine_cap(
                    self.name,
                    balance=balance,
                    capital_profile=_v6_profile,
                )
                if _dbg_live:
                    _engine_max = _dbg_adjust_engine_cap(
                        int(_engine_max or 1),
                        _dbg_level(_dbg_levels, "engine_cap"),
                        engine_name=self.name,
                    )
                if _reg_engine_count(self.name) >= int(_engine_max or 1):
                    _bump_silent_signal_counter("capital_block", self.name, coin, _s.get_engine_role(self.name))
                    _rej(
                        self.name,
                        coin,
                        direction,
                        _v6_conf if _v6_conf else votes,
                        "engine_cap",
                        f"{_reg_engine_count(self.name)}/{int(_engine_max or 1)} positions open for {self.name}"
                        + _probe_suffix(
                            self.name,
                            coin,
                            direction,
                            balance=balance,
                            capital_profile=_v6_profile,
                            candidate_quality=float(_candidate_quality or 0.0),
                            confidence=float(_signal_conf or 0.0),
                            intent=_intent,
                            candidate_size_usdt=_effective_exec_floor(balance),
                        ),
                    )
                    return

            # ── Correlation filter ─────────────────────────────────────────────
            # Block if too many correlated coins already open in same direction.
            _corr_ok, _corr_reason = _correlation_check(
                coin,
                direction,
                balance=balance,
                capital_profile=_v6_profile,
                candidate_quality=_candidate_quality,
                debug_override_level=_dbg_level(_dbg_levels, "correlation_cap") if _dbg_live else "normal",
            )
            if not _corr_ok:
                _bump_silent_signal_counter("capital_block", self.name, coin, _s.get_engine_role(self.name))
                _rej(
                    self.name,
                    coin,
                    direction,
                    _v6_conf if _v6_conf else votes,
                    "correlation",
                    _corr_reason
                    + _probe_suffix(
                        self.name,
                        coin,
                        direction,
                        balance=balance,
                        capital_profile=_v6_profile,
                        candidate_quality=float(_candidate_quality or 0.0),
                        confidence=float(_signal_conf or 0.0),
                        intent=_intent,
                        candidate_size_usdt=_effective_exec_floor(balance),
                    ),
                )
                return

            # ── Global open positions cap ──────────────────────────────────────
            # Hard ceiling across all engines combined.
            _effective_global_max, _slot_meta = _effective_global_slot_limit(
                balance,
                capital_profile=_v6_profile,
                candidate_quality=_candidate_quality,
                intent=_intent,
            )
            if _dbg_live:
                _effective_global_max = _dbg_adjust_global_cap(
                    int(_effective_global_max or 1),
                    _dbg_level(_dbg_levels, "global_cap"),
                )
            _policy = str(_s.get_val("trade_selection_policy", "replace_weakest") or "replace_weakest").strip().lower()
            _policy = _policy if _policy in {"first_come", "best_score", "replace_weakest"} else "replace_weakest"

            # Best-score policy: reserve the last slots for higher-quality trades.
            if _policy == "best_score":
                try:
                    reserve_n = int(_s.get_val("best_score_reserve_slots", 2) or 2)
                except Exception:
                    reserve_n = 2
                try:
                    floor_q = float(_s.get_val("best_score_quality_floor", 55.0) or 55.0)
                except Exception:
                    floor_q = 55.0

                # Winrate optimization: tune reserved-slot behavior without turning it into a freeze.
                if bool(_s.get_val("winrate_optimization_enabled", False)):
                    wr_profile = str(_s.get_val("winrate_profile", "balanced") or "balanced").strip().lower()
                    wr_profile = wr_profile if wr_profile in {"balanced", "strict", "adaptive"} else "balanced"
                    try:
                        reg = str(_regime.get_regime() if _regime else "") or ""
                    except Exception:
                        reg = ""
                    reg_u = reg.upper().strip()
                    if wr_profile == "strict":
                        reserve_n = max(reserve_n, 3)
                        floor_q += 4.0
                        if reg_u in {"VOLATILE", "RANGING"}:
                            floor_q += 2.0
                    elif wr_profile == "adaptive":
                        if reg_u == "VOLATILE":
                            floor_q += 2.0
                        elif reg_u == "RANGING":
                            floor_q += 1.0
                remaining = int(_effective_global_max - _reg_count())
                if reserve_n > 0 and remaining <= reserve_n and float(_candidate_quality or 0.0) < floor_q:
                    _bump_silent_signal_counter("low_quality", self.name, coin, _s.get_engine_role(self.name))
                    _wr_note = ""
                    if bool(_s.get_val("winrate_optimization_enabled", False)):
                        _wr_note = f" | wr={str(_s.get_val('winrate_profile','balanced') or 'balanced').strip().lower()}"
                    _rej(
                        self.name,
                        coin,
                        direction,
                        _v6_conf if _v6_conf else votes,
                        "selection_policy",
                        f"reserved last {reserve_n} slot(s) for quality >= {floor_q:.1f} "
                        f"(remaining={remaining}, q={float(_candidate_quality or 0.0):.2f}){_wr_note}",
                    )
                    return
            if _reg_count() >= _effective_global_max:
                # Slot rotation is the "replace weakest" model. Allow it under both:
                #  - replace_weakest (explicit)
                #  - best_score (default ordering; rotation still allowed when full)
                if _policy in {"replace_weakest", "best_score"}:
                    _rotation = _slot_rotation_candidate(
                        _candidate_quality,
                        balance,
                        candidate_confidence=float(_signal_conf or 0.0),
                        capital_profile=_v6_profile,
                        intent=_intent,
                    )
                    if _rotation and _rotation.get("replacement_ok"):
                        _rot_ok, _rot_reason = await _execute_slot_rotation(
                            rotation=_rotation,
                            new_engine=self.name,
                            new_coin=coin,
                            new_direction=direction,
                            candidate_quality=_candidate_quality,
                        )
                        if not _rot_ok:
                            _bump_silent_signal_counter("capital_block", self.name, coin, _s.get_engine_role(self.name))
                            _rej(
                                self.name,
                                coin,
                                direction,
                                _v6_conf if _v6_conf else votes,
                                "global_cap",
                                f"max positions {_reg_count()}/{_effective_global_max} reached under {_intent.mode} "
                                f"[{_slot_meta.get('policy_mode', 'Adaptive')}] | rotation_failed={_rot_reason}"
                                + _probe_suffix(
                                    self.name,
                                    coin,
                                    direction,
                                    balance=balance,
                                    capital_profile=_v6_profile,
                                    candidate_quality=float(_candidate_quality or 0.0),
                                    confidence=float(_signal_conf or 0.0),
                                    intent=_intent,
                                    candidate_size_usdt=_effective_exec_floor(balance),
                                ),
                            )
                            return
                        # Record informational replacement event for Diagnostics → Slots.
                        try:
                            _rjlog.record(
                                self.name,
                                coin,
                                direction,
                                float(_v6_conf.confidence_score if _v6_conf is not None and hasattr(_v6_conf, "confidence_score") else votes),
                                "replaced_by_better_trade",
                                str(_rot_reason or "replacement executed"),
                            )
                        except Exception:
                            pass
                    else:
                        _bump_silent_signal_counter("capital_block", self.name, coin, _s.get_engine_role(self.name))
                        _detail = (
                            f"max positions {_reg_count()}/{_effective_global_max} reached under {_intent.mode} "
                            f"[{_slot_meta.get('policy_mode', 'Adaptive')}]"
                        )
                        if _rotation:
                            _detail += (
                                f" | newcomer quality={_candidate_quality:.2f} < required "
                                f"{float(_rotation.get('required_quality', 0.0) or 0.0):.2f} "
                                f"to replace {_rotation.get('coin', '?')} q={float(_rotation.get('quality', 0.0) or 0.0):.2f}"
                            )
                        _rej(
                            self.name,
                            coin,
                            direction,
                            _v6_conf if _v6_conf else votes,
                            "global_cap",
                            _detail
                            + _probe_suffix(
                                self.name,
                                coin,
                                direction,
                                balance=balance,
                                capital_profile=_v6_profile,
                                candidate_quality=float(_candidate_quality or 0.0),
                                confidence=float(_signal_conf or 0.0),
                                intent=_intent,
                                candidate_size_usdt=_effective_exec_floor(balance),
                            ),
                        )
                        return
                else:
                    _bump_silent_signal_counter("capital_block", self.name, coin, _s.get_engine_role(self.name))
                    _rej(
                        self.name,
                        coin,
                        direction,
                        _v6_conf if _v6_conf else votes,
                        "global_cap",
                        f"max positions {_reg_count()}/{_effective_global_max} reached under {_intent.mode} "
                        f"[{_slot_meta.get('policy_mode', 'Adaptive')}] policy={_policy}"
                        + _probe_suffix(
                            self.name,
                            coin,
                            direction,
                            balance=balance,
                            capital_profile=_v6_profile,
                            candidate_quality=float(_candidate_quality or 0.0),
                            confidence=float(_signal_conf or 0.0),
                            intent=_intent,
                            candidate_size_usdt=_effective_exec_floor(balance),
                        ),
                    )
                    return

            _portfolio_candidate_size = _effective_exec_floor(balance)
            if _s.get_val("v6_enabled", False):
                _portfolio_cfg = float(_s.get_val("position_size_usdt", 100.0))
                _portfolio_lev = max(int(_s.get_val("leverage_default", 10)), 1)
                _deployment_target = float(
                    getattr(_v6_profile, "deployment_per_position_target_usdt", 0.0) or 0.0
                )
                _portfolio_margin_cap = (
                    balance
                    * float(_s.get_val("v6_max_position_pct_of_balance", 0.03))
                    * _portfolio_lev
                )
                if _deployment_target > 0:
                    _portfolio_margin_cap = max(_portfolio_margin_cap, _deployment_target)
                _portfolio_exp_cap = (
                    balance * (_v6_profile.max_exposure_pct if _v6_profile else 60.0) / 100.0
                    if balance > 0 else _portfolio_cfg
                )
                _portfolio_target = _deployment_target if _deployment_target > 0 else _portfolio_cfg
                _portfolio_candidate_size = max(
                    _effective_exec_floor(balance),
                    min(_portfolio_target, _portfolio_margin_cap, _portfolio_exp_cap),
                )
            _reserved_size_usdt = float(_portfolio_candidate_size or 0.0)

            # ── Entry filter stack (blackswan, weekly cap, 3-5-7, HTF, session) ─
            from position_registry import get_active_details as _get_all_details
            import entry_filter as _entry_filter
            ticker_ef = {}
            try:
                ticker_ef = await _candidate_preview_ticker(self.okx, coin, engine_name=self.name)
                price_now = float(ticker_ef.get("last", 0))
                try:
                    import candidate_expectancy_telemetry as _expectancy_telemetry

                    _expectancy_telemetry.observe_price(
                        coin,
                        price_now,
                        bid=float(ticker_ef.get("bid", ticker_ef.get("bidPx", 0.0)) or 0.0),
                        ask=float(ticker_ef.get("ask", ticker_ef.get("askPx", 0.0)) or 0.0),
                        high=float(highs[-1]) if highs else None,
                        low=float(lows[-1]) if lows else None,
                    )
                except Exception:
                    logger.debug("Executable counterfactual observation failed for %s", coin, exc_info=True)
            except Exception:
                price_now = 0.0
            _capital_mode = _v6_profile.mode if _v6_profile else ""
            allowed_ef, reason_ef = await _entry_filter.check(
                coin=coin,
                direction=direction.lower(),
                balance=balance,
                funding=funding,
                open_trades=_get_all_details(),
                price_now=price_now,
                capital_mode=_capital_mode,
                candidate_quality=float(_candidate_quality or 0.0),
                confidence_score=float(_admission_conf or _signal_conf or 0.0),
            )

            # ── Soft accept: one soft filter + high confidence → allow at reduced size ─
            _soft_accept_mult = 1.0
            _soft_accept_active = False
            _soft_accept_reason = ""
            _soft_accept_mode = _capital_mode_name(balance, _v6_profile)
            _soft_accept_session = _soft_accept_session_name()
            _soft_accept_min_conf = _soft_accept_min_confidence(_soft_accept_mode, _soft_accept_session)
            if not allowed_ef:
                _soft_failures = [reason_ef] if _is_soft_ef_reason(reason_ef) else []
                _conf_for_sa = float(_admission_conf or _signal_conf or votes or 0.0)
                _sa_max_failures = max(int(_s.get_val("soft_accept_max_soft_failures", 1) or 1), 0)
                if (
                    _s.get_val("soft_accept_enabled", True)
                    and len(_soft_failures) <= _sa_max_failures
                    and len(_soft_failures) > 0
                    and _conf_for_sa >= _soft_accept_min_conf
                ):
                    _soft_accept_active = True
                    _soft_accept_reason = reason_ef
                    _soft_accept_mult = float(
                        _s.get_val(
                            f"soft_accept_size_multiplier_{_soft_accept_mode.lower()}",
                            _s.get_val("soft_accept_size_mult", 0.65),
                        )
                    )
                    _reserved_size_usdt = _apply_soft_accept_size(
                        _portfolio_candidate_size,
                        balance,
                        _soft_accept_mult,
                    )
                    logger.info(
                        "SOFT ACCEPT [%s] %s %s conf=%.2f | mode=%s session=%s threshold=%.2f "
                        "projected=$%.2f from $%.2f ×%.2f due to: %s",
                        self.name,
                        coin,
                        direction,
                        _conf_for_sa,
                        _soft_accept_mode,
                        _soft_accept_session,
                        _soft_accept_min_conf,
                        _reserved_size_usdt,
                        _portfolio_candidate_size,
                        _soft_accept_mult,
                        reason_ef,
                    )
                    _rjlog.record(self.name, coin, direction, _conf_for_sa,
                                  "soft_accept",
                                  f"soft_accept[{_soft_accept_mode}/{_soft_accept_session}] "
                                  f"conf={_conf_for_sa:.2f}/{_soft_accept_min_conf:.2f} base={float(_signal_conf or 0.0):.2f} "
                                  f"size=${_reserved_size_usdt:.2f} from ${_portfolio_candidate_size:.2f}: {reason_ef}")
                else:
                    _role = _s.get_engine_role(self.name)
                    _bump_silent_signal_counter("spread_liquidity_block", self.name, coin, _role)
                    _record_spread_liquidity_block(
                        symbol=coin,
                        engine=self.name,
                        side=direction,
                        ticker=ticker_ef,
                        detail_reason=reason_ef,
                        spread_threshold=_s.get_val("v6_spread_max_pct", 0.30),
                        volume_threshold=_s.get_val("v6_min_volume_usd_24h", 2_000_000),
                        market_regime=_cur_regime,
                        role=_role,
                    )
                    _rej(self.name, coin, direction, _v6_conf if _v6_conf else votes,
                         _ef_tag(reason_ef),
                         str(reason_ef or "")
                         + _probe_suffix(
                             self.name,
                             coin,
                             direction,
                             balance=balance,
                             capital_profile=_v6_profile,
                             candidate_quality=float(_candidate_quality or 0.0),
                             confidence=float(_signal_conf or 0.0),
                             intent=_intent,
                             candidate_size_usdt=float(_reserved_size_usdt or _portfolio_candidate_size or 0.0),
                         ))
                    return

            # ── Weekly cap (adaptive; does not freeze trading) ───────────────────────────
            _repeat_loss_note = ""
            _sd_state = None
            _net_expectancy_trace: dict = {}
            _degraded_flow, _degraded_flow_reason = _rate_limit_degraded_flow(self.okx)
            if bool(_s.get_val("symbol_direction_loss_cooldown_enabled", False)):
                try:
                    _sd_state = _symbol_direction_rehab_state(
                        coin,
                        direction,
                        candidate_quality=float(_candidate_quality or 0.0),
                        consensus_count=int(_quality_snapshot.get("consensus_count", 0) or 0),
                        selector_bonus=float(_quality_snapshot.get("selector_bonus", 0.0) or 0.0),
                    )
                    _cool_remaining = _symbol_direction_cooldown_remaining(coin, direction)
                    if _cool_remaining > 0 and not bool(_sd_state.override_allowed):
                        _bump_silent_signal_counter("cooldown_block", self.name, coin, _s.get_engine_role(self.name))
                        _rej(
                            self.name,
                            coin,
                            direction,
                            _v6_conf if _v6_conf else votes,
                            "symbol_direction_cooldown",
                            f"localized symbol-direction cooldown {coin} {direction} remaining={_cool_remaining/60.0:.1f}m",
                        )
                        try:
                            _s.bump_counter("metric_symbol_direction_cooldowns", 1)
                        except Exception:
                            pass
                        return
                    if _cool_remaining > 0 and bool(_sd_state.override_allowed):
                        logger.info(
                            "SYMBOL-DIRECTION OVERRIDE [%s] %s %s cooldown remaining=%.1fm losses=%d quality=%.1f consensus=%d",
                            engine_log_name(self.name),
                            coin,
                            str(direction or "").upper(),
                            _cool_remaining / 60.0,
                            int(_sd_state.loss_count or 0),
                            float(_candidate_quality or 0.0),
                            int(_quality_snapshot.get("consensus_count", 0) or 0),
                        )
                    if _sd_state.active:
                        _sd_required = float(_s.get_val("min_vote_score", 3.0) or 3.0) + float(_sd_state.confidence_bump or 0.0)
                        _sd_consensus = int(_quality_snapshot.get("consensus_count", 0) or 0)
                        _sd_selector = float(_quality_snapshot.get("selector_bonus", 0.0) or 0.0)
                        if (
                            bool(_sd_state.require_confirmation)
                            and not bool(_sd_state.override_allowed)
                            and _sd_consensus < int(_sd_state.min_consensus or 0)
                            and _sd_selector <= 0.0
                        ):
                            _bump_silent_signal_counter("admission_confidence_low", self.name, coin, _s.get_engine_role(self.name))
                            _rej(
                                self.name,
                                coin,
                                direction,
                                _v6_conf if _v6_conf else votes,
                                "symbol_direction_loss_confirm",
                                (
                                    f"{_sd_state.reason} stage={_sd_state.stage} losses={_sd_state.loss_count} "
                                    f"requires_consensus={int(_sd_state.min_consensus or 0)} quality_floor={float(_sd_state.quality_floor or 0.0):.1f}"
                                ),
                            )
                            return
                        if float(_admission_conf or _signal_conf or 0.0) + 1e-9 < _sd_required and not bool(_sd_state.override_allowed):
                            _bump_silent_signal_counter("admission_confidence_low", self.name, coin, _s.get_engine_role(self.name))
                            _rej(
                                self.name,
                                coin,
                                direction,
                                _v6_conf if _v6_conf else votes,
                                "symbol_direction_loss_confirm",
                                f"{_sd_state.reason} stage={_sd_state.stage} losses={_sd_state.loss_count} conf_bump={_sd_state.confidence_bump:.2f}",
                            )
                            return
                        if float(_sd_state.size_multiplier or 1.0) < 0.999:
                            _before_sd = float(_reserved_size_usdt or 0.0)
                            _reserved_size_usdt = _apply_soft_accept_size(_reserved_size_usdt, balance, float(_sd_state.size_multiplier))
                            _repeat_loss_note = (
                                f"symbol_direction_loss stage={_sd_state.stage} losses={_sd_state.loss_count} "
                                f"size x{float(_sd_state.size_multiplier):.2f}"
                            )
                            logger.info(
                                "SYMBOL-DIRECTION REDUCE [%s] %s %s stage=%s size $%.2f -> $%.2f",
                                engine_log_name(self.name),
                                coin,
                                str(direction or "").upper(),
                                str(_sd_state.stage or ""),
                                _before_sd,
                                float(_reserved_size_usdt or 0.0),
                            )
                except Exception:
                    pass
            if bool(_s.get_val("repeat_loss_guard_enabled", True)):
                try:
                    _repeat_window = int(float(_s.get_val("repeat_loss_guard_window_mins", 720) or 720))
                    _repeat_min = int(float(_s.get_val("repeat_loss_guard_min_losses", 2) or 2))
                    _repeat_losses = int(_perf.get_recent_directional_loss_count(coin, direction, _repeat_window) or 0)
                    if _repeat_losses >= max(1, _repeat_min):
                        _repeat_extra = float(_s.get_val("repeat_loss_guard_extra_conf", 0.60) or 0.60)
                        _base_req = float(_s.get_val("weekly_cap_min_confidence", 4.2) or 4.2)
                        _repeat_req = _base_req + max(0.0, _repeat_extra)
                        _repeat_loss_note = (
                            f"repeat_loss_guard {coin} {direction} losses={_repeat_losses} "
                            f"requires_conf={_repeat_req:.2f}"
                        )
                        if float(_admission_conf or _signal_conf or 0.0) + 1e-9 < _repeat_req:
                            _bump_silent_signal_counter("admission_confidence_low", self.name, coin, _s.get_engine_role(self.name))
                            _rej(
                                self.name,
                                coin,
                                direction,
                                _v6_conf if _v6_conf else votes,
                                "repeat_loss",
                                f"{_repeat_loss_note} conf={float(_admission_conf or _signal_conf or 0.0):.2f} base={float(_signal_conf or 0.0):.2f}"
                                + _probe_suffix(
                                    self.name,
                                    coin,
                                    direction,
                                    balance=balance,
                                    capital_profile=_v6_profile,
                                    candidate_quality=float(_candidate_quality or 0.0),
                                    confidence=float(_signal_conf or 0.0),
                                    intent=_intent,
                                    candidate_size_usdt=float(_reserved_size_usdt or 0.0),
                                ),
                            )
                            try:
                                _s.bump_counter("metric_repeat_loss_blocks", 1)
                            except Exception:
                                pass
                            return
                        _instab_mult = float(_s.get_val("symbol_instability_size_mult", 0.75) or 0.75)
                        _instab_mult = max(0.10, min(1.0, _instab_mult))
                        if _instab_mult < 0.999:
                            _before_instab = float(_reserved_size_usdt or 0.0)
                            _reserved_size_usdt = _apply_soft_accept_size(_reserved_size_usdt, balance, _instab_mult)
                            _repeat_loss_note = f"{_repeat_loss_note} size x{_instab_mult:.2f}"
                            logger.info(
                                "REPEAT LOSS REDUCE [%s] %s %s conf=%.2f | losses=%d size $%.2f -> $%.2f (x%.2f)",
                                engine_log_name(self.name),
                                coin,
                                str(direction or "").upper(),
                                float(_signal_conf or 0.0),
                                _repeat_losses,
                                _before_instab,
                                float(_reserved_size_usdt or 0.0),
                                _instab_mult,
                            )
                            try:
                                _rjlog.record(
                                    self.name,
                                    coin,
                                    direction,
                                    float(_signal_conf or 0.0),
                                    "repeat_loss_reduced",
                                    f"{_repeat_loss_note} size ${_before_instab:.2f} -> ${float(_reserved_size_usdt or 0.0):.2f}",
                                )
                            except Exception:
                                pass
                            try:
                                _s.bump_counter("metric_repeat_loss_reductions", 1)
                            except Exception:
                                pass
                except Exception:
                    _repeat_loss_note = ""

            if bool(_s.get_val("net_expectancy_gate_enabled", True)):
                try:
                    _last_px = float(price_now if price_now > 0 else (closes[-1] if closes else 0.0) or 0.0)
                    _atr_gate = float(calc_atr(highs, lows, closes, 14) or 0.0)
                    _geometry_preview = _preview_candidate_geometry(
                        engine=self.name,
                        coin=coin,
                        direction=direction,
                        entry_price=_last_px,
                        closes=closes,
                        highs=highs,
                        lows=lows,
                        volumes=volumes,
                        fallback_atr=_atr_gate,
                    )
                    _target_move_pct = float(_geometry_preview.get("target_move_pct", 0.0) or 0.0)
                    _stop_move_pct = float(_geometry_preview.get("stop_move_pct", 0.0) or 0.0)
                    if not bool(_geometry_preview.get("valid")):
                        _target_move_pct = float(_s.get_val("net_expectancy_default_target_move_pct", 0.55) or 0.55)
                        _stop_move_pct = float(_s.get_val("min_sl_dist_pct", 1.0) or 1.0)
                    from live_fee_schedule import current as _current_fee
                    _fee_rate, _fee_schedule = _current_fee(execution_type="market")
                    _spread_quality = float(_quality_snapshot.get("spread_quality", 0.0) or 0.0)
                    _spread_pct = 0.0 if _spread_quality >= 0.75 else max((0.75 - _spread_quality) * 0.04, 0.0)
                    _repeat_for_edge = int(getattr(_sd_state, "loss_count", 0) or 0)
                    if _repeat_for_edge <= 0:
                        _edge_window = int(float(_s.get_val("symbol_direction_memory_decay_hours", 24.0) or 24.0) * 60.0)
                        _repeat_for_edge = int(_perf.get_recent_directional_loss_count(coin, direction, max(_edge_window, 1)) or 0)
                    _min_cost_mult, _weak_cost_mult = _micro_sizing.net_expectancy_cost_multipliers(balance, _s)
                    if bool(_s.get_val("profitability_control_plane_enabled", True)):
                        _edge = _profitability.evaluate_candidate(
                            size_usdt=float(_reserved_size_usdt or 0.0),
                            target_move_pct=_target_move_pct,
                            stop_move_pct=_stop_move_pct,
                            entry_fee_rate=_fee_rate,
                            exit_fee_rate=_fee_rate,
                            spread_pct=_spread_pct,
                            slippage_pct=float(_s.get_val("net_expectancy_default_slippage_pct", 0.03) or 0.03),
                            engine=self.name,
                            direction=direction,
                            regime=str(_quality_snapshot.get("regime", "") or ""),
                            contributors=list((_role_ctx or {}).get("contributors", []) or []),
                            history=_profitability.load_closed_history(),
                            min_cost_multiple=_min_cost_mult,
                            soft_size_multiplier=float(_s.get_val("net_expectancy_soft_size_mult", 0.75) or 0.75),
                            execution_route="taker_taker",
                            production_calibration=os.getenv("PAPER_TRADING", "true").strip().lower() == "false",
                        )
                    else:
                        _edge = _life.assess_net_expectancy(
                            size_usdt=float(_reserved_size_usdt or 0.0),
                            target_move_pct=_target_move_pct,
                            fee_rate=_fee_rate,
                            spread_pct=_spread_pct,
                            slippage_pct=float(_s.get_val("net_expectancy_default_slippage_pct", 0.03) or 0.03),
                            repeat_losses=_repeat_for_edge,
                            confidence=float(_admission_conf or _signal_conf or 0.0),
                            candidate_quality=float(_candidate_quality or 0.0),
                            engine_net=float(_engine_perf(self.name).get("last10_net", 0.0) or 0.0),
                            degraded_data=bool(_degraded_flow),
                            min_cost_multiple=_min_cost_mult,
                            weak_cost_multiple=_weak_cost_mult,
                            soft_size_multiplier=float(_s.get_val("net_expectancy_soft_size_mult", 0.75) or 0.75),
                        )
                    _net_expectancy_trace = _edge.to_dict()
                    _net_expectancy_trace["geometry_preview"] = dict(_geometry_preview or {})
                    _net_expectancy_trace["fee_schedule"] = {
                        "maker": float(_fee_schedule.maker),
                        "taker": float(_fee_schedule.taker),
                        "source": str(_fee_schedule.source),
                        "expected_route": "taker_taker",
                    }
                    try:
                        import candidate_expectancy_telemetry as _expectancy_telemetry

                        _contributors = list((_role_ctx or {}).get("consensus_engines", []) or [])
                        _engine_roles = {
                            str(name).upper(): _engine_role(str(name))
                            for name in _contributors
                        }
                        _feature_snapshot = _candidate_feature_snapshot(
                            coin=coin,
                            direction=direction,
                            engine_name=self.name,
                            entry_price=_last_px,
                            ticker=ticker_ef,
                            closes=closes,
                            highs=highs,
                            lows=lows,
                            volumes=volumes,
                            closes_15m=closes_15m,
                            closes_1h=closes_1h,
                            closes_4h=closes_4h,
                            funding=funding,
                            oi_series=raijuu_oi_series,
                            taker_buy_volume=taker_buy_vol,
                            taker_sell_volume=taker_sell_vol,
                            geometry=_geometry_preview,
                            quality_snapshot=_quality_snapshot,
                            role_context=_role_ctx,
                            desired_notional=float(_reserved_size_usdt or 0.0),
                            balance=float(balance or 0.0),
                        )
                        _expectancy_telemetry.record_evaluation(
                            symbol=coin,
                            direction=direction,
                            engine=self.name,
                            confidence=float(_admission_conf or _signal_conf or 0.0),
                            role=_s.get_engine_role(self.name),
                            contributors=_contributors,
                            quality_snapshot=dict(_quality_snapshot or {}),
                            economics=dict(_net_expectancy_trace),
                            disposition=str(_edge.action or ""),
                            feature_snapshot=_feature_snapshot,
                            scan_id=f"{self.name}-{int(time.time())}",
                            raw_score=float(votes or 0.0),
                            engine_roles=_engine_roles,
                            engine_family=str(_feature_snapshot.get("engine_family", "") or ""),
                            exit_profile=str(_feature_snapshot.get("exit_profile", "") or ""),
                        )
                        candidate_created_at = time.time()
                        candidate_handoff_at = time.time()
                        candidate_age_inputs = {}
                        try:
                            candidate_age_inputs = _signal_age_inputs(coin)
                            _latency.record_signal_age(
                                instrument=coin,
                                engine=self.name,
                                decision_time=candidate_created_at,
                                engine_started_at=engine_started_at,
                                engine_completed_at=candidate_created_at,
                                candidate_created_at=candidate_created_at,
                                candidate_handoff_at=candidate_handoff_at,
                                **candidate_age_inputs,
                            )
                        except Exception:
                            logger.debug("Candidate-age telemetry failed for %s %s", self.name, coin, exc_info=True)
                        _research_funnel.record(
                            "candidate", engine=self.name, symbol=coin,
                            reason=str(_edge.action or "evaluated"), direction=direction,
                            confidence=float(_admission_conf or _signal_conf or 0.0),
                            gross_edge_usdt=float(getattr(_edge, "expected_gross_usdt", 0.0) or 0.0),
                            net_edge_usdt=float(getattr(_edge, "expected_net_usdt", 0.0) or 0.0),
                            decision_time=candidate_created_at,
                            engine_started_at=engine_started_at,
                            engine_completed_at=candidate_created_at,
                            **candidate_age_inputs,
                            candidate_created_at=candidate_created_at,
                            candidate_handoff_at=candidate_handoff_at,
                        )
                    except Exception:
                        logger.exception("Candidate expectancy telemetry failed for %s %s", self.name, coin)
                    if _production_research_only():
                        logger.info(
                            "PRODUCTION RESEARCH ONLY [%s] %s %s | candidate persisted; execution disabled while unarmed",
                            engine_log_name(self.name), coin, str(direction or "").upper(),
                        )
                        return
                    if _edge.action == "reduce" and float(_edge.size_multiplier or 1.0) < 0.999:
                        _before_edge = float(_reserved_size_usdt or 0.0)
                        _reserved_size_usdt = _apply_soft_accept_size(_reserved_size_usdt, balance, float(_edge.size_multiplier or 1.0))
                        _edge_note = (
                            f"net_expectancy {str(_edge.reason or '')} expected=${float(_edge.expected_net_usdt):.4f} "
                            f"required=${float(_edge.required_net_usdt):.4f} cost=${float(_edge.estimated_cost_usdt):.4f} "
                            f"x{float(_edge.size_multiplier or 1.0):.2f}"
                        )
                        _repeat_loss_note = f"{_repeat_loss_note} | {_edge_note}" if _repeat_loss_note else _edge_note
                        logger.info(
                            "NET EXPECTANCY REDUCE [%s] %s %s size $%.2f -> $%.2f | %s",
                            engine_log_name(self.name),
                            coin,
                            str(direction or "").upper(),
                            _before_edge,
                            float(_reserved_size_usdt or 0.0),
                            _edge_note,
                        )
                        try:
                            _rjlog.record(
                                self.name,
                                coin,
                                direction,
                                float(_signal_conf or 0.0),
                                "net_expectancy_reduced",
                                f"{_edge_note} size ${_before_edge:.2f} -> ${float(_reserved_size_usdt or 0.0):.2f}",
                            )
                            _s.bump_counter("metric_net_expectancy_reductions", 1)
                        except Exception:
                            pass
                    elif not bool(_edge.allowed):
                        _bump_silent_signal_counter("capital_block", self.name, coin, _s.get_engine_role(self.name))
                        _rej(
                            self.name,
                            coin,
                            direction,
                            _v6_conf if _v6_conf else votes,
                            "net_expectancy_non_positive",
                            (
                                f"action={_edge.action} expected=${float(_edge.expected_net_usdt):.4f} "
                                f"required=${float(_edge.required_net_usdt):.4f} cost=${float(_edge.estimated_cost_usdt):.4f}"
                            ),
                            balance=balance,
                            candidate_quality=float(_candidate_quality or 0.0),
                            regime=str(_quality_snapshot.get("regime", "") or ""),
                            size_usdt=float(_reserved_size_usdt or 0.0),
                            extra={"profitability_control_plane": _edge.to_dict()},
                        )
                        return
                    elif str(_edge.reason or "") == "positive_net_expectancy":
                        logger.info(
                            "NET EXPECTANCY ALLOW [%s] %s %s size $%.2f | expected=$%.4f required=$%.4f cost=$%.4f mult=%.2f/%.2f reason=%s",
                            engine_log_name(self.name),
                            coin,
                            str(direction or "").upper(),
                            float(_reserved_size_usdt or 0.0),
                            float(_edge.expected_net_usdt),
                            float(_edge.required_net_usdt),
                            float(_edge.estimated_cost_usdt),
                            float(_min_cost_mult),
                            float(_weak_cost_mult),
                            str(_edge.reason or ""),
                        )
                except Exception as e:
                    logger.debug("net expectancy shaping skipped for %s %s: %s", coin, direction, e)
                    _net_expectancy_trace = {}

            _weekly_cap_decision = "ACCEPT"
            _weekly_cap_mult = 1.0
            _weekly_cap_reason = ""
            if bool(_s.get_val("weekly_cap_enabled", True)):
                try:
                    _wk_level = _dbg_level(_dbg_levels, "weekly_cap") if _dbg_live else "normal"
                    _wk_res = _wcap.evaluate(
                        balance=float(balance or 0.0),
                        confidence=float(_admission_conf or _signal_conf or 0.0),
                        candidate_quality=float(_candidate_quality or 0.0),
                        selector_bonus=float(_quality_snapshot.get("selector_bonus", 0.0) or 0.0),
                        consensus_count=int(_quality_snapshot.get("consensus_count", 0) or 0),
                        engine_name=self.name,
                        override_level=_wk_level,
                        mutate_state=False,  # mark cooldown only after a trade truly opens
                        degraded_data=_degraded_flow,
                        degraded_reason=_degraded_flow_reason,
                    )
                    _weekly_cap_decision = str(_wk_res.decision or "ACCEPT").upper()
                    _weekly_cap_mult = float(_wk_res.size_multiplier or 1.0)
                    _weekly_cap_reason = (
                        f"{_wk_res.reason} | dd={float(_wk_res.drawdown_pct or 0.0):.1f}% "
                        f"thr={float(_wk_res.threshold_pct or 0.0):.1f}% mode={str(_wk_res.mode or '')}"
                    )
                    if float(getattr(_wk_res, "confidence_deficit", 0.0) or 0.0) > 0.0:
                        _weekly_cap_reason += f" deficit={float(_wk_res.confidence_deficit or 0.0):.2f}"
                    if bool(getattr(_wk_res, "degraded_data", False)):
                        _weekly_cap_reason += f" degraded={_degraded_flow_reason or 'data'}"
                        try:
                            _s.bump_counter("metric_degraded_flow_impacts", 1)
                        except Exception:
                            pass
                    _conf_trace = _confidence_stage_trace(
                        base_confidence=float(_signal_conf or 0.0),
                        consensus_lift=float(_consensus_conf_lift or 0.0),
                        quality_snapshot=_quality_snapshot,
                        degraded_data=bool(getattr(_wk_res, "degraded_data", False)),
                        recovery_deficit=float(getattr(_wk_res, "confidence_deficit", 0.0) or 0.0),
                        admission_confidence=float(_admission_conf or _signal_conf or 0.0),
                    )
                    if _repeat_loss_note:
                        _weekly_cap_reason += f" | {_repeat_loss_note}"
                    if _weekly_cap_decision == "BLOCK":
                        _bump_silent_signal_counter("weekly_cap_block", self.name, coin, _s.get_engine_role(self.name))
                        try:
                            _s.bump_counter("metric_weekly_cap_blocks", 1)
                            if "recovery" in str(_weekly_cap_reason or "").lower():
                                _s.bump_counter("metric_weekly_recovery_blocks", 1)
                            if "conf" in str(_weekly_cap_reason or "").lower():
                                _s.bump_counter("metric_weekly_conf_blocks", 1)
                        except Exception:
                            pass
                        _rej(
                            self.name,
                            coin,
                            direction,
                            _v6_conf if _v6_conf else votes,
                            "weekly_cap",
                            _weekly_cap_reason
                            + _probe_suffix(
                                self.name,
                                coin,
                                direction,
                                balance=balance,
                                capital_profile=_v6_profile,
                                candidate_quality=float(_candidate_quality or 0.0),
                                confidence=float(_signal_conf or 0.0),
                                intent=_intent,
                                candidate_size_usdt=float(_reserved_size_usdt or 0.0),
                            ),
                        )
                        return
                    if _weekly_cap_decision == "ACCEPT_REDUCED" and _weekly_cap_mult < 0.999:
                        _before = float(_reserved_size_usdt or 0.0)
                        _reserved_size_usdt = _apply_soft_accept_size(_reserved_size_usdt, balance, _weekly_cap_mult)
                        if abs(float(_reserved_size_usdt or 0.0) - _before) > 0.009:
                            logger.info(
                                "WEEKLY CAP REDUCE [%s] %s %s conf=%.2f | %s size $%.2f -> $%.2f (x%.2f)",
                                engine_log_name(self.name),
                                coin,
                                str(direction or '').upper(),
                                float(_admission_conf or _signal_conf or 0.0),
                                str(_weekly_cap_reason or "weekly_cap"),
                                _before,
                                float(_reserved_size_usdt or 0.0),
                                float(_weekly_cap_mult or 1.0),
                            )
                            try:
                                _rjlog.record(
                                    self.name,
                                    coin,
                                    direction,
                                    float(_signal_conf or 0.0),
                                    "weekly_cap_reduced",
                                    f"{_weekly_cap_reason} size ${_before:.2f} -> ${float(_reserved_size_usdt or 0.0):.2f}",
                                )
                            except Exception:
                                pass
                except Exception:
                    # Weekly cap should never crash the entry pipeline.
                    _weekly_cap_decision = "ACCEPT"
                    _weekly_cap_mult = 1.0
                    _weekly_cap_reason = ""

            def _soft_block_detail(detail: str) -> str:
                if not _soft_accept_active:
                    return detail
                return (
                    f"{detail} | soft_accept[{_soft_accept_mode}/{_soft_accept_session}] "
                    f"size ${_reserved_size_usdt:.2f} via {_soft_accept_reason}"
                )

            _scaled_reserved_size, _scale_reason = _apply_participation_scaling(
                self.name,
                direction,
                _reserved_size_usdt,
                balance,
                capital_profile=_v6_profile,
                soft_accept=_soft_accept_active,
                session=_soft_accept_session,
            )
            if _scale_reason:
                logger.info(
                    "PARTICIPATION SCALE [%s] %s %s conf=%.2f | %s",
                    engine_log_name(self.name),
                    coin,
                    direction,
                    _signal_conf,
                    _scale_reason,
                )
                _reserved_size_usdt = _scaled_reserved_size

            _lane_override_pending = False
            _lane_override_conf = _lane_override_confidence(_v6_conf, votes)
            _lane_override_reason = ""
            from portfolio_allocator import check_lane_budget as _check_lane_budget_early
            _lane_ok, _lane_reason = _check_lane_budget_early(
                engine_log_name(self.name),
                _reserved_size_usdt,
                balance,
                capital_profile=_v6_profile,
                soft_accept=_soft_accept_active,
                session=_soft_accept_session,
            )
            if not _lane_ok:
                _lane_override_reason = _soft_block_detail(_lane_reason)
                if not _lane_override_enabled_for_mode(_soft_accept_mode):
                    _bump_silent_signal_counter("capital_block", self.name, coin, _s.get_engine_role(self.name))
                    _rej(self.name, coin, direction, _v6_conf if _v6_conf else votes,
                         "lane_budget", _lane_override_reason)
                    return
                _override_ok, _override_reason = _lane_override_precheck(
                    balance,
                    _v6_profile,
                    _lane_override_conf,
                    _soft_accept_session,
                )
                if not _override_ok:
                    _bump_silent_signal_counter("capital_block", self.name, coin, _s.get_engine_role(self.name))
                    _log_lane_override_block(
                        self.name,
                        coin,
                        direction,
                        _lane_override_conf,
                        _override_reason,
                    )
                    return
                _lane_override_pending = True
            allowed_portfolio, reason_portfolio = self._portfolio_gate(
                coin,
                direction,
                balance,
                candidate_size_usdt=_reserved_size_usdt,
                capital_profile=_v6_profile,
                candidate_quality=_candidate_quality,
            )
            if not allowed_portfolio:
                _bump_silent_signal_counter("capital_block", self.name, coin, _s.get_engine_role(self.name))
                _portfolio_tag = "headroom" if "headroom" in reason_portfolio.lower() else "portfolio"
                _rej(self.name, coin, direction, _v6_conf if _v6_conf else votes,
                     _portfolio_tag, _soft_block_detail(reason_portfolio))
                return

            _size_preview = None
            if _s.get_val("pre_reserve_size_validation_enabled", True):
                from candidate_economic_state import store as _candidate_economic_state

                _state_dependencies, _participation_cap_usdt, _participation_details = _candidate_wait_dependencies(
                    engine_name=self.name,
                    direction=direction,
                    balance=balance,
                    spread_quality=float(_quality_snapshot.get("spread_quality", 0.0) or 0.0),
                    signal_score=float(_admission_conf or _signal_conf or votes or 0.0),
                    capital_profile=_v6_profile,
                    soft_accept_reason=_soft_accept_reason,
                    soft_accept_session=_soft_accept_session,
                )
                _unchanged_wait, _wait_row = _candidate_economic_state.unchanged(
                    self.name, coin, direction, _state_dependencies
                )
                if _unchanged_wait:
                    _bump_silent_signal_counter("sizing_floor_block", self.name, coin, _s.get_engine_role(self.name))
                    _rej(
                        self.name,
                        coin,
                        direction,
                        _v6_conf if _v6_conf else votes,
                        "candidate_waiting",
                        f"{_wait_row.get('state', 'WAITING_FOR_ECONOMIC_SIZE')}: {_wait_row.get('reason', '')}",
                        balance=balance,
                        candidate_quality=float(_candidate_quality or 0.0),
                        size_usdt=float((_wait_row.get("evidence") or {}).get("size_usdt", 0.0) or 0.0),
                        extra={"candidate_economic_state": _wait_row, "reservation_attempted": False},
                    )
                    return
                _size_preview = _preview_trade_execution(
                    coin=coin,
                    engine_name=self.name,
                    side=side,
                    balance=balance,
                    votes=float(votes),
                    closes=closes,
                    highs=highs,
                    lows=lows,
                    entry_px=price_now if price_now > 0 else (closes[-1] if closes else 0.0),
                    v6_conf=_v6_conf,
                    v6_dd=_v6_dd,
                    v6_gate=_v6_gate,
                    v6_profile=_v6_profile,
                    soft_accept_mult=_soft_accept_mult,
                    soft_accept_reason=_soft_accept_reason,
                    soft_accept_mode=_soft_accept_mode,
                    soft_accept_session=_soft_accept_session,
                    weekly_cap_mult=_weekly_cap_mult,
                    weekly_cap_reason=_weekly_cap_reason,
                    weekly_cap_decision=_weekly_cap_decision,
                    candidate_quality=float(_candidate_quality or 0.0),
                )
                if (
                    abs(float(_size_preview.get("projected_size_usdt", 0.0) or 0.0) - float(_size_preview.get("reduced_size_usdt", 0.0) or 0.0)) > 0.009
                    or abs(float(_size_preview.get("gate_factor", 1.0) or 1.0) - 1.0) > 0.009
                ):
                    logger.info(
                        "RISK REDUCE [%s] %s %s projected=$%.2f factor=perf x%.2f dd x%.2f gate x%.2f reduced=$%.2f min=$%.2f",
                        engine_log_name(self.name),
                        coin,
                        direction.upper(),
                        float(_size_preview.get("projected_size_usdt", 0.0) or 0.0),
                        float(_size_preview.get("performance_factor", 1.0) or 1.0),
                        float(_size_preview.get("drawdown_factor", 1.0) or 1.0),
                        float(_size_preview.get("gate_factor", 1.0) or 1.0),
                        float(_size_preview.get("reduced_size_usdt", 0.0) or 0.0),
                        float(_size_preview.get("exec_floor", 0.0) or 0.0),
                    )
                if _size_preview.get("floor_preserved"):
                    logger.info(
                        "FLOOR PRESERVE [%s] %s %s clamped_to_min=$%.2f mode=%s deploy=%s",
                        engine_log_name(self.name),
                        coin,
                        direction.upper(),
                        float(_size_preview.get("exec_floor", 0.0) or 0.0),
                        str(_s.get_val("trading_mode_preset", "") or "custom").strip() or "custom",
                        str(getattr(_v6_profile, "deployment_effective_mode", getattr(_v6_profile, "deployment_mode", "Guarded")) or "Guarded"),
                    )
                if _size_preview.get("skip"):
                    _candidate_economic_state.wait(
                        self.name,
                        coin,
                        direction,
                        state="WAITING_FOR_ECONOMIC_SIZE",
                        reason=str(_size_preview.get("reject_reason") or "candidate below executable size"),
                        dependencies=_state_dependencies,
                        evidence={
                            "size_usdt": float(_size_preview.get("size_usdt", 0.0) or 0.0),
                            "minimum_exec_usdt": float(_size_preview.get("exec_floor", 0.0) or 0.0),
                            "projected_size_usdt": float(_size_preview.get("projected_size_usdt", 0.0) or 0.0),
                        },
                    )
                    _bump_silent_signal_counter("sizing_floor_block", self.name, coin, _s.get_engine_role(self.name))
                    _size_trace = {
                        "blocking_subsystem": "sizing",
                        "engine": self.name,
                        "role": _s.get_engine_role(self.name),
                        "symbol": coin,
                        "direction": direction.upper(),
                        "confidence": float(_signal_conf or 0.0),
                        "admission_confidence": float(_admission_conf or _signal_conf or 0.0),
                        "confidence_stage_trace": dict(_conf_trace or {}),
                        "regime": str(_regime.get_regime() if _regime else ""),
                        "selector_score": float(_quality_snapshot.get("selector_bonus", 0.0) or 0.0),
                        "booster_score": float(_quality_snapshot.get("positive_engine_bonus", 0.0) or 0.0),
                        "consensus_count": int(_quality_snapshot.get("consensus_count", 0) or 0),
                        "size_mode": str(_s.get_val("size_mode", "blended") or "blended"),
                        "base_size": float(_size_preview.get("base_size_usdt", 0.0) or 0.0),
                        "min_size": float(_size_preview.get("exec_floor", 0.0) or 0.0),
                        "max_size": float(_s.get_val("v6_max_position_size_usdt", 0.0) or 0.0),
                        "balance_pct": float(_s.get_val("position_size_pct", 0.0) or 0.0),
                        "risk_pct": float(_s.get_val("risk_pct", 0.0) or 0.0),
                        "dynamic_sizing": bool(_s.get_val("dynamic_sizing_enabled", _s.get_val("v6_dynamic_sizing_enabled", True))),
                        "performance_multiplier": float(_size_preview.get("performance_factor", 1.0) or 1.0),
                        "exposure_state": " | ".join(_size_preview.get("participation_details", []) or []),
                        "weekly_cap_decision": str(_size_preview.get("weekly_cap_decision", "") or ""),
                        "weekly_cap_multiplier": float(_size_preview.get("weekly_cap_mult", 1.0) or 1.0),
                        "weekly_cap_reason": str(_size_preview.get("weekly_cap_reason", "") or ""),
                        "net_expectancy": dict(_net_expectancy_trace or {}),
                        "probation_modifier": "probation" if _s.get_engine_mode(self.name) == "probation" else "",
                        "duplicate_merge_result": "pre_reserve",
                        "cooldown_result": "passed",
                        "risk_filter_result": str(_size_preview.get("size_reason", "") or ""),
                        "final_decision": "rejected",
                        "rejection_reasons": [str(_size_preview.get("reject_reason") or "reduced size below executable minimum")],
                        "final_notional": float(_size_preview.get("size_usdt", 0.0) or 0.0),
                        "leverage": int(_size_preview.get("leverage", _s.get_val("leverage_default", 10)) or 10),
                        "sizing_modifiers": dict(_size_preview.get("sizing_modifiers", {}) or {}),
                    }
                    logger.info(
                        "PRE-RESERVE REJECT [%s] %s %s %s",
                        engine_log_name(self.name),
                        coin,
                        direction.upper(),
                        str(_size_preview.get("reject_reason") or "reduced size below executable minimum"),
                    )
                    _rej(
                        self.name,
                        coin,
                        direction,
                        _v6_conf if _v6_conf else votes,
                        str(_size_preview.get("reject_tag") or "sizing"),
                        str(_size_preview.get("reject_reason") or "reduced size below executable minimum"),
                        balance=balance,
                        candidate_quality=float(_candidate_quality or 0.0),
                        regime=str(_regime.get_regime() if _regime else ""),
                        size_usdt=float(_size_preview.get("size_usdt", 0.0) or 0.0),
                        size_decision=str(_size_preview.get("size_reason", "") or ""),
                        extra=_size_trace,
                    )
                    return
                _candidate_economic_state.clear(self.name, coin, direction)
                _reserved_size_usdt = float(_size_preview.get("size_usdt", _reserved_size_usdt) or _reserved_size_usdt)

            if _lane_override_pending:
                return {
                    "lane_override_candidate": {
                        "coin": coin,
                        "direction": direction,
                        "side": side,
                        "balance": balance,
                        "votes": float(votes),
                        "confidence": _lane_override_conf,
                        "reserved_size_usdt": _reserved_size_usdt,
                        "quality_score": _candidate_quality,
                        "lane_reason": _lane_override_reason,
                        "closes": closes,
                        "highs": highs,
                        "lows": lows,
                        "volumes": volumes,
                        "v6_conf": _v6_conf,
                        "v6_dd": _v6_dd,
                        "v6_gate": _v6_gate,
                        "v6_profile": _v6_profile,
                        "soft_accept_mult": _soft_accept_mult,
                        "soft_accept_reason": _soft_accept_reason,
                        "soft_accept_mode": _soft_accept_mode,
                        "soft_accept_session": _soft_accept_session,
                        "weekly_cap_mult": _weekly_cap_mult,
                        "weekly_cap_reason": _weekly_cap_reason,
                        "weekly_cap_decision": _weekly_cap_decision,
                        "net_expectancy": dict(_net_expectancy_trace or {}),
                        "size_preview": _size_preview,
                    }
                }

            async with self._entry_lock:
                live_tag, live_reason = await self._recheck_live_entry_constraints(
                    coin,
                    direction,
                    balance,
                    _reserved_size_usdt,
                    votes,
                    conf_obj=_v6_conf,
                    capital_profile=_v6_profile,
                    candidate_quality=_candidate_quality,
                    soft_accept_active=_soft_accept_active,
                    soft_accept_reason=_soft_accept_reason,
                    soft_accept_mode=_soft_accept_mode,
                    soft_accept_session=_soft_accept_session,
                )
                if live_tag == "confirm":
                    _bump_silent_signal_counter("duplicate_position", self.name, coin, _s.get_engine_role(self.name))
                    return
                if live_tag:
                    _live_reason_key = "duplicate_position" if str(live_tag or "").lower() in {"duplicate", "confirm"} else "execution_admission_block"
                    _bump_silent_signal_counter(_live_reason_key, self.name, coin, _s.get_engine_role(self.name))
                    _rej(
                        self.name,
                        coin,
                        direction,
                        _v6_conf if _v6_conf else votes,
                        live_tag,
                        str(live_reason or "")
                        + _probe_suffix(
                            self.name,
                            coin,
                            direction,
                            balance=balance,
                            capital_profile=_v6_profile,
                            candidate_quality=float(_candidate_quality or 0.0),
                        confidence=float(_admission_conf or _signal_conf or 0.0),
                            intent=_intent,
                            candidate_size_usdt=float(_reserved_size_usdt or 0.0),
                        ),
                    )
                    return

                allowed, reason = await can_open_and_reserve(
                    coin,
                    direction.lower(),
                    self.name,
                    _reserved_size_usdt,
                    metadata={
                        "pending_reservation": True,
                        "reserved_notional_usdt": round(_reserved_size_usdt, 4),
                        "reserved_at": time.time(),
                        "net_expectancy": dict(_net_expectancy_trace or {}),
                        **_projected_risk_reserve_metadata(_size_preview),
                        **_role_registry_metadata(_role_ctx, self.name),
                    },
                    balance_usdt=float(balance or 0.0),
                )
                if not allowed:
                    _existing = _registry.get(coin.upper(), {})
                    _existing_dir = str(_existing.get("direction", "") or "").lower()
                    _existing_engine = str(_existing.get("engine", "") or "")
                    _autonomy_on = bool(_s.get_val("engine_autonomy_enabled", True))
                    _confirm_on = bool(_s.get_val("confirm_path_enabled", True))
                    if (
                        _autonomy_on
                        and _confirm_on
                        and _existing
                        and _existing_dir == direction.lower()
                        and _existing_engine.upper() != self.name.upper()
                        and _within_duplicate_merge_window(_existing)
                    ):
                        _confirm_meta = await reg_confirm(
                            coin,
                            self.name,
                            metadata={
                                "last_confirm_direction": direction.lower(),
                                "last_confirm_reason": "same_coin_same_direction",
                                **_role_registry_metadata(_role_ctx, self.name),
                            },
                        )
                        if _confirm_meta:
                            _conf_val = float(
                                _v6_conf.confidence_score
                                if _v6_conf and hasattr(_v6_conf, "confidence_score")
                                else votes
                            )
                            logger.info(
                                "CONFIRM [%s] %s %s conf=%.2f | existing by %s confirmations=%d",
                                self.name,
                                coin,
                                direction,
                                _conf_val,
                                _confirm_meta.get("engine") or _existing_engine,
                                int(_confirm_meta.get("confirmation_count", 0) or 0),
                            )
                            _bump_silent_signal_counter("duplicate_position", self.name, coin, _s.get_engine_role(self.name))
                            return
                    _bump_silent_signal_counter("duplicate_position" if "duplicate" in str(reason or "").lower() else "capital_block", self.name, coin, _s.get_engine_role(self.name))
                    _rej(self.name, coin, direction, _v6_conf if _v6_conf else votes,
                         "reserve", reason)
                    return

                _handoff_key = f"{self.name}:{coin}:{direction.lower()}"
                _handoff_ok = _execution_supervisor.handoff(
                    _handoff_key,
                    self._place_trade(
                    coin, side, balance, votes, closes, highs, lows, volumes,
                    _v6_conf=_v6_conf, _v6_dd=_v6_dd, _v6_gate=_v6_gate, _v6_profile=_v6_profile,
                    _soft_accept_mult=_soft_accept_mult,
                    _soft_accept_reason=_soft_accept_reason,
                    _soft_accept_mode=_soft_accept_mode,
                    _soft_accept_session=_soft_accept_session,
                    _weekly_cap_mult=_weekly_cap_mult,
                    _weekly_cap_reason=_weekly_cap_reason,
                    _weekly_cap_decision=_weekly_cap_decision,
                    _candidate_quality=_candidate_quality,
                    _size_preview=_size_preview,
                    ),
                    engine=self.name,
                    coin=coin,
                    direction=direction.lower(),
                    lane_override=False,
                )
                if not _handoff_ok:
                    reg_release_pre_submit(coin)
                    _rej(self.name, coin, direction, _v6_conf if _v6_conf else votes,
                         "execution_handoff", "duplicate_active_handoff")
                else:
                    await asyncio.sleep(0)
                    await asyncio.sleep(0)
        except Exception:
            _bump_silent_signal_counter("unknown_exception", self.name, coin, _s.get_engine_role(self.name))
            logger.exception("[%s] evaluate %s failed", self.name, coin)

    async def _place_trade(self, coin, side, balance, votes, closes, highs, lows, volumes=None,
                           _v6_conf=None, _v6_dd=None, _v6_gate=None, _v6_profile=None,
                           _soft_accept_mult: float = 1.0,
                           _soft_accept_reason: str = "",
                           _soft_accept_mode: str = "",
                           _soft_accept_session: str = "",
                           _weekly_cap_mult: float = 1.0,
                           _weekly_cap_reason: str = "",
                           _weekly_cap_decision: str = "ACCEPT",
                           _candidate_quality: float = 0.0,
                           _lane_override: bool = False,
                           _size_preview: dict | None = None) -> None:
        try:
            direction = "long" if side == "buy" else "short"
            atr_val  = calc_atr(highs, lows, closes, 14)
            ticker   = await self.okx.get_ticker(coin)
            entry_px = float(ticker["last"])

            # Sanity check: ticker price should agree with recent candle closes within 10x.
            # A massive divergence (e.g. stale/wrong ticker) produces nonsensical ATR-based
            # TP/SL prices (observed: PEPE ticker returning ETH-range values → TP ≈ 2122).
            if closes and closes[-1] > 0 and entry_px > 0:
                _price_ratio = entry_px / closes[-1]
                if _price_ratio > 10.0 or _price_ratio < 0.1:
                    logger.warning(
                        "[%s] %s: ticker price %.8f diverges %.1fx from last candle %.8f "
                        "— likely stale/bad data. Skipping with 5m cooldown.",
                        self.name, coin, entry_px, _price_ratio, closes[-1],
                    )
                    _release(
                        self.name,
                        coin,
                        direction,
                        _v6_conf if _v6_conf else votes,
                        f"ticker_divergence: ticker={entry_px:.8f}, candle={closes[-1]:.8f}, ratio={_price_ratio:.2f}x",
                    )
                    self._cooldowns[coin] = time.time() + 300
                    return

            if "_conf_trace" not in locals():
                _fallback_conf = float(
                    _v6_conf.confidence_score
                    if _v6_conf is not None and hasattr(_v6_conf, "confidence_score")
                    else votes
                )
                _conf_trace = _confidence_stage_trace(
                    base_confidence=_fallback_conf,
                    consensus_lift=0.0,
                    quality_snapshot=locals().get("_quality_snapshot", {}),
                    admission_confidence=_fallback_conf,
                )
            return await self._place_trade_resolved(
                coin,
                side,
                balance,
                votes,
                closes,
                highs,
                lows,
                volumes=volumes,
                ticker=ticker,
                entry_px=entry_px,
                atr_val=atr_val,
                _v6_conf=_v6_conf,
                _v6_dd=_v6_dd,
                _v6_gate=_v6_gate,
                _v6_profile=_v6_profile,
                _soft_accept_mult=_soft_accept_mult,
                _soft_accept_reason=_soft_accept_reason,
                _soft_accept_mode=_soft_accept_mode,
                _soft_accept_session=_soft_accept_session,
                _weekly_cap_mult=_weekly_cap_mult,
                _weekly_cap_reason=_weekly_cap_reason,
                _weekly_cap_decision=_weekly_cap_decision,
                _candidate_quality=_candidate_quality,
                _lane_override=_lane_override,
                _size_preview=_size_preview,
                _conf_trace=_conf_trace,
            )

            if self.name == "TENJIN":
                lev = int(_s.get_val("tenjin_leverage", 8))
            elif self.name == "AMATERASU":
                lev = int(_s.get_val("amaterasu_leverage", 15))
            elif self.name == "TITAN":
                lev = int(_s.get_val("titan_leverage", 15))
            elif self.name == "RONIN":
                lev = int(_s.get_val("ronin_leverage", 15))
            elif self.name == "OROCHI":
                lev = int(_s.get_val("orochi_leverage", 15))
            else:
                lev = int(_s.get_val("leverage_default", 10))
            if _s.get_val("vol_leverage_enabled", True) and self.name not in ("TENJIN", "AMATERASU", "TITAN", "RONIN", "OROCHI"):
                lev = risk.get_vol_adjusted_leverage(atr_val, entry_px, lev)
            # All engines share the same configurable leverage cap/floor.
            lev = min(lev, int(_s.get_val("leverage_max", 15)))
            lev = max(lev, int(_s.get_val("leverage_min", 5)))

            _intent = _trade_intent.get_trade_intent(
                balance=balance,
                regime=str(_regime.get_regime() if _regime else ""),
            )
            _consensus_snapshot = _consensus.get_snapshot(coin, direction.upper())
            if not _s.get_val("pre_reserve_size_validation_enabled", True):
                _participation_cap_usdt, _participation_details = _participation_cap_window(
                    self.name,
                    direction,
                    balance,
                    capital_profile=_v6_profile,
                    soft_accept=bool(_soft_accept_reason),
                    session=_soft_accept_session,
                )

            size_mult = 0.85 if self.name == "SHOGUN" else 1.0
            cfg_floor, size_mode = _size_mode_base_notional(balance, self.name)
            risk_res  = risk.calculate_atr_size(balance, entry_px, atr_val, lev)
            # ── Flat notional sizing ──────────────────────────────────────────
            # size = max(position_size_usdt, balance × position_size_pct)
            # floor $100 notional (= $10 margin at 10x), cap via position guard.
            # risk_res used below for tier_obj (passed to exit_manager for sizing metadata).
            _pct_size  = balance * float(_s.get_val("position_size_pct", 0.05))
            _cfg_cap_pct = float(_s.get_val("position_size_cap_pct_balance", 100.0))
            _cfg_cap_usdt = float(_s.get_val("position_size_cap_usdt", 250.0))
            _raw_size  = cfg_floor
            base_size  = _raw_size * size_mult

            # Vote-based position sizing: higher confidence = larger position
            # TITAN & RONIN have their own per-engine multipliers (higher leverage + size on 3/3)
            if self.name == "TITAN":
                if votes >= 3:
                    size = base_size * float(_s.get_val("titan_size_mult_3votes", 2.0))
                else:
                    size = base_size * float(_s.get_val("titan_size_mult_2votes", 1.2))
            elif self.name == "IZANAGI":
                if votes >= 3:
                    size = base_size * float(_s.get_val("izanagi_size_mult_3votes", 0.9))
                else:
                    size = base_size * float(_s.get_val("izanagi_size_mult_2votes", 0.6))
            elif self.name == "RONIN":
                if votes >= 3:
                    size = base_size * float(_s.get_val("ronin_size_mult_3votes", 0.9))
                else:
                    size = base_size * float(_s.get_val("ronin_size_mult_2votes", 0.55))
            elif self.name == "OROCHI":
                if votes >= 3:
                    size = base_size * float(_s.get_val("orochi_size_mult_3votes", 0.9))
                else:
                    size = base_size * float(_s.get_val("orochi_size_mult_2votes", 0.6))
            elif votes >= 3.0:
                size = base_size * float(_s.get_val("size_multiplier_votes_3", 1.5))  # 150% for 3/3
            elif votes >= 2.5:
                size = base_size * 1.0  # 100% for 2.5+ votes (strong signal)
            else:
                size = base_size * float(_s.get_val("size_multiplier_votes_2", 0.7))  # 70% for 2/3
            # Hard max: cap_pct% of balance or cap_usdt — whichever is smaller.
            # Prevents a 2.0× vote multiplier on a large balance blowing through risk limits.
            # V6.1 dynamic sizing runs after this block and will further reduce if enabled.
            _v6_abs_floor = _effective_exec_floor(balance, coin, entry_px)
            _balance_cap = balance * (_cfg_cap_pct / 100)
            _risk_max_size_source = (
                "position_size_cap_pct_balance"
                if _balance_cap <= _cfg_cap_usdt
                else "position_size_cap_usdt"
            )
            max_size  = min(balance * (_cfg_cap_pct / 100), _cfg_cap_usdt)
            # Use V6 absolute floor (not cfg_floor) as the minimum for max_size.
            # On tiny accounts cfg_floor ($100) > balance_cap ($14) which would silently
            # override the balance cap and send oversized orders. V6.1 sizing handles
            # the final floor; here we only need to keep the window open.
            max_size  = max(max_size, _v6_abs_floor)
            if abs(max_size - _v6_abs_floor) <= 0.01:
                _risk_max_size_source = "exec_floor"
            min_size  = _v6_abs_floor
            size      = round(size * _perf.get_size_multiplier(), 2)   # apply perf mult BEFORE hard cap
            if not (_s.get_val("v6_enabled", False) and _s.get_val("dynamic_sizing_enabled", _s.get_val("v6_dynamic_sizing_enabled", False))):
                size = round(size * max(float(_intent.size_multiplier or 1.0), 0.0), 2)
            size      = max(min(size, max_size), min_size)

            # ── V6 / V6.1: Dynamic sizing overlay ────────────────────────────
            # V6.1: passes capital_profile so mode-based multiplier applies.
            # V5 size is treated as the base; V6.1 adjusts it.
            _size_reason = f"mode {size_mode} static base ${size:.2f} | intent {_intent.mode}"
            if _s.get_val("v6_enabled", False) and _s.get_val("dynamic_sizing_enabled", _s.get_val("v6_dynamic_sizing_enabled", False)):
                try:
                    from sizing_engine import compute_size as _v6_size
                    import telemetry_v6 as _t6
                    _conf_score = (_v6_conf.confidence_score
                                   if _v6_conf is not None else float(votes))
                    _dd_mult = (_v6_dd.size_multiplier if _v6_dd is not None else 1.0)
                    # Optional review gate. None is neutral and keeps sizing at multiplier 1.0.
                    _gate_mult = (_v6_gate.size_multiplier
                                  if _v6_gate is not None else 1.0)
                    _sz = _v6_size(
                        base_size=size * _gate_mult,
                        confidence_score=_conf_score,
                        atr_val=atr_val or 0.0,
                        entry_px=entry_px,
                        leverage=lev,
                        balance=balance,
                        dd_size_multiplier=_dd_mult,
                        capital_profile=_v6_profile,
                        intent_size_multiplier=_intent.size_multiplier,
                        effective_cap_usdt=_participation_cap_usdt,
                    )
                    _t6.log_sizing(self.name, coin, _sz)
                    size = _sz.final_size_usdt
                    _size_reason = _sz.reason
                except Exception:
                    logger.exception("[%s] V6.1 sizing error for %s — keeping V5 size %.2f", self.name, coin, size)
            # ── end V6/V6.1 dynamic sizing ────────────────────────────────────

            # ── Soft-accept size reduction ────────────────────────────────────
            # Applies only when a soft filter was bypassed (e.g. mild HTF, funding).
            # Reduces position size to compensate for the lower-quality entry.
            if _soft_accept_mult < 1.0:
                size = _apply_soft_accept_size(size, balance, _soft_accept_mult)
                logger.debug(
                    "[%s] soft-accept size ×%.2f → $%.2f for %s",
                    self.name, _soft_accept_mult, size, coin,
                )

            # ── Exchange minimum guard ────────────────────────────────────────
            # OKX perpetual minimum notional is $5; our internal minimum is
            # v6_min_position_size_usdt (default $25). Skip here so a tiny
            # balance can't generate a guaranteed-rejected order.
            _exch_min = _effective_exec_floor(balance, coin, entry_px)
            if (
                _participation_cap_usdt is not None
                and _participation_cap_usdt > 0
                and size > _participation_cap_usdt + 0.01
                and _participation_cap_usdt >= _exch_min
            ):
                logger.info(
                    "PARTICIPATION SCALE [%s] %s %s conf=%.2f | size $%.2f -> $%.2f | %s",
                    engine_log_name(self.name),
                    coin,
                    direction.upper(),
                    float(_v6_conf.confidence_score if _v6_conf is not None and hasattr(_v6_conf, "confidence_score") else votes),
                    size,
                    _participation_cap_usdt,
                    " | ".join(_participation_details),
                )
                size = round(float(_participation_cap_usdt), 2)
                _size_reason = f"{_size_reason} | participation cap ${size:.2f}"
            if size < _exch_min:
                _size_detail = f"size ${size:.2f} < min ${_exch_min:.2f} (bal ${balance:.2f})"
                if _soft_accept_reason:
                    _size_detail += (
                        f" | soft_accept[{_soft_accept_mode}/{_soft_accept_session}] "
                        f"via {_soft_accept_reason}"
                    )
                _rej(self.name, coin, direction, _v6_conf if _v6_conf else votes,
                     "sizing",
                     _size_detail)
                _release(
                    self.name,
                    coin,
                    direction,
                    _v6_conf if _v6_conf else votes,
                    f"sizing: {_size_detail}",
                )
                return

            if not _lane_override:
                from portfolio_allocator import check_lane_budget as _check_lane_budget
                _lane_ok, _lane_reason = _check_lane_budget(
                    self.name,
                    size,
                    balance,
                    capital_profile=_v6_profile,
                    soft_accept=bool(_soft_accept_reason),
                    session=_soft_accept_session,
                    debug_override_level=_dbg_level(_dbg_levels, "lane_budget") if _dbg_live else "normal",
                )
                if not _lane_ok:
                    if _soft_accept_reason:
                        _lane_reason = (
                            f"{_lane_reason} | soft_accept[{_soft_accept_mode}/{_soft_accept_session}] "
                            f"size ${size:.2f} via {_soft_accept_reason}"
                        )
                    _rej(self.name, coin, direction, _v6_conf if _v6_conf else votes,
                         "lane_budget",
                        _lane_reason
                        + _probe_suffix(
                            self.name,
                            coin,
                            direction,
                            balance=balance,
                            capital_profile=_v6_profile,
                            candidate_quality=float(_candidate_quality or 0.0),
                            confidence=float(_v6_conf.confidence_score if _v6_conf is not None and hasattr(_v6_conf, "confidence_score") else votes),
                            intent=_intent,
                            candidate_size_usdt=float(size or 0.0),
                        ))
                    _release(
                        self.name,
                        coin,
                        direction,
                        _v6_conf if _v6_conf else votes,
                        f"lane_budget: {_lane_reason}",
                    )
                    return

            tier_obj = risk_res["tier"]
            _short_rehab_note = ""
            _symbol_rehab_note = ""

            # ── Symbol-direction rehab (non-blocking; size-first) ─────────────
            try:
                state = _symbol_direction_rehab_state(
                    coin,
                    direction,
                    candidate_quality=float(_candidate_quality or 0.0),
                    consensus_count=int(_quality_snapshot.get("consensus_count", 0) or 0),
                    selector_bonus=float(_quality_snapshot.get("selector_bonus", 0.0) or 0.0),
                )
                if state.active and float(state.size_multiplier or 1.0) < 0.999:
                    _before = float(size or 0.0)
                    size = float(size or 0.0) * float(state.size_multiplier or 1.0)
                    _min_exec = float(_effective_exec_floor(balance, coin, entry_px) or 0.0)
                    if _min_exec > 0:
                        size = max(size, _min_exec)
                    if abs(size - _before) > 0.005:
                        _symbol_rehab_note = (
                            f"symbol_rehab stage={str(state.stage or '')} losses={int(state.loss_count or 0)} "
                            f"x{float(state.size_multiplier or 1.0):.2f}"
                        )
                        _size_reason = f"{_size_reason} | {_symbol_rehab_note}"
            except Exception:
                _symbol_rehab_note = ""

            # ── AMATERASU macro sizing hint (non-blocking) ───────────────────
            if (
                str(self.name or "").upper() == "AMATERASU"
                and float(_macro_size_mult or 1.0) != 1.0
            ):
                try:
                    _before_macro = float(size or 0.0)
                    _macro_applied = float(_macro_size_mult or 1.0)
                    if _macro_applied > 0:
                        size = float(size or 0.0) * _macro_applied
                        _min_exec = float(_effective_exec_floor(balance, coin, entry_px) or 0.0)
                        if _min_exec > 0:
                            size = max(size, _min_exec)
                        if abs(size - _before_macro) > 0.005:
                            _size_reason = f"{_size_reason} | amaterasu_macro x{_macro_applied:.2f}"
                except Exception:
                    pass

            # ── Short-side rehab (non-blocking; size-only) ───────────────────
            if (
                str(direction or "").lower() == "short"
                and bool(_s.get_val("short_rehab_enabled", True))
            ):
                try:
                    _weak_regime = str(_regime.get_regime() if _regime else "").upper()
                    _weak = _weak_regime in {"RANGING", "CHOPPY", "SIDEWAYS"}
                    _adx_1h = 0.0
                    try:
                        if closes_1h and highs_1h and lows_1h and len(closes_1h) >= 15 and len(highs_1h) >= 15 and len(lows_1h) >= 15:
                            _adx_1h = float(calc_adx(highs_1h, lows_1h, closes_1h, 14) or 0.0)
                    except Exception:
                        _adx_1h = 0.0
                    _adx_floor = float(_s.get_val("short_rehab_min_adx_1h", 18.0) or 18.0)
                    if _weak or (_adx_1h > 0 and _adx_1h < _adx_floor):
                        _before = float(size or 0.0)
                        _mult = float(_s.get_val("short_rehab_weak_regime_size_mult", 0.90) or 0.90)
                        if _mult > 0 and _mult < 0.999:
                            size = float(size or 0.0) * _mult
                            _min_exec = float(_effective_exec_floor(balance, coin, entry_px) or 0.0)
                            if _min_exec > 0:
                                size = max(size, _min_exec)
                            if abs(size - _before) > 0.005:
                                _short_rehab_note = f"short_rehab regime={_weak_regime or 'unknown'} adx1h={_adx_1h:.1f}/{_adx_floor:.1f} x{_mult:.2f}"
                                _size_reason = f"{_size_reason} | {_short_rehab_note}"
                except Exception:
                    pass
            # ── Per-engine SL/TP (structure-aware, minimum 1:4 RRR) ───────────
            stop_atr_val = _stop_geometry_atr(coin, atr_val)
            stop, take, tp2_price = _sl_tp.compute(
                engine   = self.name,
                side     = side,
                entry    = entry_px,
                closes   = closes,
                highs    = highs,
                lows     = lows,
                atr_val  = stop_atr_val,
                coin     = coin,
                leverage = lev,
                volumes  = volumes or [],
            )

            # ── TP distance multiplier (non-blocking; profit_target no longer blocks entries) ─
            _tp_adjust = _profit_targets.apply_profit_targeting(
                entry_px,
                side,
                take,
                tp2_price,
                size,
                tp_distance_multiplier=_intent.tp_multiplier,
            )
            take = float(_tp_adjust.get("tp1", take) or take)
            tp2_price = float(_tp_adjust.get("tp2", tp2_price) or tp2_price)
            _tp_reason = str(_tp_adjust.get("profit_reason") or "base targets")

            # If the proposed SL sits inside a dense liquidation cluster,
            # widen it by 0.3% so we don't get swept by the liq cascade.
            # Uses cached TSUNAMI data as the freshness signal; falls back
            # to synthetic heatmap zones from liq_heatmap.estimate_zones().
            if _s.get_val("liq_heatmap_sl_enabled", True) and stop > 0:
                try:
                    from liq_heatmap import check_sl_in_liq_zone
                    _liq_risky, _liq_msg = check_sl_in_liq_zone(
                        stop, entry_px, side, buffer_pct=0.3
                    )
                    if _liq_risky:
                        _widen = entry_px * 0.003
                        stop = (round(stop - _widen, 8) if side == "buy"
                                else round(stop + _widen, 8))
                        logger.debug(
                            "[%s] SL widened 0.3%% (liq cluster): %.8f | %s",
                            self.name, stop, _liq_msg,
                        )
                except Exception:
                    pass

            _risk_size_meta = {}
            _risk_size = _risk_sized_notional_from_stop(
                engine_name=self.name,
                coin=coin,
                side=side,
                entry_px=float(entry_px or 0.0),
                stop_px=float(stop or 0.0),
                balance=float(balance or 0.0),
                current_size_usdt=float(size or 0.0),
                base_size_usdt=float(base_size or size or 0.0),
                min_exec_usdt=float(_effective_exec_floor(balance, coin, entry_px) or 0.0),
                max_size_usdt=float(max_size or 0.0),
                max_size_source=_risk_max_size_source,
            )
            _risk_size_meta = dict(_risk_size.get("meta", {}) or {})
            if _risk_size.get("skip"):
                _size_detail = str(_risk_size.get("reason") or "risk_sizing_below_min")
                _paper_validation.record_skip(
                    **_paper_validation_payload(
                        engine_name=self.name,
                        coin=coin,
                        side=side,
                        entry_px=float(entry_px or 0.0),
                        stop_px=float(stop or 0.0),
                        final_size_usdt=float(_risk_size.get("size_usdt", 0.0) or 0.0),
                        risk_meta=_risk_size_meta,
                    ),
                    skip_reason=_size_detail,
                )
                _rej(self.name, coin, direction, _v6_conf if _v6_conf else votes, "sizing", _size_detail)
                _release(
                    self.name,
                    coin,
                    direction,
                    _v6_conf if _v6_conf else votes,
                    f"sizing: {_size_detail}",
                )
                return
            _before_risk_size = float(size or 0.0)
            size = float(_risk_size.get("size_usdt", size) or size)
            if abs(size - _before_risk_size) > 0.009:
                _size_reason = f"{_size_reason} | {_risk_size.get('reason', 'risk_sized')}"

            # Guard: sub-tick coins (e.g. SATS at 0.00000001) cause TP/SL to round
            # to 0 or equal entry — OKX rejects tpTriggerPx/slTriggerPx in both cases.
            # Add a 1-hour cooldown so the engine stops hammering OKX with bad orders.
            if stop <= 0 or take <= 0 or stop == entry_px or take == entry_px:
                logger.warning(
                    "[%s] _place_trade %s: TP/SL degenerate at price %.8f "
                    "(tp=%.8f sl=%.8f) — coin price too small. Skipping with 1h cooldown.",
                    self.name, coin, entry_px, take, stop,
                )
                _release(
                    self.name,
                    coin,
                    direction,
                    _v6_conf if _v6_conf else votes,
                    f"tpsl_degenerate: entry={entry_px:.8f}, tp={take:.8f}, sl={stop:.8f}",
                )
                self._cooldowns[coin] = time.time() + 3600
                return

            # ── Dynamic realized risk ceiling (prevents oversized SL tails) ───
            _risk_cap_meta = {}
            try:
                from risk_ceiling import apply_realized_risk_ceiling as _apply_risk_ceiling
                _min_exec = float(_effective_exec_floor(balance, coin, entry_px) or 0.0)
                _rc = _apply_risk_ceiling(
                    entry_px=float(entry_px or 0.0),
                    stop_px=float(stop or 0.0),
                    size_usdt=float(size or 0.0),
                    balance_usdt=float(balance or 0.0),
                    min_exec_usdt=_min_exec,
                    side=str(side or ""),
                    settings=_s,
                )
                size = float(_rc.size_usdt or size)
                stop = float(_rc.stop_loss or stop)
                _risk_cap_meta = dict(_rc.meta or {})
                if _risk_size_meta:
                    _risk_cap_meta["risk_sizing"] = _risk_size_meta
                if bool(_s.get_val("realized_risk_ceiling_telemetry_enabled", True)) and str(_risk_cap_meta.get("risk_ceiling_action", "none")) != "none":
                    logger.info(
                        "RISK CEILING [%s] %s %s cap=$%.2f expected=$%.3f sl_dist=%.2f%% size=%.2f->%.2f sl=%.8f->%.8f",
                        self.name,
                        coin,
                        str(direction or "").upper(),
                        float(_risk_cap_meta.get("risk_cap_usdt", 0.0) or 0.0),
                        float(_risk_cap_meta.get("expected_risk_usdt", 0.0) or 0.0),
                        float(_risk_cap_meta.get("sl_dist_pct", 0.0) or 0.0),
                        float(_risk_cap_meta.get("size_before", 0.0) or 0.0),
                        float(_risk_cap_meta.get("size_after", 0.0) or 0.0),
                        float(_risk_cap_meta.get("sl_before", 0.0) or 0.0),
                        float(_risk_cap_meta.get("sl_after", 0.0) or 0.0),
                    )
            except Exception:
                _risk_cap_meta = {}

            # ── V6 / V6.1: Execution router + telemetry summary ─────────────
            _alloc_meta_input = dict(_risk_cap_meta or {})
            if _risk_size_meta and "risk_sizing" not in _alloc_meta_input:
                _alloc_meta_input["risk_sizing"] = _risk_size_meta
            _alloc_meta_input["side"] = str(side or "")
            _alloc_meta_input["tp2_price"] = float(tp2_price or 0.0)
            _alloc_meta_input["expected_slippage_pct"] = float(
                _s.get_val("net_expectancy_default_slippage_pct", 0.03) or 0.03
            )
            _alloc_result = _adaptive_capital_allocation(
                coin=coin,
                balance=float(balance or 0.0),
                size_usdt=float(size or 0.0),
                entry_px=float(entry_px or 0.0),
                stop_px=float(stop or 0.0),
                leverage=int(lev or 1),
                risk_meta=_alloc_meta_input,
                participation_cap_usdt=_participation_cap_usdt,
                okx=self.okx,
                side=str(side or ""),
            )
            _alloc = await _alloc_result if inspect.isawaitable(_alloc_result) else _alloc_result
            _alloc_meta = dict(_alloc.get("meta", {}) or {})
            if _alloc.get("skip"):
                _size_detail = str(_alloc.get("reason") or "allocator_rejected")
                _paper_validation.record_skip(
                    **_paper_validation_payload(
                        engine_name=self.name,
                        coin=coin,
                        side=side,
                        entry_px=float(entry_px or 0.0),
                        stop_px=float(stop or 0.0),
                        final_size_usdt=float(_alloc.get("size_usdt", 0.0) or 0.0),
                        risk_meta={**_risk_size_meta, **_alloc_meta},
                    ),
                    skip_reason=_size_detail,
                )
                _rej(self.name, coin, direction, _v6_conf if _v6_conf else votes, "capital", _size_detail)
                _release(
                    self.name,
                    coin,
                    direction,
                    _v6_conf if _v6_conf else votes,
                    f"capital: {_size_detail}",
                )
                return
            _before_alloc_size = float(size or 0.0)
            size = float(_alloc.get("size_usdt", size) or size)
            if abs(size - _before_alloc_size) > 0.009:
                _size_reason = f"{_size_reason} | {_alloc.get('reason', 'allocator_scaled')}"
            if _alloc_meta:
                _risk_cap_meta["capital_allocator"] = _alloc_meta

            if _s.get_val("v6_enabled", False):
                try:
                    from execution_router import build_execution_plan as _v6_exec
                    import telemetry_v6 as _t6
                    # V6.1: compute live spread for routing decision
                    _spread_pct = 0.0
                    try:
                        _bid = float(ticker.get("bidPx", 0) or 0)
                        _ask = float(ticker.get("askPx", 0) or 0)
                        if _bid > 0 and _ask > 0 and entry_px > 0:
                            _spread_pct = (_ask - _bid) / entry_px * 100
                    except Exception:
                        pass
                    _exec_plan = _v6_exec(
                        coin, side, size, entry_px, atr_val or 0.0,
                        spread_pct=_spread_pct,
                        capital_profile=_v6_profile,
                    )
                    _t6.log_execution(self.name, coin, _exec_plan)
                    _t6.log_v6_summary(
                        engine=self.name, coin=coin, direction=side,
                        conf=_v6_conf,
                        sz=None,
                        gate=_v6_gate,
                        dd=_v6_dd,
                        plan=_exec_plan,
                        final_action="trade",
                        capital_mode=_v6_profile.mode if _v6_profile else "",
                    )
                except Exception:
                    pass
            # ── end V6/V6.1 execution router ──────────────────────────────────

            # Resolve one profit-taking owner.  HYBRID uses local staged
            # management when enabled; only EXCHANGE_FULL receives an exchange TP.
            tp_mode = str(_s.get_val("tp_mode", "reference_only") or "reference_only").strip().lower()
            if tp_mode not in {"off", "reference_only", "exchange_tp", "hybrid"}:
                tp_mode = "reference_only"
            from tp_ownership import derive_tp_owner, owner_flags
            tp_owner = derive_tp_owner(
                tp_mode,
                partial_enabled=bool(_s.get_val("partial_exit_enabled", True)),
            )
            local_tp1_enabled, local_tp2_enabled = owner_flags(
                tp_owner,
                partial_enabled=bool(_s.get_val("partial_exit_enabled", True)),
            )
            tp_ref = float(tp2_price or 0.0) if tp_mode != "off" else 0.0
            tp_exchange = float(tp2_price or 0.0) if tp_owner == "EXCHANGE_FULL" else 0.0

            result = await _execute_central_entry(
                okx=self.okx,
                engine_name=self.name,
                coin=coin,
                side=side,
                size_usdt=size,
                entry_px=entry_px,
                stop_px=stop,
                take_profit=tp_exchange,
                tp1_price=take,
                tp2_price=tp2_price,
                atr_value=float(atr_val or 0.0),
                leverage=lev,
                allocator_meta=_alloc_meta,
                execution_plan=_exec_plan,
                confidence=float(_v6_conf.confidence_score if _v6_conf is not None and hasattr(_v6_conf, "confidence_score") else votes),
                score=float(votes or 0.0),
                tp_owner=tp_owner,
                tp_mode_at_entry=tp_mode,
            )
            if not result:
                _release(
                    self.name,
                    coin,
                    direction,
                    _v6_conf if _v6_conf else votes,
                    self.okx.consume_last_order_skip_reason() or "place_market_order returned no result",
                )
                return

            # V6.1: record trade entry for frequency tracking
            if _s.get_val("v6_enabled", False) and _s.get_val("v6_trade_frequency_enabled", True):
                try:
                    from capital_manager import record_trade_entry as _v6_record_entry
                    _v6_record_entry()
                except Exception:
                    pass

            actual_coin = result.get("actual_coin", coin).upper()
            effective_lev = int(
                result.get("position_effective_leverage")
                or 1
            )
            leverage_truth_metadata = {
                "requested_leverage": int(result.get("requested_leverage", lev) or lev),
                "exchange_configured_leverage": result.get("exchange_configured_leverage"),
                "position_effective_leverage": result.get("position_effective_leverage"),
                "leverage_truth_status": str(result.get("leverage_truth_status", "") or ""),
                "leverage_truth_timestamp": str(result.get("leverage_truth_timestamp", "") or ""),
                "leverage_truth_source": str(result.get("leverage_truth_source", "") or ""),
                "leverage_truth_mismatch": bool(result.get("leverage_truth_mismatch", False)),
            }
            actual_size = float(result.get("size_usdt", size) or size)
            if actual_coin != coin:
                logger.info("[%s] NANO rerouted %s -> %s", self.name, coin, actual_coin)
                _release(
                    self.name,
                    coin,
                    direction,
                    _v6_conf if _v6_conf else votes,
                    f"nano_reroute: rerouted_to={actual_coin}",
                )
                reroute_metadata = None
                if _lane_override:
                    reroute_metadata = {
                        "lane_override_active": True,
                        "lane_override_reason": "lane_budget",
                    }
                ok, why = await can_open_and_reserve(
                    actual_coin,
                    "long" if side == "buy" else "short",
                    self.name,
                    actual_size,
                    entry=float(result.get("fillPx", 0.0) or entry_px or 0.0),
                    stop_loss=float(stop or 0.0),
                    metadata=reroute_metadata,
                    balance_usdt=float(balance or 0.0),
                )
                if not ok:
                    # Position is OPEN on exchange but can't register locally — close it immediately
                    logger.error("[%s] NANO %s blocked (%s) - closing orphan position on exchange.", self.name, actual_coin, why)
                    try:
                        pos_side = "long" if side == "buy" else "short"
                        from execution_router import router_for
                        await router_for(self.okx).close(actual_coin, pos_side, actual_size, emergency=True)
                    except Exception as close_err:
                        logger.error("[%s] Failed to close orphan NANO %s: %s", self.name, actual_coin, close_err)
                    return

                # ── NANO SL/TP FIX ──────────────────────────────────────────────
                # The original coin's stop/take are priced in a completely different
                # scale (e.g. $2176 SL for a $0.000003 PEPE trade).  place_market_order
                # now passes sl_price=None so OKX computes fresh base SL/TP for the
                # NANO coin — pull those corrected values back from the result dict.
                stop      = result.get("stop_loss",   stop)
                # FINAL PASS: exchange TP is disabled; keep internal targets for telemetry/exit planning.
                take      = take
                # Recalculate tp2 using actual_coin's own ATR so it's in the right
                # price space, not inherited from the original (expensive) coin.
                _a_highs  = get_highs(actual_coin)
                _a_lows   = get_lows(actual_coin)
                _a_closes = get_closes(actual_coin)
                if _a_highs and _a_lows and _a_closes and len(_a_closes) >= 14:
                    _nano_atr = calc_atr(_a_highs, _a_lows, _a_closes, 14)
                else:
                    _nano_atr = atr_val   # fallback: original coin's ATR (still better than nothing)
                if _nano_atr and _nano_atr > 0:
                    _tp2_dist = _nano_atr * float(_s.get_val("atr_tp2_multiplier", 10.0))
                else:
                    _tp2_dist = abs(take - stop) * 2   # simple 2× SL-distance fallback
                fill_for_tp2 = float(result.get("fillPx", 0)) or entry_px
                if not (_nano_atr and _nano_atr > 0):
                    # When ATR isn't available, compute TP2 distance from SL distance (not from any exchange TP).
                    _tp2_dist = abs(fill_for_tp2 - stop) * 2
                tp2_price = (round(fill_for_tp2 + _tp2_dist, 8) if side == "buy"
                             else round(fill_for_tp2 - _tp2_dist, 8))
                # Set cooldown on the NANO target coin so that other original coins
                # that also NANO-route here cannot pile in before the cooldown expires.
                self._cooldowns[actual_coin] = time.time()
                logger.info(
                    "[%s] NANO %s recalculated SL=%.8f TP=%.8f TP2=%.8f",
                    self.name, actual_coin, stop, take, tp2_price,
                )
                # ── end NANO SL/TP FIX ──────────────────────────────────────────

            fill = float(result.get("fillPx", 0)) or entry_px
            oid  = result.get("ordId", "--")
            cloid = str(result.get("clOrdId", result.get("client_order_id", "")) or "")
            _paper_validation_ctx = _paper_validation_payload(
                engine_name=self.name,
                coin=actual_coin,
                side=side,
                entry_px=float(fill or 0.0),
                stop_px=float(stop or 0.0),
                final_size_usdt=float(actual_size or 0.0),
                risk_meta=_risk_size_meta,
                order_id=oid,
            )
            _paper_validation.record_entry(**_paper_validation_ctx)
            _conf_val = float(
                _v6_conf.confidence_score
                if _v6_conf is not None and hasattr(_v6_conf, "confidence_score")
                else votes
            )
            _quality_snap = _candidate_quality_snapshot(
                self.name,
                actual_coin,
                direction.upper(),
                balance,
                float(votes),
                float(_conf_val),
            ) or {}
            _quality_score = float(_quality_snap.get("quality_score", 0.0) or 0.0)
            _opened_at = datetime.now(timezone.utc).isoformat()
            _role_ctx = _position_strength_context(
                self.name,
                actual_coin,
                direction.upper(),
                _quality_snap,
                float(_conf_val),
            )
            _role_adj = _role_quality_adjustment(_role_ctx)
            _quality_score = max(round(_quality_score + _role_adj, 3), 0.0)
            _quality_snap["quality_score"] = _quality_score
            _quality_snap["role_quality_adjustment"] = _role_adj
            _role_ctx = _position_strength_context(
                self.name,
                actual_coin,
                direction.upper(),
                _quality_snap,
                float(_conf_val),
            )
            if actual_coin in _live_registry_view():
                # Fix: populate size/entry/SL/TP/leverage so 3-5-7 rule and risk checks use real data
                _reg_update(
                    actual_coin,
                    size_usdt=actual_size,
                    entry=fill,
                    leverage=effective_lev,
                    stop_loss=stop,
                    take_profit=take,
                    order_id=oid,
                    metadata={
                        "atr": float(atr_val or 0.0),
                        "vote_score": float(votes),
                        "confidence_score": float(_conf_val),
                        "quality_score": float(_quality_score),
                        "quality_score_base": float(_quality_snap.get("quality_score_base", 0.0) or 0.0),
                        "quality_score_winrate_adj": float(_quality_snap.get("quality_score_winrate_adj", 0.0) or 0.0),
                        "quality_regime_bonus": float(_quality_snap.get("quality_regime_bonus", 0.0) or 0.0),
                        "quality_confluence_bonus": float(_quality_snap.get("quality_confluence_bonus", 0.0) or 0.0),
                        "quality_strictness_penalty": float(_quality_snap.get("quality_strictness_penalty", 0.0) or 0.0),
                        "quality_advisory_penalty": float(_quality_snap.get("advisory_penalty", 0.0) or 0.0),
                        "role_quality_adjustment": float(_role_adj),
                        "winrate_profile": str(_quality_snap.get("winrate_profile", "") or ""),
                        "winrate_enabled": bool(_quality_snap.get("winrate_enabled", False)),
                        "regime_at_entry": str(_quality_snap.get("regime", "") or ""),
                        "lane": str(_quality_snap.get("lane", "") or ""),
                        "tp2_price": float(tp2_price),
                        "tp_mode": str(tp_mode),
                        "tp_mode_at_entry": str(tp_mode),
                        "tp_owner": str(tp_owner),
                        "local_tp1_enabled": bool(local_tp1_enabled),
                        "local_tp2_enabled": bool(local_tp2_enabled),
                        "ownership_timestamp": datetime.now(timezone.utc).isoformat(),
                        "tp_owner_timestamp": datetime.now(timezone.utc).isoformat(),
                        "partials_enabled_at_entry": bool(_s.get_val("partial_exit_enabled", True)),
                        "tp_ref_price": float(tp_ref or 0.0),
                        "requested_coin": coin,
                        "entry_client_order_id": cloid,
                        "entry_clOrdId": cloid,
                        "recovery_mode": "managed",
                        **leverage_truth_metadata,
                        "policy_tag": "2026-04-02",
                        "managed_by_engine": True,
                        "opened_at": _opened_at,
                        "opened_ts": time.time(),
                        "weekly_cap_decision": str(_wk_decision or ""),
                        "weekly_cap_mult": float(_wk_mult or 1.0),
                        "weekly_cap_reason": str(_wk_reason or ""),
                        "admission_confidence": float(_admission_conf or _signal_conf or 0.0),
                        "confidence_stage_trace": dict(_conf_trace or {}),
                        **_role_registry_metadata(_role_ctx, self.name),
                        **(_dbg_registry_meta()),
                        **({"lane_override_active": True} if _lane_override else {}),
                    },
                )

            _em.register(
                actual_coin,
                direction,
                fill,
                actual_size,
                atr_val or 0.0,
                stop,
                take,
                tp2_price,
                oid,
                client_order_id=cloid,
                engine_name=self.name,
                vote_score=votes,
                leverage=effective_lev,
                requested_leverage=leverage_truth_metadata["requested_leverage"],
                exchange_configured_leverage=leverage_truth_metadata["exchange_configured_leverage"],
                position_effective_leverage=leverage_truth_metadata["position_effective_leverage"],
                leverage_truth_status=leverage_truth_metadata["leverage_truth_status"],
                leverage_truth_timestamp=leverage_truth_metadata["leverage_truth_timestamp"],
                requested_vs_configured_mismatch=bool(leverage_truth_metadata.get("requested_vs_configured_mismatch", False)),
                configured_vs_effective_mismatch=bool(leverage_truth_metadata.get("configured_vs_effective_mismatch", False)),
                macro_runner_trust_mult=float(_macro_runner_trust or 1.0),
                expected_risk_usdt=float((_risk_cap_meta or {}).get("expected_risk_usdt", 0.0) or 0.0),
                risk_cap_usdt=float((_risk_cap_meta or {}).get("risk_cap_usdt", 0.0) or 0.0),
                risk_ceiling_action=str((_risk_cap_meta or {}).get("risk_ceiling_action", "") or ""),
                paper_validation=dict(_paper_validation_ctx),
                tp_owner=tp_owner,
                tp_mode_at_entry=tp_mode,
                local_tp1_enabled=local_tp1_enabled,
                local_tp2_enabled=local_tp2_enabled,
            )
            _dbg_record_accept(
                self.name,
                actual_coin,
                direction.upper(),
                _conf_val,
                size_usdt=actual_size,
                candidate_quality=_quality_score,
                regime=str(_quality_snap.get("regime", "") or ""),
                size_decision=_size_reason,
                reason=_entry_reason,
            )
            if str(_wk_decision or "").upper() == "ACCEPT_REDUCED" and float(_wk_mult or 1.0) < 0.999:
                try:
                    _wcap.mark_reduced_accept()
                except Exception:
                    pass

            self._open[actual_coin] = {
                "side": side, "entry": fill, "stop_loss": stop, "take_profit": take,
                "tp2": tp2_price, "size": actual_size, "leverage": effective_lev, "atr": atr_val or 0.0,
                "vote_score": votes, "order_id": oid, "engine": self.name,
                "entry_client_order_id": cloid,
                "requested_coin": coin,
                "confidence_score": float(_conf_val),
                "quality_score": float(_quality_score),
                "role_quality_adjustment": float(_role_adj),
                "opened_at": _opened_at,
                "position_strength": float(_role_ctx.get("position_strength", 0.0) or 0.0),
                "trade_class": str(_role_ctx.get("trade_class", "") or ""),
                "preferred_exit_style": str(_role_ctx.get("preferred_exit_style", "") or ""),
                "contributor_roles": list(_role_ctx.get("contributor_roles", []) or []),
                **_engine_attribution.build_attribution(
                    primary_engine=self.name,
                    confirming_engines=list(_role_ctx.get("confirming_engines", []) or []),
                    role_contributions=dict(_role_ctx.get("role_contributions", {}) or {}),
                    size_multiplier=float(_role_ctx.get("size_multiplier", 1.0) or 1.0),
                ),
                "weekly_cap_decision": str(_wk_decision or ""),
                "weekly_cap_mult": float(_wk_mult or 1.0),
                "paper_validation": dict(_paper_validation_ctx),
            }
            _live_consensus = _consensus.get_snapshot(actual_coin, direction.upper())
            _entry_reason = "standard entry"
            if _soft_accept_reason:
                _entry_reason = (
                    f"soft_accept[{_soft_accept_mode}/{_soft_accept_session}] via {_soft_accept_reason}"
                )
            elif _lane_override:
                _entry_reason = "lane override"
            elif _live_consensus.agreement_count > 1:
                _entry_reason = f"multi-engine agreement: {_live_consensus.summary}"
            _trade_explanation = _format_trade_explanation(
                intent=_intent,
                consensus=_live_consensus,
                confidence=_conf_val,
                quality_score=_quality_score,
                base_size=base_size,
                final_size=actual_size,
                leverage=effective_lev,
                size_reason=_size_reason,
                margin_used=(actual_size / effective_lev) if effective_lev > 0 else actual_size,
                tp_reason=_tp_reason,
                entry_reason=_entry_reason,
            )
            _accept_suffix = ""
            if _soft_accept_reason:
                _accept_suffix = f" soft={_soft_accept_mode}/{_soft_accept_session}"
            if str(_wk_decision or "").upper() == "ACCEPT_REDUCED" and float(_wk_mult or 1.0) < 0.999:
                _accept_suffix = f"{_accept_suffix} wk=x{float(_wk_mult or 1.0):.2f}".rstrip()
            logger.info(
                "ACCEPT [%s] %s %s conf=%.2f quality=%.1f | size=$%.2f lev=%dx%s",
                self.name,
                actual_coin,
                direction.upper(),
                _conf_val,
                _quality_score,
                actual_size,
                effective_lev,
                _accept_suffix,
            )
            star   = "***" if votes >= 3 else "**+"
            routed = f" [NANO->{actual_coin}]" if actual_coin != coin else ""
            logger.info("[%s] OPEN %s %s%s %s lev=%dx $%.2f SL=%.6f TP=%.6f",
                        self.name, side.upper(), actual_coin, routed, star, effective_lev, actual_size, stop, take)
            await alert_trade_executed(
                side.upper(),
                actual_coin,
                actual_size,
                fill,
                stop,
                tp_ref,
                oid,
                effective_lev,
                extra=f"{self.emoji} {self.name}",
                tp_mode=tp_mode,
                explanation="",
            )
        except Exception as e:
            try:
                _requested = str(coin or "").upper()
                _preserved_coin = ""
                _registry_view = _live_registry_view()
                for _coin_key, _entry in list(_registry_view.items()):
                    if not isinstance(_entry, dict):
                        continue
                    _matches_request = (
                        _coin_key == _requested
                        or str(_entry.get("requested_coin", "") or "").upper() == _requested
                    )
                    _has_live_evidence = (
                        bool(_entry.get("exchange_confirmed", False))
                        or float(_entry.get("entry", 0.0) or 0.0) > 0.0
                        or bool(str(_entry.get("order_id", "") or "").strip())
                    )
                    if _matches_request and _has_live_evidence:
                        _preserved_coin = _coin_key
                        break
                if _preserved_coin:
                    logger.exception(
                        "[%s] post-placement error — trade preserved %s order=%s: %s",
                        self.name,
                        _preserved_coin,
                        _registry_view.get(_preserved_coin, {}).get("order_id", "") or "?",
                        e,
                    )
                    try:
                        _reg_update(
                            _preserved_coin,
                            metadata={
                                "post_placement_error": f"{type(e).__name__}: {e}",
                                "post_placement_error_at": time.time(),
                                "recovery_mode": "managed",
                                "managed_by_engine": True,
                                "exchange_confirmed": os.getenv("PAPER_TRADING", "true").strip().lower() != "true",
                                "paper_confirmed": os.getenv("PAPER_TRADING", "true").strip().lower() == "true",
                            },
                        )
                    except Exception:
                        logger.exception("[%s] failed to mark preserved trade after post-placement error", self.name)
                    return
            except Exception:
                pass
            logger.exception("[%s] place_trade %s failed", self.name, coin)
            _release(
                self.name,
                coin,
                direction if 'direction' in locals() else ("long" if side == "buy" else "short"),
                _v6_conf if _v6_conf else votes,
                f"exception: {type(e).__name__}: {e}",
            )
            # Trigger global margin cooldown on "insufficient balance" errors
            # so all engines stop hammering OKX with doomed orders.
            is_margin_error = isinstance(e, OKXOrderError) and getattr(e, "category", "") == "insufficient_margin"
            err_str = str(e).lower()
            if not is_margin_error and (
                "insufficient margin" in err_str
                or "insufficient balance" in err_str
                or "too low for borrowing" in err_str
            ):
                is_margin_error = True
            if is_margin_error:
                _set_margin_cooldown()

    async def _place_trade_resolved(self, coin, side, balance, votes, closes, highs, lows, volumes=None,
                                     ticker=None, entry_px: float = 0.0, atr_val: float = 0.0,
                                     _v6_conf=None, _v6_dd=None, _v6_gate=None, _v6_profile=None,
                                     _soft_accept_mult: float = 1.0,
                                     _soft_accept_reason: str = "",
                                     _soft_accept_mode: str = "",
                                     _soft_accept_session: str = "",
                                     _weekly_cap_mult: float = 1.0,
                                     _weekly_cap_reason: str = "",
                                     _weekly_cap_decision: str = "ACCEPT",
                                     _candidate_quality: float = 0.0,
                                     _lane_override: bool = False,
                                     _size_preview: dict | None = None,
                                     _conf_trace: dict | None = None) -> None:
        direction = "long" if side == "buy" else "short"
        _conf_trace = dict(_conf_trace or {})
        _signal_conf = float(
            _v6_conf.confidence_score
            if _v6_conf is not None and hasattr(_v6_conf, "confidence_score")
            else votes
        )
        _admission_conf = float(_conf_trace.get("admission_confidence", _signal_conf) or _signal_conf)

        # Debug override integration must NEVER break execution.
        # Default posture is "off" -> lane_budget debug_override_level="normal".
        try:
            _dbg = _dbg_snap()
            _dbg_mode_name = _dbg_mode(_dbg.get("mode"))
            _dbg_live = (_dbg_mode_name == "live_override")
            _dbg_levels = _dbg.get("levels", {}) or {}
        except Exception:
            _dbg_mode_name = "off"
            _dbg_live = False
            _dbg_levels = {}

        ticker = ticker or await self.okx.get_ticker(coin)
        entry_px = float(entry_px or ticker.get("last", 0.0) or 0.0)
        atr_val = float(atr_val or calc_atr(highs, lows, closes, 14) or 0.0)

        _preview = _size_preview
        if _preview is None:
            _preview = _preview_trade_execution(
                coin=coin,
                engine_name=self.name,
                side=side,
                balance=balance,
                votes=float(votes),
                closes=closes,
                highs=highs,
                lows=lows,
                entry_px=entry_px,
                v6_conf=_v6_conf,
                v6_dd=_v6_dd,
                v6_gate=_v6_gate,
                v6_profile=_v6_profile,
                soft_accept_mult=_soft_accept_mult,
                soft_accept_reason=_soft_accept_reason,
                soft_accept_mode=_soft_accept_mode,
                soft_accept_session=_soft_accept_session,
                weekly_cap_mult=_weekly_cap_mult,
                weekly_cap_reason=_weekly_cap_reason,
                weekly_cap_decision=_weekly_cap_decision,
                candidate_quality=float(_candidate_quality or 0.0),
            )
        if _preview.get("skip"):
            _size_detail = str(_preview.get("reject_reason") or "reduced size below executable minimum")
            _rej(self.name, coin, direction, _v6_conf if _v6_conf else votes, "sizing", _size_detail)
            _release(
                self.name,
                coin,
                direction,
                _v6_conf if _v6_conf else votes,
                f"sizing: {_size_detail}",
            )
            return

        lev = int(_preview.get("leverage", _requested_leverage_for_engine(self.name, atr_val, entry_px)) or _requested_leverage_for_engine(self.name, atr_val, entry_px))
        lev = min(lev, int(_s.get_val("leverage_max", 15)))
        lev = max(lev, int(_s.get_val("leverage_min", 5)))
        atr_val = float(_preview.get("atr_val", atr_val) or atr_val or 0.0)
        _intent = _preview.get("intent") or _trade_intent.get_trade_intent(
            balance=balance,
            regime=str(_regime.get_regime() if _regime else ""),
        )
        _participation_details = list(_preview.get("participation_details") or [])
        _participation_cap_usdt = (
            float(_preview.get("participation_cap_usdt", 0.0) or 0.0)
            if _participation_details else None
        )
        size = float(_preview.get("size_usdt", 0.0) or 0.0)
        base_size = float(_preview.get("base_size_usdt", size) or size)
        _size_reason = str(_preview.get("size_reason") or f"preview live ${size:.2f}")
        _candidate_floor = float(_candidate_exec_floor(coin, entry_px, balance) or 0.0)
        _short_rehab_note = ""
        _symbol_rehab_note = ""
        _risk_cap_meta: dict = {}
        _macro_ctx: dict = {}
        _macro_runner_trust = 1.0

        _wk_decision = str(_preview.get("weekly_cap_decision", _weekly_cap_decision) or "").upper()
        _wk_mult = float(_preview.get("weekly_cap_mult", _weekly_cap_mult) or 1.0)
        _wk_reason = str(_preview.get("weekly_cap_reason", _weekly_cap_reason) or "")

        # ── Symbol-direction rehab (non-blocking; size-first) ─────────────────
        try:
            state = _symbol_direction_rehab_state(
                coin,
                direction,
                candidate_quality=float(_preview.get("quality_score", 0.0) or 0.0),
                consensus_count=int(_preview.get("consensus_count", 0) or 0),
                selector_bonus=float(_preview.get("selector_bonus", 0.0) or 0.0),
            )
            if state.active and float(state.size_multiplier or 1.0) < 0.999:
                before = float(size or 0.0)
                size = float(size or 0.0) * float(state.size_multiplier or 1.0)
                _min_exec = _candidate_floor
                if _min_exec > 0:
                    size = max(size, _min_exec)
                if abs(size - before) > 0.005:
                    _symbol_rehab_note = f"symbol_rehab stage={str(state.stage or '')} losses={int(state.loss_count or 0)} x{float(state.size_multiplier or 1.0):.2f}"
                    _size_reason = f"{_size_reason} | {_symbol_rehab_note}"
        except Exception:
            _symbol_rehab_note = ""

        # ── AMATERASU macro sizing hint (Daily/Weekly; non-blocking) ──────────
        if str(self.name or "").upper() == "AMATERASU" and bool(_s.get_val("amaterasu_macro_layer_enabled", True)):
            try:
                from strategy_categories import amaterasu_macro_context as _am_macro_context

                closes_1d = list(get_1d_closes(coin) or [])
                _macro_ctx = _am_macro_context(direction.upper(), closes_1d=closes_1d, coin=coin)
                _macro_runner_trust = float(_macro_ctx.get("runner_trust_mult", 1.0) or 1.0)
                _macro_mult = float(_macro_ctx.get("size_multiplier", 1.0) or 1.0)
                if _macro_mult > 0 and abs(_macro_mult - 1.0) > 1e-6:
                    before = float(size or 0.0)
                    size = float(size or 0.0) * _macro_mult
                    _min_exec = _candidate_floor
                    if _min_exec > 0:
                        size = max(size, _min_exec)
                    if abs(size - before) > 0.005:
                        _size_reason = f"{_size_reason} | amaterasu_macro x{_macro_mult:.2f}"
            except Exception:
                _macro_ctx = {}
                _macro_runner_trust = 1.0

        # ── Short-side rehab (non-blocking; size-only) ────────────────────────
        if str(direction or "").lower() == "short" and bool(_s.get_val("short_rehab_enabled", True)):
            try:
                weak_regime = str(_regime.get_regime() if _regime else "").upper()
                weak = weak_regime in {"RANGING", "CHOPPY", "SIDEWAYS"}
                adx_1h = 0.0
                try:
                    closes_1h = list(get_1h_closes(coin) or [])
                    highs_1h = list(get_1h_highs(coin) or [])
                    lows_1h = list(get_1h_lows(coin) or [])
                    if closes_1h and highs_1h and lows_1h and len(closes_1h) >= 15 and len(highs_1h) >= 15 and len(lows_1h) >= 15:
                        adx_1h = float(calc_adx(highs_1h, lows_1h, closes_1h, 14) or 0.0)
                except Exception:
                    adx_1h = 0.0
                adx_floor = float(_s.get_val("short_rehab_min_adx_1h", 18.0) or 18.0)
                if weak or (adx_1h > 0 and adx_1h < adx_floor):
                    before = float(size or 0.0)
                    mult = float(_s.get_val("short_rehab_weak_regime_size_mult", 0.90) or 0.90)
                    if mult > 0 and mult < 0.999:
                        size = float(size or 0.0) * mult
                        _min_exec = _candidate_floor
                        if _min_exec > 0:
                            size = max(size, _min_exec)
                        if abs(size - before) > 0.005:
                            _short_rehab_note = f"short_rehab regime={weak_regime or 'unknown'} adx1h={adx_1h:.1f}/{adx_floor:.1f} x{mult:.2f}"
                            _size_reason = f"{_size_reason} | {_short_rehab_note}"
            except Exception:
                _short_rehab_note = ""

        if (
            str(_s.get_val("trading_mode_preset", "") or "").strip().lower() == "full_deployment"
            or str(getattr(_v6_profile, "deployment_effective_mode", getattr(_v6_profile, "deployment_mode", "")) or "").lower() in {"max deploy", "single shot"}
        ):
            logger.info(
                "FULL DEPLOYMENT [%s] %s %s utilization=%.1f%% size=$%.2f deployable=$%.2f max_pos=%d",
                engine_log_name(self.name),
                coin,
                direction.upper(),
                float(getattr(_v6_profile, "deployment_target_utilization_pct", 0.0) or 0.0),
                size,
                float(getattr(_v6_profile, "deployment_total_deployable_usdt", 0.0) or 0.0),
                int(getattr(_v6_profile, "deployment_max_positions", 0) or 0),
            )

        if not _lane_override:
            from portfolio_allocator import check_lane_budget as _check_lane_budget
            _lane_ok, _lane_reason = _check_lane_budget(
                self.name,
                size,
                balance,
                capital_profile=_v6_profile,
                soft_accept=bool(_soft_accept_reason),
                session=_soft_accept_session,
                debug_override_level=(
                    _dbg_level(_dbg_levels, "lane_budget")
                    if (_dbg_live and _dbg_levels)
                    else "normal"
                ),
            )
            if not _lane_ok:
                if _soft_accept_reason:
                    _lane_reason = (
                        f"{_lane_reason} | soft_accept[{_soft_accept_mode}/{_soft_accept_session}] "
                        f"size ${size:.2f} via {_soft_accept_reason}"
                    )
                _rej(
                    self.name,
                    coin,
                    direction,
                    _v6_conf if _v6_conf else votes,
                    "lane_budget",
                    _lane_reason
                    + _probe_suffix(
                        self.name,
                        coin,
                        direction,
                        balance=balance,
                        capital_profile=_v6_profile,
                        candidate_quality=float(_candidate_quality or 0.0),
                        confidence=float(_v6_conf.confidence_score if _v6_conf is not None and hasattr(_v6_conf, "confidence_score") else votes),
                        intent=_intent,
                        candidate_size_usdt=float(size or 0.0),
                    ),
                )
                _release(
                    self.name,
                    coin,
                    direction,
                    _v6_conf if _v6_conf else votes,
                    f"lane_budget: {_lane_reason}",
                )
                return

        stop_atr_val = _stop_geometry_atr(coin, atr_val)
        stop, take, tp2_price = _sl_tp.compute(
            engine=self.name,
            side=side,
            entry=entry_px,
            closes=closes,
            highs=highs,
            lows=lows,
            atr_val=stop_atr_val,
            coin=coin,
            leverage=lev,
            volumes=volumes or [],
        )

        # TP distance multiplier (non-blocking; profit_target no longer blocks entries)
        _tp_adjust = _profit_targets.apply_profit_targeting(
            entry_px,
            side,
            take,
            tp2_price,
            size,
            tp_distance_multiplier=_intent.tp_multiplier,
        )
        take = float(_tp_adjust.get("tp1", take) or take)
        tp2_price = float(_tp_adjust.get("tp2", tp2_price) or tp2_price)
        _tp_reason = str(_tp_adjust.get("profit_reason") or "base targets")

        if _s.get_val("liq_heatmap_sl_enabled", True) and stop > 0:
            try:
                from liq_heatmap import check_sl_in_liq_zone
                _liq_risky, _liq_msg = check_sl_in_liq_zone(
                    stop, entry_px, side, buffer_pct=0.3
                )
                if _liq_risky:
                    _widen = entry_px * 0.003
                    stop = (round(stop - _widen, 8) if side == "buy"
                            else round(stop + _widen, 8))
                    logger.debug(
                        "[%s] SL widened 0.3%% (liq cluster): %.8f | %s",
                        self.name, stop, _liq_msg,
                    )
            except Exception:
                pass

        _resolved_cfg_cap_pct = float(_s.get_val("position_size_cap_pct_balance", 100.0))
        _resolved_cfg_cap_usdt = float(_s.get_val("position_size_cap_usdt", 250.0))
        _resolved_exec_floor = float(_candidate_exec_floor(coin, entry_px, balance) or 0.0)
        _resolved_balance_cap = float(balance or 0.0) * (_resolved_cfg_cap_pct / 100.0)
        _resolved_max_source = (
            "position_size_cap_pct_balance"
            if _resolved_balance_cap <= _resolved_cfg_cap_usdt
            else "position_size_cap_usdt"
        )
        _resolved_max_size = max(
            min(float(balance or 0.0) * (_resolved_cfg_cap_pct / 100.0), _resolved_cfg_cap_usdt),
            _resolved_exec_floor,
        )
        if abs(_resolved_max_size - _resolved_exec_floor) <= 0.01:
            _resolved_max_source = "exec_floor"
        if (
            _participation_cap_usdt is not None
            and _participation_cap_usdt > 0
            and _participation_cap_usdt >= _resolved_exec_floor
        ):
            _resolved_max_size = min(_resolved_max_size, _participation_cap_usdt)
            _resolved_max_source = "participation_cap_usdt"
        _risk_size_meta = {}
        _risk_size = _risk_sized_notional_from_stop(
            engine_name=self.name,
            coin=coin,
            side=side,
            entry_px=float(entry_px or 0.0),
            stop_px=float(stop or 0.0),
            balance=float(balance or 0.0),
            current_size_usdt=float(size or 0.0),
            base_size_usdt=float(base_size or size or 0.0),
            min_exec_usdt=_resolved_exec_floor,
            max_size_usdt=_resolved_max_size,
            max_size_source=_resolved_max_source,
        )
        _risk_size_meta = dict(_risk_size.get("meta", {}) or {})
        if _risk_size.get("skip"):
            _size_detail = str(_risk_size.get("reason") or "risk_sizing_below_min")
            _paper_validation.record_skip(
                **_paper_validation_payload(
                    engine_name=self.name,
                    coin=coin,
                    side=side,
                    entry_px=float(entry_px or 0.0),
                    stop_px=float(stop or 0.0),
                    final_size_usdt=float(_risk_size.get("size_usdt", 0.0) or 0.0),
                    risk_meta=_risk_size_meta,
                ),
                skip_reason=_size_detail,
            )
            _rej(self.name, coin, direction, _v6_conf if _v6_conf else votes, "sizing", _size_detail)
            _release(
                self.name,
                coin,
                direction,
                _v6_conf if _v6_conf else votes,
                f"sizing: {_size_detail}",
            )
            return
        before_risk_size = float(size or 0.0)
        size = float(_risk_size.get("size_usdt", size) or size)
        if abs(size - before_risk_size) > 0.009:
            _size_reason = f"{_size_reason} | {_risk_size.get('reason', 'risk_sized')}"

        if stop <= 0 or take <= 0 or stop == entry_px or take == entry_px:
            logger.warning(
                "[%s] _place_trade %s: TP/SL degenerate at price %.8f "
                "(tp=%.8f sl=%.8f) - coin price too small. Skipping with 1h cooldown.",
                self.name, coin, entry_px, take, stop,
            )
            _release(
                self.name,
                coin,
                direction,
                _v6_conf if _v6_conf else votes,
                f"tpsl_degenerate: entry={entry_px:.8f}, tp={take:.8f}, sl={stop:.8f}",
            )
            self._cooldowns[coin] = time.time() + 3600
            return

        # ── Dynamic realized risk ceiling (prevents oversized SL tails) ───────
        try:
            from risk_ceiling import apply_realized_risk_ceiling as _apply_risk_ceiling

            _min_exec = float(_resolved_exec_floor or 0.0)
            _rc = _apply_risk_ceiling(
                entry_px=float(entry_px or 0.0),
                stop_px=float(stop or 0.0),
                size_usdt=float(size or 0.0),
                balance_usdt=float(balance or 0.0),
                min_exec_usdt=_min_exec,
                side=str(side or ""),
                settings=_s,
            )
            size = float(_rc.size_usdt or size)
            stop = float(_rc.stop_loss or stop)
            _risk_cap_meta = dict(_rc.meta or {})
            if _risk_size_meta:
                _risk_cap_meta["risk_sizing"] = _risk_size_meta
            if bool(_s.get_val("realized_risk_ceiling_telemetry_enabled", True)) and str(_risk_cap_meta.get("risk_ceiling_action", "none")) != "none":
                logger.info(
                    "RISK CEILING [%s] %s %s cap=$%.2f expected=$%.3f sl_dist=%.2f%% size=%.2f->%.2f sl=%.8f->%.8f",
                    self.name,
                    coin,
                    str(direction or "").upper(),
                    float(_risk_cap_meta.get("risk_cap_usdt", 0.0) or 0.0),
                    float(_risk_cap_meta.get("expected_risk_usdt", 0.0) or 0.0),
                    float(_risk_cap_meta.get("sl_dist_pct", 0.0) or 0.0),
                    float(_risk_cap_meta.get("size_before", 0.0) or 0.0),
                    float(_risk_cap_meta.get("size_after", 0.0) or 0.0),
                    float(_risk_cap_meta.get("sl_before", 0.0) or 0.0),
                    float(_risk_cap_meta.get("sl_after", 0.0) or 0.0),
                )
        except Exception:
            _risk_cap_meta = {}

        _alloc_meta_input = dict(_risk_cap_meta or {})
        if _risk_size_meta and "risk_sizing" not in _alloc_meta_input:
            _alloc_meta_input["risk_sizing"] = _risk_size_meta
        _alloc_result = _adaptive_capital_allocation(
            coin=coin,
            balance=float(balance or 0.0),
            size_usdt=float(size or 0.0),
            entry_px=float(entry_px or 0.0),
            stop_px=float(stop or 0.0),
            leverage=int(lev or 1),
            risk_meta=_alloc_meta_input,
            participation_cap_usdt=_participation_cap_usdt,
            okx=self.okx,
            side=str(side or ""),
        )
        _alloc = await _alloc_result if inspect.isawaitable(_alloc_result) else _alloc_result
        _alloc_meta = dict(_alloc.get("meta", {}) or {})
        if _alloc.get("skip"):
            _size_detail = str(_alloc.get("reason") or "allocator_rejected")
            _paper_validation.record_skip(
                **_paper_validation_payload(
                    engine_name=self.name,
                    coin=coin,
                    side=side,
                    entry_px=float(entry_px or 0.0),
                    stop_px=float(stop or 0.0),
                    final_size_usdt=float(_alloc.get("size_usdt", 0.0) or 0.0),
                    risk_meta={**_risk_size_meta, **_alloc_meta},
                ),
                skip_reason=_size_detail,
            )
            _rej(self.name, coin, direction, _v6_conf if _v6_conf else votes, "capital", _size_detail)
            _release(
                self.name,
                coin,
                direction,
                _v6_conf if _v6_conf else votes,
                f"capital: {_size_detail}",
            )
            return
        before_alloc_size = float(size or 0.0)
        size = float(_alloc.get("size_usdt", size) or size)
        if abs(size - before_alloc_size) > 0.009:
            _size_reason = f"{_size_reason} | {_alloc.get('reason', 'allocator_scaled')}"
        if _alloc_meta:
            _risk_cap_meta["capital_allocator"] = _alloc_meta
        configured_leverage_for_economics = int(
            _alloc_meta.get("exchange_configured_leverage", 0)
            or _alloc_meta.get("configured_leverage", 0)
            or lev
            or 1
        )

        try:
            _desired_economic_size = float(
                _risk_size_meta.get("raw_size_usdt", before_alloc_size) or before_alloc_size
            )
            _final_economics = _final_candidate_economics(
                coin=coin,
                engine_name=self.name,
                direction=direction,
                entry_px=float(entry_px or 0.0),
                stop_px=float(stop or 0.0),
                tp2_px=float(tp2_price or take or 0.0),
                desired_size_usdt=_desired_economic_size,
                final_size_usdt=float(size or 0.0),
                balance_usdt=float(balance or 0.0),
                leverage=configured_leverage_for_economics,
                ticker=ticker,
            )
        except Exception as exc:
            logger.error("FINAL ECONOMICS [%s] %s failed closed: %s", self.name, coin, exc)
            _release(self.name, coin, direction, _v6_conf if _v6_conf else votes, f"final_economics_exception:{exc}")
            return
        _risk_cap_meta["final_candidate_economics"] = _final_economics
        if not bool(_final_economics.get("allowed")):
            _economic_reason = str(_final_economics.get("reason") or "final_size_not_economic")
            _rej(
                self.name,
                coin,
                direction,
                _v6_conf if _v6_conf else votes,
                "final_economics",
                _economic_reason,
                balance=balance,
                candidate_quality=float(_candidate_quality or 0.0),
                size_usdt=float(size or 0.0),
                extra={"final_candidate_economics": _final_economics},
            )
            _release(self.name, coin, direction, _v6_conf if _v6_conf else votes, f"final_economics:{_economic_reason}")
            return
        logger.info(
            "FINAL ECONOMICS [%s] %s %s desired=$%.2f final=$%.2f net=$%.4f costs=$%.4f exit=%s fee_source=%s",
            self.name,
            coin,
            direction.upper(),
            _desired_economic_size,
            float(size or 0.0),
            float((_final_economics.get("final") or {}).get("expected_net_usdt", 0.0) or 0.0),
            float((_final_economics.get("final") or {}).get("estimated_cost_usdt", 0.0) or 0.0),
            str(_final_economics.get("exit_management_mode") or ""),
            str((_final_economics.get("fee_schedule") or {}).get("source") or ""),
        )

        _exec_plan = None
        if _s.get_val("v6_enabled", False):
            try:
                from execution_router import build_execution_plan as _v6_exec
                import telemetry_v6 as _t6
                _spread_pct = 0.0
                try:
                    _bid = float(ticker.get("bidPx", 0) or 0)
                    _ask = float(ticker.get("askPx", 0) or 0)
                    if _bid > 0 and _ask > 0 and entry_px > 0:
                        _spread_pct = (_ask - _bid) / entry_px * 100
                except Exception:
                    pass
                _exec_plan = _v6_exec(
                    coin, side, size, entry_px, atr_val or 0.0,
                    spread_pct=_spread_pct,
                    capital_profile=_v6_profile,
                )
                _t6.log_execution(self.name, coin, _exec_plan)
                _t6.log_v6_summary(
                    engine=self.name, coin=coin, direction=side,
                    conf=_v6_conf,
                    sz=None,
                    gate=_v6_gate,
                    dd=_v6_dd,
                    plan=_exec_plan,
                    final_action="trade",
                    capital_mode=_v6_profile.mode if _v6_profile else "",
                )
            except Exception:
                pass

        # Resolve one profit-taking owner.  HYBRID uses local staged
        # management when enabled; only EXCHANGE_FULL receives an exchange TP.
        tp_mode = str(_s.get_val("tp_mode", "reference_only") or "reference_only").strip().lower()
        if tp_mode not in {"off", "reference_only", "exchange_tp", "hybrid"}:
            tp_mode = "reference_only"
        from tp_ownership import derive_tp_owner, owner_flags
        tp_owner = derive_tp_owner(
            tp_mode,
            partial_enabled=bool(_s.get_val("partial_exit_enabled", True)),
        )
        local_tp1_enabled, local_tp2_enabled = owner_flags(
            tp_owner,
            partial_enabled=bool(_s.get_val("partial_exit_enabled", True)),
        )
        tp_ref = float(tp2_price or 0.0) if tp_mode != "off" else 0.0
        tp_exchange = float(tp2_price or 0.0) if tp_owner == "EXCHANGE_FULL" else 0.0

        logger.info(
            "ORDER_PAYLOAD_READY [%s] %s %s size=$%.2f lev=%dx tp_mode=%s exchange_tp=%.8f",
            self.name,
            coin,
            direction.upper(),
            size,
            lev,
            tp_mode,
            tp_exchange,
        )
        result = await _execute_central_entry(
            okx=self.okx,
            engine_name=self.name,
            coin=coin,
            side=side,
            size_usdt=size,
            entry_px=entry_px,
            stop_px=stop,
            take_profit=tp_exchange,
            tp1_price=take,
            tp2_price=tp2_price,
            atr_value=float(atr_val or 0.0),
            leverage=lev,
            allocator_meta=_alloc_meta,
            execution_plan=_exec_plan,
            confidence=float(_v6_conf.confidence_score if _v6_conf is not None and hasattr(_v6_conf, "confidence_score") else votes),
            score=float(votes or 0.0),
            tp_owner=tp_owner,
            tp_mode_at_entry=tp_mode,
        )
        if not result:
            _release(
                self.name,
                coin,
                direction,
                _v6_conf if _v6_conf else votes,
                self.okx.consume_last_order_skip_reason() or "place_market_order returned no result",
            )
            return

        if bool(result.get("mock")) or bool(result.get("paper")):
            logger.info(
                "ORDER_MOCK_SUCCESS [%s] %s %s order=%s size=$%.2f",
                self.name,
                coin,
                direction.upper(),
                result.get("ordId", "--"),
                float(result.get("size_usdt", size) or size),
            )

        if _s.get_val("v6_enabled", False) and _s.get_val("v6_trade_frequency_enabled", True):
            try:
                from capital_manager import record_trade_entry as _v6_record_entry
                _v6_record_entry()
            except Exception:
                pass

        actual_coin = result.get("actual_coin", coin).upper()
        effective_lev = int(
            result.get("position_effective_leverage")
            or 1
        )
        leverage_truth_metadata = {
            "requested_leverage": int(result.get("requested_leverage", lev) or lev),
            "exchange_configured_leverage": result.get("exchange_configured_leverage"),
            "position_effective_leverage": result.get("position_effective_leverage"),
            "leverage_truth_status": str(result.get("leverage_truth_status", "") or ""),
            "leverage_truth_timestamp": str(result.get("leverage_truth_timestamp", "") or ""),
            "leverage_truth_source": str(result.get("leverage_truth_source", "") or ""),
            "leverage_truth_mismatch": bool(result.get("leverage_truth_mismatch", False)),
        }
        actual_size = float(result.get("size_usdt", size) or size)
        if actual_coin != coin:
            logger.info("[%s] NANO rerouted %s -> %s", self.name, coin, actual_coin)
            _release(
                self.name,
                coin,
                direction,
                _v6_conf if _v6_conf else votes,
                f"nano_reroute: rerouted_to={actual_coin}",
            )
            reroute_metadata = None
            if _lane_override:
                reroute_metadata = {
                    "lane_override_active": True,
                    "lane_override_reason": "lane_budget",
                }
            ok, why = await can_open_and_reserve(
                actual_coin,
                "long" if side == "buy" else "short",
                self.name,
                actual_size,
                entry=float(result.get("fillPx", 0.0) or entry_px or 0.0),
                stop_loss=float(stop or 0.0),
                metadata=reroute_metadata,
                balance_usdt=float(balance or 0.0),
            )
            if not ok:
                logger.error("[%s] NANO %s blocked (%s) - closing orphan position on exchange.", self.name, actual_coin, why)
                try:
                    pos_side = "long" if side == "buy" else "short"
                    from execution_router import router_for
                    await router_for(self.okx).close(actual_coin, pos_side, actual_size, emergency=True)
                except Exception as close_err:
                    logger.error("[%s] Failed to close orphan NANO %s: %s", self.name, actual_coin, close_err)
                return

            stop = result.get("stop_loss", stop)
            take = result.get("take_profit", take)
            _a_highs = get_highs(actual_coin)
            _a_lows = get_lows(actual_coin)
            _a_closes = get_closes(actual_coin)
            if _a_highs and _a_lows and _a_closes and len(_a_closes) >= 14:
                _nano_atr = calc_atr(_a_highs, _a_lows, _a_closes, 14)
            else:
                _nano_atr = atr_val
            if _nano_atr and _nano_atr > 0:
                _tp2_dist = _nano_atr * float(_s.get_val("atr_tp2_multiplier", 10.0))
            else:
                _tp2_dist = abs(take - stop) * 2
            fill_for_tp2 = float(result.get("fillPx", 0)) or entry_px
            tp2_price = (round(fill_for_tp2 + _tp2_dist, 8) if side == "buy"
                         else round(fill_for_tp2 - _tp2_dist, 8))
            self._cooldowns[actual_coin] = time.time()
            logger.info(
                "[%s] NANO %s recalculated SL=%.8f TP=%.8f TP2=%.8f",
                self.name, actual_coin, stop, take, tp2_price,
            )

        fill = float(result.get("fillPx", 0)) or entry_px
        oid = result.get("ordId", "--")
        cloid = str(result.get("clOrdId", result.get("client_order_id", "")) or "")
        _paper_validation_ctx = _paper_validation_payload(
            engine_name=self.name,
            coin=actual_coin,
            side=side,
            entry_px=float(fill or 0.0),
            stop_px=float(stop or 0.0),
            final_size_usdt=float(actual_size or 0.0),
            risk_meta=_risk_size_meta,
            order_id=oid,
        )
        _paper_validation.record_entry(**_paper_validation_ctx)
        _conf_val = float(
            _v6_conf.confidence_score
            if _v6_conf is not None and hasattr(_v6_conf, "confidence_score")
            else votes
        )
        _quality_snap = _candidate_quality_snapshot(
            self.name,
            actual_coin,
            direction.upper(),
            balance,
            float(votes),
            float(_conf_val),
        ) or {}
        _quality_score = float(_quality_snap.get("quality_score", 0.0) or 0.0)
        _opened_at = datetime.now(timezone.utc).isoformat()
        _role_ctx = _position_strength_context(
            self.name,
            actual_coin,
            direction.upper(),
            _quality_snap,
            float(_conf_val),
        )
        _role_adj = _role_quality_adjustment(_role_ctx)
        _quality_score = max(round(_quality_score + _role_adj, 3), 0.0)
        _quality_snap["quality_score"] = _quality_score
        _quality_snap["role_quality_adjustment"] = _role_adj
        _role_ctx = _position_strength_context(
            self.name,
            actual_coin,
            direction.upper(),
            _quality_snap,
            float(_conf_val),
        )
        if actual_coin in _live_registry_view():
            _reg_update(
                actual_coin,
                size_usdt=actual_size,
                entry=fill,
                leverage=effective_lev,
                stop_loss=stop,
                take_profit=take,
                order_id=oid,
                    metadata={
                        "atr": float(atr_val or 0.0),
                        "vote_score": float(votes),
                        "confidence_score": float(_conf_val),
                        "quality_score": float(_quality_score),
                        "quality_score_base": float(_quality_snap.get("quality_score_base", 0.0) or 0.0),
                        "quality_score_winrate_adj": float(_quality_snap.get("quality_score_winrate_adj", 0.0) or 0.0),
                        "quality_regime_bonus": float(_quality_snap.get("quality_regime_bonus", 0.0) or 0.0),
                        "quality_confluence_bonus": float(_quality_snap.get("quality_confluence_bonus", 0.0) or 0.0),
                        "quality_strictness_penalty": float(_quality_snap.get("quality_strictness_penalty", 0.0) or 0.0),
                        "quality_advisory_penalty": float(_quality_snap.get("advisory_penalty", 0.0) or 0.0),
                        "role_quality_adjustment": float(_role_adj),
                        "winrate_profile": str(_quality_snap.get("winrate_profile", "") or ""),
                        "winrate_enabled": bool(_quality_snap.get("winrate_enabled", False)),
                        "regime_at_entry": str(_quality_snap.get("regime", "") or ""),
                        "lane": str(_quality_snap.get("lane", "") or ""),
                        "macro_daily_alignment": str((_macro_ctx or {}).get("daily_alignment", _quality_snap.get("macro_daily_alignment", "")) or ""),
                        "macro_weekly_alignment": str((_macro_ctx or {}).get("weekly_alignment", _quality_snap.get("macro_weekly_alignment", "")) or ""),
                        "macro_agreement_score": float((_macro_ctx or {}).get("macro_agreement_score", _quality_snap.get("macro_agreement_score", 0.0)) or 0.0),
                        "macro_conflict": bool((_macro_ctx or {}).get("macro_conflict", _quality_snap.get("macro_conflict", False))),
                        "macro_conf_modifier": float((_macro_ctx or {}).get("conf_modifier", _quality_snap.get("macro_conf_modifier", 0.0)) or 0.0),
                        "macro_size_multiplier": float((_macro_ctx or {}).get("size_multiplier", _quality_snap.get("macro_size_multiplier", 1.0)) or 1.0),
                        "macro_runner_trust_mult": float((_macro_ctx or {}).get("runner_trust_mult", _quality_snap.get("macro_runner_trust_mult", 1.0)) or 1.0),
                        "short_rehab_note": str(_short_rehab_note or ""),
                        "symbol_rehab_note": str(_symbol_rehab_note or ""),
                        "expected_risk_usdt": float((_risk_cap_meta or {}).get("expected_risk_usdt", 0.0) or 0.0),
                         "risk_cap_usdt": float((_risk_cap_meta or {}).get("risk_cap_usdt", 0.0) or 0.0),
                         "risk_ceiling_action": str((_risk_cap_meta or {}).get("risk_ceiling_action", "") or ""),
                         **leverage_truth_metadata,
                         "entry_fee_usdt": (
                             None if result.get("fee") in {None, ""}
                             else float(result.get("fee") or 0.0)
                         ),
                         "tp2_price": float(tp2_price),
                        "exit_management_mode": str(_final_economics.get("exit_management_mode") or "partial"),
                        "tp_mode": str(tp_mode),
                        "tp_mode_at_entry": str(tp_mode),
                        "tp_owner": str(tp_owner),
                        "local_tp1_enabled": bool(local_tp1_enabled),
                        "local_tp2_enabled": bool(local_tp2_enabled),
                        "ownership_timestamp": datetime.now(timezone.utc).isoformat(),
                        "tp_owner_timestamp": datetime.now(timezone.utc).isoformat(),
                        "partials_enabled_at_entry": bool(_s.get_val("partial_exit_enabled", True)),
                        "tp_ref_price": float(tp_ref or 0.0),
                        "requested_coin": coin,
                        "entry_client_order_id": cloid,
                        "entry_clOrdId": cloid,
                        "recovery_mode": "managed",
                        "policy_tag": "2026-04-02",
                        "managed_by_engine": True,
                        "opened_at": _opened_at,
                        "opened_ts": time.time(),
                        "exchange_confirmed": os.getenv("PAPER_TRADING", "true").strip().lower() != "true",
                        "exchange_confirmed_at": (
                            time.time() if os.getenv("PAPER_TRADING", "true").strip().lower() != "true" else None
                        ),
                        "paper_confirmed": os.getenv("PAPER_TRADING", "true").strip().lower() == "true",
                        "weekly_cap_decision": str(_wk_decision or ""),
                        "weekly_cap_mult": float(_wk_mult or 1.0),
                        "weekly_cap_reason": str(_wk_reason or ""),
                        "admission_confidence": float(_admission_conf or _signal_conf or 0.0),
                        "confidence_stage_trace": dict(_conf_trace or {}),
                        **(_risk_cap_meta or {}),
                        **_role_registry_metadata(_role_ctx, self.name),
                        **(_dbg_registry_meta()),
                        **({"lane_override_active": True} if _lane_override else {}),
                    },
                )
            logger.info(
                "REGISTRY_ACTIVATED [%s] %s %s order=%s size=$%.2f lev=%dx",
                self.name,
                actual_coin,
                direction.upper(),
                oid,
                actual_size,
                effective_lev,
            )

        _em.register(
            actual_coin,
            direction,
            fill,
            actual_size,
            atr_val or 0.0,
            stop,
            take,
            tp2_price,
            oid,
            client_order_id=cloid,
            engine_name=self.name,
            vote_score=votes,
            leverage=effective_lev,
            requested_leverage=leverage_truth_metadata["requested_leverage"],
            exchange_configured_leverage=leverage_truth_metadata["exchange_configured_leverage"],
            position_effective_leverage=leverage_truth_metadata["position_effective_leverage"],
            leverage_truth_status=leverage_truth_metadata["leverage_truth_status"],
            leverage_truth_timestamp=leverage_truth_metadata["leverage_truth_timestamp"],
            requested_vs_configured_mismatch=bool(leverage_truth_metadata.get("requested_vs_configured_mismatch", False)),
            configured_vs_effective_mismatch=bool(leverage_truth_metadata.get("configured_vs_effective_mismatch", False)),
            macro_runner_trust_mult=float(_macro_runner_trust or 1.0),
            expected_risk_usdt=float((_risk_cap_meta or {}).get("expected_risk_usdt", 0.0) or 0.0),
            risk_cap_usdt=float((_risk_cap_meta or {}).get("risk_cap_usdt", 0.0) or 0.0),
            risk_ceiling_action=str((_risk_cap_meta or {}).get("risk_ceiling_action", "") or ""),
            paper_validation=dict(_paper_validation_ctx),
            partial_mode_override=str(_final_economics.get("exit_management_mode") or "partial") == "partial",
            entry_fee_usdt=(
                None if result.get("fee") in {None, ""}
                else float(result.get("fee") or 0.0)
            ),
            tp_owner=tp_owner,
            tp_mode_at_entry=tp_mode,
            local_tp1_enabled=local_tp1_enabled,
            local_tp2_enabled=local_tp2_enabled,
        )
        logger.info(
            "EXIT_PLAN_CREATED [%s] %s %s order=%s SL=%.8f TP1=%.8f TP2=%.8f",
            self.name,
            actual_coin,
            direction.upper(),
            oid,
            stop,
            take,
            tp2_price,
        )

        self._open[actual_coin] = {
            "side": side, "entry": fill, "stop_loss": stop, "take_profit": take,
            "tp2": tp2_price, "size": actual_size, "leverage": effective_lev, "atr": atr_val or 0.0,
            "vote_score": votes, "order_id": oid, "engine": self.name,
            "entry_client_order_id": cloid,
            "requested_coin": coin,
            "confidence_score": float(_conf_val),
            "quality_score": float(_quality_score),
            "role_quality_adjustment": float(_role_adj),
            "opened_at": _opened_at,
            "position_strength": float(_role_ctx.get("position_strength", 0.0) or 0.0),
            "trade_class": str(_role_ctx.get("trade_class", "") or ""),
            "preferred_exit_style": str(_role_ctx.get("preferred_exit_style", "") or ""),
            "contributor_roles": list(_role_ctx.get("contributor_roles", []) or []),
            **_engine_attribution.build_attribution(
                primary_engine=self.name,
                confirming_engines=list(_role_ctx.get("confirming_engines", []) or []),
                role_contributions=dict(_role_ctx.get("role_contributions", {}) or {}),
                size_multiplier=float(_role_ctx.get("size_multiplier", 1.0) or 1.0),
            ),
            "paper_validation": dict(_paper_validation_ctx),
        }
        _live_consensus = _consensus.get_snapshot(actual_coin, direction.upper())
        _entry_reason = "standard entry"
        if _soft_accept_reason:
            _entry_reason = (
                f"soft_accept[{_soft_accept_mode}/{_soft_accept_session}] via {_soft_accept_reason}"
            )
        elif _lane_override:
            _entry_reason = "lane override"
        elif _live_consensus.agreement_count > 1:
            _entry_reason = f"multi-engine agreement: {_live_consensus.summary}"
        _dbg_record_accept(
            self.name,
            actual_coin,
            direction.upper(),
            _conf_val,
            size_usdt=actual_size,
            candidate_quality=float(_quality_score or 0.0),
            regime=str(_quality_snap.get("regime", "") or ""),
            size_decision=_size_reason,
            reason=_entry_reason,
        )
        _trade_explanation = _format_trade_explanation(
            intent=_intent,
            consensus=_live_consensus,
            confidence=_conf_val,
            quality_score=_quality_score,
            base_size=base_size,
            final_size=actual_size,
            leverage=effective_lev,
            size_reason=_size_reason,
            margin_used=(actual_size / effective_lev) if effective_lev > 0 else actual_size,
            tp_reason=_tp_reason,
            entry_reason=_entry_reason,
        )
        _accept_suffix = ""
        if _soft_accept_reason:
            _accept_suffix = f" soft={_soft_accept_mode}/{_soft_accept_session}"
        if str(_wk_decision or "").upper() == "ACCEPT_REDUCED" and float(_wk_mult or 1.0) < 0.999:
            _accept_suffix = f"{_accept_suffix} wk=x{float(_wk_mult or 1.0):.2f}".rstrip()
        logger.info(
            "ACCEPT [%s] %s %s conf=%.2f quality=%.1f | size=$%.2f lev=%dx%s",
            self.name,
            actual_coin,
            direction.upper(),
            _conf_val,
            _quality_score,
            actual_size,
            effective_lev,
            _accept_suffix,
        )
        star = "***" if votes >= 3 else "**+"
        routed = f" [NANO->{actual_coin}]" if actual_coin != coin else ""
        logger.info("[%s] OPEN %s %s%s %s lev=%dx $%.2f SL=%.6f TP=%.6f",
                    self.name, side.upper(), actual_coin, routed, star, effective_lev, actual_size, stop, take)
        await alert_trade_executed(
            side.upper(),
            actual_coin,
            actual_size,
            fill,
            stop,
            tp_ref,
            oid,
            effective_lev,
            extra=f"{self.emoji} {self.name}",
            tp_mode=tp_mode,
            explanation="",
        )


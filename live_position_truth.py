"""Strict OKX-backed open-position truth for Telegram reporting.

This module is reporting-only.  It never mutates the registry, closes positions,
or influences admission/exit logic.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import random
import time
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from tp_ownership import normalize_persisted_owner, normalize_tp_mode

logger = logging.getLogger("OxyPulse.PnLTruth")

DRIFT_LOG_PATH = Path("data/pnl_drift_log.jsonl")

_cache: dict[str, Any] = {"snapshot": None, "ts": 0.0, "ttl": 0.0}
_lock = asyncio.Lock()
_inflight: asyncio.Task | None = None
_position_state: dict[str, dict[str, Any]] = {}
_last_drift_log: dict[str, dict[str, Any]] = {}


def reset_runtime_state() -> None:
    """Test helper; safe in production but normally unused."""
    global _inflight
    _cache.update({"snapshot": None, "ts": 0.0, "ttl": 0.0})
    _inflight = None
    _position_state.clear()
    _last_drift_log.clear()


def _setting(settings, key: str, default: Any) -> Any:
    try:
        if settings is not None and hasattr(settings, "get_val"):
            return settings.get_val(key, default)
    except Exception:
        pass
    return default


def _configured_tp_mode(settings) -> str:
    return normalize_tp_mode(_setting(settings, "tp_mode", "reference_only"))


def _float(value: Any, default: float | None = 0.0) -> float | None:
    try:
        if value in ("", None):
            return default
        return float(value)
    except Exception:
        return default


def _int(value: Any, default: int | None = 0) -> int | None:
    try:
        if value in ("", None):
            return default
        return int(float(value))
    except Exception:
        return default


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _coin_from_inst(inst_id: str) -> str:
    inst = str(inst_id or "").upper()
    for suffix in ("-USDT-SWAP", "-USDT"):
        if inst.endswith(suffix):
            return inst[: -len(suffix)]
    return inst


def _side_from_okx(pos: dict) -> str:
    side = str(pos.get("posSide", "") or "").lower()
    if side in {"long", "short"}:
        return side
    qty = _float(pos.get("pos"), 0.0) or 0.0
    return "long" if qty >= 0 else "short"


def _key(symbol: str, side: str) -> str:
    return f"{str(symbol or '').upper()}:{str(side or '').lower()}"


def _registry_side(row: dict) -> str:
    direction = str(row.get("direction", "") or row.get("side", "") or "long").lower()
    if direction in {"buy", "long"}:
        return "long"
    if direction in {"sell", "short"}:
        return "short"
    return "long"


def _parse_time(value: Any) -> float | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00")).timestamp()
    except Exception:
        return None


def _age_from_open(row: dict, now: float) -> float | None:
    opened = _parse_time(row.get("opened_at") or row.get("reserved_at"))
    if opened is None:
        return None
    return max(now - opened, 0.0)


def _jitter_ttl(settings) -> float:
    base = max(float(_setting(settings, "live_position_refresh_sec", 3.0) or 3.0), 1.0)
    jitter = max(float(_setting(settings, "live_position_refresh_jitter_sec", 0.5) or 0.0), 0.0)
    lo = max(base - jitter, 1.0)
    hi = max(base + jitter, lo)
    return random.uniform(lo, hi)


def compute_confidence(source: str, status: str, age_sec: float | None, settings=None) -> str:
    max_stale = float(_setting(settings, "max_live_position_stale_sec", 10.0) or 10.0)
    if source == "PAPER" and status == "PAPER":
        return "HIGH"
    if source == "EXCHANGE_TRUTH" and (age_sec is None or age_sec <= max_stale) and status != "TEMPORARY_DESYNC":
        return "HIGH"
    if source == "ESTIMATE" and status not in {"STALE", "SYNCING", "TEMPORARY_DESYNC"}:
        return "MEDIUM"
    return "LOW"


def execution_truth_is_fresh(snapshot: dict | None, settings=None) -> bool:
    """Return whether exchange position/exposure truth is safe for routing."""
    if not isinstance(snapshot, dict) or not bool(snapshot.get("ok")):
        return False
    if str(snapshot.get("source", "") or "") != "EXCHANGE_TRUTH":
        return False
    max_stale = float(_setting(settings, "max_live_position_stale_sec", 10.0) or 10.0)
    age = snapshot.get("age_sec")
    if age is None or float(age) > max_stale:
        return False
    if int(snapshot.get("stale_count", 0) or 0) > 0:
        return False
    for row in (snapshot.get("positions", {}) or {}).values():
        if str(row.get("status", "") or "") in {"STALE", "SYNCING", "TEMPORARY_DESYNC"}:
            return False
        if str(row.get("source", "") or "") != "EXCHANGE_TRUTH":
            return False
    return True


def execution_position_truth_is_safe(snapshot: dict | None, settings=None) -> bool:
    """Return whether fresh exchange truth is safe for a new live order."""
    if not execution_truth_is_fresh(snapshot, settings):
        return False
    if int(snapshot.get("mismatch_count", 0) or 0) > 0:
        return False
    for row in (snapshot.get("positions", {}) or {}).values():
        # MATCH is the only reconciled live-position status. Other statuses
        # represent an unresolved exchange/local truth condition.
        if str(row.get("status", "") or "").upper() != "MATCH":
            return False
    return True


def source_label(source: str, status: str | None = None) -> str:
    if source == "PAPER":
        return "PAPER"
    if status == "TEMPORARY_DESYNC":
        return "⚠️ DESYNC"
    if status == "SYNCING":
        return "⚪ SYNC"
    if source == "EXCHANGE_TRUTH":
        return "🟢 OKX"
    if source == "ESTIMATE":
        return "🟡 EST"
    if source == "STALE" or status == "STALE":
        return "🔴 STALE"
    return "🔴 STALE"


def _local_estimate(reg: dict | None, mark_price: float | None) -> float | None:
    if not reg or not mark_price or mark_price <= 0:
        return None
    entry = _float(reg.get("entry"), None)
    size = _float(reg.get("size_usdt"), None)
    if not entry or not size or entry <= 0 or size <= 0:
        return None
    side = _registry_side(reg)
    pct = ((mark_price - entry) / entry) if side == "long" else ((entry - mark_price) / entry)
    return round(float(size) * pct, 6)


def _drift(okx_upl: float | None, local_est: float | None) -> tuple[float | None, float | None]:
    if okx_upl is None or local_est is None:
        return None, None
    diff = round(float(okx_upl) - float(local_est), 6)
    denom = max(abs(float(local_est)), 0.01)
    return diff, round(diff / denom * 100.0, 3)


def _should_log_drift(symbol: str, drift_usdt: float | None, drift_pct: float | None, settings) -> bool:
    if drift_usdt is None or drift_pct is None:
        return False
    warn_usd = abs(float(_setting(settings, "pnl_drift_warn_usd", 0.05) or 0.05))
    warn_pct = abs(float(_setting(settings, "pnl_drift_warn_pct", 10.0) or 10.0))
    if abs(drift_usdt) < warn_usd or abs(drift_pct) < warn_pct:
        return False
    prev = _last_drift_log.get(symbol.upper())
    now = time.monotonic()
    if prev and now - float(prev.get("ts", 0.0) or 0.0) < 60.0:
        if abs(float(prev.get("drift_usdt", 0.0) or 0.0) - drift_usdt) < warn_usd:
            return False
    _last_drift_log[symbol.upper()] = {"ts": now, "drift_usdt": drift_usdt}
    return True


def _log_drift(row: dict, settings) -> None:
    symbol = str(row.get("symbol", "") or "").upper()
    drift_usdt = row.get("drift_usdt")
    drift_pct = row.get("drift_pct")
    if not symbol or not _should_log_drift(symbol, drift_usdt, drift_pct, settings):
        return
    try:
        DRIFT_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "timestamp": _utc_now(),
            "symbol": symbol,
            "okx_upl": row.get("okx_upl"),
            "local_estimate": row.get("local_estimate"),
            "drift_usdt": drift_usdt,
            "drift_pct": drift_pct,
            "state": row.get("status"),
        }
        with DRIFT_LOG_PATH.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(payload, sort_keys=True) + "\n")
        logger.warning(
            "PnL drift %s okx=%s local=%s drift=%s (%s%%) state=%s",
            symbol,
            row.get("okx_upl"),
            row.get("local_estimate"),
            drift_usdt,
            drift_pct,
            row.get("status"),
        )
    except Exception as exc:
        logger.debug("PnL drift log failed: %s", exc)


def _margin_from_okx(pos: dict, notional: float | None, leverage: int | None) -> float | None:
    for key in ("margin", "imr", "marginBal"):
        val = _float(pos.get(key), None)
        if val and val > 0:
            return abs(val)
    if notional and notional > 0 and leverage and leverage > 0:
        return abs(float(notional) / max(int(leverage), 1))
    return None


def _normalize_okx_position(pos: dict, fetched_at: str, age_sec: float, registry: dict, settings) -> dict | None:
    symbol = _coin_from_inst(pos.get("instId", ""))
    if not symbol:
        return None
    qty = _float(pos.get("pos"), 0.0) or 0.0
    if qty == 0:
        return None
    side = _side_from_okx(pos)
    entry = _float(pos.get("avgPx"), None)
    mark = _float(pos.get("markPx"), None)
    upl = _float(pos.get("upl"), None)
    notional = abs(_float(pos.get("notionalUsd", pos.get("notionalUSD")), 0.0) or 0.0)
    leverage = _int(pos.get("lever"), None)
    margin = _margin_from_okx(pos, notional, leverage)
    roi = round((upl / margin * 100.0), 3) if upl is not None and margin and margin > 0 else None
    reg = registry.get(symbol.upper(), {}) or {}
    requested_leverage = _int(reg.get("requested_leverage"), None)
    configured_leverage = _int(reg.get("exchange_configured_leverage"), leverage)
    local = _local_estimate(reg, mark)
    drift_usdt, drift_pct = _drift(upl, local)
    mismatch = _mismatch_state(reg, side, entry, notional, leverage, margin)
    status = "DRIFT" if mismatch else "MATCH"
    row = {
        "symbol": symbol.upper(),
        "side": side,
        "size": abs(qty),
        "notional_usd": notional if notional > 0 else None,
        "entry_price": entry,
        "mark_price": mark,
        "leverage": leverage,
        "requested_leverage": requested_leverage,
        "exchange_configured_leverage": configured_leverage,
        "position_effective_leverage": leverage,
        "requested_vs_configured_mismatch": bool(
            reg.get("requested_vs_configured_mismatch", requested_leverage and configured_leverage and requested_leverage != configured_leverage)
        ),
        "configured_vs_effective_mismatch": bool(
            reg.get("configured_vs_effective_mismatch", configured_leverage and leverage and configured_leverage != leverage)
        ),
        "leverage_truth_status": "POSITION_EFFECTIVE" if leverage else "LEVERAGE_UNKNOWN",
        "leverage_truth_timestamp": fetched_at,
        "margin": margin,
        "unrealised_pnl": upl,
        "okx_upl": upl,
        "upl_ratio": _float(pos.get("uplRatio"), None),
        "roi_pct": roi,
        "source": "EXCHANGE_TRUTH",
        "source_label": source_label("EXCHANGE_TRUTH", status),
        "fetched_at": fetched_at,
        "age_sec": age_sec,
        "status": status,
        "mismatch_state": mismatch or "OK",
        "confidence": compute_confidence("EXCHANGE_TRUTH", status, age_sec, settings),
        "local_estimate": local,
        "drift_usdt": drift_usdt,
        "drift_pct": drift_pct,
        "registry": reg,
    }
    return _apply_desync_guard(row, settings)


def _mismatch_state(
    reg: dict | None,
    side: str,
    entry: float | None,
    notional: float | None,
    leverage: int | None,
    margin: float | None,
) -> str:
    if not reg:
        return "EXCHANGE_ONLY"
    parts: list[str] = []
    reg_side = _registry_side(reg)
    if reg_side != side:
        parts.append(f"side registry={reg_side} okx={side}")
    reg_entry = _float(reg.get("entry"), None)
    if reg_entry and entry and entry > 0:
        if abs(reg_entry - entry) / entry * 100.0 > 0.25:
            parts.append(f"entry registry={reg_entry:.8g} okx={entry:.8g}")
    reg_notional = _float(reg.get("size_usdt"), None)
    if reg_notional and notional and notional > 0:
        if abs(reg_notional - notional) / notional * 100.0 > 10.0:
            parts.append(f"notional registry={reg_notional:.2f} okx={notional:.2f}")
    reg_lev = _int(reg.get("leverage"), None)
    if reg_lev and leverage and reg_lev != leverage:
        parts.append(f"lev registry={reg_lev} okx={leverage}")
    return "; ".join(parts)


def _apply_desync_guard(row: dict, settings) -> dict:
    key = _key(row["symbol"], row["side"])
    now = time.monotonic()
    state = _position_state.setdefault(key, {})
    prev = state.get("last_good")
    upl = row.get("unrealised_pnl")
    prev_upl = (prev or {}).get("unrealised_pnl")
    notional = row.get("notional_usd") or 0.0
    prev_notional = (prev or {}).get("notional_usd") or 0.0
    jump_pct = abs(float(_setting(settings, "pnl_desync_jump_pct", 50.0) or 50.0))
    hold_sec = max(float(_setting(settings, "pnl_desync_hold_sec", 3.0) or 3.0), 0.0)
    size_changed = False
    if notional and prev_notional:
        size_changed = abs(notional - prev_notional) / max(abs(prev_notional), 0.01) * 100.0 > 2.0
    if (
        prev
        and upl is not None
        and prev_upl is not None
        and not size_changed
        and abs(prev_upl) >= 0.05
    ):
        delta_pct = abs((float(upl) - float(prev_upl)) / max(abs(float(prev_upl)), 0.01) * 100.0)
        confirm = state.get("desync_candidate")
        if delta_pct > jump_pct:
            if confirm and abs(float(confirm.get("upl", 0.0)) - float(upl)) < 0.0001:
                state.pop("desync_candidate", None)
            elif now <= float(state.get("desync_until", 0.0) or 0.0):
                held = deepcopy(prev)
                held.update({
                    "status": "TEMPORARY_DESYNC",
                    "source_label": source_label("EXCHANGE_TRUTH", "TEMPORARY_DESYNC"),
                    "confidence": "LOW",
                    "mismatch_state": "temporary OKX PnL jump; holding last confirmed value",
                })
                return held
            else:
                state["desync_candidate"] = {"upl": upl, "ts": now}
                state["desync_until"] = now + hold_sec
                held = deepcopy(prev)
                held.update({
                    "status": "TEMPORARY_DESYNC",
                    "source_label": source_label("EXCHANGE_TRUTH", "TEMPORARY_DESYNC"),
                    "confidence": "LOW",
                    "mismatch_state": "temporary OKX PnL jump; confirming next refresh",
                })
                return held
    state["last_good"] = deepcopy(row)
    state["last_source"] = "EXCHANGE_TRUTH"
    state["last_state"] = row.get("status", "MATCH")
    state["last_okx_seen_ts"] = now
    state["okx_lock_until"] = now + max(float(_setting(settings, "pnl_okx_truth_lock_sec", 4.0) or 4.0), 0.0)
    state.pop("desync_candidate", None)
    return row


async def _mark_price(okx, symbol: str) -> float | None:
    try:
        if hasattr(okx, "get_mark_price"):
            mark = _float(await okx.get_mark_price(symbol), None)
            if mark and mark > 0:
                return mark
        if hasattr(okx, "get_ticker"):
            ticker_getter = getattr(okx, "get_execution_ticker", None)
            ticker = await (ticker_getter(symbol) if ticker_getter else okx.get_ticker(symbol))
            if isinstance(ticker, dict):
                last = _float(ticker.get("last"), None)
                if last and last > 0:
                    return last
    except Exception:
        return None
    return None


def _paper_mode_enabled() -> bool:
    return os.getenv("PAPER_TRADING", "false").strip().lower() == "true"


def _truth_registry(registry: dict[str, dict] | None) -> dict[str, dict]:
    """Exclude only explicit pre-submit reservations from position truth.

    The position registry remains authoritative for duplicate/risk/capacity
    controls. Truth reconciliation receives only rows that represent an actual
    position or an ambiguous/uncertain state that must fail closed.
    """
    try:
        from position_registry import is_pre_submit_reservation, is_terminal_historical_position
    except Exception:
        is_pre_submit_reservation = lambda row: False
        is_terminal_historical_position = lambda row: False
    return {
        str(symbol).upper(): dict(row or {})
        for symbol, row in (registry or {}).items()
        if not is_pre_submit_reservation(row) and not is_terminal_historical_position(row)
    }


def _local_exposure_summary(registry: dict[str, dict] | None) -> dict:
    try:
        from position_registry import (
            exposure_status,
            EXPOSURE_ACTIVE,
            EXPOSURE_UNKNOWN,
            EXPOSURE_TERMINAL,
        )
    except Exception:
        return {
            "local_active_count": 0,
            "local_active_symbols": [],
            "local_unknown_count": 0,
            "local_unknown_symbols": [],
            "local_terminal_count": 0,
        }
    active, unknown, terminal = [], [], []
    for symbol, row in (registry or {}).items():
        status = exposure_status(row)
        if status == EXPOSURE_ACTIVE:
            active.append(str(symbol).upper())
        elif status == EXPOSURE_UNKNOWN:
            unknown.append(str(symbol).upper())
        elif status == EXPOSURE_TERMINAL:
            terminal.append(str(symbol).upper())
    return {
        "local_active_count": len(active),
        "local_active_symbols": sorted(active),
        "local_unknown_count": len(unknown),
        "local_unknown_symbols": sorted(unknown),
        "local_terminal_count": len(terminal),
    }


def _latest_price(symbol: str) -> float | None:
    try:
        from market_data_stream import latest_price

        price = _float(latest_price(symbol), None)
        return price if price and price > 0 else None
    except Exception:
        return None


def _paper_registry_row(symbol: str, reg: dict, mark: float | None, settings) -> dict:
    side = _registry_side(reg)
    entry = _float(reg.get("entry"), None)
    notional = _float(reg.get("size_usdt"), None)
    lev = _int(reg.get("leverage"), None)
    requested_leverage = _int(reg.get("requested_leverage"), lev)
    configured_leverage = _int(reg.get("exchange_configured_leverage"), lev)
    margin = (notional / lev) if notional and lev and lev > 0 else None
    local = _local_estimate(reg, mark) if mark and mark > 0 else None
    roi = round((local / margin * 100.0), 3) if local is not None and margin and margin > 0 else None
    status = "PAPER" if local is not None else "PAPER_PRICE_UNAVAILABLE"
    return {
        "symbol": symbol.upper(),
        "side": side,
        "size": None,
        "notional_usd": notional,
        "entry_price": entry,
        "mark_price": mark,
        "leverage": lev,
        "requested_leverage": requested_leverage,
        "exchange_configured_leverage": configured_leverage,
        "position_effective_leverage": lev,
        "requested_vs_configured_mismatch": bool(
            reg.get("requested_vs_configured_mismatch", requested_leverage and configured_leverage and requested_leverage != configured_leverage)
        ),
        "configured_vs_effective_mismatch": bool(
            reg.get("configured_vs_effective_mismatch", configured_leverage and lev and configured_leverage != lev)
        ),
        "leverage_truth_status": str(reg.get("leverage_truth_status", "PAPER_LOCAL") or "PAPER_LOCAL"),
        "leverage_truth_timestamp": str(reg.get("leverage_truth_timestamp", "") or ""),
        "margin": margin,
        "unrealised_pnl": local,
        "okx_upl": None,
        "upl_ratio": None,
        "roi_pct": roi,
        "source": "PAPER",
        "source_label": source_label("PAPER", status),
        "fetched_at": _utc_now(),
        "age_sec": 0.0,
        "status": status,
        "mismatch_state": "paper registry + latest_price" if local is not None else "paper latest_price unavailable",
        "confidence": "HIGH" if local is not None else "LOW",
        "local_estimate": local,
        "drift_usdt": None,
        "drift_pct": None,
        "registry": reg,
    }


async def _build_paper_snapshot(okx, registry: dict[str, dict], settings) -> dict:
    local_summary = _local_exposure_summary(registry)
    registry = _truth_registry(registry)
    positions = {}
    for symbol, reg in sorted(registry.items()):
        mark = _latest_price(symbol)
        if not mark:
            mark = await _mark_price(okx, symbol)
        positions[symbol] = _paper_registry_row(symbol, reg, mark, settings)
    open_pnl_values = [
        float(p["unrealised_pnl"])
        for p in positions.values()
        if p.get("unrealised_pnl") is not None
    ]
    return {
        "ok": True,
        "reason": "paper mode",
        "source": "PAPER",
        "source_label": source_label("PAPER"),
        "confidence": "HIGH" if positions and all(p.get("confidence") == "HIGH" for p in positions.values()) else ("LOW" if positions else "HIGH"),
        "fetched_at": _utc_now(),
        "age_sec": 0.0,
        "open_pnl_usdt": round(sum(open_pnl_values), 6) if open_pnl_values else (None if positions else 0.0),
        "local_estimate_pnl_usdt": round(sum(open_pnl_values), 6) if open_pnl_values else None,
        "drift_usdt": None,
        "drift_pct": None,
        "stale_count": 0,
        "mismatch_count": 0,
        **local_summary,
        "positions": positions,
        "configured_tp_mode": _configured_tp_mode(settings),
    }


async def _registry_fallback(symbol: str, reg: dict, okx, okx_ok: bool, reason: str, now: float, settings) -> dict:
    side = _registry_side(reg)
    key = _key(symbol, side)
    state = _position_state.setdefault(key, {})
    now_mono = time.monotonic()
    age_open = _age_from_open(reg, now)
    max_stale = float(_setting(settings, "max_live_position_stale_sec", 10.0) or 10.0)
    grace = float(_setting(settings, "position_sync_grace_sec", 3.0) or 3.0)
    locked = state.get("last_good")
    if locked and now_mono <= float(state.get("okx_lock_until", 0.0) or 0.0):
        locked_row = deepcopy(locked)
        locked_row["age_sec"] = max(now_mono - float(state.get("last_okx_seen_ts", now_mono) or now_mono), 0.0)
        locked_row["mismatch_state"] = "OKX lock active; awaiting next confirmed refresh"
        locked_row["confidence"] = compute_confidence(
            locked_row.get("source", "EXCHANGE_TRUTH"),
            locked_row.get("status", "MATCH"),
            locked_row.get("age_sec"),
            settings,
        )
        return locked_row
    if age_open is not None and (age_open <= grace or state.get("last_state") == "SYNCING") and age_open <= max_stale:
        state.setdefault("sync_started_ts", now_mono)
        state["last_state"] = "SYNCING"
        return _fallback_row(symbol, reg, None, "UNKNOWN", "SYNCING", "waiting for OKX position truth", settings)
    if age_open is not None and age_open > max_stale:
        state["last_state"] = "STALE"
        return _fallback_row(symbol, reg, None, "STALE", "STALE", f"OKX position missing > {max_stale:.0f}s", settings)
    mark = await _mark_price(okx, symbol) if okx_ok else None
    if mark and mark > 0:
        local = _local_estimate(reg, mark)
        return _fallback_row(symbol, reg, mark, "ESTIMATE", "REGISTRY_ONLY", "registry + fresh OKX mark", settings, local)
    status = "STALE" if not okx_ok else "REGISTRY_ONLY"
    source = "STALE" if not okx_ok else "UNKNOWN"
    return _fallback_row(symbol, reg, None, source, status, reason or "OKX truth unavailable", settings)


def _fallback_row(
    symbol: str,
    reg: dict,
    mark: float | None,
    source: str,
    status: str,
    mismatch: str,
    settings,
    local: float | None = None,
) -> dict:
    side = _registry_side(reg)
    entry = _float(reg.get("entry"), None)
    notional = _float(reg.get("size_usdt"), None)
    lev = _int(reg.get("leverage"), None)
    requested_leverage = _int(reg.get("requested_leverage"), lev)
    configured_leverage = _int(reg.get("exchange_configured_leverage"), lev)
    margin = (notional / lev) if notional and lev and lev > 0 else None
    pnl = local if source == "ESTIMATE" else None
    row = {
        "symbol": symbol.upper(),
        "side": side,
        "size": None,
        "notional_usd": notional,
        "entry_price": entry,
        "mark_price": mark,
        "leverage": lev,
        "requested_leverage": requested_leverage,
        "exchange_configured_leverage": configured_leverage,
        "position_effective_leverage": _int(reg.get("position_effective_leverage"), None),
        "requested_vs_configured_mismatch": bool(
            reg.get("requested_vs_configured_mismatch", requested_leverage and configured_leverage and requested_leverage != configured_leverage)
        ),
        "configured_vs_effective_mismatch": bool(
            reg.get("configured_vs_effective_mismatch", configured_leverage and reg.get("position_effective_leverage") and configured_leverage != _int(reg.get("position_effective_leverage"), None))
        ),
        "leverage_truth_status": str(reg.get("leverage_truth_status", "LEVERAGE_UNKNOWN") or "LEVERAGE_UNKNOWN"),
        "leverage_truth_timestamp": str(reg.get("leverage_truth_timestamp", "") or ""),
        "margin": margin,
        "unrealised_pnl": pnl,
        "okx_upl": None,
        "upl_ratio": None,
        "roi_pct": None,
        "source": source,
        "source_label": source_label(source, status),
        "fetched_at": _utc_now(),
        "age_sec": 0.0,
        "status": status,
        "mismatch_state": mismatch,
        "confidence": compute_confidence(source, status, 0.0, settings),
        "local_estimate": local,
        "drift_usdt": None,
        "drift_pct": None,
        "registry": reg,
    }
    return row


async def _build_snapshot(okx, registry: dict[str, dict], settings) -> dict:
    if _paper_mode_enabled():
        return await _build_paper_snapshot(okx, registry, settings)

    fetched_at = _utc_now()
    now_epoch = time.time()
    okx_ok = False
    reason = ""
    raw_positions: list[dict] = []
    try:
        try:
            status = await okx.get_positions_status(priority="status")
        except TypeError:
            status = await okx.get_positions_status()
        okx_ok = bool(status.get("ok", False))
        reason = str(status.get("reason", "") or "")
        raw_positions = list(status.get("positions", []) or []) if okx_ok else []
    except Exception as exc:
        okx_ok = False
        reason = f"{type(exc).__name__}: {exc}"
        raw_positions = []

    local_summary = _local_exposure_summary(registry)
    registry = _truth_registry(registry)
    positions: dict[str, dict] = {}
    seen_symbols: set[str] = set()
    for pos in raw_positions:
        row = _normalize_okx_position(pos, fetched_at, 0.0, registry, settings)
        if not row:
            continue
        symbol = row["symbol"]
        seen_symbols.add(symbol)
        positions[symbol] = row
        _log_drift(row, settings)

    for symbol, reg in sorted(registry.items()):
        if symbol in seen_symbols:
            continue
        positions[symbol] = await _registry_fallback(symbol, reg, okx, okx_ok, reason, now_epoch, settings)

    open_pnl_values = [
        float(p["unrealised_pnl"])
        for p in positions.values()
        if p.get("source") == "EXCHANGE_TRUTH" and p.get("unrealised_pnl") is not None
    ]
    local_values = [
        float(p["local_estimate"])
        for p in positions.values()
        if p.get("local_estimate") is not None
    ]
    drift_values = [
        float(p["drift_usdt"])
        for p in positions.values()
        if p.get("drift_usdt") is not None
    ]
    statuses = {str(p.get("status", "")) for p in positions.values()}
    sources = {str(p.get("source", "")) for p in positions.values()}
    if not positions:
        source = "EXCHANGE_TRUTH" if okx_ok else "STALE"
    elif len(sources) == 1:
        source = next(iter(sources))
    else:
        source = "MIXED"
    confidence = "HIGH" if positions and all(p.get("confidence") == "HIGH" for p in positions.values()) else (
        "MEDIUM" if positions and any(p.get("confidence") == "MEDIUM" for p in positions.values()) else "LOW"
    )
    return {
        "ok": okx_ok,
        "reason": reason,
        "source": source,
        "source_label": _snapshot_label(positions, okx_ok),
        "confidence": confidence,
        "fetched_at": fetched_at,
        "age_sec": 0.0,
        "open_pnl_usdt": round(sum(open_pnl_values), 6) if open_pnl_values else (0.0 if okx_ok else None),
        "local_estimate_pnl_usdt": round(sum(local_values), 6) if local_values else None,
        "drift_usdt": round(sum(drift_values), 6) if drift_values else None,
        "drift_pct": None,
        "stale_count": sum(1 for p in positions.values() if p.get("status") in {"STALE", "TEMPORARY_DESYNC"}),
        "mismatch_count": sum(1 for p in positions.values() if p.get("status") in {"DRIFT", "REGISTRY_ONLY", "EXCHANGE_ONLY"}),
        **local_summary,
        "positions": positions,
        "configured_tp_mode": _configured_tp_mode(settings),
    }


def _snapshot_label(positions: dict[str, dict], okx_ok: bool) -> str:
    if positions and all(p.get("source") == "EXCHANGE_TRUTH" for p in positions.values()):
        return "🟢 OKX"
    if positions and any(p.get("status") == "TEMPORARY_DESYNC" for p in positions.values()):
        return "⚠️ DESYNC"
    if positions and any(p.get("status") == "SYNCING" for p in positions.values()):
        return "⚪ SYNC"
    if positions and any(p.get("source") == "ESTIMATE" for p in positions.values()):
        return "🟡 EST"
    if okx_ok:
        return "🟢 OKX"
    return "🔴 STALE"


def _with_cache_age(snapshot: dict, now: float) -> dict:
    snap = deepcopy(snapshot)
    age = max(now - float(_cache.get("ts", now) or now), 0.0)
    snap["age_sec"] = age
    for row in snap.get("positions", {}).values():
        row["age_sec"] = age
        row["confidence"] = compute_confidence(row.get("source", ""), row.get("status", ""), age)
    return snap


async def get_live_position_snapshot(
    okx,
    *,
    registry: dict[str, dict] | None = None,
    settings=None,
    force_refresh: bool = False,
) -> dict:
    """Return the shared strict PnL truth snapshot for Telegram/reporting."""
    if registry is None:
        try:
            from position_registry import get_all_details
            registry = get_all_details()
        except Exception:
            registry = {}
    now = time.monotonic()
    cached = _cache.get("snapshot")
    ttl = float(_cache.get("ttl", 0.0) or 0.0)
    if cached and not force_refresh and now - float(_cache.get("ts", 0.0) or 0.0) <= ttl:
        return _with_cache_age(cached, now)
    async with _lock:
        now = time.monotonic()
        cached = _cache.get("snapshot")
        ttl = float(_cache.get("ttl", 0.0) or 0.0)
        if cached and not force_refresh and now - float(_cache.get("ts", 0.0) or 0.0) <= ttl:
            return _with_cache_age(cached, now)
        snapshot = await _build_snapshot(okx, registry or {}, settings)
        _cache["snapshot"] = snapshot
        _cache["ts"] = time.monotonic()
        _cache["ttl"] = _jitter_ttl(settings)
        return _with_cache_age(snapshot, time.monotonic())


def cached_live_position_snapshot() -> dict | None:
    """Return the latest shared snapshot without issuing any network request."""
    cached = _cache.get("snapshot")
    if not isinstance(cached, dict):
        return None
    return _with_cache_age(cached, time.monotonic())

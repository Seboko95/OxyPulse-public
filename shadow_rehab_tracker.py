"""Telemetry-only shadow engine rehabilitation ledger.

This module is intentionally isolated from execution, sizing, risk, and engine
mode control. It records shadow/probation evidence for later human review.
"""
from __future__ import annotations

import atexit
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

PATH = Path("data/shadow_rehab_ledger.json")

REHAB_ENGINES = {"FUJIN", "HAYATE", "KAGUTSUCHI", "RYUJIN", "RYUU", "AMATERASU"}
IZANAGI = "IZANAGI"

HORIZONS = {
    "15m": 15 * 60,
    "1h": 60 * 60,
    "4h": 4 * 60 * 60,
    "24h": 24 * 60 * 60,
}

MAX_SIGNALS_PER_ENGINE = 2000
MAX_PROBATION_PER_ENGINE = 1000
FLUSH_INTERVAL_SEC = 45.0

_LEDGER: dict[str, Any] | None = None
_LEDGER_PATH: Path | None = None
_DIRTY = False
_LAST_LOAD_TS = 0.0
_LAST_FLUSH_TS = time.monotonic()
_DIAG: dict[str, Any] = {
    "loads": 0,
    "saves": 0,
    "skipped_saves": 0,
    "update_calls": 0,
    "flush_calls": 0,
    "last_error": "",
}


def _now_ts() -> float:
    return time.time()


def _iso(ts: float | None = None) -> str:
    return datetime.fromtimestamp(float(ts if ts is not None else _now_ts()), timezone.utc).isoformat()


def _empty_engine(engine: str) -> dict[str, Any]:
    return {
        "engine": engine,
        "signals": [],
        "real_probation": [],
    }


def _empty_ledger() -> dict[str, Any]:
    return {
        "version": 1,
        "created_at": _iso(),
        "rehab_engines": {engine: _empty_engine(engine) for engine in sorted(REHAB_ENGINES)},
        "izanagi_outcomes": {
            "engine": IZANAGI,
            "signals": [],
        },
    }


def _normalize_ledger(ledger: dict[str, Any]) -> dict[str, Any]:
    ledger.setdefault("version", 1)
    ledger.setdefault("created_at", _iso())
    engines = ledger.setdefault("rehab_engines", {})
    for engine in sorted(REHAB_ENGINES):
        bucket = engines.setdefault(engine, _empty_engine(engine))
        bucket.setdefault("engine", engine)
        bucket.setdefault("signals", [])
        bucket.setdefault("real_probation", [])
    iz = ledger.setdefault("izanagi_outcomes", {"engine": IZANAGI, "signals": []})
    iz.setdefault("engine", IZANAGI)
    iz.setdefault("signals", [])
    return ledger


def _read_from_disk() -> dict[str, Any]:
    global _LAST_LOAD_TS
    try:
        if PATH.exists():
            raw = json.loads(PATH.read_text(encoding="utf-8"))
            if isinstance(raw, dict):
                ledger = raw
            else:
                ledger = _empty_ledger()
        else:
            ledger = _empty_ledger()
    except Exception:
        _DIAG["last_error"] = "load_failed"
        ledger = _empty_ledger()
    _DIAG["loads"] = int(_DIAG.get("loads", 0) or 0) + 1
    _LAST_LOAD_TS = time.monotonic()
    return _normalize_ledger(ledger)


def _load() -> dict[str, Any]:
    """Return the process-local ledger, loading from disk only once per path."""
    global _LEDGER, _LEDGER_PATH
    current_path = Path(PATH)
    if _LEDGER is None or _LEDGER_PATH != current_path:
        _LEDGER = _read_from_disk()
        _LEDGER_PATH = current_path
    return _LEDGER


def _save(ledger: dict[str, Any]) -> None:
    """Compatibility wrapper: mark and force-flush the provided ledger."""
    global _LEDGER, _LEDGER_PATH, _DIRTY
    _LEDGER = _normalize_ledger(ledger if isinstance(ledger, dict) else _empty_ledger())
    _LEDGER_PATH = Path(PATH)
    _DIRTY = True
    flush(force=True)


def _write_atomic(path: Path, ledger: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(f"{path.name}.{os.getpid()}.tmp")
    try:
        with temp.open("w", encoding="utf-8") as fh:
            json.dump(ledger, fh, indent=2, sort_keys=True, ensure_ascii=False)
            fh.flush()
            os.fsync(fh.fileno())
        if temp.stat().st_size <= 0:
            raise RuntimeError("refusing to replace ledger with empty temp file")
        os.replace(temp, path)
    finally:
        try:
            if temp.exists():
                temp.unlink()
        except Exception:
            pass


def _mark_dirty() -> None:
    global _DIRTY
    _DIRTY = True


def _maybe_flush(*, force: bool = False) -> None:
    if force or (time.monotonic() - _LAST_FLUSH_TS) >= FLUSH_INTERVAL_SEC:
        flush(force=force)


def flush(*, force: bool = False) -> dict[str, Any]:
    """Flush dirty in-memory rehab telemetry to disk using atomic replace."""
    global _DIRTY, _LAST_FLUSH_TS
    _DIAG["flush_calls"] = int(_DIAG.get("flush_calls", 0) or 0) + 1
    ledger = _load()
    if not _DIRTY and not force:
        _DIAG["skipped_saves"] = int(_DIAG.get("skipped_saves", 0) or 0) + 1
        return {"status": "SKIPPED", "dirty": False, "path": str(PATH)}
    try:
        target = _LEDGER_PATH or Path(PATH)
        _write_atomic(target, ledger)
        _DIRTY = False
        _LAST_FLUSH_TS = time.monotonic()
        _DIAG["saves"] = int(_DIAG.get("saves", 0) or 0) + 1
        _DIAG["last_error"] = ""
        return {"status": "OK", "dirty": False, "path": str(target)}
    except Exception as exc:
        _DIAG["last_error"] = str(exc)
        return {"status": "ERROR", "dirty": True, "path": str(PATH), "error": str(exc)}


def get_diagnostics() -> dict[str, Any]:
    """Read-only tracker diagnostics for runtime pressure inspection."""
    now = time.monotonic()
    return {
        "path": str(PATH),
        "loaded": _LEDGER is not None,
        "dirty": _DIRTY,
        "loads": int(_DIAG.get("loads", 0) or 0),
        "saves": int(_DIAG.get("saves", 0) or 0),
        "skipped_saves": int(_DIAG.get("skipped_saves", 0) or 0),
        "update_calls": int(_DIAG.get("update_calls", 0) or 0),
        "flush_calls": int(_DIAG.get("flush_calls", 0) or 0),
        "last_load_age_sec": round(now - _LAST_LOAD_TS, 3) if _LAST_LOAD_TS else None,
        "last_flush_age_sec": round(now - _LAST_FLUSH_TS, 3) if _LAST_FLUSH_TS else None,
        "last_error": str(_DIAG.get("last_error", "") or ""),
    }


def _side(side: str) -> str:
    text = str(side or "").upper()
    if text in {"BUY", "LONG"}:
        return "LONG"
    if text in {"SELL", "SHORT"}:
        return "SHORT"
    return text or "UNKNOWN"


def _signed_move_pct(entry: float, price: float, side: str) -> float:
    entry = float(entry or 0.0)
    price = float(price or 0.0)
    if entry <= 0 or price <= 0:
        return 0.0
    if _side(side) == "SHORT":
        return ((entry - price) / entry) * 100.0
    return ((price - entry) / entry) * 100.0


def _mfe_mae_from_bar(entry: float, side: str, price: float, high: float | None, low: float | None) -> tuple[float, float]:
    values = [_signed_move_pct(entry, price, side)]
    if high and float(high) > 0:
        values.append(_signed_move_pct(entry, float(high), side))
    if low and float(low) > 0:
        values.append(_signed_move_pct(entry, float(low), side))
    return max(values), min(values)


def _new_signal(
    *,
    engine: str,
    symbol: str,
    side: str,
    entry_price: float,
    confidence: float,
    quality: float,
    advisor_strength: float = 0.0,
    consensus_count: int = 0,
    role_context: dict[str, Any] | None = None,
    source: str,
    ts: float | None = None,
) -> dict[str, Any]:
    t = float(ts if ts is not None else _now_ts())
    engine = str(engine or "").upper()
    symbol = str(symbol or "").upper()
    side_n = _side(side)
    return {
        "id": f"{engine}:{symbol}:{side_n}:{int(t * 1000)}",
        "engine": engine,
        "symbol": symbol,
        "side": side_n,
        "source": source,
        "timestamp": t,
        "timestamp_iso": _iso(t),
        "entry_price": float(entry_price or 0.0),
        "confidence": float(confidence or 0.0),
        "quality": float(quality or 0.0),
        "advisor_strength": float(advisor_strength or 0.0),
        "consensus_count": int(consensus_count or 0),
        "role_context": dict(role_context or {}),
        "mfe_pct": 0.0,
        "mae_pct": 0.0,
        "last_price": float(entry_price or 0.0),
        "last_update_ts": t,
        "outcomes": {name: None for name in HORIZONS},
    }


def record_shadow_signal(
    *,
    engine: str,
    symbol: str,
    side: str,
    entry_price: float,
    confidence: float,
    quality: float,
    advisor_strength: float = 0.0,
    consensus_count: int = 0,
    role_context: dict[str, Any] | None = None,
    ts: float | None = None,
) -> None:
    engine_u = str(engine or "").upper()
    if engine_u not in REHAB_ENGINES:
        return
    ledger = _load()
    bucket = ledger["rehab_engines"].setdefault(engine_u, _empty_engine(engine_u))
    bucket["signals"].append(_new_signal(
        engine=engine_u,
        symbol=symbol,
        side=side,
        entry_price=entry_price,
        confidence=confidence,
        quality=quality,
        advisor_strength=advisor_strength,
        consensus_count=consensus_count,
        role_context=role_context,
        source="shadow_signal",
        ts=ts,
    ))
    bucket["signals"] = bucket["signals"][-MAX_SIGNALS_PER_ENGINE:]
    _mark_dirty()
    _maybe_flush()


def record_izanagi_contribution(
    *,
    symbol: str,
    side: str,
    entry_price: float,
    confidence: float,
    quality: float,
    advisor_strength: float,
    consensus_count: int = 0,
    role_context: dict[str, Any] | None = None,
    ts: float | None = None,
) -> None:
    ledger = _load()
    bucket = ledger.setdefault("izanagi_outcomes", {"engine": IZANAGI, "signals": []})
    bucket["signals"].append(_new_signal(
        engine=IZANAGI,
        symbol=symbol,
        side=side,
        entry_price=entry_price,
        confidence=confidence,
        quality=quality,
        advisor_strength=advisor_strength,
        consensus_count=consensus_count,
        role_context=role_context,
        source="izanagi_advisor",
        ts=ts,
    ))
    bucket["signals"] = bucket["signals"][-MAX_SIGNALS_PER_ENGINE:]
    _mark_dirty()
    _maybe_flush()


def update_market_price(
    symbol: str,
    price: float,
    *,
    high: float | None = None,
    low: float | None = None,
    ts: float | None = None,
) -> None:
    _DIAG["update_calls"] = int(_DIAG.get("update_calls", 0) or 0) + 1
    symbol_u = str(symbol or "").upper()
    price_f = float(price or 0.0)
    if not symbol_u or price_f <= 0:
        return
    t = float(ts if ts is not None else _now_ts())
    ledger = _load()
    changed = False
    signal_lists = [ledger.get("izanagi_outcomes", {}).get("signals", [])]
    signal_lists.extend(bucket.get("signals", []) for bucket in ledger.get("rehab_engines", {}).values())
    for signals in signal_lists:
        for sig in signals:
            if str(sig.get("symbol", "")).upper() != symbol_u:
                continue
            entry = float(sig.get("entry_price", 0.0) or 0.0)
            if entry <= 0:
                continue
            mfe, mae = _mfe_mae_from_bar(entry, str(sig.get("side", "")), price_f, high, low)
            sig["mfe_pct"] = round(max(float(sig.get("mfe_pct", 0.0) or 0.0), mfe), 4)
            sig["mae_pct"] = round(min(float(sig.get("mae_pct", 0.0) or 0.0), mae), 4)
            sig["last_price"] = price_f
            sig["last_update_ts"] = t
            outcomes = sig.setdefault("outcomes", {name: None for name in HORIZONS})
            elapsed = max(t - float(sig.get("timestamp", t) or t), 0.0)
            signed = round(_signed_move_pct(entry, price_f, str(sig.get("side", ""))), 4)
            for name, seconds in HORIZONS.items():
                if outcomes.get(name) is not None or elapsed < seconds:
                    continue
                outcomes[name] = {
                    "resolved_at": t,
                    "resolved_at_iso": _iso(t),
                    "elapsed_sec": round(elapsed, 3),
                    "resolution_mode": "scan" if elapsed <= seconds + 90 else "delayed_scan",
                    "exit_price": price_f,
                    "pnl_pct": signed,
                    "win": signed > 0,
                    "mfe_pct": float(sig.get("mfe_pct", 0.0) or 0.0),
                    "mae_pct": float(sig.get("mae_pct", 0.0) or 0.0),
                }
            changed = True
    if changed:
        _mark_dirty()
        _maybe_flush()


def record_probation_close(
    *,
    engine: str,
    symbol: str,
    side: str,
    entry_price: float,
    exit_price: float,
    size_usdt: float,
    pnl_usdt: float,
    fee_usdt: float = 0.0,
    opened_at: str = "",
    closed_at: str = "",
    mfe_pct: float | None = None,
    mae_pct: float | None = None,
) -> None:
    engine_u = str(engine or "").upper()
    if engine_u not in REHAB_ENGINES:
        return
    side_n = _side(side)
    exit_move = _signed_move_pct(float(entry_price or 0.0), float(exit_price or 0.0), side_n)
    rec = {
        "engine": engine_u,
        "symbol": str(symbol or "").upper(),
        "side": side_n,
        "source": "real_probation",
        "opened_at": opened_at,
        "closed_at": closed_at or _iso(),
        "entry_price": float(entry_price or 0.0),
        "exit_price": float(exit_price or 0.0),
        "size_usdt": float(size_usdt or 0.0),
        "pnl_usdt": float(pnl_usdt or 0.0),
        "fee_usdt": float(fee_usdt or 0.0),
        "win": float(pnl_usdt or 0.0) > 0,
        "mfe_pct": round(float(mfe_pct if mfe_pct is not None else max(exit_move, 0.0)), 4),
        "mae_pct": round(float(mae_pct if mae_pct is not None else min(exit_move, 0.0)), 4),
    }
    ledger = _load()
    bucket = ledger["rehab_engines"].setdefault(engine_u, _empty_engine(engine_u))
    episode_key = (
        engine_u,
        rec["symbol"],
        side_n,
        str(opened_at or ""),
        round(float(entry_price or 0.0), 12),
    )
    for existing in reversed(bucket.get("real_probation", [])):
        existing_key = (
            str(existing.get("engine", "")).upper(),
            str(existing.get("symbol", "")).upper(),
            _side(str(existing.get("side", ""))),
            str(existing.get("opened_at", "") or ""),
            round(float(existing.get("entry_price", 0.0) or 0.0), 12),
        )
        if existing_key == episode_key:
            return
    bucket["real_probation"].append(rec)
    bucket["real_probation"] = bucket["real_probation"][-MAX_PROBATION_PER_ENGINE:]
    _mark_dirty()
    _maybe_flush(force=True)


def _metrics(rows: list[dict[str, Any]], pnl_key: str) -> dict[str, Any]:
    count = len(rows)
    wins = [r for r in rows if bool(r.get("win"))]
    losses = [r for r in rows if not bool(r.get("win"))]
    pnl_values = [float(r.get(pnl_key, 0.0) or 0.0) for r in rows]
    gross_win = sum(v for v in pnl_values if v > 0)
    gross_loss = -sum(v for v in pnl_values if v < 0)
    mfe_values = [float(r.get("mfe_pct", 0.0) or 0.0) for r in rows]
    mae_values = [float(r.get("mae_pct", 0.0) or 0.0) for r in rows]
    return {
        "trades": count,
        "wins": len(wins),
        "losses": len(losses),
        "wr": round((len(wins) / count) * 100.0, 2) if count else 0.0,
        "net_pnl": round(sum(pnl_values), 4),
        "pf": round(gross_win / gross_loss, 4) if gross_loss > 0 else (999.0 if gross_win > 0 else 0.0),
        "expectancy": round(sum(pnl_values) / count, 4) if count else 0.0,
        "avg_win": round(gross_win / len(wins), 4) if wins else 0.0,
        "avg_loss": round(-gross_loss / len(losses), 4) if losses else 0.0,
        "avg_mfe": round(sum(mfe_values) / count, 4) if count else 0.0,
        "avg_mae": round(sum(mae_values) / count, 4) if count else 0.0,
        "best_mfe": round(max(mfe_values), 4) if mfe_values else 0.0,
        "worst_mae": round(min(mae_values), 4) if mae_values else 0.0,
    }


def _status(metrics: dict[str, Any]) -> str:
    trades = int(metrics.get("trades", 0) or 0)
    if trades <= 0:
        return "INSUFFICIENT_DATA"
    if trades < 20:
        return "COLLECTING"
    if (
        float(metrics.get("wr", 0.0) or 0.0) < 35.0
        or float(metrics.get("pf", 0.0) or 0.0) < 0.75
        or float(metrics.get("expectancy", 0.0) or 0.0) < 0.0
    ):
        return "FAILING"
    if (
        trades >= 30
        and float(metrics.get("wr", 0.0) or 0.0) >= 55.0
        and float(metrics.get("pf", 0.0) or 0.0) >= 1.25
        and float(metrics.get("expectancy", 0.0) or 0.0) > 0.0
        and float(metrics.get("net_pnl", 0.0) or 0.0) > 0.0
    ):
        return "READY_FOR_REVIEW"
    return "COLLECTING"


def _resolved_shadow_rows(signals: list[dict[str, Any]], horizon: str = "1h") -> list[dict[str, Any]]:
    rows = []
    for sig in signals:
        outcome = (sig.get("outcomes", {}) or {}).get(horizon)
        if outcome:
            row = dict(outcome)
            row["symbol"] = sig.get("symbol")
            row["side"] = sig.get("side")
            row["pnl_pct"] = float(row.get("pnl_pct", 0.0) or 0.0) / 100.0
            rows.append(row)
    return rows


def get_rehab_snapshot(settings: Any | None = None) -> dict[str, Any]:
    ledger = _load()
    engines = []
    for engine in sorted(REHAB_ENGINES):
        bucket = ledger.get("rehab_engines", {}).get(engine, _empty_engine(engine))
        shadow_rows = _resolved_shadow_rows(bucket.get("signals", []), "1h")
        real_rows = list(bucket.get("real_probation", []) or [])
        virtual_metrics = _metrics(shadow_rows, "pnl_pct")
        real_metrics = _metrics(real_rows, "pnl_usdt")
        composite = _metrics(
            [{"win": r.get("win"), "pnl": r.get("pnl_pct"), "mfe_pct": r.get("mfe_pct"), "mae_pct": r.get("mae_pct")} for r in shadow_rows]
            + [{"win": r.get("win"), "pnl": r.get("pnl_usdt"), "mfe_pct": r.get("mfe_pct"), "mae_pct": r.get("mae_pct")} for r in real_rows],
            "pnl",
        )
        mode = "UNKNOWN"
        if settings is not None:
            try:
                mode = str(settings.get_engine_mode(engine)).upper()
            except Exception:
                try:
                    mode = str(settings.get_val(f"engine_{engine.lower()}_mode", "unknown")).upper()
                except Exception:
                    mode = "UNKNOWN"
        engines.append({
            "engine": engine,
            "mode": mode,
            "status": _status(composite),
            "probation_status": "TRACKING" if real_metrics["trades"] else "NONE",
            "post_upgrade": composite,
            "virtual_shadow": virtual_metrics,
            "real_probation": real_metrics,
            "pending_signals": sum(1 for sig in bucket.get("signals", []) if not (sig.get("outcomes", {}) or {}).get("1h")),
            "total_signals": len(bucket.get("signals", []) or []),
        })
    return {"engines": engines, "ledger_path": str(PATH), "version": ledger.get("version", 1)}


def get_izanagi_snapshot() -> dict[str, Any]:
    ledger = _load()
    signals = list((ledger.get("izanagi_outcomes", {}) or {}).get("signals", []) or [])
    horizons = {}
    best_symbols: dict[str, float] = {}
    worst_symbols: dict[str, float] = {}
    for horizon in HORIZONS:
        rows = _resolved_shadow_rows(signals, horizon)
        metrics = _metrics(rows, "pnl_pct")
        horizons[horizon] = metrics
        if horizon == "1h":
            by_symbol: dict[str, list[float]] = {}
            for row in rows:
                by_symbol.setdefault(str(row.get("symbol", "") or "?"), []).append(float(row.get("pnl_pct", 0.0) or 0.0))
            ranked = sorted(((sym, sum(vals) / len(vals)) for sym, vals in by_symbol.items() if vals), key=lambda item: item[1], reverse=True)
            best_symbols = dict((sym, round(val, 4)) for sym, val in ranked[:5])
            worst_symbols = dict((sym, round(val, 4)) for sym, val in ranked[-5:])
    return {
        "signals": len(signals),
        "pending_1h": sum(1 for sig in signals if not (sig.get("outcomes", {}) or {}).get("1h")),
        "horizons": horizons,
        "best_symbols": best_symbols,
        "worst_symbols": worst_symbols,
        "status": "INSUFFICIENT_DATA" if not signals else ("COLLECTING" if not any(h["trades"] for h in horizons.values()) else "TRACKING"),
        "ledger_path": str(PATH),
    }


atexit.register(flush)

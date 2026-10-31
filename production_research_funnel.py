"""Persisted, analytics-only production research funnel telemetry."""
from __future__ import annotations

import json
import os
import threading
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

EVENT_PATH = Path("data/production_research_funnel.jsonl")
LATEST_PATH = Path("data/production_research_funnel_latest.json")
_LOCK = threading.RLock()
_SESSION_ID = f"{os.getpid()}-{int(time.time())}"
_current: dict = {}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def begin_scan(scan_id: str, symbols: list[str], readiness: dict, regime: dict) -> None:
    global _current
    with _LOCK:
        symbol_rows = dict(readiness.get("symbols", {}) or {})
        data_ready = sum(1 for row in symbol_rows.values() if (row.get("1m") or {}).get("indicator_ready"))
        regime_name = str(regime.get("regime", "UNKNOWN") or "UNKNOWN").upper()
        regime_ready = bool(regime.get("readiness") == "READY" and regime_name != "UNKNOWN")
        restricted = regime_name == "DEAD" or str(regime.get("readiness", "")) == "RESTRICTED"
        _current = {
            "event": "research_scan", "session_id": _SESSION_ID, "scan_id": scan_id,
            "timestamp": _now(), "symbols_seen": len(symbols), "symbols_data_ready": data_ready,
            "symbols_regime_ready": len(symbols) if regime_ready else 0,
            "symbols_UNKNOWN": len(symbols) if regime_name == "UNKNOWN" else 0,
            "symbols_restricted": len(symbols) if restricted else 0,
            "symbols_tradeable_regime": len(symbols) if regime_ready and not restricted else 0,
            "regime": regime, "timeframes": readiness, "counts": Counter(),
            "reasons": Counter(), "by_engine": {},
        }


def record(stage: str, *, engine: str = "", symbol: str = "", reason: str = "", **evidence) -> None:
    with _LOCK:
        if not _current:
            return
        stage = str(stage or "unknown")
        _current["counts"][stage] += 1
        if reason:
            _current["reasons"][str(reason)] += 1
        if engine:
            row = _current["by_engine"].setdefault(str(engine).upper(), {"counts": Counter(), "reasons": Counter()})
            row["counts"][stage] += 1
            if reason:
                row["reasons"][str(reason)] += 1
        if stage in {"raw_signal", "pre_candidate", "candidate"}:
            _current.setdefault("recent_evidence", []).append({
                "stage": stage, "engine": str(engine).upper(), "symbol": str(symbol).upper(),
                "reason": str(reason), **evidence,
            })
            _current["recent_evidence"] = _current["recent_evidence"][-100:]


def complete_scan(**extra) -> dict:
    with _LOCK:
        if not _current:
            return {}
        row = dict(_current)
        row["finished_at"] = _now()
        row["counts"] = dict(row.get("counts", {}))
        row["reasons"] = dict(row.get("reasons", {}))
        row["by_engine"] = {
            key: {"counts": dict(value.get("counts", {})), "reasons": dict(value.get("reasons", {}))}
            for key, value in row.get("by_engine", {}).items()
        }
        row.update(extra)
        if "PYTEST_CURRENT_TEST" not in os.environ or EVENT_PATH != Path("data/production_research_funnel.jsonl"):
            EVENT_PATH.parent.mkdir(parents=True, exist_ok=True)
            with EVENT_PATH.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n")
            temp = LATEST_PATH.with_suffix(".tmp")
            temp.write_text(json.dumps(row, indent=2, sort_keys=True), encoding="utf-8")
            temp.replace(LATEST_PATH)
        return row


def latest() -> dict:
    try:
        return json.loads(LATEST_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}


def summarize() -> dict:
    rows = []
    try:
        rows = [json.loads(line) for line in EVENT_PATH.read_text(encoding="utf-8").splitlines() if line.strip()]
    except Exception:
        pass
    totals = Counter()
    reasons = Counter()
    for row in rows:
        totals.update(row.get("counts", {}) or {})
        reasons.update(row.get("reasons", {}) or {})
    return {"scans": len(rows), "counts": dict(totals), "reasons": dict(reasons), "latest": rows[-1] if rows else {}}

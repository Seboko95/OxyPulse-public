from __future__ import annotations

import threading
from datetime import datetime, timezone

import settings_store as _s

_lock = threading.Lock()
_sources: dict[str, dict] = {}


def _ts() -> str:
    return datetime.now(timezone.utc).isoformat()


def _thresholds() -> tuple[int, int]:
    degraded = max(1, int(_s.get_val("sync_health_degraded_failures", 1) or 1))
    down = max(degraded + 1, int(_s.get_val("sync_health_down_failures", 3) or 3))
    return degraded, down


def _source_snapshot(source: str) -> dict:
    snap = _sources.get(source)
    if snap is None:
        snap = {
            "status": "OK",
            "consecutive_failures": 0,
            "last_error": "",
            "last_successful_sync": "",
            "last_failure_at": "",
            "source": source,
        }
        _sources[source] = snap
    return snap


def mark_success(source: str) -> None:
    with _lock:
        snap = _source_snapshot(source)
        snap["status"] = "OK"
        snap["consecutive_failures"] = 0
        snap["last_error"] = ""
        snap["last_successful_sync"] = _ts()


def mark_failure(source: str, reason: str) -> None:
    degraded_at, down_at = _thresholds()
    with _lock:
        snap = _source_snapshot(source)
        failures = int(snap.get("consecutive_failures", 0) or 0) + 1
        snap["consecutive_failures"] = failures
        snap["last_error"] = str(reason or "")
        snap["last_failure_at"] = _ts()
        if failures >= down_at:
            snap["status"] = "DOWN"
        elif failures >= degraded_at:
            snap["status"] = "DEGRADED"
        else:
            snap["status"] = "OK"


def get_snapshot() -> dict:
    with _lock:
        if not _sources:
            return {
                "status": "OK",
                "status_emoji": "🟢",
                "reason": "",
                "sources": {},
            }
        worst_rank = {"OK": 0, "DEGRADED": 1, "DOWN": 2}
        worst = "OK"
        worst_reason = ""
        ordered_sources: dict[str, dict] = {}
        for source, raw in _sources.items():
            snap = dict(raw)
            status = str(snap.get("status", "OK") or "OK").upper()
            snap["status"] = status
            ordered_sources[source] = snap
            if worst_rank.get(status, 0) > worst_rank.get(worst, 0):
                worst = status
                worst_reason = str(snap.get("last_error", "") or "")
        return {
            "status": worst,
            "status_emoji": {"OK": "🟢", "DEGRADED": "🟡", "DOWN": "🔴"}.get(worst, "⚪"),
            "reason": worst_reason,
            "sources": ordered_sources,
        }


def get_status_label(*, running: bool = False, ws_active: bool = True) -> str:
    snap = get_snapshot()
    status = str(snap.get("status", "OK") or "OK").upper()
    reason = str(snap.get("reason", "") or "")
    if running and not ws_active and status == "OK":
        status = "DEGRADED"
        reason = reason or "ws_unavailable"
    emoji = {"OK": "🟢", "DEGRADED": "🟡", "DOWN": "🔴"}.get(status, "⚪")
    if reason:
        return f"{emoji} {status} - {reason}"
    return f"{emoji} {status}"


def reset() -> None:
    with _lock:
        _sources.clear()

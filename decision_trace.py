from __future__ import annotations

import threading
import time
from collections import deque
from dataclasses import dataclass, asdict
from datetime import datetime, timezone

_MAXLEN = 300


@dataclass
class DecisionTrace:
    ts: str
    symbol: str
    side: str
    engine: str
    raw_score: float = 0.0
    confidence: float = 0.0
    hachiman_influence: float = 0.0
    selector_influence: float = 0.0
    regime: str = ""
    slot_state: str = ""
    exposure_state: str = ""
    size_usdt: float = 0.0
    size_decision: str = ""
    final_decision: str = ""
    reasons: str = ""
    candidate_quality: float = 0.0
    notes: str = ""
    extra: dict | None = None


_lock = threading.Lock()
_buf: deque[DecisionTrace] = deque(maxlen=_MAXLEN)


def _runtime_state() -> dict:
    try:
        import settings_store as _ss
        import position_registry as _pr

        reg = getattr(_pr, "_registry", {}) or {}
        open_count = len(reg)
        global_cap = int(_ss.get_val("max_open_positions", 10) or 10)
        slot_mode = str(_ss.get_val("global_slot_mode", "fixed") or "fixed").strip().lower()
        exposure_mode = str(_ss.get_val("exposure_control_mode", "Strict") or "Strict")
        return {
            "slot_state": f"{open_count}/{global_cap} {slot_mode}",
            "exposure_state": exposure_mode,
        }
    except Exception:
        return {"slot_state": "", "exposure_state": ""}


def record(trace: dict | DecisionTrace) -> dict:
    if isinstance(trace, DecisionTrace):
        payload = asdict(trace)
    else:
        payload = dict(trace or {})
    payload.setdefault("ts", datetime.now(timezone.utc).isoformat())
    payload.setdefault("symbol", "")
    payload.setdefault("side", "")
    payload.setdefault("engine", "")
    payload.setdefault("raw_score", 0.0)
    payload.setdefault("confidence", 0.0)
    payload.setdefault("hachiman_influence", 0.0)
    payload.setdefault("selector_influence", 0.0)
    payload.setdefault("regime", "")
    payload.setdefault("slot_state", "")
    payload.setdefault("exposure_state", "")
    payload.setdefault("size_usdt", 0.0)
    payload.setdefault("size_decision", "")
    payload.setdefault("final_decision", "")
    payload.setdefault("reasons", "")
    payload.setdefault("candidate_quality", 0.0)
    payload.setdefault("notes", "")
    payload.setdefault("extra", {})
    state = _runtime_state()
    if not payload.get("slot_state"):
        payload["slot_state"] = state.get("slot_state", "")
    if not payload.get("exposure_state"):
        payload["exposure_state"] = state.get("exposure_state", "")
    with _lock:
        _buf.append(DecisionTrace(**payload))
    return payload


def get_recent(limit: int = 25, *, decision: str | None = None) -> list[dict]:
    with _lock:
        items = list(_buf)
    if decision:
        target = str(decision).strip().lower()
        items = [item for item in items if str(item.final_decision).strip().lower() == target]
    return [asdict(item) for item in items[-limit:]]


def get_summary() -> dict:
    with _lock:
        items = list(_buf)
    counts: dict[str, int] = {}
    for item in items:
        key = str(item.final_decision or "unknown").strip().lower() or "unknown"
        counts[key] = counts.get(key, 0) + 1
    return dict(sorted(counts.items(), key=lambda kv: (-kv[1], kv[0])))


def clear() -> None:
    with _lock:
        _buf.clear()

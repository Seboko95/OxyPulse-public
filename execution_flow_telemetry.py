"""Durable production execution-funnel events for operations and Telegram."""
from __future__ import annotations

import json
import os
import threading
import time
import uuid
from collections import Counter, deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from executable_minimum import FUNNEL_COUNTER_KEYS, classify_pre_okx_reason


EVENT_PATH = Path("data/live_execution_events.jsonl")
COUNTERS_PATH = Path("data/execution_funnel_counters.json")
PAYLOAD_AUDIT_PATH = Path("data/payload_terminal_audit.jsonl")
_LOCK = threading.RLock()
_RECENT: deque[dict[str, Any]] = deque(maxlen=500)
_COUNTERS: Counter[str] = Counter()


def _iso(ts: float | None = None) -> str:
    return datetime.fromtimestamp(ts or time.time(), timezone.utc).isoformat()


def _pytest_default_path(path: Path) -> bool:
    if "PYTEST_CURRENT_TEST" not in os.environ:
        return False
    defaults = {
        Path("data/live_execution_events.jsonl"),
        Path("data/execution_funnel_counters.json"),
        Path("data/payload_terminal_audit.jsonl"),
    }
    try:
        return path.resolve() in {item.resolve() for item in defaults}
    except OSError:
        return path in defaults


def _load_recent() -> None:
    try:
        rows = EVENT_PATH.read_text(encoding="utf-8").splitlines()[-500:]
        _RECENT.extend(json.loads(line) for line in rows if line.strip())
    except Exception:
        return


def _load_counters() -> None:
    try:
        if COUNTERS_PATH.exists():
            payload = json.loads(COUNTERS_PATH.read_text(encoding="utf-8"))
            raw = payload.get("counters", payload) if isinstance(payload, dict) else {}
            _COUNTERS.update({str(k): int(v) for k, v in dict(raw or {}).items()})
    except Exception:
        return


def _save_counters() -> None:
    if _pytest_default_path(COUNTERS_PATH):
        return
    COUNTERS_PATH.parent.mkdir(parents=True, exist_ok=True)
    COUNTERS_PATH.write_text(
        json.dumps(
            {
                "updated_at": _iso(),
                "counters": {key: int(_COUNTERS.get(key, 0)) for key in FUNNEL_COUNTER_KEYS},
            },
            indent=2,
            ensure_ascii=True,
        ),
        encoding="utf-8",
    )


_load_recent()
_load_counters()


def bump(counter: str, amount: int = 1) -> int:
    key = str(counter or "").strip().lower()
    if not key:
        return 0
    with _LOCK:
        _COUNTERS[key] += int(amount)
        _save_counters()
        return int(_COUNTERS[key])


def counters_snapshot() -> dict[str, int]:
    with _LOCK:
        return {key: int(_COUNTERS.get(key, 0)) for key in FUNNEL_COUNTER_KEYS}


def reset_counters_for_tests() -> None:
    with _LOCK:
        _COUNTERS.clear()
        if not _pytest_default_path(COUNTERS_PATH) and COUNTERS_PATH.exists():
            try:
                COUNTERS_PATH.unlink()
            except OSError:
                pass


def record(stage: str, **fields: Any) -> dict[str, Any]:
    row = {
        "event_id": uuid.uuid4().hex,
        "timestamp": _iso(),
        "timestamp_epoch": time.time(),
        "stage": str(stage or "UNKNOWN").upper(),
        "execution_environment": "production" if os.getenv("PAPER_TRADING", "false").lower() != "true" else "paper",
        **fields,
    }
    counter = str(fields.get("funnel_counter", "") or "").strip().lower()
    with _LOCK:
        _RECENT.append(row)
        if counter:
            _COUNTERS[counter] += 1
            _save_counters()
        if not _pytest_default_path(EVENT_PATH):
            EVENT_PATH.parent.mkdir(parents=True, exist_ok=True)
            with EVENT_PATH.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(row, sort_keys=True, separators=(",", ":"), default=str) + "\n")
    return dict(row)


def record_pre_okx_skip(**fields: Any) -> dict[str, Any]:
    reason = str(fields.get("reason", "") or "")
    counter = str(fields.get("funnel_counter", "") or classify_pre_okx_reason(reason))
    payload = {
        k: v for k, v in fields.items()
        if k not in {"funnel_counter", "terminal_class", "exchange_contacted"}
    }
    return record(
        "PRE_OKX_SKIP",
        funnel_counter=counter,
        terminal_class=counter,
        exchange_contacted=False,
        **payload,
    )


def record_payload_terminal(
    *,
    intent_id: str,
    terminal_class: str,
    reason: str,
    exchange_contacted: bool,
    **fields: Any,
) -> dict[str, Any]:
    # The caller supplies a durable terminal-event ID.  Deduping at the sink
    # closes the crash window after append but before the lifecycle outbox can
    # be marked delivered.
    durable_event_id = str(fields.pop("terminal_event_id", "") or "")
    row = {
        "event_id": durable_event_id or uuid.uuid4().hex,
        "timestamp": _iso(),
        "timestamp_epoch": time.time(),
        "intent_id": str(intent_id or ""),
        "terminal_class": str(terminal_class or "rejected_other"),
        "reason": str(reason or ""),
        "exchange_contacted": bool(exchange_contacted),
        "execution_environment": "production" if os.getenv("PAPER_TRADING", "false").lower() != "true" else "paper",
        **fields,
    }
    with _LOCK:
        if not _pytest_default_path(PAYLOAD_AUDIT_PATH):
            PAYLOAD_AUDIT_PATH.parent.mkdir(parents=True, exist_ok=True)
            if durable_event_id and PAYLOAD_AUDIT_PATH.exists():
                for line in PAYLOAD_AUDIT_PATH.read_text(encoding="utf-8").splitlines():
                    try:
                        if json.loads(line).get("event_id") == durable_event_id:
                            return dict(row)
                    except (TypeError, ValueError):
                        continue
            with PAYLOAD_AUDIT_PATH.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(row, sort_keys=True, separators=(",", ":"), default=str) + "\n")
    return dict(row)


def snapshot(limit: int = 20) -> dict[str, Any]:
    with _LOCK:
        rows = [dict(row) for row in _RECENT if row.get("execution_environment") == "production"]
        counters = {key: int(_COUNTERS.get(key, 0)) for key in FUNNEL_COUNTER_KEYS}
    counts = Counter(str(row.get("stage", "UNKNOWN")) for row in rows)
    blockers = Counter(
        str(row.get("reason", "") or "")
        for row in rows
        if row.get("reason") and str(row.get("stage", "")).upper() in {
            "REJECTED", "FAILED_FINAL", "ENGINE_TIMEOUT", "PRE_SUBMIT_REJECTED",
            "PRE_OKX_SKIP", "RESERVATION_FAILED", "SUBMIT_UNCERTAIN",
        }
    )
    return {
        "source": "live_execution_events",
        "generated_at": _iso(),
        "counts": dict(counts),
        "funnel_counters": counters,
        "top_blockers": blockers.most_common(10),
        "recent": rows[-max(int(limit), 0):][::-1],
        "total_events": len(rows),
    }

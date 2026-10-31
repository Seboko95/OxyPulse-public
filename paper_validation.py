"""Paper-mode validation telemetry for risk-based sizing."""
from __future__ import annotations

import json
import logging
import os
import hashlib
from threading import RLock
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

PATH = Path("data/paper_validation_trades.jsonl")
logger = logging.getLogger("OxyPulse.PaperValidation")
_LOCK = RLock()
_SEEN_EVENT_IDS: set[str] | None = None


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(v) for v in value]
    if isinstance(value, bool) or value is None or isinstance(value, str):
        return value
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return round(value, 10)
    try:
        return float(value)
    except Exception:
        return str(value)


def append_event(event: str, **fields: Any) -> None:
    if "PYTEST_CURRENT_TEST" in os.environ and PATH == Path("data/paper_validation_trades.jsonl"):
        return
    timestamp = _now_iso()
    lifecycle_id = str(fields.get("lifecycle_id", fields.get("order_id", fields.get("candidate_id", ""))) or "")
    identity = "|".join((
        str(event), lifecycle_id or timestamp,
        str(fields.get("partial_index", "")), str(fields.get("exit_reason", "")),
    ))
    event_id = str(fields.get("event_id", "") or hashlib.sha256(identity.encode("utf-8")).hexdigest()[:24])
    record = {
        "timestamp": timestamp,
        "event": str(event),
        "event_id": event_id,
        "lifecycle_id": lifecycle_id,
        **{str(k): _json_safe(v) for k, v in fields.items()},
    }
    try:
        global _SEEN_EVENT_IDS
        with _LOCK:
            if _SEEN_EVENT_IDS is None:
                _SEEN_EVENT_IDS = set()
                if PATH.exists():
                    for line in PATH.read_text(encoding="utf-8", errors="ignore").splitlines():
                        try:
                            prior = json.loads(line)
                            if prior.get("event_id"):
                                _SEEN_EVENT_IDS.add(str(prior["event_id"]))
                        except Exception:
                            continue
            if event_id in _SEEN_EVENT_IDS:
                logger.warning("paper validation duplicate event suppressed: %s", event_id)
                return
            PATH.parent.mkdir(parents=True, exist_ok=True)
            with PATH.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n")
            _SEEN_EVENT_IDS.add(event_id)
    except Exception as exc:
        logger.warning("paper validation telemetry write failed: %s", exc)


def record_entry(**fields: Any) -> None:
    merged = dict(fields)
    opened_at = str(merged.get("opened_at", "") or _now_iso())
    merged["opened_at"] = opened_at
    merged.setdefault("first_fill_at", opened_at)
    merged.setdefault("last_entry_fill_at", opened_at)
    append_event("entry", outcome="pending", primary_state="OPEN", final_state=False, **merged)


def record_skip(**fields: Any) -> None:
    append_event("skip", outcome="skipped", primary_state="SKIPPED", final_state=True, **fields)


def record_close(*, context: dict[str, Any] | None = None, **fields: Any) -> None:
    merged = dict(context or {})
    merged.update(fields)
    pnl = float(merged.get("pnl_usdt", 0.0) or 0.0)
    fee = float(merged.get("fee_usdt", 0.0) or 0.0)
    net = float(merged.get("net_pnl_usdt", pnl - fee) or 0.0)
    merged["net_pnl_usdt"] = round(net, 10)
    reason = str(merged.get("exit_reason", "") or "").lower()
    if reason == "slot_rotation" and abs(pnl) < 1e-9:
        outcome = "neutral"
    else:
        outcome = "win" if net > 0 else "loss"
    merged["outcome"] = outcome
    closed_at = str(merged.get("closed_at", "") or _now_iso())
    merged["closed_at"] = closed_at
    merged.setdefault("first_exit_fill_at", closed_at)
    merged.setdefault("last_exit_fill_at", closed_at)
    merged["primary_state"] = "CLOSED"
    merged["final_state"] = True
    opened_at = str(merged.get("opened_at", "") or "")
    if opened_at:
        try:
            opened_dt = datetime.fromisoformat(opened_at.replace("Z", "+00:00"))
            closed_dt = datetime.fromisoformat(closed_at.replace("Z", "+00:00"))
            merged["duration_seconds"] = max((closed_dt - opened_dt).total_seconds(), 0.0)
        except Exception:
            merged.setdefault("duration_seconds", None)
    append_event("close", **merged)

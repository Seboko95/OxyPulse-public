"""Persistent queue for valid opportunities waiting on temporary capacity."""
from __future__ import annotations

import hashlib
import json
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from threading import RLock
from typing import Any, Callable

import settings_store as _s


def candidate_id(pair: str, direction: str, engine: str, signal_timestamp: float) -> str:
    raw = f"{pair.upper()}|{direction.lower()}|{engine.upper()}|{signal_timestamp:.6f}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]


@dataclass
class Opportunity:
    candidate_id: str
    pair: str
    direction: str
    engine: str
    signal_timestamp: float
    expiry_timestamp: float
    original_score: float
    current_score: float
    confidence: float
    intended_entry: float
    stop: float
    tp_structure: dict
    risk_requirement: float
    estimated_margin_requirement: float
    instrument_eligibility: str
    deployment_mode: str
    correlation_group: str
    queued_reason: str
    allocation_attempts: int = 0
    last_allocation_attempt: float = 0.0
    terminal_reason: str = ""
    market_geometry_timestamp: float = 0.0
    payload: dict = field(default_factory=dict)


class OpportunityQueue:
    def __init__(self, path: Path | str = "data/opportunity_queue.json"):
        self.path = Path(path)
        self._lock = RLock()
        self._items: dict[str, Opportunity] = {}
        self._load()

    def _load(self) -> None:
        try:
            if self.path.exists():
                raw = json.loads(self.path.read_text(encoding="utf-8"))
                self._items = {row["candidate_id"]: Opportunity(**row) for row in raw.get("opportunities", [])}
                self.expire()
        except Exception:
            self._items = {}

    def _save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = {"opportunities": [asdict(row) for row in self._items.values()]}
        self.path.write_text(json.dumps(payload, indent=2, ensure_ascii=True), encoding="utf-8")

    def expire(self, now: float | None = None) -> int:
        current = float(now or time.time())
        with self._lock:
            expired = [cid for cid, row in self._items.items() if row.expiry_timestamp <= current]
            for cid in expired:
                self._items.pop(cid, None)
            if expired:
                self._save()
            return len(expired)

    def enqueue(self, opportunity: Opportunity) -> tuple[bool, str]:
        with self._lock:
            self.expire()
            duplicate = next(
                (row for row in self._items.values() if row.pair == opportunity.pair and row.direction == opportunity.direction),
                None,
            )
            if duplicate:
                if opportunity.current_score <= duplicate.current_score:
                    return False, "weaker_duplicate_pair_direction"
                self._items.pop(duplicate.candidate_id, None)
                opportunity.terminal_reason = f"replaced_{duplicate.candidate_id}"
            conflict = next((row for row in self._items.values() if row.pair == opportunity.pair and row.direction != opportunity.direction), None)
            if conflict and not bool(_s.get_val("opportunity_queue_allow_opposing", False)):
                if opportunity.current_score <= conflict.current_score:
                    return False, "conflicting_direction"
                self._items.pop(conflict.candidate_id, None)
            limit = max(int(_s.get_val("opportunity_queue_max_size", 100)), 1)
            if len(self._items) >= limit:
                weakest = min(self._items.values(), key=lambda row: (row.current_score, row.signal_timestamp))
                if opportunity.current_score <= weakest.current_score:
                    return False, "queue_full_weaker_than_existing"
                self._items.pop(weakest.candidate_id, None)
            self._items[opportunity.candidate_id] = opportunity
            self._save()
            return True, "queued"

    def remove(self, candidate_id_value: str, reason: str = "funded") -> Opportunity | None:
        with self._lock:
            row = self._items.pop(candidate_id_value, None)
            if row:
                row.terminal_reason = reason
                self._save()
            return row

    def reattemptable(self, *, now: float | None = None, rescore: Callable[[Opportunity], float] | None = None) -> list[Opportunity]:
        current = float(now or time.time())
        with self._lock:
            self.expire(current)
            geometry_ttl = float(_s.get_val("opportunity_geometry_ttl_secs", 300.0))
            rows: list[Opportunity] = []
            for row in self._items.values():
                if row.market_geometry_timestamp and current - row.market_geometry_timestamp > geometry_ttl:
                    continue
                if rescore:
                    row.current_score = float(rescore(row))
                row.allocation_attempts += 1
                row.last_allocation_attempt = current
                rows.append(row)
            self._save()
            return sorted(rows, key=lambda row: (row.current_score, row.confidence, row.signal_timestamp), reverse=True)

    def snapshot(self, limit: int = 10) -> dict:
        with self._lock:
            self.expire()
            rows = sorted(self._items.values(), key=lambda row: row.current_score, reverse=True)
            return {
                "count": len(rows),
                "queued_margin_demand": round(sum(row.estimated_margin_requirement for row in rows), 6),
                "top": [asdict(row) for row in rows[: max(int(limit), 0)]],
            }


queue = OpportunityQueue()

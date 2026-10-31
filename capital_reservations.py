"""Durable, atomic margin reservations for pending OxyPulse order intents."""
from __future__ import annotations

import asyncio
import json
import time
import uuid
from pathlib import Path
from typing import Any


# Wall-clock expiry is safe only while a reservation is explicitly proved to
# be pre-submit.  Every other value is deliberately retained: an unresolved
# exchange mutation must continue to consume capacity after a restart.
PRE_SUBMIT_OWNERSHIP = "PRE_SUBMIT_PROVEN"
RECOVERY_OWNED = "RECOVERY_OWNED"


class CapitalReservationBook:
    def __init__(self, path: Path | str = "data/capital_reservations.json"):
        self.path = Path(path)
        self._lock = asyncio.Lock()
        self._items: dict[str, dict[str, Any]] = {}
        self._healthy = True
        self._load_error = ""
        self._load()

    def health(self) -> dict[str, Any]:
        return {"healthy": bool(self._healthy), "error": str(self._load_error or ""), "path": str(self.path)}

    def _load(self) -> None:
        try:
            if self.path.exists():
                raw = json.loads(self.path.read_text(encoding="utf-8"))
                if not isinstance(raw, dict):
                    raise ValueError("reservation_store_top_level_must_be_object")
                persisted = raw.get("reservations")
                if not isinstance(persisted, dict):
                    raise ValueError("reservation_store_reservations_must_be_object")
                if any(not isinstance(row, dict) for row in persisted.values()):
                    raise ValueError("reservation_store_row_must_be_object")
                self._items = {
                    str(reservation_id): dict(row)
                    for reservation_id, row in persisted.items()
                }
                # Historical rows have no durable proof that they remained
                # pre-submit.  Pin them until explicit terminal reconciliation
                # rather than allowing an upgrade to free capital by TTL.
                normalized = False
                for row in self._items.values():
                    if str(row.get("ownership_state", "") or "") != PRE_SUBMIT_OWNERSHIP:
                        if not row.get("ownership_state"):
                            row["ownership_state"] = RECOVERY_OWNED
                            row["ownership_reason"] = "legacy_reservation_ownership_unknown"
                            normalized = True
                if normalized:
                    self._save()
                self.expire_sync()
        except Exception as exc:
            # A reservation ledger whose contents cannot be read may contain
            # exchange-owned capacity.  It must never become an empty book.
            self._items = {}
            self._healthy = False
            self._load_error = f"{type(exc).__name__}:{exc}"

    def _save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(
            json.dumps({"reservations": self._items}, indent=2, ensure_ascii=True),
            encoding="utf-8",
        )

    def expire_sync(self, now: float | None = None) -> int:
        if not self._healthy:
            return 0
        current = float(now or time.time())
        expired = [
            rid for rid, row in self._items.items()
            if str(row.get("ownership_state", "") or "") == PRE_SUBMIT_OWNERSHIP
            and float(row.get("expires_at", 0)) <= current
        ]
        for rid in expired:
            self._items.pop(rid, None)
        if expired:
            self._save()
        return len(expired)

    def total_margin(self) -> float:
        if not self._healthy:
            return float("inf")
        self.expire_sync()
        return sum(float(row.get("margin_usdt", 0.0) or 0.0) for row in self._items.values())

    def snapshot(self) -> list[dict]:
        if not self._healthy:
            return [{"reservation_store_unhealthy": True, "error": self._load_error, "margin_usdt": float("inf")}]
        self.expire_sync()
        return [dict(row) for row in self._items.values()]

    def get(self, reservation_id: str) -> dict | None:
        """Return a current reservation without exposing mutable book state."""
        if not self._healthy:
            return None
        self.expire_sync()
        row = self._items.get(str(reservation_id or ""))
        return dict(row) if row else None

    async def reserve(
        self,
        *,
        intent_id: str,
        candidate_id: str,
        margin_usdt: float,
        risk_usdt: float,
        max_total_margin_usdt: float,
        ttl_secs: float = 120.0,
        metadata: dict | None = None,
    ) -> tuple[bool, str, str]:
        async with self._lock:
            if not self._healthy:
                return False, "capital_reservation_store_unavailable", ""
            self.expire_sync()
            existing = next((row for row in self._items.values() if row.get("intent_id") == intent_id), None)
            if existing:
                return True, "existing_reservation", str(existing["reservation_id"])
            requested = max(float(margin_usdt), 0.0)
            projected = self.total_margin() + requested
            if max_total_margin_usdt >= 0 and projected > max_total_margin_usdt + 1e-9:
                return False, "temporary_insufficient_deployable_margin", ""
            reservation_id = f"RSV-{uuid.uuid4().hex[:20]}"
            now = time.time()
            self._items[reservation_id] = {
                "reservation_id": reservation_id,
                "intent_id": intent_id,
                "candidate_id": candidate_id,
                "margin_usdt": requested,
                "risk_usdt": max(float(risk_usdt), 0.0),
                "created_at": now,
                "expires_at": now + max(float(ttl_secs), 1.0),
                "ownership_state": PRE_SUBMIT_OWNERSHIP,
                "ownership_reason": "reservation_created_pre_submit",
                "ownership_updated_at": now,
                "metadata": dict(metadata or {}),
            }
            self._save()
            return True, "reserved", reservation_id

    async def adjust(self, reservation_id: str, *, margin_usdt: float, risk_usdt: float | None = None) -> bool:
        async with self._lock:
            if not self._healthy:
                return False
            row = self._items.get(reservation_id)
            if row is None:
                return False
            row["margin_usdt"] = max(float(margin_usdt), 0.0)
            if risk_usdt is not None:
                row["risk_usdt"] = max(float(risk_usdt), 0.0)
            self._save()
            return True

    async def pin(
        self,
        reservation_id: str,
        *,
        ownership_state: str = RECOVERY_OWNED,
        reason: str = "",
    ) -> bool:
        """Durably prevent TTL release once exchange/recovery ownership exists.

        This is intentionally monotonic: a post-boundary pin cannot be
        downgraded to pre-submit ownership by a retry or a stale caller.
        Explicit terminal ``release`` remains the only way to free it.
        """
        state = str(ownership_state or RECOVERY_OWNED)
        if state == PRE_SUBMIT_OWNERSHIP:
            raise ValueError("reservation_pin_cannot_downgrade_to_pre_submit")
        async with self._lock:
            if not self._healthy:
                return False
            row = self._items.get(str(reservation_id or ""))
            if row is None:
                return False
            prior = str(row.get("ownership_state", "") or "")
            if prior == PRE_SUBMIT_OWNERSHIP or prior != state:
                row["ownership_state"] = state
                row["ownership_reason"] = str(reason or state.lower())
                row["ownership_updated_at"] = time.time()
                self._save()
            return True

    def pin_sync(
        self,
        reservation_id: str,
        *,
        ownership_state: str = RECOVERY_OWNED,
        reason: str = "",
    ) -> bool:
        """Synchronous no-POST boundary pin for callback-based client APIs.

        The router invokes this from its ``before_send`` callback in the same
        event-loop turn, after all admission checks and immediately before the
        HTTP request.  No await point exists between the durable write and the
        transport boundary.
        """
        state = str(ownership_state or RECOVERY_OWNED)
        if state == PRE_SUBMIT_OWNERSHIP:
            raise ValueError("reservation_pin_cannot_downgrade_to_pre_submit")
        row = self._items.get(str(reservation_id or ""))
        if not self._healthy:
            return False
        if row is None:
            return False
        prior = str(row.get("ownership_state", "") or "")
        if prior == PRE_SUBMIT_OWNERSHIP or prior != state:
            row["ownership_state"] = state
            row["ownership_reason"] = str(reason or state.lower())
            row["ownership_updated_at"] = time.time()
            self._save()
        return True

    async def release(self, reservation_id: str) -> bool:
        async with self._lock:
            if not self._healthy:
                return False
            removed = self._items.pop(reservation_id, None) is not None
            if removed:
                self._save()
            return removed


book = CapitalReservationBook()

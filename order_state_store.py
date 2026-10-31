"""Durable order-intent and fill-event state for idempotent execution."""
from __future__ import annotations

import json
import copy
import os
import tempfile
import time
from pathlib import Path
from threading import RLock
from typing import Any


STATES = {
    "INTENT_CREATED", "CAPITAL_RESERVED", "ROUTER_VALIDATING", "PAYLOAD_READY",
    "SUBMITTING", "SUBMISSION_ATTEMPTED", "SUBMITTED", "ACKNOWLEDGED",
    "PARTIALLY_FILLED", "FILLED", "CANCEL_PENDING", "CANCELLED", "REJECTED",
    "SUBMIT_UNCERTAIN", "RECOVERY_QUERY", "EXPIRED", "FAILED_FINAL",
}
TERMINAL_STATES = {"FILLED", "CANCELLED", "REJECTED", "EXPIRED", "FAILED_FINAL"}


class OrderStateStoreUnavailable(RuntimeError):
    """Durable intent storage cannot be used as an empty in-memory ledger."""


class OrderStateStore:
    def __init__(self, path: Path | str = "data/order_intents.json"):
        self.path = Path(path)
        self._lock = RLock()
        self._intents: dict[str, dict[str, Any]] = {}
        self._healthy = True
        self._load_error = ""
        self._load()

    def health(self) -> dict[str, Any]:
        """Expose storage admission truth without ever treating corruption as empty."""
        with self._lock:
            return {
                "healthy": bool(self._healthy),
                "error": str(self._load_error or ""),
                "path": str(self.path),
            }

    def _mark_unhealthy(self, error: Exception | str) -> None:
        self._healthy = False
        self._load_error = f"{type(error).__name__}:{error}" if isinstance(error, Exception) else str(error)

    def _require_healthy(self) -> None:
        if not self._healthy:
            raise OrderStateStoreUnavailable(
                f"order_state_store_unavailable:{self._load_error or 'unknown'}"
            )

    def _load(self) -> None:
        try:
            if not self.path.exists():
                return
            raw = json.loads(self.path.read_text(encoding="utf-8"))
            if not isinstance(raw, dict) or not isinstance(raw.get("intents", {}), dict):
                raise ValueError("invalid_order_state_store_schema")
            self._intents = dict(raw.get("intents", {}))
        except Exception as exc:
            # Corrupt, unreadable, or malformed durable state is not an empty
            # lifecycle.  Keep all admission and recovery callers blocked so a
            # later save can never replace unknown exchange-owned truth.
            self._intents = {}
            self._mark_unhealthy(exc)
            return
        # Older stores used a terminal-looking ``state`` without the immutable
        # accounting marker introduced by the terminal finalizer.  Those rows
        # are already historical terminal truth; never let a delayed callback
        # turn them back into a mutable lifecycle merely because they predate
        # that marker.  Backfill only additive compatibility facts, preserving
        # the original terminal state and evidence verbatim.
        normalized = False
        for row in self._intents.values():
            if row.get("state") not in TERMINAL_STATES or row.get("terminal_finalized_at"):
                continue
            terminal_at = float(row.get("updated_at", row.get("created_at", time.time())) or time.time())
            for entry in reversed(list(row.get("history", []) or [])):
                if entry.get("state") == row.get("state"):
                    terminal_at = float(entry.get("at", terminal_at) or terminal_at)
                    break
            row["terminal_finalized_at"] = terminal_at
            row.setdefault("terminal_class", "legacy_terminal")
            row.setdefault("terminal_reason", f"legacy_terminal:{str(row.get('state', '')).lower()}")
            normalized = True
        if normalized:
            try:
                self._save()
            except Exception:
                # The source was readable and atomic replace preserves it on
                # failure.  Keep the known in-memory terminal boundary; a
                # later successful write may persist this additive backfill.
                pass

    def _save(self) -> None:
        self._require_healthy()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = json.dumps({"intents": self._intents}, indent=2, ensure_ascii=True)
        temp_path: str | None = None
        try:
            fd, temp_path = tempfile.mkstemp(
                prefix=f".{self.path.name}.", suffix=".tmp", dir=str(self.path.parent),
            )
            with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temp_path, self.path)
            temp_path = None
        except Exception:
            # Atomic replace leaves the last known durable file intact.  The
            # caller rolls back its in-memory mutation, so this is not the
            # corrupt/unreadable-store state handled by ``_load``.
            raise
        finally:
            if temp_path:
                try:
                    os.unlink(temp_path)
                except FileNotFoundError:
                    pass

    def create(self, intent: dict) -> dict:
        intent_id = str(intent["intent_id"])
        with self._lock:
            self._require_healthy()
            if intent_id in self._intents:
                return dict(self._intents[intent_id])
            row = dict(intent)
            row.setdefault("state", "INTENT_CREATED")
            row.setdefault("created_at", time.time())
            row.setdefault("updated_at", row["created_at"])
            row.setdefault("fills", [])
            row.setdefault("history", [{"state": row["state"], "at": row["created_at"]}])
            self._intents[intent_id] = row
            try:
                self._save()
            except Exception:
                self._intents.pop(intent_id, None)
                raise
            return dict(row)

    def transition(self, intent_id: str, state: str, **fields: Any) -> dict:
        if state not in STATES:
            raise ValueError(f"invalid order state: {state}")
        with self._lock:
            self._require_healthy()
            row = self._intents[intent_id]
            # Terminal finalization is an accounting boundary.  A delayed
            # callback, cleanup retry, or restart must never rewrite the
            # exchange truth, its taxonomy, or its evidence after that
            # boundary.  Returning the original row also makes ordinary
            # transition callers safely idempotent.
            if row.get("terminal_finalized_at"):
                return dict(row)
            if state in TERMINAL_STATES:
                # Compatibility bridge for older lifecycle callers: they
                # cannot create an unclassified terminal-looking row.  The
                # store still commits through the one immutable finalizer and
                # returns its canonical row.  Router paths supply the precise
                # taxonomy; this conservative fallback keeps legacy/manual
                # recovery facts durable while they are being reconciled.
                terminal_class = str(fields.pop("terminal_class", "reconciliation_terminal"))
                terminal_reason = str(
                    fields.pop("terminal_reason", fields.pop("rejection_reason", f"terminal_transition:{state.lower()}"))
                )
                finalized, _ = self.finalize_terminal(
                    intent_id, state, terminal_class=terminal_class,
                    terminal_reason=terminal_reason, **fields,
                )
                return finalized
            before = copy.deepcopy(row)
            now = time.time()
            row.update(fields)
            row["state"] = state
            row["updated_at"] = now
            row.setdefault("history", []).append({"state": state, "at": now})
            try:
                self._save()
            except Exception:
                self._intents[intent_id] = before
                raise
            return dict(row)

    def finalize_terminal(
        self,
        intent_id: str,
        state: str,
        *,
        terminal_class: str,
        terminal_reason: str,
        **fields: Any,
    ) -> tuple[dict, bool]:
        """Persist one immutable terminal outcome for an intent.

        A terminal outcome is an accounting fact, not merely a state change.
        Retried cleanup and recovery callers therefore receive the original row
        without appending another terminal history entry or audit event.
        """
        if state not in TERMINAL_STATES:
            raise ValueError(f"terminal state required, got: {state}")
        with self._lock:
            self._require_healthy()
            row = self._intents[intent_id]
            if row.get("terminal_finalized_at"):
                return dict(row), False
            before = copy.deepcopy(row)
            now = time.time()
            row.update(fields)
            row.update({
                "state": state,
                "terminal_class": str(terminal_class),
                "terminal_reason": str(terminal_reason),
                "terminal_finalized_at": now,
                "updated_at": now,
            })
            row.setdefault("history", []).append({
                "state": state,
                "at": now,
                "terminal_class": str(terminal_class),
                "terminal_reason": str(terminal_reason),
            })
            try:
                self._save()
            except Exception:
                self._intents[intent_id] = before
                raise
            return dict(row), True

    def mark_terminal_cleanup(
        self,
        intent_id: str,
        *,
        status: str,
        error: str = "",
    ) -> dict:
        """Persist retryable post-terminal resource cleanup independently.

        Final lifecycle truth is immutable.  Reservation release is a separate
        financial-resource action, so a failed release must remain visible and
        retryable without producing another terminal transition/history row.
        """
        with self._lock:
            self._require_healthy()
            row = self._intents[intent_id]
            if not row.get("terminal_finalized_at"):
                raise ValueError("terminal cleanup requires a finalized intent")
            before = copy.deepcopy(row)
            row["terminal_cleanup_status"] = str(status)
            row["terminal_cleanup_error"] = str(error or "")
            row["terminal_cleanup_updated_at"] = time.time()
            try:
                self._save()
            except Exception:
                self._intents[intent_id] = before
                raise
            return dict(row)

    def mark_terminal_telemetry(self, intent_id: str, *, status: str, error: str = "") -> dict:
        """Update delivery state for the immutable terminal audit outbox."""
        with self._lock:
            self._require_healthy()
            row = self._intents[intent_id]
            if not row.get("terminal_finalized_at"):
                raise ValueError("terminal telemetry requires a finalized intent")
            before = copy.deepcopy(row)
            row["terminal_telemetry_status"] = str(status)
            row["terminal_telemetry_error"] = str(error or "")
            row["terminal_telemetry_updated_at"] = time.time()
            try:
                self._save()
            except Exception:
                self._intents[intent_id] = before
                raise
            return dict(row)

    def prepare_cancel_boundary(
        self, intent_id: str, *, order_id: str = "", client_order_id: str = "",
    ) -> tuple[dict, bool]:
        """Persist one cancellation mutation boundary before the cancel POST.

        Unlike an ordinary CANCEL_PENDING state, this durable marker means a
        physical cancellation request may already have reached the exchange.
        It is therefore an idempotency boundary, not a request to retry.
        """
        with self._lock:
            self._require_healthy()
            row = self._intents[intent_id]
            if row.get("terminal_finalized_at"):
                return dict(row), False
            existing = dict(row.get("cancel_boundary", {}) or {})
            if existing:
                if (
                    order_id and existing.get("order_id")
                    and str(existing["order_id"]) != str(order_id)
                ) or (
                    client_order_id and existing.get("client_order_id")
                    and str(existing["client_order_id"]) != str(client_order_id)
                ):
                    raise ValueError("conflicting_cancel_boundary_identity")
                return dict(row), False
            before = copy.deepcopy(row)
            now = time.time()
            row["cancel_boundary"] = {
                "state": "POST_ATTEMPTED",
                "order_id": str(order_id or row.get("exchange_order_id", "") or ""),
                "client_order_id": str(client_order_id or row.get("client_order_id", "") or ""),
                "at": now,
            }
            row["state"] = "CANCEL_PENDING"
            row["updated_at"] = now
            row.setdefault("history", []).append({"state": "CANCEL_PENDING", "at": now, "cancel_boundary": True})
            try:
                self._save()
            except Exception:
                self._intents[intent_id] = before
                raise
            return dict(row), True

    def prepare_risk_slot_operation(self, intent_id: str, *, operation_id: str, action: str) -> dict:
        """Durably create one router-owned risk-slot operation before its sink.

        The operation ID is deterministic from the execution intent.  The
        router may safely retry a ``pending`` operation after a process/store
        recreation because the risk sink deduplicates the same identity.
        """
        operation_id = str(operation_id or "")
        action = str(action or "").lower()
        if not operation_id or action not in {"open", "close"}:
            raise ValueError("valid risk slot operation identity and action required")
        with self._lock:
            self._require_healthy()
            row = self._intents[intent_id]
            before = copy.deepcopy(row)
            operations = copy.deepcopy(dict(row.get("risk_slot_operations", {}) or {}))
            existing = dict(operations.get(action, {}) or {})
            if existing:
                if existing.get("operation_id") != operation_id:
                    raise ValueError("conflicting risk slot operation identity")
                return dict(row)
            operations[action] = {
                "operation_id": operation_id,
                "action": action,
                "status": "pending",
                "error": "",
                "created_at": time.time(),
            }
            row["risk_slot_operations"] = operations
            # Compatibility evidence for previously persisted lifecycle rows
            # and existing operational diagnostics.
            row["risk_slot_status"] = "pending"
            row["risk_slot_error"] = ""
            row["risk_slot_updated_at"] = time.time()
            try:
                self._save()
            except Exception:
                self._intents[intent_id] = before
                raise
            return dict(row)

    def mark_risk_slot_operation(
        self, intent_id: str, *, operation_id: str, action: str, status: str, error: str = "",
    ) -> dict:
        """Acknowledge an already durable risk-slot operation independently."""
        operation_id = str(operation_id or "")
        action = str(action or "").lower()
        if action not in {"open", "close"}:
            raise ValueError("valid risk slot action required")
        with self._lock:
            self._require_healthy()
            row = self._intents[intent_id]
            before = copy.deepcopy(row)
            operations = copy.deepcopy(dict(row.get("risk_slot_operations", {}) or {}))
            operation = dict(operations.get(action, {}) or {})
            if operation.get("operation_id") != operation_id:
                raise ValueError("risk slot operation is missing or conflicts")
            operation["status"] = str(status)
            operation["error"] = str(error or "")
            operation["updated_at"] = time.time()
            operations[action] = operation
            row["risk_slot_operations"] = operations
            row["risk_slot_status"] = str(status)
            row["risk_slot_error"] = str(error or "")
            row["risk_slot_updated_at"] = time.time()
            try:
                self._save()
            except Exception:
                self._intents[intent_id] = before
                raise
            return dict(row)

    def mark_risk_slot(self, intent_id: str, *, status: str, error: str = "") -> dict:
        """Compatibility bridge for legacy rows with one close operation."""
        row = self.get(intent_id) or {}
        operation_id = str(row.get("risk_slot_operation_id", "") or f"risk-close:{intent_id}")
        if not dict(row.get("risk_slot_operations", {}) or {}).get("close"):
            self.prepare_risk_slot_operation(intent_id, operation_id=operation_id, action="close")
        return self.mark_risk_slot_operation(
            intent_id, operation_id=operation_id, action="close", status=status, error=error,
        )

    def add_fill(self, intent_id: str, fill: dict) -> dict:
        with self._lock:
            self._require_healthy()
            row = self._intents[intent_id]
            before = copy.deepcopy(row)
            fill_id = str(fill.get("fillId", fill.get("tradeId", fill.get("fill_id", ""))) or "")
            fills = row.setdefault("fills", [])
            if fill_id and any(str(existing.get("fillId", existing.get("tradeId", existing.get("fill_id", "")))) == fill_id for existing in fills):
                return dict(row)
            fills.append(dict(fill))
            total_qty = sum(float(item.get("fillSz", item.get("qty", 0.0)) or 0.0) for item in fills)
            weighted = sum(
                float(item.get("fillSz", item.get("qty", 0.0)) or 0.0)
                * float(item.get("fillPx", item.get("price", 0.0)) or 0.0)
                for item in fills
            )
            now = time.time()
            row["filled_contracts"] = total_qty
            row["average_fill_price"] = weighted / total_qty if total_qty > 0 else 0.0
            row.setdefault("first_fill_at", now)
            row["last_fill_at"] = now
            row["updated_at"] = now
            try:
                self._save()
            except Exception:
                self._intents[intent_id] = before
                raise
            return dict(row)

    def get(self, intent_id: str) -> dict | None:
        with self._lock:
            self._require_healthy()
            row = self._intents.get(intent_id)
            return dict(row) if row else None

    def find_by_client_order_id(self, client_order_id: str) -> dict | None:
        with self._lock:
            self._require_healthy()
            for row in self._intents.values():
                if row.get("client_order_id") == client_order_id:
                    return dict(row)
        return None

    def find_active(self, **criteria: Any) -> dict | None:
        """Return the first non-terminal intent matching durable fields."""
        with self._lock:
            self._require_healthy()
            for row in self._intents.values():
                if row.get("state") in TERMINAL_STATES:
                    continue
                if all(row.get(key) == value for key, value in criteria.items()):
                    return dict(row)
        return None

    def unresolved(self) -> list[dict]:
        with self._lock:
            self._require_healthy()
            return [dict(row) for row in self._intents.values() if row.get("state") not in TERMINAL_STATES]

    def intent_prefix(self, prefix: str) -> list[dict]:
        """Return durable lineage candidates, including terminal boundary evidence."""
        with self._lock:
            self._require_healthy()
            return [dict(row) for intent_id, row in self._intents.items() if str(intent_id).startswith(prefix)]

    def terminal_cleanup_pending(self) -> list[dict]:
        with self._lock:
            self._require_healthy()
            return [dict(row) for row in self._intents.values()
                    if row.get("terminal_finalized_at") and row.get("terminal_cleanup_status") in {"pending", "failed"}]

    def terminal_telemetry_pending(self) -> list[dict]:
        with self._lock:
            self._require_healthy()
            return [dict(row) for row in self._intents.values()
                    if row.get("terminal_finalized_at")
                    and row.get("terminal_telemetry_event")
                    and row.get("terminal_telemetry_status") in {"pending", "failed"}]

    def risk_slot_pending(self) -> list[dict]:
        with self._lock:
            self._require_healthy()
            return [
                dict(row) for row in self._intents.values()
                if any(
                    str(dict(operation or {}).get("status", "")) in {"pending", "failed"}
                    for operation in dict(row.get("risk_slot_operations", {}) or {}).values()
                )
                or (
                    not row.get("risk_slot_operations")
                    and row.get("risk_slot_status") in {"pending", "failed"}
                )
            ]

    def snapshot(self) -> dict:
        self._require_healthy()
        unresolved = self.unresolved()
        return {"total": len(self._intents), "unresolved": len(unresolved), "states": {state: sum(1 for row in self._intents.values() if row.get("state") == state) for state in STATES}}


store = OrderStateStore()

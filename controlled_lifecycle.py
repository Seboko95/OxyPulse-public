"""Persisted one-shot production lifecycle authorization.

This gate narrows an explicit operator authorization to one entry intent. It
does not arm normal live trading and it never gates protective or close orders.
"""
from __future__ import annotations

from datetime import datetime, timezone
import json
import os
from pathlib import Path
from threading import RLock
import time
from typing import Any


class ControlledLifecycleGate:
    def __init__(self, path: Path | str = "data/controlled_lifecycle.json"):
        self.path = Path(path)
        self._lock = RLock()

    def _pytest_default_isolated(self) -> bool:
        return "PYTEST_CURRENT_TEST" in os.environ and self.path == Path("data/controlled_lifecycle.json")

    def _load(self) -> dict[str, Any]:
        try:
            if self.path.exists():
                value = json.loads(self.path.read_text(encoding="utf-8"))
                return dict(value) if isinstance(value, dict) else {}
        except Exception:
            return {"status": "INVALID", "reason": "state_unreadable"}
        return {}

    def _save(self, value: dict[str, Any]) -> dict[str, Any]:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps(value, indent=2, sort_keys=True), encoding="utf-8")
        return dict(value)

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            value = self._load()
            if value.get("status") == "AUTHORIZED" and float(value.get("expires_at", 0.0) or 0.0) <= time.time():
                value.update(status="EXPIRED", reason="authorization_window_expired", updated_at=time.time())
                self._save(value)
            return dict(value)

    def authorize(
        self,
        *,
        authorization_id: str,
        expires_in_seconds: float,
        account_snapshot: dict[str, Any],
    ) -> dict[str, Any]:
        now = time.time()
        with self._lock:
            return self._save({
                "authorization_id": str(authorization_id),
                "scope": "one_production_lifecycle",
                "maximum_new_positions": 1,
                "maximum_submission_attempts": 1,
                "status": "AUTHORIZED",
                "authorized_at": now,
                "authorized_at_iso": datetime.now(timezone.utc).isoformat(),
                "expires_at": now + max(float(expires_in_seconds), 1.0),
                "account_snapshot": dict(account_snapshot),
                "claimed_intent_id": "",
                "submission_attempts": 0,
                "updated_at": now,
            })

    def can_auto_arm(self) -> bool:
        if self._pytest_default_isolated():
            return False
        value = self.snapshot()
        return value.get("scope") == "one_production_lifecycle" and value.get("status") == "AUTHORIZED"

    def enforced(self) -> bool:
        if self._pytest_default_isolated():
            return False
        value = self.snapshot()
        return value.get("scope") == "one_production_lifecycle" and value.get("status") in {
            "AUTHORIZED", "CLAIMED", "SUBMITTED", "MANAGING", "COMPLETED", "EXPIRED", "INVALID"
        }

    def retire(self, reason: str) -> dict[str, Any]:
        """Preserve a finished one-shot record while removing it from active gating."""
        if self._pytest_default_isolated():
            return {}
        with self._lock:
            value = self._load()
            if value.get("scope") == "one_production_lifecycle" and value.get("status") in {
                "COMPLETED", "EXPIRED", "INVALID",
            }:
                value.update(
                    scope="historical_one_production_lifecycle",
                    status="RETIRED",
                    retirement_reason=str(reason),
                    retired_at=time.time(),
                    updated_at=time.time(),
                )
                return self._save(value)
            return dict(value)

    def claim(self, intent_id: str, request: dict[str, Any]) -> tuple[bool, str]:
        with self._lock:
            value = self.snapshot()
            if value.get("scope") != "one_production_lifecycle":
                return True, "normal_live_authorization"
            existing = str(value.get("claimed_intent_id", "") or "")
            if existing == str(intent_id) and value.get("status") in {"CLAIMED", "SUBMITTED", "MANAGING"}:
                return True, "existing_controlled_intent"
            if value.get("status") != "AUTHORIZED":
                return False, f"controlled_lifecycle_{str(value.get('status', 'inactive')).lower()}"
            value.update(
                status="CLAIMED",
                claimed_intent_id=str(intent_id),
                claimed_at=time.time(),
                request=dict(request),
                submission_attempts=1,
                updated_at=time.time(),
            )
            self._save(value)
            return True, "controlled_lifecycle_claimed"

    def mark_submission(self, *, intent_id: str, order_id: str = "", state: str = "", reason: str = "") -> dict[str, Any]:
        with self._lock:
            value = self._load()
            if str(value.get("claimed_intent_id", "")) != str(intent_id):
                return dict(value)
            value.update(
                status="SUBMITTED" if order_id else "MANAGING",
                exchange_order_id=str(order_id or ""),
                submission_state=str(state or ""),
                submission_reason=str(reason or ""),
                submission_recorded_at=time.time(),
                updated_at=time.time(),
            )
            return self._save(value)

    def complete(self, *, reason: str, evidence: dict[str, Any] | None = None) -> dict[str, Any]:
        with self._lock:
            value = self._load()
            value.update(
                status="COMPLETED",
                completion_reason=str(reason),
                completion_evidence=dict(evidence or {}),
                completed_at=time.time(),
                updated_at=time.time(),
            )
            return self._save(value)


gate = ControlledLifecycleGate()

"""Persistent fail-closed live arming state machine."""
from __future__ import annotations

import json
import os
import tempfile
import time
from pathlib import Path
from threading import RLock

STATES = {
    "LIVE_DISABLED", "LIVE_PREFLIGHT_FAILED", "LIVE_READY_UNARMED",
    "LIVE_ARMED", "LIVE_HALTED", "LIVE_EMERGENCY_STOP",
}


class LiveStateMachine:
    def __init__(self, path: Path | str = "data/live_state.json"):
        self.path = Path(path)
        self._lock = RLock()
        self._data = {"state": "LIVE_DISABLED", "armed": False, "updated_at": time.time(), "reason": "initial"}
        self._load_fail_closed()

    def _paper(self) -> bool:
        configured = os.getenv("PAPER_TRADING")
        if configured is None:
            try:
                for line in Path(".env").read_text(encoding="utf-8").splitlines():
                    if line.strip().startswith("PAPER_TRADING="):
                        configured = line.split("=", 1)[1].strip().strip("\"'")
                        break
            except OSError:
                configured = None
        return str(configured if configured is not None else "true").strip().lower() == "true"

    def _load_fail_closed(self) -> None:
        try:
            if self.path.exists():
                loaded = json.loads(self.path.read_text(encoding="utf-8"))
                previous = str(loaded.get("state", "LIVE_DISABLED"))
                if self._paper():
                    self._data = {"state": "LIVE_DISABLED", "armed": False, "updated_at": time.time(), "reason": "paper_mode"}
                else:
                    self._data = {"state": "LIVE_READY_UNARMED", "armed": False, "updated_at": time.time(), "reason": f"restart_from_{previous}"}
                # Fail closed in this process without rewriting shared state.
                # Test tools and read-only subprocesses import this module too;
                # they must not disarm the authoritative running process on disk.
        except Exception:
            self._save()

    def _save(self, data: dict | None = None) -> None:
        """Persist state atomically so readers never observe a torn state file."""
        payload = json.dumps(data if data is not None else self._data, indent=2, ensure_ascii=True)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd, temp_name = tempfile.mkstemp(
            prefix=f".{self.path.name}.", suffix=".tmp", dir=str(self.path.parent)
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8", newline="") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temp_name, self.path)
            try:
                dir_fd = os.open(str(self.path.parent), os.O_RDONLY)
                try:
                    os.fsync(dir_fd)
                finally:
                    os.close(dir_fd)
            except (AttributeError, OSError):
                pass
        finally:
            try:
                os.unlink(temp_name)
            except OSError:
                pass

    def transition(self, state: str, reason: str) -> dict:
        if state not in STATES:
            raise ValueError(state)
        with self._lock:
            next_data = {
                "state": state,
                "armed": state == "LIVE_ARMED",
                "updated_at": time.time(),
                "reason": reason,
            }
            self._save(next_data)
            self._data = next_data
            return dict(next_data)

    def record_preflight(self, ok: bool, reason: str = "") -> dict:
        if self._paper():
            return self.transition("LIVE_DISABLED", "paper_mode")
        return self.transition("LIVE_READY_UNARMED" if ok else "LIVE_PREFLIGHT_FAILED", reason or ("preflight_passed" if ok else "preflight_failed"))

    def arm(self, preflight: dict) -> tuple[bool, str]:
        if self._paper():
            self.transition("LIVE_DISABLED", "paper_mode_cannot_arm")
            return False, "paper_mode_cannot_arm"
        if not bool(preflight.get("ok")):
            self.transition("LIVE_PREFLIGHT_FAILED", str(preflight.get("reason", "preflight_failed")))
            return False, "preflight_failed"
        required = [
            "account_equity", "available_balance", "instrument_metadata", "metadata_freshness",
            "quantity_conversion", "capital_allocator", "execution_router", "protection_manager",
            "pnl_reconciler", "leverage_set_and_readback", "no_unresolved_orders",
            "no_unknown_exchange_positions", "no_unknown_local_exposure", "no_orphan_algo_orders",
        ]
        missing = [key for key in required if not bool(preflight.get("checks", {}).get(key, {}).get("ok"))]
        if missing:
            self.transition("LIVE_PREFLIGHT_FAILED", "missing:" + ",".join(missing))
            return False, "preflight_services_incomplete"
        self.transition("LIVE_ARMED", "explicit_operator_arm")
        return True, "armed"

    def disarm(self, reason: str = "operator_disarm") -> dict:
        return self.transition("LIVE_DISABLED" if self._paper() else "LIVE_READY_UNARMED", reason)

    def halt(self, reason: str, emergency: bool = False) -> dict:
        return self.transition("LIVE_EMERGENCY_STOP" if emergency else "LIVE_HALTED", reason)

    def can_submit_live(self) -> tuple[bool, str]:
        if self._paper():
            return False, "paper_mode"
        if self._data.get("state") != "LIVE_ARMED" or not self._data.get("armed"):
            return False, f"live_not_armed:{self._data.get('state')}"
        return True, "OK"

    def snapshot(self) -> dict:
        return dict(self._data)


state = LiveStateMachine()

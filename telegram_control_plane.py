"""Low-overhead telemetry for the Telegram operator control plane."""
from __future__ import annotations

import asyncio
import json
import math
import time
import uuid
from collections import Counter, deque
from datetime import datetime, timezone
from pathlib import Path
from threading import RLock
from typing import Any


def _iso(ts: float | None = None) -> str:
    return datetime.fromtimestamp(ts or time.time(), timezone.utc).isoformat()


def _percentile(values: list[float], percentile: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    rank = max(0, min(len(ordered) - 1, math.ceil(percentile * len(ordered)) - 1))
    return round(float(ordered[rank]), 3)


class TelegramControlPlane:
    def __init__(self, path: Path | str = "data/telegram_control_plane.json") -> None:
        self.path = Path(path)
        self._lock = RLock()
        self._pending: dict[str, dict[str, Any]] = {}
        self._recent: deque[dict[str, Any]] = deque(maxlen=500)
        self._counters: Counter[str] = Counter()
        self._last_update_received_at = ""
        self._last_response_sent_at = ""
        self._last_api_error = ""
        self._last_api_error_at = ""
        self._process_started_at = _iso()
        self._loop_lag_samples: deque[float] = deque(maxlen=600)
        self._flush_task: asyncio.Task | None = None
        self._loop_probe_task: asyncio.Task | None = None
        self._dirty = False
        self._load()

    def _load(self) -> None:
        try:
            if not self.path.exists():
                return
            payload = json.loads(self.path.read_text(encoding="utf-8"))
            self._recent.extend(payload.get("recent", []))
            self._counters.update(payload.get("counters", {}))
            self._last_update_received_at = str(payload.get("last_update_received_at", "") or "")
            self._last_response_sent_at = str(payload.get("last_response_sent_at", "") or "")
            self._last_api_error = str(payload.get("last_api_error", "") or "")
            self._last_api_error_at = str(payload.get("last_api_error_at", "") or "")
        except Exception:
            # Telemetry corruption must never block Telegram or trading startup.
            self._counters["telegram_telemetry_load_errors"] += 1

    def begin(self, kind: str, action: str, *, update_id: int | str | None = None) -> str:
        now = time.time()
        token = uuid.uuid4().hex
        row = {
            "token": token,
            "kind": str(kind or "command"),
            "action": str(action or "unknown"),
            "update_id": update_id,
            "received_at": _iso(now),
            "received_epoch": now,
            "dispatch_at": "",
            "backend_complete_at": "",
            "send_at": "",
            "total_latency_ms": None,
            "backend_latency_ms": None,
            "telegram_send_latency_ms": None,
            "ok": None,
            "error": "",
        }
        with self._lock:
            self._pending[token] = row
            self._last_update_received_at = row["received_at"]
            self._counters[f"telegram_{row['kind']}s_total"] += 1
            self._dirty = True
        return token

    def dispatch(self, token: str) -> None:
        with self._lock:
            row = self._pending.get(token)
            if row:
                row["dispatch_epoch"] = time.time()
                row["dispatch_at"] = _iso(row["dispatch_epoch"])

    def backend_complete(self, token: str) -> None:
        with self._lock:
            row = self._pending.get(token)
            if row and not row.get("backend_complete_epoch"):
                row["backend_complete_epoch"] = time.time()
                row["backend_complete_at"] = _iso(row["backend_complete_epoch"])

    def is_pending(self, token: str) -> bool:
        with self._lock:
            return token in self._pending

    def sent(self, token: str) -> None:
        with self._lock:
            row = self._pending.pop(token, None)
            if not row:
                return
            now = time.time()
            backend = float(row.get("backend_complete_epoch") or now)
            dispatch = float(row.get("dispatch_epoch") or row["received_epoch"])
            row["send_at"] = _iso(now)
            row["total_latency_ms"] = round((now - float(row["received_epoch"])) * 1000.0, 3)
            row["backend_latency_ms"] = round((backend - dispatch) * 1000.0, 3)
            row["telegram_send_latency_ms"] = round((now - backend) * 1000.0, 3)
            row["ok"] = True
            for key in ("received_epoch", "dispatch_epoch", "backend_complete_epoch"):
                row.pop(key, None)
            self._recent.append(row)
            self._last_response_sent_at = row["send_at"]
            self._dirty = True

    def error(self, token: str, exc: BaseException | str, *, api_failure: bool = False) -> None:
        with self._lock:
            row = self._pending.pop(token, None)
            now = time.time()
            message = f"{type(exc).__name__}: {exc}" if isinstance(exc, BaseException) else str(exc)
            if row:
                row["send_at"] = _iso(now)
                row["total_latency_ms"] = round((now - float(row["received_epoch"])) * 1000.0, 3)
                row["ok"] = False
                row["error"] = message[:500]
                for key in ("received_epoch", "dispatch_epoch", "backend_complete_epoch"):
                    row.pop(key, None)
                self._recent.append(row)
            self._counters["telegram_errors_total"] += 1
            if api_failure:
                self._counters["telegram_api_failures_total"] += 1
                self._last_api_error = message[:500]
                self._last_api_error_at = _iso(now)
            self._dirty = True

    def count(self, name: str, amount: int = 1) -> None:
        with self._lock:
            self._counters[str(name)] += int(amount)
            self._dirty = True

    def observe_loop_lag(self, lag_ms: float) -> None:
        with self._lock:
            self._loop_lag_samples.append(max(float(lag_ms), 0.0))
            # Persist current-process health even when no operator command arrives.
            self._dirty = True

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            now = time.time()
            pending = list(self._pending.values())
            recent = list(self._recent)
            by_action: dict[str, dict[str, float | int]] = {}
            for action in sorted({str(row.get("action", "unknown")) for row in recent}):
                rows = [row for row in recent if str(row.get("action", "unknown")) == action and row.get("ok")]
                total = [float(row.get("total_latency_ms", 0.0) or 0.0) for row in rows]
                backend = [float(row.get("backend_latency_ms", 0.0) or 0.0) for row in rows]
                send = [float(row.get("telegram_send_latency_ms", 0.0) or 0.0) for row in rows]
                by_action[action] = {
                    "count": len(rows),
                    "median_ms": _percentile(total, 0.50),
                    "p95_ms": _percentile(total, 0.95),
                    "max_ms": round(max(total, default=0.0), 3),
                    "backend_p95_ms": _percentile(backend, 0.95),
                    "send_p95_ms": _percentile(send, 0.95),
                }
            oldest_age = max((now - float(row.get("received_epoch", now))) for row in pending) if pending else 0.0
            loop_lag = list(self._loop_lag_samples)
            return {
                "generated_at": _iso(now),
                "process_started_at": self._process_started_at,
                "last_update_received_at": self._last_update_received_at,
                "last_response_sent_at": self._last_response_sent_at,
                "last_api_error": self._last_api_error,
                "last_api_error_at": self._last_api_error_at,
                "handler_backlog": len(pending),
                "oldest_pending_command_age_sec": round(oldest_age, 3),
                "counters": dict(self._counters),
                "latency_by_action": by_action,
                "event_loop_lag_ms": {
                    "samples": len(loop_lag),
                    "median": _percentile(loop_lag, 0.50),
                    "p95": _percentile(loop_lag, 0.95),
                    "max": round(max(loop_lag, default=0.0), 3),
                },
                "recent": recent[-100:],
            }

    def _save(self) -> None:
        payload = self.snapshot()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(self.path.suffix + ".tmp")
        tmp.write_text(json.dumps(payload, indent=2, ensure_ascii=True), encoding="utf-8")
        tmp.replace(self.path)
        with self._lock:
            self._dirty = False

    async def start(self) -> None:
        # Process-local backlog always starts empty. Force an early rewrite so a
        # prior process's persisted backlog/age cannot be displayed as current.
        with self._lock:
            self._dirty = True
        if not self._flush_task or self._flush_task.done():
            async def _flush_worker() -> None:
                while True:
                    await asyncio.sleep(2.0)
                    if self._dirty:
                        await asyncio.to_thread(self._save)
            self._flush_task = asyncio.create_task(_flush_worker(), name="telegram-telemetry-flush")
        if not self._loop_probe_task or self._loop_probe_task.done():
            async def _loop_probe() -> None:
                interval = 0.5
                expected = time.monotonic() + interval
                while True:
                    await asyncio.sleep(interval)
                    now = time.monotonic()
                    self.observe_loop_lag((now - expected) * 1000.0)
                    expected = now + interval
            self._loop_probe_task = asyncio.create_task(_loop_probe(), name="telegram-loop-lag-probe")

    async def stop(self) -> None:
        for task in (self._flush_task, self._loop_probe_task):
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        self._flush_task = None
        self._loop_probe_task = None
        if self._dirty:
            await asyncio.to_thread(self._save)


control_plane = TelegramControlPlane()

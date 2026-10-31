"""Own live execution tasks independently from engine scan lifetimes."""
from __future__ import annotations

import asyncio
import time
from typing import Any, Coroutine

import execution_flow_telemetry as telemetry


class ExecutionSupervisor:
    def __init__(self) -> None:
        self._tasks: dict[str, asyncio.Task] = {}
        self._metadata: dict[str, dict[str, Any]] = {}

    def handoff(self, key: str, coroutine: Coroutine, **metadata: Any) -> bool:
        existing = self._tasks.get(key)
        if existing and not existing.done():
            coroutine.close()
            telemetry.record("HANDOFF_REJECTED", handoff_key=key, reason="duplicate_active_handoff", **metadata)
            return False

        async def _runner() -> None:
            telemetry.record("EXECUTION_WORKER_STARTED", handoff_key=key, **metadata)
            try:
                await coroutine
                telemetry.record("EXECUTION_WORKER_COMPLETED", handoff_key=key, **metadata)
            except asyncio.CancelledError:
                telemetry.record("EXECUTION_WORKER_CANCELLED", handoff_key=key, reason="runtime_shutdown", **metadata)
                raise
            except Exception as exc:
                telemetry.record(
                    "EXECUTION_WORKER_FAILED", handoff_key=key,
                    reason=f"{type(exc).__name__}:{exc}", **metadata,
                )
            finally:
                self._tasks.pop(key, None)
                self._metadata.pop(key, None)

        self._metadata[key] = {**metadata, "started_at": time.time()}
        self._tasks[key] = asyncio.create_task(_runner(), name=f"live-execution-{key}")
        telemetry.record("EXECUTION_HANDOFF", handoff_key=key, **metadata)
        return True

    def active_coins(self, engine: str = "") -> set[str]:
        engine_name = str(engine or "").upper()
        return {
            str(row.get("coin", "") or "").upper()
            for row in self._metadata.values()
            if row.get("coin") and (not engine_name or str(row.get("engine", "")).upper() == engine_name)
        }

    def snapshot(self) -> dict[str, Any]:
        active = []
        now = time.time()
        for key, row in self._metadata.items():
            active.append({"handoff_key": key, **row, "age_sec": round(now - float(row.get("started_at", now)), 3)})
        return {"active_count": len(active), "active": active}

    async def wait_all(self) -> None:
        tasks = [task for task in self._tasks.values() if not task.done()]
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)


supervisor = ExecutionSupervisor()

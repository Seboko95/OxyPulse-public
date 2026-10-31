"""Authoritative read models used by Telegram production panels."""
from __future__ import annotations

import asyncio
import copy
import concurrent.futures
import functools
import json
import logging
import os
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


logger = logging.getLogger("OxyPulse.TelegramBackend")


_analytical_cache: dict[str, dict[str, Any]] = {}
_analytical_locks: dict[str, asyncio.Lock] = {}
_operational_exchange_cache: dict[str, Any] = {}
_OPERATIONAL_CACHE_PATH = Path("data/telegram_operational_snapshot.json")
_operational_refresh_task: asyncio.Task | None = None
_operational_cache_write_lock = asyncio.Lock()
_operational_cache_write_task: asyncio.Task | None = None
_operational_cache_write_pending: dict[str, Any] | None = None
_operational_cache_write_failures = 0
_operational_cache_executor = ThreadPoolExecutor(
    max_workers=1,
    thread_name_prefix="oxypulse-diagnostic-cache",
)
_operational_cache_executor_closed = False
_PROCESS_STARTED_EPOCH = time.time()


class ControlWorkQueueFull(RuntimeError):
    """Read-only control work could not be admitted without unbounded queuing."""


class ControlWorkDispatcherClosed(RuntimeError):
    """A queued read-only control job was resolved during dispatcher shutdown."""


class ControlWorkDispatcher:
    """Bounded, priority-aware executor for synchronous Telegram control work.

    Only the fixed dispatcher workers submit to the explicit thread pool.  This
    keeps the executor's internal queue bounded in practice as well as at the
    async admission boundary.
    """

    def __init__(self, *, max_workers: int = 4, max_pending: int = 12):
        self.max_workers = max(1, int(max_workers))
        self.max_pending = max(self.max_workers, int(max_pending))
        self._queue: asyncio.PriorityQueue = asyncio.PriorityQueue(maxsize=self.max_pending)
        self._executor = ThreadPoolExecutor(
            max_workers=self.max_workers,
            thread_name_prefix="oxypulse-control",
        )
        self._workers: list[asyncio.Task] = []
        self._inflight: dict[str, asyncio.Future] = {}
        self._running_futures: set[asyncio.Future] = set()
        self._sequence = 0
        self._accepting = True
        self._state = "OPEN"
        self._active = 0
        self._shutdown_task: asyncio.Task | None = None
        self._metrics = {
            "submitted_jobs": 0,
            "coalesced_jobs": 0,
            "started_jobs": 0,
            "completed_jobs": 0,
            "rejected_or_fallback_jobs": 0,
            "active_workers": 0,
            "queue_wait_sec": [],
            "execution_time_sec": [],
        }

    def _ensure_workers(self) -> None:
        if self._workers:
            return
        self._workers = [
            asyncio.create_task(self._worker(), name=f"telegram-control-worker-{i}")
            for i in range(self.max_workers)
        ]

    async def _worker(self) -> None:
        loop = asyncio.get_running_loop()
        while True:
            _priority, _sequence, enqueued_at, future, func, args, kwargs, key = await self._queue.get()
            if future is None:
                self._queue.task_done()
                return
            if future.cancelled():
                self._queue.task_done()
                if key and self._inflight.get(key) is future:
                    self._inflight.pop(key, None)
                continue
            self._active += 1
            self._running_futures.add(future)
            self._metrics["active_workers"] = self._active
            self._metrics["started_jobs"] += 1
            self._metrics["queue_wait_sec"].append(max(time.monotonic() - enqueued_at, 0.0))
            started = time.monotonic()
            try:
                value = await loop.run_in_executor(self._executor, functools.partial(func, *args, **kwargs))
                if not future.done():
                    future.set_result(value)
                self._metrics["completed_jobs"] += 1
            except asyncio.CancelledError:
                if not future.done():
                    future.cancel()
                raise
            except Exception as exc:
                if not future.done():
                    future.set_exception(exc)
            finally:
                self._metrics["execution_time_sec"].append(max(time.monotonic() - started, 0.0))
                self._active -= 1
                self._running_futures.discard(future)
                self._metrics["active_workers"] = self._active
                if key and self._inflight.get(key) is future:
                    self._inflight.pop(key, None)
                self._queue.task_done()

    async def run(self, func, *args, priority: str = "normal", coalesce_key: str | None = None,
                  admission_timeout: float = 0.05, **kwargs):
        if not self._accepting or self._state != "OPEN":
            self._metrics["rejected_or_fallback_jobs"] += 1
            raise ControlWorkQueueFull("control work dispatcher is shutting down")
        self._ensure_workers()
        if coalesce_key:
            existing = self._inflight.get(coalesce_key)
            if existing is not None and not existing.done():
                self._metrics["coalesced_jobs"] += 1
                return await asyncio.shield(existing)
        future = asyncio.get_running_loop().create_future()
        if coalesce_key:
            self._inflight[coalesce_key] = future
        self._sequence += 1
        rank = {"critical": 0, "high": 1, "normal": 2, "background": 3}.get(str(priority).lower(), 2)
        item = (rank, self._sequence, time.monotonic(), future, func, args, kwargs, coalesce_key)
        try:
            await asyncio.wait_for(self._queue.put(item), timeout=max(float(admission_timeout), 0.0))
        except asyncio.TimeoutError:
            if coalesce_key and self._inflight.get(coalesce_key) is future:
                self._inflight.pop(coalesce_key, None)
            if not future.done():
                future.cancel()
            self._metrics["rejected_or_fallback_jobs"] += 1
            raise ControlWorkQueueFull("control work queue is full")
        except asyncio.CancelledError:
            if coalesce_key and self._inflight.get(coalesce_key) is future:
                self._inflight.pop(coalesce_key, None)
            if not future.done():
                future.cancel()
            raise
        self._metrics["submitted_jobs"] += 1
        return await asyncio.shield(future)

    def snapshot(self) -> dict[str, Any]:
        def stats(values):
            values = sorted(float(v) for v in values)
            if not values:
                return {"p50": 0.0, "p95": 0.0, "max": 0.0}
            return {
                "p50": values[(len(values) - 1) // 2],
                "p95": values[min(len(values) - 1, int(len(values) * 0.95))],
                "max": values[-1],
            }
        return {
            "max_workers": self.max_workers,
            "max_pending": self.max_pending,
            "queue_depth": self._queue.qsize(),
            "pending_jobs": self._queue.qsize(),
            "state": self._state,
            "active_workers": self._active,
            "submitted_jobs": self._metrics["submitted_jobs"],
            "coalesced_jobs": self._metrics["coalesced_jobs"],
            "started_jobs": self._metrics["started_jobs"],
            "completed_jobs": self._metrics["completed_jobs"],
            "rejected_or_fallback_jobs": self._metrics["rejected_or_fallback_jobs"],
            "queue_wait": stats(self._metrics["queue_wait_sec"]),
            "execution_time": stats(self._metrics["execution_time_sec"]),
        }

    def _drain_queued_jobs(self) -> None:
        while True:
            try:
                _priority, _sequence, _enqueued_at, future, _func, _args, _kwargs, key = self._queue.get_nowait()
            except asyncio.QueueEmpty:
                return
            self._queue.task_done()
            if key and self._inflight.get(key) is future:
                self._inflight.pop(key, None)
            if future is not None and not future.done():
                try:
                    future.set_exception(ControlWorkDispatcherClosed("control work dispatcher is closed"))
                except RuntimeError:
                    # A dispatcher can be replaced after its owning loop has
                    # already closed.  There is no waiter left to notify on
                    # that loop; draining the queue must still be harmless.
                    pass

    def _wait_for_owned_executor(self) -> concurrent.futures.Future:
        """Join the owned pool without blocking the asyncio event loop."""
        completed: concurrent.futures.Future = concurrent.futures.Future()

        def join() -> None:
            try:
                self._executor.shutdown(wait=True, cancel_futures=True)
                completed.set_result(None)
            except Exception as exc:
                completed.set_exception(exc)

        threading.Thread(
            target=join,
            name="oxypulse-control-shutdown",
            daemon=False,
        ).start()
        return completed

    async def _shutdown_impl(self) -> None:
        self._state = "CLOSING"
        self._accepting = False
        self._drain_queued_jobs()
        workers = list(self._workers)
        for _ in workers:
            self._sequence += 1
            self._queue.put_nowait((999, self._sequence, time.monotonic(), None, None, (), {}, None))
        if workers:
            await asyncio.gather(*workers, return_exceptions=True)
        await asyncio.wrap_future(self._wait_for_owned_executor())
        self._workers = []
        self._inflight.clear()
        self._active = 0
        self._metrics["active_workers"] = 0
        self._state = "CLOSED"

    async def shutdown(self) -> None:
        """Close deterministically; active jobs finish before this returns."""
        if self._state == "CLOSED":
            return
        # A caller may outlive the asyncio loop that created this dispatcher.
        # Never pass tasks owned by that closed loop to gather on a new loop.
        stale_workers = [worker for worker in self._workers if worker.get_loop().is_closed()]
        if stale_workers:
            retirement = self.retire_from_replaced_loop()
            await asyncio.wrap_future(retirement)
            return
        if self._shutdown_task is None:
            self._shutdown_task = asyncio.create_task(self._shutdown_impl(), name="telegram-control-shutdown")
        await asyncio.shield(self._shutdown_task)

    def retire_from_replaced_loop(self) -> concurrent.futures.Future:
        """Retire a dispatcher whose original asyncio loop is already gone."""
        self._state = "CLOSING"
        self._accepting = False
        self._drain_queued_jobs()
        for future in self._inflight.values():
            if future not in self._running_futures and not future.done():
                try:
                    future.cancel()
                except RuntimeError:
                    # The future belongs to the retired, already-closed loop.
                    pass
        self._inflight.clear()
        for worker in self._workers:
            worker.cancel()
        self._workers = []
        completed = self._wait_for_owned_executor()

        def mark_closed(_future) -> None:
            self._active = 0
            self._metrics["active_workers"] = 0
            self._state = "CLOSED"

        completed.add_done_callback(mark_closed)
        return completed


_control_dispatcher: ControlWorkDispatcher | None = None
_control_dispatcher_loop: asyncio.AbstractEventLoop | None = None
_control_replacement_future: concurrent.futures.Future | None = None
_control_snapshot_cache: dict[str, dict[str, Any]] = {}


async def _get_control_dispatcher() -> ControlWorkDispatcher:
    global _control_dispatcher, _control_dispatcher_loop, _control_replacement_future
    loop = asyncio.get_running_loop()
    if _control_dispatcher is None or _control_dispatcher_loop is not loop:
        old_dispatcher = _control_dispatcher
        if old_dispatcher is not None:
            retirement = _control_replacement_future
            if retirement is None:
                retirement = old_dispatcher.retire_from_replaced_loop()
                _control_replacement_future = retirement
            await asyncio.wrap_future(retirement)
            if _control_replacement_future is retirement:
                _control_replacement_future = None
            if _control_dispatcher is old_dispatcher:
                _control_dispatcher = None
                _control_dispatcher_loop = None
        if _control_dispatcher is None:
            _control_dispatcher = ControlWorkDispatcher()
            _control_dispatcher_loop = loop
    return _control_dispatcher


async def run_control_work(func, *args, priority: str = "normal", coalesce_key: str | None = None,
                           admission_timeout: float = 0.05, **kwargs):
    dispatcher = await _get_control_dispatcher()
    return await dispatcher.run(
        func, *args, priority=priority, coalesce_key=coalesce_key,
        admission_timeout=admission_timeout, **kwargs,
    )


def control_work_snapshot() -> dict[str, Any]:
    return _control_dispatcher.snapshot() if _control_dispatcher is not None else {
        "max_workers": 4, "max_pending": 12, "queue_depth": 0, "active_workers": 0,
    }


async def shutdown_control_work() -> None:
    global _control_dispatcher, _control_dispatcher_loop, _control_replacement_future
    dispatcher, _control_dispatcher = _control_dispatcher, None
    _control_dispatcher_loop = None
    _control_replacement_future = None
    if dispatcher is not None:
        await dispatcher.shutdown()


def _load_operational_cache() -> None:
    try:
        payload = json.loads(_OPERATIONAL_CACHE_PATH.read_text(encoding="utf-8"))
        if isinstance(payload, dict):
            _operational_exchange_cache.update(payload)
    except Exception:
        return


def _save_operational_cache(snapshot: dict[str, Any] | None = None) -> bool:
    """Persist an independent diagnostic snapshot with atomic replacement."""
    tmp_path: Path | None = None
    try:
        _OPERATIONAL_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
        payload = copy.deepcopy(snapshot if snapshot is not None else _operational_exchange_cache)
        encoded = json.dumps(payload, indent=2, default=str)
        tmp_path = _OPERATIONAL_CACHE_PATH.parent / (
            f".{_OPERATIONAL_CACHE_PATH.name}.{uuid.uuid4().hex}.tmp"
        )
        with tmp_path.open("w", encoding="utf-8", newline="\n") as handle:
            handle.write(encoded)
            handle.flush()
            try:
                os.fsync(handle.fileno())
            except (AttributeError, OSError):
                pass
        os.replace(tmp_path, _OPERATIONAL_CACHE_PATH)
        return True
    except Exception:
        if tmp_path is not None:
            try:
                tmp_path.unlink(missing_ok=True)
            except Exception:
                pass
        return False


async def _operational_cache_writer_loop() -> None:
    global _operational_cache_write_pending, _operational_cache_write_task
    while True:
        async with _operational_cache_write_lock:
            snapshot = _operational_cache_write_pending
            _operational_cache_write_pending = None
        if snapshot is not None:
            loop = asyncio.get_running_loop()
            ok = await loop.run_in_executor(
                _operational_cache_executor,
                functools.partial(_save_operational_cache, snapshot),
            )
            if not ok:
                global _operational_cache_write_failures
                _operational_cache_write_failures += 1
                logger.warning("Operational diagnostic cache write failed")
        async with _operational_cache_write_lock:
            if _operational_cache_write_pending is None:
                if _operational_cache_write_task is asyncio.current_task():
                    _operational_cache_write_task = None
                return


async def _queue_operational_cache_write() -> None:
    """Coalesce diagnostic writes so at most one worker owns the file."""
    global _operational_cache_write_pending, _operational_cache_write_task
    snapshot = copy.deepcopy(_operational_exchange_cache)
    async with _operational_cache_write_lock:
        _operational_cache_write_pending = snapshot
        task = _operational_cache_write_task
        if task is None or task.done():
            task = asyncio.create_task(_operational_cache_writer_loop(), name="telegram-cache-writer")
            _operational_cache_write_task = task


async def _flush_operational_cache_writer() -> None:
    async with _operational_cache_write_lock:
        task = _operational_cache_write_task
    if task is not None and not task.done():
        await asyncio.shield(task)


_load_operational_cache()


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def runtime_snapshot(engine, settings) -> dict[str, Any]:
    from capital_reservations import book as reservation_book
    from live_state import state as live_state
    from opportunity_queue import queue as opportunity_queue
    from order_state_store import store as order_store
    from position_registry import get_all_details

    live = live_state.snapshot()
    registry = get_all_details()
    reservations = reservation_book.snapshot()
    order_snapshot = order_store.snapshot()
    queue_snapshot = opportunity_queue.snapshot(limit=8)
    scan = engine.trade_flow_snapshot() if hasattr(engine, "trade_flow_snapshot") else {}
    return {
        "source": "production_runtime_state",
        "generated_at": _iso_now(),
        "paper_trading": os.getenv("PAPER_TRADING", "false").strip().lower() == "true",
        "process_pid": os.getpid(),
        "process_uptime_sec": round(time.time() - _PROCESS_STARTED_EPOCH, 3),
        "live_state": live,
        "engine_running": bool(engine.running),
        "engines_running": int(engine.running_count()),
        "engines_total": len(getattr(engine, "engines", {}) or {}),
        "registry_positions": registry,
        "registry_count": len(registry),
        "reservations": reservations,
        "reservation_count": len(reservations),
        "orders": order_snapshot,
        "opportunities": queue_snapshot,
        "scan": scan,
        "preflight": dict(getattr(engine, "_last_preflight", {}) or {}),
        "settings": {
            "daily_cap_enabled": bool(settings.get_val("daily_cap_enabled", True)),
            "daily_loss_pct": float(settings.get_val("daily_loss_pct", 0.0) or 0.0),
            "weekly_cap_enabled": bool(settings.get_val("weekly_cap_enabled", True)),
            "weekly_loss_pct": float(settings.get_val("weekly_loss_pct", 0.0) or 0.0),
            "max_open_positions": int(settings.get_val("max_open_positions", 0) or 0),
            "leverage_min": int(settings.get_val("leverage_min", 0) or 0),
            "leverage_max": int(settings.get_val("leverage_max", 0) or 0),
        },
    }


async def _read_account_snapshot_status(okx) -> dict:
    try:
        account = await okx.get_account_snapshot(priority="status")
    except TypeError:
        account = await okx.get_account_snapshot()
    return account


async def account_snapshot(okx, *, position_snapshot: dict | None = None) -> dict[str, Any]:
    account = await _read_account_snapshot_status(okx)
    return account_snapshot_from_values(account, position_snapshot=position_snapshot)


def account_snapshot_from_values(account: dict, *, position_snapshot: dict | None = None) -> dict[str, Any]:
    from capital_reservations import book as reservation_book

    positions = position_snapshot or {}
    position_rows = list((positions.get("positions", {}) or {}).values())
    unrealized = sum(float(row.get("unrealised_pnl", 0.0) or 0.0) for row in position_rows)
    isolated_margin = sum(float(row.get("isolated_margin", row.get("margin", 0.0)) or 0.0) for row in position_rows)
    open_risk = sum(float(row.get("risk_at_stop_usdt", 0.0) or 0.0) for row in position_rows)
    reservations = reservation_book.snapshot()
    reserved_margin = sum(float(row.get("margin_usdt", 0.0) or 0.0) for row in reservations)
    observed_at = str(
        account.get("observed_at")
        or account.get("exchange_snapshot_at")
        or account.get("received_at")
        or ""
    )
    rendered_at = _iso_now()
    age_sec = freshness_age_seconds(observed_at) if observed_at else None
    freshness_status = str(account.get("freshness_status") or ("FRESH" if observed_at else "UNKNOWN_FRESHNESS"))
    return {
        "source": "okx_authenticated_account",
        "generated_at": observed_at or rendered_at,
        "observed_at": observed_at,
        "exchange_snapshot_at": observed_at,
        "received_at": str(account.get("received_at") or observed_at or rendered_at),
        "rendered_at": rendered_at,
        "age_sec": age_sec,
        "stale": bool(account.get("stale", False)) or not bool(observed_at),
        "freshness_status": freshness_status,
        "equity_usdt": float(account.get("total_eq_usdt", 0.0) or 0.0),
        "available_usdt": float(account.get("avail_eq_usdt", account.get("usdt_avail_bal", 0.0)) or 0.0),
        "cash_usdt": float(account.get("usdt_cash_bal", 0.0) or 0.0),
        "frozen_usdt": float(account.get("usdt_frozen_bal", 0.0) or 0.0),
        "isolated_margin_usdt": isolated_margin,
        "unrealized_pnl_usdt": unrealized,
        "open_risk_usdt": open_risk,
        "reserved_margin_usdt": reserved_margin,
        "pending_reservations": len(reservations),
    }


async def _legacy_fast_operational_exchange_snapshot(okx, position_loader, *, timeout_sec: float = 4.0) -> tuple[dict, dict]:
    """Bound Telegram refresh time and fall back only to timestamped last-good truth."""
    try:
        positions, raw_account = await asyncio.wait_for(
            asyncio.gather(position_loader(), _read_account_snapshot_status(okx)),
            timeout=max(float(timeout_sec), 0.5),
        )
        account = account_snapshot_from_values(raw_account, position_snapshot=positions)
        _operational_exchange_cache.update({
            "account": account,
            "positions": positions,
            "updated_at": _iso_now(),
        })
        return account, positions
    except Exception as exc:
        cached_account = _cached_component("account", type(exc).__name__)
        cached_positions = _cached_component("positions", type(exc).__name__)
        if cached_account and cached_positions:
            return cached_account, cached_positions
        return (
            {
                "source": "UNAVAILABLE",
                "generated_at": _iso_now(),
                "equity_usdt": None,
                "available_usdt": None,
                "cash_usdt": None,
                "frozen_usdt": None,
                "isolated_margin_usdt": None,
                "unrealized_pnl_usdt": None,
                "open_risk_usdt": None,
                "reserved_margin_usdt": None,
                "pending_reservations": 0,
                "stale": True,
                "refresh_error": type(exc).__name__,
            },
            {
                "ok": False,
                "source": "STALE",
                "source_label": "🔴 STALE",
                "positions": {},
                "open_pnl_usdt": None,
                "age_sec": 0.0,
                "stale": True,
                "refresh_error": type(exc).__name__,
            },
        )


def _cached_component(name: str, refresh_error: str) -> dict:
    cached = dict(_operational_exchange_cache.get(name, {}) or {})
    if not cached:
        return {}
    updated_at = str(
        cached.get("observed_at")
        or cached.get("exchange_snapshot_at")
        or cached.get("fetched_at")
        or _operational_exchange_cache.get(f"{name}_updated_at", cached.get("generated_at", ""))
        or ""
    )
    cached.update({
        "stale": True,
        "refresh_error": refresh_error or "refresh_failed",
        "last_good_at": updated_at,
        "age_sec": freshness_age_seconds(updated_at),
    })
    if name == "positions":
        cached.update({"source": "CACHED_OKX", "source_label": "CACHED OKX", "semantic_state": "STALE_SNAPSHOT"})
    return cached


async def fast_operational_exchange_snapshot(okx, position_loader, *, timeout_sec: float = 4.0) -> tuple[dict, dict]:
    """Refresh account and positions independently within a bounded control-path SLA."""
    timeout = max(float(timeout_sec), 0.5)
    account_task = asyncio.create_task(_read_account_snapshot_status(okx))
    position_task = asyncio.create_task(position_loader())
    done, pending = await asyncio.wait({account_task, position_task}, timeout=timeout)
    for task in pending:
        task.cancel()
    if pending:
        await asyncio.gather(*pending, return_exceptions=True)

    positions = None
    raw_account = None
    position_error = ""
    account_error = ""
    if position_task in done:
        try:
            positions = position_task.result()
        except Exception as exc:
            position_error = type(exc).__name__
    else:
        position_error = "TimeoutError"
    if account_task in done:
        try:
            raw_account = account_task.result()
        except Exception as exc:
            account_error = type(exc).__name__
    else:
        account_error = "TimeoutError"

    now_iso = _iso_now()
    if isinstance(positions, dict):
        positions = _normalize_position_snapshot(positions, rendered_at=now_iso)
        if not positions["positions"] and positions.get("ok", False):
            positions["open_pnl_usdt"] = 0.0
            positions["semantic_state"] = "NO_OPEN_POSITIONS"
        _operational_exchange_cache.update({"positions": positions, "positions_updated_at": now_iso})
    else:
        positions = _cached_component("positions", position_error) or {
            "ok": False, "source": "UNAVAILABLE", "source_label": "UNAVAILABLE",
            "positions": {}, "open_pnl_usdt": None, "age_sec": None,
            "stale": True, "semantic_state": "OKX_DATA_UNAVAILABLE",
            "refresh_error": position_error,
        }

    if isinstance(raw_account, dict):
        account = account_snapshot_from_values(raw_account, position_snapshot=positions)
        _operational_exchange_cache.update({"account": account, "account_updated_at": now_iso})
    else:
        account = _cached_component("account", account_error) or {
            "source": "UNAVAILABLE", "generated_at": now_iso,
            "equity_usdt": None, "available_usdt": None, "cash_usdt": None,
            "frozen_usdt": None, "isolated_margin_usdt": None,
            "unrealized_pnl_usdt": None, "open_risk_usdt": None,
            "reserved_margin_usdt": None, "pending_reservations": 0,
            "stale": True, "age_sec": None, "refresh_error": account_error,
        }
    # Cache persistence is diagnostic and must not occupy the Telegram/event
    # loop after the bounded P0 reads have completed.
    await _queue_operational_cache_write()
    return account, positions


def _normalize_position_snapshot(raw: dict, *, rendered_at: str) -> dict[str, Any]:
    """Preserve authoritative loader freshness instead of inventing it."""
    result = dict(raw)
    result.setdefault("positions", {})
    observed_at = str(
        result.get("observed_at")
        or result.get("exchange_snapshot_at")
        or result.get("fetched_at")
        or ""
    )
    raw_status = str(result.get("freshness_status") or "").upper()
    source = str(result.get("source") or "OKX")
    semantic = str(result.get("semantic_state") or "").upper()
    stale = bool(result.get("stale", False)) or raw_status in {"STALE", "UNKNOWN_FRESHNESS"}
    stale = stale or "STALE" in semantic or "UNKNOWN" in semantic or not bool(result.get("ok", True))
    if not observed_at:
        freshness_status = "UNKNOWN_FRESHNESS"
        stale = True
        age_sec = None
    else:
        freshness_status = "STALE" if stale else "FRESH"
        age_sec = freshness_age_seconds(observed_at)
        if age_sec is None:
            freshness_status = "UNKNOWN_FRESHNESS"
            stale = True
    result.update({
        "source": source,
        "source_label": str(result.get("source_label") or source),
        "generated_at": str(result.get("generated_at") or observed_at or rendered_at),
        "observed_at": observed_at,
        "exchange_snapshot_at": observed_at,
        "rendered_at": rendered_at,
        "age_sec": age_sec,
        "stale": stale,
        "freshness_status": freshness_status,
        "ok": bool(result.get("ok", True)),
    })
    return result


async def start_operational_refresh(okx, position_loader, *, interval_sec: float = 10.0) -> None:
    global _operational_refresh_task
    if _operational_refresh_task and not _operational_refresh_task.done():
        return

    async def _loop() -> None:
        while True:
            try:
                await fast_operational_exchange_snapshot(okx, position_loader)
            except asyncio.CancelledError:
                raise
            except Exception:
                pass
            await asyncio.sleep(max(float(interval_sec), 2.0))

    _operational_refresh_task = asyncio.create_task(_loop(), name="telegram-operational-refresh")


async def stop_operational_refresh() -> None:
    global _operational_refresh_task, _operational_cache_executor_closed
    task = _operational_refresh_task
    _operational_refresh_task = None
    if task and not task.done():
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
    await _flush_operational_cache_writer()
    if not _operational_cache_executor_closed:
        _operational_cache_executor.shutdown(wait=False, cancel_futures=True)
        _operational_cache_executor_closed = True


def production_pnl_snapshot() -> dict[str, Any]:
    from pnl_reconciler import reconciler

    snapshot = reconciler.production_performance_snapshot()
    snapshot["generated_at_iso"] = _iso_now()
    return snapshot


def _control_fallback(key: str, error: Exception) -> dict[str, Any]:
    cached = copy.deepcopy(_control_snapshot_cache.get(key, {}) or {})
    if cached:
        cached["source"] = "CACHED_CONTROL"
        cached["stale"] = True
        cached["refresh_status"] = "CONTROL_QUEUE_FULL"
        cached["refresh_error"] = type(error).__name__
        generated = cached.get("generated_at") or cached.get("generated_at_iso")
        cached["age_sec"] = freshness_age_seconds(str(generated)) if generated else None
        return cached
    return {
        "source": "CONTROL_UNAVAILABLE",
        "stale": True,
        "refresh_status": "CONTROL_QUEUE_FULL",
        "refresh_error": type(error).__name__,
        "age_sec": None,
    }


async def control_runtime_snapshot(engine, settings, *, priority: str = "high") -> dict[str, Any]:
    key = "runtime_snapshot"
    try:
        value = await run_control_work(
            runtime_snapshot, engine, settings, priority=priority,
            coalesce_key=key,
        )
    except ControlWorkQueueFull as exc:
        return _control_fallback(key, exc)
    _control_snapshot_cache[key] = copy.deepcopy(value)
    return value


async def control_pnl_snapshot(*, priority: str = "normal") -> dict[str, Any]:
    key = "production_pnl_snapshot"
    try:
        value = await run_control_work(
            production_pnl_snapshot, priority=priority, coalesce_key=key,
        )
    except ControlWorkQueueFull as exc:
        return _control_fallback(key, exc)
    _control_snapshot_cache[key] = copy.deepcopy(value)
    return value


def telegram_health_snapshot() -> dict[str, Any]:
    from telegram_control_plane import control_plane

    return control_plane.snapshot()


async def cached_analytical_snapshot(
    key: str,
    builder,
    *,
    ttl_sec: float = 30.0,
    provenance: str,
) -> dict[str, Any]:
    """Build expensive read-only analytics off-loop and expose freshness."""
    now = time.time()
    cached = _analytical_cache.get(key)
    if cached and now - float(cached.get("generated_epoch", 0.0) or 0.0) <= max(float(ttl_sec), 0.0):
        return {**cached, "cache_hit": True, "age_sec": round(now - float(cached["generated_epoch"]), 3)}
    lock = _analytical_locks.setdefault(key, asyncio.Lock())
    async with lock:
        now = time.time()
        cached = _analytical_cache.get(key)
        if cached and now - float(cached.get("generated_epoch", 0.0) or 0.0) <= max(float(ttl_sec), 0.0):
            return {**cached, "cache_hit": True, "age_sec": round(now - float(cached["generated_epoch"]), 3)}
        value = await run_control_work(
            builder, priority="normal", coalesce_key=f"analytical:{key}",
        )
        row = {
            "value": value,
            "generated_at": _iso_now(),
            "generated_epoch": time.time(),
            "provenance": provenance,
            "cache_hit": False,
            "age_sec": 0.0,
        }
        _analytical_cache[key] = row
        return dict(row)


def freshness_age_seconds(iso_value: str) -> float | None:
    try:
        return max(time.time() - datetime.fromisoformat(iso_value.replace("Z", "+00:00")).timestamp(), 0.0)
    except Exception:
        return None

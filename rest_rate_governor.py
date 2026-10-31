"""Process-global, endpoint-specific OKX REST admission control.

The exchange publishes limits per endpoint and rule key.  This module keeps
those budgets shared across the independent HTTP pools without turning them
into one global lock.  Admission is queued, never rejected, and each retry
must acquire a new admission.
"""
from __future__ import annotations

import asyncio
import threading
import time
import weakref
from collections import deque
from dataclasses import dataclass


@dataclass(frozen=True)
class RestRateSpec:
    endpoint: str
    method: str
    capacity: int
    window_sec: float
    rule: str
    source: str
    secondary: str = ""


@dataclass
class _Waiter:
    future: asyncio.Future
    priority: str
    created_at: float
    sequence: int
    window_sec: float
    buckets: tuple[str, ...] = ()
    rate_ready_at: float = 0.0
    active: bool = True


@dataclass
class RateAdmissionTicket:
    """A non-consuming request admission that is finalized at send time."""

    method: str
    path: str
    params: dict | None
    data: dict | None
    priority: str
    created_at: float
    waiters: tuple[_Waiter, ...]
    loop: asyncio.AbstractEventLoop
    active: bool = True
    admitted_at: float = 0.0


async def run_with_rate_admission(
    method: str,
    path: str,
    *,
    params: dict | None = None,
    data: dict | None = None,
    priority: str | None = None,
    pool_gate: asyncio.Semaphore | None = None,
    send,
):
    """Run a direct REST call with just-in-time rate admission.

    Direct public clients use this because their HTTPX connection pool is
    otherwise outside ``OKXClient``'s semaphore accounting.
    """
    ticket = await process_rest_rate_governor.prepare(
        method, path, params=params, data=data, priority=priority,
    )
    acquired = False
    try:
        while True:
            await process_rest_rate_governor.wait_until_admissible(ticket)
            if pool_gate is not None:
                try:
                    await asyncio.wait_for(pool_gate.acquire(), timeout=0.05)
                    acquired = True
                except asyncio.TimeoutError:
                    continue
            if await process_rest_rate_governor.try_admit(ticket):
                break
            if acquired and pool_gate is not None:
                pool_gate.release()
                acquired = False
        bucket = resolve_rest_rate_buckets(method, path, params, data)[0][1]
        return await send(), bucket
    except BaseException:
        await process_rest_rate_governor.cancel(ticket)
        raise
    finally:
        if acquired and pool_gate is not None:
            pool_gate.release()


class UnclassifiedRestEndpoint(RuntimeError):
    """Raised when runtime code tries to send an endpoint without a mapping."""


_P0 = {"p0", "critical", "execution"}
_P1 = {"p1", "leverage_truth", "mutation_readiness"}
_P2 = {"p2", "status", "historical", "reconciliation", "background", "enrichment"}


# Values and rule scopes come from the OKX API guide.  The four legacy Rubik /
# liquidation paths are explicitly marked as local conservative classifications
# because they are used by this repository but are not assigned a current
# endpoint limit in the v5 guide.
_SPECS = {
    ("GET", "/api/v5/public/time"): RestRateSpec("public/time", "GET", 10, 2.0, "ip", "OKX API guide"),
    ("GET", "/api/v5/public/instruments"): RestRateSpec("public/instruments", "GET", 20, 2.0, "ip_inst_type", "OKX API guide"),
    ("GET", "/api/v5/public/open-interest"): RestRateSpec("public/open-interest", "GET", 20, 2.0, "ip_instrument", "OKX API guide"),
    ("GET", "/api/v5/public/mark-price"): RestRateSpec("public/mark-price", "GET", 10, 2.0, "ip_instrument", "OKX API guide"),
    ("GET", "/api/v5/public/funding-rate"): RestRateSpec("public/funding-rate", "GET", 1, 1.0, "local_ip_instrument", "local conservative classification; legacy endpoint"),
    ("GET", "/api/v5/market/ticker"): RestRateSpec("market/ticker", "GET", 20, 2.0, "ip_instrument", "OKX API guide"),
    ("GET", "/api/v5/market/tickers"): RestRateSpec("market/tickers", "GET", 20, 2.0, "ip", "OKX API guide"),
    ("GET", "/api/v5/market/candles"): RestRateSpec("market/candles", "GET", 40, 2.0, "ip", "OKX API guide"),
    ("GET", "/api/v5/public/liquidation-orders"): RestRateSpec("public/liquidation-orders", "GET", 1, 1.0, "local_ip_instrument", "local conservative classification; legacy endpoint"),
    ("GET", "/api/v5/rubik/stat/taker-volume"): RestRateSpec("rubik/taker-volume", "GET", 1, 1.0, "local_ip", "local conservative classification; legacy endpoint"),
    ("GET", "/api/v5/rubik/stat/contracts/long-short-account-ratio"): RestRateSpec("rubik/long-short-ratio", "GET", 1, 1.0, "local_ip", "local conservative classification; legacy endpoint"),
    ("GET", "/api/v5/rubik/stat/contracts/open-interest-volume"): RestRateSpec("rubik/open-interest-volume", "GET", 1, 1.0, "local_ip", "local conservative classification; legacy endpoint"),
    ("GET", "/api/v5/account/balance"): RestRateSpec("account/balance", "GET", 10, 2.0, "user", "OKX API guide"),
    ("GET", "/api/v5/account/positions"): RestRateSpec("account/positions", "GET", 10, 2.0, "user", "OKX API guide"),
    ("GET", "/api/v5/account/config"): RestRateSpec("account/config", "GET", 5, 2.0, "user", "OKX API guide"),
    ("GET", "/api/v5/account/max-size"): RestRateSpec("account/max-size", "GET", 20, 2.0, "user", "OKX API guide"),
    ("GET", "/api/v5/account/leverage-info"): RestRateSpec("account/leverage-info", "GET", 20, 2.0, "user", "OKX API guide"),
    ("GET", "/api/v5/account/trade-fee"): RestRateSpec("account/trade-fee", "GET", 5, 2.0, "user", "OKX API guide"),
    ("GET", "/api/v5/account/bills"): RestRateSpec("account/bills", "GET", 5, 1.0, "user", "OKX API guide"),
    ("GET", "/api/v5/account/positions-history"): RestRateSpec("account/positions-history", "GET", 10, 2.0, "user", "OKX API guide"),
    ("POST", "/api/v5/account/set-leverage"): RestRateSpec("account/set-leverage", "POST", 20, 2.0, "user", "OKX API guide"),
    ("GET", "/api/v5/trade/orders-pending"): RestRateSpec("trade/orders-pending", "GET", 60, 2.0, "user", "OKX API guide"),
    ("GET", "/api/v5/trade/order"): RestRateSpec("trade/order", "GET", 60, 2.0, "user_instrument", "OKX API guide"),
    ("GET", "/api/v5/trade/orders-history"): RestRateSpec("trade/orders-history", "GET", 40, 2.0, "user", "OKX API guide"),
    ("GET", "/api/v5/trade/fills"): RestRateSpec("trade/fills", "GET", 60, 2.0, "user", "OKX API guide"),
    ("POST", "/api/v5/trade/order"): RestRateSpec(
        "trade/order", "POST", 60, 2.0, "user_instrument", "OKX API guide",
        secondary="subaccount_order",
    ),
    ("POST", "/api/v5/trade/cancel-order"): RestRateSpec("trade/cancel-order", "POST", 60, 2.0, "user_instrument", "OKX API guide"),
    ("GET", "/api/v5/trade/orders-algo-pending"): RestRateSpec("trade/orders-algo-pending", "GET", 20, 2.0, "user", "OKX API guide"),
    ("GET", "/api/v5/trade/order-algo"): RestRateSpec("trade/order-algo", "GET", 20, 2.0, "user", "OKX API guide"),
    ("GET", "/api/v5/trade/orders-algo-history"): RestRateSpec("trade/orders-algo-history", "GET", 20, 2.0, "user", "OKX API guide"),
    ("POST", "/api/v5/trade/order-algo"): RestRateSpec("trade/order-algo", "POST", 20, 2.0, "user_instrument", "OKX API guide"),
    ("POST", "/api/v5/trade/cancel-algos"): RestRateSpec("trade/cancel-algos", "POST", 20, 2.0, "user_instrument", "OKX API guide"),
    # Synthetic endpoint used only by existing transport tests.
    ("GET", "/api/v5/public/test"): RestRateSpec("test/public", "GET", 1000, 1.0, "test", "test-only"),
}

_SECONDARY_SPECS = {
    "subaccount_order": RestRateSpec(
        "trade/orders-secondary", "POST", 1000, 2.0,
        "secondary_account", "OKX API guide",
    ),
}


def _value(params: dict | None, data: dict | None, key: str) -> str:
    for source in (params or {}, data or {}):
        value = source.get(key)
        if value not in (None, ""):
            return str(value)
    return "*"


def resolve_rest_rate_spec(method: str, path: str, params: dict | None = None, data: dict | None = None) -> tuple[RestRateSpec, str]:
    key = (str(method or "GET").upper(), str(path or "").split("?", 1)[0])
    spec = _SPECS.get(key)
    if spec is None:
        raise UnclassifiedRestEndpoint(f"No OKX REST rate classification for {key[0]} {key[1]}")
    instrument = _value(params, data, "instId")
    family = _value(params, data, "instFamily")
    inst_type = _value(params, data, "instType")
    if spec.rule in {"user_instrument", "local_ip_instrument"}:
        scope = instrument if instrument != "*" else family
        bucket = f"{spec.endpoint}:instrument:{scope}"
    elif spec.rule == "ip_instrument":
        bucket = f"{spec.endpoint}:ip:{instrument if instrument != '*' else family}"
    elif spec.rule == "ip_inst_type":
        bucket = f"{spec.endpoint}:ip:{inst_type}"
    elif spec.rule == "user":
        bucket = f"{spec.endpoint}:user"
    elif spec.rule == "local_ip_instrument":
        bucket = f"{spec.endpoint}:ip:{instrument if instrument != '*' else family}"
    elif spec.rule == "local_ip":
        bucket = f"{spec.endpoint}:ip"
    else:
        bucket = f"{spec.endpoint}:shared"
    return spec, bucket


def resolve_rest_rate_buckets(
    method: str,
    path: str,
    params: dict | None = None,
    data: dict | None = None,
) -> tuple[tuple[RestRateSpec, str], ...]:
    """Return every exchange budget that applies to one physical request."""
    spec, bucket = resolve_rest_rate_spec(method, path, params, data)
    result = [(spec, bucket)]
    if spec.secondary == "subaccount_order":
        secondary = _SECONDARY_SPECS[spec.secondary]
        result.append(
            (secondary, "trade/orders-secondary:account")
        )
    return tuple(result)


def runtime_endpoint_mapping() -> dict[str, dict]:
    return {
        f"{method} {path}": {
            "endpoint": spec.endpoint,
            "capacity": spec.capacity,
            "window_sec": spec.window_sec,
            "rule": spec.rule,
            "secondary": spec.secondary,
            "source": spec.source,
        }
        for (method, path), spec in sorted(_SPECS.items())
        if spec.source != "test-only"
    }


class ProcessRestRateGovernor:
    """Shared sliding-window admission with priority and bounded aging."""

    def __init__(self) -> None:
        self._states: weakref.WeakKeyDictionary = weakref.WeakKeyDictionary()
        self._states_lock = threading.RLock()
        self._metrics_lock = threading.RLock()
        self._pressure: dict[str, dict] = {}
        self._metrics = self._new_metrics()

    @staticmethod
    def _new_metrics() -> dict:
        return {
            "rate_admissions": 0,
            "rate_waiters": 0,
            "rate_wait_ms": 0.0,
            "rate_wait_p95": 0.0,
            "rate_wait_max": 0.0,
            "rate_bucket_saturation": 0,
            "multi_bucket_admissions": 0,
            "partial_bucket_consumptions": 0,
            "rate_limited_responses": 0,
            "http_429_responses": 0,
            "business_50011_responses": 0,
            "per_bucket": {},
        }

    def _loop_lock(self, loop: asyncio.AbstractEventLoop) -> asyncio.Lock:
        with self._states_lock:
            loop_states = self._states.setdefault(loop, {})
            lock = loop_states.get("__lock__")
            if lock is None:
                lock = asyncio.Lock()
                loop_states["__lock__"] = lock
            return lock

    def _state(self, loop: asyncio.AbstractEventLoop, bucket: str, spec: RestRateSpec) -> dict:
        with self._states_lock:
            loop_states = self._states.setdefault(loop, {})
            state = loop_states.get(bucket)
            if state is None:
                state = {
                    "timestamps": deque(),
                    "admission_priorities": deque(),
                    "waiters": [],
                    "sequence": 0,
                    "spec": spec,
                }
                loop_states[bucket] = state
            return state

    @staticmethod
    def _rank(priority: str) -> int:
        value = str(priority or "p2").strip().lower()
        if value in _P0:
            return 0
        if value in _P1:
            return 1
        return 2

    @staticmethod
    def _cleanup_state(state: dict, now: float) -> None:
        spec: RestRateSpec = state["spec"]
        timestamps: deque = state["timestamps"]
        priorities: deque = state["admission_priorities"]
        while timestamps and now - timestamps[0] >= spec.window_sec:
            timestamps.popleft()
        while priorities and now - priorities[0][0] >= spec.window_sec:
            priorities.popleft()
        state["waiters"][:] = [waiter for waiter in state["waiters"] if waiter.active]

    def _allowed(self, waiter: _Waiter, state: dict, now: float) -> bool:
        self._cleanup_state(state, now)
        spec: RestRateSpec = state["spec"]
        if len(state["timestamps"]) >= spec.capacity or waiter not in state["waiters"]:
            return False
        rank = self._rank(waiter.priority)
        waiters = list(state["waiters"])
        high = [row for row in waiters if self._rank(row.priority) < 2]
        low = [row for row in waiters if self._rank(row.priority) == 2]
        if rank < 2:
            if any(self._rank(row.priority) < rank for row in high):
                return False
            if low and spec.capacity >= 2:
                low_used = sum(item_rank == 2 for _, item_rank in state["admission_priorities"])
                if low_used == 0 and len(state["timestamps"]) == spec.capacity - 1:
                    return False
            if low and spec.capacity == 1:
                oldest_low = min(low, key=lambda row: (row.created_at, row.sequence))
                if now - oldest_low.created_at >= max(spec.window_sec * 2.0, 0.5):
                    return False
            return True
        if not low:
            return False
        oldest_low = min(low, key=lambda row: (row.created_at, row.sequence))
        if waiter is not oldest_low:
            return False
        if not high:
            return True
        low_used = sum(item_rank == 2 for _, item_rank in state["admission_priorities"])
        if spec.capacity >= 2:
            return low_used == 0 and len(state["timestamps"]) < spec.capacity - 1
        return now - oldest_low.created_at >= max(spec.window_sec * 2.0, 0.5)

    async def prepare(self, method: str, path: str, *, params: dict | None = None, data: dict | None = None, priority: str | None = None) -> RateAdmissionTicket:
        loop = asyncio.get_running_loop()
        resolved = resolve_rest_rate_buckets(method, path, params, data)
        created = time.monotonic()
        waiters = []
        async with self._loop_lock(loop):
            for index, (spec, bucket) in enumerate(resolved):
                state = self._state(loop, bucket, spec)
                state["sequence"] += 1
                waiter = _Waiter(
                    loop.create_future(), str(priority or "p2"), created,
                    state["sequence"], spec.window_sec,
                    tuple(row[1] for row in resolved),
                )
                state["waiters"].append(waiter)
                waiters.append(waiter)
        with self._metrics_lock:
            self._metrics["rate_waiters"] += 1
        return RateAdmissionTicket(
            str(method).upper(), str(path), params, data,
            str(priority or "p2"), created, tuple(waiters), loop,
        )

    async def _ticket_states(self, ticket: RateAdmissionTicket):
        resolved = resolve_rest_rate_buckets(ticket.method, ticket.path, ticket.params, ticket.data)
        return tuple(
            self._state(ticket.loop, bucket, spec)
            for spec, bucket in resolved
        )

    async def is_admissible(self, ticket: RateAdmissionTicket) -> bool:
        if not ticket.active:
            return False
        async with self._loop_lock(ticket.loop):
            states = await self._ticket_states(ticket)
            now = time.monotonic()
            if str(ticket.method).upper() == "GET" and any(
                self._pressure_remaining(bucket, now) > 0.0
                for _, bucket in resolve_rest_rate_buckets(
                    ticket.method, ticket.path, ticket.params, ticket.data
                )
            ):
                return False
            return all(self._allowed(waiter, state, now) for waiter, state in zip(ticket.waiters, states))

    async def wait_until_admissible(self, ticket: RateAdmissionTicket) -> None:
        while ticket.active:
            if await self.is_admissible(ticket):
                for waiter in ticket.waiters:
                    waiter.rate_ready_at = time.monotonic()
                return
            await asyncio.sleep(0.01)
        raise asyncio.CancelledError()

    async def try_admit(self, ticket: RateAdmissionTicket) -> bool:
        if not ticket.active:
            return False
        async with self._loop_lock(ticket.loop):
            states = await self._ticket_states(ticket)
            now = time.monotonic()
            if not all(self._allowed(waiter, state, now) for waiter, state in zip(ticket.waiters, states)):
                return False
            for waiter, state in zip(ticket.waiters, states):
                state["timestamps"].append(now)
                state["admission_priorities"].append((now, self._rank(waiter.priority)))
                if waiter in state["waiters"]:
                    state["waiters"].remove(waiter)
                waiter.active = False
            ticket.active = False
            ticket.admitted_at = now
            ready_at = max((waiter.rate_ready_at for waiter in ticket.waiters), default=ticket.created_at)
            wait_sec = max(ready_at - ticket.created_at, 0.0)
            with self._metrics_lock:
                self._metrics["rate_admissions"] += 1
                self._metrics["rate_wait_ms"] += wait_sec * 1000.0
                self._metrics["rate_wait_max"] = max(self._metrics["rate_wait_max"], wait_sec * 1000.0)
                samples = self._metrics.setdefault("_samples", deque(maxlen=512))
                samples.append(wait_sec * 1000.0)
                self._metrics["rate_wait_p95"] = _percentile(samples, 95.0)
                if len(states) > 1:
                    self._metrics["multi_bucket_admissions"] += 1
                for state in states:
                    endpoint = state["spec"].endpoint
                    bucket = self._metrics["per_bucket"].setdefault(endpoint, {"admissions": 0, "saturation": 0})
                    bucket["admissions"] += 1
            return True

    async def cancel(self, ticket: RateAdmissionTicket) -> None:
        if not ticket.active:
            return
        async with self._loop_lock(ticket.loop):
            states = await self._ticket_states(ticket)
            for waiter, state in zip(ticket.waiters, states):
                waiter.active = False
                if waiter in state.get("waiters", []):
                    state["waiters"].remove(waiter)
            ticket.active = False

    async def acquire(self, method: str, path: str, *, params: dict | None = None, data: dict | None = None, priority: str | None = None) -> str:
        ticket = await self.prepare(method, path, params=params, data=data, priority=priority)
        try:
            while True:
                await self.wait_until_admissible(ticket)
                if await self.try_admit(ticket):
                    resolved = resolve_rest_rate_buckets(method, path, params, data)
                    return resolved[0][1]
        except BaseException:
            await self.cancel(ticket)
            raise

    def _record_rate_limited(self, bucket: str, field: str | None = None) -> None:
        bucket = str(bucket or "")
        now = time.monotonic()
        with self._metrics_lock:
            self._metrics["rate_limited_responses"] += 1
            if field:
                self._metrics[field] += 1
            endpoint = str(bucket or "").split(":", 1)[0] or "unknown"
            row = self._metrics["per_bucket"].setdefault(endpoint, {"admissions": 0, "saturation": 0})
            row["rate_limited_responses"] = int(row.get("rate_limited_responses", 0)) + 1
            if field:
                row[field] = int(row.get(field, 0)) + 1
            pressure = self._pressure.setdefault(bucket, {"level": 0, "backoff_until": 0.0})
            pressure["level"] = min(int(pressure.get("level", 0) or 0) + 1, 6)
            delay = min(8.0, 0.25 * (2 ** max(pressure["level"] - 1, 0)))
            pressure["backoff_until"] = max(
                float(pressure.get("backoff_until", 0.0) or 0.0), now + delay
            )
            row["backoff_until"] = pressure["backoff_until"]
            row["backoff_level"] = pressure["level"]

    def _pressure_remaining(self, bucket: str, now: float) -> float:
        with self._metrics_lock:
            return max(float(self._pressure.get(str(bucket), {}).get("backoff_until", 0.0) or 0.0) - now, 0.0)

    def record_http_429(self, bucket: str) -> None:
        self._record_rate_limited(bucket, "http_429_responses")

    def record_business_50011(self, bucket: str) -> None:
        self._record_rate_limited(bucket, "business_50011_responses")

    def record_rate_limited(self, bucket: str) -> None:
        """Compatibility total for callers without a response subtype."""
        self._record_rate_limited(bucket)

    def snapshot(self) -> dict:
        with self._metrics_lock:
            snapshot = {key: value for key, value in self._metrics.items() if key != "_samples"}
            snapshot["per_bucket"] = {key: dict(value) for key, value in self._metrics["per_bucket"].items()}
            return snapshot

    def reset(self) -> None:
        with self._metrics_lock:
            self._metrics = self._new_metrics()
            self._pressure = {}
        with self._states_lock:
            self._states = weakref.WeakKeyDictionary()


def _percentile(values, percentile: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(float(value) for value in values)
    index = min(len(ordered) - 1, max(0, int(round((percentile / 100.0) * (len(ordered) - 1)))))
    return round(ordered[index], 3)


process_rest_rate_governor = ProcessRestRateGovernor()


def get_process_rest_rate_snapshot() -> dict:
    return process_rest_rate_governor.snapshot()


__all__ = [
    "ProcessRestRateGovernor",
    "RateAdmissionTicket",
    "RestRateSpec",
    "UnclassifiedRestEndpoint",
    "get_process_rest_rate_snapshot",
    "process_rest_rate_governor",
    "resolve_rest_rate_spec",
    "resolve_rest_rate_buckets",
    "run_with_rate_admission",
    "runtime_endpoint_mapping",
]

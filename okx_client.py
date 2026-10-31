"""
OxyPulse — okx_client.py  v2
OKX REST client with NANO Growth Mode.

KEY FIXES v2
──────────────────────────────
1. Exchange-attached TP/SL on every futures market order.
   OKX algo orders fire automatically — no exit_manager polling needed.
   Fields: tpTriggerPx, tpOrdPx=-1 (market), slTriggerPx, slOrdPx=-1
2. actual_coin returned in every order dict.
   When NANO routing switches coin, callers know which coin is actually open.
3. HMAC signing fixed: GET query string included in signed path.
4. Integer contract sizes only (no decimals) for futures.
"""
from __future__ import annotations

import os
import asyncio
import hmac
import hashlib
import base64
import json
import logging
import re
import time
import random
import threading
import uuid
from copy import deepcopy
from datetime import datetime, timezone
from dataclasses import dataclass

import httpx

from okx_mutation_results import (
    MUTATION_DEFINITIVE_REJECTION,
    MUTATION_READBACK_REQUIRED,
    MUTATION_SUCCESS,
    classify_mutation_response,
)
from dotenv import load_dotenv

import execution_admission as _admission
import latency_telemetry as _latency
from risk_manager import risk
from coin_universe import (
    get_full_universe, get_min_order, get_last_top10,
    get_estimated_ct_val, get_nano_growth_coins,
)
import market_mode as mm
from leverage_truth import LeverageTruthCache, LeverageTruthUnavailable
from rest_rate_governor import (
    get_process_rest_rate_snapshot,
    process_rest_rate_governor,
    resolve_rest_rate_buckets,
)
from okx_client_ids import InvalidOKXClientID, validate_client_id

load_dotenv(override=True)

logger = logging.getLogger("OxyPulse.OKX")

OKX_API_KEY        = os.getenv("OKX_API_KEY", "")
OKX_API_SECRET     = os.getenv("OKX_API_SECRET", "")
OKX_API_PASSPHRASE = os.getenv("OKX_API_PASSPHRASE", "")
PAPER_TRADING      = os.getenv("PAPER_TRADING", "false").lower() == "true"

BASE_URL = "https://www.okx.com"

_universe_cache: set[str] = set()
_ct_val_cache:  dict[str, float] = {}
_instrument_leverage_cap: dict[str, int] = {}
_working_leverage_cache: dict[tuple[str, str], int] = {}  # (coin, pos_side) -> leverage
_taker_volume_cache: dict[str, dict] = {}
_long_short_ratio_cache: dict[str, dict] = {}
_ticker_cache: dict[str, dict] = {}
_taker_volume_lock: asyncio.Lock | None = None
_long_short_ratio_lock: asyncio.Lock | None = None
_taker_volume_locks: dict[str, asyncio.Lock] = {}
_long_short_ratio_locks: dict[str, asyncio.Lock] = {}
# Kept as inert compatibility attributes for existing test/adaptor resets.
# The ticker path never reads or acquires these globals.
_ticker_lock: asyncio.Lock | None = None
_execution_ticker_lock: asyncio.Lock | None = None
_ticker_inflight: dict[tuple[int, str, str], asyncio.Task] = {}
_ticker_metrics = {
    "logical_callers": 0,
    "cache_hits": 0,
    "coalesced_callers": 0,
    "underlying_requests": 0,
    "failures": 0,
}
_ticker_metrics_lock = threading.RLock()
_account_snapshot_cache: dict[str, object] = {"value": None, "ts": 0.0}
_ACCOUNT_SNAPSHOT_TTL_SEC = 2.0
_TICKER_CACHE_TTL_SEC = 1.5

# Persistent connection pool — reused across all OKX REST calls.
# Avoids the ~150ms TCP+TLS handshake overhead on every request.
# limits: 20 total connections, 10 per host (OKX rate-limit safe).
_http_client: httpx.AsyncClient = httpx.AsyncClient(
    timeout=httpx.Timeout(20.0, connect=5.0),
    limits=httpx.Limits(max_connections=20, max_keepalive_connections=10),
    http2=False,   # OKX does not support HTTP/2; keep HTTP/1.1 keep-alive
)

# Keep bulk market/enrichment traffic bounded while reserving independent
# capacity for exchange truth and execution-critical requests.  The gates are
# created lazily because this module is imported before the runtime event loop.
def _new_critical_http_client() -> httpx.AsyncClient:
    return httpx.AsyncClient(
        timeout=httpx.Timeout(20.0, connect=5.0),
        limits=httpx.Limits(max_connections=6, max_keepalive_connections=3),
        http2=False,
    )


def _new_mutation_http_client() -> httpx.AsyncClient:
    return httpx.AsyncClient(
        timeout=httpx.Timeout(20.0, connect=5.0),
        limits=httpx.Limits(max_connections=4, max_keepalive_connections=2),
        http2=False,
    )


def _new_leverage_http_client() -> httpx.AsyncClient:
    """Dedicated read capacity for candidate leverage truth."""
    return httpx.AsyncClient(
        timeout=httpx.Timeout(20.0, connect=5.0),
        limits=httpx.Limits(max_connections=4, max_keepalive_connections=2),
        http2=False,
    )


def _new_reconciliation_http_client() -> httpx.AsyncClient:
    """Bounded capacity for historical/reconciliation reads."""
    return httpx.AsyncClient(
        timeout=httpx.Timeout(20.0, connect=5.0),
        limits=httpx.Limits(max_connections=2, max_keepalive_connections=1),
        http2=False,
    )


_critical_http_client: httpx.AsyncClient = _new_critical_http_client()
# Mutation requests have their own client and are never replaced by read-only
# recovery.  A request that may have reached OKX must retain its uncertainty.
_mutation_http_client: httpx.AsyncClient = _new_mutation_http_client()
_leverage_http_client: httpx.AsyncClient = _new_leverage_http_client()
_reconciliation_http_client: httpx.AsyncClient = _new_reconciliation_http_client()
_background_request_gate: asyncio.Semaphore | None = None
_critical_request_gate: asyncio.Semaphore | None = None
_mutation_request_gate: asyncio.Semaphore | None = None
_leverage_request_gate: asyncio.Semaphore | None = None
_reconciliation_request_gate: asyncio.Semaphore | None = None
_BACKGROUND_REQUEST_LIMIT = 20
_CRITICAL_REQUEST_LIMIT = 6
_MUTATION_REQUEST_LIMIT = 4
_LEVERAGE_REQUEST_LIMIT = 4
_RECONCILIATION_REQUEST_LIMIT = 2
_MUTATION_READINESS_TTL_SEC = 30.0
_mutation_readiness_lock: asyncio.Lock | None = None
_mutation_readiness_state = {"ok": False, "checked_at": 0.0, "reason": ""}


@dataclass
class _CriticalReadGeneration:
    generation_id: int
    client: httpx.AsyncClient
    active_requests: int = 0
    retired: bool = False
    close_pending: bool = False
    closed: bool = False


_critical_generation_lock = threading.RLock()
_critical_generation_seq = 0
_critical_current_generation: _CriticalReadGeneration | None = None
_critical_generations: dict[int, _CriticalReadGeneration] = {}
_critical_recovery = {
    "current_generation": 0,
    "recovery_count": 0,
    "retired_generation_count": 0,
    "last_recovery_reason": "",
    "last_recovery_timestamp": "",
}
# Process-global REST health is deliberately separate from the per-scan
# ContextVar collector.  Reconciliation, Telegram refresh, startup, and
# execution requests may run outside a scan context but still share these
# module-global transports.
_process_health_lock = threading.RLock()


def _new_process_rest_health() -> dict[str, dict[str, float | int]]:
    fields = {
        "request_count": 0, "retry_count": 0, "timeout_count": 0,
        "pool_timeout_count": 0, "error_count": 0,
        "physical_request_count": 0,
        "total_wall_time_sec": 0.0, "max_wall_time_sec": 0.0,
    }
    return {
        name: dict(fields)
        for name in ("background", "critical_read", "leverage_truth", "mutation", "reconciliation")
    }


def _new_process_rest_capacity() -> dict[str, dict[str, float | int]]:
    fields = {
        "active_requests": 0, "peak_active_requests": 0,
        "queue_wait_total_sec": 0.0, "queue_wait_max_sec": 0.0,
        "rate_wait_total_sec": 0.0, "rate_wait_max_sec": 0.0,
        "connect_timeout_count": 0, "read_timeout_count": 0,
        "http_error_count": 0, "business_error_count": 0,
    }
    return {name: dict(fields) for name in ("background", "critical_read", "leverage_truth", "mutation", "reconciliation")}


_process_rest_health = _new_process_rest_health()
_process_rest_capacity = _new_process_rest_capacity()

# Truth reads are coalesced process-wide, including calls made by different
# OKXClient instances.  Tasks are scoped by event loop so test/runtime loops
# cannot await each other's futures.
_truth_cache: dict[str, dict[str, object]] = {}
_truth_inflight: dict[tuple[int, str], asyncio.Task] = {}
_truth_metrics = {
    "logical_callers": 0,
    "cache_hits": 0,
    "coalesced_requests": 0,
    "underlying_requests": 0,
    "successful_reads": 0,
    "failed_reads": 0,
}
_truth_metrics_lock = threading.RLock()
_protection_latency_lock = threading.RLock()
_protection_latency = {"read_count": 0, "total_sec": 0.0, "max_sec": 0.0}
_rest_scope = {
    "process_started_at": datetime.now(timezone.utc).isoformat(),
    "session_id": uuid.uuid4().hex,
    "arm_epoch": 0,
    "arm_started_at": "",
}


def mark_rest_arm_epoch() -> dict:
    """Mark an explicit operator arm without resetting lifetime counters."""
    with _truth_metrics_lock:
        _rest_scope["arm_epoch"] = int(_rest_scope.get("arm_epoch", 0) or 0) + 1
        _rest_scope["arm_started_at"] = datetime.now(timezone.utc).isoformat()
        return dict(_rest_scope)


async def _coalesced_truth_read(
    key: str,
    reader,
    *,
    ttl_sec: float,
    force: bool = False,
    cacheable=None,
    join_keys: tuple[str, ...] = (),
):
    with _truth_metrics_lock:
        _truth_metrics["logical_callers"] = int(_truth_metrics.get("logical_callers", 0) or 0) + 1
    now = time.monotonic()
    if not force:
        cached = _truth_cache.get(key)
        if cached and now - float(cached.get("ts", 0.0) or 0.0) <= max(float(ttl_sec), 0.0):
            with _truth_metrics_lock:
                _truth_metrics["cache_hits"] += 1
            return deepcopy(cached.get("value"))

    loop = asyncio.get_running_loop()
    inflight_key = (id(loop), key)
    task = _truth_inflight.get(inflight_key)
    if task is None or task.done():
        for candidate in join_keys:
            candidate_key = (id(loop), candidate)
            candidate_task = _truth_inflight.get(candidate_key)
            if candidate_task is not None and not candidate_task.done():
                inflight_key = candidate_key
                task = candidate_task
                break
    if task is None or task.done():
        task = asyncio.create_task(reader(), name=f"okx-truth:{key}")
        _truth_inflight[inflight_key] = task
        with _truth_metrics_lock:
            _truth_metrics["underlying_requests"] = int(_truth_metrics.get("underlying_requests", 0) or 0) + 1
        def _cleanup(done_task, *, _key=inflight_key):
            if _truth_inflight.get(_key) is done_task:
                _truth_inflight.pop(_key, None)
        task.add_done_callback(_cleanup)
    else:
        with _truth_metrics_lock:
            _truth_metrics["coalesced_requests"] += 1
    try:
        value = await asyncio.shield(task)
    except BaseException:
        with _truth_metrics_lock:
            _truth_metrics["failed_reads"] += 1
        raise
    usable = cacheable is None or bool(cacheable(value))
    with _truth_metrics_lock:
        _truth_metrics["successful_reads" if usable else "failed_reads"] += 1
    if usable:
        _truth_cache[key] = {"value": deepcopy(value), "ts": time.monotonic()}
    return deepcopy(value)


def _truth_priority_tier(priority: str | None) -> str:
    """Map callers to explicit freshness contracts for truth coalescing."""
    value = str(priority or "").strip().lower()
    if value in {"status", "historical", "reconciliation", "p2", "background", "enrichment"}:
        return "p2"
    if value in {"p1", "leverage_truth", "mutation_readiness"}:
        return "p1"
    return "p0"


def _truth_join_keys(domain: str, tier: str) -> tuple[str, ...]:
    """Return only stronger contracts that satisfy this caller's semantics."""
    if tier == "p2":
        return (f"{domain}:p0", f"{domain}:p1")
    if tier == "p1":
        return (f"{domain}:p0",)
    return ()


def _truth_health_snapshot() -> dict:
    with _truth_metrics_lock:
        metrics = dict(_truth_metrics)
    metrics["inflight"] = len(_truth_inflight)
    return metrics


def _record_protection_latency(duration_sec: float) -> None:
    with _protection_latency_lock:
        _protection_latency["read_count"] += 1
        _protection_latency["total_sec"] += max(float(duration_sec), 0.0)
        _protection_latency["max_sec"] = max(_protection_latency["max_sec"], max(float(duration_sec), 0.0))

_CRITICAL_REST_PATHS = frozenset({
    "/api/v5/account/balance",
    "/api/v5/account/positions",
    "/api/v5/account/config",
    "/api/v5/account/max-size",
    "/api/v5/account/leverage-info",
    "/api/v5/account/trade-fee",
    "/api/v5/trade/orders-pending",
    "/api/v5/trade/orders-algo-pending",
    "/api/v5/trade/order",
    "/api/v5/trade/fills",
    "/api/v5/account/bills",
    "/api/v5/account/positions-history",
    "/api/v5/trade/order-algo",
    "/api/v5/trade/cancel-order",
    "/api/v5/trade/cancel-algos",
})


def _get_background_request_gate() -> asyncio.Semaphore:
    global _background_request_gate
    if _background_request_gate is None:
        _background_request_gate = asyncio.Semaphore(_BACKGROUND_REQUEST_LIMIT)
    return _background_request_gate


def _get_critical_request_gate() -> asyncio.Semaphore:
    global _critical_request_gate
    if _critical_request_gate is None:
        _critical_request_gate = asyncio.Semaphore(_CRITICAL_REQUEST_LIMIT)
    return _critical_request_gate


def _get_mutation_request_gate() -> asyncio.Semaphore:
    global _mutation_request_gate
    if _mutation_request_gate is None:
        _mutation_request_gate = asyncio.Semaphore(_MUTATION_REQUEST_LIMIT)
    return _mutation_request_gate


def _get_leverage_request_gate() -> asyncio.Semaphore:
    global _leverage_request_gate
    if _leverage_request_gate is None:
        _leverage_request_gate = asyncio.Semaphore(_LEVERAGE_REQUEST_LIMIT)
    return _leverage_request_gate


def _get_reconciliation_request_gate() -> asyncio.Semaphore:
    global _reconciliation_request_gate
    if _reconciliation_request_gate is None:
        _reconciliation_request_gate = asyncio.Semaphore(_RECONCILIATION_REQUEST_LIMIT)
    return _reconciliation_request_gate


def _acquire_critical_generation() -> _CriticalReadGeneration:
    global _critical_generation_seq, _critical_current_generation
    with _critical_generation_lock:
        if (
            _critical_current_generation is None
            or _critical_current_generation.client is not _critical_http_client
        ):
            _critical_generation_seq += 1
            generation = _CriticalReadGeneration(_critical_generation_seq, _critical_http_client)
            _critical_generations[generation.generation_id] = generation
            _critical_current_generation = generation
            _critical_recovery["current_generation"] = generation.generation_id
        generation = _critical_current_generation
        generation.active_requests += 1
        return generation


async def _release_critical_generation(generation: _CriticalReadGeneration) -> None:
    close_client = None
    with _critical_generation_lock:
        generation.active_requests = max(generation.active_requests - 1, 0)
        if generation.retired and generation.active_requests == 0 and not generation.closed:
            generation.closed = True
            close_client = generation.client
            _critical_generations.pop(generation.generation_id, None)
    if close_client is not None:
        try:
            await close_client.aclose()
        except Exception:
            logger.debug("Retired critical read client close failed", exc_info=True)


async def _retire_critical_generation(generation: _CriticalReadGeneration, reason: str) -> None:
    global _critical_generation_seq, _critical_current_generation, _critical_http_client
    close_client = None
    with _critical_generation_lock:
        if _critical_current_generation is not generation:
            return
        _critical_generation_seq += 1
        replacement = _CriticalReadGeneration(
            _critical_generation_seq,
            _new_critical_http_client(),
        )
        generation.retired = True
        generation.close_pending = True
        _critical_generations[replacement.generation_id] = replacement
        _critical_current_generation = replacement
        _critical_http_client = replacement.client
        _critical_recovery.update({
            "current_generation": replacement.generation_id,
            "recovery_count": int(_critical_recovery.get("recovery_count", 0) or 0) + 1,
            "retired_generation_count": int(_critical_recovery.get("retired_generation_count", 0) or 0) + 1,
            "last_recovery_reason": str(reason or "critical_read_transport_failure")[:240],
            "last_recovery_timestamp": datetime.now(timezone.utc).isoformat(),
        })
        if generation.active_requests == 0 and not generation.closed:
            generation.closed = True
            close_client = generation.client
            _critical_generations.pop(generation.generation_id, None)
    try:
        _latency.record_client_recovery(
            _critical_recovery["current_generation"],
            _critical_recovery["recovery_count"],
        )
    except Exception:
        pass
    if close_client is not None:
        try:
            await close_client.aclose()
        except Exception:
            logger.debug("Retired critical read client close failed", exc_info=True)


def _record_process_request_health(
    request_class: str,
    duration_sec: float,
    *,
    retries: int,
    pool_timeouts: int,
    outcome: str,
    connect_timeouts: int = 0,
    read_timeouts: int = 0,
    http_errors: int = 0,
    business_errors: int = 0,
    queue_wait_sec: float = 0.0,
) -> None:
    """Record one logical request in the process-global bounded health view."""
    bucket_name = str(request_class or "background")
    duration = max(float(duration_sec), 0.0)
    outcome_key = str(outcome or "error").lower()
    try:
        with _process_health_lock:
            if bucket_name not in _process_rest_health:
                bucket_name = "background"
            bucket = _process_rest_health[bucket_name]
            bucket["request_count"] += 1
            bucket["retry_count"] += max(int(retries), 0)
            bucket["physical_request_count"] += 1 + max(int(retries), 0)
            bucket["pool_timeout_count"] += max(int(pool_timeouts), 0)
            bucket["total_wall_time_sec"] += duration
            bucket["max_wall_time_sec"] = max(bucket["max_wall_time_sec"], duration)
            if outcome_key in {"timeout", "pool_timeout", "connect_timeout", "read_timeout"}:
                bucket["timeout_count"] += 1
            if outcome_key != "ok":
                bucket["error_count"] += 1
            capacity = _process_rest_capacity.get(bucket_name, _process_rest_capacity["background"])
            capacity["connect_timeout_count"] += max(int(connect_timeouts), 0)
            capacity["read_timeout_count"] += max(int(read_timeouts), 0)
            capacity["http_error_count"] += max(int(http_errors), 0)
            capacity["business_error_count"] += max(int(business_errors), 0)
    except Exception:
        # Health accounting is observational and must never affect a request.
        logger.debug("Process-global REST health recording failed", exc_info=True)


def _record_process_request_start(request_class: str, queue_wait_sec: float) -> None:
    with _process_health_lock:
        bucket = _process_rest_capacity.get(request_class, _process_rest_capacity["background"])
        bucket["active_requests"] += 1
        bucket["peak_active_requests"] = max(bucket["peak_active_requests"], bucket["active_requests"])
        bucket["queue_wait_total_sec"] += max(float(queue_wait_sec), 0.0)
        bucket["queue_wait_max_sec"] = max(bucket["queue_wait_max_sec"], max(float(queue_wait_sec), 0.0))


def _record_process_rate_wait(request_class: str, wait_sec: float) -> None:
    with _process_health_lock:
        bucket = _process_rest_capacity.get(request_class, _process_rest_capacity["background"])
        bucket["rate_wait_total_sec"] += max(float(wait_sec), 0.0)
        bucket["rate_wait_max_sec"] = max(bucket["rate_wait_max_sec"], max(float(wait_sec), 0.0))


def _record_process_request_end(request_class: str) -> None:
    with _process_health_lock:
        bucket = _process_rest_capacity.get(request_class, _process_rest_capacity["background"])
        bucket["active_requests"] = max(bucket["active_requests"] - 1, 0)


async def _acquire_governed_slot(
    method: str,
    path: str,
    *,
    params: dict | None,
    data: dict | None,
    priority: str | None,
    request_gate: asyncio.Semaphore,
):
    """Pair final rate admission with an immediately usable HTTP slot.

    The governor only consumes its allowance after the pool slot is available.
    If another request wins the final rate race, the slot is released before
    retrying, so neither scarce resource is held while waiting for the other.
    """
    ticket = await process_rest_rate_governor.prepare(
        method, path, params=params, data=data, priority=priority,
    )
    pool_wait_sec = 0.0
    try:
        while True:
            await process_rest_rate_governor.wait_until_admissible(ticket)
            pool_started = time.monotonic()
            acquired = False
            try:
                await asyncio.wait_for(request_gate.acquire(), timeout=0.05)
                acquired = True
            except asyncio.TimeoutError:
                pool_wait_sec += time.monotonic() - pool_started
                continue
            pool_wait_sec += time.monotonic() - pool_started
            if await process_rest_rate_governor.try_admit(ticket):
                ready_at = max(
                    (waiter.rate_ready_at for waiter in ticket.waiters),
                    default=ticket.created_at,
                )
                return ticket, pool_wait_sec, max(ready_at - ticket.created_at, 0.0)
            if acquired:
                request_gate.release()
    except BaseException:
        await process_rest_rate_governor.cancel(ticket)
        raise


def get_critical_rest_health_snapshot() -> dict:
    """Return bounded process-global REST and generation health.

    ``client_recovery`` remains as a compatibility alias for existing
    diagnostics; ``recovery`` is the canonical process-global field.
    ``retired_generation_count`` is cumulative, while
    ``active_retired_generations`` counts retired clients still waiting to
    drain and close.
    """
    with _critical_generation_lock:
        recovery = dict(_critical_recovery)
        active_retired = sum(
            1 for generation in _critical_generations.values() if generation.retired
        )
        recovery["active_retired_generations"] = active_retired
        recovery["retired_generations_pending_close"] = active_retired
        recovery["active_generations"] = sum(
            1 for generation in _critical_generations.values() if not generation.retired
        )
    with _process_health_lock:
        health = {
            key: dict(value) for key, value in _process_rest_health.items()
        }
        capacity = {key: dict(value) for key, value in _process_rest_capacity.items()}
    health["recovery"] = dict(recovery)
    health["client_recovery"] = dict(recovery)
    health["coalescing"] = _truth_health_snapshot()
    health["capacity"] = capacity
    with _protection_latency_lock:
        health["protection_latency"] = dict(_protection_latency)
    health["topology"] = get_rest_topology_snapshot()
    health["rate_governor"] = get_process_rest_rate_snapshot()
    with _truth_metrics_lock:
        health["scope"] = dict(_rest_scope)
    return health


def get_rest_topology_snapshot() -> dict:
    """Return the configured REST capacity topology without making a request."""
    return {
        "background": {"connections": _BACKGROUND_REQUEST_LIMIT, "timeout_sec": 20.0, "connect_timeout_sec": 5.0, "retries": 4},
        "critical_read": {"connections": _CRITICAL_REQUEST_LIMIT, "timeout_sec": 20.0, "connect_timeout_sec": 5.0, "retries": 4},
        "mutation": {"connections": _MUTATION_REQUEST_LIMIT, "timeout_sec": 20.0, "connect_timeout_sec": 5.0, "retries": 1},
        "leverage_truth": {"connections": _LEVERAGE_REQUEST_LIMIT, "timeout_sec": 20.0, "connect_timeout_sec": 5.0, "retries": 4},
        "reconciliation": {"connections": _RECONCILIATION_REQUEST_LIMIT, "timeout_sec": 20.0, "connect_timeout_sec": 5.0, "retries": 4},
    }


def _is_critical_request(method: str, path: str, priority: str | None = None) -> bool:
    requested = str(priority or "").strip().lower()
    if requested in {"critical", "execution", "p0", "p1"}:
        return True
    if requested in {"background", "enrichment", "historical", "reconciliation", "status", "p2"}:
        return False
    return method.upper() != "GET" or path.split("?", 1)[0] in _CRITICAL_REST_PATHS

# Taker-volume circuit breaker: OKX /rubik/stat/taker-volume returns 400 for
# most coins. After the first 400 we disable the endpoint for the session so
# FUJIN stops hammering OKX with hundreds of doomed requests per minute.
_taker_volume_ok: bool = True
_taker_volume_backoff_until: float = 0.0
_long_short_ratio_backoff_until: float = 0.0
_request_429_count: int = 0
_endpoint_pressure: dict[str, dict[str, float | int]] = {}
_PUBLIC_SIGNAL_TTL_SEC = 900.0      # 1H OKX public stats do not need per-minute refresh
_PUBLIC_SIGNAL_BACKOFF_SEC = 900.0  # cool off for 15m after a 429
_LEVERAGE_CAP_RE = re.compile(r"limit of\s*(\d+)x", re.IGNORECASE)


async def close_http_clients() -> None:
    """Close persistent OKX clients during orderly process shutdown."""
    clients = [_http_client, _critical_http_client, _leverage_http_client, _mutation_http_client, _reconciliation_http_client]
    with _critical_generation_lock:
        clients.extend(generation.client for generation in _critical_generations.values())
        _critical_generations.clear()
    seen: set[int] = set()
    for client in clients:
        if id(client) in seen:
            continue
        seen.add(id(client))
        close = getattr(client, "aclose", None)
        if close is not None:
            try:
                await close()
            except Exception:
                logger.debug("OKX client close failed", exc_info=True)


def _get_mutation_readiness_lock() -> asyncio.Lock:
    global _mutation_readiness_lock
    if _mutation_readiness_lock is None:
        _mutation_readiness_lock = asyncio.Lock()
    return _mutation_readiness_lock

OKX_ERROR_HINTS: dict[str, str] = {
    "51001": "Parameter error — check symbol, side, size, or payload format.",
    "51008": "Position does not exist — local state is stale or already closed.",
    "51009": "Insufficient margin — reduce size or leverage before retrying.",
    "51010": "Position already closed — duplicate close request or stale registry.",
    "51011": "Order not found — attached algo may have expired or been cancelled.",
    "51019": "Order size too small — raise the minimum notional/contract size.",
    "51020": "Order size too large — reduce notional or leverage.",
    "51022": "Too many open orders — cool down or reduce concurrency.",
    "51025": "Invalid leverage — requested leverage is not allowed for this instrument.",
    "51029": "Market unavailable — instrument is not currently tradable.",
}


OKX_CATEGORY_HINTS: dict[str, str] = {
    "insufficient_margin": "Insufficient margin - reduce concurrency, leverage, or per-trade notional.",
    "invalid_leverage": "Invalid leverage - instrument cap is below the requested leverage.",
    "min_size": "Order size too small - contract floor was not met.",
    "max_size": "Order size too large - reduce notional or allow preflight downsize.",
    "too_many_orders": "Too many open orders - reduce concurrency or cooldown frequency.",
    "market_unavailable": "Instrument unavailable - skip and retry later.",
    "position_missing": "Position does not exist - exchange and local state are out of sync.",
    "already_closed": "Position already closed - duplicate close or stale local state.",
}


class OKXOrderError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        category: str = "",
        code: str = "",
        s_code: str = "",
        hint: str = "",
        requested_leverage: int | None = None,
        effective_leverage: int | None = None,
    ) -> None:
        super().__init__(message)
        self.category = category
        self.code = code
        self.s_code = s_code
        self.hint = hint
        self.requested_leverage = requested_leverage
        self.effective_leverage = effective_leverage


class OKXDefinitiveRejection(OKXOrderError):
    """The exchange returned a business response proving mutation rejection."""

    def __init__(
        self,
        message: str,
        *,
        code: str = "",
        s_code: str = "",
        hint: str = "",
        evidence: dict | None = None,
    ) -> None:
        super().__init__(message, category="definitive_rejection", code=code, s_code=s_code, hint=hint)
        self.evidence = dict(evidence or {})


def _require_place_order_acknowledgement(result: object, client_order_id: str = "") -> dict:
    """Enforce the OKX /trade/order acknowledgement contract exactly.

    The generic mutation classifier also supports algo endpoints, where an
    ``algoId`` is sufficient.  A regular order acknowledgement is stricter:
    it requires top code=0, one usable item with sCode=0, a nonempty ordId,
    and no conflicting returned client identity.
    """
    if not isinstance(result, dict) or str(result.get("code", "")) != "0":
        raise RuntimeError("OKX order acknowledgement requires reconciliation: top_level_code")
    items = result.get("data")
    if not isinstance(items, list) or len(items) != 1 or not isinstance(items[0], dict):
        raise RuntimeError("OKX order acknowledgement requires reconciliation: usable_item_missing")
    item = dict(items[0])
    if str(item.get("sCode", "")) != "0":
        raise RuntimeError("OKX order acknowledgement requires reconciliation: item_sCode_not_zero")
    if not str(item.get("ordId", "") or "").strip():
        raise RuntimeError("OKX order acknowledgement requires reconciliation: ordId_missing")
    returned_client_id = str(item.get("clOrdId", "") or "").strip()
    if client_order_id and not returned_client_id:
        raise RuntimeError("OKX order acknowledgement requires reconciliation: client_id_missing")
    if client_order_id and returned_client_id != client_order_id:
        raise RuntimeError("OKX order acknowledgement requires reconciliation: client_id_mismatch")
    return item


_ORDER_NOT_FOUND_CODE = "100011"
_ORDER_NOT_FOUND_MESSAGES = {"order does not exist", "order not exist"}


def _order_not_found_evidence(result: dict) -> tuple[bool, str, str]:
    """Recognize only OKX's explicit order-absence business response."""
    rows = result.get("data", []) if isinstance(result, dict) else []
    candidates = [result]
    if isinstance(rows, list):
        candidates.extend(row for row in rows if isinstance(row, dict))

    fallback_message = ""
    for candidate in candidates:
        code = str(candidate.get("sCode", candidate.get("code", "")) or "")
        message = str(candidate.get("sMsg", candidate.get("msg", "")) or "")
        normalized = " ".join(message.strip().lower().split())
        if code == _ORDER_NOT_FOUND_CODE:
            return True, code, message
        if normalized in _ORDER_NOT_FOUND_MESSAGES:
            fallback_message = message

    return bool(fallback_message), "", fallback_message


async def get_universe() -> set[str]:
    global _universe_cache
    if not _universe_cache:
        _universe_cache = await get_full_universe()
    return _universe_cache


async def refresh_universe() -> set[str]:
    global _universe_cache
    _universe_cache = await get_full_universe()
    return _universe_cache


def _coin_from_inst(inst_id: str) -> str:
    """Parse base coin from OKX instId string, e.g. 'BTC-USDT-SWAP' → 'BTC'."""
    return inst_id.replace("-USDT-SWAP", "").replace("-USDT", "").upper()


def _extract_leverage_cap(*messages: str) -> int | None:
    for message in messages:
        if not message:
            continue
        match = _LEVERAGE_CAP_RE.search(message)
        if match:
            try:
                cap = int(match.group(1))
            except Exception:
                continue
            if cap > 0:
                return cap
    return None


def _safe_float(value, default: float = 0.0) -> float:
    try:
        if value in (None, ""):
            return default
        return float(value)
    except Exception:
        return default


def _fallback_leverages(start: int) -> list[int]:
    try:
        import settings_store as _settings
        configured_max = int(_settings.get_val("leverage_max", 15) or 15)
    except Exception:
        configured_max = 15
    lev = max(int(start or 0), 5)
    original = lev
    lev = min(lev, configured_max)
    if lev % 5 != 0:
        lev -= lev % 5
    fallbacks: list[int] = []
    if original > lev and lev >= 5:
        fallbacks.append(lev)
    while lev > 5:
        lev -= 5
        if lev >= 5:
            fallbacks.append(lev)
    return fallbacks


def _classify_okx_error(code: str, s_code: str, msg: str, s_msg: str) -> str:
    text = f"{msg} {s_msg}".lower()
    if code == "51009" or "insufficient margin" in text or "too low for borrowing" in text:
        return "insufficient_margin"
    if code == "51025" or s_code == "51186" or "invalid leverage" in text or "exceeds the platform limit" in text:
        return "invalid_leverage"
    if code == "51019" or "order size too small" in text:
        return "min_size"
    if code == "51020":
        return "max_size"
    if code == "51022":
        return "too_many_orders"
    if code == "51029":
        return "market_unavailable"
    if code == "51008" or "position does not exist" in text:
        return "position_missing"
    if code == "51010" or "already closed" in text:
        return "already_closed"
    return ""


def _okx_error_payload(result: dict) -> tuple[str, str, str, str, str]:
    code = str(result.get("code", "") or "")
    msg = str(result.get("msg", "") or "")
    data_list = result.get("data", [])
    s_code = str(data_list[0].get("sCode", "") if data_list else "")
    s_msg = str(data_list[0].get("sMsg", "") if data_list else "")
    category = _classify_okx_error(code, s_code, msg, s_msg)
    hint = OKX_CATEGORY_HINTS.get(category, "") or OKX_ERROR_HINTS.get(code, "") or OKX_ERROR_HINTS.get(s_code, "")
    if not hint and _classify_okx_error(code, s_code, msg, s_msg) == "invalid_leverage":
        hint = "Invalid leverage — instrument cap is below the requested leverage."
    return code, s_code, msg, s_msg, hint


def _public_cache_get(cache: dict[str, dict], coin: str, *, ttl: float = _PUBLIC_SIGNAL_TTL_SEC):
    entry = cache.get(coin.upper())
    if not entry:
        return None
    if time.monotonic() - float(entry.get("ts", 0.0) or 0.0) > ttl:
        return None
    return entry.get("value")


def _public_cache_get_stale(cache: dict[str, dict], coin: str):
    entry = cache.get(coin.upper())
    if not entry:
        return None
    return entry.get("value")


def _public_cache_set(cache: dict[str, dict], coin: str, value) -> None:
    cache[coin.upper()] = {"value": value, "ts": time.monotonic()}


def _public_signal_fallback(cache: dict[str, dict], coin: str, default: dict, reason: str) -> dict:
    cached = _public_cache_get_stale(cache, coin)
    value = dict(cached) if isinstance(cached, dict) else dict(default)
    value.setdefault("buy", 0.0)
    value.setdefault("sell", 0.0)
    value["degraded"] = True
    value["source"] = "stale_cache" if cached is not None else "neutral_fallback"
    value["degraded_reason"] = reason
    return value


def _endpoint_backoff_remaining(path: str) -> float:
    keys = [path]
    if not str(path).upper().startswith(("GET ", "POST ")):
        keys.append(f"GET {path}")
    now = time.monotonic()
    remaining = 0.0
    for key in keys:
        state = _endpoint_pressure.get(key, {}) or {}
        remaining = max(remaining, float(state.get("backoff_until", 0.0) or 0.0) - now)
    return max(remaining, 0.0)


def _mark_endpoint_backoff(path: str, seconds: float) -> None:
    key = path if str(path).upper().startswith(("GET ", "POST ")) else f"GET {path}"
    state = _endpoint_pressure.setdefault(key, {"count": 0, "429": 0, "last": 0.0, "backoff_until": 0.0})
    state["429"] = int(state.get("429", 0) or 0) + 1
    state["last"] = time.monotonic()
    state["backoff_until"] = max(float(state.get("backoff_until", 0.0) or 0.0), time.monotonic() + max(seconds, 0.0))


def _get_taker_volume_lock(coin: str | None = None) -> asyncio.Lock:
    global _taker_volume_lock
    if coin is not None:
        key = str(coin).upper()
        lock = _taker_volume_locks.get(key)
        if lock is None:
            lock = asyncio.Lock()
            _taker_volume_locks[key] = lock
        return lock
    if _taker_volume_lock is None:
        _taker_volume_lock = asyncio.Lock()
    return _taker_volume_lock


def _get_long_short_ratio_lock(coin: str | None = None) -> asyncio.Lock:
    global _long_short_ratio_lock
    if coin is not None:
        key = str(coin).upper()
        lock = _long_short_ratio_locks.get(key)
        if lock is None:
            lock = asyncio.Lock()
            _long_short_ratio_locks[key] = lock
        return lock
    if _long_short_ratio_lock is None:
        _long_short_ratio_lock = asyncio.Lock()
    return _long_short_ratio_lock


def _ticker_priority_rank(priority: str) -> int:
    value = str(priority or "").strip().lower()
    if value in {"critical", "execution", "p0"}:
        return 0
    if value in {"p1", "status", "operator"}:
        return 1
    return 2


def _ticker_priority_tier(priority: str) -> str:
    return f"p{_ticker_priority_rank(priority)}"


def _ticker_priority_label(rank: int) -> str:
    return {0: "p0", 1: "p1", 2: "p2"}.get(int(rank), "p2")


def _ticker_observed_at(raw_ts: object) -> str:
    try:
        timestamp_ms = float(raw_ts or 0.0)
        if timestamp_ms > 0:
            return datetime.fromtimestamp(timestamp_ms / 1000.0, tz=timezone.utc).isoformat()
    except (TypeError, ValueError, OSError, OverflowError):
        pass
    return datetime.now(timezone.utc).isoformat()


def _ticker_public_value(cached: dict, *, stale: bool, refresh_error: str = "") -> dict:
    value = dict(cached.get("value", {}) or {})
    observed_at = str(cached.get("observed_at", "") or "")
    age_sec = None
    if observed_at:
        try:
            age_sec = max(
                time.time() - datetime.fromisoformat(observed_at.replace("Z", "+00:00")).timestamp(),
                0.0,
            )
        except (TypeError, ValueError, OverflowError):
            age_sec = None
    value.update({
        "observed_at": observed_at,
        "source": str(cached.get("source", "okx_rest") or "okx_rest"),
        "stale": bool(stale),
        "freshness_status": "STALE" if stale else "FRESH",
        "age_sec": age_sec,
    })
    if refresh_error:
        value["refresh_error"] = refresh_error
    return value


def update_ticker_from_ws(inst_id: str, raw: list | dict, connection_generation: int = 0) -> None:
    """Publish a public WS ticker without making it satisfy P0 REST truth."""
    if isinstance(raw, dict):
        payload = dict(raw)
    else:
        return
    key = str(inst_id or payload.get("instId", "")).upper()
    if not key:
        return
    previous = _ticker_cache.get(key, {})
    if int(previous.get("connection_generation", 0) or 0) > int(connection_generation or 0):
        return
    last = float(payload.get("last", 0) or 0)
    base_volume = float(payload.get("volCcy24h", 0) or 0)
    _ticker_cache[key] = {
        "value": {
            "last": last,
            "bid": float(payload.get("bidPx", 0) or 0),
            "ask": float(payload.get("askPx", 0) or 0),
            "bidPx": float(payload.get("bidPx", 0) or 0),
            "askPx": float(payload.get("askPx", 0) or 0),
            "vol24h": float(payload.get("vol24h", 0) or 0),
            "volCcy24h": base_volume,
            "quoteVolume24h": base_volume * last,
        },
        "ts": time.monotonic(),
        "observed_at": _ticker_observed_at(payload.get("ts")),
        "source": "okx_public_websocket",
        "stale": False,
        "refresh_status": "fresh",
        "connection_generation": int(connection_generation or 0),
        # Scanner-grade WS data must not satisfy execution/P0 callers.
        "priority_rank": 2,
    }


def get_ticker_coalescing_snapshot() -> dict:
    """Return bounded ticker single-flight telemetry for scan/control reports."""
    with _ticker_metrics_lock:
        result = dict(_ticker_metrics)
    logical = int(result.get("logical_callers", 0) or 0)
    result["coalescing_ratio"] = round(
        int(result.get("coalesced_callers", 0) or 0) / logical if logical else 0.0,
        4,
    )
    result["inflight"] = len(_ticker_inflight)
    return result


def reset_ticker_coalescing_metrics() -> None:
    with _ticker_metrics_lock:
        for key in _ticker_metrics:
            _ticker_metrics[key] = 0


class OKXClient:
    def __init__(self):
        self._leverage_truth_cache = LeverageTruthCache()

    def _order_skip_reason_key(self) -> int:
        try:
            task = asyncio.current_task()
        except RuntimeError:
            task = None
        return id(task) if task is not None else 0

    def _set_last_order_skip_reason(self, reason: str) -> None:
        if not hasattr(self, "_order_skip_reasons"):
            self._order_skip_reasons = {}
        key = self._order_skip_reason_key()
        text = str(reason or "").strip()
        if text:
            self._order_skip_reasons[key] = text
        else:
            self._order_skip_reasons.pop(key, None)

    def consume_last_order_skip_reason(self) -> str:
        if not hasattr(self, "_order_skip_reasons"):
            self._order_skip_reasons = {}
        key = self._order_skip_reason_key()
        return str(self._order_skip_reasons.pop(key, "") or "")

    # ── Auth ──────────────────────────────────────────────────────────────────
    @staticmethod
    def _timestamp() -> str:
        return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

    @staticmethod
    def _sign(ts: str, method: str, path: str, body: str = "") -> str:
        msg = f"{ts}{method.upper()}{path}{body}"
        sig = hmac.new(OKX_API_SECRET.encode(), msg.encode(), hashlib.sha256).digest()
        return base64.b64encode(sig).decode()

    def _headers(self, method: str, path: str, body: str = "") -> dict:
        ts = self._timestamp()
        headers = {
            "OK-ACCESS-KEY":        OKX_API_KEY,
            "OK-ACCESS-SIGN":       self._sign(ts, method, path, body),
            "OK-ACCESS-TIMESTAMP":  ts,
            "OK-ACCESS-PASSPHRASE": OKX_API_PASSPHRASE,
            "Content-Type":         "application/json",
        }
        if os.getenv("OKX_DEMO_TRADING", "false").strip().lower() in {"1", "true", "yes", "on"}:
            headers["x-simulated-trading"] = "1"
        return headers

    async def _request(
        self, method: str, path: str,
        params: dict | None = None,
        data:   dict | None = None,
        *,
        priority: str | None = None,
        before_send=None,
    ) -> dict:
        global _request_429_count, _endpoint_pressure
        body_str = json.dumps(data) if data else ""
        endpoint_key = f"{method.upper()} {path}"
        ep_state = _endpoint_pressure.setdefault(endpoint_key, {"count": 0, "429": 0, "last": 0.0, "backoff_until": 0.0})
        ep_state["count"] = int(ep_state.get("count", 0) or 0) + 1
        ep_state["last"] = time.monotonic()
        if params and method.upper() == "GET":
            from urllib.parse import urlencode
            sign_path = path + "?" + urlencode(params)
        else:
            sign_path = path
        attempts = 4 if method.upper() == "GET" else 1
        request_started = time.monotonic()
        retry_count = 0
        pool_timeout_count = 0
        connect_timeout_count = 0
        read_timeout_count = 0
        http_error_count = 0
        business_error_count = 0
        outcome = "error"
        rate_bucket = ""
        requested_priority = str(priority or "").strip().lower()
        critical = _is_critical_request(method, path, priority)
        historical_read = method.upper() == "GET" and requested_priority in {"historical", "reconciliation", "status", "p2"}
        request_class = "other" if historical_read else ("critical" if critical else "background")
        leverage_truth_read = (
            method.upper() == "GET"
            and str(priority or "").strip().lower() == "leverage_truth"
        )
        mutation_readiness = (
            method.upper() == "GET"
            and str(priority or "").strip().lower() == "mutation_readiness"
        )
        critical_read = (
            critical
            and method.upper() == "GET"
            and not mutation_readiness
            and not leverage_truth_read
        )
        process_request_class = (
            "mutation" if method.upper() != "GET"
            else "leverage_truth" if leverage_truth_read
            else "reconciliation" if historical_read
            else "critical_read" if critical_read
            else "background"
        )
        governor_priority = str(priority or "").strip() or (
            "P0" if method.upper() != "GET" or critical else "P2"
        )
        if leverage_truth_read or mutation_readiness:
            governor_priority = "P1"
        elif historical_read:
            governor_priority = "P2"
        critical_generation = _acquire_critical_generation() if critical_read else None
        if mutation_readiness:
            request_client = _mutation_http_client
            request_gate = _get_mutation_request_gate()
        elif leverage_truth_read:
            request_client = _leverage_http_client
            request_gate = _get_leverage_request_gate()
        elif historical_read:
            request_client = _reconciliation_http_client
            request_gate = _get_reconciliation_request_gate()
        elif critical_read:
            request_client = critical_generation.client
            request_gate = _get_critical_request_gate()
        elif critical:
            request_client = _mutation_http_client
            request_gate = _get_mutation_request_gate()
        else:
            request_client = _http_client
            request_gate = _get_background_request_gate()
        recovery_reason = ""
        try:
            for attempt in range(1, attempts + 1):
                try:
                    # Every retry prepares a new non-consuming admission.  The
                    # allowance is consumed only after a pool slot is usable;
                    # this prevents a material pool wait from idling a rate
                    # token and prevents a pool slot from waiting on the rate.
                    ticket, pool_wait_sec, rate_wait_sec = await _acquire_governed_slot(
                        method, path, params=params, data=data, priority=governor_priority,
                        request_gate=request_gate,
                    )
                    rate_bucket = resolve_rest_rate_buckets(method, path, params, data)[0][1]
                    _record_process_rate_wait(process_request_class, rate_wait_sec)
                    _record_process_request_start(process_request_class, pool_wait_sec)
                    try:
                        # Authentication timestamps are short-lived. Build the
                        # signature after both queues, immediately before the
                        # actual transport attempt.
                        headers = self._headers(method, sign_path, body_str)
                        if before_send is not None:
                            before_send()
                        resp = await request_client.request(
                            method, BASE_URL + path,
                            headers=headers, params=params,
                            content=body_str or None,
                        )
                    finally:
                        _record_process_request_end(process_request_class)
                        request_gate.release()
                    if (
                        method.upper() == "GET"
                        and resp.status_code in {408, 429, 500, 502, 503, 504}
                        and attempt < attempts
                    ):
                        retry_count = attempt
                        if resp.status_code == 429:
                            _request_429_count += 1
                            ep_state["429"] = int(ep_state.get("429", 0) or 0) + 1
                            ep_state["backoff_until"] = time.monotonic() + _PUBLIC_SIGNAL_BACKOFF_SEC
                            process_rest_rate_governor.record_http_429(rate_bucket)
                        delay = 0.5 * (2 ** (attempt - 1))
                        logger.warning(
                            "OKX transient HTTP %s on %s %s (attempt %d/%d). Retrying in %.1fs.",
                            resp.status_code, method.upper(), path, attempt, attempts, delay,
                        )
                        await asyncio.sleep(delay)
                        continue
                    resp.raise_for_status()
                    payload = resp.json()
                    business_error_count = int(
                        isinstance(payload, dict) and str(payload.get("code", "0")) not in {"", "0"}
                    )
                    if isinstance(payload, dict) and str(payload.get("code", "")) == "50011":
                        process_rest_rate_governor.record_business_50011(rate_bucket)
                    outcome = "business_error" if business_error_count else "ok"
                    return payload
                except httpx.HTTPStatusError as exc:
                    if exc.response.status_code == 429:
                        _request_429_count += 1
                        ep_state["429"] = int(ep_state.get("429", 0) or 0) + 1
                        ep_state["backoff_until"] = time.monotonic() + _PUBLIC_SIGNAL_BACKOFF_SEC
                        process_rest_rate_governor.record_http_429(rate_bucket)
                    body_preview = ""
                    try:
                        body_preview = exc.response.text[:500]
                    except Exception:
                        body_preview = "(unavailable)"
                    if (
                        method.upper() == "GET"
                        and exc.response.status_code in {408, 429, 500, 502, 503, 504}
                        and attempt < attempts
                    ):
                        retry_count = attempt
                        delay = 0.5 * (2 ** (attempt - 1))
                        logger.warning(
                            "OKX HTTP %s on %s %s (attempt %d/%d). Retrying in %.1fs. response=%s",
                            exc.response.status_code, method.upper(), path, attempt, attempts, delay, body_preview,
                        )
                        await asyncio.sleep(delay)
                        continue
                    http_error_count += 1
                    outcome = "timeout" if exc.response.status_code == 408 else "http_error"
                    logger.error(
                        "OKX HTTP error %s %s status=%s params=%s body=%s response=%s",
                        method.upper(), path, exc.response.status_code, params, data, body_preview,
                    )
                    try:
                        payload = exc.response.json()
                    except Exception:
                        payload = None
                    if isinstance(payload, dict) and any(
                        payload.get(key) not in (None, "", [])
                        for key in ("code", "msg", "data")
                    ):
                        submitted_client_id = ""
                        if isinstance(data, dict):
                            submitted_client_id = str(
                                data.get("algoClOrdId")
                                or data.get("clOrdId")
                                or ""
                            )
                        classification = classify_mutation_response(
                            payload,
                            expected_items=0,
                            submitted_client_id=submitted_client_id,
                        )
                        code, s_code, msg, s_msg, hint = _okx_error_payload(payload)
                        if classification.outcome == MUTATION_DEFINITIVE_REJECTION:
                            business_error_count += 1
                            outcome = "business_error"
                            raise OKXDefinitiveRejection(
                                f"OKX mutation rejected: {msg or 'unknown'} (code {code or '?'}) {s_msg or ''} {hint}".strip(),
                                code=classification.top_level_code or code,
                                s_code=classification.item_s_code or s_code,
                                hint=hint,
                                evidence=classification.to_dict(),
                            ) from exc
                        # A post-boundary HTTP body can be structurally
                        # ambiguous.  Preserve reconciliation ownership rather
                        # than converting it to a local business rejection.
                        raise RuntimeError(
                            "OKX mutation response requires reconciliation: "
                            f"{classification.to_dict()}"
                        ) from exc
                    raise RuntimeError(
                        f"OKX HTTP {exc.response.status_code} on {method.upper()} {path}: {body_preview}"
                    ) from exc
                except httpx.RequestError as exc:
                    error_type = type(exc).__name__
                    if isinstance(exc, httpx.PoolTimeout):
                        pool_timeout_count += 1
                    elif isinstance(exc, httpx.ConnectTimeout):
                        connect_timeout_count += 1
                    elif isinstance(exc, httpx.ReadTimeout):
                        read_timeout_count += 1
                    if method.upper() == "GET" and attempt < attempts:
                        retry_count = attempt
                        delay = 0.5 * (2 ** (attempt - 1))
                        logger.warning(
                            "OKX request error on %s %s type=%s (attempt %d/%d): %s. Retrying in %.1fs.",
                            method.upper(), path, error_type, attempt, attempts, exc, delay,
                        )
                        await asyncio.sleep(delay)
                        continue
                    if isinstance(exc, httpx.PoolTimeout):
                        outcome = "pool_timeout"
                    elif isinstance(exc, httpx.ConnectTimeout):
                        outcome = "connect_timeout"
                    elif isinstance(exc, httpx.ReadTimeout):
                        outcome = "read_timeout"
                    else:
                        outcome = "timeout" if isinstance(exc, httpx.TimeoutException) else "error"
                    logger.error(
                        "OKX request error %s %s type=%s params=%s body=%s err=%s",
                        method.upper(), path, error_type, params, data, exc,
                    )
                    if critical_read and isinstance(exc, (httpx.PoolTimeout, httpx.ConnectTimeout)):
                        recovery_reason = f"{error_type} exhausted on {method.upper()} {path}"
                    raise RuntimeError(f"OKX request error on {method.upper()} {path} ({error_type}): {exc}") from exc
            raise RuntimeError(f"OKX request failed after retries: {method.upper()} {path}")
        finally:
            request_duration = time.monotonic() - request_started
            try:
                _record_process_request_health(
                    process_request_class,
                    request_duration,
                    retries=retry_count,
                    outcome=outcome,
                    pool_timeouts=pool_timeout_count,
                    connect_timeouts=connect_timeout_count,
                    read_timeouts=read_timeout_count,
                    http_errors=http_error_count,
                    business_errors=business_error_count,
                    queue_wait_sec=0.0,
                )
            except Exception:
                logger.debug("Process-global REST health sink failed", exc_info=True)
            try:
                _latency.record_rest(
                    path,
                    request_duration,
                    retries=retry_count,
                    physical_attempts=1 + retry_count,
                    outcome=outcome,
                    pool_timeouts=pool_timeout_count,
                    request_class=request_class,
                    critical_read=critical_read,
                    priority=governor_priority,
                    transport=(
                        "critical_generation" if critical_read else
                        "mutation" if mutation_readiness or critical else
                        "leverage_truth" if leverage_truth_read else
                        "reconciliation" if historical_read else
                        "background"
                    ),
                )
            except Exception:
                logger.debug("Scan-local REST health sink failed", exc_info=True)
            if critical_read:
                if recovery_reason and critical_generation is not None:
                    await _retire_critical_generation(critical_generation, recovery_reason)
                if critical_generation is not None:
                    await _release_critical_generation(critical_generation)

    def get_rate_limit_snapshot(self) -> dict:
        now = time.monotonic()
        endpoint_rows = []
        for path, state in sorted(_endpoint_pressure.items(), key=lambda kv: (-int(kv[1].get("429", 0) or 0), kv[0])):
            count = int(state.get("count", 0) or 0)
            hits_429 = int(state.get("429", 0) or 0)
            backoff_until = float(state.get("backoff_until", 0.0) or 0.0)
            pressure = (hits_429 / count * 100.0) if count else 0.0
            endpoint_rows.append({
                "endpoint": path,
                "count": count,
                "429": hits_429,
                "pressure_pct": round(pressure, 2),
                "backoff_active": backoff_until > now,
                "backoff_remaining_sec": max(backoff_until - now, 0.0),
            })
        stale_warnings: list[str] = []
        if time.monotonic() < _taker_volume_backoff_until:
            stale_warnings.append("taker volume cache backing off")
        if time.monotonic() < _long_short_ratio_backoff_until:
            stale_warnings.append("long/short ratio cache backing off")
        return {
            "recent_429_count": _request_429_count,
            "endpoint_pressure": endpoint_rows[:8],
            "stale_warnings": stale_warnings,
            "ticker_singleflight": get_ticker_coalescing_snapshot(),
            **get_critical_rest_health_snapshot(),
            "cache_status": {
                "taker_volume_cached": bool(_taker_volume_cache),
                "long_short_ratio_cached": bool(_long_short_ratio_cache),
                "account_snapshot_cached": bool(_account_snapshot_cache.get("value")),
                "account_snapshot_age_sec": round(now - float(_account_snapshot_cache.get("ts", 0.0) or 0.0), 1) if _account_snapshot_cache.get("value") else 0.0,
            },
        }

    async def mutation_transport_ready(self, *, force: bool = False) -> tuple[bool, str]:
        """Warm and verify the dedicated mutation transport with an auth GET.

        This never calls a mutation endpoint. A failed readiness check is
        cached briefly so candidate discovery is not coupled to a retry storm;
        the router can retry readiness on the next TTL window.
        """
        global _mutation_readiness_state
        now = time.monotonic()
        if (
            not force
            and now - float(_mutation_readiness_state.get("checked_at", 0.0) or 0.0)
            < _MUTATION_READINESS_TTL_SEC
        ):
            return bool(_mutation_readiness_state.get("ok", False)), str(
                _mutation_readiness_state.get("reason", "") or ""
            )
        async with _get_mutation_readiness_lock():
            now = time.monotonic()
            if (
                not force
                and now - float(_mutation_readiness_state.get("checked_at", 0.0) or 0.0)
                < _MUTATION_READINESS_TTL_SEC
            ):
                return bool(_mutation_readiness_state.get("ok", False)), str(
                    _mutation_readiness_state.get("reason", "") or ""
                )
            try:
                result = await self._request(
                    "GET",
                    "/api/v5/account/config",
                    priority="mutation_readiness",
                )
                ok = str(result.get("code", "")) == "0"
                reason = "" if ok else str(result.get("msg", "mutation_readiness_rejected") or "mutation_readiness_rejected")
            except Exception as exc:
                ok = False
                reason = f"{type(exc).__name__}:{exc}"
            _mutation_readiness_state = {
                "ok": ok,
                "checked_at": time.monotonic(),
                "reason": reason,
            }
            return ok, reason

    # ── Contract value helpers ─────────────────────────────────────────────────
    async def fetch_contract_value(self, coin: str) -> float:
        global _ct_val_cache
        coin = coin.upper()
        if coin in _ct_val_cache:
            return _ct_val_cache[coin]
        inst = f"{coin}-USDT-SWAP"
        try:
            result = await self._request(
                "GET", "/api/v5/public/instruments",
                params={"instType": "SWAP", "instId": inst},
            )
            data = result.get("data", [])
            if data:
                ct_val = float(data[0].get("ctVal"))
                if not math.isfinite(ct_val) or ct_val <= 0:
                    raise ValueError("invalid_authoritative_ctVal")
                _ct_val_cache[coin] = ct_val
                return ct_val
        except Exception as e:
            logger.warning("ctVal fetch failed for %s: %s", coin, e)
        if PAPER_TRADING:
            fallback = get_estimated_ct_val(coin)
            if fallback > 0:
                # Estimated paper metadata is deliberately not cached: a
                # later live-mode call in this process must never consume it
                # as authoritative exchange truth.
                return fallback
        raise ValueError(f"authoritative_contract_value_unavailable:{coin}")

    async def get_contract_cost(self, coin: str, price: float | None = None) -> float:
        if price is None:
            try:
                t     = await self.get_ticker(coin)
                price = t["last"]
            except Exception as exc:
                if not PAPER_TRADING:
                    raise ValueError(f"execution_price_unavailable:{coin}") from exc
                price = 1.0
        if float(price or 0.0) <= 0:
            raise ValueError(f"execution_price_invalid:{coin}")
        ct_val = await self.fetch_contract_value(coin)
        return ct_val * price

    async def _futures_sz(self, coin: str, size_usdt: float, price: float) -> int:
        ct_val        = await self.fetch_contract_value(coin)
        cost_per_cont = ct_val * price
        if cost_per_cont <= 0:
            raise ValueError(f"invalid_contract_cost:{coin}")
        sz = int(size_usdt / cost_per_cont)
        if sz < 1:
            raise ValueError(
                f"${size_usdt:.2f} can't buy 1 {coin} futures contract "
                f"(1 contract ≈ ${cost_per_cont:.2f})."
            )
        return sz

    async def _find_affordable_futures_coin(
        self, size_usdt: float
    ) -> tuple | None:
        candidates = get_nano_growth_coins()
        for coin in candidates:
            try:
                ticker = await self.get_execution_ticker(coin)
                price  = ticker["last"]
                if price <= 0:
                    continue
                ct_val        = await self.fetch_contract_value(coin)
                cost_per_cont = ct_val * price
                sz            = int(size_usdt / cost_per_cont)
                if sz >= 1:
                    logger.info(
                        "NANO auto-route → %s  sz=%d  ($%.4f/contract)",
                        coin, sz, cost_per_cont,
                    )
                    return coin, price, sz
            except Exception as e:
                logger.debug("NANO route skip %s: %s", coin, e)
        logger.debug("NANO routing: no affordable contract for $%.2f — skipping.", size_usdt)
        return None

    # ── Futures setup ─────────────────────────────────────────────────────────
    async def set_leverage(self, coin: str, leverage: int | None = None, pos_side: str = "long") -> dict:
        requested_lev = int(leverage or mm.get_leverage())
        inst   = mm.inst_id(coin)
        margin = mm.get_margin_mode()
        if PAPER_TRADING or not OKX_API_KEY:
            return {"requested_leverage": requested_lev, "effective_leverage": requested_lev, "paper": True}
        lev = min(requested_lev, _instrument_leverage_cap.get(inst, requested_lev))
        # OKX isolated hedge mode requires posSide in set-leverage call.
        # Without it the API returns 400 Bad Request.
        body   = {"instId": inst, "lever": str(lev), "mgnMode": margin, "posSide": pos_side}
        result = await self._request("POST", "/api/v5/account/set-leverage", data=body)
        if result.get("code") == "0":
            _instrument_leverage_cap[inst] = max(_instrument_leverage_cap.get(inst, 0), lev)
            logger.debug("Leverage set: %s %dx %s %s", inst, lev, margin, pos_side)
            return {
                "requested_leverage": requested_lev,
                "effective_leverage": lev,
                "result": result,
            }

        code, s_code, msg, s_msg, hint = _okx_error_payload(result)
        category = _classify_okx_error(code, s_code, msg, s_msg)
        cap = _extract_leverage_cap(s_msg, msg)
        if category == "invalid_leverage" and cap and cap < lev:
            _instrument_leverage_cap[inst] = cap
            logger.warning(
                "Leverage %dx rejected for %s (%s). Retrying with exchange cap %dx.",
                lev, inst, s_msg or msg, cap,
            )
            body["lever"] = str(cap)
            retry = await self._request("POST", "/api/v5/account/set-leverage", data=body)
            if retry.get("code") == "0":
                logger.info("Leverage set: %s requested=%dx effective=%dx %s", inst, requested_lev, cap, pos_side)
                return {
                    "requested_leverage": requested_lev,
                    "effective_leverage": cap,
                    "result": retry,
                }
            code, s_code, msg, s_msg, hint = _okx_error_payload(retry)
            category = _classify_okx_error(code, s_code, msg, s_msg)

        raise OKXOrderError(
            f"OKX leverage rejected: {msg or 'unknown'} (code {code or '?'}) {s_msg} {hint}".strip(),
            category=category,
            code=code,
            s_code=s_code,
            hint=hint,
            requested_leverage=requested_lev,
            effective_leverage=cap if category == "invalid_leverage" else lev,
        )

    async def get_open_interest(self, coin: str) -> float:
        if mm.is_spot():
            return 0.0
        inst = f"{coin.upper()}-USDT-SWAP"
        try:
            if PAPER_TRADING or not OKX_API_KEY:
                return 0.0
            result = await self._request("GET", "/api/v5/public/open-interest",
                params={"instType": "SWAP", "instId": inst})
            data = result.get("data", [])
            if data:
                return float(data[0].get("oiUsd", data[0].get("oi", 0)))
        except Exception as e:
            logger.debug("OI fetch failed %s: %s", coin, e)
        return 0.0

    async def get_taker_volume(self, coin: str) -> dict:
        global _taker_volume_ok, _taker_volume_backoff_until
        path = "/api/v5/rubik/stat/taker-volume"
        # Circuit breaker: endpoint returns 400 for all coins on most accounts.
        # After the first failure we stop trying to avoid flooding the event loop.
        if not _taker_volume_ok:
            return _public_signal_fallback(_taker_volume_cache, coin, {"buy": 0.0, "sell": 0.0}, "endpoint_disabled")
        cached = _public_cache_get(_taker_volume_cache, coin)
        if cached is not None:
            value = dict(cached)
            value.setdefault("degraded", False)
            value.setdefault("source", "cache")
            return value
        if time.monotonic() < _taker_volume_backoff_until or _endpoint_backoff_remaining(path) > 0:
            return _public_signal_fallback(_taker_volume_cache, coin, {"buy": 0.0, "sell": 0.0}, "rate_limit_backoff")
        async with _get_taker_volume_lock(coin):
            cached = _public_cache_get(_taker_volume_cache, coin)
            if cached is not None:
                value = dict(cached)
                value.setdefault("degraded", False)
                value.setdefault("source", "cache")
                return value
            if time.monotonic() < _taker_volume_backoff_until or _endpoint_backoff_remaining(path) > 0:
                return _public_signal_fallback(_taker_volume_cache, coin, {"buy": 0.0, "sell": 0.0}, "rate_limit_backoff")
            try:
                if PAPER_TRADING or not OKX_API_KEY:
                    return {"buy": 0.0, "sell": 0.0, "degraded": True, "source": "paper"}
                await asyncio.sleep(random.uniform(0.02, 0.12))
                result = await self._request("GET", path,
                    params={"ccy": coin.upper(), "instType": "CONTRACTS", "period": "1H"})
                data = result.get("data", [])
                if data:
                    row = data[0]
                    parsed = {
                        "buy":  float(row[1]) if len(row) > 1 else 0.0,
                        "sell": float(row[2]) if len(row) > 2 else 0.0,
                        "degraded": False,
                        "source": "okx",
                    }
                    _public_cache_set(_taker_volume_cache, coin, parsed)
                    return parsed
            except Exception as e:
                err = str(e)
                if "400" in err or "Bad Request" in err.lower() or "not supported" in err.lower():
                    _taker_volume_ok = False
                    logger.warning(
                        "Taker-volume API returned 400 — endpoint disabled for this session. "
                        "FUJIN will run without order-flow data. (Error: %s)", e
                    )
                elif "429" in err or "50011" in err or "too many requests" in err.lower():
                    _backoff = _PUBLIC_SIGNAL_BACKOFF_SEC + random.uniform(15.0, 90.0)
                    _taker_volume_backoff_until = time.monotonic() + _backoff
                    _mark_endpoint_backoff(path, _backoff)
                    logger.warning(
                        "Taker-volume API rate limited by OKX — backing off for %dm and reusing stale cache.",
                        int(_PUBLIC_SIGNAL_BACKOFF_SEC // 60),
                    )
                else:
                    logger.debug("Taker volume failed %s: %s", coin, e)
        return _public_signal_fallback(_taker_volume_cache, coin, {"buy": 0.0, "sell": 0.0}, "fetch_failed")

    async def get_long_short_ratio(self, coin: str) -> float:
        global _long_short_ratio_backoff_until
        path = "/api/v5/rubik/stat/contracts/long-short-account-ratio"
        cached = _public_cache_get(_long_short_ratio_cache, coin)
        if cached is not None:
            return float(cached)
        if time.monotonic() < _long_short_ratio_backoff_until or _endpoint_backoff_remaining(path) > 0:
            cached = _public_cache_get_stale(_long_short_ratio_cache, coin)
            return float(cached) if cached is not None else 1.0
        async with _get_long_short_ratio_lock(coin):
            cached = _public_cache_get(_long_short_ratio_cache, coin)
            if cached is not None:
                return float(cached)
            if time.monotonic() < _long_short_ratio_backoff_until or _endpoint_backoff_remaining(path) > 0:
                cached = _public_cache_get_stale(_long_short_ratio_cache, coin)
                return float(cached) if cached is not None else 1.0
            try:
                if PAPER_TRADING or not OKX_API_KEY:
                    return 1.0
                await asyncio.sleep(random.uniform(0.02, 0.12))
                result = await self._request(
                    "GET", path,
                    params={"ccy": coin.upper(), "period": "1H"})
                data = result.get("data", [])
                if data:
                    ratio = float(data[0][1]) if len(data[0]) > 1 else 1.0
                    ratio = ratio if ratio > 0 else 1.0
                    _public_cache_set(_long_short_ratio_cache, coin, ratio)
                    return ratio
            except Exception as e:
                err = str(e)
                if "429" in err or "50011" in err or "too many requests" in err.lower():
                    _backoff = _PUBLIC_SIGNAL_BACKOFF_SEC + random.uniform(15.0, 90.0)
                    _long_short_ratio_backoff_until = time.monotonic() + _backoff
                    _mark_endpoint_backoff(path, _backoff)
                    logger.warning(
                        "Long/short-ratio API rate limited by OKX — backing off for %dm and reusing stale cache.",
                        int(_PUBLIC_SIGNAL_BACKOFF_SEC // 60),
                    )
                    cached = _public_cache_get_stale(_long_short_ratio_cache, coin)
                    return float(cached) if cached is not None else 1.0
                logger.debug("L/S ratio failed %s: %s", coin, e)
        return 1.0

    async def get_funding_rate(self, coin: str) -> float:
        if mm.is_spot():
            return 0.0
        inst = f"{coin.upper()}-USDT-SWAP"
        try:
            if PAPER_TRADING or not OKX_API_KEY:
                return 0.0001
            result = await self._request(
                "GET", "/api/v5/public/funding-rate", params={"instId": inst}
            )
            data = result.get("data", [{}])
            if data:
                return float(data[0].get("fundingRate", 0))
        except Exception as e:
            logger.warning("Funding rate fetch failed %s: %s", coin, e)
        return 0.0

    # ── Balance ───────────────────────────────────────────────────────────────
    async def get_account_snapshot(self, *, force_refresh: bool = False, priority: str = "P1") -> dict:
        global _account_snapshot_cache
        if PAPER_TRADING or not OKX_API_KEY:
            observed_at = datetime.now(timezone.utc).isoformat()
            return {
                "total_eq_usdt": 500.0,
                "avail_eq_usdt": 500.0,
                "usdt_avail_bal": 500.0,
                "usdt_cash_bal": 500.0,
                "usdt_frozen_bal": 0.0,
                "mgn_ratio": 0.0,
                "observed_at": observed_at,
                "exchange_snapshot_at": observed_at,
                "received_at": observed_at,
                "details": {
                    "USDT": {
                        "availBal": 500.0,
                        "cashBal": 500.0,
                        "availEq": 500.0,
                        "eqUsd": 500.0,
                        "frozenBal": 0.0,
                    }
                },
            }

        now = time.monotonic()
        cached = _account_snapshot_cache.get("value")
        if not force_refresh and cached and (now - float(_account_snapshot_cache.get("ts", 0.0) or 0.0)) <= _ACCOUNT_SNAPSHOT_TTL_SEC:
            with _truth_metrics_lock:
                _truth_metrics["cache_hits"] += 1
            return deepcopy(cached)

        async def fetch():
            global _account_snapshot_cache
            result = await self._request("GET", "/api/v5/account/balance", priority=priority)
            data = result.get("data", [{}])[0]
            details: dict[str, dict] = {}
            for row in data.get("details", []):
                ccy = str(row.get("ccy", "") or "").upper()
                if not ccy:
                    continue
                details[ccy] = {
                    "availBal": _safe_float(row.get("availBal", 0)),
                    "cashBal": _safe_float(row.get("cashBal", 0)),
                    "availEq": _safe_float(row.get("availEq", 0)),
                    "eqUsd": _safe_float(row.get("eqUsd", 0)),
                    "frozenBal": _safe_float(row.get("frozenBal", 0)),
                }
            usdt = details.get("USDT", {})
            observed_at = datetime.now(timezone.utc).isoformat()
            snapshot = {
                "total_eq_usdt": _safe_float(data.get("totalEq", 0)),
                "avail_eq_usdt": max(
                    _safe_float(data.get("availEq", 0)),
                    _safe_float(usdt.get("availEq", 0)),
                    _safe_float(usdt.get("availBal", 0)),
                ),
                "usdt_avail_bal": _safe_float(usdt.get("availBal", 0)),
                "usdt_cash_bal": _safe_float(usdt.get("cashBal", 0)),
                "usdt_frozen_bal": _safe_float(usdt.get("frozenBal", 0)),
                "mgn_ratio": _safe_float(data.get("mgnRatio", 0)),
                "details": details,
                "observed_at": observed_at,
                "exchange_snapshot_at": observed_at,
                "received_at": observed_at,
            }
            _account_snapshot_cache = {"value": snapshot, "ts": time.monotonic()}
            return snapshot

        coalesce_tier = _truth_priority_tier(priority)
        return await _coalesced_truth_read(
            f"account_snapshot:{coalesce_tier}", fetch,
            ttl_sec=0.0,
            force=force_refresh,
            join_keys=_truth_join_keys("account_snapshot", coalesce_tier),
        )

    async def get_balance(self) -> dict[str, str]:
        snapshot = await self.get_account_snapshot()
        balances = {}
        for ccy, detail in snapshot.get("details", {}).items():
            avail_bal = _safe_float(detail.get("availBal", 0))
            if avail_bal > 0:
                balances[ccy] = f"{avail_bal}"
        return balances

    async def get_usdt_balance(self) -> float:
        snapshot = await self.get_account_snapshot()
        return float(snapshot.get("avail_eq_usdt", 0.0) or 0.0)

    async def get_max_order_size(
        self,
        coin: str,
        *,
        leverage: int | None = None,
        price: float | None = None,
    ) -> dict:
        if PAPER_TRADING or not OKX_API_KEY:
            return {"max_buy_contracts": 999999, "max_sell_contracts": 999999}
        params = {
            "instId": mm.inst_id(coin),
            "tdMode": mm.td_mode(),
        }
        if leverage:
            params["leverage"] = str(int(leverage))
        if price and price > 0:
            params["px"] = f"{price:.10f}".rstrip("0").rstrip(".")
        result = await self._request("GET", "/api/v5/account/max-size", params=params)
        if result.get("code") != "0":
            code, s_code, msg, s_msg, hint = _okx_error_payload(result)
            raise OKXOrderError(
                f"OKX max-size rejected: {msg or 'unknown'} (code {code or '?'}) {s_msg} {hint}".strip(),
                category=_classify_okx_error(code, s_code, msg, s_msg),
                code=code,
                s_code=s_code,
                hint=hint,
            )
        row = result.get("data", [{}])[0]
        return {
            "max_buy_contracts": int(_safe_float(row.get("maxBuy", 0))),
            "max_sell_contracts": int(_safe_float(row.get("maxSell", 0))),
        }

    async def get_positions(self) -> list[dict]:
        status = await self.get_positions_status()
        if not bool(status.get("ok", False)):
            raise RuntimeError(str(status.get("reason", "positions_unavailable") or "positions_unavailable"))
        return list(status.get("positions", []) or [])

    async def get_positions_status(self, *, force_refresh: bool = False, priority: str = "P0") -> dict:
        if PAPER_TRADING or not OKX_API_KEY:
            return {"ok": True, "positions": [], "reason": "paper/local"}
        async def fetch():
            try:
                result = await self._request(
                    "GET", "/api/v5/account/positions",
                    params={"instType": "SWAP"}, priority=priority,
                )
                if str(result.get("code", "")) != "0":
                    return {
                        "ok": False,
                        "positions": None,
                        "reason": str(result.get("msg", "private_read_failed") or "private_read_failed"),
                        "code": str(result.get("code", "") or ""),
                    }
                observed_at = datetime.now(timezone.utc).isoformat()
                return {
                    "ok": True,
                    "positions": list(result.get("data", []) or []),
                    "reason": "",
                    "observed_at": observed_at,
                    "exchange_snapshot_at": observed_at,
                    "received_at": observed_at,
                }
            except Exception as e:
                logger.warning("get_positions failed: %s", e)
                return {"ok": False, "positions": None, "reason": str(e)}
        coalesce_tier = _truth_priority_tier(priority)
        return await _coalesced_truth_read(
            f"positions_status:{coalesce_tier}", fetch, ttl_sec=0.75, force=force_refresh,
            cacheable=lambda value: bool(isinstance(value, dict) and value.get("ok")),
            join_keys=_truth_join_keys("positions_status", coalesce_tier),
        )

    async def auth_preflight(self) -> dict:
        """Check private OKX read auth without using paper-mode local fallbacks."""
        checks: list[dict] = []
        endpoints = [
            ("balance", "/api/v5/account/balance", None),
            ("positions", "/api/v5/account/positions", {"instType": "SWAP"}),
        ]
        for label, path, params in endpoints:
            try:
                result = await self._request("GET", path, params=params)
                ok = str(result.get("code", "")) == "0"
                checks.append({
                    "label": label,
                    "ok": ok,
                    "code": str(result.get("code", "")),
                    "reason": "" if ok else str(result.get("msg", "private_read_failed") or "private_read_failed"),
                })
            except Exception as exc:
                checks.append({
                    "label": label,
                    "ok": False,
                    "code": "exception",
                    "reason": str(exc),
                })
        ok = all(bool(item.get("ok")) for item in checks)
        reason = "; ".join(
            f"{item['label']} code={item.get('code')} reason={item.get('reason')}"
            for item in checks
            if not bool(item.get("ok"))
        )
        return {"ok": ok, "checks": checks, "reason": reason}

    async def get_positions_with_pnl(self) -> list[dict]:
        positions = await self.get_positions()
        enriched  = []
        for p in positions:
            try:
                upl = float(p.get("upl", 0))
            except Exception:
                upl = 0.0
            p["unrealised_pnl"] = upl
            enriched.append(p)
        return enriched

    async def get_live_open_coins(self) -> set[str]:
        """
        Return the set of coin symbols that have a non-zero open position on OKX.
        Used by exit_manager and category_engine to sync internal state with exchange.
        """
        status = await self.get_live_open_coins_status()
        if not bool(status.get("ok", False)):
            return set()
        return set(status.get("coins", set()) or set())

    async def get_live_open_coins_status(self) -> dict:
        """
        Same as get_live_open_coins(), but preserves failure state so callers can
        distinguish "no positions" from "could not verify positions".
        """
        try:
            positions = await self.get_positions()
            coins = set()
            for p in positions:
                if float(p.get("pos", 0)) != 0:
                    coin = _coin_from_inst(p.get("instId", ""))
                    if coin:
                        coins.add(coin)
            return {"ok": True, "coins": coins, "reason": ""}
        except Exception as e:
            logger.warning("get_live_open_coins failed: %s", e)
            return {"ok": False, "coins": None, "reason": str(e)}

    async def get_ticker(
        self,
        coin: str,
        *,
        priority: str = "background",
        allow_stale: bool | None = None,
    ) -> dict:
        inst = mm.inst_id(coin)
        cache_key = inst.upper()
        rank = _ticker_priority_rank(priority)
        if allow_stale is None:
            allow_stale = rank >= 2
        _latency.record_shared_resource("ticker", logical=1)
        with _ticker_metrics_lock:
            _ticker_metrics["logical_callers"] += 1
        now = time.monotonic()
        cached = _ticker_cache.get(cache_key)
        cached_rank = int(cached.get("priority_rank", 2) or 2) if cached else 2
        if (
            cached
            and cached_rank <= rank
            and now - float(cached.get("ts", 0.0) or 0.0) <= _TICKER_CACHE_TTL_SEC
        ):
            _latency.record_shared_resource("ticker", cache_hits=1)
            with _ticker_metrics_lock:
                _ticker_metrics["cache_hits"] += 1
            return _ticker_public_value(cached, stale=False)
        loop = asyncio.get_running_loop()
        inflight_key = None
        task = None
        # A weaker caller may join stronger work, never the reverse.
        for candidate_rank in range(rank + 1):
            candidate_key = (id(loop), cache_key, _ticker_priority_label(candidate_rank))
            candidate_task = _ticker_inflight.get(candidate_key)
            if candidate_task is not None and not candidate_task.done():
                inflight_key = candidate_key
                task = candidate_task
                break
        if inflight_key is None:
            inflight_key = (id(loop), cache_key, _ticker_priority_label(rank))
            task = _ticker_inflight.get(inflight_key)
        joined = task is not None and not task.done()
        if task is None or task.done():
            task = asyncio.create_task(
                self._fetch_ticker_singleflight(inst, cache_key, priority, rank),
                name=f"okx-ticker:{cache_key}:p{rank}",
            )
            _ticker_inflight[inflight_key] = task
            with _ticker_metrics_lock:
                _ticker_metrics["underlying_requests"] += 1
            _latency.record_shared_resource("ticker", underlying=1)

            def _cleanup(done_task, *, key=inflight_key):
                if _ticker_inflight.get(key) is done_task:
                    _ticker_inflight.pop(key, None)

            task.add_done_callback(_cleanup)
        else:
            with _ticker_metrics_lock:
                _ticker_metrics["coalesced_callers"] += 1
            _latency.record_shared_resource("ticker", coalesced=1)
        wait_started = time.monotonic()
        try:
            value = await asyncio.shield(task)
            if joined:
                _latency.record_shared_resource(
                    "ticker",
                    singleflight_join_wait_sec=time.monotonic() - wait_started,
                )
            return dict(value or {})
        except asyncio.CancelledError:
            raise
        except BaseException:
            with _ticker_metrics_lock:
                _ticker_metrics["failures"] += 1
            _latency.record_shared_resource("ticker", failures=1)
            if cached and allow_stale:
                error = "refresh_failed"
                if task.done() and not task.cancelled():
                    try:
                        error = type(task.exception()).__name__ if task.exception() else error
                    except BaseException:
                        pass
                return _ticker_public_value(cached, stale=True, refresh_error=error)
            raise

    async def _fetch_ticker_singleflight(
        self,
        inst: str,
        cache_key: str,
        priority: str,
        priority_rank: int,
    ) -> dict:
        result = await self._request(
            "GET", "/api/v5/market/ticker", params={"instId": inst},
            priority=priority,
        )
        if not result.get("data"):
            raise RuntimeError(f"No ticker for {inst}")
        raw = result["data"][0]
        last = float(raw.get("last", 0) or 0)
        base_volume_24h = float(raw.get("volCcy24h", 0) or 0)
        value = {
            "last": last,
            "bid": float(raw.get("bidPx", 0) or 0),
            "ask": float(raw.get("askPx", 0) or 0),
            "bidPx": float(raw.get("bidPx", 0) or 0),
            "askPx": float(raw.get("askPx", 0) or 0),
            "vol24h": float(raw.get("vol24h", 0) or 0),
            "volCcy24h": base_volume_24h,
            "quoteVolume24h": base_volume_24h * last,
        }
        _ticker_cache[cache_key] = {
            "value": value,
            "ts": time.monotonic(),
            "observed_at": _ticker_observed_at(raw.get("ts")),
            "source": "okx_rest",
            "stale": False,
            "priority_rank": int(priority_rank),
        }
        return _ticker_public_value(_ticker_cache[cache_key], stale=False)

    async def get_execution_ticker(self, coin: str) -> dict:
        """Fetch a ticker using reserved execution-critical REST capacity."""
        getter = self.get_ticker
        # Preserve compatibility with injected lightweight clients/tests that
        # replace get_ticker with a legacy one-argument callable.
        if getattr(getter, "__self__", None) is not self:
            return await getter(coin)
        return await getter(coin, priority="execution", allow_stale=False)

    # ── Order placement ───────────────────────────────────────────────────────
    async def get_mark_price(self, coin: str) -> float | None:
        """Fetch OKX mark price for reporting-only fallback estimates."""
        if PAPER_TRADING or not OKX_API_KEY:
            return None
        inst = mm.inst_id(coin)
        try:
            result = await self._request(
                "GET",
                "/api/v5/public/mark-price",
                params={"instType": "SWAP", "instId": inst},
            )
            data = result.get("data", []) or []
            if data:
                mark = float(data[0].get("markPx", 0) or 0)
                return mark if mark > 0 else None
        except Exception as e:
            logger.debug("get_mark_price failed for %s: %s", coin, e)
        return None

    async def place_market_order(
        self,
        coin:         str,
        side:         str,
        amount_usdt:  float | None = None,
        sl_price:     float | None = None,
        tp_price:     float | None = None,
        leverage:     int | None = None,
        allow_nano_route: bool = True,
        _nano_routed: bool = False,
        _leverage_retried: bool = False,
        client_order_id: str = "",
        contracts_override: float | None = None,
        on_exchange_request=None,
        position_mode: str = "",
    ) -> dict | None:
        """
        Place a futures market order with exchange-attached TP/SL algo orders.

        OKX attaches the TP/SL as algo orders to the same position.
        When price hits TP or SL, OKX closes the position automatically.
        The exit_manager acts as a safety backup only.

        Returns a dict that always includes 'actual_coin' so callers know
        exactly which coin was opened (NANO routing may change this).
        """
        self._set_last_order_skip_reason("")
        coin    = coin.upper()
        cooled_down, cooldown_reason = _admission.is_symbol_cooled_down(coin)
        if cooled_down:
            logger.warning(
                "Order skipped for %s %s - symbol cooldown active (%s).",
                side.upper(), coin, cooldown_reason,
            )
            self._set_last_order_skip_reason(cooldown_reason)
            return None
        snapshot = await self.get_account_snapshot()
        balance = float(snapshot.get("avail_eq_usdt", 0.0) or 0.0)
        import settings_store as _s
        import micro_sizing as _micro_sizing
        min_buffer = float(_micro_sizing.margin_buffer_usdt(balance, _s))
        if not PAPER_TRADING and balance <= min_buffer:
            logger.warning(
                "Order skipped for %s %s - free USDT %.2f is below safety buffer %.2f.",
                side.upper(), coin, balance, min_buffer,
            )
            self._set_last_order_skip_reason(
                f"free_balance_buffer: free_usdt={balance:.2f}, buffer={min_buffer:.2f}"
            )
            return None
        if not PAPER_TRADING and str(mm.td_mode()).lower() != "isolated":
            reason = f"isolated_margin_required: tdMode={mm.td_mode()}"
            self._set_last_order_skip_reason(reason)
            return None

        # Validate + compute size
        if amount_usdt is None:
            v = risk.validate_trade(coin, side, balance)
            if not v["valid"]:
                raise ValueError(v["reason"])
            size = v["size_usdt"]
            tier = v["tier"]
        else:
            v    = risk.validate_trade(coin, side, balance)
            tier = v["tier"]
            if not v["valid"] and "Daily loss" in v.get("reason", ""):
                raise ValueError(v["reason"])
            size = float(amount_usdt)
        requested_size_usdt = float(size)

        # Live price
        try:
            ticker = await self.get_execution_ticker(coin)
            price  = ticker["last"]
        except Exception as exc:
            if not PAPER_TRADING:
                self._set_last_order_skip_reason(f"execution_price_unavailable:{type(exc).__name__}:{exc}")
                return None
            price = 1.0
        inst = mm.inst_id(coin)
        requested_leverage = int(leverage or mm.get_leverage())
        effective_leverage = requested_leverage
        cache_key = inst.upper()
        _lev_side = "long" if side == "buy" else "short"
        leverage_truth = None
        if PAPER_TRADING:
            effective_leverage = requested_leverage
        else:
            try:
                leverage_truth = await self.get_authoritative_leverage(
                    coin, _lev_side, requested_leverage=requested_leverage, margin_mode="isolated",
                )
                effective_leverage = int(leverage_truth["exchange_configured_leverage"])
            except LeverageTruthUnavailable as exc:
                reason = f"leverage_truth_unavailable:{exc}"
                logger.warning("Order skipped for %s %s - %s", side.upper(), coin, reason)
                self._set_last_order_skip_reason(reason)
                return None

        # Margin headroom check (prevents OKX 51009 insufficient margin)
        if not PAPER_TRADING:
            req_margin = requested_size_usdt / max(effective_leverage, 1)
            headroom   = balance - min_buffer
            # Require a 5% cushion above the calculated margin
            if headroom <= 0 or req_margin * 1.05 >= headroom:
                logger.warning(
                    "Order skipped for %s %s - margin headroom too small "
                    "(req_margin=%.2f, headroom=%.2f, buffer=%.2f, lev=%dx).",
                    side.upper(), coin, req_margin, headroom, min_buffer, effective_leverage,
                )
                self._set_last_order_skip_reason(
                    f"margin_headroom: req_margin={req_margin:.2f}, headroom={headroom:.2f}, "
                    f"buffer={min_buffer:.2f}, lev={effective_leverage}x"
                )
                try:
                    _micro_sizing.record_sizing_diagnostics(
                        **_micro_sizing.diagnostics_snapshot(
                            balance_usdt=balance,
                            settings=_s,
                            leverage=effective_leverage,
                            final_size_usdt=requested_size_usdt,
                            block_reason="margin_headroom",
                        )
                    )
                except Exception:
                    pass
                return None

        # Resolve SL/TP — use caller-provided ATR-based values if given
        base_stop, base_take = risk.calculate_stop_take(price, side, tier, leverage=effective_leverage)
        stop = sl_price if (sl_price and sl_price > 0) else base_stop
        # FINAL PASS: allow explicitly disabling exchange TP by passing tp_price <= 0.
        attach_tp = True
        take = base_take
        if tp_price is not None:
            if float(tp_price) > 0:
                take = float(tp_price)
            else:
                attach_tp = False
                take = 0.0

        # Guard: price may have moved since category_engine evaluated the coin.
        # If the SL or TP is now on the wrong side of the current price, OKX will
        # reject the order. Reanchor to the fresh price using the tier-based fallback.
        if price > 0:
            if side == "buy" and stop >= price:
                logger.warning(
                    "SL %.8f >= market %.8f for LONG %s — reanchoring SL.",
                    stop, price, coin,
                )
                stop = base_stop
            elif side == "sell" and stop <= price:
                logger.warning(
                    "SL %.8f <= market %.8f for SHORT %s — reanchoring SL.",
                    stop, price, coin,
                )
                stop = base_stop

            if attach_tp and side == "buy" and take <= price:
                logger.warning(
                    "TP %.8f <= market %.8f for LONG %s — reanchoring TP.",
                    take, price, coin,
                )
                take = base_take
            elif attach_tp and side == "sell" and take >= price:
                logger.warning(
                    "TP %.8f >= market %.8f for SHORT %s — reanchoring TP.",
                    take, price, coin,
                )
                take = base_take
        inst = mm.inst_id(coin)

        # ── Paper mode ───────────────────────────────────────────────────────
        if PAPER_TRADING:
            logger.info(
                "ORDER_MOCK_SUCCESS coin=%s side=%s size_usdt=%.2f leverage=%dx paper=True",
                coin,
                side,
                size,
                effective_leverage,
            )
            return {
                "ordId":       client_order_id or f"PAPER-{side.upper()}-{coin}-{uuid.uuid4().hex[:12]}",
                "instId":      inst,
                "actual_coin": coin,
                "side":        side,
                "sz":          f"${size:.2f}",
                "fillPx":      str(price),
                "stop_loss":   stop,
                "take_profit": take,
                "tier":        tier.name,
                "market_mode": mm.get_mode(),
                "size_usdt":   size,
                "requested_leverage": requested_leverage,
                "exchange_configured_leverage": effective_leverage,
                "effective_leverage": effective_leverage,
                "leverage_truth_status": str((leverage_truth or {}).get("status", "PAPER_LOCAL")),
                "leverage_truth_timestamp": str((leverage_truth or {}).get("timestamp", "") or ""),
                "state":       "filled",
                "paper":       True,
                "clOrdId":     client_order_id,
                "contracts":   float(contracts_override or 0.0),
            }

        # ── Build live order body ─────────────────────────────────────────────
        if str(mm.td_mode()).lower() != "isolated":
            reason = f"isolated_margin_required: tdMode={mm.td_mode()}"
            logger.critical("Live order blocked for %s %s - %s", side.upper(), coin, reason)
            self._set_last_order_skip_reason(reason)
            return None

        body = {
            "instId":  inst,
            "tdMode":  mm.td_mode(),
            "side":    side,
            "ordType": "market",
        }
        if client_order_id:
            body["clOrdId"] = validate_client_id(client_order_id, field="clOrdId")

        # Futures: calculate contract count
        try:
            sz = float(contracts_override) if contracts_override is not None and float(contracts_override) > 0 else await self._futures_sz(coin, size, price)
        except ValueError as e:
            if _nano_routed:
                raise RuntimeError(str(e))
            if not allow_nano_route:
                logger.warning(
                    "NANO routing disabled for %s - skipping affordable substitute after size failure: %s",
                    coin, e,
                )
                self._set_last_order_skip_reason(f"nano_route_disabled: {e}")
                return None
            logger.info("NANO routing triggered for %s: %s", coin, e)
            _route = await self._find_affordable_futures_coin(size)
            if _route is None:
                logger.debug("NANO: $%.2f too small for any contract, skipping.", size)
                self._set_last_order_skip_reason(
                    f"nano_route_unavailable: size_usdt={size:.2f} too small for any contract"
                )
                return None
            alt_coin, _alt_price, _alt_sz = _route
            # FIX: do NOT forward original coin's SL/TP geometry to the NANO coin.
            # The original coin's price (e.g. ~$2000) is completely wrong for
            # a micro-price coin like PEPE ($0.000003). Passing None forces
            # place_market_order to compute fresh base_stop/base_take for the
            # actual NANO coin's price via risk.calculate_stop_take().
            #
            # IMPORTANT: Preserve explicit "disable exchange TP" semantics. If the caller passed
            # tp_price <= 0, keep it <= 0 so the NANO coin also does not attach a TP.
            nano_tp = tp_price
            try:
                if tp_price is not None and float(tp_price) > 0:
                    nano_tp = None
            except Exception:
                nano_tp = None
            return await self.place_market_order(
                alt_coin, side, amount_usdt=amount_usdt,
                sl_price=None, tp_price=nano_tp,
                leverage=requested_leverage,
                allow_nano_route=allow_nano_route,
                _nano_routed=True,
                _leverage_retried=_leverage_retried,
                client_order_id=client_order_id,
                contracts_override=contracts_override,
                position_mode=position_mode,
                on_exchange_request=on_exchange_request,
            )

        contract_cost = await self.get_contract_cost(coin, price)
        if _s.get_val("preflight_contract_cap_enabled", True):
            try:
                limit_key = "max_buy_contracts" if side == "buy" else "max_sell_contracts"
                attempted_leverages: list[int] = []
                max_contracts = 0
                while True:
                    attempted_leverages.append(int(effective_leverage))
                    limits = await self.get_max_order_size(coin, leverage=effective_leverage, price=price)
                    max_contracts = int(limits.get(limit_key, 0) or 0)
                    if max_contracts > 0:
                        if effective_leverage > 0:
                            _working_leverage_cache[cache_key] = int(effective_leverage)
                        if len(attempted_leverages) > 1:
                            logger.info(
                                "LEVERAGE FALLBACK CHOSEN %s %s | leverage=%dx contracts=%d attempts=%s",
                                side.upper(),
                                coin,
                                effective_leverage,
                                max_contracts,
                                " -> ".join(f"{lev}x" for lev in attempted_leverages),
                            )
                        break

                    logger.warning(
                        "LEVERAGE FALLBACK ZERO %s %s | leverage=%dx returned 0 contracts.",
                        side.upper(),
                        coin,
                        effective_leverage,
                    )
                    detail = " -> ".join(f"{lev}x" for lev in attempted_leverages)
                    cooldown_applied = _admission.record_preflight_zero_contract(
                        coin,
                        detail=f"attempts={detail}",
                    )
                    reason = f"preflight_zero_contracts: attempts={detail}"
                    if cooldown_applied:
                        reason += " cooldown_applied"
                    self._set_last_order_skip_reason(reason)
                    return None
                if sz > max_contracts:
                    if not _s.get_val("preflight_max_reduce_enabled", True):
                        raise OKXOrderError(
                            f"Preflight max-size blocked {coin}: requested {sz} contracts, allowed {max_contracts}.",
                            category="insufficient_margin",
                            hint=OKX_CATEGORY_HINTS["insufficient_margin"],
                            requested_leverage=requested_leverage,
                            effective_leverage=effective_leverage,
                        )
                    old_sz = sz
                    sz = max_contracts
                    size = round(max(float(sz) * contract_cost, 1.0), 2)
                    logger.warning(
                        "Preflight downsized %s %s from %d to %d contracts (size_usdt %.2f -> %.2f).",
                        side.upper(), coin, old_sz, sz, requested_size_usdt, size,
                    )
                # Guard: if downsizing drives notional below our floor, skip to avoid dust positions.
                # Authoritative floor shared with capital_allocator / execution_router.
                from executable_minimum import placement_min_notional_usdt as _placement_min_notional

                _instrument_min_notional = 0.0
                try:
                    from instrument_registry import registry as _instrument_registry
                    _instrument_meta = _instrument_registry.get(coin, require_fresh=True)
                    if _instrument_meta is not None:
                        _instrument_min_notional = float(_instrument_meta.min_sz) * float(
                            _instrument_meta.contract_notional(float(price))
                        )
                except Exception:
                    _instrument_min_notional = 0.0

                _contract_aware_floor = bool(
                    _s.get_val("contract_aware_execution_floor_enabled", False)
                    and _instrument_min_notional > 0.0
                )
                min_notional = _placement_min_notional(
                    balance,
                    tier_min_trade_usdt=(
                        0.0
                        if _contract_aware_floor
                        else float(getattr(tier, "min_trade_usdt", 0.0) or 0.0)
                    ),
                    instrument_min_notional_usdt=_instrument_min_notional,
                    settings=_s,
                    contract_aware=_contract_aware_floor,
                )
                if size < min_notional:
                    logger.warning(
                        "Order skipped for %s %s - downsized notional %.2f below floor %.2f.",
                        side.upper(), coin, size, min_notional,
                    )
                    _admission.record_exchange_admission_failure(
                        coin,
                        "downsized_below_floor",
                        detail=f"size_usdt={size:.2f}, floor={min_notional:.2f}",
                    )
                    self._set_last_order_skip_reason(
                        f"downsized_below_floor: size_usdt={size:.2f}, floor={min_notional:.2f}"
                    )
                    try:
                        _micro_sizing.record_sizing_diagnostics(
                            **_micro_sizing.diagnostics_snapshot(
                                balance_usdt=balance,
                                settings=_s,
                                leverage=effective_leverage,
                                exchange_floor_usdt=min_notional,
                                final_size_usdt=size,
                                block_reason="downsized_below_floor",
                            )
                        )
                    except Exception:
                        pass
                    return None
            except OKXOrderError:
                raise
            except Exception as e:
                logger.warning("OKX preflight max-size check failed for %s: %s", coin, e)

        if not PAPER_TRADING:
            req_margin = size / max(effective_leverage, 1)
            headroom = balance - min_buffer
            if headroom <= 0 or req_margin * 1.05 >= headroom:
                logger.warning(
                    "Order skipped for %s %s - margin headroom too small "
                    "(req_margin=%.2f, headroom=%.2f, buffer=%.2f, lev=%dx).",
                    side.upper(), coin, req_margin, headroom, min_buffer, effective_leverage,
                )
                self._set_last_order_skip_reason(
                    f"margin_headroom: req_margin={req_margin:.2f}, headroom={headroom:.2f}, "
                    f"buffer={min_buffer:.2f}, lev={effective_leverage}x"
                )
                try:
                    _micro_sizing.record_sizing_diagnostics(
                        **_micro_sizing.diagnostics_snapshot(
                            balance_usdt=balance,
                            settings=_s,
                            leverage=effective_leverage,
                            final_size_usdt=size,
                            block_reason="margin_headroom",
                        )
                    )
                except Exception:
                    pass
                return None

        body["sz"] = str(sz)
        resolved_position_mode = str(position_mode or "").lower()
        if resolved_position_mode not in {"net_mode", "long_short_mode"}:
            self._set_last_order_skip_reason("position_mode_unavailable")
            return None
        body["posSide"] = "net" if resolved_position_mode == "net_mode" else ("long" if side == "buy" else "short")

        # ── Final SL/TP sanity check ────────────────────────────────────────────
        # After all reanchoring, verify SL is on the correct side of the price.
        # If it's still wrong, skip the trade entirely rather than send a bad order.
        if price > 0 and stop > 0:
            if side == "buy" and stop >= price:
                logger.warning(
                    "Order skipped for %s %s — SL invalid after reanchor "
                    "(SL=%.8f TP=%.8f price=%.8f)",
                    side.upper(), coin, stop, take, price,
                )
                self._set_last_order_skip_reason(
                    f"sl_invalid_after_reanchor: sl={stop:.8f}, tp={take:.8f}, price={price:.8f}"
                )
                return None
            elif side == "sell" and stop <= price:
                logger.warning(
                    "Order skipped for %s %s — SL invalid after reanchor "
                    "(SL=%.8f TP=%.8f price=%.8f)",
                    side.upper(), coin, stop, take, price,
                )
                self._set_last_order_skip_reason(
                    f"sl_invalid_after_reanchor: sl={stop:.8f}, tp={take:.8f}, price={price:.8f}"
                )
                return None

        # ── Attach TP/SL via attachAlgoOrds (OKX API requirement as of 2025) ──
        # OKX deprecated inline tpTriggerPx/slTriggerPx on /trade/order.
        # Must use attachAlgoOrds array. Without it: "The current function is
        # not supported… use the attachAlgoOrds array to place orders via Open API."
        #
        # IMPORTANT: Use f-string decimal formatting, NOT str(), to avoid
        # scientific notation (e.g. "1e-08") which OKX rejects as invalid.
        _algo: dict = {}
        if take and take > 0:
            _tp_str = f"{round(take, 8):.10f}".rstrip("0").rstrip(".")
            # OKX rejects tpTriggerPx of "0" — drop TP if it rounded to zero
            if float(_tp_str) > 0:
                _algo["tpTriggerPx"]     = _tp_str
                _algo["tpOrdPx"]         = "-1"      # -1 = market order at trigger
                _algo["tpTriggerPxType"] = "last"
            else:
                logger.warning("place_market_order: tpTriggerPx rounded to 0 for take=%.10f — omitting TP algo", take)
        if stop and stop > 0:
            _sl_str = f"{round(stop, 8):.10f}".rstrip("0").rstrip(".")
            if float(_sl_str) > 0:
                _algo["slTriggerPx"]     = _sl_str
                _algo["slOrdPx"]         = "-1"      # -1 = market order at trigger
                _algo["slTriggerPxType"] = "last"
            else:
                logger.warning("place_market_order: slTriggerPx rounded to 0 for stop=%.10f — omitting SL algo", stop)
        if _algo:
            body["attachAlgoOrds"] = [_algo]

        logger.info(
            "ORDER_PAYLOAD_READY coin=%s side=%s posSide=%s size_usdt=%.2f "
            "leverage=%dx contracts=%s reduceOnly=%s ordType=%s",
            coin,
            side,
            body.get("posSide", ""),
            size,
            effective_leverage,
            body.get("sz", "?"),
            body.get("reduceOnly", False),
            body.get("ordType", ""),
        )
        logger.info(
            "Placing order: coin=%s side=%s size_usdt=%.2f leverage=%s "
            "price=%.8f SL=%.8f TP=%.8f contracts=%s nano=%s body=%s",
            coin, side, size, effective_leverage, price, stop, take,
            body.get("sz", "?"), _nano_routed, body,
        )
        result = await self._request(
            "POST", "/api/v5/trade/order", data=body,
            before_send=on_exchange_request,
        )
        logger.info("OKX order response: %s", result)

        classification = classify_mutation_response(
            result, expected_items=1, submitted_client_id=client_order_id,
        )
        if classification.outcome != MUTATION_SUCCESS:
            code, s_code, msg, s_msg, hint = _okx_error_payload(result)
            category = _classify_okx_error(code, s_code, msg, s_msg)
            cap = _extract_leverage_cap(s_msg, msg)
            if category == "invalid_leverage" and cap and not _leverage_retried:
                _instrument_leverage_cap[inst] = cap
                logger.warning(
                    "Order leverage rejected for %s at %dx (%s); retaining exchange cap %dx for a later, new intent.",
                    coin, effective_leverage, s_msg or msg, cap,
                )
            if category in {"invalid_leverage", "market_unavailable", "min_size", "max_size"}:
                _admission.record_exchange_admission_failure(
                    coin,
                    category or "order_rejected",
                    detail=f"code={code or '?'} sCode={s_code or '?'} msg={s_msg or msg}",
                )
            logger.error(
                "Order rejected by OKX: coin=%s side=%s code=%s sCode=%s msg=%s sMsg=%s hint=%s category=%s",
                coin, side, code, s_code, msg, s_msg, hint, category,
            )
            if classification.outcome == MUTATION_DEFINITIVE_REJECTION:
                raise OKXDefinitiveRejection(
                    f"OKX order rejected: {msg or s_msg or 'unknown'} "
                    f"(code {code or '?'}) {s_msg} {hint}".strip(),
                    code=classification.top_level_code or code,
                    s_code=classification.item_s_code or s_code,
                    hint=hint,
                    evidence=classification.to_dict(),
                )
            # A malformed or uncorrelatable acknowledgement is not proof of a
            # rejected mutation.  The router must retain recovery ownership.
            raise RuntimeError(
                "OKX order acknowledgement requires reconciliation: "
                f"{classification.evidence or {}}"
            )

        order = _require_place_order_acknowledgement(result, client_order_id)
        self._set_last_order_skip_reason("")

        # ── Determine actual filled coin (handles NANO routing traceability) ──
        returned_inst = order.get("instId", inst)
        actual_coin   = _coin_from_inst(returned_inst)

        # Fill price: OKX sometimes returns "" on immediate market fills
        okx_fill  = order.get("fillPx", "") or order.get("avgPx", "")
        has_okx_fill = bool(okx_fill and float(okx_fill) > 0)
        real_fill = float(okx_fill) if has_okx_fill else price

        order.update({
            "fillPx":      real_fill,
            "expected_entry_price": price,
            "fill_price_source": "OKX_FILL" if has_okx_fill else "PROVISIONAL_LAST",
            "provisional_fill": not has_okx_fill,
            "stop_loss":   stop,
            "take_profit": take,
            "tier":        tier.name,
            "market_mode": mm.get_mode(),
            "size_usdt":   size,
            "contracts":   sz,
            "requested_size_usdt": requested_size_usdt,
            "actual_coin": actual_coin,   # ← key fix: always reflects real coin
            "requested_leverage": requested_leverage,
            "exchange_configured_leverage": effective_leverage,
            "effective_leverage": effective_leverage,
            "leverage_truth_status": str((leverage_truth or {}).get("status", "EXCHANGE_CONFIGURED")),
            "leverage_truth_timestamp": str((leverage_truth or {}).get("timestamp", "") or ""),
        })
        _admission.record_success(actual_coin)
        return order

    async def place_limit_order(
        self,
        coin:        str,
        side:        str,
        price:       float,
        amount_usdt: float | None = None,
        pos_side:    str | None = None,
        sl_price:    float | None = None,
        tp_price:    float | None = None,
        leverage:    int | None = None,
        client_order_id: str = "",
        contracts_override: float | None = None,
        on_exchange_request=None,
        position_mode: str = "",
    ) -> dict:
        coin    = coin.upper()
        balance = await self.get_usdt_balance()
        v       = risk.validate_trade(coin, side, balance)
        size    = float(amount_usdt) if amount_usdt else (v["size_usdt"] if v["valid"] else 2.0)
        tier    = risk.get_tier(balance)
        base_stop, base_take = risk.calculate_stop_take(price, side, tier)
        stop = sl_price if (sl_price and sl_price > 0) else base_stop
        attach_tp = True
        take = base_take
        if tp_price is not None:
            if float(tp_price) > 0:
                take = float(tp_price)
            else:
                attach_tp = False
                take = 0.0
        inst = mm.inst_id(coin)

        requested_leverage = int(leverage or mm.get_leverage())
        _lev_side_lim = "long" if side == "buy" else "short"
        leverage_truth = None
        if not PAPER_TRADING:
            try:
                leverage_truth = await self.get_authoritative_leverage(
                    coin, _lev_side_lim, requested_leverage=requested_leverage, margin_mode="isolated",
                )
                leverage = int(leverage_truth["exchange_configured_leverage"])
            except LeverageTruthUnavailable as exc:
                self._set_last_order_skip_reason(f"leverage_truth_unavailable:{exc}")
                return None
        else:
            leverage = requested_leverage

        if PAPER_TRADING:
            return {
                "ordId":       client_order_id or f"PAPER-LIMIT-{side.upper()}-{coin}-{uuid.uuid4().hex[:12]}",
                "instId":      inst,
                "actual_coin": coin,
                "side":        side,
                "sz":          str(size),
                "px":          str(price),
                "stop_loss":   stop,
                "take_profit": take,
                "tier":        tier.name,
                "market_mode": mm.get_mode(),
                "state":       "live",
                "paper":       True,
                "fillPx":      str(price),
                "size_usdt":   size,
                "contracts":   float(contracts_override or 0.0),
                "clOrdId":     client_order_id,
                "requested_leverage": requested_leverage,
                "exchange_configured_leverage": int(leverage),
                "effective_leverage": int(leverage),
                "leverage_truth_status": str((leverage_truth or {}).get("status", "PAPER_LOCAL")),
                "leverage_truth_timestamp": str((leverage_truth or {}).get("timestamp", "") or ""),
            }

        if str(mm.td_mode()).lower() != "isolated":
            raise RuntimeError(f"isolated_margin_required: tdMode={mm.td_mode()}")

        body = {
            "instId":  inst, "tdMode": mm.td_mode(),
            "side":    side, "ordType": "limit", "px": str(price),
        }
        if client_order_id:
            body["clOrdId"] = validate_client_id(client_order_id, field="clOrdId")
        try:
            sz = float(contracts_override) if contracts_override is not None and float(contracts_override) > 0 else await self._futures_sz(coin, size, price)
        except ValueError as exc:
            self._set_last_order_skip_reason(f"quantity_conversion_failed:{exc}")
            return None
        body["sz"] = str(sz)
        resolved_position_mode = str(position_mode or "").lower()
        if resolved_position_mode not in {"net_mode", "long_short_mode"}:
            self._set_last_order_skip_reason("position_mode_unavailable")
            return None
        body["posSide"] = "net" if resolved_position_mode == "net_mode" else (pos_side or ("long" if side == "buy" else "short"))

        # Attach TP/SL via attachAlgoOrds (OKX API requirement as of 2025)
        # Use decimal formatting to avoid scientific notation (e.g. "1e-08")
        _algo_lim: dict = {}
        if take and take > 0:
            _algo_lim["tpTriggerPx"]     = f"{round(take, 8):.10f}".rstrip("0").rstrip(".")
            _algo_lim["tpOrdPx"]         = "-1"
            _algo_lim["tpTriggerPxType"] = "last"
        if stop and stop > 0:
            _algo_lim["slTriggerPx"]     = f"{round(stop, 8):.10f}".rstrip("0").rstrip(".")
            _algo_lim["slOrdPx"]         = "-1"
            _algo_lim["slTriggerPxType"] = "last"
        if _algo_lim:
            body["attachAlgoOrds"] = [_algo_lim]

        result = await self._request(
            "POST", "/api/v5/trade/order", data=body,
            before_send=on_exchange_request,
        )
        classification = classify_mutation_response(
            result, expected_items=1, submitted_client_id=client_order_id,
        )
        if classification.outcome != MUTATION_SUCCESS:
            if classification.outcome == MUTATION_DEFINITIVE_REJECTION:
                raise OKXDefinitiveRejection(
                    classification.item_s_msg or classification.top_level_msg or "limit order rejected",
                    code=classification.top_level_code,
                    s_code=classification.item_s_code,
                    evidence=classification.to_dict(),
                )
            raise RuntimeError(
                "OKX limit acknowledgement requires reconciliation: "
                f"{classification.evidence or {}}"
            )
        order = _require_place_order_acknowledgement(result, client_order_id)
        returned_inst = order.get("instId", inst)
        actual_coin   = _coin_from_inst(returned_inst)
        order.update({
            "stop_loss":   stop,
            "take_profit": take,
            "tier":        tier.name,
            "market_mode": mm.get_mode(),
            "actual_coin": actual_coin,
        })
        return order

    async def cancel_order(self, inst: str, ord_id: str = "", *, client_order_id: str = "") -> dict:
        if PAPER_TRADING or not OKX_API_KEY:
            # A cancel request is not terminal order truth.  The router
            # reconciles the risk slot only after authoritative zero-fill
            # cancellation, exactly as it does for live OKX responses.
            return {"cancelled": True}
        body = {"instId": inst}
        if ord_id:
            body["ordId"] = ord_id
        if client_order_id:
            body["clOrdId"] = validate_client_id(client_order_id, field="clOrdId")
        result = await self._request("POST", "/api/v5/trade/cancel-order", data=body)
        if result.get("code") != "0":
            raise RuntimeError(f"Cancel failed: {result.get('msg')}")
        rows = result.get("data", []) or []
        if len(rows) != 1 or not isinstance(rows[0], dict) or str(rows[0].get("sCode", "")) != "0":
            raise RuntimeError("Cancel acknowledgement requires reconciliation: item_sCode_not_zero")
        # A successful cancel call only acknowledges the request.  It is not
        # terminal order truth; callers must read the order state.
        return dict(rows[0])

    async def get_open_orders(self, coin: str | None = None, *, priority: str = "P0") -> list[dict]:
        params = {}
        if coin:
            params["instId"] = mm.inst_id(coin)
        else:
            params["instType"] = "SWAP"
        if PAPER_TRADING or not OKX_API_KEY:
            return []
        result = await self._request("GET", "/api/v5/trade/orders-pending", params=params, priority=priority)
        return result.get("data", [])

    async def get_order(self, coin: str, *, order_id: str = "", client_order_id: str = "", priority: str = "P0") -> dict:
        if PAPER_TRADING:
            return {"ordId": order_id, "clOrdId": client_order_id, "state": "filled"}
        params = {"instId": mm.inst_id(coin)}
        if order_id:
            params["ordId"] = order_id
        elif client_order_id:
            params["clOrdId"] = client_order_id
        else:
            return {}
        result = await self._request("GET", "/api/v5/trade/order", params=params, priority=priority)
        data = result.get("data", [])
        return dict(data[0]) if str(result.get("code", "0")) == "0" and data else {}

    async def get_order_status(self, coin: str, *, order_id: str = "", client_order_id: str = "", priority: str = "P0") -> dict:
        """Return FOUND, NOT_FOUND, or QUERY_FAILED semantics for recovery."""
        if PAPER_TRADING:
            order = await self.get_order(coin, order_id=order_id, client_order_id=client_order_id, priority=priority)
            return {
                "ok": True,
                "found": bool(order),
                "classification": "FOUND" if order else "NOT_FOUND",
                "order": order,
                "reason": "paper/local",
            }
        params = {"instId": mm.inst_id(coin)}
        if order_id:
            params["ordId"] = order_id
        elif client_order_id:
            params["clOrdId"] = client_order_id
        else:
            return {
                "ok": False,
                "found": False,
                "classification": "QUERY_FAILED",
                "order": {},
                "reason": "order_identifier_missing",
            }
        result = await self._request("GET", "/api/v5/trade/order", params=params, priority=priority)
        code = str(result.get("code", ""))
        data = result.get("data", []) or []
        not_found, evidence_code, evidence_message = _order_not_found_evidence(result)
        if not_found:
            return {
                "ok": True,
                "found": False,
                "classification": "NOT_FOUND",
                "order": {},
                "reason": "order_not_found",
                "exchange_code": evidence_code,
                "exchange_message": evidence_message,
            }
        if code == "0" and data:
            return {
                "ok": True,
                "found": True,
                "classification": "FOUND",
                "order": dict(data[0]),
                "reason": "",
            }
        return {
            "ok": False,
            "found": False,
            "classification": "QUERY_FAILED",
            "order": {},
            "reason": str(result.get("msg", "order_detail_failed") or "order_detail_failed"),
        }

    async def get_order_history(self, *, inst_id: str = "", limit: int = 100, priority: str = "historical") -> list[dict]:
        """Read recent normal orders for bounded uncertain-submit recovery."""
        if PAPER_TRADING:
            return []
        params = {"instType": "SWAP", "limit": str(max(min(int(limit), 100), 1))}
        if inst_id:
            params["instId"] = inst_id
        result = await self._request("GET", "/api/v5/trade/orders-history", params=params, priority=priority)
        return list(result.get("data", []) or []) if str(result.get("code", "0")) == "0" else []

    async def get_order_history_status(self, *, inst_id: str = "", limit: int = 100, priority: str = "historical") -> dict:
        if PAPER_TRADING:
            return {"ok": True, "orders": [], "reason": "paper/local"}
        params = {"instType": "SWAP", "limit": str(max(min(int(limit), 100), 1))}
        if inst_id:
            params["instId"] = inst_id
        result = await self._request("GET", "/api/v5/trade/orders-history", params=params, priority=priority)
        code = str(result.get("code", ""))
        return {
            "ok": code == "0",
            "orders": list(result.get("data", []) or []) if code == "0" else [],
            "reason": "" if code == "0" else str(result.get("msg", "order_history_failed") or "order_history_failed"),
        }

    async def get_fills(self, *, inst_id: str = "", order_id: str = "", limit: int = 100, priority: str = "historical") -> list[dict]:
        if PAPER_TRADING:
            return []
        params = {"instType": "SWAP", "limit": str(max(min(int(limit), 100), 1))}
        if inst_id:
            params["instId"] = inst_id
        if order_id:
            params["ordId"] = order_id
        result = await self._request("GET", "/api/v5/trade/fills", params=params, priority=priority)
        return list(result.get("data", []) or []) if str(result.get("code", "0")) == "0" else []

    async def get_fills_status(self, *, inst_id: str = "", order_id: str = "", limit: int = 100, priority: str = "historical") -> dict:
        if PAPER_TRADING:
            return {
                "ok": True,
                "status": "SUCCESS_EMPTY",
                "fills": [],
                "reason": "paper/local",
                "source": "okx.fills",
                "timestamp": time.time(),
            }
        params = {"instType": "SWAP", "limit": str(max(min(int(limit), 100), 1))}
        if inst_id:
            params["instId"] = inst_id
        if order_id:
            params["ordId"] = order_id
        result = await self._request("GET", "/api/v5/trade/fills", params=params, priority=priority)
        code = str(result.get("code", ""))
        data = list(result.get("data", []) or []) if code == "0" else []
        return {
            "ok": code == "0",
            "status": "SUCCESS_DATA" if code == "0" and data else ("SUCCESS_EMPTY" if code == "0" else "QUERY_FAILED"),
            "fills": data,
            "reason": "" if code == "0" else str(result.get("msg", "fills_failed") or "fills_failed"),
            "source": "okx.fills",
            "timestamp": time.time(),
        }

    async def get_bills(self, *, inst_id: str = "", limit: int = 100, priority: str = "historical") -> list[dict]:
        if PAPER_TRADING:
            return []
        params = {"instType": "SWAP", "limit": str(max(min(int(limit), 100), 1))}
        if inst_id:
            params["instId"] = inst_id
        result = await self._request("GET", "/api/v5/account/bills", params=params, priority=priority)
        return list(result.get("data", []) or []) if str(result.get("code", "0")) == "0" else []

    async def get_bills_status(self, *, inst_id: str = "", limit: int = 100, priority: str = "historical") -> dict:
        """Return funding evidence without collapsing failed reads into empty bills."""
        if PAPER_TRADING:
            return {
                "ok": True,
                "status": "SUCCESS_EMPTY",
                "bills": [],
                "reason": "paper/local",
                "source": "okx.bills",
                "timestamp": time.time(),
            }
        params = {"instType": "SWAP", "limit": str(max(min(int(limit), 100), 1))}
        if inst_id:
            params["instId"] = inst_id
        result = await self._request("GET", "/api/v5/account/bills", params=params, priority=priority)
        code = str(result.get("code", ""))
        data = list(result.get("data", []) or []) if code == "0" else []
        return {
            "ok": code == "0",
            "status": "SUCCESS_DATA" if code == "0" and data else ("SUCCESS_EMPTY" if code == "0" else "QUERY_FAILED"),
            "bills": data,
            "reason": "" if code == "0" else str(result.get("msg", "bills_failed") or "bills_failed"),
            "source": "okx.bills",
            "timestamp": time.time(),
        }

    async def get_position_history(self, *, inst_id: str = "", limit: int = 100, priority: str = "historical") -> list[dict]:
        if PAPER_TRADING:
            return []
        params = {"instType": "SWAP", "limit": str(max(min(int(limit), 100), 1))}
        if inst_id:
            params["instId"] = inst_id
        result = await self._request("GET", "/api/v5/account/positions-history", params=params, priority=priority)
        return list(result.get("data", []) or []) if str(result.get("code", "0")) == "0" else []

    async def get_position_history_status(self, *, inst_id: str = "", limit: int = 100, priority: str = "historical") -> dict:
        """Return realized-PnL history evidence with explicit query provenance."""
        if PAPER_TRADING:
            return {
                "ok": True,
                "status": "SUCCESS_EMPTY",
                "history": [],
                "reason": "paper/local",
                "source": "okx.position_history",
                "timestamp": time.time(),
            }
        params = {"instType": "SWAP", "limit": str(max(min(int(limit), 100), 1))}
        if inst_id:
            params["instId"] = inst_id
        result = await self._request("GET", "/api/v5/account/positions-history", params=params, priority=priority)
        code = str(result.get("code", ""))
        data = list(result.get("data", []) or []) if code == "0" else []
        return {
            "ok": code == "0",
            "status": "SUCCESS_DATA" if code == "0" and data else ("SUCCESS_EMPTY" if code == "0" else "QUERY_FAILED"),
            "history": data,
            "reason": "" if code == "0" else str(result.get("msg", "position_history_failed") or "position_history_failed"),
            "source": "okx.position_history",
            "timestamp": time.time(),
        }

    async def get_trade_fee(self, *, inst_type: str = "SWAP", inst_family: str = "") -> dict:
        """Read the authenticated fee schedule used by economic decisions."""
        if PAPER_TRADING:
            import settings_store as _s
            return {
                "instType": inst_type,
                "maker": str(_s.get_val("live_maker_fee_rate", 0.0002)),
                "taker": str(_s.get_val("live_taker_fee_rate", 0.0005)),
                "paper": True,
            }
        params = {"instType": str(inst_type or "SWAP").upper()}
        if inst_family:
            params["instFamily"] = str(inst_family)
        result = await self._request("GET", "/api/v5/account/trade-fee", params=params)
        data = list(result.get("data", []) or [])
        if str(result.get("code", "0")) != "0" or not data:
            raise RuntimeError(f"trade_fee_unavailable:{result.get('code')}:{result.get('msg')}")
        return dict(data[0])

    async def get_account_config(self, *, priority: str = "P1") -> dict:
        if PAPER_TRADING:
            return {"acctLv": "2", "posMode": "long_short_mode", "paper": True}
        result = await self._request("GET", "/api/v5/account/config", priority=priority)
        data = result.get("data", [])
        return dict(data[0]) if str(result.get("code", "0")) == "0" and data else {}

    async def get_leverage_info(self, coin: str, *, margin_mode: str = "isolated") -> list[dict]:
        inst_id = str(coin or "").upper()
        if "-USDT-" not in inst_id:
            inst_id = mm.inst_id(inst_id)
        if PAPER_TRADING:
            return [{
                "instId": inst_id, "lever": str(mm.get_leverage()),
                "mgnMode": margin_mode, "paper": True,
                "posSide": "long", "side": "long",
            }, {
                "instId": inst_id, "lever": str(mm.get_leverage()),
                "mgnMode": margin_mode, "paper": True,
                "posSide": "short", "side": "short",
            }]
        result = await self._request(
            "GET", "/api/v5/account/leverage-info",
            params={"instId": inst_id, "mgnMode": margin_mode},
            priority="leverage_truth",
        )
        return list(result.get("data", []) or []) if str(result.get("code", "0")) == "0" else []

    async def get_authoritative_leverage(
        self,
        coin: str,
        pos_side: str,
        *,
        requested_leverage: int = 1,
        margin_mode: str = "isolated",
        force: bool = False,
    ) -> dict:
        """Read configured leverage for this exact instrument and side.

        This method is intentionally read-only. Entry placement must never
        call ``set_leverage`` as a side effect of sizing or submission.
        """
        truth = await self._leverage_truth_cache.resolve(
            self,
            mm.inst_id(coin),
            pos_side,
            margin_mode,
            int(requested_leverage or mm.get_leverage()),
            force=force,
        )
        return truth.to_dict()

    async def place_protective_order(
        self, *, coin: str, direction: str, contracts: float, stop_price: float,
        take_profit: float = 0.0, client_order_id: str = "",
    ) -> dict:
        if PAPER_TRADING:
            paper_id = client_order_id or f"PAPER-PROTECT-{coin.upper()}-{uuid.uuid4().hex[:12]}"
            return {"code": "0", "data": [{"algoId": paper_id}], "paper": True}
        if str(mm.td_mode()).lower() != "isolated":
            raise RuntimeError("isolated_margin_required")
        algo_client_id = (
            validate_client_id(client_order_id, field="algoClOrdId")
            if client_order_id else ""
        )
        close_side = "sell" if direction.lower() in {"long", "buy"} else "buy"
        pos_side = "long" if direction.lower() in {"long", "buy"} else "short"
        body = {
            "instId": mm.inst_id(coin),
            "tdMode": "isolated",
            "side": close_side,
            "posSide": pos_side,
            "ordType": "oco" if take_profit > 0 else "conditional",
            "sz": str(contracts),
            "reduceOnly": "true",
            "slTriggerPx": str(stop_price),
            "slOrdPx": "-1",
            "slTriggerPxType": "last",
        }
        if take_profit > 0:
            body.update({"tpTriggerPx": str(take_profit), "tpOrdPx": "-1", "tpTriggerPxType": "last"})
        if client_order_id:
            body["algoClOrdId"] = algo_client_id
        result = await self._request("POST", "/api/v5/trade/order-algo", data=body)
        classification = classify_mutation_response(
            result,
            expected_items=1,
            submitted_client_id=algo_client_id,
        )
        if classification.outcome == MUTATION_DEFINITIVE_REJECTION:
            raise OKXDefinitiveRejection(
                classification.item_s_msg or classification.top_level_msg or "protective order rejected",
                code=classification.top_level_code,
                s_code=classification.item_s_code,
                evidence=classification.to_dict(),
            )
        return result

    async def cancel_protective_orders(self, orders: list[dict]) -> dict:
        if PAPER_TRADING:
            return {"code": "0", "data": [], "paper": True}
        payload = [{"instId": str(row["instId"]), "algoId": str(row["algoId"])} for row in orders]
        result = await self._request("POST", "/api/v5/trade/cancel-algos", data=payload)
        classification = classify_mutation_response(result, expected_items=len(payload))
        if classification.outcome == MUTATION_DEFINITIVE_REJECTION:
            raise OKXDefinitiveRejection(
                classification.item_s_msg or classification.top_level_msg or "protection cancellation rejected",
                code=classification.top_level_code,
                s_code=classification.item_s_code,
                evidence=classification.to_dict(),
            )
        return result

    def get_risk_summary(self, balance_usdt: float) -> str:
        return risk.summary(balance_usdt)

    async def close_futures_position(
        self, coin: str, pos_side: str, size_usdt: float,
        *, contracts_override: float | None = None, client_order_id: str = "",
        instrument_metadata=None, margin_mode: str = "", position_mode: str = "",
        verify_authoritative_position: bool = False, before_send=None,
    ) -> dict:
        """
        Close a futures position using reduceOnly market order.
        pos_side: "long" or "short"

        Returns a result dict. If the position is already closed on the exchange
        (e.g. TP/SL was hit), returns a synthetic result rather than raising.
        """
        coin = coin.upper()
        inst = str(getattr(instrument_metadata, "inst_id", "") or mm.inst_id(coin))

        if float(size_usdt or 0.0) <= 0.0 and not (contracts_override is not None and float(contracts_override) > 0):
            logger.info(
                "close_futures_position skipped %s pos_side=%s size_usdt=%.8f",
                coin,
                pos_side,
                float(size_usdt or 0.0),
            )
            return {
                "ordId": f"SKIPPED-ZERO-{coin}",
                "actual_coin": coin,
                "state": "skipped",
                "exchange_closed": False,
                "post_attempted": False,
            }

        if PAPER_TRADING:
            return {
                "ordId":       client_order_id or f"PAPER-CLOSE-{coin}-{uuid.uuid4().hex[:12]}",
                "actual_coin": coin,
                "state":       "filled",
                "paper":       True,
                "post_attempted": False,
            }

        resolved_position_mode = str(position_mode or "").lower()
        if verify_authoritative_position and not resolved_position_mode:
            reader = getattr(self, "get_account_config", None)
            if not callable(reader):
                raise RuntimeError("close_position_mode_unavailable:reader_missing")
            try:
                try:
                    config = await reader(priority="P0")
                except TypeError:
                    config = await reader()
            except Exception as exc:
                raise RuntimeError(f"close_position_mode_unavailable:{exc}") from exc
            resolved_position_mode = str((config or {}).get("posMode", "") or "").lower()
        if verify_authoritative_position and resolved_position_mode not in {"net_mode", "long_short_mode"}:
            raise RuntimeError(f"close_position_mode_unknown:{resolved_position_mode or 'missing'}")

        # Operator closes re-read authoritative quantity immediately before
        # the mutation. Strategy exits retain the legacy compatibility path.
        if verify_authoritative_position:
            status = await self.get_positions_status(force_refresh=True, priority="P0")
            if not bool(status.get("ok", False)):
                raise RuntimeError(f"close_position_truth_unavailable:{status.get('reason', 'unknown')}")
            live_row = None
            for row in list(status.get("positions", []) or []):
                if str(row.get("instId", "")).upper() != inst.upper():
                    continue
                row_side = str(row.get("posSide", "") or "").lower()
                if row_side in {"long", "short"} and row_side != str(pos_side or "").lower():
                    continue
                try:
                    live_qty = abs(float(row.get("pos", row.get("availPos", 0.0)) or 0.0))
                except Exception:
                    live_qty = 0.0
                if live_qty > 0:
                    live_row = row
                    break
            if live_row is None:
                return {
                    "ordId": f"EXCHANGE-CLOSED-{coin}",
                    "actual_coin": coin,
                    "state": "filled",
                    "exchange_closed": True,
                    "mutation_outcome": MUTATION_SUCCESS,
                    "post_attempted": False,
                }
            try:
                live_qty = abs(float(live_row.get("pos", live_row.get("availPos", 0.0)) or 0.0))
            except Exception:
                live_qty = 0.0
            if contracts_override is not None and float(contracts_override) > 0:
                contracts_override = min(float(contracts_override), live_qty)
            else:
                contracts_override = live_qty
            if contracts_override <= 0:
                return {
                    "ordId": f"EXCHANGE-CLOSED-{coin}",
                    "actual_coin": coin,
                    "state": "filled",
                    "exchange_closed": True,
                    "mutation_outcome": MUTATION_SUCCESS,
                    "post_attempted": False,
                }
        else:
            # Preserve the existing strategy-exit behavior: an exchange
            # closure observed before the POST is a read-only synthetic result.
            try:
                status = await self.get_live_open_coins_status()
                if bool(status.get("ok", False)) and coin not in set(status.get("coins", set()) or set()):
                    return {
                        "ordId": f"EXCHANGE-CLOSED-{coin}",
                        "actual_coin": coin,
                        "state": "filled",
                        "exchange_closed": True,
                        "mutation_outcome": MUTATION_SUCCESS,
                        "post_attempted": False,
                    }
            except Exception:
                pass

        try:
            ticker = await self.get_execution_ticker(coin)
            price  = ticker["last"]
        except Exception as exc:
            self._set_last_order_skip_reason(f"close_price_unavailable:{type(exc).__name__}:{exc}")
            return {
                "ok": False,
                "status": "CLOSE_FAILED_BEFORE_SUBMISSION",
                "reason": "close_price_unavailable",
                "post_attempted": False,
            }

        try:
            sz = float(contracts_override) if contracts_override is not None and float(contracts_override) > 0 else await self._futures_sz(coin, size_usdt, price)
        except Exception as exc:
            self._set_last_order_skip_reason(f"close_quantity_conversion_failed:{type(exc).__name__}:{exc}")
            return {
                "ok": False,
                "status": "CLOSE_FAILED_BEFORE_SUBMISSION",
                "reason": "close_quantity_conversion_failed",
                "post_attempted": False,
            }

        side = "sell" if pos_side == "long" else "buy"

        body = {
            "instId":     inst,
            "tdMode":     str(margin_mode or mm.td_mode()).lower(),
            "side":       side,
            "ordType":    "market",
            "sz":         str(sz),
            "reduceOnly": "true",
        }
        body["posSide"] = "net" if resolved_position_mode == "net_mode" else pos_side
        if client_order_id:
            body["clOrdId"] = validate_client_id(client_order_id, field="clOrdId")

        logger.info(
            "Closing order: coin=%s pos_side=%s close_side=%s size_usdt=%.2f "
            "contracts=%s price=%.8f body=%s",
            coin, pos_side, side, size_usdt, sz, price, body,
        )
        request_kwargs = {"data": body}
        if before_send is not None:
            request_kwargs["before_send"] = before_send
        result = await self._request("POST", "/api/v5/trade/order", **request_kwargs)
        logger.info("OKX close response: %s", result)

        classification = classify_mutation_response(
            result, expected_items=1, submitted_client_id=client_order_id,
        )
        if classification.outcome == MUTATION_DEFINITIVE_REJECTION:
            raise OKXDefinitiveRejection(
                classification.item_s_msg or classification.top_level_msg or "close rejected",
                code=classification.top_level_code,
                s_code=classification.item_s_code,
                evidence=classification.to_dict(),
            )
        if classification.outcome == MUTATION_READBACK_REQUIRED:
            return {
                "actual_coin": coin,
                "mutation_outcome": MUTATION_READBACK_REQUIRED,
                "mutation_evidence": classification.to_dict(),
                "clOrdId": client_order_id,
            }

        order = dict(result.get("data", [{}])[0] or {})
        okx_fill = order.get("fillPx", "") or order.get("avgPx", "")
        real_fill = float(okx_fill) if okx_fill and float(okx_fill) > 0 else price
        order["fillPx"]      = real_fill
        order["actual_coin"] = coin
        order["mutation_outcome"] = MUTATION_SUCCESS
        order["mutation_evidence"] = classification.to_dict()
        order["exchange_order_id"] = classification.exchange_order_id or str(order.get("ordId", "") or "")
        order["client_order_id"] = client_order_id
        order["post_attempted"] = True
        return order

    async def get_algo_orders(self, coin: str | None = None, *, force_refresh: bool = False, priority: str = "P0") -> list[dict]:
        """
        Fetch pending algo orders (attached TP/SL) for a coin or all positions.
        Useful for verifying TP/SL were successfully attached after order placement.
        """
        if PAPER_TRADING or not OKX_API_KEY:
            return []
        params: dict = {"ordType": "conditional", "instType": "SWAP"}
        if coin:
            params["instId"] = mm.inst_id(coin)
        async def fetch():
            result = await self._request(
                "GET", "/api/v5/trade/orders-algo-pending", params=params, priority=priority
            )
            if str(result.get("code", "0")) != "0":
                raise RuntimeError(f"algo_orders:{result.get('code', '')}:{result.get('msg', '')}")
            return list(result.get("data", []) or [])
        try:
            return await _coalesced_truth_read(
                f"algo_orders:{params.get('instId', '*')}:{_truth_priority_tier(priority)}", fetch,
                ttl_sec=1.0, force=force_refresh,
                join_keys=_truth_join_keys(
                    f"algo_orders:{params.get('instId', '*')}",
                    _truth_priority_tier(priority),
                ),
            )
        except Exception as e:
            logger.debug("get_algo_orders failed: %s", e)
            return []

    async def list_live_protection_orders(
        self,
        coin: str | None = None,
        *,
        pos_side: str | None = None,
        force_refresh: bool = False,
        priority: str = "P0",
    ) -> dict:
        """Return the complete live exchange protection view for a position.

        OKX exposes attached OCO protection and standalone conditional
        protection through separate ``ordType`` filters.  A successful query
        of only one feed is not sufficient to decide whether a fill is already
        protected, because treating the other feed as empty can create a
        duplicate live order.
        """
        if PAPER_TRADING:
            return {"ok": True, "orders": [], "errors": [], "queried_types": []}
        if not OKX_API_KEY:
            return {"ok": False, "orders": [], "errors": ["missing_api_key"], "queried_types": []}

        inst_id = mm.inst_id(coin) if coin else ""
        async def fetch():
            orders: list[dict] = []
            errors: list[str] = []
            queried_types: list[str] = []
            for ord_type in ("oco", "conditional"):
                params: dict[str, str] = {"ordType": ord_type, "instType": "SWAP"}
                if inst_id:
                    params["instId"] = inst_id
                queried_types.append(ord_type)
                try:
                    result = await self._request(
                        "GET", "/api/v5/trade/orders-algo-pending", params=params,
                        priority=priority,
                    )
                    if str(result.get("code", "0")) != "0":
                        errors.append(f"{ord_type}:code={result.get('code', '')}")
                        continue
                    for row in list(result.get("data", []) or []):
                        normalized = dict(row)
                        normalized["_protection_ord_type"] = ord_type
                        orders.append(normalized)
                except Exception as exc:
                    errors.append(f"{ord_type}:{type(exc).__name__}")
            if errors:
                raise RuntimeError(";".join(errors))
            return {"ok": True, "orders": orders, "errors": [], "queried_types": queried_types}

        started = time.monotonic()
        try:
            result = await _coalesced_truth_read(
                f"protection_orders:{inst_id or '*'}:{_truth_priority_tier(priority)}", fetch,
                ttl_sec=1.0, force=force_refresh,
                join_keys=_truth_join_keys(
                    f"protection_orders:{inst_id or '*'}",
                    _truth_priority_tier(priority),
                ),
            )
        except Exception as exc:
            return {"ok": False, "orders": [], "errors": [str(exc)], "queried_types": ["oco", "conditional"]}
        finally:
            _record_protection_latency(time.monotonic() - started)
        orders = [
            row for row in list(result.get("orders", []) or [])
            if not pos_side or str(row.get("posSide", "")).lower() == str(pos_side).lower()
        ]
        return {
            "ok": True,
            "orders": orders,
            "errors": [],
            "queried_types": list(result.get("queried_types", []) or []),
        }

    async def get_last_fill_price(self, coin: str) -> float | None:
        """
        Fetch the actual fill price of the most recent executed trade for a coin.
        Used by exit_manager._handle_exchange_closed() to report accurate exit
        prices when exchange algo TP/SL orders fire, instead of using stale
        latest_price() which reflects the market at sync time, not at fill time.
        Returns None if unavailable (caller should fall back to latest_price).
        """
        if PAPER_TRADING or not OKX_API_KEY:
            return None
        inst = mm.inst_id(coin)
        try:
            result = await self._request(
                "GET", "/api/v5/trade/fills",
                params={"instType": "SWAP", "instId": inst, "limit": "1"},
                priority="P0",
            )
            data = result.get("data", [])
            if data and data[0].get("fillPx"):
                fill_px = float(data[0]["fillPx"])
                if fill_px > 0:
                    logger.debug("get_last_fill_price %s → %.8f", coin, fill_px)
                    return fill_px
        except Exception as e:
            logger.debug("get_last_fill_price %s: %s", coin, e)
        return None

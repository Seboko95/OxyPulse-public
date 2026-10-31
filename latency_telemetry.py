"""Bounded, observational latency metrics for one scan cycle.

This module deliberately has no network or filesystem access.  Callers record
events in memory and the scan coordinator decides when the bounded snapshot is
persisted alongside the existing scan report.
"""
from __future__ import annotations

import threading
import time
import math
from contextvars import Context, ContextVar, Token, copy_context


SCHEMA_VERSION = 2
DEFAULT_TOP_N = 10
MAX_ENGINE_KEYS = 64
MAX_ENDPOINT_KEYS = 64
REST_REQUEST_CLASSES = ("critical", "background", "other")
OVERFLOW_KEY = "__other__"

# Process-wide shared-data telemetry.  These counters are observational only;
# the scan coordinator persists the bounded snapshot with the existing report.
_SHARED_RESOURCE_LOCK = threading.RLock()
_SHARED_RESOURCES: dict[str, dict] = {}
_MARKET_REQUEST_LOCK = threading.RLock()
_MARKET_REQUESTS: dict[str, dict] = {}
_MAX_MARKET_REQUEST_KEYS = 4096
_MARKET_REQUEST_TOTALS = {
    "logical_requests": 0,
    "cache_hits": 0,
    "coalesced_callers": 0,
    "underlying_requests": 0,
    "sequential_duplicate_count": 0,
    "concurrent_duplicate_count": 0,
}
_MARKET_REQUEST_REASON_TOTALS: dict[str, dict[str, int]] = {}
_SIGNAL_AGE_LOCK = threading.RLock()
_SIGNAL_AGE_SAMPLES: list[dict] = []
_MAX_SIGNAL_AGE_SAMPLES = 512
_ENGINE_DEPENDENCY_LOCK = threading.RLock()
_ENGINE_DEPENDENCIES: dict[str, dict] = {}


def record_signal_age(
    *,
    scan_generation: str | int = "unknown",
    instrument: str,
    engine: str,
    decision_time: float,
    ticker_observed_at: float | str | None = None,
    one_minute_observed_at: float | str | None = None,
    relevant_htf_observed_at: float | str | None = None,
    oldest_required_input_age: float | None = None,
    newest_input_age: float | None = None,
    engine_started_at: float | None = None,
    engine_completed_at: float | None = None,
    candidate_created_at: float | None = None,
    candidate_handoff_at: float | None = None,
    ws_connection_generation: int | None = None,
    rest_fallback_used: bool = False,
    gap_repair_state: str = "",
    one_minute_bar_age: float | None = None,
    one_minute_transport_age: float | None = None,
    one_minute_confirmed_complete: bool | None = None,
    relevant_htf_bar_age: float | None = None,
    relevant_htf_transport_age: float | None = None,
    relevant_htf_confirmed_complete: bool | None = None,
    ticker_transport_age: float | None = None,
    one_minute_bar_open_ts: float | None = None,
    relevant_htf_bar_open_ts: float | None = None,
) -> None:
    """Store bounded signal-age evidence without changing strategy decisions."""
    decision = float(decision_time or time.time())

    def _number(value):
        try:
            return float(value) if value not in (None, "") else None
        except (TypeError, ValueError):
            return None

    observations = [
        value for value in (
            _number(ticker_observed_at),
            _number(one_minute_observed_at),
            _number(relevant_htf_observed_at),
        ) if value is not None and value > 0.0
    ]
    ages = [max(decision - value, 0.0) for value in observations]
    sample = {
        "scan_generation": str(scan_generation or "unknown"),
        "instrument": str(instrument or "").upper(),
        "engine": str(engine or "").upper(),
        "decision_time": decision,
        "ticker_observed_at": _number(ticker_observed_at),
        "1m_observed_at": _number(one_minute_observed_at),
        "relevant_HTF_observed_at": _number(relevant_htf_observed_at),
        "oldest_required_input_age": (
            max(ages) if ages else max(float(oldest_required_input or 0.0), 0.0)
        ),
        "newest_input_age": (
            min(ages) if ages else max(float(newest_input_age or 0.0), 0.0)
        ),
        "engine_started_at": _number(engine_started_at),
        "engine_completed_at": _number(engine_completed_at),
        "candidate_created_at": _number(candidate_created_at),
        "candidate_handoff_at": _number(candidate_handoff_at),
        "ticker_age_at_decision": max(decision - _number(ticker_observed_at), 0.0) if _number(ticker_observed_at) else None,
        "1m_age_at_decision": max(decision - _number(one_minute_observed_at), 0.0) if _number(one_minute_observed_at) else None,
        "HTF_age_at_decision": max(decision - _number(relevant_htf_observed_at), 0.0) if _number(relevant_htf_observed_at) else None,
        "1m_bar_age_at_decision": max(float(one_minute_bar_age or 0.0), 0.0),
        "1m_transport_age_at_decision": max(float(one_minute_transport_age or 0.0), 0.0),
        "1m_confirmed_complete": one_minute_confirmed_complete,
        "1m_bar_open_ts": _number(one_minute_bar_open_ts),
        "HTF_bar_age_at_decision": max(float(relevant_htf_bar_age or 0.0), 0.0),
        "HTF_transport_age_at_decision": max(float(relevant_htf_transport_age or 0.0), 0.0),
        "HTF_confirmed_complete": relevant_htf_confirmed_complete,
        "HTF_bar_open_ts": _number(relevant_htf_bar_open_ts),
        "ticker_transport_age": max(float(ticker_transport_age or 0.0), 0.0),
        "ws_connection_generation": int(ws_connection_generation or 0),
        "rest_fallback_used": bool(rest_fallback_used),
        "gap_repair_state": str(gap_repair_state or ""),
    }
    if sample["engine_started_at"] is not None and sample["engine_completed_at"] is not None:
        sample["engine_duration_sec"] = max(
            sample["engine_completed_at"] - sample["engine_started_at"], 0.0
        )
    else:
        sample["engine_duration_sec"] = None
    if sample["candidate_created_at"] is not None and sample["candidate_handoff_at"] is not None:
        sample["candidate_handoff_delay_sec"] = max(
            sample["candidate_handoff_at"] - sample["candidate_created_at"], 0.0
        )
    else:
        sample["candidate_handoff_delay_sec"] = None
    with _SIGNAL_AGE_LOCK:
        _SIGNAL_AGE_SAMPLES.append(sample)
        del _SIGNAL_AGE_SAMPLES[:-_MAX_SIGNAL_AGE_SAMPLES]


def signal_age_snapshot() -> dict:
    with _SIGNAL_AGE_LOCK:
        samples = [dict(row) for row in _SIGNAL_AGE_SAMPLES]

    def _percentiles(field: str) -> dict[str, float]:
        values = sorted(
            float(row[field]) for row in samples
            if row.get(field) is not None
        )
        if not values:
            return {"p50": 0.0, "p95": 0.0, "max": 0.0}
        return {
            "p50": round(values[(len(values) - 1) // 2], 6),
            "p95": round(values[max(0, min(len(values) - 1, math.ceil(len(values) * 0.95) - 1))], 6),
            "max": round(values[-1], 6),
        }

    return {
        "sample_count": len(samples),
        "oldest_required_input_age_sec": _percentiles("oldest_required_input_age"),
        "newest_input_age_sec": _percentiles("newest_input_age"),
        "1m_age_at_decision_sec": _percentiles("1m_age_at_decision"),
        "HTF_age_at_decision_sec": _percentiles("HTF_age_at_decision"),
        "1m_bar_age_at_decision_sec": _percentiles("1m_bar_age_at_decision"),
        "1m_transport_age_at_decision_sec": _percentiles("1m_transport_age_at_decision"),
        "HTF_bar_age_at_decision_sec": _percentiles("HTF_bar_age_at_decision"),
        "HTF_transport_age_at_decision_sec": _percentiles("HTF_transport_age_at_decision"),
        "ticker_transport_age_sec": _percentiles("ticker_transport_age"),
        "ticker_age_at_decision_sec": _percentiles("ticker_age_at_decision"),
        "engine_duration_sec": _percentiles("engine_duration_sec"),
        "candidate_handoff_delay_sec": _percentiles("candidate_handoff_delay_sec"),
        "samples": samples[-128:],
    }


def reset_signal_age_metrics() -> None:
    with _SIGNAL_AGE_LOCK:
        _SIGNAL_AGE_SAMPLES.clear()


def record_engine_dependency(
    *,
    engine: str,
    instrument: str,
    operation: str,
    data_type: str,
    duration_sec: float,
    outcome: str = "ok",
) -> None:
    """Attribute pre-strategy market dependencies to their actual wait."""
    key = f"{str(engine or 'unknown').upper()}|{str(operation or 'unknown')[:80]}"
    duration = max(float(duration_sec or 0.0), 0.0)
    with _ENGINE_DEPENDENCY_LOCK:
        row = _ENGINE_DEPENDENCIES.setdefault(
            key,
            {
                "engine": str(engine or "unknown").upper(),
                "operation": str(operation or "unknown")[:80],
                "data_type": str(data_type or "unknown")[:80],
                "request_count": 0,
                "total_duration_sec": 0.0,
                "max_duration_sec": 0.0,
                "success_count": 0,
                "failure_count": 0,
                "instruments": {},
            },
        )
        row["request_count"] += 1
        row["total_duration_sec"] += duration
        row["max_duration_sec"] = max(row["max_duration_sec"], duration)
        if str(outcome or "").lower() == "ok":
            row["success_count"] += 1
        else:
            row["failure_count"] += 1
        instrument_key = str(instrument or "unknown").upper()[:40]
        if instrument_key in row["instruments"] or len(row["instruments"]) < 128:
            row["instruments"][instrument_key] = int(row["instruments"].get(instrument_key, 0) or 0) + 1
        else:
            row["instruments"]["__other__"] = int(row["instruments"].get("__other__", 0) or 0) + 1


def engine_dependency_snapshot() -> dict:
    with _ENGINE_DEPENDENCY_LOCK:
        return {key: dict(value) for key, value in _ENGINE_DEPENDENCIES.items()}


def reset_engine_dependency_metrics() -> None:
    with _ENGINE_DEPENDENCY_LOCK:
        _ENGINE_DEPENDENCIES.clear()


def record_market_request(
    *,
    normalized_key: str,
    instrument: str,
    timeframe: str,
    bar: str,
    limit: int,
    caller: str = "market_data_stream",
    scan_generation: str | int = "unknown",
    logical: int = 1,
    cache_hit: bool = False,
    coalesced: bool = False,
    underlying: bool = False,
    sequential_duplicate: bool = False,
    concurrent_duplicate: bool = False,
    success: bool | None = None,
    request_reason: str = "EXPLICIT_FALLBACK",
    trigger_type: str = "EXPLICIT_CALL",
) -> None:
    """Record bounded market-request identity and reuse observations."""
    key = str(normalized_key or "unknown")[:240]
    with _MARKET_REQUEST_LOCK:
        if key not in _MARKET_REQUESTS and len(_MARKET_REQUESTS) >= _MAX_MARKET_REQUEST_KEYS:
            key = "__other__"
        row = _MARKET_REQUESTS.setdefault(
            key,
            {
                "normalized_key": key,
                "instrument": str(instrument or "").upper(),
                "timeframe": str(timeframe or "").lower(),
                "bar": str(bar or ""),
                "limit": int(limit or 0),
                "logical_requests": 0,
                "cache_hits": 0,
                "coalesced_callers": 0,
                "underlying_requests": 0,
                "sequential_duplicate_count": 0,
                "concurrent_duplicate_count": 0,
                "success_count": 0,
                "failure_count": 0,
                "callers": {},
                "scan_generations": {},
                "request_reasons": {},
                "trigger_types": {},
            },
        )
        reason = str(request_reason or "OTHER").upper()[:48]
        trigger = str(trigger_type or "EXPLICIT_CALL")[:64]
        row["request_reasons"][reason] = int(row["request_reasons"].get(reason, 0) or 0) + 1
        row["trigger_types"][trigger] = int(row["trigger_types"].get(trigger, 0) or 0) + 1
        reason_totals = _MARKET_REQUEST_REASON_TOTALS.setdefault(reason, {})
        reason_totals["logical"] = int(reason_totals.get("logical", 0) or 0) + max(int(logical or 0), 0)
        reason_totals["underlying"] = int(reason_totals.get("underlying", 0) or 0) + int(bool(underlying))
        reason_totals["cache_hits"] = int(reason_totals.get("cache_hits", 0) or 0) + int(bool(cache_hit))
        reason_totals["coalesced"] = int(reason_totals.get("coalesced", 0) or 0) + int(bool(coalesced))
        row["logical_requests"] += max(int(logical or 0), 0)
        _MARKET_REQUEST_TOTALS["logical_requests"] += max(int(logical or 0), 0)
        for flag, field in (
            (cache_hit, "cache_hits"),
            (coalesced, "coalesced_callers"),
            (underlying, "underlying_requests"),
            (sequential_duplicate, "sequential_duplicate_count"),
            (concurrent_duplicate, "concurrent_duplicate_count"),
        ):
            if flag:
                row[field] += 1
                _MARKET_REQUEST_TOTALS[field] += 1
        if int(logical or 0) > 0:
            caller_key = str(caller or "unknown")[:80]
            row["callers"][caller_key] = int(row["callers"].get(caller_key, 0) or 0) + int(logical)
            generation_key = str(scan_generation or "unknown")[:80]
            if generation_key not in row["scan_generations"] and len(row["scan_generations"]) >= 32:
                generation_key = "__other__"
            row["scan_generations"][generation_key] = (
                int(row["scan_generations"].get(generation_key, 0) or 0) + int(logical)
            )
        if success is True:
            row["success_count"] += 1
        elif success is False:
            row["failure_count"] += 1


def market_request_snapshot() -> dict:
    """Return bounded request-identity and duplicate-reuse telemetry."""
    with _MARKET_REQUEST_LOCK:
        rows = {key: dict(value) for key, value in _MARKET_REQUESTS.items()}
        totals = dict(_MARKET_REQUEST_TOTALS)
    per_instrument: dict[str, int] = {}
    per_engine: dict[str, int] = {}
    per_shape: dict[str, int] = {}
    requests_per_key: dict[str, int] = {}
    for key, row in rows.items():
        count = int(row.get("logical_requests", 0) or 0)
        requests_per_key[key] = count
        instrument = str(row.get("instrument", "unknown"))
        per_instrument[instrument] = per_instrument.get(instrument, 0) + count
        shape = f"{row.get('timeframe', 'unknown')}|{row.get('bar', '')}|{row.get('limit', 0)}"
        per_shape[shape] = per_shape.get(shape, 0) + count
        for caller, caller_count in (row.get("callers", {}) or {}).items():
            per_engine[caller] = per_engine.get(caller, 0) + int(caller_count or 0)
    return {
        "totals": totals,
        "unique_normalized_keys": len(rows),
        "requests_per_unique_key": requests_per_key,
        "requests_per_instrument": per_instrument,
        "requests_per_engine_or_caller": per_engine,
        "requests_per_request_shape": per_shape,
        "by_request_reason": {
            reason: dict(values) for reason, values in _MARKET_REQUEST_REASON_TOTALS.items()
        },
        "keys": rows,
    }


def reset_market_request_metrics() -> None:
    with _MARKET_REQUEST_LOCK:
        _MARKET_REQUESTS.clear()
        for key in _MARKET_REQUEST_TOTALS:
            _MARKET_REQUEST_TOTALS[key] = 0
        _MARKET_REQUEST_REASON_TOTALS.clear()


def record_shared_resource(
    domain: str,
    *,
    logical: int = 0,
    coalesced: int = 0,
    underlying: int = 0,
    cache_hits: int = 0,
    failures: int = 0,
    wait_sec: float = 0.0,
    singleflight_join_wait_sec: float = 0.0,
) -> None:
    key = str(domain or "other")[:80]
    with _SHARED_RESOURCE_LOCK:
        row = _SHARED_RESOURCES.setdefault(
            key,
            {
                "logical_callers": 0,
                "coalesced_callers": 0,
                "underlying_requests": 0,
                "cache_hits": 0,
                "failures": 0,
                "caller_wait_sec": 0.0,
                "caller_wait_samples": [],
                "singleflight_join_wait_sec": 0.0,
                "singleflight_join_wait_samples": [],
            },
        )
        for field, value in (
            ("logical_callers", logical),
            ("coalesced_callers", coalesced),
            ("underlying_requests", underlying),
            ("cache_hits", cache_hits),
            ("failures", failures),
        ):
            row[field] = int(row.get(field, 0) or 0) + max(int(value or 0), 0)
        # ``wait_sec`` is retained as a compatibility input, but is explicitly
        # named caller wait in the snapshot. It is never reported as lock wait.
        caller_wait = max(float(wait_sec or 0.0), 0.0)
        join_wait = max(float(singleflight_join_wait_sec or 0.0), 0.0)
        row["caller_wait_sec"] = float(row.get("caller_wait_sec", 0.0) or 0.0) + caller_wait
        row["singleflight_join_wait_sec"] = (
            float(row.get("singleflight_join_wait_sec", 0.0) or 0.0) + join_wait
        )
        if caller_wait > 0.0:
            samples = row.setdefault("caller_wait_samples", [])
            samples.append(round(caller_wait, 6))
            del samples[:-256]
        if join_wait > 0.0:
            samples = row.setdefault("singleflight_join_wait_samples", [])
            samples.append(round(join_wait, 6))
            del samples[:-256]


def shared_resource_snapshot() -> dict[str, dict]:
    with _SHARED_RESOURCE_LOCK:
        result = {key: dict(value) for key, value in _SHARED_RESOURCES.items()}
    for row in result.values():
        logical = int(row.get("logical_callers", 0) or 0)
        caller_samples = sorted(float(value) for value in row.pop("caller_wait_samples", []) or [])
        join_samples = sorted(float(value) for value in row.pop("singleflight_join_wait_samples", []) or [])

        def _percentiles(samples: list[float]) -> dict[str, float]:
            if not samples:
                return {"p50": 0.0, "p95": 0.0, "max": 0.0}
            def _pct(percentile: float) -> float:
                index = max(0, min(len(samples) - 1, math.ceil(percentile * len(samples)) - 1))
                return round(samples[index], 6)
            return {"p50": _pct(0.50), "p95": _pct(0.95), "max": round(samples[-1], 6)}

        caller = _percentiles(caller_samples)
        join = _percentiles(join_samples)
        row["caller_wait_p50_sec"] = caller["p50"]
        row["caller_wait_p95_sec"] = caller["p95"]
        row["caller_wait_max_sec"] = caller["max"]
        row["singleflight_join_wait_p50_sec"] = join["p50"]
        row["singleflight_join_wait_p95_sec"] = join["p95"]
        row["singleflight_join_wait_max_sec"] = join["max"]
        row["coalescing_ratio"] = round(
            int(row.get("coalesced_callers", 0) or 0) / logical if logical else 0.0,
            4,
        )
    return result


def reset_shared_resource_metrics() -> None:
    with _SHARED_RESOURCE_LOCK:
        _SHARED_RESOURCES.clear()

_CURRENT_SCAN: ContextVar["ScanLatencyTelemetry | None"] = ContextVar(
    "oxypulse_current_scan_latency", default=None
)


def activate(collector: "ScanLatencyTelemetry") -> Token:
    return _CURRENT_SCAN.set(collector)


def reset(token: Token) -> None:
    _CURRENT_SCAN.reset(token)


def current() -> "ScanLatencyTelemetry | None":
    return _CURRENT_SCAN.get()


def detached_context() -> Context:
    """Return a child-task context that cannot write to a foreground scan."""
    context = copy_context()
    context.run(_CURRENT_SCAN.set, None)
    return context


def _top(items: list[dict], row: dict, limit: int) -> None:
    items.append(row)
    items.sort(key=lambda item: float(item.get("duration_sec", 0.0) or 0.0), reverse=True)
    del items[limit:]


def _bounded_key(mapping: dict, key: str, limit: int) -> str:
    key = str(key or "unknown")[:120]
    if key in mapping or key == OVERFLOW_KEY:
        return key
    if len(mapping) >= max(int(limit) - 1, 1):
        return OVERFLOW_KEY
    return key


def _safe_endpoint_key(value: str) -> str:
    raw = str(value or "unknown").split("?", 1)[0].strip()
    if not raw.startswith("/"):
        return "unknown"
    if any(ch not in "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_./-" for ch in raw):
        return "unknown"
    return raw[:120]


def _normalize_request_class(value: str | None) -> str:
    key = str(value or "").strip().lower()
    return key if key in REST_REQUEST_CLASSES else "other"


def _new_rest_aggregate() -> dict[str, float | int]:
    return {
        "request_count": 0,
        "total_wall_time_sec": 0.0,
        "max_wall_time_sec": 0.0,
        "retry_count": 0,
        "physical_attempt_count": 0,
        "pool_timeout_count": 0,
        "timeout_count": 0,
        "error_count": 0,
        "lock_wait_sec": 0.0,
    }


def _record_rest_aggregate(
    row: dict[str, float | int],
    duration: float,
    retries: int,
    physical_attempts: int | None,
    pool_timeouts: int,
    outcome_key: str,
    lock_wait_sec: float = 0.0,
) -> None:
    row["request_count"] = int(row.get("request_count", 0) or 0) + 1
    row["total_wall_time_sec"] = float(row.get("total_wall_time_sec", 0.0) or 0.0) + duration
    row["max_wall_time_sec"] = max(float(row.get("max_wall_time_sec", 0.0) or 0.0), duration)
    row["retry_count"] = int(row.get("retry_count", 0) or 0) + max(int(retries), 0)
    row["physical_attempt_count"] = int(row.get("physical_attempt_count", 0) or 0) + max(
        int(physical_attempts if physical_attempts is not None else 1 + max(int(retries), 0)), 0
    )
    row["pool_timeout_count"] = int(row.get("pool_timeout_count", 0) or 0) + max(int(pool_timeouts), 0)
    row["lock_wait_sec"] = float(row.get("lock_wait_sec", 0.0) or 0.0) + max(float(lock_wait_sec), 0.0)
    if outcome_key == "timeout":
        row["timeout_count"] = int(row.get("timeout_count", 0) or 0) + 1
    elif outcome_key != "ok":
        row["error_count"] = int(row.get("error_count", 0) or 0) + 1


class ScanLatencyTelemetry:
    """Thread-safe bounded accumulator for one scan."""

    def __init__(self, cycle_number: int, *, top_n: int = DEFAULT_TOP_N) -> None:
        self.cycle_number = int(cycle_number)
        self.top_n = max(int(top_n), 1)
        self.started_monotonic = time.monotonic()
        self._lock = threading.RLock()
        self._phases: dict[str, float] = {}
        self._engines: dict[str, dict] = {}
        self._slow_symbols: list[dict] = []
        self._slow_symbols_by_engine: dict[str, list[dict]] = {}
        self._ticker = {
            "ws_price_used": 0,
            "rest_fallback": 0,
            "rest_fallback_duration_sec": 0.0,
            "rest_fallback_failures": 0,
            "rest_lock_wait_sec": 0.0,
            "slow_fallback_samples": [],
        }
        self._rest = {
            "request_count": 0,
            "total_wall_time_sec": 0.0,
            "max_wall_time_sec": 0.0,
            "retry_count": 0,
            "physical_attempt_count": 0,
            "pool_timeout_count": 0,
            "timeout_count": 0,
            "error_count": 0,
            "lock_wait_sec": 0.0,
            "by_request_class": {
                key: _new_rest_aggregate() for key in REST_REQUEST_CLASSES
            },
            "by_endpoint": {},
        }
        self._critical_read = _new_rest_aggregate()
        self._signal_writes = {
            "write_count": 0,
            "cumulative_write_duration_sec": 0.0,
            "maximum_write_duration_sec": 0.0,
            "failure_count": 0,
        }
        self._execution = {
            "handoff_count": 0,
            "total_duration_sec": 0.0,
            "max_duration_sec": 0.0,
        }
        self._client_recovery = {
            "recovery_count": 0,
            "current_generation": 0,
        }

    def record_phase(self, name: str, duration_sec: float) -> None:
        with self._lock:
            self._phases[str(name)] = round(max(float(duration_sec), 0.0), 6)

    def record_symbol(self, engine: str, symbol: str, duration_sec: float) -> None:
        duration = max(float(duration_sec), 0.0)
        with self._lock:
            engine_key = _bounded_key(self._slow_symbols_by_engine, engine, MAX_ENGINE_KEYS)
            row = {
                "engine": engine_key,
                "symbol": str(symbol or "").upper(),
                "duration_sec": round(duration, 6),
            }
            _top(self._slow_symbols, row, self.top_n)
            by_engine = self._slow_symbols_by_engine.setdefault(engine_key, [])
            _top(by_engine, row, self.top_n)

    def record_batch(
        self,
        engine: str,
        batch_number: int,
        symbol_count: int,
        duration_sec: float,
        slowest_symbol: str = "",
        slowest_duration_sec: float = 0.0,
    ) -> None:
        duration = max(float(duration_sec), 0.0)
        row = {
            "batch_number": int(batch_number),
            "symbol_count": int(symbol_count),
            "duration_sec": round(duration, 6),
            "slowest_symbol": str(slowest_symbol or "").upper(),
            "slowest_symbol_duration_sec": round(max(float(slowest_duration_sec), 0.0), 6),
        }
        with self._lock:
            engine_key = _bounded_key(self._engines, engine, MAX_ENGINE_KEYS)
            row["engine"] = engine_key
            state = self._engines.setdefault(
                engine_key,
                {
                    "engine": engine_key,
                    "batch_count": 0,
                    "total_symbols_evaluated": 0,
                    "total_evaluation_duration_sec": 0.0,
                    "timeout": False,
                    "error": "",
                    "slowest_batches": [],
                },
            )
            state["batch_count"] += 1
            state["total_symbols_evaluated"] += int(symbol_count)
            state["total_evaluation_duration_sec"] += duration
            _top(state["slowest_batches"], row, min(self.top_n, 5))

    def record_engine_summary(
        self,
        engine: str,
        symbols_evaluated: int,
        duration_sec: float,
        *,
        error: str = "",
    ) -> None:
        with self._lock:
            engine_key = _bounded_key(self._engines, engine, MAX_ENGINE_KEYS)
            state = self._engines.setdefault(
                engine_key,
                {
                    "engine": engine_key,
                    "batch_count": 0,
                    "total_symbols_evaluated": 0,
                    "total_evaluation_duration_sec": 0.0,
                    "timeout": False,
                    "error": "",
                    "slowest_batches": [],
                },
            )
            state["total_symbols_evaluated"] = max(
                int(state.get("total_symbols_evaluated", 0) or 0), int(symbols_evaluated)
            )
            state["total_evaluation_duration_sec"] = round(max(float(duration_sec), 0.0), 6)
            if error:
                state["error"] = str(error)[:240]

    def record_engine_timeout(self, engine: str, duration_sec: float) -> None:
        with self._lock:
            engine_key = _bounded_key(self._engines, engine, MAX_ENGINE_KEYS)
            state = self._engines.setdefault(
                engine_key,
                {
                    "engine": engine_key,
                    "batch_count": 0,
                    "total_symbols_evaluated": 0,
                    "total_evaluation_duration_sec": 0.0,
                    "timeout": False,
                    "error": "",
                    "slowest_batches": [],
                },
            )
            state["timeout"] = True
            state["total_evaluation_duration_sec"] = round(max(float(duration_sec), 0.0), 6)

    def record_ticker(
        self,
        engine: str,
        symbol: str,
        source: str,
        duration_sec: float,
        *,
        success: bool = True,
        lock_wait_sec: float = 0.0,
    ) -> None:
        source_key = str(source or "").lower()
        duration = max(float(duration_sec), 0.0)
        with self._lock:
            if source_key == "ws":
                self._ticker["ws_price_used"] += 1
                return
            self._ticker["rest_fallback"] += 1
            self._ticker["rest_fallback_duration_sec"] += duration
            self._ticker["rest_lock_wait_sec"] += max(float(lock_wait_sec), 0.0)
            if not success:
                self._ticker["rest_fallback_failures"] += 1
            _top(
                self._ticker["slow_fallback_samples"],
                {
                    "engine": str(engine or "UNKNOWN"),
                    "symbol": str(symbol or "").upper(),
                    "duration_sec": round(duration, 6),
                    "success": bool(success),
                },
                self.top_n,
            )

    def record_rest(
        self,
        endpoint_class: str,
        duration_sec: float,
        *,
        retries: int = 0,
        physical_attempts: int | None = None,
        outcome: str = "ok",
        lock_wait_sec: float = 0.0,
        pool_timeouts: int = 0,
        request_class: str = "other",
        critical_read: bool = False,
        priority: str = "",
        transport: str = "",
    ) -> None:
        endpoint = _safe_endpoint_key(endpoint_class)
        request_class_key = _normalize_request_class(request_class)
        duration = max(float(duration_sec), 0.0)
        outcome_key = str(outcome or "ok").lower()
        with self._lock:
            _record_rest_aggregate(
                self._rest,
                duration,
                retries,
                physical_attempts,
                pool_timeouts,
                outcome_key,
                lock_wait_sec,
            )
            _record_rest_aggregate(
                self._rest["by_request_class"][request_class_key],
                duration,
                retries,
                physical_attempts,
                pool_timeouts,
                outcome_key,
                lock_wait_sec,
            )
            if critical_read:
                _record_rest_aggregate(
                    self._critical_read,
                    duration,
                    retries,
                    physical_attempts,
                    pool_timeouts,
                    outcome_key,
                    lock_wait_sec,
                )
            endpoint_key = _bounded_key(self._rest["by_endpoint"], endpoint, MAX_ENDPOINT_KEYS)
            row = self._rest["by_endpoint"].setdefault(
                endpoint_key,
                {
                    "request_count": 0, "total_wall_time_sec": 0.0,
                    "max_wall_time_sec": 0.0, "retry_count": 0,
                    "physical_attempt_count": 0,
                    "timeout_count": 0, "error_count": 0,
                    "transport_classes": {}, "priority_classes": {},
                },
            )
            row["request_count"] += 1
            row["total_wall_time_sec"] += duration
            row["max_wall_time_sec"] = max(row["max_wall_time_sec"], duration)
            row["retry_count"] += max(int(retries), 0)
            row["physical_attempt_count"] += max(
                int(physical_attempts if physical_attempts is not None else 1 + max(int(retries), 0)), 0
            )
            row["pool_timeout_count"] = int(row.get("pool_timeout_count", 0) or 0) + max(int(pool_timeouts), 0)
            transport_key = str(transport or "unknown")[:48]
            priority_key = str(priority or "unknown")[:32]
            transports = row.setdefault("transport_classes", {})
            priorities = row.setdefault("priority_classes", {})
            transports[transport_key] = int(transports.get(transport_key, 0) or 0) + 1
            priorities[priority_key] = int(priorities.get(priority_key, 0) or 0) + 1
            if outcome_key == "timeout":
                row["timeout_count"] += 1
            elif outcome_key != "ok":
                row["error_count"] += 1

    def record_rest_lock_wait(self, duration_sec: float, request_class: str = "other") -> None:
        request_class_key = _normalize_request_class(request_class)
        with self._lock:
            duration = max(float(duration_sec), 0.0)
            self._rest["lock_wait_sec"] += duration
            row = self._rest["by_request_class"][request_class_key]
            row["lock_wait_sec"] = float(row.get("lock_wait_sec", 0.0) or 0.0) + duration

    def record_signal_write(self, duration_sec: float, *, success: bool) -> None:
        duration = max(float(duration_sec), 0.0)
        with self._lock:
            self._signal_writes["write_count"] += 1
            self._signal_writes["cumulative_write_duration_sec"] += duration
            self._signal_writes["maximum_write_duration_sec"] = max(
                self._signal_writes["maximum_write_duration_sec"], duration
            )
            if not success:
                self._signal_writes["failure_count"] += 1

    def record_execution_handoff(self, duration_sec: float) -> None:
        duration = max(float(duration_sec), 0.0)
        with self._lock:
            self._execution["handoff_count"] += 1
            self._execution["total_duration_sec"] += duration
            self._execution["max_duration_sec"] = max(self._execution["max_duration_sec"], duration)

    def record_client_recovery(self, generation_id: int, recovery_count: int) -> None:
        with self._lock:
            self._client_recovery["current_generation"] = max(int(generation_id or 0), 0)
            self._client_recovery["recovery_count"] = max(int(recovery_count or 0), 0)

    def snapshot(
        self,
        *,
        status: str,
        duration_sec: float | None = None,
        phase_durations_sec: dict | None = None,
    ) -> dict:
        with self._lock:
            phases = dict(phase_durations_sec or self._phases)
            total = max(
                float(duration_sec) if duration_sec is not None else time.monotonic() - self.started_monotonic,
                0.0,
            )
            measured_keys = {
                "universe", "htf_refresh", "market_refresh_1m", "regime", "readiness",
                "research_begin", "ws_update", "position_sync", "engine_guardrails",
                "balance_refresh", "pre_engine_misc", "engine_evaluation", "research_completion",
            }
            measured = sum(float(phases.get(key, 0.0) or 0.0) for key in measured_keys)
            additive_phases = {
                key: round(max(float(value or 0.0), 0.0), 6)
                for key, value in phases.items()
                if key != "market_refresh"
            }
            aggregate_phases = {}
            if "market_refresh" in phases:
                aggregate_phases["market_refresh"] = round(
                    max(float(phases["market_refresh"] or 0.0), 0.0), 6
                )
            rest = dict(self._rest)
            rest["by_request_class"] = {
                key: dict(value) for key, value in self._rest["by_request_class"].items()
            }
            # Dedicated read-only view; normalized critical also includes
            # mutation requests for compatibility with existing reports.
            rest["critical_read"] = dict(self._critical_read)
            rest["by_endpoint"] = {key: dict(value) for key, value in rest["by_endpoint"].items()}
            engines = {}
            for key, value in self._engines.items():
                row = dict(value)
                row["total_evaluation_duration_sec"] = round(float(row.get("total_evaluation_duration_sec", 0.0) or 0.0), 6)
                row["slowest_batches"] = list(row.get("slowest_batches", []))
                engines[key] = row
            return {
                "schema_version": SCHEMA_VERSION,
                "scan_status": str(status),
                "cycle_number": self.cycle_number,
                "total_duration_sec": round(total, 6),
                "additive_phase_durations_sec": additive_phases,
                "aggregate_phase_durations_sec": aggregate_phases,
                "phase_timing_metadata": {
                    "additive_fields": sorted(additive_phases),
                    "aggregate_fields": sorted(aggregate_phases),
                    "overlaps": {"market_refresh": ["htf_refresh", "market_refresh_1m"]},
                },
                "other_unattributed_duration_sec": round(max(total - measured, 0.0), 6),
                "engines": engines,
                "slow_symbols_overall": list(self._slow_symbols),
                "slow_symbols_by_engine": {key: list(value) for key, value in self._slow_symbols_by_engine.items()},
                "ticker": dict(self._ticker),
                "shared_resources": shared_resource_snapshot(),
                "market_request_audit": market_request_snapshot(),
                "signal_age": signal_age_snapshot(),
                "engine_dependencies": engine_dependency_snapshot(),
                "rest": rest,
                "signal_funnel_writes": dict(self._signal_writes),
                "execution_handoff": dict(self._execution),
                "client_recovery": dict(self._client_recovery),
            }


def record_ticker(*args, **kwargs) -> None:
    try:
        collector = current()
        if collector is not None:
            collector.record_ticker(*args, **kwargs)
    except Exception:
        return None


def record_rest(*args, **kwargs) -> None:
    try:
        collector = current()
        if collector is not None:
            collector.record_rest(*args, **kwargs)
    except Exception:
        return None


def record_rest_lock_wait(duration_sec: float, request_class: str = "other") -> None:
    try:
        collector = current()
        if collector is not None:
            collector.record_rest_lock_wait(duration_sec, request_class=request_class)
    except Exception:
        return None


def record_signal_write(*args, **kwargs) -> None:
    try:
        collector = current()
        if collector is not None:
            collector.record_signal_write(*args, **kwargs)
    except Exception:
        return None


def record_execution_handoff(*args, **kwargs) -> None:
    try:
        collector = current()
        if collector is not None:
            collector.record_execution_handoff(*args, **kwargs)
    except Exception:
        return None


def record_client_recovery(*args, **kwargs) -> None:
    try:
        collector = current()
        if collector is not None:
            collector.record_client_recovery(*args, **kwargs)
    except Exception:
        return None


def record_phase(name: str, duration_sec: float) -> None:
    try:
        collector = current()
        if collector is not None:
            collector.record_phase(name, duration_sec)
    except Exception:
        return None


def record_symbol(*args, **kwargs) -> None:
    try:
        collector = current()
        if collector is not None:
            collector.record_symbol(*args, **kwargs)
    except Exception:
        return None


def record_batch(*args, **kwargs) -> None:
    try:
        collector = current()
        if collector is not None:
            collector.record_batch(*args, **kwargs)
    except Exception:
        return None


def record_engine_summary(*args, **kwargs) -> None:
    try:
        collector = current()
        if collector is not None:
            collector.record_engine_summary(*args, **kwargs)
    except Exception:
        return None


def record_engine_timeout(*args, **kwargs) -> None:
    try:
        collector = current()
        if collector is not None:
            collector.record_engine_timeout(*args, **kwargs)
    except Exception:
        return None

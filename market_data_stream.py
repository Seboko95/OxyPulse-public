"""
OxyPulse — market_data_stream.py
Multi-timeframe candle fetching. 1m for execution, 4H for HTF trend filter,
5m for regime detection. Parallel fetching via asyncio.gather.

Fixed v2:
  + fetch_candles_5m()     — 5m candles for regime detector & auto_finder
  + refresh_funding_all()  — batch funding rate fetch, populates funding cache
  + get_cached_funding_rate() / set_cached_funding_rate() — funding cache API
"""
from __future__ import annotations
import asyncio
from collections import Counter, deque
from contextvars import ContextVar
from dataclasses import dataclass, asdict
import logging
import math
import time
import httpx
import market_mode as mm
import latency_telemetry as _latency
import okx_client as _okx_transport
from rest_rate_governor import process_rest_rate_governor, run_with_rate_admission

logger   = logging.getLogger("OxyPulse.MarketData")
BASE_URL = "https://www.okx.com"
LIMIT    = 200   # candles per fetch for 1m/5m

_cache_1m:      dict[str, dict]  = {}   # coin → {opens, highs, lows, closes, volumes}
_cache_4h:      dict[str, dict]  = {}   # coin → same shape, 4H bars
_cache_5m:      dict[str, dict]  = {}   # coin → same shape, 5m bars (regime)
_cache_15m:     dict[str, dict]  = {}   # coin → 15m bars (AMATERASU MTF engine)
_cache_1h:      dict[str, dict]  = {}   # coin → 1H bars (AMATERASU MTF engine)
_cache_funding: dict[str, float] = {}   # coin → latest funding rate (float)
_cache_funding_ts: dict[str, float] = {}   # coin → monotonic ts of funding refresh
_cache_1d:      dict[str, dict]  = {}   # coin → daily bars (200 days) for Elliott Wave

# Candles share the OKX client's bounded background public transport. The
# client carries no credentials for these GETs; critical/private/mutation
# requests use separate clients inside okx_client.py.
_http: httpx.AsyncClient = _okx_transport._http_client
_http_gate = asyncio.Semaphore(30)
_rate_lock = asyncio.Lock()
_request_times: deque[float] = deque()
_fetch_health: Counter = Counter()
_last_candle_failure_reason: ContextVar[str] = ContextVar("last_candle_failure_reason", default="")
_candle_inflight: dict[tuple[int, str, str, str, int, str], asyncio.Task] = {}
_snapshot_generation: dict[tuple[str, str], int] = {}
_bootstrap_inflight: dict[tuple[str, str], asyncio.Task] = {}
_active_provenance: ContextVar[dict | None] = ContextVar("oxypulse_candle_provenance", default=None)
_REQUEST_REASONS = {"COLD_BOOTSTRAP", "PROVEN_GAP_REPAIR", "EXPLICIT_FALLBACK", "P0", "AUXILIARY", "OTHER"}
_GAP_TRIGGER_TYPES = {
    "WS_TIMESTAMP_GAP",
    "CONFIRMATION_GRACE_EXPIRED",
    "RECONNECT_CONTINUITY_GAP",
    "BOOTSTRAP_CONTINUITY_GAP",
}
_request_reason_metrics: dict[str, dict[str, Counter]] = {}
_request_provenance_samples: deque[dict] = deque(maxlen=256)
_TIMEFRAME_SECONDS = {
    "1m": 60,
    "5m": 300,
    "15m": 900,
    "1h": 3600,
    "4h": 14400,
    "1d": 86400,
}
_BAR_BY_TIMEFRAME = {
    "1m": "1m", "5m": "5m", "15m": "15m", "1h": "1H", "4h": "4H", "1d": "1D",
}
REQUIRED_CONSUMER_DEPTH = {
    "1m": 30, "5m": 50, "15m": 60, "1h": 60, "4h": 60, "1d": 120,
}


@dataclass(frozen=True)
class CandleRequestProvenance:
    request_reason: str
    trigger_type: str
    trigger_id: str
    instrument: str
    timeframe: str
    missing_start: float | None = None
    missing_end: float | None = None
    market_state_version: int = 0
    ws_generation: int = 0
    scan_generation: str | int = "unknown"
    scheduling_module: str = "market_data_stream"
    scheduling_function: str = "_fetch_and_store"
    physical_caller: str = "market_data_stream._fetch_once"
    attempt: int = 0

    def as_dict(self) -> dict:
        return asdict(self)


def _provenance_for(
    coin: str,
    timeframe: str,
    *,
    request_reason: str = "EXPLICIT_FALLBACK",
    trigger_type: str = "EXPLICIT_CALL",
    trigger_id: str = "",
    missing_start: float | None = None,
    missing_end: float | None = None,
    market_state_version: int = 0,
    ws_generation: int = 0,
    scan_generation: str | int = "unknown",
    scheduling_module: str = "market_data_stream",
    scheduling_function: str = "_fetch_and_store",
    physical_caller: str = "market_data_stream._fetch_once",
    attempt: int = 0,
) -> dict:
    reason = str(request_reason or "OTHER").upper()
    if reason not in _REQUEST_REASONS:
        reason = "OTHER"
    return CandleRequestProvenance(
        request_reason=reason,
        trigger_type=str(trigger_type or "EXPLICIT_CALL"),
        trigger_id=str(trigger_id or ""),
        instrument=str(coin or "").strip().upper(),
        timeframe=str(timeframe or "").strip().lower(),
        missing_start=missing_start,
        missing_end=missing_end,
        market_state_version=int(market_state_version or 0),
        ws_generation=int(ws_generation or 0),
        scan_generation=scan_generation,
        scheduling_module=str(scheduling_module or "market_data_stream"),
        scheduling_function=str(scheduling_function or "_fetch_and_store"),
        physical_caller=str(physical_caller or "market_data_stream._fetch_once"),
        attempt=int(attempt or 0),
    ).as_dict()


def _record_request_reason(provenance: dict, *, outcome: str = "physical") -> None:
    timeframe = str(provenance.get("timeframe", "unknown") or "unknown").lower()
    reason = str(provenance.get("request_reason", "OTHER") or "OTHER").upper()
    if reason not in _REQUEST_REASONS:
        reason = "OTHER"
    row = _request_reason_metrics.setdefault(timeframe, {})
    counters = row.setdefault(reason, Counter())
    counters[outcome] += 1
    if outcome == "physical":
        _request_provenance_samples.append(dict(provenance))


def request_reason_snapshot() -> dict:
    return {
        "by_timeframe": {
            timeframe: {reason: dict(counters) for reason, counters in reasons.items()}
            for timeframe, reasons in _request_reason_metrics.items()
        },
        "physical_samples": list(_request_provenance_samples)[-64:],
    }


def reset_request_reason_metrics() -> None:
    _request_reason_metrics.clear()
    _request_provenance_samples.clear()


def normalize_candle_request(
    coin: str,
    timeframe: str,
    bar: str,
    limit: int,
    *,
    data_type: str = "candles",
) -> dict:
    """Return the canonical identity used for cache and single-flight lookup."""
    normalized_tf = str(timeframe or "").strip().lower()
    canonical_bar = _BAR_BY_TIMEFRAME.get(normalized_tf, str(bar or "").strip())
    return {
        "instrument": str(coin or "").strip().upper(),
        "timeframe": normalized_tf,
        "bar": canonical_bar,
        "limit": max(int(limit or 0), 1),
        "data_type": str(data_type or "candles").strip().lower(),
    }


def _request_key(request: dict) -> str:
    return "|".join(
        str(request.get(field, ""))
        for field in ("instrument", "timeframe", "bar", "data_type", "limit")
    )


def _expected_completed_start(timeframe: str, now: float | None = None) -> float:
    bar_sec = int(_TIMEFRAME_SECONDS.get(str(timeframe or "").lower(), 60))
    current = int((time.time() if now is None else now) // bar_sec) * bar_sec
    return float(current - bar_sec)


def _completed_indices(row: dict) -> list[int]:
    timestamps = list(row.get("timestamps", []) or [])
    confirmed = list(row.get("confirmed", []) or [])
    return [
        index for index in range(len(timestamps))
        if index >= len(confirmed) or bool(confirmed[index])
    ]


def canonical_candle_points(row: dict) -> list[tuple[float, bool]]:
    """Return timestamp-idempotent candle state in opening-time order."""
    timestamps = list(row.get("timestamps", []) or [])
    confirmed = list(row.get("confirmed", []) or [])
    points: dict[float, bool] = {}
    for index, value in enumerate(timestamps):
        try:
            timestamp = float(value)
        except (TypeError, ValueError):
            continue
        done = index >= len(confirmed) or bool(confirmed[index])
        # Confirmation is monotonic when duplicate or overlapping sources meet.
        points[timestamp] = bool(points.get(timestamp, False) or done)
    return sorted(points.items())


def required_tail_analysis(
    row: dict,
    timeframe: str,
    required_count: int | None = None,
    now: float | None = None,
) -> dict:
    """Describe only the newest confirmed tail consumed by the strategy."""
    tf = str(timeframe or "").lower()
    points = canonical_candle_points(row)
    closed = [(timestamp, done) for timestamp, done in points if done]
    needed = max(int(required_count if required_count is not None else REQUIRED_CONSUMER_DEPTH.get(tf, 1)), 1)
    step = float(_TIMEFRAME_SECONDS.get(tf, 60))
    latest_point = points[-1] if points else (0.0, False)
    forming_present = bool(points and not latest_point[1])
    expected_boundary = (
        float(latest_point[0]) - step if forming_present
        else _expected_completed_start(tf, now)
    )
    latest_confirmed = float(closed[-1][0]) if closed else 0.0
    boundary_satisfied = bool(latest_confirmed and latest_confirmed >= expected_boundary - 0.5)
    tail_end = latest_confirmed if boundary_satisfied else expected_boundary
    tail_start = float(tail_end - (needed - 1) * step)
    confirmed_by_timestamp = {float(timestamp): bool(done) for timestamp, done in closed}
    required_timestamps = [float(tail_start + index * step) for index in range(needed)]
    missing_required = [
        timestamp for timestamp in required_timestamps
        if not any(abs(candidate - timestamp) < 0.5 for candidate in confirmed_by_timestamp)
    ]
    confirmed_tail_count = sum(
        1 for timestamp in required_timestamps
        if any(abs(candidate - timestamp) < 0.5 for candidate in confirmed_by_timestamp)
    )
    older_gap_count = 0
    for (previous, _), (current, _) in zip(closed, closed[1:]):
        if current - previous > step + 0.5 and current - step < tail_start - 0.5:
            older_gap_count += max(int(round((current - previous) / step)) - 1, 1)
    return {
        "required_count": needed,
        "step": step,
        "latest_required_confirmed_open": latest_confirmed,
        "expected_latest_confirmed_boundary": expected_boundary,
        "required_tail_start": tail_start,
        "required_tail_end": float(tail_end),
        "confirmed_tail_count": confirmed_tail_count,
        "missing_required_timestamps": missing_required,
        "older_noncritical_gap_count": older_gap_count,
        "boundary_satisfied": boundary_satisfied,
        "forming_present": forming_present,
        "complete": bool(boundary_satisfied and not missing_required),
    }


def confirmed_history_complete(row: dict, timeframe: str, required_count: int = 1, now: float | None = None) -> bool:
    """Return whether the newest required confirmed tail is complete."""
    return bool(required_tail_analysis(row, timeframe, required_count, now=now)["complete"])


def candle_range_status(
    row: dict,
    timeframe: str,
    start: float,
    end: float,
) -> dict:
    """Check an inclusive requested range without judging unrelated history."""
    step = float(_TIMEFRAME_SECONDS.get(str(timeframe or "").lower(), 60))
    points = dict(canonical_candle_points(row))
    try:
        first = float(start)
        last = float(end)
    except (TypeError, ValueError):
        return {"satisfied": False, "missing": [], "unconfirmed": []}
    if not math.isfinite(first) or not math.isfinite(last) or first > last:
        return {"satisfied": False, "missing": [], "unconfirmed": []}
    count = max(int(round((last - first) / step)) + 1, 1)
    targets = [first + index * step for index in range(count)]
    missing: list[float] = []
    unconfirmed: list[float] = []
    for target in targets:
        match = next(((timestamp, done) for timestamp, done in points.items() if abs(timestamp - target) < 0.5), None)
        if match is None:
            missing.append(target)
        elif not match[1]:
            unconfirmed.append(target)
    return {"satisfied": not missing and not unconfirmed, "missing": missing, "unconfirmed": unconfirmed}


def _row_satisfies_request(row: dict, request: dict, required_count: int | None = None) -> bool:
    if not row or str(row.get("timeframe", "")).lower() != request["timeframe"]:
        return False
    if bool(row.get("stale", False)) or str(row.get("refresh_status", "")).lower() in {
        "disconnected", "gap_detected", "failed",
    }:
        return False
    if str(row.get("bar", "")) != request["bar"] or str(row.get("data_type", "candles")) != request["data_type"]:
        return False
    completed = _completed_indices(row)
    needed = max(int(required_count if required_count is not None else request["limit"]), 1)
    timestamps = list(row.get("timestamps", []) or [])
    # OKX may return the current first candle with confirm=0.  It is still a
    # valid retained row; callers requiring closed history use
    # confirmed_history_complete separately.  Counting rows here prevents a
    # compatible bootstrap snapshot from being polled again solely because
    # its newest row is forming.
    if len(timestamps) < needed:
        return False
    if not completed or float(timestamps[completed[-1]]) < _expected_completed_start(request["timeframe"]):
        return False
    return True


def _find_compatible_flight(loop_id: int, request: dict) -> tuple[tuple, asyncio.Task] | None:
    candidates = []
    for key, task in list(_candle_inflight.items()):
        if task.done():
            continue
        if key[:2] != (loop_id, request["instrument"]):
            continue
        if key[2] != request["timeframe"] or key[3] != request["bar"] or key[5] != request["data_type"]:
            continue
        if int(key[4]) >= int(request["limit"]):
            candidates.append((int(key[4]), key, task))
    if not candidates:
        return None
    _limit, key, task = min(candidates, key=lambda item: item[0])
    return key, task


async def _throttle_candle_request() -> None:
    """Stay below OKX's documented 40 candle requests per two seconds."""
    while True:
        async with _rate_lock:
            now = time.monotonic()
            while _request_times and now - _request_times[0] >= 2.0:
                _request_times.popleft()
            if len(_request_times) < 36:
                _request_times.append(now)
                return
            wait = max(2.0 - (now - _request_times[0]), 0.01)
        await asyncio.sleep(wait)

# ── Internal fetch helper ─────────────────────────────────────────────────────

async def _fetch_once(coin: str, bar: str, limit: int, *, attempt: int = 0) -> list | None:
    inst = mm.inst_id(coin)
    inferred_timeframe = next(
        (timeframe for timeframe, candidate in _BAR_BY_TIMEFRAME.items()
         if str(candidate).lower() == str(bar or "").lower()),
        "",
    )
    provenance = dict(_active_provenance.get() or _provenance_for(coin, inferred_timeframe, attempt=attempt))
    provenance["attempt"] = int(attempt)
    _last_candle_failure_reason.set("")
    _record_request_reason(provenance, outcome="physical")
    try:
        resp, rate_bucket = await run_with_rate_admission(
            "GET", "/api/v5/market/candles",
            params={"instId": inst, "bar": bar, "limit": str(limit)}, priority="P2",
            pool_gate=_http_gate,
            send=lambda: _http.get(
                f"{BASE_URL}/api/v5/market/candles",
                params={"instId": inst, "bar": bar, "limit": str(limit)},
            ),
        )
        if resp.status_code == 429:
            process_rest_rate_governor.record_http_429(rate_bucket)
        resp.raise_for_status()
        payload = resp.json()
        if str(payload.get("code", "")) == "50011":
            process_rest_rate_governor.record_business_50011(rate_bucket)
        data = payload.get("data", [])
        if not data:
            _last_candle_failure_reason.set("NO_ROWS_RETURNED")
            _record_request_reason(provenance, outcome="failure")
            return None
        data.reverse()   # oldest-first so indicators run left→right
        return data
    except Exception as e:
        error_name = type(e).__name__.lower()
        _last_candle_failure_reason.set("HTTP_TIMEOUT" if "timeout" in error_name else "HTTP_ERROR")
        _record_request_reason(provenance, outcome="failure")
        logger.debug("Candle fetch %s %s: %s", coin, bar, e)
        return None


def last_candle_failure_reason() -> str:
    return str(_last_candle_failure_reason.get() or "")


async def _fetch(coin: str, bar: str, limit: int) -> list | None:
    for attempt in range(3):
        await _throttle_candle_request()
        if attempt:
            provenance = dict(_active_provenance.get() or {})
            if provenance:
                _record_request_reason({**provenance, "attempt": attempt}, outcome="retry")
        data = await _fetch_once(coin, bar, limit, attempt=attempt)
        if data:
            _fetch_health["ok"] += 1
            return data
        _fetch_health["errors"] += 1
        if attempt < 2:
            await asyncio.sleep(0.25 * (2 ** attempt))
    logger.warning("Candle fetch failed after retries %s %s", coin, bar)
    return None


def fetch_health_snapshot() -> dict:
    return dict(_fetch_health)


def _parse_candles(data: list) -> dict:
    """Convert raw OKX candle list to typed dict."""
    received_at = time.time()
    timestamps = [int(c[0]) / 1000.0 for c in data]
    return {
        "timestamps": timestamps,
        "opens":   [float(c[1]) for c in data],
        "highs":   [float(c[2]) for c in data],
        "lows":    [float(c[3]) for c in data],
        "closes":  [float(c[4]) for c in data],
        "volumes": [float(c[5]) for c in data],
        "confirmed": [str(c[8]) == "1" if len(c) > 8 else True for c in data],
        "observed_at": received_at,
        "received_at": received_at,
        "last_update_received_at": received_at,
        "last_exchange_payload_ts": timestamps[-1] if timestamps else 0.0,
    }


def _cache_for_timeframe(timeframe: str) -> dict[str, dict]:
    return {
        "1m": _cache_1m, "5m": _cache_5m, "15m": _cache_15m,
        "1h": _cache_1h, "4h": _cache_4h, "1d": _cache_1d,
    }.get(str(timeframe or "").lower(), {})


def coins_requiring_refresh(coins: set[str], timeframe: str, required_count: int) -> set[str]:
    """Return only symbols missing the most recently completed candle."""
    tf = str(timeframe or "").lower()
    cache = _cache_for_timeframe(tf)
    needed: set[str] = set()
    for raw_coin in coins:
        coin = str(raw_coin or "").upper()
        row = cache.get(coin, {})
        request = normalize_candle_request(coin, tf, _BAR_BY_TIMEFRAME.get(tf, tf), required_count)
        # A socket-wide disconnect is not permission to redownload the whole
        # universe. Preserve the last observation as stale and let reconnect
        # continuity checks schedule per-symbol repairs.
        if (
            str(row.get("source", "")).endswith("websocket")
            and str(row.get("refresh_status", "")).lower() == "disconnected"
            and row.get("timestamps")
        ):
            continue
        # Preserve compatibility with test/paper adapters that seed the raw
        # candle arrays without the newer provenance metadata.  Such rows can
        # decide refresh eligibility, but are never used for superset reuse.
        if "timeframe" not in row:
            timestamps = list(row.get("timestamps", []) or [])
            confirmed = list(row.get("confirmed", []) or [])
            completed = [
                float(timestamps[index]) for index in range(len(timestamps))
                if index >= len(confirmed) or bool(confirmed[index])
            ]
            row_is_current = (
                len(completed) >= int(required_count)
                and bool(completed)
                and completed[-1] >= _expected_completed_start(tf)
            )
        else:
            row_is_current = _row_satisfies_request(row, request, required_count)
        if not row_is_current:
            needed.add(coin)
    return needed


def timeframe_snapshot(coin: str, timeframe: str, required_count: int = 1) -> dict:
    """Return JSON-safe cache readiness and freshness metadata."""
    tf = str(timeframe or "").lower()
    row = _cache_for_timeframe(tf).get(str(coin or "").upper(), {})
    timestamps = list(row.get("timestamps", []) or [])
    confirmed = list(row.get("confirmed", []) or [])
    completed_idx = _completed_indices(row)
    latest = timestamps[completed_idx[-1]] if completed_idx else 0.0
    oldest = timestamps[completed_idx[0]] if completed_idx else 0.0
    count = len(completed_idx)
    bar_sec = int(_TIMEFRAME_SECONDS.get(tf, 60))
    now = time.time()
    bar_age = max(now - latest, 0.0) if latest else None
    received_at = float(row.get("last_update_received_at") or row.get("received_at") or 0.0)
    transport_age = max(now - received_at, 0.0) if received_at else None
    forming_present = bool(confirmed and not confirmed[-1])
    tail = required_tail_analysis(row, tf, int(required_count), now=now)
    confirmed_complete = bool(tail["complete"])
    expected_latest = float(tail["expected_latest_confirmed_boundary"])
    bar_stale = latest <= 0 or not confirmed_complete or bool(bar_age is not None and bar_age > bar_sec * 2.5)
    transport_limit = max(90.0, min(float(bar_sec) * 0.25, 900.0))
    transport_stale = transport_age is None or transport_age > transport_limit
    stale = bool(row.get("stale", False)) or bar_stale or transport_stale
    reason = ""
    # Preserve the established diagnostic for a sufficiently populated but stale
    # cache; required-tail depth still controls readiness independently.
    if int(tail["confirmed_tail_count"]) < int(required_count) and count < int(required_count):
        reason = f"insufficient_{tf}_samples"
    elif bar_stale:
        reason = f"stale_{tf}_candle"
    elif transport_stale:
        reason = f"stale_{tf}_transport"
    return {
        "timeframe": tf, "required_count": int(required_count), "available_count": count,
        "oldest_timestamp": oldest, "latest_timestamp": latest, "age_sec": bar_age,
        "bar_age_sec": bar_age, "transport_age_sec": transport_age,
        "transport_fresh": not transport_stale, "transport_stale": transport_stale,
        "bar_stale": bar_stale, "bar_open_ts": latest,
        "last_update_received_at": received_at,
        "last_exchange_payload_ts": row.get("last_exchange_payload_ts", ""),
        "latest_confirmed_bar_open": row.get("latest_confirmed_bar_open", latest),
        "expected_latest_confirmed_boundary": expected_latest,
        "confirmation_received_at": row.get("confirmation_received_at", ""),
        "confirmed_history_complete": confirmed_complete,
        "required_tail_start": tail["required_tail_start"],
        "latest_required_confirmed_open": tail["latest_required_confirmed_open"],
        "confirmed_tail_count": tail["confirmed_tail_count"],
        "required_tail_missing": list(tail["missing_required_timestamps"]),
        "older_noncritical_gap_count": tail["older_noncritical_gap_count"],
        "forming_candle_present": forming_present,
        "observed_at": row.get("observed_at", ""),
        "exchange_observed_at": row.get("exchange_observed_at", ""),
        "version": int(row.get("version", 0) or 0),
        "request_limit": int(row.get("request_limit", 0) or 0),
        "request_key": row.get("request_key", ""),
        "source": row.get("source", "okx_public_candles"),
        "refresh_status": row.get("refresh_status", "unknown"),
        "refresh_error": row.get("refresh_error", ""),
        "last_refresh_attempt_at": row.get("last_refresh_attempt_at", ""),
        "received_at": row.get("received_at", ""),
        "connection_generation": int(row.get("connection_generation", 0) or 0),
        "ready": bool(row.get("ready", False)),
        "continuity_ready": bool(row.get("ready", False) and confirmed_complete and not row.get("stale", False)),
        "stale": stale,
        "indicator_ready": not reason, "fresh": not stale, "reason": reason,
    }


def _merge_candle_rows(existing: dict, incoming: dict) -> dict:
    """Merge REST history without allowing it to overwrite newer WS bars."""
    if not existing:
        return incoming
    keys = ("opens", "highs", "lows", "closes", "volumes", "confirmed")
    merged: dict[float, tuple] = {}
    existing_ts = list(existing.get("timestamps", []) or [])
    existing_confirmed = list(existing.get("confirmed", []) or [])
    for index, timestamp in enumerate(existing_ts):
        if index >= len(existing.get("closes", []) or []):
            continue
        merged[float(timestamp)] = tuple(
            list((existing.get(key, []) or [])[index] for key in keys[:-1])
            + [bool(existing_confirmed[index]) if index < len(existing_confirmed) else True]
        )
    incoming_ts = list(incoming.get("timestamps", []) or [])
    for index, timestamp in enumerate(incoming_ts):
        if index >= len(incoming.get("closes", []) or []):
            continue
        value = tuple(
            list((incoming.get(key, []) or [])[index] for key in keys[:-1])
            + [bool((incoming.get("confirmed", []) or [True] * len(incoming_ts))[index])]
        )
        # WS is the live authority for a bar already observed on the socket.
        # REST may fill missing history, but a slower response cannot roll it
        # back to an older forming/confirmed value.
        timestamp = float(timestamp)
        if timestamp not in merged:
            merged[timestamp] = value
        else:
            # Keep the live WS OHLCV values authoritative, but confirmation is
            # monotonic across sources.  REST may be the first source to
            # confirm a bar that WS previously published as forming.
            current = merged[timestamp]
            merged[timestamp] = (*current[:-1], bool(current[-1] or value[-1]))
    timestamps = sorted(merged)
    if len(timestamps) > max(len(existing_ts), len(incoming_ts), LIMIT):
        timestamps = timestamps[-max(len(existing_ts), len(incoming_ts), LIMIT):]
    result = {
        "timestamps": timestamps,
        **{key: [merged[timestamp][index] for timestamp in timestamps] for index, key in enumerate(keys[:-1])},
        "confirmed": [merged[timestamp][-1] for timestamp in timestamps],
    }
    # Preserve live provenance when the current snapshot contains a WS bar.
    result.update(incoming)
    result.update({
        "timestamps": timestamps,
        **{key: [merged[timestamp][index] for timestamp in timestamps] for index, key in enumerate(keys[:-1])},
        "confirmed": [merged[timestamp][-1] for timestamp in timestamps],
    })
    if str(existing.get("source", "")).endswith("websocket"):
        for field in (
            "observed_at", "received_at", "last_update_received_at", "last_exchange_payload_ts",
            "source", "connection_generation", "stale", "latest_confirmed_bar_open",
            "confirmation_received_at",
        ):
            if field in existing:
                result[field] = existing[field]
        result["refresh_status"] = "fresh" if not existing.get("stale") else existing.get("refresh_status", "stale")
        result["refresh_error"] = existing.get("refresh_error", "")
    return result


def execution_price_snapshot(coin: str, max_age_sec: float = 90.0) -> dict:
    """Return freshness for the latest 1m price, including the forming candle."""
    row = _cache_1m.get(str(coin or "").upper(), {})
    timestamps = list(row.get("timestamps", []) or [])
    closes = list(row.get("closes", []) or [])
    latest_timestamp = float(timestamps[-1]) if timestamps else 0.0
    price = float(closes[-1]) if closes else 0.0
    age = max(time.time() - latest_timestamp, 0.0) if latest_timestamp else None
    received_at = float(row.get("last_update_received_at") or row.get("received_at") or 0.0)
    transport_age = max(time.time() - received_at, 0.0) if received_at else None
    max_age = max(float(max_age_sec or 0.0), 1.0)
    fresh = price > 0.0 and latest_timestamp > 0.0 and age is not None and age <= max_age
    return {
        "price": price,
        "latest_timestamp": latest_timestamp,
        "last_update_received_at": received_at,
        "age_sec": age,
        "bar_age_sec": age,
        "transport_age_sec": transport_age,
        "source": row.get("source", ""),
        "connection_generation": int(row.get("connection_generation", 0) or 0),
        "stale": bool(row.get("stale", False)),
        "max_age_sec": max_age,
        "fresh": fresh,
        "reason": "" if fresh else ("market_price_unavailable" if price <= 0.0 else "stale_execution_price"),
    }


def completed_5m_series(coin: str) -> dict:
    """Return only completed 5m bars so partial volume cannot drive regime state."""
    row = _cache_5m.get(str(coin or "").upper(), {})
    keys = ("timestamps", "opens", "highs", "lows", "closes", "volumes")
    if not row:
        return {key: [] for key in keys}
    confirmed = list(row.get("confirmed", []) or [])
    indices = [i for i in range(len(row.get("closes", []))) if i >= len(confirmed) or confirmed[i]]
    return {key: [row.get(key, [])[i] for i in indices if i < len(row.get(key, []))] for key in keys}


def market_readiness_snapshot(coins) -> dict:
    """Summarize the actual multi-timeframe cache available to a scan."""
    requirements = dict(REQUIRED_CONSUMER_DEPTH)
    rows = {}
    for coin in sorted({str(item or "").upper() for item in coins or [] if item}):
        rows[coin] = {tf: timeframe_snapshot(coin, tf, need) for tf, need in requirements.items()}
    return {
        "requirements": requirements,
        "symbols": rows,
        "rest_fetch_health": fetch_health_snapshot(),
        "candle_request_reasons": request_reason_snapshot(),
    }


# ── Public fetch functions ────────────────────────────────────────────────────

async def _fetch_and_store(
    coin: str,
    timeframe: str,
    bar: str,
    limit: int,
    *,
    mirror_1m: bool = False,
    caller: str = "market_data_stream",
    scan_generation: str | int = "unknown",
    request_reason: str = "EXPLICIT_FALLBACK",
    trigger_type: str = "EXPLICIT_CALL",
    trigger_id: str = "",
    missing_start: float | None = None,
    missing_end: float | None = None,
    market_state_version: int = 0,
    ws_generation: int = 0,
    scheduling_module: str = "market_data_stream",
    scheduling_function: str = "_fetch_and_store",
) -> bool:
    """Reuse completed compatible snapshots, then single-flight new fetches."""
    request = normalize_candle_request(coin, timeframe, bar, limit)
    normalized_coin = request["instrument"]
    normalized_tf = request["timeframe"]
    cache = _cache_for_timeframe(normalized_tf)
    cache_row = cache.get(normalized_coin, {})
    provenance = _provenance_for(
        normalized_coin,
        normalized_tf,
        request_reason=request_reason,
        trigger_type=trigger_type,
        trigger_id=trigger_id,
        missing_start=missing_start,
        missing_end=missing_end,
        market_state_version=market_state_version,
        ws_generation=ws_generation,
        scan_generation=scan_generation,
        scheduling_module=scheduling_module,
        scheduling_function=scheduling_function,
        physical_caller="market_data_stream._fetch_once",
    )
    domain = f"htf:{normalized_tf}"
    _latency.record_shared_resource(domain, logical=1)

    # Completed snapshots are checked before creating a task.  This handles
    # sequential engine callers, which an in-flight table cannot coalesce.
    if _row_satisfies_request(cache_row, request):
        _latency.record_shared_resource(domain, cache_hits=1)
        _latency.record_market_request(
            normalized_key=_request_key(request), instrument=normalized_coin,
            timeframe=normalized_tf, bar=request["bar"], limit=request["limit"],
            caller=caller, scan_generation=scan_generation, cache_hit=True,
            sequential_duplicate=True, success=True,
            request_reason=provenance["request_reason"], trigger_type=provenance["trigger_type"],
        )
        return True

    loop = asyncio.get_running_loop()
    loop_id = id(loop)
    compatible = _find_compatible_flight(loop_id, request)
    concurrent_duplicate = compatible is not None
    task = compatible[1] if compatible else None
    key = compatible[0] if compatible else (
        loop_id, normalized_coin, normalized_tf, request["bar"], request["limit"], request["data_type"]
    )
    if task is None:
        async def _produce() -> bool:
            token = _active_provenance.set(provenance)
            try:
                data = await _fetch(normalized_coin, request["bar"], request["limit"])
            finally:
                _active_provenance.reset(token)
            if not data:
                row = cache.get(normalized_coin)
                if row:
                    row["refresh_status"] = "failed"
                    row["refresh_error"] = "candle_refresh_failed"
                    row["last_refresh_attempt_at"] = time.time()
                return False
            parsed = await asyncio.to_thread(_parse_candles, data)
            previous = cache.get(normalized_coin, {})
            if (
                int(previous.get("request_limit", 0) or 0) > request["limit"]
                and _row_satisfies_request(previous, request)
            ):
                # A larger compatible response won the race.  Do not publish
                # a smaller replacement or advance the generation for a
                # snapshot that did not change.
                return True
            generation_key = (normalized_coin, normalized_tf)
            generation = int(_snapshot_generation.get(generation_key, 0) or 0) + 1
            _snapshot_generation[generation_key] = generation
            parsed.update({
                "observed_at": time.time(),
                "received_at": time.time(),
                "last_update_received_at": time.time(),
                "last_exchange_payload_ts": parsed.get("timestamps", [0.0])[-1] if parsed.get("timestamps") else 0.0,
                "version": generation,
                "request_limit": request["limit"],
                "request_key": _request_key(request),
                "timeframe": normalized_tf,
                "bar": request["bar"],
                "data_type": request["data_type"],
                "source": "okx_public_candles",
                "refresh_status": "fresh",
                "refresh_error": "",
                "last_refresh_attempt_at": time.time(),
                "ready": True,
                **provenance,
            })
            parsed = _merge_candle_rows(previous, parsed)
            cache[normalized_coin] = parsed
            if mirror_1m and (normalized_coin not in _cache_1m or not _cache_1m[normalized_coin].get("closes")):
                _cache_1m[normalized_coin] = parsed
            return True

        # ensure_future keeps this helper compatible with paper-mode task
        # adapters that monkeypatch create_task on the shared asyncio module.
        task = asyncio.ensure_future(_produce())
        _candle_inflight[key] = task
        _latency.record_shared_resource(domain, underlying=1)
        _latency.record_market_request(
            normalized_key=_request_key(request), instrument=normalized_coin,
            timeframe=normalized_tf, bar=request["bar"], limit=request["limit"],
            caller=caller, scan_generation=scan_generation, underlying=True,
            request_reason=provenance["request_reason"], trigger_type=provenance["trigger_type"],
        )

        def _cleanup(done_task, *, _key=key):
            if _candle_inflight.get(_key) is done_task:
                _candle_inflight.pop(_key, None)
        if hasattr(task, "add_done_callback"):
            task.add_done_callback(_cleanup)
    else:
        _latency.record_shared_resource(domain, coalesced=1)
        _latency.record_market_request(
            normalized_key=_request_key(request), instrument=normalized_coin,
            timeframe=normalized_tf, bar=request["bar"], limit=request["limit"],
            caller=caller, scan_generation=scan_generation, coalesced=True,
            concurrent_duplicate=True, request_reason=provenance["request_reason"],
            trigger_type=provenance["trigger_type"],
        )
    try:
        result = bool(await asyncio.shield(task))
        if not concurrent_duplicate:
            _latency.record_market_request(
                normalized_key=_request_key(request), instrument=normalized_coin,
                timeframe=normalized_tf, bar=request["bar"], limit=request["limit"],
                caller=caller, scan_generation=scan_generation, logical=0, success=result,
                request_reason=provenance["request_reason"], trigger_type=provenance["trigger_type"],
            )
        return result
    except BaseException:
        _latency.record_shared_resource(domain, failures=1)
        _latency.record_market_request(
            normalized_key=_request_key(request), instrument=normalized_coin,
            timeframe=normalized_tf, bar=request["bar"], limit=request["limit"],
            caller=caller, scan_generation=scan_generation, logical=0, success=False,
            request_reason=provenance["request_reason"], trigger_type=provenance["trigger_type"],
        )
        raise


async def fetch_candles(coin: str) -> bool:
    """Fetch 1m candles and populate the 1m cache for *coin*."""
    return await _fetch_and_store(coin, "1m", "1m", LIMIT)


async def bootstrap_universe(
    coins: set[str] | list[str],
    *,
    requirements: dict[str, int] | None = None,
    scan_generation: str | int = "bootstrap",
    max_concurrency: int = 8,
) -> dict:
    """Own bounded cold history acquisition outside scanner/engine reads."""
    requirements = requirements or dict(REQUIRED_CONSUMER_DEPTH)
    normalized = sorted({str(coin or "").upper() for coin in coins or [] if coin})
    keys = [
        (coin, str(timeframe).lower(), int(limit))
        for timeframe, limit in requirements.items()
        for coin in normalized
    ]
    queue: asyncio.Queue = asyncio.Queue()
    for item in keys:
        queue.put_nowait(item)
    results = Counter()

    async def _one(item: tuple[str, str, int]) -> None:
        coin, timeframe, limit = item
        key = (coin, timeframe)
        existing = _bootstrap_inflight.get(key)
        if existing is None or existing.done():
            bar = _BAR_BY_TIMEFRAME[timeframe]
            task = asyncio.create_task(
                _fetch_and_store(
                    coin, timeframe, bar, max(limit, 1),
                    caller="market_data_bootstrap",
                    scan_generation=scan_generation,
                    request_reason="COLD_BOOTSTRAP",
                    trigger_type="BOOTSTRAP_CONTINUITY_GAP",
                    trigger_id=f"bootstrap:{coin}:{timeframe}",
                    scheduling_module=__name__,
                    scheduling_function="bootstrap_universe",
                ),
                name=f"market-bootstrap:{coin}:{timeframe}",
            )
            _bootstrap_inflight[key] = task
        else:
            task = existing
        try:
            results["ok" if await asyncio.shield(task) else "failed"] += 1
        except asyncio.CancelledError:
            raise
        except Exception:
            results["failed"] += 1
        finally:
            if _bootstrap_inflight.get(key) is task and task.done():
                _bootstrap_inflight.pop(key, None)

    async def _worker() -> None:
        while True:
            try:
                item = queue.get_nowait()
            except asyncio.QueueEmpty:
                return
            try:
                await _one(item)
            finally:
                queue.task_done()

    worker_count = min(max(int(max_concurrency), 1), max(len(keys), 1))
    await asyncio.gather(*(_worker() for _ in range(worker_count)))
    return {
        "requested": len(keys),
        "ok": int(results.get("ok", 0)),
        "failed": int(results.get("failed", 0)),
        "request_reason": "COLD_BOOTSTRAP",
    }


async def fetch_candles_4h(coin: str) -> bool:
    """Fetch 4H candles for HTF trend filter (200 EMA needs 200+ candles)."""
    return await _fetch_and_store(coin, "4h", "4H", 220)


async def fetch_candles_5m(coin: str) -> bool:
    """
    Fetch 5m candles for regime detection and auto_finder analysis.
    Populates the 5m cache AND copies into the 1m cache so that the
    standard get_closes/get_highs/… accessors return useful data when
    no 1m fetch has been done yet (e.g. regime_detector at cold start).
    """
    return await _fetch_and_store(coin, "5m", "5m", LIMIT, mirror_1m=True)


# ── Batch refresh helpers ─────────────────────────────────────────────────────

async def refresh_all(coins: set[str], concurrency: int = 20) -> None:
    """Fetch only 1m histories not already current in the shared WS cache."""
    coins = coins_requiring_refresh(coins, "1m", 30)
    sem = asyncio.Semaphore(concurrency)

    async def _one(coin: str) -> None:
        async with sem:
            await fetch_candles(coin)

    await asyncio.gather(*[_one(c) for c in coins], return_exceptions=True)


async def fetch_candles_15m(coin: str) -> bool:
    """Fetch 15m candles for AMATERASU multi-timeframe analysis."""
    return await _fetch_and_store(coin, "15m", "15m", 200)


async def fetch_candles_1h(coin: str) -> bool:
    """Fetch 1H candles for AMATERASU multi-timeframe analysis."""
    return await _fetch_and_store(coin, "1h", "1H", 100)


async def refresh_4h_all(coins: set[str], concurrency: int = 10) -> None:
    """Fetch 4H candles for HTF filter. Call less frequently (every 4h is enough)."""
    coins = coins_requiring_refresh(coins, "4h", 60)
    sem = asyncio.Semaphore(concurrency)

    async def _one(coin: str) -> None:
        async with sem:
            await fetch_candles_4h(coin)

    await asyncio.gather(*[_one(c) for c in coins], return_exceptions=True)


async def refresh_15m_all(coins: set[str], concurrency: int = 10) -> None:
    """Fetch 15m candles for AMATERASU MTF engine."""
    coins = coins_requiring_refresh(coins, "15m", 60)
    sem = asyncio.Semaphore(concurrency)

    async def _one(coin: str) -> None:
        async with sem:
            await fetch_candles_15m(coin)

    await asyncio.gather(*[_one(c) for c in coins], return_exceptions=True)


async def refresh_1h_all(coins: set[str], concurrency: int = 10) -> None:
    """Fetch 1H candles for AMATERASU MTF engine."""
    coins = coins_requiring_refresh(coins, "1h", 60)
    sem = asyncio.Semaphore(concurrency)

    async def _one(coin: str) -> None:
        async with sem:
            await fetch_candles_1h(coin)

    await asyncio.gather(*[_one(c) for c in coins], return_exceptions=True)


async def refresh_funding_all(
    coins: list[str],
    okx,                      # OKXClient instance — passed by TradingOrchestrator
    concurrency: int = 4,
) -> None:
    """
    Fetch funding rates for *coins* via the OKX REST API and store them in
    the local funding cache.  Called by TradingOrchestrator every cycle for
    funding-aware futures engines so that get_cached_funding_rate() is warm.

    Args:
        coins:       list of base coin symbols, e.g. ["BTC","ETH","SOL"]
        okx:         OKXClient instance that has get_funding_rate(coin) method
        concurrency: max simultaneous OKX requests (keep low to avoid 429)
    """
    sem = asyncio.Semaphore(concurrency)

    async def _one(coin: str) -> None:
        async with sem:
            try:
                rate = await okx.get_funding_rate(coin)
                _cache_funding[coin] = float(rate)
                _cache_funding_ts[coin] = time.monotonic()
                logger.debug("Funding %s → %.6f", coin, rate)
            except Exception as exc:
                logger.debug("Funding fetch %s failed: %s", coin, exc)

    await asyncio.gather(*[_one(c) for c in coins], return_exceptions=True)


async def refresh_funding_batch(
    coins: list[str],
    okx,
    *,
    max_symbols: int = 24,
    concurrency: int = 2,
    request_timeout_sec: float = 6.0,
) -> dict:
    """Refresh the stalest funding rows without occupying the scan critical path."""
    ordered = sorted(
        {str(coin or "").upper() for coin in coins if coin},
        key=lambda coin: (float(_cache_funding_ts.get(coin, 0.0) or 0.0), coin),
    )[:max(int(max_symbols), 1)]
    sem = asyncio.Semaphore(max(int(concurrency), 1))
    counts = Counter()

    async def _one(coin: str) -> None:
        async with sem:
            try:
                rate = await asyncio.wait_for(
                    okx.get_funding_rate(coin),
                    timeout=max(float(request_timeout_sec), 0.05),
                )
                _cache_funding[coin] = float(rate)
                _cache_funding_ts[coin] = time.monotonic()
                counts["ok"] += 1
            except asyncio.TimeoutError:
                counts["timeout"] += 1
            except Exception:
                counts["error"] += 1

    await asyncio.gather(*[_one(coin) for coin in ordered], return_exceptions=True)
    return {"requested": len(ordered), **dict(counts)}


# ── 1m accessors ──────────────────────────────────────────────────────────────
def get_opens(coin: str)    -> list[float]: return _cache_1m.get(coin, {}).get("opens",   [])
def get_closes(coin: str)   -> list[float]: return _cache_1m.get(coin, {}).get("closes",  [])
def get_highs(coin: str)    -> list[float]: return _cache_1m.get(coin, {}).get("highs",   [])
def get_lows(coin: str)     -> list[float]: return _cache_1m.get(coin, {}).get("lows",    [])
def get_volumes(coin: str)  -> list[float]: return _cache_1m.get(coin, {}).get("volumes", [])
def candle_count(coin: str) -> int:         return len(_cache_1m.get(coin, {}).get("closes", []))
def latest_price(coin: str) -> float | None:
    c = _cache_1m.get(coin, {}).get("closes", [])
    return c[-1] if c else None


# ── 4H accessors ──────────────────────────────────────────────────────────────
def get_4h_opens(coin: str)   -> list[float]: return _cache_4h.get(coin, {}).get("opens",   [])
def get_4h_closes(coin: str)  -> list[float]: return _cache_4h.get(coin, {}).get("closes",  [])
def get_4h_highs(coin: str)   -> list[float]: return _cache_4h.get(coin, {}).get("highs",   [])
def get_4h_lows(coin: str)    -> list[float]: return _cache_4h.get(coin, {}).get("lows",    [])
def get_4h_volumes(coin: str) -> list[float]: return _cache_4h.get(coin, {}).get("volumes", [])
def has_4h_data(coin: str)    -> bool:        return bool(_cache_4h.get(coin, {}).get("closes"))


# ── 5m accessors ──────────────────────────────────────────────────────────────
def get_5m_opens(coin: str)   -> list[float]: return _cache_5m.get(coin, {}).get("opens",   [])
def get_5m_closes(coin: str)  -> list[float]: return _cache_5m.get(coin, {}).get("closes",  [])
def get_5m_highs(coin: str)   -> list[float]: return _cache_5m.get(coin, {}).get("highs",   [])
def get_5m_lows(coin: str)    -> list[float]: return _cache_5m.get(coin, {}).get("lows",    [])
def get_5m_volumes(coin: str) -> list[float]: return _cache_5m.get(coin, {}).get("volumes", [])
def has_5m_data(coin: str)    -> bool:        return bool(_cache_5m.get(coin, {}).get("closes"))


# ── 15m accessors (AMATERASU MTF) ─────────────────────────────────────────────
def get_15m_opens(coin: str)   -> list[float]: return _cache_15m.get(coin, {}).get("opens",   [])
def get_15m_closes(coin: str)  -> list[float]: return _cache_15m.get(coin, {}).get("closes",  [])
def get_15m_highs(coin: str)   -> list[float]: return _cache_15m.get(coin, {}).get("highs",   [])
def get_15m_lows(coin: str)    -> list[float]: return _cache_15m.get(coin, {}).get("lows",    [])
def get_15m_volumes(coin: str) -> list[float]: return _cache_15m.get(coin, {}).get("volumes", [])
def has_15m_data(coin: str)    -> bool:        return bool(_cache_15m.get(coin, {}).get("closes"))


# ── 1H accessors (AMATERASU MTF) ──────────────────────────────────────────────
def get_1h_opens(coin: str)   -> list[float]: return _cache_1h.get(coin, {}).get("opens",   [])
def get_1h_closes(coin: str)  -> list[float]: return _cache_1h.get(coin, {}).get("closes",  [])
def get_1h_highs(coin: str)   -> list[float]: return _cache_1h.get(coin, {}).get("highs",   [])
def get_1h_lows(coin: str)    -> list[float]: return _cache_1h.get(coin, {}).get("lows",    [])
def has_1h_data(coin: str)    -> bool:        return bool(_cache_1h.get(coin, {}).get("closes"))


# ── Funding rate cache ────────────────────────────────────────────────────────
def set_cached_funding_rate(coin: str, rate: float) -> None:
    """Manually store a funding rate. Called by orchestrator after REST fetch."""
    _cache_funding[coin] = rate
    _cache_funding_ts[coin] = time.monotonic()


def get_cached_funding_rate(coin: str) -> float:
    """Return cached funding rate for *coin*, or 0.0 if not yet fetched."""
    return _cache_funding.get(coin, 0.0)


def funding_is_fresh(coin: str, max_age_sec: float = 900.0) -> bool:
    ts = _cache_funding_ts.get(coin)
    if ts is None:
        return False
    return (time.monotonic() - ts) <= max_age_sec


# ── 1D daily candle cache (Elliott Wave history) ──────────────────────────────

async def fetch_candles_1d(coin: str) -> bool:
    """
    Fetch 200 daily candles for *coin* — ~200 days of history.
    Used by IZANAGI's Elliott Wave engine to identify multi-week impulse waves.
    Refreshed once per day by TradingOrchestrator._central_cycle.
    """
    return await _fetch_and_store(coin, "1d", "1D", 200)


async def refresh_1d_all(coins: set[str], concurrency: int = 5) -> None:
    coins = coins_requiring_refresh(coins, "1d", 120)
    """
    Fetch 1D candles for *coins* in parallel (low concurrency — these are
    large responses and we only need to refresh once per day).
    Typically called at boot for the top-30 coins and then every ~2880 1m cycles.
    """
    sem = asyncio.Semaphore(concurrency)

    async def _one(coin: str) -> None:
        async with sem:
            await fetch_candles_1d(coin)

    await asyncio.gather(*[_one(c) for c in coins], return_exceptions=True)


# ── 1D accessors ──────────────────────────────────────────────────────────────
def get_1d_opens(coin: str)   -> list[float]: return _cache_1d.get(coin, {}).get("opens",   [])
def get_1d_closes(coin: str)  -> list[float]: return _cache_1d.get(coin, {}).get("closes",  [])
def get_1d_highs(coin: str)   -> list[float]: return _cache_1d.get(coin, {}).get("highs",   [])
def get_1d_lows(coin: str)    -> list[float]: return _cache_1d.get(coin, {}).get("lows",    [])
def get_1d_volumes(coin: str) -> list[float]: return _cache_1d.get(coin, {}).get("volumes", [])
def has_1d_data(coin: str)    -> bool:        return bool(_cache_1d.get(coin, {}).get("closes"))

"""WS-first public market-data hub for OxyPulse.

The hub owns long-lived OKX business/public connections and publishes candle
and ticker snapshots into the existing market-data accessors. REST remains a
bootstrap and targeted-repair plane. No private or mutation channel is used.
"""
from __future__ import annotations

import asyncio
import copy
import json
import logging
import math
import random
import re
import time
from typing import Optional
import uuid
import latency_telemetry as _latency
import market_data_stream as _market_data

logger = logging.getLogger("OxyPulse.WSStream")

try:
    import websockets  # type: ignore
    _WS_LIB = True
except ImportError:
    _WS_LIB = False
    logger.info("websockets library is unavailable; REST fallback remains enabled")

OKX_WS_URL = "wss://ws.okx.com:8443/ws/v5/business"
OKX_PUBLIC_WS_URL = "wss://ws.okx.com:8443/ws/v5/public"
PING_INTERVAL = 15
PONG_DEADLINE = 15
RECONNECT_WAIT_BASE = 5
RECONNECT_WAIT_MAX = 60
MAX_SUB_ARGS = 50
MAX_SUB_BYTES = 60 * 1024
SUB_OPERATION_LIMIT = 480
SUB_ACK_TIMEOUT = 10
SUB_WATCHDOG_INTERVAL = 0.5
_WS_YIELD_EVERY = 8
_HISTORY_LIMIT = 220
_MAX_PENDING_REPAIRS = 64
_MAX_DEFERRED_REPAIRS = 2048
_MAX_CONFIRMATION_RETRIES = 3
_GAP_GRACE_MAX_SEC = 3.0
_DISPATCH_QUEUE_MAX = 4096
_DISPATCH_ENQUEUE_TIMEOUT_SEC = 2.0
_TRANSPORT_GAP_ADMISSION_SEC = 1.0
_TRANSPORT_RECONCILE_SEC = 3.0
_TRANSPORT_RECONCILE_SETTLE_SEC = 0.5
_TRANSPORT_KEY_STALL_SEC = 5.0
_TRANSPORT_COHORT_MIN_KEYS = 8
_MAX_TRANSPORT_KEY_STATS = 8192
_CANDLE_CHANNELS = {
    "1m": "candle1m",
    "5m": "candle5m",
    "15m": "candle15m",
    "1h": "candle1H",
    "4h": "candle4H",
    "1d": "candle1D",
}
_TIMEFRAME_SECONDS = {"1m": 60, "5m": 300, "15m": 900, "1h": 3600, "4h": 14400, "1d": 86400}
_REQUIRED_HISTORY = dict(_market_data.REQUIRED_CONSUMER_DEPTH)
_BOOTSTRAP_LIMITS = {"1m": 200, "5m": 200, "15m": 200, "1h": 100, "4h": 220, "1d": 200}
_WS_REQUEST_ID_PATTERN = re.compile(r"^[A-Za-z0-9]{1,32}$")
_REQUEST_PROTOCOL_REJECTED = "REQUEST_PROTOCOL_REJECTED"
_TRANSIENT_SUBSCRIPTION_ERROR = "TRANSIENT"
_REPAIR_FAILURE_REASONS = (
    "EMPTY_RANGE", "FORMING_TARGET", "NO_ROWS_RETURNED", "TARGET_NOT_IN_RESPONSE",
    "CONTINUITY_STILL_MISSING", "TARGET_STILL_UNCONFIRMED", "HTTP_TIMEOUT", "HTTP_ERROR",
    "CANCELLED", "OTHER",
)
_RECOVERY_KIND_MISSING = "MISSING_TIMESTAMP_RANGE"
_RECOVERY_KIND_CONFIRMATION = "UNCONFIRMED_BOUNDARY"


class WSRequestEnvelopeError(ValueError):
    """Raised when a client-built WS request violates the wire contract."""


def validate_ws_request_id(request_id: str) -> str:
    """Validate an optional OKX WS request id before it reaches the socket."""
    if not isinstance(request_id, str) or _WS_REQUEST_ID_PATTERN.fullmatch(request_id) is None:
        raise WSRequestEnvelopeError("WS request id must be 1-32 ASCII alphanumeric characters")
    return request_id


def _coin_from_inst(inst_id: str) -> str:
    value = str(inst_id or "").upper()
    for suffix in ("-USDT-SWAP", "-USDT", "-USD-SWAP", "-USD"):
        if value.endswith(suffix):
            return value[: -len(suffix)]
    return value


def _channel_timeframe(channel: str) -> str | None:
    value = str(channel or "")
    for timeframe, candidate in _CANDLE_CHANNELS.items():
        if value == candidate:
            return timeframe
    return None


def _cache_for_timeframe(timeframe: str) -> dict:
    import market_data_stream
    return market_data_stream._cache_for_timeframe(timeframe)


class WebSocketStream:
    """Own all public WS tasks and publish completed snapshots by timeframe."""

    def __init__(self) -> None:
        self._coins: list[str] = []
        self._timeframes = tuple(_CANDLE_CHANNELS)
        self._running = False
        self._connected = False
        self._public_connected = False
        self._task: Optional[asyncio.Task] = None
        self._public_task: Optional[asyncio.Task] = None
        self._repair_tasks: dict[tuple[str, str], asyncio.Task] = {}
        self._pending_repairs: dict[tuple[str, str], dict] = {}
        self._repair_scheduler_task: Optional[asyncio.Task] = None
        self._bootstrap_task: Optional[asyncio.Task] = None
        self._repair_gate = asyncio.Semaphore(4)
        self._owned_tasks: set[asyncio.Task] = set()
        self._owned_sockets: dict[int, object] = {}
        self._dispatch_queues: dict[tuple[str, int, int], asyncio.Queue] = {}
        self._dispatch_queue_metrics: dict[tuple[str, int, int], dict] = {}
        self._connection_tasks: dict[tuple[str, int, int], tuple[asyncio.Task, ...]] = {}
        self._backpressure_recovery_tasks: dict[tuple[str, int, int], asyncio.Task] = {}
        self._transport_observability_enabled = False
        self._transport_admission_tasks: dict[str, asyncio.Task] = {}
        self._transport_reconcile_tasks: dict[str, asyncio.Task] = {}
        self._transport_reconcile_owners: dict[str, dict | None] = {"business": None, "public": None}
        self._transport_incident_sequence = 0
        self._transport_incidents: dict[str, dict | None] = {"business": None, "public": None}
        self._transport_gap_candidates: dict[str, dict[tuple[str, str], dict]] = {
            "business": {}, "public": {},
        }
        self._transport_key_stats: dict[tuple[str, str, str], dict] = {}
        self._transport_metrics = {
            "business": {
                "frames_received": 0, "frames_applied": 0, "frames_dropped": 0,
                "apply_failures": 0, "generation_changes": 0, "conn_id_changes": 0,
                "queue_depth": 0, "queue_max_depth": 0, "queue_oldest_age_s": 0.0,
                "queue_last_dequeue_latency_s": 0.0, "queue_max_dequeue_latency_s": 0.0,
                "backpressure_faults": 0, "transport_incidents": 0,
                "transport_gap_deferred": 0, "transport_resync_requests": 0,
            },
            "public": {
                "frames_received": 0, "frames_applied": 0, "frames_dropped": 0,
                "apply_failures": 0, "generation_changes": 0, "conn_id_changes": 0,
                "queue_depth": 0, "queue_max_depth": 0, "queue_oldest_age_s": 0.0,
                "queue_last_dequeue_latency_s": 0.0, "queue_max_dequeue_latency_s": 0.0,
                "backpressure_faults": 0, "transport_incidents": 0,
                "transport_gap_deferred": 0, "transport_resync_requests": 0,
            },
        }
        self._retry_handles: dict[str, asyncio.TimerHandle] = {}
        self._transport_started = False
        self._gap_grace_pending: dict[tuple[str, str], dict] = {}
        self._confirmation_states_created = 0
        self._confirmation_resolved_inside_grace = 0
        self._confirmation_grace_expiries = 0
        self._confirmation_rechecks = 0
        self._confirmation_attempts = 0
        self._confirmation_retry_exhausted = 0
        self._msg_count = 0
        self._error_count = 0
        self._event_count = 0
        self._data_count = 0
        self._connect_time = 0.0
        self._last_msg_time = 0.0
        self._last_candle_epoch = 0.0
        self._last_confirmed_candle_epoch = 0.0
        self._connection_generation = 0
        self._public_connection_generation = 0
        self._health_state = {"business": "DISCONNECTED", "public": "DISCONNECTED"}
        self._heartbeat_state = {"business": {}, "public": {}}
        self._generation_heartbeat: dict[str, dict[int, dict]] = {"business": {}, "public": {}}
        self._subscription_expected: set[tuple[str, str, str]] = set()
        self._subscription_confirmed: set[tuple[str, str, str]] = set()
        self._subscription_state: dict[tuple[str, str, str], dict] = {}
        self._subscription_batches: dict[str, dict] = {}
        self._subscription_sequence = 0
        self._invalid_request_id_count = 0
        self._protocol_rejection_count = 0
        self._subscription_operations = {"business": {}, "public": {}}
        self._subscription_send_locks = {"business": asyncio.Lock(), "public": asyncio.Lock()}
        self._subscription_ready_events = {
            "business": asyncio.Event(), "public": asyncio.Event(),
        }
        self._notice_events = {"business": asyncio.Event(), "public": asyncio.Event()}
        self._fault_events = {"business": asyncio.Event(), "public": asyncio.Event()}
        self._fault_reasons: dict[str, dict] = {}
        self._active_generations = {"business": 0, "public": 0}
        self._pending_connections: dict[str, dict | None] = {"business": None, "public": None}
        self._pending_subscriptions: dict[str, dict | None] = {"business": None, "public": None}
        self._gap_state: dict[tuple[str, str], dict] = {}
        self._gap_event_counts: dict[str, int] = {}
        self._gap_event_by_timeframe: dict[str, dict[str, int]] = {}
        self._gap_count = 0
        self._gap_duplicate_count = 0
        self._gap_range_extension_count = 0
        self._repair_scheduled = 0
        self._repair_count = 0
        self._repair_success = 0
        self._repair_failures = 0
        self._repair_deferred = 0
        self._repair_retry_count = 0
        self._repair_failure_reasons = {reason: 0 for reason in _REPAIR_FAILURE_REASONS}
        self._repair_samples: list[dict] = []
        self._gap_decision_counts = {decision: 0 for decision in ("ADJACENT", "DUPLICATE", "OUT_OF_ORDER", "GAP")}
        self._gap_decision_samples: list[dict] = []
        self._boundary_events_seen = {timeframe: 0 for timeframe in self._timeframes}
        self._new_adjacent_transitions = {timeframe: 0 for timeframe in self._timeframes}
        self._continuity_pair_checks = {timeframe: 0 for timeframe in self._timeframes}
        self._task_exception_count = 0
        self._task_exception_last: dict | None = None
        self._heartbeat_metrics = {
            "business": {"pings": 0, "pongs": 0, "pong_timeouts": 0, "faults": 0},
            "public": {"pings": 0, "pongs": 0, "pong_timeouts": 0, "faults": 0},
        }
        self._reconnect_counts = {"business": 0, "public": 0}

    def is_available(self) -> bool:
        return _WS_LIB and self._health_state["business"] == "HEALTHY"

    def market_data_ready_for(
        self, coins: set[str], timeframes: tuple[str, ...] = ("1m",),
    ) -> bool:
        """Return engine readiness, distinct from transport/subscription health."""
        return self.is_ready_for(coins, timeframes)

    def is_ready_for(self, coins: set[str], timeframes: tuple[str, ...] = ("1m",)) -> bool:
        wanted = {str(c).upper() for c in coins if c}
        if not wanted or not self.is_available():
            return False
        import market_data_stream
        snapshots = [
            market_data_stream.timeframe_snapshot(
                coin, tf, _REQUIRED_HISTORY.get(tf, 1)
            )
            for coin in wanted for tf in timeframes
        ]
        subscriptions_ready = all(
            self._subscription_state.get(("business", _CANDLE_CHANNELS[tf], self._inst_id(coin)), {}).get("status") == "ACTIVE"
            for coin in wanted for tf in timeframes if tf in _CANDLE_CHANNELS
        )
        return bool(snapshots) and subscriptions_ready and all(
            snapshot.get("ready", False)
            and str(snapshot.get("source", "")).startswith("okx_public_")
            and not snapshot.get("stale", True)
            and snapshot.get("continuity_ready", False)
            for snapshot in snapshots
        )

    @staticmethod
    def _inst_id(coin: str) -> str:
        import market_mode as mm
        return mm.inst_id(coin)

    @staticmethod
    def _domain(public: bool) -> str:
        return "public" if public else "business"

    def _next_generation(self, public: bool, minimum: int = 0) -> int:
        domain = self._domain(public)
        if public:
            self._public_connection_generation += 1
            self._public_connection_generation = max(self._public_connection_generation, int(minimum or 0))
            return self._public_connection_generation
        self._connection_generation += 1
        self._connection_generation = max(self._connection_generation, int(minimum or 0))
        return self._connection_generation

    def _connection_health(self, public: bool) -> dict:
        return self._heartbeat_state[self._domain(public)]

    def _generation_health(self, public: bool, generation: int) -> dict:
        domain = self._domain(public)
        return self._generation_heartbeat.setdefault(domain, {}).setdefault(int(generation), {})

    def _set_health(self, public: bool, state: str) -> None:
        self._health_state[self._domain(public)] = state

    def _subscription_key(self, arg: dict, public: bool) -> tuple[str, str, str]:
        return (
            self._domain(public),
            str(arg.get("channel", "")),
            str(arg.get("instId", arg.get("instType", ""))),
        )

    @staticmethod
    def _message_metadata(value: str) -> tuple[dict, str, str, str | None]:
        """Extract bounded routing metadata without applying market state."""
        message = json.loads(value) if isinstance(value, str) else dict(value)
        arg = message.get("arg") or {}
        channel = str(arg.get("channel", ""))
        instrument = str(arg.get("instId", arg.get("instType", "")))
        conn_id = message.get("connId") or arg.get("connId")
        return message, channel, instrument, str(conn_id) if conn_id else None

    def _transport_key(self, public: bool, channel: str, instrument: str) -> tuple[str, str, str]:
        return self._domain(public), str(channel), str(instrument)

    def _transport_stats_state(self, key: tuple[str, str, str]) -> dict:
        """Return one telemetry row while enforcing deterministic LRU capacity."""
        state = self._transport_key_stats.get(key)
        if state is not None:
            self._transport_key_stats.pop(key)
            self._transport_key_stats[key] = state
            return state
        while len(self._transport_key_stats) >= _MAX_TRANSPORT_KEY_STATS:
            self._transport_key_stats.pop(next(iter(self._transport_key_stats)))
        state = {}
        self._transport_key_stats[key] = state
        return state

    def _observe_wire_frame(
        self, value: str, *, public: bool, generation: int, received_at: float,
        connection_id: tuple[str, int, int] | None = None,
    ) -> None:
        domain = self._domain(public)
        metrics = self._transport_metrics[domain]
        metrics["frames_received"] += 1
        try:
            message, channel, instrument, conn_id = self._message_metadata(value)
        except Exception:
            metrics["frames_dropped"] += 1
            return
        active_generation = int(self._active_generations.get(domain, 0) or 0)
        if conn_id and (not active_generation or active_generation == int(generation)):
            previous = metrics.get("conn_id")
            if previous and previous != conn_id:
                metrics["conn_id_changes"] += 1
            metrics["conn_id"] = conn_id
        event = str(message.get("event", "") or "").lower()
        if not channel and not event:
            return
        key = self._transport_key(public, channel, instrument)
        if channel or instrument:
            state = self._transport_stats_state(key)
            state.update({
                "domain": domain,
                "channel": channel,
                "instrument": instrument,
                "generation": int(generation),
                "socket_id": (int(connection_id[2]) if connection_id is not None else state.get("socket_id", 0)),
                "conn_id": conn_id or state.get("conn_id"),
                "last_wire_rx_monotonic": float(received_at),
                "frames_received": int(state.get("frames_received", 0)) + 1,
                "subscription_status": self._subscription_state.get(key, {}).get("status"),
            })
            data = message.get("data") or []
            if data and channel in _CANDLE_CHANNELS:
                try:
                    state["latest_exchange_ts"] = max(
                        float(row[0]) / 1000.0 if float(row[0]) > 10_000_000_000 else float(row[0])
                        for row in data if row
                    )
                except (TypeError, ValueError, IndexError):
                    pass

    def _record_frame_applied(
        self, value: str, *, public: bool, generation: int, received_at: float,
        applied_at: float, dropped: bool = False,
        connection_id: tuple[str, int, int] | None = None,
    ) -> None:
        domain = self._domain(public)
        metrics = self._transport_metrics[domain]
        metrics["frames_dropped" if dropped else "frames_applied"] += 1
        try:
            message, channel, instrument, conn_id = self._message_metadata(value)
        except Exception:
            return
        if not channel and not instrument:
            return
        key = self._transport_key(public, channel, instrument)
        state = self._transport_stats_state(key)
        state.update({
            "socket_id": (int(connection_id[2]) if connection_id is not None else state.get("socket_id", 0)),
            "conn_id": conn_id or state.get("conn_id"),
            "last_apply_monotonic": float(applied_at),
            "apply_latency_ms": round(max(float(applied_at) - float(received_at), 0.0) * 1000.0, 3),
            "generation": int(generation),
            "subscription_status": self._subscription_state.get(key, {}).get("status"),
            "frames_applied": int(state.get("frames_applied", 0)) + (0 if dropped else 1),
        })

    def _transport_key_age(self, key: tuple[str, str, str], now: float) -> float | None:
        state = self._transport_key_stats.get(key, {})
        received = float(state.get("last_wire_rx_monotonic", 0.0) or 0.0)
        return max(now - received, 0.0) if received else None

    def _queue_metrics_row(self, connection_id: tuple[str, int, int] | None) -> dict | None:
        if connection_id is None:
            return None
        return self._dispatch_queue_metrics.setdefault(connection_id, {
            "queue_depth": 0,
            "queue_max_depth": 0,
            "queue_oldest_age_s": 0.0,
            "queue_last_dequeue_latency_s": 0.0,
            "queue_max_dequeue_latency_s": 0.0,
        })

    def _queue_metrics_are_authoritative(
        self, domain: str, connection_id: tuple[str, int, int] | None,
    ) -> bool:
        return connection_id is None or int(connection_id[1]) == int(
            self._active_generations.get(domain, 0) or 0
        )

    def _update_queue_metrics(
        self, domain: str, queue: asyncio.Queue,
        connection_id: tuple[str, int, int] | None = None,
    ) -> None:
        depth = queue.qsize()
        oldest_age = 0.0
        if depth:
            try:
                oldest = queue._queue[0][1]  # type: ignore[attr-defined]
                oldest_age = max(time.monotonic() - float(oldest), 0.0)
            except (AttributeError, IndexError, TypeError, ValueError):
                oldest_age = 0.0
        row = self._queue_metrics_row(connection_id)
        if row is not None:
            row["queue_depth"] = depth
            row["queue_max_depth"] = max(int(row.get("queue_max_depth", 0)), depth)
            row["queue_oldest_age_s"] = oldest_age
        if self._queue_metrics_are_authoritative(domain, connection_id):
            metrics = self._transport_metrics[domain]
            metrics["queue_depth"] = depth
            metrics["queue_max_depth"] = max(int(metrics.get("queue_max_depth", 0)), depth)
            metrics["queue_oldest_age_s"] = oldest_age

    def _record_queue_dequeue_latency(
        self, domain: str, latency_s: float,
        connection_id: tuple[str, int, int] | None = None,
    ) -> None:
        latency_s = max(float(latency_s), 0.0)
        row = self._queue_metrics_row(connection_id)
        if row is not None:
            row["queue_last_dequeue_latency_s"] = latency_s
            row["queue_max_dequeue_latency_s"] = max(
                float(row.get("queue_max_dequeue_latency_s", 0.0)), latency_s,
            )
        if self._queue_metrics_are_authoritative(domain, connection_id):
            metrics = self._transport_metrics[domain]
            metrics["queue_last_dequeue_latency_s"] = latency_s
            metrics["queue_max_dequeue_latency_s"] = max(
                float(metrics.get("queue_max_dequeue_latency_s", 0.0)), latency_s,
            )

    def _schedule_backpressure_recovery(
        self, value: str, *, public: bool, generation: int,
        connection_id: tuple[str, int, int] | None,
    ) -> None:
        domain = self._domain(public)
        owner = connection_id or (domain, int(generation), 0)
        existing = self._backpressure_recovery_tasks.get(owner)
        if existing is not None and not existing.done():
            return
        task = self._create_owned_task(
            self._recover_backpressure_drop(
                value, public=public, generation=generation, connection_id=connection_id,
            ),
            name=f"ws-backpressure-recovery:{domain}:{generation}:{owner[2]}",
        )
        self._backpressure_recovery_tasks[owner] = task

        def _cleanup(done_task, *, _owner=owner):
            if self._backpressure_recovery_tasks.get(_owner) is done_task:
                self._backpressure_recovery_tasks.pop(_owner, None)

        task.add_done_callback(_cleanup)

    async def _recover_backpressure_drop(
        self, value: str, *, public: bool, generation: int,
        connection_id: tuple[str, int, int] | None,
    ) -> None:
        domain = self._domain(public)
        if int(self._active_generations.get(domain, 0) or 0) != int(generation):
            return
        if connection_id is not None and connection_id not in self._dispatch_queues:
            return
        try:
            message, channel, instrument, _conn_id = self._message_metadata(value)
        except Exception:
            return
        timeframe = _channel_timeframe(channel)
        if timeframe is None:
            self._record_gap_event("backpressure_non_candle_drop", "*", str(channel or "unknown"))
            return
        coin = _coin_from_inst(instrument)
        if not coin:
            return
        row = _cache_for_timeframe(timeframe).get(coin, {})
        self._check_continuity(
            coin, timeframe, row,
            trigger_type="DISPATCH_BACKPRESSURE_DROP",
        )
        try:
            import market_data_stream
            for candle in message.get("data") or []:
                timestamp = float(candle[0]) / 1000.0 if float(candle[0]) > 10_000_000_000 else float(candle[0])
                confirmed = str(candle[8]) == "1" if len(candle) > 8 else False
                if not confirmed:
                    continue
                target = market_data_stream.candle_range_status(
                    row, timeframe, timestamp, timestamp,
                )
                if target["missing"]:
                    self._mark_gap(
                        coin, timeframe, timestamp, timestamp,
                        trigger_type="DISPATCH_BACKPRESSURE_DROP",
                        recovery_kind=_RECOVERY_KIND_MISSING,
                    )
                elif target["unconfirmed"]:
                    self._mark_gap(
                        coin, timeframe, timestamp, timestamp,
                        trigger_type="DISPATCH_BACKPRESSURE_DROP",
                        recovery_kind=_RECOVERY_KIND_CONFIRMATION,
                    )
        except (TypeError, ValueError, IndexError):
            return
        self._record_gap_event("backpressure_candle_recovery", coin, timeframe)

    def _transport_is_stalled(self, key: tuple[str, str], now: float) -> bool:
        coin, timeframe = key
        if timeframe != "1m":
            return False
        instrument = self._inst_id(coin)
        transport_key = self._transport_key(False, _CANDLE_CHANNELS[timeframe], instrument)
        transport = self._transport_key_stats.get(transport_key)
        if transport is None:
            # Bounded telemetry is observational. Eviction or no observation is
            # insufficient evidence for a transport stall.
            return False
        if int(transport.get("generation", 0) or 0) != int(
            self._active_generations.get("business", 0) or 0
        ):
            # Prior-generation telemetry is unknown for current authority; it
            # cannot itself establish a current-generation transport stall.
            return False
        status = self._subscription_state.get(
            ("business", _CANDLE_CHANNELS[timeframe], instrument), {},
        ).get("status")
        age = self._transport_key_age(transport_key, now)
        return bool(
            status not in {None, "ACTIVE"}
            or (age is not None and age >= _TRANSPORT_KEY_STALL_SEC)
        )

    @staticmethod
    def _batch_keys(metadata: dict | set[tuple[str, str, str]]) -> set[tuple[str, str, str]]:
        if isinstance(metadata, dict):
            return set(metadata.get("expected_keys", set()))
        return set(metadata)

    def _pending_subscription_profile(self, domain: str, generation: int) -> dict | None:
        profile = self._pending_subscriptions.get(domain)
        if profile and int(profile.get("generation", 0) or 0) == int(generation):
            return profile
        return None

    def _subscription_ready(self, public: bool, generation: int) -> bool:
        domain = self._domain(public)
        profile = self._pending_subscription_profile(domain, generation)
        if profile is not None:
            keys = set(profile["expected"])
            states = profile["state"]
        else:
            keys = {key for key in self._subscription_expected if key[0] == domain}
            states = self._subscription_state
        return bool(keys) and all(
            states.get(key, {}).get("status") == "ACTIVE"
            and int(states.get(key, {}).get("generation", 0) or 0) == int(generation)
            for key in keys
        )

    def _refresh_subscription_ready_event(self, public: bool, generation: int) -> None:
        domain = self._domain(public)
        if not self._subscription_ready(public, generation):
            return
        profile = self._pending_subscription_profile(domain, generation)
        if profile is not None:
            profile["ready_event"].set()
            profile["health"] = "READY"
            return
        self._subscription_ready_events[domain].set()
        if int(self._active_generations.get(domain, 0) or 0) != int(generation):
            return
        self._set_health(public, "HEALTHY")
        if self._transport_incident_active(domain):
            self._start_transport_reconcile(domain, generation)

    def _report_connection_fault(self, public: bool, generation: int, reason: str) -> bool:
        """Report a fault to its pending or authoritative connection owner."""
        domain = self._domain(public)
        profile = self._pending_subscription_profile(domain, generation)
        pending = self._pending_connections.get(domain) or {}
        if (
            profile is not None
            and int(pending.get("generation", 0) or 0) == int(generation)
        ):
            if profile["fault_event"].is_set():
                return False
            profile["fault_reason"] = {
                "generation": int(generation),
                "reason": str(reason),
                "reported_at": time.time(),
            }
            profile["health"] = "FAULTED"
            profile["fault_event"].set()
            return True
        state = self._heartbeat_state.get(domain, {})
        if int(state.get("generation", 0) or 0) != int(generation):
            return False
        if int(self._active_generations.get(domain, 0) or 0) != int(generation):
            return False
        if self._fault_events[domain].is_set():
            return False
        self._fault_reasons[domain] = {
            "generation": int(generation),
            "reason": str(reason),
            "reported_at": time.time(),
        }
        self._heartbeat_metrics[domain]["faults"] += 1
        self._set_health(public, "DEGRADED")
        self._fault_events[domain].set()
        return True

    def _observe_owned_task(self, task: asyncio.Task) -> None:
        """Retrieve task exceptions so no WS task fails silently."""
        self._owned_tasks.discard(task)
        if task.cancelled():
            return
        try:
            exception = task.exception()
        except BaseException:
            return
        if exception is not None:
            self._task_exception_count += 1
            self._task_exception_last = {
                "task": task.get_name(),
                "exception": type(exception).__name__,
                "message": str(exception),
                "observed_at": time.time(),
            }
            logger.error(
                "WS owned task failed name=%s exception=%s",
                task.get_name(), type(exception).__name__,
                exc_info=(type(exception), exception, exception.__traceback__),
            )

    def _create_owned_task(self, coro, *, name: str) -> asyncio.Task:
        task = asyncio.create_task(coro, name=name)
        self._owned_tasks.add(task)
        task.add_done_callback(self._observe_owned_task)
        return task

    def _own_socket(self, ws) -> None:
        if ws is not None:
            self._owned_sockets[id(ws)] = ws

    def _owned_task_snapshot(self) -> list[asyncio.Task]:
        return [task for task in self._owned_tasks if not task.done()]

    def owned_task_names(self) -> list[str]:
        return sorted(str(task.get_name()) for task in self._owned_task_snapshot())

    async def start(self, coins: list[str]) -> bool:
        if not _WS_LIB:
            return False
        self._coins = sorted({str(c).upper() for c in coins if c})
        self._running = True
        self._transport_started = False
        self._set_health(False, "CONNECTING")
        self._set_health(True, "CONNECTING")
        loop = asyncio.get_running_loop()
        if self._task is None or self._task.done():
            self._task = self._create_owned_task(self._run_loop(), name="ws-market-candles")
        if self._public_task is None or self._public_task.done():
            self._public_task = self._create_owned_task(self._run_public_loop(), name="ws-market-public")
        if self._repair_scheduler_task is None or self._repair_scheduler_task.done():
            self._repair_scheduler_task = self._create_owned_task(
                self._repair_scheduler(), name="ws-gap-repair-scheduler",
            )
        if self._bootstrap_task is None or self._bootstrap_task.done():
            self._bootstrap_task = self._create_owned_task(
                self._bootstrap_market_data(), name="market-data-cold-bootstrap",
            )
        deadline = time.monotonic() + 4.0
        while time.monotonic() < deadline and not self._connected:
            await asyncio.sleep(0.1)
        if self._connected:
            self._transport_started = True
            logger.info("WS market hub active: %d instruments, timeframes=%s", len(self._coins), self._timeframes)
        else:
            logger.warning("WS market hub did not connect within 4s; REST bootstrap/fallback remains active")
        return self._transport_started

    async def stop(self) -> None:
        self._running = False
        self._transport_started = False
        self._connected = False
        self._public_connected = False
        for handle in self._retry_handles.values():
            handle.cancel()
        self._retry_handles.clear()
        tasks = list({
            *self._owned_task_snapshot(), self._task, self._public_task,
            self._repair_scheduler_task, self._bootstrap_task, *self._repair_tasks.values(),
        })
        self._repair_tasks.clear()
        self._pending_repairs.clear()
        self._gap_grace_pending.clear()
        self._transport_gap_candidates = {"business": {}, "public": {}}
        self._transport_incidents = {"business": None, "public": None}
        self._dispatch_queues.clear()
        self._dispatch_queue_metrics.clear()
        self._connection_tasks.clear()
        self._backpressure_recovery_tasks.clear()
        self._pending_subscriptions = {"business": None, "public": None}
        self._transport_admission_tasks.clear()
        self._transport_reconcile_tasks.clear()
        self._transport_reconcile_owners = {"business": None, "public": None}
        for task in tasks:
            if task and not task.done():
                task.cancel()
        for task in tasks:
            if task:
                try:
                    await task
                except BaseException:
                    pass
        self._task = None
        self._public_task = None
        self._repair_scheduler_task = None
        self._bootstrap_task = None
        self._owned_tasks.clear()
        for ws in list(self._owned_sockets.values()):
            await self._close_socket(ws)
        self._owned_sockets.clear()
        self._set_health(False, "CLOSED")
        self._set_health(True, "CLOSED")
        self._mark_all_disconnected()
        logger.info("WebSocket market hub stopped")

    async def update_coins(self, coins: list[str]) -> None:
        new_coins = sorted({str(c).upper() for c in coins if c})
        if new_coins == self._coins:
            return
        added_coins = sorted(set(new_coins) - set(self._coins))
        self._coins = new_coins
        if self._running:
            if added_coins:
                self._create_owned_task(
                    self._bootstrap_market_data(added_coins),
                    name="market-data-incremental-bootstrap",
                )
            for domain, task in (
                ("business", self._task), ("public", self._public_task),
            ):
                if task and not task.done():
                    self._notice_events[domain].set()
                elif domain == "business":
                    self._task = self._create_owned_task(self._run_loop(), name="ws-market-candles")
                else:
                    self._public_task = self._create_owned_task(self._run_public_loop(), name="ws-market-public")

    async def _bootstrap_market_data(self, coins: list[str] | None = None) -> dict:
        """Run one bounded cold/incremental bootstrap owned by the WS plane."""
        import market_data_stream
        target = list(coins if coins is not None else self._coins)
        result = await market_data_stream.bootstrap_universe(
            target,
            requirements={timeframe: _BOOTSTRAP_LIMITS[timeframe] for timeframe in self._timeframes},
            scan_generation="ws-bootstrap",
            max_concurrency=8,
        )
        for coin in target:
            for timeframe in self._timeframes:
                row = _cache_for_timeframe(timeframe).get(str(coin).upper(), {})
                if row:
                    self._check_continuity(
                        str(coin).upper(), timeframe, row,
                        now=time.time(), trigger_type="BOOTSTRAP_CONTINUITY_GAP",
                    )
        logger.info(
            "Market data bootstrap complete: requested=%d ok=%d failed=%d",
            result.get("requested", 0), result.get("ok", 0), result.get("failed", 0),
        )
        return result

    def stats(self) -> dict:
        now = time.monotonic()
        import market_data_stream
        completeness_by_timeframe = {}
        for timeframe in self._timeframes:
            required = _REQUIRED_HISTORY[timeframe]
            history_complete = 0
            transport_fresh = 0
            market_ready = 0
            subscription_active = 0
            for coin in self._coins:
                snapshot = market_data_stream.timeframe_snapshot(coin, timeframe, required)
                key = ("business", _CANDLE_CHANNELS[timeframe], self._inst_id(coin))
                if self._subscription_state.get(key, {}).get("status") == "ACTIVE":
                    subscription_active += 1
                history_complete += int(bool(snapshot.get("confirmed_history_complete")))
                transport_fresh += int(bool(snapshot.get("transport_fresh")))
                market_ready += int(bool(
                    snapshot.get("ready") and snapshot.get("continuity_ready")
                    and not snapshot.get("stale")
                ))
            completeness_by_timeframe[timeframe] = {
                "universe": len(self._coins),
                "subscription_active": subscription_active,
                "history_complete": history_complete,
                "transport_fresh": transport_fresh,
                "market_ready": market_ready,
                "gaps_open": sum(
                    1 for (coin, gap_timeframe), state in self._gap_state.items()
                    if gap_timeframe == timeframe and state.get("active")
                ),
                "repairs_pending": sum(
                    1 for (coin, gap_timeframe) in self._pending_repairs
                    if gap_timeframe == timeframe
                ),
                "repairs_inflight": sum(
                    1 for (coin, gap_timeframe) in self._repair_tasks
                    if gap_timeframe == timeframe
                ),
            }
        request_reason_metrics = market_data_stream.request_reason_snapshot()
        transport_snapshot = {
            "enabled": self._transport_observability_enabled,
            "domains": copy.deepcopy(self._transport_metrics),
            "incidents": {
                domain: {
                    key: (sorted(value) if isinstance(value, set) else value)
                    for key, value in incident.items()
                    if key != "deferred_keys"
                } | {
                    "deferred_keys": sorted(incident.get("deferred_keys", set()))
                }
                if incident else None
                for domain, incident in self._transport_incidents.items()
            },
            "queues": {
                "|".join(str(value) for value in connection_id): copy.deepcopy(metrics)
                for connection_id, metrics in self._dispatch_queue_metrics.items()
            },
            "keys": {},
        }
        for key, state in self._transport_key_stats.items():
            received = float(state.get("last_wire_rx_monotonic", 0.0) or 0.0)
            applied = float(state.get("last_apply_monotonic", 0.0) or 0.0)
            transport_snapshot["keys"]["|".join(key)] = {
                **{name: value for name, value in state.items()
                   if name not in {"last_wire_rx_monotonic", "last_apply_monotonic"}},
                "last_rx_age_s": max(now - received, 0.0) if received else None,
                "last_apply_age_s": max(now - applied, 0.0) if applied else None,
            }
        return {
            "transport_started": self._transport_started,
            "connected": self._connected,
            "public_connected": self._public_connected,
            "library": _WS_LIB,
            "coins": len(self._coins),
            "timeframes": list(self._timeframes),
            "messages": self._msg_count,
            "errors": self._error_count,
            "event_messages": self._event_count,
            "data_messages": self._data_count,
            "uptime_s": int(now - self._connect_time) if self._connected else 0,
            "last_msg_s": max(now - self._last_msg_time, 0.0) if self._last_msg_time else -1,
            "last_candle_epoch": self._last_candle_epoch,
            "last_confirmed_candle_epoch": self._last_confirmed_candle_epoch,
            "connection_generation": self._connection_generation,
            "public_connection_generation": self._public_connection_generation,
            "health_state": dict(self._health_state),
            "heartbeat": {
                domain: {
                    "last_rx_monotonic": state.get("last_rx_monotonic", 0.0),
                    "last_pong_monotonic": state.get("last_pong_monotonic", 0.0),
                    "ping_sent_at": state.get("ping_sent_at", 0.0),
                    "awaiting_pong": bool(state.get("awaiting_pong", False)),
                }
                for domain, state in self._heartbeat_state.items()
            },
            "subscriptions_expected": len(self._subscription_expected),
            "subscriptions_confirmed": len(self._subscription_confirmed),
            "subscription_ready": bool(self._subscription_expected) and all(
                row.get("status") == "ACTIVE" for row in self._subscription_state.values()
                if row.get("expected")
            ),
            "market_data_ready": self.market_data_ready_for(set(self._coins), self._timeframes),
            "owned_task_count": len(self._owned_task_snapshot()),
            "owned_task_names": self.owned_task_names(),
            "subscription_states": {
                status: sum(1 for row in self._subscription_state.values() if row.get("status") == status)
                for status in ("REQUESTED", "ACTIVE", "FAILED", "RETRY_WAIT", "RECOVERING", "INACTIVE")
            },
            "subscription_operations": {
                domain: int(state.get("count", 0)) for domain, state in self._subscription_operations.items()
            },
            "invalid_request_ids_local": self._invalid_request_id_count,
            "protocol_rejections": self._protocol_rejection_count,
            "gap_count": self._gap_count,
            "targeted_repairs": self._repair_count,
            "repair_success": self._repair_success,
            "repair_failures": self._repair_failures,
            "repair_deferred": self._repair_deferred,
            "repair_retry": self._repair_retry_count,
            "repair_failure_reasons": dict(self._repair_failure_reasons),
            "repair_samples": copy.deepcopy(self._repair_samples[-64:]),
            "gap_decision_counts": dict(self._gap_decision_counts),
            "gap_decision_samples": copy.deepcopy(self._gap_decision_samples),
            "boundary_events_seen": dict(self._boundary_events_seen),
            "new_adjacent_transitions": dict(self._new_adjacent_transitions),
            "continuity_pair_checks": dict(self._continuity_pair_checks),
            "gap_duplicate_coalesced": self._gap_duplicate_count,
            "gap_range_extensions": self._gap_range_extension_count,
            "repair_scheduled": self._repair_scheduled,
            "pending_repairs": len(self._pending_repairs),
            "gap_events": copy.deepcopy(self._gap_event_counts),
            "gap_events_by_timeframe": copy.deepcopy(self._gap_event_by_timeframe),
            "confirmation_recovery": {
                "states_created": self._confirmation_states_created,
                "resolved_inside_grace": self._confirmation_resolved_inside_grace,
                "grace_expiries": self._confirmation_grace_expiries,
                "canonical_rechecks": self._confirmation_rechecks,
                "rest_attempts": self._confirmation_attempts,
                "retry_exhausted": self._confirmation_retry_exhausted,
                "pending": sum(
                    1 for state in self._gap_grace_pending.values()
                    if state.get("recovery_kind") == _RECOVERY_KIND_CONFIRMATION
                ),
            },
            "completeness_by_timeframe": completeness_by_timeframe,
            "stale_instruments": sum(1 for row in self._gap_state.values() if row.get("active")),
            "candle_clock_skew_s": max(time.time() - self._last_candle_epoch, 0.0) if self._last_candle_epoch else None,
            "heartbeat_metrics": copy.deepcopy(self._heartbeat_metrics),
            "reconnect_counts": dict(self._reconnect_counts),
            "task_exception_count": self._task_exception_count,
            "task_exception_last": copy.deepcopy(self._task_exception_last),
            "fault_reasons": copy.deepcopy(self._fault_reasons),
            "candle_request_reasons": request_reason_metrics,
            "market_request_audit": _latency.market_request_snapshot(),
            "transport": transport_snapshot,
        }

    async def _run_loop(self) -> None:
        await self._run_connection(OKX_WS_URL, public=False)

    async def _run_public_loop(self) -> None:
        await self._run_connection(OKX_PUBLIC_WS_URL, public=True)

    async def _run_connection(self, url: str, *, public: bool) -> None:
        backoff = RECONNECT_WAIT_BASE
        while self._running:
            ws = None
            try:
                ws = await websockets.connect(url, ping_interval=None, open_timeout=10)  # type: ignore[union-attr]
                self._own_socket(ws)
                generation = self._next_generation(public)
                self._transport_observability_enabled = True
                self._reset_connection_domain(public, generation)
                self._heartbeat_state[self._domain(public)]["socket_id"] = id(ws)
                if public:
                    await self._subscribe_public(ws, generation)
                    self._public_connected = True
                else:
                    await self._subscribe_candles(ws, generation)
                    self._connected = True
                    self._connect_time = time.monotonic()
                backoff = RECONNECT_WAIT_BASE
                tasks = None
                while self._running and ws is not None:
                    replacement = await self._monitor_connection(
                        ws, public, generation, prestarted=tasks,
                    )
                    await self._close_socket(ws)
                    if replacement is None:
                        if self._fault_events[self._domain(public)].is_set() and self._running:
                            self._reconnect_counts[self._domain(public)] += 1
                            self._set_health(public, "RECONNECTING")
                            await asyncio.sleep(
                                backoff + random.uniform(0.0, min(1.0, backoff * 0.1)),
                            )
                            backoff = min(backoff * 2, RECONNECT_WAIT_MAX)
                        break
                    self._retire_generation(public, generation)
                    ws, generation, tasks = replacement
            except asyncio.CancelledError:
                break
            except Exception as exc:
                self._error_count += 1
                self._reconnect_counts[self._domain(public)] += 1
                logger.warning("WS %s connection lost: %s; reconnecting in %ss", "public" if public else "candle", exc, backoff)
                self._set_health(public, "RECONNECTING")
                await asyncio.sleep(backoff + random.uniform(0.0, min(1.0, backoff * 0.1)))
                backoff = min(backoff * 2, RECONNECT_WAIT_MAX)
            finally:
                if ws is not None:
                    await self._close_socket(ws)
                if public:
                    self._public_connected = False
                    if self._health_state["public"] != "CLOSED":
                        self._set_health(True, "DISCONNECTED")
                else:
                    self._connected = False
                    if self._health_state["business"] != "CLOSED":
                        self._set_health(False, "DISCONNECTED")
                    self._mark_all_disconnected()
        if public:
            self._public_connected = False
        else:
            self._connected = False

    async def _close_socket(self, ws) -> None:
        socket_id = id(ws)
        connection_tasks: list[asyncio.Task] = []
        try:
            close = getattr(ws, "close", None)
            if close is not None:
                result = close()
                if asyncio.iscoroutine(result):
                    await result
        except Exception:
            logger.debug("WS close failed", exc_info=True)
        finally:
            self._owned_sockets.pop(socket_id, None)
            for connection_id in [
                value for value in self._dispatch_queues if value[2] == socket_id
            ]:
                self._dispatch_queues.pop(connection_id, None)
                self._dispatch_queue_metrics.pop(connection_id, None)
                connection_tasks.extend(self._connection_tasks.pop(connection_id, ()))
                recovery = self._backpressure_recovery_tasks.pop(connection_id, None)
                if recovery is not None:
                    connection_tasks.append(recovery)
            await self._cancel_owned_tasks(connection_tasks)

    def _reset_connection_domain(
        self, public: bool, generation: int, *, activate: bool = True,
    ) -> None:
        """Prepare a connection domain, optionally without changing authority."""
        domain = self._domain(public)
        previous_state = self._heartbeat_state.get(domain, {})
        previous_generation = int(previous_state.get("generation", 0) or 0)
        if previous_state.get("generation"):
            self._generation_heartbeat.setdefault(domain, {})[
                int(previous_state["generation"])
            ] = previous_state
        if activate and previous_generation and previous_generation != int(generation):
            self._transport_metrics[domain]["generation_changes"] += 1
            if self._transport_observability_enabled:
                self._begin_transport_incident(
                    domain, int(generation), "connection_generation_change", set(),
                    evidence_kind="CONNECTION_GENERATION",
                )
        now = time.monotonic()
        state = {
            "generation": generation,
            "last_rx_monotonic": now,
            "last_pong_monotonic": now,
            "ping_sent_at": 0.0,
            "awaiting_pong": False,
            "socket_id": 0,
        }
        self._generation_heartbeat.setdefault(domain, {})[generation] = state
        if activate:
            self._subscription_expected = {key for key in self._subscription_expected if key[0] != domain}
            self._subscription_confirmed = {key for key in self._subscription_confirmed if key[0] != domain}
            self._subscription_state = {
                key: row for key, row in self._subscription_state.items() if key[0] != domain
            }
            self._subscription_batches = {
                request_id: metadata for request_id, metadata in self._subscription_batches.items()
                if not any(key[0] == domain for key in self._batch_keys(metadata))
            }
            self._subscription_operations[domain] = {"count": 0, "window_start": now}
            self._subscription_ready_events[domain].clear()
            self._notice_events[domain].clear()
            self._fault_events[domain].clear()
            self._fault_reasons.pop(domain, None)
            self._pending_subscriptions[domain] = None
            self._heartbeat_state[domain] = state
            self._active_generations[domain] = generation
            self._pending_connections[domain] = None
            self._set_health(public, "SUBSCRIBING")
        else:
            self._pending_connections[domain] = {
                "generation": int(generation), "socket_id": 0,
            }
            self._pending_subscriptions[domain] = {
                "generation": int(generation),
                "expected": set(), "confirmed": set(), "state": {}, "batches": {},
                "operations": {"count": 0, "window_start": now},
                "ready_event": asyncio.Event(), "notice_event": asyncio.Event(),
                "fault_event": asyncio.Event(), "fault_reason": None,
                "health": "SUBSCRIBING",
            }

    def _start_connection_tasks(self, ws, *, public: bool, generation: int, handover: bool = False):
        domain = self._domain(public)
        connection_id = (domain, int(generation), id(ws))
        self._transport_observability_enabled = True
        queue: asyncio.Queue = asyncio.Queue(maxsize=_DISPATCH_QUEUE_MAX)
        self._dispatch_queues[connection_id] = queue
        suffix = "-handover" if handover else ""
        receiver = self._create_owned_task(
            self._receive_loop(
                ws, public=public, generation=generation, queue=queue,
                connection_id=connection_id,
            ),
            name=f"ws-{domain}{suffix}-receive",
        )
        dispatcher = self._create_owned_task(
            self._dispatch_loop(
                queue, public=public, generation=generation,
                connection_id=connection_id,
            ),
            name=f"ws-{domain}{suffix}-dispatch",
        )
        heartbeat = self._create_owned_task(
            self._heartbeat(ws, public=public, generation=generation),
            name=f"ws-{domain}{suffix}-heartbeat",
        )
        watchdog = self._create_owned_task(
            self._subscription_watchdog(ws, public=public, generation=generation),
            name=f"ws-subscription-watchdog:{domain}",
        )
        self._connection_tasks[connection_id] = (
            receiver, dispatcher, heartbeat, watchdog,
        )
        self._queue_metrics_row(connection_id)
        return receiver, dispatcher, heartbeat, watchdog

    async def _monitor_connection(self, ws, public: bool, generation: int, *, prestarted=None):
        domain = self._domain(public)
        if prestarted is None:
            receiver, dispatcher, heartbeat, watchdog = self._start_connection_tasks(
                ws, public=public, generation=generation,
            )
        else:
            if len(prestarted) == 3:
                receiver, heartbeat, watchdog = prestarted
                dispatcher = None
            else:
                receiver, dispatcher, heartbeat, watchdog = prestarted
        while self._running:
            notice_wait = self._create_owned_task(
                self._notice_events[domain].wait(), name=f"ws-notice:{domain}",
            )
            fault_wait = self._create_owned_task(
                self._fault_events[domain].wait(), name=f"ws-fault:{domain}",
            )
            owned_connection_tasks = {receiver, heartbeat, watchdog}
            if dispatcher is not None:
                owned_connection_tasks.add(dispatcher)
            tasks = {*owned_connection_tasks, notice_wait, fault_wait}
            done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            if fault_wait in done and self._fault_events[domain].is_set() and self._running:
                for task in pending:
                    task.cancel()
                for task in (*pending, *done):
                    if task is fault_wait:
                        continue
                    try:
                        await task
                    except BaseException:
                        pass
                return None
            if notice_wait in done and self._notice_events[domain].is_set() and self._running:
                previous_health = self._health_state[domain]
                previous_coins = tuple(self._coins)
                self._set_health(public, "HANDOVER_PENDING")
                replacement = await self._open_replacement(
                    url_for_public=public, old_generation=generation,
                    previous_health=previous_health,
                )
                if replacement is None:
                    self._notice_events[domain].clear()
                    self._set_health(public, previous_health)
                    if tuple(self._coins) != previous_coins:
                        self._schedule_handover_retry(domain)
                    continue
                for task in (*owned_connection_tasks, notice_wait, fault_wait):
                    if task is not notice_wait and not task.done():
                        task.cancel()
                for task in (*owned_connection_tasks, notice_wait, fault_wait):
                    if not task.done():
                        try:
                            await task
                        except BaseException:
                            pass
                return replacement
            for task in pending:
                task.cancel()
            for task in pending:
                try:
                    await task
                except BaseException:
                    pass
            for task in done:
                if task in {notice_wait, fault_wait}:
                    continue
                exception = task.exception()
                if exception:
                    raise exception
                if task in owned_connection_tasks and self._running:
                    self._report_connection_fault(
                        public, generation,
                        f"{domain} {task.get_name()} ended without a connection fault",
                    )
            return None
        return None

    def _schedule_handover_retry(self, domain: str, delay: float = 1.0) -> None:
        existing = self._retry_handles.get(domain)
        if existing is not None:
            existing.cancel()
        loop = asyncio.get_running_loop()
        self._retry_handles[domain] = loop.call_later(
            max(float(delay), 0.1), self._notice_events[domain].set,
        )

    def _capture_domain_state(self, domain: str, previous_health: str | None = None) -> dict:
        """Capture immutable diagnostics only; active authority is never restored by copy."""
        return {
            "generation": int(self._active_generations[domain]),
            "health": previous_health or self._health_state[domain],
        }

    def _restore_domain_state(self, domain: str, snapshot: dict) -> None:
        """Retire only the failed pending generation; preserve active ownership intact."""
        pending = self._pending_connections.get(domain) or {}
        pending_generation = int(pending.get("generation", 0) or 0)
        profile = self._pending_subscriptions.get(domain)
        if profile and int(profile.get("generation", 0) or 0) == pending_generation:
            self._pending_subscriptions[domain] = None
        if pending_generation:
            self._generation_heartbeat.get(domain, {}).pop(pending_generation, None)
        self._pending_connections[domain] = None

    def _pending_transport_viability_error(
        self, public: bool, generation: int,
    ) -> str | None:
        """Return why a pending replacement cannot safely become authoritative."""
        domain = self._domain(public)
        pending = self._pending_connections.get(domain) or {}
        profile = self._pending_subscription_profile(domain, generation)
        if profile is None or profile["fault_event"].is_set():
            return "pending_fault_event"
        socket_id = int(pending.get("socket_id", 0) or 0)
        if socket_id <= 0:
            return "missing_pending_socket_id"
        state = self._generation_heartbeat.get(domain, {}).get(int(generation))
        if state is None or int(state.get("socket_id", 0) or 0) != socket_id:
            return "pending_heartbeat_identity_mismatch"
        if self._owned_sockets.get(socket_id) is None:
            return "pending_socket_not_owned"
        connection_id = (domain, int(generation), socket_id)
        if connection_id not in self._dispatch_queues:
            return "pending_dispatch_queue_missing"
        tasks = self._connection_tasks.get(connection_id)
        if tasks is None or len(tasks) < 3:
            return "pending_connection_tasks_missing"
        for role, task in zip(("receiver", "dispatcher", "heartbeat"), tasks[:3]):
            if (
                task is None
                or task.done()
                or task.cancelled()
                or bool(getattr(task, "cancelling", lambda: 0)())
            ):
                return f"pending_{role}_not_running"
        return None

    def _retire_active_incident_for_promotion(
        self, domain: str, previous_generation: int, successor_generation: int,
    ) -> None:
        """Terminally retire old authority's incident and release its work once."""
        incident = self._transport_incidents.get(domain)
        if not (
            incident
            and incident.get("active")
            and int(incident.get("generation", 0) or 0) == int(previous_generation)
        ):
            return
        deferred = set(incident.get("deferred_keys", set()))
        incident.update(
            active=False,
            recovery_status="SUPERSEDED",
            failure_reason="generation_superseded",
            completed_at=time.monotonic(),
            superseded_by_generation=int(successor_generation),
            reconcile_owner=None,
        )
        owner = self._transport_reconcile_owners.get(domain) or {}
        owner_task = owner.get("task")
        if (
            str(owner.get("incident_id", "")) == str(incident.get("incident_id", ""))
            and int(owner.get("generation", 0) or 0) == int(previous_generation)
        ):
            if owner_task is not None and not owner_task.done():
                owner_task.cancel()
            self._transport_reconcile_owners[domain] = None
            if self._transport_reconcile_tasks.get(domain) is owner_task:
                self._transport_reconcile_tasks.pop(domain, None)
        self._record_gap_event("transport_incident_superseded", "*", "1m")
        self._recheck_transport_deferred_keys(deferred, re_admit=True)

    def _promote_pending_connection(self, public: bool, generation: int) -> None:
        """Atomically make a viable, subscription-ready replacement authoritative."""
        domain = self._domain(public)
        pending = self._pending_connections.get(domain) or {}
        if int(pending.get("generation", 0) or 0) != int(generation):
            raise RuntimeError(f"no pending {domain} generation {generation} to promote")
        profile = self._pending_subscription_profile(domain, generation)
        if profile is None or not profile["ready_event"].is_set() or not self._subscription_ready(public, generation):
            raise RuntimeError(f"pending {domain} generation {generation} is not subscription-ready")
        viability_error = self._pending_transport_viability_error(public, generation)
        if viability_error is not None:
            self._report_connection_fault(public, generation, viability_error)
            raise RuntimeError(f"pending {domain} generation {generation} is not transport-viable: {viability_error}")
        state = self._generation_heartbeat.get(domain, {}).get(int(generation))
        if state is None:
            raise RuntimeError(f"missing heartbeat state for pending {domain} generation {generation}")
        self._subscription_expected = {
            key for key in self._subscription_expected if key[0] != domain
        } | set(profile["expected"])
        self._subscription_confirmed = {
            key for key in self._subscription_confirmed if key[0] != domain
        } | set(profile["confirmed"])
        self._subscription_state = {
            key: row for key, row in self._subscription_state.items() if key[0] != domain
        }
        self._subscription_state.update(copy.deepcopy(profile["state"]))
        self._subscription_batches = {
            request_id: metadata for request_id, metadata in self._subscription_batches.items()
            if not any(key[0] == domain for key in self._batch_keys(metadata))
        }
        self._subscription_batches.update(copy.deepcopy(profile["batches"]))
        self._subscription_operations[domain] = copy.deepcopy(profile["operations"])
        if profile["ready_event"].is_set():
            self._subscription_ready_events[domain].set()
        else:
            self._subscription_ready_events[domain].clear()
        if profile["notice_event"].is_set():
            self._notice_events[domain].set()
        else:
            self._notice_events[domain].clear()
        if profile["fault_event"].is_set():
            self._fault_events[domain].set()
            if profile.get("fault_reason") is not None:
                self._fault_reasons[domain] = copy.deepcopy(profile["fault_reason"])
        else:
            self._fault_events[domain].clear()
            self._fault_reasons.pop(domain, None)
        previous_generation = int(self._active_generations.get(domain, 0) or 0)
        self._heartbeat_state[domain] = state
        self._active_generations[domain] = int(generation)
        self._pending_connections[domain] = None
        self._pending_subscriptions[domain] = None
        self._retire_active_incident_for_promotion(
            domain, previous_generation, int(generation),
        )
        if public:
            self._public_connected = True
        else:
            self._connected = True
        if previous_generation and previous_generation != int(generation):
            self._transport_metrics[domain]["generation_changes"] += 1
        self._set_health(public, "HEALTHY")

    def _retire_generation(self, public: bool, generation: int) -> None:
        domain = self._domain(public)
        if int(self._active_generations.get(domain, 0) or 0) != int(generation):
            self._generation_heartbeat.get(domain, {}).pop(int(generation), None)

    async def _cancel_owned_tasks(self, tasks: list[asyncio.Task]) -> None:
        for task in tasks:
            if task and not task.done():
                task.cancel()
        for task in tasks:
            if task:
                try:
                    await task
                except BaseException:
                    pass

    async def _open_replacement(
        self, *, url_for_public: bool, old_generation: int,
        previous_health: str | None = None,
    ):
        domain = self._domain(url_for_public)
        url = OKX_PUBLIC_WS_URL if url_for_public else OKX_WS_URL
        new_ws = None
        tasks = []
        promoted = False
        previous = self._capture_domain_state(domain, previous_health)
        try:
            new_ws = await websockets.connect(url, ping_interval=None, open_timeout=10)  # type: ignore[union-attr]
            self._own_socket(new_ws)
            generation = self._next_generation(url_for_public, old_generation + 1)
            self._transport_observability_enabled = True
            self._reset_connection_domain(url_for_public, generation, activate=False)
            self._generation_heartbeat[domain][generation]["socket_id"] = id(new_ws)
            self._pending_connections[domain]["socket_id"] = id(new_ws)
            if url_for_public:
                await self._subscribe_public(new_ws, generation)
            else:
                await self._subscribe_candles(new_ws, generation)
            tasks = list(self._start_connection_tasks(
                new_ws, public=url_for_public, generation=generation, handover=True,
            ))
            pending_profile = self._pending_subscription_profile(domain, generation)
            if pending_profile is None:
                raise RuntimeError(f"missing pending {domain} subscription profile")
            await asyncio.wait_for(
                pending_profile["ready_event"].wait(), timeout=SUB_ACK_TIMEOUT,
            )
            self._promote_pending_connection(url_for_public, generation)
            promoted = True
            return new_ws, generation, tuple(tasks)
        except asyncio.CancelledError:
            raise
        except Exception:
            return None
        finally:
            if not promoted:
                await self._cancel_owned_tasks(tasks)
                if new_ws is not None:
                    await self._close_socket(new_ws)
                self._restore_domain_state(domain, previous)

    async def _receive_loop(
        self, ws, *, public: bool, generation: int | None = None, queue: asyncio.Queue | None = None,
        connection_id: tuple[str, int, int] | None = None,
    ) -> None:
        if generation is None:
            generation = self._active_generations[self._domain(public)]
        counter = 0
        async for raw in ws:
            if not self._running:
                return
            try:
                value = raw.decode() if isinstance(raw, bytes) else raw
                now = time.monotonic()
                state = self._generation_heartbeat.get(self._domain(public), {}).get(int(generation))
                if state is None:
                    current = self._heartbeat_state.get(self._domain(public), {})
                    if (
                        int(current.get("generation", 0) or 0) == int(generation)
                        or int(self._active_generations.get(self._domain(public), 0) or 0) == int(generation)
                    ):
                        current.setdefault("generation", int(generation))
                        state = current
                if state is None:
                    return
                state["last_rx_monotonic"] = now
                if value == "pong":
                    state["last_pong_monotonic"] = now
                    state["awaiting_pong"] = False
                    if int(self._active_generations.get(self._domain(public), 0) or 0) == int(generation):
                        self._heartbeat_metrics[self._domain(public)]["pongs"] += 1
                        if self._subscription_ready(public, generation):
                            self._set_health(public, "HEALTHY")
                        self._last_msg_time = now
                else:
                    domain = self._domain(public)
                    active_generation = int(self._active_generations.get(domain, 0) or 0)
                    if active_generation and active_generation != int(generation):
                        pending = self._pending_connections.get(domain) or {}
                        if int(pending.get("generation", 0) or 0) == int(generation):
                            self._handle_message(
                                value, public=public, generation=generation, control_only=True,
                            )
                            continue
                        metrics = self._transport_metrics[domain]
                        metrics["stale_generation_frames_dropped"] = int(
                            metrics.get("stale_generation_frames_dropped", 0)
                        ) + 1
                        continue
                    self._msg_count += 1
                    self._last_msg_time = now
                    self._observe_wire_frame(
                        value, public=public, generation=generation, received_at=now,
                        connection_id=connection_id,
                    )
                    if queue is None:
                        self._handle_message(value, public=public, generation=generation)
                    else:
                        try:
                            await asyncio.wait_for(
                                queue.put((value, now)), timeout=_DISPATCH_ENQUEUE_TIMEOUT_SEC,
                            )
                            self._update_queue_metrics(
                                domain, queue, connection_id,
                            )
                        except asyncio.TimeoutError:
                            metrics = self._transport_metrics[domain]
                            metrics["backpressure_faults"] += 1
                            metrics["frames_dropped"] += 1
                            self._record_gap_event("dispatch_backpressure_drop", "*", "transport")
                            self._schedule_backpressure_recovery(
                                value, public=public, generation=generation,
                                connection_id=connection_id,
                            )
                            self._report_connection_fault(public, generation, "dispatch_backpressure")
                            return
                counter += 1
                if counter >= _WS_YIELD_EVERY:
                    counter = 0
                    await asyncio.sleep(0)
            except Exception:
                self._error_count += 1
                logger.debug("WS message handling failed", exc_info=True)

    async def _dispatch_loop(
        self, queue: asyncio.Queue, *, public: bool, generation: int,
        connection_id: tuple[str, int, int] | None = None,
    ) -> None:
        """Apply frames outside the socket reader while preserving ownership."""
        domain = self._domain(public)
        while self._running:
            value, received_at = await queue.get()
            metrics = self._transport_metrics[domain]
            active_frame = False
            try:
                active_generation = int(self._active_generations.get(domain, 0) or 0)
                if active_generation and active_generation != int(generation):
                    metrics["stale_generation_frames_dropped"] = int(
                        metrics.get("stale_generation_frames_dropped", 0)
                    ) + 1
                    continue
                active_frame = True
                started = time.monotonic()
                self._record_queue_dequeue_latency(
                    domain, started - float(received_at), connection_id,
                )
                self._update_queue_metrics(domain, queue, connection_id)
                self._handle_message(value, public=public, generation=generation)
                self._record_frame_applied(
                    value, public=public, generation=generation,
                    received_at=received_at, applied_at=time.monotonic(),
                    connection_id=connection_id,
                )
            except asyncio.CancelledError:
                raise
            except Exception:
                metrics["apply_failures"] += 1
                self._record_frame_applied(
                    value, public=public, generation=generation,
                    received_at=received_at, applied_at=time.monotonic(), dropped=True,
                    connection_id=connection_id,
                )
                self._error_count += 1
                logger.error(
                    "WS frame apply failed domain=%s generation=%s",
                    domain, generation, exc_info=True,
                )
            finally:
                queue.task_done()
                if active_frame:
                    self._update_queue_metrics(domain, queue, connection_id)

    async def _heartbeat(self, ws, *, public: bool = False, generation: int | None = None) -> None:
        domain = self._domain(public)
        generation = generation or self._active_generations[domain]
        while self._running:
            state = self._generation_heartbeat.get(domain, {}).get(int(generation))
            if state is None:
                current = self._heartbeat_state.get(domain, {})
                if (
                    int(current.get("generation", 0) or 0) == int(generation)
                    or int(self._active_generations.get(domain, 0) or 0) == int(generation)
                ):
                    current.setdefault("generation", int(generation))
                    state = current
            if state is None:
                return
            is_primary = (
                int(self._active_generations.get(domain, 0) or 0) == int(generation)
                and self._heartbeat_state.get(domain) is state
            )
            if int(state.get("socket_id", 0) or 0) not in {0, id(ws)}:
                self._report_connection_fault(
                    public, generation,
                    f"{domain} heartbeat socket identity mismatch for generation {generation}",
                )
                return
            now = time.monotonic()
            if state.get("awaiting_pong"):
                if now - float(state.get("ping_sent_at", now)) >= PONG_DEADLINE:
                    if is_primary:
                        self._heartbeat_metrics[domain]["pong_timeouts"] += 1
                    self._report_connection_fault(
                        public, generation,
                        f"{domain} pong deadline exceeded for generation {generation}",
                    )
                    return
                await asyncio.sleep(min(0.5, PONG_DEADLINE / 4))
                continue
            quiet_for = now - float(state.get("last_rx_monotonic", now))
            if quiet_for < PING_INTERVAL:
                await asyncio.sleep(min(0.5, max(PING_INTERVAL - quiet_for, 0.05)))
                continue
            try:
                await ws.send("ping")
                sent = time.monotonic()
                state["ping_sent_at"] = sent
                state["awaiting_pong"] = True
                if is_primary:
                    self._heartbeat_metrics[domain]["pings"] += 1
                    self._set_health(public, "PING_PENDING")
            except Exception:
                self._report_connection_fault(
                    public, generation, f"{domain} heartbeat send failed",
                )
                return

    def _subscription_args(self) -> list[dict]:
        import market_mode as mm
        return [
            {"channel": channel, "instId": mm.inst_id(coin)}
            for timeframe, channel in _CANDLE_CHANNELS.items()
            for coin in self._coins
        ]

    def _next_subscription_id(self) -> str:
        self._subscription_sequence += 1
        return validate_ws_request_id(uuid.uuid4().hex)

    def _consume_subscription_operation(
        self, public: bool, generation: int | None = None,
    ) -> bool:
        domain = self._domain(public)
        generation = int(generation if generation is not None else self._active_generations[domain])
        profile = self._pending_subscription_profile(domain, generation)
        state = profile["operations"] if profile is not None else self._subscription_operations[domain]
        now = time.monotonic()
        if now - float(state.get("window_start", now)) >= 3600:
            state.update(count=0, window_start=now)
        if int(state.get("count", 0)) >= SUB_OPERATION_LIMIT:
            return False
        state["count"] = int(state.get("count", 0)) + 1
        return True

    def _split_subscription_batches(self, args: list[dict]) -> list[list[dict]]:
        batches: list[list[dict]] = []
        current: list[dict] = []
        for arg in args:
            candidate = current + [arg]
            payload = json.dumps({"id": "x", "op": "subscribe", "args": candidate}, separators=(",", ":"))
            if current and (len(candidate) > MAX_SUB_ARGS or len(payload.encode("utf-8")) > MAX_SUB_BYTES):
                batches.append(current)
                current = [arg]
            else:
                current = candidate
        if current:
            batches.append(current)
        return batches

    def _set_subscription_state(self, key: tuple[str, str, str], status: str, **updates) -> None:
        domain = key[0]
        generation = int(updates.get("generation", self._active_generations.get(domain, 0)) or 0)
        profile = self._pending_subscription_profile(domain, generation)
        if profile is not None:
            profile["expected"].add(key)
            row = profile["state"].setdefault(key, {"status": "INACTIVE", "expected": True})
        else:
            row = self._subscription_state.setdefault(key, {"status": "INACTIVE", "expected": True})
        row.update(status=status, **updates)

    async def _send_subscriptions(self, ws, args: list[dict], *, public: bool, generation: int | None = None) -> None:
        domain = self._domain(public)
        generation = int(generation if generation is not None else self._active_generations[domain])
        profile = self._pending_subscription_profile(domain, generation)
        states = profile["state"] if profile is not None else self._subscription_state
        batches = profile["batches"] if profile is not None else self._subscription_batches
        async with self._subscription_send_locks[domain]:
            for batch in self._split_subscription_batches(args):
                request_id = self._next_subscription_id()
                keys = {self._subscription_key(arg, public) for arg in batch}
                try:
                    validate_ws_request_id(request_id)
                except WSRequestEnvelopeError as exc:
                    self._invalid_request_id_count += 1
                    logger.error("WS_REQUEST_ID_INVALID_LOCAL length=%s", len(str(request_id)))
                    for key in keys:
                        self._set_subscription_state(
                            key, "FAILED", generation=generation,
                            error_code="WS_REQUEST_ID_INVALID_LOCAL",
                            error_message=str(exc), error_class=_REQUEST_PROTOCOL_REJECTED,
                            retry_allowed=False,
                        )
                    continue
                if not self._consume_subscription_operation(public, generation):
                    for key in keys:
                        self._set_subscription_state(
                            key, "RETRY_WAIT", generation=generation,
                            error_code="subscription_operation_budget",
                            next_retry_at=time.monotonic() + 60,
                        )
                    continue
                payload = json.dumps(
                    {"id": request_id, "op": "subscribe", "args": batch},
                    separators=(",", ":"),
                )
                for key in keys:
                    self._set_subscription_state(
                        key, "REQUESTED", generation=generation, request_id=request_id,
                        last_attempt=time.monotonic(), error_code="", error_message="",
                        error_class="", retry_allowed=True,
                    )
                batches[request_id] = {
                    "domain": domain,
                    "generation": generation,
                    "operation": "subscribe",
                    "expected_keys": set(keys),
                    "attempt": max(
                        int(states.get(key, {}).get("retry_count", 0) or 0)
                        for key in keys
                    ) if keys else 0,
                    "sent_at": time.monotonic(),
                }
                await ws.send(payload)
                await asyncio.sleep(0)

    async def _subscribe_candles(self, ws, generation: int | None = None) -> None:
        generation = int(generation if generation is not None else self._active_generations["business"])
        args = self._subscription_args()
        profile = self._pending_subscription_profile("business", generation)
        expected = {("business", str(a["channel"]), str(a["instId"])) for a in args}
        if profile is not None:
            profile["expected"] = expected
            profile["confirmed"].clear()
            profile["ready_event"].clear()
        else:
            self._subscription_expected = {
                key for key in self._subscription_expected if key[0] != "business"
            } | expected
            self._subscription_confirmed = {
                key for key in self._subscription_confirmed if key[0] != "business"
            }
            self._subscription_ready_events["business"].clear()
        await self._send_subscriptions(ws, args, public=False, generation=generation)

    async def _subscribe_public(self, ws, generation: int | None = None) -> None:
        generation = int(generation if generation is not None else self._active_generations["public"])
        import market_mode as mm
        args = [{"channel": "tickers", "instId": mm.inst_id(coin)} for coin in self._coins]
        profile = self._pending_subscription_profile("public", generation)
        expected = {("public", "tickers", str(a["instId"])) for a in args}
        expected.add(("public", "instruments", "SWAP"))
        if profile is not None:
            profile["expected"].update(expected)
            profile["ready_event"].clear()
        else:
            self._subscription_expected.update(expected)
            self._subscription_ready_events["public"].clear()
        await self._send_subscriptions(ws, args, public=True, generation=generation)
        await self._send_subscriptions(
            ws, [{"channel": "instruments", "instType": "SWAP"}], public=True, generation=generation,
        )

    def _subscription_args_for_keys(self, keys: list[tuple[str, str, str]]) -> list[dict]:
        return [
            ({"channel": channel, "instId": instrument} if channel != "instruments"
             else {"channel": channel, "instType": instrument})
            for _domain, channel, instrument in keys
        ]

    async def _subscription_watchdog(self, ws, *, public: bool, generation: int) -> None:
        domain = self._domain(public)
        while self._running:
            await asyncio.sleep(SUB_WATCHDOG_INTERVAL)
            profile = self._pending_subscription_profile(domain, generation)
            states = profile["state"] if profile is not None else self._subscription_state
            now = time.monotonic()
            due: list[tuple[str, str, str]] = []
            for key, row in list(states.items()):
                if key[0] != domain or int(row.get("generation", 0) or 0) != int(generation):
                    continue
                status = row.get("status")
                if status == "REQUESTED" and now - float(row.get("last_attempt", now)) >= SUB_ACK_TIMEOUT:
                    retry_count = int(row.get("retry_count", 0)) + 1
                    self._set_subscription_state(
                        key, "RETRY_WAIT", generation=generation, retry_count=retry_count,
                        next_retry_at=now + min(60.0, 2.0 ** min(max(retry_count - 1, 0), 5)) + random.uniform(0.0, 0.5),
                    )
                elif (
                    status in {"FAILED", "RETRY_WAIT", "RECOVERING"}
                    and row.get("retry_allowed", True)
                    and now >= float(row.get("next_retry_at", 0.0))
                ):
                    self._set_subscription_state(key, "RECOVERING", generation=generation)
                    due.append(key)
            if due:
                await self._send_subscriptions(
                    ws, self._subscription_args_for_keys(due), public=public, generation=generation,
                )

    def _handle_message(
        self, raw: str, *, public: bool = False, generation: int | None = None,
        control_only: bool = False,
    ) -> None:
        domain = self._domain(public)
        generation = generation or self._active_generations[domain]
        msg = json.loads(raw) if isinstance(raw, str) else dict(raw)
        event = str(msg.get("event", "") or "").lower()
        if event:
            self._event_count += 1
            arg = msg.get("arg") or {}
            key = self._subscription_key(arg, public)
            profile = self._pending_subscription_profile(domain, generation)
            expected = profile["expected"] if profile is not None else self._subscription_expected
            states = profile["state"] if profile is not None else self._subscription_state
            batches = profile["batches"] if profile is not None else self._subscription_batches
            confirmed = profile["confirmed"] if profile is not None else self._subscription_confirmed
            request_id = str(msg.get("id", "") or "")
            batch = batches.get(request_id, {}) if request_id else {}
            targets = {key} if key in expected else self._batch_keys(batch)
            if event == "subscribe":
                for target in targets:
                    current = states.get(target, {})
                    if current and int(current.get("generation", generation) or generation) != generation:
                        continue
                    confirmed.add(target)
                    self._set_subscription_state(target, "ACTIVE", generation=generation, last_success=time.monotonic())
                self._refresh_subscription_ready_event(public, generation)
            elif event == "error":
                self._error_count += 1
                code = str(msg.get("code", ""))
                protocol_rejected = code == "60033"
                if protocol_rejected:
                    self._protocol_rejection_count += 1
                for target in targets:
                    current = states.get(target, {})
                    if current and int(current.get("generation", generation) or generation) != generation:
                        continue
                    self._set_subscription_state(
                        target, "FAILED", generation=generation,
                        error_code=code, error_message=str(msg.get("msg", "")),
                        error_class=(_REQUEST_PROTOCOL_REJECTED if protocol_rejected else _TRANSIENT_SUBSCRIPTION_ERROR),
                        retry_allowed=not protocol_rejected,
                        next_retry_at=(0.0 if protocol_rejected else time.monotonic() + 1.0),
                    )
                logger.error(
                    "WS subscription error code=%s class=%s msg=%s arg=%s request_id=%s",
                    code,
                    _REQUEST_PROTOCOL_REJECTED if protocol_rejected else _TRANSIENT_SUBSCRIPTION_ERROR,
                    msg.get("msg", ""),
                    arg,
                    request_id or "<absent>",
                )
            elif (
                not control_only
                and event == "notice"
                and str(msg.get("code", "")) == "64008"
            ):
                if profile is not None:
                    profile["notice_event"].set()
                else:
                    self._notice_events[domain].set()
                    self._set_health(public, "HANDOVER_PENDING")
            return
        if control_only:
            return
        data = msg.get("data") or []
        if not data:
            return
        self._data_count += 1
        arg = msg.get("arg") or {}
        channel = str(arg.get("channel", ""))
        if public and channel == "tickers":
            for row in data:
                self._upsert_ticker(arg.get("instId", ""), row, generation)
            return
        if public and channel == "instruments":
            self._apply_instrument_updates(data)
            return
        timeframe = _channel_timeframe(channel)
        if timeframe:
            for candle in data:
                self._upsert_candle(str(arg.get("instId", "")), timeframe, candle, generation=generation)

    def _upsert_ticker(self, inst_id: str, raw: list | dict, generation: int | None = None) -> None:
        try:
            from okx_client import update_ticker_from_ws
            update_ticker_from_ws(inst_id, raw, generation or self._connection_generation)
        except Exception:
            logger.debug("WS ticker publication failed", exc_info=True)

    def _apply_instrument_updates(self, data: list) -> None:
        try:
            from instrument_registry import registry
            for row in data:
                if isinstance(row, dict):
                    registry.apply_ws_update(row)
        except Exception:
            logger.debug("WS instrument update publication failed", exc_info=True)

    def _upsert_candle(
        self, inst_id: str, timeframe: str, candle: list, *, generation: int | None = None,
    ) -> None:
        if len(candle) < 6:
            return
        try:
            timestamp = float(candle[0]) / 1000.0 if float(candle[0]) > 10_000_000_000 else float(candle[0])
            values = (float(candle[1]), float(candle[2]), float(candle[3]), float(candle[4]), float(candle[5]))
            confirmed = str(candle[8]) == "1" if len(candle) > 8 else False
        except (TypeError, ValueError, IndexError):
            return
        coin = _coin_from_inst(inst_id)
        if not coin:
            return
        generation = int(generation or self._active_generations["business"] or self._connection_generation)
        cache = _cache_for_timeframe(timeframe)
        row = cache.setdefault(coin, {})
        if int(row.get("connection_generation", 0) or 0) > generation:
            return
        timestamps = row.setdefault("timestamps", [])
        keys = ("opens", "highs", "lows", "closes", "volumes")
        for key in keys:
            row.setdefault(key, [])
        confirmations = row.setdefault("confirmed", [True] * len(timestamps))
        if len(confirmations) < len(timestamps):
            confirmations.extend([True] * (len(timestamps) - len(confirmations)))
        existing_index = next((i for i, value in enumerate(timestamps) if float(value) == timestamp), None)
        previous_latest = float(timestamps[-1]) if timestamps else 0.0
        if existing_index is None:
            if previous_latest and timestamp > previous_latest:
                self._boundary_events_seen[timeframe] = self._boundary_events_seen.get(timeframe, 0) + 1
                if abs(timestamp - previous_latest - _TIMEFRAME_SECONDS[timeframe]) <= 0.5:
                    self._new_adjacent_transitions[timeframe] = self._new_adjacent_transitions.get(timeframe, 0) + 1
            if timestamp < previous_latest:
                self._record_gap_decision(
                    timeframe=timeframe, prev_ts=previous_latest, new_ts=timestamp,
                    prev_confirm=True, new_confirm=confirmed, source="WS",
                    decision="OUT_OF_ORDER",
                )
                insert_at = next((i for i, value in enumerate(timestamps) if timestamp < float(value)), len(timestamps))
                timestamps.insert(insert_at, timestamp)
                for key, value in zip(keys, values):
                    row[key].insert(insert_at, value)
                confirmations.insert(insert_at, confirmed)
            else:
                timestamps.append(timestamp)
                for key, value in zip(keys, values):
                    row[key].append(value)
                confirmations.append(confirmed)
        else:
            self._record_gap_decision(
                timeframe=timeframe, prev_ts=timestamp, new_ts=timestamp,
                prev_confirm=confirmations[existing_index], new_confirm=confirmed,
                source="WS", decision="DUPLICATE",
            )
            for key, value in zip(keys, values):
                row[key][existing_index] = value
            if confirmed and not confirmations[existing_index]:
                confirmations[existing_index] = True
        for key in (*keys, "timestamps", "confirmed"):
            if len(row[key]) > _HISTORY_LIMIT:
                del row[key][:-_HISTORY_LIMIT]
        latest_confirmed = max((float(ts) for ts, done in zip(timestamps, confirmations) if done), default=0.0)
        if latest_confirmed > float(row.get("confirmed_latest_timestamp", 0.0) or 0.0):
            row["version"] = int(row.get("version", 0) or 0) + 1
        now = time.time()
        row.update({
            "observed_at": now,
            "received_at": now,
            "last_update_received_at": now,
            "exchange_observed_at": max(float(row.get("exchange_observed_at", 0.0) or 0.0), timestamp),
            "last_exchange_payload_ts": timestamp,
            "request_limit": len(row["closes"]),
            "request_key": f"{coin}|{timeframe}|{_CANDLE_CHANNELS[timeframe]}|candles|{len(row['closes'])}",
            "timeframe": timeframe,
            "bar": _CANDLE_CHANNELS[timeframe].replace("candle", ""),
            "data_type": "candles",
            "source": "okx_public_websocket",
            "refresh_status": "fresh",
            "refresh_error": "",
            "stale": False,
            "connection_generation": generation,
            "confirmed_latest_timestamp": latest_confirmed,
            "latest_confirmed_bar_open": latest_confirmed,
            "confirmation_received_at": now if confirmed else row.get("confirmation_received_at", 0.0),
            "ready": bool(row.get("ready", False) or len([done for done in confirmations if done]) >= _REQUIRED_HISTORY[timeframe]),
        })
        self._last_candle_epoch = max(self._last_candle_epoch, timestamp)
        if confirmed:
            self._last_confirmed_candle_epoch = max(self._last_confirmed_candle_epoch, timestamp)
            pending = self._gap_grace_pending.get((coin, timeframe))
            if (
                pending
                and pending.get("recovery_kind") == _RECOVERY_KIND_CONFIRMATION
                and abs(float(pending.get("target_ts", timestamp)) - timestamp) <= 0.5
            ):
                # A target confirmation is authoritative for its exact
                # boundary and must resolve the matching grace state before
                # continuity performs any further recovery decision.
                self._clear_confirmation_grace((coin, timeframe), resolved=True)
        self._check_continuity(coin, timeframe, row, trigger_type="WS_TIMESTAMP_GAP")
        try:
            import market_data_stream
            row["confirmed_history_complete"] = market_data_stream.confirmed_history_complete(
                row, timeframe, _REQUIRED_HISTORY[timeframe], now=time.time(),
            )
        except Exception:
            row["confirmed_history_complete"] = False
        if timeframe == "1m":
            try:
                import candidate_expectancy_telemetry as counterfactual
                counterfactual.observe_price(coin, values[3], high=values[1], low=values[2], timestamp_epoch=timestamp)
            except Exception:
                logger.debug("Counterfactual WS observation failed", exc_info=True)

    def _gap_grace_seconds(self, timeframe: str) -> float:
        return min(_GAP_GRACE_MAX_SEC, max(0.5, _TIMEFRAME_SECONDS[timeframe] * 0.05))

    @staticmethod
    def _target_forming_relation(
        current_forming: float | None, timeframe: str, target: float,
    ) -> str:
        if current_forming is None:
            return "NO_CURRENT_FORMING"
        if abs(float(current_forming) - float(target)) <= 0.5:
            return "CURRENT_FORMING"
        step = _TIMEFRAME_SECONDS[timeframe]
        if abs(float(current_forming) - float(target) - step) <= 0.5:
            return "PRIOR_BOUNDARY"
        return "OTHER_FORMING_BOUNDARY"

    def _record_gap_event(self, event: str, coin: str, timeframe: str) -> None:
        self._gap_event_counts[event] = int(self._gap_event_counts.get(event, 0)) + 1
        by_timeframe = self._gap_event_by_timeframe.setdefault(timeframe, {})
        by_timeframe[event] = int(by_timeframe.get(event, 0)) + 1

    def _record_gap_decision(
        self, *, timeframe: str, prev_ts: float, new_ts: float,
        prev_confirm: bool, new_confirm: bool, source: str, decision: str,
        missing_count: int = 0, missing_start: float | None = None,
        missing_end: float | None = None,
    ) -> None:
        decision = str(decision or "OTHER")
        self._continuity_pair_checks[timeframe] = self._continuity_pair_checks.get(timeframe, 0) + 1
        if decision in self._gap_decision_counts:
            self._gap_decision_counts[decision] += 1
        if decision == "ADJACENT" or len(self._gap_decision_samples) >= 128:
            return
        step = _TIMEFRAME_SECONDS[timeframe]
        self._gap_decision_samples.append({
            "timeframe": timeframe,
            "prev_ts": float(prev_ts),
            "new_ts": float(new_ts),
            "delta_ms": round((float(new_ts) - float(prev_ts)) * 1000.0, 3),
            "interval_ms": int(step * 1000),
            "computed_missing_count": int(missing_count),
            "missing_start": missing_start,
            "missing_end": missing_end,
            "prev_confirm": bool(prev_confirm),
            "new_confirm": bool(new_confirm),
            "source": str(source),
            "decision": decision,
        })

    def _schedule_gap_repair(self, key: tuple[str, str]) -> None:
        if not self._running or key in self._repair_tasks:
            return
        if key in self._pending_repairs:
            return
        if len(self._repair_tasks) >= _MAX_PENDING_REPAIRS:
            if len(self._pending_repairs) < _MAX_DEFERRED_REPAIRS:
                self._pending_repairs[key] = {
                    "next_retry_at": time.monotonic(), "attempt": 0,
                }
                self._repair_deferred += 1
                self._record_gap_event("repair_deferred", *key)
            return
        coin, timeframe = key
        try:
            task = self._create_owned_task(
                self._repair_gap(coin, timeframe),
                name=f"ws-gap-repair:{coin}:{timeframe}",
            )
            self._repair_tasks[key] = task
            self._repair_scheduled += 1
            self._record_gap_event("repair_scheduled", *key)
            task.add_done_callback(self._repair_done_callback)
        except RuntimeError:
            if len(self._pending_repairs) < _MAX_DEFERRED_REPAIRS:
                self._pending_repairs[key] = {
                    "next_retry_at": time.monotonic() + 1.0, "attempt": 0,
                }
                self._repair_deferred += 1
                self._record_gap_event("repair_deferred", *key)

    def _record_repair_failure(self, reason: str) -> None:
        reason = str(reason or "OTHER")
        if reason not in self._repair_failure_reasons:
            reason = "OTHER"
        self._repair_failure_reasons[reason] += 1

    def _schedule_confirmation_grace(
        self, key: tuple[str, str], target: float, *, now: float,
    ) -> None:
        """Own one confirmation ambiguity per key and boundary."""
        existing = self._gap_grace_pending.get(key)
        if (
            existing
            and existing.get("recovery_kind") == _RECOVERY_KIND_CONFIRMATION
            and abs(float(existing.get("target_ts", target)) - float(target)) <= 0.5
        ):
            return
        coin, timeframe = key
        step = _TIMEFRAME_SECONDS[timeframe]
        due_at = float(target) + step + self._gap_grace_seconds(timeframe)
        self._gap_grace_pending[key] = {
            "start": float(target), "end": float(target),
            "target_ts": float(target), "kind": "confirmation",
            "recovery_kind": _RECOVERY_KIND_CONFIRMATION,
            "due_at": time.monotonic() + max(due_at - float(now), 0.0),
        }
        self._confirmation_states_created += 1
        self._record_gap_event("confirmation_state_created", coin, timeframe)

    def _clear_confirmation_grace(
        self, key: tuple[str, str], *, resolved: bool = False,
    ) -> None:
        state = self._gap_grace_pending.pop(key, None)
        if (
            resolved
            and state
            and state.get("recovery_kind") == _RECOVERY_KIND_CONFIRMATION
            and not state.get("expired", False)
        ):
            self._confirmation_resolved_inside_grace += 1
            self._record_gap_event("confirmation_resolved_inside_grace", *key)

    def _recheck_confirmation_boundary(
        self, coin: str, timeframe: str, state: dict,
    ) -> None:
        """Recheck the boundary before selecting a recovery kind or REST."""
        import market_data_stream
        key = (coin, timeframe)
        row = _cache_for_timeframe(timeframe).get(coin, {})
        target = float(state.get("target_ts", state.get("start", 0.0)) or 0.0)
        status = market_data_stream.candle_range_status(
            row, timeframe, target, target,
        )
        self._confirmation_rechecks += 1
        if not status["missing"] and not status["unconfirmed"]:
            self._record_gap_event("confirmation_recheck_already_confirmed", coin, timeframe)
            return
        if status["unconfirmed"]:
            self._mark_gap(
                coin, timeframe, target, target,
                trigger_type="CONFIRMATION_GRACE_EXPIRED",
                recovery_kind=_RECOVERY_KIND_CONFIRMATION,
            )
            return
        # A target absent after the grace recheck is a real timestamp gap;
        # it is no longer treated as a confirmation-only condition.
        self._mark_gap(
            coin, timeframe, target, target,
            trigger_type="WS_TIMESTAMP_GAP",
            recovery_kind=_RECOVERY_KIND_MISSING,
        )

    def _check_continuity(
        self, coin: str, timeframe: str, row: dict, *, now: float | None = None,
        trigger_type: str = "RECONNECT_CONTINUITY_GAP",
    ) -> None:
        now = float(now if now is not None else time.time())
        key = (coin, timeframe)
        import market_data_stream
        points = market_data_stream.canonical_candle_points(row)
        tail = market_data_stream.required_tail_analysis(
            row, timeframe, _REQUIRED_HISTORY[timeframe], now=now,
        )
        closed_count = sum(1 for _, done in points if done)
        if not points or closed_count < _REQUIRED_HISTORY[timeframe]:
            # Bootstrap owns insufficient or not-yet-current history.  A gap
            # is actionable only inside the newest required consumer tail.
            return
        step = _TIMEFRAME_SECONDS[timeframe]
        tail_start = float(tail["required_tail_start"])
        tail_end = float(tail["required_tail_end"])
        latest_point = float(points[-1][0])
        continuity_points = [
            (timestamp, done) for timestamp, done in points
            if tail_start - step - 0.5 <= timestamp <= max(tail_end, latest_point) + 0.5
        ]
        gap = None
        for (previous, previous_done), (current, current_done) in zip(continuity_points, continuity_points[1:]):
            delta = float(current - previous)
            steps = delta / step if step else 0.0
            if steps <= 0.0:
                self._record_gap_decision(
                    timeframe=timeframe, prev_ts=previous, new_ts=current,
                    prev_confirm=previous_done, new_confirm=current_done,
                    source="CANONICAL_HISTORY", decision="OUT_OF_ORDER",
                )
                continue
            if steps <= 1.0 + 0.001:
                self._record_gap_decision(
                    timeframe=timeframe, prev_ts=previous, new_ts=current,
                    prev_confirm=previous_done, new_confirm=current_done,
                    source="CANONICAL_HISTORY", decision="ADJACENT",
                )
                continue
            missing_count = max(int(round(steps)) - 1, 1)
            missing_start = previous + step
            missing_end = current - step
            required_start = max(missing_start, tail_start)
            required_end = min(missing_end, tail_end)
            self._record_gap_decision(
                timeframe=timeframe, prev_ts=previous, new_ts=current,
                prev_confirm=previous_done, new_confirm=current_done,
                source="CANONICAL_HISTORY", decision="GAP",
                missing_count=missing_count, missing_start=missing_start,
                missing_end=missing_end,
            )
            if (
                math.isfinite(required_start) and math.isfinite(required_end)
                and required_start <= required_end
            ):
                gap = (required_start, required_end)
                break

        if gap is not None:
            missing_start, missing_end = gap
            latest_current, latest_confirmed = continuity_points[-1]
            boundary_ambiguity = bool(
                not latest_confirmed
                and abs(missing_start - missing_end) <= 0.5
                and abs(latest_current - missing_end - step) <= 0.5
            )
            if boundary_ambiguity:
                self._schedule_confirmation_grace(key, missing_start, now=now)
                return
            if now - float(missing_start) < self._gap_grace_seconds(timeframe):
                self._gap_grace_pending[key] = {
                    "start": missing_start, "end": missing_end,
                    "kind": "timestamp",
                    "recovery_kind": _RECOVERY_KIND_MISSING,
                    "due_at": time.monotonic() + self._gap_grace_seconds(timeframe),
                }
                return
            self._gap_grace_pending.pop(key, None)
            self._mark_gap(
                coin, timeframe, missing_start, missing_end,
                trigger_type=trigger_type, recovery_kind=_RECOVERY_KIND_MISSING,
            )
            return

        # A timestamp that exists but is still forming is a confirmation issue,
        # not a missing timestamp. Only a later adjacent bar can make the
        # prior unconfirmed bar eligible for bounded confirmation grace.
        confirmation_gap = None
        for (previous, previous_done), (current, _) in zip(continuity_points, continuity_points[1:]):
            if (
                tail_start - 0.5 <= previous <= tail_end + 0.5
                and not previous_done
                and abs((current - previous) - step) <= 0.5
            ):
                confirmation_gap = previous
                break
        if confirmation_gap is not None:
            self._schedule_confirmation_grace(key, confirmation_gap, now=now)
            return

        self._clear_confirmation_grace(key, resolved=True)
        if self._gap_state.get(key, {}).get("active"):
            self._gap_state[key] = {"active": False, "last_repaired_at": now}
            self._record_gap_event("gap_cleared", coin, timeframe)
            if row:
                row["stale"] = False
                row["refresh_status"] = "fresh"
                row["refresh_error"] = ""

    def _transport_incident_active(self, domain: str) -> bool:
        incident = self._transport_incidents.get(domain)
        return bool(incident and incident.get("active"))

    @staticmethod
    def _incident_implicates_key(incident: dict, key: tuple[str, str]) -> bool:
        """Return whether this incident's own evidence covers a gap key."""
        if str(incident.get("evidence_kind", "")) == "CONNECTION_GENERATION":
            return True
        return key in set(incident.get("implicated_keys", set()))

    def _admit_gap_for_repair(self, key: tuple[str, str]) -> None:
        """Classify one active, unowned gap before admitting recovery."""
        if not self._gap_state.get(key, {}).get("active"):
            return
        if key in self._repair_tasks or key in self._pending_repairs:
            return
        domain = "business" if key[1] == "1m" else None
        if not self._transport_observability_enabled or domain is None:
            self._schedule_gap_repair(key)
            return
        incident = self._transport_incidents.get(domain)
        if incident and incident.get("active"):
            if self._incident_implicates_key(incident, key):
                incident.setdefault("deferred_keys", set()).add(key)
                self._transport_metrics[domain]["transport_gap_deferred"] += 1
                self._record_gap_event("transport_gap_deferred", *key)
            else:
                self._schedule_gap_repair(key)
            return
        candidates = self._transport_gap_candidates[domain]
        candidates.setdefault(key, {"observed_at": time.monotonic()})
        task = self._transport_admission_tasks.get(domain)
        if task is None or task.done():
            self._transport_admission_tasks[domain] = self._create_owned_task(
                self._release_transport_gap_candidates(domain),
                name="ws-transport-gap-admission:business",
            )

    async def _release_transport_gap_candidates(self, domain: str) -> None:
        await asyncio.sleep(_TRANSPORT_GAP_ADMISSION_SEC)
        candidates = self._transport_gap_candidates.get(domain, {})
        self._transport_gap_candidates[domain] = {}
        active_candidates = {
            key: state for key, state in candidates.items()
            if self._gap_state.get(key, {}).get("active")
            and key not in self._repair_tasks
            and key not in self._pending_repairs
        }
        if not active_candidates:
            return
        generation = int(self._active_generations.get(domain, 0) or 0)
        incident = self._transport_incidents.get(domain)
        if (
            incident
            and incident.get("active")
            and int(incident.get("generation", 0) or 0) == generation
        ):
            for key in active_candidates:
                if self._incident_implicates_key(incident, key):
                    incident.setdefault("deferred_keys", set()).add(key)
                    self._transport_metrics[domain]["transport_gap_deferred"] += 1
                    self._record_gap_event("transport_gap_deferred", *key)
                else:
                    # Preserve the established unrelated-gap policy: an
                    # incumbent incident cannot absorb an unrelated key.
                    self._admit_gap_for_repair(key)
            return
        now = time.monotonic()
        cohort_window = _TRANSPORT_GAP_ADMISSION_SEC + _TRANSPORT_RECONCILE_SETTLE_SEC
        implicated = {
            key for key, state in active_candidates.items()
            if now - float(state.get("observed_at", now) or now) <= cohort_window
            and self._transport_is_stalled(key, now)
        }
        if len(implicated) >= _TRANSPORT_COHORT_MIN_KEYS:
            self._begin_transport_incident(
                domain, generation, "synchronized_1m_transport_stall", implicated,
                evidence_kind="COHORT",
            )
            return
        for key in active_candidates:
            self._schedule_gap_repair(key)

    def _reconcile_owner_is_current(
        self, domain: str, incident_id: str, generation: int,
        task: asyncio.Task | None, *, require_owner: bool = True,
    ) -> bool:
        incident = self._transport_incidents.get(domain)
        if not (
            incident
            and (incident.get("active") if require_owner else not incident.get("active"))
            and str(incident.get("incident_id", "")) == str(incident_id)
            and int(incident.get("generation", 0) or 0) == int(generation)
            and int(self._active_generations.get(domain, 0) or 0) == int(generation)
        ):
            return False
        if not require_owner:
            return True
        owner = self._transport_reconcile_owners.get(domain) or {}
        return bool(
            str(owner.get("incident_id", "")) == str(incident_id)
            and int(owner.get("generation", 0) or 0) == int(generation)
            and owner.get("task") is task
        )

    def _clear_transport_reconcile_owner(
        self, domain: str, incident_id: str, generation: int, task: asyncio.Task | None,
    ) -> None:
        owner = self._transport_reconcile_owners.get(domain) or {}
        if (
            str(owner.get("incident_id", "")) != str(incident_id)
            or int(owner.get("generation", 0) or 0) != int(generation)
            or owner.get("task") is not task
        ):
            return
        self._transport_reconcile_owners[domain] = None
        if self._transport_reconcile_tasks.get(domain) is task:
            self._transport_reconcile_tasks.pop(domain, None)

    def _start_transport_reconcile(self, domain: str, generation: int) -> asyncio.Task | None:
        incident = self._transport_incidents.get(domain)
        if not incident or not incident.get("active"):
            return None
        incident_id = str(incident.get("incident_id", ""))
        if not incident_id or int(incident.get("generation", 0) or 0) != int(generation):
            return None
        owner = self._transport_reconcile_owners.get(domain) or {}
        existing = owner.get("task")
        if (
            existing is not None
            and not existing.done()
            and str(owner.get("incident_id", "")) == incident_id
            and int(owner.get("generation", 0) or 0) == int(generation)
        ):
            return existing
        if existing is not None and not existing.done():
            existing.cancel()
        task = self._create_owned_task(
            self._reconcile_transport_incident(domain, generation, incident_id),
            name=f"ws-transport-reconcile:{domain}:{generation}:{incident_id}",
        )
        self._transport_reconcile_owners[domain] = {
            "incident_id": incident_id, "generation": int(generation), "task": task,
        }
        self._transport_reconcile_tasks[domain] = task
        incident["reconcile_owner"] = incident_id
        incident["reconcile_started_at"] = time.monotonic()

        def _cleanup(done_task, *, _domain=domain, _incident_id=incident_id, _generation=generation):
            self._clear_transport_reconcile_owner(
                _domain, _incident_id, _generation, done_task,
            )

        task.add_done_callback(_cleanup)
        return task

    def _begin_transport_incident(
        self, domain: str, generation: int, reason: str, keys: set[tuple[str, str]],
        *, evidence_kind: str = "COHORT",
    ) -> None:
        if (
            int(self._active_generations.get(domain, 0) or 0) != int(generation)
            and str(reason) != "connection_generation_change"
        ):
            return
        current = self._transport_incidents.get(domain)
        if current and current.get("active") and int(current.get("generation", 0)) == int(generation):
            current.setdefault("implicated_keys", set()).update(keys)
            current.setdefault("deferred_keys", set()).update(keys)
            self._start_transport_reconcile(domain, generation)
            return
        self._transport_incident_sequence += 1
        incident = {
            "incident_id": f"{domain}:transport:{int(time.time() * 1000)}:{self._transport_incident_sequence}",
            "reason": str(reason),
            "generation": int(generation),
            "started_at": time.monotonic(),
            "implicated_keys": set(keys),
            "deferred_keys": set(keys),
            "evidence_kind": str(evidence_kind),
            "active": True,
            "recovery_status": "RECOVERING",
        }
        self._transport_incidents[domain] = incident
        self._transport_metrics[domain]["transport_incidents"] += 1
        self._record_gap_event("transport_incident_started", "*", "1m")
        self._mark_transport_keys_recovering(domain, generation, keys)
        self._start_transport_reconcile(domain, generation)

    def _mark_transport_keys_recovering(
        self, domain: str, generation: int, keys: set[tuple[str, str]] | None = None,
    ) -> None:
        for sub_key, row in list(self._subscription_state.items()):
            if sub_key[0] != domain or sub_key[1] != _CANDLE_CHANNELS["1m"]:
                continue
            coin = _coin_from_inst(sub_key[2])
            if (
                (keys is not None and (coin, "1m") in keys)
                or self._transport_is_stalled((coin, "1m"), time.monotonic())
            ):
                self._set_subscription_state(
                    sub_key, "RECOVERING", generation=generation,
                    next_retry_at=time.monotonic(), error_class="TRANSPORT_RECONCILE",
                )
                self._transport_metrics[domain]["transport_resync_requests"] += 1

    def _recheck_transport_deferred_keys(
        self, deferred: set[tuple[str, str]], *, re_admit: bool = False,
    ) -> bool:
        for coin, timeframe in deferred:
            key = (coin, timeframe)
            row = _cache_for_timeframe(timeframe).get(coin)
            if row:
                self._check_continuity(
                    coin, timeframe, row,
                    trigger_type="RECONNECT_CONTINUITY_GAP",
                )
            if re_admit and self._gap_state.get(key, {}).get("active"):
                self._admit_gap_for_repair(key)
        return not any(self._gap_state.get(key, {}).get("active") for key in deferred)

    def _transport_recovery_evidence(
        self, domain: str, generation: int, incident: dict, *, continuity_recovered: bool,
    ) -> bool:
        if not continuity_recovered or not self._subscription_ready(domain == "public", generation):
            return False
        if int(self._active_generations.get(domain, 0) or 0) != int(generation):
            return False
        started_at = float(
            incident.get("reconcile_started_at", incident.get("started_at", 0.0)) or 0.0
        )
        if str(incident.get("evidence_kind", "")) == "CONNECTION_GENERATION":
            state = self._heartbeat_state.get(domain, {})
            return bool(
                int(state.get("generation", 0) or 0) == int(generation)
                and float(state.get("last_rx_monotonic", 0.0) or 0.0) >= started_at
            )
        implicated = set(incident.get("implicated_keys", set()))
        if not implicated:
            return False
        for coin, timeframe in implicated:
            channel = _CANDLE_CHANNELS.get(timeframe, "")
            instrument = self._inst_id(coin)
            sub_key = (domain, channel, instrument)
            transport = self._transport_key_stats.get(
                self._transport_key(domain == "public", channel, instrument), {},
            )
            if (
                self._subscription_state.get(sub_key, {}).get("status") != "ACTIVE"
                or int(self._subscription_state.get(sub_key, {}).get("generation", 0) or 0) != int(generation)
                or int(transport.get("generation", 0) or 0) != int(generation)
                or float(transport.get("last_wire_rx_monotonic", 0.0) or 0.0) < started_at
            ):
                return False
        return True

    async def _reconcile_transport_incident(
        self, domain: str, generation: int, incident_id: str | None = None,
    ) -> None:
        task = asyncio.current_task()
        incident = self._transport_incidents.get(domain)
        resolved_incident_id = str(incident_id or (incident or {}).get("incident_id", ""))
        require_owner = incident_id is not None
        try:
            if not self._reconcile_owner_is_current(
                domain, resolved_incident_id, generation, task, require_owner=require_owner,
            ):
                return
            incident = self._transport_incidents[domain]
            self._mark_transport_keys_recovering(
                domain, generation, set(incident.get("deferred_keys", set())) or None,
            )
            await asyncio.sleep(_TRANSPORT_RECONCILE_SEC)
            if not self._reconcile_owner_is_current(
                domain, resolved_incident_id, generation, task, require_owner=require_owner,
            ):
                return
            await asyncio.sleep(_TRANSPORT_RECONCILE_SETTLE_SEC)
            if not self._reconcile_owner_is_current(
                domain, resolved_incident_id, generation, task, require_owner=require_owner,
            ):
                return
            incident = self._transport_incidents[domain]
            deferred = set(incident.get("deferred_keys", set()))
            continuity_recovered = self._recheck_transport_deferred_keys(deferred)
            subscription_ready = self._subscription_ready(domain == "public", generation)
            if subscription_ready and self._transport_recovery_evidence(
                domain, generation, incident, continuity_recovered=continuity_recovered,
            ):
                incident["active"] = False
                incident["recovery_status"] = "HEALED"
                incident["completed_at"] = time.monotonic()
                self._record_gap_event("transport_incident_reconciled", "*", "1m")
                return
            # Bounded failure is explicit: expiry alone is never healing
            # evidence, and every deferred key is re-evaluated or released.
            incident["active"] = False
            incident["recovery_status"] = "FAILED"
            incident["failure_reason"] = (
                "transport_progress_timeout"
                if subscription_ready
                else "subscription_readiness_timeout"
            )
            incident["completed_at"] = time.monotonic()
            self._record_gap_event("transport_incident_recovery_failed", "*", "1m")
            for key in deferred:
                row = _cache_for_timeframe(key[1]).get(key[0])
                if row:
                    self._check_continuity(
                        key[0], key[1], row,
                        trigger_type="RECONNECT_CONTINUITY_GAP",
                    )
                if row is None or self._gap_state.get(key, {}).get("active"):
                    self._schedule_gap_repair(key)
        finally:
            self._clear_transport_reconcile_owner(
                domain, resolved_incident_id, generation, task,
            )

    def _mark_gap(
        self, coin: str, timeframe: str, start: float, end: float,
        *, trigger_type: str = "WS_TIMESTAMP_GAP",
        recovery_kind: str = _RECOVERY_KIND_MISSING,
    ) -> None:
        try:
            start = float(start)
            end = float(end)
        except (TypeError, ValueError):
            return
        if not math.isfinite(start) or not math.isfinite(end) or start > end:
            return
        key = (coin, timeframe)
        self._gap_grace_pending.pop(key, None)
        row = _cache_for_timeframe(timeframe).get(coin)
        state = self._gap_state.get(key, {})
        if state.get("active"):
            old_start = float(state.get("start", start))
            old_end = float(state.get("end", end))
            merged_start = min(old_start, start)
            merged_end = max(old_end, end)
            if merged_start == old_start and merged_end == old_end:
                self._gap_duplicate_count += 1
                self._record_gap_event("duplicate_gap_coalesced", coin, timeframe)
            else:
                self._gap_range_extension_count += 1
                self._record_gap_event("gap_range_extended", coin, timeframe)
                state.update(
                    start=merged_start,
                    end=merged_end,
                    last_discovered_at=time.time(),
                    trigger_type=str(trigger_type),
                    recovery_kind=str(recovery_kind),
                )
            self._admit_gap_for_repair(key)
            return
        self._gap_count += 1
        self._record_gap_event("gap_discovered", coin, timeframe)
        self._gap_state[key] = {
            "active": True, "start": start, "end": end,
            "detected_at": time.time(), "last_discovered_at": time.time(),
            "trigger_type": str(trigger_type),
            "recovery_kind": str(recovery_kind),
            "trigger_id": f"gap:{coin}:{timeframe}:{float(start):.3f}:{float(end):.3f}",
            "market_state_version": int(row.get("version", 0) or 0) if row else 0,
        }
        if row:
            row["stale"] = True
            row["refresh_status"] = "gap_detected"
            row["refresh_error"] = "missing_candle_interval"
        self._admit_gap_for_repair(key)

    def _repair_done_callback(self, done: asyncio.Task) -> None:
        key = next((candidate for candidate, task in list(self._repair_tasks.items()) if task is done), None)
        if key is not None:
            self._repair_tasks.pop(key, None)
            if done.cancelled():
                return
            try:
                done.exception()
            except BaseException:
                return
            state = self._gap_state.get(key, {})
            if self._running and state.get("active"):
                pending = self._pending_repairs.setdefault(key, {"attempt": 0})
                pending["attempt"] = int(pending.get("attempt", 0)) + 1
                if (
                    state.get("recovery_kind") == _RECOVERY_KIND_CONFIRMATION
                    and pending["attempt"] >= _MAX_CONFIRMATION_RETRIES
                ):
                    self._confirmation_retry_exhausted += 1
                    self._record_gap_event("confirmation_retry_exhausted", *key)
                    self._pending_repairs.pop(key, None)
                    return
                pending["next_retry_at"] = time.monotonic() + min(60.0, 2.0 ** min(pending["attempt"], 5))
                self._repair_retry_count += 1
                self._record_gap_event("repair_retry", *key)

    async def _repair_scheduler(self) -> None:
        while self._running:
            now = time.monotonic()
            for key, state in list(self._gap_grace_pending.items()):
                if float(state.get("due_at", 0.0)) > now:
                    continue
                self._gap_grace_pending.pop(key, None)
                coin, timeframe = key
                state = dict(state)
                if state.get("recovery_kind") == _RECOVERY_KIND_CONFIRMATION:
                    state["expired"] = True
                    self._confirmation_grace_expiries += 1
                    self._record_gap_event("confirmation_grace_expired", coin, timeframe)
                    self._recheck_confirmation_boundary(coin, timeframe, state)
                else:
                    row = _cache_for_timeframe(timeframe).get(coin, {})
                    self._check_continuity(
                        coin, timeframe, row, now=time.time(),
                        trigger_type="CONFIRMATION_GRACE_EXPIRED",
                    )
            due = [
                key for key, state in list(self._pending_repairs.items())
                if float(state.get("next_retry_at", 0.0)) <= now
                and key not in self._repair_tasks
            ]
            while due and len(self._repair_tasks) < _MAX_PENDING_REPAIRS:
                key = due.pop(0)
                self._pending_repairs.pop(key, None)
                self._schedule_gap_repair(key)
            await asyncio.sleep(0.5)

    async def _repair_gap(self, coin: str, timeframe: str) -> None:
        self._repair_count += 1
        self._record_gap_event("repair_physical_request", coin, timeframe)
        async with self._repair_gate:
            try:
                from market_data_stream import _fetch_and_store
                import market_data_stream
                bar = _CANDLE_CHANNELS[timeframe].replace("candle", "")
                gap = dict(self._gap_state.get((coin, timeframe), {}))
                start = gap.get("start")
                end = gap.get("end")
                recovery_kind = str(gap.get("recovery_kind", _RECOVERY_KIND_MISSING))
                if recovery_kind == _RECOVERY_KIND_CONFIRMATION:
                    self._confirmation_attempts += 1
                if start is None or end is None or not math.isfinite(float(start)) or not math.isfinite(float(end)) or float(start) > float(end):
                    self._record_repair_failure("EMPTY_RANGE")
                    return
                before_row = _cache_for_timeframe(timeframe).get(coin, {})
                before_points = market_data_stream.canonical_candle_points(before_row)
                before_forming = (
                    float(before_points[-1][0])
                    if before_points and not before_points[-1][1]
                    else None
                )
                before_target = market_data_stream.candle_range_status(
                    before_row, timeframe, float(start), float(end),
                )
                ok = await _fetch_and_store(
                    coin,
                    timeframe,
                    bar,
                    _HISTORY_LIMIT,
                    caller=(
                        "ws_confirmation_recovery"
                        if recovery_kind == _RECOVERY_KIND_CONFIRMATION
                        else "ws_gap_recovery"
                    ),
                    request_reason=(
                        "CONFIRMATION_RECOVERY"
                        if recovery_kind == _RECOVERY_KIND_CONFIRMATION
                        else "PROVEN_GAP_REPAIR"
                    ),
                    trigger_type=str(gap.get("trigger_type", "WS_TIMESTAMP_GAP")),
                    trigger_id=str(gap.get("trigger_id", f"gap:{coin}:{timeframe}")),
                    missing_start=start,
                    missing_end=end,
                    market_state_version=int(gap.get("market_state_version", 0) or 0),
                    ws_generation=int(self._active_generations.get("business", 0) or 0),
                    scheduling_module=__name__,
                    scheduling_function="_repair_gap",
                )
                row = _cache_for_timeframe(timeframe).get(coin, {})
                after_points = market_data_stream.canonical_candle_points(row)
                after_forming = (
                    float(after_points[-1][0])
                    if after_points and not after_points[-1][1]
                    else None
                )
                target = market_data_stream.candle_range_status(row, timeframe, float(start), float(end))
                if recovery_kind == _RECOVERY_KIND_CONFIRMATION:
                    if target["missing"]:
                        failure_reason = "TARGET_NOT_IN_RESPONSE"
                    elif target["unconfirmed"]:
                        failure_reason = "TARGET_STILL_UNCONFIRMED"
                    else:
                        failure_reason = ""
                else:
                    if target["satisfied"]:
                        failure_reason = ""
                    elif target["missing"]:
                        failure_reason = "TARGET_NOT_IN_RESPONSE"
                    elif target["unconfirmed"]:
                        failure_reason = "TARGET_STILL_UNCONFIRMED"
                    else:
                        failure_reason = "CONTINUITY_STILL_MISSING"
                tail_after = market_data_stream.required_tail_analysis(
                    row, timeframe, _REQUIRED_HISTORY[timeframe], now=time.time(),
                )
                result = "SUCCESS" if ok and not failure_reason else (
                    market_data_stream.last_candle_failure_reason() if not ok else failure_reason
                )
                self._repair_samples.append({
                    "recovery_kind": recovery_kind,
                    "target_start": float(start),
                    "target_end": float(end),
                    "current_forming_before": before_forming,
                    "current_forming_after": after_forming,
                    "target_vs_current_forming_before": self._target_forming_relation(
                        before_forming, timeframe, float(start),
                    ),
                    "target_vs_current_forming_after": self._target_forming_relation(
                        after_forming, timeframe, float(start),
                    ),
                    "inside_required_window": bool(
                        float(start) >= float(tail_after["required_tail_start"]) - 0.5
                        and float(end) <= float(tail_after["required_tail_end"]) + 0.5
                    ),
                    "target_present_before": not before_target["missing"],
                    "target_confirm_before": not before_target["unconfirmed"],
                    "target_present_after": not target["missing"],
                    "target_confirm_after": not target["unconfirmed"],
                    "target_satisfied": bool(target["satisfied"]),
                    "other_required_gaps_after": len(tail_after["missing_required_timestamps"]),
                    "other_noncritical_gaps_after": int(tail_after["older_noncritical_gap_count"]),
                    "result": result,
                })
                if len(self._repair_samples) > 64:
                    del self._repair_samples[:-64]
                state = self._gap_state.get((coin, timeframe), {})
                if ok and not failure_reason and state.get("active"):
                    self._repair_success += 1
                    self._record_gap_event("repair_success", coin, timeframe)
                    self._gap_state[(coin, timeframe)] = {"active": False, "last_repaired_at": time.time()}
                    row["stale"] = False
                    row["refresh_status"] = "fresh"
                    row["refresh_error"] = ""
                    # Re-evaluate the required tail so another real required
                    # gap remains independently observable after this target
                    # succeeds. Older cache holes are ignored by this check.
                    self._check_continuity(
                        coin, timeframe, row, now=time.time(),
                        trigger_type="RECONNECT_CONTINUITY_GAP",
                    )
                    if self._gap_state.get((coin, timeframe), {}).get("active"):
                        row["stale"] = True
                        row["refresh_status"] = "gap_detected"
                elif ok and not failure_reason:
                    self._repair_success += 1
                    self._record_gap_event("repair_success", coin, timeframe)
                else:
                    self._repair_failures += 1
                    self._record_repair_failure(
                        market_data_stream.last_candle_failure_reason() if not ok else failure_reason
                    )
                    self._record_gap_event("repair_failure", coin, timeframe)
                    self._pending_repairs[(coin, timeframe)] = {
                        "next_retry_at": time.monotonic() + 5.0,
                        "attempt": int(self._pending_repairs.get((coin, timeframe), {}).get("attempt", 0)),
                    }
            except asyncio.CancelledError:
                self._record_repair_failure("CANCELLED")
                raise
            except Exception:
                self._repair_failures += 1
                self._record_repair_failure("OTHER")
                self._record_gap_event("repair_failure", coin, timeframe)
                self._pending_repairs[(coin, timeframe)] = {
                    "next_retry_at": time.monotonic() + 5.0,
                    "attempt": int(self._pending_repairs.get((coin, timeframe), {}).get("attempt", 0)),
                }
                logger.debug("Targeted WS gap repair failed for %s %s", coin, timeframe, exc_info=True)

    def _mark_all_disconnected(self) -> None:
        import market_data_stream
        for timeframe in self._timeframes:
            for row in market_data_stream._cache_for_timeframe(timeframe).values():
                if row.get("source") == "okx_public_websocket":
                    row["stale"] = True
                    row["refresh_status"] = "disconnected"
                    row["refresh_error"] = "websocket_disconnected"


MarketDataHub = WebSocketStream
stream = MarketDataHub()
hub = stream

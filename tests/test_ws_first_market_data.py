import asyncio
import json
import re
import time

import pytest
import market_data_stream as market
import okx_client
import ws_stream
from ws_stream import (
    WSRequestEnvelopeError,
    WebSocketStream,
    _CANDLE_CHANNELS,
    validate_ws_request_id,
)


def _candle(ts, close, confirm="1"):
    return [str(int(ts * 1000)), str(close - 1), str(close + 1), str(close - 2), str(close), "10", "10", "10", confirm]


def _seed_row(count=40, start=None):
    start = float(start or (int(time.time() // 60) * 60 - count * 60))
    rows = [_candle(start + index * 60, 100 + index, "1") for index in range(count)]
    return rows


def _complete_timeframe_row(timeframe, count, start, *, final_confirm=True):
    step = ws_stream._TIMEFRAME_SECONDS[timeframe]
    rows = [_candle(start + index * step, 100 + index, "1") for index in range(count)]
    if not final_confirm:
        rows[-1][-1] = "0"
    return market._parse_candles(rows)


class _FakeWS:
    def __init__(self):
        self.sent = []

    async def send(self, value):
        if value == "ping":
            self.sent.append(value)
        else:
            self.sent.append(json.loads(value) if isinstance(value, str) else value)

    async def close(self):
        return None


class _BlockingWS(_FakeWS):
    def __aiter__(self):
        return self

    async def __anext__(self):
        await asyncio.Event().wait()


async def _promote_ready_pending_business(stream, coins):
    replacement = _BlockingWS()
    stream._reset_connection_domain(False, 2, activate=False)
    stream._own_socket(replacement)
    stream._generation_heartbeat["business"][2]["socket_id"] = id(replacement)
    stream._pending_connections["business"]["socket_id"] = id(replacement)
    stream._start_connection_tasks(
        replacement, public=False, generation=2, handover=True,
    )
    for coin in coins:
        key = ("business", "candle1m", f"{coin}-USDT-SWAP")
        stream._set_subscription_state(key, "REQUESTED", generation=2)
        stream._handle_message(json.dumps({
            "event": "subscribe",
            "arg": {"channel": "candle1m", "instId": f"{coin}-USDT-SWAP"},
        }), public=False, generation=2, control_only=True)
    assert stream._subscription_ready(False, 2) is True
    stream._promote_pending_connection(False, 2)
    return replacement


def test_full_universe_has_stable_multitimeframe_subscription_coverage():
    stream = WebSocketStream()
    stream._coins = [f"C{index}" for index in range(297)]
    args = stream._subscription_args()
    assert len(args) == 297 * len(_CANDLE_CHANNELS)
    assert len({(row["channel"], row["instId"]) for row in args}) == len(args)
    assert {row["channel"] for row in args} == set(_CANDLE_CHANNELS.values())


def test_subscription_acknowledgement_is_tracked():
    async def scenario():
        stream = WebSocketStream()
        stream._coins = ["BTC"]
        ws = _FakeWS()
        await stream._subscribe_candles(ws)
        assert len(ws.sent) == 1
        first = ws.sent[0]["args"][0]
        stream._handle_message(json.dumps({"event": "subscribe", "arg": first}), public=False)
        assert stream.stats()["subscriptions_confirmed"] == 1
        assert stream.stats()["subscriptions_expected"] == len(_CANDLE_CHANNELS)

    asyncio.run(scenario())


def test_current_confirmed_duplicate_and_out_of_order_updates_are_deterministic(monkeypatch):
    market._cache_1m.pop("BTC", None)
    stream = WebSocketStream()
    base = int(time.time() // 60) * 60 - 120
    stream._handle_message(json.dumps({"arg": {"channel": "candle1m", "instId": "BTC-USDT-SWAP"}, "data": [_candle(base, 100, "0")]}))
    stream._handle_message(json.dumps({"arg": {"channel": "candle1m", "instId": "BTC-USDT-SWAP"}, "data": [_candle(base, 101, "1")]}))
    stream._handle_message(json.dumps({"arg": {"channel": "candle1m", "instId": "BTC-USDT-SWAP"}, "data": [_candle(base - 60, 99, "1")]}))
    stream._handle_message(json.dumps({"arg": {"channel": "candle1m", "instId": "BTC-USDT-SWAP"}, "data": [_candle(base, 102, "1")]}))
    row = market._cache_1m["BTC"]
    assert row["timestamps"] == sorted(row["timestamps"])
    assert row["closes"][-1] == 102.0
    assert row["confirmed"][-1] is True
    assert row["source"] == "okx_public_websocket"


def test_ws_history_is_bounded_and_disconnect_marks_truth_stale():
    market._cache_1m.pop("ETH", None)
    stream = WebSocketStream()
    base = int(time.time() // 60) * 60 - 400 * 60
    for index in range(400):
        stream._handle_message(json.dumps({"arg": {"channel": "candle1m", "instId": "ETH-USDT-SWAP"}, "data": [_candle(base + index * 60, index + 1)]}))
    assert len(market._cache_1m["ETH"]["timestamps"]) <= 220
    stream._mark_all_disconnected()
    row = market._cache_1m["ETH"]
    assert row["stale"] is True
    assert row["refresh_status"] == "disconnected"


def test_rest_bootstrap_cannot_overwrite_newer_ws_bar():
    existing = {
        "timestamps": [100.0], "opens": [1.0], "highs": [3.0], "lows": [0.5], "closes": [2.5],
        "volumes": [4.0], "confirmed": [True], "source": "okx_public_websocket", "observed_at": 200.0,
        "timeframe": "1m", "bar": "1m", "data_type": "candles",
    }
    incoming = {
        "timestamps": [100.0, 160.0], "opens": [1.0, 2.0], "highs": [2.0, 3.0], "lows": [0.0, 1.0],
        "closes": [2.0, 2.5], "volumes": [3.0, 4.0], "confirmed": [True, True], "source": "okx_public_candles",
    }
    merged = market._merge_candle_rows(existing, incoming)
    assert merged["closes"][0] == 2.5
    assert merged["source"] == "okx_public_websocket"
    assert merged["timestamps"] == [100.0, 160.0]


def test_rest_confirmation_upgrades_existing_ws_forming_bar_without_overwriting_ohlcv():
    existing = {
        "timestamps": [100.0], "opens": [1.0], "highs": [3.0], "lows": [0.5], "closes": [2.5],
        "volumes": [4.0], "confirmed": [False], "source": "okx_public_websocket",
    }
    incoming = {
        "timestamps": [100.0], "opens": [9.0], "highs": [10.0], "lows": [8.0], "closes": [9.5],
        "volumes": [40.0], "confirmed": [True], "source": "okx_public_candles",
    }
    merged = market._merge_candle_rows(existing, incoming)
    assert merged["closes"] == [2.5]
    assert merged["volumes"] == [4.0]
    assert merged["confirmed"] == [True]


def test_ws_update_during_rest_bootstrap_remains_authoritative(monkeypatch):
    async def scenario():
        release_rest = asyncio.Event()
        rest_payload = [_candle(1_700_000_000, 10, "1")]

        async def fetch(*args, **kwargs):
            await release_rest.wait()
            return rest_payload

        monkeypatch.setattr(market, "_fetch", fetch)
        task = asyncio.create_task(market._fetch_and_store("RACE", "1m", "1m", 30))
        await asyncio.sleep(0)
        stream = WebSocketStream()
        stream._upsert_candle(
            "RACE-USDT-SWAP", "1m", _candle(1_700_000_000, 99, "1"), generation=7,
        )
        release_rest.set()
        assert await task is True
        row = market._cache_1m["RACE"]
        assert row["closes"][0] == 99.0
        assert row["source"] == "okx_public_websocket"

    asyncio.run(scenario())


def test_gap_repair_is_targeted_to_one_instrument(monkeypatch):
    async def scenario():
        stream = WebSocketStream()
        calls = []

        async def repair(coin, timeframe, bar, limit, **kwargs):
            calls.append((coin, timeframe, limit, kwargs.get("caller")))
            market._cache_1m[coin] = {
                "timestamps": [60.0, 120.0, 180.0, 240.0],
                "closes": [1.0, 1.0, 1.0, 1.0],
                "confirmed": [True, True, True, True],
            }
            return True

        monkeypatch.setattr(market, "_fetch_and_store", repair)
        stream._running = True
        stream._mark_gap("DOGE", "1m", 120.0, 240.0)
        task = stream._repair_tasks[("DOGE", "1m")]
        await task
        assert calls == [("DOGE", "1m", 220, "ws_gap_recovery")]
        assert stream._gap_state[("DOGE", "1m")]["active"] is False

    asyncio.run(scenario())


def test_reconnect_forming_bar_detects_gap_before_confirmation():
    market._cache_1m.pop("RECONNECT", None)
    stream = WebSocketStream()
    stream._running = False
    base = int(time.time() // 60) * 60 - 300
    for index in range(30):
        stream._handle_message(json.dumps({"arg": {"channel": "candle1m", "instId": "RECONNECT-USDT-SWAP"}, "data": [_candle(base - (29 - index) * 60, 100 + index, "1")]}))
    stream._handle_message(json.dumps({"arg": {"channel": "candle1m", "instId": "RECONNECT-USDT-SWAP"}, "data": [_candle(base + 240, 104, "0")] }))
    state = stream._gap_state[("RECONNECT", "1m")]
    assert state["active"] is True
    assert state["start"] == base + 60
    assert market._cache_1m["RECONNECT"]["stale"] is True


@pytest.mark.parametrize(
    "timeframe,required",
    [("1m", 30), ("5m", 50), ("15m", 60), ("1h", 60), ("4h", 60), ("1d", 120)],
)
def test_normal_adjacent_boundary_never_creates_timestamp_gap(timeframe, required):
    stream = WebSocketStream()
    stream._running = False
    step = ws_stream._TIMEFRAME_SECONDS[timeframe]
    base = 1_700_000_000.0
    row = _complete_timeframe_row(timeframe, required, base)
    row["timestamps"].append(base + required * step)
    row["confirmed"].append(False)
    stream._check_continuity("ADJACENT", timeframe, row, now=base + required * step + 1)
    assert stream._gap_state.get(("ADJACENT", timeframe), {}).get("active", False) is False
    assert stream._repair_count == 0
    assert stream.stats()["gap_decision_counts"]["GAP"] == 0


def test_unconfirmed_previous_bar_uses_confirmation_grace_not_timestamp_gap():
    stream = WebSocketStream()
    stream._running = False
    base = 1_700_000_000.0
    row = _complete_timeframe_row("1m", 31, base)
    row["confirmed"][-2] = False
    row["confirmed"].append(False)
    row["timestamps"].append(base + 31 * 60)
    stream._check_continuity("CONFIRM_ONLY", "1m", row, now=base + 30 * 60 + 0.1)
    assert ("CONFIRM_ONLY", "1m") not in stream._gap_state
    assert stream._gap_grace_pending[("CONFIRM_ONLY", "1m")]["kind"] == "confirmation"
    assert stream.stats()["gap_decision_counts"]["GAP"] == 0


@pytest.mark.parametrize(
    "timeframe,skip_count",
    [("1m", 1), ("5m", 2), ("15m", 2)],
)
def test_fixed_interval_gap_math_excludes_new_candle(timeframe, skip_count):
    stream = WebSocketStream()
    stream._running = False
    step = ws_stream._TIMEFRAME_SECONDS[timeframe]
    required = ws_stream._REQUIRED_HISTORY[timeframe]
    base = 1_700_000_000.0
    row = _complete_timeframe_row(timeframe, required, base)
    previous = row["timestamps"][-1]
    new_ts = previous + (skip_count + 1) * step
    row["timestamps"].append(new_ts)
    # A confirmed new candle proves a timestamp gap.  A forming new
    # candle immediately after one absent boundary is handled by the
    # confirmation-grace state below.
    row["confirmed"].append(True)
    stream._check_continuity("MATH", timeframe, row, now=new_ts + 60)
    state = stream._gap_state[("MATH", timeframe)]
    assert state["start"] == previous + step
    assert state["end"] == new_ts - step
    assert state["end"] < new_ts


def test_full_330_instrument_normal_boundaries_produce_zero_repairs():
    stream = WebSocketStream()
    stream._running = False
    transitions = {"1m": 20, "5m": 4, "15m": 2}
    for timeframe, count in transitions.items():
        step = ws_stream._TIMEFRAME_SECONDS[timeframe]
        required = ws_stream._REQUIRED_HISTORY[timeframe]
        for index in range(330):
            base = 1_700_000_000.0 + index * step * 1000
            row = _complete_timeframe_row(timeframe, required, base)
            for offset in range(1, count + 1):
                row["timestamps"].append(base + (required + offset - 1) * step)
                row["confirmed"].append(True)
                stream._check_continuity(
                    f"U{index}", timeframe, row,
                    now=base + (required + offset) * step + 10,
                )
    assert stream.stats()["gap_count"] == 0
    assert stream.stats()["repair_scheduled"] == 0
    assert stream._repair_count == 0


def test_three_actual_boundary_gap_keys_are_discovered_only_for_their_ranges():
    stream = WebSocketStream()
    stream._running = False
    cases = (("GAP1", "1m", 1), ("GAP2", "5m", 2), ("GAP3", "15m", 2))
    for coin, timeframe, missing_count in cases:
        step = ws_stream._TIMEFRAME_SECONDS[timeframe]
        required = ws_stream._REQUIRED_HISTORY[timeframe]
        base = 1_700_000_000.0
        row = _complete_timeframe_row(timeframe, required, base)
        previous = row["timestamps"][-1]
        new_ts = previous + (missing_count + 1) * step
        row["timestamps"].append(new_ts)
        row["confirmed"].append(True)
        stream._check_continuity(coin, timeframe, row, now=new_ts + 10)
    assert set(stream._gap_state) == {("GAP1", "1m"), ("GAP2", "5m"), ("GAP3", "15m")}
    assert stream.stats()["gap_count"] == 3
    assert stream.stats()["repair_scheduled"] == 0


def test_bootstrap_order_is_canonical_before_continuity_evaluation():
    base = 1_700_000_000.0
    row = market._parse_candles([
        _candle(base + 120, 3, "1"),
        _candle(base, 1, "1"),
        _candle(base + 60, 2, "1"),
        _candle(base + 60, 99, "0"),
    ])
    points = market.canonical_candle_points(row)
    assert [timestamp for timestamp, _ in points] == [base, base + 60, base + 120]
    assert points[1][1] is True
    assert market.confirmed_history_complete(row, "1m", 3, now=base + 180)


def test_empty_gap_range_cannot_enter_ledger():
    stream = WebSocketStream()
    stream._mark_gap("EMPTY", "1m", 120.0, 60.0)
    assert ("EMPTY", "1m") not in stream._gap_state
    assert stream.stats()["repair_scheduled"] == 0


def test_repair_failure_reason_is_classified(monkeypatch):
    async def scenario():
        stream = WebSocketStream()
        monkeypatch.setattr(market, "_fetch_and_store", lambda *args, **kwargs: asyncio.sleep(0, result=False))
        monkeypatch.setattr(market, "last_candle_failure_reason", lambda: "NO_ROWS_RETURNED")
        stream._gap_state[("FAILURE", "1m")] = {"active": True, "start": 120.0, "end": 120.0}
        await stream._repair_gap("FAILURE", "1m")
        assert stream.stats()["repair_failure_reasons"]["NO_ROWS_RETURNED"] == 1

    asyncio.run(scenario())


def _one_minute_history_with_gaps(missing=()):
    base = float(int(time.time() // 60) * 60 - 200 * 60)
    rows = [
        _candle(base + index * 60, 100 + index, "1")
        for index in range(200) if index not in set(missing)
    ]
    return base, market._parse_candles(rows)


@pytest.mark.parametrize("missing_index, expected", [(189, False), (170, False), (169, True), (99, True)])
def test_required_tail_uses_consumer_depth_not_cache_capacity(missing_index, expected):
    base, row = _one_minute_history_with_gaps({missing_index})
    analysis = market.required_tail_analysis(row, "1m", 30, now=base + 200 * 60)
    assert analysis["required_tail_start"] == base + 170 * 60
    assert analysis["complete"] is expected
    if expected:
        assert analysis["older_noncritical_gap_count"] == 1
    else:
        assert analysis["missing_required_timestamps"]


def test_old_required_tail_gap_does_not_schedule_urgent_repair():
    base, row = _one_minute_history_with_gaps({99})
    stream = WebSocketStream()
    stream._running = False
    stream._check_continuity("OLDGAP", "1m", row, now=base + 200 * 60)
    assert stream._gap_state == {}
    assert stream.stats()["repair_scheduled"] == 0


def test_market_ready_telemetry_matches_per_key_required_tail():
    base, row = _one_minute_history_with_gaps({99})
    row.update({
        "timeframe": "1m", "bar": "1m", "data_type": "candles", "ready": True,
        "source": "okx_public_candles", "stale": False, "refresh_status": "fresh",
        "last_update_received_at": time.time(),
    })
    market._cache_1m["TAILREADY"] = row
    stream = WebSocketStream()
    stream._coins = ["TAILREADY"]
    stream._health_state["business"] = "HEALTHY"
    stream._active_generations["business"] = 1
    key = ("business", "candle1m", "TAILREADY-USDT-SWAP")
    stream._subscription_expected.add(key)
    stream._set_subscription_state(key, "ACTIVE", generation=1)
    assert stream.market_data_ready_for({"TAILREADY"}, ("1m",)) is True
    assert stream.stats()["completeness_by_timeframe"]["1m"]["market_ready"] == 1


def test_target_local_repair_success_ignores_unrelated_old_gap(monkeypatch):
    async def scenario():
        base, row = _one_minute_history_with_gaps({99, 189})
        row.update({"timeframe": "1m", "bar": "1m", "data_type": "candles", "ready": True, "stale": True})
        market._cache_1m["TARGETLOCAL"] = row
        stream = WebSocketStream()
        stream._gap_state[("TARGETLOCAL", "1m")] = {
            "active": True, "start": base + 189 * 60, "end": base + 189 * 60,
            "recovery_kind": ws_stream._RECOVERY_KIND_MISSING,
        }

        async def repair(*_args, **_kwargs):
            _, repaired = _one_minute_history_with_gaps({99})
            repaired.update({"timeframe": "1m", "bar": "1m", "data_type": "candles", "ready": True, "stale": True})
            market._cache_1m["TARGETLOCAL"] = repaired
            return True

        monkeypatch.setattr(market, "_fetch_and_store", repair)
        await stream._repair_gap("TARGETLOCAL", "1m")
        assert stream.stats()["repair_success"] == 1
        assert stream.stats()["repair_failure_reasons"]["CONTINUITY_STILL_MISSING"] == 0
        assert stream._gap_state[("TARGETLOCAL", "1m")]["active"] is False
        assert market.confirmed_history_complete(
            market._cache_1m["TARGETLOCAL"], "1m", 30, now=base + 200 * 60,
        ) is True

    asyncio.run(scenario())


def test_confirmation_repair_success_is_distinct_from_timestamp_gap(monkeypatch):
    async def scenario():
        base = float(int(time.time() // 60) * 60 - 40 * 60)
        row = market._parse_candles([
            _candle(base + index * 60, 100 + index, "0" if index == 38 else "1")
            for index in range(40)
        ])
        market._cache_1m["CONFIRMREPAIR"] = row
        stream = WebSocketStream()
        stream._gap_state[("CONFIRMREPAIR", "1m")] = {
            "active": True, "start": base + 38 * 60, "end": base + 38 * 60,
            "recovery_kind": ws_stream._RECOVERY_KIND_CONFIRMATION,
            "trigger_type": "CONFIRMATION_GRACE_EXPIRED",
        }

        observed = {}

        async def repair(*_args, **kwargs):
            observed.update(kwargs)
            fixed = market._cache_1m["CONFIRMREPAIR"]
            fixed["confirmed"][38] = True
            return True

        monkeypatch.setattr(market, "_fetch_and_store", repair)
        await stream._repair_gap("CONFIRMREPAIR", "1m")
        assert stream.stats()["repair_success"] == 1
        assert stream.stats()["repair_failure_reasons"]["CONTINUITY_STILL_MISSING"] == 0
        sample = stream.stats()["repair_samples"][-1]
        assert sample["target_vs_current_forming_before"] == "NO_CURRENT_FORMING"
        assert sample["target_confirm_before"] is False
        assert sample["target_confirm_after"] is True
        assert observed["request_reason"] == "CONFIRMATION_RECOVERY"
        assert observed["caller"] == "ws_confirmation_recovery"

    asyncio.run(scenario())


def test_unconfirmed_confirmation_repair_uses_bounded_failure_reason(monkeypatch):
    async def scenario():
        base = float(int(time.time() // 60) * 60 - 40 * 60)
        row = market._parse_candles([
            _candle(base + index * 60, 100 + index, "0" if index == 38 else "1")
            for index in range(40)
        ])
        market._cache_1m["CONFIRMFAIL"] = row
        stream = WebSocketStream()
        stream._gap_state[("CONFIRMFAIL", "1m")] = {
            "active": True, "start": base + 38 * 60, "end": base + 38 * 60,
            "recovery_kind": ws_stream._RECOVERY_KIND_CONFIRMATION,
        }
        monkeypatch.setattr(market, "_fetch_and_store", lambda *_args, **_kwargs: asyncio.sleep(0, result=True))
        await stream._repair_gap("CONFIRMFAIL", "1m")
        assert stream.stats()["repair_failure_reasons"]["TARGET_STILL_UNCONFIRMED"] == 1
        assert stream._gap_state[("CONFIRMFAIL", "1m")]["active"] is True

    asyncio.run(scenario())


def test_confirmation_retry_is_finite_per_boundary():
    async def scenario():
        stream = WebSocketStream()
        stream._running = True
        key = ("RETRYBOUNDARY", "1m")
        stream._gap_state[key] = {
            "active": True,
            "recovery_kind": ws_stream._RECOVERY_KIND_CONFIRMATION,
        }

        async def completed():
            return None

        task = asyncio.create_task(completed())
        await task
        for _ in range(ws_stream._MAX_CONFIRMATION_RETRIES):
            stream._repair_tasks[key] = task
            stream._repair_done_callback(task)
        assert stream.stats()["confirmation_recovery"]["retry_exhausted"] == 1
        assert key not in stream._pending_repairs
        stream._running = False

    asyncio.run(scenario())


def test_full_332_instrument_adjacent_boundaries_ignore_old_holes():
    stream = WebSocketStream()
    stream._running = False
    base = float(int(time.time() // 60) * 60 - 200 * 60)
    stream._coins = [f"TAIL{index:03d}" for index in range(332)]
    for index in range(332):
        coin = f"TAIL{index:03d}"
        row = market._parse_candles([
            _candle(base + position * 60, 100 + position, "1")
            for position in range(200) if position != 99
        ])
        for boundary in range(1, 21):
            row["timestamps"].append(base + (199 + boundary) * 60)
            row["closes"].append(500 + boundary)
            row["confirmed"].append(True)
            stream._check_continuity(
                coin, "1m", row, now=base + (200 + boundary) * 60,
            )
    assert stream.stats()["gap_count"] == 0
    assert stream.stats()["repair_scheduled"] == 0


def test_boundary_ingestion_telemetry_separates_events_from_pair_checks():
    stream = WebSocketStream()
    base = float(int(time.time() // 60) * 60 - 30 * 60)
    market._cache_1m["COUNTERS"] = _complete_timeframe_row("1m", 30, base)
    stream._upsert_candle("COUNTERS-USDT-SWAP", "1m", _candle(base + 30 * 60, 101, "1"), generation=1)
    stats = stream.stats()
    assert stats["boundary_events_seen"]["1m"] == 1
    assert stats["new_adjacent_transitions"]["1m"] == 1
    assert stats["continuity_pair_checks"]["1m"] >= 1


def test_ticker_ws_snapshot_is_scanner_grade_not_p0_rest_truth(monkeypatch):
    okx_client._ticker_cache.clear()
    okx_client.update_ticker_from_ws("BTC-USDT-SWAP", {"last": "100", "bidPx": "99", "askPx": "101", "ts": "1700000000000"}, 7)

    async def scenario():
        client = okx_client.OKXClient()
        value = await client.get_ticker("BTC", priority="background")
        assert value["source"] == "okx_public_websocket"
        assert value["stale"] is False
        calls = []

        async def p0(*args, **kwargs):
            calls.append((args, kwargs))
            return {"data": [{"last": "101", "bidPx": "100", "askPx": "102", "ts": "1700000001000"}]}

        monkeypatch.setattr(client, "_request", p0)
        await client.get_ticker("BTC", priority="P0", allow_stale=False)
        assert calls

    asyncio.run(scenario())


def test_required_htf_channels_are_direct_ws_sources():
    assert _CANDLE_CHANNELS == {
        "1m": "candle1m", "5m": "candle5m", "15m": "candle15m",
        "1h": "candle1H", "4h": "candle4H", "1d": "candle1D",
    }


def test_direct_ws_htf_payload_preserves_okx_candle_fields_without_local_aggregation():
    stream = WebSocketStream()
    for index, (timeframe, channel) in enumerate(_CANDLE_CHANNELS.items()):
        if timeframe == "1m":
            continue
        cache = market._cache_for_timeframe(timeframe)
        cache.pop("HTF", None)
        timestamp = int(time.time() // 60) * 60 - (index + 1) * 60
        payload = _candle(timestamp, 200 + index, "1")
        stream._handle_message(json.dumps({"arg": {"channel": channel, "instId": "HTF-USDT-SWAP"}, "data": [payload]}))
        row = cache["HTF"]
        assert row["timestamps"] == [float(timestamp)]
        assert row["opens"] == [float(payload[1])]
        assert row["highs"] == [float(payload[2])]
        assert row["lows"] == [float(payload[3])]
        assert row["closes"] == [float(payload[4])]
        assert row["volumes"] == [float(payload[5])]
        assert row["confirmed"] == [True]
        assert row["source"] == "okx_public_websocket"


def test_steady_state_ready_ws_rows_require_no_normal_1m_rest(monkeypatch):
    market._cache_1m["BTC"] = market._parse_candles(_seed_row())
    row = market._cache_1m["BTC"]
    row.update({"timeframe": "1m", "bar": "1m", "data_type": "candles", "source": "okx_public_websocket", "refresh_status": "fresh", "stale": False})
    calls = []

    async def fail_fetch(*args, **kwargs):
        calls.append(args)
        return None

    async def scenario():
        monkeypatch.setattr(market, "_fetch", fail_fetch)
        await market.refresh_all({"BTC"})
        assert calls == []

    asyncio.run(scenario())


def test_full_297_instrument_28_consumer_steady_state_has_zero_candle_rest(monkeypatch):
    coins = {f"C{index}" for index in range(297)}
    for coin in coins:
        row = market._parse_candles(_seed_row())
        row.update({"timeframe": "1m", "bar": "1m", "data_type": "candles", "source": "okx_public_websocket", "refresh_status": "fresh", "stale": False, "ready": True})
        market._cache_1m[coin] = row
    calls = []

    async def fail_fetch(*args, **kwargs):
        calls.append(args)
        return None

    async def consumer():
        return sum(1 for coin in coins if market.get_closes(coin))

    async def scenario():
        monkeypatch.setattr(market, "_fetch", fail_fetch)
        results = await asyncio.gather(*(consumer() for _ in range(28)))
        await market.refresh_all(coins)
        assert results == [297] * 28
        assert calls == []

    asyncio.run(scenario())


def test_healthy_direct_ws_htf_snapshots_skip_normal_rest_refresh(monkeypatch):
    now = time.time()
    requirements = {"1m": 30, "15m": 60, "1h": 60, "4h": 60, "1d": 120}
    seconds = {"1m": 60, "15m": 900, "1h": 3600, "4h": 14400, "1d": 86400}
    bars = {"1m": "1m", "15m": "15m", "1h": "1H", "4h": "4H", "1d": "1D"}
    for timeframe, count in requirements.items():
        step = seconds[timeframe]
        start = int(now // step) * step - (count + 1) * step
        row = market._parse_candles([_candle(start + index * step, 100 + index) for index in range(count + 1)])
        row.update({"timeframe": timeframe, "bar": bars[timeframe], "data_type": "candles", "source": "okx_public_websocket", "refresh_status": "fresh", "stale": False, "ready": True})
        market._cache_for_timeframe(timeframe)["READY"] = row
    calls = []

    async def fail_fetch(*args, **kwargs):
        calls.append(args)
        return None

    async def scenario():
        monkeypatch.setattr(market, "_fetch", fail_fetch)
        await market.refresh_all({"READY"})
        await market.refresh_15m_all({"READY"})
        await market.refresh_1h_all({"READY"})
        await market.refresh_4h_all({"READY"})
        await market.refresh_1d_all({"READY"})
        assert calls == []

    asyncio.run(scenario())


def test_rubik_same_key_shares_per_instrument_lock(monkeypatch):
    okx_client._taker_volume_cache.clear()
    okx_client._taker_volume_locks.clear()
    monkeypatch.setattr(okx_client, "_taker_volume_ok", True)
    monkeypatch.setattr(okx_client, "PAPER_TRADING", False)
    monkeypatch.setattr(okx_client, "OKX_API_KEY", "fixture")

    async def scenario():
        client = okx_client.OKXClient()
        calls = []

        async def request(*args, **kwargs):
            calls.append(kwargs.get("params", {}).get("ccy"))
            await asyncio.sleep(0.01)
            return {"data": [["1700000000000", "5", "3"]]}

        monkeypatch.setattr(client, "_request", request)
        values = await asyncio.gather(client.get_taker_volume("BTC"), client.get_taker_volume("BTC"))
        assert len(calls) == 1
        assert values[0]["buy"] == values[1]["buy"] == 5.0

    asyncio.run(scenario())


def test_heartbeat_sends_ping_and_requires_pong(monkeypatch):
    async def scenario():
        stream = WebSocketStream()
        stream._running = True
        stream._active_generations["business"] = 1
        stream._heartbeat_state["business"] = {
            "generation": 1, "last_rx_monotonic": time.monotonic() - 1,
            "last_pong_monotonic": time.monotonic(), "ping_sent_at": 0.0,
            "awaiting_pong": False,
        }
        ws = _FakeWS()
        monkeypatch.setattr(ws_stream, "PING_INTERVAL", 0.01)
        monkeypatch.setattr(ws_stream, "PONG_DEADLINE", 0.03)
        task = asyncio.create_task(stream._heartbeat(ws, generation=1))
        for _ in range(20):
            if ws.sent:
                break
            await asyncio.sleep(0.005)
        assert ws.sent == ["ping"]
        with_time = stream._heartbeat_state["business"]
        assert with_time["awaiting_pong"] is True
        with_time["awaiting_pong"] = False
        with_time["last_pong_monotonic"] = time.monotonic()
        with_time["last_rx_monotonic"] = time.monotonic()
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    asyncio.run(scenario())


def test_heartbeat_without_pong_degrades_and_reports_fault(monkeypatch):
    async def scenario():
        stream = WebSocketStream()
        stream._running = True
        stream._active_generations["business"] = 1
        stream._heartbeat_state["business"] = {
            "generation": 1, "last_rx_monotonic": time.monotonic() - 1,
            "last_pong_monotonic": time.monotonic(), "ping_sent_at": 0.0,
            "awaiting_pong": False,
        }
        monkeypatch.setattr(ws_stream, "PING_INTERVAL", 0.01)
        monkeypatch.setattr(ws_stream, "PONG_DEADLINE", 0.03)
        await asyncio.wait_for(stream._heartbeat(_FakeWS(), generation=1), 0.2)
        assert stream._health_state["business"] == "DEGRADED"
        assert stream._fault_events["business"].is_set() is True

    asyncio.run(scenario())


def test_64008_marks_handover_pending_and_replacement_is_make_before_break(monkeypatch):
    async def scenario():
        stream = WebSocketStream()
        stream._running = True
        stream._coins = ["BTC"]
        stream._active_generations["business"] = 1
        stream._handle_message(json.dumps({
            "event": "notice", "code": "64008", "msg": "upgrade",
        }), public=False, generation=1)
        assert stream._notice_events["business"].is_set()
        assert stream._health_state["business"] == "HANDOVER_PENDING"

        class BlockingWS(_FakeWS):
            def __init__(self):
                super().__init__()
                self.release = asyncio.Event()

            def __aiter__(self):
                return self

            async def __anext__(self):
                await self.release.wait()
                raise StopAsyncIteration

        replacement = BlockingWS()
        async def connect(*args, **kwargs):
            return replacement

        async def subscribe(ws, generation=None):
            key = ("business", "candle1m", "BTC-USDT-SWAP")
            stream._subscription_expected.add(key)
            stream._set_subscription_state(key, "ACTIVE", generation=generation)
            stream._refresh_subscription_ready_event(False, generation)

        monkeypatch.setattr(ws_stream.websockets, "connect", connect)
        monkeypatch.setattr(stream, "_subscribe_candles", subscribe)
        result = await stream._open_replacement(url_for_public=False, old_generation=1)
        assert result is not None
        assert result[1] == 2
        assert stream._health_state["business"] == "HEALTHY"
        for task in result[2]:
            task.cancel()
            try:
                await task
            except BaseException:
                pass
        await replacement.close()

    asyncio.run(scenario())


def test_subscription_error_is_per_key_and_healthy_key_remains_active():
    async def scenario():
        stream = WebSocketStream()
        stream._coins = ["BTC"]
        ws = _FakeWS()
        await stream._subscribe_candles(ws, generation=1)
        first = ws.sent[0]["args"][0]
        second = ws.sent[0]["args"][1]
        stream._handle_message(json.dumps({"event": "subscribe", "arg": first}), generation=1)
        stream._handle_message(json.dumps({
            "event": "error", "code": "60012", "msg": "rejected", "arg": second,
        }), generation=1)
        first_key = ("business", first["channel"], first["instId"])
        second_key = ("business", second["channel"], second["instId"])
        assert stream._subscription_state[first_key]["status"] == "ACTIVE"
        assert stream._subscription_state[second_key]["status"] == "FAILED"
        assert stream._subscription_state[second_key]["error_code"] == "60012"

    asyncio.run(scenario())


def test_subscription_batches_obey_argument_and_serialized_byte_limits():
    stream = WebSocketStream()
    stream._coins = [f"C{index}" for index in range(297)]
    batches = stream._split_subscription_batches(stream._subscription_args())
    assert sum(len(batch) for batch in batches) == 297 * len(_CANDLE_CHANNELS)
    assert len(batches) == 36
    for batch in batches:
        payload = json.dumps({"id": "x", "op": "subscribe", "args": batch}, separators=(",", ":"))
        assert len(batch) <= ws_stream.MAX_SUB_ARGS
        assert len(payload.encode("utf-8")) <= ws_stream.MAX_SUB_BYTES


def test_subscription_operation_budget_counts_batches_not_arguments():
    stream = WebSocketStream()
    stream._coins = [f"C{index}" for index in range(297)]
    batches = stream._split_subscription_batches(stream._subscription_args())
    for _ in batches:
        assert stream._consume_subscription_operation(False) is True
    assert stream._subscription_operations["business"]["count"] == len(batches)
    assert len(batches) < ws_stream.SUB_OPERATION_LIMIT


def test_old_request_id_contract_violation_is_replaced_by_canonical_token():
    old = "oxypulse-sub-1"
    assert not re.fullmatch(r"[A-Za-z0-9]{1,32}", old)
    stream = WebSocketStream()
    values = [stream._next_subscription_id() for _ in range(10000)]
    assert len(set(values)) == 10000
    assert all(re.fullmatch(r"[A-Za-z0-9]{1,32}", value) for value in values)
    assert all(len(value) <= 32 for value in values)


def test_request_id_validation_rejects_non_protocol_tokens():
    for value in ("", "with-hyphen", "with_space", "with:colon", "x" * 33):
        with pytest.raises(WSRequestEnvelopeError):
            validate_ws_request_id(value)


def test_business_public_and_instrument_envelopes_use_valid_ids():
    async def scenario():
        stream = WebSocketStream()
        stream._coins = ["BTC"]
        business = _FakeWS()
        public = _FakeWS()
        await stream._subscribe_candles(business, generation=1)
        await stream._subscribe_public(public, generation=1)
        for ws in (business, public):
            assert ws.sent
            for payload in ws.sent:
                assert re.fullmatch(r"[A-Za-z0-9]{1,32}", payload["id"])
                assert payload["op"] == "subscribe"
                assert payload["args"]
        assert any(arg["channel"] == "candle1m" for arg in business.sent[0]["args"])
        assert any(arg["channel"] == "tickers" for arg in public.sent[0]["args"])
        assert any(arg["channel"] == "instruments" for arg in public.sent[-1]["args"])

    asyncio.run(scenario())


def test_full_306_instrument_plan_has_valid_ids_and_bounded_payloads():
    async def scenario():
        stream = WebSocketStream()
        stream._coins = [f"C{index}" for index in range(306)]
        business = _FakeWS()
        public = _FakeWS()
        await stream._subscribe_candles(business, generation=1)
        await stream._subscribe_public(public, generation=1)
        all_payloads = business.sent + public.sent
        assert sum(len(payload["args"]) for payload in business.sent) == 306 * 6
        assert sum(len(payload["args"]) for payload in public.sent) == 306 + 1
        for payload in all_payloads:
            assert re.fullmatch(r"[A-Za-z0-9]{1,32}", payload["id"])
            assert payload["op"] == "subscribe"
            assert payload["args"]
            assert len(json.dumps(payload, separators=(",", ":")).encode("utf-8")) <= ws_stream.MAX_SUB_BYTES
            for arg in payload["args"]:
                assert set(arg) in ({"channel", "instId"}, {"channel", "instType"})

    asyncio.run(scenario())


def test_ack_without_response_id_resolves_only_returned_key():
    async def scenario():
        stream = WebSocketStream()
        stream._coins = ["BTC"]
        ws = _FakeWS()
        await stream._subscribe_candles(ws, generation=1)
        first, second = ws.sent[0]["args"][:2]
        stream._handle_message(json.dumps({"event": "subscribe", "arg": first}), generation=1)
        first_key = ("business", first["channel"], first["instId"])
        second_key = ("business", second["channel"], second["instId"])
        assert stream._subscription_state[first_key]["status"] == "ACTIVE"
        assert stream._subscription_state[second_key]["status"] == "REQUESTED"

    asyncio.run(scenario())


def test_60033_is_protocol_rejection_without_unchanged_retry_storm():
    async def scenario():
        stream = WebSocketStream()
        stream._running = True
        stream._coins = ["BTC"]
        ws = _FakeWS()
        await stream._subscribe_candles(ws, generation=1)
        request_id = ws.sent[0]["id"]
        keys = set(stream._subscription_batches[request_id]["expected_keys"])
        stream._handle_message(json.dumps({
            "event": "error", "id": request_id, "code": "60033",
            "msg": "Parameter id error", "connId": "fixture",
        }), generation=1)
        assert stream.stats()["protocol_rejections"] == 1
        assert len(ws.sent) == 1
        assert all(
            stream._subscription_state[key]["error_class"] == "REQUEST_PROTOCOL_REJECTED"
            and stream._subscription_state[key]["retry_allowed"] is False
            and stream._subscription_state[key]["status"] == "FAILED"
            for key in keys
        )
        watchdog = stream._create_owned_task(
            stream._subscription_watchdog(ws, public=False, generation=1),
            name="protocol-rejection-watchdog",
        )
        await asyncio.sleep(0.03)
        stream._running = False
        watchdog.cancel()
        with pytest.raises(asyncio.CancelledError):
            await watchdog
        assert len(ws.sent) == 1

    asyncio.run(scenario())


def test_invalid_local_request_id_is_not_sent(monkeypatch):
    async def scenario():
        stream = WebSocketStream()
        stream._coins = ["BTC"]
        ws = _FakeWS()
        monkeypatch.setattr(stream, "_next_subscription_id", lambda: "bad-id")
        await stream._subscribe_candles(ws, generation=1)
        assert ws.sent == []
        assert stream.stats()["invalid_request_ids_local"] == 1

    asyncio.run(scenario())


def test_freshness_separates_bar_age_transport_age_and_confirmation():
    now = time.time()
    for timeframe, step in (("4h", 14400), ("1d", 86400)):
        latest = float(int(now // step) * step - step)
        row = {
            "timestamps": [latest, latest + step],
            "confirmed": [True, False],
            "ready": True,
            "stale": False,
            "source": "okx_public_websocket",
            "refresh_status": "fresh",
            "observed_at": now,
            "received_at": now,
            "last_update_received_at": now,
            "connection_generation": 4,
        }
        market._cache_for_timeframe(timeframe)["FRESHNESS"] = row
        snapshot = market.timeframe_snapshot("FRESHNESS", timeframe, 1)
        assert snapshot["bar_age_sec"] >= step - 1
        assert snapshot["transport_age_sec"] < 1
        assert snapshot["transport_fresh"] is True
        assert snapshot["confirmed_history_complete"] is True
        assert snapshot["stale"] is False


def test_dead_transport_is_stale_even_when_forming_bar_timestamp_is_current():
    now = time.time()
    latest = float(int(now // 60) * 60 - 60)
    market._cache_1m["DEAD"] = {
        "timestamps": [latest, latest + 60], "confirmed": [True, False], "ready": True,
        "stale": False, "source": "okx_public_websocket", "refresh_status": "fresh",
        "observed_at": now - 180, "received_at": now - 180,
        "last_update_received_at": now - 180, "connection_generation": 2,
    }
    snapshot = market.timeframe_snapshot("DEAD", "1m", 1)
    assert snapshot["transport_stale"] is True
    assert snapshot["stale"] is True


def test_forming_bar_does_not_hide_missing_confirmed_history():
    now = time.time()
    latest_confirmed = float(int(now // 60) * 60 - 180)
    market._cache_1m["INCOMPLETE"] = {
        "timestamps": [latest_confirmed, latest_confirmed + 120],
        "confirmed": [True, False], "ready": True, "stale": False,
        "source": "okx_public_websocket", "refresh_status": "fresh",
        "observed_at": now, "received_at": now, "last_update_received_at": now,
    }
    snapshot = market.timeframe_snapshot("INCOMPLETE", "1m", 1)
    assert snapshot["forming_candle_present"] is True
    assert snapshot["confirmed_history_complete"] is False
    assert snapshot["stale"] is True


def test_start_reports_transport_started_without_claiming_market_data_ready(monkeypatch):
    async def scenario():
        stream = WebSocketStream()
        monkeypatch.setattr(ws_stream, "_WS_LIB", True)

        async def business_loop():
            stream._connected = True
            while stream._running:
                await asyncio.sleep(0.01)

        async def public_loop():
            stream._public_connected = True
            while stream._running:
                await asyncio.sleep(0.01)

        monkeypatch.setattr(stream, "_run_loop", business_loop)
        monkeypatch.setattr(stream, "_run_public_loop", public_loop)
        assert await stream.start(["BTC"]) is True
        assert stream.stats()["transport_started"] is True
        assert stream.is_available() is False
        assert stream.market_data_ready_for({"BTC"}) is False
        await stream.stop()
        assert stream.owned_task_names() == []

    asyncio.run(scenario())


def test_market_data_ready_requires_ack_history_and_continuity():
    stream = WebSocketStream()
    stream._health_state["business"] = "HEALTHY"
    stream._active_generations["business"] = 1
    key = ("business", "candle1m", "BTC-USDT-SWAP")
    stream._subscription_expected.add(key)
    stream._set_subscription_state(key, "ACTIVE", generation=1)
    row = market._parse_candles(_seed_row())
    row.update({
        "timeframe": "1m", "bar": "1m", "ready": True,
        "source": "okx_public_websocket", "stale": False,
        "refresh_status": "fresh", "last_update_received_at": time.time(),
    })
    market._cache_1m["BTC"] = row
    assert stream.market_data_ready_for({"BTC"}) is True

    row["source"] = "okx_public_candles"
    assert stream.market_data_ready_for({"BTC"}) is True

    stream._set_subscription_state(key, "FAILED", generation=1, error_code="60012")
    assert stream.market_data_ready_for({"BTC"}) is False


def test_partial_readiness_keeps_healthy_key_usable():
    stream = WebSocketStream()
    stream._health_state["business"] = "HEALTHY"
    stream._active_generations["business"] = 1
    for coin in ("READY", "FAILED"):
        key = ("business", "candle1m", f"{coin}-USDT-SWAP")
        stream._subscription_expected.add(key)
        stream._set_subscription_state(
            key, "ACTIVE" if coin == "READY" else "FAILED", generation=1,
        )
        row = market._parse_candles(_seed_row())
        row.update({
            "timeframe": "1m", "bar": "1m", "ready": True,
            "source": "okx_public_websocket", "stale": False,
            "refresh_status": "fresh", "last_update_received_at": time.time(),
        })
        market._cache_1m[coin] = row
    assert stream.market_data_ready_for({"READY"}) is True
    assert stream.market_data_ready_for({"READY", "FAILED"}) is False


def test_missing_subscription_ack_enters_retry_state(monkeypatch):
    async def scenario():
        stream = WebSocketStream()
        stream._running = True
        stream._coins = ["ACK"]
        ws = _FakeWS()
        await stream._subscribe_candles(ws, generation=1)
        monkeypatch.setattr(ws_stream, "SUB_ACK_TIMEOUT", 0.01)
        monkeypatch.setattr(ws_stream, "SUB_WATCHDOG_INTERVAL", 0.01)
        task = stream._create_owned_task(
            stream._subscription_watchdog(ws, public=False, generation=1),
            name="test-sub-watchdog",
        )
        await asyncio.sleep(2.25)
        key = next(key for key in stream._subscription_state if key[2] == "ACK-USDT-SWAP")
        assert len(ws.sent) > 1
        assert stream._subscription_state[key]["status"] in {"REQUESTED", "RECOVERING", "RETRY_WAIT"}
        stream._running = False
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(scenario())


def test_g1_g2_overlap_is_idempotent_and_confirm_cannot_regress():
    stream = WebSocketStream()
    coin = "OVERLAP-USDT-SWAP"
    timestamp = int(time.time() // 60) * 60 - 60
    stream._upsert_candle(coin, "1m", _candle(timestamp, 100, "0"), generation=1)
    stream._upsert_candle(coin, "1m", _candle(timestamp, 101, "0"), generation=2)
    stream._upsert_candle(coin, "1m", _candle(timestamp, 99, "1"), generation=1)
    stream._upsert_candle(coin, "1m", _candle(timestamp, 101, "1"), generation=2)
    row = market._cache_1m["OVERLAP"]
    assert row["timestamps"].count(float(timestamp)) == 1
    assert row["confirmed"] == [True]
    assert row["closes"] == [101.0]
    assert row["connection_generation"] == 2


def test_pending_handover_isolates_active_subscription_readiness_and_health(monkeypatch):
    async def scenario():
        market._cache_1m.pop("PENDINGAUTH", None)
        stream = WebSocketStream()
        stream._running = True
        stream._reset_connection_domain(False, 1)
        active_key = ("business", "candle1m", "ACTIVE-USDT-SWAP")
        stream._subscription_expected.add(active_key)
        stream._set_subscription_state(active_key, "ACTIVE", generation=1)
        stream._subscription_confirmed.add(active_key)
        stream._subscription_ready_events["business"].set()
        stream._set_health(False, "HEALTHY")
        active_state = stream._heartbeat_state["business"]

        stream._reset_connection_domain(False, 2, activate=False)
        pending_key = ("business", "candle1m", "PENDING-USDT-SWAP")
        stream._set_subscription_state(pending_key, "REQUESTED", generation=2)
        pending = stream._pending_subscriptions["business"]
        assert stream._active_generations["business"] == 1
        assert stream._subscription_expected == {active_key}
        assert stream._subscription_confirmed == {active_key}
        assert stream._subscription_state[active_key]["status"] == "ACTIVE"
        assert stream._subscription_ready_events["business"].is_set()
        assert stream._heartbeat_state["business"] is active_state
        assert stream._health_state["business"] == "HEALTHY"
        assert pending is not None and pending["expected"] == {pending_key}
        assert pending["state"][pending_key]["status"] == "REQUESTED"

        stream._handle_message(json.dumps({"event": "subscribe", "arg": {
            "channel": "candle1m", "instId": "PENDING-USDT-SWAP",
        }}), public=False, generation=2, control_only=True)
        assert pending["ready_event"].is_set()
        assert stream._subscription_state[active_key]["status"] == "ACTIVE"
        assert stream._subscription_ready_events["business"].is_set()

        pending_heartbeat = stream._generation_heartbeat["business"][2]
        pending_heartbeat["last_rx_monotonic"] = time.monotonic() - 1.0
        monkeypatch.setattr(ws_stream, "PING_INTERVAL", 0.0)
        ping_ws = _FakeWS()
        heartbeat = asyncio.create_task(stream._heartbeat(ping_ws, generation=2))
        for _ in range(10):
            if ping_ws.sent:
                break
            await asyncio.sleep(0)
        assert ping_ws.sent == ["ping"]
        assert stream._health_state["business"] == "HEALTHY"
        heartbeat.cancel()
        with pytest.raises(asyncio.CancelledError):
            await heartbeat

        await stream._receive_loop(_BurstWS([json.dumps({
            "arg": {"channel": "candle1m", "instId": "PENDINGAUTH-USDT-SWAP"},
            "data": [_candle(1_700_000_000, 101, "1")],
        })]), public=False, generation=1)
        assert market._cache_1m["PENDINGAUTH"]["closes"] == [101.0]

        class BlockingWS(_FakeWS):
            def __aiter__(self):
                return self

            async def __anext__(self):
                await asyncio.Event().wait()

        pending_ws = BlockingWS()
        stream._own_socket(pending_ws)
        stream._generation_heartbeat["business"][2]["socket_id"] = id(pending_ws)
        stream._pending_connections["business"]["socket_id"] = id(pending_ws)
        stream._start_connection_tasks(
            pending_ws, public=False, generation=2, handover=True,
        )

        stream._promote_pending_connection(False, 2)
        assert stream._active_generations["business"] == 2
        assert stream._pending_subscriptions["business"] is None
        assert stream._subscription_expected == {pending_key}
        assert stream._subscription_state[pending_key]["status"] == "ACTIVE"
        assert stream._subscription_ready_events["business"].is_set()
        stream._running = False
        await stream._close_socket(pending_ws)

    asyncio.run(scenario())


def test_pending_rollback_preserves_active_incident_reconciliation_owner(monkeypatch):
    async def scenario():
        stream = WebSocketStream()
        stream._running = True
        stream._coins = ["ROLLBACK"]
        stream._reset_connection_domain(False, 1)
        active_key = ("business", "candle1m", "ROLLBACK-USDT-SWAP")
        stream._subscription_expected.add(active_key)
        stream._subscription_confirmed.add(active_key)
        stream._set_subscription_state(active_key, "ACTIVE", generation=1)
        stream._subscription_ready_events["business"].set()
        stream._set_health(False, "HEALTHY")
        monkeypatch.setattr(stream, "_mark_transport_keys_recovering", lambda *_args, **_kwargs: None)
        monkeypatch.setattr(ws_stream, "_TRANSPORT_RECONCILE_SEC", 0.04)
        monkeypatch.setattr(ws_stream, "_TRANSPORT_RECONCILE_SETTLE_SEC", 0.01)
        deferred = {("ROLLBACK", "1m")}
        stream._begin_transport_incident("business", 1, "active_before_handover", deferred)
        incident = stream._transport_incidents["business"]
        owner = stream._transport_reconcile_owners["business"]
        reconcile = stream._transport_reconcile_tasks["business"]
        stream._transport_key_stats[active_key] = {
            "generation": 1, "last_wire_rx_monotonic": time.monotonic(),
        }
        await asyncio.sleep(0)

        replacement = _FakeWS()

        async def connect(*_args, **_kwargs):
            return replacement

        original_subscribe = stream._subscribe_candles

        async def pending_subscribe(ws, generation=None):
            await original_subscribe(ws, generation)
            first = ws.sent[0]["args"][0]
            stream._handle_message(
                json.dumps({"event": "subscribe", "arg": first}),
                public=False, generation=generation, control_only=True,
            )

        monkeypatch.setattr(ws_stream.websockets, "connect", connect)
        monkeypatch.setattr(stream, "_subscribe_candles", pending_subscribe)
        monkeypatch.setattr(ws_stream, "SUB_ACK_TIMEOUT", 0.01)
        assert await stream._open_replacement(url_for_public=False, old_generation=1) is None
        assert stream._active_generations["business"] == 1
        assert stream._transport_incidents["business"] is incident
        assert incident["active"] is True
        assert incident["deferred_keys"] == deferred
        assert stream._transport_reconcile_tasks["business"] is reconcile
        assert stream._transport_reconcile_owners["business"] is owner
        assert owner["generation"] == 1 and owner["task"] is reconcile
        assert stream._subscription_expected == {active_key}
        assert stream._subscription_confirmed == {active_key}
        assert stream._subscription_state[active_key]["status"] == "ACTIVE"
        assert stream._subscription_ready_events["business"].is_set()
        assert stream._pending_connections["business"] is None
        assert stream._pending_subscriptions["business"] is None
        assert 2 not in stream._generation_heartbeat["business"]
        assert not any("handover" in name for name in stream.owned_task_names())

        await reconcile
        assert incident["recovery_status"] == "HEALED"
        assert stream._transport_reconcile_owners["business"] is None
        assert "business" not in stream._transport_reconcile_tasks
        stream._running = False
        await stream.stop()

    asyncio.run(scenario())


def test_replacement_promotes_only_after_readiness_and_then_rejects_g1(monkeypatch):
    async def scenario():
        market._cache_1m.pop("HANDOVERAUTH", None)
        stream = WebSocketStream()
        stream._running = True
        stream._coins = ["HANDOVERAUTH"]
        stream._health_state["business"] = "HEALTHY"
        stream._connected = True
        stream._active_generations["business"] = 1
        old_state = {
            "generation": 1, "last_rx_monotonic": time.monotonic(),
            "last_pong_monotonic": time.monotonic(), "awaiting_pong": False,
        }
        stream._heartbeat_state["business"] = old_state
        stream._generation_heartbeat["business"] = {1: old_state}
        replacement_ready = asyncio.Event()

        class BlockingWS(_FakeWS):
            def __aiter__(self):
                return self

            async def __anext__(self):
                await asyncio.Event().wait()

        replacement = BlockingWS()

        async def connect(*_args, **_kwargs):
            return replacement

        async def subscribe(_ws, generation=None):
            key = ("business", "candle1m", "HANDOVERAUTH-USDT-SWAP")
            stream._subscription_expected.add(key)
            stream._set_subscription_state(key, "REQUESTED", generation=generation)
            await replacement_ready.wait()
            stream._set_subscription_state(key, "ACTIVE", generation=generation)
            stream._refresh_subscription_ready_event(False, generation)

        monkeypatch.setattr(ws_stream.websockets, "connect", connect)
        monkeypatch.setattr(stream, "_subscribe_candles", subscribe)
        handover = asyncio.create_task(
            stream._open_replacement(url_for_public=False, old_generation=1),
        )
        for _ in range(20):
            if stream._pending_connections["business"] is not None:
                break
            await asyncio.sleep(0)
        assert stream._pending_connections["business"]["generation"] == 2
        assert stream._active_generations["business"] == 1
        await stream._receive_loop(_BurstWS([json.dumps({
            "arg": {"channel": "candle1m", "instId": "HANDOVERAUTH-USDT-SWAP"},
            "data": [_candle(1_700_000_000, 101, "1")],
        })]), public=False, generation=1)
        assert market._cache_1m["HANDOVERAUTH"]["closes"] == [101.0]
        replacement_ready.set()
        result = await handover
        assert result is not None
        assert stream._pending_connections["business"] is None
        assert stream._active_generations["business"] == 2
        await stream._receive_loop(_BurstWS([json.dumps({
            "arg": {"channel": "candle1m", "instId": "HANDOVERAUTH-USDT-SWAP"},
            "data": [_candle(1_700_000_060, 102, "1")],
        })]), public=False, generation=1)
        assert market._cache_1m["HANDOVERAUTH"]["closes"] == [101.0]
        for task in result[2]:
            task.cancel()
            try:
                await task
            except BaseException:
                pass
        await stream.stop()

    asyncio.run(scenario())


def test_ack_then_pending_transport_death_blocks_authority_transfer(monkeypatch):
    async def scenario():
        market._cache_1m.pop("PENDINGDEAD", None)
        stream = WebSocketStream()
        stream._running = True
        stream._coins = ["PENDINGDEAD"]
        stream._reset_connection_domain(False, 1)
        active_key = ("business", "candle1m", "PENDINGDEAD-USDT-SWAP")
        stream._subscription_expected.add(active_key)
        stream._subscription_confirmed.add(active_key)
        stream._set_subscription_state(active_key, "ACTIVE", generation=1)
        stream._subscription_ready_events["business"].set()
        stream._set_health(False, "HEALTHY")
        stream._connected = True
        stream._transport_metrics["business"]["conn_id"] = "g1-physical"

        class BlockingWS(_FakeWS):
            def __init__(self):
                super().__init__()
                self.closed = False

            def __aiter__(self):
                return self

            async def __anext__(self):
                await asyncio.Event().wait()

            async def close(self):
                self.closed = True

        replacement = BlockingWS()

        async def connect(*_args, **_kwargs):
            return replacement

        async def subscribe(_ws, generation=None):
            key = ("business", "candle1m", "PENDINGDEAD-USDT-SWAP")
            stream._set_subscription_state(key, "REQUESTED", generation=generation)

        monkeypatch.setattr(ws_stream.websockets, "connect", connect)
        monkeypatch.setattr(stream, "_subscribe_candles", subscribe)
        handover = asyncio.create_task(
            stream._open_replacement(url_for_public=False, old_generation=1),
        )
        pending_id = None
        for _ in range(20):
            pending = stream._pending_connections["business"]
            if pending is not None:
                candidate = ("business", 2, pending["socket_id"])
                if candidate in stream._connection_tasks:
                    pending_id = candidate
                    break
            await asyncio.sleep(0)
        assert pending_id is not None
        profile = stream._pending_subscriptions["business"]
        assert profile is not None and profile["ready_event"].is_set() is False
        stream._handle_message(json.dumps({
            "event": "subscribe",
            "arg": {"channel": "candle1m", "instId": "PENDINGDEAD-USDT-SWAP"},
        }), public=False, generation=2, control_only=True)
        assert profile["ready_event"].is_set() is True
        receiver = stream._connection_tasks[pending_id][0]
        receiver.cancel()

        assert await handover is None
        assert replacement.closed is True
        assert stream._active_generations["business"] == 1
        assert stream._health_state["business"] == "HEALTHY"
        assert stream._transport_metrics["business"]["conn_id"] == "g1-physical"
        assert stream._fault_events["business"].is_set() is False
        assert stream._pending_connections["business"] is None
        assert stream._pending_subscriptions["business"] is None
        assert 2 not in stream._generation_heartbeat["business"]
        assert pending_id not in stream._dispatch_queues
        assert pending_id not in stream._connection_tasks
        assert not any("handover" in name for name in stream.owned_task_names())

        await stream._receive_loop(_BurstWS([json.dumps({
            "arg": {"channel": "candle1m", "instId": "PENDINGDEAD-USDT-SWAP"},
            "data": [_candle(1_700_000_000, 101, "1")],
        })]), public=False, generation=1)
        assert market._cache_1m["PENDINGDEAD"]["closes"] == [101.0]
        stream._running = False
        await stream.stop()

    asyncio.run(scenario())


def test_successful_promotion_retires_old_incident_and_releases_deferred_keys(monkeypatch):
    async def scenario():
        stream = WebSocketStream()
        stream._running = True
        stream._coins = ["HANDOFFINCIDENT"]
        stream._reset_connection_domain(False, 1)
        active_key = ("business", "candle1m", "HANDOFFINCIDENT-USDT-SWAP")
        stream._subscription_expected.add(active_key)
        stream._subscription_confirmed.add(active_key)
        stream._set_subscription_state(active_key, "ACTIVE", generation=1)
        stream._subscription_ready_events["business"].set()
        stream._set_health(False, "HEALTHY")
        stream._connected = True
        stream._transport_observability_enabled = True
        monkeypatch.setattr(stream, "_mark_transport_keys_recovering", lambda *_args, **_kwargs: None)
        deferred = {("HANDOFFINCIDENT", "1m")}
        released = []
        monkeypatch.setattr(stream, "_schedule_gap_repair", lambda key: released.append(key))
        stream._begin_transport_incident("business", 1, "g1_handover", deferred)
        old_incident = stream._transport_incidents["business"]
        old_owner = stream._transport_reconcile_owners["business"]
        old_task = stream._transport_reconcile_tasks["business"]
        await asyncio.sleep(0)

        class BlockingWS(_FakeWS):
            def __aiter__(self):
                return self

            async def __anext__(self):
                await asyncio.Event().wait()

        replacement = BlockingWS()

        async def connect(*_args, **_kwargs):
            return replacement

        async def subscribe(_ws, generation=None):
            key = ("business", "candle1m", "HANDOFFINCIDENT-USDT-SWAP")
            stream._set_subscription_state(key, "REQUESTED", generation=generation)
            stream._handle_message(json.dumps({
                "event": "subscribe",
                "arg": {"channel": "candle1m", "instId": "HANDOFFINCIDENT-USDT-SWAP"},
            }), public=False, generation=generation, control_only=True)

        monkeypatch.setattr(ws_stream.websockets, "connect", connect)
        monkeypatch.setattr(stream, "_subscribe_candles", subscribe)
        result = await stream._open_replacement(url_for_public=False, old_generation=1)
        assert result is not None
        assert stream._active_generations["business"] == 2
        assert stream._transport_incidents["business"] is old_incident
        assert old_incident["active"] is False
        assert old_incident["recovery_status"] == "SUPERSEDED"
        assert old_incident["superseded_by_generation"] == 2
        assert old_incident["deferred_keys"] == deferred
        assert released == []
        assert stream._transport_reconcile_owners["business"] is None
        assert "business" not in stream._transport_reconcile_tasks
        assert old_owner["generation"] == 1 and old_owner["task"] is old_task

        await asyncio.sleep(0)
        with pytest.raises(asyncio.CancelledError):
            await old_task
        assert stream._transport_incidents["business"] is old_incident
        assert old_incident["active"] is False
        assert stream._transport_reconcile_owners["business"] is None
        assert "business" not in stream._transport_reconcile_tasks
        assert released == []

        stream._running = False
        await stream.stop()

    asyncio.run(scenario())


def test_promotion_release_single_deferred_gap_uses_one_real_recovery_path(monkeypatch):
    async def scenario():
        coin = "RELEASEONE"
        key = (coin, "1m")
        _base, row = _one_minute_history_with_gaps({189})
        market._cache_1m[coin] = row
        stream = WebSocketStream()
        stream._running = False
        stream._check_continuity(coin, "1m", row)
        assert stream._gap_state[key]["active"] is True

        stream._reset_connection_domain(False, 1)
        active_key = ("business", "candle1m", f"{coin}-USDT-SWAP")
        stream._subscription_expected.add(active_key)
        stream._subscription_confirmed.add(active_key)
        stream._set_subscription_state(active_key, "ACTIVE", generation=1)
        stream._subscription_ready_events["business"].set()
        stream._set_health(False, "HEALTHY")
        stream._running = True
        stream._transport_observability_enabled = True
        calls = []

        async def repair(repair_coin, timeframe):
            calls.append((repair_coin, timeframe))
            stream._gap_state[(repair_coin, timeframe)] = {"active": False}

        monkeypatch.setattr(stream, "_repair_gap", repair)
        monkeypatch.setattr(ws_stream, "_TRANSPORT_GAP_ADMISSION_SEC", 0.0)
        stream._begin_transport_incident("business", 1, "g1_single_release", {key})
        old_incident = stream._transport_incidents["business"]
        old_task = stream._transport_reconcile_tasks["business"]
        replacement = await _promote_ready_pending_business(stream, [coin])
        admission = stream._transport_admission_tasks["business"]
        await admission
        for _ in range(20):
            if calls:
                break
            await asyncio.sleep(0)
        await asyncio.sleep(0)

        assert stream._active_generations["business"] == 2
        assert old_incident["active"] is False
        assert old_incident["recovery_status"] == "SUPERSEDED"
        assert old_incident["deferred_keys"] == {key}
        assert calls == [key]
        assert stream.stats()["repair_scheduled"] == 1
        assert stream._transport_gap_candidates["business"] == {}
        assert stream._transport_incidents["business"] is old_incident
        with pytest.raises(asyncio.CancelledError):
            await old_task
        await asyncio.sleep(0)
        assert calls == [key]
        assert stream._transport_reconcile_owners["business"] is None
        assert "business" not in stream._transport_reconcile_tasks

        stream._running = False
        await stream._close_socket(replacement)
        await stream.stop()

    asyncio.run(scenario())


def test_promotion_release_cohort_uses_one_successor_incident_without_repairs(monkeypatch):
    async def scenario():
        coins = [f"RELEASECOHORT{index}" for index in range(ws_stream._TRANSPORT_COHORT_MIN_KEYS)]
        deferred = {(coin, "1m") for coin in coins}
        stream = WebSocketStream()
        stream._running = False
        for coin in coins:
            _base, row = _one_minute_history_with_gaps({189})
            market._cache_1m[coin] = row
            stream._check_continuity(coin, "1m", row)
            assert stream._gap_state[(coin, "1m")]["active"] is True

        stream._reset_connection_domain(False, 1)
        for coin in coins:
            active_key = ("business", "candle1m", f"{coin}-USDT-SWAP")
            stream._subscription_expected.add(active_key)
            stream._subscription_confirmed.add(active_key)
            stream._set_subscription_state(active_key, "ACTIVE", generation=1)
            stream._transport_key_stats[active_key] = {
                "generation": 1,
                "last_wire_rx_monotonic": time.monotonic(),
            }
        stream._subscription_ready_events["business"].set()
        stream._set_health(False, "HEALTHY")
        stream._running = True
        stream._transport_observability_enabled = True
        monkeypatch.setattr(stream, "_mark_transport_keys_recovering", lambda *_args, **_kwargs: None)
        monkeypatch.setattr(ws_stream, "_TRANSPORT_GAP_ADMISSION_SEC", 0.0)
        stream._begin_transport_incident("business", 1, "g1_cohort_release", deferred)
        old_incident = stream._transport_incidents["business"]
        old_task = stream._transport_reconcile_tasks["business"]
        replacement = await _promote_ready_pending_business(stream, coins)
        stale_received = time.monotonic() - ws_stream._TRANSPORT_KEY_STALL_SEC - 1.0
        for coin in coins:
            stream._observe_wire_frame(
                json.dumps({
                    "arg": {"channel": "candle1m", "instId": f"{coin}-USDT-SWAP"},
                    "data": [],
                }),
                public=False, generation=2, received_at=stale_received,
            )
        assert all(
            stream._transport_is_stalled((coin, "1m"), time.monotonic())
            for coin in coins
        )
        admission = stream._transport_admission_tasks["business"]
        await admission
        await asyncio.sleep(0)

        successor = stream._transport_incidents["business"]
        successor_owner = stream._transport_reconcile_owners["business"]
        assert stream._active_generations["business"] == 2
        assert old_incident["active"] is False
        assert old_incident["recovery_status"] == "SUPERSEDED"
        assert old_incident["deferred_keys"] == deferred
        assert successor is not old_incident
        assert successor["active"] is True
        assert successor["generation"] == 2
        assert successor["reason"] == "synchronized_1m_transport_stall"
        assert successor["evidence_kind"] == "COHORT"
        assert successor["implicated_keys"] == deferred
        assert successor["deferred_keys"] == deferred
        assert stream.stats()["repair_scheduled"] == 0
        assert stream._repair_tasks == {}
        assert stream._transport_gap_candidates["business"] == {}
        assert successor_owner is not None
        assert successor_owner["generation"] == 2
        assert successor_owner["task"] is stream._transport_reconcile_tasks["business"]

        with pytest.raises(asyncio.CancelledError):
            await old_task
        assert stream._transport_incidents["business"] is successor
        assert stream._transport_reconcile_owners["business"] is successor_owner
        assert successor["active"] is True

        stream._running = False
        await stream._close_socket(replacement)
        await stream.stop()

    asyncio.run(scenario())


def test_promotion_release_resolved_deferred_key_creates_no_recovery_owner():
    async def scenario():
        coin = "RELEASERESOLVED"
        key = (coin, "1m")
        _base, row = _one_minute_history_with_gaps({189})
        market._cache_1m[coin] = row
        stream = WebSocketStream()
        stream._running = False
        stream._check_continuity(coin, "1m", row)
        assert stream._gap_state[key]["active"] is True
        _base, resolved = _one_minute_history_with_gaps()
        market._cache_1m[coin] = resolved
        stream._check_continuity(coin, "1m", resolved)
        assert stream._gap_state[key]["active"] is False

        stream._reset_connection_domain(False, 1)
        active_key = ("business", "candle1m", f"{coin}-USDT-SWAP")
        stream._subscription_expected.add(active_key)
        stream._subscription_confirmed.add(active_key)
        stream._set_subscription_state(active_key, "ACTIVE", generation=1)
        stream._subscription_ready_events["business"].set()
        stream._set_health(False, "HEALTHY")
        stream._running = True
        stream._transport_observability_enabled = True
        stream._begin_transport_incident("business", 1, "g1_resolved_release", {key})
        old_incident = stream._transport_incidents["business"]
        old_task = stream._transport_reconcile_tasks["business"]
        replacement = await _promote_ready_pending_business(stream, [coin])
        await asyncio.sleep(0)

        assert stream._active_generations["business"] == 2
        assert old_incident["active"] is False
        assert old_incident["recovery_status"] == "SUPERSEDED"
        assert old_incident["deferred_keys"] == {key}
        assert stream._gap_state[key]["active"] is False
        assert stream._repair_tasks == {}
        assert stream._pending_repairs == {}
        assert stream._transport_gap_candidates["business"] == {}
        assert "business" not in stream._transport_admission_tasks
        assert stream._transport_incidents["business"] is old_incident
        with pytest.raises(asyncio.CancelledError):
            await old_task
        assert stream._transport_reconcile_owners["business"] is None

        stream._running = False
        await stream._close_socket(replacement)
        await stream.stop()

    asyncio.run(scenario())


def test_failed_replacement_preserves_healthy_g1(monkeypatch):
    async def scenario():
        stream = WebSocketStream()
        stream._running = True
        stream._coins = ["G1"]
        stream._health_state["business"] = "HEALTHY"
        stream._connected = True
        stream._active_generations["business"] = 1
        replacement = _FakeWS()

        async def connect(*args, **kwargs):
            return replacement

        async def subscribe(ws, generation=None):
            key = ("business", "candle1m", "G1-USDT-SWAP")
            stream._subscription_expected.add(key)
            stream._set_subscription_state(key, "REQUESTED", generation=generation)

        monkeypatch.setattr(ws_stream.websockets, "connect", connect)
        monkeypatch.setattr(stream, "_subscribe_candles", subscribe)
        monkeypatch.setattr(ws_stream, "SUB_ACK_TIMEOUT", 0.01)
        result = await stream._open_replacement(
            url_for_public=False, old_generation=1, previous_health="HEALTHY",
        )
        assert result is None
        assert stream._health_state["business"] == "HEALTHY"
        assert stream._active_generations["business"] == 1
        assert stream.owned_task_names() == []
        await stream.stop()

    asyncio.run(scenario())


def test_failed_replacement_retires_pending_incident_and_unblocks_later_gap(monkeypatch):
    async def scenario():
        stream = WebSocketStream()
        stream._running = True
        stream._coins = ["AFTERFAIL"]
        stream._health_state["business"] = "HEALTHY"
        stream._connected = True
        stream._active_generations["business"] = 1
        active_state = {"generation": 1, "last_rx_monotonic": time.monotonic()}
        stream._heartbeat_state["business"] = active_state
        stream._generation_heartbeat["business"] = {1: active_state}
        key = ("business", "candle1m", "AFTERFAIL-USDT-SWAP")
        stream._subscription_expected.add(key)
        stream._set_subscription_state(key, "ACTIVE", generation=1)
        stream._transport_key_stats[key] = {
            "generation": 1, "last_wire_rx_monotonic": time.monotonic(),
        }

        async def connect(*_args, **_kwargs):
            return _FakeWS()

        async def subscribe(_ws, generation=None):
            stream._begin_transport_incident(
                "business", generation, "pending_replacement_fixture", {("PENDING", "1m")},
            )
            raise RuntimeError("replacement subscription failed")

        monkeypatch.setattr(ws_stream.websockets, "connect", connect)
        monkeypatch.setattr(stream, "_subscribe_candles", subscribe)
        assert await stream._open_replacement(url_for_public=False, old_generation=1) is None
        await asyncio.sleep(0)
        assert stream._active_generations["business"] == 1
        assert stream._pending_connections["business"] is None
        assert stream._transport_incidents["business"] is None
        assert all(
            ":2" not in name for name in stream.owned_task_names()
        )

        scheduled = []
        monkeypatch.setattr(ws_stream, "_TRANSPORT_GAP_ADMISSION_SEC", 0.0)
        monkeypatch.setattr(stream, "_schedule_gap_repair", lambda repair_key: scheduled.append(repair_key))
        stream._mark_gap("AFTERFAIL", "1m", 1_700_000_000, 1_700_000_000)
        await stream._transport_admission_tasks["business"]
        assert scheduled == [("AFTERFAIL", "1m")]
        stream._running = False
        await stream.stop()

    asyncio.run(scenario())


def test_universe_change_requests_make_before_break_without_canceling_g1():
    async def scenario():
        stream = WebSocketStream()
        stream._running = True
        stream._coins = ["U1"]
        stream._task = stream._create_owned_task(asyncio.sleep(10), name="g1-business")
        stream._public_task = stream._create_owned_task(asyncio.sleep(10), name="g1-public")
        await stream.update_coins(["U2"])
        assert stream._task.done() is False
        assert stream._public_task.done() is False
        assert stream._notice_events["business"].is_set()
        assert stream._notice_events["public"].is_set()
        await stream.stop()

    asyncio.run(scenario())


def test_cancelled_replacement_cleans_g2_and_repropagates(monkeypatch):
    async def scenario():
        stream = WebSocketStream()
        stream._running = True
        stream._coins = ["G1"]
        stream._health_state["business"] = "HEALTHY"
        stream._connected = True
        stream._active_generations["business"] = 1

        class BlockingWS(_FakeWS):
            def __init__(self):
                super().__init__()
                self.closed = False

            def __aiter__(self):
                return self

            async def __anext__(self):
                await asyncio.Event().wait()

            async def close(self):
                self.closed = True

        replacement = BlockingWS()

        async def connect(*args, **kwargs):
            return replacement

        async def subscribe(ws, generation=None):
            key = ("business", "candle1m", "G1-USDT-SWAP")
            stream._subscription_expected.add(key)
            stream._set_subscription_state(key, "REQUESTED", generation=generation)

        monkeypatch.setattr(ws_stream.websockets, "connect", connect)
        monkeypatch.setattr(stream, "_subscribe_candles", subscribe)
        task = asyncio.create_task(stream._open_replacement(url_for_public=False, old_generation=1))
        for _ in range(50):
            if any("handover" in name for name in stream.owned_task_names()):
                break
            await asyncio.sleep(0.002)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert replacement.closed is True
        assert stream.owned_task_names() == []
        assert stream._active_generations["business"] == 1
        await stream.stop()

    asyncio.run(scenario())


def test_deferred_gap_eventually_executes(monkeypatch):
    async def scenario():
        stream = WebSocketStream()
        stream._running = True
        monkeypatch.setattr(ws_stream, "_MAX_PENDING_REPAIRS", 0)
        called = []

        async def repair(coin, timeframe):
            called.append((coin, timeframe))
            stream._gap_state[(coin, timeframe)] = {"active": False}

        monkeypatch.setattr(stream, "_repair_gap", repair)
        stream._mark_gap("DEFERRED", "1m", 1.0, 2.0)
        assert ("DEFERRED", "1m") in stream._pending_repairs
        scheduler = stream._create_owned_task(stream._repair_scheduler(), name="test-repair-scheduler")
        monkeypatch.setattr(ws_stream, "_MAX_PENDING_REPAIRS", 1)
        for _ in range(20):
            if called:
                break
            await asyncio.sleep(0.06)
        assert called == [("DEFERRED", "1m")]
        assert ("DEFERRED", "1m") not in stream._pending_repairs
        stream._running = False
        scheduler.cancel()
        with pytest.raises(asyncio.CancelledError):
            await scheduler

    asyncio.run(scenario())


def test_delayed_confirmation_inside_grace_does_not_repair(monkeypatch):
    stream = WebSocketStream()
    stream._running = False
    base = 1_000_000.0
    row = {
        "timestamps": [base + index * 60 for index in range(30)] + [base + 31 * 60],
        "confirmed": [True] * 30 + [False], "ready": True,
    }
    stream._check_continuity("GRACE", "1m", row, now=base + 30 * 60 + 1)
    assert ("GRACE", "1m") in stream._gap_grace_pending
    row["timestamps"].insert(30, base + 30 * 60)
    row["confirmed"].insert(30, True)
    stream._check_continuity("GRACE", "1m", row, now=base + 30 * 60 + 2)
    assert ("GRACE", "1m") not in stream._gap_state
    assert ("GRACE", "1m") not in stream._gap_grace_pending


def test_delayed_confirmation_beyond_grace_becomes_repairable():
    stream = WebSocketStream()
    stream._running = False
    base = 1_000_000.0
    row = {
        "timestamps": [base + index * 60 for index in range(30)] + [base + 31 * 60],
        "confirmed": [True] * 30 + [False], "ready": True,
    }
    now = base + 30 * 60 + stream._gap_grace_seconds("1m") + 0.1
    stream._check_continuity("BEYOND", "1m", row, now=now)
    assert stream._gap_state.get(("BEYOND", "1m"), {}).get("active", False) is False
    assert stream._gap_grace_pending[("BEYOND", "1m")]["recovery_kind"] == ws_stream._RECOVERY_KIND_CONFIRMATION


def test_absent_prior_boundary_before_forming_bar_is_confirmation_ambiguity():
    stream = WebSocketStream()
    stream._running = False
    base = 1_000_000.0
    row = {
        "timestamps": [base + index * 60 for index in range(30)] + [base + 31 * 60],
        "confirmed": [True] * 30 + [False], "ready": True,
    }
    stream._check_continuity("BOUNDARY", "1m", row, now=base + 31 * 60 + 0.1)
    state = stream._gap_grace_pending[("BOUNDARY", "1m")]
    assert state["recovery_kind"] == ws_stream._RECOVERY_KIND_CONFIRMATION
    assert state["target_ts"] == base + 30 * 60
    assert stream._gap_state == {}
    assert stream.stats()["gap_decision_counts"]["GAP"] == 1


def test_confirmation_recheck_promotes_absent_target_to_timestamp_gap():
    stream = WebSocketStream()
    stream._running = False
    base = 1_000_000.0
    row = {
        "timestamps": [base + index * 60 for index in range(30)] + [base + 31 * 60],
        "confirmed": [True] * 30 + [False], "ready": True,
    }
    market._cache_1m["RECHECKMISSING"] = row
    stream._recheck_confirmation_boundary(
        "RECHECKMISSING", "1m",
        {"target_ts": base + 30 * 60, "expired": True},
    )
    state = stream._gap_state[("RECHECKMISSING", "1m")]
    assert state["recovery_kind"] == ws_stream._RECOVERY_KIND_MISSING
    assert state["trigger_type"] == "WS_TIMESTAMP_GAP"


def test_confirmation_recheck_keeps_present_unconfirmed_target_distinct():
    stream = WebSocketStream()
    stream._running = False
    base = 1_000_000.0
    row = {
        "timestamps": [base + index * 60 for index in range(31)] + [base + 31 * 60],
        "confirmed": [True] * 30 + [False, False], "ready": True,
    }
    market._cache_1m["RECHECKUNCONFIRMED"] = row
    stream._recheck_confirmation_boundary(
        "RECHECKUNCONFIRMED", "1m",
        {"target_ts": base + 30 * 60, "expired": True},
    )
    state = stream._gap_state[("RECHECKUNCONFIRMED", "1m")]
    assert state["recovery_kind"] == ws_stream._RECOVERY_KIND_CONFIRMATION
    assert state["trigger_type"] == "CONFIRMATION_GRACE_EXPIRED"


@pytest.mark.parametrize("timeframe", ["1m", "5m", "15m", "1h", "4h", "1d"])
def test_single_missing_boundary_before_forming_bar_uses_one_confirmation_state(timeframe):
    stream = WebSocketStream()
    stream._running = False
    step = ws_stream._TIMEFRAME_SECONDS[timeframe]
    required = ws_stream._REQUIRED_HISTORY[timeframe]
    base = 1_700_000_000.0
    row = _complete_timeframe_row(timeframe, required, base)
    previous = row["timestamps"][-1]
    row["timestamps"].append(previous + 2 * step)
    row["closes"].append(999.0)
    row["confirmed"].append(False)
    stream._check_continuity("SINGLEFORMING", timeframe, row, now=previous + 2 * step + 0.1)
    state = stream._gap_grace_pending[("SINGLEFORMING", timeframe)]
    assert state["recovery_kind"] == ws_stream._RECOVERY_KIND_CONFIRMATION
    assert state["target_ts"] == previous + step
    assert stream._gap_state == {}


def test_full_330_forming_rollovers_do_not_create_timestamp_repairs():
    stream = WebSocketStream()
    stream._running = False
    for timeframe in ("1m", "5m", "15m"):
        step = ws_stream._TIMEFRAME_SECONDS[timeframe]
        required = ws_stream._REQUIRED_HISTORY[timeframe]
        for index in range(330):
            row = _complete_timeframe_row(
                timeframe, required, 1_700_000_000.0 + index * step * 1000,
            )
            previous = row["timestamps"][-1]
            row["timestamps"].append(previous + 2 * step)
            row["closes"].append(999.0)
            row["confirmed"].append(False)
            stream._check_continuity(
                f"FORMING{index:03d}", timeframe, row,
                now=previous + 2 * step + 0.1,
            )
    assert stream.stats()["gap_count"] == 0
    assert stream.stats()["repair_scheduled"] == 0
    assert stream.stats()["repair_deferred"] == 0
    assert stream.stats()["confirmation_recovery"]["states_created"] == 990


def test_confirmation_inside_grace_is_counted_and_does_not_repair():
    stream = WebSocketStream()
    stream._running = False
    base = 1_000_000.0
    row = {
        "timestamps": [base + index * 60 for index in range(30)] + [base + 31 * 60],
        "confirmed": [True] * 30 + [False], "ready": True,
    }
    stream._check_continuity("GRACECOUNT", "1m", row, now=base + 31 * 60 + 0.1)
    row["timestamps"].insert(30, base + 30 * 60)
    row["confirmed"].insert(30, True)
    stream._check_continuity("GRACECOUNT", "1m", row, now=base + 31 * 60 + 1.0)
    assert stream.stats()["confirmation_recovery"]["resolved_inside_grace"] == 1
    assert stream._gap_state == {}
    assert stream._gap_grace_pending == {}


def test_ws_confirmed_target_clears_matching_confirmation_grace():
    market._cache_1m.pop("CONFIRMFRAME", None)
    stream = WebSocketStream()
    base = float(int(time.time() // 60) * 60 - 40 * 60)
    for index in range(30):
        stream._upsert_candle(
            "CONFIRMFRAME-USDT-SWAP", "1m", _candle(base + index * 60, 100 + index, "1"),
            generation=1,
        )
    forming_ts = base + 31 * 60
    stream._upsert_candle(
        "CONFIRMFRAME-USDT-SWAP", "1m", _candle(forming_ts, 200, "0"), generation=1,
    )
    pending = stream._gap_grace_pending[("CONFIRMFRAME", "1m")]
    assert pending["target_ts"] == base + 30 * 60
    stream._upsert_candle(
        "CONFIRMFRAME-USDT-SWAP", "1m", _candle(base + 30 * 60, 199, "1"), generation=1,
    )
    assert stream._gap_grace_pending == {}
    assert stream.stats()["confirmation_recovery"]["resolved_inside_grace"] == 1
    assert market._cache_1m["CONFIRMFRAME"]["confirmed"][30] is True


def test_daily_and_other_timeframe_rest_ws_canonical_parity():
    assert market._BAR_BY_TIMEFRAME["1d"] == "1D"
    assert _CANDLE_CHANNELS["1d"] == "candle1D"
    assert "candle1Dutc" not in _CANDLE_CHANNELS.values()
    # 16:00 UTC is the ordinary UTC+8 daily opening boundary; 00:00 UTC is
    # the distinct 1Dutc boundary.
    ordinary = 1_755_456_000.0
    utc_boundary = ordinary + 8 * 3600
    assert ordinary != utc_boundary
    assert market._parse_candles([_candle(ordinary, 201, "1")])["timestamps"] != market._parse_candles([_candle(utc_boundary, 201, "1")])["timestamps"]
    for timeframe, step in (("1m", 60), ("5m", 300), ("15m", 900), ("1h", 3600), ("4h", 14400), ("1d", 86400)):
        payload = [_candle(ordinary - step, 200, "1"), _candle(ordinary, 201, "0")]
        rest = market._parse_candles(payload)
        ws = market._parse_candles(payload)
        assert ws["timestamps"] == rest["timestamps"]
        assert ws["opens"] == rest["opens"]
        assert ws["highs"] == rest["highs"]
        assert ws["lows"] == rest["lows"]
        assert ws["closes"] == rest["closes"]
        assert ws["volumes"] == rest["volumes"]
        assert ws["confirmed"] == rest["confirmed"]


class _BurstWS:
    def __init__(self, messages):
        self.messages = list(messages)

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self.messages:
            return self.messages.pop(0)
        raise StopAsyncIteration


def test_full_universe_receive_loop_burst_is_bounded_and_fair():
    async def scenario():
        stream = WebSocketStream()
        coins = [f"BURST{index}" for index in range(297)]
        stream._coins = coins
        stream._running = True
        stream._active_generations["business"] = 1
        stream._active_generations["public"] = 1
        base = int(time.time() // 60) * 60 - 60
        steps = {"1m": 60, "5m": 300, "15m": 900, "1h": 3600, "4h": 14400, "1d": 86400}
        candle_messages = []
        ticker_messages = []
        for wave in range(4):
            for index, coin in enumerate(coins):
                inst_id = f"{coin}-USDT-SWAP"
                ticker_messages.append(json.dumps({
                    "arg": {"channel": "tickers", "instId": inst_id},
                    "data": [{"last": str(100 + wave), "bidPx": "99", "askPx": "101", "volCcy24h": "10", "ts": str(int(time.time() * 1000))}],
                }))
                for timeframe, channel in _CANDLE_CHANNELS.items():
                    timestamp = base - steps[timeframe]
                    candle_messages.append(json.dumps({
                        "arg": {"channel": channel, "instId": inst_id},
                        "data": [_candle(timestamp, 100 + index + wave, "1" if wave >= 2 else "0")],
                    }))
        # An older update arriving after the latest revision must be merged
        # without regressing the newest valid row or confirmation state.
        for index, coin in enumerate(coins):
            inst_id = f"{coin}-USDT-SWAP"
            timestamp = base - 2 * steps["1m"]
            candle_messages.append(json.dumps({
                "arg": {"channel": _CANDLE_CHANNELS["1m"], "instId": inst_id},
                "data": [_candle(timestamp, 1 + index, "1")],
            }))
        heartbeat = []
        async def beat():
            while stream._running:
                heartbeat.append(time.perf_counter())
                await asyncio.sleep(0.001)
        start = time.perf_counter()
        beat_task = asyncio.create_task(beat())
        candle_ws = _BurstWS(candle_messages)
        ticker_ws = _BurstWS(ticker_messages)
        candle_task = asyncio.create_task(stream._receive_loop(candle_ws, public=False, generation=1))
        ticker_task = asyncio.create_task(stream._receive_loop(ticker_ws, public=True, generation=1))
        await asyncio.gather(candle_task, ticker_task)
        elapsed = time.perf_counter() - start
        stream._running = False
        await beat_task
        all_rows = [
            len(market._cache_for_timeframe(timeframe).get(coin, {}).get("timestamps", []))
            for timeframe in _CANDLE_CHANNELS for coin in coins
        ]
        assert candle_ws.messages == []
        assert ticker_ws.messages == []
        assert max(all_rows) <= 220
        assert all(
            market._cache_for_timeframe(timeframe)[coin]["confirmed"][-1] is True
            for timeframe in _CANDLE_CHANNELS for coin in coins
        )
        assert len(heartbeat) > 5
        assert elapsed < 10.0

    asyncio.run(scenario())


def test_normal_shutdown_leaves_zero_owned_ws_tasks():
    async def scenario():
        stream = WebSocketStream()
        stream._running = True
        stream._create_owned_task(asyncio.sleep(10), name="ws-test-reader")
        await stream.stop()
        assert stream.owned_task_names() == []

    asyncio.run(scenario())


def test_continuous_market_rx_does_not_ping(monkeypatch):
    async def scenario():
        stream = WebSocketStream()
        stream._running = True
        stream._active_generations["business"] = 1
        stream._heartbeat_state["business"] = {
            "generation": 1, "last_rx_monotonic": time.monotonic(),
            "last_pong_monotonic": time.monotonic(), "ping_sent_at": 0.0,
            "awaiting_pong": False,
        }

        class SlowMessages:
            def __init__(self):
                self.index = 0

            def __aiter__(self):
                return self

            async def __anext__(self):
                if self.index >= 12:
                    raise StopAsyncIteration
                self.index += 1
                await asyncio.sleep(0.003)
                return json.dumps({"arg": {"channel": "tickers", "instId": "BTC-USDT-SWAP"}, "data": []})

        ws = _FakeWS()
        monkeypatch.setattr(ws_stream, "PING_INTERVAL", 0.05)
        reader = asyncio.create_task(stream._receive_loop(SlowMessages(), public=False, generation=1))
        heartbeat = asyncio.create_task(stream._heartbeat(ws, generation=1))
        await reader
        stream._running = False
        heartbeat.cancel()
        with pytest.raises(asyncio.CancelledError):
            await heartbeat
        assert ws.sent == []

    asyncio.run(scenario())


def test_raw_pong_is_consumed_before_json_parser():
    async def scenario():
        stream = WebSocketStream()
        stream._running = True
        stream._active_generations["business"] = 1
        state = {
            "generation": 1, "last_rx_monotonic": 0.0,
            "last_pong_monotonic": 0.0, "ping_sent_at": time.monotonic(),
            "awaiting_pong": True,
        }
        stream._heartbeat_state["business"] = state
        await stream._receive_loop(_BurstWS(["pong"]), public=False, generation=1)
        assert state["awaiting_pong"] is False
        assert stream.stats()["heartbeat_metrics"]["business"]["pongs"] == 1
        assert stream.stats()["errors"] == 0

    asyncio.run(scenario())


def test_stale_generation_timeout_and_pong_cannot_affect_new_primary():
    async def scenario():
        stream = WebSocketStream()
        stream._running = True
        stream._active_generations["business"] = 2
        old = {
            "generation": 1, "last_rx_monotonic": time.monotonic(),
            "last_pong_monotonic": time.monotonic(),
            "ping_sent_at": time.monotonic() - 10, "awaiting_pong": True,
        }
        current = {
            "generation": 2, "last_rx_monotonic": time.monotonic(),
            "last_pong_monotonic": time.monotonic(),
            "ping_sent_at": 0.0, "awaiting_pong": True,
        }
        stream._generation_heartbeat["business"] = {1: old, 2: current}
        stream._heartbeat_state["business"] = current
        await stream._heartbeat(_FakeWS(), generation=1)
        assert stream._fault_events["business"].is_set() is False
        await stream._receive_loop(_BurstWS(["pong"]), public=False, generation=1)
        assert current["awaiting_pong"] is True
        assert stream._active_generations["business"] == 2

    asyncio.run(scenario())


def test_transport_queue_separates_wire_receive_from_candle_apply():
    async def scenario():
        stream = WebSocketStream()
        stream._running = True
        stream._active_generations["business"] = 1
        queue = asyncio.Queue(maxsize=2)
        messages = [
            json.dumps({
                "arg": {"channel": "candle1m", "instId": "QUEUE-USDT-SWAP"},
                "data": [_candle(1_700_000_000 + index * 60, 100 + index, "1")],
            })
            for index in range(3)
        ]
        reader = asyncio.create_task(
            stream._receive_loop(_BurstWS(messages), public=False, generation=1, queue=queue),
        )
        dispatcher = asyncio.create_task(
            stream._dispatch_loop(queue, public=False, generation=1),
        )
        await reader
        await queue.join()
        stream._running = False
        dispatcher.cancel()
        with pytest.raises(asyncio.CancelledError):
            await dispatcher
        transport = stream.stats()["transport"]
        domain = transport["domains"]["business"]
        key = transport["keys"]["business|candle1m|QUEUE-USDT-SWAP"]
        assert domain["frames_received"] == 3
        assert domain["frames_applied"] == 3
        assert domain["frames_dropped"] == 0
        assert key["frames_received"] == 3
        assert key["frames_applied"] == 3
        assert key["apply_latency_ms"] >= 0

    asyncio.run(scenario())


def test_stale_generation_frame_is_dropped_before_canonical_apply():
    async def scenario():
        stream = WebSocketStream()
        stream._running = True
        stream._active_generations["business"] = 2
        queue = asyncio.Queue()
        await queue.put((json.dumps({
            "arg": {"channel": "candle1m", "instId": "OLD-USDT-SWAP"},
            "data": [_candle(1_700_000_000, 100, "1")],
        }), time.monotonic()))
        dispatcher = asyncio.create_task(
            stream._dispatch_loop(queue, public=False, generation=1),
        )
        await queue.join()
        stream._running = False
        dispatcher.cancel()
        with pytest.raises(asyncio.CancelledError):
            await dispatcher
        assert market._cache_1m.get("OLD", {}).get("timestamps", []) == []
        assert stream.stats()["transport"]["domains"]["business"].get("stale_generation_frames_dropped") == 1
        assert stream.stats()["transport"]["domains"]["business"]["frames_dropped"] == 0

    asyncio.run(scenario())


def test_stale_receive_frame_cannot_contaminate_active_generation_telemetry():
    async def scenario():
        market._cache_1m.pop("STALETELEMETRY", None)
        stream = WebSocketStream()
        stream._running = True
        active_key = ("business", "candle1m", "STALETELEMETRY-USDT-SWAP")
        active_state = {
            "generation": 2,
            "last_rx_monotonic": 222.0,
            "last_pong_monotonic": 222.0,
            "awaiting_pong": False,
        }
        stream._active_generations["business"] = 2
        stream._heartbeat_state["business"] = active_state
        stream._generation_heartbeat["business"] = {
            1: {"generation": 1, "last_rx_monotonic": 111.0},
            2: active_state,
        }
        stream._transport_metrics["business"]["conn_id"] = "active-physical"
        stream._transport_key_stats[active_key] = {
            "generation": 2,
            "conn_id": "active-physical",
            "last_wire_rx_monotonic": 222.0,
            "frames_received": 7,
        }
        queue = asyncio.Queue()
        dispatcher = asyncio.create_task(
            stream._dispatch_loop(queue, public=False, generation=1),
        )
        stale_frame = json.dumps({
            "connId": "old-physical",
            "arg": {"channel": "candle1m", "instId": "STALETELEMETRY-USDT-SWAP"},
            "data": [_candle(1_700_000_000, 100, "1")],
        })
        await stream._receive_loop(
            _BurstWS([stale_frame]), public=False, generation=1, queue=queue,
        )
        assert queue.empty()
        assert market._cache_1m.get("STALETELEMETRY", {}).get("timestamps", []) == []
        active_telemetry = stream._transport_key_stats[active_key]
        assert active_telemetry["generation"] == 2
        assert active_telemetry["conn_id"] == "active-physical"
        assert active_telemetry["last_wire_rx_monotonic"] == 222.0
        assert active_telemetry["frames_received"] == 7
        assert stream._transport_metrics["business"]["conn_id"] == "active-physical"
        assert stream._transport_metrics["business"]["frames_received"] == 0
        assert stream._transport_metrics["business"]["stale_generation_frames_dropped"] == 1
        stream._running = False
        dispatcher.cancel()
        with pytest.raises(asyncio.CancelledError):
            await dispatcher

    asyncio.run(scenario())


def test_generation_transport_incident_defers_and_rechecks_gap():
    async def scenario():
        stream = WebSocketStream()
        stream._running = True
        stream._transport_observability_enabled = True
        stream._active_generations["business"] = 1
        stream._heartbeat_state["business"] = {"generation": 1}
        stream._reset_connection_domain(False, 2)
        base = float(int(time.time() // 60) * 60 - 40 * 60)
        market._cache_1m["INCIDENT"] = _complete_timeframe_row("1m", 40, base)
        stream._mark_gap("INCIDENT", "1m", base + 10 * 60, base + 10 * 60)
        assert stream._repair_tasks == {}
        assert ("INCIDENT", "1m") in stream._transport_incidents["business"]["deferred_keys"]
        incident = stream._transport_incidents["business"]
        incident["active"] = False
        await stream._reconcile_transport_incident("business", 2)
        assert stream._repair_tasks == {}
        assert stream._gap_state[("INCIDENT", "1m")]["active"] is False
        stream._running = False

    asyncio.run(scenario())


def test_candidate_flush_defers_key_to_current_connection_incident(monkeypatch):
    async def scenario():
        coin = "CANDIDATEOWNER"
        key = (coin, "1m")
        _base, row = _one_minute_history_with_gaps({189})
        market._cache_1m[coin] = row
        stream = WebSocketStream()
        stream._running = True
        stream._transport_observability_enabled = True
        stream._active_generations["business"] = 2
        stream._subscription_state[("business", "candle1m", f"{coin}-USDT-SWAP")] = {
            "status": "ACTIVE", "generation": 2,
        }
        scheduled = []
        monkeypatch.setattr(ws_stream, "_TRANSPORT_GAP_ADMISSION_SEC", 0.01)
        monkeypatch.setattr(stream, "_schedule_gap_repair", lambda repair_key: scheduled.append(repair_key))

        stream._check_continuity(coin, "1m", row)
        assert stream._gap_state[key]["active"] is True
        assert key in stream._transport_gap_candidates["business"]
        admission = stream._transport_admission_tasks["business"]

        stream._begin_transport_incident(
            "business", 2, "connection_generation_change", set(),
            evidence_kind="CONNECTION_GENERATION",
        )
        incident = stream._transport_incidents["business"]
        owner = stream._transport_reconcile_owners["business"]
        owner_task = stream._transport_reconcile_tasks["business"]
        assert owner is not None and owner["task"] is owner_task
        assert owner_task.done() is False

        await admission
        assert scheduled == []
        assert stream._repair_tasks == {}
        assert stream._transport_incidents["business"] is incident
        assert stream.stats()["transport"]["domains"]["business"]["transport_incidents"] == 1
        assert key in incident["deferred_keys"]
        assert stream._transport_gap_candidates["business"] == {}
        assert stream._transport_reconcile_owners["business"] is owner
        assert stream._transport_reconcile_tasks["business"] is owner_task

        stream._admit_gap_for_repair(key)
        await asyncio.sleep(0)
        assert scheduled == []
        assert stream._repair_tasks == {}

        owner_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await owner_task
        stream._running = False

    asyncio.run(scenario())


def test_candidate_flush_keeps_unrelated_key_out_of_current_incident(monkeypatch):
    async def scenario():
        coin = "CANDIDATEUNRELATED"
        key = (coin, "1m")
        owned = {("INCIDENTOWNED", "1m")}
        _base, row = _one_minute_history_with_gaps({189})
        market._cache_1m[coin] = row
        stream = WebSocketStream()
        stream._running = True
        stream._transport_observability_enabled = True
        stream._active_generations["business"] = 2
        stream._subscription_state[("business", "candle1m", f"{coin}-USDT-SWAP")] = {
            "status": "ACTIVE", "generation": 2,
        }
        scheduled = []
        monkeypatch.setattr(ws_stream, "_TRANSPORT_GAP_ADMISSION_SEC", 0.01)
        monkeypatch.setattr(stream, "_schedule_gap_repair", lambda repair_key: scheduled.append(repair_key))

        stream._check_continuity(coin, "1m", row)
        assert key in stream._transport_gap_candidates["business"]
        admission = stream._transport_admission_tasks["business"]
        stream._begin_transport_incident("business", 2, "fixture_cohort", owned)
        incident = stream._transport_incidents["business"]
        owner_task = stream._transport_reconcile_tasks["business"]

        await admission
        assert scheduled == [key]
        assert key not in incident["implicated_keys"]
        assert key not in incident["deferred_keys"]
        assert incident["implicated_keys"] == owned
        assert stream._transport_gap_candidates["business"] == {}
        assert stream.stats()["transport"]["domains"]["business"]["transport_incidents"] == 1

        owner_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await owner_task
        stream._running = False

    asyncio.run(scenario())


def test_prior_generation_telemetry_is_unknown_for_current_cohort_admission(monkeypatch):
    async def scenario():
        coins = [f"PRIORGEN{index}" for index in range(ws_stream._TRANSPORT_COHORT_MIN_KEYS)]
        stream = WebSocketStream()
        stream._running = True
        stream._transport_observability_enabled = True
        stream._active_generations["business"] = 2
        scheduled = []
        stale = time.monotonic() - ws_stream._TRANSPORT_KEY_STALL_SEC - 1.0
        monkeypatch.setattr(ws_stream, "_TRANSPORT_GAP_ADMISSION_SEC", 0.0)
        monkeypatch.setattr(stream, "_schedule_gap_repair", lambda repair_key: scheduled.append(repair_key))
        for coin in coins:
            transport_key = ("business", "candle1m", f"{coin}-USDT-SWAP")
            stream._subscription_state[transport_key] = {"status": "ACTIVE", "generation": 2}
            stream._transport_key_stats[transport_key] = {
                "generation": 1, "last_wire_rx_monotonic": stale,
            }
            stream._mark_gap(coin, "1m", 1_700_000_000, 1_700_000_000)

        assert all(
            stream._transport_is_stalled((coin, "1m"), time.monotonic()) is False
            for coin in coins
        )
        admission = stream._transport_admission_tasks["business"]
        await admission
        assert stream._transport_incidents["business"] is None
        assert sorted(scheduled) == [(coin, "1m") for coin in coins]
        assert all(stream._gap_state[key]["active"] for key in scheduled)
        assert stream._repair_tasks == {}
        stream._running = False

    asyncio.run(scenario())


def test_isolated_transport_gap_remains_one_targeted_repair(monkeypatch):
    async def scenario():
        stream = WebSocketStream()
        stream._running = True
        stream._transport_observability_enabled = True
        stream._active_generations["business"] = 1
        key = ("business", "candle1m", "ISOLATED-USDT-SWAP")
        stream._subscription_state[key] = {"status": "ACTIVE", "generation": 1}
        stream._transport_key_stats[key] = {
            "generation": 1, "last_wire_rx_monotonic": time.monotonic(),
        }
        async def repair(*_args, **_kwargs):
            return True
        monkeypatch.setattr(stream, "_repair_gap", repair)
        monkeypatch.setattr(ws_stream, "_TRANSPORT_GAP_ADMISSION_SEC", 0.0)
        stream._mark_gap("ISOLATED", "1m", 1_700_000_000, 1_700_000_000)
        await stream._transport_admission_tasks["business"]
        tasks = list(stream._repair_tasks.values())
        await asyncio.gather(*tasks)
        assert stream.stats()["repair_scheduled"] == 1
        assert stream.stats()["transport"]["domains"]["business"]["transport_incidents"] == 0
        stream._running = False

    asyncio.run(scenario())


def test_isolated_stalled_key_uses_targeted_repair_not_transport_incident(monkeypatch):
    async def scenario():
        stream = WebSocketStream()
        stream._running = True
        stream._transport_observability_enabled = True
        stream._active_generations["business"] = 1
        stale = time.monotonic() - (ws_stream._TRANSPORT_KEY_STALL_SEC + 1)
        key = ("business", "candle1m", "ONE-USDT-SWAP")
        stream._subscription_state[key] = {"status": "ACTIVE", "generation": 1}
        stream._transport_key_stats[key] = {"generation": 1, "last_wire_rx_monotonic": stale}
        scheduled = []
        monkeypatch.setattr(ws_stream, "_TRANSPORT_GAP_ADMISSION_SEC", 0.0)
        monkeypatch.setattr(stream, "_schedule_gap_repair", lambda repair_key: scheduled.append(repair_key))
        stream._mark_gap("ONE", "1m", 1_700_000_000, 1_700_000_000)
        await stream._transport_admission_tasks["business"]
        assert stream._transport_incidents["business"] is None
        assert scheduled == [("ONE", "1m")]
        stream._running = False

    asyncio.run(scenario())


def test_subthreshold_stalled_cohort_uses_targeted_repairs(monkeypatch):
    async def scenario():
        stream = WebSocketStream()
        stream._running = True
        stream._transport_observability_enabled = True
        stream._active_generations["business"] = 1
        stale = time.monotonic() - (ws_stream._TRANSPORT_KEY_STALL_SEC + 1)
        scheduled = []
        monkeypatch.setattr(ws_stream, "_TRANSPORT_GAP_ADMISSION_SEC", 0.0)
        monkeypatch.setattr(stream, "_schedule_gap_repair", lambda repair_key: scheduled.append(repair_key))
        for index in range(ws_stream._TRANSPORT_COHORT_MIN_KEYS - 1):
            coin = f"SUB{index}"
            key = ("business", "candle1m", f"{coin}-USDT-SWAP")
            stream._subscription_state[key] = {"status": "ACTIVE", "generation": 1}
            stream._transport_key_stats[key] = {"generation": 1, "last_wire_rx_monotonic": stale}
            stream._mark_gap(coin, "1m", 1_700_000_000, 1_700_000_000)
        await stream._transport_admission_tasks["business"]
        assert stream._transport_incidents["business"] is None
        assert sorted(scheduled) == [(f"SUB{index}", "1m") for index in range(7)]
        stream._running = False

    asyncio.run(scenario())


def test_systemic_1m_stall_is_one_incident_not_universe_repair_fanout(monkeypatch):
    async def scenario():
        stream = WebSocketStream()
        stream._running = True
        stream._transport_observability_enabled = True
        stream._active_generations["business"] = 1
        stale = time.monotonic() - (ws_stream._TRANSPORT_KEY_STALL_SEC + 1)
        monkeypatch.setattr(ws_stream, "_TRANSPORT_GAP_ADMISSION_SEC", 0.0)
        for index in range(330):
            coin = f"STALL{index}"
            key = ("business", "candle1m", f"{coin}-USDT-SWAP")
            stream._subscription_state[key] = {"status": "ACTIVE", "generation": 1}
            stream._transport_key_stats[key] = {"generation": 1, "last_wire_rx_monotonic": stale}
            stream._mark_gap(coin, "1m", 1_700_000_000, 1_700_000_000)
        await stream._transport_admission_tasks["business"]
        incident = stream._transport_incidents["business"]
        assert incident is not None and incident["active"] is True
        assert incident["reason"] == "synchronized_1m_transport_stall"
        assert incident["evidence_kind"] == "COHORT"
        assert len(incident["implicated_keys"]) == 330
        assert len(incident["deferred_keys"]) == 330
        assert stream._repair_tasks == {}
        assert stream.stats()["transport"]["domains"]["business"]["transport_incidents"] == 1
        stream._running = False
        for task in list(stream._owned_task_snapshot()):
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task

    asyncio.run(scenario())


def test_generation_change_is_explicit_independent_connection_incident():
    async def scenario():
        stream = WebSocketStream()
        stream._running = True
        stream._transport_observability_enabled = True
        stream._active_generations["business"] = 1
        stream._heartbeat_state["business"] = {"generation": 1, "last_rx_monotonic": time.monotonic()}
        stream._reset_connection_domain(False, 2)
        incident = stream._transport_incidents["business"]
        assert incident is not None and incident["active"] is True
        assert incident["reason"] == "connection_generation_change"
        assert incident["evidence_kind"] == "CONNECTION_GENERATION"
        assert incident["implicated_keys"] == set()
        task = stream._transport_reconcile_tasks["business"]
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        stream._running = False

    asyncio.run(scenario())


def test_unrelated_gap_is_not_absorbed_by_active_cohort_incident(monkeypatch):
    async def scenario():
        stream = WebSocketStream()
        stream._running = True
        stream._transport_observability_enabled = True
        stream._active_generations["business"] = 1
        cohort = {(f"COHORT{index}", "1m") for index in range(ws_stream._TRANSPORT_COHORT_MIN_KEYS)}
        stream._begin_transport_incident("business", 1, "fixture_cohort", cohort)
        incident = stream._transport_incidents["business"]
        task = stream._transport_reconcile_tasks["business"]
        scheduled = []
        monkeypatch.setattr(stream, "_schedule_gap_repair", lambda repair_key: scheduled.append(repair_key))
        stream._mark_gap("UNRELATED", "1m", 1_700_000_000, 1_700_000_000)
        assert ("UNRELATED", "1m") not in incident["deferred_keys"]
        assert incident["implicated_keys"] == cohort
        assert scheduled == [("UNRELATED", "1m")]
        assert stream._transport_incidents["business"] is incident
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        stream._running = False

    asyncio.run(scenario())


def test_reconcile_owner_replacement_cannot_be_completed_by_old_task():
    async def scenario():
        stream = WebSocketStream()
        stream._running = True
        stream._transport_observability_enabled = True
        stream._active_generations["business"] = 1
        stream._begin_transport_incident("business", 1, "old", {("OLDOWNER", "1m")})
        old_task = stream._transport_reconcile_tasks["business"]
        await asyncio.sleep(0)
        stream._active_generations["business"] = 2
        stream._begin_transport_incident("business", 2, "new", {("NEWOWNER", "1m")})
        new_incident = stream._transport_incidents["business"]
        new_task = stream._transport_reconcile_tasks["business"]
        assert new_task is not old_task
        with pytest.raises(asyncio.CancelledError):
            await old_task
        assert stream._transport_incidents["business"] is new_incident
        assert new_incident["active"] is True
        assert new_incident["generation"] == 2
        assert new_incident["deferred_keys"] == {("NEWOWNER", "1m")}
        assert stream._transport_reconcile_tasks["business"] is new_task
        new_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await new_task
        stream._running = False

    asyncio.run(scenario())


def test_reconcile_completion_requires_subscription_readiness_and_implicated_progress(monkeypatch):
    async def scenario():
        stream = WebSocketStream()
        stream._running = True
        stream._active_generations["business"] = 1
        key = ("business", "candle1m", "RECOVERED-USDT-SWAP")
        stream._subscription_expected.add(key)
        stream._set_subscription_state(key, "ACTIVE", generation=1)
        monkeypatch.setattr(ws_stream, "_TRANSPORT_RECONCILE_SEC", 0.01)
        monkeypatch.setattr(ws_stream, "_TRANSPORT_RECONCILE_SETTLE_SEC", 0.01)
        stream._begin_transport_incident("business", 1, "ready_fixture", {("RECOVERED", "1m")})
        task = stream._transport_reconcile_tasks["business"]
        await asyncio.sleep(0)
        stream._set_subscription_state(key, "ACTIVE", generation=1)
        stream._transport_key_stats[key] = {
            "generation": 1, "last_wire_rx_monotonic": time.monotonic(),
        }
        await task
        incident = stream._transport_incidents["business"]
        assert incident["active"] is False
        assert incident["recovery_status"] == "HEALED"
        assert stream._gap_event_counts["transport_incident_reconciled"] == 1
        stream._running = False

    asyncio.run(scenario())


def test_reconcile_timer_and_readiness_without_implicated_progress_falls_back(monkeypatch):
    async def scenario():
        stream = WebSocketStream()
        stream._running = True
        stream._active_generations["business"] = 1
        key = ("business", "candle1m", "NOPROGRESS-USDT-SWAP")
        stream._subscription_expected.add(key)
        stream._set_subscription_state(key, "ACTIVE", generation=1)
        released = []
        monkeypatch.setattr(ws_stream, "_TRANSPORT_RECONCILE_SEC", 0.01)
        monkeypatch.setattr(ws_stream, "_TRANSPORT_RECONCILE_SETTLE_SEC", 0.01)
        monkeypatch.setattr(stream, "_schedule_gap_repair", lambda repair_key: released.append(repair_key))
        stream._begin_transport_incident("business", 1, "no_progress_fixture", {("NOPROGRESS", "1m")})
        task = stream._transport_reconcile_tasks["business"]
        await asyncio.sleep(0)
        stream._set_subscription_state(key, "ACTIVE", generation=1)
        await task
        incident = stream._transport_incidents["business"]
        assert incident["active"] is False
        assert incident["recovery_status"] == "FAILED"
        assert incident["failure_reason"] == "transport_progress_timeout"
        assert released == [("NOPROGRESS", "1m")]
        assert stream._gap_event_counts.get("transport_incident_reconciled", 0) == 0
        stream._running = False

    asyncio.run(scenario())


def test_reconcile_failed_subscription_releases_deferred_keys(monkeypatch):
    async def scenario():
        stream = WebSocketStream()
        stream._running = True
        stream._active_generations["business"] = 1
        key = ("business", "candle1m", "FAILEDRECOVERY-USDT-SWAP")
        stream._subscription_expected.add(key)
        stream._set_subscription_state(key, "FAILED", generation=1, retry_allowed=False)
        released = []
        monkeypatch.setattr(ws_stream, "_TRANSPORT_RECONCILE_SEC", 0.01)
        monkeypatch.setattr(ws_stream, "_TRANSPORT_RECONCILE_SETTLE_SEC", 0.01)
        monkeypatch.setattr(stream, "_schedule_gap_repair", lambda repair_key: released.append(repair_key))
        stream._begin_transport_incident("business", 1, "failed_fixture", {("FAILEDRECOVERY", "1m")})
        task = stream._transport_reconcile_tasks["business"]
        await asyncio.sleep(0)
        stream._set_subscription_state(key, "FAILED", generation=1, retry_allowed=False)
        await task
        incident = stream._transport_incidents["business"]
        assert incident["active"] is False
        assert incident["recovery_status"] == "FAILED"
        assert incident["failure_reason"] == "subscription_readiness_timeout"
        assert released == [("FAILEDRECOVERY", "1m")]
        assert stream._gap_event_counts.get("transport_incident_reconciled", 0) == 0
        stream._running = False

    asyncio.run(scenario())


def test_330_instrument_twenty_adjacent_1m_boundaries_have_zero_repairs():
    stream = WebSocketStream()
    stream._running = True
    stream._transport_observability_enabled = True
    stream._active_generations["business"] = 1
    base = float(int(time.time() // 60) * 60 - 40 * 60)
    for index in range(330):
        coin = f"HEALTHY{index}"
        market._cache_1m[coin] = _complete_timeframe_row("1m", 30, base)
        key = ("business", "candle1m", f"{coin}-USDT-SWAP")
        stream._subscription_state[key] = {"status": "ACTIVE", "generation": 1}
        stream._transport_key_stats[key] = {
            "generation": 1, "last_wire_rx_monotonic": time.monotonic(),
        }
    for boundary in range(20):
        timestamp = base + (30 + boundary) * 60
        for index in range(330):
            stream._upsert_candle(
                f"HEALTHY{index}-USDT-SWAP", "1m",
                _candle(timestamp, 200 + index + boundary, "1"), generation=1,
            )
    assert stream.stats()["gap_decision_counts"]["GAP"] == 0
    assert stream._repair_tasks == {}
    assert stream._pending_repairs == {}
    stream._running = False


def test_dispatch_backpressure_is_a_connection_fault_not_a_silent_drop(monkeypatch):
    async def scenario():
        stream = WebSocketStream()
        stream._running = True
        stream._active_generations["business"] = 1
        stream._heartbeat_state["business"] = {"generation": 1}
        queue = asyncio.Queue(maxsize=1)
        await queue.put(("occupied", time.monotonic()))
        monkeypatch.setattr(ws_stream, "_DISPATCH_ENQUEUE_TIMEOUT_SEC", 0.01)
        messages = [json.dumps({
            "arg": {"channel": "candle1m", "instId": "BACKPRESSURE-USDT-SWAP"},
            "data": [_candle(1_700_000_000, 100, "1")],
        })]
        await stream._receive_loop(_BurstWS(messages), public=False, generation=1, queue=queue)
        assert stream.stats()["transport"]["domains"]["business"]["backpressure_faults"] == 1
        assert stream._fault_events["business"].is_set()
        stream._running = False

    asyncio.run(scenario())


def test_owned_task_exception_is_observed_and_classified():
    async def scenario():
        stream = WebSocketStream()

        async def fail():
            raise RuntimeError("heartbeat fixture fault")

        stream._create_owned_task(fail(), name="ws-test-fault")
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        assert stream.stats()["task_exception_count"] == 1
        assert stream.stats()["task_exception_last"]["exception"] == "RuntimeError"
        assert stream.owned_task_names() == []

    asyncio.run(scenario())


def test_canonical_history_completeness_survives_reconnect_generations():
    base = float(int(time.time() // 60) * 60 - 40 * 60)
    timestamps = [base + index * 60 for index in range(40)]
    market._cache_1m["COMPLETE"] = {
        "timestamps": timestamps, "closes": [1.0] * 40,
        "confirmed": [True] * 40, "ready": True,
        "source": "okx_public_candles", "stale": False,
    }
    stream = WebSocketStream()
    assert market.confirmed_history_complete(market._cache_1m["COMPLETE"], "1m", 30)
    stream._reset_connection_domain(False, 1)
    stream._reset_connection_domain(False, 2)
    assert market.confirmed_history_complete(market._cache_1m["COMPLETE"], "1m", 30)
    assert stream._gap_state.get(("COMPLETE", "1m"), {}).get("active", False) is False


def test_missing_confirmed_boundary_is_incomplete_even_with_fresh_forming_bar():
    base = float(int(time.time() // 60) * 60 - 120)
    row = {
        "timestamps": [base, base + 120], "closes": [1.0, 2.0],
        "confirmed": [True, False], "ready": True,
    }
    assert market.confirmed_history_complete(row, "1m", 1) is False


def test_reconnect_without_boundary_crossing_does_not_schedule_repair():
    stream = WebSocketStream()
    stream._running = True
    base = float(int(time.time() // 60) * 60 - 180)
    row = {
        "timestamps": [base, base + 60, base + 120],
        "confirmed": [True, True, False], "ready": True,
    }
    stream._check_continuity("NOGAP", "1m", row, now=base + 121)
    assert stream._gap_state.get(("NOGAP", "1m"), {}).get("active", False) is False
    assert stream._repair_tasks == {}
    stream._running = False


def test_twenty_no_gap_generations_preserve_history_and_do_not_repair():
    stream = WebSocketStream()
    base = float(int(time.time() // 60) * 60 - 40 * 60)
    market._cache_1m["GENERATIONS"] = {
        "timestamps": [base + index * 60 for index in range(40)],
        "closes": [1.0] * 40, "confirmed": [True] * 40, "ready": True,
    }
    for generation in range(1, 21):
        stream._reset_connection_domain(False, generation)
        stream._check_continuity("GENERATIONS", "1m", market._cache_1m["GENERATIONS"])
    assert market.confirmed_history_complete(market._cache_1m["GENERATIONS"], "1m", 30)
    assert stream._repair_count == 0
    assert stream._pending_repairs == {}


def test_same_generation_fault_is_coalesced_to_one_reconnect_signal():
    stream = WebSocketStream()
    stream._active_generations["business"] = 4
    stream._heartbeat_state["business"] = {"generation": 4}
    assert stream._report_connection_fault(False, 4, "pong timeout") is True
    assert stream._report_connection_fault(False, 4, "reader closed") is False
    assert stream.stats()["heartbeat_metrics"]["business"]["faults"] == 1


def test_repeated_gap_observation_unions_and_coalesces():
    stream = WebSocketStream()
    stream._mark_gap("UNION", "1m", 120.0, 180.0)
    stream._mark_gap("UNION", "1m", 180.0, 300.0)
    stream._mark_gap("UNION", "1m", 120.0, 180.0)
    state = stream._gap_state[("UNION", "1m")]
    assert (state["start"], state["end"]) == (120.0, 300.0)
    assert stream.stats()["gap_range_extensions"] == 1
    assert stream.stats()["gap_duplicate_coalesced"] == 1


def test_full_universe_three_injected_gap_keys_repair_only_those_keys(monkeypatch):
    async def scenario():
        stream = WebSocketStream()
        stream._running = True
        calls = []

        async def repair(coin, timeframe, bar, limit, **kwargs):
            calls.append((coin, timeframe, kwargs.get("caller")))
            base = 1_700_000_000.0
            market._cache_for_timeframe(timeframe)[coin] = {
                "timestamps": [base + index * ws_stream._TIMEFRAME_SECONDS[timeframe] for index in range(40)],
                "closes": [1.0] * 40,
                "confirmed": [True] * 40,
                "ready": True,
            }
            return True

        monkeypatch.setattr(market, "_fetch_and_store", repair)
        for coin, timeframe in (("GAP0", "1m"), ("GAP1", "5m"), ("GAP2", "1h")):
            stream._mark_gap(coin, timeframe, 120.0, 180.0)
        tasks = list(stream._repair_tasks.values())
        await asyncio.gather(*tasks)
        assert sorted(calls) == [
            ("GAP0", "1m", "ws_gap_recovery"),
            ("GAP1", "5m", "ws_gap_recovery"),
            ("GAP2", "1h", "ws_gap_recovery"),
        ]
        assert len(calls) == 3
        stream._running = False

    asyncio.run(scenario())


def test_snapshot_reads_do_not_enqueue_gap_recovery(monkeypatch):
    calls = []

    async def repair(*args, **kwargs):
        calls.append(kwargs.get("caller"))
        return True

    monkeypatch.setattr(market, "_fetch_and_store", repair)
    for _ in range(25):
        market.timeframe_snapshot("READONLY", "1m", 1)
    assert calls == []


def test_candle_physical_request_carries_explicit_provenance(monkeypatch):
    async def scenario():
        market.reset_request_reason_metrics()
        market._cache_1m.pop("PROVENANCE", None)
        observed = []

        async def fetch_once(_coin, _bar, _limit, **kwargs):
            observed.append(dict(market._active_provenance.get() or {}))
            return _seed_row()

        monkeypatch.setattr(market, "_fetch_once", fetch_once)
        assert await market._fetch_and_store(
            "PROVENANCE", "1m", "1m", 30,
            caller="ws_gap_recovery",
            request_reason="PROVEN_GAP_REPAIR",
            trigger_type="WS_TIMESTAMP_GAP",
            trigger_id="gap:PROVENANCE:1m:120:180",
            missing_start=120.0,
            missing_end=180.0,
            market_state_version=7,
            ws_generation=3,
            scheduling_module="ws_stream",
            scheduling_function="_repair_gap",
        )
        assert observed and observed[0]["request_reason"] == "PROVEN_GAP_REPAIR"
        assert observed[0]["trigger_type"] == "WS_TIMESTAMP_GAP"
        assert observed[0]["trigger_id"].startswith("gap:PROVENANCE")
        assert observed[0]["missing_start"] == 120.0
        assert observed[0]["request_reason"] == "PROVEN_GAP_REPAIR"

    asyncio.run(scenario())


def test_governed_candle_transport_records_explicit_provenance(monkeypatch):
    class Response:
        status_code = 200

        def raise_for_status(self):
            return None

        def json(self):
            return {"code": "0", "data": _seed_row()}

    async def admission(*_args, **kwargs):
        return Response(), "fixture"

    async def scenario():
        market.reset_request_reason_metrics()
        monkeypatch.setattr(market, "run_with_rate_admission", admission)
        token = market._active_provenance.set({
            "request_reason": "PROVEN_GAP_REPAIR",
            "trigger_type": "RECONNECT_CONTINUITY_GAP",
            "trigger_id": "fixture-gap",
            "instrument": "BOUNDARY",
            "timeframe": "1m",
            "missing_start": 60.0,
            "missing_end": 120.0,
            "market_state_version": 4,
            "ws_generation": 2,
        })
        try:
            assert await market._fetch_once("BOUNDARY", "1m", 30) is not None
        finally:
            market._active_provenance.reset(token)
        metrics = market.request_reason_snapshot()
        row = metrics["by_timeframe"]["1m"]["PROVEN_GAP_REPAIR"]
        assert row["physical"] == 1
        assert metrics["physical_samples"][-1]["trigger_id"] == "fixture-gap"

    asyncio.run(scenario())


def test_cold_bootstrap_is_owned_single_flight_and_reason_separated(monkeypatch):
    async def scenario():
        import latency_telemetry
        market.reset_request_reason_metrics()
        latency_telemetry.reset_market_request_metrics()
        market._cache_1m.pop("BOOT", None)
        market._cache_5m.pop("BOOT", None)
        calls = []

        async def fetch(_coin, bar, limit):
            calls.append((bar, limit))
            await asyncio.sleep(0)
            return _seed_row(max(40, limit))

        monkeypatch.setattr(market, "_fetch", fetch)
        result = await asyncio.gather(
            market.bootstrap_universe(
                {"BOOT"}, requirements={"1m": 30, "5m": 40}, max_concurrency=1,
            ),
            market.bootstrap_universe(
                {"BOOT"}, requirements={"1m": 30, "5m": 40}, max_concurrency=1,
            ),
        )
        assert result[0]["requested"] == result[1]["requested"] == 2
        assert len(calls) == 2
        metrics = latency_telemetry.market_request_snapshot()["by_request_reason"]
        assert metrics["COLD_BOOTSTRAP"]["underlying"] == 2

    asyncio.run(scenario())


def test_repeated_not_ready_reads_are_network_side_effect_free(monkeypatch):
    stream = WebSocketStream()
    stream._coins = ["NOTREADY"]
    key = ("business", "candle1m", "NOTREADY-USDT-SWAP")
    stream._subscription_expected.add(key)
    stream._set_subscription_state(key, "FAILED", generation=1, retry_allowed=True)
    calls = []

    async def forbidden(*_args, **_kwargs):
        calls.append(True)
        raise AssertionError("read path attempted acquisition")

    monkeypatch.setattr(market, "_fetch_and_store", forbidden)
    for _ in range(28 * 5):
        market.timeframe_snapshot("NOTREADY", "1m", 30)
        stream.market_data_ready_for({"NOTREADY"}, ("1m",))
    assert calls == []


def test_scan_and_regime_reads_do_not_call_candle_acquisition():
    import inspect
    import regime_detector
    import trading_engine

    scan_source = inspect.getsource(trading_engine.MultiEngine._scan_cycle_impl)
    regime_source = inspect.getsource(regime_detector.update_regime)
    assert "await refresh_all" not in scan_source
    assert "refresh_15m_all" not in scan_source
    assert "fetch_candles_5m" not in regime_source


def test_full_327_instrument_reads_remain_pure_across_all_timeframes(monkeypatch):
    coins = {f"C{index:03d}" for index in range(327)}
    stream = WebSocketStream()
    stream._coins = sorted(coins)
    stream._running = True
    calls = []

    async def forbidden(*_args, **_kwargs):
        calls.append(True)
        raise AssertionError("scanner read attempted candle REST")

    monkeypatch.setattr(market, "_fetch_and_store", forbidden)
    for _ in range(3):
        readiness = market.market_readiness_snapshot(coins)
        assert len(readiness["symbols"]) == 327
        for coin in coins:
            for timeframe in stream._timeframes:
                market.timeframe_snapshot(coin, timeframe, 1)
    assert calls == []


def test_request_reason_metrics_distinguish_bootstrap_and_gap_repair():
    async def scenario():
        import latency_telemetry
        market.reset_request_reason_metrics()
        latency_telemetry.reset_market_request_metrics()
        market._cache_1m.pop("REASONS", None)
        market._cache_5m.pop("REASONS", None)

        async def fetch(_coin, _bar, limit):
            return _seed_row(max(40, limit))

        async def repair(_coin, _bar, limit):
            return _seed_row(max(40, limit))

        original = market._fetch
        market._fetch = fetch
        try:
            await market.bootstrap_universe({"REASONS"}, requirements={"1m": 30}, max_concurrency=1)
            await market._fetch_and_store(
                "REASONS", "5m", "5m", 30,
                request_reason="PROVEN_GAP_REPAIR",
                trigger_type="RECONNECT_CONTINUITY_GAP",
                trigger_id="gap:REASONS:5m",
            )
        finally:
            market._fetch = original
        snapshot = latency_telemetry.market_request_snapshot()["by_request_reason"]
        assert snapshot["COLD_BOOTSTRAP"]["underlying"] == 1
        assert snapshot["PROVEN_GAP_REPAIR"]["underlying"] == 1

    asyncio.run(scenario())


def test_transport_key_telemetry_is_hard_bounded_on_wire_and_apply_paths():
    async def scenario():
        market._cache_1m.pop("SATURATED", None)
        stream = WebSocketStream()
        stream._running = True
        stream._active_generations["business"] = 1
        stream._heartbeat_state["business"] = {"generation": 1}
        for index in range(ws_stream._MAX_TRANSPORT_KEY_STATS):
            stream._transport_key_stats[("business", "candle1m", f"CAP{index}-USDT-SWAP")] = {
                "generation": 1,
                "last_wire_rx_monotonic": float(index),
            }
        existing = ("business", "candle1m", "CAP0-USDT-SWAP")
        wire = json.dumps({
            "connId": "capacity-wire",
            "arg": {"channel": "candle1m", "instId": "WIRECAP-USDT-SWAP"},
            "data": [_candle(1_700_000_000, 101, "1")],
        })
        stream._observe_wire_frame(wire, public=False, generation=1, received_at=time.monotonic())
        assert len(stream._transport_key_stats) == ws_stream._MAX_TRANSPORT_KEY_STATS
        stream._observe_wire_frame(
            json.dumps({"arg": {"channel": "candle1m", "instId": existing[2]}, "data": []}),
            public=False, generation=1, received_at=time.monotonic(),
        )
        assert len(stream._transport_key_stats) == ws_stream._MAX_TRANSPORT_KEY_STATS
        assert existing in stream._transport_key_stats
        applied = json.dumps({
            "connId": "capacity-apply",
            "arg": {"channel": "candle1m", "instId": "APPLYCAP-USDT-SWAP"},
            "data": [_candle(1_700_000_060, 102, "1")],
        })
        stream._record_frame_applied(
            applied, public=False, generation=1,
            received_at=time.monotonic(), applied_at=time.monotonic(),
        )
        assert len(stream._transport_key_stats) == ws_stream._MAX_TRANSPORT_KEY_STATS
        canonical = json.dumps({
            "arg": {"channel": "candle1m", "instId": "SATURATED-USDT-SWAP"},
            "data": [_candle(1_700_000_120, 103, "1")],
        })
        await stream._receive_loop(_BurstWS([canonical]), public=False, generation=1)
        assert market._cache_1m["SATURATED"]["closes"] == [103.0]
        assert len(stream._transport_key_stats) == ws_stream._MAX_TRANSPORT_KEY_STATS
        stream._running = False

    asyncio.run(scenario())


def test_evicted_transport_telemetry_is_unknown_but_explicit_stalls_remain_actionable(monkeypatch):
    async def scenario():
        stream = WebSocketStream()
        stream._running = True
        stream._transport_observability_enabled = True
        stream._active_generations["business"] = 1
        unknown = ("EVICTED", "1m")
        unknown_sub = ("business", "candle1m", "EVICTED-USDT-SWAP")
        stream._subscription_state[unknown_sub] = {"status": "ACTIVE", "generation": 1}
        assert stream._transport_is_stalled(unknown, time.monotonic()) is False
        scheduled = []
        monkeypatch.setattr(ws_stream, "_TRANSPORT_GAP_ADMISSION_SEC", 0.0)
        monkeypatch.setattr(stream, "_schedule_gap_repair", lambda key: scheduled.append(key))
        for index in range(ws_stream._TRANSPORT_COHORT_MIN_KEYS):
            coin = f"UNKNOWN{index}"
            stream._subscription_state[("business", "candle1m", f"{coin}-USDT-SWAP")] = {
                "status": "ACTIVE", "generation": 1,
            }
            stream._mark_gap(coin, "1m", 1_700_000_000, 1_700_000_000)
        await stream._transport_admission_tasks["business"]
        assert stream._transport_incidents["business"] is None
        assert sorted(scheduled) == [(f"UNKNOWN{index}", "1m") for index in range(8)]
        stalled = ("STALLED", "1m")
        stream._subscription_state[("business", "candle1m", "STALLED-USDT-SWAP")] = {
            "status": "ACTIVE", "generation": 1,
        }
        stream._transport_key_stats[("business", "candle1m", "STALLED-USDT-SWAP")] = {
            "generation": 1,
            "last_wire_rx_monotonic": time.monotonic() - ws_stream._TRANSPORT_KEY_STALL_SEC - 1,
        }
        assert stream._transport_is_stalled(stalled, time.monotonic()) is True
        stream._running = False

    asyncio.run(scenario())


def test_backpressure_drop_is_accounted_once_and_recovers_candle_continuity(monkeypatch):
    async def scenario():
        market._cache_5m.pop("BPDROP", None)
        stream = WebSocketStream()
        stream._running = True
        stream._active_generations["business"] = 1
        stream._heartbeat_state["business"] = {"generation": 1}
        queue = asyncio.Queue(maxsize=1)
        await queue.put(("occupied", time.monotonic()))
        scheduled = []
        monkeypatch.setattr(ws_stream, "_DISPATCH_ENQUEUE_TIMEOUT_SEC", 0.01)
        monkeypatch.setattr(stream, "_schedule_gap_repair", lambda key: scheduled.append(key))
        frame = json.dumps({
            "arg": {"channel": "candle5m", "instId": "BPDROP-USDT-SWAP"},
            "data": [_candle(1_700_000_000, 101, "1")],
        })
        await asyncio.wait_for(
            stream._receive_loop(_BurstWS([frame]), public=False, generation=1, queue=queue),
            timeout=0.1,
        )
        # Recovery is owned and cleaned up as soon as the task completes; on
        # a fast deterministic fixture it may already be absent by inspection.
        recovery = next(iter(stream._backpressure_recovery_tasks.values()), None)
        if recovery is not None:
            await recovery
        metrics = stream.stats()["transport"]["domains"]["business"]
        assert metrics["frames_received"] == 1
        assert metrics["frames_dropped"] == 1
        assert metrics["backpressure_faults"] == 1
        assert stream._fault_events["business"].is_set() is True
        assert market._cache_5m.get("BPDROP", {}).get("timestamps", []) == []
        assert scheduled == [("BPDROP", "5m")]
        assert stream._transport_incidents["business"] is None

        stream._active_generations["business"] = 2
        await stream._receive_loop(_BurstWS([frame]), public=False, generation=1, queue=queue)
        assert metrics["frames_dropped"] == 1
        assert stream.stats()["transport"]["domains"]["business"]["stale_generation_frames_dropped"] == 1
        stream._running = False

    asyncio.run(scenario())


def test_queue_latency_is_durable_and_socket_teardown_is_exact(monkeypatch):
    async def scenario():
        stream = WebSocketStream()
        stream._running = True
        stream._active_generations["business"] = 2
        clock = [100.0]
        monkeypatch.setattr(ws_stream.time, "monotonic", lambda: clock[0])
        old_id = ("business", 1, 101)
        new_id = ("business", 2, 202)
        old_queue = asyncio.Queue()
        new_queue = asyncio.Queue()
        stream._dispatch_queues[old_id] = old_queue
        stream._dispatch_queues[new_id] = new_queue
        await new_queue.put(("delayed", 90.0))
        stream._update_queue_metrics("business", new_queue, new_id)
        stream._record_queue_dequeue_latency("business", 10.0, new_id)
        new_queue.get_nowait()
        new_queue.task_done()
        clock[0] = 101.0
        stream._update_queue_metrics("business", new_queue, new_id)
        stream._record_queue_dequeue_latency("business", 0.0, new_id)
        domain = stream.stats()["transport"]["domains"]["business"]
        per_socket = stream.stats()["transport"]["queues"]["business|2|202"]
        assert domain["queue_oldest_age_s"] == 0.0
        assert domain["queue_max_dequeue_latency_s"] == 10.0
        assert per_socket["queue_max_dequeue_latency_s"] == 10.0
        stream._record_queue_dequeue_latency("business", 8.0, old_id)
        assert stream.stats()["transport"]["domains"]["business"]["queue_max_dequeue_latency_s"] == 10.0
        assert stream.stats()["transport"]["queues"]["business|1|101"]["queue_last_dequeue_latency_s"] == 8.0

        # These metric-only fixture registrations were not lifecycle-created
        # sockets and must not be removed by an unrelated socket teardown.
        for connection_id in (old_id, new_id):
            stream._dispatch_queues.pop(connection_id)
            stream._dispatch_queue_metrics.pop(connection_id)
        assert old_id not in stream._dispatch_queues
        assert new_id not in stream._dispatch_queues

        class BlockingWS(_FakeWS):
            def __aiter__(self):
                return self

            async def __anext__(self):
                await asyncio.Event().wait()

        old_ws = BlockingWS()
        replacement_ws = BlockingWS()
        stream._reset_connection_domain(False, 2)
        stream._generation_heartbeat["business"][2]["socket_id"] = id(old_ws)
        old_tasks = stream._start_connection_tasks(old_ws, public=False, generation=2)
        old_connection = ("business", 2, id(old_ws))
        stream._reset_connection_domain(False, 3, activate=False)
        stream._generation_heartbeat["business"][3]["socket_id"] = id(replacement_ws)
        stream._pending_connections["business"]["socket_id"] = id(replacement_ws)
        replacement_tasks = stream._start_connection_tasks(
            replacement_ws, public=False, generation=3, handover=True,
        )
        replacement_connection = ("business", 3, id(replacement_ws))
        stream._begin_transport_incident("business", 2, "teardown_fixture", {("TEARDOWN", "1m")})
        reconcile = stream._transport_reconcile_tasks["business"]
        await stream._close_socket(old_ws)
        assert old_connection not in stream._dispatch_queues
        assert replacement_connection in stream._dispatch_queues
        assert old_connection not in stream._connection_tasks
        assert replacement_connection in stream._connection_tasks
        reconcile.cancel()
        with pytest.raises(asyncio.CancelledError):
            await reconcile
        assert stream._transport_reconcile_owners["business"] is None
        stream._running = False
        await stream._cancel_owned_tasks([*old_tasks, *replacement_tasks])
        await stream._close_socket(replacement_ws)
        assert replacement_connection not in stream._dispatch_queues
        assert replacement_connection not in stream._connection_tasks
        assert stream._dispatch_queues == {}
        assert stream._dispatch_queue_metrics == {}
        assert stream._connection_tasks == {}
        assert stream._transport_reconcile_owners["business"] is None
        assert stream.owned_task_names() == []

    asyncio.run(scenario())


def test_overlap_keeps_physical_queue_identity_until_own_socket_cleanup():
    async def scenario():
        stream = WebSocketStream()
        stream._running = True

        class BlockingWS(_FakeWS):
            def __aiter__(self):
                return self

            async def __anext__(self):
                await asyncio.Event().wait()

        old_ws = BlockingWS()
        pending_ws = BlockingWS()
        stream._own_socket(old_ws)
        stream._own_socket(pending_ws)
        stream._reset_connection_domain(False, 1)
        stream._generation_heartbeat["business"][1]["socket_id"] = id(old_ws)
        old_tasks = stream._start_connection_tasks(old_ws, public=False, generation=1)
        old_id = ("business", 1, id(old_ws))
        old_queue = stream._dispatch_queues[old_id]
        stream._transport_metrics["business"]["conn_id"] = "old-connection"

        stream._reset_connection_domain(False, 2, activate=False)
        stream._generation_heartbeat["business"][2]["socket_id"] = id(pending_ws)
        stream._pending_connections["business"]["socket_id"] = id(pending_ws)
        pending_tasks = stream._start_connection_tasks(
            pending_ws, public=False, generation=2, handover=True,
        )
        pending_id = ("business", 2, id(pending_ws))
        pending_queue = stream._dispatch_queues[pending_id]
        assert old_id in stream._dispatch_queues
        assert pending_id in stream._dispatch_queues
        assert old_queue is not pending_queue
        assert stream._active_generations["business"] == 1
        assert stream._transport_metrics["business"]["conn_id"] == "old-connection"

        pending_frame = json.dumps({
            "connId": "pending-connection",
            "arg": {"channel": "candle1m", "instId": "OVERLAPQ-USDT-SWAP"},
            "data": [_candle(1_700_000_000, 101, "1")],
        })
        await stream._receive_loop(
            _BurstWS([pending_frame]), public=False, generation=2,
            queue=pending_queue, connection_id=pending_id,
        )
        assert pending_queue.empty()
        assert stream._transport_metrics["business"]["conn_id"] == "old-connection"

        pending_key = ("business", "candle1m", "OVERLAPQ-USDT-SWAP")
        stream._set_subscription_state(pending_key, "REQUESTED", generation=2)
        pending_profile = stream._pending_subscriptions["business"]
        assert pending_profile is not None
        assert pending_profile["ready_event"].is_set() is False
        assert stream._subscription_ready(False, 2) is False
        assert stream._active_generations["business"] == 1
        stream._handle_message(json.dumps({
            "event": "subscribe",
            "arg": {"channel": "candle1m", "instId": "OVERLAPQ-USDT-SWAP"},
        }), public=False, generation=2, control_only=True)
        assert pending_profile["ready_event"].is_set() is True
        assert stream._subscription_ready(False, 2) is True
        assert stream._active_generations["business"] == 1

        stream._promote_pending_connection(False, 2)
        assert stream._active_generations["business"] == 2
        stale_old = json.dumps({
            "connId": "old-after-promotion",
            "arg": {"channel": "candle1m", "instId": "OVERLAPQ-USDT-SWAP"},
            "data": [_candle(1_700_000_060, 102, "1")],
        })
        await stream._receive_loop(
            _BurstWS([stale_old]), public=False, generation=1,
            queue=old_queue, connection_id=old_id,
        )
        assert old_queue.empty()
        assert stream._transport_metrics["business"]["conn_id"] == "old-connection"

        await stream._receive_loop(
            _BurstWS([pending_frame]), public=False, generation=2,
            queue=pending_queue, connection_id=pending_id,
        )
        await pending_queue.join()
        assert stream._transport_metrics["business"]["conn_id"] == "pending-connection"
        assert stream._transport_metrics["business"]["conn_id_changes"] == 1
        await stream._close_socket(old_ws)
        assert old_id not in stream._dispatch_queues
        assert pending_id in stream._dispatch_queues

        stream._running = False
        await stream._cancel_owned_tasks([*old_tasks, *pending_tasks])
        await stream._close_socket(pending_ws)

    asyncio.run(scenario())

import asyncio
import inspect
import threading
import time

import pytest


def _ticker_payload(last=100.0):
    return {
        "code": "0",
        "data": [{
            "last": str(last), "bidPx": str(last - 0.1), "askPx": str(last + 0.1),
            "vol24h": "10", "volCcy24h": "1000",
        }],
    }


def test_ticker_same_key_single_flight(monkeypatch):
    import okx_client

    okx_client._ticker_cache.clear()
    okx_client._ticker_inflight.clear()
    okx_client.reset_ticker_coalescing_metrics()
    calls = 0
    entered = asyncio.Event()
    release = asyncio.Event()

    async def request(_method, _path, *, params=None, priority=None, **_kwargs):
        nonlocal calls
        calls += 1
        entered.set()
        await release.wait()
        return _ticker_payload()

    async def run():
        client = okx_client.OKXClient()
        client._request = request
        tasks = [asyncio.create_task(client.get_ticker("BTC")) for _ in range(10)]
        await entered.wait()
        release.set()
        results = await asyncio.gather(*tasks)
        return results

    results = asyncio.run(run())
    assert calls == 1
    assert len(results) == 10
    metrics = okx_client.get_ticker_coalescing_snapshot()
    assert metrics["underlying_requests"] == 1
    assert metrics["coalesced_callers"] == 9


def test_ticker_different_instruments_are_not_globally_serialized():
    import okx_client

    okx_client._ticker_cache.clear()
    okx_client._ticker_inflight.clear()
    active = 0
    maximum = 0

    async def run():
        nonlocal active, maximum

        async def request(_method, _path, *, params=None, priority=None, **_kwargs):
            nonlocal active, maximum
            active += 1
            maximum = max(maximum, active)
            await asyncio.sleep(0.02)
            active -= 1
            return _ticker_payload()

        client = okx_client.OKXClient()
        client._request = request
        return await asyncio.gather(*(client.get_ticker(coin) for coin in ("BTC", "ETH")))

    asyncio.run(run())
    assert maximum == 2


def test_ticker_failure_clears_inflight_and_later_request_can_retry(monkeypatch):
    import okx_client

    okx_client._ticker_cache.clear()
    okx_client._ticker_inflight.clear()
    attempts = 0

    async def run():
        nonlocal attempts
        client = okx_client.OKXClient()

        async def fail(*_args, **_kwargs):
            nonlocal attempts
            attempts += 1
            raise RuntimeError("temporary")

        client._request = fail
        with pytest.raises(RuntimeError):
            await client.get_ticker("BTC")
        await asyncio.sleep(0)
        client._request = lambda *_args, **_kwargs: asyncio.sleep(0, result=_ticker_payload(101.0))
        return await client.get_ticker("BTC")

    result = asyncio.run(run())
    assert result["last"] == 101.0
    assert attempts == 1


def test_cancelled_ticker_joiner_does_not_cancel_shared_fetch():
    import okx_client

    okx_client._ticker_cache.clear()
    okx_client._ticker_inflight.clear()
    entered = asyncio.Event()
    release = asyncio.Event()
    calls = 0

    async def run():
        nonlocal calls
        client = okx_client.OKXClient()

        async def request(*_args, **_kwargs):
            nonlocal calls
            calls += 1
            entered.set()
            await release.wait()
            return _ticker_payload()

        client._request = request
        first = asyncio.create_task(client.get_ticker("BTC"))
        await entered.wait()
        second = asyncio.create_task(client.get_ticker("BTC"))
        first.cancel()
        with pytest.raises(asyncio.CancelledError):
            await first
        release.set()
        return await second

    assert asyncio.run(run())["last"] == 100.0
    assert calls == 1


def _candle_rows(count=4, start_ms=None, interval_ms=60_000):
    if start_ms is None:
        end_ms = (int(time.time()) // 60) * 60_000
        start_ms = end_ms - (max(int(count), 1) - 1) * interval_ms
    return [[
        str(int(start_ms) + index * int(interval_ms)),
        "1", "2", "0.5", "1.5", "10", "0", "0", "1",
    ] for index in range(max(int(count), 1))]


def test_htf_same_key_single_flight_and_versioned_snapshot(monkeypatch):
    import market_data_stream as market

    market._candle_inflight.clear()
    market._cache_4h.clear()
    market._snapshot_generation.clear()
    calls = 0

    async def fetch(_coin, _bar, _limit):
        nonlocal calls
        calls += 1
        await asyncio.sleep(0.01)
        return _candle_rows()

    async def run():
        monkeypatch.setattr(market, "_fetch", fetch)
        return await asyncio.gather(market.fetch_candles_4h("BTC"), market.fetch_candles_4h("BTC"))

    assert asyncio.run(run()) == [True, True]
    assert calls == 1
    snapshot = market.timeframe_snapshot("BTC", "4h", 1)
    assert snapshot["version"] >= 1
    assert snapshot["observed_at"]


def test_htf_different_keys_progress_concurrently(monkeypatch):
    import market_data_stream as market

    market._candle_inflight.clear()
    market._cache_4h.clear()
    active = 0
    maximum = 0

    async def fetch(_coin, _bar, _limit):
        nonlocal active, maximum
        active += 1
        maximum = max(maximum, active)
        await asyncio.sleep(0.02)
        active -= 1
        return _candle_rows()

    async def run():
        monkeypatch.setattr(market, "_fetch", fetch)
        return await asyncio.gather(market.fetch_candles_4h("BTC"), market.fetch_candles_4h("ETH"))

    assert asyncio.run(run()) == [True, True]
    assert maximum == 2


def test_metadata_refresh_same_flight_and_failure_cleanup(tmp_path):
    from instrument_registry import InstrumentRegistry

    registry = InstrumentRegistry(tmp_path / "instruments.json")
    row = {
        "instId": "BTC-USDT-SWAP", "instType": "SWAP", "settleCcy": "USDT",
        "ctVal": "1", "ctValCcy": "USDT", "ctType": "linear",
        "lotSz": "1", "minSz": "1", "tickSz": "0.1", "state": "live",
    }
    calls = 0

    class Client:
        async def _request(self, *_args, **_kwargs):
            nonlocal calls
            calls += 1
            await asyncio.sleep(0.01)
            return {"code": "0", "data": [row]}

    async def run():
        return await asyncio.gather(
            registry.refresh(Client(), force=True, priority="P0"),
            registry.refresh(Client(), force=True, priority="background"),
        )

    assert asyncio.run(run()) == [1, 1]
    assert calls == 1
    assert registry.snapshot()["count"] == 1


def test_metadata_p0_does_not_join_active_p2_refresh(tmp_path):
    from instrument_registry import InstrumentRegistry

    registry = InstrumentRegistry(tmp_path / "instruments.json")
    row = {
        "instId": "BTC-USDT-SWAP", "instType": "SWAP", "settleCcy": "USDT",
        "ctVal": "1", "ctValCcy": "USDT", "ctType": "linear",
        "lotSz": "1", "minSz": "1", "tickSz": "0.1", "state": "live",
    }
    calls = 0
    started = asyncio.Event()
    release = asyncio.Event()

    class Client:
        async def _request(self, *_args, **_kwargs):
            nonlocal calls
            calls += 1
            started.set()
            await release.wait()
            return {"code": "0", "data": [row]}

    async def run():
        background = asyncio.create_task(registry.refresh(Client(), force=True, priority="P2"))
        await started.wait()
        critical = asyncio.create_task(registry.refresh(Client(), force=True, priority="P0"))
        while calls < 2:
            await asyncio.sleep(0)
        release.set()
        return await asyncio.gather(background, critical)

    assert asyncio.run(run()) == [1, 1]
    assert calls == 2


def test_metadata_newer_flight_cannot_be_overwritten_by_older_completion(tmp_path):
    from instrument_registry import InstrumentRegistry

    registry = InstrumentRegistry(tmp_path / "instruments.json")
    rows = {
        "old": {"instId": "BTC-USDT-SWAP", "instType": "SWAP", "settleCcy": "USDT", "ctVal": "1", "ctValCcy": "USDT", "ctType": "linear", "lotSz": "1", "minSz": "1", "tickSz": "0.1", "state": "live"},
        "new": {"instId": "BTC-USDT-SWAP", "instType": "SWAP", "settleCcy": "USDT", "ctVal": "2", "ctValCcy": "USDT", "ctType": "linear", "lotSz": "1", "minSz": "1", "tickSz": "0.1", "state": "live"},
    }
    release_old = asyncio.Event()

    class Client:
        def __init__(self):
            self.calls = 0

        async def _request(self, *_args, **_kwargs):
            self.calls += 1
            if self.calls == 1:
                await release_old.wait()
                return {"code": "0", "data": [rows["old"]]}
            return {"code": "0", "data": [rows["new"]]}

    async def run():
        client = Client()
        old = asyncio.create_task(registry.refresh(client, force=True, priority="P2"))
        await asyncio.sleep(0)
        new = asyncio.create_task(registry.refresh(client, force=True, priority="P0"))
        await new
        release_old.set()
        await old

    asyncio.run(run())
    assert registry.get("BTC").ct_val == 2


def test_metadata_failure_clears_single_flight(tmp_path):
    from instrument_registry import InstrumentRegistry

    registry = InstrumentRegistry(tmp_path / "instruments.json")
    calls = 0

    class Client:
        async def _request(self, *_args, **_kwargs):
            nonlocal calls
            calls += 1
            raise RuntimeError("metadata unavailable")

    async def run():
        with pytest.raises(RuntimeError):
            await registry.refresh(Client(), force=True)
        await asyncio.sleep(0)
        with pytest.raises(RuntimeError):
            await registry.refresh(Client(), force=True)

    asyncio.run(run())
    assert calls == 2
    assert registry.snapshot()["refresh_inflight"] is False


def test_telegram_position_snapshot_is_bounded_and_truthful():
    import telegram_backend

    class Account:
        async def get_account_snapshot(self, **_kwargs):
            return {"total_eq_usdt": 1.0}

    async def slow_position_loader():
        await asyncio.sleep(2.0)
        return {"ok": True, "positions": {}}

    started = time.monotonic()
    account, positions = asyncio.run(
        telegram_backend.fast_operational_exchange_snapshot(
            Account(), slow_position_loader, timeout_sec=0.5,
        )
    )
    assert time.monotonic() - started < 0.8
    assert positions["stale"] is True
    assert positions["source"] in {"UNAVAILABLE", "STALE", "CACHED_OKX"}
    assert account["source"] == "okx_authenticated_account"


def test_position_snapshot_preserves_stale_loader_truth(monkeypatch, tmp_path):
    import telegram_backend

    monkeypatch.setattr(telegram_backend, "_OPERATIONAL_CACHE_PATH", tmp_path / "snapshot.json")

    class Account:
        async def get_account_snapshot(self, **_kwargs):
            return {"total_eq_usdt": 1.0, "observed_at": "2020-01-01T00:00:00+00:00"}

    async def loader():
        return {
            "ok": True,
            "positions": {"XRP": {"pos": "1"}},
            "observed_at": "2020-01-01T00:00:00+00:00",
            "stale": True,
            "source": "CACHED_OKX",
        }

    _account, positions = asyncio.run(
        telegram_backend.fast_operational_exchange_snapshot(Account(), loader, timeout_sec=0.5)
    )
    assert positions["stale"] is True
    assert positions["observed_at"] == "2020-01-01T00:00:00+00:00"
    assert positions["freshness_status"] == "STALE"
    assert positions["age_sec"] > 0


def test_position_snapshot_without_observation_is_unknown_not_fresh(monkeypatch, tmp_path):
    import telegram_backend

    monkeypatch.setattr(telegram_backend, "_OPERATIONAL_CACHE_PATH", tmp_path / "snapshot.json")

    class Account:
        async def get_account_snapshot(self, **_kwargs):
            return {"total_eq_usdt": 1.0, "observed_at": "2020-01-01T00:00:00+00:00"}

    async def loader():
        return {"ok": True, "positions": {"XRP": {"pos": "1"}}}

    _account, positions = asyncio.run(
        telegram_backend.fast_operational_exchange_snapshot(Account(), loader, timeout_sec=0.5)
    )
    assert positions["stale"] is True
    assert positions["freshness_status"] == "UNKNOWN_FRESHNESS"
    assert positions["observed_at"] == ""
    assert positions["age_sec"] is None


def test_position_snapshot_fresh_loader_preserves_observation(monkeypatch, tmp_path):
    import telegram_backend

    monkeypatch.setattr(telegram_backend, "_OPERATIONAL_CACHE_PATH", tmp_path / "snapshot.json")

    class Account:
        async def get_account_snapshot(self, **_kwargs):
            return {"total_eq_usdt": 1.0, "observed_at": "2020-01-01T00:00:00+00:00"}

    async def loader():
        return {
            "ok": True,
            "positions": {},
            "observed_at": "2026-01-01T00:00:00+00:00",
            "source": "OKX",
        }

    _account, positions = asyncio.run(
        telegram_backend.fast_operational_exchange_snapshot(Account(), loader, timeout_sec=0.5)
    )
    assert positions["stale"] is False
    assert positions["observed_at"] == "2026-01-01T00:00:00+00:00"
    assert positions["semantic_state"] == "NO_OPEN_POSITIONS"


def test_diagnostic_writer_serializes_and_coalesces(monkeypatch):
    import telegram_backend

    calls = 0
    active = 0
    maximum = 0

    def fake_save(snapshot):
        nonlocal calls, active, maximum
        calls += 1
        active += 1
        maximum = max(maximum, active)
        time.sleep(0.02)
        active -= 1
        assert isinstance(snapshot, dict)
        return True

    monkeypatch.setattr(telegram_backend, "_save_operational_cache", fake_save)
    telegram_backend._operational_exchange_cache.clear()

    async def run():
        tasks = []
        for index in range(12):
            telegram_backend._operational_exchange_cache["generation"] = index
            tasks.append(asyncio.create_task(telegram_backend._queue_operational_cache_write()))
        await asyncio.gather(*tasks)
        await telegram_backend._flush_operational_cache_writer()

    asyncio.run(run())
    assert maximum == 1
    assert calls <= 2


def test_atomic_diagnostic_write_failure_preserves_previous_file(monkeypatch, tmp_path):
    import telegram_backend

    target = tmp_path / "snapshot.json"
    target.write_text('{"generation": 1}', encoding="utf-8")
    monkeypatch.setattr(telegram_backend, "_OPERATIONAL_CACHE_PATH", target)
    monkeypatch.setattr(telegram_backend.os, "replace", lambda *_args: (_ for _ in ()).throw(OSError("replace")))

    assert telegram_backend._save_operational_cache({"generation": 2}) is False
    assert target.read_text(encoding="utf-8") == '{"generation": 1}'
    assert not list(tmp_path.glob("*.tmp"))


def test_ticker_priority_direction_and_freshness(monkeypatch):
    import okx_client

    okx_client._ticker_cache.clear()
    okx_client._ticker_inflight.clear()
    calls = []
    entered = asyncio.Event()
    release = asyncio.Event()

    async def run():
        client = okx_client.OKXClient()

        async def request(_method, _path, *, priority=None, **_kwargs):
            calls.append(priority)
            entered.set()
            await release.wait()
            return _ticker_payload()

        client._request = request
        producer = asyncio.create_task(client.get_ticker("BTC", priority="P0", allow_stale=False))
        await entered.wait()
        weak = asyncio.create_task(client.get_ticker("BTC", priority="P2"))
        await asyncio.sleep(0)
        release.set()
        return await asyncio.gather(producer, weak)

    fresh, joined = asyncio.run(run())
    assert calls == ["P0"]
    assert fresh["stale"] is False
    assert joined["observed_at"] == fresh["observed_at"]


def test_ticker_failed_refresh_preserves_old_observation_as_stale():
    import okx_client

    okx_client._ticker_cache.clear()
    okx_client._ticker_inflight.clear()
    old = "2025-01-01T00:00:00+00:00"
    okx_client._ticker_cache["BTC-USDT-SWAP"] = {
        "value": {"last": 100.0}, "ts": 0.0, "observed_at": old,
        "source": "okx_rest", "priority_rank": 2,
    }

    async def run():
        client = okx_client.OKXClient()

        async def fail(*_args, **_kwargs):
            raise RuntimeError("offline")

        client._request = fail
        return await client.get_ticker("BTC", priority="P2")

    result = asyncio.run(run())
    assert result["stale"] is True
    assert result["observed_at"] == old
    assert result["refresh_error"] == "RuntimeError"


def test_ticker_stronger_caller_does_not_join_weaker_flight():
    import okx_client

    okx_client._ticker_cache.clear()
    okx_client._ticker_inflight.clear()
    calls = []
    entered = asyncio.Event()
    release = asyncio.Event()

    async def run():
        client = okx_client.OKXClient()

        async def request(_method, _path, *, priority=None, **_kwargs):
            calls.append(priority)
            if len(calls) == 1:
                entered.set()
                await release.wait()
            return _ticker_payload()

        client._request = request
        weak = asyncio.create_task(client.get_ticker("BTC", priority="P2"))
        await entered.wait()
        strong = asyncio.create_task(client.get_ticker("BTC", priority="P0", allow_stale=False))
        while len(calls) < 2:
            await asyncio.sleep(0)
        release.set()
        return await asyncio.gather(weak, strong)

    asyncio.run(run())
    assert calls == ["P2", "P0"]


def test_htf_request_shape_does_not_collide(monkeypatch):
    import market_data_stream as market

    market._candle_inflight.clear()
    market._cache_4h.clear()
    market._snapshot_generation.clear()
    calls = []

    async def fetch(_coin, _bar, limit):
        calls.append(limit)
        await asyncio.sleep(0.01)
        return _candle_rows()

    async def run():
        monkeypatch.setattr(market, "_fetch", fetch)
        return await asyncio.gather(
            market._fetch_and_store("BTC", "4h", "4H", 100),
            market._fetch_and_store("BTC", "4h", "4H", 200),
        )

    assert asyncio.run(run()) == [True, True]
    assert sorted(calls) == [100, 200]


def _reset_candle_state(market, cache):
    market._candle_inflight.clear()
    cache.clear()
    market._snapshot_generation.clear()


def test_completed_1m_snapshot_reused_sequentially(monkeypatch):
    import latency_telemetry
    import market_data_stream as market

    _reset_candle_state(market, market._cache_1m)
    latency_telemetry.reset_market_request_metrics()
    calls = 0

    async def fetch(_coin, _bar, limit):
        nonlocal calls
        calls += 1
        return _candle_rows(max(limit, 8))

    async def run():
        monkeypatch.setattr(market, "_fetch", fetch)
        assert await market._fetch_and_store(" btc-usdt-swap ", "1M", "1m", 8)
        assert await market._fetch_and_store("BTC-USDT-SWAP", "1m", "1m", 8)

    asyncio.run(run())
    assert calls == 1
    audit = latency_telemetry.market_request_snapshot()
    assert audit["totals"]["underlying_requests"] == 1
    assert audit["totals"]["cache_hits"] == 1
    assert audit["totals"]["sequential_duplicate_count"] == 1
    assert audit["unique_normalized_keys"] == 1


def test_concurrent_same_1m_request_still_single_flights(monkeypatch):
    import latency_telemetry
    import market_data_stream as market

    _reset_candle_state(market, market._cache_1m)
    latency_telemetry.reset_market_request_metrics()
    calls = 0
    release = asyncio.Event()

    async def fetch(_coin, _bar, limit):
        nonlocal calls
        calls += 1
        await release.wait()
        return _candle_rows(max(limit, 8))

    async def run():
        monkeypatch.setattr(market, "_fetch", fetch)
        tasks = [asyncio.create_task(market._fetch_and_store("BTC", "1m", "1m", 8)) for _ in range(10)]
        await asyncio.sleep(0)
        release.set()
        return await asyncio.gather(*tasks)

    assert asyncio.run(run()) == [True] * 10
    assert calls == 1
    totals = latency_telemetry.market_request_snapshot()["totals"]
    assert totals["logical_requests"] == 10
    assert totals["underlying_requests"] == 1
    assert totals["coalesced_callers"] == 9
    assert totals["concurrent_duplicate_count"] == 9


def test_larger_1m_history_satisfies_smaller_request(monkeypatch):
    import market_data_stream as market

    _reset_candle_state(market, market._cache_1m)
    calls = []

    async def fetch(_coin, _bar, limit):
        calls.append(limit)
        return _candle_rows(max(limit, 200))

    async def run():
        monkeypatch.setattr(market, "_fetch", fetch)
        assert await market._fetch_and_store("BTC", "1m", "1m", 200)
        assert await market._fetch_and_store("BTC", "1m", "1m", 50)

    asyncio.run(run())
    assert calls == [200]
    assert market.timeframe_snapshot("BTC", "1m", 50)["request_limit"] == 200


def test_smaller_1m_history_cannot_satisfy_larger_request(monkeypatch):
    import market_data_stream as market

    _reset_candle_state(market, market._cache_1m)
    calls = []

    async def fetch(_coin, _bar, limit):
        calls.append(limit)
        return _candle_rows(max(limit, 50))

    async def run():
        monkeypatch.setattr(market, "_fetch", fetch)
        assert await market._fetch_and_store("BTC", "1m", "1m", 50)
        assert await market._fetch_and_store("BTC", "1m", "1m", 100)

    asyncio.run(run())
    assert calls == [50, 100]


def test_completed_htf_snapshot_reused_at_same_generation(monkeypatch):
    import market_data_stream as market

    _reset_candle_state(market, market._cache_4h)
    calls = 0

    async def fetch(_coin, _bar, limit):
        nonlocal calls
        calls += 1
        return _candle_rows(max(limit, 80))

    async def run():
        monkeypatch.setattr(market, "_fetch", fetch)
        assert await market._fetch_and_store("BTC", "4H", "4h", 80)
        assert await market._fetch_and_store("BTC", "4h", "4H", 60)

    asyncio.run(run())
    assert calls == 1
    assert market.timeframe_snapshot("BTC", "4h", 60)["version"] == 1


def test_signal_age_snapshot_contains_observation_and_handoff_fields():
    import latency_telemetry

    latency_telemetry.reset_signal_age_metrics()
    started = time.time() - 2.0
    decision = time.time()
    latency_telemetry.record_signal_age(
        scan_generation="scan-1",
        instrument="BTC",
        engine="FUJIN",
        decision_time=decision,
        ticker_observed_at=decision - 1.0,
        one_minute_observed_at=decision - 4.0,
        relevant_htf_observed_at=decision - 20.0,
        engine_started_at=started,
        engine_completed_at=decision,
        candidate_created_at=decision + 0.1,
        candidate_handoff_at=decision + 0.3,
    )
    sample = latency_telemetry.signal_age_snapshot()["samples"][-1]
    assert sample["instrument"] == "BTC"
    assert sample["1m_observed_at"] == pytest.approx(decision - 4.0, abs=0.01)
    assert sample["oldest_required_input_age"] >= 19.0
    assert sample["candidate_handoff_delay_sec"] == pytest.approx(0.2, abs=0.02)


def test_engine_dependency_wait_attribution_is_bounded_and_explicit():
    import latency_telemetry

    latency_telemetry.reset_engine_dependency_metrics()
    latency_telemetry.record_engine_dependency(
        engine="FUJIN", instrument="SNDK", operation="get_taker_volume",
        data_type="taker_volume", duration_sec=1.25,
    )
    row = latency_telemetry.engine_dependency_snapshot()["FUJIN|get_taker_volume"]
    assert row["data_type"] == "taker_volume"
    assert row["request_count"] == 1
    assert row["total_duration_sec"] == pytest.approx(1.25)
    assert row["instruments"] == {"SNDK": 1}


def test_expired_1m_generation_refreshes_and_failed_refresh_preserves_truth(monkeypatch):
    import market_data_stream as market

    _reset_candle_state(market, market._cache_1m)
    calls = 0

    async def fetch(_coin, _bar, limit):
        nonlocal calls
        calls += 1
        return _candle_rows(max(limit, 8))

    async def run():
        monkeypatch.setattr(market, "_fetch", fetch)
        assert await market._fetch_and_store("BTC", "1m", "1m", 8)
        first_observed_at = market._cache_1m["BTC"]["observed_at"]
        market._cache_1m["BTC"]["timestamps"][-1] = time.time() - 600.0
        assert await market._fetch_and_store("BTC", "1m", "1m", 8)
        assert calls == 2
        original_observed_at = market._cache_1m["BTC"]["observed_at"]
        assert original_observed_at >= first_observed_at
        market._cache_1m["BTC"]["timestamps"][-1] = time.time() - 600.0

        async def failed(_coin, _bar, _limit):
            return None

        monkeypatch.setattr(market, "_fetch", failed)
        assert await market._fetch_and_store("BTC", "1m", "1m", 8) is False
        snapshot = market.timeframe_snapshot("BTC", "1m", 8)
        assert snapshot["observed_at"] == original_observed_at
        assert snapshot["stale"] is True
        assert snapshot["fresh"] is False
        assert snapshot["refresh_status"] == "failed"
        assert snapshot["refresh_error"] == "candle_refresh_failed"

    asyncio.run(run())


def test_full_universe_28_consumer_audit_reuses_297_1m_snapshots(monkeypatch):
    import latency_telemetry
    import market_data_stream as market

    _reset_candle_state(market, market._cache_1m)
    latency_telemetry.reset_market_request_metrics()
    calls = 0
    coins = {f"C{index:03d}" for index in range(297)}

    async def fetch(_coin, _bar, limit):
        nonlocal calls
        calls += 1
        return _candle_rows(max(limit, 8))

    async def run():
        monkeypatch.setattr(market, "_fetch", fetch)
        for engine in range(28):
            for coin in coins:
                await market._fetch_and_store(coin, "1m", "1m", 8, caller=f"engine_{engine}")

    asyncio.run(run())
    assert calls == 297
    audit = latency_telemetry.market_request_snapshot()
    assert audit["totals"]["logical_requests"] == 297 * 28
    assert audit["totals"]["underlying_requests"] == 297
    assert audit["totals"]["cache_hits"] == 297 * 27
    assert audit["unique_normalized_keys"] == 297
    assert len(audit["requests_per_instrument"]) == 297


def test_shared_wait_telemetry_is_not_lock_wait():
    import latency_telemetry

    latency_telemetry.reset_shared_resource_metrics()
    latency_telemetry.record_shared_resource("ticker", logical=2, coalesced=1, singleflight_join_wait_sec=0.25)
    row = latency_telemetry.shared_resource_snapshot()["ticker"]
    assert row["singleflight_join_wait_p95_sec"] == 0.25
    assert "lock_wait_sec" not in row


def test_public_universe_client_shutdown_is_idempotent(monkeypatch):
    import coin_universe

    class FakeClient:
        is_closed = False
        closes = 0

        async def aclose(self):
            self.closes += 1
            self.is_closed = True

    client = FakeClient()
    monkeypatch.setattr(coin_universe, "_universe_http", client)
    asyncio.run(coin_universe.close_public_http_client())
    asyncio.run(coin_universe.close_public_http_client())
    assert client.closes == 1


def test_public_universe_client_recreates_after_shutdown(monkeypatch):
    import coin_universe

    class FakeClient:
        is_closed = False

        async def aclose(self):
            self.is_closed = True

    original = FakeClient()
    replacement = FakeClient()
    monkeypatch.setattr(coin_universe, "_universe_http", original)
    monkeypatch.setattr(coin_universe, "_new_universe_http", lambda: replacement)
    asyncio.run(coin_universe.close_public_http_client())
    assert coin_universe._get_universe_http() is replacement


def test_mixed_load_keeps_p0_and_28_engine_tasks_progressing(monkeypatch, tmp_path):
    import instrument_registry
    import okx_client
    import telegram_backend

    okx_client._ticker_cache.clear()
    okx_client._ticker_inflight.clear()
    monkeypatch.setattr(telegram_backend, "_queue_operational_cache_write", lambda: asyncio.sleep(0))
    registry = instrument_registry.InstrumentRegistry(tmp_path / "instruments.json")
    row = {
        "instId": "BTC-USDT-SWAP", "instType": "SWAP", "settleCcy": "USDT",
        "ctVal": "1", "ctValCcy": "USDT", "ctType": "linear",
        "lotSz": "1", "minSz": "1", "tickSz": "0.1", "state": "live",
    }
    p0_started = asyncio.Event()
    calls = 0

    class Client:
        async def _request(self, _method, path, *, priority=None, **_kwargs):
            nonlocal calls
            calls += 1
            if path.endswith("/instruments"):
                p0_started.set()
                await asyncio.sleep(0)
                return {"code": "0", "data": [row]}
            await asyncio.sleep(0)
            return _ticker_payload()

        async def get_account_snapshot(self, **_kwargs):
            return {"total_eq_usdt": 1.0, "observed_at": "2026-01-01T00:00:00+00:00"}

    async def positions():
        return {"ok": True, "positions": {}, "observed_at": "2026-01-01T00:00:00+00:00"}

    async def engine_task():
        await asyncio.sleep(0)

    async def reconcile_task():
        await asyncio.sleep(0)

    async def run():
        client = Client()
        market_client = okx_client.OKXClient()
        market_client._request = client._request
        ticker_tasks = [asyncio.create_task(market_client.get_ticker(f"C{index}")) for index in range(315)]
        engines = [asyncio.create_task(engine_task()) for _ in range(28)]
        control = asyncio.create_task(
            telegram_backend.fast_operational_exchange_snapshot(client, positions, timeout_sec=0.5)
        )
        metadata = asyncio.create_task(registry.refresh(client, force=True, priority="P0"))
        await asyncio.gather(*ticker_tasks, *engines, control, metadata, reconcile_task())

    asyncio.run(run())
    assert p0_started.is_set()
    assert calls >= 316


def test_positions_timeout_fallback_preserves_observation_and_rendered_truth(monkeypatch, tmp_path):
    import telegram_backend as backend

    backend._operational_exchange_cache.clear()
    monkeypatch.setattr(backend, "_OPERATIONAL_CACHE_PATH", tmp_path / "snapshot.json")
    old = "2025-01-01T00:00:00+00:00"
    backend._operational_exchange_cache.update({
        "positions": {
            "ok": True,
            "source": "CACHED_OKX",
            "source_label": "CACHED OKX",
            "observed_at": old,
            "positions": {"XRP": {"size": "1"}},
            "open_pnl_usdt": 1.0,
        },
        "account": {"observed_at": old, "total_eq_usdt": 10.0},
    })

    class SlowAccount:
        async def get_account_snapshot(self, **_kwargs):
            await asyncio.sleep(1.0)
            return {"total_eq_usdt": 10.0, "observed_at": old}

    async def slow_positions():
        await asyncio.sleep(1.0)
        return {"ok": True, "positions": {}, "observed_at": old}

    account, positions = asyncio.run(
        backend.fast_operational_exchange_snapshot(SlowAccount(), slow_positions, timeout_sec=0.05)
    )
    assert positions["observed_at"] == old
    assert positions["stale"] is True
    assert positions["source"] == "CACHED_OKX"
    assert positions["refresh_error"] == "TimeoutError"
    assert positions["age_sec"] > 0
    assert account["stale"] is True

    import bot
    rendered = bot._positions_text({}, positions)
    assert "cached" in rendered.lower() or "stale" in rendered.lower()
    assert old[:10] not in rendered  # age is rendered, never a new observation timestamp


def test_control_dispatcher_is_bounded_and_keeps_event_loop_responsive():
    import telegram_backend as backend

    async def run():
        dispatcher = backend.ControlWorkDispatcher(max_workers=2, max_pending=4)
        heartbeat = 0
        stop = False

        async def beat():
            nonlocal heartbeat
            while not stop:
                heartbeat += 1
                await asyncio.sleep(0)

        def job(value):
            time.sleep(0.01)
            return value

        beat_task = asyncio.create_task(beat())
        tasks = [asyncio.create_task(dispatcher.run(job, i, priority="normal", admission_timeout=0.01)) for i in range(50)]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        stop = True
        await beat_task
        snapshot = dispatcher.snapshot()
        await dispatcher.shutdown()
        return results, heartbeat, snapshot

    results, heartbeat, snapshot = asyncio.run(run())
    assert heartbeat > 10
    assert snapshot["max_workers"] == 2
    assert snapshot["max_pending"] == 4
    assert snapshot["active_workers"] == 0
    assert snapshot["rejected_or_fallback_jobs"] > 0
    assert all(isinstance(value, (int, backend.ControlWorkQueueFull)) for value in results)


def test_control_dispatcher_coalesces_and_joiner_cancel_does_not_cancel_producer():
    import telegram_backend as backend

    async def run():
        dispatcher = backend.ControlWorkDispatcher(max_workers=1, max_pending=2)
        started = asyncio.Event()
        release = asyncio.Event()
        calls = 0
        loop = asyncio.get_running_loop()

        def work():
            nonlocal calls
            calls += 1
            loop.call_soon_threadsafe(started.set)
            while not release.is_set():
                time.sleep(0.001)
            return "ok"

        producer = asyncio.create_task(dispatcher.run(work, coalesce_key="runtime"))
        await started.wait()
        joiner = asyncio.create_task(dispatcher.run(work, coalesce_key="runtime"))
        await asyncio.sleep(0)
        joiner.cancel()
        with pytest.raises(asyncio.CancelledError):
            await joiner
        release.set()
        result = await producer
        snapshot = dispatcher.snapshot()
        await dispatcher.shutdown()
        return result, calls, snapshot

    result, calls, snapshot = asyncio.run(run())
    assert result == "ok"
    assert calls == 1
    assert snapshot["coalesced_jobs"] == 1


def test_callbacks_use_bounded_control_helpers_before_expensive_rendering(monkeypatch):
    import bot

    events = []

    class User:
        id = 1

    class Message:
        text = None
        caption = None
        reply_markup = None

        async def reply_text(self, *_args, **_kwargs):
            events.append("reply")

    class Query:
        data = "terminal:status"
        from_user = User()
        message = Message()

        async def answer(self, *_args, **_kwargs):
            events.append("ack")

        async def edit_message_text(self, *_args, **_kwargs):
            events.append("edit")

    class Update:
        callback_query = Query()

    async def runtime(*_args, **_kwargs):
        events.append("runtime")
        await asyncio.sleep(0)
        return {"live_state": {"state": "LIVE_READY_UNARMED", "armed": False}}

    async def pnl(*_args, **_kwargs):
        events.append("pnl")
        await asyncio.sleep(0)
        return {}

    async def exchange(*_args, **_kwargs):
        events.append("exchange")
        return {}, {"ok": True, "positions": {}, "observed_at": "2026-01-01T00:00:00+00:00", "stale": False}

    monkeypatch.setattr(bot, "is_allowed", lambda _uid: True)
    monkeypatch.setattr(bot._tg_backend, "control_runtime_snapshot", runtime)
    monkeypatch.setattr(bot._tg_backend, "control_pnl_snapshot", pnl)
    monkeypatch.setattr(bot._tg_backend, "fast_operational_exchange_snapshot", exchange)
    monkeypatch.setattr(bot._tg_backend, "telegram_health_snapshot", lambda: {})
    monkeypatch.setattr(bot, "_production_status_text", lambda *_args: "status")
    monkeypatch.setattr(bot, "_with_mode_banner", lambda text: text)
    monkeypatch.setattr(bot._tp, "kb_terminal_back", lambda: None)

    asyncio.run(bot.callbacks(Update(), None))
    assert events[:2] == ["ack", "runtime"]
    assert "exchange" in events
    assert "pnl" in events
    source = inspect.getsource(bot.callbacks)
    assert "_tg_backend.runtime_snapshot(" not in source
    assert "_tg_backend.production_pnl_snapshot(" not in source


def test_retry_telemetry_separates_logical_coalesced_underlying_and_physical_attempts():
    import latency_telemetry

    latency_telemetry.reset_shared_resource_metrics()
    collector = latency_telemetry.ScanLatencyTelemetry(1)
    latency_telemetry.record_shared_resource("ticker", logical=10, coalesced=9, underlying=1)
    collector.record_rest(
        "GET /api/v5/market/ticker",
        0.3,
        retries=2,
        physical_attempts=3,
        request_class="background",
    )
    snapshot = collector.snapshot(status="complete")
    shared = snapshot["shared_resources"]["ticker"]
    rest = snapshot["rest"]
    assert shared["logical_callers"] == 10
    assert shared["coalesced_callers"] == 9
    assert shared["underlying_requests"] == 1
    assert rest["request_count"] == 1
    assert rest["retry_count"] == 2
    assert rest["physical_attempt_count"] == 3


def test_okx_retry_loop_records_three_physical_attempts_for_one_request(monkeypatch):
    import httpx
    import okx_client
    import latency_telemetry

    collector = latency_telemetry.ScanLatencyTelemetry(1)
    monkeypatch.setattr(okx_client, "_latency", collector)
    original_sleep = asyncio.sleep
    monkeypatch.setattr(okx_client.asyncio, "sleep", lambda *_args: original_sleep(0))

    class FakeHTTP:
        def __init__(self):
            self.calls = 0

        async def request(self, method, url, **_kwargs):
            self.calls += 1
            request = httpx.Request(method, url)
            if self.calls < 3:
                return httpx.Response(503, request=request)
            return httpx.Response(200, json={"code": "0", "data": []}, request=request)

    transport = FakeHTTP()

    async def admit(*_args, **_kwargs):
        return object(), 0.0, 0.0

    monkeypatch.setattr(okx_client, "_http_client", transport)
    monkeypatch.setattr(okx_client, "_reconciliation_http_client", transport)
    monkeypatch.setattr(okx_client, "_acquire_governed_slot", admit)

    async def run():
        return await okx_client.OKXClient()._request("GET", "/api/v5/market/ticker", priority="P2")

    assert asyncio.run(run())["code"] == "0"
    assert transport.calls == 3
    rest = collector.snapshot(status="complete")["rest"]
    assert rest["request_count"] == 1
    assert rest["retry_count"] == 2
    assert rest["physical_attempt_count"] == 3


def test_control_dispatcher_shutdown_waits_for_active_worker_and_keeps_loop_live():
    import telegram_backend as backend

    async def run():
        dispatcher = backend.ControlWorkDispatcher(max_workers=1, max_pending=2)
        started = threading.Event()
        release = threading.Event()
        heartbeat = 0

        def blocked_job():
            started.set()
            release.wait(2.0)
            return "done"

        async def beat():
            nonlocal heartbeat
            while not release.is_set():
                heartbeat += 1
                await asyncio.sleep(0)

        job = asyncio.create_task(dispatcher.run(blocked_job, coalesce_key="blocked"))
        while not started.is_set():
            await asyncio.sleep(0)
        heartbeat_task = asyncio.create_task(beat())
        shutdown = asyncio.create_task(dispatcher.shutdown())
        await asyncio.sleep(0.03)
        assert not shutdown.done()
        assert dispatcher.snapshot()["state"] == "CLOSING"
        release.set()
        assert await job == "done"
        await shutdown
        await heartbeat_task
        await dispatcher.shutdown()
        return heartbeat, dispatcher.snapshot()

    heartbeat, snapshot = asyncio.run(run())
    assert heartbeat > 10
    assert snapshot["state"] == "CLOSED"
    assert snapshot["active_workers"] == 0
    assert snapshot["pending_jobs"] == 0
    assert not [thread for thread in threading.enumerate() if thread.name.startswith("oxypulse-control_")]


def test_control_dispatcher_shutdown_resolves_queued_jobs_and_joins_threads():
    import telegram_backend as backend

    async def run():
        dispatcher = backend.ControlWorkDispatcher(max_workers=2, max_pending=3)
        started = 0
        started_event = asyncio.Event()
        release = threading.Event()

        def blocked_job():
            nonlocal started
            started += 1
            if started == 2:
                started_event_loop.call_soon_threadsafe(started_event.set)
            release.wait(2.0)
            return "done"

        started_event_loop = asyncio.get_running_loop()
        running = [asyncio.create_task(dispatcher.run(blocked_job)) for _ in range(2)]
        await started_event.wait()
        queued = [asyncio.create_task(dispatcher.run(blocked_job, admission_timeout=0.5)) for _ in range(3)]
        while dispatcher.snapshot()["queue_depth"] < 3:
            await asyncio.sleep(0)
        shutdown = asyncio.create_task(dispatcher.shutdown())
        await asyncio.sleep(0.02)
        assert not shutdown.done()
        release.set()
        results = await asyncio.gather(*running, *queued, return_exceptions=True)
        await shutdown
        return results, dispatcher.snapshot()

    results, snapshot = asyncio.run(run())
    assert results[:2] == ["done", "done"]
    assert all(isinstance(result, backend.ControlWorkDispatcherClosed) for result in results[2:])
    assert snapshot["state"] == "CLOSED"
    assert snapshot["active_workers"] == 0
    assert snapshot["pending_jobs"] == 0
    assert not [thread for thread in threading.enumerate() if thread.name.startswith("oxypulse-control_")]


def test_control_dispatcher_replacement_retires_old_executor(monkeypatch):
    import telegram_backend as backend

    old_dispatcher = backend.ControlWorkDispatcher(max_workers=1, max_pending=1)
    started = threading.Event()
    release = threading.Event()

    def blocked_job():
        started.set()
        release.wait(2.0)
        return "old"

    async def prepare_old():
        backend._control_dispatcher = old_dispatcher
        backend._control_dispatcher_loop = asyncio.get_running_loop()
        asyncio.create_task(old_dispatcher.run(blocked_job, coalesce_key="old"))
        while not started.is_set():
            await asyncio.sleep(0)

    asyncio.run(prepare_old())
    releaser = threading.Thread(target=lambda: (time.sleep(0.05), release.set()), daemon=True)
    releaser.start()

    async def use_new():
        return await backend.run_control_work(lambda: "new", coalesce_key="new")

    assert asyncio.run(use_new()) == "new"
    asyncio.run(backend.shutdown_control_work())
    releaser.join(timeout=1.0)
    assert old_dispatcher.snapshot()["state"] == "CLOSED"
    assert not [thread for thread in threading.enumerate() if thread.name.startswith("oxypulse-control_")]


def test_mixed_load_runs_actual_htf_1m_handlers_and_diagnostic_writer(monkeypatch, tmp_path):
    import bot
    import live_position_truth
    import market_data_stream as market
    import okx_client
    import telegram_backend as backend

    rows = _candle_rows()
    scanner_started = asyncio.Event()
    htf_started = asyncio.Event()
    market_release = asyncio.Event()

    async def fetch(_coin, _bar, _limit):
        scanner_started.set()
        htf_started.set()
        await market_release.wait()
        return rows

    monkeypatch.setattr(market, "_fetch", fetch)
    market._candle_inflight.clear()
    market._cache_4h.clear()
    market._cache_1m.clear()
    market._snapshot_generation.clear()
    monkeypatch.setattr(backend, "_OPERATIONAL_CACHE_PATH", tmp_path / "snapshot.json")
    backend._operational_exchange_cache.clear()
    monkeypatch.setattr(backend, "runtime_snapshot", lambda *_args: {"live_state": {"state": "LIVE_READY_UNARMED", "armed": False}, "engines_total": 28})
    monkeypatch.setattr(backend, "production_pnl_snapshot", lambda: {})
    monkeypatch.setattr(bot, "is_allowed", lambda _uid: True)
    monkeypatch.setattr(bot, "_tracked_reply", lambda *_args, **_kwargs: asyncio.sleep(0))
    monkeypatch.setattr(bot, "_with_mode_banner", lambda text: text)
    monkeypatch.setattr(bot._tg_backend, "telegram_health_snapshot", lambda: {})
    monkeypatch.setattr(bot, "_production_status_text", lambda *_args: "status")
    monkeypatch.setattr(bot, "_positions_text", lambda *_args: "positions")
    monkeypatch.setattr(bot, "positions_kb", lambda *_args: None)

    account_started = asyncio.Event()
    positions_started = asyncio.Event()
    truth_release = asyncio.Event()

    class P0Client:
        async def get_account_snapshot(self, **_kwargs):
            account_started.set()
            await truth_release.wait()
            return {"total_eq_usdt": 1.0, "avail_eq_usdt": 0.9, "observed_at": "2026-01-01T00:00:00+00:00"}

        async def get_positions_status(self, **_kwargs):
            positions_started.set()
            await truth_release.wait()
            return {"ok": True, "positions": []}

    p0_client = P0Client()
    monkeypatch.setattr(bot, "okx", p0_client)
    live_position_truth._cache.clear()

    class Engine:
        engines = {f"engine_{i}": object() for i in range(28)}
        running = False

        def all_open_positions(self):
            return []

    monkeypatch.setattr(bot, "engine", Engine())

    class User:
        id = 1

    class Message:
        text = None
        caption = None
        reply_markup = None

        async def reply_text(self, *_args, **_kwargs):
            return None

    class Query:
        data = "terminal:status"
        from_user = User()
        message = Message()

        async def answer(self, *_args, **_kwargs):
            return None

        async def edit_message_text(self, *_args, **_kwargs):
            return None

    class Update:
        callback_query = Query()

    class UpdateCommand:
        effective_user = User()
        message = Message()

    callback_started = asyncio.Event()
    original_answer = Query.answer

    async def answer(self, *_args, **_kwargs):
        callback_started.set()
        return await original_answer(self, *_args, **_kwargs)

    monkeypatch.setattr(Query, "answer", answer)

    p2_metadata_started = asyncio.Event()
    p0_metadata_started = asyncio.Event()
    p2_metadata_release = asyncio.Event()

    row = {
        "instId": "BTC-USDT-SWAP", "instType": "SWAP", "settleCcy": "USDT",
        "ctVal": "1", "ctValCcy": "USDT", "ctType": "linear",
        "lotSz": "1", "minSz": "1", "tickSz": "0.1", "state": "live",
    }

    class MetadataClient:
        async def _request(self, _method, _path, *, priority=None, **_kwargs):
            if str(priority).upper() == "P2" or str(priority).lower() == "background":
                p2_metadata_started.set()
                await p2_metadata_release.wait()
            else:
                p0_metadata_started.set()
            return {"code": "0", "data": [row]}

    from instrument_registry import InstrumentRegistry
    registry = InstrumentRegistry(tmp_path / "mixed_instruments.json")

    engine_started = 0
    engine_release = asyncio.Event()
    control_pressure_started = threading.Event()

    def control_pressure_job():
        control_pressure_started.set()
        time.sleep(0.05)
        return True

    async def engine_task():
        nonlocal engine_started
        engine_started += 1
        await engine_release.wait()
        return True

    async def run():
        market_tasks = [asyncio.create_task(market.fetch_candles_4h("BTC")), asyncio.create_task(market.fetch_candles("BTC"))]
        market_client = okx_client.OKXClient()
        async def ticker_request(*_args, **_kwargs):
            scanner_started.set()
            await market_release.wait()
            return _ticker_payload()

        market_client._request = ticker_request
        ticker_tasks = [asyncio.create_task(market_client.get_ticker(f"C{i}", priority="P2")) for i in range(315)]
        ticker_tasks.extend(asyncio.create_task(market_client.get_ticker("BTC", priority="P2")) for _ in range(10))
        engines = [asyncio.create_task(engine_task()) for _ in range(28)]
        metadata_p2 = asyncio.create_task(registry.refresh(MetadataClient(), force=True, priority="P2"))
        await asyncio.gather(scanner_started.wait(), htf_started.wait(), p2_metadata_started.wait())
        metadata_p0 = asyncio.create_task(registry.refresh(MetadataClient(), force=True, priority="P0"))
        pressure = [
            asyncio.create_task(backend.run_control_work(control_pressure_job, priority="background", admission_timeout=0.01))
            for _ in range(20)
        ]
        while not control_pressure_started.is_set():
            await asyncio.sleep(0)
        status = asyncio.create_task(bot.cmd_status(UpdateCommand(), None))
        positions = asyncio.create_task(bot.cmd_positions(UpdateCommand(), None))
        callback = asyncio.create_task(bot.callbacks(Update(), None))
        writer = asyncio.create_task(backend._queue_operational_cache_write())
        await asyncio.gather(account_started.wait(), positions_started.wait(), p0_metadata_started.wait())
        assert not market_release.is_set()
        assert not p2_metadata_release.is_set()
        truth_release.set()
        await asyncio.gather(status, positions, callback)
        p2_metadata_release.set()
        market_release.set()
        engine_release.set()
        await asyncio.gather(*market_tasks, *ticker_tasks, *engines, metadata_p2, metadata_p0, writer, *pressure, return_exceptions=True)
        await backend._flush_operational_cache_writer()
        return backend.control_work_snapshot(), callback_started.is_set()

    snapshot, callback_ran = asyncio.run(run())
    assert market.timeframe_snapshot("BTC", "4h", 1)["version"] >= 1
    assert market.timeframe_snapshot("BTC", "1m", 1)["version"] >= 1
    assert engine_started == 28
    assert callback_ran
    assert snapshot["queue_depth"] <= snapshot["max_pending"]
    assert snapshot["active_workers"] <= snapshot["max_workers"]
    assert (tmp_path / "snapshot.json").exists()

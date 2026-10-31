import asyncio

import httpx

import okx_client


def _response(method="GET", url="https://example.test", data=None):
    return httpx.Response(
        200,
        request=httpx.Request(method, url),
        json={"code": "0", "data": [] if data is None else data},
    )


def _reset(monkeypatch):
    for name in (
        "_background_request_gate", "_critical_request_gate", "_mutation_request_gate",
        "_leverage_request_gate", "_reconciliation_request_gate",
    ):
        monkeypatch.setattr(okx_client, name, None)
    monkeypatch.setattr(okx_client, "_process_rest_health", okx_client._new_process_rest_health())
    monkeypatch.setattr(okx_client, "_process_rest_capacity", okx_client._new_process_rest_capacity())
    monkeypatch.setattr(okx_client, "_critical_current_generation", None)
    monkeypatch.setattr(okx_client, "_critical_generations", {})
    monkeypatch.setattr(okx_client, "_truth_cache", {})
    monkeypatch.setattr(okx_client, "_truth_inflight", {})
    monkeypatch.setattr(okx_client, "_truth_metrics", {
        "cache_hits": 0, "coalesced_requests": 0,
        "successful_reads": 0, "failed_reads": 0,
    })


def test_concurrent_account_reads_share_one_p1_request(monkeypatch):
    _reset(monkeypatch)
    monkeypatch.setattr(okx_client, "PAPER_TRADING", False)
    monkeypatch.setattr(okx_client, "OKX_API_KEY", "test-key")
    monkeypatch.setattr(okx_client, "_account_snapshot_cache", {"value": None, "ts": 0.0})
    calls = 0
    started = asyncio.Event()

    class HTTP:
        async def request(self, *args, **kwargs):
            nonlocal calls
            calls += 1
            started.set()
            await asyncio.sleep(0.02)
            return _response(*args[:2], data=[{
                "totalEq": "10", "availEq": "9", "details": [{"ccy": "USDT", "availBal": "9"}],
            }])

    monkeypatch.setattr(okx_client, "_reconciliation_http_client", HTTP())
    monkeypatch.setattr(okx_client, "_critical_http_client", HTTP())
    client_a = okx_client.OKXClient()
    client_b = okx_client.OKXClient()
    client_a._headers = lambda *_args: {}
    client_b._headers = lambda *_args: {}

    async def run():
        first = asyncio.create_task(client_a.get_account_snapshot(force_refresh=True))
        await asyncio.wait_for(started.wait(), timeout=0.5)
        second = asyncio.create_task(client_b.get_account_snapshot(force_refresh=True))
        return await asyncio.gather(first, second)

    rows = asyncio.run(run())
    assert calls == 1
    assert rows[0]["avail_eq_usdt"] == rows[1]["avail_eq_usdt"] == 9.0
    assert okx_client.get_critical_rest_health_snapshot()["coalescing"]["coalesced_requests"] == 1


def test_position_cache_is_not_used_for_forced_p0_truth(monkeypatch):
    _reset(monkeypatch)
    monkeypatch.setattr(okx_client, "PAPER_TRADING", False)
    monkeypatch.setattr(okx_client, "OKX_API_KEY", "test-key")
    calls = 0

    class HTTP:
        async def request(self, *args, **kwargs):
            nonlocal calls
            calls += 1
            return _response(*args[:2], data=[{"instId": "BTC-USDT-SWAP", "pos": "1"}])

    monkeypatch.setattr(okx_client, "_reconciliation_http_client", HTTP())
    monkeypatch.setattr(okx_client, "_critical_http_client", HTTP())
    client = okx_client.OKXClient()
    client._headers = lambda *_args: {}

    async def run():
        first = await client.get_positions_status(priority="status")
        second = await client.get_positions_status(force_refresh=True, priority="P0")
        return first, second

    first, second = asyncio.run(run())
    assert first["positions"] == second["positions"]
    assert calls == 2


def test_p0_position_truth_does_not_wait_behind_p2_status_read(monkeypatch):
    _reset(monkeypatch)
    monkeypatch.setattr(okx_client, "PAPER_TRADING", False)
    monkeypatch.setattr(okx_client, "OKX_API_KEY", "test-key")
    low_started = asyncio.Event()
    low_release = asyncio.Event()

    class ReconciliationHTTP:
        async def request(self, *args, **kwargs):
            low_started.set()
            await low_release.wait()
            return _response(*args[:2], data=[])

    class CriticalHTTP:
        async def request(self, *args, **kwargs):
            return _response(*args[:2], data=[{"instId": "BTC-USDT-SWAP", "pos": "1"}])

    monkeypatch.setattr(okx_client, "_reconciliation_http_client", ReconciliationHTTP())
    monkeypatch.setattr(okx_client, "_critical_http_client", CriticalHTTP())
    client = okx_client.OKXClient()
    client._headers = lambda *_args: {}

    async def run():
        low = asyncio.create_task(client.get_positions_status(priority="status"))
        await asyncio.wait_for(low_started.wait(), timeout=0.5)
        high = await asyncio.wait_for(
            client.get_positions_status(force_refresh=True, priority="P0"), timeout=0.5
        )
        low_release.set()
        await low
        return high

    high = asyncio.run(run())
    assert high["ok"] is True
    assert high["positions"][0]["pos"] == "1"


def test_protection_discovery_coalesces_both_algo_feeds(monkeypatch):
    _reset(monkeypatch)
    monkeypatch.setattr(okx_client, "PAPER_TRADING", False)
    monkeypatch.setattr(okx_client, "OKX_API_KEY", "test-key")
    calls = []
    started = asyncio.Event()

    class HTTP:
        async def request(self, *args, **kwargs):
            calls.append(kwargs.get("params", {}).get("ordType"))
            started.set()
            await asyncio.sleep(0.02)
            return _response(*args[:2])

    monkeypatch.setattr(okx_client, "_critical_http_client", HTTP())
    client = okx_client.OKXClient()
    client._headers = lambda *_args: {}

    async def run():
        first = asyncio.create_task(client.list_live_protection_orders("BTC", pos_side="long", force_refresh=True))
        await asyncio.wait_for(started.wait(), timeout=0.5)
        second = asyncio.create_task(client.list_live_protection_orders("BTC", pos_side="long", force_refresh=True))
        return await asyncio.gather(first, second)

    first, second = asyncio.run(run())
    assert first["ok"] and second["ok"]
    assert sorted(calls) == ["conditional", "oco"]
    assert okx_client.get_critical_rest_health_snapshot()["coalescing"]["coalesced_requests"] == 1


def test_failed_truth_read_is_not_cached_and_next_read_retries(monkeypatch):
    _reset(monkeypatch)
    monkeypatch.setattr(okx_client, "PAPER_TRADING", False)
    monkeypatch.setattr(okx_client, "OKX_API_KEY", "test-key")
    calls = 0

    class HTTP:
        async def request(self, *args, **kwargs):
            nonlocal calls
            calls += 1
            if calls <= 4:
                raise httpx.ConnectError("temporary")
            return _response(*args[:2], data=[])

    monkeypatch.setattr(okx_client, "_critical_http_client", HTTP())
    client = okx_client.OKXClient()
    client._headers = lambda *_args: {}
    original_sleep = asyncio.sleep
    monkeypatch.setattr(okx_client.asyncio, "sleep", lambda *_args, **_kwargs: original_sleep(0))

    async def run():
        first = await client.get_positions_status(force_refresh=True)
        second = await client.get_positions_status(force_refresh=True)
        return first, second

    first, second = asyncio.run(run())
    assert not first["ok"]
    assert second["ok"]
    assert calls == 5


def test_historical_reads_use_separate_reconciliation_capacity(monkeypatch):
    _reset(monkeypatch)
    monkeypatch.setattr(okx_client, "PAPER_TRADING", False)
    monkeypatch.setattr(okx_client, "OKX_API_KEY", "test-key")
    historical_calls = []

    class HistoricalHTTP:
        async def request(self, *args, **kwargs):
            historical_calls.append(args[1])
            return _response(*args[:2])

    class CriticalHTTP:
        async def request(self, *args, **kwargs):
            raise AssertionError("historical read used critical capacity")

    monkeypatch.setattr(okx_client, "_reconciliation_http_client", HistoricalHTTP())
    monkeypatch.setattr(okx_client, "_critical_http_client", CriticalHTTP())
    client = okx_client.OKXClient()
    client._headers = lambda *_args: {}

    result = asyncio.run(client.get_order_history_status(inst_id="BTC-USDT-SWAP"))
    assert result["ok"]
    assert historical_calls == ["https://www.okx.com/api/v5/trade/orders-history"]
    health = okx_client.get_critical_rest_health_snapshot()
    assert health["reconciliation"]["request_count"] == 1
    assert health["critical_read"]["request_count"] == 0


def test_p0_order_history_recovery_uses_critical_capacity(monkeypatch):
    _reset(monkeypatch)
    monkeypatch.setattr(okx_client, "PAPER_TRADING", False)
    monkeypatch.setattr(okx_client, "OKX_API_KEY", "test-key")
    critical_calls = []

    class CriticalHTTP:
        async def request(self, *args, **kwargs):
            critical_calls.append(args[1])
            return _response(*args[:2])

    class HistoricalHTTP:
        async def request(self, *args, **kwargs):
            raise AssertionError("P0 recovery used reconciliation capacity")

    monkeypatch.setattr(okx_client, "_critical_http_client", CriticalHTTP())
    monkeypatch.setattr(okx_client, "_reconciliation_http_client", HistoricalHTTP())
    client = okx_client.OKXClient()
    client._headers = lambda *_args: {}
    result = asyncio.run(client.get_order_history_status(inst_id="BTC-USDT-SWAP", priority="P0"))
    assert result["ok"]
    assert critical_calls == ["https://www.okx.com/api/v5/trade/orders-history"]


def test_timeout_types_remain_distinct_in_process_health(monkeypatch):
    _reset(monkeypatch)
    monkeypatch.setattr(okx_client, "PAPER_TRADING", False)
    monkeypatch.setattr(okx_client, "OKX_API_KEY", "test-key")
    original_sleep = asyncio.sleep
    monkeypatch.setattr(okx_client.asyncio, "sleep", lambda *_args, **_kwargs: original_sleep(0))

    class HTTP:
        def __init__(self, error):
            self.error = error
        async def request(self, *args, **kwargs):
            raise self.error

    client = okx_client.OKXClient()
    client._headers = lambda *_args: {}
    for error in (httpx.PoolTimeout("pool"), httpx.ConnectTimeout("connect"), httpx.ReadTimeout("read")):
        monkeypatch.setattr(okx_client, "_reconciliation_http_client", HTTP(error))
        with __import__("pytest").raises(RuntimeError):
            asyncio.run(client._request("GET", "/api/v5/account/positions", priority="historical"))
    bucket = okx_client.get_critical_rest_health_snapshot()["reconciliation"]
    assert bucket["pool_timeout_count"] == 4
    capacity = okx_client.get_critical_rest_health_snapshot()["capacity"]["reconciliation"]
    assert capacity["connect_timeout_count"] == 4
    assert capacity["read_timeout_count"] == 4
    assert bucket["timeout_count"] == 3


def test_topology_reports_bounded_pools_without_network():
    topology = okx_client.get_rest_topology_snapshot()
    assert topology["critical_read"]["connections"] == okx_client._CRITICAL_REQUEST_LIMIT
    assert topology["reconciliation"]["connections"] == okx_client._RECONCILIATION_REQUEST_LIMIT
    assert topology["mutation"]["retries"] == 1


def test_rest_health_exposes_process_and_arm_scope_without_resetting_counters(monkeypatch):
    _reset(monkeypatch)
    before = okx_client.get_critical_rest_health_snapshot()["scope"]
    marked = okx_client.mark_rest_arm_epoch()
    after = okx_client.get_critical_rest_health_snapshot()["scope"]
    assert marked["session_id"] == before["session_id"]
    assert after["arm_epoch"] == before["arm_epoch"] + 1
    assert after["process_started_at"] == before["process_started_at"]

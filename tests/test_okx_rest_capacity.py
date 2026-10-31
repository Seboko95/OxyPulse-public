import asyncio

import httpx
import pytest

import latency_telemetry
import okx_client


def _response(*args) -> httpx.Response:
    method = args[0] if len(args) > 0 else "GET"
    url = args[1] if len(args) > 1 else "https://example.test"
    return httpx.Response(200, request=httpx.Request(method, url), json={"code": "0", "data": []})


def _reset_gates(monkeypatch):
    monkeypatch.setattr(okx_client, "_background_request_gate", None)
    monkeypatch.setattr(okx_client, "_critical_request_gate", None)
    monkeypatch.setattr(okx_client, "_mutation_request_gate", None)
    monkeypatch.setattr(okx_client, "_leverage_request_gate", None)
    monkeypatch.setattr(okx_client, "_process_rest_health", okx_client._new_process_rest_health())
    monkeypatch.setattr(okx_client, "_critical_current_generation", None)
    monkeypatch.setattr(okx_client, "_critical_generations", {})


def test_background_saturation_cannot_starve_positions(monkeypatch):
    monkeypatch.setattr(okx_client, "_BACKGROUND_REQUEST_LIMIT", 1)
    _reset_gates(monkeypatch)
    started = asyncio.Event()
    release = asyncio.Event()
    critical_calls = []

    class BackgroundHTTP:
        async def request(self, *args, **kwargs):
            started.set()
            await release.wait()
            return _response(*args[:2])

    class CriticalHTTP:
        async def request(self, *args, **kwargs):
            critical_calls.append((args, kwargs))
            return _response(*args[:2])

    monkeypatch.setattr(okx_client, "_http_client", BackgroundHTTP())
    monkeypatch.setattr(okx_client, "_critical_http_client", CriticalHTTP())
    client = okx_client.OKXClient()
    monkeypatch.setattr(client, "_headers", lambda *_args: {})

    async def scenario():
        background = asyncio.create_task(client._request(
            "GET", "/api/v5/public/open-interest", priority="background"
        ))
        await asyncio.wait_for(started.wait(), timeout=0.5)
        critical = await asyncio.wait_for(
            client._request("GET", "/api/v5/account/positions"), timeout=0.5
        )
        release.set()
        await background
        return critical

    result = asyncio.run(scenario())
    assert result["code"] == "0"
    assert len(critical_calls) == 1
    assert "/api/v5/account/positions" in critical_calls[0][0][1]


def test_balance_routes_through_critical_capacity(monkeypatch):
    _reset_gates(monkeypatch)
    monkeypatch.setattr(okx_client, "PAPER_TRADING", False)
    monkeypatch.setattr(okx_client, "OKX_API_KEY", "unit-test-key")
    monkeypatch.setattr(okx_client, "_account_snapshot_cache", {"value": None, "ts": 0.0})
    background_calls = []
    critical_calls = []

    class BackgroundHTTP:
        async def request(self, *args, **kwargs):
            background_calls.append(args[1])
            raise AssertionError("account balance used background capacity")

    class CriticalHTTP:
        async def request(self, *args, **kwargs):
            critical_calls.append(args[1])
            return httpx.Response(
                200,
                request=httpx.Request(args[0], args[1]),
                json={
                    "code": "0",
                    "data": [{
                        "totalEq": "12.0",
                        "availEq": "11.0",
                        "details": [{"ccy": "USDT", "availBal": "11.0"}],
                    }],
                },
            )

    monkeypatch.setattr(okx_client, "_http_client", BackgroundHTTP())
    monkeypatch.setattr(okx_client, "_critical_http_client", CriticalHTTP())
    client = okx_client.OKXClient()
    monkeypatch.setattr(client, "_headers", lambda *_args: {})

    assert asyncio.run(client.get_balance()) == {"USDT": "11.0"}
    assert critical_calls == ["https://www.okx.com/api/v5/account/balance"]
    assert background_calls == []


def test_leverage_truth_uses_dedicated_capacity(monkeypatch):
    _reset_gates(monkeypatch)
    leverage_calls = []

    class LeverageHTTP:
        async def request(self, *args, **kwargs):
            leverage_calls.append(args[1])
            return _response(*args[:2])

    class CriticalHTTP:
        async def request(self, *args, **kwargs):
            raise AssertionError("leverage truth used lifecycle-critical capacity")

    monkeypatch.setattr(okx_client, "_leverage_http_client", LeverageHTTP())
    monkeypatch.setattr(okx_client, "_critical_http_client", CriticalHTTP())
    client = okx_client.OKXClient()
    monkeypatch.setattr(client, "_headers", lambda *_args: {})

    result = asyncio.run(client._request(
        "GET", "/api/v5/account/leverage-info",
        priority="leverage_truth",
    ))
    assert result["code"] == "0"
    assert len(leverage_calls) == 1
    health = okx_client.get_critical_rest_health_snapshot()
    assert health["leverage_truth"]["request_count"] == 1
    assert health["critical_read"]["request_count"] == 0


def test_distinct_leverage_reads_queue_on_dedicated_capacity(monkeypatch):
    monkeypatch.setattr(okx_client, "_LEVERAGE_REQUEST_LIMIT", 1)
    _reset_gates(monkeypatch)
    active = 0
    max_active = 0
    calls = []

    class LeverageHTTP:
        async def request(self, *args, **kwargs):
            nonlocal active, max_active
            active += 1
            max_active = max(max_active, active)
            calls.append(args[1])
            await asyncio.sleep(0.02)
            active -= 1
            return _response(*args[:2])

    monkeypatch.setattr(okx_client, "_leverage_http_client", LeverageHTTP())
    client = okx_client.OKXClient()
    monkeypatch.setattr(client, "_headers", lambda *_args: {})

    async def run():
        return await asyncio.gather(
            client._request(
                "GET", "/api/v5/account/leverage-info",
                params={"instId": "XRP-USDT-SWAP"}, priority="leverage_truth",
            ),
            client._request(
                "GET", "/api/v5/account/leverage-info",
                params={"instId": "BTC-USDT-SWAP"}, priority="leverage_truth",
            ),
        )

    results = asyncio.run(run())
    assert all(result["code"] == "0" for result in results)
    assert len(calls) == 2
    assert max_active == 1
    assert okx_client.get_critical_rest_health_snapshot()["leverage_truth"]["request_count"] == 2


def test_successful_requests_release_background_capacity(monkeypatch):
    monkeypatch.setattr(okx_client, "_BACKGROUND_REQUEST_LIMIT", 1)
    _reset_gates(monkeypatch)
    active = 0
    max_active = 0
    calls = 0

    class HTTP:
        async def request(self, *args, **kwargs):
            nonlocal active, max_active, calls
            calls += 1
            active += 1
            max_active = max(max_active, active)
            await asyncio.sleep(0.01)
            active -= 1
            return _response(*args[:2])

    monkeypatch.setattr(okx_client, "_http_client", HTTP())
    client = okx_client.OKXClient()
    monkeypatch.setattr(client, "_headers", lambda *_args: {})

    async def scenario():
        return await asyncio.gather(
            client._request("GET", "/api/v5/public/open-interest"),
            client._request("GET", "/api/v5/public/open-interest"),
        )

    result = asyncio.run(scenario())
    assert [row["code"] for row in result] == ["0", "0"]
    assert calls == 2
    assert max_active == 1


def test_request_exception_releases_capacity_before_retry(monkeypatch):
    monkeypatch.setattr(okx_client, "_BACKGROUND_REQUEST_LIMIT", 1)
    _reset_gates(monkeypatch)
    calls = 0

    class HTTP:
        async def request(self, *args, **kwargs):
            nonlocal calls
            calls += 1
            if calls == 1:
                raise httpx.ConnectError("transport failure")
            return _response(*args[:2])

    monkeypatch.setattr(okx_client, "_http_client", HTTP())
    original_sleep = asyncio.sleep
    monkeypatch.setattr(okx_client.asyncio, "sleep", lambda *_args, **_kwargs: original_sleep(0))
    client = okx_client.OKXClient()
    monkeypatch.setattr(client, "_headers", lambda *_args: {})

    result = asyncio.run(client._request("GET", "/api/v5/public/open-interest"))
    assert result["code"] == "0"
    assert calls == 2


def test_cancelled_request_releases_capacity(monkeypatch):
    monkeypatch.setattr(okx_client, "_BACKGROUND_REQUEST_LIMIT", 1)
    _reset_gates(monkeypatch)
    started = asyncio.Event()
    release = asyncio.Event()
    calls = 0

    class HTTP:
        async def request(self, *args, **kwargs):
            nonlocal calls
            calls += 1
            if calls == 1:
                started.set()
                await release.wait()
            return _response()

    monkeypatch.setattr(okx_client, "_http_client", HTTP())
    client = okx_client.OKXClient()
    monkeypatch.setattr(client, "_headers", lambda *_args: {})

    async def scenario():
        cancelled = asyncio.create_task(client._request("GET", "/api/v5/public/open-interest"))
        await asyncio.wait_for(started.wait(), timeout=0.5)
        cancelled.cancel()
        with pytest.raises(asyncio.CancelledError):
            await cancelled
        return await asyncio.wait_for(
            client._request("GET", "/api/v5/public/open-interest"), timeout=0.5
        )

    result = asyncio.run(scenario())
    assert result["code"] == "0"
    assert calls == 2


def test_execution_ticker_uses_reserved_capacity(monkeypatch):
    _reset_gates(monkeypatch)
    monkeypatch.setattr(okx_client, "_ticker_cache", {})
    monkeypatch.setattr(okx_client, "_ticker_lock", None)
    monkeypatch.setattr(okx_client, "_execution_ticker_lock", None)
    critical_calls = []

    class BackgroundHTTP:
        async def request(self, *args, **kwargs):
            raise AssertionError("execution ticker used background capacity")

    class CriticalHTTP:
        async def request(self, *args, **kwargs):
            critical_calls.append(args[1])
            return httpx.Response(
                200,
                request=httpx.Request(args[0], args[1]),
                json={"code": "0", "data": [{"last": "10"}]},
            )

    monkeypatch.setattr(okx_client, "_http_client", BackgroundHTTP())
    monkeypatch.setattr(okx_client, "_critical_http_client", CriticalHTTP())
    client = okx_client.OKXClient()
    monkeypatch.setattr(client, "_headers", lambda *_args: {})

    result = asyncio.run(client.get_execution_ticker("BTC"))
    assert result["last"] == 10.0
    assert critical_calls == ["https://www.okx.com/api/v5/market/ticker"]


def test_execution_ticker_does_not_wait_on_background_ticker_lock(monkeypatch):
    _reset_gates(monkeypatch)
    monkeypatch.setattr(okx_client, "_ticker_cache", {})
    monkeypatch.setattr(okx_client, "_ticker_lock", None)
    monkeypatch.setattr(okx_client, "_execution_ticker_lock", None)
    started = asyncio.Event()
    release = asyncio.Event()

    class BackgroundHTTP:
        async def request(self, *args, **kwargs):
            started.set()
            await release.wait()
            return httpx.Response(
                200,
                request=httpx.Request(args[0], args[1]),
                json={"code": "0", "data": [{"last": "9"}]},
            )

    class CriticalHTTP:
        async def request(self, *args, **kwargs):
            return httpx.Response(
                200,
                request=httpx.Request(args[0], args[1]),
                json={"code": "0", "data": [{"last": "10"}]},
            )

    monkeypatch.setattr(okx_client, "_http_client", BackgroundHTTP())
    monkeypatch.setattr(okx_client, "_critical_http_client", CriticalHTTP())
    client = okx_client.OKXClient()
    monkeypatch.setattr(client, "_headers", lambda *_args: {})

    async def scenario():
        background = asyncio.create_task(client.get_ticker("ETH"))
        await asyncio.wait_for(started.wait(), timeout=0.5)
        critical = await asyncio.wait_for(client.get_execution_ticker("BTC"), timeout=0.5)
        release.set()
        await background
        return critical

    result = asyncio.run(scenario())
    assert result["last"] == 10.0


def test_successful_requests_are_attributed_to_the_selected_class(monkeypatch):
    _reset_gates(monkeypatch)

    class HTTP:
        async def request(self, *args, **kwargs):
            return _response(*args[:2])

    monkeypatch.setattr(okx_client, "_http_client", HTTP())
    monkeypatch.setattr(okx_client, "_critical_http_client", HTTP())
    monkeypatch.setattr(okx_client, "_mutation_http_client", HTTP())
    client = okx_client.OKXClient()
    monkeypatch.setattr(client, "_headers", lambda *_args: {})
    collector = latency_telemetry.ScanLatencyTelemetry(1)
    token = latency_telemetry.activate(collector)
    try:
        asyncio.run(client._request("GET", "/api/v5/account/positions"))
        asyncio.run(client._request("GET", "/api/v5/public/open-interest"))
    finally:
        latency_telemetry.reset(token)

    rest = collector.snapshot(status="completed")["rest"]
    by_class = rest["by_request_class"]
    assert by_class["critical"]["request_count"] == 1
    assert by_class["background"]["request_count"] == 1
    assert by_class["other"]["request_count"] == 0
    assert rest["critical_read"]["request_count"] == 1


def test_scanning_and_execution_tickers_record_distinct_request_classes(monkeypatch):
    _reset_gates(monkeypatch)
    monkeypatch.setattr(okx_client, "_ticker_cache", {})
    monkeypatch.setattr(okx_client, "_ticker_lock", None)
    monkeypatch.setattr(okx_client, "_execution_ticker_lock", None)

    class HTTP:
        async def request(self, *args, **kwargs):
            return httpx.Response(
                200,
                request=httpx.Request(args[0], args[1]),
                json={"code": "0", "data": [{"last": "10"}]},
            )

    monkeypatch.setattr(okx_client, "_http_client", HTTP())
    monkeypatch.setattr(okx_client, "_critical_http_client", HTTP())
    client = okx_client.OKXClient()
    monkeypatch.setattr(client, "_headers", lambda *_args: {})
    collector = latency_telemetry.ScanLatencyTelemetry(1)
    token = latency_telemetry.activate(collector)
    try:
        asyncio.run(client.get_ticker("ETH"))
        asyncio.run(client.get_execution_ticker("BTC"))
    finally:
        latency_telemetry.reset(token)

    by_class = collector.snapshot(status="completed")["rest"]["by_request_class"]
    assert by_class["background"]["request_count"] == 1
    assert by_class["critical"]["request_count"] == 1


def test_pool_timeout_is_classified_without_changing_get_retry_count(monkeypatch):
    _reset_gates(monkeypatch)
    calls = 0

    class HTTP:
        async def request(self, *args, **kwargs):
            nonlocal calls
            calls += 1
            raise httpx.PoolTimeout("pool exhausted")

    monkeypatch.setattr(okx_client, "_http_client", HTTP())
    original_sleep = asyncio.sleep
    monkeypatch.setattr(okx_client.asyncio, "sleep", lambda *_args, **_kwargs: original_sleep(0))
    client = okx_client.OKXClient()
    monkeypatch.setattr(client, "_headers", lambda *_args: {})
    collector = latency_telemetry.ScanLatencyTelemetry(1)
    token = latency_telemetry.activate(collector)
    try:
        with pytest.raises(RuntimeError, match="PoolTimeout"):
            asyncio.run(client._request("GET", "/api/v5/public/open-interest"))
    finally:
        latency_telemetry.reset(token)

    rest = collector.snapshot(status="completed")["rest"]
    assert calls == 4
    assert rest["retry_count"] == 3
    assert rest["pool_timeout_count"] == 4
    assert rest["by_request_class"]["background"]["pool_timeout_count"] == 4
    assert rest["by_request_class"]["critical"]["pool_timeout_count"] == 0


def test_critical_pool_timeout_is_attributed_to_critical(monkeypatch):
    _reset_gates(monkeypatch)

    class HTTP:
        async def request(self, *args, **kwargs):
            raise httpx.PoolTimeout("critical pool exhausted")

    monkeypatch.setattr(okx_client, "_critical_http_client", HTTP())
    original_sleep = asyncio.sleep
    monkeypatch.setattr(okx_client.asyncio, "sleep", lambda *_args, **_kwargs: original_sleep(0))
    client = okx_client.OKXClient()
    monkeypatch.setattr(client, "_headers", lambda *_args: {})
    collector = latency_telemetry.ScanLatencyTelemetry(1)
    token = latency_telemetry.activate(collector)
    try:
        with pytest.raises(RuntimeError, match="PoolTimeout"):
            asyncio.run(client._request("GET", "/api/v5/account/positions"))
    finally:
        latency_telemetry.reset(token)

    by_class = collector.snapshot(status="completed")["rest"]["by_request_class"]
    assert by_class["critical"]["pool_timeout_count"] == 4
    assert by_class["background"]["pool_timeout_count"] == 0


def test_request_class_telemetry_is_fixed_and_unknown_values_use_other():
    collector = latency_telemetry.ScanLatencyTelemetry(1)
    collector.record_rest("/api/v5/public/test", 0.1, request_class="unexpected")
    by_class = collector.snapshot(status="completed")["rest"]["by_request_class"]
    assert set(by_class) == {"critical", "background", "other"}
    assert by_class["other"]["request_count"] == 1


def test_post_failure_is_never_replayed(monkeypatch):
    _reset_gates(monkeypatch)
    calls = 0

    class HTTP:
        async def request(self, *args, **kwargs):
            nonlocal calls
            calls += 1
            raise httpx.ConnectError("post transport failure")

    monkeypatch.setattr(okx_client, "_mutation_http_client", HTTP())
    client = okx_client.OKXClient()
    monkeypatch.setattr(client, "_headers", lambda *_args: {})

    with pytest.raises(RuntimeError, match="ConnectError"):
        asyncio.run(client._request("POST", "/api/v5/trade/order", data={"sz": "1"}))
    assert calls == 1


def test_persistent_clients_close_cleanly(monkeypatch):
    closed = []

    class Client:
        def __init__(self, name):
            self.name = name

        async def aclose(self):
            closed.append(self.name)

    monkeypatch.setattr(okx_client, "_http_client", Client("background"))
    monkeypatch.setattr(okx_client, "_critical_http_client", Client("critical"))
    monkeypatch.setattr(okx_client, "_leverage_http_client", Client("leverage"))
    monkeypatch.setattr(okx_client, "_mutation_http_client", Client("mutation"))
    asyncio.run(okx_client.close_http_clients())
    assert closed == ["background", "critical", "leverage", "mutation"]

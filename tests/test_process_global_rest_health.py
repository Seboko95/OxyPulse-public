import asyncio

import httpx
import pytest

import latency_telemetry
import okx_client


def _response(method="GET", path="/api/v5/public/test", *, status=200):
    request = httpx.Request(method, okx_client.BASE_URL + path)
    return httpx.Response(status, request=request, json={"code": "0", "data": []})


def _reset(monkeypatch):
    monkeypatch.setattr(okx_client, "_process_rest_health", okx_client._new_process_rest_health())
    monkeypatch.setattr(okx_client, "_critical_current_generation", None)
    monkeypatch.setattr(okx_client, "_critical_generations", {})
    monkeypatch.setattr(okx_client, "_critical_generation_seq", 0)
    monkeypatch.setattr(okx_client, "_critical_recovery", {
        "current_generation": 0,
        "recovery_count": 0,
        "retired_generation_count": 0,
        "last_recovery_reason": "",
        "last_recovery_timestamp": "",
    })
    monkeypatch.setattr(okx_client, "_background_request_gate", None)
    monkeypatch.setattr(okx_client, "_critical_request_gate", None)
    monkeypatch.setattr(okx_client, "_mutation_request_gate", None)


class HTTP:
    async def request(self, method, url, **_kwargs):
        return _response(method, url.replace(okx_client.BASE_URL, ""))


def _client(monkeypatch, transport=None):
    transport = transport or HTTP()
    monkeypatch.setattr(okx_client, "_http_client", transport)
    monkeypatch.setattr(okx_client, "_critical_http_client", transport)
    monkeypatch.setattr(okx_client, "_mutation_http_client", transport)
    client = okx_client.OKXClient()
    monkeypatch.setattr(client, "_headers", lambda *_args: {})
    return client


def test_critical_get_outside_scan_context_is_process_global(monkeypatch):
    _reset(monkeypatch)
    client = _client(monkeypatch)

    assert asyncio.run(client._request("GET", "/api/v5/account/positions"))["code"] == "0"

    health = okx_client.get_critical_rest_health_snapshot()
    assert health["critical_read"]["request_count"] == 1
    assert latency_telemetry.current() is None


def test_reconciliation_style_request_outside_scan_context_is_visible(monkeypatch):
    _reset(monkeypatch)
    client = _client(monkeypatch)

    asyncio.run(client._request("GET", "/api/v5/account/positions"))

    assert okx_client.get_critical_rest_health_snapshot()["critical_read"]["request_count"] == 1


def test_critical_get_inside_scan_context_updates_both_views(monkeypatch):
    _reset(monkeypatch)
    client = _client(monkeypatch)
    collector = latency_telemetry.ScanLatencyTelemetry(1)
    token = latency_telemetry.activate(collector)
    try:
        asyncio.run(client._request("GET", "/api/v5/account/positions"))
    finally:
        latency_telemetry.reset(token)

    assert collector.snapshot(status="completed")["rest"]["critical_read"]["request_count"] == 1
    assert okx_client.get_critical_rest_health_snapshot()["critical_read"]["request_count"] == 1


def test_scan_local_counter_remains_local_while_process_counter_survives_replacement(monkeypatch):
    _reset(monkeypatch)
    client = _client(monkeypatch)
    first = latency_telemetry.ScanLatencyTelemetry(1)
    second = latency_telemetry.ScanLatencyTelemetry(2)

    token = latency_telemetry.activate(first)
    try:
        asyncio.run(client._request("GET", "/api/v5/account/positions"))
    finally:
        latency_telemetry.reset(token)
    token = latency_telemetry.activate(second)
    try:
        asyncio.run(client._request("GET", "/api/v5/account/positions"))
    finally:
        latency_telemetry.reset(token)

    assert first.snapshot(status="completed")["rest"]["request_count"] == 1
    assert second.snapshot(status="completed")["rest"]["request_count"] == 1
    assert okx_client.get_critical_rest_health_snapshot()["critical_read"]["request_count"] == 2


def test_process_global_retry_and_background_accounting(monkeypatch):
    _reset(monkeypatch)
    responses = iter([_response(status=503), _response(status=200)])

    class RetryingHTTP:
        async def request(self, *args, **_kwargs):
            return next(responses)

    monkeypatch.setattr(okx_client, "_http_client", RetryingHTTP())
    client = okx_client.OKXClient()
    monkeypatch.setattr(client, "_headers", lambda *_args: {})
    original_sleep = asyncio.sleep
    monkeypatch.setattr(okx_client.asyncio, "sleep", lambda *_args, **_kwargs: original_sleep(0))

    asyncio.run(client._request("GET", "/api/v5/public/open-interest"))

    background = okx_client.get_critical_rest_health_snapshot()["background"]
    assert background["request_count"] == 1
    assert background["retry_count"] == 1


def test_pool_timeout_is_attributed_process_globally_and_rotates_generation(monkeypatch):
    _reset(monkeypatch)

    class FailingHTTP:
        async def request(self, *_args, **_kwargs):
            raise httpx.PoolTimeout("pool exhausted")

        async def aclose(self):
            return None

    class ReplacementHTTP(HTTP):
        async def aclose(self):
            return None

    monkeypatch.setattr(okx_client, "_critical_http_client", FailingHTTP())
    monkeypatch.setattr(okx_client, "_new_critical_http_client", lambda: ReplacementHTTP())
    client = okx_client.OKXClient()
    monkeypatch.setattr(client, "_headers", lambda *_args: {})
    original_sleep = asyncio.sleep
    monkeypatch.setattr(okx_client.asyncio, "sleep", lambda *_args, **_kwargs: original_sleep(0))

    with pytest.raises(RuntimeError, match="PoolTimeout"):
        asyncio.run(client._request("GET", "/api/v5/account/positions"))

    health = okx_client.get_critical_rest_health_snapshot()
    assert health["critical_read"]["request_count"] == 1
    assert health["critical_read"]["retry_count"] == 3
    assert health["critical_read"]["pool_timeout_count"] == 4
    assert health["critical_read"]["error_count"] == 1
    assert health["recovery"]["current_generation"] == 2
    assert health["recovery"]["recovery_count"] == 1
    assert health["recovery"]["retired_generation_count"] == 1
    assert health["recovery"]["active_retired_generations"] == 0
    assert health["recovery"]["last_recovery_reason"].startswith("PoolTimeout exhausted")


def test_connect_timeout_is_attributed_process_globally(monkeypatch):
    _reset(monkeypatch)

    class FailingHTTP:
        async def request(self, *_args, **_kwargs):
            raise httpx.ConnectTimeout("connect timeout")

        async def aclose(self):
            return None

    monkeypatch.setattr(okx_client, "_critical_http_client", FailingHTTP())
    monkeypatch.setattr(okx_client, "_new_critical_http_client", lambda: HTTP())
    client = okx_client.OKXClient()
    monkeypatch.setattr(client, "_headers", lambda *_args: {})
    original_sleep = asyncio.sleep
    monkeypatch.setattr(okx_client.asyncio, "sleep", lambda *_args, **_kwargs: original_sleep(0))

    with pytest.raises(RuntimeError, match="ConnectTimeout"):
        asyncio.run(client._request("GET", "/api/v5/account/balance"))

    health = okx_client.get_critical_rest_health_snapshot()
    assert health["critical_read"]["timeout_count"] == 1
    assert health["critical_read"]["pool_timeout_count"] == 0
    assert health["recovery"]["recovery_count"] == 1


def test_mutation_is_global_but_not_critical_read(monkeypatch):
    _reset(monkeypatch)
    client = _client(monkeypatch)

    asyncio.run(client._request("POST", "/api/v5/trade/order", data={"sz": "1"}))

    health = okx_client.get_critical_rest_health_snapshot()
    assert health["mutation"]["request_count"] == 1
    assert health["critical_read"]["request_count"] == 0


def test_process_health_failure_is_fail_open(monkeypatch):
    _reset(monkeypatch)
    client = _client(monkeypatch)
    monkeypatch.setattr(
        okx_client,
        "_record_process_request_health",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("telemetry")),
    )

    assert asyncio.run(client._request("GET", "/api/v5/account/positions"))["code"] == "0"


def test_concurrent_process_updates_do_not_lose_counts(monkeypatch):
    _reset(monkeypatch)
    client = _client(monkeypatch)

    async def scenario():
        await asyncio.gather(*(
            client._request("GET", "/api/v5/public/open-interest")
            for _ in range(40)
        ))

    asyncio.run(scenario())
    assert okx_client.get_critical_rest_health_snapshot()["background"]["request_count"] == 40


def test_process_health_schema_is_bounded_and_snapshot_has_no_network_side_effect(monkeypatch):
    _reset(monkeypatch)
    monkeypatch.setattr(
        okx_client,
        "_new_critical_http_client",
        lambda: (_ for _ in ()).throw(AssertionError("snapshot created a network client")),
    )

    health = okx_client.get_critical_rest_health_snapshot()
    assert set(health) >= {"background", "critical_read", "mutation", "recovery"}
    assert set(health["background"]) == {
        "request_count", "retry_count", "timeout_count", "pool_timeout_count",
        "error_count", "physical_request_count", "total_wall_time_sec", "max_wall_time_sec",
    }
    assert health["recovery"]["active_retired_generations"] == 0


def test_recovery_outside_scan_context_updates_global_recovery(monkeypatch):
    _reset(monkeypatch)

    class Client:
        async def aclose(self):
            return None

    old = Client()
    replacement = Client()
    monkeypatch.setattr(okx_client, "_critical_http_client", old)
    monkeypatch.setattr(okx_client, "_new_critical_http_client", lambda: replacement)
    generation = okx_client._acquire_critical_generation()
    asyncio.run(okx_client._retire_critical_generation(generation, "test outside scan"))

    health = okx_client.get_critical_rest_health_snapshot()
    assert health["recovery"]["current_generation"] == 2
    assert health["recovery"]["recovery_count"] == 1
    assert health["recovery"]["last_recovery_reason"] == "test outside scan"

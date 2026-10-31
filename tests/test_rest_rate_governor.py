import asyncio
import time
from pathlib import Path

import httpx
import pytest

import okx_client
import rest_rate_governor
import telegram_backend
from rest_rate_governor import (
    ProcessRestRateGovernor,
    UnclassifiedRestEndpoint,
    resolve_rest_rate_buckets,
    resolve_rest_rate_spec,
    runtime_endpoint_mapping,
)


def test_runtime_endpoint_mapping_is_explicit_and_scoped():
    mapping = runtime_endpoint_mapping()
    assert "GET /api/v5/account/positions" in mapping
    assert "POST /api/v5/trade/order" in mapping
    assert "GET /api/v5/market/candles" in mapping
    assert all(row["capacity"] > 0 and row["window_sec"] > 0 for row in mapping.values())
    with pytest.raises(UnclassifiedRestEndpoint):
        resolve_rest_rate_spec("GET", "/api/v5/public/new-runtime-endpoint")


def test_documented_rule_keys_preserve_independent_buckets():
    _, account_bucket = resolve_rest_rate_spec("GET", "/api/v5/account/positions")
    _, ticker_a = resolve_rest_rate_spec("GET", "/api/v5/market/ticker", params={"instId": "BTC-USDT-SWAP"})
    _, ticker_b = resolve_rest_rate_spec("GET", "/api/v5/market/ticker", params={"instId": "ETH-USDT-SWAP"})
    assert account_bucket != ticker_a
    assert ticker_a != ticker_b


def test_trade_order_requires_instrument_and_secondary_account_buckets():
    resolved = resolve_rest_rate_buckets(
        "POST", "/api/v5/trade/order", data={"instId": "XRP-USDT-SWAP"}
    )
    assert len(resolved) == 2
    assert resolved[0][1] == "trade/order:instrument:XRP-USDT-SWAP"
    assert resolved[1][1] == "trade/orders-secondary:account"
    assert resolved[1][0].capacity == 1000
    assert resolved[1][0].window_sec == 2.0


def test_process_global_governor_shares_same_bucket_but_not_other_buckets():
    governor = ProcessRestRateGovernor()
    admitted = []

    async def run():
        # Replace the synthetic capacities with a two-per-window budget.
        from rest_rate_governor import RestRateSpec
        rest_rate_governor._SPECS[("GET", "/api/v5/test/a")] = RestRateSpec("test/a", "GET", 2, 0.08, "shared", "test")
        rest_rate_governor._SPECS[("GET", "/api/v5/test/b")] = RestRateSpec("test/b", "GET", 1, 0.08, "shared", "test")
        await asyncio.gather(
            governor.acquire("GET", "/api/v5/test/a", priority="P0"),
            governor.acquire("GET", "/api/v5/test/a", priority="P0"),
        )
        third = asyncio.create_task(governor.acquire("GET", "/api/v5/test/a", priority="P2"))
        await asyncio.sleep(0.01)
        assert not third.done()
        other = asyncio.create_task(governor.acquire("GET", "/api/v5/test/b", priority="P2"))
        await asyncio.wait_for(other, timeout=0.05)
        await asyncio.wait_for(third, timeout=0.2)

    asyncio.run(run())


def test_p0_outranks_queued_p2_and_p2_eventually_progresses():
    governor = ProcessRestRateGovernor()
    from rest_rate_governor import RestRateSpec
    import rest_rate_governor

    rest_rate_governor._SPECS[("GET", "/api/v5/test/priority")] = RestRateSpec(
        "test/priority", "GET", 1, 0.08, "shared", "test",
    )
    order = []

    async def run():
        await governor.acquire("GET", "/api/v5/test/priority", priority="P2")
        p2 = asyncio.create_task(governor.acquire("GET", "/api/v5/test/priority", priority="P2"))
        await asyncio.sleep(0.005)
        p0 = asyncio.create_task(governor.acquire("GET", "/api/v5/test/priority", priority="P0"))
        await asyncio.sleep(0.09)
        assert p0.done()
        order.append("p0")
        await asyncio.wait_for(p2, timeout=0.2)
        order.append("p2")

    asyncio.run(run())
    assert order == ["p0", "p2"]


def test_sustained_priority_load_bounds_p0_and_allows_p2_progress(monkeypatch):
    governor = ProcessRestRateGovernor()
    path = "/api/v5/test/fairness"
    monkeypatch.setitem(
        rest_rate_governor._SPECS,
        ("GET", path),
        rest_rate_governor.RestRateSpec("test/fairness", "GET", 2, 0.02, "shared", "test"),
    )
    completions = []

    async def run():
        async def admit(priority: str):
            started = time.monotonic()
            await governor.acquire("GET", path, priority=priority)
            completions.append((priority, time.monotonic() - started))

        p2_tasks = [asyncio.create_task(admit("P2")) for _ in range(4)]
        await asyncio.sleep(0.001)
        p0_tasks = [asyncio.create_task(admit("P0")) for _ in range(8)]
        await asyncio.gather(*p0_tasks, *p2_tasks)

    asyncio.run(asyncio.wait_for(run(), timeout=1.5))
    snapshot = governor.snapshot()
    assert snapshot["rate_admissions"] == 12
    assert snapshot["multi_bucket_admissions"] == 0
    assert [priority for priority, _ in completions].count("P2") == 4
    assert max(wait for priority, wait in completions if priority == "P0") < 0.35
    assert max(wait for priority, wait in completions if priority == "P2") < 0.75


def test_sustained_p1_load_completes_without_starving_p2(monkeypatch):
    governor = ProcessRestRateGovernor()
    path = "/api/v5/test/p1-throughput"
    monkeypatch.setitem(
        rest_rate_governor._SPECS,
        ("GET", path),
        rest_rate_governor.RestRateSpec("test/p1-throughput", "GET", 2, 0.02, "shared", "test"),
    )
    completed = []

    async def run():
        async def admit(priority: str):
            await governor.acquire("GET", path, priority=priority)
            completed.append(priority)

        tasks = [asyncio.create_task(admit("P1")) for _ in range(10)]
        tasks.extend(asyncio.create_task(admit("P2")) for _ in range(2))
        await asyncio.gather(*tasks)

    asyncio.run(asyncio.wait_for(run(), timeout=1.5))
    assert completed.count("P1") == 10
    assert completed.count("P2") == 2


def test_profit_retention_p0_protection_truth_gets_service_under_status_pressure(monkeypatch):
    governor = ProcessRestRateGovernor()
    path = "/api/v5/test/protection-truth"
    monkeypatch.setitem(
        rest_rate_governor._SPECS,
        ("GET", path),
        rest_rate_governor.RestRateSpec("test/protection-truth", "GET", 2, 0.02, "shared", "test"),
    )
    protection_posts = []

    async def run():
        async def status_read():
            await governor.acquire("GET", path, priority="P2")

        async def protection_truth():
            started = time.monotonic()
            await governor.acquire("GET", path, priority="P0")
            return time.monotonic() - started

        status_tasks = [asyncio.create_task(status_read()) for _ in range(12)]
        await asyncio.sleep(0.001)
        p0 = asyncio.create_task(protection_truth())
        p0_wait = await asyncio.wait_for(p0, timeout=0.5)
        await asyncio.gather(*status_tasks)
        return p0_wait

    p0_wait = asyncio.run(run())
    assert p0_wait < 0.35
    assert protection_posts == []


def test_multi_bucket_admission_is_atomic_and_has_no_partial_consumption(monkeypatch):
    governor = ProcessRestRateGovernor()
    path = "/api/v5/test/multi"
    monkeypatch.setitem(
        rest_rate_governor._SPECS,
        ("POST", path),
        rest_rate_governor.RestRateSpec(
            "test/multi", "POST", 2, 0.05, "shared", "test",
            secondary="subaccount_order",
        ),
    )
    monkeypatch.setitem(
        rest_rate_governor._SECONDARY_SPECS,
        "subaccount_order",
        rest_rate_governor.RestRateSpec("secondary", "POST", 1, 0.05, "shared", "test"),
    )

    async def run():
        first = await governor.prepare("POST", path, data={"instId": "XRP-USDT-SWAP"}, priority="P1")
        await governor.wait_until_admissible(first)
        assert await governor.try_admit(first)
        second = await governor.prepare("POST", path, data={"instId": "XRP-USDT-SWAP"}, priority="P1")
        states = await governor._ticket_states(second)
        assert len(states) == 2
        assert all(len(state["timestamps"]) == 1 for state in states)
        await governor.wait_until_admissible(second)
        assert await governor.try_admit(second)

    asyncio.run(asyncio.wait_for(run(), timeout=1.0))
    snapshot = governor.snapshot()
    assert snapshot["multi_bucket_admissions"] == 2
    assert snapshot["partial_bucket_consumptions"] == 0


def test_pool_rate_race_releases_pool_and_retries_final_admission(monkeypatch):
    governor = okx_client.process_rest_rate_governor
    governor.reset()
    path = "/api/v5/test/pool-race"
    monkeypatch.setitem(
        rest_rate_governor._SPECS,
        ("GET", path),
        rest_rate_governor.RestRateSpec("test/pool-race", "GET", 1, 0.08, "shared", "test"),
    )

    async def run():
        gate = asyncio.Semaphore(0)
        task = asyncio.create_task(
            okx_client._acquire_governed_slot(
                "GET", path, params=None, data=None, priority="P0", request_gate=gate,
            )
        )
        await asyncio.sleep(0.02)
        await governor.acquire("GET", path, priority="P0")
        gate.release()
        ticket, _pool_wait, _rate_wait = await asyncio.wait_for(task, timeout=1.0)
        assert ticket.admitted_at > 0
        gate.release()

    asyncio.run(run())


def test_direct_rate_helper_waits_for_pool_before_consuming_token(monkeypatch):
    governor = rest_rate_governor.process_rest_rate_governor
    governor.reset()
    path = "/api/v5/test/direct-helper"
    monkeypatch.setitem(
        rest_rate_governor._SPECS,
        ("GET", path),
        rest_rate_governor.RestRateSpec("test/direct-helper", "GET", 1, 0.08, "shared", "test"),
    )
    sent = []

    async def run():
        gate = asyncio.Semaphore(1)
        await gate.acquire()

        async def send():
            sent.append(time.monotonic())
            return "ok"

        task = asyncio.create_task(
            rest_rate_governor.run_with_rate_admission(
                "GET", path, priority="P2", pool_gate=gate, send=send,
            )
        )
        await asyncio.sleep(0.03)
        assert governor.snapshot()["rate_admissions"] == 0
        gate.release()
        assert await asyncio.wait_for(task, timeout=0.5) == ("ok", "test/direct-helper:shared")

    asyncio.run(run())
    assert len(sent) == 1
    assert governor.snapshot()["rate_admissions"] == 1


def test_rate_limited_gets_apply_bounded_bucket_pressure(monkeypatch):
    governor = ProcessRestRateGovernor()
    path = "/api/v5/test/pressure"
    monkeypatch.setitem(
        rest_rate_governor._SPECS,
        ("GET", path),
        rest_rate_governor.RestRateSpec("test/pressure", "GET", 10, 0.02, "shared", "test"),
    )

    async def run():
        governor.record_http_429("test/pressure:shared")
        ticket = await governor.prepare("GET", path, priority="P2")
        await asyncio.sleep(0.05)
        assert not await governor.is_admissible(ticket)
        await asyncio.wait_for(governor.wait_until_admissible(ticket), timeout=0.5)
        assert await governor.try_admit(ticket)

    asyncio.run(run())
    assert governor.snapshot()["http_429_responses"] == 1


def test_final_rate_admission_is_immediately_before_send(monkeypatch):
    governor = okx_client.process_rest_rate_governor
    governor.reset()
    path = "/api/v5/test/send-timing"
    monkeypatch.setitem(
        rest_rate_governor._SPECS,
        ("GET", path),
        rest_rate_governor.RestRateSpec("test/send-timing", "GET", 1, 0.08, "shared", "test"),
    )

    async def run():
        gate = asyncio.Semaphore(1)
        ticket, pool_wait, _rate_wait = await okx_client._acquire_governed_slot(
            "GET", path, params=None, data=None, priority="P0", request_gate=gate,
        )
        send_started = time.monotonic()
        gate.release()
        assert send_started - ticket.admitted_at < 0.05
        assert pool_wait < 0.05

    asyncio.run(run())


def test_each_retry_consumes_a_new_rate_admission(monkeypatch):
    okx_client._truth_cache = {}
    okx_client._truth_inflight = {}
    okx_client.PAPER_TRADING = False
    okx_client.OKX_API_KEY = "test-key"
    governor = okx_client.process_rest_rate_governor
    governor.reset()
    calls = 0

    class HTTP:
        async def request(self, *args, **kwargs):
            nonlocal calls
            calls += 1
            if calls == 1:
                return httpx.Response(503, request=httpx.Request("GET", "https://example.test"))
            return httpx.Response(200, request=httpx.Request("GET", "https://example.test"), json={"code": "0", "data": []})

    monkeypatch.setattr(okx_client, "_critical_http_client", HTTP())
    original_sleep = asyncio.sleep
    monkeypatch.setattr(okx_client.asyncio, "sleep", lambda *_args, **_kwargs: original_sleep(0))
    client = okx_client.OKXClient()
    client._headers = lambda *_args: {}
    result = asyncio.run(client._request("GET", "/api/v5/account/positions", priority="P0"))
    assert result["code"] == "0"
    assert calls == 2
    assert governor.snapshot()["rate_admissions"] == 2


def test_http_429_and_business_50011_are_distinct_rate_telemetry(monkeypatch):
    okx_client.PAPER_TRADING = False
    okx_client.OKX_API_KEY = "test-key"
    governor = okx_client.process_rest_rate_governor
    governor.reset()

    class HTTP:
        def __init__(self, response):
            self.response = response

        async def request(self, method, url, **kwargs):
            return self.response

    client = okx_client.OKXClient()
    client._headers = lambda *_args: {}
    monkeypatch.setattr(
        okx_client,
        "_mutation_http_client",
        HTTP(httpx.Response(429, request=httpx.Request("POST", "https://example.test"))),
    )
    with pytest.raises(RuntimeError):
        asyncio.run(client._request("POST", "/api/v5/trade/order", data={"instId": "BTC-USDT-SWAP"}))

    monkeypatch.setattr(
        okx_client,
        "_critical_http_client",
        HTTP(httpx.Response(
            200,
            request=httpx.Request("GET", "https://example.test"),
            json={"code": "50011", "msg": "Too Many Requests", "data": []},
        )),
    )
    result = asyncio.run(client._request("GET", "/api/v5/account/positions", priority="P0"))
    assert result["code"] == "50011"
    snapshot = governor.snapshot()
    assert snapshot["http_429_responses"] == 1
    assert snapshot["business_50011_responses"] == 1
    assert snapshot["rate_limited_responses"] == 2


def test_mutation_rate_wait_does_not_call_before_send_until_transport(monkeypatch):
    okx_client.PAPER_TRADING = False
    okx_client.OKX_API_KEY = "test-key"
    governor = okx_client.process_rest_rate_governor
    governor.reset()
    from rest_rate_governor import RestRateSpec
    import rest_rate_governor
    rest_rate_governor._SPECS[("POST", "/api/v5/test/mutation")] = RestRateSpec(
        "test/mutation", "POST", 1, 0.08, "shared", "test",
    )
    marker = []

    class HTTP:
        async def request(self, *args, **kwargs):
            marker.append("http")
            return httpx.Response(200, request=httpx.Request("POST", "https://example.test"), json={"code": "0", "data": []})

    monkeypatch.setattr(okx_client, "_mutation_http_client", HTTP())
    client = okx_client.OKXClient()
    client._headers = lambda *_args: {}

    async def run():
        await client._request("POST", "/api/v5/test/mutation", before_send=lambda: marker.append("before"))

    asyncio.run(run())
    assert marker == ["before", "http"]


def test_forced_fresh_protection_does_not_join_low_priority_read(monkeypatch):
    okx_client.PAPER_TRADING = False
    okx_client.OKX_API_KEY = "test-key"
    okx_client._truth_cache = {}
    okx_client._truth_inflight = {}
    okx_client._truth_metrics = {"cache_hits": 0, "coalesced_requests": 0, "successful_reads": 0, "failed_reads": 0}
    calls = []
    low_started = asyncio.Event()
    low_release = asyncio.Event()

    class HTTP:
        async def request(self, *args, **kwargs):
            priority = kwargs.get("params", {}).get("ordType")
            calls.append(priority)
            if len(calls) == 1:
                low_started.set()
                await low_release.wait()
            return httpx.Response(200, request=httpx.Request("GET", "https://example.test"), json={"code": "0", "data": []})

    monkeypatch.setattr(okx_client, "_reconciliation_http_client", HTTP())
    monkeypatch.setattr(okx_client, "_critical_http_client", HTTP())
    client = okx_client.OKXClient()
    client._headers = lambda *_args: {}

    async def run():
        low = asyncio.create_task(client.list_live_protection_orders("BTC", force_refresh=False, priority="P2"))
        await asyncio.wait_for(low_started.wait(), timeout=0.5)
        high = await asyncio.wait_for(
            client.list_live_protection_orders("BTC", force_refresh=True, priority="P0"), timeout=0.5,
        )
        low_release.set()
        await low
        return high

    high = asyncio.run(run())
    assert high["ok"] is True
    assert len(calls) == 4


def test_low_priority_account_read_joins_inflight_critical_read(monkeypatch):
    okx_client.PAPER_TRADING = False
    okx_client.OKX_API_KEY = "test-key"
    okx_client._account_snapshot_cache = {"value": None, "ts": 0.0}
    okx_client.process_rest_rate_governor.reset()
    calls = 0
    started = asyncio.Event()
    release = asyncio.Event()

    class HTTP:
        async def request(self, *args, **kwargs):
            nonlocal calls
            calls += 1
            started.set()
            await release.wait()
            return httpx.Response(
                200,
                request=httpx.Request("GET", "https://example.test"),
                json={"code": "0", "data": [{"totalEq": "10", "availEq": "9", "details": []}]},
            )

    monkeypatch.setattr(okx_client, "_critical_http_client", HTTP())
    client = okx_client.OKXClient()
    client._headers = lambda *_args: {}

    async def run():
        critical = asyncio.create_task(client.get_account_snapshot(force_refresh=True, priority="P0"))
        await asyncio.wait_for(started.wait(), timeout=0.5)
        low = asyncio.create_task(client.get_account_snapshot(force_refresh=True, priority="status"))
        await asyncio.sleep(0)
        release.set()
        return await asyncio.gather(critical, low)

    rows = asyncio.run(run())
    assert calls == 1
    assert rows[0]["avail_eq_usdt"] == rows[1]["avail_eq_usdt"] == 9.0
    assert okx_client.process_rest_rate_governor.snapshot()["rate_admissions"] == 1


def test_ten_same_key_callers_have_one_underlying_request_and_admission(monkeypatch):
    okx_client.PAPER_TRADING = False
    okx_client.OKX_API_KEY = "test-key"
    okx_client._account_snapshot_cache = {"value": None, "ts": 0.0}
    okx_client._truth_cache = {}
    okx_client._truth_inflight = {}
    okx_client._truth_metrics = {"cache_hits": 0, "coalesced_requests": 0, "successful_reads": 0, "failed_reads": 0}
    okx_client.process_rest_rate_governor.reset()
    calls = 0
    started = asyncio.Event()
    release = asyncio.Event()

    class HTTP:
        async def request(self, *args, **kwargs):
            nonlocal calls
            calls += 1
            started.set()
            await release.wait()
            return httpx.Response(
                200,
                request=httpx.Request("GET", "https://example.test"),
                json={"code": "0", "data": [{"totalEq": "10", "availEq": "9", "details": []}]},
            )

    monkeypatch.setattr(okx_client, "_critical_http_client", HTTP())
    client = okx_client.OKXClient()
    client._headers = lambda *_args: {}

    async def run():
        tasks = [asyncio.create_task(client.get_account_snapshot(force_refresh=True, priority="P0")) for _ in range(10)]
        await asyncio.wait_for(started.wait(), timeout=0.5)
        release.set()
        return await asyncio.gather(*tasks)

    rows = asyncio.run(run())
    assert len(rows) == 10
    assert calls == 1
    coalescing = okx_client.get_critical_rest_health_snapshot()["coalescing"]
    assert coalescing["logical_callers"] == 10
    assert coalescing["coalesced_requests"] == 9
    assert coalescing["underlying_requests"] == 1
    assert okx_client.process_rest_rate_governor.snapshot()["rate_admissions"] == 1


def test_p0_does_not_join_weaker_p1_account_read(monkeypatch):
    okx_client.PAPER_TRADING = False
    okx_client.OKX_API_KEY = "test-key"
    okx_client._account_snapshot_cache = {"value": None, "ts": 0.0}
    calls = 0
    started = asyncio.Event()
    release = asyncio.Event()

    class HTTP:
        async def request(self, *args, **kwargs):
            nonlocal calls
            calls += 1
            if calls == 1:
                started.set()
                await release.wait()
            return httpx.Response(
                200,
                request=httpx.Request("GET", "https://example.test"),
                json={"code": "0", "data": [{"totalEq": "10", "availEq": "9", "details": []}]},
            )

    monkeypatch.setattr(okx_client, "_critical_http_client", HTTP())
    client = okx_client.OKXClient()
    client._headers = lambda *_args: {}

    async def run():
        p1 = asyncio.create_task(client.get_account_snapshot(force_refresh=True, priority="P1"))
        await asyncio.wait_for(started.wait(), timeout=0.5)
        p0 = asyncio.create_task(client.get_account_snapshot(force_refresh=True, priority="P0"))
        await asyncio.sleep(0)
        release.set()
        await asyncio.gather(p1, p0)

    asyncio.run(run())
    assert calls == 2


def test_p1_does_not_join_weaker_p2_account_read(monkeypatch):
    okx_client.PAPER_TRADING = False
    okx_client.OKX_API_KEY = "test-key"
    okx_client._account_snapshot_cache = {"value": None, "ts": 0.0}
    calls = 0
    started = asyncio.Event()
    release = asyncio.Event()

    class HTTP:
        async def request(self, *args, **kwargs):
            nonlocal calls
            calls += 1
            if calls == 1:
                started.set()
                await release.wait()
            return httpx.Response(
                200,
                request=httpx.Request("GET", "https://example.test"),
                json={"code": "0", "data": [{"totalEq": "10", "availEq": "9", "details": []}]},
            )

    monkeypatch.setattr(okx_client, "_critical_http_client", HTTP())
    monkeypatch.setattr(okx_client, "_reconciliation_http_client", okx_client._critical_http_client)
    client = okx_client.OKXClient()
    client._headers = lambda *_args: {}

    async def run():
        p2 = asyncio.create_task(client.get_account_snapshot(force_refresh=True, priority="status"))
        await asyncio.wait_for(started.wait(), timeout=0.5)
        p1 = asyncio.create_task(client.get_account_snapshot(force_refresh=True, priority="P1"))
        await asyncio.sleep(0.05)
        assert calls == 2
        release.set()
        await asyncio.gather(p2, p1)

    asyncio.run(run())


def test_public_market_ws_has_no_order_mutation_operation():
    source = (Path(__file__).resolve().parents[1] / "ws_stream.py").read_text(encoding="utf-8")
    assert '"op": "subscribe"' in source
    assert '"op": "order"' not in source
    assert '"op": "cancel-order"' not in source
    assert '"op": "amend-order"' not in source


def test_positions_50011_is_not_cached_as_successful_empty_truth(monkeypatch):
    okx_client.PAPER_TRADING = False
    okx_client.OKX_API_KEY = "test-key"
    okx_client._truth_cache = {}
    okx_client._truth_inflight = {}
    okx_client._truth_metrics = {"cache_hits": 0, "coalesced_requests": 0, "successful_reads": 0, "failed_reads": 0}
    okx_client.process_rest_rate_governor.reset()
    calls = 0

    class HTTP:
        async def request(self, *args, **kwargs):
            nonlocal calls
            calls += 1
            return httpx.Response(
                200,
                request=httpx.Request("GET", "https://example.test"),
                json={"code": "50011", "msg": "Too Many Requests", "data": []},
            )

    monkeypatch.setattr(okx_client, "_critical_http_client", HTTP())
    client = okx_client.OKXClient()
    client._headers = lambda *_args: {}

    first = asyncio.run(client.get_positions_status(force_refresh=True, priority="P0"))
    second = asyncio.run(client.get_positions_status(force_refresh=True, priority="P0"))
    assert first["ok"] is False
    assert second["ok"] is False
    assert calls == 2


def test_low_priority_protection_read_joins_inflight_critical_read(monkeypatch):
    okx_client.PAPER_TRADING = False
    okx_client.OKX_API_KEY = "test-key"
    calls = []
    started = asyncio.Event()
    release = asyncio.Event()

    class HTTP:
        async def request(self, *args, **kwargs):
            calls.append(kwargs.get("params", {}).get("ordType"))
            if len(calls) == 1:
                started.set()
                await release.wait()
            return httpx.Response(
                200,
                request=httpx.Request("GET", "https://example.test"),
                json={"code": "0", "data": []},
            )

    monkeypatch.setattr(okx_client, "_critical_http_client", HTTP())
    client = okx_client.OKXClient()
    client._headers = lambda *_args: {}

    async def run():
        critical = asyncio.create_task(client.list_live_protection_orders("BTC", force_refresh=True, priority="P0"))
        await asyncio.wait_for(started.wait(), timeout=0.5)
        low = asyncio.create_task(client.list_live_protection_orders("BTC", force_refresh=True, priority="status"))
        release.set()
        return await asyncio.gather(critical, low)

    first, second = asyncio.run(run())
    assert first["ok"] and second["ok"]
    assert sorted(calls) == ["conditional", "oco"]


def test_telegram_cached_account_preserves_observation_age(monkeypatch):
    telegram_backend._operational_exchange_cache.clear()
    observed_at = (time.time() - 12.0)
    observed_iso = telegram_backend.datetime.fromtimestamp(observed_at, telegram_backend.timezone.utc).isoformat()

    class OKX:
        async def get_account_snapshot(self, **kwargs):
            return {"total_eq_usdt": 10.0, "avail_eq_usdt": 9.0, "observed_at": observed_iso, "received_at": observed_iso}

    async def positions():
        return {"ok": True, "source": "EXCHANGE_TRUTH", "fetched_at": observed_iso, "positions": {}}

    account, position = asyncio.run(
        telegram_backend.fast_operational_exchange_snapshot(OKX(), positions, timeout_sec=0.5)
    )
    assert account["age_sec"] >= 11.0
    assert position["age_sec"] >= 11.0
    assert account["generated_at"] == observed_iso
    assert position["generated_at"] == observed_iso


def test_telegram_mixed_snapshot_preserves_component_observation_ages():
    base = time.time() - 15.0
    account_iso = telegram_backend.datetime.fromtimestamp(
        base, telegram_backend.timezone.utc
    ).isoformat()
    positions_iso = telegram_backend.datetime.fromtimestamp(
        base + 4.0, telegram_backend.timezone.utc
    ).isoformat()

    class OKX:
        async def get_account_snapshot(self, **kwargs):
            return {"total_eq_usdt": 10.0, "avail_eq_usdt": 9.0, "observed_at": account_iso}

    async def positions():
        return {
            "ok": True,
            "source": "EXCHANGE_TRUTH",
            "fetched_at": positions_iso,
            "positions": {},
        }

    account, position = asyncio.run(
        telegram_backend.fast_operational_exchange_snapshot(OKX(), positions, timeout_sec=0.5)
    )
    assert account["age_sec"] > position["age_sec"]
    assert account["generated_at"] == account_iso
    assert position["generated_at"] == positions_iso

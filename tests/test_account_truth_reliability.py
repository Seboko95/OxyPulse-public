from __future__ import annotations

import asyncio
import json
import inspect
from dataclasses import replace

import httpx
import pytest

import category_engine
import execution_router
import latency_telemetry
import live_position_truth as lpt
import okx_client
import position_registry
import trading_engine
from capital_reservations import CapitalReservationBook
from execution_router import ExecutionRouter
from order_state_store import OrderStateStore
from test_adaptive_live_phase2 import FakeClient, FakeInstruments, FakeLiveState, request


def _reset_critical_state(monkeypatch):
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
    monkeypatch.setattr(okx_client, "_process_rest_health", okx_client._new_process_rest_health())
    monkeypatch.setattr(okx_client, "_critical_request_gate", None)
    monkeypatch.setattr(okx_client, "_mutation_request_gate", None)


def _response(path="/api/v5/account/positions"):
    return httpx.Response(
        200,
        request=httpx.Request("GET", okx_client.BASE_URL + path),
        json={"code": "0", "data": []},
    )


def test_critical_read_recovery_keeps_get_attempt_budget_and_next_request_uses_new_generation(monkeypatch):
    _reset_critical_state(monkeypatch)
    calls = []

    class FailingHTTP:
        async def request(self, *args, **kwargs):
            calls.append("old")
            raise httpx.PoolTimeout("pool exhausted")

        async def aclose(self):
            return None

    class ReplacementHTTP:
        def __init__(self):
            self.calls = 0

        async def request(self, *args, **kwargs):
            self.calls += 1
            return _response()

        async def aclose(self):
            return None

    replacement = ReplacementHTTP()
    monkeypatch.setattr(okx_client, "_critical_http_client", FailingHTTP())
    monkeypatch.setattr(okx_client, "_new_critical_http_client", lambda: replacement)
    original_sleep = asyncio.sleep
    monkeypatch.setattr(okx_client.asyncio, "sleep", lambda *_args, **_kwargs: original_sleep(0))
    client = okx_client.OKXClient()
    monkeypatch.setattr(client, "_headers", lambda *_args: {})

    with pytest.raises(RuntimeError, match="PoolTimeout"):
        asyncio.run(client._request("GET", "/api/v5/account/positions"))

    assert len(calls) == 4
    health = okx_client.get_critical_rest_health_snapshot()
    assert health["critical_read"]["retry_count"] == 3
    assert health["client_recovery"]["recovery_count"] == 1

    result = asyncio.run(client._request("GET", "/api/v5/account/positions"))
    assert result["code"] == "0"
    assert replacement.calls == 1


def test_only_one_replacement_generation_is_created_for_concurrent_failures(monkeypatch):
    _reset_critical_state(monkeypatch)
    factory_calls = []

    class FailingHTTP:
        async def request(self, *args, **kwargs):
            raise httpx.ConnectTimeout("connect failed")

        async def aclose(self):
            return None

    class ReplacementHTTP:
        async def request(self, *args, **kwargs):
            return _response()

        async def aclose(self):
            return None

    monkeypatch.setattr(okx_client, "_critical_http_client", FailingHTTP())

    def factory():
        factory_calls.append(True)
        return ReplacementHTTP()

    monkeypatch.setattr(okx_client, "_new_critical_http_client", factory)
    original_sleep = asyncio.sleep
    monkeypatch.setattr(okx_client.asyncio, "sleep", lambda *_args, **_kwargs: original_sleep(0))

    async def run():
        client = okx_client.OKXClient()
        monkeypatch.setattr(client, "_headers", lambda *_args: {})
        return await asyncio.gather(
            client._request("GET", "/api/v5/account/positions"),
            client._request("GET", "/api/v5/account/positions"),
            return_exceptions=True,
        )

    results = asyncio.run(run())
    assert all(isinstance(item, RuntimeError) for item in results)
    assert len(factory_calls) == 1
    assert okx_client.get_critical_rest_health_snapshot()["client_recovery"]["recovery_count"] == 1


def test_retired_read_generation_waits_for_active_request_before_close(monkeypatch):
    _reset_critical_state(monkeypatch)
    closed = []

    class HTTP:
        async def aclose(self):
            closed.append("old")

    replacement = HTTP()
    monkeypatch.setattr(okx_client, "_critical_http_client", HTTP())
    monkeypatch.setattr(okx_client, "_new_critical_http_client", lambda: replacement)
    generation = okx_client._acquire_critical_generation()
    asyncio.run(okx_client._retire_critical_generation(generation, "test"))
    assert closed == []
    asyncio.run(okx_client._release_critical_generation(generation))
    assert closed == ["old"]


def test_read_recovery_never_replaces_or_replays_mutation_client(monkeypatch):
    _reset_critical_state(monkeypatch)
    calls = []

    class MutationHTTP:
        async def request(self, *args, **kwargs):
            calls.append(True)
            raise httpx.ConnectTimeout("mutation timeout")

        async def aclose(self):
            return None

    monkeypatch.setattr(okx_client, "_mutation_http_client", MutationHTTP())
    monkeypatch.setattr(okx_client, "_new_critical_http_client", lambda: (_ for _ in ()).throw(AssertionError("read recovery touched mutation")))
    client = okx_client.OKXClient()
    monkeypatch.setattr(client, "_headers", lambda *_args: {})

    with pytest.raises(RuntimeError, match="ConnectTimeout"):
        asyncio.run(client._request("POST", "/api/v5/trade/order", data={"sz": "1"}))
    assert len(calls) == 1
    assert okx_client.get_critical_rest_health_snapshot()["client_recovery"]["recovery_count"] == 0


def test_cancelled_critical_read_drains_retired_generation_without_retry(monkeypatch):
    _reset_critical_state(monkeypatch)
    started = asyncio.Event()

    class BlockingHTTP:
        def __init__(self):
            self.calls = 0
            self.closed = 0

        async def request(self, *args, **kwargs):
            self.calls += 1
            started.set()
            await asyncio.Event().wait()

        async def aclose(self):
            self.closed += 1

    class ReplacementHTTP:
        def __init__(self):
            self.calls = 0

        async def request(self, *args, **kwargs):
            self.calls += 1
            return _response()

        async def aclose(self):
            return None

    old = BlockingHTTP()
    replacement = ReplacementHTTP()
    mutation = object()
    monkeypatch.setattr(okx_client, "_critical_http_client", old)
    monkeypatch.setattr(okx_client, "_mutation_http_client", mutation)
    monkeypatch.setattr(okx_client, "_new_critical_http_client", lambda: replacement)
    client = okx_client.OKXClient()
    monkeypatch.setattr(client, "_headers", lambda *_args: {})

    async def scenario():
        task = asyncio.create_task(client._request("GET", "/api/v5/account/positions"))
        await asyncio.wait_for(started.wait(), timeout=0.5)
        generation = okx_client._critical_current_generation
        assert generation is not None
        assert generation.active_requests == 1

        await okx_client._retire_critical_generation(generation, "test_retire")
        assert generation.active_requests == 1
        assert old.closed == 0

        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        assert generation.active_requests == 0
        assert generation.active_requests >= 0
        assert old.closed == 1
        health = okx_client.get_critical_rest_health_snapshot()
        assert health["client_recovery"]["retired_generations_pending_close"] == 0
        assert okx_client._mutation_http_client is mutation

        result = await client._request("GET", "/api/v5/account/positions")
        assert result["code"] == "0"
        assert old.calls == 1
        assert replacement.calls == 1

    asyncio.run(scenario())


class LiveClient(FakeClient):
    def __init__(self, *, positions_ok=True):
        super().__init__()
        self.positions_ok = positions_ok
        self.position_calls = 0
        self.transport_calls = 0
        self._filled_positions = []

    def _record_successful_fill(self):
        self._filled_positions = [{
            "instId": "TEST-USDT-SWAP",
            "pos": "10",
            "posSide": "long",
            "lever": "10",
            "avgPx": "100",
        }]

    async def get_account_snapshot(self):
        return {"avail_eq_usdt": 50.0, "total_eq_usdt": 50.0}

    async def get_account_config(self):
        return {"posMode": "long_short_mode"}

    async def get_positions_status(self):
        self.position_calls += 1
        if not self.positions_ok:
            return {"ok": False, "positions": None, "reason": "PoolTimeout"}
        return {"ok": True, "positions": [dict(row) for row in self._filled_positions], "reason": ""}

    async def get_execution_ticker(self, coin):
        return {"last": 100.0}

    async def place_market_order(self, *args, **kwargs):
        self.market_calls += 1
        kwargs["on_exchange_request"]()
        self.transport_calls += 1
        self._record_successful_fill()
        return {"ordId": "O1", "fillPx": "100", "contracts": 10, "actual_coin": "TEST"}


def _live_route(tmp_path, client):
    return ExecutionRouter(
        client,
        instruments=FakeInstruments(),
        reservations=CapitalReservationBook(tmp_path / "reservations.json"),
        orders=OrderStateStore(tmp_path / "orders.json"),
        live_state=FakeLiveState(),
    )


def _seed_pre_submit(monkeypatch, tmp_path, coin="TEST"):
    monkeypatch.setattr(position_registry, "_PATH", tmp_path / "position_registry.json")
    monkeypatch.setattr(
        position_registry,
        "_registry",
        {
            coin: {
                "direction": "long",
                "engine": "TEST",
                "size_usdt": 4.0,
                "entry": 0.0,
                "leverage": 10,
                "stop_loss": 99.0,
                "take_profit": 103.0,
                "order_id": "",
                "pending_reservation": True,
                "entry_lifecycle": "PRE_SUBMIT_RESERVED",
                "mutation_attempted": False,
            }
        },
    )


def test_fresh_position_truth_allows_normal_live_execution(monkeypatch, tmp_path):
    monkeypatch.setenv("PAPER_TRADING", "false")
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    monkeypatch.setattr(execution_router._ss, "get_effective_min_order_usdt", lambda balance: 1.0)
    monkeypatch.setattr(execution_router._ss, "get_tier", lambda name: {"min_trade_usdt": 0.0})
    monkeypatch.setattr(execution_router._ss, "get_val", lambda key, default=None: default)
    import market_data_stream
    monkeypatch.setattr(market_data_stream, "execution_price_snapshot", lambda *args: {"fresh": True, "price": 100.0, "age_sec": 0.0})
    lpt.reset_runtime_state()
    _seed_pre_submit(monkeypatch, tmp_path)
    client = LiveClient()
    route = _live_route(tmp_path, client)
    ok, _, rid = asyncio.run(route.reservations.reserve(
        intent_id="truth-ok", candidate_id="candidate", margin_usdt=1,
        risk_usdt=0.1, max_total_margin_usdt=100,
    ))
    assert ok
    result = asyncio.run(route.submit(replace(request(intent="truth-ok"), reservation_id=rid)))
    assert result.ok
    # One fresh truth read gates admission; the lifecycle read adopts the
    # exchange-effective leverage after the fill.
    assert client.position_calls == 2
    assert "PAYLOAD_READY" in [h["state"] for h in route.orders.get("truth-ok")["history"]]


def test_router_ignores_only_its_explicit_pre_submit_reservation_and_marks_fill(
    monkeypatch, tmp_path,
):
    monkeypatch.setenv("PAPER_TRADING", "false")
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    monkeypatch.setattr(execution_router._ss, "get_effective_min_order_usdt", lambda balance: 1.0)
    monkeypatch.setattr(execution_router._ss, "get_tier", lambda name: {"min_trade_usdt": 0.0})
    monkeypatch.setattr(execution_router._ss, "get_val", lambda key, default=None: default)
    import market_data_stream

    monkeypatch.setattr(
        market_data_stream,
        "execution_price_snapshot",
        lambda *args: {"fresh": True, "price": 100.0, "age_sec": 0.0},
    )
    lpt.reset_runtime_state()
    monkeypatch.setattr(
        position_registry,
        "_registry",
        {
            "TEST": {
                "direction": "long",
                "engine": "TEST",
                "size_usdt": 4.0,
                "entry": 0.0,
                "leverage": 10,
                "pending_reservation": True,
                "entry_lifecycle": "PRE_SUBMIT_RESERVED",
                "mutation_attempted": False,
                "order_id": "",
            }
        },
    )
    monkeypatch.setattr(position_registry, "_PATH", tmp_path / "position_registry.json")
    client = LiveClient()
    route = _live_route(tmp_path, client)
    ok, _, rid = asyncio.run(route.reservations.reserve(
        intent_id="pre-submit", candidate_id="candidate", margin_usdt=1,
        risk_usdt=0.1, max_total_margin_usdt=100,
    ))
    assert ok

    result = asyncio.run(route.submit(replace(request(intent="pre-submit"), reservation_id=rid)))

    assert result.ok
    assert position_registry._registry["TEST"]["entry_lifecycle"] == "FILLED"
    assert position_registry._registry["TEST"]["mutation_attempted"] is True
    assert "PAYLOAD_READY" in [h["state"] for h in route.orders.get("pre-submit")["history"]]


def test_uncertain_submission_marks_pending_registry_and_remains_visible(
    monkeypatch, tmp_path,
):
    monkeypatch.setenv("PAPER_TRADING", "false")
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    monkeypatch.setattr(execution_router._ss, "get_effective_min_order_usdt", lambda balance: 1.0)
    monkeypatch.setattr(execution_router._ss, "get_tier", lambda name: {"min_trade_usdt": 0.0})
    monkeypatch.setattr(execution_router._ss, "get_val", lambda key, default=None: default)
    import market_data_stream

    monkeypatch.setattr(
        market_data_stream,
        "execution_price_snapshot",
        lambda *args: {"fresh": True, "price": 100.0, "age_sec": 0.0},
    )
    lpt.reset_runtime_state()
    monkeypatch.setattr(
        position_registry,
        "_registry",
        {
            "TEST": {
                "direction": "long",
                "engine": "TEST",
                "size_usdt": 4.0,
                "entry": 0.0,
                "leverage": 10,
                "pending_reservation": True,
                "entry_lifecycle": "PRE_SUBMIT_RESERVED",
                "mutation_attempted": False,
                "order_id": "",
            }
        },
    )
    monkeypatch.setattr(position_registry, "_PATH", tmp_path / "position_registry.json")

    class UncertainClient(LiveClient):
        async def place_market_order(self, *args, **kwargs):
            self.market_calls += 1
            kwargs["on_exchange_request"]()
            raise TimeoutError("unknown submit outcome")

    client = UncertainClient()
    route = _live_route(tmp_path, client)
    ok, _, rid = asyncio.run(route.reservations.reserve(
        intent_id="uncertain-pending", candidate_id="candidate", margin_usdt=1,
        risk_usdt=0.1, max_total_margin_usdt=100,
    ))
    assert ok

    result = asyncio.run(
        route.submit(replace(request(intent="uncertain-pending"), reservation_id=rid))
    )

    assert not result.ok
    assert result.state == "SUBMIT_UNCERTAIN"
    assert position_registry._registry["TEST"]["entry_lifecycle"] == "SUBMISSION_ATTEMPTED"
    assert position_registry._registry["TEST"]["mutation_attempted"] is True

    snapshot = asyncio.run(
        lpt.get_live_position_snapshot(
            client,
            registry=position_registry.get_all_details(),
            settings=execution_router._ss,
            force_refresh=True,
        )
    )
    assert "TEST" in snapshot["positions"]
    assert not lpt.execution_position_truth_is_safe(snapshot, execution_router._ss)


def test_generic_category_cleanup_preserves_mutation_attempted_registry_row(monkeypatch, tmp_path):
    monkeypatch.setattr(position_registry, "_PATH", tmp_path / "position_registry.json")
    monkeypatch.setattr(
        position_registry,
        "_registry",
        {
            "TEST": {
                "direction": "short",
                "engine": "TEST",
                "size_usdt": 4.0,
                "entry": 0.0,
                "stop_loss": 101.0,
                "take_profit": 90.0,
                "order_id": "",
                "pending_reservation": True,
                "entry_lifecycle": "SUBMISSION_ATTEMPTED",
                "mutation_attempted": True,
            }
        },
    )

    category_engine._release("TEST", "TEST", "short", 1.0, "submit_uncertain")

    assert "TEST" in position_registry._registry
    assert position_registry._registry["TEST"]["mutation_attempted"] is True


def test_generic_category_cleanup_releases_only_explicit_pre_submit_row(monkeypatch, tmp_path):
    monkeypatch.setattr(position_registry, "_PATH", tmp_path / "position_registry.json")
    monkeypatch.setattr(
        position_registry,
        "_registry",
        {
            "TEST": {
                "direction": "long",
                "engine": "TEST",
                "size_usdt": 4.0,
                "entry": 0.0,
                "stop_loss": 99.0,
                "take_profit": 103.0,
                "order_id": "",
                "pending_reservation": True,
                "entry_lifecycle": "PRE_SUBMIT_RESERVED",
                "mutation_attempted": False,
            }
        },
    )

    category_engine._release("TEST", "TEST", "long", 1.0, "local_rejection")

    assert "TEST" not in position_registry._registry


def test_connect_timeout_is_one_post_and_submit_uncertain(monkeypatch, tmp_path):
    monkeypatch.setenv("PAPER_TRADING", "false")
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    monkeypatch.setattr(execution_router._ss, "get_effective_min_order_usdt", lambda balance: 1.0)
    monkeypatch.setattr(execution_router._ss, "get_tier", lambda name: {"min_trade_usdt": 0.0})
    monkeypatch.setattr(execution_router._ss, "get_val", lambda key, default=None: default)
    import market_data_stream
    monkeypatch.setattr(
        market_data_stream,
        "execution_price_snapshot",
        lambda *args: {"fresh": True, "price": 100.0, "age_sec": 0.0},
    )
    _seed_pre_submit(monkeypatch, tmp_path)

    class ConnectTimeoutClient(LiveClient):
        async def mutation_transport_ready(self):
            return True, ""

        async def place_market_order(self, *args, **kwargs):
            self.market_calls += 1
            kwargs["on_exchange_request"]()
            raise httpx.ConnectTimeout("connect failed")

    client = ConnectTimeoutClient()
    route = _live_route(tmp_path, client)
    ok, _, rid = asyncio.run(route.reservations.reserve(
        intent_id="connect-timeout", candidate_id="candidate", margin_usdt=1,
        risk_usdt=0.1, max_total_margin_usdt=100,
    ))
    assert ok

    result = asyncio.run(route.submit(replace(request(intent="connect-timeout"), reservation_id=rid)))

    assert not result.ok
    assert result.state == "SUBMIT_UNCERTAIN"
    assert client.market_calls == 1
    assert route.orders.get("connect-timeout")["state"] == "SUBMIT_UNCERTAIN"


def _seed_uncertain_intent(route, req, rid):
    route.orders.create({
        **req.__dict__,
        "client_order_id": execution_router.deterministic_client_order_id(req.intent_id),
        "reservation_id": rid,
        "state": "SUBMIT_UNCERTAIN",
    })


class ExplicitRecoveryClient(LiveClient):
    def __init__(self, *, detail, history=None, fills=None, positions=None):
        super().__init__()
        self.detail = detail
        self.history = history if history is not None else []
        self.fills = fills if fills is not None else []
        self.positions = positions if positions is not None else []

    async def get_order_status(self, coin, **kwargs):
        self.query_calls += 1
        if isinstance(self.detail, BaseException):
            raise self.detail
        return dict(self.detail)

    async def get_order_history_status(self, **kwargs):
        if isinstance(self.history, BaseException):
            raise self.history
        if isinstance(self.history, dict):
            return dict(self.history)
        return {"ok": True, "orders": list(self.history), "reason": ""}

    async def get_fills_status(self, **kwargs):
        if isinstance(self.fills, BaseException):
            raise self.fills
        if isinstance(self.fills, dict):
            return dict(self.fills)
        return {"ok": True, "fills": list(self.fills), "reason": ""}

    async def get_positions_status(self):
        self.position_calls += 1
        if isinstance(self.positions, BaseException):
            raise self.positions
        if isinstance(self.positions, dict):
            return dict(self.positions)
        return {"ok": True, "positions": list(self.positions), "reason": ""}


def _not_found_detail():
    return {
        "ok": True,
        "found": False,
        "classification": "NOT_FOUND",
        "order": {},
        "reason": "order_not_found",
        "exchange_code": "100011",
    }


def _run_explicit_recovery(monkeypatch, tmp_path, client, intent):
    monkeypatch.setenv("PAPER_TRADING", "false")
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    _seed_pre_submit(monkeypatch, tmp_path)
    route = _live_route(tmp_path, client)
    req = replace(request(intent=intent), reservation_id="reservation")
    _seed_uncertain_intent(route, req, "reservation")
    result = asyncio.run(route._recover_uncertain(route.orders.get(req.intent_id), req))
    return result, route, req


def test_okx_order_detail_classifies_structured_order_not_found(monkeypatch):
    monkeypatch.setattr(okx_client, "PAPER_TRADING", False)
    client = okx_client.OKXClient()

    async def request(*_args, **_kwargs):
        return {"code": "0", "data": [{"sCode": "100011", "sMsg": "Order does not exist"}]}

    monkeypatch.setattr(client, "_request", request)
    result = asyncio.run(client.get_order_status("ETH", client_order_id="CID"))

    assert result["ok"] is True
    assert result["found"] is False
    assert result["classification"] == "NOT_FOUND"
    assert result["exchange_code"] == "100011"
    assert result["order"] == {}


def test_okx_order_detail_keeps_unknown_api_error_as_query_failed(monkeypatch):
    monkeypatch.setattr(okx_client, "PAPER_TRADING", False)
    client = okx_client.OKXClient()

    async def request(*_args, **_kwargs):
        return {"code": "50101", "msg": "API key does not match"}

    monkeypatch.setattr(client, "_request", request)
    result = asyncio.run(client.get_order_status("ETH", client_order_id="CID"))

    assert result["ok"] is False
    assert result["found"] is False
    assert result["classification"] == "QUERY_FAILED"


def test_explicit_not_found_with_clean_evidence_resolves_without_replay(monkeypatch, tmp_path):
    client = ExplicitRecoveryClient(detail=_not_found_detail())
    result, route, req = _run_explicit_recovery(monkeypatch, tmp_path, client, "explicit-empty")

    assert not result.ok
    # A bounded NOT_FOUND response does not prove that a timed-out POST never
    # mutated the exchange.  The current router therefore retains recovery
    # ownership instead of terminalizing from absence alone.
    assert result.state == "SUBMIT_UNCERTAIN"
    assert "uncertain_submission_no_authoritative_evidence" in result.reason
    assert client.market_calls == 0
    assert route.orders.get(req.intent_id)["recovery_evidence"]["order_detail"]["classification"] == "NOT_FOUND"
    assert "TEST" in position_registry._registry


@pytest.mark.parametrize(
    "source",
    [
        {"history": {"ok": False, "orders": [], "reason": "PoolTimeout"}},
        {"fills": {"ok": False, "fills": [], "reason": "ConnectTimeout"}},
        {"positions": {"ok": False, "positions": None, "reason": "401 unauthorized"}},
    ],
)
def test_explicit_not_found_with_untrusted_evidence_stays_uncertain(monkeypatch, tmp_path, source):
    client = ExplicitRecoveryClient(detail=_not_found_detail(), **source)
    result, route, req = _run_explicit_recovery(monkeypatch, tmp_path, client, "explicit-query-failure")

    assert not result.ok
    assert result.state == "SUBMIT_UNCERTAIN"
    assert route.orders.get(req.intent_id)["state"] == "SUBMIT_UNCERTAIN"
    assert "recovery_query_failed" in route.orders.get(req.intent_id)["submit_error"]
    assert client.market_calls == 0
    assert "TEST" in position_registry._registry


def test_explicit_not_found_with_matching_history_is_not_resolved_absent(monkeypatch, tmp_path):
    intent = "explicit-history-match"
    client_order_id = execution_router.deterministic_client_order_id(intent)
    client = ExplicitRecoveryClient(
        detail=_not_found_detail(),
        history=[{"ordId": "H1", "clOrdId": client_order_id, "state": "live"}],
    )
    result, route, _ = _run_explicit_recovery(monkeypatch, tmp_path, client, intent)

    assert result.state == "ACKNOWLEDGED"
    assert route.orders.unresolved()[0]["state"] == "ACKNOWLEDGED"
    assert client.market_calls == 0


def test_explicit_not_found_with_matching_fill_recovers_without_replay(monkeypatch, tmp_path):
    intent = "explicit-fill-match"
    client_order_id = execution_router.deterministic_client_order_id(intent)
    client = ExplicitRecoveryClient(
        detail=_not_found_detail(),
        fills=[{"ordId": "F1", "clOrdId": client_order_id, "fillId": "fill-1", "fillSz": "10", "fillPx": "100"}],
        positions=[{"instId": "TEST-USDT-SWAP", "pos": "10", "posSide": "long", "lever": "10", "avgPx": "100"}],
    )
    result, _, _ = _run_explicit_recovery(monkeypatch, tmp_path, client, intent)

    # Event fills and a same-symbol position are correlated evidence, but they
    # cannot replace an authoritative order cumulative-fill field.
    assert not result.ok
    assert result.state == "SUBMIT_UNCERTAIN"
    assert "correlated_fill_without_authoritative_order_cumulative_fill" in result.reason
    assert client.market_calls == 0


def test_explicit_not_found_with_exchange_position_recovers_without_replay(monkeypatch, tmp_path):
    client = ExplicitRecoveryClient(
        detail=_not_found_detail(),
        positions=[{"instId": "TEST-USDT-SWAP", "pos": "10", "posSide": "long", "lever": "10", "avgPx": "100"}],
    )
    result, _, _ = _run_explicit_recovery(monkeypatch, tmp_path, client, "explicit-position-match")

    assert not result.ok
    assert result.state == "SUBMIT_UNCERTAIN"
    assert "same_symbol_position_without_authoritative_order_cumulative_fill" in result.reason
    assert client.market_calls == 0


@pytest.mark.parametrize(
    "failure",
    [
        httpx.ConnectTimeout("read unavailable"),
        httpx.PoolTimeout("pool unavailable"),
        RuntimeError("401 unauthorized"),
    ],
    ids=["connect-timeout", "pool-timeout", "auth-error"],
)
def test_order_detail_query_failures_never_clear_uncertain_intent(monkeypatch, tmp_path, failure):
    client = ExplicitRecoveryClient(detail=failure)
    result, route, req = _run_explicit_recovery(monkeypatch, tmp_path, client, "explicit-detail-failure")

    assert not result.ok
    assert result.state == "SUBMIT_UNCERTAIN"
    assert route.orders.get(req.intent_id)["state"] == "SUBMIT_UNCERTAIN"
    assert client.market_calls == 0


def test_uncertain_recovery_by_fill_restores_registry_and_protection(monkeypatch, tmp_path):
    monkeypatch.setenv("PAPER_TRADING", "false")
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    _seed_pre_submit(monkeypatch, tmp_path)

    class FilledClient(LiveClient):
        async def get_order(self, coin, **kwargs):
            self.query_calls += 1
            return {"ordId": "O-FILL", "clOrdId": kwargs.get("client_order_id"), "state": "filled", "accFillSz": "10", "avgPx": "100"}

        async def get_order_history(self, **kwargs):
            return []

        async def get_fills(self, **kwargs):
            return [{"ordId": "O-FILL", "clOrdId": kwargs.get("client_order_id", ""), "fillId": "F1", "fillSz": "10", "fillPx": "100", "fee": "-0.01"}]

        async def get_positions_status(self):
            return {
                "ok": True,
                "positions": [{
                    "instId": "TEST-USDT-SWAP",
                    "pos": "10",
                    "posSide": "long",
                    "lever": "10",
                    "avgPx": "100",
                }],
                "reason": "",
            }

    client = FilledClient()
    route = _live_route(tmp_path, client)
    req = replace(request(intent="recover-fill"), reservation_id="reservation")
    _seed_uncertain_intent(route, req, "reservation")

    result = asyncio.run(route._recover_uncertain(route.orders.get(req.intent_id), req))

    assert result.ok
    assert result.state == "FILLED"
    assert client.market_calls == 0
    assert position_registry._registry["TEST"]["entry_lifecycle"] == "FILLED"
    assert position_registry._registry["TEST"]["mutation_attempted"] is True
    assert position_registry._registry["TEST"]["exchange_confirmed"] is True
    assert route.orders.get(req.intent_id)["protection"]["protected"] is True


def test_uncertain_recovery_confirmed_not_found_is_terminal_without_replay(monkeypatch, tmp_path):
    monkeypatch.setenv("PAPER_TRADING", "false")
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    _seed_pre_submit(monkeypatch, tmp_path)

    class EmptyRecoveryClient(LiveClient):
        async def get_order(self, coin, **kwargs):
            self.query_calls += 1
            return {}

        async def get_order_history(self, **kwargs):
            return []

        async def get_fills(self, **kwargs):
            # An uncorrelated fill must never be accepted as evidence for
            # this uncertain client order.
            return [{"fillSz": "10", "fillPx": "100"}]

        async def get_positions_status(self):
            return {"ok": True, "positions": [], "reason": ""}

    client = EmptyRecoveryClient()
    route = _live_route(tmp_path, client)
    req = replace(request(intent="recover-empty"), reservation_id="reservation")
    _seed_uncertain_intent(route, req, "reservation")

    result = asyncio.run(route._recover_uncertain(route.orders.get(req.intent_id), req))

    assert not result.ok
    assert result.state == "SUBMIT_UNCERTAIN"
    assert "uncertain_submission_no_authoritative_evidence" in result.reason
    assert client.market_calls == 0
    assert "TEST" in position_registry._registry


def test_uncertain_recovery_query_failure_preserves_uncertainty(monkeypatch, tmp_path):
    monkeypatch.setenv("PAPER_TRADING", "false")
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    _seed_pre_submit(monkeypatch, tmp_path)

    class FailedRecoveryClient(LiveClient):
        async def get_order(self, coin, **kwargs):
            raise httpx.ConnectTimeout("read unavailable")

        async def get_order_history(self, **kwargs):
            raise httpx.ConnectTimeout("history unavailable")

        async def get_fills(self, **kwargs):
            raise httpx.ConnectTimeout("fills unavailable")

        async def get_positions_status(self):
            return {"ok": False, "positions": None, "reason": "read unavailable"}

    client = FailedRecoveryClient()
    route = _live_route(tmp_path, client)
    req = replace(request(intent="recover-failed"), reservation_id="reservation")
    _seed_uncertain_intent(route, req, "reservation")

    result = asyncio.run(route._recover_uncertain(route.orders.get(req.intent_id), req))

    assert not result.ok
    assert result.state == "SUBMIT_UNCERTAIN"
    assert "TEST" in position_registry._registry
    assert route.orders.get(req.intent_id)["state"] == "SUBMIT_UNCERTAIN"


def test_uncertain_intent_survives_store_reload(tmp_path):
    path = tmp_path / "orders.json"
    first = OrderStateStore(path)
    first.create({"intent_id": "restart-uncertain", "client_order_id": "CID", "state": "SUBMIT_UNCERTAIN"})

    second = OrderStateStore(path)

    assert second.get("restart-uncertain")["state"] == "SUBMIT_UNCERTAIN"


def test_mutation_readiness_failure_blocks_before_payload_and_post(monkeypatch, tmp_path):
    monkeypatch.setenv("PAPER_TRADING", "false")
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    monkeypatch.setattr(execution_router._ss, "get_effective_min_order_usdt", lambda balance: 1.0)
    monkeypatch.setattr(execution_router._ss, "get_tier", lambda name: {"min_trade_usdt": 0.0})
    monkeypatch.setattr(execution_router._ss, "get_val", lambda key, default=None: default)
    import market_data_stream
    monkeypatch.setattr(
        market_data_stream,
        "execution_price_snapshot",
        lambda *args: {"fresh": True, "price": 100.0, "age_sec": 0.0},
    )
    _seed_pre_submit(monkeypatch, tmp_path)

    class NotReadyClient(LiveClient):
        async def mutation_transport_ready(self):
            return False, "ConnectTimeout"

    client = NotReadyClient()
    route = _live_route(tmp_path, client)
    ok, _, rid = asyncio.run(route.reservations.reserve(
        intent_id="readiness-block", candidate_id="candidate", margin_usdt=1,
        risk_usdt=0.1, max_total_margin_usdt=100,
    ))
    assert ok

    result = asyncio.run(route.submit(replace(request(intent="readiness-block"), reservation_id=rid)))

    assert not result.ok
    assert "mutation_transport_not_ready" in result.reason
    assert client.market_calls == 0
    assert "PAYLOAD_READY" not in [h["state"] for h in route.orders.get("readiness-block")["history"]]


def test_mutation_readiness_uses_dedicated_authenticated_get_only(monkeypatch):
    calls = []

    class ReadinessHTTP:
        async def request(self, method, url, **kwargs):
            calls.append((method, str(url)))
            return httpx.Response(
                200,
                request=httpx.Request(method, str(url)),
                json={"code": "0", "data": [{"posMode": "long_short_mode"}]},
            )

        async def aclose(self):
            return None

    monkeypatch.setattr(okx_client, "_mutation_http_client", ReadinessHTTP())
    monkeypatch.setattr(okx_client, "_mutation_request_gate", None)
    monkeypatch.setattr(okx_client, "_mutation_readiness_lock", None)
    monkeypatch.setattr(
        okx_client,
        "_mutation_readiness_state",
        {"ok": False, "checked_at": 0.0, "reason": ""},
    )
    client = okx_client.OKXClient()
    monkeypatch.setattr(client, "_headers", lambda *_args: {})

    ready, reason = asyncio.run(client.mutation_transport_ready(force=True))

    assert ready
    assert reason == ""
    assert calls == [("GET", okx_client.BASE_URL + "/api/v5/account/config")]
    assert all(method == "GET" for method, _url in calls)


def test_strict_submission_lifecycle_persistence_failure_never_posts(
    monkeypatch, tmp_path,
):
    monkeypatch.setenv("PAPER_TRADING", "false")
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    monkeypatch.setattr(execution_router._ss, "get_effective_min_order_usdt", lambda balance: 1.0)
    monkeypatch.setattr(execution_router._ss, "get_tier", lambda name: {"min_trade_usdt": 0.0})
    monkeypatch.setattr(execution_router._ss, "get_val", lambda key, default=None: default)
    import market_data_stream
    monkeypatch.setattr(
        market_data_stream,
        "execution_price_snapshot",
        lambda *args: {"fresh": True, "price": 100.0, "age_sec": 0.0},
    )
    lpt.reset_runtime_state()
    _seed_pre_submit(monkeypatch, tmp_path)

    def fail_strict_save(*, strict=False):
        if strict:
            raise OSError("disk full")

    monkeypatch.setattr(position_registry, "_save", fail_strict_save)
    client = LiveClient()
    route = _live_route(tmp_path, client)
    ok, _, rid = asyncio.run(route.reservations.reserve(
        intent_id="persist-failure", candidate_id="candidate", margin_usdt=1,
        risk_usdt=0.1, max_total_margin_usdt=100,
    ))
    assert ok

    result = asyncio.run(
        route.submit(replace(request(intent="persist-failure"), reservation_id=rid))
    )

    assert not result.ok
    assert "lifecycle_persistence_failed" in result.reason
    assert client.transport_calls == 0
    row = route.orders.get("persist-failure")
    assert row["exchange_contacted"] is False
    assert row["terminal_class"] == "rejected_other"
    assert row.get("submission_attempted", False) is False
    assert "PAYLOAD_READY" in [item["state"] for item in row["history"]]
    assert "SUBMISSION_ATTEMPTED" not in [item["state"] for item in row["history"]]
    assert "EXCHANGE_SUBMIT_ATTEMPTED" not in [item["state"] for item in row["history"]]
    # A strict persistence failure must not erase the in-memory pending row;
    # retaining it is the fail-closed recovery contract.
    assert position_registry._registry["TEST"]["pending_reservation"] is True


def test_submission_lifecycle_is_persisted_before_post_boundary(monkeypatch, tmp_path):
    monkeypatch.setenv("PAPER_TRADING", "false")
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    monkeypatch.setattr(execution_router._ss, "get_effective_min_order_usdt", lambda balance: 1.0)
    monkeypatch.setattr(execution_router._ss, "get_tier", lambda name: {"min_trade_usdt": 0.0})
    monkeypatch.setattr(execution_router._ss, "get_val", lambda key, default=None: default)
    import market_data_stream
    monkeypatch.setattr(
        market_data_stream,
        "execution_price_snapshot",
        lambda *args: {"fresh": True, "price": 100.0, "age_sec": 0.0},
    )
    lpt.reset_runtime_state()
    _seed_pre_submit(monkeypatch, tmp_path)
    events = []
    original_save = position_registry._save

    def observed_save(*, strict=False):
        if strict:
            events.append("persist")
        return original_save(strict=strict)

    monkeypatch.setattr(position_registry, "_save", observed_save)

    class OrderedClient(LiveClient):
        async def place_market_order(self, *args, **kwargs):
            self.market_calls += 1
            kwargs["on_exchange_request"]()
            events.append("post_boundary")
            events.append("post_return")
            self._record_successful_fill()
            return {"ordId": "O1", "fillPx": "100", "contracts": 10, "actual_coin": "TEST"}

    client = OrderedClient()
    route = _live_route(tmp_path, client)
    ok, _, rid = asyncio.run(route.reservations.reserve(
        intent_id="callback-order", candidate_id="candidate", margin_usdt=1,
        risk_usdt=0.1, max_total_margin_usdt=100,
    ))
    assert ok

    result = asyncio.run(
        route.submit(replace(request(intent="callback-order"), reservation_id=rid))
    )

    assert result.ok
    assert events[:3] == ["persist", "post_boundary", "post_return"]
    assert client.market_calls == 1
    assert position_registry._registry["TEST"]["entry_lifecycle"] == "FILLED"


def test_registry_restart_prunes_only_explicit_never_submitted_rows(monkeypatch, tmp_path):
    path = tmp_path / "position_registry.json"
    path.write_text(json.dumps({
        "SAFE": {
            "pending_reservation": True,
            "entry_lifecycle": "PRE_SUBMIT_RESERVED",
            "mutation_attempted": False,
            "order_id": "",
            "entry": 0.0,
        },
        "SUBMISSION": {
            "pending_reservation": True,
            "entry_lifecycle": "SUBMISSION_ATTEMPTED",
            "mutation_attempted": True,
            "order_id": "",
            "entry": 0.0,
        },
        "LEGACY": {"pending_reservation": True, "entry": 0.0},
        "FILLED": {"pending_reservation": False, "entry": 100.0, "order_id": "O1"},
        "MALFORMED_SHAPE": {"pending_reservation": True, "entry_lifecycle": "UNKNOWN"},
        "MALFORMED_VALUE": "preserve me",
    }), encoding="utf-8")
    monkeypatch.setattr(position_registry, "_PATH", path)
    monkeypatch.setattr(position_registry, "_registry", {})

    position_registry._load()

    # Any malformed row blocks loading as a unit; the registry must not
    # silently downgrade to a partial/empty exposure view.
    assert position_registry._registry == {}
    assert position_registry.health()["healthy"] is False
    persisted = json.loads(path.read_text(encoding="utf-8"))
    assert "SAFE" in persisted
    assert "SUBMISSION" in persisted


def test_submission_attempted_restart_is_visible_and_not_replayed(monkeypatch, tmp_path):
    path = tmp_path / "position_registry.json"
    path.write_text(json.dumps({
        "TEST": {
            "direction": "long",
            "pending_reservation": True,
            "entry_lifecycle": "SUBMISSION_ATTEMPTED",
            "mutation_attempted": True,
            "order_id": "",
            "entry": 0.0,
        }
    }), encoding="utf-8")
    monkeypatch.setattr(position_registry, "_PATH", path)
    monkeypatch.setattr(position_registry, "_registry", {})
    position_registry._load()

    assert "TEST" in position_registry.get_all_details()
    assert position_registry.get_all_details()["TEST"]["entry_lifecycle"] == "SUBMISSION_ATTEMPTED"
    assert not position_registry.is_pre_submit_reservation(position_registry.get_all_details()["TEST"])


def test_failed_position_truth_is_local_skip_before_payload_or_post(monkeypatch, tmp_path):
    monkeypatch.setenv("PAPER_TRADING", "false")
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    monkeypatch.setattr(execution_router._ss, "get_effective_min_order_usdt", lambda balance: 1.0)
    monkeypatch.setattr(execution_router._ss, "get_tier", lambda name: {"min_trade_usdt": 0.0})
    monkeypatch.setattr(execution_router._ss, "get_val", lambda key, default=None: default)
    import market_data_stream
    monkeypatch.setattr(market_data_stream, "execution_price_snapshot", lambda *args: {"fresh": True, "price": 100.0, "age_sec": 0.0})
    lpt.reset_runtime_state()
    client = LiveClient(positions_ok=False)
    route = _live_route(tmp_path, client)
    ok, _, rid = asyncio.run(route.reservations.reserve(
        intent_id="truth-bad", candidate_id="candidate", margin_usdt=1,
        risk_usdt=0.1, max_total_margin_usdt=100,
    ))
    assert ok
    result = asyncio.run(route.submit(replace(request(intent="truth-bad"), reservation_id=rid)))
    assert not result.ok
    assert result.reason == "position_truth_unavailable"
    row = route.orders.get("truth-bad")
    assert "PAYLOAD_READY" not in [h["state"] for h in row["history"]]
    assert client.market_calls == 0
    assert row["terminal_class"] == "rejected_account_state"


def test_stale_cached_truth_forces_one_fresh_revalidation(monkeypatch, tmp_path):
    monkeypatch.setenv("PAPER_TRADING", "false")
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    monkeypatch.setattr(execution_router._ss, "get_effective_min_order_usdt", lambda balance: 1.0)
    monkeypatch.setattr(execution_router._ss, "get_tier", lambda name: {"min_trade_usdt": 0.0})
    monkeypatch.setattr(execution_router._ss, "get_val", lambda key, default=None: default)
    import market_data_stream
    monkeypatch.setattr(market_data_stream, "execution_price_snapshot", lambda *args: {"fresh": True, "price": 100.0, "age_sec": 0.0})
    lpt.reset_runtime_state()
    client = LiveClient()
    asyncio.run(lpt.get_live_position_snapshot(client, registry={}, settings=execution_router._ss, force_refresh=True))
    lpt._cache["ts"] -= 20.0
    _seed_pre_submit(monkeypatch, tmp_path)
    route = _live_route(tmp_path, client)
    ok, _, rid = asyncio.run(route.reservations.reserve(
        intent_id="truth-refresh", candidate_id="candidate", margin_usdt=1,
        risk_usdt=0.1, max_total_margin_usdt=100,
    ))
    assert ok
    result = asyncio.run(route.submit(replace(request(intent="truth-refresh"), reservation_id=rid)))
    assert result.ok
    # The explicit warm snapshot is followed by pre-submit validation and the
    # post-fill lifecycle leverage read.
    assert client.position_calls == 3


@pytest.mark.parametrize(
    ("case_name", "row_status", "mismatch_state", "mismatch_count"),
    [
        ("drift", "DRIFT", "entry registry=100 okx=101", 1),
        ("exchange_only", "DRIFT", "EXCHANGE_ONLY", 1),
    ],
    ids=["drift", "exchange_only"],
)
def test_fresh_mismatched_position_truth_blocks_payload_and_mutation(
    monkeypatch, tmp_path, case_name, row_status, mismatch_state, mismatch_count,
):
    monkeypatch.setenv("PAPER_TRADING", "false")
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    monkeypatch.setattr(execution_router._ss, "get_effective_min_order_usdt", lambda balance: 1.0)
    monkeypatch.setattr(execution_router._ss, "get_tier", lambda name: {"min_trade_usdt": 0.0})
    monkeypatch.setattr(execution_router._ss, "get_val", lambda key, default=None: default)
    import market_data_stream
    monkeypatch.setattr(
        market_data_stream,
        "execution_price_snapshot",
        lambda *args: {"fresh": True, "price": 100.0, "age_sec": 0.0},
    )
    lpt.reset_runtime_state()
    client = LiveClient()
    route = _live_route(tmp_path, client)
    unsafe_snapshot = {
        "ok": True,
        "source": "EXCHANGE_TRUTH",
        "age_sec": 0.0,
        "stale_count": 0,
        "mismatch_count": mismatch_count,
        "positions": {
            "TEST": {
                "source": "EXCHANGE_TRUTH",
                "status": row_status,
                "mismatch_state": mismatch_state,
            },
        },
    }
    async def fresh_mismatch(*args, **kwargs):
        return unsafe_snapshot

    monkeypatch.setattr(
        execution_router._position_truth,
        "get_live_position_snapshot",
        fresh_mismatch,
    )
    ok, _, rid = asyncio.run(route.reservations.reserve(
        intent_id=f"truth-{case_name}", candidate_id="candidate",
        margin_usdt=1, risk_usdt=0.1, max_total_margin_usdt=100,
    ))
    assert ok
    intent_id = f"truth-{case_name}"
    result = asyncio.run(route.submit(replace(request(intent=intent_id), reservation_id=rid)))
    assert not result.ok
    assert result.reason == "position_truth_mismatch"
    row = route.orders.get(intent_id)
    states = [item["state"] for item in row["history"]]
    assert "PAYLOAD_READY" not in states
    assert row["terminal_class"] == "rejected_account_state"
    assert client.market_calls == 0
    events = [
        event
        for event in execution_router._flow.snapshot(limit=100)["recent"]
        if event.get("intent_id") == intent_id
    ]
    assert any(
        event.get("stage") == "PRE_OKX_SKIP"
        and event.get("funnel_counter") == "rejected_account_state"
        for event in events
    )
    assert not any(
        event.get("stage") in {"PAYLOAD_READY", "EXCHANGE_SUBMIT_ATTEMPTED"}
        for event in events
    )


def test_stale_revalidation_returning_mismatch_remains_blocked(monkeypatch, tmp_path):
    monkeypatch.setenv("PAPER_TRADING", "false")
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    monkeypatch.setattr(execution_router._ss, "get_effective_min_order_usdt", lambda balance: 1.0)
    monkeypatch.setattr(execution_router._ss, "get_tier", lambda name: {"min_trade_usdt": 0.0})
    monkeypatch.setattr(execution_router._ss, "get_val", lambda key, default=None: default)
    import market_data_stream
    monkeypatch.setattr(
        market_data_stream,
        "execution_price_snapshot",
        lambda *args: {"fresh": True, "price": 100.0, "age_sec": 0.0},
    )
    lpt.reset_runtime_state()
    stale = {
        "ok": True, "source": "EXCHANGE_TRUTH", "age_sec": 99.0,
        "stale_count": 0, "mismatch_count": 0, "positions": {},
    }
    unsafe = {
        "ok": True, "source": "EXCHANGE_TRUTH", "age_sec": 0.0,
        "stale_count": 0, "mismatch_count": 1,
        "positions": {"TEST": {"source": "EXCHANGE_TRUTH", "status": "DRIFT"}},
    }
    monkeypatch.setattr(execution_router._position_truth, "cached_live_position_snapshot", lambda: stale)
    async def fresh_mismatch(*args, **kwargs):
        return unsafe

    monkeypatch.setattr(execution_router._position_truth, "get_live_position_snapshot", fresh_mismatch)
    client = LiveClient()
    route = _live_route(tmp_path, client)
    intent_id = "truth-refresh-drift"
    ok, _, rid = asyncio.run(route.reservations.reserve(
        intent_id=intent_id, candidate_id="candidate", margin_usdt=1,
        risk_usdt=0.1, max_total_margin_usdt=100,
    ))
    assert ok
    result = asyncio.run(route.submit(replace(request(intent=intent_id), reservation_id=rid)))
    assert not result.ok
    assert result.reason == "position_truth_mismatch"
    assert client.market_calls == 0
    events = [
        event
        for event in execution_router._flow.snapshot(limit=100)["recent"]
        if event.get("intent_id") == intent_id
    ]
    assert any(event.get("stage") == "PRE_OKX_SKIP" for event in events)
    assert not any(event.get("stage") == "PAYLOAD_READY" for event in events)


def test_reconciliation_worker_is_independent_and_survives_failure(monkeypatch):
    monkeypatch.setattr(trading_engine, "SYNC_INTERVAL_SEC", 0.01)
    instance = trading_engine.MultiEngine()
    instance.running = True
    calls = 0
    first = asyncio.Event()
    second = asyncio.Event()

    async def fake_sync(*args, **kwargs):
        nonlocal calls
        calls += 1
        if calls == 1:
            instance._last_sync_error = "PoolTimeout"
            first.set()
            return "⚠️ Sync _uncertain_"
        instance._last_sync_error = ""
        second.set()
        return "🔄 Sync"

    monkeypatch.setattr(instance, "sync_positions", fake_sync)

    async def run():
        task = asyncio.create_task(instance._position_sync_loop())
        await asyncio.wait_for(second.wait(), timeout=1.0)
        assert calls >= 2
        assert instance._position_sync_worker_health["consecutive_failures"] == 0
        assert instance._position_sync_worker_health["last_success"]
        instance.running = False
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert instance._position_sync_worker_health["task_alive"] is False

    asyncio.run(run())


def test_scan_cycle_no_longer_owns_periodic_position_sync():
    source = inspect.getsource(trading_engine.MultiEngine._scan_cycle)
    assert "self.sync_positions(" not in source

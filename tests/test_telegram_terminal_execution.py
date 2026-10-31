from __future__ import annotations

import asyncio
import time

import httpx
import pytest


def test_zero_position_snapshot_is_authoritative_zero(monkeypatch, tmp_path):
    import telegram_backend as backend

    backend._operational_exchange_cache.clear()
    monkeypatch.setattr(backend, "_OPERATIONAL_CACHE_PATH", tmp_path / "snapshot.json")

    class OKX:
        async def get_account_snapshot(self):
            return {"total_eq_usdt": 11.0, "avail_eq_usdt": 10.5}

    async def positions():
        return {"ok": True, "source": "OKX", "positions": {}, "open_pnl_usdt": 99.0}

    account, position_snapshot = asyncio.run(backend.fast_operational_exchange_snapshot(OKX(), positions))
    assert account["equity_usdt"] == 11.0
    assert position_snapshot["open_pnl_usdt"] == 0.0
    assert position_snapshot["semantic_state"] == "NO_OPEN_POSITIONS"
    assert position_snapshot["stale"] is True
    assert position_snapshot["freshness_status"] == "UNKNOWN_FRESHNESS"


def test_account_failure_preserves_independent_position_truth(monkeypatch, tmp_path):
    import telegram_backend as backend

    backend._operational_exchange_cache.clear()
    monkeypatch.setattr(backend, "_OPERATIONAL_CACHE_PATH", tmp_path / "snapshot.json")

    class OKX:
        async def get_account_snapshot(self):
            raise RuntimeError("account unavailable")

    async def positions():
        return {"ok": True, "source": "OKX", "positions": {}, "open_pnl_usdt": 0.0}

    account, position_snapshot = asyncio.run(backend.fast_operational_exchange_snapshot(OKX(), positions))
    assert account["equity_usdt"] is None
    assert position_snapshot["source"] == "OKX"
    assert position_snapshot["open_pnl_usdt"] == 0.0


def test_production_home_contains_only_terminal_rooms():
    import tg_panels

    callbacks = {
        button.callback_data
        for row in tg_panels.kb_home(True).inline_keyboard
        for button in row
        if button.callback_data
    }
    required = {
        "terminal:status", "terminal:account", "terminal:positions",
        "terminal:opportunities", "terminal:execution", "terminal:risk",
        "terminal:system", "terminal:why_no_trade", "terminal:emergency",
    }
    assert required <= callbacks
    assert not any("paper" in callback.lower() or callback.startswith("ai") for callback in callbacks)


def test_execution_handoff_survives_parent_cancellation(monkeypatch, tmp_path):
    import execution_flow_telemetry as telemetry
    from execution_supervisor import ExecutionSupervisor

    monkeypatch.setattr(telemetry, "EVENT_PATH", tmp_path / "events.jsonl")
    supervisor = ExecutionSupervisor()
    completed = asyncio.Event()

    async def run_case():
        async def execution():
            await asyncio.sleep(0.03)
            completed.set()

        async def scan_parent():
            assert supervisor.handoff("SUI:LONG", execution(), engine="TENSEI", coin="SUI")
            await asyncio.sleep(10)

        task = asyncio.create_task(scan_parent())
        await asyncio.sleep(0.005)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        await asyncio.wait_for(completed.wait(), timeout=1.0)
        await supervisor.wait_all()

    asyncio.run(run_case())


def test_market_cache_refresh_only_requests_missing_completed_bar(monkeypatch):
    import market_data_stream as market

    now = int(time.time())
    expected = (now // 60) * 60 - 60
    monkeypatch.setattr(market, "_cache_1m", {
        "BTC": {"timestamps": [expected], "confirmed": [True], "closes": [1.0] * 30},
        "ETH": {"timestamps": [expected - 120], "confirmed": [True], "closes": [1.0] * 30},
    })
    assert market.coins_requiring_refresh({"BTC", "ETH", "SOL"}, "1m", 1) == {"ETH", "SOL"}


def test_funding_refresh_is_bounded_and_prioritizes_stale_rows(monkeypatch):
    import market_data_stream as market

    calls = []

    class OKX:
        async def get_funding_rate(self, coin):
            calls.append(coin)
            return 0.0001

    monkeypatch.setattr(market, "_cache_funding", {})
    monkeypatch.setattr(market, "_cache_funding_ts", {"BTC": 50.0, "ETH": 10.0})
    result = asyncio.run(market.refresh_funding_batch(
        ["BTC", "ETH", "SOL", "ADA"], OKX(), max_symbols=2, concurrency=1,
    ))
    assert result == {"requested": 2, "ok": 2}
    assert calls == ["ADA", "SOL"]


def test_funding_refresh_times_out_without_retrying_the_scan(monkeypatch):
    import market_data_stream as market

    class SlowOKX:
        async def get_funding_rate(self, coin):
            await asyncio.sleep(1.0)
            return 0.0001

    monkeypatch.setattr(market, "_cache_funding", {})
    monkeypatch.setattr(market, "_cache_funding_ts", {})
    started = time.monotonic()
    result = asyncio.run(market.refresh_funding_batch(
        ["BTC", "ETH"], SlowOKX(), max_symbols=2, concurrency=2, request_timeout_sec=0.05,
    ))
    assert result == {"requested": 2, "timeout": 2}
    assert time.monotonic() - started < 0.5


def test_okx_retry_rebuilds_auth_headers(monkeypatch):
    import okx_client

    seen = []

    class HTTP:
        async def request(self, method, url, **kwargs):
            seen.append(kwargs["headers"]["OK-ACCESS-TIMESTAMP"])
            request = httpx.Request(method, url)
            if len(seen) == 1:
                return httpx.Response(503, request=request, json={"code": "unavailable"})
            return httpx.Response(200, request=request, json={"code": "0", "data": []})

    client = okx_client.OKXClient()
    counter = iter(["2026-01-01T00:00:00.000Z", "2026-01-01T00:00:01.000Z"])
    original_sleep = asyncio.sleep
    monkeypatch.setattr(okx_client, "_http_client", HTTP())
    monkeypatch.setattr(okx_client, "_critical_http_client", HTTP())
    monkeypatch.setattr(client, "_timestamp", lambda: next(counter))
    monkeypatch.setattr(okx_client.asyncio, "sleep", lambda *_args, **_kwargs: original_sleep(0))

    asyncio.run(client._request("GET", "/api/v5/account/balance"))
    assert seen == ["2026-01-01T00:00:00.000Z", "2026-01-01T00:00:01.000Z"]


def test_router_recovers_stale_stream_with_authoritative_rest_ticker(monkeypatch, tmp_path):
    import execution_router
    from capital_reservations import CapitalReservationBook
    from order_state_store import OrderStateStore

    class Client:
        async def get_ticker(self, coin):
            return {"last": "101.25"}

        async def get_account_snapshot(self):
            return {"avail_eq_usdt": 10.0}

        async def get_account_config(self):
            return {"posMode": "long_short_mode"}

        async def get_positions_status(self):
            return {"ok": True, "positions": [], "reason": ""}

    class Instruments:
        def require(self, coin):
            return type("Instrument", (), {"inst_id": f"{coin}-USDT-SWAP"})()

    class Live:
        def can_submit_live(self):
            return True, "OK"

    async def run_case():
        reservations = CapitalReservationBook(tmp_path / "reservations.json")
        ok, _, reservation_id = await reservations.reserve(
            intent_id="intent", candidate_id="candidate", margin_usdt=1.0,
            risk_usdt=0.1, max_total_margin_usdt=10.0,
        )
        assert ok
        route = execution_router.ExecutionRouter(
            Client(), instruments=Instruments(), reservations=reservations,
            orders=OrderStateStore(tmp_path / "orders.json"), live_state=Live(),
        )
        request = execution_router.ExecutionRequest(
            intent_id="intent", candidate_id="candidate", reservation_id=reservation_id,
            mode="Guarded", engine="RYUU", coin="SUI", direction="long",
            requested_contracts=1.0, approved_contracts=1.0,
            approved_notional_usdt=10.0, order_type="market", price=100.0,
            stop_price=99.0, take_profit=105.0, leverage=10,
            metadata={"signal_timestamp": time.time()},
        )
        valid, reason, evidence = await route._validate_live_submission(request)
        assert valid is True
        assert reason == "OK"
        assert evidence["current_price"] == 101.25
        assert evidence["market_freshness"]["source"] == "okx_rest_fallback"
        assert evidence["market_freshness"]["stream_fresh"] is False

    monkeypatch.setenv("PAPER_TRADING", "false")
    monkeypatch.setattr(
        "market_data_stream.execution_price_snapshot",
        lambda *args, **kwargs: {"fresh": False, "price": 100.0, "age_sec": 240.0, "reason": "stale_execution_price"},
    )
    async def fresh_position_truth(*args, **kwargs):
        return {
            "ok": True,
            "source": "EXCHANGE_TRUTH",
            "age_sec": 0.0,
            "stale_count": 0,
            "positions": {},
        }

    monkeypatch.setattr(execution_router._position_truth, "get_live_position_snapshot", fresh_position_truth)
    execution_router._position_truth.reset_runtime_state()
    asyncio.run(run_case())

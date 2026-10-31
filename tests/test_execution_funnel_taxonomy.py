"""Phase-1 executable minimum + funnel taxonomy regressions."""
from __future__ import annotations

import asyncio
import time

import pytest

import capital_allocator
import executable_minimum as emin
import execution_flow_telemetry as flow
import execution_router
from capital_reservations import CapitalReservationBook
from execution_router import ExecutionRequest, ExecutionRouter
from instrument_registry import InstrumentMetadata
from order_state_store import OrderStateStore
from test_adaptive_live_phase2 import FakeClient, FakeInstruments, FakeLiveState, instrument, router


def test_placement_min_is_shared_source(monkeypatch):
    values = {
        "adaptive_min_order_floor_enabled": True,
        "micro_sizing_enabled": True,
        "v6_micro_max_balance": 100.0,
        "micro_min_order_usdt_floor": 5.0,
        "min_order_usdt_micro": 25.0,
        "min_order_usdt": 100.0,
        "v6_min_position_size_usdt": 25.0,
        "adaptive_min_economic_notional_usdt": 1.0,
    }

    class S:
        def get_val(self, key, default=None):
            return values.get(key, default)

        def get_effective_min_order_usdt(self, balance):
            # Mirror micro_sizing tiny-account path used in production evidence.
            if balance is None:
                return 100.0
            if balance < 10:
                return 5.0
            if balance < 25:
                blend = (balance - 10.0) / 15.0
                return 5.0 + (25.0 - 5.0) * blend
            return 25.0

    settings = S()
    floor = emin.placement_min_notional_usdt(11.0, instrument_min_notional_usdt=1.04, settings=settings)
    assert floor == pytest.approx(6.333333, rel=1e-3)
    assert floor > 4.4


def test_tier_floor_is_derived_even_when_callers_omit_argument(monkeypatch):
    class Settings:
        def get_effective_min_order_usdt(self, balance):
            return 5.0

        def get_tier(self, name):
            return {"min_trade_usdt": 6.0}

    settings = Settings()
    assert emin.placement_min_notional_usdt(50.0, settings=settings) == pytest.approx(6.0)
    # Legacy callers that still pass the tier value resolve identically.
    assert emin.placement_min_notional_usdt(
        50.0, tier_min_trade_usdt=6.0, settings=settings,
    ) == pytest.approx(6.0)


def test_allocator_preserves_authoritative_tier_floor_when_safe(monkeypatch):
    values = {
        "adaptive_capital_allocator_enabled": True,
        "adaptive_min_economic_notional_usdt": 1.0,
        "position_size_cap_usdt": 1000.0,
        "position_size_cap_pct_balance": 100.0,
        "leverage_default": 10,
        "rule_357_enabled": True,
        "rule_357_max_open_risk_pct": 12.0,
        "min_sl_dist_pct": 1.0,
        "live_taker_fee_rate": 0.0005,
    }
    monkeypatch.setattr(capital_allocator._s, "get_val", lambda key, default=None: values.get(key, default))
    monkeypatch.setattr(capital_allocator._s, "get_effective_min_order_usdt", lambda balance: 5.0)
    monkeypatch.setattr(capital_allocator._s, "get_tier", lambda name: {"min_trade_usdt": 6.0})
    monkeypatch.setattr(capital_allocator.micro_sizing, "usable_margin_usdt", lambda balance, settings: balance)

    decision = capital_allocator.allocate_entry(
        balance_usdt=50.0,
        requested_size_usdt=5.5,
        entry_price=100.0,
        stop_price=99.0,
        leverage=10,
        instrument_metadata=instrument(ct_val=0.01, min_sz=1.0, lot_sz=1.0),
        available_margin_usdt=50.0,
    )
    assert decision.approved
    assert decision.min_exec_usdt == pytest.approx(6.0)
    assert decision.final_size_usdt == pytest.approx(6.0)


def test_router_uses_fresh_revalidated_balance_for_final_floor(tmp_path, monkeypatch):
    monkeypatch.setenv("PAPER_TRADING", "false")
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    monkeypatch.setattr(execution_router._ss, "get_effective_min_order_usdt", lambda balance: 25.0 if balance >= 25.0 else 5.0)
    monkeypatch.setattr(execution_router._ss, "get_tier", lambda name: {"min_trade_usdt": 0.0})
    class Client(FakeClient):
        async def place_market_order(self, coin, side, amount_usdt, **kwargs):
            self.market_calls += 1
            kwargs["on_exchange_request"]()
            return {"ordId": "O1", "fillPx": "100", "contracts": 1, "actual_coin": coin}

        async def get_account_snapshot(self):
            return {"avail_eq_usdt": 11.0, "total_eq_usdt": 11.0}

        async def get_account_config(self):
            return {"posMode": "long_short_mode"}

        async def get_positions_status(self, **kwargs):
            return {
                "ok": True,
                "positions": [{
                    "instId": "TEST-USDT-SWAP",
                    "pos": "1",
                    "posSide": "long",
                    "lever": "10",
                    "avgPx": "100",
                }],
                "reason": "",
            }

    route, book = _live_router(tmp_path, monkeypatch, Client())
    async def fresh_validation(_request):
        return True, "OK", {"available_balance_usdt": 11.0, "equity_usdt": 11.0}

    monkeypatch.setattr(route, "_validate_live_submission", fresh_validation)
    ok, _, rid = asyncio.run(book.reserve(
        intent_id="fresh-floor", candidate_id="c1", margin_usdt=1,
        risk_usdt=0.1, max_total_margin_usdt=100,
    ))
    assert ok
    result = asyncio.run(route.submit(_req(
        intent_id="fresh-floor", reservation_id=rid,
        approved_notional_usdt=6.2,
        metadata={"allocator_equity": 50.0},
    )))
    assert result.ok
    row = route.orders.get("fresh-floor")
    assert row["placement_min_notional_usdt"] == pytest.approx(5.0)
    assert row["local_executable_check"]["balance_source"] == "live_revalidation"


def test_tier_floor_violation_rejects_before_payload_and_exchange(tmp_path, monkeypatch):
    monkeypatch.setenv("PAPER_TRADING", "true")
    monkeypatch.setattr(execution_router._ss, "get_effective_min_order_usdt", lambda balance: 5.0)
    monkeypatch.setattr(execution_router._ss, "get_tier", lambda name: {"min_trade_usdt": 6.0})
    client = FakeClient()
    route = router(tmp_path, monkeypatch, client)
    ok, _, rid = asyncio.run(route.reservations.reserve(
        intent_id="tier-block", candidate_id="c1", margin_usdt=1,
        risk_usdt=0.1, max_total_margin_usdt=100,
    ))
    assert ok
    result = asyncio.run(route.submit(_req(
        intent_id="tier-block", reservation_id=rid,
        approved_notional_usdt=5.5,
        metadata={"allocator_equity": 50.0},
    )))
    assert not result.ok
    assert "min_notional" in result.reason
    row = route.orders.get("tier-block")
    assert "PAYLOAD_READY" not in [h["state"] for h in row["history"]]
    assert client.market_calls == 0


def test_allocator_uses_placement_floor_and_rejects_below(monkeypatch):
    values = {
        "adaptive_capital_allocator_enabled": True,
        "position_size_cap_usdt": 10_000.0,
        "position_size_cap_pct_balance": 40.0,
        "leverage_default": 10,
        "rule_357_enabled": True,
        "rule_357_max_open_risk_pct": 12.0,
        "min_sl_dist_pct": 1.0,
        "live_taker_fee_rate": 0.0005,
        "deployment_mode": "Guarded",
        "adaptive_min_economic_notional_usdt": 1.0,
    }
    monkeypatch.setattr(capital_allocator._s, "get_val", lambda key, default=None: values.get(key, default))
    monkeypatch.setattr(
        capital_allocator._s,
        "get_effective_min_order_usdt",
        lambda balance: 6.35 if balance and balance < 25 else 25.0,
    )
    monkeypatch.setattr(capital_allocator.micro_sizing, "usable_margin_usdt", lambda balance, settings: balance * 0.88)

    decision = capital_allocator.allocate_entry(
        balance_usdt=11.0,
        requested_size_usdt=7.97,
        entry_price=100.0,
        stop_price=98.5,
        leverage=10,
        instrument_metadata=instrument(ct_val=0.01, min_sz=0.01, lot_sz=0.01),
        available_margin_usdt=9.69,
    )
    # 40% of 11 = 4.4 notional max, below placement floor 6.35 => reject
    assert not decision.approved
    assert "below_min_exec" in decision.reason
    assert decision.min_exec_usdt == pytest.approx(6.35)


def test_allocator_and_helper_agree_on_floor(monkeypatch):
    monkeypatch.setattr(capital_allocator._s, "get_val", lambda key, default=None: {
        "adaptive_capital_allocator_enabled": True,
        "position_size_cap_usdt": 10_000.0,
        "position_size_cap_pct_balance": 100.0,
        "leverage_default": 10,
        "rule_357_enabled": True,
        "rule_357_max_open_risk_pct": 12.0,
        "min_sl_dist_pct": 1.0,
        "live_taker_fee_rate": 0.0005,
        "adaptive_min_economic_notional_usdt": 1.0,
    }.get(key, default))
    monkeypatch.setattr(capital_allocator._s, "get_effective_min_order_usdt", lambda balance: 5.0)
    monkeypatch.setattr(capital_allocator.micro_sizing, "usable_margin_usdt", lambda balance, settings: balance)

    floor = emin.placement_min_notional_usdt(50.0, instrument_min_notional_usdt=2.0, settings=capital_allocator._s)
    decision = capital_allocator.allocate_entry(
        balance_usdt=50.0,
        requested_size_usdt=20.0,
        entry_price=100.0,
        stop_price=99.0,
        leverage=10,
        instrument_metadata=instrument(),
        available_margin_usdt=50.0,
    )
    assert decision.approved
    assert decision.min_exec_usdt == pytest.approx(floor)


def _live_router(tmp_path, monkeypatch, client=None):
    monkeypatch.setenv("PAPER_TRADING", "false")
    monkeypatch.setattr("position_registry.update", lambda *args, **kwargs: True)
    book = CapitalReservationBook(tmp_path / "reservations.json")
    route = ExecutionRouter(
        client or FakeClient(),
        instruments=FakeInstruments(),
        reservations=book,
        orders=OrderStateStore(tmp_path / "orders.json"),
        live_state=FakeLiveState(),
    )
    return route, book


def _req(**overrides):
    base = dict(
        intent_id="i1",
        candidate_id="c1",
        reservation_id="r1",
        mode="Guarded",
        engine="SHOGUN",
        coin="TEST",
        direction="long",
        requested_contracts=10,
        approved_contracts=10,
        approved_notional_usdt=10.0,
        order_type="market",
        price=100.0,
        stop_price=99.0,
        take_profit=103.0,
        margin_mode="isolated",
        leverage=10,
        timeout_secs=0.1,
        metadata={"allocator_equity": 50.0, "signal_timestamp": time.time()},
    )
    base.update(overrides)
    return ExecutionRequest(**base)


@pytest.fixture(autouse=True)
def _reset_funnel_counters():
    flow.reset_counters_for_tests()
    yield
    flow.reset_counters_for_tests()


def test_below_floor_rejected_before_payload_ready(tmp_path, monkeypatch):
    monkeypatch.setenv("PAPER_TRADING", "true")
    monkeypatch.setattr(execution_router._ss, "get_effective_min_order_usdt", lambda balance: 6.35)
    monkeypatch.setattr(execution_router._ss, "get_val", lambda key, default=None: default)
    client = FakeClient()
    route = router(tmp_path, monkeypatch, client)
    book = route.reservations
    ok, _, rid = asyncio.run(book.reserve(intent_id="i1", candidate_id="c1", margin_usdt=1, risk_usdt=0.1, max_total_margin_usdt=100))
    assert ok
    req = _req(reservation_id=rid, approved_notional_usdt=4.16, approved_contracts=0.5, metadata={"allocator_equity": 11.0})
    result = asyncio.run(route.submit(req))
    assert not result.ok
    assert "min_notional" in result.reason or "placement_floor" in result.reason
    row = route.orders.get("i1")
    assert row["state"] == "REJECTED"
    assert "PAYLOAD_READY" not in [h["state"] for h in row["history"]]
    assert "SUBMISSION_ATTEMPTED" not in [h["state"] for h in row["history"]]
    assert client.market_calls == 0
    counters = flow.counters_snapshot()
    assert counters["rejected_min_notional"] >= 1
    assert counters["payload_ready"] == 0
    assert counters["submission_attempted"] == 0
    assert counters["exchange_rejected"] == 0


def test_zero_contracts_rejected_before_payload_ready(tmp_path, monkeypatch):
    monkeypatch.setenv("PAPER_TRADING", "true")
    monkeypatch.setattr(execution_router._ss, "get_effective_min_order_usdt", lambda balance: 1.0)
    monkeypatch.setattr(execution_router._ss, "get_val", lambda key, default=None: 1.0 if key == "adaptive_min_economic_notional_usdt" else default)
    client = FakeClient()
    route = router(tmp_path, monkeypatch, client)
    book = route.reservations
    ok, _, rid = asyncio.run(book.reserve(intent_id="i1", candidate_id="c1", margin_usdt=1, risk_usdt=0.1, max_total_margin_usdt=100))
    assert ok
    req = _req(reservation_id=rid, approved_contracts=0.0, approved_notional_usdt=0.0, metadata={"allocator_equity": 50.0})
    result = asyncio.run(route.submit(req))
    assert not result.ok
    assert "zero_contract" in result.reason
    assert client.market_calls == 0
    assert flow.counters_snapshot()["rejected_zero_contracts"] >= 1
    assert flow.counters_snapshot()["exchange_rejected"] == 0


def test_valid_candidate_reaches_payload_and_one_exchange_attempt(tmp_path, monkeypatch):
    monkeypatch.setattr(execution_router._ss, "get_effective_min_order_usdt", lambda balance: 1.0)
    monkeypatch.setattr(execution_router._ss, "get_val", lambda key, default=None: default)

    class Client(FakeClient):
        async def place_market_order(self, coin, side, amount_usdt, **kwargs):
            self.market_calls += 1
            if kwargs.get("on_exchange_request"):
                kwargs["on_exchange_request"]()
            return {
                "ordId": f"M{self.market_calls}",
                "fillPx": "100",
                "contracts": kwargs.get("contracts_override", 1),
                "paper": True,
                "actual_coin": coin,
                "size_usdt": amount_usdt,
            }

        async def get_account_snapshot(self):
            return {"avail_eq_usdt": 50.0}

        async def get_account_config(self):
            return {"posMode": "long_short_mode"}

    client = Client()
    route, book = _live_router(tmp_path, monkeypatch, client)
    # Force paper mode path for fill simplicity while still recording attempt via client callback
    monkeypatch.setenv("PAPER_TRADING", "true")
    ok, _, rid = asyncio.run(book.reserve(intent_id="i1", candidate_id="c1", margin_usdt=1, risk_usdt=0.1, max_total_margin_usdt=100))
    assert ok
    result = asyncio.run(route.submit(_req(reservation_id=rid, metadata={"allocator_equity": 50.0})))
    assert result.ok
    row = route.orders.get("i1")
    assert "PAYLOAD_READY" in [h["state"] for h in row["history"]]
    assert "SUBMISSION_ATTEMPTED" in [h["state"] for h in row["history"]]
    assert client.market_calls == 1
    counters = flow.counters_snapshot()
    assert counters["payload_ready"] >= 1
    assert counters["submission_attempted"] >= 1
    assert counters["filled"] >= 1


def test_response_none_before_exchange_is_not_exchange_rejected(tmp_path, monkeypatch):
    monkeypatch.setenv("PAPER_TRADING", "true")
    monkeypatch.setattr(execution_router._ss, "get_effective_min_order_usdt", lambda balance: 1.0)
    monkeypatch.setattr(execution_router._ss, "get_val", lambda key, default=None: default)

    class SkipClient(FakeClient):
        def __init__(self):
            super().__init__()
            self._order_skip_reasons = {}

        def consume_last_order_skip_reason(self):
            return "downsized_below_floor: size_usdt=4.16, floor=6.35"

        async def place_market_order(self, *args, **kwargs):
            self.market_calls += 1
            # Intentionally do NOT call on_exchange_request
            return None

    client = SkipClient()
    route = router(tmp_path, monkeypatch, client)
    book = route.reservations
    ok, _, rid = asyncio.run(book.reserve(intent_id="preflight-skip", candidate_id="c1", margin_usdt=1, risk_usdt=0.1, max_total_margin_usdt=100))
    assert ok
    result = asyncio.run(route.submit(_req(intent_id="preflight-skip", reservation_id=rid, metadata={"allocator_equity": 50.0})))
    assert not result.ok
    row = route.orders.get("preflight-skip")
    assert row.get("exchange_request_at") is None
    assert "SUBMISSION_ATTEMPTED" not in [h["state"] for h in row["history"]]
    assert "downsized_below_floor" in result.reason or "min_notional" in result.reason or "pre_okx" in result.reason
    assert flow.counters_snapshot()["exchange_rejected"] == 0
    assert flow.counters_snapshot()["rejected_min_notional"] >= 1


def test_post_boundary_empty_response_is_exchange_uncertainty_not_rejection(tmp_path, monkeypatch):
    monkeypatch.setenv("PAPER_TRADING", "true")
    monkeypatch.setattr(execution_router._ss, "get_effective_min_order_usdt", lambda balance: 1.0)
    monkeypatch.setattr(execution_router._ss, "get_val", lambda key, default=None: default)

    class RejectClient(FakeClient):
        async def place_market_order(self, *args, **kwargs):
            self.market_calls += 1
            kwargs["on_exchange_request"]()
            return None  # after request boundary

    client = RejectClient()
    route = router(tmp_path, monkeypatch, client)
    book = route.reservations
    ok, _, rid = asyncio.run(book.reserve(intent_id="ex-rej", candidate_id="c1", margin_usdt=1, risk_usdt=0.1, max_total_margin_usdt=100))
    assert ok
    result = asyncio.run(route.submit(_req(intent_id="ex-rej", reservation_id=rid, metadata={"allocator_equity": 50.0})))
    assert not result.ok
    row = route.orders.get("ex-rej")
    assert row.get("exchange_request_at") is not None
    assert flow.counters_snapshot()["submission_attempted"] >= 1
    # No response body is not a definitive exchange business rejection; the
    # durable intent must remain available for readback reconciliation.
    assert flow.counters_snapshot()["exchange_rejected"] == 0
    assert flow.counters_snapshot()["exchange_uncertain"] >= 1


def test_exchange_timeout_is_uncertain(tmp_path, monkeypatch):
    monkeypatch.setenv("PAPER_TRADING", "true")
    monkeypatch.setattr(execution_router._ss, "get_effective_min_order_usdt", lambda balance: 1.0)
    monkeypatch.setattr(execution_router._ss, "get_val", lambda key, default=None: default)

    class TimeoutClient(FakeClient):
        async def place_market_order(self, *args, **kwargs):
            self.market_calls += 1
            kwargs["on_exchange_request"]()
            raise TimeoutError("unknown submit outcome")

    client = TimeoutClient()
    route = router(tmp_path, monkeypatch, client)
    book = route.reservations
    ok, _, rid = asyncio.run(book.reserve(intent_id="unc", candidate_id="c1", margin_usdt=1, risk_usdt=0.1, max_total_margin_usdt=100))
    assert ok
    result = asyncio.run(route.submit(_req(intent_id="unc", reservation_id=rid, metadata={"allocator_equity": 50.0})))
    assert not result.ok
    assert flow.counters_snapshot()["exchange_uncertain"] >= 1


def test_telegram_execution_and_why_use_funnel_counters(monkeypatch):
    import bot

    flow.bump("router_requested", 3)
    flow.bump("payload_ready", 1)
    flow.bump("rejected_min_notional", 2)
    flow.bump("submission_attempted", 1)
    flow.bump("filled", 0)

    class Eng:
        running = True
        def get_sync_health_snapshot(self):
            return {"status": "OK"}
        def status_snapshot(self):
            return {"ws_active": True}
        def trade_flow_snapshot(self):
            return {}

    monkeypatch.setattr(bot, "engine", Eng())
    monkeypatch.setattr("capital_reservations.book.snapshot", lambda: [])
    monkeypatch.setattr("execution_supervisor.supervisor.snapshot", lambda: {"active_count": 0, "active": []})
    monkeypatch.setattr("order_state_store.store.snapshot", lambda: {"unresolved": 0})
    monkeypatch.setattr(
        "production_research_funnel.latest",
        lambda: {"symbols_seen": 10, "symbols_data_ready": 9, "counts": {"raw_signal": 1, "candidate": 1}, "reasons": {"hold": 1}},
    )

    exec_text = bot._terminal_execution_text()
    why_text = bot._terminal_why_no_trade_text()
    assert "Payload ready" in exec_text
    assert "Submitted to OKX" in exec_text
    assert "Min notional" in exec_text
    assert "`2`" in exec_text  # rejected_min_notional
    assert "Submitted to OKX" in why_text
    assert "Min notional" in why_text
    assert "Acknowledged by OKX" in why_text


def test_classify_reason_mapping():
    assert emin.classify_pre_okx_reason("downsized_below_floor: size=1 floor=6") == "rejected_min_notional"
    assert emin.classify_pre_okx_reason("preflight_zero_contracts: attempts=15x") == "rejected_zero_contracts"
    assert emin.classify_pre_okx_reason("stale_execution_price") == "rejected_market_stale"
    assert emin.classify_pre_okx_reason("candidate_expired_before_submission") == "rejected_candidate_stale"
    assert emin.classify_pre_okx_reason("capital_reservation_missing_or_expired") == "rejected_reservation"

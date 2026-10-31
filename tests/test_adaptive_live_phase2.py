import asyncio
import json
import time
from dataclasses import replace

import pytest

import capital_allocator
import category_engine
import execution_router
import position_registry
import trading_engine
from capital_reservations import CapitalReservationBook
from execution_router import ExecutionRequest, ExecutionRouter, deterministic_client_order_id
from instrument_registry import InstrumentMetadata, InstrumentRegistry
from live_state import LiveStateMachine
from opportunity_queue import Opportunity, OpportunityQueue
from order_state_store import OrderStateStore
from pnl_reconciler import PnLReconciler
from protection_manager import ProtectionManager
from risk_manager import RiskManager


@pytest.fixture(autouse=True)
def isolate_durable_execution_state(tmp_path, monkeypatch):
    """Keep this module's live-path proofs out of shared runtime ledgers."""
    import risk_manager

    monkeypatch.setattr(position_registry, "_PATH", tmp_path / "position_registry.json")
    monkeypatch.setattr(position_registry, "_registry", {})
    monkeypatch.setattr(risk_manager, "risk", RiskManager(tmp_path / "risk_slots.sqlite3"))
    yield


def instrument(**overrides):
    values = {
        "inst_id": "TEST-USDT-SWAP",
        "inst_type": "SWAP",
        "settle_ccy": "USDT",
        "ct_val": 0.01,
        "ct_val_ccy": "TEST",
        "ct_type": "linear",
        "lot_sz": 1.0,
        "min_sz": 1.0,
        "tick_sz": 0.1,
        "state": "live",
        "max_mkt_sz": 1000.0,
        "max_lmt_sz": 1000.0,
        "max_leverage": 20.0,
        "refreshed_at": time.time(),
        "source_hash": "abc",
        "raw": {},
    }
    values.update(overrides)
    return InstrumentMetadata(**values)


def test_instrument_contract_conversion_and_round_down():
    meta = instrument(lot_sz=0.1, min_sz=0.1)
    assert meta.notional_to_raw_contracts(10.09, 100.0) == pytest.approx(10.09)
    assert meta.notional_to_contracts(10.09, 100.0) == pytest.approx(10.0)
    assert meta.contracts_to_notional(10.0, 100.0) == pytest.approx(10.0)
    assert meta.required_margin(10.0, 100.0, 10) == pytest.approx(1.0)


def test_instrument_minimum_and_disabled_rejection():
    meta = instrument(min_sz=2.0)
    assert meta.quantity_feasible(1.0)[0] is False
    assert instrument(state="suspend").eligible() == (False, "instrument_state_suspend")


def test_directional_tick_rounding_is_protective_and_fee_safe():
    meta = instrument(tick_sz=0.1)
    assert meta.round_price(99.94, side="long", purpose="stop") == 100.0
    assert meta.round_price(100.06, side="short", purpose="stop") == 100.0
    assert meta.round_price(105.01, side="long", purpose="take_profit") == 105.1
    assert meta.round_price(94.99, side="short", purpose="take_profit") == 94.9


def test_stale_metadata_is_not_returned(tmp_path):
    registry = InstrumentRegistry(tmp_path / "meta.json")
    registry._items["TEST-USDT-SWAP"] = instrument(refreshed_at=1.0)
    assert registry.get("TEST", require_fresh=True) is None


def test_metadata_refresh_persists_authoritative_payload(tmp_path):
    registry = InstrumentRegistry(tmp_path / "meta.json")

    class Client:
        async def _request(self, method, path, params=None):
            return {"code": "0", "data": [{
                "instId": "TEST-USDT-SWAP", "instType": "SWAP", "settleCcy": "USDT",
                "ctVal": "0.01", "ctValCcy": "TEST", "ctType": "linear", "lotSz": "1",
                "minSz": "1", "tickSz": "0.1", "state": "live", "maxMktSz": "10", "maxLmtSz": "20",
            }]}

    assert asyncio.run(registry.refresh(Client(), force=True)) == 1
    assert registry.require("TEST").source_hash
    assert json.loads((tmp_path / "meta.json").read_text())["instruments"][0]["inst_id"] == "TEST-USDT-SWAP"


def _allocator_settings(monkeypatch, risk_cap=12.0):
    values = {
        "adaptive_capital_allocator_enabled": True,
        "position_size_cap_usdt": 10_000.0,
        "position_size_cap_pct_balance": 100.0,
        "leverage_default": 10,
        "rule_357_enabled": True,
        "rule_357_max_open_risk_pct": risk_cap,
        "min_sl_dist_pct": 1.0,
        "live_taker_fee_rate": 0.0005,
        "deployment_mode": "Guarded",
    }
    monkeypatch.setattr(capital_allocator._s, "get_val", lambda key, default=None: values.get(key, default))
    monkeypatch.setattr(capital_allocator._s, "get_effective_min_order_usdt", lambda balance: 1.0)
    # These tests isolate adaptive allocator behavior; tier-floor coverage is
    # exercised explicitly in the executable-minimum regression tests.
    monkeypatch.setattr(capital_allocator._s, "get_tier", lambda name: {"min_trade_usdt": 0.0})
    monkeypatch.setattr(capital_allocator.micro_sizing, "usable_margin_usdt", lambda balance, settings: balance)


@pytest.mark.parametrize("balance", [5.0, 50.0, 500.0])
def test_allocator_is_balance_adaptive_with_exchange_contracts(monkeypatch, balance):
    _allocator_settings(monkeypatch)
    decision = capital_allocator.allocate_entry(
        balance_usdt=balance,
        requested_size_usdt=balance,
        entry_price=100.0,
        stop_price=99.0,
        leverage=10,
        instrument_metadata=instrument(),
        available_margin_usdt=balance,
    )
    assert decision.approved
    assert decision.approved_contracts > 0
    assert decision.estimated_margin <= balance
    assert decision.estimated_entry_fee > 0


def test_allocator_includes_pending_reservation_load(monkeypatch):
    _allocator_settings(monkeypatch)
    decision = capital_allocator.allocate_entry(
        balance_usdt=50.0,
        requested_size_usdt=50.0,
        entry_price=100.0,
        stop_price=99.0,
        leverage=10,
        instrument_metadata=instrument(),
        currently_reserved_margin_usdt=49.0,
        available_margin_usdt=50.0,
    )
    assert decision.approved
    assert decision.final_size_usdt == pytest.approx(9.0)
    assert decision.currently_reserved_margin == pytest.approx(49.0)


def test_allocator_rechecks_floor_after_contract_rounding(monkeypatch):
    _allocator_settings(monkeypatch)
    decision = capital_allocator.allocate_entry(
        balance_usdt=5.0,
        requested_size_usdt=1.0,
        entry_price=60.0,
        stop_price=59.4,
        leverage=10,
        instrument_metadata=instrument(ct_val=0.01, min_sz=1.0, lot_sz=1.0),
        available_margin_usdt=5.0,
    )
    assert not decision.approved
    assert "after_lot_round" in decision.reason


def test_atomic_reservations_prevent_joint_overcommit(tmp_path):
    book = CapitalReservationBook(tmp_path / "reservations.json")

    async def run():
        return await asyncio.gather(
            book.reserve(intent_id="A", candidate_id="A", margin_usdt=6, risk_usdt=1, max_total_margin_usdt=10),
            book.reserve(intent_id="B", candidate_id="B", margin_usdt=6, risk_usdt=1, max_total_margin_usdt=10),
        )

    results = asyncio.run(run())
    assert sum(1 for allowed, _, _ in results if allowed) == 1


def opportunity(cid="one", score=1.0, direction="long"):
    now = time.time()
    return Opportunity(
        candidate_id=cid, pair="TEST", direction=direction, engine="SHOGUN",
        signal_timestamp=now, expiry_timestamp=now + 60, original_score=score,
        current_score=score, confidence=score, intended_entry=100, stop=99,
        tp_structure={"tp2": 103}, risk_requirement=1, estimated_margin_requirement=5,
        instrument_eligibility="eligible", deployment_mode="Guarded", correlation_group="test",
        queued_reason="temporary_insufficient_deployable_margin", market_geometry_timestamp=now,
    )


def test_queue_replaces_weaker_duplicate_and_recovers(tmp_path, monkeypatch):
    monkeypatch.setattr("opportunity_queue._s.get_val", lambda key, default=None: 10 if key == "opportunity_queue_max_size" else default)
    path = tmp_path / "queue.json"
    queue = OpportunityQueue(path)
    assert queue.enqueue(opportunity("weak", 1.0))[0]
    assert queue.enqueue(opportunity("strong", 2.0))[0]
    assert queue.snapshot()["count"] == 1
    recovered = OpportunityQueue(path)
    assert recovered.snapshot()["top"][0]["candidate_id"] == "strong"


def test_queue_expires_stale_candidates(tmp_path):
    queue = OpportunityQueue(tmp_path / "queue.json")
    row = opportunity()
    row.expiry_timestamp = time.time() - 1
    queue._items[row.candidate_id] = row
    assert queue.expire() == 1
    assert queue.snapshot()["count"] == 0


class FakeInstruments:
    def __init__(self):
        self.meta = instrument()
        self.refreshes = 0

    def require(self, coin):
        return self.meta

    def get(self, coin):
        return self.meta

    async def refresh_after_metadata_error(self, client):
        self.refreshes += 1


class FakeLiveState:
    def can_submit_live(self):
        return True, "OK"


class FakeClient:
    def __init__(self):
        self.market_calls = 0
        self.limit_calls = 0
        self.cancel_calls = 0
        self.query_calls = 0

    async def place_market_order(self, coin, side, amount_usdt, **kwargs):
        self.market_calls += 1
        return {"ordId": f"M{self.market_calls}", "fillPx": "100", "contracts": kwargs.get("contracts_override", 1), "paper": True, "actual_coin": coin, "size_usdt": amount_usdt}

    async def place_limit_order(self, coin, side, price, amount_usdt, **kwargs):
        self.limit_calls += 1
        return {"ordId": f"L{self.limit_calls}", "fillPx": str(price), "contracts": kwargs.get("contracts_override", 1), "paper": True, "actual_coin": coin, "size_usdt": amount_usdt}

    async def get_order(self, coin, **kwargs):
        self.query_calls += 1
        return {"state": "filled", "accFillSz": "10", "avgPx": "100"}

    async def cancel_order(self, *args, **kwargs):
        self.cancel_calls += 1
        return {"cancelled": True}

    async def get_algo_orders(self, coin):
        return []

    async def place_protective_order(self, **kwargs):
        return {"code": "0", "data": [{"algoId": "P1"}]}

    async def close_futures_position(self, *args, **kwargs):
        return {"ordId": "C1", "state": "filled"}

    async def get_ticker(self, coin):
        return {"last": 100.0}


def request(intent="intent", order_type="market"):
    return ExecutionRequest(
        intent_id=intent, candidate_id="candidate", reservation_id="reservation",
        mode="Guarded", engine="SHOGUN", coin="TEST", direction="long",
        requested_contracts=10, approved_contracts=10, approved_notional_usdt=10,
        order_type=order_type, price=100, stop_price=99, take_profit=103,
        margin_mode="isolated", leverage=10, timeout_secs=0.1,
        metadata={"execution_chunks": [{"fraction": 0.5, "price_offset_pct": 0}, {"fraction": 0.5, "price_offset_pct": -0.001}]},
    )


def router(tmp_path, monkeypatch, client=None):
    monkeypatch.setenv("PAPER_TRADING", "true")
    monkeypatch.setattr(position_registry, "update", lambda *args, **kwargs: True)
    route = ExecutionRouter(
        client or FakeClient(),
        instruments=FakeInstruments(),
        reservations=CapitalReservationBook(tmp_path / "reservations.json"),
        orders=OrderStateStore(tmp_path / "orders.json"),
        live_state=FakeLiveState(),
    )
    # The request fixture names a durable reservation explicitly.  Seed that
    # same id through the reservation store so the tests exercise the router
    # lifecycle rather than failing before the mutation boundary.
    route.reservations._items["reservation"] = {
        "reservation_id": "reservation",
        "intent_id": "intent",
        "candidate_id": "candidate",
        "margin_usdt": 1.0,
        "risk_usdt": 0.1,
        "created_at": time.time(),
        "expires_at": time.time() + 120.0,
        "ownership_state": "PRE_SUBMIT_RESERVED",
        "ownership_reason": "test_fixture",
        "ownership_updated_at": time.time(),
        "metadata": {},
    }
    route.reservations._save()
    return route


def test_router_client_order_id_is_deterministic():
    assert deterministic_client_order_id("same") == deterministic_client_order_id("same")
    assert deterministic_client_order_id("same") != deterministic_client_order_id("different")


def test_execution_policy_blocks_uneconomic_spread(monkeypatch):
    values = {"v6_smart_execution_enabled": True, "execution_max_economic_spread_pct": 0.3, "execution_max_slippage_pct": 0.35}
    monkeypatch.setattr(execution_router._ss, "get_val", lambda key, default=None: values.get(key, default))
    plan = execution_router.build_execution_plan("TEST", "buy", 10, 100, 1, spread_pct=0.5)
    assert plan.mode == "blocked"
    assert not plan.economically_viable


def test_router_market_is_idempotent(tmp_path, monkeypatch):
    client = FakeClient()
    route = router(tmp_path, monkeypatch, client)
    first = asyncio.run(route.submit(request()))
    second = asyncio.run(route.submit(request()))
    assert first.ok and second.ok
    assert client.market_calls == 1


def test_router_staged_executes_real_chunks(tmp_path, monkeypatch):
    client = FakeClient()
    route = router(tmp_path, monkeypatch, client)
    result = asyncio.run(route.submit(request(order_type="staged")))
    assert result.ok
    assert client.limit_calls == 2
    assert result.filled_contracts == pytest.approx(10)


def test_unarmed_router_blocks_staged_before_any_child_submission(tmp_path, monkeypatch):
    class UnarmedState:
        def can_submit_live(self):
            return False, "live_not_armed:LIVE_READY_UNARMED"

    monkeypatch.setenv("PAPER_TRADING", "false")
    client = FakeClient()
    route = ExecutionRouter(
        client,
        instruments=FakeInstruments(),
        reservations=CapitalReservationBook(tmp_path / "r.json"),
        orders=OrderStateStore(tmp_path / "o.json"),
        live_state=UnarmedState(),
    )
    result = asyncio.run(route.submit(request(order_type="staged")))
    assert not result.ok
    assert result.reason == "live_not_armed:LIVE_READY_UNARMED"
    assert client.limit_calls == client.market_calls == 0


def test_research_runtime_latch_blocks_router_even_if_live_state_is_armed(tmp_path, monkeypatch):
    monkeypatch.setenv("PAPER_TRADING", "false")
    monkeypatch.setenv("OXYPULSE_PRODUCTION_RESEARCH_ONLY", "true")
    client = FakeClient()
    route = ExecutionRouter(
        client,
        instruments=FakeInstruments(),
        reservations=CapitalReservationBook(tmp_path / "r.json"),
        orders=OrderStateStore(tmp_path / "o.json"),
        live_state=FakeLiveState(),
    )

    result = asyncio.run(route.submit(request(intent="research-latch")))

    assert not result.ok
    assert result.reason == "production_research_runtime_execution_disabled"
    assert client.market_calls == 0


def test_uncertain_submit_queries_before_any_retry(tmp_path, monkeypatch):
    class UncertainClient(FakeClient):
        async def place_market_order(self, *args, **kwargs):
            self.market_calls += 1
            kwargs["on_exchange_request"]()
            raise TimeoutError("unknown submit outcome")

        async def get_order(self, coin, **kwargs):
            self.query_calls += 1
            return {
                "ordId": "M1",
                "clOrdId": kwargs.get("client_order_id", ""),
                "state": "filled",
                "accFillSz": "10",
                "avgPx": "100",
            }

        async def get_positions_status(self, **kwargs):
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

    monkeypatch.setenv("PAPER_TRADING", "false")
    import position_registry
    monkeypatch.setattr(position_registry, "_PATH", tmp_path / "position_registry.json")
    monkeypatch.setattr(position_registry, "_registry", {
        "TEST": {
            "direction": "long",
            "engine": "SHOGUN",
            "size_usdt": 10.0,
            "entry": 0.0,
            "stop_loss": 99.0,
            "take_profit": 103.0,
            "pending_reservation": True,
            "entry_lifecycle": "PRE_SUBMIT_RESERVED",
            "mutation_attempted": False,
            "order_id": "",
        },
    })
    client = UncertainClient()
    route = ExecutionRouter(client, instruments=FakeInstruments(), reservations=CapitalReservationBook(tmp_path / "r.json"), orders=OrderStateStore(tmp_path / "o.json"), live_state=FakeLiveState())
    route.reservations._items["reservation"] = {
        "reservation_id": "reservation", "intent_id": "intent",
        "candidate_id": "candidate", "margin_usdt": 1.0,
        "risk_usdt": 0.1, "created_at": time.time(),
        "expires_at": time.time() + 120.0,
        "ownership_state": "PRE_SUBMIT_RESERVED",
        "ownership_reason": "test_fixture",
        "ownership_updated_at": time.time(), "metadata": {},
    }
    route.reservations._save()
    first = asyncio.run(route.submit(request()))
    second = asyncio.run(route.submit(request()))
    assert not first.ok
    assert second.ok
    assert client.market_calls == 1
    assert client.query_calls == 1


def test_preflight_skip_never_claims_exchange_submission(tmp_path, monkeypatch):
    class PreflightSkipClient(FakeClient):
        async def place_market_order(self, *args, **kwargs):
            self.market_calls += 1
            return {
                "status": "REJECTED",
                "reason": "preflight_skip",
                "post_attempted": False,
            }

    client = PreflightSkipClient()
    route = router(tmp_path, monkeypatch, client)
    route.reservations._items["reservation"]["intent_id"] = "preflight-skip"
    route.reservations._save()
    result = asyncio.run(route.submit(request(intent="preflight-skip")))
    row = route.orders.get("preflight-skip")

    assert not result.ok
    assert row["state"] == "REJECTED"
    assert row.get("exchange_request_at") is None
    assert "SUBMISSION_ATTEMPTED" not in [event["state"] for event in row["history"]]


def test_protection_failure_emergency_closes_unprotected_fill():
    class Client(FakeClient):
        async def place_protective_order(self, **kwargs):
            raise RuntimeError("protection rejected")

    routed = []

    async def durable_close(**kwargs):
        routed.append(kwargs)
        return {"status": "CLOSE_FILLED"}

    client = Client()
    result = asyncio.run(ProtectionManager(client, emergency_close=durable_close).ensure(
        coin="TEST", direction="long", contracts=1, stop_price=99, paper=False,
    ))
    assert not result.protected
    assert result.emergency_closed
    assert routed and routed[0]["source"] == "protection_unprotected_fill"


def test_protection_replaces_wrong_quantity():
    class Client(FakeClient):
        def __init__(self):
            super().__init__()
            self.cancelled_algos = 0

        async def get_algo_orders(self, coin):
            return [{"algoId": "OLD", "instId": "TEST-USDT-SWAP", "state": "live", "slTriggerPx": "99", "sz": "1"}]

        async def cancel_protective_orders(self, orders):
            self.cancelled_algos += 1
            return {
                "code": "0",
                "data": [{"sCode": "0", "algoId": str(row.get("algoId", ""))} for row in orders],
            }

    client = Client()
    result = asyncio.run(ProtectionManager(client).ensure(
        coin="TEST", direction="long", contracts=2, stop_price=99, paper=False,
        position_lifecycle_id="adaptive-test-lifecycle",
    ))
    assert result.protected
    assert client.cancelled_algos == 1


def test_pnl_reconciliation_deduplicates_fills_and_excludes_provisional(tmp_path, monkeypatch):
    applied = []
    monkeypatch.setattr("risk_manager.risk.record_realized_pnl", lambda pnl, **kwargs: applied.append(pnl))
    reconciler = PnLReconciler(tmp_path / "pnl.json")
    reconciler.record_provisional("T1", local_estimated_net_pnl=-1.0)
    assert reconciler.confirmed_total() == 0.0
    row = reconciler.reconcile_payloads(
        "T1",
        entry_fills=[
            {"fillId": "1", "fillSz": "1", "fillPx": "100", "fee": "-0.1"},
            {"fillId": "1", "fillSz": "1", "fillPx": "100", "fee": "-0.1"},
        ],
        exit_fills=[{"fillId": "2", "fillSz": "1", "fillPx": "99", "fee": "-0.1"}],
        bills=[{"billId": "F", "type": "funding", "balChg": "-0.05"}],
        exchange_realized_pnl=-1.2,
        evidence={
            "fills": {"status": "SUCCESS_DATA", "data": [{}]},
            "funding": {"status": "SUCCESS_DATA", "data": [{"billId": "F"}]},
            "position_history": {"status": "SUCCESS_DATA", "data": [{}], "realized_pnl_present": True},
            "fees": {"status": "SUCCESS_DATA", "data": [{}]},
        },
    )
    assert row["entry_fee"] == pytest.approx(-0.1)
    assert row["funding"] == pytest.approx(-0.05)
    assert row["pnl_status"] == "MISMATCH"
    assert reconciler.confirmed_total() == pytest.approx(0.0)
    assert sum(applied) == pytest.approx(-1.2)


def test_live_state_restart_never_remains_armed(tmp_path, monkeypatch):
    monkeypatch.setenv("PAPER_TRADING", "false")
    path = tmp_path / "live.json"
    path.write_text(json.dumps({"state": "LIVE_ARMED", "armed": True}))
    state = LiveStateMachine(path)
    assert state.snapshot()["state"] == "LIVE_READY_UNARMED"
    assert not state.snapshot()["armed"]


def test_paper_mode_cannot_arm(tmp_path, monkeypatch):
    monkeypatch.setenv("PAPER_TRADING", "true")
    state = LiveStateMachine(tmp_path / "live.json")
    allowed, reason = state.arm({"ok": True, "checks": {}})
    assert not allowed
    assert reason == "paper_mode_cannot_arm"


def test_failed_preflight_cannot_arm(tmp_path, monkeypatch):
    monkeypatch.setenv("PAPER_TRADING", "false")
    state = LiveStateMachine(tmp_path / "live.json")
    allowed, reason = state.arm({"ok": False, "reason": "metadata stale", "checks": {}})
    assert not allowed
    assert reason == "preflight_failed"
    assert state.snapshot()["state"] == "LIVE_PREFLIGHT_FAILED"


def test_completed_controlled_lifecycle_can_be_retired_for_normal_live(tmp_path):
    from controlled_lifecycle import ControlledLifecycleGate

    path = tmp_path / "controlled.json"
    path.write_text(json.dumps({"scope": "one_production_lifecycle", "status": "COMPLETED"}))
    gate = ControlledLifecycleGate(path)
    assert gate.enforced() is True
    gate.retire("normal_production_operator_authorized")
    assert gate.enforced() is False
    assert gate.snapshot()["status"] == "RETIRED"


def test_live_submission_revalidation_requires_matching_reservation(tmp_path, monkeypatch):
    monkeypatch.setenv("PAPER_TRADING", "false")
    route = ExecutionRouter(
        FakeClient(),
        instruments=FakeInstruments(),
        reservations=CapitalReservationBook(tmp_path / "r.json"),
        orders=OrderStateStore(tmp_path / "o.json"),
        live_state=FakeLiveState(),
    )
    valid, reason, _ = asyncio.run(route._validate_live_submission(request(intent="missing-reservation")))
    assert valid is False
    assert reason == "capital_reservation_missing_or_expired"


def test_live_submission_accepts_fresh_forming_execution_price(tmp_path, monkeypatch):
    import market_data_stream

    monkeypatch.setenv("PAPER_TRADING", "false")
    now = time.time()
    monkeypatch.setitem(market_data_stream._cache_1m, "TEST", {
        "timestamps": [now - 15.0],
        "opens": [100.0], "highs": [101.0], "lows": [99.0],
        "closes": [100.25], "volumes": [1.0], "confirmed": [False],
    })
    reservations = CapitalReservationBook(tmp_path / "r.json")
    ok, _, reservation_id = asyncio.run(reservations.reserve(
        intent_id="fresh", candidate_id="candidate", margin_usdt=1.0,
        risk_usdt=0.1, max_total_margin_usdt=10.0,
    ))
    assert ok is True
    client = FakeClient()

    async def account_snapshot():
        return {"avail_eq_usdt": 10.0}

    async def account_config():
        return {"posMode": "long_short_mode"}

    async def positions_status():
        return {"ok": True, "positions": [], "reason": ""}

    client.get_account_snapshot = account_snapshot
    client.get_account_config = account_config
    client.get_positions_status = positions_status
    route = ExecutionRouter(
        client,
        instruments=FakeInstruments(),
        reservations=reservations,
        orders=OrderStateStore(tmp_path / "o.json"),
        live_state=FakeLiveState(),
    )
    req = replace(request(intent="fresh"), reservation_id=reservation_id)
    valid, reason, evidence = asyncio.run(route._validate_live_submission(req))

    assert valid is True, reason
    assert evidence["current_price"] == 100.25
    assert evidence["market_freshness"]["fresh"] is True


def test_engine_timeout_invokes_uncommitted_registry_cleanup(monkeypatch):
    import capital_reservations
    import order_state_store

    cleaned = []

    class SlowEngine:
        async def evaluate_all(self, universe, balance):
            await asyncio.sleep(60)

    async def force_timeout(awaitable, timeout):
        awaitable.close()
        raise asyncio.TimeoutError

    async def cleanup(engine, *, protected_coins=None):
        cleaned.append((engine, set(protected_coins or set())))
        return ["ORPHAN"]

    monkeypatch.setattr(trading_engine.asyncio, "wait_for", force_timeout)
    monkeypatch.setattr(trading_engine, "registry_release_uncommitted_pending", cleanup)
    monkeypatch.setattr(capital_reservations.book, "snapshot", lambda: [])
    monkeypatch.setattr(order_state_store.store, "unresolved", lambda: [])
    result = asyncio.run(
        trading_engine.MultiEngine()._run_engine_scan_with_timeout(
            "AMATERASU", SlowEngine(), ["TEST"], 10.0
        )
    )

    assert result["timeout"] is True
    assert cleaned == [("AMATERASU", set())]


def test_candidate_preview_uses_fresh_stream_price_without_rest(monkeypatch):
    class Client:
        def __init__(self):
            self.calls = 0

        async def get_ticker(self, coin):
            self.calls += 1
            return {"last": 99.0}

    client = Client()
    monkeypatch.setattr(category_engine, "execution_price_snapshot", lambda *args, **kwargs: {
        "fresh": True, "price": 101.25, "latest_timestamp": time.time(),
    })

    ticker = asyncio.run(category_engine._candidate_preview_ticker(client, "TEST"))

    assert ticker["last"] == 101.25
    assert ticker["source"] == "ws_1m"
    assert client.calls == 0


def test_candidate_preview_falls_back_to_rest_when_stream_is_stale(monkeypatch):
    class Client:
        def __init__(self):
            self.calls = 0

        async def get_ticker(self, coin):
            self.calls += 1
            return {"last": 99.0}

    client = Client()
    monkeypatch.setattr(category_engine, "execution_price_snapshot", lambda *args, **kwargs: {
        "fresh": False, "price": 101.25, "latest_timestamp": 1.0,
    })

    ticker = asyncio.run(category_engine._candidate_preview_ticker(client, "TEST"))

    assert ticker["last"] == 99.0
    assert ticker["source"] == "okx_rest"
    assert client.calls == 1


def test_fill_store_persists_first_and_last_fill_timestamps(tmp_path):
    store = OrderStateStore(tmp_path / "orders.json")
    store.create({"intent_id": "T1"})
    first = store.add_fill("T1", {"fillId": "F1", "fillSz": 1, "fillPx": 100})
    second = store.add_fill("T1", {"fillId": "F2", "fillSz": 1, "fillPx": 101})
    assert first["first_fill_at"] > 0
    assert second["first_fill_at"] == first["first_fill_at"]
    assert second["last_fill_at"] >= first["first_fill_at"]

import asyncio

import pytest

import exit_manager
import execution_router
from instrument_registry import InstrumentMetadata


class FakePartialClient:
    def __init__(self, positions, *, response=None, fills=None, order=None, raise_on_close=False):
        self.positions = list(positions)
        self.response = dict(response if response is not None else {"ordId": "O-1", "state": "filled"})
        self.fills = list(fills or [])
        self.order = dict(order if order is not None else self.response)
        self.raise_on_close = raise_on_close
        self.post_calls = 0
        self.order_queries = 0
        self.fill_queries = 0
        self.last_fill_order_id = ""

    async def get_positions(self):
        if len(self.positions) > 1:
            row = dict(self.positions.pop(0))
        else:
            row = dict(self.positions[0]) if self.positions else None
        if row is None:
            return []
        row.setdefault("cTime", "1700000000000")
        return [row]

    async def get_positions_status(self, **_kwargs):
        return {"ok": True, "positions": await self.get_positions(), "reason": ""}

    async def get_account_config(self, **_kwargs):
        return {"posMode": "long_short_mode"}

    async def close_futures_position(self, coin, pos_side, size_usdt, *, contracts_override=None, client_order_id="", before_send=None, **_kwargs):
        self.post_calls += 1
        if before_send:
            before_send()
        if self.raise_on_close:
            raise TimeoutError("connect timeout")
        result = dict(self.response)
        result.setdefault("clOrdId", client_order_id)
        if contracts_override is not None:
            result.setdefault("requested_contracts", contracts_override)
        # The production close router consumes the current OKX mutation
        # envelope (top-level code plus one attributable item).  Keep the
        # test's compact response inputs, but expose that same contract to
        # the router so these fakes exercise the real classification path.
        if "data" not in result and "code" not in result:
            state = str(result.get("state", "filled") or "filled").lower()
            if state in {"rejected", "canceled", "cancelled"}:
                return {
                    "code": "0",
                    "ordId": str(result.get("ordId", "O-1") or "O-1"),
                    "state": state,
                    "data": [{"ordId": str(result.get("ordId", "O-1") or "O-1"), "sCode": "0"}],
                    "mutation_outcome": "SUCCESS",
                    "post_attempted": True,
                }
            return {
                "code": "0",
                "ordId": str(result.get("ordId", "O-1") or "O-1"),
                "clOrdId": str(result.get("clOrdId", client_order_id) or client_order_id),
                "state": state,
                "accFillSz": result.get("accFillSz", result.get("fillSz", "")),
                "avgPx": result.get("avgPx", result.get("fillPx", "")),
                "data": [{
                    "ordId": str(result.get("ordId", "O-1") or "O-1"),
                    "clOrdId": str(result.get("clOrdId", client_order_id) or client_order_id),
                    "sCode": "0",
                }],
                "mutation_outcome": "SUCCESS",
                "post_attempted": True,
            }
        return result

    async def get_order_status(self, coin, *, order_id="", client_order_id=""):
        self.order_queries += 1
        if not self.order:
            return {"ok": True, "found": False, "classification": "NOT_FOUND", "order": {}}
        return {"ok": True, "found": True, "classification": "FOUND", "order": dict(self.order)}

    async def get_fills_status(self, *, inst_id="", order_id="", limit=100):
        self.fill_queries += 1
        self.last_fill_order_id = order_id
        return {
            "ok": True,
            "status": "SUCCESS_DATA" if self.fills else "SUCCESS_EMPTY",
            "fills": list(self.fills),
        }


def metadata():
    return InstrumentMetadata(
        inst_id="TEST-USDT-SWAP", inst_type="SWAP", settle_ccy="USDT",
        ct_val=100.0, ct_val_ccy="USDT", ct_type="linear", lot_sz=0.01,
        min_sz=0.01, tick_sz=0.01, state="live", refreshed_at=9999999999,
    )


def plan(**kwargs):
    values = {
        "coin": "TEST", "direction": "long", "engine_name": "TEST",
        "entry_price": 100.0, "size_usdt": 100.0, "atr_val": 1.0,
        "hard_sl": 98.0, "tp1_price": 102.0, "tp2_price": 105.0,
        "order_id": "ENTRY-1",
    }
    values.update(kwargs)
    return exit_manager.ExitPlan(**values)


@pytest.fixture
def setup_partial(monkeypatch):
    monkeypatch.setenv("PAPER_TRADING", "false")
    monkeypatch.setattr(exit_manager, "_partial_instrument", lambda coin: metadata())
    monkeypatch.setattr(exit_manager, "_partial_persist", lambda *args, **kwargs: True)
    monkeypatch.setattr(exit_manager, "_record_closure_handoff_reconciliation", lambda *args, **kwargs: None)
    # The operator-close compatibility route requires durable lifecycle
    # bindings. Keep this unit fixture isolated from repository runtime data
    # while still exercising the exchange-truth path.
    monkeypatch.setattr("position_registry.get_all_details", lambda: {})
    monkeypatch.setattr("position_registry.update", lambda *args, **kwargs: True)
    monkeypatch.setattr("position_registry.adopt_exchange_position", lambda *args, **kwargs: True)
    exit_manager._plans.clear()
    yield
    exit_manager._plans.clear()


def run_partial(client, p, pct=25.0, stage="tp1"):
    exit_manager._okx_client = client
    exit_manager._plans[p.coin] = p
    router = execution_router.router_for(client)
    router.instruments.get = lambda *_args, **_kwargs: metadata()
    router.instruments.require = lambda *_args, **_kwargs: metadata()
    return asyncio.run(exit_manager._execute_partial(p.coin, 102.0, pct, stage=stage))


def test_requested_25_filled_25_updates_from_verified_quantity(setup_partial):
    client = FakePartialClient(
        [{"instId": "TEST-USDT-SWAP", "posSide": "long", "pos": "1.0"},
         {"instId": "TEST-USDT-SWAP", "posSide": "long", "pos": "0.75"}],
        fills=[{"fillId": "F1", "ordId": "O-1", "fillSz": "0.25", "fillPx": "102", "fee": "-0.02"}],
        order={"ordId": "O-1", "state": "filled", "accFillSz": "0.25", "avgPx": "102"},
    )
    result = run_partial(client, plan())
    assert result["ok"] is True
    assert result["state"] == exit_manager.PARTIAL_FILLED
    assert result["verified_filled_contracts"] == pytest.approx(0.25)
    assert result["remaining_contracts"] == pytest.approx(0.75)
    assert result["fees_usdt"] == pytest.approx(0.02)
    assert client.post_calls == 1


def test_requested_25_filled_15_does_not_complete_tp1(setup_partial):
    client = FakePartialClient(
        [{"instId": "TEST-USDT-SWAP", "posSide": "long", "pos": "1.0"},
         {"instId": "TEST-USDT-SWAP", "posSide": "long", "pos": "0.85"}],
        fills=[{"fillId": "F1", "ordId": "O-1", "fillSz": "0.15", "fillPx": "102", "fee": "-0.01"}],
        order={"ordId": "O-1", "state": "partially_filled", "accFillSz": "0.15", "avgPx": "102"},
    )
    p = plan()
    result = run_partial(client, p)
    assert result["state"] == exit_manager.PARTIAL_PARTIALLY_FILLED
    assert result["milestone_complete"] is False
    assert p.confirmed_remaining_contracts == pytest.approx(0.85)
    assert p.size_usdt == pytest.approx(85.0)
    assert p.remaining_pct == pytest.approx(85.0)


@pytest.mark.parametrize("order_state", ["rejected", "canceled"])
def test_zero_fill_keeps_remaining_and_milestone_unset(setup_partial, order_state):
    client = FakePartialClient(
        [{"instId": "TEST-USDT-SWAP", "posSide": "long", "pos": "1.0"},
         {"instId": "TEST-USDT-SWAP", "posSide": "long", "pos": "1.0"}],
        response={"ordId": "O-1", "state": order_state},
        order={"ordId": "O-1", "state": order_state},
    )
    p = plan()
    result = run_partial(client, p)
    assert result["ok"] is False
    assert result["state"] == exit_manager.PARTIAL_REJECTED
    assert p.size_usdt == pytest.approx(100.0)
    assert p.tp1_done is False


def test_multiple_fills_are_aggregated_and_duplicate_fill_is_ignored(setup_partial):
    fills = [
        {"fillId": "F1", "ordId": "O-1", "fillSz": "0.10", "fillPx": "101", "fee": "-0.01"},
        {"fillId": "F2", "ordId": "O-1", "fillSz": "0.08", "fillPx": "102", "fee": "-0.01"},
        {"fillId": "F3", "ordId": "O-1", "fillSz": "0.07", "fillPx": "103", "fee": "-0.01"},
        {"fillId": "F2", "ordId": "O-1", "fillSz": "0.08", "fillPx": "102", "fee": "-0.01"},
    ]
    client = FakePartialClient(
        [{"instId": "TEST-USDT-SWAP", "posSide": "long", "pos": "1.0"},
         {"instId": "TEST-USDT-SWAP", "posSide": "long", "pos": "0.75"}],
        fills=fills,
        order={"ordId": "O-1", "state": "filled"},
    )
    result = run_partial(client, plan())
    assert result["verified_filled_contracts"] == pytest.approx(0.25)
    assert result["fill_count"] == 3
    assert result["fill_price"] == pytest.approx((0.10 * 101 + 0.08 * 102 + 0.07 * 103) / 0.25)
    assert result["fees_usdt"] == pytest.approx(0.03)


def test_fill_and_position_delta_mismatch_keeps_economics_uncertain_but_updates_exposure(setup_partial):
    client = FakePartialClient(
        [{"instId": "TEST-USDT-SWAP", "posSide": "long", "pos": "1.0"},
         {"instId": "TEST-USDT-SWAP", "posSide": "long", "pos": "0.75"}],
        fills=[{"fillId": "F1", "ordId": "O-1", "fillSz": "0.20", "fillPx": "102", "fee": "-0.01"}],
        order={"ordId": "O-1", "state": "partially_filled", "accFillSz": "0.20", "avgPx": "102"},
    )
    p = plan()
    result = run_partial(client, p)
    assert result["ok"] is False
    assert result["state"] == exit_manager.PARTIAL_UNCERTAIN
    assert result["recovery_state"] == "FILL_POSITION_DELTA_MISMATCH"
    assert p.size_usdt == pytest.approx(75.0)
    assert p.confirmed_remaining_contracts == pytest.approx(0.75)
    assert p.exposure_truth_status == exit_manager.EXPOSURE_ACTIVE
    assert p.partial_attribution_status == "UNCERTAIN"
    assert p.partial_realized_pnl_usdt == pytest.approx(0.0)


def test_tp2_milestone_also_requires_verified_fill_attribution(setup_partial):
    client = FakePartialClient(
        [{"instId": "TEST-USDT-SWAP", "posSide": "long", "pos": "1.0"},
         {"instId": "TEST-USDT-SWAP", "posSide": "long", "pos": "0.75"}],
        fills=[{"fillId": "F2", "ordId": "O-2", "fillSz": "0.20", "fillPx": "105", "fee": "-0.01"}],
        order={"ordId": "O-2", "state": "partially_filled", "accFillSz": "0.20", "avgPx": "105"},
    )
    p = plan(tp1_done=True)
    result = run_partial(client, p, stage="tp2")
    assert result["state"] == exit_manager.PARTIAL_UNCERTAIN
    assert p.tp2_done is False
    assert p.partial_realized_pnl_usdt == pytest.approx(0.0)


def test_ambiguous_post_recovers_read_only_and_never_replays(setup_partial):
    client = FakePartialClient(
        [{"instId": "TEST-USDT-SWAP", "posSide": "long", "pos": "1.0"},
         {"instId": "TEST-USDT-SWAP", "posSide": "long", "pos": "0.75"}],
        raise_on_close=True,
        fills=[{"fillId": "F1", "ordId": "O-1", "fillSz": "0.25", "fillPx": "102", "fee": "-0.01"}],
        order={"ordId": "O-1", "state": "filled", "accFillSz": "0.25", "avgPx": "102"},
    )
    p = plan()
    result = run_partial(client, p)
    assert result["ok"] is True
    assert result["verified_filled_contracts"] == pytest.approx(0.25)
    assert client.post_calls == 0
    second = asyncio.run(exit_manager._execute_partial("TEST", 102.0, 25.0, stage="tp1"))
    assert second["ok"] is False
    assert second["recovery_state"] == "already_resolved_no_replay"
    assert client.post_calls == 0


def test_restart_after_submission_attempted_recovers_without_post(setup_partial):
    client = FakePartialClient(
        [{"instId": "TEST-USDT-SWAP", "posSide": "long", "pos": "0.85"}],
        fills=[{"fillId": "F1", "ordId": "O-1", "fillSz": "0.15", "fillPx": "102", "fee": "-0.01"}],
        order={"ordId": "O-1", "state": "partially_filled", "accFillSz": "0.15", "avgPx": "102"},
    )
    p = plan(partial_exit_state={"tp1": {
        "intent_id": "PARTIAL-ENTRY-1-TP1",
        "client_order_id": "OXYCdeadbeef",
        "lifecycle": exit_manager.PARTIAL_SUBMISSION_ATTEMPTED,
        "mutation_attempted": True,
        "before_contracts": 1.0,
        "requested_close_contracts": 0.25,
        "response": {},
    }})
    result = run_partial(client, p)
    assert result["state"] == exit_manager.PARTIAL_PARTIALLY_FILLED
    assert client.post_calls == 0
    assert p.confirmed_remaining_contracts == pytest.approx(0.85)


def test_client_id_recovery_promotes_order_id_before_fill_query(setup_partial):
    client = FakePartialClient(
        [{"instId": "TEST-USDT-SWAP", "posSide": "long", "pos": "0.75"}],
        fills=[{"fillId": "F1", "ordId": "O-2", "fillSz": "0.25", "fillPx": "102", "fee": "-0.01"}],
        order={"ordId": "O-2", "state": "filled", "avgPx": "102"},
    )
    p = plan(partial_exit_state={"tp1": {
        "intent_id": "PARTIAL-ENTRY-1-TP1",
        "client_order_id": "OXYCclientid",
        "lifecycle": exit_manager.PARTIAL_SUBMISSION_ATTEMPTED,
        "mutation_attempted": True,
        "before_contracts": 1.0,
        "requested_close_contracts": 0.25,
        "response": {},
    }})
    result = run_partial(client, p)
    assert result["state"] == exit_manager.PARTIAL_FILLED
    assert client.last_fill_order_id == "O-2"


def test_full_close_race_creates_non_actionable_closure_handoff(setup_partial):
    client = FakePartialClient(
        [{"instId": "TEST-USDT-SWAP", "posSide": "long", "pos": "1.0"},
         {"instId": "TEST-USDT-SWAP", "posSide": "long", "pos": "0.0"}],
        fills=[],
        order={"ordId": "O-3", "state": "filled", "avgPx": "102"},
    )
    p = plan()
    result = run_partial(client, p)
    assert result["ok"] is False
    assert result["state"] == exit_manager.PARTIAL_UNCERTAIN
    assert result["recovery_state"] == "EXCHANGE_CLOSED_AWAITING_RECONCILIATION"
    assert p.size_usdt == pytest.approx(0.0)
    assert p.confirmed_remaining_contracts == pytest.approx(0.0)
    assert p.exposure_truth_status == exit_manager.EXCHANGE_CLOSED_AWAITING_RECONCILIATION
    assert p.partial_attribution_status == "AWAITING_RECONCILIATION"
    assert p.partial_realized_pnl_usdt == pytest.approx(0.0)


def test_read_only_not_found_with_no_fill_remains_uncertain(setup_partial):
    client = FakePartialClient(
        [{"instId": "TEST-USDT-SWAP", "posSide": "long", "pos": "1.0"}],
        fills=[],
        order={},
    )
    p = plan(partial_exit_state={"tp1": {
        "intent_id": "PARTIAL-ENTRY-1-TP1",
        "order_id": "O-4",
        "client_order_id": "OXYCnotfound",
        "lifecycle": exit_manager.PARTIAL_SUBMISSION_ATTEMPTED,
        "mutation_attempted": True,
        "before_contracts": 1.0,
        "requested_close_contracts": 0.25,
        "response": {"ordId": "O-4"},
    }})
    result = run_partial(client, p)
    assert result["ok"] is False
    assert result["state"] == exit_manager.PARTIAL_UNCERTAIN
    assert result["recovery_state"] == "FILL_EVIDENCE_INCOMPLETE"
    assert p.size_usdt == pytest.approx(100.0)
    assert client.post_calls == 0


def test_paper_partial_is_local_and_tracks_actual_simulated_quantity(monkeypatch):
    monkeypatch.setenv("PAPER_TRADING", "true")
    monkeypatch.setattr(exit_manager, "_partial_persist", lambda *args, **kwargs: True)
    client = FakePartialClient([])
    p = plan()
    exit_manager._okx_client = client
    exit_manager._plans[p.coin] = p
    result = asyncio.run(exit_manager._execute_partial("TEST", 102.0, 25.0, stage="tp1"))
    assert result["ok"] is True
    assert p.confirmed_remaining_contracts == pytest.approx(75.0)
    assert client.post_calls == 0
    exit_manager._plans.clear()


def test_persistence_failure_before_requested_intent_never_posts(monkeypatch):
    monkeypatch.setenv("PAPER_TRADING", "false")
    monkeypatch.setattr(exit_manager, "_partial_instrument", lambda coin: metadata())
    client = FakePartialClient([{"instId": "TEST-USDT-SWAP", "posSide": "long", "pos": "1.0"}])
    monkeypatch.setattr(exit_manager, "_partial_persist", lambda *args, **kwargs: False)
    p = plan()
    result = run_partial(client, p)
    assert result["recovery_state"] == "intent_persistence_failed_before_post"
    assert client.post_calls == 0


def test_persistence_failure_before_submission_attempt_never_posts(monkeypatch):
    monkeypatch.setenv("PAPER_TRADING", "false")
    monkeypatch.setattr(exit_manager, "_partial_instrument", lambda coin: metadata())
    client = FakePartialClient([{"instId": "TEST-USDT-SWAP", "posSide": "long", "pos": "1.0"}])
    calls = {"count": 0}

    def persist(*args, **kwargs):
        calls["count"] += 1
        return calls["count"] == 1

    monkeypatch.setattr(exit_manager, "_partial_persist", persist)
    p = plan()
    result = run_partial(client, p)
    assert result["recovery_state"] == "attempt_persistence_failed_before_post"
    assert client.post_calls == 0


def test_partial_then_cancel_keeps_verified_exposure_without_replay(setup_partial):
    client = FakePartialClient(
        [{"instId": "TEST-USDT-SWAP", "posSide": "long", "pos": "0.85"}],
        fills=[{"fillId": "F1", "ordId": "O-1", "fillSz": "0.15", "fillPx": "102", "fee": "-0.01"}],
        order={"ordId": "O-1", "state": "canceled", "accFillSz": "0.15", "avgPx": "102"},
    )
    p = plan(partial_exit_state={"tp1": {
        "intent_id": "PARTIAL-ENTRY-1-TP1",
        "order_id": "O-1",
        "client_order_id": "OXYCpartialcancel",
        "lifecycle": exit_manager.PARTIAL_UNCERTAIN,
        "mutation_attempted": True,
        "before_contracts": 1.0,
        "requested_close_contracts": 0.25,
        "response": {"ordId": "O-1", "state": "canceled"},
    }})
    result = run_partial(client, p)
    assert client.post_calls == 0
    assert result["state"] == exit_manager.PARTIAL_PARTIALLY_FILLED
    assert p.confirmed_remaining_contracts == pytest.approx(0.85)
    assert p.tp1_done is False


def test_repeated_reconciliation_is_idempotent_for_partial_economics(monkeypatch, setup_partial):
    calls = []
    monkeypatch.setattr(exit_manager._perf, "record_trade", lambda *args, **kwargs: calls.append((args, kwargs)))
    client = FakePartialClient(
        [{"instId": "TEST-USDT-SWAP", "posSide": "long", "pos": "0.85"}],
        fills=[{"fillId": "F1", "ordId": "O-1", "fillSz": "0.15", "fillPx": "102", "fee": "-0.01"}],
        order={"ordId": "O-1", "state": "partially_filled", "accFillSz": "0.15", "avgPx": "102"},
    )
    p = plan(partial_exit_state={"tp1": {
        "intent_id": "PARTIAL-ENTRY-1-TP1",
        "order_id": "O-1",
        "client_order_id": "OXYCrepeat",
        "lifecycle": exit_manager.PARTIAL_SUBMISSION_ATTEMPTED,
        "mutation_attempted": True,
        "before_contracts": 1.0,
        "requested_close_contracts": 0.25,
        "response": {"ordId": "O-1"},
    }})
    first = run_partial(client, p)
    second = asyncio.run(exit_manager._execute_partial("TEST", 102.0, 25.0, stage="tp1"))
    assert first["state"] == exit_manager.PARTIAL_PARTIALLY_FILLED
    assert second["recovery_state"] == "already_resolved_no_replay"
    assert client.post_calls == 0
    assert len(calls) == 1
    assert p.partial_exit_events[0]["economic_event_id"]


def test_final_close_requires_fresh_positive_contracts_and_uses_them(monkeypatch):
    monkeypatch.setenv("PAPER_TRADING", "false")
    exit_manager._plans.clear()
    p = plan(confirmed_remaining_contracts=0.25)
    calls = []

    class Client:
        async def get_positions(self):
            return [{"instId": "TEST-USDT-SWAP", "posSide": "long", "pos": "0.75"}]

    class Router:
        async def close(self, coin, side, size, **kwargs):
            calls.append((coin, side, size, kwargs))
            return {"ordId": "CLOSE-1"}

    monkeypatch.setattr(exit_manager, "_partial_persist", lambda *args, **kwargs: True)
    monkeypatch.setattr(exit_manager, "router_for", Router, raising=False)
    monkeypatch.setattr("execution_router.router_for", lambda client: Router())
    exit_manager._okx_client = Client()
    try:
        assert asyncio.run(exit_manager._execute_exit("TEST", 101.0, "manual", p)) is True
        assert calls[0][3]["contracts"] == pytest.approx(0.75)
    finally:
        exit_manager._plans.clear()


@pytest.mark.parametrize("positions, expected, status", [
    ([], True, exit_manager.EXCHANGE_CLOSED_AWAITING_RECONCILIATION),
    (None, False, exit_manager.EXPOSURE_UNKNOWN),
])
def test_final_close_zero_or_unknown_quantity_never_posts(monkeypatch, positions, expected, status):
    monkeypatch.setenv("PAPER_TRADING", "false")
    exit_manager._plans.clear()
    p = plan()
    calls = []

    class Client:
        async def get_positions(self):
            if positions is None:
                raise TimeoutError("positions unavailable")
            return positions

    class Router:
        async def close(self, *args, **kwargs):
            calls.append((args, kwargs))
            return {"ordId": "CLOSE-1"}

    monkeypatch.setattr(exit_manager, "_partial_persist", lambda *args, **kwargs: True)
    monkeypatch.setattr("execution_router.router_for", lambda client: Router())
    exit_manager._okx_client = Client()
    try:
        result = asyncio.run(exit_manager._execute_exit("TEST", 101.0, "manual", p))
        assert result is expected
        assert calls == []
        assert p.exposure_truth_status == status
    finally:
        exit_manager._plans.clear()


def test_closure_handoff_blocks_reprotection_mutation(monkeypatch):
    monkeypatch.setenv("PAPER_TRADING", "false")
    p = plan(exposure_truth_status=exit_manager.EXCHANGE_CLOSED_AWAITING_RECONCILIATION)
    calls = []

    class Protection:
        async def reprotect(self, **kwargs):
            calls.append(kwargs)
            return None

    old_manager = exit_manager._protection_manager
    exit_manager._protection_manager = Protection()
    try:
        asyncio.run(exit_manager._reprotect_if_needed("TEST", p))
        assert calls == []
    finally:
        exit_manager._protection_manager = old_manager


def test_economic_event_identity_survives_missing_plan_flag(monkeypatch, tmp_path):
    import performance_tracker as perf

    old_path, old_state = perf._PATH, perf._state
    monkeypatch.setattr(perf, "_PATH", tmp_path / "performance.json")
    perf._state = perf.PerformanceState()
    try:
        perf.record_trade("TEST", "long", 100.0, 102.0, 10.0, 0.2, "partial_tp1", event_id="econ-1")
        assert len(perf._state.trades) == 1
        persisted_trade = perf._state.trades[0]
        perf._state = perf.PerformanceState(trades=[persisted_trade], applied_event_ids=[])
        perf.record_trade("TEST", "long", 100.0, 102.0, 10.0, 0.2, "partial_tp1", event_id="econ-1")
        assert len(perf._state.trades) == 1
        assert perf._state.applied_event_ids == ["econ-1"]
    finally:
        perf._PATH, perf._state = old_path, old_state


def test_closure_handoff_is_reconciliation_visible_but_not_actionable(monkeypatch):
    import position_registry as registry

    original_registry = registry._registry
    original_save = registry._save
    registry._registry = {
        "TEST": {
            "direction": "long",
            "engine": "TEST",
            "size_usdt": 0.0,
            "entry": 100.0,
            "exposure_truth_status": registry.EXCHANGE_CLOSED_AWAITING_RECONCILIATION,
        }
    }
    registry._save = lambda *args, **kwargs: None
    try:
        assert registry.count() == 0
        assert registry.get_all() == {}
        assert registry.get_all_details()["TEST"]["exposure_truth_status"] == registry.EXCHANGE_CLOSED_AWAITING_RECONCILIATION
        assert registry.is_open("TEST") is False
        allowed, reason = asyncio.run(
            registry.can_open_and_reserve("TEST", "long", "TEST", size_usdt=50.0, entry=100.0, stop_loss=99.0, balance_usdt=500.0)
        )
        assert allowed is False
        assert "closure reconciliation pending" in reason
        assert set(registry._registry) == {"TEST"}
    finally:
        registry._registry = original_registry
        registry._save = original_save


def test_restart_restores_zero_exposure_closure_handoff_without_replay(monkeypatch):
    import category_engine

    monkeypatch.setenv("PAPER_TRADING", "false")
    exit_manager._plans.clear()
    engine = category_engine.CategoryEngine.__new__(category_engine.CategoryEngine)
    engine.name = "TEST"
    engine._open = {}
    details = {
        "direction": "long",
        "entry": 100.0,
        "size_usdt": 0.0,
        "leverage": 10,
        "stop_loss": 98.0,
        "take_profit": 102.0,
        "tp2_price": 105.0,
        "atr": 1.0,
        "order_id": "ENTRY-1",
        "entry_contracts": 1.0,
        "confirmed_remaining_contracts": 0.0,
        "exposure_truth_status": exit_manager.EXCHANGE_CLOSED_AWAITING_RECONCILIATION,
        "partial_attribution_status": "AWAITING_RECONCILIATION",
        "partial_exit_state": {"tp1": {"lifecycle": exit_manager.PARTIAL_UNCERTAIN}},
    }
    try:
        assert engine.restore_position("TEST", details) is True
        restored = exit_manager.get_plan("TEST")
        assert restored is not None
        assert restored.exposure_truth_status == exit_manager.EXCHANGE_CLOSED_AWAITING_RECONCILIATION
        assert restored.confirmed_remaining_contracts == pytest.approx(0.0)
        client = FakePartialClient([])
        exit_manager._okx_client = client
        result = asyncio.run(exit_manager._execute_partial("TEST", 102.0, 25.0, stage="tp1"))
        assert result["recovery_state"] == "EXCHANGE_CLOSED_AWAITING_RECONCILIATION"
        assert client.post_calls == 0
    finally:
        engine._open.clear()
        exit_manager._plans.clear()


def test_telegram_can_show_exchange_exposure_separately_from_partial_attribution():
    import tg_panels

    text = tg_panels.panel_pnl_sync_text({
        "source_label": "EXCHANGE_TRUTH",
        "confidence": "HIGH",
        "open_pnl_usdt": 0.4,
        "local_estimate_pnl_usdt": 0.3,
        "drift_usdt": 0.1,
        "age_sec": 1.0,
        "stale_count": 0,
        "mismatch_count": 0,
        "positions": {
            "TEST": {
                "status": "OPEN",
                "source_label": "EXCHANGE_TRUTH",
                "confidence": "HIGH",
                "unrealised_pnl": 0.4,
                "registry": {
                    "confirmed_remaining_contracts": 0.75,
                    "exposure_truth_status": "ACTIVE_POSITION",
                    "exchange_position_timestamp": "2026-08-14T00:00:00+00:00",
                    "partial_exit_state": {"tp1": {
                        "stage": "tp1",
                        "lifecycle": "PARTIAL_UNCERTAIN",
                        "verified_filled_contracts": 0.15,
                        "remaining_exchange_contracts": 0.75,
                    }},
                },
            }
        },
    })
    assert "ACTIVE_POSITION" in text
    assert "0.75000000" in text
    assert "PARTIAL_UNCERTAIN" in text

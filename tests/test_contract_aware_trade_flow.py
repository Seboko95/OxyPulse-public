from __future__ import annotations

import asyncio
import time

import capital_allocator
import pytest
from execution_router import ExecutionRequest, ExecutionResult, ExecutionRouter
from instrument_registry import InstrumentMetadata


def _instrument(**overrides):
    values = {
        "inst_id": "TEST-USDT-SWAP",
        "inst_type": "SWAP",
        "settle_ccy": "USDT",
        "ct_val": 0.006,
        "ct_val_ccy": "TEST",
        "ct_type": "linear",
        "lot_sz": 1.0,
        "min_sz": 1.0,
        "tick_sz": 0.01,
        "state": "live",
        "max_mkt_sz": 1000.0,
        "max_lmt_sz": 1000.0,
        "max_leverage": 20.0,
        "refreshed_at": time.time(),
        "source_hash": "test",
        "raw": {},
    }
    values.update(overrides)
    return InstrumentMetadata(**values)


class _Settings:
    def __init__(self, values):
        self.values = values

    def get_val(self, key, default=None):
        return self.values.get(key, default)

    def get_effective_min_order_usdt(self, balance):
        return float(self.values.get("effective_floor", 25.0))


def _allocator_settings(monkeypatch, **overrides):
    values = {
        "adaptive_capital_allocator_enabled": True,
        "contract_aware_execution_floor_enabled": True,
        "adaptive_min_economic_notional_usdt": 1.0,
        "position_size_cap_usdt": 250.0,
        "position_size_cap_pct_balance": 100.0,
        "leverage_default": 10,
        "rule_357_enabled": True,
        "rule_357_max_open_risk_pct": 12.0,
        "min_sl_dist_pct": 1.0,
        "live_taker_fee_rate": 0.0005,
        "deployment_mode": "Full Deployment",
    }
    values.update(overrides)
    monkeypatch.setattr(capital_allocator._s, "get_val", lambda key, default=None: values.get(key, default))
    monkeypatch.setattr(capital_allocator.micro_sizing, "usable_margin_usdt", lambda balance, settings: balance)


def test_contract_aware_floor_uses_smallest_lot_aligned_quantity(monkeypatch):
    _allocator_settings(monkeypatch)
    meta = _instrument(ct_val=0.006, lot_sz=1.0, min_sz=1.0)
    decision = capital_allocator.allocate_entry(
        balance_usdt=10.0,
        requested_size_usdt=0.40,
        entry_price=100.0,
        stop_price=99.0,
        leverage=10,
        instrument_metadata=meta,
        metadata={"tp2_price": 106.0},
        available_margin_usdt=10.0,
    )

    assert decision.approved
    assert decision.minimum_executable_contracts == 2.0
    assert decision.approved_contracts == 2.0
    assert decision.final_size_usdt == 1.2
    assert decision.contract_floor_uplift_applied
    assert decision.contract_floor_uplift_from_usdt == 1.0


def test_contract_floor_uplift_is_refused_when_risk_headroom_is_insufficient(monkeypatch):
    _allocator_settings(monkeypatch, rule_357_max_open_risk_pct=0.5)
    meta = _instrument(ct_val=0.006, lot_sz=1.0, min_sz=1.0)
    decision = capital_allocator.allocate_entry(
        balance_usdt=10.0,
        requested_size_usdt=0.40,
        entry_price=100.0,
        stop_price=90.0,
        leverage=10,
        instrument_metadata=meta,
        available_margin_usdt=10.0,
    )

    assert not decision.approved
    assert "below_min_exec" in decision.reason or "open_risk" in decision.reason
    assert decision.approved_contracts == 0.0


def test_contract_floor_uplift_is_refused_when_fee_negative(monkeypatch):
    _allocator_settings(monkeypatch)
    meta = _instrument(ct_val=0.006, lot_sz=1.0, min_sz=1.0)
    decision = capital_allocator.allocate_entry(
        balance_usdt=10.0,
        requested_size_usdt=0.40,
        entry_price=100.0,
        stop_price=99.0,
        leverage=10,
        instrument_metadata=meta,
        metadata={"tp2_price": 100.01},
        available_margin_usdt=10.0,
    )

    assert not decision.approved
    assert decision.reason == "allocator_contract_floor_fee_negative"


def test_small_staged_order_is_sent_as_one_market_request():
    instrument = _instrument(ct_val=0.01, lot_sz=1.0, min_sz=1.0)

    class Instruments:
        def require(self, coin):
            return instrument

    class Orders:
        def create(self, row):
            raise AssertionError("small order should bypass staged parent creation")

    router = ExecutionRouter(object(), instruments=Instruments(), orders=Orders())
    seen = []

    async def fake_submit(request):
        seen.append(request)
        return ExecutionResult(
            ok=True,
            reason="test",
            intent_id=request.intent_id,
            client_order_id="test",
            filled_contracts=request.approved_contracts,
            average_fill_price=request.price,
        )

    router.submit = fake_submit
    request = ExecutionRequest(
        intent_id="I",
        candidate_id="C",
        reservation_id="R",
        mode="Guarded",
        engine="TEST",
        coin="TEST",
        direction="long",
        requested_contracts=2.0,
        approved_contracts=2.0,
        approved_notional_usdt=2.0,
        order_type="staged",
        price=100.0,
        leverage=10,
        metadata={"execution_chunks": [{"fraction": 0.4}, {"fraction": 0.35}, {"fraction": 0.25}]},
    )

    result = asyncio.run(router._submit_staged(request))
    assert result.ok
    assert len(seen) == 1
    assert seen[0].order_type == "market"
    assert seen[0].metadata["small_order_unsplit"] is True


def test_router_accepts_contract_aware_floor_after_allocator_approval(monkeypatch):
    instrument = _instrument(ct_val=0.006, lot_sz=1.0, min_sz=1.0)

    class Instruments:
        def require(self, coin):
            return instrument

    router = ExecutionRouter(object(), instruments=Instruments())
    request = ExecutionRequest(
        intent_id="I2",
        candidate_id="C2",
        reservation_id="R2",
        mode="Guarded",
        engine="TEST",
        coin="TEST",
        direction="long",
        requested_contracts=2.0,
        approved_contracts=2.0,
        approved_notional_usdt=1.2,
        order_type="market",
        price=100.0,
        leverage=10,
        metadata={"contract_aware_minimum": True},
    )

    ok, reason, detail = router._local_executable_check(request, balance_usdt=10.0)
    assert ok, reason
    assert detail["placement_min_notional_usdt"] == 1.0


def test_contract_metadata_conversion_never_rounds_strategy_size_up():
    instrument = _instrument(ct_val=0.006, lot_sz=1.0, min_sz=1.0)
    assert instrument.round_contracts_down(1.9) == 1.0
    assert instrument.minimum_executable_contracts(1.0, 100.0) == 2.0


def test_notional_cap_blocks_bnb_minimum_even_when_margin_fits(monkeypatch):
    _allocator_settings(monkeypatch)
    instrument = _instrument(ct_val=0.01, lot_sz=1.0, min_sz=1.0)
    decision = capital_allocator.allocate_entry(
        balance_usdt=11.01,
        requested_size_usdt=6.0,
        entry_price=611.5,
        stop_price=605.0,
        leverage=15,
        instrument_metadata=instrument,
        metadata={"tp2_price": 650.0},
        available_margin_usdt=11.01,
        participation_cap_usdt=4.404,
    )

    assert not decision.approved
    assert "participation_cap_exceeded" not in decision.reason
    assert "participation_cap_usdt" in decision.reason
    minimum_contracts = instrument.minimum_executable_contracts(1.0, 611.5)
    assert instrument.contracts_to_notional(minimum_contracts, 611.5) == pytest.approx(6.115)
    assert instrument.required_margin(minimum_contracts, 611.5, 15) < 0.5


def test_under_cap_eth_uplift_remains_eligible(monkeypatch):
    _allocator_settings(monkeypatch)
    instrument = _instrument(ct_val=0.1, lot_sz=0.01, min_sz=0.01)
    decision = capital_allocator.allocate_entry(
        balance_usdt=11.01,
        requested_size_usdt=6.0,
        entry_price=1887.02,
        stop_price=1868.0,
        leverage=15,
        instrument_metadata=instrument,
        metadata={"tp2_price": 2010.0},
        available_margin_usdt=11.01,
        participation_cap_usdt=4.404,
    )

    assert decision.approved
    assert decision.final_size_usdt == pytest.approx(3.77404)
    assert decision.final_size_usdt <= 4.404
    assert decision.participation_cap_usdt == pytest.approx(4.404)


def test_lot_boundary_that_requires_above_cap_minimum_is_blocked(monkeypatch):
    _allocator_settings(monkeypatch)
    instrument = _instrument(ct_val=1.0, lot_sz=1.0, min_sz=1.0)
    decision = capital_allocator.allocate_entry(
        balance_usdt=11.01,
        requested_size_usdt=4.404,
        entry_price=4.5,
        stop_price=4.4,
        leverage=25,
        instrument_metadata=instrument,
        metadata={"tp2_price": 5.0},
        available_margin_usdt=11.01,
        participation_cap_usdt=4.404,
    )

    assert not decision.approved
    assert "participation_cap" in decision.reason
    minimum_contracts = instrument.minimum_executable_contracts(1.0, 4.5)
    assert instrument.contracts_to_notional(minimum_contracts, 4.5) == pytest.approx(4.5)


def test_notional_cap_is_leverage_independent(monkeypatch):
    _allocator_settings(monkeypatch)
    instrument = _instrument(ct_val=0.01, lot_sz=1.0, min_sz=1.0)
    for leverage in (10, 25):
        decision = capital_allocator.allocate_entry(
            balance_usdt=11.01,
            requested_size_usdt=6.0,
            entry_price=611.5,
            stop_price=605.0,
            leverage=leverage,
            instrument_metadata=instrument,
            metadata={"tp2_price": 650.0},
            available_margin_usdt=11.01,
            participation_cap_usdt=4.404,
        )
        assert not decision.approved
        assert "participation_cap_usdt" in decision.reason


def test_risk_limit_still_blocks_under_notional_cap(monkeypatch):
    _allocator_settings(monkeypatch, rule_357_max_open_risk_pct=0.5)
    instrument = _instrument(ct_val=0.006, lot_sz=1.0, min_sz=1.0)
    decision = capital_allocator.allocate_entry(
        balance_usdt=10.0,
        requested_size_usdt=0.40,
        entry_price=100.0,
        stop_price=90.0,
        leverage=10,
        instrument_metadata=instrument,
        metadata={"tp2_price": 106.0},
        available_margin_usdt=10.0,
        participation_cap_usdt=4.404,
    )

    assert not decision.approved
    assert "risk" in decision.reason or "open_risk" in decision.reason


def test_above_cap_candidate_does_not_block_next_valid_candidate(monkeypatch):
    _allocator_settings(monkeypatch)
    bnb = _instrument(ct_val=0.01, lot_sz=1.0, min_sz=1.0)
    eth = _instrument(ct_val=0.1, lot_sz=0.01, min_sz=0.01)
    cap = 4.404
    blocked = capital_allocator.allocate_entry(
        balance_usdt=11.01,
        requested_size_usdt=6.0,
        entry_price=611.5,
        stop_price=605.0,
        leverage=15,
        instrument_metadata=bnb,
        metadata={"tp2_price": 650.0},
        available_margin_usdt=11.01,
        participation_cap_usdt=cap,
    )
    allowed = capital_allocator.allocate_entry(
        balance_usdt=11.01,
        requested_size_usdt=0.40,
        entry_price=1887.02,
        stop_price=1868.0,
        leverage=15,
        instrument_metadata=eth,
        metadata={"tp2_price": 2010.0},
        available_margin_usdt=11.01,
        participation_cap_usdt=cap,
    )

    assert not blocked.approved
    assert allowed.approved


def test_router_defense_blocks_payload_above_participation_cap():
    instrument = _instrument(ct_val=0.01, lot_sz=1.0, min_sz=1.0)

    class Instruments:
        def require(self, coin):
            return instrument

    router = ExecutionRouter(object(), instruments=Instruments())
    request = ExecutionRequest(
        intent_id="cap-defense",
        candidate_id="C3",
        reservation_id="R3",
        mode="Guarded",
        engine="TEST",
        coin="TEST",
        direction="long",
        requested_contracts=1.0,
        approved_contracts=1.0,
        approved_notional_usdt=6.115,
        order_type="market",
        price=611.5,
        leverage=15,
        metadata={
            "contract_aware_minimum": True,
            "participation_cap_usdt": 4.404,
        },
    )

    ok, reason, detail = router._local_executable_check(request, balance_usdt=11.01)
    assert not ok
    assert "participation_cap_exceeded" in reason
    assert detail["funnel_counter"] == "rejected_participation_cap"

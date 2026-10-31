import math
import time

import pytest

import capital_allocator
from candidate_economic_state import CandidateEconomicStateStore
from candidate_economics import decompose_floor, evaluate_final_size, solve_minimum_equity
from instrument_registry import InstrumentMetadata
from live_fee_schedule import current, record_okx, round_trip_cost


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
        "tick_sz": 0.01,
        "state": "live",
        "max_mkt_sz": 100000.0,
        "max_lmt_sz": 100000.0,
        "max_leverage": 20.0,
        "refreshed_at": time.time(),
        "source_hash": "test",
        "raw": {},
    }
    values.update(overrides)
    return InstrumentMetadata(**values)


def test_economically_valid_equity_is_not_candidate_viability():
    result = evaluate_final_size(
        desired_size_usdt=5,
        final_size_usdt=4,
        target_move_pct=0.01,
        stop_move_pct=1,
        fee_rate=0.0005,
        spread_pct=0.05,
        slippage_pct=0.03,
    )
    assert not result["allowed"]
    assert result["final"]["expected_net_usdt"] < 0


def test_desired_to_resized_decomposition_recomputes_both_sizes():
    result = evaluate_final_size(
        desired_size_usdt=7.97,
        final_size_usdt=4.55,
        target_move_pct=3,
        stop_move_pct=1.4,
        fee_rate=0.0005,
        spread_pct=0.01,
        slippage_pct=0.03,
    )
    assert result["desired_size_usdt"] == pytest.approx(7.97)
    assert result["final_size_usdt"] == pytest.approx(4.55)
    assert result["desired"]["estimated_cost_usdt"] > result["final"]["estimated_cost_usdt"]


def test_economic_floor_decomposition_is_instrument_specific():
    floor = decompose_floor(
        instrument=instrument(ct_val=0.1, min_sz=0.1, lot_sz=0.1),
        price=500,
        configured_candidate_floor_usdt=1,
        fee_rate=0.0005,
        target_move_pct=3,
        exit_legs=3,
    )
    assert floor.exchange_min_notional_usdt == pytest.approx(5)
    assert floor.partial_exit_floor_usdt >= 15
    assert floor.binding_constraint == "partial_exit_contract_quantity"


def test_expectancy_recalculated_after_resize():
    result = evaluate_final_size(
        desired_size_usdt=100,
        final_size_usdt=40,
        target_move_pct=4,
        stop_move_pct=1,
        fee_rate=0.0005,
        spread_pct=0.01,
        slippage_pct=0.02,
    )
    assert result["expectancy_recomputed"]
    assert result["final"]["expected_gross_usdt"] != result["desired"]["expected_gross_usdt"]


def test_fees_recalculated_after_resize():
    result = evaluate_final_size(
        desired_size_usdt=100,
        final_size_usdt=25,
        target_move_pct=4,
        stop_move_pct=1,
        fee_rate=0.0005,
        spread_pct=0,
        slippage_pct=0,
    )
    assert result["fees_recomputed"]
    assert result["final"]["costs"]["entry_fee"] == pytest.approx(0.0125)


def test_exit_plan_recalculated_after_resize():
    meta = instrument(ct_val=1, min_sz=1, lot_sz=1)
    result = evaluate_final_size(
        desired_size_usdt=500,
        final_size_usdt=150,
        target_move_pct=4,
        stop_move_pct=1,
        fee_rate=0.0005,
        spread_pct=0,
        slippage_pct=0,
        instrument=meta,
        entry_price=100,
        exit_legs=3,
    )
    assert result["full_exit_viable"]
    assert not result["partial_exits_viable"]
    assert result["exit_management_mode"] == "full_position_only"


def test_inverse_minimum_equity_solver():
    result = solve_minimum_equity(
        lambda equity: {
            "viable": equity * 0.4 >= 5,
            "final_size_usdt": equity * 0.4,
            "contracts": math.floor(equity * 0.4),
            "economic_floor_usdt": 5,
            "minimum_available_margin_usdt": equity * 0.04,
            "binding_constraint": "position_size_cap_pct_balance",
        },
        lower=1,
        upper=100,
    )
    assert result["solved"]
    assert result["minimum_equity_usdt"] == pytest.approx(12.5, abs=0.001)


def test_inverse_solver_respects_nonlinear_contract_rounding():
    result = solve_minimum_equity(
        lambda equity: {
            "viable": math.floor((equity * 0.4) / 3.0) * 3.0 >= 6.0,
            "final_size_usdt": math.floor((equity * 0.4) / 3.0) * 3.0,
            "contracts": math.floor((equity * 0.4) / 3.0),
            "economic_floor_usdt": 6,
            "binding_constraint": "contract_rounding",
        },
        lower=1,
        upper=100,
    )
    assert result["minimum_equity_usdt"] == pytest.approx(15.0, abs=0.001)


@pytest.mark.parametrize("stop_pct,expected", [(1.0, 100.0), (2.0, 50.0), (4.0, 25.0)])
def test_minimum_capital_changes_with_stop_distance(stop_pct, expected):
    risk_amount = 1.0
    assert risk_amount / (stop_pct / 100.0) == pytest.approx(expected)


@pytest.mark.parametrize("fee_rate", [0.0002, 0.0005, 0.0010])
def test_minimum_capital_changes_with_fee_class(fee_rate):
    floor = decompose_floor(
        instrument=instrument(), price=100, configured_candidate_floor_usdt=1,
        fee_rate=fee_rate, target_move_pct=3, minimum_net_profit_usdt=0.1,
    )
    assert floor.partial_exit_floor_usdt > 0


def test_authoritative_fee_schedule_round_trip(tmp_path):
    path = tmp_path / "fees.json"
    record_okx({"maker": "-0.0002", "taker": "-0.0005"}, path=path, fetched_at=123)
    rate, schedule = current(path=path)
    assert rate == pytest.approx(0.0005)
    assert schedule.source == "okx_authenticated"
    assert round_trip_cost(100, path=path) == pytest.approx(0.1)


def test_candidate_wait_state_deduplicates_unchanged_input(tmp_path):
    store = CandidateEconomicStateStore(tmp_path / "states.json")
    deps = store.dependencies(equity=11.38, spread_pct=0.01, signal_score=3, risk_capacity_usdt=4.55)
    store.wait("ENMA", "ETH", "long", state="WAITING_FOR_ECONOMIC_SIZE", reason="below floor", dependencies=deps)
    unchanged, _ = store.unchanged("ENMA", "ETH", "long", deps)
    assert unchanged


@pytest.mark.parametrize(
    "changed",
    [
        {"equity": 15.0},
        {"spread_pct": 0.03},
        {"risk_capacity_usdt": 6.0},
    ],
)
def test_candidate_reconsidered_on_relevant_change(tmp_path, changed):
    store = CandidateEconomicStateStore(tmp_path / "states.json")
    base = store.dependencies(equity=11.38, spread_pct=0.01, signal_score=3, risk_capacity_usdt=4.55)
    store.wait("ENMA", "ETH", "long", state="WAITING_FOR_ECONOMIC_SIZE", reason="below floor", dependencies=base)
    updated = dict(base)
    updated.update(changed)
    assert not store.unchanged("ENMA", "ETH", "long", updated)[0]


def test_wait_state_does_not_create_registry_reservation(tmp_path):
    store = CandidateEconomicStateStore(tmp_path / "states.json")
    deps = store.dependencies(equity=10, spread_pct=0, signal_score=3, risk_capacity_usdt=4)
    store.wait("A", "BTC", "long", state="WAITING_FOR_CAPITAL_CHANGE", reason="capital", dependencies=deps)
    assert store.snapshot()["count"] == 1


def test_wait_state_does_not_reduce_engine_scan_flow(tmp_path):
    store = CandidateEconomicStateStore(tmp_path / "states.json")
    observed = []
    for engine in ("A", "B", "C"):
        observed.append(engine)
        deps = store.dependencies(equity=10, spread_pct=0, signal_score=3, risk_capacity_usdt=4)
        store.wait(engine, "BTC", "long", state="WAITING_FOR_ECONOMIC_SIZE", reason="capital", dependencies=deps)
    assert observed == ["A", "B", "C"]
    assert store.snapshot()["count"] == 3


def test_allocator_uses_placement_floor_matching_final_order_path(monkeypatch):
    """Phase-1 correction: admission must use the same placement floor as okx_client.

    Previously adaptive_allocator_use_legacy_min_order_floor=False allowed ~$4.55
    notionals that later failed place_market_order against ~$6.84 effective floor.
    That false-admit path is intentionally closed.
    """
    values = {
        "adaptive_capital_allocator_enabled": True,
        "adaptive_allocator_use_legacy_min_order_floor": False,
        "adaptive_min_economic_notional_usdt": 1.0,
        "position_size_cap_usdt": 250.0,
        "position_size_cap_pct_balance": 40.0,
        "rule_357_enabled": True,
        "rule_357_max_open_risk_pct": 12.0,
        "min_sl_dist_pct": 1.0,
    }
    monkeypatch.setattr(capital_allocator._s, "get_val", lambda key, default=None: values.get(key, default))
    monkeypatch.setattr(capital_allocator._s, "get_effective_min_order_usdt", lambda balance: 6.84)
    monkeypatch.setattr(capital_allocator.micro_sizing, "usable_margin_usdt", lambda balance, settings: balance)
    decision = capital_allocator.allocate_entry(
        balance_usdt=11.38,
        requested_size_usdt=4.55,
        entry_price=100,
        stop_price=99,
        leverage=10,
        instrument_metadata=instrument(ct_val=0.01, min_sz=1, lot_sz=1),
        available_margin_usdt=11.38,
    )
    assert not decision.approved
    assert "below_min_exec" in decision.reason
    assert decision.min_exec_usdt == pytest.approx(6.84)

import inspect

import pytest

import capital_efficiency
import capital_manager
import exit_manager
from candidate_economic_state import CandidateEconomicStateStore
from candidate_economics import resolve_exit_management_mode
from profitability_control_plane import evaluate_candidate
from profit_targeting import apply_profit_targeting


def snapshot(balance=11.0, desired=7.7, final=4.4, stop=1.43, leverage=10, gross=0.08, cost=0.02, net=0.06):
    return capital_efficiency.build_snapshot(
        equity_usdt=balance,
        desired_notional_usdt=desired,
        final_notional_usdt=final,
        stop_dist_pct=stop,
        leverage=leverage,
        expected_gross_usdt=gross,
        expected_cost_usdt=cost,
        expected_net_usdt=net,
        binding_constraint="position_size_cap_pct_balance",
        exit_management_mode="full_position_only",
        position_cap_pct_equity=40.0,
    )


def balance_model(balance, stop_pct=1.5, risk_pct=1.0, cap_pct=40.0, leverage=10):
    risk_budget = balance * risk_pct / 100.0
    desired = risk_budget / (stop_pct / 100.0)
    final = min(desired, balance * cap_pct / 100.0, 250.0)
    return snapshot(
        balance=balance,
        desired=desired,
        final=final,
        stop=stop_pct,
        leverage=leverage,
        gross=final * 0.02,
        cost=final * 0.0014,
        net=final * 0.0186,
    )


def test_notional_and_margin_are_distinct_units():
    row = snapshot(final=40, leverage=10)
    assert row["position_notional_usdt"] == 40
    assert row["isolated_margin_required_usdt"] == 4


def test_notional_and_stop_risk_are_distinct_units():
    row = snapshot(final=40, stop=2)
    assert row["position_notional_usdt"] == 40
    assert row["risk_at_stop_usdt"] == pytest.approx(0.8)


def test_leverage_changes_margin_not_stop_risk():
    low = snapshot(final=40, stop=2, leverage=5)
    high = snapshot(final=40, stop=2, leverage=15)
    assert low["risk_at_stop_usdt"] == high["risk_at_stop_usdt"]
    assert low["isolated_margin_required_usdt"] == pytest.approx(8)
    assert high["isolated_margin_required_usdt"] == pytest.approx(40 / 15)


def test_40_percent_cap_is_notional_pct_equity():
    row = balance_model(50)
    assert row["position_notional_usdt"] == pytest.approx(20)
    assert row["notional_pct_equity"] == pytest.approx(40)
    assert row["position_cap_basis"] == "notional_pct_equity"
    assert row["minimum_equity_for_position_cap_usdt"] == pytest.approx(50)


def test_small_account_risk_sizing_precedes_cap():
    row = balance_model(11, stop_pct=1.5)
    assert row["desired_notional_usdt"] == pytest.approx(7.33333333)
    assert row["position_notional_usdt"] == pytest.approx(4.4)


def test_fee_burden_ratios_are_exposed():
    row = snapshot(final=10, stop=2, gross=0.20, cost=0.05, net=0.15)
    assert row["cost_to_gross_edge_ratio"] == pytest.approx(0.25)
    assert row["cost_to_expected_profit_ratio"] == pytest.approx(1 / 3)
    assert row["cost_to_risk_ratio"] == pytest.approx(0.25)


def test_fixed_usdt_profit_is_not_an_admission_input():
    parameters = inspect.signature(evaluate_candidate).parameters
    assert "minimum_profit_usdt" not in parameters
    assert "min_full_close_profit_usdt" not in parameters


def test_relative_economic_floor_accepts_positive_net_edge():
    decision = evaluate_candidate(
        size_usdt=4,
        target_move_pct=10,
        stop_move_pct=1,
        fee_rate=0.0005,
        spread_pct=0.01,
        slippage_pct=0.01,
    )
    assert decision.expected_net_usdt > 0
    assert decision.allowed


def test_absolute_fee_floor_remains_visible_in_costs():
    decision = evaluate_candidate(
        size_usdt=5,
        target_move_pct=3,
        stop_move_pct=1,
        fee_rate=0.0005,
    )
    assert decision.costs["entry_fee"] == pytest.approx(0.0025)
    assert decision.costs["exit_fee"] == pytest.approx(0.0025)


def test_partial_exit_simplifies_below_economic_floor():
    viable, mode, reason = resolve_exit_management_mode(
        full_exit_viable=True,
        quantity_partial_viable=True,
        final_notional_usdt=8,
        partial_exit_floor_usdt=20,
    )
    assert not viable
    assert mode == "full_position_only"
    assert reason == "partial_exit_economic_floor"


def test_5_dollar_capital_model_uses_same_formula():
    row = balance_model(5)
    assert row["position_notional_usdt"] == pytest.approx(2)


def test_11_dollar_capital_model_uses_same_formula():
    row = balance_model(11)
    assert row["position_notional_usdt"] == pytest.approx(4.4)


def test_50_dollar_capital_model_uses_same_formula():
    row = balance_model(50)
    assert row["position_notional_usdt"] == pytest.approx(20)


def test_500_dollar_capital_model_uses_same_formula():
    row = balance_model(500)
    assert row["position_notional_usdt"] == pytest.approx(200)


def test_positive_edge_small_dollar_candidate_is_not_rejected_for_small_dollars():
    decision = evaluate_candidate(
        size_usdt=2,
        target_move_pct=12,
        stop_move_pct=1,
        fee_rate=0.0005,
        spread_pct=0.01,
    )
    assert 0 < decision.expected_net_usdt < 1
    assert decision.allowed


def test_fee_dominated_micro_candidate_is_rejected():
    decision = evaluate_candidate(
        size_usdt=2,
        target_move_pct=0.05,
        stop_move_pct=1,
        fee_rate=0.0005,
        spread_pct=0.10,
        slippage_pct=0.10,
    )
    assert decision.expected_net_usdt < 0
    assert not decision.allowed or decision.action == "reduce"


def test_profit_targeting_does_not_stretch_for_fixed_dollar_profit():
    result = apply_profit_targeting(100, "buy", 102, 104, 2, tp_distance_multiplier=1.0)
    assert result["tp1"] == 102
    assert result["tp2"] == 104
    assert not result["skip"]


def test_deployment_modes_adapt_from_same_balance_engine():
    guarded = capital_manager.get_profile_for_mode(50, capital_manager.DEPLOYMENT_GUARDED)
    full = capital_manager.get_profile_for_mode(50, capital_manager.DEPLOYMENT_FULL_DEPLOYMENT)
    assert guarded.balance == full.balance == 50
    assert guarded.deployment_mode == capital_manager.DEPLOYMENT_GUARDED
    assert full.deployment_mode == capital_manager.DEPLOYMENT_FULL_DEPLOYMENT


def test_candidate_queue_is_preserved_for_unfunded_candidate(tmp_path):
    store = CandidateEconomicStateStore(tmp_path / "candidate_states.json")
    deps = store.dependencies(equity=11, spread_pct=0.02, signal_score=3, risk_capacity_usdt=4.4)
    store.wait("TEST", "ETH", "long", state="WAITING_FOR_CAPITAL_CHANGE", reason="capital", dependencies=deps)
    assert store.snapshot()["count"] == 1


def test_capital_diagnostic_has_no_production_order_side_effect():
    source = inspect.getsource(capital_efficiency)
    assert "place_order" not in source
    assert "execution_router" not in source


def test_expected_net_return_on_margin_is_explicit():
    row = snapshot(final=10, leverage=10, net=0.10)
    assert row["expected_net_return_on_margin"] == pytest.approx(0.10)


def test_profitability_decision_exposes_proportional_metrics():
    row = evaluate_candidate(
        size_usdt=10,
        target_move_pct=8,
        stop_move_pct=2,
        fee_rate=0.0005,
    ).to_dict()
    assert row["expected_net_edge_bps"] is not None
    assert row["cost_to_risk_ratio"] is not None


def test_exit_manager_honors_full_position_override():
    coin = "TESTCAP"
    try:
        plan = exit_manager.register(
            coin, "long", 100, 8, 1, 98, 103, 106,
            partial_mode_override=False,
        )
        assert plan.partial_mode is False
    finally:
        exit_manager.remove(coin)

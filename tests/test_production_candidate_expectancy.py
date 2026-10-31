import inspect
import json
import os
import threading

import pytest

import candidate_expectancy_telemetry as telemetry
import category_engine
from candidate_economics import evaluate_final_size, resolve_exit_management_mode
from profitability_control_plane import (
    calibrate_probabilities,
    classify_expectancy,
    evaluate_candidate,
)


def decision(**overrides):
    values = dict(
        size_usdt=100.0,
        target_move_pct=4.0,
        stop_move_pct=1.0,
        entry_fee_rate=0.0005,
        exit_fee_rate=0.0005,
        spread_pct=0.02,
        entry_slippage_pct=0.01,
        exit_slippage_pct=0.01,
        production_calibration=True,
    )
    values.update(overrides)
    return evaluate_candidate(**values)


def test_negative_gross_edge_classification():
    assert decision(target_move_pct=0.1).classification == "NEGATIVE_GROSS_EDGE"


def test_cost_destroyed_classification():
    row = decision(target_move_pct=1.3, spread_pct=1.0, entry_slippage_pct=0.5)
    assert row.expected_gross_usdt > 0 > row.expected_net_usdt
    assert row.classification == "POSITIVE_GROSS_EDGE_COST_DESTROYED"


def test_uncertainty_destroyed_classification():
    row = decision(target_move_pct=2.0, min_cost_multiple=2.0)
    assert row.expected_net_usdt > 0
    assert row.expected_net_usdt < row.required_net_usdt
    assert row.classification == "POSITIVE_GROSS_EDGE_UNCERTAINTY_DESTROYED"


def test_outcome_decomposition_sums_to_gross():
    row = decision()
    contributions = sum(v["expected_contribution_usdt"] for v in row.outcome_decomposition.values())
    assert contributions == pytest.approx(row.expected_gross_usdt, abs=1e-5)


def test_probabilities_sum_to_one():
    row = decision()
    assert sum(v["probability"] for v in row.outcome_decomposition.values()) == pytest.approx(1.0)


def test_entry_fee_applied_once():
    assert decision(spread_pct=0, entry_slippage_pct=0, exit_slippage_pct=0).costs["entry_fee"] == pytest.approx(0.05)


def test_exit_fee_applied_once():
    assert decision(spread_pct=0, entry_slippage_pct=0, exit_slippage_pct=0).costs["exit_fee"] == pytest.approx(0.05)


def test_spread_applied_once():
    row = decision(entry_fee_rate=0, exit_fee_rate=0, spread_pct=0.02, entry_slippage_pct=0, exit_slippage_pct=0)
    assert row.costs["spread"] == pytest.approx(0.02)


def test_entry_and_exit_slippage_are_distinct():
    row = decision(entry_fee_rate=0, exit_fee_rate=0, spread_pct=0, entry_slippage_pct=0.01, exit_slippage_pct=0.02)
    assert row.costs["entry_slippage"] == pytest.approx(0.01)
    assert row.costs["exit_slippage"] == pytest.approx(0.02)


def test_legacy_slippage_is_total_route_cost():
    row = evaluate_candidate(size_usdt=100, target_move_pct=4, stop_move_pct=1, fee_rate=0, slippage_pct=0.03)
    assert row.costs["entry_slippage"] == pytest.approx(0.03)
    assert row.costs["exit_slippage"] == 0


def test_funding_only_when_relevant():
    assert decision(funding_pct=0.05, funding_applicable=False).costs["funding"] == 0
    assert decision(funding_pct=0.05, funding_applicable=True).costs["funding"] == pytest.approx(0.05)


def test_model_uncertainty_applied_once_as_required_margin():
    row = decision()
    assert row.expected_net_usdt == pytest.approx(row.expected_gross_usdt - row.estimated_cost_usdt, abs=1e-6)
    assert row.safety_margin_usdt == pytest.approx(row.model_uncertainty_usdt + row.execution_uncertainty_usdt)


@pytest.mark.parametrize(
    "route,entry,exit",
    [
        ("maker_maker", 0.0002, 0.0002),
        ("maker_taker", 0.0002, 0.0005),
        ("taker_maker", 0.0005, 0.0002),
        ("taker_taker", 0.0005, 0.0005),
    ],
)
def test_execution_route_economics(route, entry, exit):
    row = decision(execution_route=route, entry_fee_rate=entry, exit_fee_rate=exit)
    assert row.execution_route == route
    assert row.costs["entry_fee"] == pytest.approx(100 * entry)
    assert row.costs["exit_fee"] == pytest.approx(100 * exit)


def test_engine_family_route_is_explicit_not_inferred():
    assert decision(engine="RYUU", execution_route="taker_taker").execution_route == "taker_taker"


def test_full_position_only_has_one_economic_exit_plan():
    viable, mode, _ = resolve_exit_management_mode(
        full_exit_viable=True,
        quantity_partial_viable=False,
        final_notional_usdt=6.0,
    )
    assert not viable and mode == "full_position_only"


def test_resized_size_recalculates_every_cost():
    row = evaluate_final_size(
        desired_size_usdt=100,
        final_size_usdt=40,
        target_move_pct=4,
        stop_move_pct=1,
        fee_rate=0.0005,
        spread_pct=0.02,
        slippage_pct=0.03,
    )
    assert row["desired"]["costs"]["entry_fee"] == pytest.approx(0.05)
    assert row["final"]["costs"]["entry_fee"] == pytest.approx(0.02)


def test_historical_strong_move_can_be_positive():
    row = decision(size_usdt=200, target_move_pct=8.06, stop_move_pct=1.0)
    assert row.expected_net_usdt > 0
    assert row.classification == "POSITIVE_EXPECTANCY"


def test_production_calibration_excludes_paper_and_unknown():
    history = [
        {"engine": "RYUU", "closed_at": "x", "exit_reason": "tp2_hit", "environment": "paper"},
        {"engine": "RYUU", "closed_at": "x", "exit_reason": "tp2_hit"},
    ]
    row = calibrate_probabilities(engine="RYUU", direction="long", history=history, production_calibration=True)
    assert row.sample_size == 0
    assert row.paper_sample_size == 1
    assert row.unknown_sample_size == 1


def test_production_calibration_accepts_exchange_final_live():
    history = [{
        "engine": "RYUU", "direction": "long", "closed_at": "x", "exit_reason": "tp2_hit",
        "environment": "live", "exchange_final": True,
    }]
    row = calibrate_probabilities(engine="RYUU", direction="long", history=history, production_calibration=True)
    assert row.sample_size == row.live_sample_size == 1


def test_counterfactual_rejection_tracking(tmp_path, monkeypatch):
    monkeypatch.setattr(telemetry, "EVENT_PATH", tmp_path / "events.jsonl")
    monkeypatch.setattr(telemetry, "REJECTION_PATH", tmp_path / "rejections.jsonl")
    monkeypatch.setattr(telemetry, "STATE_PATH", tmp_path / "state.json")
    economics = decision(target_move_pct=0.1).to_dict()
    economics["geometry_preview"] = {"entry_price": 100, "stop_price": 99, "tp1_price": 101, "tp2_price": 102}
    telemetry.record_evaluation(
        symbol="TEST", direction="long", engine="RYUU", confidence=2,
        role="hunter", contributors=[], quality_snapshot={}, economics=economics, disposition="queue",
    )
    assert telemetry.observe_price("TEST", 102) == 1
    summary = telemetry.summarize()
    assert summary["rejected_count"] == summary["resolved_count"] == 1


def test_false_negative_rate_uses_resolved_rejections(tmp_path, monkeypatch):
    path = tmp_path / "rejections.jsonl"
    path.write_text("\n".join([
        json.dumps({"event": "expectancy_rejection_open"}),
        json.dumps({"event": "expectancy_rejection_close", "hypothetical_net_pnl_usdt": 1}),
    ]))
    assert telemetry.summarize(path)["false_negative_rate"] == 1.0


def test_geometry_preview_uses_production_stop_engine(monkeypatch):
    monkeypatch.setattr(category_engine, "_stop_geometry_atr", lambda coin, fallback: 2.0)
    monkeypatch.setattr(category_engine, "_requested_leverage_for_engine", lambda *args: 10)
    monkeypatch.setattr(category_engine._sl_tp, "compute", lambda **kwargs: (98.0, 102.0, 104.0))
    row = category_engine._preview_candidate_geometry(
        engine="RYUU", coin="TEST", direction="long", entry_price=100,
        closes=[100] * 20, highs=[101] * 20, lows=[99] * 20, volumes=[1] * 20, fallback_atr=1,
    )
    assert row["stop_move_pct"] == 2
    assert row["target_move_pct"] == 4
    assert row["atr_timeframe"] == "1H"


def test_deterministic_paper_environment(monkeypatch):
    monkeypatch.setenv("PAPER_TRADING", "true")
    assert not decision(production_calibration=None).probability.source.startswith("host_env")


def test_deterministic_production_environment_excludes_unknown(monkeypatch):
    monkeypatch.setenv("PAPER_TRADING", "false")
    row = evaluate_candidate(
        size_usdt=100, target_move_pct=4, stop_move_pct=1, fee_rate=0.0005,
        history=[{"closed_at": "x", "exit_reason": "tp2_hit"}],
    )
    assert row.probability.sample_size == 0


def test_diagnostics_cannot_submit_production_order():
    sources = inspect.getsource(telemetry) + inspect.getsource(classify_expectancy)
    assert "place_order" not in sources
    assert "submit(" not in sources


def test_counterfactual_state_lock_is_process_local_and_explicit():
    assert isinstance(telemetry._LOCK, type(threading.RLock()))


def test_classification_function_is_pure():
    assert classify_expectancy(
        expected_gross_usdt=-1,
        expected_net_usdt=-2,
        required_net_usdt=1,
        model_uncertainty_usdt=1,
    ) == "NEGATIVE_GROSS_EDGE"

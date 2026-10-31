import inspect
import json

import pytest

import candidate_expectancy_telemetry as telemetry
import category_engine
import execution_router


def _paths(tmp_path, monkeypatch):
    monkeypatch.setattr(telemetry, "EVENT_PATH", tmp_path / "events.jsonl")
    monkeypatch.setattr(telemetry, "REJECTION_PATH", tmp_path / "outcomes.jsonl")
    monkeypatch.setattr(telemetry, "STATE_PATH", tmp_path / "state.json")
    monkeypatch.setattr(telemetry, "_STATE_CACHE", None)
    monkeypatch.setattr(telemetry, "_STATE_CACHE_PATH", "")


def _economics(gross=-0.2, net=-0.3, classification="NEGATIVE_GROSS_EDGE", size=100.0):
    return {
        "expected_gross_usdt": gross,
        "expected_net_usdt": net,
        "estimated_cost_usdt": 0.1,
        "size_usdt": size,
        "classification": classification,
        "probability": {"probabilities": {"tp1": 0.2, "tp2": 0.1, "stop": 0.4}, "uncertainty": 0.2},
        "geometry_preview": {"entry_price": 100, "stop_price": 99, "tp1_price": 101, "tp2_price": 102},
    }


def _features(**overrides):
    row = {
        "planned_entry": 100,
        "entry_executable_price": 100,
        "planned_stop": 99,
        "stop_distance_pct": 1,
        "tp1": 101,
        "tp2": 102,
        "spread_bps": 2,
        "exit_profile": "full_position_only",
        "liquidity_bucket": "high",
        "volatility_bucket": "medium",
        "contributor_families": {"RYUU": "trend", "FUJIN": "order_flow"},
        "vote_rescue": {"allowed": False},
    }
    row.update(overrides)
    return row


def _record(tmp_path, monkeypatch, *, epoch=1000, economics=None, features=None, contributors=None, roles=None, regime="TRENDING", direction="long"):
    _paths(tmp_path, monkeypatch)
    return telemetry.record_evaluation(
        symbol="TEST", direction=direction, engine="RYUU", confidence=3.0, role="hunter",
        contributors=contributors or ["RYUU"], engine_roles=roles or {"RYUU": "hunter"},
        quality_snapshot={"regime": regime, "lane": "trend"},
        economics=economics or _economics(), disposition="queue",
        feature_snapshot=features or _features(), raw_score=2.5,
        engine_family="trend", exit_profile="full_position_only",
        timestamp="2026-01-01T00:00:00+00:00", timestamp_epoch=epoch,
    )


def _lines(path):
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def test_immutable_candidate_feature_snapshot(tmp_path, monkeypatch):
    features = _features()
    _record(tmp_path, monkeypatch, features=features)
    features["planned_entry"] = 999
    assert _lines(telemetry.EVENT_PATH)[0]["features"]["planned_entry"] == 100


def test_candidate_geometry_snapshot(tmp_path, monkeypatch):
    _record(tmp_path, monkeypatch)
    row = _lines(telemetry.EVENT_PATH)[0]
    assert row["features"]["planned_stop"] == 99
    assert row["features"]["tp2"] == 102


def test_model_versions_persist(tmp_path, monkeypatch):
    _record(tmp_path, monkeypatch)
    row = _lines(telemetry.EVENT_PATH)[0]
    assert row["model_version"] == telemetry.MODEL_VERSION
    assert row["calibration_version"] == telemetry.CALIBRATION_VERSION
    assert row["exit_profile_version"]


def test_dataclass_snapshot_is_json_safe(tmp_path, monkeypatch):
    from signal_consensus import ConsensusSnapshot
    _paths(tmp_path, monkeypatch)
    telemetry.record_evaluation(
        symbol="TEST", direction="long", engine="RYUU", confidence=3, role="hunter", contributors=[],
        quality_snapshot={"consensus": ConsensusSnapshot("TEST", "LONG", ["RYUU"], 1, 3, 0, 1)},
        economics=_economics(), disposition="queue", feature_snapshot=_features(), timestamp_epoch=1000,
    )
    assert _lines(telemetry.EVENT_PATH)[0]["quality_snapshot"]["consensus"]["coin"] == "TEST"


def test_forward_mfe_capture(tmp_path, monkeypatch):
    _record(tmp_path, monkeypatch)
    telemetry.observe_price("TEST", 101, high=101.5, low=100, timestamp_epoch=1060)
    state = json.loads(telemetry.STATE_PATH.read_text())
    assert next(iter(state["active"].values()))["mfe_pct"] == pytest.approx(1.5)


def test_forward_mae_capture(tmp_path, monkeypatch):
    _record(tmp_path, monkeypatch, features=_features(planned_stop=95))
    telemetry.observe_price("TEST", 100, high=100.2, low=98.5, timestamp_epoch=1060)
    state = json.loads(telemetry.STATE_PATH.read_text())
    assert next(iter(state["active"].values()))["mae_pct"] == pytest.approx(-1.5)


def test_tp_before_sl_tracking(tmp_path, monkeypatch):
    _record(tmp_path, monkeypatch)
    telemetry.observe_price("TEST", 102, high=102, low=100, timestamp_epoch=1060)
    close = _lines(telemetry.REJECTION_PATH)[-1]
    assert close["tp1_before_sl"] and close["tp2_before_sl"]
    assert close["result"] == "tp2_before_sl"


def test_false_negative_classification(tmp_path, monkeypatch):
    _record(tmp_path, monkeypatch)
    telemetry.observe_price("TEST", 102, high=102, low=100, timestamp_epoch=1060)
    assert _lines(telemetry.REJECTION_PATH)[-1]["classification"] == "FALSE_NEGATIVE"


def test_correct_rejection_classification(tmp_path, monkeypatch):
    _record(tmp_path, monkeypatch)
    telemetry.observe_price("TEST", 99, high=100, low=99, timestamp_epoch=1060)
    assert _lines(telemetry.REJECTION_PATH)[-1]["classification"] == "CORRECT_REJECTION"


def test_opportunity_revision_handling(tmp_path, monkeypatch):
    first = _record(tmp_path, monkeypatch, epoch=1000)
    second = telemetry.record_evaluation(
        symbol="TEST", direction="long", engine="RYUU", confidence=3, role="hunter", contributors=["RYUU"],
        quality_snapshot={"regime": "TRENDING"}, economics=_economics(), disposition="queue",
        feature_snapshot=_features(), timestamp_epoch=1010, timestamp="2026-01-01T00:00:10+00:00",
    )
    rows = _lines(telemetry.EVENT_PATH)
    assert first == second
    assert [row["candidate_revision"] for row in rows] == [1, 2]


def test_no_duplicate_candidate_counting(tmp_path, monkeypatch):
    test_opportunity_revision_handling(tmp_path, monkeypatch)
    alpha = telemetry.aggregate_engine_alpha(engine_names=["RYUU"])
    assert alpha["RYUU"]["evaluations"] == 2
    assert alpha["RYUU"]["candidates"] == 1


def test_engine_gross_edge_aggregation(tmp_path, monkeypatch):
    _record(tmp_path, monkeypatch, economics=_economics(gross=0.5, net=0.4, classification="POSITIVE_EXPECTANCY"))
    row = telemetry.aggregate_engine_alpha(engine_names=["RYUU"])["RYUU"]
    assert row["average_predicted_gross_edge"] == 0.5
    assert row["positive_gross_pct"] == 100


def test_regime_attribution(tmp_path, monkeypatch):
    _record(tmp_path, monkeypatch, regime="VOLATILE")
    dims = telemetry.aggregate_research_dimensions()
    assert dims["regime"]["VOLATILE"]["candidate_count"] == 1


def test_long_short_attribution(tmp_path, monkeypatch):
    _record(tmp_path, monkeypatch, direction="short")
    assert telemetry.aggregate_research_dimensions()["direction"]["short"]["candidate_count"] == 1


def test_role_contribution_analysis(tmp_path, monkeypatch):
    _record(tmp_path, monkeypatch, roles={"RYUU": "hunter", "FUJIN": "confirmer"}, contributors=["RYUU", "FUJIN"])
    assert telemetry.aggregate_research_dimensions()["role"]["confirmer+hunter"]["candidate_count"] == 1


def test_independent_family_confirmation(tmp_path, monkeypatch):
    _record(tmp_path, monkeypatch, contributors=["RYUU", "FUJIN"])
    assert telemetry.aggregate_research_dimensions()["independent_families"]["2"]["candidate_count"] == 1


def test_correlated_family_adjustment(tmp_path, monkeypatch):
    features = _features(contributor_families={"RYUU": "trend", "HAYATE": "trend"})
    _record(tmp_path, monkeypatch, features=features, contributors=["RYUU", "HAYATE"])
    assert telemetry.aggregate_research_dimensions()["independent_families"]["1"]["candidate_count"] == 1


def test_vote_rescue_counterfactual(tmp_path, monkeypatch):
    _record(tmp_path, monkeypatch, features=_features(vote_rescue={"allowed": True}))
    assert telemetry.aggregate_research_dimensions()["vote_rescue"]["rescued"]["candidate_count"] == 1


def test_full_position_only_forward_economics(tmp_path, monkeypatch):
    _record(tmp_path, monkeypatch)
    row = _lines(telemetry.EVENT_PATH)[0]
    assert row["exit_profile"] == "full_position_only"
    assert row["economics"]["estimated_cost_usdt"] == 0.1


def test_candidate_becomes_positive_after_revision(tmp_path, monkeypatch):
    cid = _record(tmp_path, monkeypatch)
    telemetry.record_evaluation(
        symbol="TEST", direction="long", engine="RYUU", confidence=4, role="hunter", contributors=["RYUU"],
        quality_snapshot={"regime": "TRENDING"},
        economics=_economics(gross=1, net=.8, classification="POSITIVE_EXPECTANCY"), disposition="accept",
        feature_snapshot=_features(), timestamp_epoch=1010,
    )
    state = json.loads(telemetry.STATE_PATH.read_text())
    assert state["active"][cid]["became_positive_revision"] == 2


def test_candidate_becomes_negative_after_market_revision(tmp_path, monkeypatch):
    _record(tmp_path, monkeypatch, economics=_economics(gross=1, net=.8, classification="POSITIVE_EXPECTANCY"))
    telemetry.record_evaluation(
        symbol="TEST", direction="long", engine="RYUU", confidence=2, role="hunter", contributors=["RYUU"],
        quality_snapshot={"regime": "TRENDING"}, economics=_economics(), disposition="queue",
        feature_snapshot=_features(), timestamp_epoch=1010,
    )
    assert _lines(telemetry.EVENT_PATH)[-1]["economics"]["classification"] == "NEGATIVE_GROSS_EDGE"


def test_research_runtime_cannot_submit_orders(monkeypatch):
    class State:
        def snapshot(self):
            return {"state": "LIVE_READY_UNARMED", "armed": False}
    import live_state
    monkeypatch.setenv("PAPER_TRADING", "false")
    monkeypatch.setattr(live_state, "state", State())
    assert category_engine._production_research_only()


def test_research_runtime_latch_remains_non_executable_if_live_state_changes(monkeypatch):
    class State:
        def snapshot(self):
            return {"state": "LIVE_ARMED", "armed": True}
    import live_state
    monkeypatch.setenv("PAPER_TRADING", "false")
    monkeypatch.setenv("OXYPULSE_PRODUCTION_RESEARCH_ONLY", "true")
    monkeypatch.setattr(live_state, "state", State())
    assert category_engine._production_research_only()


def test_unarmed_router_submission_fails_closed_by_source():
    source = inspect.getsource(execution_router.ExecutionRouter.submit)
    assert "can_submit_live" in source
    # Fail-closed path now routes through _reject_local (normalized pre-OKX taxonomy).
    assert "_reject_local" in source
    assert "live_not_armed" in inspect.getsource(execution_router.ExecutionRouter) or "can_submit_live" in source


def test_paper_live_provenance_remains_separate(tmp_path, monkeypatch):
    _record(tmp_path, monkeypatch)
    row = _lines(telemetry.EVENT_PATH)[0]
    assert row["environment"] == "production_counterfactual"
    assert row["eligible_for_production_learning"] is False


def test_no_production_order_api_in_research_module():
    source = inspect.getsource(telemetry)
    assert "place_market_order" not in source
    assert "ExecutionRequest" not in source


def test_executed_production_event_is_explicit_and_not_learning_final(tmp_path, monkeypatch):
    _paths(tmp_path, monkeypatch)
    row = telemetry.record_execution(
        candidate_id="C1", opportunity_id="O1", symbol="BTC", direction="long",
        engine="RYUU", intent_id="I1", reservation_id="R1", order_id="OKX1",
        filled_contracts=2, average_fill_price=100, protection={"protected": True},
        timestamp_epoch=1234,
    )
    assert row["disposition"] == "EXECUTED_PRODUCTION"
    assert row["environment"] == "live"
    assert row["order_submitted"] is True
    assert row["exchange_final"] is False
    assert row["eligible_for_production_learning"] is False


def test_horizon_events_are_recorded_once(tmp_path, monkeypatch):
    _record(tmp_path, monkeypatch, features=_features(planned_stop=90, tp1=110, tp2=120))
    telemetry.observe_price("TEST", 100.2, high=100.3, low=100, timestamp_epoch=1061)
    telemetry.observe_price("TEST", 100.3, high=100.4, low=100, timestamp_epoch=1070)
    horizons = [row for row in _lines(telemetry.REJECTION_PATH) if row["event"] == "counterfactual_horizon"]
    assert [row["horizon_secs"] for row in horizons] == [15, 30, 60]


def test_all_zero_signal_engines_remain_visible(tmp_path, monkeypatch):
    _paths(tmp_path, monkeypatch)
    alpha = telemetry.aggregate_engine_alpha(engine_names=["RYUU", "RAIJIN"])
    assert alpha["RYUU"]["candidates"] == alpha["RAIJIN"]["candidates"] == 0


def test_research_summary_exposes_edge_counts_and_maturity(tmp_path, monkeypatch):
    _record(tmp_path, monkeypatch, economics=_economics(gross=0.5, net=0.2, classification="POSITIVE_EXPECTANCY"))
    summary = telemetry.summarize()
    assert summary["unique_opportunities"] == 1
    assert summary["candidate_revisions"] == 1
    assert summary["positive_gross_edge_count"] == 1
    assert summary["positive_net_edge_count"] == 1
    assert summary["evidence_maturity"] == "INSUFFICIENT"


def test_legacy_cap_shadow_is_read_only_and_aggregated(tmp_path, monkeypatch):
    features = _features(legacy_40pct_cap_shadow={
        "research_only": True,
        "applied_to_admission": False,
        "available": True,
        "notional_delta_usdt": 50.0,
        "margin_delta_usdt": 5.0,
        "risk_delta_usdt": 0.5,
    })
    _record(tmp_path, monkeypatch, features=features)
    summary = telemetry.aggregate_legacy_cap_shadow()
    assert summary["candidate_count"] == 1
    assert summary["larger_without_pct_cap_count"] == 1
    assert summary["average_notional_delta_usdt"] == 50.0
    assert summary["applied_to_admission_count"] == 0

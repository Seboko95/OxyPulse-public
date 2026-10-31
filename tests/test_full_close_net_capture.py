from __future__ import annotations

import asyncio

import pytest

import exit_manager as em
from full_close_economics import (
    ALREADY_EXCHANGE_CLOSED,
    CAPITAL_MANAGEMENT,
    MANDATORY_OR_PROTECTIVE,
    MANDATORY_EXIT_NO_FEE_GATE,
    MICRO_PROFIT_FEE_BLEED,
    NET_CAPTURE_HEALTHY,
    NET_CAPTURE_UNKNOWN,
    OPTIONAL_PROFIT_CAPTURE,
    ECONOMIC_EVIDENCE_COMPLETE,
    ECONOMIC_EVIDENCE_ESTIMATED_CONSERVATIVE,
    ECONOMIC_EVIDENCE_INSUFFICIENT,
    ECONOMIC_EVIDENCE_UNKNOWN,
    build_position_lifecycle_economics,
    candidate_stop_is_economically_eligible,
    evaluate_optional_full_close,
    required_cost_relative_net,
    classify_full_close_reason,
    evaluate_full_close_net_capture,
)


def _decision(**overrides):
    values = {
        "reason": "tp",
        "current_remaining_contracts": 0.4,
        "current_remaining_notional": 100.0,
        "gross_remaining_profit": 1.0,
        "already_realized_partial_gross": 0.0,
        "known_entry_fee": 0.05,
        "known_partial_exit_fees": 0.0,
        "estimated_final_exit_fee": 0.025,
        "known_funding": 0.0,
        "funding_status": "SUCCESS_EMPTY",
        "slippage_allowance": 0.01,
        "required_min_net_capture": 0.12,
    }
    values.update(overrides)
    return evaluate_full_close_net_capture(**values)


def _plan() -> em.ExitPlan:
    plan = em.ExitPlan(
        coin="LINK",
        direction="long",
        engine_name="AMATERASU",
        entry_price=100.0,
        size_usdt=100.0,
        atr_val=1.0,
        hard_sl=98.0,
        tp1_price=101.0,
        tp2_price=102.0,
        leverage=10,
        entry_contracts=1.0,
        confirmed_remaining_contracts=1.0,
        entry_fee_usdt=0.05,
        funding_usdt=0.0,
        funding_status="SUCCESS_EMPTY",
    )
    return plan


def test_close_reason_classes_are_centralized_and_fail_closed():
    assert classify_full_close_reason("tp") == OPTIONAL_PROFIT_CAPTURE
    assert classify_full_close_reason("tp1_hit") == OPTIONAL_PROFIT_CAPTURE
    assert classify_full_close_reason("tp2_hit") == OPTIONAL_PROFIT_CAPTURE
    assert classify_full_close_reason("slot_rotation") == CAPITAL_MANAGEMENT
    assert classify_full_close_reason("stale_exit") == CAPITAL_MANAGEMENT
    assert classify_full_close_reason("tp_exchange") == ALREADY_EXCHANGE_CLOSED
    assert classify_full_close_reason("sl_exchange") == ALREADY_EXCHANGE_CLOSED
    assert classify_full_close_reason("unrecognized_reason") == MANDATORY_OR_PROTECTIVE


def test_link_style_exchange_sl_bypasses_fee_gate():
    result = _decision(reason="sl_exchange", gross_remaining_profit=-1.0)
    assert result.status == MANDATORY_EXIT_NO_FEE_GATE
    assert result.should_defer is False


def test_hard_sl_emergency_manual_and_trailing_bypass_fee_gate():
    for reason in ("hard_sl", "emergency", "manual", "trailing_stop_hit"):
        result = _decision(reason=reason, gross_remaining_profit=-0.01)
        assert result.status == MANDATORY_EXIT_NO_FEE_GATE
        assert result.should_defer is False


def test_optional_target_with_healthy_net_capture_closes():
    result = _decision()
    assert result.status == NET_CAPTURE_HEALTHY
    assert result.estimated_net_capture == 0.915
    assert result.should_defer is False


def test_optional_positive_gross_negative_net_defers():
    result = _decision(
        gross_remaining_profit=0.05,
        known_entry_fee=0.05,
        estimated_final_exit_fee=0.05,
        slippage_allowance=0.02,
    )
    assert result.status == MICRO_PROFIT_FEE_BLEED
    assert result.estimated_net_capture < 0.0
    assert result.should_defer is True


def test_optional_tiny_positive_below_floor_defers_and_floor_is_inclusive():
    below = _decision(gross_remaining_profit=0.20, required_min_net_capture=0.18)
    assert below.status == MICRO_PROFIT_FEE_BLEED
    assert below.should_defer is True
    at_floor = _decision(gross_remaining_profit=0.205, required_min_net_capture=0.12)
    assert at_floor.status == NET_CAPTURE_HEALTHY
    assert at_floor.estimated_net_capture == 0.12


def test_partial_fees_and_current_remainder_are_used_not_initial_notional():
    result = _decision(
        current_remaining_contracts=0.1,
        current_remaining_notional=10.0,
        gross_remaining_profit=0.30,
        already_realized_partial_gross=0.20,
        known_entry_fee=0.05,
        known_partial_exit_fees=0.08,
        estimated_final_exit_fee=0.0025,
        slippage_allowance=0.001,
    )
    assert result.status == NET_CAPTURE_HEALTHY
    assert result.estimated_net_capture == 0.3665


def test_funding_paid_received_and_unknown_are_distinguished():
    paid = _decision(known_funding=-0.03, funding_status="PAID")
    received = _decision(known_funding=0.03, funding_status="RECEIVED")
    unknown = _decision(known_funding=None, funding_status="QUERY_FAILED")
    assert paid.estimated_net_capture == 0.885
    assert received.estimated_net_capture == 0.945
    assert unknown.status == NET_CAPTURE_UNKNOWN
    assert "funding" in unknown.unknown_fields
    assert unknown.should_defer is False


def test_unknown_quantity_or_fee_evidence_is_not_converted_to_zero():
    result = _decision(
        current_remaining_contracts=None,
        current_remaining_notional=None,
        known_entry_fee=None,
        known_partial_exit_fees=None,
        estimated_final_exit_fee=None,
        slippage_allowance=None,
        funding_status="QUERY_FAILED",
        known_funding=None,
    )
    assert result.status == NET_CAPTURE_UNKNOWN
    assert result.should_defer is False
    assert set(result.unknown_fields) >= {
        "remaining_quantity", "entry_fee", "partial_exit_fees", "final_exit_fee",
        "funding", "slippage_allowance",
    }


def test_optional_close_is_deferred_but_protective_close_is_not(monkeypatch):
    executed = []

    async def fake_execute_exit(coin, price, reason, plan):
        executed.append(reason)
        return True

    async def fake_alert(**kwargs):
        return None

    monkeypatch.setattr(em, "_execute_exit", fake_execute_exit)
    monkeypatch.setattr(em, "alert_trade_closed", fake_alert)
    monkeypatch.setattr(em, "_notify_engine", lambda *args, **kwargs: None)
    monkeypatch.setattr(em, "remove", lambda coin: None)
    monkeypatch.setattr(em._s, "get_val", lambda key, default=None: {
        "min_full_close_profit_usdt": 0.12,
        "net_expectancy_default_slippage_pct": 0.03,
    }.get(key, default))

    plan = _plan()
    assert asyncio.run(em._do_close("LINK", 100.01, "tp", plan)) is False
    assert executed == []
    assert plan.full_close_economics["status"] == MICRO_PROFIT_FEE_BLEED

    assert asyncio.run(em._do_close("LINK", 98.0, "hard_sl", plan)) is True
    assert executed == ["hard_sl"]


def test_optional_decision_telemetry_is_deduplicated(monkeypatch):
    plan = _plan()
    persisted = []
    monkeypatch.setattr(em, "_persist_exit_state", lambda coin, current: persisted.append(coin))
    monkeypatch.setattr(em._s, "get_val", lambda key, default=None: {
        "min_full_close_profit_usdt": 0.12,
        "net_expectancy_default_slippage_pct": 0.03,
    }.get(key, default))
    first = em._optional_full_close_decision("LINK", 100.01, "tp", plan)
    second = em._optional_full_close_decision("LINK", 100.01, "tp", plan)
    assert first.status == second.status == MICRO_PROFIT_FEE_BLEED
    assert len(persisted) == 1
    assert plan.full_close_economics["reason"] == "tp"


def test_link_sized_trailing_candidate_is_cost_relative_not_dollar_gated():
    economics = build_position_lifecycle_economics(
        original_entry_contracts=0.4,
        current_remaining_contracts=0.4,
        current_remaining_notional=3.532,
        entry_price=8.831,
        side="long",
        current_price=8.896,
        candidate_stop_price=8.8738,
        known_entry_fee=0.0017662,
        known_partial_exit_fees=0.0,
        estimated_remaining_exit_fee=0.0017662,
        known_funding=-0.0006019822,
        funding_status="PAID",
        slippage_allowance=0.0010596,
    )
    assert economics.economic_evidence_status == ECONOMIC_EVIDENCE_COMPLETE
    assert economics.candidate_stop_estimated_net_pnl > 0.0
    assert economics.candidate_stop_estimated_net_pnl < 1.605
    assert candidate_stop_is_economically_eligible(economics, fee_multiple=1.0)


def test_cost_relative_floor_scales_from_current_costs_not_position_size():
    small = build_position_lifecycle_economics(
        original_entry_contracts=1.0, current_remaining_contracts=1.0,
        current_remaining_notional=3.0, entry_price=100.0, side="long",
        current_price=101.0, candidate_stop_price=100.8,
        known_entry_fee=0.0015, known_partial_exit_fees=0.0,
        estimated_remaining_exit_fee=0.0015, known_funding=0.0,
        funding_status="SUCCESS_EMPTY", slippage_allowance=0.001,
    )
    large = build_position_lifecycle_economics(
        original_entry_contracts=1.0, current_remaining_contracts=1.0,
        current_remaining_notional=300.0, entry_price=100.0, side="long",
        current_price=101.0, candidate_stop_price=100.8,
        known_entry_fee=0.15, known_partial_exit_fees=0.0,
        estimated_remaining_exit_fee=0.15, known_funding=0.0,
        funding_status="SUCCESS_EMPTY", slippage_allowance=0.1,
    )
    assert required_cost_relative_net(large, 1.0) > required_cost_relative_net(small, 1.0)


def test_unknown_current_contracts_are_explicit_even_when_notional_exists():
    economics = build_position_lifecycle_economics(
        original_entry_contracts=0.4,
        current_remaining_contracts=None,
        current_remaining_notional=3.532,
        entry_price=8.831,
        side="long",
        current_price=8.896,
        candidate_stop_price=8.8738,
        known_entry_fee=0.0017662,
        known_partial_exit_fees=0.0,
        estimated_remaining_exit_fee=0.0017662,
        known_funding=0.0,
        funding_status="SUCCESS_EMPTY",
        slippage_allowance=0.001,
    )
    assert economics.economic_evidence_status == ECONOMIC_EVIDENCE_UNKNOWN
    assert "remaining_contracts" in economics.unknown_fields
    assert candidate_stop_is_economically_eligible(economics, 1.0) is False


def test_optional_close_uses_dynamic_floor_and_ignores_historical_1_07_setting():
    economics = build_position_lifecycle_economics(
        original_entry_contracts=0.4, current_remaining_contracts=0.4,
        current_remaining_notional=3.532, entry_price=8.831, side="long",
        current_price=8.896, candidate_stop_price=None,
        known_entry_fee=0.0017662, known_partial_exit_fees=0.0,
        estimated_remaining_exit_fee=0.0017662, known_funding=-0.0006019822,
        funding_status="PAID", slippage_allowance=0.0010596,
    )
    decision = evaluate_optional_full_close(reason="tp", economics=economics, fee_multiple=1.0)
    assert decision.status == NET_CAPTURE_HEALTHY
    assert decision.required_min_net_capture < 0.12
    assert decision.required_min_net_capture < 1.07


def test_candidate_be_can_remain_deferred_when_stop_is_net_negative():
    economics = build_position_lifecycle_economics(
        original_entry_contracts=0.4, current_remaining_contracts=0.4,
        current_remaining_notional=3.532, entry_price=8.831, side="long",
        current_price=8.85, candidate_stop_price=8.833,
        known_entry_fee=0.0017662, known_partial_exit_fees=0.0,
        estimated_remaining_exit_fee=0.0017662, known_funding=-0.0006019822,
        funding_status="PAID", slippage_allowance=0.0010596,
    )
    assert economics.candidate_stop_estimated_net_pnl < 0.0
    assert candidate_stop_is_economically_eligible(economics, 1.0) is False


def test_repeated_optional_deferral_logs_only_on_material_signature_change(monkeypatch, caplog):
    plan = _plan()
    monkeypatch.setattr(em, "_persist_exit_state", lambda *args, **kwargs: None)
    monkeypatch.setattr(em._s, "get_val", lambda key, default=None: {
        "minimum_net_capture_fee_multiple": 1.0,
        "net_expectancy_default_slippage_pct": 0.03,
    }.get(key, default))
    caplog.set_level("INFO", logger="OxyPulse.ExitManager")
    assert asyncio.run(em._maybe_defer_optional_full_close("LINK", 100.01, "tp", plan)) is True
    first = caplog.text.count("DEFER_OPTIONAL")
    for _ in range(9):
        assert asyncio.run(em._maybe_defer_optional_full_close("LINK", 100.01, "tp", plan)) is True
    assert caplog.text.count("DEFER_OPTIONAL") == first


def test_link_sized_milestone_trailing_arms_from_candidate_stop_economics(monkeypatch):
    plan = em.ExitPlan(
        coin="LINK", direction="long", engine_name="AMATERASU", entry_price=8.831,
        size_usdt=3.532, atr_val=0.0, hard_sl=8.70, tp1_price=9.0, tp2_price=9.2,
        leverage=15, entry_contracts=0.4, confirmed_remaining_contracts=0.4,
        entry_fee_usdt=0.0017662, funding_usdt=-0.0006019822,
        funding_status="PAID", peak_price=8.896, tp_owner="NO_PROFIT_TP",
    )
    values = {
        "breakeven_enabled": False,
        "profit_lock_enabled": False,
        "tsl_enabled": True,
        "trailing_activation_pct": 6.0,
        "trailing_distance_pct": 3.75,
        "trailing_after_tp1_pct": 3.75,
        "trailing_after_tp2_pct": 3.75,
        "trail_distance_mode": "percent",
        "trail_distance_atr_mult": 2.5,
        "minimum_net_capture_fee_multiple": 1.0,
        "net_expectancy_default_slippage_pct": 0.03,
        "partial_exit_enabled": False,
        "stale_trade_exit_enabled": False,
    }
    monkeypatch.setattr(em._s, "get_val", lambda key, default=None: values.get(key, default))
    monkeypatch.setattr(em, "_maybe_positive_mfe_profit_guard", lambda *args, **kwargs: False)
    monkeypatch.setattr(em, "_maybe_positive_profit_lock_enrichment", lambda *args, **kwargs: False)
    monkeypatch.setattr(em, "_continuation_active", lambda *args, **kwargs: (False, 0.0, ""))
    async def _false_async(*args, **kwargs):
        return False

    monkeypatch.setattr(em, "_maybe_stale_exit", _false_async)
    monkeypatch.setattr(em, "_maybe_early_invalidation_exit", _false_async)

    import asyncio
    asyncio.run(em._apply_milestone_profit_system("LINK", plan, 8.896, True))
    assert plan.tsl_armed is True
    assert plan.hard_sl > 8.70
    assert plan.full_close_economics == {}


def test_historical_absolute_setting_is_not_read_by_protection_or_optional_gate():
    source = __import__("pathlib").Path("exit_manager.py").read_text(encoding="utf-8-sig")
    assert 'get_val("min_full_close_profit_usdt"' not in source


def test_fee_multiple_one_x_is_cost_relative_across_notional_and_leverage():
    for notional in (1.0, 3.5, 5.0, 10.0, 50.0, 300.0):
        economics = build_position_lifecycle_economics(
            original_entry_contracts=1.0,
            current_remaining_contracts=1.0,
            current_remaining_notional=notional,
            entry_price=100.0,
            side="long",
            current_price=101.0,
            candidate_stop_price=100.8,
            known_entry_fee=notional * 0.0005,
            known_partial_exit_fees=0.0,
            estimated_remaining_exit_fee=notional * 0.0005,
            known_funding=0.0,
            funding_status="SUCCESS_EMPTY",
            slippage_allowance=notional * 0.0001,
        )
        assert required_cost_relative_net(economics, 1.0) == pytest.approx(notional * 0.0011)
        for _leverage in (10, 15, 25):
            assert required_cost_relative_net(economics, 1.0) > 0.0


def test_funding_unavailable_can_use_explicit_conservative_estimate():
    economics = build_position_lifecycle_economics(
        original_entry_contracts=0.4,
        current_remaining_contracts=0.4,
        current_remaining_notional=3.532,
        entry_price=8.831,
        side="long",
        current_price=8.896,
        candidate_stop_price=8.8738,
        known_entry_fee=0.0017662,
        known_partial_exit_fees=0.0,
        estimated_remaining_exit_fee=0.0017662,
        known_funding=-0.002,
        funding_status="ESTIMATED_CONSERVATIVE",
        funding_evidence=ECONOMIC_EVIDENCE_ESTIMATED_CONSERVATIVE,
        slippage_allowance=0.0010596,
    )
    assert economics.economic_evidence_status == ECONOMIC_EVIDENCE_ESTIMATED_CONSERVATIVE
    assert economics.evidence_class == "COST_COMPONENT_UNKNOWN"
    assert candidate_stop_is_economically_eligible(economics, 1.0)


def test_unknown_contracts_block_then_authoritative_recovery_allows_evaluation():
    common = dict(
        original_entry_contracts=0.4,
        current_remaining_notional=3.532,
        entry_price=8.831,
        side="long",
        current_price=8.896,
        candidate_stop_price=8.8738,
        known_entry_fee=0.0017662,
        known_partial_exit_fees=0.0,
        estimated_remaining_exit_fee=0.0017662,
        known_funding=-0.0006019822,
        funding_status="PAID",
        slippage_allowance=0.0010596,
    )
    missing = build_position_lifecycle_economics(current_remaining_contracts=None, **common)
    assert missing.economic_evidence_status == ECONOMIC_EVIDENCE_INSUFFICIENT
    assert missing.evidence_class == "CRITICAL_EXPOSURE_UNKNOWN"
    assert not candidate_stop_is_economically_eligible(missing, 1.0)

    recovered = build_position_lifecycle_economics(current_remaining_contracts=0.4, **common)
    assert recovered.economic_evidence_status == ECONOMIC_EVIDENCE_COMPLETE
    assert candidate_stop_is_economically_eligible(recovered, 1.0)


def test_live_zero_contract_snapshot_never_falls_back_to_requested_quantity(monkeypatch):
    plan = _plan()
    plan.confirmed_remaining_contracts = 0.0
    monkeypatch.setattr(em, "_paper_trading_enabled", lambda: False)
    economics = em._lifecycle_economics(plan, current_price=100.1, candidate_stop_price=100.05)
    assert economics.current_remaining_contracts is None
    assert economics.economic_evidence_status == ECONOMIC_EVIDENCE_INSUFFICIENT
    assert economics.evidence_class == "CRITICAL_EXPOSURE_UNKNOWN"
    assert candidate_stop_is_economically_eligible(economics, 1.0) is False


def test_minimum_net_capture_setting_is_real_hot_setting_and_telegram_room_has_custom():
    import settings_store as ss
    import tg_panels as tp

    old = ss.get_val("minimum_net_capture_fee_multiple", 1.0)
    try:
        ss.set_val("minimum_net_capture_fee_multiple", 25.0)
        assert ss.get_val("minimum_net_capture_fee_multiple") == 25.0
        ss.set_val("minimum_net_capture_fee_multiple", -1.0)
        assert ss.get_val("minimum_net_capture_fee_multiple") == 25.0
        callbacks = [
            button.callback_data
            for row in tp.kb_fg_profit_lock(ss).inline_keyboard
            for button in row
            if getattr(button, "callback_data", None)
        ]
        assert "set:room:minimum_net_capture_fee_multiple" in callbacks
        room_callbacks = [
            button.callback_data
            for row in tp.kb_room_number(
                cb_dn="room:num:minimum_net_capture_fee_multiple:dn",
                cb_up="room:num:minimum_net_capture_fee_multiple:up",
                cb_custom="room:num:minimum_net_capture_fee_multiple:custom",
                back_feature="set:fg:profit_lock",
                back_category="set:cat:execution",
            ).inline_keyboard
            for button in row
            if getattr(button, "callback_data", None)
        ]
        assert "room:num:minimum_net_capture_fee_multiple:custom" in room_callbacks
    finally:
        ss.set_val("minimum_net_capture_fee_multiple", old)

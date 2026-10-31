from __future__ import annotations

import asyncio
import copy
import logging
from pathlib import Path

import exit_manager as em
import settings_store as ss


def _plan() -> em.ExitPlan:
    plan = em.ExitPlan(
        coin="SUI",
        direction="long",
        engine_name="ENMA",
        entry_price=1.0,
        size_usdt=10.0,
        atr_val=0.01,
        hard_sl=0.95,
        tp1_price=1.05,
        tp2_price=1.2,
        leverage=5,
        order_id="ord-1",
        created_at=1000.0,
        entry_contracts=1.0,
        confirmed_remaining_contracts=1.0,
        entry_fee_usdt=0.0,
        funding_usdt=0.0,
        funding_status="SUCCESS_EMPTY",
    )
    plan.mfe_roi = 4.2
    plan.mae_roi = -1.1
    return plan


def _metadata():
    return {
        "confidence_stage_trace": {
            "novo": {
                "quality_score": 91,
                "quality_tier": "ELITE",
            },
            "novo_enrichment": {
                "status": "OK",
                "band": "ELITE",
                "suggestions": {
                    "tp2_multiplier": 1.12,
                    "trailing_activation_multiplier": 1.10,
                    "break_even_delay_multiplier": 1.10,
                    "profit_lock_multiplier": 1.08,
                    "partial_min_notional_multiplier": 1.20,
                    "execution_split_preference": "LIMIT_PREFERRED",
                    "size_multiplier": 1.12,
                },
            },
        }
    }


def _candidate_memory():
    return {
        "record_type": "novo_candidate_observation",
        "symbol": "SUI",
        "side": "LONG",
        "engine": "ENMA",
        "quality_score": 77,
        "quality_tier": "GOOD",
        "novo_modules": {"market_dna": {"status": "OK", "score": 75, "bias": "LONG"}},
        "novo_enrichment": {"band": "GOOD", "suggestions": {"partial_min_notional_multiplier": 1.05}},
    }


def test_default_setting_exists_and_is_false():
    assert ss.DEFAULTS["novo_enrichment_outcome_telemetry_enabled"] is False


def test_telemetry_skipped_when_flag_disabled(monkeypatch):
    calls = []
    monkeypatch.setattr(em._s, "get_val", lambda key, default=None: False if key == "novo_enrichment_outcome_telemetry_enabled" else default)
    monkeypatch.setattr("novo_trade_memory.append_memory", lambda record: calls.append(record))

    em._maybe_record_novo_enrichment_outcome("SUI", 1.1, 0.5, "tp_exchange", _plan(), fee_usdt=0.02)

    assert calls == []


def test_telemetry_appends_once_when_enabled_and_close_data_exists(monkeypatch):
    calls = []
    em._novo_enrichment_outcome_seen.clear()
    monkeypatch.setattr(em._s, "get_val", lambda key, default=None: True if key == "novo_enrichment_outcome_telemetry_enabled" else default)
    monkeypatch.setattr(em, "_registry_row_for_plan", lambda coin: _metadata())
    monkeypatch.setattr("novo_trade_memory.append_memory", lambda record: calls.append(record))

    plan = _plan()
    em._maybe_record_novo_enrichment_outcome("SUI", 1.1, 0.5, "tp_exchange", plan, fee_usdt=0.02, via_exchange=True)
    em._maybe_record_novo_enrichment_outcome("SUI", 1.1, 0.5, "tp_exchange", plan, fee_usdt=0.02, via_exchange=True)

    assert len(calls) == 1
    record = calls[0]
    assert record["record_type"] == "novo_enrichment_outcome"
    assert record["dry_run"] is True
    assert record["symbol"] == "SUI"
    assert record["side"] == "LONG"
    assert record["engine"] == "ENMA"
    assert record["order_id"] == "ord-1"
    assert record["quality_score"] == 91
    assert record["quality_tier"] == "ELITE"
    assert record["enrichment_band"] == "ELITE"
    assert record["novo_modules"] == {}
    assert record["position_id"] == "ord-1"
    assert "trace_id" in record
    assert record["suggestions"]["tp2_multiplier"] == 1.12
    assert record["actual_pnl"] == 0.5
    assert record["actual_fee"] == 0.02
    assert record["net_pnl"] == 0.48
    assert record["mfe"] == 4.2
    assert record["mae"] == -1.1
    assert record["exit_reason"] == "tp_exchange"
    assert record["would_have_helped"] == "UNKNOWN"


def test_missing_novo_metadata_records_collecting_defaults(monkeypatch):
    calls = []
    em._novo_enrichment_outcome_seen.clear()
    monkeypatch.setattr(em._s, "get_val", lambda key, default=None: True if key == "novo_enrichment_outcome_telemetry_enabled" else default)
    monkeypatch.setattr(em, "_registry_row_for_plan", lambda coin: {})
    monkeypatch.setattr(em, "_latest_novo_candidate_for_plan", lambda coin, plan: {})
    monkeypatch.setattr("novo_trade_memory.append_memory", lambda record: calls.append(record))

    em._maybe_record_novo_enrichment_outcome("SUI", 1.1, 0.5, "sl", _plan(), fee_usdt=0.02)

    assert calls[0]["quality_tier"] == "COLLECTING"
    assert calls[0]["enrichment_band"] == "COLLECTING"
    assert calls[0]["suggestions"]["execution_split_preference"] == "UNCHANGED"


def test_missing_registry_metadata_falls_back_to_recent_candidate_memory(monkeypatch):
    calls = []
    em._novo_enrichment_outcome_seen.clear()
    monkeypatch.setattr(em._s, "get_val", lambda key, default=None: True if key == "novo_enrichment_outcome_telemetry_enabled" else default)
    monkeypatch.setattr(em, "_registry_row_for_plan", lambda coin: {})
    monkeypatch.setattr(em, "_latest_novo_candidate_for_plan", lambda coin, plan: _candidate_memory())
    monkeypatch.setattr("novo_trade_memory.append_memory", lambda record: calls.append(record))

    em._maybe_record_novo_enrichment_outcome("SUI", 1.1, 0.5, "tp_exchange", _plan(), fee_usdt=0.02)

    assert calls[0]["quality_score"] == 77
    assert calls[0]["quality_tier"] == "GOOD"
    assert calls[0]["enrichment_band"] == "GOOD"
    assert calls[0]["novo_modules"]["market_dna"]["status"] == "OK"


def test_malformed_enrichment_metadata_does_not_crash(monkeypatch):
    calls = []
    em._novo_enrichment_outcome_seen.clear()
    monkeypatch.setattr(em._s, "get_val", lambda key, default=None: True if key == "novo_enrichment_outcome_telemetry_enabled" else default)
    monkeypatch.setattr(em, "_registry_row_for_plan", lambda coin: {"confidence_stage_trace": {"novo": "bad", "novo_enrichment": "bad"}})
    monkeypatch.setattr(em, "_latest_novo_candidate_for_plan", lambda coin, plan: {})
    monkeypatch.setattr("novo_trade_memory.append_memory", lambda record: calls.append(record))

    em._maybe_record_novo_enrichment_outcome("SUI", 1.1, 0.5, "sl", _plan(), fee_usdt=0.02)

    assert calls[0]["quality_tier"] == "COLLECTING"
    assert calls[0]["enrichment_band"] == "COLLECTING"


def test_append_failure_does_not_interrupt_close_flow(monkeypatch):
    em._novo_enrichment_outcome_seen.clear()
    monkeypatch.setattr(em._s, "get_val", lambda key, default=None: True if key == "novo_enrichment_outcome_telemetry_enabled" else default)
    monkeypatch.setattr(em, "_registry_row_for_plan", lambda coin: _metadata())

    def fail(_record):
        raise RuntimeError("append failed")

    monkeypatch.setattr("novo_trade_memory.append_memory", fail)

    em._maybe_record_novo_enrichment_outcome("SUI", 1.1, 0.5, "sl", _plan(), fee_usdt=0.02)


def test_performance_tracker_record_trade_arguments_remain_unchanged(monkeypatch):
    calls = []
    monkeypatch.setattr(em._s, "get_val", lambda key, default=None: False if key == "novo_enrichment_outcome_telemetry_enabled" else default)
    monkeypatch.setattr(em._perf, "record_trade", lambda *args, **kwargs: calls.append((args, kwargs)))
    monkeypatch.setattr(em, "fee_cost_usdt", lambda size, lev: 0.03)

    plan = _plan()
    plan.engine_name = "UNKNOWN"
    em._notify_engine("SUI", 1.1, 0.5, "tp_exchange", plan, via_exchange=True)

    assert len(calls) == 1
    args, kwargs = calls[0]
    assert args == ("SUI", "long", 1.0, 1.1, 10.0, 0.5, "tp_exchange")
    assert kwargs == {"fee_usdt": 0.03, "opened_at": em._opened_at_iso(plan)}


def test_no_tp_sl_sizing_admission_execution_fields_are_modified(monkeypatch):
    em._novo_enrichment_outcome_seen.clear()
    monkeypatch.setattr(em._s, "get_val", lambda key, default=None: True if key == "novo_enrichment_outcome_telemetry_enabled" else default)
    monkeypatch.setattr(em, "_registry_row_for_plan", lambda coin: _metadata())
    monkeypatch.setattr("novo_trade_memory.append_memory", lambda record: {"status": "OK"})
    plan = _plan()
    before = copy.deepcopy(plan.__dict__)

    em._maybe_record_novo_enrichment_outcome("SUI", 1.1, 0.5, "tp_exchange", plan, fee_usdt=0.02)

    assert plan.__dict__ == before


def test_fee_bleed_guard_default_false_allows_partial(monkeypatch):
    monkeypatch.setattr(em._s, "get_val", lambda key, default=None: False if key == "novo_fee_bleed_guard_enabled" else default)

    allowed, meta = em._novo_fee_bleed_guard_allows_partial("SUI", _plan(), stage="tp1", close_pct=25.0, price=1.1)

    assert allowed is True
    assert meta["enabled"] is False


def test_fee_bleed_guard_defers_uneconomic_partial(monkeypatch):
    def fake_get(key, default=None):
        values = {
            "novo_fee_bleed_guard_enabled": True,
            "novo_fee_bleed_min_partial_notional_usdt": 0.0,
            "novo_fee_bleed_min_notional_fee_mult": 8.0,
            "novo_fee_bleed_max_fee_share": 0.55,
            "fee_bleed_guard_min_net_profit_usdt": 0.10,
            "fee_bleed_guard_max_fee_share": 0.55,
            "fee_bleed_guard_min_partial_notional_usdt": 0.0,
            "partial_skip_log_cooldown_sec": 0.0,
        }
        return values.get(key, default)

    monkeypatch.setattr(em._s, "get_val", fake_get)
    plan = _plan()

    allowed, meta = em._novo_fee_bleed_guard_allows_partial("SUI", plan, stage="tp1", close_pct=25.0, price=1.001)

    assert allowed is False
    assert meta["enabled"] is True
    assert meta["net_pnl"] <= 0.0 or meta["fee_share"] > meta["max_fee_share"] or meta["net_pnl"] < meta["min_net_profit"]
    assert plan.tp1_done is False


def test_fee_bleed_guard_allows_economic_partial(monkeypatch):
    def fake_get(key, default=None):
        values = {
            "novo_fee_bleed_guard_enabled": True,
            "novo_fee_bleed_min_partial_notional_usdt": 0.0,
            "novo_fee_bleed_min_notional_fee_mult": 1.0,
            "novo_fee_bleed_max_fee_share": 0.55,
            "fee_bleed_guard_min_net_profit_usdt": 0.10,
            "fee_bleed_guard_max_fee_share": 0.55,
            "fee_bleed_guard_min_partial_notional_usdt": 0.0,
        }
        return values.get(key, default)

    monkeypatch.setattr(em._s, "get_val", fake_get)

    allowed, meta = em._novo_fee_bleed_guard_allows_partial("SUI", _plan(), stage="tp1", close_pct=25.0, price=1.2)

    assert allowed is True
    assert meta["net_pnl"] > 0.0


def test_fee_bleed_guard_evaluation_markers_are_emitted(monkeypatch, caplog):
    def fake_get(key, default=None):
        values = {
            "novo_fee_bleed_guard_enabled": True,
            "novo_fee_bleed_min_notional_fee_mult": 1.0,
            "fee_bleed_guard_min_net_profit_usdt": 0.10,
            "fee_bleed_guard_max_fee_share": 0.55,
            "fee_bleed_guard_min_partial_notional_usdt": 0.0,
            "fee_bleed_guard_trace_all_small_exits": True,
            "partial_skip_log_cooldown_sec": 0.0,
        }
        return values.get(key, default)

    monkeypatch.setattr(em._s, "get_val", fake_get)
    caplog.set_level(logging.INFO, logger="OxyPulse.ExitManager")

    allowed, meta = em._novo_fee_bleed_guard_allows_partial("SUI", _plan(), stage="tp1", close_pct=25.0, price=1.001)

    assert allowed is False
    assert meta["deferred"] is True
    text = caplog.text
    assert "FEE_BLEED_GUARD_EVALUATED" in text
    assert "NOVO_FEE_BLEED_GUARD_DEFERRED_PARTIAL" in text


def test_fee_bleed_guard_allowed_small_exit_marker(monkeypatch, caplog):
    def fake_get(key, default=None):
        values = {
            "novo_fee_bleed_guard_enabled": True,
            "novo_fee_bleed_min_notional_fee_mult": 1.0,
            "fee_bleed_guard_min_net_profit_usdt": 0.01,
            "fee_bleed_guard_max_fee_share": 0.55,
            "fee_bleed_guard_min_partial_notional_usdt": 0.0,
            "fee_bleed_guard_trace_all_small_exits": True,
        }
        return values.get(key, default)

    monkeypatch.setattr(em._s, "get_val", fake_get)
    caplog.set_level(logging.INFO, logger="OxyPulse.ExitManager")

    allowed, meta = em._novo_fee_bleed_guard_allows_partial("SUI", _plan(), stage="tp1", close_pct=25.0, price=1.03)

    assert allowed is True
    assert meta["net_pnl"] >= meta["min_net_profit"]
    assert "FEE_BLEED_GUARD_EVALUATED" in caplog.text
    assert "FEE_BLEED_GUARD_ALLOWED_SMALL_EXIT" in caplog.text


def test_fee_bleed_guard_does_not_touch_full_close_helpers():
    source = Path("exit_manager.py").read_text(encoding="utf-8-sig")
    do_close_start = source.index("async def _do_close")
    do_close_end = source.index("\ndef _notify_engine", do_close_start)
    execute_exit_start = source.index("async def _execute_exit")
    execute_exit_end = source.index("\nasync def _execute_partial", execute_exit_start)

    assert "novo_fee_bleed_guard" not in source[do_close_start:do_close_end]
    assert "novo_fee_bleed_guard" not in source[execute_exit_start:execute_exit_end]


def test_fee_bleed_guard_enabled_does_not_block_sl_or_manual_full_close(monkeypatch):
    async def fake_execute_exit(coin, price, reason, plan):
        calls.append((coin, price, reason, plan.size_usdt))
        return True

    async def fake_alert_trade_closed(**kwargs):
        alerts.append(kwargs)

    calls = []
    alerts = []
    monkeypatch.setattr(em._s, "get_val", lambda key, default=None: True if key == "novo_fee_bleed_guard_enabled" else default)
    monkeypatch.setattr(em, "_execute_exit", fake_execute_exit)
    monkeypatch.setattr(em, "alert_trade_closed", fake_alert_trade_closed)
    monkeypatch.setattr(em, "_notify_engine", lambda *args, **kwargs: None)
    monkeypatch.setattr(em, "remove", lambda coin: None)

    sl_plan = _plan()
    manual_plan = _plan()
    manual_plan.order_id = "ord-2"

    assert asyncio.run(em._do_close("SUI", 0.95, "sl", sl_plan)) is True
    assert asyncio.run(em._do_close("SUI", 1.01, "manual", manual_plan)) is True

    assert calls[0][2] in {"sl", "sl_exchange"}
    assert calls[1][2] == "manual"
    assert len(alerts) == 2


def _positive_mfe_settings(enabled=True):
    values = {
        "positive_mfe_profit_guard_enabled": enabled,
        "positive_mfe_guard_mfe_roi_trigger": 3.0,
        "positive_mfe_guard_min_profit_usdt": 0.10,
        "positive_mfe_guard_fee_mult": 2.0,
        "positive_mfe_guard_lock_roi_pct": 0.35,
        "positive_mfe_guard_min_age_sec": 30.0,
        "positive_mfe_guard_max_lock_levels": 1,
        "net_expectancy_default_slippage_pct": 0.0,
    }
    return lambda key, default=None: values.get(key, default)


def _profit_lock_enrichment_settings(enabled=True):
    values = {
        "positive_profit_lock_enrichment_enabled": enabled,
        "profit_lock_enrichment_mfe_trigger_roi": 25.0,
        "profit_lock_enrichment_lock_pct_1": 0.20,
        "profit_lock_enrichment_lock_pct_2": 0.35,
        "profit_lock_enrichment_lock_pct_3": 0.50,
        "profit_lock_enrichment_max_level": 3,
        "min_full_close_profit_usdt": 0.05,
    }
    return lambda key, default=None: values.get(key, default)


def test_positive_mfe_profit_guard_default_false_does_nothing(monkeypatch):
    monkeypatch.setattr(em._s, "get_val", _positive_mfe_settings(False))
    monkeypatch.setattr(em, "fee_cost_usdt", lambda size, lev: 0.02)
    plan = _plan()
    before = copy.deepcopy(plan.__dict__)

    applied = em._maybe_positive_mfe_profit_guard("SUI", plan, 1.05, True)

    assert applied is False
    assert plan.__dict__ == before


def test_positive_mfe_profit_guard_applies_fee_safe_forward_stop(monkeypatch, caplog):
    monkeypatch.setattr(em._s, "get_val", _positive_mfe_settings(True))
    monkeypatch.setattr(em, "fee_cost_usdt", lambda size, lev: 0.001)
    caplog.set_level(logging.INFO, logger="OxyPulse.ExitManager")
    plan = _plan()
    plan.created_at = 1.0
    old_sl = plan.hard_sl

    applied = em._maybe_positive_mfe_profit_guard("SUI", plan, 1.05, True)

    assert applied is True
    assert plan.hard_sl > old_sl
    assert plan.hard_sl > plan.entry_price
    assert plan.hard_sl < 1.05
    assert plan.profit_lock_level == 1
    assert plan.last_protection_reason == "positive_mfe_profit_guard"
    assert "POSITIVE_MFE_PROFIT_GUARD_LOCKED" in caplog.text


def test_positive_mfe_profit_guard_does_not_trigger_below_fee_safe_profit(monkeypatch):
    def fake_get(key, default=None):
        values = {
            "positive_mfe_profit_guard_enabled": True,
            "positive_mfe_guard_mfe_roi_trigger": 3.0,
            "positive_mfe_guard_min_profit_usdt": 0.10,
            "positive_mfe_guard_fee_mult": 20.0,
            "positive_mfe_guard_lock_roi_pct": 0.35,
            "positive_mfe_guard_min_age_sec": 30.0,
            "positive_mfe_guard_max_lock_levels": 1,
        }
        return values.get(key, default)

    monkeypatch.setattr(em._s, "get_val", fake_get)
    monkeypatch.setattr(em, "fee_cost_usdt", lambda size, lev: 0.05)
    plan = _plan()
    old_sl = plan.hard_sl

    applied = em._maybe_positive_mfe_profit_guard("SUI", plan, 1.02, True)

    assert applied is False
    assert plan.hard_sl == old_sl
    assert plan.profit_lock_level == 0


def test_positive_mfe_profit_guard_does_not_trigger_before_min_age(monkeypatch):
    monkeypatch.setattr(em._s, "get_val", _positive_mfe_settings(True))
    monkeypatch.setattr(em, "fee_cost_usdt", lambda size, lev: 0.02)
    monkeypatch.setattr(em.time, "time", lambda: 1020.0)
    plan = _plan()
    plan.created_at = 1000.0

    applied = em._maybe_positive_mfe_profit_guard("SUI", plan, 1.05, True)

    assert applied is False
    assert plan.profit_lock_level == 0


def test_positive_mfe_profit_guard_does_not_loosen_existing_better_stop(monkeypatch):
    monkeypatch.setattr(em._s, "get_val", _positive_mfe_settings(True))
    monkeypatch.setattr(em, "fee_cost_usdt", lambda size, lev: 0.02)
    plan = _plan()
    plan.hard_sl = 1.04
    old_sl = plan.hard_sl

    applied = em._maybe_positive_mfe_profit_guard("SUI", plan, 1.05, True)

    assert applied is False
    assert plan.hard_sl == old_sl
    assert plan.profit_lock_level == 0


def test_positive_mfe_profit_guard_sets_lock_only_after_stop_improvement_succeeds(monkeypatch):
    monkeypatch.setattr(em._s, "get_val", _positive_mfe_settings(True))
    monkeypatch.setattr(em, "fee_cost_usdt", lambda size, lev: 0.02)
    monkeypatch.setattr(em, "_apply_stop_improvement", lambda *args, **kwargs: None)
    plan = _plan()

    applied = em._maybe_positive_mfe_profit_guard("SUI", plan, 1.05, True)

    assert applied is False
    assert plan.profit_lock_level == 0
    assert plan.last_protection_reason == ""


def test_early_invalidation_is_skipped_after_positive_mfe_guard_locks(monkeypatch):
    monkeypatch.setattr(em._s, "get_val", _positive_mfe_settings(True))
    monkeypatch.setattr(em, "fee_cost_usdt", lambda size, lev: 0.001)
    plan = _plan()
    assert em._maybe_positive_mfe_profit_guard("SUI", plan, 1.05, True) is True

    closed = asyncio.run(em._maybe_early_invalidation_exit("SUI", plan, 0.98, True))

    assert closed is False
    assert plan.early_invalidation_breach_time == 0.0


def test_profit_lock_enrichment_default_false_does_nothing(monkeypatch):
    monkeypatch.setattr(em._s, "get_val", _profit_lock_enrichment_settings(False))
    monkeypatch.setattr(em, "fee_cost_usdt", lambda size, lev: 0.02)
    plan = _plan()
    plan.mfe_roi = 50.0
    before = copy.deepcopy(plan.__dict__)

    applied = em._maybe_positive_profit_lock_enrichment("SUI", plan, 1.12, True)

    assert applied is False
    assert plan.__dict__ == before


def test_profit_lock_enrichment_l1_improves_stop(monkeypatch, caplog):
    monkeypatch.setattr(em._s, "get_val", _profit_lock_enrichment_settings(True))
    monkeypatch.setattr(em, "fee_cost_usdt", lambda size, lev: 0.02)
    caplog.set_level(logging.INFO, logger="OxyPulse.ExitManager")
    plan = _plan()
    plan.mfe_roi = 25.0
    old_sl = plan.hard_sl

    applied = em._maybe_positive_profit_lock_enrichment("SUI", plan, 1.06, True)

    assert applied is True
    assert plan.hard_sl > old_sl
    assert plan.hard_sl > plan.entry_price
    assert plan.hard_sl < 1.06
    assert plan.profit_lock_level == 1
    assert plan.last_protection_reason == "profit_lock_enrichment_l1"
    assert "PROFIT_LOCK_ENRICHMENT_L1" in caplog.text


def test_profit_lock_enrichment_l2_improves_stop(monkeypatch, caplog):
    monkeypatch.setattr(em._s, "get_val", _profit_lock_enrichment_settings(True))
    monkeypatch.setattr(em, "fee_cost_usdt", lambda size, lev: 0.02)
    caplog.set_level(logging.INFO, logger="OxyPulse.ExitManager")
    plan = _plan()
    plan.mfe_roi = 50.0
    plan.profit_lock_level = 1
    plan.hard_sl = 1.01

    applied = em._maybe_positive_profit_lock_enrichment("SUI", plan, 1.12, True)

    assert applied is True
    assert plan.hard_sl > 1.01
    assert plan.hard_sl < 1.12
    assert plan.profit_lock_level == 2
    assert plan.last_protection_reason == "profit_lock_enrichment_l2"
    assert "PROFIT_LOCK_ENRICHMENT_L2" in caplog.text


def test_profit_lock_enrichment_l3_improves_stop(monkeypatch, caplog):
    monkeypatch.setattr(em._s, "get_val", _profit_lock_enrichment_settings(True))
    monkeypatch.setattr(em, "fee_cost_usdt", lambda size, lev: 0.02)
    caplog.set_level(logging.INFO, logger="OxyPulse.ExitManager")
    plan = _plan()
    plan.mfe_roi = 100.0
    plan.profit_lock_level = 2
    plan.hard_sl = 1.04

    applied = em._maybe_positive_profit_lock_enrichment("SUI", plan, 1.25, True)

    assert applied is True
    assert plan.hard_sl > 1.04
    assert plan.hard_sl < 1.25
    assert plan.profit_lock_level == 3
    assert plan.last_protection_reason == "profit_lock_enrichment_l3"
    assert "PROFIT_LOCK_ENRICHMENT_L3" in caplog.text


def test_profit_lock_enrichment_never_loosens_stop(monkeypatch):
    monkeypatch.setattr(em._s, "get_val", _profit_lock_enrichment_settings(True))
    monkeypatch.setattr(em, "fee_cost_usdt", lambda size, lev: 0.02)
    plan = _plan()
    plan.mfe_roi = 50.0
    plan.profit_lock_level = 1
    plan.hard_sl = 1.08

    applied = em._maybe_positive_profit_lock_enrichment("SUI", plan, 1.12, True)

    assert applied is False
    assert plan.hard_sl == 1.08
    assert plan.profit_lock_level == 1


def test_profit_lock_enrichment_sets_level_only_after_stop_improvement(monkeypatch):
    monkeypatch.setattr(em._s, "get_val", _profit_lock_enrichment_settings(True))
    monkeypatch.setattr(em, "fee_cost_usdt", lambda size, lev: 0.02)
    monkeypatch.setattr(em, "_apply_stop_improvement", lambda *args, **kwargs: None)
    plan = _plan()
    plan.mfe_roi = 50.0
    plan.profit_lock_level = 1

    applied = em._maybe_positive_profit_lock_enrichment("SUI", plan, 1.12, True)

    assert applied is False
    assert plan.profit_lock_level == 1
    assert plan.last_protection_reason == ""


def test_profit_lock_enrichment_not_in_full_close_or_entry_paths():
    source = Path("exit_manager.py").read_text(encoding="utf-8-sig")
    do_close_start = source.index("async def _do_close")
    do_close_end = source.index("\ndef _notify_engine", do_close_start)
    execute_exit_start = source.index("async def _execute_exit")
    execute_exit_end = source.index("\nasync def _execute_partial", execute_exit_start)

    assert "positive_profit_lock_enrichment" not in source[do_close_start:do_close_end]
    assert "positive_profit_lock_enrichment" not in source[execute_exit_start:execute_exit_end]


def test_protected_data_files_are_not_written_when_append_is_monkeypatched(monkeypatch, tmp_path):
    protected = [Path("data/performance.json"), Path("data/position_registry.json"), Path("data/settings.json")]
    before = {p: (p.read_bytes() if p.exists() else None) for p in protected}
    em._novo_enrichment_outcome_seen.clear()
    monkeypatch.setattr(em._s, "get_val", lambda key, default=None: True if key == "novo_enrichment_outcome_telemetry_enabled" else default)
    monkeypatch.setattr(em, "_registry_row_for_plan", lambda coin: _metadata())
    monkeypatch.setattr("novo_trade_memory.append_memory", lambda record: {"status": "OK"})

    em._maybe_record_novo_enrichment_outcome("SUI", 1.1, 0.5, "tp_exchange", _plan(), fee_usdt=0.02)

    after = {p: (p.read_bytes() if p.exists() else None) for p in protected}
    assert after == before


def test_no_forbidden_execution_risk_sizing_order_imports_added_for_telemetry():
    source = Path("exit_manager.py").read_text(encoding="utf-8-sig")
    start = source.index("def _maybe_record_novo_enrichment_outcome")
    end = source.index("\ndef _normalize_ops_mode", start)
    block = source[start:end]
    forbidden = (
        "execution_router",
        "execution_admission",
        "sizing_engine",
        "okx_client",
        "place_market_order",
        "create_order",
        "order_payload",
        "risk_ceiling",
    )
    for token in forbidden:
        assert token not in block

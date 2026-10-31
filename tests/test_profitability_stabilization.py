import math


class _Settings:
    def __init__(self, values: dict):
        self._v = dict(values)

    def get_val(self, key: str, default=None):
        return self._v.get(key, default)


def test_risk_ceiling_prefers_resize():
    from risk_ceiling import apply_realized_risk_ceiling

    s = _Settings(
        {
            "realized_risk_ceiling_enabled": True,
            "realized_risk_cap_pct_balance": 1.0,
            "realized_risk_cap_min_usdt": 1.0,
            "realized_risk_cap_max_usdt": 3.0,
            "realized_risk_ceiling_tighten_sl_if_needed": True,
            "min_sl_dist_pct": 1.0,
        }
    )
    # 3% SL distance on $100 size -> $3 expected, cap is $2 (1% of $200).
    res = apply_realized_risk_ceiling(
        entry_px=100.0,
        stop_px=97.0,
        size_usdt=100.0,
        balance_usdt=200.0,
        min_exec_usdt=25.0,
        side="buy",
        settings=s,
    )
    assert res.meta["risk_ceiling_action"] == "size_reduced"
    assert res.size_usdt < 100.0
    assert math.isclose(res.stop_loss, 97.0)


def test_risk_ceiling_tightens_sl_when_min_exec_binds():
    from risk_ceiling import apply_realized_risk_ceiling

    s = _Settings(
        {
            "realized_risk_ceiling_enabled": True,
            "realized_risk_cap_pct_balance": 1.0,
            "realized_risk_cap_min_usdt": 1.0,
            "realized_risk_cap_max_usdt": 3.0,
            "realized_risk_ceiling_tighten_sl_if_needed": True,
            "min_sl_dist_pct": 1.0,
        }
    )
    # Cap is $1.75 (1% of $175), but min exec is $75 so resize cannot satisfy.
    res = apply_realized_risk_ceiling(
        entry_px=100.0,
        stop_px=97.0,
        size_usdt=75.0,
        balance_usdt=175.0,
        min_exec_usdt=75.0,
        side="buy",
        settings=s,
    )
    assert res.meta["risk_ceiling_action"] == "sl_tightened"
    assert res.size_usdt == 75.0
    assert res.stop_loss > 97.0  # tightened (closer to entry for longs)


def test_amaterasu_macro_context_alignment():
    from strategy_categories import amaterasu_macro_context

    closes_1d = [100.0 + i * 0.5 for i in range(90)]
    ctx = amaterasu_macro_context("LONG", closes_1d=closes_1d, coin="X")
    assert ctx["macro_agreement_score"] >= 1
    assert ctx["size_multiplier"] >= 1.0


def test_structure_failure_signals_long():
    from exit_manager import _structure_failure_signals

    closes = [10.0 - i * 0.02 for i in range(80)]
    price = closes[-1]
    structure, momentum, evidence = _structure_failure_signals(
        "X",
        closes,
        price=price,
        entry_price=10.0,
        is_long=True,
    )
    assert structure is True
    assert isinstance(evidence, dict)


def test_symbol_direction_cooling_state_triggers_size_mult():
    from lifecycle_analytics import symbol_direction_cooling_state

    state = symbol_direction_cooling_state(
        loss_count=3,
        threshold=3,
        cooldown_minutes=30.0,
        confidence_bump=0.0,
        size_multiplier=0.85,
    )
    assert state.active is True
    assert state.size_multiplier < 1.0


def test_symbol_direction_rehab_stages_are_graduated_and_direction_local():
    from lifecycle_analytics import symbol_direction_cooling_state

    long_state = symbol_direction_cooling_state(
        loss_count=0,
        threshold=3,
        cooldown_minutes=20.0,
        confidence_bump=0.6,
        size_multiplier=0.85,
        mild_threshold=2,
        strong_threshold=3,
        cooldown_threshold=5,
    )
    mild = symbol_direction_cooling_state(
        loss_count=2,
        threshold=3,
        cooldown_minutes=20.0,
        confidence_bump=0.6,
        size_multiplier=0.85,
        mild_threshold=2,
        strong_threshold=3,
        cooldown_threshold=5,
        mild_confidence_bump=0.25,
        mild_size_multiplier=0.90,
    )
    confirm = symbol_direction_cooling_state(
        loss_count=3,
        threshold=3,
        cooldown_minutes=20.0,
        confidence_bump=0.6,
        size_multiplier=0.85,
        mild_threshold=2,
        strong_threshold=3,
        cooldown_threshold=5,
    )
    cooldown = symbol_direction_cooling_state(
        loss_count=5,
        threshold=3,
        cooldown_minutes=20.0,
        confidence_bump=0.6,
        size_multiplier=0.85,
        mild_threshold=2,
        strong_threshold=3,
        cooldown_threshold=5,
    )

    assert long_state.active is False
    assert mild.stage == "mild"
    assert mild.require_confirmation is False
    assert math.isclose(mild.size_multiplier, 0.90)
    assert confirm.stage == "confirm"
    assert confirm.require_confirmation is True
    assert cooldown.stage == "cooldown"
    assert cooldown.cooldown_minutes == 20.0


def test_symbol_direction_high_quality_override_allows_cooldown_bypass():
    from lifecycle_analytics import symbol_direction_cooling_state

    state = symbol_direction_cooling_state(
        loss_count=5,
        threshold=3,
        cooldown_minutes=30.0,
        confidence_bump=0.6,
        size_multiplier=0.85,
        mild_threshold=2,
        strong_threshold=3,
        cooldown_threshold=5,
        min_consensus=2,
        quality_floor=82.0,
        candidate_quality=86.0,
        consensus_count=2,
    )

    assert state.stage == "cooldown"
    assert state.override_allowed is True


def test_net_expectancy_gate_reduces_only_weak_context():
    from lifecycle_analytics import assess_net_expectancy

    weak = assess_net_expectancy(
        size_usdt=50.0,
        target_move_pct=0.12,
        fee_rate=0.001,
        slippage_pct=0.03,
        repeat_losses=3,
        confidence=3.1,
        candidate_quality=55.0,
        engine_net=-1.0,
    )
    strong = assess_net_expectancy(
        size_usdt=50.0,
        target_move_pct=0.12,
        fee_rate=0.001,
        slippage_pct=0.03,
        repeat_losses=0,
        confidence=4.8,
        candidate_quality=90.0,
        engine_net=1.0,
    )

    assert weak.action == "reduce"
    assert weak.size_multiplier < 1.0
    assert strong.action == "accept"
    assert strong.size_multiplier == 1.0


def test_meaningful_profit_protection_secures_profit_and_preserves_runner():
    from lifecycle_analytics import meaningful_profit_protection

    decision = meaningful_profit_protection(
        size_usdt=80.0,
        initial_size_usdt=100.0,
        profit_usdt=1.10,
        estimated_fee_usdt=0.08,
        min_profit_floor_usdt=0.20,
        runner_quality=3.5,
        continuation_score=3.5,
        consensus_score=4.0,
        candidate_quality=0.0,
        remaining_pct=80.0,
        runner_min_remaining_pct=40.0,
        base_close_pct=35.0,
        max_close_pct=55.0,
    )

    assert decision.should_secure is True
    assert 0.0 < decision.close_pct <= 40.0
    assert decision.runner_preserved is True


def test_meaningful_profit_protection_does_not_force_tiny_wins():
    from lifecycle_analytics import meaningful_profit_protection

    decision = meaningful_profit_protection(
        size_usdt=80.0,
        initial_size_usdt=100.0,
        profit_usdt=0.12,
        estimated_fee_usdt=0.08,
        min_profit_floor_usdt=0.20,
        runner_quality=4.0,
        continuation_score=4.0,
        consensus_score=4.0,
        candidate_quality=0.0,
        remaining_pct=80.0,
        runner_min_remaining_pct=40.0,
        base_close_pct=35.0,
        max_close_pct=55.0,
    )

    assert decision.should_secure is False
    assert decision.reason == "below_meaningful_target"


def test_low_quality_early_invalidation_remains_fast():
    from lifecycle_analytics import assess_early_invalidation

    decision = assess_early_invalidation(
        roi=-1.4,
        mfe_roi=0.0,
        mae_roi=-1.6,
        age_sec=90.0,
        base_hold_sec=45.0,
        min_hold_floor_sec=20.0,
        max_hold_cap_sec=720.0,
        failure_threshold=2.5,
        patience_threshold=1.5,
        structure_failed=True,
        momentum_flipped=True,
        continuation_quality=0.0,
        consensus_score=1.0,
        runner_quality=0.0,
    )

    assert decision.should_invalidate is True


def test_high_quality_winner_patience_offsets_structure_noise():
    from lifecycle_analytics import assess_early_invalidation

    decision = assess_early_invalidation(
        roi=0.2,
        mfe_roi=1.6,
        mae_roi=-0.4,
        age_sec=90.0,
        base_hold_sec=45.0,
        min_hold_floor_sec=20.0,
        max_hold_cap_sec=720.0,
        failure_threshold=2.5,
        patience_threshold=1.5,
        structure_failed=True,
        momentum_flipped=True,
        trend_regime=True,
        continuation_quality=4.0,
        consensus_score=4.0,
        runner_quality=4.0,
        trend_patience_mult=1.35,
    )

    assert decision.should_invalidate is False
    assert decision.patience_score >= decision.failure_score


def test_internal_sl_confirmation_uses_separate_setting(monkeypatch):
    import time
    import exit_manager as em

    class Settings:
        def get_val(self, key, default=None):
            values = {
                "sl_timeout_enabled": True,
                "sl_timeout_secs": 8,
                "internal_sl_confirm_secs": 12,
            }
            return values.get(key, default)

    async def fake_close(*args, **kwargs):
        return True

    plan = em.ExitPlan(
        coin="TEST",
        direction="long",
        engine_name="SHOGUN",
        entry_price=100.0,
        size_usdt=25.0,
        atr_val=1.0,
        hard_sl=98.0,
        tp1_price=103.0,
        tp2_price=108.0,
    )

    monkeypatch.setattr(em, "_s", Settings())
    monkeypatch.setattr(em, "_do_close", fake_close)

    assert em.asyncio.run(em._timeout_close(plan, "TEST", 97.9, "sl")) is False
    plan.sl_breach_time = time.time() - 11.0
    assert em.asyncio.run(em._timeout_close(plan, "TEST", 97.9, "sl")) is False
    plan.sl_breach_time = time.time() - 12.1
    assert em.asyncio.run(em._timeout_close(plan, "TEST", 97.9, "sl")) is True


def test_risk_sized_notional_uses_actual_stop_distance():
    import category_engine as ce

    result = ce._risk_sized_notional_from_stop(
        engine_name="SHOGUN",
        coin="TEST",
        side="buy",
        entry_px=100.0,
        stop_px=98.0,
        balance=1000.0,
        current_size_usdt=100.0,
        base_size_usdt=100.0,
        min_exec_usdt=5.0,
        max_size_usdt=1000.0,
    )

    assert result["skip"] is False
    assert result["size_usdt"] == 500.0
    assert result["meta"]["risk_amount_usdt"] == 10.0
    assert result["meta"]["sl_dist_pct"] == 2.0


def test_risk_sized_notional_skips_below_min_exec():
    import category_engine as ce

    result = ce._risk_sized_notional_from_stop(
        engine_name="SHOGUN",
        coin="TEST",
        side="buy",
        entry_px=100.0,
        stop_px=98.0,
        balance=100.0,
        current_size_usdt=100.0,
        base_size_usdt=100.0,
        min_exec_usdt=100.0,
        max_size_usdt=1000.0,
    )

    assert result["skip"] is True
    assert result["size_usdt"] == 50.0
    assert "risk_sizing_below_min" in result["reason"]

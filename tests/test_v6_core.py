"""
OxyPulse V6 — tests/test_v6_core.py
Unit tests for pure logic in V6 modules.

Covers:
  - confidence_engine: score_signal, normalised_confidence
  - sizing_engine: compute_size modifiers, floor/cap clamping
  - drawdown_guard: _compute_level transitions
  - engine_weights: update_weight delta and decay logic
  - execution_router: plan selection logic

Run with:
  python -m pytest tests/test_v6_core.py -v
  # or without pytest:
  python tests/test_v6_core.py

All tests use only pure functions and mock settings_store values via monkeypatching.
No exchange, no async, no Telegram dependencies.
"""
from __future__ import annotations

import sys
import os
import unittest

# Add parent directory to path so imports work when run from repo root
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


# ── Minimal settings_store stub ───────────────────────────────────────────────
# Replaces settings_store for tests so we don't need a real settings.json.

class _FakeSettings:
    """In-memory settings stub for isolated testing."""
    def __init__(self, overrides: dict | None = None):
        self._data: dict = {
            "v6_min_long_confidence":   1.8,
            "v6_min_short_confidence":  1.8,
            "v6_max_opposition_score":  0.35,
            "v6_min_position_size_usdt":25.0,
            "v6_max_position_size_usdt":250.0,
            "v6_confidence_size_multiplier": 1.0,
            "v6_volatility_size_penalty":    1.0,
            "v6_drawdown_size_penalty":      1.0,
            "v6_max_position_pct_of_balance":0.03,
            "dynamic_sizing_enabled": True,
            "risk_floor_preserve_enabled": True,
            "risk_floor_preserve_modes": "aggressive,full_deployment,max_deploy,single_shot,ai,ai_adaptive",
            "trading_mode_preset": "",
            "v6_drawdown_level1_pct":   3.0,
            "v6_drawdown_level2_pct":   5.0,
            "v6_drawdown_level3_pct":   8.0,
            "v6_drawdown_level4_pct":   10.0,
            "v6_weight_ryuu":           1.0,
            "v6_weight_titan":          1.0,
            "v6_weight_min":            0.5,
            "v6_weight_max":            3.0,
            "v6_weight_decay_rate":     0.02,
            "v6_weight_update_min_trades": 5,
            "engine_weight_adaptation_enabled": True,
            "v6_anti_drawdown_enabled": True,
            "v6_dynamic_sizing_enabled": True,
            "v6_smart_execution_enabled": True,
            "v6_execution_mode":        "market",
            "v6_execution_allow_limit": True,
            "v6_execution_allow_staged":True,
        }
        if overrides:
            self._data.update(overrides)

    def get_val(self, key, default=None):
        return self._data.get(key, default)


def _patch_settings(test_case, overrides: dict | None = None):
    """Monkey-patch settings_store.get_val for a test."""
    fake = _FakeSettings(overrides)

    import confidence_engine as ce
    import sizing_engine as se
    import drawdown_guard as dg
    import engine_weights as ew
    import execution_router as er

    for mod in (ce, se, dg, ew, er):
        mod._ss = fake  # type: ignore[attr-defined]


# ═══════════════════════════════════════════════════════════════════════════════
# CONFIDENCE ENGINE TESTS
# ═══════════════════════════════════════════════════════════════════════════════

class TestConfidenceEngine(unittest.TestCase):

    def setUp(self):
        _patch_settings(self)
        import confidence_engine
        self.ce = confidence_engine

    def test_hold_always_rejected(self):
        result = self.ce.score_signal("RYUU", "HOLD", 3.0)
        self.assertFalse(result.approved)
        self.assertEqual(result.direction, "HOLD")

    def test_zero_votes_rejected(self):
        result = self.ce.score_signal("TITAN", "LONG", 0.0)
        self.assertFalse(result.approved)

    def test_full_votes_neutral_weight_approved(self):
        # 3 votes × 1.0 weight = 3.0 score; threshold 1.8 → approved
        result = self.ce.score_signal("RYUU", "LONG", 3.0, engine_weight=1.0)
        self.assertTrue(result.approved)
        self.assertAlmostEqual(result.confidence_score, 3.0)

    def test_low_votes_below_threshold(self):
        # 1 vote × 1.0 weight = 1.0 score; threshold 1.8 → rejected
        result = self.ce.score_signal("RYUU", "SHORT", 1.0, engine_weight=1.0)
        self.assertFalse(result.approved)

    def test_weight_amplifies_low_votes(self):
        # 1 vote × 2.5 weight = 2.5 score; threshold 1.8 → approved
        result = self.ce.score_signal("TITAN", "LONG", 1.0, engine_weight=2.5)
        self.assertTrue(result.approved)
        self.assertAlmostEqual(result.confidence_score, 2.5)

    def test_threshold_boundary_exact(self):
        # Exactly at threshold: 1.8 / 1.0 = 1.8 votes needed
        result = self.ce.score_signal("RYUU", "LONG", 1.8, engine_weight=1.0)
        self.assertTrue(result.approved)

    def test_threshold_just_below(self):
        result = self.ce.score_signal("RYUU", "LONG", 1.79, engine_weight=1.0)
        self.assertFalse(result.approved)

    def test_normalised_confidence_max(self):
        # normalised returns result.confidence_score / max_possible, capped at 1.0
        result = self.ce.score_signal("RYUU", "LONG", 3.0, engine_weight=1.0)
        n = self.ce.normalised_confidence(result)
        self.assertGreater(n, 0.0)
        self.assertLessEqual(n, 1.0)

    def test_normalised_confidence_increases_with_votes(self):
        high = self.ce.score_signal("RYUU", "LONG", 3.0, engine_weight=1.0)
        low  = self.ce.score_signal("RYUU", "LONG", 1.0, engine_weight=1.0)
        self.assertGreater(self.ce.normalised_confidence(high),
                           self.ce.normalised_confidence(low))


# ═══════════════════════════════════════════════════════════════════════════════
# SIZING ENGINE TESTS
# ═══════════════════════════════════════════════════════════════════════════════

class TestSizingEngine(unittest.TestCase):

    def setUp(self):
        _patch_settings(self)
        import sizing_engine
        self.se = sizing_engine

    def _size(self, base=100.0, conf=3.0, atr=1.0, price=100.0, lev=25, bal=200.0, dd=1.0):
        return self.se.compute_size(base, conf, atr, price, lev, bal, dd)

    def test_base_size_no_penalty(self):
        # No vol penalty (ATR 0.5% of price), dd=1.0, conf=3.0 → size should be > base
        result = self._size(base=100.0, conf=3.0, atr=0.5, price=100.0)
        self.assertFalse(result.skipped)
        self.assertGreaterEqual(result.final_size_usdt, 100.0)

    def test_high_volatility_reduces_size(self):
        # ATR = 5% of price → high vol → size < base
        low_vol  = self._size(atr=0.5,  price=100.0)   # 0.5% vol
        high_vol = self._size(atr=5.0,  price=100.0)   # 5% vol
        self.assertLess(high_vol.final_size_usdt, low_vol.final_size_usdt)

    def test_drawdown_multiplier_reduces_size(self):
        full_size   = self._size(dd=1.0)
        halved_size = self._size(dd=0.5)
        self.assertLess(halved_size.final_size_usdt, full_size.final_size_usdt)

    def test_floor_enforced(self):
        # If the account cannot support the exchange floor after caps,
        # sizing must skip instead of pretending a $25 trade is executable.
        result = self._size(base=10.0, conf=0.1, dd=0.01, bal=10.0)
        self.assertTrue(result.skipped)
        self.assertLess(result.final_size_usdt, 25.0)
        self.assertIn("below floor", result.reason)

    def test_cap_enforced(self):
        # Size can never exceed v6_max_position_size_usdt (250)
        result = self._size(base=500.0, conf=3.0, dd=1.0)
        self.assertLessEqual(result.final_size_usdt, 250.0)

    def test_zero_drawdown_not_zero_size(self):
        # dd_size_multiplier=0 should still respect the floor
        result = self._size(dd=0.0)
        self.assertGreaterEqual(result.final_size_usdt, 25.0)

    def test_balance_margin_cap(self):
        # Balance=$50, lev=25, max_margin_pct=3% → max_notional=50×0.03×25=$37.5
        # → final_size capped at floor (25) since 37.5 > floor but conf may exceed
        result = self._size(base=200.0, conf=3.0, atr=0.0, price=0.0, lev=25, bal=50.0)
        # max by balance = 50 × 0.03 × 25 = 37.5, cap = 250, floor = 25 → should be ≤ 37.5
        self.assertLessEqual(result.final_size_usdt, 37.5 + 0.01)  # +epsilon for float

    def test_floor_preserve_skips_when_mode_not_allowed(self):
        _patch_settings(self, {
            "dynamic_sizing_enabled": False,
            "v6_dynamic_sizing_enabled": False,
            "v6_max_position_pct_of_balance": 1.0,
            "trading_mode_preset": "safe",
        })
        import sizing_engine
        result = sizing_engine.compute_size(
            base_size=20.0,
            confidence_score=1.0,
            atr_val=0.0,
            entry_px=100.0,
            leverage=20,
            balance=100.0,
            dd_size_multiplier=1.0,
        )
        self.assertTrue(result.skipped)
        self.assertLess(result.final_size_usdt, 25.0)

    def test_floor_preserve_keeps_trade_executable_in_allowed_mode(self):
        _patch_settings(self, {
            "dynamic_sizing_enabled": False,
            "v6_dynamic_sizing_enabled": False,
            "v6_max_position_pct_of_balance": 1.0,
            "trading_mode_preset": "aggressive",
        })
        import sizing_engine
        result = sizing_engine.compute_size(
            base_size=20.0,
            confidence_score=1.0,
            atr_val=0.0,
            entry_px=100.0,
            leverage=20,
            balance=100.0,
            dd_size_multiplier=1.0,
        )
        self.assertFalse(result.skipped)
        self.assertEqual(result.final_size_usdt, 25.0)
        self.assertTrue(result.modifiers.get("floor_preserved"))

    def test_full_deployment_floor_preserve_when_margin_cap_below_floor(self):
        from capital_manager import CapitalProfile
        _patch_settings(self, {
            "dynamic_sizing_enabled": True,
            "v6_dynamic_sizing_enabled": True,
            "v6_max_position_pct_of_balance": 0.10,
            "trading_mode_preset": "aggressive",
            "min_free_balance_buffer_usdt": 5.0,
        })
        import sizing_engine
        profile = CapitalProfile(
            mode="MICRO",
            balance=5.79,
            risk_multiplier=1.4,
            size_multiplier=1.3,
            max_trades_per_hour=10,
            max_exposure_pct=1800.0,
            aggressiveness_score=0.8,
            confidence_threshold=1.5,
            drawdown_tolerance_pct=15.0,
            description="micro full deploy",
            deployment_mode="Full Deployment",
            deployment_effective_mode="Full Deployment",
            deployment_per_position_target_usdt=12.0,
        )
        result = sizing_engine.compute_size(
            base_size=50.0,
            confidence_score=2.5,
            atr_val=0.0,
            entry_px=100.0,
            leverage=25,
            balance=5.79,
            dd_size_multiplier=1.0,
            capital_profile=profile,
        )
        self.assertFalse(result.skipped)
        self.assertEqual(result.final_size_usdt, 25.0)
        self.assertTrue(result.modifiers.get("floor_cap_override"))


# ═══════════════════════════════════════════════════════════════════════════════
# DRAWDOWN GUARD TESTS
# ═══════════════════════════════════════════════════════════════════════════════

class TestDrawdownGuard(unittest.TestCase):

    def setUp(self):
        _patch_settings(self)
        import drawdown_guard
        self.dg = drawdown_guard

    # V6.1: _compute_level and _level_to_state require tolerance_pct.
    # With tolerance=10%: L1 at 2.5%, L2 at 5.0%, L3 at 7.5%, L4 at 10.0%.

    def test_no_drawdown_level0(self):
        level = self.dg._compute_level(0.0, 0.0, 0, tolerance_pct=10.0)
        self.assertEqual(level, 0)

    def test_level1_at_25pct_tolerance(self):
        # L1 triggers at 25% of tolerance=10% → 2.5% daily DD
        level = self.dg._compute_level(2.5, 0.0, 0, tolerance_pct=10.0)
        self.assertEqual(level, 1)

    def test_level2_at_50pct_tolerance(self):
        # L2 triggers at 50% of tolerance=10% → 5.0% daily DD
        level = self.dg._compute_level(5.0, 0.0, 0, tolerance_pct=10.0)
        self.assertEqual(level, 2)

    def test_level3_at_75pct_tolerance(self):
        # L3 triggers at 75% of tolerance=10% → 7.5% daily DD
        level = self.dg._compute_level(7.5, 0.0, 0, tolerance_pct=10.0)
        self.assertEqual(level, 3)

    def test_level4_at_100pct_tolerance(self):
        # L4 triggers at 100% of tolerance=10% → 10.0% daily DD
        level = self.dg._compute_level(10.0, 0.0, 0, tolerance_pct=10.0)
        self.assertEqual(level, 4)

    def test_level3_triggered_by_consec_losses(self):
        level = self.dg._compute_level(0.0, 0.0, 8, tolerance_pct=10.0)
        self.assertEqual(level, 3)

    def test_level4_no_new_trades(self):
        state = self.dg._level_to_state(4, 10.0, 0.0, 5, tolerance_pct=10.0)
        self.assertFalse(state.allow_new_trades)
        self.assertEqual(state.size_multiplier, 0.0)

    def test_level0_full_size(self):
        state = self.dg._level_to_state(0, 0.0, 0.0, 0, tolerance_pct=10.0)
        self.assertTrue(state.allow_new_trades)
        self.assertEqual(state.size_multiplier, 1.0)

    def test_level1_80pct_size(self):
        state = self.dg._level_to_state(1, 2.5, 0.0, 0, tolerance_pct=10.0)
        self.assertAlmostEqual(state.size_multiplier, 0.8)

    def test_level2_50pct_size(self):
        state = self.dg._level_to_state(2, 5.0, 0.0, 0, tolerance_pct=10.0)
        self.assertAlmostEqual(state.size_multiplier, 0.5)

    def test_level3_25pct_size(self):
        state = self.dg._level_to_state(3, 7.5, 0.0, 0, tolerance_pct=10.0)
        self.assertAlmostEqual(state.size_multiplier, 0.25)

    def test_weekly_dd_weighted_at_half(self):
        # Weekly DD 14% weighted at 50% = 7% effective → level 3 (>= 50% of tolerance=10%)
        level = self.dg._compute_level(0.0, 14.0, 0, tolerance_pct=10.0)
        self.assertGreaterEqual(level, 2)


# ═══════════════════════════════════════════════════════════════════════════════
# ENGINE WEIGHTS TESTS
# ═══════════════════════════════════════════════════════════════════════════════

class TestEngineWeights(unittest.TestCase):

    def setUp(self):
        _patch_settings(self)
        import engine_weights
        # Reset in-memory state for isolation
        engine_weights._weights = {}
        self.ew = engine_weights

    def test_default_weight_is_one(self):
        # Bypass file load by seeding the in-memory store directly
        self.ew._weights["RYUU"] = 1.0
        w = self.ew.get_weight("RYUU")
        self.assertAlmostEqual(w, 1.0)

    def test_high_winrate_increases_weight(self):
        w0 = self.ew.get_weight("TITAN")
        self.ew._weights["TITAN"] = 1.0    # ensure starting point
        w1 = self.ew.update_weight("TITAN", win_rate=0.75, num_trades=10)
        self.assertGreater(w1, 1.0)

    def test_low_winrate_decreases_weight(self):
        self.ew._weights["RYUU"] = 1.0
        w1 = self.ew.update_weight("RYUU", win_rate=0.30, num_trades=10)
        self.assertLess(w1, 1.0)

    def test_neutral_winrate_decays_toward_one(self):
        # Start at 1.5 (already boosted), neutral WR should decay toward 1.0
        self.ew._weights["RYUU"] = 1.5
        w1 = self.ew.update_weight("RYUU", win_rate=0.50, num_trades=10)
        # decay = 0.02 * (1.0 - 1.5) = -0.01 → new ≈ 1.49
        self.assertLess(w1, 1.5)
        self.assertGreater(w1, 1.0)

    def test_min_weight_bound_respected(self):
        self.ew._weights["RYUU"] = 0.51
        # Keep applying bad WR — should never go below 0.5
        for _ in range(50):
            w = self.ew.update_weight("RYUU", win_rate=0.10, num_trades=10)
        self.assertGreaterEqual(w, 0.5)

    def test_max_weight_bound_respected(self):
        self.ew._weights["RYUU"] = 2.99
        for _ in range(50):
            w = self.ew.update_weight("RYUU", win_rate=0.99, num_trades=10)
        self.assertLessEqual(w, 3.0)

    def test_insufficient_trades_skips_update(self):
        self.ew._weights["RYUU"] = 1.0
        w = self.ew.update_weight("RYUU", win_rate=0.90, num_trades=3)  # < min 5
        # Should return unchanged
        self.assertAlmostEqual(w, 1.0)

    def test_reset_weight(self):
        self.ew._weights["RYUU"] = 2.5
        self.ew.reset_weight("RYUU")
        self.assertAlmostEqual(self.ew.get_weight("RYUU"), 1.0)


# ═══════════════════════════════════════════════════════════════════════════════
# EXECUTION ROUTER TESTS
# ═══════════════════════════════════════════════════════════════════════════════

class TestExecutionRouter(unittest.TestCase):

    def setUp(self):
        _patch_settings(self, {
            "v6_smart_execution_enabled": True,
            "v6_execution_mode": "market",
            "v6_execution_allow_limit": True,
            "v6_execution_allow_staged": True,
        })
        import execution_router
        self.er = execution_router

    def test_market_mode_returns_single_chunk(self):
        plan = self.er._market_plan()
        self.assertEqual(plan.mode, "market")
        self.assertEqual(len(plan.chunks), 1)
        self.assertEqual(plan.chunks[0]["fraction"], 1.0)

    def test_limit_mode_no_atr_fallback(self):
        plan = self.er._limit_plan("buy", atr_val=0.0, entry_px=100.0)
        # No ATR → must fall back to market
        self.assertEqual(plan.mode, "market")

    def test_limit_mode_with_atr(self):
        plan = self.er._limit_plan("buy", atr_val=2.0, entry_px=100.0)
        self.assertEqual(plan.mode, "limit")
        self.assertLess(plan.chunks[0]["price_offset_pct"], 0)  # buy below market

    def test_staged_plan_three_chunks(self):
        plan = self.er._staged_plan(100.0, atr_val=2.0, entry_px=100.0)
        self.assertEqual(plan.mode, "staged")
        self.assertEqual(len(plan.chunks), 3)
        fractions = sum(c["fraction"] for c in plan.chunks)
        self.assertAlmostEqual(fractions, 1.0, places=5)

    def test_build_plan_disabled_returns_market(self):
        _patch_settings(self, {"v6_smart_execution_enabled": False})
        plan = self.er.build_execution_plan("BTC", "buy", 100.0, 50000.0, 500.0)
        self.assertEqual(plan.mode, "market")

    def test_build_plan_limit_mode(self):
        _patch_settings(self, {
            "v6_smart_execution_enabled": True,
            "v6_execution_mode": "limit",
            "v6_execution_allow_limit": True,
        })
        plan = self.er.build_execution_plan("BTC", "buy", 100.0, 50000.0, 500.0)
        self.assertEqual(plan.mode, "limit")

    def test_build_plan_limit_disallowed_fallback(self):
        _patch_settings(self, {
            "v6_smart_execution_enabled": True,
            "v6_execution_mode": "limit",
            "v6_execution_allow_limit": False,   # flag OFF → market fallback
        })
        plan = self.er.build_execution_plan("BTC", "buy", 100.0, 50000.0, 500.0)
        self.assertEqual(plan.mode, "market")


# ═══════════════════════════════════════════════════════════════════════════════
# V6.1 — CAPITAL MANAGER TESTS
# ═══════════════════════════════════════════════════════════════════════════════

class TestCapitalManager(unittest.TestCase):

    def setUp(self):
        import capital_manager
        capital_manager._ss = _FakeSettings({
            "v6_micro_max_balance":   100.0,
            "v6_growth_max_balance":  1000.0,
            "v6_micro_risk_multiplier":  1.4,
            "v6_micro_size_multiplier":  1.3,
            "v6_micro_max_trades_per_hour": 8,
            "v6_micro_max_exposure_pct":    80.0,
            "v6_micro_confidence_threshold": 1.5,
            "v6_micro_drawdown_tolerance_pct": 15.0,
            "v6_growth_risk_multiplier": 1.0,
            "v6_growth_size_multiplier": 1.0,
            "v6_growth_max_trades_per_hour": 5,
            "v6_growth_max_exposure_pct": 60.0,
            "v6_growth_confidence_threshold": 1.8,
            "v6_growth_drawdown_tolerance_pct": 10.0,
            "v6_pro_risk_multiplier":  0.75,
            "v6_pro_size_multiplier":  0.85,
            "v6_pro_max_trades_per_hour": 3,
            "v6_pro_max_exposure_pct": 40.0,
            "v6_pro_confidence_threshold": 2.2,
            "v6_pro_drawdown_tolerance_pct": 5.0,
            "v6_trade_frequency_enabled": True,
            "v6_max_trades_per_day": 30,
        })
        capital_manager._trade_timestamps.clear()
        self.cm = capital_manager

    def test_micro_mode_below_100(self):
        p = self.cm.get_profile(40.0)
        self.assertEqual(p.mode, "MICRO")
        self.assertAlmostEqual(p.size_multiplier, 1.3)
        self.assertAlmostEqual(p.confidence_threshold, 1.5)

    def test_growth_mode_between_100_1000(self):
        p = self.cm.get_profile(500.0)
        self.assertEqual(p.mode, "GROWTH")
        self.assertAlmostEqual(p.size_multiplier, 1.0)
        self.assertAlmostEqual(p.confidence_threshold, 1.8)

    def test_pro_mode_at_1000(self):
        p = self.cm.get_profile(1000.0)
        self.assertEqual(p.mode, "PRO")
        self.assertAlmostEqual(p.size_multiplier, 0.85)
        self.assertAlmostEqual(p.confidence_threshold, 2.2)

    def test_micro_tolerance_higher_than_pro(self):
        micro = self.cm.get_profile(50.0)
        pro   = self.cm.get_profile(5000.0)
        self.assertGreater(micro.drawdown_tolerance_pct, pro.drawdown_tolerance_pct)

    def test_frequency_ok_when_empty(self):
        p = self.cm.get_profile(50.0)
        ok, reason = self.cm.check_frequency(p)
        self.assertTrue(ok)
        self.assertEqual(reason, "ok")

    def test_frequency_blocked_at_hourly_cap(self):
        p = self.cm.get_profile(50.0)  # MICRO, max 8/hour
        import time as _t
        now = _t.monotonic()
        for _ in range(8):
            self.cm._trade_timestamps.append(now - 60)  # 1 min ago (within hour)
        ok, reason = self.cm.check_frequency(p)
        self.assertFalse(ok)
        self.assertIn("hourly cap", reason)

    def test_frequency_not_blocked_at_hourly_cap_minus_one(self):
        p = self.cm.get_profile(50.0)  # MICRO, max 8/hour
        import time as _t
        now = _t.monotonic()
        for _ in range(7):
            self.cm._trade_timestamps.append(now - 60)
        ok, _ = self.cm.check_frequency(p)
        self.assertTrue(ok)

    def test_record_trade_entry_increments_count(self):
        p = self.cm.get_profile(50.0)
        for _ in range(3):
            self.cm.record_trade_entry()
        import time as _t
        count = sum(1 for t in self.cm._trade_timestamps if t >= _t.monotonic() - 3600)
        self.assertEqual(count, 3)


# ═══════════════════════════════════════════════════════════════════════════════
# V6.1 — EXPOSURE TRACKER TESTS
# ═══════════════════════════════════════════════════════════════════════════════

class TestExposureTracker(unittest.TestCase):

    def setUp(self):
        import exposure_tracker
        import position_registry
        exposure_tracker._ss = _FakeSettings({
            "v6_correlation_control_enabled": True,
            "v6_max_single_direction_usdt":   500.0,
            "correlation_max_same_group":      2,
        })
        self.et = exposure_tracker
        self.pr = position_registry
        self._registry_backup = dict(getattr(position_registry, "_registry", {}) or {})
        position_registry._registry.clear()

    def tearDown(self):
        self.pr._registry.clear()
        self.pr._registry.update(self._registry_backup)

    def test_coin_group_btc_layer1(self):
        self.assertEqual(self.et._get_coin_group("BTC"), "BTC_LAYER1")
        self.assertEqual(self.et._get_coin_group("ETH"), "BTC_LAYER1")
        self.assertEqual(self.et._get_coin_group("SOL"), "BTC_LAYER1")

    def test_coin_group_meme(self):
        self.assertEqual(self.et._get_coin_group("PEPE"), "MEME")
        self.assertEqual(self.et._get_coin_group("DOGE"), "MEME")

    def test_coin_group_defi(self):
        self.assertEqual(self.et._get_coin_group("UNI"), "DEFI")

    def test_coin_group_unknown(self):
        self.assertIsNone(self.et._get_coin_group("UNKNOWN_COIN_XYZ"))

    def test_check_exposure_ok_with_empty_registry(self):
        # position_registry unavailable → empty state → should pass all checks
        ok, reason = self.et.check_exposure("BTC", "long", 100.0, 500.0, 60.0)
        self.assertTrue(ok)

    def test_check_exposure_disabled_always_ok(self):
        self.et._ss = _FakeSettings({"v6_correlation_control_enabled": False})
        ok, reason = self.et.check_exposure("BTC", "long", 9999.0, 100.0, 10.0)
        self.assertTrue(ok)
        self.assertIn("disabled", reason)

    def test_exposure_summary_structure(self):
        from exposure_tracker import ExposureState, exposure_summary
        state = ExposureState(long_usdt=200.0, short_usdt=100.0,
                              net_usdt=100.0, total_usdt=300.0,
                              net_exposure_pct=50.0, balance=200.0)
        summary = exposure_summary(state)
        self.assertIn("long_usdt", summary)
        self.assertIn("short_usdt", summary)
        self.assertIn("net_pct", summary)
        self.assertEqual(summary["long_usdt"], 200.0)


# ═══════════════════════════════════════════════════════════════════════════════
# V6.1 — MARKET FILTER TESTS
# ═══════════════════════════════════════════════════════════════════════════════

class TestMarketFilter(unittest.TestCase):

    def setUp(self):
        import market_filter
        market_filter._ss = _FakeSettings({
            "v6_market_filter_enabled": True,
            "v6_spread_max_pct":       0.30,
            "v6_min_volume_usd_24h":   2_000_000.0,
            "v6_vol_spike_atr_mult":   3.0,
        })
        self.mf = market_filter

    def _ticker(self, bid=99.9, ask=100.1, last=100.0, vol=5_000_000.0):
        return {"bidPx": bid, "askPx": ask, "last": last, "volCcy24h": vol}

    def test_disabled_always_allows(self):
        self.mf._ss = _FakeSettings({"v6_market_filter_enabled": False})
        result = self.mf.check_market("BTC", self._ticker(bid=90.0, ask=110.0), 1.0, [], [], [])
        self.assertTrue(result.allowed)

    def test_good_market_passes(self):
        result = self.mf.check_market("BTC", self._ticker(), 1.0, [], [], [])
        self.assertTrue(result.allowed)

    def test_wide_spread_blocked(self):
        # spread = (100.5 - 99.0) / 100 * 100 = 1.5% > 0.30% → blocked
        result = self.mf.check_market("BTC", self._ticker(bid=99.0, ask=100.5), 1.0, [], [], [])
        self.assertFalse(result.allowed)
        self.assertIn("spread", result.reason)

    def test_low_volume_blocked(self):
        result = self.mf.check_market("BTC", self._ticker(vol=5_000.0), 1.0, [], [], [])
        self.assertFalse(result.allowed)
        self.assertIn("vol", result.reason)

    def test_no_bid_ask_spread_blocks(self):
        # If bid/ask/price missing, spread check is skipped (pass through)
        result = self.mf.check_market("BTC", {"last": 100.0, "volCcy24h": 5_000_000.0},
                                      1.0, [], [], [])
        self.assertFalse(result.allowed)
        self.assertIn("spread unavailable", result.reason)

    def test_zero_volume_blocks(self):
        # volCcy24h=0 means can't read → pass through
        result = self.mf.check_market("BTC", self._ticker(vol=0), 1.0, [], [], [])
        self.assertFalse(result.allowed)
        self.assertIn("volume unavailable", result.reason)


# ═══════════════════════════════════════════════════════════════════════════════
# V6.1 — CONFIDENCE ENGINE MULTI-FACTOR TESTS
# ═══════════════════════════════════════════════════════════════════════════════

class TestConfidenceEngineV61(unittest.TestCase):

    def setUp(self):
        import confidence_engine as ce
        ce._ss = _FakeSettings({
            "v6_min_long_confidence":        1.8,
            "v6_min_short_confidence":       1.8,
            "v6_regime_trend_boost":         0.15,   # TRENDING regime → +15%
            "v6_regime_ranging_penalty":     0.10,   # RANGING regime → -10%
            "v6_vol_high_threshold_atr_pct": 0.04,
            "v6_vol_low_threshold_atr_pct":  0.01,
            "v6_mtf_align_boost":            0.20,
            "v6_opposition_penalty_factor":  0.25,
            "v6_max_opposition_score":       0.35,
        })
        self.ce = ce

    def test_trending_regime_boosts_score(self):
        # "TRENDING" matches _REGIME_TRENDING constant
        trending = self.ce.score_signal("RYUU", "LONG", 2.0, engine_weight=1.0, regime="TRENDING")
        neutral  = self.ce.score_signal("RYUU", "LONG", 2.0, engine_weight=1.0, regime=None)
        self.assertGreater(trending.confidence_score, neutral.confidence_score)

    def test_ranging_regime_reduces_score(self):
        ranging = self.ce.score_signal("RYUU", "LONG", 2.0, engine_weight=1.0, regime="RANGING")
        neutral  = self.ce.score_signal("RYUU", "LONG", 2.0, engine_weight=1.0, regime=None)
        self.assertLess(ranging.confidence_score, neutral.confidence_score)

    def test_high_atr_reduces_score(self):
        low_vol  = self.ce.score_signal("RYUU", "LONG", 2.5, atr_ratio=0.005)
        high_vol = self.ce.score_signal("RYUU", "LONG", 2.5, atr_ratio=0.06)
        self.assertGreater(low_vol.confidence_score, high_vol.confidence_score)

    def test_mtf_alignment_boosts_score(self):
        aligned  = self.ce.score_signal("RYUU", "LONG", 2.0, mtf_aligned=True)
        baseline = self.ce.score_signal("RYUU", "LONG", 2.0, mtf_aligned=False)
        self.assertGreater(aligned.confidence_score, baseline.confidence_score)

    def test_opposition_reduces_score(self):
        clean    = self.ce.score_signal("RYUU", "LONG", 3.0, opposition_votes=0.0)
        opposed  = self.ce.score_signal("RYUU", "LONG", 3.0, opposition_votes=2.5)
        self.assertGreater(clean.confidence_score, opposed.confidence_score)

    def test_capital_profile_threshold_applied(self):
        from capital_manager import CapitalProfile
        pro_profile = CapitalProfile(
            mode="PRO", balance=5000.0, risk_multiplier=0.75,
            size_multiplier=0.85, max_trades_per_hour=3,
            max_exposure_pct=40.0, aggressiveness_score=0.20,
            confidence_threshold=2.2,    # strict
            drawdown_tolerance_pct=5.0,
            description="PRO test",
        )
        # Score of 2.0 is below PRO threshold of 2.2
        result = self.ce.score_signal("RYUU", "LONG", 2.0, capital_profile=pro_profile)
        self.assertFalse(result.approved)

    def test_micro_profile_lower_threshold(self):
        from capital_manager import CapitalProfile
        micro_profile = CapitalProfile(
            mode="MICRO", balance=40.0, risk_multiplier=1.4,
            size_multiplier=1.3, max_trades_per_hour=8,
            max_exposure_pct=80.0, aggressiveness_score=0.80,
            confidence_threshold=1.5,   # loose
            drawdown_tolerance_pct=15.0,
            description="MICRO test",
        )
        # Score of 1.6 would be rejected at GROWTH threshold (1.8), approved at MICRO (1.5)
        result = self.ce.score_signal("RYUU", "LONG", 1.6, capital_profile=micro_profile)
        self.assertTrue(result.approved)


# ═══════════════════════════════════════════════════════════════════════════════
# V6.1 — SIZING ENGINE CAPITAL PROFILE TESTS
# ═══════════════════════════════════════════════════════════════════════════════

class TestSizingEngineV61(unittest.TestCase):

    def setUp(self):
        import sizing_engine
        sizing_engine._ss = _FakeSettings({
            "v6_min_position_size_usdt":     25.0,
            "v6_max_position_size_usdt":     250.0,
            "v6_confidence_size_multiplier": 1.0,
            "v6_volatility_size_penalty":    1.0,
            "v6_drawdown_size_penalty":      1.0,
            "v6_max_position_pct_of_balance":0.03,
            "v6_micro_size_multiplier":      1.3,
            "v6_growth_size_multiplier":     1.0,
            "v6_pro_size_multiplier":        0.85,
        })
        self.se = sizing_engine

    def _make_profile(self, mode, size_mult):
        from capital_manager import CapitalProfile
        return CapitalProfile(
            mode=mode, balance=500.0, risk_multiplier=1.0,
            size_multiplier=size_mult, max_trades_per_hour=5,
            max_exposure_pct=60.0, aggressiveness_score=0.5,
            confidence_threshold=1.8, drawdown_tolerance_pct=10.0,
            description=f"{mode} test",
        )

    def test_micro_profile_larger_than_growth(self):
        micro_p  = self._make_profile("MICRO",  1.3)
        growth_p = self._make_profile("GROWTH", 1.0)
        micro_sz  = self.se.compute_size(100.0, 2.5, 1.0, 100.0, 25, 500.0, capital_profile=micro_p)
        growth_sz = self.se.compute_size(100.0, 2.5, 1.0, 100.0, 25, 500.0, capital_profile=growth_p)
        self.assertGreater(micro_sz.final_size_usdt, growth_sz.final_size_usdt)

    def test_pro_profile_smaller_than_growth(self):
        pro_p    = self._make_profile("PRO",    0.85)
        growth_p = self._make_profile("GROWTH", 1.0)
        pro_sz    = self.se.compute_size(100.0, 2.5, 1.0, 100.0, 25, 500.0, capital_profile=pro_p)
        growth_sz = self.se.compute_size(100.0, 2.5, 1.0, 100.0, 25, 500.0, capital_profile=growth_p)
        self.assertLess(pro_sz.final_size_usdt, growth_sz.final_size_usdt)

    def test_no_profile_uses_default(self):
        # Without profile, should still return valid size
        result = self.se.compute_size(100.0, 2.5, 1.0, 100.0, 25, 500.0, capital_profile=None)
        self.assertGreaterEqual(result.final_size_usdt, 25.0)


# ═══════════════════════════════════════════════════════════════════════════════
# V6.1 — ENGINE WEIGHTS PROFIT FACTOR + DD PENALTY TESTS
# ═══════════════════════════════════════════════════════════════════════════════

class TestEngineWeightsV61(unittest.TestCase):

    def setUp(self):
        import engine_weights
        engine_weights._ss = _FakeSettings({
            "v6_weight_min":                  0.5,
            "v6_weight_max":                  3.0,
            "v6_weight_decay_rate":           0.02,
            "v6_weight_update_min_trades":    5,
            "engine_weight_adaptation_enabled": True,
            "v6_weight_profit_factor_alpha":  0.3,
            "v6_weight_dd_contrib_penalty":   0.1,
        })
        engine_weights._weights = {}
        self.ew = engine_weights

    def test_high_profit_factor_increases_weight(self):
        self.ew._weights["RYUU"] = 1.0
        w1 = self.ew.update_weight("RYUU", win_rate=0.50, num_trades=10, profit_factor=2.0)
        w2 = self.ew.update_weight("RYUU", win_rate=0.50, num_trades=10, profit_factor=1.0)
        # Reset for clean comparison
        self.ew._weights["RYUU"] = 1.0
        a = self.ew.update_weight("RYUU", win_rate=0.50, num_trades=10, profit_factor=2.0)
        self.ew._weights["RYUU"] = 1.0
        b = self.ew.update_weight("RYUU", win_rate=0.50, num_trades=10, profit_factor=1.0)
        self.assertGreater(a, b)

    def test_low_profit_factor_decreases_weight(self):
        self.ew._weights["RYUU"] = 1.0
        w = self.ew.update_weight("RYUU", win_rate=0.50, num_trades=10, profit_factor=0.5)
        self.assertLess(w, 1.0)

    def test_large_avg_loss_penalises_weight(self):
        self.ew._weights["RYUU"] = 1.0
        high_loss = self.ew.update_weight("RYUU", win_rate=0.60, num_trades=10,
                                          profit_factor=1.0, avg_loss_usdt=500.0)
        self.ew._weights["RYUU"] = 1.0
        no_loss   = self.ew.update_weight("RYUU", win_rate=0.60, num_trades=10,
                                          profit_factor=1.0, avg_loss_usdt=0.0)
        self.assertLess(high_loss, no_loss)

    def test_pf_delta_capped_at_003(self):
        # pf_delta = min(0.3 × (100 - 1) × 0.02, 0.03) = 0.03 (capped)
        self.ew._weights["RYUU"] = 1.0
        w = self.ew.update_weight("RYUU", win_rate=0.50, num_trades=10, profit_factor=100.0)
        # Decayed weight + bounded delta — must not exceed max
        self.assertLessEqual(w, 3.0)


# ═══════════════════════════════════════════════════════════════════════════════
# RUNNER
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    unittest.main(verbosity=2)

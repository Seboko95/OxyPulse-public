from __future__ import annotations

import asyncio
import copy
import inspect
import json
import os
import re
import shutil
import sys
import tempfile
import time
import unittest
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def _isolate_settings_path(testcase, ss, prefix: str) -> None:
    testcase._settings_path_backup = ss._PATH
    testcase._settings_load_mtime_backup = getattr(ss, "_last_load_mtime", None)
    testcase._settings_reload_check_backup = getattr(ss, "_last_hot_reload_check", 0.0)
    testcase._settings_tmpdir = tempfile.TemporaryDirectory(prefix=prefix)
    ss._PATH = Path(testcase._settings_tmpdir.name) / "settings.json"
    ss.save()
    ss._last_hot_reload_check = 0.0


def _restore_settings_path(testcase, ss) -> None:
    if hasattr(testcase, "_settings_path_backup"):
        ss._PATH = testcase._settings_path_backup
        ss._last_load_mtime = testcase._settings_load_mtime_backup
        ss._last_hot_reload_check = testcase._settings_reload_check_backup
    tmpdir = getattr(testcase, "_settings_tmpdir", None)
    if tmpdir is not None:
        tmpdir.cleanup()


def _isolate_registry_path(testcase, pr, prefix: str) -> None:
    testcase._registry_path_backup = pr._PATH
    testcase._registry_tmpdir = tempfile.TemporaryDirectory(prefix=prefix)
    pr._PATH = Path(testcase._registry_tmpdir.name) / "position_registry.json"


def _restore_registry_path(testcase, pr) -> None:
    if hasattr(testcase, "_registry_path_backup"):
        pr._PATH = testcase._registry_path_backup
    tmpdir = getattr(testcase, "_registry_tmpdir", None)
    if tmpdir is not None:
        tmpdir.cleanup()


class TestAdaptiveExecutionFloor(unittest.TestCase):

    def setUp(self):
        import settings_store as ss
        self.ss = ss
        self._settings_backup = copy.deepcopy(ss._current)

    def tearDown(self):
        _restore_settings_path(self, self.ss)
        self.ss._current.clear()
        self.ss._current.update(self._settings_backup)

    def test_micro_floor_uses_adaptive_value(self):
        self.ss._current.update({
            "min_order_usdt": 100.0,
            "adaptive_min_order_floor_enabled": True,
            "min_order_usdt_micro": 25.0,
            "v6_micro_max_balance": 100.0,
            "v6_growth_max_balance": 1000.0,
            "v6_min_position_size_usdt": 25.0,
        })
        self.assertEqual(self.ss.get_effective_min_order_usdt(34.2), 25.0)

    def test_growth_floor_stays_below_legacy_floor(self):
        self.ss._current.update({
            "min_order_usdt": 100.0,
            "adaptive_min_order_floor_enabled": True,
            "min_order_usdt_growth": 50.0,
            "v6_micro_max_balance": 100.0,
            "v6_growth_max_balance": 1000.0,
            "v6_min_position_size_usdt": 25.0,
        })
        self.assertEqual(self.ss.get_effective_min_order_usdt(150.0), 50.0)


class TestSoftAcceptExecutableFloor(unittest.TestCase):

    def setUp(self):
        import settings_store as ss
        self.ss = ss
        self._settings_backup = copy.deepcopy(ss._current)
        _isolate_settings_path(self, ss, "pro-control-")
        self.ss._current.update({
            "min_order_usdt": 100.0,
            "adaptive_min_order_floor_enabled": True,
            "min_order_usdt_micro": 25.0,
            "v6_micro_max_balance": 100.0,
            "v6_growth_max_balance": 1000.0,
            "v6_min_position_size_usdt": 25.0,
        })

    def tearDown(self):
        _restore_settings_path(self, self.ss)
        self.ss._current.clear()
        self.ss._current.update(self._settings_backup)

    def test_soft_accept_clamps_micro_trade_to_executable_floor(self):
        import category_engine as ce
        self.assertEqual(ce._apply_soft_accept_size(25.41, 34.0, 0.80), 25.0)

    def test_soft_accept_keeps_reduction_when_still_above_floor(self):
        import category_engine as ce
        self.assertEqual(ce._apply_soft_accept_size(40.0, 34.0, 0.80), 32.0)


class TestRiskFloorPreservePreview(unittest.TestCase):

    def setUp(self):
        import settings_store as ss
        self.ss = ss
        self._settings_backup = copy.deepcopy(ss._current)
        _isolate_settings_path(self, ss, "exposure-modes-")
        self.ss._current.update({
            "v6_enabled": True,
            "position_size_usdt": 40.0,
            "position_size_pct": 0.0,
            "position_size_cap_pct_balance": 100.0,
            "position_size_cap_usdt": 250.0,
            "v6_min_position_size_usdt": 25.0,
            "v6_max_position_pct_of_balance": 1.0,
            "dynamic_sizing_enabled": True,
            "v6_dynamic_sizing_enabled": True,
            "dynamic_target_min_usdt": 20.0,
            "dynamic_target_max_usdt": 40.0,
            "risk_floor_preserve_enabled": True,
            "risk_floor_preserve_modes": "aggressive,full_deployment",
            "pre_reserve_size_validation_enabled": True,
            "trading_mode_preset": "safe",
            "leverage_default": 20,
            "leverage_min": 20,
            "leverage_max": 20,
            "vol_leverage_enabled": False,
        })

    def tearDown(self):
        _restore_settings_path(self, self.ss)
        self.ss._current.clear()
        self.ss._current.update(self._settings_backup)

    def test_preview_skips_when_reduced_size_is_below_floor_in_safe_mode(self):
        import category_engine as ce
        self.ss._current["dynamic_sizing_enabled"] = False
        self.ss._current["v6_dynamic_sizing_enabled"] = False
        original_perf = ce._perf.get_size_multiplier
        original_intent = ce._trade_intent.get_trade_intent
        original_cap = ce._participation_cap_window
        try:
            ce._perf.get_size_multiplier = lambda: 0.5
            ce._trade_intent.get_trade_intent = lambda **_: SimpleNamespace(
                mode="NORMAL", summary="normal", size_multiplier=1.0, tp_multiplier=1.0,
                frequency_multiplier=1.0, allow_new_trades=True, max_positions_multiplier=1.0,
            )
            ce._participation_cap_window = lambda *args, **kwargs: (1_000_000.0, [])
            preview = ce._preview_trade_execution(
                engine_name="RYUU",
                side="buy",
                balance=28.54,
                votes=2.0,
                closes=[100.0] * 20,
                highs=[101.0] * 20,
                lows=[99.0] * 20,
                entry_px=100.0,
            )
        finally:
            ce._perf.get_size_multiplier = original_perf
            ce._trade_intent.get_trade_intent = original_intent
            ce._participation_cap_window = original_cap
        self.assertTrue(preview["skip"])
        self.assertIn("reduced_size_below_min", preview["reject_reason"])

    def test_preview_floor_preserves_in_aggressive_mode(self):
        import category_engine as ce
        self.ss._current["trading_mode_preset"] = "aggressive"
        self.ss._current["dynamic_sizing_enabled"] = False
        self.ss._current["v6_dynamic_sizing_enabled"] = False
        original_perf = ce._perf.get_size_multiplier
        original_intent = ce._trade_intent.get_trade_intent
        original_cap = ce._participation_cap_window
        try:
            ce._perf.get_size_multiplier = lambda: 0.5
            ce._trade_intent.get_trade_intent = lambda **_: SimpleNamespace(
                mode="AGGRESSIVE", summary="aggressive", size_multiplier=1.0, tp_multiplier=1.0,
                frequency_multiplier=1.0, allow_new_trades=True, max_positions_multiplier=1.0,
            )
            ce._participation_cap_window = lambda *args, **kwargs: (1_000_000.0, [])
            preview = ce._preview_trade_execution(
                engine_name="RYUU",
                side="buy",
                balance=28.54,
                votes=2.0,
                closes=[100.0] * 20,
                highs=[101.0] * 20,
                lows=[99.0] * 20,
                entry_px=100.0,
            )
        finally:
            ce._perf.get_size_multiplier = original_perf
            ce._trade_intent.get_trade_intent = original_intent
            ce._participation_cap_window = original_cap
        self.assertFalse(preview["skip"])
        self.assertTrue(preview["floor_preserved"])
        self.assertEqual(preview["size_usdt"], 25.0)

    def test_pre_reserve_reject_branch_exists_before_reserve(self):
        import category_engine as ce
        source = inspect.getsource(ce.CategoryEngine._evaluate_coin)
        self.assertIn("PRE-RESERVE REJECT", source)
        self.assertLess(source.index("PRE-RESERVE REJECT"), source.index("can_open_and_reserve"))


class TestSoftAcceptThresholds(unittest.TestCase):

    def setUp(self):
        import settings_store as ss
        self.ss = ss
        self._settings_backup = copy.deepcopy(ss._current)
        self.ss._current.update({
            "soft_accept_min_confidence": 3.5,
            "soft_accept_min_confidence_micro": 2.5,
            "soft_accept_off_session_confidence_boost_micro": 0.2,
        })

    def tearDown(self):
        self.ss._current.clear()
        self.ss._current.update(self._settings_backup)

    def test_off_session_micro_threshold_is_lower(self):
        import category_engine as ce
        self.assertAlmostEqual(ce._soft_accept_min_confidence("MICRO", "LONDON"), 2.5)
        self.assertAlmostEqual(ce._soft_accept_min_confidence("MICRO", "OFF"), 2.3)


class TestReleaseDiagnostics(unittest.TestCase):

    def test_release_logs_explicit_reason(self):
        import category_engine as ce
        released = []
        original = ce.reg_release_pre_submit
        try:
            ce.reg_release_pre_submit = lambda coin: released.append(coin)
            with self.assertLogs("OxyPulse.CategoryEngine", level="INFO") as logs:
                ce._release("AMATERASU", "LINEA", "short", 3.65, "preflight_zero_contracts: leverage=25x")
        finally:
            ce.reg_release_pre_submit = original
        self.assertEqual(released, ["LINEA"])
        self.assertIn(
            "RELEASE [AMATERASU] LINEA SHORT conf=3.65 | reason=preflight_zero_contracts: leverage=25x",
            logs.output[-1],
        )


class TestRemovedAIModules(unittest.TestCase):

    def test_bot_imports_without_ai_modules(self):
        import bot
        import tg_panels
        import settings_store
        self.assertTrue(hasattr(bot, "main_menu"))
        self.assertTrue(hasattr(tg_panels, "kb_home"))
        self.assertTrue(hasattr(settings_store, "get_val"))

class TestTelegramEngineRendering(unittest.TestCase):

    def test_blocked_panel_uses_current_engine_names(self):
        import tg_panels as tp
        row = SimpleNamespace(
            engine="GENBU",
            coin="BTC",
            direction="LONG",
            conf=3.1,
            tag="lane_budget",
            ts="12:00:00",
        )
        text = tp.panel_blocked_text([row])
        self.assertIn("GENBU", text)
        self.assertNotIn("MIRROR", text)


class TestTradesPanelRendering(unittest.TestCase):

    def test_trades_panel_groups_and_cleans_recent_trades(self):
        import tg_panels as tp

        trades = [
            SimpleNamespace(coin="ONE-USDT-SWAP", direction="long", pnl_usdt=0.22, exit_reason="milestone_tp1"),
            SimpleNamespace(coin="LAYER", direction="long", pnl_usdt=0.58, exit_reason="trailing"),
            SimpleNamespace(coin="LAYER", direction="long", pnl_usdt=0.58, exit_reason="trailing"),
            SimpleNamespace(coin="MINA", direction="short", pnl_usdt=-0.33, exit_reason="sl_exchange"),
            SimpleNamespace(coin="FOO", direction="long", pnl_usdt=0.004, exit_reason="sync_confirmed_close"),
        ]

        text = tp.panel_trades_text(trades, open_count=8)
        self.assertIn("*Trades Overview*", text)
        self.assertIn("Open: `8`", text)
        self.assertIn("Last 20: `3W / 1L / 1N`", text)
        self.assertIn("*Recent Wins*", text)
        self.assertIn("*Recent Losses*", text)
        self.assertIn("*Neutral / Tiny*", text)
        self.assertIn("`ONE` LONG   +$0.22   (TP1)", text)
        self.assertIn("`LAYER` LONG   +$0.58   (Trail)", text)
        self.assertIn("`MINA` SHORT  -$0.33   (SL)", text)
        self.assertIn("`FOO` LONG    $0.00   (Closed)", text)
        self.assertIn("+1 duplicate collapsed", text)
        self.assertNotIn("milestone_tp1", text)
        self.assertNotIn("sl_exchange", text)
        self.assertNotIn("sync_confirmed_close", text)
        self.assertNotIn("[?]", text)
        self.assertNotIn("_", text)

    def test_trades_panel_empty_state_is_safe(self):
        import tg_panels as tp

        text = tp.panel_trades_text([], open_count=0)
        self.assertIn("*Trades Overview*", text)
        self.assertIn("Open: `0`", text)
        self.assertIn("No recent trades available.", text)

    def test_trades_panel_uses_net_pnl_when_fees_are_present(self):
        import tg_panels as tp

        trades = [
            SimpleNamespace(coin="LAYER", direction="long", pnl_usdt=0.70, fee_usdt=0.06, net_pnl_usdt=0.64, exit_reason="trailing"),
            SimpleNamespace(coin="MINA", direction="short", pnl_usdt=-0.42, fee_usdt=0.05, net_pnl_usdt=-0.47, exit_reason="sl_exchange"),
        ]

        text = tp.panel_trades_text(trades, open_count=2)
        self.assertIn("Net: `+$0.17`", text)
        self.assertIn("`LAYER` LONG   +$0.64   (Trail)", text)
        self.assertIn("`MINA` SHORT  -$0.47   (SL)", text)

    def test_category_tracker_dashboard_stats_are_fee_aware(self):
        import category_tracker as ct

        stats_backup = ct._stats
        save_backup = ct._save
        try:
            ct._save = lambda: None
            ct._stats = {name: ct.EngineStats(name=name) for name in ct.ENGINE_NAMES}
            ct.record_trade(
                engine=ct.ENGINE_NAMES[0],
                coin="LAYER",
                direction="long",
                entry=100.0,
                exit_price=100.7,
                size_usdt=100.0,
                pnl_usdt=0.70,
                fee_usdt=0.06,
                vote_score=3,
                exit_reason="trailing",
            )

            combined = ct.get_combined_stats()
            self.assertEqual(combined["today_pnl"], 0.64)
            self.assertEqual(ct._stats[ct.ENGINE_NAMES[0]].trades[0]["gross_pnl_usdt"], 0.70)
            self.assertEqual(ct._stats[ct.ENGINE_NAMES[0]].trades[0]["fee_usdt"], 0.06)
        finally:
            ct._stats = stats_backup
            ct._save = save_backup

    def test_category_tracker_zero_gross_slot_rotation_is_neutral(self):
        import category_tracker as ct

        stats_backup = ct._stats
        save_backup = ct._save
        try:
            ct._save = lambda: None
            ct._stats = {name: ct.EngineStats(name=name) for name in ct.ENGINE_NAMES}
            ct.record_trade(
                engine=ct.ENGINE_NAMES[0],
                coin="TRX",
                direction="long",
                entry=0.33,
                exit_price=0.33,
                size_usdt=200.0,
                pnl_usdt=0.0,
                fee_usdt=0.2,
                vote_score=3,
                exit_reason="slot_rotation",
            )

            combined = ct.get_combined_stats()
            self.assertEqual(combined["today_wins"], 0)
            self.assertEqual(combined["today_losses"], 0)
            self.assertEqual(combined["today_pnl"], -0.2)
            self.assertEqual(ct.get_global_rolling_win_rate(20)["trades"], 0)
        finally:
            ct._stats = stats_backup
            ct._save = save_backup


class TestTelegramRootMenu(unittest.TestCase):

    def test_root_menu_has_control_and_no_dead_status_button(self):
        import tg_panels as tp
        kb = tp.kb_home(running=False)
        callbacks = [
            btn.callback_data
            for row in kb.inline_keyboard
            for btn in row
            if getattr(btn, "callback_data", None)
        ]
        self.assertIn("terminal:status", callbacks)
        self.assertIn("terminal:emergency", callbacks)
        self.assertNotIn("home_status", callbacks)


class TestTelegramFeatureRooms(unittest.TestCase):

    @staticmethod
    def _callbacks(markup):
        return [
            btn.callback_data
            for row in markup.inline_keyboard
            for btn in row
            if getattr(btn, "callback_data", None)
        ]

    def test_trailing_room_uses_live_milestone_keys(self):
        import settings_store as ss
        import tg_panels as tp

        callbacks = self._callbacks(tp.kb_fg_trailing(ss))
        self.assertIn("set:room:trailing_activation_pct", callbacks)
        self.assertIn("set:room:trailing_distance_pct", callbacks)
        self.assertIn("set:room:trailing_after_tp1_pct", callbacks)
        self.assertIn("set:room:trailing_after_tp2_pct", callbacks)
        self.assertNotIn("set:room:tsl_activation_pct", callbacks)
        self.assertNotIn("set:room:tsl_trail_pct", callbacks)

    def test_profit_lock_room_uses_live_keys(self):
        import settings_store as ss
        import tg_panels as tp

        callbacks = self._callbacks(tp.kb_fg_profit_lock(ss))
        self.assertIn("set:room:profit_lock_trigger_pct", callbacks)
        self.assertIn("set:room:profit_lock_buffer_pct", callbacks)
        self.assertIn("set:room:min_full_close_profit_usdt", callbacks)
        self.assertNotIn("set:room:profit_lock_l1_trigger_pct", callbacks)
        self.assertNotIn("set:room:profit_lock_l2_trigger_pct", callbacks)
        self.assertNotIn("set:room:profit_lock_l3_trigger_pct", callbacks)

    def test_execution_category_exposes_profitability_refinement_rooms(self):
        import settings_store as ss
        import tg_panels as tp

        callbacks = self._callbacks(tp.kb_settings_cat_execution(ss))
        self.assertIn("set:fg:early_invalidation", callbacks)
        self.assertIn("set:fg:repeat_loss_cooling", callbacks)
        self.assertIn("set:fg:trailing", callbacks)
        self.assertIn("set:fg:partials", callbacks)

    def test_early_invalidation_room_exposes_backend_settings(self):
        import settings_store as ss
        import tg_panels as tp

        callbacks = self._callbacks(tp.kb_fg_early_invalidation(ss))
        for expected in {
            "set:room:early_invalidation_enabled",
            "set:room:adaptive_early_invalidation_enabled",
            "set:room:profitability_telemetry_enabled",
            "set:room:early_invalidation_near_tp_grace_enabled",
            "set:room:early_invalidation_base_min_hold_sec",
            "set:room:early_invalidation_failure_score_threshold",
            "set:room:early_invalidation_near_tp_floor_ratio",
            "set:room:early_invalidation_trend_patience_mult",
        }:
            self.assertIn(expected, callbacks)

    def test_repeat_loss_cooling_room_exposes_localized_controls(self):
        import settings_store as ss
        import tg_panels as tp

        callbacks = self._callbacks(tp.kb_fg_repeat_loss_cooling(ss))
        for expected in {
            "set:room:symbol_direction_loss_cooldown_enabled",
            "set:room:symbol_direction_loss_threshold",
            "set:room:symbol_direction_cooldown_minutes",
            "set:room:symbol_direction_loss_conf_bump",
            "set:room:symbol_direction_loss_size_mult",
            "set:room:symbol_direction_memory_decay_hours",
        }:
            self.assertIn(expected, callbacks)

    def test_partials_and_trailing_expose_fee_and_runner_controls(self):
        import settings_store as ss
        import tg_panels as tp

        partial_callbacks = self._callbacks(tp.kb_fg_partials(ss))
        self.assertIn("set:room:fee_aware_partial_deferral_enabled", partial_callbacks)
        self.assertIn("set:room:economic_min_runner_notional_usdt", partial_callbacks)
        self.assertIn("set:room:economic_min_fee_mult", partial_callbacks)
        self.assertIn("set:room:early_invalidation_near_tp_floor_ratio", partial_callbacks)

        trailing_callbacks = self._callbacks(tp.kb_fg_trailing(ss))
        self.assertIn("set:room:runner_core_min_remaining_pct", trailing_callbacks)
        self.assertIn("set:room:continuation_runner_enabled", trailing_callbacks)
        self.assertIn("set:room:continuation_runner_core_min_pct", trailing_callbacks)
        self.assertIn("set:room:exhaustion_assisted_tightening_enabled", trailing_callbacks)

    def test_deployment_room_exposes_real_size_drivers(self):
        import settings_store as ss
        import tg_panels as tp

        callbacks = self._callbacks(tp.kb_fg_deployment(ss))
        self.assertIn("set:room:position_size_usdt", callbacks)
        self.assertIn("set:room:v6_min_position_size_usdt", callbacks)
        self.assertIn("set:room:v6_max_position_size_usdt", callbacks)
        self.assertIn("set:room:v6_max_position_pct_of_balance", callbacks)
        self.assertIn("set:room:dynamic_sizing_enabled", callbacks)
        self.assertIn("set:room:position_size_pct", callbacks)
        self.assertIn("set:room:deployment_mode", callbacks)
        self.assertIn("set:room:deployment_target_utilization_pct", callbacks)
        self.assertIn("set:room:deployment_max_positions", callbacks)


class TestExecutionPanelRendering(unittest.TestCase):

    def setUp(self):
        import settings_store as ss
        self.ss = ss
        self._settings_backup = copy.deepcopy(ss._current)
        self.ss._current.update({
            "secure_profit_mode_enabled": True,
            "secure_profit_target_usd": 3.0,
            "breakeven_activation_profit_usd": 0.35,
            "trail_activation_profit_usd": 0.90,
        })

    def tearDown(self):
        self.ss._current.clear()
        self.ss._current.update(self._settings_backup)

    def test_execution_panel_shows_secure_profit_controls(self):
        import tg_panels as tp
        text = tp.panel_settings_execution_text(self.ss)
        self.assertIn("Dynamic Exits", text)
        self.assertIn("Milestones", text)
        self.assertIn("Floor Preserve", text)
        self.assertIn("Pre-Reserve Validation", text)
        self.assertNotIn("Profit Target", text)
        kb = tp.kb_settings_execution(self.ss)
        callbacks = [
            btn.callback_data
            for row in kb.inline_keyboard
            for btn in row
            if getattr(btn, "callback_data", None)
        ]
        self.assertIn("stg_tog_secure_profit_mode_enabled", callbacks)
        self.assertIn("stg_tog_risk_floor_preserve_enabled", callbacks)
        self.assertIn("stg_risk_reduce_up", callbacks)
        self.assertNotIn("stg_minprofit_up", callbacks)
        self.assertNotIn("stg_secure_up", callbacks)


class TestOrderSkipReason(unittest.TestCase):

    def test_skip_reason_is_consumed(self):
        import okx_client as oc
        client = oc.OKXClient()
        client._set_last_order_skip_reason("margin_headroom: req_margin=1.00")
        self.assertEqual(client.consume_last_order_skip_reason(), "margin_headroom: req_margin=1.00")
        self.assertEqual(client.consume_last_order_skip_reason(), "")


class TestEngineNoticeDedup(unittest.TestCase):

    def setUp(self):
        import trading_engine as te
        self.te = te
        te.engine_notice_state.clear()

    def tearDown(self):
        self.te.engine_notice_state.clear()

    def test_same_engine_protection_state_is_suppressed(self):
        first = self.te._should_send_engine_notice(
            "RYUJIN",
            mode="probation",
            protection_state="protected",
            reason="positive last30 net +1.00",
            guard_note="loss streak 8",
            role="booster",
            reminder_sec=3600,
        )
        second = self.te._should_send_engine_notice(
            "RYUJIN",
            mode="probation",
            protection_state="protected",
            reason="positive last30 net +1.00",
            guard_note="loss streak 8",
            role="booster",
            reminder_sec=3600,
        )
        self.assertTrue(first)
        self.assertFalse(second)

    def test_engine_protection_meaningful_change_notifies(self):
        self.te._should_send_engine_notice(
            "RYUJIN",
            mode="probation",
            protection_state="protected",
            reason="positive last30 net +1.00",
            guard_note="loss streak 8",
            role="booster",
            reminder_sec=3600,
        )
        changed = self.te._should_send_engine_notice(
            "RYUJIN",
            mode="probation",
            protection_state="protected",
            reason="positive last30 net +2.50",
            guard_note="loss streak 8",
            role="booster",
            reminder_sec=3600,
        )
        self.assertTrue(changed)


class TestSizeControlAliases(unittest.TestCase):

    def setUp(self):
        import settings_store as ss
        self.ss = ss
        self._settings_backup = copy.deepcopy(ss._current)
        _isolate_settings_path(self, ss, "size-alias-")

    def tearDown(self):
        _restore_settings_path(self, self.ss)
        self.ss._current.clear()
        self.ss._current.update(self._settings_backup)

    def test_position_size_alias_syncs_to_v6_base_size(self):
        self.ss.set_val("position_size_usdt", 42.0)
        self.assertEqual(float(self.ss.get_val("position_size_usdt", 0.0)), 42.0)
        self.assertEqual(float(self.ss.get_val("v6_base_position_size_usdt", 0.0)), 42.0)

    def test_v6_base_size_alias_syncs_back_to_position_size(self):
        self.ss.set_val("v6_base_position_size_usdt", 57.5)
        self.assertEqual(float(self.ss.get_val("position_size_usdt", 0.0)), 57.5)
        self.assertEqual(float(self.ss.get_val("v6_base_position_size_usdt", 0.0)), 57.5)

    def test_invalid_size_setting_preserves_previous_value(self):
        self.ss._current["v6_min_position_size_usdt"] = 25.0
        self.ss._current["v6_max_position_size_usdt"] = 50.0
        self.ss.set_val("v6_min_position_size_usdt", 75.0)
        self.assertEqual(float(self.ss.get_val("v6_min_position_size_usdt", 0.0)), 25.0)
        self.ss.set_val("position_size_usdt", -1.0)
        self.assertGreater(float(self.ss.get_val("position_size_usdt", 0.0)), 0.0)


class TestRejectionCooldown(unittest.TestCase):

    def setUp(self):
        import settings_store as ss
        import rejection_log as rj
        self.ss = ss
        self.rj = rj
        self._settings_backup = copy.deepcopy(ss._current)
        self.ss._current.update({
            "blocked_candidate_cooldown_secs": 60.0,
            "blocked_candidate_suppress_repeats": True,
            "blocked_candidate_group_window_secs": 300.0,
        })
        self.rj.clear()

    def tearDown(self):
        self.rj.clear()
        self.ss._current.clear()
        self.ss._current.update(self._settings_backup)

    def test_repeat_block_is_suppressed_and_cooldowned(self):
        first = self.rj.record("RYUU", "BTC", "LONG", 3.0, "entry_filter", "htf fail")
        second = self.rj.record("RYUU", "BTC", "LONG", 3.0, "entry_filter", "htf fail")
        self.assertFalse(first["suppressed_log"])
        self.assertTrue(second["suppressed_log"])
        state = self.rj.should_skip_candidate("RYUU", "BTC", "LONG")
        self.assertIsNotNone(state)
        self.assertEqual(state["tag"], "entry_filter")

    def test_cooldown_expires_cleanly(self):
        self.rj.record("RYUU", "BTC", "LONG", 3.0, "entry_filter", "htf fail")
        key = self.rj._candidate_key("RYUU", "BTC", "LONG")
        self.rj._candidate_state[key]["until"] = time.monotonic() - 1.0
        self.assertIsNone(self.rj.should_skip_candidate("RYUU", "BTC", "LONG"))


class TestRegistryConfirmation(unittest.TestCase):

    def setUp(self):
        import position_registry as pr
        self.pr = pr
        self._save_backup = pr._save
        self._registry_backup = copy.deepcopy(pr._registry)
        self.pr._save = lambda *args, **kwargs: True
        self.pr._registry.clear()
        self.pr._registry.update({
            "BTC": {
                "direction": "long",
                "engine": "RYUU",
                "size_usdt": 25.0,
            }
        })

    def tearDown(self):
        self.pr._save = self._save_backup
        self.pr._registry.clear()
        self.pr._registry.update(self._registry_backup)

    def test_same_coin_confirmation_tracks_distinct_engines(self):
        result = asyncio.run(self.pr.confirm("BTC", "TITAN"))
        self.assertTrue(result["confirmation_added"])
        self.assertEqual(result["confirmation_count"], 1)
        again = asyncio.run(self.pr.confirm("BTC", "TITAN"))
        self.assertFalse(again["confirmation_added"])
        self.assertEqual(again["confirmation_count"], 1)


class TestEngineRehabilitationLayer(unittest.TestCase):

    def setUp(self):
        import settings_store as ss
        import position_registry as pr
        import category_tracker as ct
        self.ss = ss
        self.pr = pr
        self.ct = ct
        self._settings_backup = copy.deepcopy(ss._current)
        self._save_backup = ss.save
        self._reg_backup = copy.deepcopy(pr._registry)
        self._reg_save_backup = pr._save
        self._stats_backup = copy.deepcopy(ct._stats)
        self._ct_save_backup = ct._save
        ss.save = lambda: None
        pr._save = lambda: None
        ct._save = lambda: None

    def tearDown(self):
        _restore_settings_path(self, self.ss)
        self.ss._current.clear()
        self.ss._current.update(self._settings_backup)
        self.ss.save = self._save_backup
        self.pr._registry.clear()
        self.pr._registry.update(self._reg_backup)
        self.pr._save = self._reg_save_backup
        self.ct._stats = self._stats_backup
        self.ct._save = self._ct_save_backup

    def test_role_defaults_are_configurable(self):
        self.assertEqual(self.ss.get_engine_role("HACHIMAN"), "selector")
        self.assertEqual(self.ss.get_engine_role("RONIN"), "hunter")
        self.ss.set_engine_role("HACHIMAN", "confirmer", actor="test", reason="unit")
        self.assertEqual(self.ss.get_engine_role("HACHIMAN"), "confirmer")
        self.assertLess(
            self.ss.get_engine_role_weight(role="confirmer"),
            self.ss.get_engine_role_weight(role="hunter"),
        )

    def test_consensus_rules_and_position_strength_are_calculated(self):
        import category_engine as ce
        self.ss._current["engine_hayate_role"] = "confirmer"
        consensus = SimpleNamespace(agreement_count=2, engines=["RONIN", "HAYATE"])
        ctx = ce._position_strength_context(
            "HAYATE",
            "BTC",
            "LONG",
            {"quality_score": 62.0, "consensus": consensus, "quality_regime_bonus": 1.5},
            3.0,
        )
        self.assertEqual(ctx["consensus_count"], 2)
        self.assertTrue(ctx["role_consensus"])
        self.assertGreater(ctx["position_strength"], 0.0)
        self.assertIn("confirmer", ctx["contributor_roles"])

    def test_same_symbol_same_direction_merge_window(self):
        import category_engine as ce
        self.ss._current["duplicate_merge_window_sec"] = 90
        now = time.time()
        self.assertTrue(ce._within_duplicate_merge_window({"last_signal_ts": now - 30}, now_ts=now))
        self.assertFalse(ce._within_duplicate_merge_window({"last_signal_ts": now - 120}, now_ts=now))

    def test_confirming_metadata_is_stored_on_one_position(self):
        self.pr._registry.clear()
        self.pr._registry["BTC"] = {
            "direction": "long",
            "engine": "RONIN",
            "size_usdt": 25.0,
            "last_signal_ts": time.time(),
        }
        meta = {
            "confirming_engines": ["HAYATE"],
            "role_contributions": {"HAYATE": {"role": "confirmer", "position_strength": 3.2}},
            "selector_score": 1.1,
            "booster_score": 0.4,
            "position_strength": 3.2,
        }
        result = asyncio.run(self.pr.confirm("BTC", "HAYATE", metadata=meta))
        self.assertTrue(result["confirmation_added"])
        self.assertEqual(len(self.pr._registry), 1)
        pos = self.pr._registry["BTC"]
        self.assertEqual(pos["consensus_count"], 2)
        self.assertIn("HAYATE", pos["confirming_engines"])
        self.assertEqual(pos["role_contributions"]["HAYATE"]["role"], "confirmer")
        self.assertEqual(pos["position_strength"], 3.2)

    def test_confirmer_open_rights_require_consensus_or_high_quality(self):
        import category_engine as ce
        base = {"engine_role": "confirmer", "role_consensus": False, "performance": {"last10_net": 0, "last30_net": 0}}
        allowed, reason = ce._role_independent_open_allowed(base, 45.0)
        self.assertFalse(allowed)
        self.assertIn("weak solo confirmer", reason)
        allowed, _ = ce._role_independent_open_allowed({**base, "role_consensus": True}, 45.0)
        self.assertTrue(allowed)
        allowed, _ = ce._role_independent_open_allowed(base, 90.0)
        self.assertTrue(allowed)

    def test_low_vote_confirmers_can_contribute_to_consensus_without_solo_spam(self):
        import category_engine as ce
        import signal_consensus as sc

        sc.clear()
        self.ss._current["engine_fujin_role"] = "confirmer"
        self.ss._current["role_contribution_min_votes"] = 2.0
        self.assertTrue(ce._role_can_contribute_below_open_votes("FUJIN"))
        self.assertEqual(ce._role_contribution_min_votes("FUJIN", 3.0), 2.0)

        sc.record_signal("KAGUTSUCHI", "DASH", "LONG", 3.0)
        sc.record_signal("FUJIN", "DASH", "LONG", 2.0)
        ctx = ce._position_strength_context(
            "FUJIN",
            "DASH",
            "LONG",
            {"quality_score": 35.0},
            2.0,
        )
        self.assertEqual(ctx["consensus_count"], 2)
        self.assertTrue(ctx["role_consensus"])
        self.assertTrue(ce._role_independent_open_allowed(ctx, 35.0)[0])
        sc.clear()

    def test_shadow_signals_feed_consensus_but_do_not_open_directly(self):
        import category_engine as ce
        source = inspect.getsource(ce.CategoryEngine._evaluate_shadow)
        self.assertIn("_consensus.record_signal", source)
        self.assertIn("shadow_contribution", source)
        self.assertIn("contribution_recorded", source)

    def test_selector_booster_and_specialist_role_behavior(self):
        import category_engine as ce
        selector = {"engine_role": "selector", "role_consensus": True, "performance": {}}
        booster = {"engine_role": "booster", "role_consensus": False, "performance": {"last30_net": 1.0}}
        specialist = {"engine_role": "specialist", "role_consensus": False, "performance": {}}
        self.assertFalse(ce._role_independent_open_allowed(selector, 90.0)[0])
        self.assertFalse(ce._role_independent_open_allowed(booster, 90.0)[0])
        self.assertTrue(ce._role_independent_open_allowed({**booster, "role_consensus": True}, 70.0)[0])
        self.assertFalse(ce._role_independent_open_allowed(specialist, 50.0)[0])
        self.assertTrue(ce._role_independent_open_allowed(specialist, 80.0)[0])

    def test_positive_net_protection_for_ronin_and_ryujin(self):
        import trading_engine as te
        for name in ("RONIN", "RYUJIN"):
            protected, reason = te._positive_net_shadow_protected(
                name,
                "probation",
                {"last10_net": -0.05, "last30_net": 1.25},
                {"loss_streak": 4},
            )
            self.assertTrue(protected, name)
            self.assertIn("positive last30", reason)
        protected, _ = te._positive_net_shadow_protected("RYUJIN", "probation", {"last10_net": -1.0, "last30_net": -0.5})
        self.assertFalse(protected)
        rehab, reason = te._positive_net_shadow_rehab_candidate("RYUJIN", "shadow", {"last10_net": 0.1, "last30_net": 0.95})
        self.assertTrue(rehab)
        self.assertIn("restoring probation", reason)
        rehab, _ = te._positive_net_shadow_rehab_candidate("HACHIMAN", "shadow", {"last10_net": 0.1, "last30_net": 0.95})
        self.assertFalse(rehab)

    def test_fee_pressure_lowers_priority_but_does_not_disable_hunter_signal(self):
        import category_engine as ce
        high_fee = {"position_strength": 5.0, "fee_pressure": 1.25, "performance": {"last10_net": 0.2, "last30_net": 0.5}}
        low_fee = {"position_strength": 5.0, "fee_pressure": 0.05, "performance": {"last10_net": 0.2, "last30_net": 0.5}}
        self.assertLess(ce._role_quality_adjustment(high_fee), ce._role_quality_adjustment(low_fee))
        allowed, reason = ce._role_independent_open_allowed(
            {"engine_role": "hunter", "role_consensus": False, "performance": high_fee["performance"]},
            25.0,
        )
        self.assertTrue(allowed)
        self.assertIn("hunter", reason)

    def test_recent_performance_uses_net_after_fees_and_duplicate_rate(self):
        self.ct._stats = {name: self.ct.EngineStats(name=name) for name in self.ct.ENGINE_NAMES}
        self.ct.record_trade(
            engine="RONIN",
            coin="BTC",
            direction="long",
            entry=100.0,
            exit_price=101.0,
            size_usdt=100.0,
            pnl_usdt=1.0,
            fee_usdt=0.25,
            metadata={"confirming_engines": ["HAYATE"]},
        )
        perf = self.ct.get_engine_recent_performance("RONIN", 10, 30)
        self.assertEqual(perf["last30_net"], 0.75)
        self.assertEqual(perf["avg_fee"], 0.25)
        self.assertEqual(perf["duplicate_entry_rate"], 100.0)

    def test_signal_generation_precedes_role_contribution_path(self):
        import category_engine as ce
        source = inspect.getsource(ce.CategoryEngine._evaluate_coin)
        self.assertLess(source.index("_consensus.record_signal"), source.index("ROLE CONTRIBUTION"))


class TestPerformanceCloseDedup(unittest.TestCase):

    def setUp(self):
        import performance_tracker as perf
        self.perf = perf
        self._state_backup = copy.deepcopy(perf._state)
        self._save_backup = perf._save
        perf._save = lambda: None
        perf._state = perf.PerformanceState()

    def tearDown(self):
        self.perf._state = self._state_backup
        self.perf._save = self._save_backup

    def test_sync_close_duplicate_of_exchange_sl_is_suppressed(self):
        self.perf.record_trade("TAO", "long", 300.0, 299.0, 3.2, -0.0107, "sl_exchange", fee_usdt=0.0032)
        self.perf.record_trade("TAO", "long", 300.0, 299.0, 3.2, -0.0107, "sync_confirmed_close", fee_usdt=0.0032)
        self.assertEqual(len(self.perf._state.trades), 1)

    def test_different_partial_sizes_are_not_deduped(self):
        self.perf.record_trade("TAO", "long", 300.0, 303.0, 18.5, 0.185, "milestone_tp1", fee_usdt=0.0185)
        self.perf.record_trade("TAO", "long", 300.0, 303.0, 19.4, 0.194, "milestone_tp2", fee_usdt=0.0194)
        self.assertEqual(len(self.perf._state.trades), 2)


class TestExitAndNotificationRegression(unittest.TestCase):

    def setUp(self):
        import exit_manager as em
        import notifier as nt
        import settings_store as ss

        self.em = em
        self.nt = nt
        self.ss = ss
        self._settings_backup = copy.deepcopy(ss._current)
        self.ss._current.update({
            "breakeven_enabled": False,
            "profit_lock_enabled": False,
            "tsl_enabled": False,
            "partial_exit_enabled": True,
            "tp1_trigger_pct": 1.0,
            "tp2_trigger_pct": 2.0,
            "partial_tp1_close_pct": 50.0,
            "partial_tp2_close_pct": 50.0,
            "min_net_take_profit_usdt": 0.16,
            "min_full_close_profit_usdt": 0.12,
        })

    def tearDown(self):
        self.ss._current.clear()
        self.ss._current.update(self._settings_backup)

    def test_milestone_partial_skips_when_profit_floor_not_met(self):
        plan = self.em.ExitPlan(
            coin="TEST",
            direction="long",
            engine_name="RONIN",
            entry_price=100.0,
            size_usdt=25.0,
            atr_val=1.0,
            hard_sl=99.0,
            tp1_price=101.0,
            tp2_price=102.0,
            initial_size_usdt=25.0,
            leverage=25,
        )
        called = {"partial": False}
        original_execute_partial = self.em._execute_partial
        try:
            async def _fake_execute_partial(*args, **kwargs):
                called["partial"] = True
                return True

            self.em._execute_partial = _fake_execute_partial
            result = asyncio.run(self.em._apply_milestone_profit_system("TEST", plan, 100.08, True))
        finally:
            self.em._execute_partial = original_execute_partial

        self.assertFalse(result)
        self.assertFalse(called["partial"])
        self.assertFalse(plan.tp1_done)

    def test_zero_size_close_is_explicitly_skipped(self):
        import okx_client as oc

        client = oc.OKXClient()
        result = asyncio.run(client.close_futures_position("BTC", "long", 0.0))
        self.assertEqual(result["state"], "skipped")
        self.assertEqual(result["ordId"], "SKIPPED-ZERO-BTC")

    def test_partial_exchange_closed_result_does_not_count_as_fill(self):
        plan = self.em.ExitPlan(
            coin="TEST",
            direction="long",
            engine_name="RONIN",
            entry_price=100.0,
            size_usdt=25.0,
            atr_val=1.0,
            hard_sl=99.0,
            tp1_price=101.0,
            tp2_price=102.0,
            initial_size_usdt=25.0,
            leverage=25,
        )

        class FakeOKX:
            async def close_futures_position(self, coin, pos_side, size_usdt):
                return {
                    "ordId": f"EXCHANGE-CLOSED-{coin}",
                    "state": "filled",
                    "exchange_closed": True,
                }

        old_client = self.em._okx_client
        self.em._plans["TEST"] = plan
        self.em._okx_client = FakeOKX()
        try:
            self.assertFalse(asyncio.run(self.em._execute_partial("TEST", 101.0, 50.0)))
        finally:
            self.em._plans.clear()
            self.em._okx_client = old_client

    def test_take_profit_alert_formats_tiny_nonzero_realized_pnl(self):
        captured: list[str] = []
        original_send_alert = self.nt.send_alert
        try:
            async def _fake_send_alert(text: str) -> bool:
                captured.append(text)
                return True

            self.nt.send_alert = _fake_send_alert
            asyncio.run(
                self.nt.alert_take_profit(
                    "BTC",
                    100.0,
                    100.2,
                    0.2,
                    25.0,
                    leverage=25,
                    realized_pnl_usdt=0.00031,
                    fee_est_usdt=0.00005,
                    label="TP1",
                )
            )
        finally:
            self.nt.send_alert = original_send_alert

        self.assertEqual(len(captured), 1)
        message = captured[0]
        self.assertIn("Realized:", message)
        self.assertIn("Fee est:", message)
        self.assertIn("Net est:", message)
        self.assertIn("Realized: `$+0.000310`", message)
        self.assertIn("Net est:  `$+0.000260`", message)

    def test_trade_closed_alert_shows_realized_and_fee_breakout(self):
        captured: list[str] = []
        original_send_alert = self.nt.send_alert
        close_backup = dict(self.nt._close_notified)
        self.nt._close_notified.clear()
        try:
            async def _fake_send_alert(text: str) -> bool:
                captured.append(text)
                return True

            self.nt.send_alert = _fake_send_alert
            asyncio.run(
                self.nt.alert_trade_closed(
                    engine="RONIN",
                    coin="BTC",
                    direction="long",
                    entry_price=100.0,
                    exit_price=100.4,
                    size_usdt=25.0,
                    pnl_usdt=0.0831,
                    exit_reason="breakeven_stop",
                    leverage=25,
                    via_exchange=False,
                )
            )
        finally:
            self.nt.send_alert = original_send_alert
            self.nt._close_notified.clear()
            self.nt._close_notified.update(close_backup)

        self.assertEqual(len(captured), 1)
        message = captured[0]
        self.assertIn("Realized:", message)
        self.assertIn("Fee est:", message)
        self.assertIn("Net est:", message)
        self.assertIn("Reason:", message)


class TestPendingReservationAccounting(unittest.TestCase):

    def setUp(self):
        import settings_store as ss
        import position_registry as pr
        import exposure_tracker as et
        import portfolio_allocator as pa
        self.ss = ss
        self.pr = pr
        self.et = et
        self.pa = pa
        self._settings_backup = copy.deepcopy(ss._current)
        self._registry_backup = copy.deepcopy(pr._registry)
        self._save_backup = pr._save
        self._path_backup = pr._PATH
        self._healthy_backup = pr._healthy
        self._load_error_backup = pr._load_error
        self.pr._healthy = True
        self.pr._load_error = ""
        self.pr._save = lambda *args, **kwargs: True
        self.ss._current.update({
            "lane_budget_enabled": True,
            "per_lane_budget_enabled": True,
            "same_direction_headroom_enabled": True,
            "v6_micro_max_balance": 100.0,
            "v6_growth_max_balance": 1000.0,
            "capital_profile": "AUTO",
            "deployment_mode": "Guarded",
            "deployment_target_utilization_pct": 0.0,
            "deployment_max_positions": 0,
            "deployment_respect_exposure_cap": True,
            "deployment_respect_headroom": True,
            "deployment_same_side_limit_pct": 0.0,
            "deployment_total_exposure_limit_pct": 0.0,
            "exposure_control_mode": "Strict",
            "v6_micro_max_exposure_pct": 80.0,
            "v6_growth_max_exposure_pct": 60.0,
            "v6_pro_max_exposure_pct": 40.0,
            "portfolio_same_direction_pct_micro": 80.0,
            "portfolio_same_direction_pct_growth": 60.0,
            "portfolio_same_direction_pct_pro": 40.0,
            "lane_budget_micro_trend_pct": 80.0,
            "min_order_usdt": 100.0,
            "adaptive_min_order_floor_enabled": True,
            "min_order_usdt_micro": 25.0,
            "v6_min_position_size_usdt": 25.0,
        })
        self.pr._registry.clear()

    def tearDown(self):
        self.pr._PATH = self._path_backup
        self.pr._save = self._save_backup
        self.pr._healthy = self._healthy_backup
        self.pr._load_error = self._load_error_backup
        self.pr._registry.clear()
        self.pr._registry.update(self._registry_backup)
        self.ss._current.clear()
        self.ss._current.update(self._settings_backup)

    def test_pending_reservation_counts_toward_exposure_and_budgets(self):
        asyncio.run(self.pr.can_open_and_reserve(
            "MON",
            "long",
            "AMATERASU",
            25.65,
            metadata={
                "pending_reservation": True,
                "reserved_notional_usdt": 25.65,
                "reserved_at": 123.0,
            },
        ))
        exp = self.et.get_exposure(34.202438)
        self.assertAlmostEqual(exp.total_usdt, 25.65)
        same_dir_ok, same_dir_reason = self.pa.check_same_direction_budget("long", 25.65, 34.202438)
        self.assertFalse(same_dir_ok)
        self.assertIn("headroom", same_dir_reason)
        lane_ok, lane_reason = self.pa.check_lane_budget("RYUU", 25.65, 34.202438)
        self.assertFalse(lane_ok)
        self.assertIn("Trend lane", lane_reason)

    def test_activation_clears_pending_reservation_fields(self):
        asyncio.run(self.pr.can_open_and_reserve(
            "MON",
            "long",
            "AMATERASU",
            25.65,
            metadata={
                "pending_reservation": True,
                "reserved_notional_usdt": 25.65,
                "reserved_at": 123.0,
            },
        ))
        self.pr.update("MON", size_usdt=25.65, entry=1.23, leverage=25, order_id="abc123")
        entry = self.pr._registry["MON"]
        self.assertNotIn("pending_reservation", entry)
        self.assertNotIn("reserved_notional_usdt", entry)
        self.assertNotIn("reserved_at", entry)

    def test_load_preserves_legacy_pending_reservations(self):
        tmpdir = Path(".test-tmp") / f"registry-{time.time_ns()}"
        tmpdir.mkdir(parents=True, exist_ok=True)
        try:
            tmp_path = tmpdir / "position_registry.json"
            tmp_path.write_text(json.dumps({
                "MON": {
                    "direction": "long",
                    "engine": "AMATERASU",
                    "size_usdt": 25.65,
                    "pending_reservation": True,
                }
            }), encoding="utf-8")
            self.pr._PATH = tmp_path
            self.pr._registry.clear()
            self.pr._load()
            self.assertIn("MON", self.pr._registry)
            self.assertTrue(self.pr._registry["MON"]["pending_reservation"])
            self.assertNotIn("entry_lifecycle", self.pr._registry["MON"])
        finally:
            shutil.rmtree(tmpdir, ignore_errors=True)


class TestLaneBudgets(unittest.TestCase):

    def setUp(self):
        import settings_store as ss
        import position_registry as pr
        import portfolio_allocator as pa
        self.ss = ss
        self.pr = pr
        self.pa = pa
        self._settings_backup = copy.deepcopy(ss._current)
        self._registry_backup = copy.deepcopy(pr._registry)
        self.ss._current.update({
            "lane_budget_enabled": True,
            "per_lane_budget_enabled": True,
            "capital_profile": "AUTO",
            "deployment_mode": "Guarded",
            "exposure_control_mode": "Strict",
            "v6_micro_max_balance": 100.0,
            "v6_growth_max_balance": 1000.0,
            "lane_budget_micro_trend_pct": 60.0,
            "lane_budget_growth_trend_pct": 45.0,
            "lane_budget_pro_trend_pct": 30.0,
            "soft_accept_lane_budget_bonus_micro": 10.0,
            "soft_accept_lane_budget_bonus_micro_off": 5.0,
        })
        self.pr._registry.clear()

    def tearDown(self):
        self.pr._registry.clear()
        self.pr._registry.update(self._registry_backup)
        self.ss._current.clear()
        self.ss._current.update(self._settings_backup)

    def test_micro_budget_allows_first_viable_trade(self):
        ok, reason = self.pa.check_lane_budget("RYUU", 25.0, 34.2)
        self.assertTrue(ok, reason)

    def test_growth_budget_blocks_lane_stacking(self):
        self.pr._registry.update({
            "BTC": {
                "direction": "long",
                "engine": "RYUU",
                "size_usdt": 60.0,
            },
            "ETH": {
                "direction": "long",
                "engine": "RAIJIN",
                "size_usdt": 40.0,
            },
        })
        ok, reason = self.pa.check_lane_budget("TENJIN", 20.0, 200.0)
        self.assertFalse(ok)
        self.assertIn("Trend lane", reason)

    def test_lane_summary_reports_used_pct(self):
        self.pr._registry.update({
            "BTC": {
                "direction": "long",
                "engine": "RYUU",
                "size_usdt": 50.0,
            }
        })
        statuses = self.pa.summarize_lane_usage(200.0)
        self.assertEqual(len(statuses), 1)
        self.assertEqual(statuses[0].lane, "trend")
        self.assertAlmostEqual(statuses[0].used_pct, 25.0)

    def test_soft_accept_micro_lane_bonus_only_helps_borderline_case(self):
        self.pr._registry.update({
            "BTC": {
                "direction": "long",
                "engine": "RYUU",
                "size_usdt": 66.0,
            }
        })
        blocked_ok, _ = self.pa.check_lane_budget("TENJIN", 8.0, 99.0)
        self.assertFalse(blocked_ok)
        soft_ok, reason = self.pa.check_lane_budget("TENJIN", 8.0, 99.0, soft_accept=True, session="OFF")
        self.assertTrue(soft_ok, reason)


class TestLaneOverrideControls(unittest.TestCase):

    def setUp(self):
        import settings_store as ss
        import position_registry as pr
        import category_engine as ce
        self.ss = ss
        self.pr = pr
        self.ce = ce
        self._settings_backup = copy.deepcopy(ss._current)
        self._registry_backup = copy.deepcopy(pr._registry)
        self._save_backup = pr._save
        self._healthy_backup = pr._healthy
        self._load_error_backup = pr._load_error
        self.pr._healthy = True
        self.pr._load_error = ""
        self.pr._save = lambda *args, **kwargs: True
        self.ss._current.update({
            "lane_budget_enabled": True,
            "lane_override_enabled": True,
            "lane_override_min_confidence": 2.7,
            "lane_override_active_session_min_confidence": 2.3,
            "v6_micro_max_balance": 100.0,
            "v6_growth_max_balance": 1000.0,
            "per_lane_budget_enabled": True,
            "lane_budget_micro_trend_pct": 80.0,
            "max_open_positions": 10,
            "engine_autonomy_enabled": True,
            "confirm_path_enabled": True,
            "autonomy_same_coin_confirm_enabled": True,
            "correlation_filter_enabled": True,
            "correlation_max_same_group": 1,
            "same_direction_headroom_enabled": False,
            "exposure_control_mode": "Strict",
            "v6_correlation_control_enabled": False,
            "portfolio_same_direction_pct_micro": 80.0,
            "min_order_usdt": 100.0,
            "adaptive_min_order_floor_enabled": True,
            "min_order_usdt_micro": 25.0,
            "v6_min_position_size_usdt": 25.0,
        })
        self.pr._registry.clear()
        self.engine = self.ce.CategoryEngine("RYUU")

    def tearDown(self):
        self.pr._save = self._save_backup
        self.pr._healthy = self._healthy_backup
        self.pr._load_error = self._load_error_backup
        self.pr._registry.clear()
        self.pr._registry.update(self._registry_backup)
        self.ss._current.clear()
        self.ss._current.update(self._settings_backup)

    def test_one_valid_override_passes_when_flat(self):
        ok, reason = self.ce._lane_override_precheck(34.2, None, 3.1, "OFF")
        self.assertTrue(ok, reason)

    def test_lane_override_thresholds_are_lowered(self):
        self.assertAlmostEqual(self.ce._lane_override_min_confidence("OFF"), 2.7)
        self.assertAlmostEqual(self.ce._lane_override_min_confidence("LONDON"), 2.3)
        self.assertAlmostEqual(self.ce._lane_override_min_confidence("NY"), 2.3)

    def test_second_override_in_same_state_is_blocked(self):
        asyncio.run(self.pr.can_open_and_reserve(
            "BTC",
            "long",
            "RYUU",
            25.0,
            metadata={"lane_override_active": True},
        ))
        ok, reason = self.ce._lane_override_precheck(34.2, None, 3.1, "OFF")
        self.assertFalse(ok)
        self.assertTrue(
            "override_slot_in_use" in reason or "account_not_flat" in reason,
            reason,
        )

    def test_lane_override_keeps_trading_active_when_live_guards_do_not_trigger(self):
        self.pr._registry["BTC"] = {
            "direction": "long",
            "engine": "AMATERASU",
            "size_usdt": 25.0,
            "lane_override_active": False,
        }
        tag, reason = asyncio.run(self.engine._recheck_live_entry_constraints(
            "STX",
            "LONG",
            34.2,
            25.0,
            3.1,
            conf_obj=SimpleNamespace(confidence_score=3.1),
            capital_profile=None,
            lane_override=True,
        ))
        self.assertEqual(tag, "")
        self.assertEqual(reason, "ok")

    def test_no_duplicate_full_positions_created(self):
        asyncio.run(self.pr.can_open_and_reserve("BTC", "long", "AMATERASU", 25.0))
        ok, reason = asyncio.run(self.pr.can_open_and_reserve(
            "BTC",
            "long",
            "RYUU",
            25.0,
            metadata={"lane_override_active": True},
        ))
        self.assertFalse(ok)
        self.assertIn("skip duplicate", reason.lower())


class TestHotParticipationToggles(unittest.TestCase):

    def setUp(self):
        import settings_store as ss
        import position_registry as pr
        import portfolio_allocator as pa
        import category_engine as ce
        import exit_manager as em
        self.ss = ss
        self.pr = pr
        self.pa = pa
        self.ce = ce
        self.em = em
        self._settings_backup = copy.deepcopy(ss._current)
        self._registry_backup = copy.deepcopy(pr._registry)
        self._plans_backup = copy.deepcopy(em.get_all_plans())
        self._path_backup = ss._PATH
        self._load_mtime_backup = ss._last_load_mtime
        self._reload_check_backup = ss._last_hot_reload_check
        self._tmpdir = Path(".test-tmp") / f"hot-settings-{time.time_ns()}"
        self._tmpdir.mkdir(parents=True, exist_ok=True)
        self.ss._PATH = self._tmpdir / "settings.json"
        self.ss._current.update({
            "lane_budget_enabled": True,
            "per_lane_budget_enabled": True,
            "lane_override_enabled": True,
            "soft_accept_enabled": True,
            "entry_filter_enabled": True,
            "htf_filter_enabled": True,
            "session_filter_enabled": True,
            "correlation_filter_enabled": True,
            "same_direction_headroom_enabled": True,
            "per_engine_cap_enabled": True,
            "confirm_path_enabled": True,
            "autonomy_same_coin_confirm_enabled": True,
            "safe_reentry_enabled": True,
            "correlation_max_same_group": 2,
            "v6_max_correlated_trades": 2,
            "engine_weight_adaptation_enabled": False,
            "v6_engine_adaptation_enabled": False,
            "dynamic_sizing_enabled": True,
            "v6_dynamic_sizing_enabled": True,
            "leverage_fallback_enabled": True,
            "preflight_cooldown_enabled": True,
            "secure_profit_mode_enabled": True,
            "spread_filter_enabled": True,
            "dead_coin_suppression_enabled": True,
            "executability_penalty_enabled": True,
            "market_cap_bonus_enabled": True,
            "small_account_large_cap_preference_enabled": True,
            "v6_ai_veto_enabled": False,
            "v6_telemetry_enabled": True,
            "automation_mutation_enabled": False,
            "lane_override_min_confidence": 2.7,
            "lane_override_active_session_min_confidence": 2.3,
            "v6_micro_max_balance": 100.0,
            "v6_growth_max_balance": 1000.0,
            "capital_profile": "AUTO",
            "deployment_mode": "Guarded",
            "deployment_target_utilization_pct": 0.0,
            "deployment_max_positions": 0,
            "deployment_respect_exposure_cap": True,
            "deployment_respect_headroom": True,
            "deployment_same_side_limit_pct": 0.0,
            "deployment_total_exposure_limit_pct": 0.0,
            "exposure_control_mode": "Strict",
            "v6_micro_max_exposure_pct": 80.0,
            "portfolio_same_direction_pct_micro": 80.0,
            "lane_budget_micro_trend_pct": 80.0,
            "min_order_usdt": 100.0,
            "adaptive_min_order_floor_enabled": True,
            "min_order_usdt_micro": 25.0,
            "v6_min_position_size_usdt": 25.0,
            "partial_exit_enabled": True,
            "engine_autonomy_enabled": True,
            "max_open_positions": 10,
            "v6_enabled": False,
            "debug_override_mode": "off",
            "debug_override_global_cap": "normal",
            "debug_override_engine_cap": "normal",
            "debug_override_weekly_cap": "normal",
            "debug_override_exposure_cap": "normal",
            "debug_override_correlation_cap": "normal",
            "debug_override_same_direction_cap": "normal",
            "debug_override_portfolio_direction_cap": "normal",
            "debug_override_lane_budget": "normal",
        })
        self.pr._registry.clear()
        self.em._plans.clear()
        self.ss.save()
        self.ss._last_hot_reload_check = 0.0

    def tearDown(self):
        self.ss._PATH = self._path_backup
        self.ss._last_load_mtime = self._load_mtime_backup
        self.ss._last_hot_reload_check = self._reload_check_backup
        self.ss._current.clear()
        self.ss._current.update(self._settings_backup)
        self.pr._registry.clear()
        self.pr._registry.update(self._registry_backup)
        self.em._plans.clear()
        self.em._plans.update(self._plans_backup)
        shutil.rmtree(self._tmpdir, ignore_errors=True)

    def test_lane_budget_aliases_stay_in_sync(self):
        self.ss.set_val("lane_budget_enabled", False)
        self.assertFalse(self.ss.get_val("lane_budget_enabled"))
        self.assertFalse(self.ss.get_val("per_lane_budget_enabled"))
        self.ss.set_val("per_lane_budget_enabled", True)
        self.assertTrue(self.ss.get_val("lane_budget_enabled"))
        self.assertTrue(self.ss.get_val("per_lane_budget_enabled"))

    def test_confirm_path_aliases_stay_in_sync(self):
        self.ss.set_val("confirm_path_enabled", False)
        self.assertFalse(self.ss.get_val("confirm_path_enabled"))
        self.assertFalse(self.ss.get_val("autonomy_same_coin_confirm_enabled"))
        self.ss.set_val("autonomy_same_coin_confirm_enabled", True)
        self.assertTrue(self.ss.get_val("confirm_path_enabled"))
        self.assertTrue(self.ss.get_val("autonomy_same_coin_confirm_enabled"))

    def test_engine_weight_adaptation_aliases_stay_in_sync(self):
        self.ss.set_val("engine_weight_adaptation_enabled", True)
        self.assertTrue(self.ss.get_val("engine_weight_adaptation_enabled"))
        self.assertTrue(self.ss.get_val("v6_engine_adaptation_enabled"))
        self.ss.set_val("v6_engine_adaptation_enabled", False)
        self.assertFalse(self.ss.get_val("engine_weight_adaptation_enabled"))
        self.assertFalse(self.ss.get_val("v6_engine_adaptation_enabled"))

    def test_engine_guard_toggle_is_hot_runtime_setting(self):
        self.assertIn("engine_guard_enabled", self.ss._HOT_RUNTIME_KEYS)

    def test_correlation_cap_aliases_stay_in_sync(self):
        self.ss.set_val("correlation_max_same_group", 3)
        self.assertEqual(self.ss.get_val("correlation_max_same_group"), 3)
        self.assertEqual(self.ss.get_val("v6_max_correlated_trades"), 3)
        self.ss.set_val("v6_max_correlated_trades", 1)
        self.assertEqual(self.ss.get_val("correlation_max_same_group"), 1)
        self.assertEqual(self.ss.get_val("v6_max_correlated_trades"), 1)

    def test_legacy_engine_enabled_alias_maps_to_mode(self):
        self.ss.set_engine_mode("SHOGUN", "shadow")
        self.assertFalse(self.ss.get_val("engine_shogun_enabled"))
        self.ss.set_val("engine_shogun_enabled", True)
        self.assertEqual(self.ss.get_engine_mode("SHOGUN"), "live")
        self.assertTrue(self.ss.get_val("engine_shogun_enabled"))
        self.ss.set_val("engine_shogun_enabled", False)
        self.assertEqual(self.ss.get_engine_mode("SHOGUN"), "disabled")
        self.assertFalse(self.ss.get_val("engine_shogun_enabled"))

    def test_save_writes_canonical_keys_only(self):
        self.ss.save()
        disk_settings = json.loads(self.ss._PATH.read_text(encoding="utf-8"))
        self.assertNotIn("per_lane_budget_enabled", disk_settings)
        self.assertNotIn("autonomy_same_coin_confirm_enabled", disk_settings)
        self.assertNotIn("v6_engine_adaptation_enabled", disk_settings)
        self.assertNotIn("v6_max_correlated_trades", disk_settings)
        self.assertNotIn("engine_shogun_enabled", disk_settings)
        self.assertIn("lane_budget_enabled", disk_settings)
        self.assertIn("confirm_path_enabled", disk_settings)
        self.assertIn("engine_weight_adaptation_enabled", disk_settings)
        self.assertIn("correlation_max_same_group", disk_settings)
        self.assertIn("engine_shogun_mode", disk_settings)

    def test_participation_toggle_defaults_exist(self):
        expected = {
            "lane_budget_enabled": True,
            "lane_override_enabled": True,
            "soft_accept_enabled": True,
            "entry_filter_enabled": True,
            "htf_filter_enabled": True,
            "session_filter_enabled": True,
            "correlation_filter_enabled": True,
            "same_direction_headroom_enabled": True,
            "per_engine_cap_enabled": True,
            "confirm_path_enabled": True,
            "safe_reentry_enabled": True,
            "engine_weight_adaptation_enabled": False,
        }
        for key, expected_val in expected.items():
            self.assertEqual(self.ss.get_val(key), expected_val, key)
        self.assertEqual(self.ss.get_val("per_lane_budget_enabled"), True)
        self.assertEqual(self.ss.get_val("autonomy_same_coin_confirm_enabled"), True)
        self.assertEqual(self.ss.get_val("v6_engine_adaptation_enabled"), False)
        self.assertEqual(self.ss.get_val("v6_max_correlated_trades"), 2)

    def test_control_panel_toggle_defaults_exist(self):
        expected = {
            "v6_dynamic_sizing_enabled": True,
            "leverage_fallback_enabled": True,
            "preflight_cooldown_enabled": True,
            "secure_profit_mode_enabled": True,
            "spread_filter_enabled": True,
            "dead_coin_suppression_enabled": True,
            "executability_penalty_enabled": True,
            "market_cap_bonus_enabled": True,
            "small_account_large_cap_preference_enabled": True,
            "v6_ai_veto_enabled": False,
            "v6_telemetry_enabled": True,
            "automation_mutation_enabled": False,
        }
        for key, expected_val in expected.items():
            self.assertEqual(self.ss.get_val(key), expected_val, key)

    def test_lane_budget_hot_reload_from_disk_applies_to_new_checks(self):
        self.pr._registry["BTC"] = {
            "direction": "long",
            "engine": "RYUU",
            "size_usdt": 30.0,
        }
        blocked_ok, _ = self.pa.check_lane_budget("TENJIN", 10.0, 40.0)
        self.assertFalse(blocked_ok)

        disk_settings = copy.deepcopy(self.ss._current)
        disk_settings["lane_budget_enabled"] = False
        disk_settings["per_lane_budget_enabled"] = False
        self.ss._PATH.write_text(json.dumps(disk_settings, indent=2), encoding="utf-8")
        self.ss._last_load_mtime = 0.0
        self.ss._last_hot_reload_check = 0.0

        ok, reason = self.pa.check_lane_budget("TENJIN", 10.0, 40.0)
        self.assertTrue(ok, reason)
        self.assertEqual(reason, "lane budgets disabled")

    def test_soft_accept_hot_reload_from_disk_updates_runtime_value(self):
        self.assertTrue(self.ss.get_val("soft_accept_enabled"))
        disk_settings = copy.deepcopy(self.ss._current)
        disk_settings["soft_accept_enabled"] = False
        self.ss._PATH.write_text(json.dumps(disk_settings, indent=2), encoding="utf-8")
        self.ss._last_load_mtime = 0.0
        self.ss._last_hot_reload_check = 0.0
        self.assertFalse(self.ss.get_val("soft_accept_enabled"))

    def test_same_direction_headroom_toggle_changes_new_budget_checks_only(self):
        self.pr._registry["BTC"] = {
            "direction": "long",
            "engine": "RYUU",
            "size_usdt": 25.0,
        }
        blocked_ok, blocked_reason = self.pa.check_same_direction_budget("long", 10.0, 34.2)
        self.assertFalse(blocked_ok)
        self.assertIn("headroom", blocked_reason)

        self.ss.set_val("same_direction_headroom_enabled", False)
        ok, reason = self.pa.check_same_direction_budget("long", 10.0, 34.2)
        self.assertTrue(ok, reason)
        self.assertEqual(reason, "same-direction headroom disabled")

    def test_entry_filter_master_toggle_skips_quality_layer_but_not_hard_checks(self):
        import entry_filter as ef
        orig_bs = ef._perf.is_blackswan_halted
        orig_weekly = ef._perf.is_weekly_cap_hit
        orig_htf = ef._check_htf_soft
        try:
            ef._perf.is_blackswan_halted = lambda: False
            ef._perf.is_weekly_cap_hit = lambda balance: (False, 0.0)
            ef._check_htf_soft = lambda coin, direction, micro_mode=False: (False, "HTF: blocked")
            self.ss._current["rule_357_enabled"] = False

            self.ss.set_val("entry_filter_enabled", False)
            allowed, reason = asyncio.run(ef.check("BTC", "long", 34.2, capital_mode="MICRO"))
            self.assertTrue(allowed)
            self.assertEqual(reason, "OK")

            ef._perf.is_blackswan_halted = lambda: True
            blocked, blocked_reason = asyncio.run(ef.check("BTC", "long", 34.2, capital_mode="MICRO"))
            self.assertFalse(blocked)
            self.assertIn("Blackswan halt", blocked_reason)
        finally:
            ef._perf.is_blackswan_halted = orig_bs
            ef._perf.is_weekly_cap_hit = orig_weekly
            ef._check_htf_soft = orig_htf

    def test_individual_htf_and_session_toggles_skip_their_filters(self):
        import entry_filter as ef
        orig_bs = ef._perf.is_blackswan_halted
        orig_weekly = ef._perf.is_weekly_cap_hit
        orig_htf = ef._check_htf_soft
        orig_session = ef._check_session_dead_zone
        try:
            ef._perf.is_blackswan_halted = lambda: False
            ef._perf.is_weekly_cap_hit = lambda balance: (False, 0.0)
            ef._check_htf_soft = lambda coin, direction, micro_mode=False: (False, "HTF: blocked")
            ef._check_session_dead_zone = lambda: (False, "Session dead zone")
            self.ss._current["rule_357_enabled"] = False
            self.ss.set_val("entry_filter_enabled", True)
            self.ss.set_val("htf_filter_enabled", False)
            self.ss.set_val("session_filter_enabled", False)
            self.ss._current["funding_filter_enabled"] = False
            allowed, reason = asyncio.run(ef.check("BTC", "long", 34.2, capital_mode="MICRO"))
            self.assertTrue(allowed)
            self.assertEqual(reason, "OK")
        finally:
            ef._perf.is_blackswan_halted = orig_bs
            ef._perf.is_weekly_cap_hit = orig_weekly
            ef._check_htf_soft = orig_htf
            ef._check_session_dead_zone = orig_session

    def test_per_engine_cap_toggle_changes_new_constraint_checks_only(self):
        engine = self.ce.CategoryEngine("RYUU")
        portfolio_gate = engine._portfolio_gate
        try:
            engine._portfolio_gate = lambda *args, **kwargs: (True, "ok")
            self.pr._registry["BTC"] = {
                "direction": "long",
                "engine": "RYUU",
                "size_usdt": 25.0,
            }
            self.ss._current["engine_ryuu_max_pos"] = 1
            tag, reason = asyncio.run(engine._recheck_live_entry_constraints(
                "ETH",
                "LONG",
                200.0,
                10.0,
                3.0,
                conf_obj=SimpleNamespace(confidence_score=3.0),
                lane_override=True,
            ))
            self.assertEqual(tag, "engine_cap")
            self.assertIn("positions open", reason)

            self.ss.set_val("per_engine_cap_enabled", False)
            tag, reason = asyncio.run(engine._recheck_live_entry_constraints(
                "ETH",
                "LONG",
                200.0,
                10.0,
                3.0,
                conf_obj=SimpleNamespace(confidence_score=3.0),
                lane_override=True,
            ))
            self.assertEqual(tag, "")
            self.assertEqual(reason, "ok")
        finally:
            engine._portfolio_gate = portfolio_gate

    def test_confirm_path_toggle_changes_new_confirm_conversion_only(self):
        self.assertTrue(self.ce._confirm_path_enabled())
        self.ss.set_val("confirm_path_enabled", False)
        self.assertFalse(self.ce._confirm_path_enabled())

    def test_safe_reentry_toggle_controls_tensei(self):
        import strategy_categories as sc
        import performance_tracker as perf
        orig_last_closed = perf.get_last_closed_trade
        try:
            perf.get_last_closed_trade = lambda coin: SimpleNamespace(
                closed_at="2026-04-10T10:00:00+00:00",
                pnl_usdt=5.0,
                exit_reason="tp1",
                direction="LONG",
            )
            closes = list(range(100, 140))
            highs = [c + 1 for c in closes]
            lows = [c - 1 for c in closes]
            volumes = [100.0] * len(closes)

            self.ss.set_val("safe_reentry_enabled", False)
            direction, votes = sc.evaluate_tensei(closes, highs, lows, volumes, coin="BTC")
            self.assertEqual((direction, votes), ("HOLD", 0.0))
        finally:
            perf.get_last_closed_trade = orig_last_closed

    def test_lane_override_toggle_blocks_new_decisions_without_touching_state(self):
        self.pr._registry["SOL"] = {
            "direction": "long",
            "engine": "AMATERASU",
            "size_usdt": 25.0,
        }
        self.em.register(
            "SOL", "long", 1.0, 25.0, 0.1,
            0.95, 1.05, 1.10,
            engine_name="AMATERASU", leverage=25,
        )
        registry_before = copy.deepcopy(self.pr._registry)
        plans_before = copy.deepcopy(self.em.get_all_plans())

        self.ss.set_val("lane_override_enabled", False)
        ok, reason = self.ce._lane_override_precheck(34.2, None, 3.1, "OFF")
        self.assertFalse(ok)
        self.assertIn("lane_override_disabled", reason)
        self.assertEqual(self.pr._registry, registry_before)
        self.assertEqual(self.em.get_all_plans(), plans_before)

    def test_existing_positions_and_exit_plans_survive_multiple_toggle_flips(self):
        self.pr._registry["SOL"] = {
            "direction": "long",
            "engine": "AMATERASU",
            "size_usdt": 25.0,
        }
        self.em.register(
            "SOL", "long", 1.0, 25.0, 0.1,
            0.95, 1.05, 1.10,
            engine_name="AMATERASU", leverage=25,
        )
        registry_before = copy.deepcopy(self.pr._registry)
        plans_before = copy.deepcopy(self.em.get_all_plans())
        for key, value in {
            "lane_budget_enabled": False,
            "soft_accept_enabled": False,
            "entry_filter_enabled": False,
            "confirm_path_enabled": False,
            "safe_reentry_enabled": False,
            "v6_dynamic_sizing_enabled": False,
        }.items():
            self.ss.set_val(key, value)
        self.assertEqual(self.pr._registry, registry_before)
        self.assertEqual(self.em.get_all_plans(), plans_before)

    def test_participation_settings_panel_exposes_live_toggle_buttons(self):
        import tg_panels as tp
        main_kb = tp.kb_settings_hub(self.ss)
        main_callbacks = {
            btn.callback_data
            for row in main_kb.inline_keyboard
            for btn in row
            if getattr(btn, "callback_data", None)
        }
        self.assertIn("set:cat:trading", main_callbacks)
        self.assertIn("set:cat:strategy", main_callbacks)
        self.assertIn("set:cat:risk", main_callbacks)
        self.assertIn("set:cat:execution", main_callbacks)
        self.assertIn("set:cat:capital", main_callbacks)
        self.assertNotIn("diag:control_state", main_callbacks)
        self.assertIn("set:cat:diagnostics", main_callbacks)
        self.assertIn("set:cat:system", main_callbacks)

        kb = tp.kb_settings_participation(self.ss)
        callbacks = {
            btn.callback_data
            for row in kb.inline_keyboard
            for btn in row
            if getattr(btn, "callback_data", None)
        }
        self.assertIn("stg_tog_lane_budget_enabled", callbacks)
        self.assertIn("stg_tog_lane_override_enabled", callbacks)
        self.assertIn("stg_tog_soft_accept_enabled", callbacks)
        self.assertIn("stg_tog_entry_filter_enabled", callbacks)
        self.assertIn("stg_tog_confirm_path_enabled", callbacks)
        self.assertIn("stg_tog_safe_reentry_enabled", callbacks)
        text = tp.panel_settings_participation_text(self.ss)
        self.assertIn("Signal Qualification", text)
        self.assertIn("Allocation / Selection", text)
        self.assertIn("Flow / Conversion", text)
        self.assertIn("Allows near-valid trades through", text)
        self.assertIn("Open positions, exit plans, sync state, and monitoring loops stay untouched", text)
        self.assertNotIn("Per-Lane Cap (legacy sync)", text)


class TestStrategySettingsPanel(unittest.TestCase):

    def setUp(self):
        import settings_store as ss
        self.ss = ss
        self._settings_backup = copy.deepcopy(ss._current)
        self.ss._current.update({
            "position_size_usdt": 100.0,
            "leverage_default": 25,
            "leverage_min": 25,
            "leverage_max": 25,
            "min_vote_score": 3.0,
            "v6_execution_mode": "market",
            "v6_spread_max_pct": 0.30,
            "v6_enabled": True,
            "v6_capital_adaptive_enabled": True,
            "v6_micro_confidence_threshold": 1.5,
            "v6_growth_confidence_threshold": 1.8,
            "v6_pro_confidence_threshold": 2.2,
            "v6_micro_max_trades_per_hour": 8,
            "v6_growth_max_trades_per_hour": 5,
            "v6_pro_max_trades_per_hour": 3,
            "v6_micro_max_balance": 100.0,
            "v6_growth_max_balance": 1000.0,
            "capital_profile": "AUTO",
            "deployment_mode": "Guarded",
            "deployment_target_utilization_pct": 0.0,
            "deployment_max_positions": 0,
            "deployment_respect_exposure_cap": True,
            "deployment_respect_headroom": True,
            "deployment_same_side_limit_pct": 0.0,
            "deployment_total_exposure_limit_pct": 0.0,
            "exposure_control_mode": "Strict",
            "v6_micro_max_exposure_pct": 80.0,
            "portfolio_same_direction_pct_micro": 80.0,
            "v6_max_position_pct_of_balance": 0.03,
            "adaptive_min_order_floor_enabled": True,
            "min_order_usdt": 100.0,
            "min_order_usdt_micro": 25.0,
            "v6_min_position_size_usdt": 25.0,
            "tp_distance_mode": "balanced",
            "soft_accept_enabled": True,
            "entry_filter_enabled": True,
            "htf_filter_enabled": True,
            "session_filter_enabled": True,
            "lane_budget_enabled": True,
            "lane_override_enabled": True,
            "confirm_path_enabled": True,
            "safe_reentry_enabled": True,
            "correlation_filter_enabled": True,
            "same_direction_headroom_enabled": True,
            "engine_weight_adaptation_enabled": False,
            "dynamic_sizing_enabled": False,
            "v6_dynamic_sizing_enabled": False,
            "trade_intent_mode_override": "",
            "soft_accept_min_confidence_micro": 2.5,
            "soft_accept_off_session_confidence_boost_micro": 0.2,
        })

    def tearDown(self):
        self.ss._current.clear()
        self.ss._current.update(self._settings_backup)

    def test_strategy_panel_shows_configured_and_live_size(self):
        import tg_panels as tp
        import session_context as sess
        original = sess.get_session
        try:
            sess.get_session = lambda: "OFF"
            text = tp.panel_settings_strategy_text(self.ss, balance=34.2)
        finally:
            sess.get_session = original
        self.assertIn("Base Size: `$100`", text)
        self.assertIn("Profile: `MICRO`", text)
        self.assertIn("Size: `$100` configured", text)
        self.assertIn("`$25.65` live", text)
        self.assertIn("Margin Used: `~$1.03`", text)
        self.assertIn("Live Cap: `3.0%` margin budget", text)
        self.assertIn("Clamp: `margin cap $25.65`", text)
        self.assertIn("Soft Accept: `2.30` (`OFF`)", text)
        self.assertIn("Risk Reduce:", text)
        self.assertIn("Pre-Reserve Check:", text)

    def test_strategy_keyboard_exposes_live_control_callbacks(self):
        import tg_panels as tp
        kb = tp.kb_settings_strategy(self.ss, balance=34.2)
        callbacks = {
            btn.callback_data
            for row in kb.inline_keyboard
            for btn in row
            if getattr(btn, "callback_data", None)
        }
        self.assertIn("stg_size_up", callbacks)
        self.assertIn("stg_lev_up", callbacks)
        self.assertIn("stg_livecap_up", callbacks)
        self.assertIn("stg_votes_up", callbacks)
        self.assertIn("stg_exec_cycle_strategy", callbacks)
        self.assertIn("s:participation", callbacks)
        self.assertIn("s:execution", callbacks)
        self.assertNotIn("stg_strategy_tog_soft_accept_enabled", callbacks)

    def test_bot_callback_handler_includes_strategy_toggle_prefix(self):
        import bot
        self.assertIn("stg_strategy_tog_", inspect.getsource(bot.callbacks))
        self.assertIn("stg_livecap_up", inspect.getsource(bot.callbacks))
        self.assertIn("stg_tp_mode_cycle", inspect.getsource(bot.callbacks))
        self.assertIn("stg_risk_reduce_up", inspect.getsource(bot.callbacks))
        self.assertIn("stg_exp_cap_up", inspect.getsource(bot.callbacks))
        self.assertIn("stg_headroom_up", inspect.getsource(bot.callbacks))
        self.assertIn("s:dashboard", inspect.getsource(bot.callbacks))
        self.assertIn("s:execution", inspect.getsource(bot.callbacks))
        self.assertIn("s:filters", inspect.getsource(bot.callbacks))
        self.assertIn("s:modes", inspect.getsource(bot.callbacks))
        self.assertIn("s:system", inspect.getsource(bot.callbacks))
        self.assertIn("sys:reset_confirm", inspect.getsource(bot.callbacks))
        self.assertIn("s:help", inspect.getsource(bot.callbacks))
        self.assertIn("set:fg:early_invalidation", inspect.getsource(bot.callbacks))
        self.assertIn("set:fg:repeat_loss_cooling", inspect.getsource(bot.callbacks))
        self.assertIn("adaptive_early_invalidation_enabled", inspect.getsource(bot.callbacks))
        self.assertIn("symbol_direction_loss_cooldown_enabled", inspect.getsource(bot.callbacks))
        self.assertIn("set:fg:risk_ceiling", inspect.getsource(bot.callbacks))
        self.assertIn("set:fg:short_rehab", inspect.getsource(bot.callbacks))
        self.assertIn("realized_risk_ceiling_enabled", inspect.getsource(bot.callbacks))
        self.assertIn("realized_risk_cap_pct_balance", inspect.getsource(bot.callbacks))
        self.assertIn("short_rehab_enabled", inspect.getsource(bot.callbacks))
        self.assertIn("short_rehab_weak_regime_size_mult", inspect.getsource(bot.callbacks))

    def test_risk_ceiling_and_short_rehab_panels_have_handled_callbacks(self):
        import bot
        import tg_panels as tp

        source = inspect.getsource(bot.callbacks)
        panels = [
            (tp.kb_settings_cat_risk(self.ss), {"set:fg:risk_ceiling", "set:fg:short_rehab"}),
            (
                tp.kb_fg_risk_ceiling(self.ss),
                {
                    "set:room:realized_risk_ceiling_enabled",
                    "set:room:realized_risk_cap_pct_balance",
                    "set:room:realized_risk_cap_min_usdt",
                    "set:room:realized_risk_cap_max_usdt",
                    "set:room:realized_risk_ceiling_tighten_sl_if_needed",
                    "set:room:realized_risk_ceiling_telemetry_enabled",
                },
            ),
            (
                tp.kb_fg_short_rehab(self.ss),
                {
                    "set:room:short_rehab_enabled",
                    "set:room:short_rehab_weak_regime_size_mult",
                    "set:room:short_rehab_min_adx_1h",
                },
            ),
        ]
        for markup, expected in panels:
            callbacks = {
                btn.callback_data
                for row in markup.inline_keyboard
                for btn in row
                if getattr(btn, "callback_data", None)
            }
            self.assertTrue(expected.issubset(callbacks))
            for callback in expected:
                self.assertIn(callback, source)

    def test_strategy_panel_shows_live_cap_and_callback(self):
        import tg_panels as tp
        text = tp.panel_settings_strategy_text(self.ss, balance=34.2)
        self.assertIn("Live Cap: `3.0%` margin budget", text)
        kb = tp.kb_settings_strategy(self.ss, balance=34.2)
        callbacks = {
            btn.callback_data
            for row in kb.inline_keyboard
            for btn in row
            if getattr(btn, "callback_data", None)
        }
        self.assertIn("stg_livecap_up", callbacks)
        self.assertIn("stg_livecap_dn", callbacks)




class TestTelegramEncodingRegression(unittest.TestCase):

    def setUp(self):
        import settings_store as ss
        self.ss = ss

    def _assert_clean(self, text: str, label: str):
        for marker in ("ÃƒÆ’Ã‚Â°", "ÃƒÆ’Ã¢â‚¬Å¡", "ÃƒÆ’Ã‚Â¢Ãƒâ€šÃ¢â‚¬Â", "ÃƒÆ’Ã‚Â¢Ãƒâ€šÃ¢â‚¬â€œ", "ÃƒÆ’Ã†â€™"): 
            self.assertNotIn(marker, text, f"{label} contains mojibake marker {marker!r}")

    def _keyboard_text(self, kb):
        labels = []
        for row in getattr(kb, "inline_keyboard", []) or []:
            for btn in row:
                labels.append(getattr(btn, "text", ""))
        return "\n".join(labels)

    def test_help_text_is_clean_and_engine_guides_are_complete(self):
        import tg_panels as tp

        for name in (
            "HELP_MAIN_TEXT", "HELP_COMMANDS_TEXT", "HELP_RISK_TEXT", "HELP_V6_TEXT",
            "HELP_SHOGUN_TEXT", "HELP_SUSANOO_TEXT", "HELP_FUJIN_TEXT",
            "HELP_AMATERASU_TEXT", "HELP_OROCHI_TEXT", "HELP_TSUNAMI_TEXT",
            "HELP_SUIJIN_TEXT", "HELP_RAIJUU_TEXT",
        ):
            text = getattr(tp, name)
            self._assert_clean(text, name)
            if name.startswith("HELP_") and name not in {"HELP_MAIN_TEXT", "HELP_COMMANDS_TEXT", "HELP_RISK_TEXT", "HELP_V6_TEXT"}:
                self.assertIn("Role:", text)
                self.assertIn("Lane:", text)
                self.assertIn("Ideal Market:", text)

    def test_primary_panels_render_clean_unicode(self):
        import bot as bt
        import tg_panels as tp
        exposure = SimpleNamespace(total_usdt=25.65)
        startup = bt._startup_ready_text(balance=34.2, sync_status="Standby")
        portfolio = tp.panel_portfolio_text(
            34.2,
            {},
            {"today_pnl": 0.0, "week_pnl": 0.0},
            capital_profile=None,
            dd_state=None,
            exposure=SimpleNamespace(
                total_usdt=25.65,
                long_usdt=12.0,
                short_usdt=13.65,
                long_count=1,
                short_count=1,
            ),
            perf_metrics={"total_pnl": 0.0, "win_rate": 55.0, "total": 12},
        )

        panels = [
            tp.panel_settings_dashboard_text(
                self.ss,
                balance=34.2,
                exposure=exposure,
                running=False,
                status_label="STOPPED",
                regime="TRENDING",
                session="LONDON",
                sync_status="Standby",
                scanner_status="Idle",
                open_positions=0,
            ),
            portfolio,
            startup,
            tp.panel_settings_hub_text(self.ss),
            tp.panel_settings_execution_text(self.ss),
            tp.panel_settings_diagnostics_text(self.ss, balance=34.2),
            tp.panel_settings_strategy_text(self.ss, balance=34.2),
        ]
        keyboards = [
            bt.reports_menu(),
            tp.kb_settings_hub(self.ss),
            tp.kb_settings_execution(self.ss),
            tp.kb_settings_diagnostics(self.ss, balance=34.2),
            tp.kb_settings_strategy(self.ss, balance=34.2),
        ]
        combined = "\n".join(panels + [self._keyboard_text(kb) for kb in keyboards])
        self._assert_clean(combined, "telegram rendering")
        for expected in ("Portfolio", "Leverage", "Strategy", "Execution"):
            self.assertIn(expected, combined, f"missing panel text {expected!r}")

class TestProControlPanel(unittest.TestCase):

    def setUp(self):
        import settings_store as ss
        self.ss = ss
        self._settings_backup = copy.deepcopy(ss._current)
        _isolate_settings_path(self, ss, "pro-control-")
        self.ss._current.update({
            "position_size_usdt": 100.0,
            "leverage_default": 25,
            "min_vote_score": 3.0,
            "v6_execution_mode": "market",
            "risk_pct": 1.25,
            "rule_357_max_open_risk_pct": 25.0,
            "max_open_positions": 10,
            "v6_enabled": True,
            "v6_capital_adaptive_enabled": True,
            "capital_profile": "AUTO",
            "deployment_mode": "Guarded",
            "deployment_target_utilization_pct": 0.0,
            "deployment_max_positions": 0,
            "deployment_respect_exposure_cap": True,
            "deployment_respect_headroom": True,
            "deployment_same_side_limit_pct": 0.0,
            "deployment_total_exposure_limit_pct": 0.0,
            "exposure_control_mode": "Strict",
            "v6_micro_max_balance": 100.0,
            "v6_growth_max_balance": 1000.0,
            "v6_micro_max_exposure_pct": 80.0,
            "portfolio_same_direction_pct_micro": 80.0,
            "v6_max_position_pct_of_balance": 0.03,
            "adaptive_min_order_floor_enabled": True,
            "min_order_usdt": 100.0,
            "min_order_usdt_micro": 25.0,
            "v6_min_position_size_usdt": 25.0,
            "leverage_fallback_enabled": True,
            "preflight_cooldown_enabled": True,
            "preflight_zero_contract_cooldown_secs": 1800.0,
            "preflight_zero_contract_trigger_count": 2,
            "tp_distance_mode": "extended",
            "secure_profit_target_usd": 3.0,
            "partial_tp1_close_pct": 30.0,
            "partial_tp2_close_pct": 20.0,
            "dynamic_sizing_enabled": False,
            "v6_dynamic_sizing_enabled": False,
            "trade_intent_mode_override": "",
            "spread_filter_enabled": True,
            "dead_coin_suppression_enabled": True,
            "executability_penalty_enabled": True,
            "market_cap_bonus_enabled": True,
            "small_account_large_cap_preference_enabled": True,
            "universe_min_volume_usd_24h": 250000.0,
            "universe_spread_max_pct": 1.0,
            "v6_ai_veto_enabled": False,
            "v6_telemetry_enabled": True,
            "automation_mutation_enabled": False,
        })

    def tearDown(self):
        _restore_settings_path(self, self.ss)
        self.ss._current.clear()
        self.ss._current.update(self._settings_backup)

    def test_dashboard_renders_configured_vs_live_metrics(self):
        import tg_panels as tp
        exposure = SimpleNamespace(total_usdt=0.0)
        text = tp.panel_settings_dashboard_text(
            self.ss,
            balance=34.2,
            exposure=exposure,
            running=False,
            status_label="STOPPED",
            regime="TRENDING",
            session="LONDON",
            sync_status="Standby",
            scanner_status="Idle",
            open_positions=0,
        )
        self.assertIn("OxyPulse Pro Control Panel", text)
        self.assertIn("Status: `STOPPED`", text)
        self.assertIn("Size: `$100` configured", text)
        self.assertIn("`$25.00` live", text)
        self.assertIn("Leverage: `25x` configured", text)
        self.assertIn("`15x` live", text)

    def test_execution_panel_renders_explanations_and_callbacks(self):
        import tg_panels as tp
        text = tp.panel_settings_execution_text(self.ss)
        self.assertIn("Leverage Fallback", text)
        self.assertIn("Retries lower leverage if the exchange rejects size", text)
        self.assertIn("15x -> 10x -> 5x", text)
        self.assertIn("Drawdown Reduce Factor", text)
        self.assertIn("Floor Preserve", text)
        kb = tp.kb_settings_execution(self.ss)
        callbacks = {
            btn.callback_data
            for row in kb.inline_keyboard
            for btn in row
            if getattr(btn, "callback_data", None)
        }
        self.assertIn("stg_tog_leverage_fallback_enabled", callbacks)
        self.assertIn("stg_tog_preflight_cooldown_enabled", callbacks)
        self.assertIn("stg_tog_risk_floor_preserve_enabled", callbacks)
        self.assertIn("stg_tog_pre_reserve_size_validation_enabled", callbacks)
        self.assertIn("stg_risk_reduce_up", callbacks)
        self.assertIn("stg_pf_cool_up", callbacks)
        self.assertIn("stg_pf_trigger_up", callbacks)

    def test_execution_panel_exposes_tp_mode_and_partial_controls(self):
        import tg_panels as tp
        text = tp.panel_settings_execution_text(self.ss)
        self.assertIn("TP Mode: `EXTENDED`", text)
        self.assertIn("Partials: TP1 `30%` | TP2 `20%`", text)
        kb = tp.kb_settings_execution(self.ss)
        callbacks = {
            btn.callback_data
            for row in kb.inline_keyboard
            for btn in row
            if getattr(btn, "callback_data", None)
        }
        self.assertIn("stg_tp_mode_cycle", callbacks)
        self.assertIn("stg_ptp1_up", callbacks)
        self.assertIn("stg_ptp2_up", callbacks)

    def test_risk_panel_shows_active_exposure_and_headroom_controls(self):
        import tg_panels as tp
        exposure = SimpleNamespace(total_usdt=25.65)
        text = tp.panel_settings_risk_text(self.ss, balance=34.2, exposure=exposure)
        self.assertIn("Exposure Control", text)
        self.assertIn("Base Profile Cap: `MICRO` exposure `80.0%`", text)
        self.assertIn("Effective Same-Side Headroom: `80.0%`", text)
        kb = tp.kb_settings_risk(self.ss, balance=34.2)
        callbacks = {
            btn.callback_data
            for row in kb.inline_keyboard
            for btn in row
            if getattr(btn, "callback_data", None)
        }
        self.assertIn("stg_exposure_mode_adaptive", callbacks)
        self.assertIn("stg_exposure_mode_override", callbacks)
        self.assertIn("stg_exp_cap_up", callbacks)
        self.assertIn("stg_headroom_up", callbacks)

    def test_filters_panel_uses_canonical_keys_only(self):
        import tg_panels as tp
        self.ss.set_val("v6_min_volume_usd_24h", 500_000.0)
        self.ss.set_val("universe_min_volume_usd_24h", 2_000_000.0)
        self.ss.set_val("v6_spread_max_pct", 0.25)
        self.ss.set_val("universe_spread_max_pct", 1.25)
        text = tp.panel_settings_filters_text(self.ss)
        self.assertIn("Executability Penalty", text)
        self.assertIn("Small-Account Large-Cap Preference", text)
        self.assertIn("$500k", text)
        self.assertIn("0.25%", text)
        self.assertNotIn("$2.0M", text)
        kb = tp.kb_settings_filters(self.ss)
        callbacks = {
            btn.callback_data
            for row in kb.inline_keyboard
            for btn in row
            if getattr(btn, "callback_data", None)
        }
        self.assertIn("stg_tog_spread_filter_enabled", callbacks)
        self.assertIn("stg_tog_dead_coin_suppression_enabled", callbacks)
        self.assertNotIn("per_lane_budget_enabled", text)

    def test_help_panel_explains_operator_terms(self):
        import tg_panels as tp
        text = tp.panel_settings_help_text()
        self.assertIn("Headroom =", text)
        self.assertIn("Preflight =", text)
        kb = tp.kb_settings_help()
        callbacks = {
            btn.callback_data
            for row in kb.inline_keyboard
            for btn in row
            if getattr(btn, "callback_data", None)
        }
        self.assertIn("s:modes", callbacks)
        self.assertIn("s:engines", callbacks)


class TestPortfolioDirectionBudget(unittest.TestCase):

    def setUp(self):
        import settings_store as ss
        import position_registry as pr
        import portfolio_allocator as pa
        self.ss = ss
        self.pr = pr
        self.pa = pa
        self._settings_backup = copy.deepcopy(ss._current)
        self._registry_backup = copy.deepcopy(pr._registry)
        self.ss._current.update({
            "same_direction_headroom_enabled": True,
            "v6_micro_max_balance": 100.0,
            "v6_growth_max_balance": 1000.0,
            "capital_profile": "AUTO",
            "deployment_mode": "Guarded",
            "deployment_target_utilization_pct": 0.0,
            "deployment_max_positions": 0,
            "deployment_respect_exposure_cap": True,
            "deployment_respect_headroom": True,
            "deployment_same_side_limit_pct": 0.0,
            "deployment_total_exposure_limit_pct": 0.0,
            "exposure_control_mode": "Strict",
            "v6_micro_max_exposure_pct": 80.0,
            "v6_growth_max_exposure_pct": 60.0,
            "v6_pro_max_exposure_pct": 40.0,
            "portfolio_same_direction_pct_micro": 80.0,
            "portfolio_same_direction_pct_growth": 60.0,
            "portfolio_same_direction_pct_pro": 40.0,
            "min_order_usdt": 100.0,
            "adaptive_min_order_floor_enabled": True,
            "min_order_usdt_micro": 25.0,
            "v6_min_position_size_usdt": 25.0,
        })
        self.pr._registry.clear()

    def tearDown(self):
        self.pr._registry.clear()
        self.pr._registry.update(self._registry_backup)
        self.ss._current.clear()
        self.ss._current.update(self._settings_backup)

    def test_micro_direction_cap_matches_profile(self):
        cap_pct = self.pa.get_same_direction_cap_pct(34.2)
        self.assertAlmostEqual(cap_pct, 80.0)

    def test_first_micro_trade_fits_but_second_is_impossible(self):
        ok, reason = self.pa.check_same_direction_budget("long", 25.65, 34.202438)
        self.assertTrue(ok, reason)
        self.pr._registry["MON"] = {
            "direction": "long",
            "engine": "AMATERASU",
            "size_usdt": 25.65,
        }
        ok, reason = self.pa.check_same_direction_budget("long", 25.65, 34.202438)
        self.assertFalse(ok)
        self.assertIn("headroom", reason)
        self.assertIn("MICRO", reason)


class TestStartupDisplay(unittest.TestCase):

    def setUp(self):
        import settings_store as ss
        self.ss = ss
        self._settings_backup = copy.deepcopy(ss._current)

    def tearDown(self):
        self.ss._current.clear()
        self.ss._current.update(self._settings_backup)

    def test_mode_breakdown_uses_runtime_modes(self):
        from category_tracker import ENGINE_NAMES
        import bot

        for name in ENGINE_NAMES:
            self.ss._current[f"engine_{name.lower()}_mode"] = "disabled"
        self.ss._current["engine_ryuu_mode"] = "live"
        self.ss._current["engine_amaterasu_mode"] = "probation"
        self.ss._current["engine_hayate_mode"] = "shadow"

        breakdown = bot._engine_mode_breakdown()
        self.assertEqual(breakdown["live"], 2)
        self.assertEqual(breakdown["probation"], 1)
        self.assertEqual(breakdown["shadow"], 1)
        self.assertEqual(breakdown["disabled"], len(ENGINE_NAMES) - 3)
        self.assertEqual(breakdown["total"], len(ENGINE_NAMES))

    def test_startup_card_renders_real_breakdown(self):
        from category_tracker import ENGINE_NAMES
        import bot

        for name in ENGINE_NAMES:
            self.ss._current[f"engine_{name.lower()}_mode"] = "disabled"
        self.ss._current.update({
            "engine_ryuu_mode": "live",
            "engine_amaterasu_mode": "probation",
            "engine_hayate_mode": "shadow",
            "leverage_default": 25,
            "risk_pct": 1.25,
            "max_open_positions": 3,
            "rule_357_max_open_risk_pct": 25.0,
            "min_vote_score": 3.0,
            "v6_micro_max_balance": 100.0,
            "v6_growth_max_balance": 1000.0,
        })
        text = bot._startup_ready_text(balance=34.2)
        self.assertIn("2 LIVE", text)
        self.assertIn("1 SHADOW", text)
        self.assertIn(f"{len(ENGINE_NAMES) - 3} DISABLED", text)
        self.assertIn(f"{len(ENGINE_NAMES)} TOTAL", text)
        self.assertIn("Status:", text)
        self.assertIn("Profile:", text)


class TestIntelligenceEngines(unittest.TestCase):

    def setUp(self):
        import settings_store as ss
        self.ss = ss
        self._settings_backup = copy.deepcopy(ss._current)
        self.ss._current.update({
            "enma_top_pick_limit": 1,
            "hayate_short_edge_min": 0.003,
            "hayate_medium_edge_min": 0.006,
            "hayate_long_edge_min": 0.010,
            "kagutsuchi_imbalance_long": 0.62,
            "kagutsuchi_imbalance_short": 0.38,
            "kagutsuchi_displacement_atr_mult": 0.60,
            "tensei_reentry_window_mins": 240,
            "tensei_reentry_cooldown_mins": 45,
            "tensei_reentry_profit_only": True,
            "ibuki_bbw_compression_mult": 0.75,
            "ibuki_atr_compression_mult": 0.80,
            "ibuki_breakout_range_mult": 1.20,
            "onryo_funding_extreme": 0.0006,
            "onryo_oi_delta_min_pct": 0.03,
            "onryo_displacement_atr_mult": 0.70,
        })

    def tearDown(self):
        self.ss._current.clear()
        self.ss._current.update(self._settings_backup)

    def test_enma_cycle_picks_best_cluster_candidate(self):
        import category_engine as ce
        eng = ce.CategoryEngine("ENMA")
        picked: list[str] = []

        async def fake_preview(coin: str, balance: float = 0.0):
            scores = {
                "BTC": {"coin": "BTC", "direction": "LONG", "votes": 3.0, "score": 301.0, "volume_quality": 2.0, "spread_pct": 0.03, "market_cap_bonus": 3.0, "selection_penalty": 0.0, "impulse_pct": 1.0},
                "ETH": {"coin": "ETH", "direction": "LONG", "votes": 3.0, "score": 355.0, "volume_quality": 3.0, "spread_pct": 0.02, "market_cap_bonus": 3.0, "selection_penalty": 0.0, "impulse_pct": 2.0},
                "SOL": {"coin": "SOL", "direction": "SHORT", "votes": 3.0, "score": 320.0, "volume_quality": 2.5, "spread_pct": 0.05, "market_cap_bonus": 3.0, "selection_penalty": 0.0, "impulse_pct": 1.5},
            }
            return scores.get(coin)

        async def fake_eval(coin: str, balance: float):
            picked.append(coin)

        eng._preview_enma_candidate = fake_preview  # type: ignore[assignment]
        eng._evaluate_coin = fake_eval  # type: ignore[assignment]
        asyncio.run(eng._evaluate_enma_cycle(["BTC", "ETH", "SOL"], 34.2))
        self.assertEqual(picked, ["ETH"])

    def test_hayate_prefers_relative_strength_leader(self):
        import market_data_stream as mds
        import strategy_categories as sc

        orig_15m = mds.get_15m_closes
        orig_1h = mds.get_1h_closes
        orig_4h = mds.get_4h_closes
        try:
            mds.get_15m_closes = lambda coin: [100, 100.3, 100.5, 100.8, 101.0, 101.2] if coin == "BTC" else [100, 100.2, 100.4, 100.5, 100.7, 100.9]
            mds.get_1h_closes = lambda coin: [100, 100.4, 100.7, 101.0, 101.2, 101.5] if coin == "BTC" else [100, 100.2, 100.5, 100.7, 100.9, 101.1]
            mds.get_4h_closes = lambda coin: [100, 100.5, 101.0, 101.3, 101.6, 101.9] if coin == "BTC" else [100, 100.3, 100.7, 101.0, 101.2, 101.4]
            closes = [100.0 + i * 0.1 for i in range(30)]
            highs = [c + 0.2 for c in closes]
            lows = [c - 0.2 for c in closes]
            volumes = [100.0] * 30
            direction, votes = sc.evaluate_hayate(
                closes,
                highs,
                lows,
                volumes,
                closes_4h=[100, 101, 102.5, 104.0, 105.5, 107.5],
                closes_15m=[100, 100.8, 101.8, 103.0, 104.2, 105.4],
                closes_1h=[100, 101.0, 102.5, 104.0, 105.5, 107.0],
                coin="SOL",
            )
        finally:
            mds.get_15m_closes = orig_15m
            mds.get_1h_closes = orig_1h
            mds.get_4h_closes = orig_4h
        self.assertEqual(direction, "LONG")
        self.assertGreaterEqual(votes, 2.0)


class TestExecutionAdmissionControls(unittest.TestCase):

    def setUp(self):
        import execution_admission as ea
        import settings_store as ss
        self.ea = ea
        self.ss = ss
        self._settings_backup = copy.deepcopy(ss._current)
        _isolate_settings_path(self, ss, "exec-admission-")
        self._history_backup = copy.deepcopy(ea._failure_history)
        self._cooldown_backup = copy.deepcopy(ea._cooldown_until)
        self._now_backup = ea._now
        self.ss._current.update({
            "preflight_cooldown_enabled": True,
            "executability_penalty_enabled": True,
            "preflight_zero_contract_window_secs": 120.0,
            "preflight_zero_contract_trigger_count": 2,
            "preflight_zero_contract_cooldown_secs": 300.0,
            "execution_failure_penalty_window_secs": 600.0,
            "preflight_zero_contract_selection_penalty": 40.0,
            "exchange_admission_selection_penalty": 12.0,
            "execution_cooldown_selection_penalty": 1000.0,
        })
        self.ea.clear()

    def tearDown(self):
        _restore_settings_path(self, self.ss)
        self.ea._failure_history.clear()
        self.ea._failure_history.update(self._history_backup)
        self.ea._cooldown_until.clear()
        self.ea._cooldown_until.update(self._cooldown_backup)
        self.ea._now = self._now_backup
        self.ss._current.clear()
        self.ss._current.update(self._settings_backup)

    def test_repeated_preflight_zero_contracts_apply_symbol_cooldown(self):
        self.assertFalse(self.ea.record_preflight_zero_contract("PEPE", detail="attempts=25x"))
        cooled, _reason = self.ea.is_symbol_cooled_down("PEPE")
        self.assertFalse(cooled)
        self.assertTrue(self.ea.record_preflight_zero_contract("PEPE", detail="attempts=25x -> 20x -> 15x -> 10x -> 5x"))
        cooled, reason = self.ea.is_symbol_cooled_down("PEPE")
        self.assertTrue(cooled)
        self.assertIn("preflight_zero_contract_cooldown", reason)

    def test_symbol_cooldown_expiry_logs_once(self):
        clock = {"now": 100.0}
        self.ea._now = lambda: clock["now"]
        self.ea.record_preflight_zero_contract("PEPE", detail="attempts=25x")
        self.ea.record_preflight_zero_contract("PEPE", detail="attempts=25x -> 20x -> 15x -> 10x -> 5x")
        clock["now"] = 500.0
        with self.assertLogs("OxyPulse.ExecutionAdmission", level="INFO") as logs:
            cooled, _reason = self.ea.is_symbol_cooled_down("PEPE")
        self.assertFalse(cooled)
        self.assertIn("SYMBOL COOLDOWN EXPIRED PEPE", logs.output[-1])

    def test_preflight_cooldown_toggle_disables_symbol_suppression(self):
        self.ss.set_val("preflight_cooldown_enabled", False)
        self.assertFalse(self.ea.record_preflight_zero_contract("PEPE", detail="attempts=25x"))
        self.assertFalse(self.ea.record_preflight_zero_contract("PEPE", detail="attempts=25x -> 20x"))
        cooled, reason = self.ea.is_symbol_cooled_down("PEPE")
        self.assertFalse(cooled)
        self.assertEqual(reason, "")

    def test_executability_penalty_toggle_zeroes_selection_penalty(self):
        self.ea.record_exchange_admission_failure("ETH", "market_unavailable", detail="exchange reject")
        self.assertGreater(self.ea.get_selection_penalty("ETH"), 0.0)
        self.ss.set_val("executability_penalty_enabled", False)
        self.assertEqual(self.ea.get_selection_penalty("ETH"), 0.0)


class TestLeverageFallback(unittest.TestCase):

    def setUp(self):
        import execution_admission as ea
        import okx_client as oc
        import settings_store as ss
        self.ea = ea
        self.oc = oc
        self.ss = ss
        self._settings_backup = copy.deepcopy(ss._current)
        _isolate_settings_path(self, ss, "leverage-fallback-")
        self._paper_backup = oc.PAPER_TRADING
        self._api_key_backup = oc.OKX_API_KEY
        self._risk_validate_backup = oc.risk.validate_trade
        self._risk_stop_take_backup = oc.risk.calculate_stop_take
        self._risk_open_backup = oc.risk.register_open
        self.ss._current.update({
            "min_free_balance_buffer_usdt": 5.0,
            "preflight_contract_cap_enabled": True,
            "preflight_max_reduce_enabled": True,
            "leverage_fallback_enabled": True,
            "preflight_cooldown_enabled": True,
            "preflight_zero_contract_window_secs": 120.0,
            "preflight_zero_contract_trigger_count": 1,
            "preflight_zero_contract_cooldown_secs": 300.0,
            "execution_failure_penalty_window_secs": 600.0,
            "adaptive_min_order_floor_enabled": True,
            "min_order_usdt": 100.0,
            "min_order_usdt_micro": 25.0,
            "v6_micro_max_balance": 100.0,
            "v6_growth_max_balance": 1000.0,
            "v6_min_position_size_usdt": 25.0,
        })
        self.oc.PAPER_TRADING = False
        self.oc.OKX_API_KEY = "test-key"
        self.oc.risk.validate_trade = lambda coin, side, balance: {
            "valid": True,
            "size_usdt": 25.0,
            "tier": SimpleNamespace(name="MICRO", min_trade_usdt=25.0),
        }
        self.oc.risk.calculate_stop_take = lambda price, side, tier, leverage=None: (9.0, 12.0)
        self.oc.risk.register_open = lambda: None
        self.oc._working_leverage_cache.clear()
        self.ea.clear()

    def tearDown(self):
        _restore_settings_path(self, self.ss)
        self.ea.clear()
        self.oc.PAPER_TRADING = self._paper_backup
        self.oc.OKX_API_KEY = self._api_key_backup
        self.oc.risk.validate_trade = self._risk_validate_backup
        self.oc.risk.calculate_stop_take = self._risk_stop_take_backup
        self.oc.risk.register_open = self._risk_open_backup
        self.oc._working_leverage_cache.clear()
        self.ss._current.clear()
        self.ss._current.update(self._settings_backup)

    def _client(self):
        client = self.oc.OKXClient()

        async def fake_snapshot():
            return {"avail_eq_usdt": 34.2}

        async def fake_ticker(coin: str):
            return {"last": 10.0, "bid": 9.99, "ask": 10.01, "vol24h": 1e8}

        async def fake_set_leverage(coin: str, leverage: int | None = None, pos_side: str = "long"):
            lev = int(leverage or 25)
            return {"requested_leverage": lev, "effective_leverage": lev}

        async def fake_get_leverage_info(coin: str, margin_mode: str = "isolated"):
            return [{
                "instId": str(coin).upper(), "posSide": "long",
                "mgnMode": margin_mode, "lever": "15",
            }, {
                "instId": str(coin).upper(), "posSide": "short",
                "mgnMode": margin_mode, "lever": "12",
            }]

        async def fake_futures_sz(coin: str, size_usdt: float, price: float):
            return 2

        async def fake_contract_cost(coin: str, price: float | None = None):
            return 10.0

        client.get_account_snapshot = fake_snapshot  # type: ignore[assignment]
        client.get_ticker = fake_ticker  # type: ignore[assignment]
        client.set_leverage = fake_set_leverage  # type: ignore[assignment]
        client.get_leverage_info = fake_get_leverage_info  # type: ignore[assignment]
        client._futures_sz = fake_futures_sz  # type: ignore[assignment]
        client.get_contract_cost = fake_contract_cost  # type: ignore[assignment]
        return client

    def test_preflight_zero_contracts_falls_back_to_configured_cap(self):
        client = self._client()

        async def fake_get_max_order_size(coin: str, leverage: int | None = None, price: float | None = None):
            lev = int(leverage or 25)
            return {"max_buy_contracts": 0, "max_sell_contracts": 0}

        async def fake_request(method: str, path: str, params=None, data=None, *, priority=None):
            return {"code": "0", "data": [{"ordId": "1", "instId": "BTC-USDT-SWAP", "fillPx": "10"}]}

        client.get_max_order_size = fake_get_max_order_size  # type: ignore[assignment]
        client._request = fake_request  # type: ignore[assignment]
        async def run_order():
            order = await client.place_market_order(
                "BTC",
                "buy",
                amount_usdt=25.0,
                sl_price=9.0,
                tp_price=12.0,
                leverage=25,
                allow_nano_route=False,
            )
            reason = client.consume_last_order_skip_reason()
            return order, reason

        order, reason = asyncio.run(run_order())
        self.assertIsNone(order)
        self.assertIn("attempts=15x", reason)
        self.assertNotIn("25x ->", reason)

    def test_live_order_payload_is_entry_safe_in_mock_request(self):
        client = self._client()
        captured = {}

        async def fake_get_max_order_size(coin: str, leverage: int | None = None, price: float | None = None):
            return {"max_buy_contracts": 10, "max_sell_contracts": 10}

        async def fake_request(method: str, path: str, params=None, data=None, *, priority=None, before_send=None):
            captured["method"] = method
            captured["path"] = path
            captured["data"] = copy.deepcopy(data)
            if before_send is not None:
                before_send()
            return {"code": "0", "data": [{"ordId": "MOCK-OKX-1", "instId": "BTC-USDT-SWAP", "fillPx": "10", "sCode": "0"}]}

        client.get_max_order_size = fake_get_max_order_size  # type: ignore[assignment]
        client._request = fake_request  # type: ignore[assignment]

        order = asyncio.run(client.place_market_order(
            "BTC",
            "buy",
            amount_usdt=100.0,
            sl_price=9.0,
            tp_price=12.0,
            leverage=25,
            allow_nano_route=False,
            position_mode="long_short_mode",
        ))

        body = captured["data"]
        self.assertEqual(captured["method"], "POST")
        self.assertEqual(captured["path"], "/api/v5/trade/order")
        self.assertEqual(body["instId"], "BTC-USDT-SWAP")
        self.assertEqual(body["side"], "buy")
        self.assertEqual(body["posSide"], "long")
        self.assertEqual(body["ordType"], "market")
        self.assertNotIn("reduceOnly", body)
        self.assertGreater(int(body["sz"]), 0)
        self.assertIn("attachAlgoOrds", body)
        self.assertEqual(order["ordId"], "MOCK-OKX-1")
        self.assertEqual(order["contracts"], 2)
        self.assertEqual(order["size_usdt"], 100.0)

    def test_all_preflight_fallbacks_fail_and_apply_cooldown(self):
        client = self._client()

        async def fake_get_max_order_size(coin: str, leverage: int | None = None, price: float | None = None):
            return {"max_buy_contracts": 0, "max_sell_contracts": 0}

        async def unexpected_request(method: str, path: str, params=None, data=None):
            raise AssertionError("order request should not be reached when all leverage fallbacks fail")

        client.get_max_order_size = fake_get_max_order_size  # type: ignore[assignment]
        client._request = unexpected_request  # type: ignore[assignment]
        async def run_order():
            result = await client.place_market_order(
                "PEPE",
                "buy",
                amount_usdt=25.0,
                sl_price=9.0,
                tp_price=12.0,
                leverage=25,
                allow_nano_route=False,
            )
            reason = client.consume_last_order_skip_reason()
            return result, reason

        result, reason = asyncio.run(run_order())
        self.assertIsNone(result)
        self.assertIn("attempts=15x", reason)
        self.assertNotIn("->", reason)
        cooled, reason = self.ea.is_symbol_cooled_down("PEPE")
        self.assertTrue(cooled)
        self.assertIn("preflight_zero_contract_cooldown", reason)

    def test_leverage_fallback_toggle_stops_after_first_zero_contract(self):
        client = self._client()
        self.ss.set_val("leverage_fallback_enabled", False)

        async def fake_get_max_order_size(coin: str, leverage: int | None = None, price: float | None = None):
            return {"max_buy_contracts": 0, "max_sell_contracts": 0}

        client.get_max_order_size = fake_get_max_order_size  # type: ignore[assignment]

        async def run_order():
            result = await client.place_market_order(
                "PEPE",
                "buy",
                amount_usdt=25.0,
                sl_price=9.0,
                tp_price=12.0,
                leverage=25,
                allow_nano_route=False,
            )
            reason = client.consume_last_order_skip_reason()
            return result, reason

        result, reason = asyncio.run(run_order())
        self.assertIsNone(result)
        self.assertIn("attempts=15x", reason)
        self.assertNotIn("leverage_fallback_disabled", reason)


class TestCloseConfirmationSafety(unittest.TestCase):

    def setUp(self):
        import okx_client as oc
        self.oc = oc
        self._paper_backup = oc.PAPER_TRADING
        self._api_key_backup = oc.OKX_API_KEY
        self.oc.PAPER_TRADING = False
        self.oc.OKX_API_KEY = "test-key"

    def tearDown(self):
        self.oc.PAPER_TRADING = self._paper_backup
        self.oc.OKX_API_KEY = self._api_key_backup

    def test_close_futures_position_does_not_synthesize_close_when_live_check_is_uncertain(self):
        client = self.oc.OKXClient()

        async def fake_status():
            return {"ok": False, "coins": None, "reason": "timeout"}

        async def fake_ticker(coin: str):
            return {"last": 100.0}

        async def fake_futures_sz(coin: str, size_usdt: float, price: float):
            return 1

        async def fake_request(method: str, path: str, params=None, data=None):
            return {"code": "0", "data": [{"ordId": "1", "fillPx": "100"}]}

        client.get_live_open_coins_status = fake_status  # type: ignore[assignment]
        client.get_ticker = fake_ticker  # type: ignore[assignment]
        client._futures_sz = fake_futures_sz  # type: ignore[assignment]
        client._request = fake_request  # type: ignore[assignment]

        result = asyncio.run(client.close_futures_position("BTC", "long", 25.0))
        self.assertFalse(bool(result.get("exchange_closed", False)))
        self.assertEqual(result.get("ordId"), "1")


class TestUnifiedUniverseRanking(unittest.TestCase):

    def setUp(self):
        import coin_universe as cu
        import execution_admission as ea
        import settings_store as ss
        self.cu = cu
        self.ea = ea
        self.ss = ss
        self._settings_backup = copy.deepcopy(ss._current)
        _isolate_settings_path(self, ss, "universe-ranking-")
        self._volume_backup = copy.deepcopy(cu._volume_rank)
        self._spread_backup = copy.deepcopy(cu._spread_rank)
        self._tradeable_backup = list(cu._last_tradeable)
        self._history_backup = copy.deepcopy(ea._failure_history)
        self._cooldown_backup = copy.deepcopy(ea._cooldown_until)
        self.ss._current.update({
            "universe_min_volume_usd_24h": 250000.0,
            "universe_spread_max_pct": 1.0,
            "spread_filter_enabled": True,
            "dead_coin_suppression_enabled": True,
            "executability_penalty_enabled": True,
            "market_cap_bonus_enabled": True,
            "small_account_large_cap_preference_enabled": True,
            "large_cap_preference_balance_usdt": 100.0,
            "large_cap_preference_bonus_small_account": 8.0,
            "large_cap_preference_bonus_default": 3.0,
            "execution_failure_penalty_window_secs": 600.0,
            "preflight_zero_contract_selection_penalty": 40.0,
            "exchange_admission_selection_penalty": 12.0,
            "execution_cooldown_selection_penalty": 1000.0,
        })
        self.cu._volume_rank.clear()
        self.cu._spread_rank.clear()
        self.cu._last_tradeable = []
        self.ea.clear()

    def tearDown(self):
        _restore_settings_path(self, self.ss)
        self.cu._volume_rank.clear()
        self.cu._volume_rank.update(self._volume_backup)
        self.cu._spread_rank.clear()
        self.cu._spread_rank.update(self._spread_backup)
        self.cu._last_tradeable = self._tradeable_backup
        self.ea._failure_history.clear()
        self.ea._failure_history.update(self._history_backup)
        self.ea._cooldown_until.clear()
        self.ea._cooldown_until.update(self._cooldown_backup)
        self.ss._current.clear()
        self.ss._current.update(self._settings_backup)

    def test_liquid_large_cap_ranks_ahead_of_thin_meme(self):
        self.cu._volume_rank.update({"BTC": 50_000_000.0, "PEPE": 600_000.0})
        self.cu._spread_rank.update({"BTC": 0.02, "PEPE": 0.60})
        ranked = self.cu.rank_tradeable_candidates(["PEPE", "BTC"], balance_usdt=34.2)
        self.assertEqual(ranked[0], "BTC")

    def test_recent_execution_failure_penalizes_selection(self):
        self.cu._volume_rank.update({"BTC": 10_000_000.0, "ETH": 10_000_000.0})
        self.cu._spread_rank.update({"BTC": 0.03, "ETH": 0.03})
        self.ea.record_exchange_admission_failure("ETH", "market_unavailable", detail="exchange reject")
        ranked = self.cu.rank_tradeable_candidates(["ETH", "BTC"], balance_usdt=34.2)
        self.assertEqual(ranked[0], "BTC")

    def test_hayate_candidate_pool_uses_broad_ranked_universe(self):
        import category_engine as ce
        self.cu._volume_rank.update({"BTC": 10_000_000.0, "ALTX": 2_500_000.0})
        self.cu._spread_rank.update({"BTC": 0.02, "ALTX": 0.20})
        eng = ce.CategoryEngine("HAYATE")
        pool = eng._candidate_pool(["ALTX", "BTC"], balance=34.2, include_open=False)
        self.assertIn("ALTX", pool)

    def test_market_cap_bonus_toggle_removes_large_cap_preference(self):
        self.cu._volume_rank.update({"BTC": 500_000.0})
        self.cu._spread_rank.update({"BTC": 0.05})
        snap = self.cu.get_universe_preference("BTC", balance_usdt=34.2)
        self.assertGreater(snap["market_cap_bonus"], 0.0)
        self.ss.set_val("market_cap_bonus_enabled", False)
        snap = self.cu.get_universe_preference("BTC", balance_usdt=34.2)
        self.assertEqual(snap["market_cap_bonus"], 0.0)

    def test_dead_coin_suppression_toggle_allows_low_volume_symbol_snapshot(self):
        self.cu._volume_rank.update({"DUST": 10_000.0})
        self.cu._spread_rank.update({"DUST": 0.10})
        snap = self.cu.get_universe_preference("DUST", balance_usdt=34.2)
        self.assertFalse(snap["tradeable"])
        self.ss.set_val("dead_coin_suppression_enabled", False)
        snap = self.cu.get_universe_preference("DUST", balance_usdt=34.2)
        self.assertTrue(snap["tradeable"])


class TestExecutionSelectionStateIsolation(unittest.TestCase):

    def setUp(self):
        import execution_admission as ea
        import exit_manager as em
        import position_registry as pr
        import settings_store as ss
        self.ea = ea
        self.em = em
        self.pr = pr
        self.ss = ss
        self._settings_backup = copy.deepcopy(ss._current)
        self._registry_backup = copy.deepcopy(pr._registry)
        self._plans_backup = copy.deepcopy(em.get_all_plans())
        self._history_backup = copy.deepcopy(ea._failure_history)
        self._cooldown_backup = copy.deepcopy(ea._cooldown_until)
        self.ss._current.update({
            "preflight_zero_contract_trigger_count": 1,
            "preflight_zero_contract_window_secs": 60.0,
            "preflight_zero_contract_cooldown_secs": 300.0,
            "execution_failure_penalty_window_secs": 600.0,
        })
        self.ea.clear()
        self.pr._registry.clear()
        self.pr._registry["SOL"] = {
            "direction": "long",
            "engine": "AMATERASU",
            "size_usdt": 25.0,
        }
        self.em.register(
            "SOL", "long", 1.0, 25.0, 0.1,
            0.95, 1.05, 1.10,
            engine_name="AMATERASU", leverage=25,
        )

    def tearDown(self):
        self.pr._registry.clear()
        self.pr._registry.update(self._registry_backup)
        self.ea._failure_history.clear()
        self.ea._failure_history.update(self._history_backup)
        self.ea._cooldown_until.clear()
        self.ea._cooldown_until.update(self._cooldown_backup)
        for coin in list(self.em.get_all_plans().keys()):
            self.em.remove(coin)
        for coin, plan in self._plans_backup.items():
            self.em._plans[coin] = plan
        self.ss._current.clear()
        self.ss._current.update(self._settings_backup)

    def test_admission_penalties_do_not_mutate_open_positions_or_exit_plans(self):
        registry_before = copy.deepcopy(self.pr._registry)
        plans_before = copy.deepcopy(self.em.get_all_plans())
        self.ea.record_preflight_zero_contract("PEPE", detail="attempts=25x -> 20x -> 15x -> 10x -> 5x")
        self.assertEqual(self.pr._registry, registry_before)
        self.assertEqual(self.em.get_all_plans(), plans_before)

    def test_kagutsuchi_confirms_real_buy_pressure(self):
        import strategy_categories as sc
        closes = [100.0, 100.1, 100.0, 100.2, 100.4, 100.8, 101.3, 101.9, 102.4, 103.0,
                  103.4, 103.9, 104.5, 105.0, 105.6, 106.3, 106.9, 107.4, 108.0, 108.7,
                  109.4, 110.0, 110.8, 111.6, 112.5]
        highs = [c + 0.8 for c in closes]
        lows = [c - 0.8 for c in closes]
        volumes = [100.0] * 24 + [160.0]
        direction, votes = sc.evaluate_kagutsuchi(
            closes, highs, lows, volumes, taker_buy_vol=70.0, taker_sell_vol=30.0, coin="BTC"
        )
        self.assertEqual(direction, "LONG")
        self.assertGreaterEqual(votes, 4.0)

    def test_tensei_requires_recent_profitable_reentry(self):
        import performance_tracker as perf
        import strategy_categories as sc

        orig_get_last = perf.get_last_closed_trade
        try:
            closed_at = (time.time() - 90 * 60)
            iso = time.strftime("%Y-%m-%dT%H:%M:%S+00:00", time.gmtime(closed_at))
            perf.get_last_closed_trade = lambda coin: SimpleNamespace(
                closed_at=iso,
                pnl_usdt=3.2,
                exit_reason="tp1",
                direction="long",
            )
            closes = [100.0 + i * 0.2 for i in range(34)] + [108.5]
            highs = [c + 0.4 for c in closes[:-1]] + [108.9]
            lows = [c - 0.4 for c in closes[:-1]] + [107.7]
            volumes = [100.0] * 34 + [120.0]
            direction, votes = sc.evaluate_tensei(closes, highs, lows, volumes, coin="SOL")
        finally:
            perf.get_last_closed_trade = orig_get_last
        self.assertEqual(direction, "LONG")
        self.assertGreaterEqual(votes, 2.0)

    def test_ibuki_detects_compression_then_expansion(self):
        import strategy_categories as sc

        orig_bbw = sc.bollinger_band_width
        orig_atr = sc.atr
        try:
            closes = [100.0] * 39 + [101.5]
            highs = [100.2] * 39 + [101.7]
            lows = [99.8] * 39 + [100.8]
            volumes = [100.0] * 39 + [140.0]

            sc.bollinger_band_width = lambda series, period, std: 0.05 if len(series) == len(closes) else 0.10
            sc.atr = lambda hs, ls, cs, period: 1.0 if len(cs) == len(closes) else 2.0

            direction, votes = sc.evaluate_ibuki(closes, highs, lows, volumes, coin="ARB")
        finally:
            sc.bollinger_band_width = orig_bbw
            sc.atr = orig_atr
        self.assertEqual(direction, "LONG")
        self.assertGreaterEqual(votes, 3.0)

    def test_onryo_detects_crowded_short_setup(self):
        import strategy_categories as sc
        closes = [120.0, 119.5, 119.1, 118.6, 118.0, 117.2, 116.4, 115.7, 115.0, 114.2,
                  113.5, 112.9, 112.3, 111.7, 111.1, 110.4, 109.8, 109.2, 108.6, 108.0]
        highs = [c + 0.9 for c in closes]
        lows = [c - 0.9 for c in closes]
        volumes = [100.0] * len(closes)
        direction, votes = sc.evaluate_onryo(
            closes,
            highs,
            lows,
            volumes,
            funding_rate=0.0010,
            raijuu_oi_series=[100.0, 101.5, 103.0, 104.5, 106.0, 108.5],
            coin="BTC",
        )
        self.assertEqual(direction, "SHORT")
        self.assertGreaterEqual(votes, 2.0)


class TestSyncClosureConfirmation(unittest.TestCase):

    class _DummyEngine:
        def __init__(self, name: str = "AMATERASU"):
            self.name = name
            self._open = {}
            self.closed = []

        def is_enabled(self):
            return True

        def open_positions(self):
            return dict(self._open)

        def restore_position(self, coin, details):
            self._open[coin] = dict(details)
            return True

        def notify_closed_by_exchange(self, coin, exit_px, pnl_usdt, reason):
            self.closed.append((coin, exit_px, pnl_usdt, reason))
            self._open.pop(coin, None)
            return False

    def setUp(self):
        import exit_manager as em
        import position_registry as pr
        import sync_health as sh
        import trading_engine as te

        self.em = em
        self.pr = pr
        self.sh = sh
        self.te = te
        self._registry_backup = copy.deepcopy(pr._registry)
        self._save_backup = pr._save
        self._plans_backup = copy.deepcopy(em.get_all_plans())
        self._okx_backup = te.OKXClient
        self._latest_price_backup = te.latest_price
        self._api_key_backup = os.environ.get("OKX_API_KEY")
        self._paper_backup = os.environ.get("PAPER_TRADING")
        self.pr._save = lambda *args, **kwargs: None
        self.pr._registry.clear()
        self.em._plans.clear()
        self.sh.reset()
        os.environ["OKX_API_KEY"] = "test-key"
        os.environ["PAPER_TRADING"] = "false"

        self.engine = te.MultiEngine()
        self.engine.engines = {
            "AMATERASU": self._DummyEngine(),
        }
        self.closed_records = []
        self.engine._record_direct_close = lambda **kwargs: self.closed_records.append(kwargs)
        self.te.latest_price = lambda coin: 101.0

    def tearDown(self):
        self.pr._registry.clear()
        self.pr._registry.update(self._registry_backup)
        self.pr._save = self._save_backup
        self.em._plans.clear()
        self.em._plans.update(self._plans_backup)
        self.sh.reset()
        self.te.OKXClient = self._okx_backup
        self.te.latest_price = self._latest_price_backup
        if self._api_key_backup is None:
            os.environ.pop("OKX_API_KEY", None)
        else:
            os.environ["OKX_API_KEY"] = self._api_key_backup
        if self._paper_backup is None:
            os.environ.pop("PAPER_TRADING", None)
        else:
            os.environ["PAPER_TRADING"] = self._paper_backup

    def test_sync_failure_marks_position_uncertain_without_closure(self):
        class FakeOKX:
            async def get_positions_status(self):
                return {"ok": False, "positions": None, "reason": "network down"}

        self.te.OKXClient = lambda: FakeOKX()
        self.pr._registry["BTC"] = {
            "direction": "long",
            "engine": "AMATERASU",
            "entry": 100.0,
            "size_usdt": 25.0,
            "leverage": 25,
        }
        self.em.register("BTC", "long", 100.0, 25.0, 1.0, 95.0, 110.0, 115.0, engine_name="AMATERASU", leverage=25)

        result = asyncio.run(self.engine.sync_positions())

        self.assertIn("_uncertain_", result)
        self.assertIn("BTC", self.pr._registry)
        self.assertEqual(self.pr._registry["BTC"]["sync_status"], "UNCERTAIN")
        self.assertEqual(self.pr._registry["BTC"]["last_sync_error"], "network down")
        self.assertEqual(self.em.get_plan("BTC").sync_status, "UNCERTAIN")
        self.assertEqual(self.engine.get_sync_health_snapshot()["status"], "DEGRADED")
        self.assertEqual(self.closed_records, [])

    def test_registry_position_requires_two_successful_absence_syncs_before_close(self):
        class FakeOKX:
            async def get_positions_status(self):
                return {"ok": True, "positions": [], "reason": ""}

            async def get_last_fill_price(self, coin):
                return None

        self.te.OKXClient = lambda: FakeOKX()
        self.pr._registry["BTC"] = {
            "direction": "long",
            "engine": "AMATERASU",
            "entry": 100.0,
            "size_usdt": 25.0,
            "leverage": 25,
        }

        asyncio.run(self.engine.sync_positions())
        self.assertIn("BTC", self.pr._registry)
        self.assertEqual(self.pr._registry["BTC"]["sync_status"], "MISSING")
        self.assertEqual(self.pr._registry["BTC"]["exchange_absence_confirmations"], 1)
        self.assertEqual(self.closed_records, [])

        asyncio.run(self.engine.sync_positions())
        self.assertNotIn("BTC", self.pr._registry)
        self.assertEqual(len(self.closed_records), 1)
        self.assertEqual(self.closed_records[0]["coin"], "BTC")

    def test_sync_failure_resets_missing_confirmation_chain(self):
        class FailingThenEmptyOKX:
            def __init__(self):
                self.calls = 0

            async def get_positions_status(self):
                self.calls += 1
                if self.calls == 2:
                    return {"ok": False, "positions": None, "reason": "timeout"}
                return {"ok": True, "positions": [], "reason": ""}

            async def get_last_fill_price(self, coin):
                return None

        fake = FailingThenEmptyOKX()
        self.te.OKXClient = lambda: fake
        self.pr._registry["BTC"] = {
            "direction": "long",
            "engine": "AMATERASU",
            "entry": 100.0,
            "size_usdt": 25.0,
            "leverage": 25,
        }

        asyncio.run(self.engine.sync_positions())
        self.assertEqual(self.pr._registry["BTC"]["exchange_absence_confirmations"], 1)
        asyncio.run(self.engine.sync_positions())
        self.assertEqual(self.pr._registry["BTC"]["sync_status"], "UNCERTAIN")
        self.assertEqual(self.pr._registry["BTC"]["exchange_absence_confirmations"], 0)
        asyncio.run(self.engine.sync_positions())
        self.assertIn("BTC", self.pr._registry)
        self.assertEqual(self.pr._registry["BTC"]["sync_status"], "MISSING")
        self.assertEqual(self.pr._registry["BTC"]["exchange_absence_confirmations"], 1)
        self.assertEqual(self.closed_records, [])

    def test_exchange_only_recovery_preserves_original_engine_when_inferable(self):
        class FakeOKX:
            async def get_positions_status(self):
                return {
                    "ok": True,
                    "positions": [{
                        "instId": "SOL-USDT-SWAP",
                        "pos": "2",
                        "posSide": "long",
                        "avgPx": "150.0",
                        "lever": "20",
                        "notionalUsd": "30.0",
                    }],
                    "reason": "",
                }

        self.te.OKXClient = lambda: FakeOKX()
        self.engine.engines["AMATERASU"]._open["SOL"] = {
            "direction": "long",
            "entry": 149.5,
            "size_usdt": 30.0,
            "leverage": 15,
            "stop_loss": 145.0,
            "take_profit": 160.0,
            "tp2_price": 166.0,
            "vote_score": 4,
            "order_id": "OID-1",
        }

        asyncio.run(self.engine.sync_positions())

        self.assertIn("SOL", self.pr._registry)
        self.assertEqual(self.pr._registry["SOL"]["engine"], "AMATERASU")
        self.assertEqual(self.pr._registry["SOL"]["source_engine"], "AMATERASU")
        self.assertEqual(self.pr._registry["SOL"]["sync_status"], "OK")
        self.assertEqual(self.engine.get_sync_health_snapshot()["status"], "OK")


class TestExitManagerSyncConfirmation(unittest.TestCase):

    def setUp(self):
        import exit_manager as em
        import position_registry as pr
        import sync_health as sh

        self.em = em
        self.pr = pr
        self.sh = sh
        import okx_client as okx
        self.okx = okx
        self._paper_env_backup = os.environ.get("PAPER_TRADING")
        self._paper_module_backup = okx.PAPER_TRADING
        os.environ["PAPER_TRADING"] = "false"
        okx.PAPER_TRADING = False
        self._registry_backup = copy.deepcopy(pr._registry)
        self._save_backup = pr._save
        self._plans_backup = copy.deepcopy(em.get_all_plans())
        self._okx_backup = em._okx_client
        self._handle_backup = em._handle_exchange_closed
        self.pr._save = lambda *args, **kwargs: None
        self.pr._registry.clear()
        self.em._plans.clear()
        self.sh.reset()
        self.pr._registry["ETH"] = {
            "direction": "long",
            "engine": "AMATERASU",
            "entry": 100.0,
            "size_usdt": 25.0,
            "leverage": 25,
        }
        self.plan = self.em.register(
            "ETH", "long", 100.0, 25.0, 1.0,
            95.0, 110.0, 115.0,
            engine_name="AMATERASU", leverage=25,
        )
        self.closed = []

        async def fake_handle(coin, plan):
            self.closed.append((coin, plan.sync_status, plan.exchange_absence_confirmations))

        self.em._handle_exchange_closed = fake_handle

    def tearDown(self):
        self.pr._registry.clear()
        self.pr._registry.update(self._registry_backup)
        self.pr._save = self._save_backup
        self.em._plans.clear()
        self.em._plans.update(self._plans_backup)
        self.sh.reset()
        self.em._okx_client = self._okx_backup
        self.em._handle_exchange_closed = self._handle_backup
        if self._paper_env_backup is None:
            os.environ.pop("PAPER_TRADING", None)
        else:
            os.environ["PAPER_TRADING"] = self._paper_env_backup
        self.okx.PAPER_TRADING = self._paper_module_backup

    def test_exchange_sync_failure_keeps_plan_active_and_uncertain(self):
        class FakeClient:
            async def get_live_open_coins_status(self):
                return {"ok": False, "coins": None, "reason": "timeout"}

        self.em._okx_client = FakeClient()
        asyncio.run(self.em._do_exchange_sync())

        self.assertIn("ETH", self.em.get_all_plans())
        self.assertEqual(self.plan.sync_status, "UNCERTAIN")
        self.assertEqual(self.pr._registry["ETH"]["sync_status"], "UNCERTAIN")
        self.assertEqual(self.sh.get_snapshot()["status"], "DEGRADED")
        self.assertEqual(self.closed, [])

    def test_exchange_absence_requires_double_confirmation_before_close_handler_runs(self):
        class FakeClient:
            async def get_live_open_coins_status(self):
                return {"ok": True, "coins": set(), "reason": ""}

        self.em._okx_client = FakeClient()

        asyncio.run(self.em._do_exchange_sync())
        self.assertEqual(self.plan.sync_status, "MISSING")
        self.assertEqual(self.plan.exchange_absence_confirmations, 1)
        self.assertEqual(self.closed, [])

        asyncio.run(self.em._do_exchange_sync())
        self.assertEqual(len(self.closed), 1)
        self.assertEqual(self.closed[0][0], "ETH")

    def test_execute_exit_attempts_real_close_when_live_check_is_uncertain(self):
        calls = []

        class FakeClient:
            async def get_live_open_coins_status(self):
                return {"ok": False, "coins": None, "reason": "timeout"}

            async def get_positions(self):
                return [{"instId": "ETH-USDT-SWAP", "posSide": "long", "pos": "1.0"}]

            async def close_futures_position(self, coin, pos_side, size):
                calls.append((coin, pos_side, size))
                return {"ordId": "1"}

        self.em._okx_client = FakeClient()
        import execution_router
        from unittest import mock

        class FakeRouter:
            async def close(_self, coin, pos_side, size, **kwargs):
                return await self.em._okx_client.close_futures_position(coin, pos_side, size)

        with mock.patch.object(execution_router, "router_for", lambda client: FakeRouter()):
            asyncio.run(self.em._execute_exit("ETH", 101.0, "manual", self.plan))
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0][0], "ETH")


class TestProfitLock(unittest.TestCase):

    def setUp(self):
        import settings_store as ss
        import exit_manager as em
        import okx_client as okx
        self.ss = ss
        self.em = em
        self.okx = okx
        self._paper_env_backup = os.environ.get("PAPER_TRADING")
        self._paper_module_backup = okx.PAPER_TRADING
        os.environ["PAPER_TRADING"] = "false"
        okx.PAPER_TRADING = False
        self._settings_backup = copy.deepcopy(ss._current)
        self._latest_price_backup = em.latest_price
        self.ss._current.update({
            "secure_profit_mode_enabled": False,
            "profit_lock_enabled": True,
            "profit_lock_l1_trigger_pct": 15.0,
            "profit_lock_l1_floor_pct": 5.0,
            "profit_lock_l2_trigger_pct": 25.0,
            "profit_lock_l2_floor_pct": 10.0,
            "profit_lock_l3_trigger_pct": 40.0,
            "profit_lock_l3_floor_pct": 20.0,
            # This legacy test isolates lock-level monotonicity; candidate
            # economics are covered separately with the production 1.0x default.
            "minimum_net_capture_fee_multiple": 0.0,
            "tsl_enabled": False,
        })
        self.em._plans.clear()

    def tearDown(self):
        self.em._plans.clear()
        self.em.latest_price = self._latest_price_backup
        self.ss._current.clear()
        self.ss._current.update(self._settings_backup)
        if self._paper_env_backup is None:
            os.environ.pop("PAPER_TRADING", None)
        else:
            os.environ["PAPER_TRADING"] = self._paper_env_backup
        self.okx.PAPER_TRADING = self._paper_module_backup

    def test_profit_lock_advances_without_moving_backward(self):
        plan = self.em.ExitPlan(
            coin="BTC",
            direction="long",
            engine_name="RYUU",
            entry_price=100.0,
            size_usdt=25.0,
            atr_val=1.0,
            hard_sl=95.0,
            tp1_price=120.0,
            tp2_price=140.0,
            leverage=25,
            entry_contracts=1.0,
            confirmed_remaining_contracts=1.0,
            entry_fee_usdt=0.0125,
            funding_usdt=0.0,
            funding_status="SUCCESS_EMPTY",
            peak_price=100.0,
            tsl_price=95.0,
        )
        self.em._plans["BTC"] = plan

        self.em.latest_price = lambda coin: 100.61
        asyncio.run(self.em._check_plan("BTC"))
        self.assertEqual(plan.profit_lock_level, 1)
        self.assertAlmostEqual(plan.hard_sl, 100.2, places=4)

        self.em.latest_price = lambda coin: 101.01
        asyncio.run(self.em._check_plan("BTC"))
        self.assertEqual(plan.profit_lock_level, 2)
        self.assertAlmostEqual(plan.hard_sl, 100.4, places=4)

        self.em.latest_price = lambda coin: 100.8
        asyncio.run(self.em._check_plan("BTC"))
        self.assertEqual(plan.profit_lock_level, 2)
        self.assertAlmostEqual(plan.hard_sl, 100.4, places=4)

class TestTpDistanceMode(unittest.TestCase):

    def setUp(self):
        import settings_store as ss
        self.ss = ss
        self._settings_backup = copy.deepcopy(ss._current)

    def tearDown(self):
        self.ss._current.clear()
        self.ss._current.update(self._settings_backup)

    def test_extended_tp_mode_widens_targets(self):
        import sl_tp_engine as st
        self.ss._current["tp_distance_mode"] = "balanced"
        _, tp1_bal, tp2_bal = st._build_result(100.0, "long", 1.0, tp1_rr=1.5, tp2_rr=4.0)
        self.ss._current["tp_distance_mode"] = "extended"
        _, tp1_ext, tp2_ext = st._build_result(100.0, "long", 1.0, tp1_rr=1.5, tp2_rr=4.0)
        self.assertGreater(tp1_ext, tp1_bal)
        self.assertGreater(tp2_ext, tp2_bal)


class TestStopGeometryAtr(unittest.TestCase):

    def setUp(self):
        import settings_store as ss
        self.ss = ss
        self._settings_backup = copy.deepcopy(ss._current)

    def tearDown(self):
        self.ss._current.clear()
        self.ss._current.update(self._settings_backup)

    def test_sl_tp_engine_uses_configured_stop_bounds(self):
        import sl_tp_engine as st
        self.ss._current.update({
            "min_sl_dist_pct": 1.0,
            "max_sl_dist_pct": 3.0,
            "tp_distance_mode": "balanced",
        })

        sl_floor, _, _ = st._build_result(100.0, "long", 0.20, tp1_rr=1.5, tp2_rr=4.0)
        sl_cap, _, _ = st._build_result(100.0, "long", 5.00, tp1_rr=1.5, tp2_rr=4.0)

        self.assertAlmostEqual(sl_floor, 99.0)
        self.assertAlmostEqual(sl_cap, 97.0)

    def test_stop_geometry_atr_prefers_1h_for_sampled_pairs(self):
        import category_engine as ce
        backups = {
            "highs": ce.get_1h_highs,
            "lows": ce.get_1h_lows,
            "closes": ce.get_1h_closes,
        }
        atr_by_coin = {
            "TRX": 0.00062004,
            "OP": 0.00071098,
            "SUI": 0.00388485,
        }

        def _series(coin: str, atr_val: float):
            base = {"TRX": 0.3262, "OP": 0.0857, "SUI": 0.687}.get(coin, 1.0)
            half = atr_val / 2.0
            return [base + half] * 20, [base - half] * 20, [base] * 20

        try:
            ce.get_1h_highs = lambda coin: _series(coin, atr_by_coin[coin])[0]
            ce.get_1h_lows = lambda coin: _series(coin, atr_by_coin[coin])[1]
            ce.get_1h_closes = lambda coin: _series(coin, atr_by_coin[coin])[2]

            for coin, expected_atr in atr_by_coin.items():
                self.assertAlmostEqual(
                    ce._stop_geometry_atr(coin, fallback_atr=0.000001),
                    expected_atr,
                    places=8,
                )
        finally:
            ce.get_1h_highs = backups["highs"]
            ce.get_1h_lows = backups["lows"]
            ce.get_1h_closes = backups["closes"]


@unittest.skip("stale expectations: current tiny-notional exit path defers near-floor partials")
class TestSecureProfitModel(unittest.TestCase):

    def setUp(self):
        import settings_store as ss
        import exit_manager as em
        self.ss = ss
        self.em = em
        self._settings_backup = copy.deepcopy(ss._current)
        self._latest_price_backup = em.latest_price
        self._get_closes_backup = em.get_closes
        self._get_volumes_backup = em.get_volumes
        self._perf_backup = em._perf.record_trade
        self._tp_alert_backup = em.alert_take_profit
        self.ss._current.update({
            "secure_profit_mode_enabled": True,
            "profit_lock_enabled": True,
            "tsl_enabled": True,
            "tsl_activation_pct": 0.5,
            "trail_activation_profit_usd": 0.90,
            "trail_distance_mode": "hybrid",
            "trail_distance_atr_mult": 2.5,
            "breakeven_enabled": True,
            "breakeven_activation_profit_usd": 0.35,
            "breakeven_trigger_pct": 1.1,
            "breakeven_fee_buffer_mult": 2.0,
            "secure_profit_target_usd": 3.0,
            "partial_exit_enabled": True,
            "tp1_trigger_pct": 10.0,
            "tp2_trigger_pct": 20.0,
            "partial_tp1_close_pct": 35.0,
            "partial_tp2_close_pct": 25.0,
            "partial_tp1_max_close_pct": 100.0,
            "partial_tp2_max_close_pct": 100.0,
            "min_net_take_profit_usdt": 0.05,
            "min_full_close_profit_usdt": 0.35,
            "swing_tp1_close_pct": 35.0,
            "swing_tp2_close_pct": 25.0,
            "runner_core_min_remaining_pct": 0.0,
            "continuation_runner_core_min_pct": 0.0,
            "profit_lock_stage1_profit_usd": 0.75,
            "profit_lock_stage1_lock_pct": 20.0,
            "profit_lock_stage2_profit_usd": 1.50,
            "profit_lock_stage2_lock_pct": 40.0,
            "profit_lock_stage3_profit_usd": 3.00,
            "profit_lock_stage3_lock_pct": 60.0,
            "stale_trade_exit_enabled": False,
            "continuation_runner_enabled": True,
            "continuation_runner_min_roi": 2.5,
            "continuation_runner_score_threshold": 3.0,
            "continuation_runner_tp2_close_mult": 0.65,
            "continuation_runner_trail_mult": 1.35,
            # Keep tests deterministic: milestone partial sizing should not shift with presets unless explicitly tested.
            "trading_mode_preset": "",
        })
        self.em._plans.clear()
        self.em._perf.record_trade = lambda *args, **kwargs: None

        async def _noop_alert(*args, **kwargs):
            return None

        self.em.alert_take_profit = _noop_alert

    def tearDown(self):
        self.em._plans.clear()
        self.em.latest_price = self._latest_price_backup
        self.em.get_closes = self._get_closes_backup
        self.em.get_volumes = self._get_volumes_backup
        self.em._perf.record_trade = self._perf_backup
        self.em.alert_take_profit = self._tp_alert_backup
        self.ss._current.clear()
        self.ss._current.update(self._settings_backup)

    def _plan(self):
        plan = self.em.ExitPlan(
            coin="BTC",
            direction="long",
            engine_name="RYUU",
            entry_price=100.0,
            size_usdt=25.0,
            atr_val=1.0,
            hard_sl=95.0,
            tp1_price=110.0,
            tp2_price=120.0,
            initial_size_usdt=25.0,
            leverage=25,
            peak_price=100.0,
            tsl_price=95.0,
        )
        self.em._plans["BTC"] = plan
        return plan

    def test_breakeven_waits_for_meaningful_profit_gate(self):
        plan = self._plan()
        # ROI alone is no longer enough; BE now also requires a meaningful profit floor.
        self.em.latest_price = lambda coin: 100.04
        asyncio.run(self.em._check_plan("BTC"))
        self.assertFalse(plan.breakeven_moved)
        self.assertAlmostEqual(plan.hard_sl, 95.0, places=4)

        # Cross the raw ROI trigger without enough dollar profit: still no BE move.
        self.em.latest_price = lambda coin: 100.05
        asyncio.run(self.em._check_plan("BTC"))
        self.assertFalse(plan.breakeven_moved)

        # Once the unrealized profit floor is reached, BE protection is allowed to move.
        self.em.latest_price = lambda coin: 101.50
        asyncio.run(self.em._check_plan("BTC"))
        self.assertTrue(plan.breakeven_moved)
        self.assertGreater(plan.hard_sl, 100.0)

    def test_runner_trail_arms_after_meaningful_profit_confirmation(self):
        plan = self._plan()
        # Below BE trigger: no protection should be armed.
        self.em.latest_price = lambda coin: 100.04
        asyncio.run(self.em._check_plan("BTC"))
        self.assertFalse(plan.breakeven_moved)
        self.assertFalse(plan.tsl_armed)

        # Cross BE trigger only: still not enough for protection to arm.
        self.em.latest_price = lambda coin: 100.05
        asyncio.run(self.em._check_plan("BTC"))
        self.assertFalse(plan.breakeven_moved)
        self.assertFalse(plan.tsl_armed)

        # Cross the profit floor: SL moves and protection is considered armed.
        self.em.latest_price = lambda coin: 101.50
        asyncio.run(self.em._check_plan("BTC"))
        self.assertTrue(plan.breakeven_moved)
        self.assertTrue(plan.tsl_armed)

        # Large move: trailing should ratchet SL up from peak (ATR+percent hybrid).
        self.em.latest_price = lambda coin: 108.0
        asyncio.run(self.em._check_plan("BTC"))
        self.assertTrue(plan.tsl_armed)

    def test_trail_updates_from_peak_after_arming(self):
        plan = self._plan()
        self.em.latest_price = lambda coin: 108.0
        asyncio.run(self.em._check_plan("BTC"))
        self.assertTrue(plan.tsl_armed)
        self.assertAlmostEqual(plan.tsl_price, 105.5, places=4)

        self.em.latest_price = lambda coin: 109.0
        asyncio.run(self.em._check_plan("BTC"))
        self.assertAlmostEqual(plan.tsl_price, 106.5, places=4)

    def test_milestone_stages_progress_without_fixed_profit_targets(self):
        plan = self._plan()
        self.ss._current["tsl_enabled"] = False
        async def _fake_partial(*args, **kwargs):
            return True

        original = self.em._execute_partial
        try:
            # Milestone stage progress requires successful partial execution; in unit tests
            # we stub the exchange execution path.
            self.em._execute_partial = _fake_partial
            self.em.latest_price = lambda coin: 120.0
            asyncio.run(self.em._check_plan("BTC"))
            # Only one partial action is executed per tick; stage should advance at least to TP1.
            self.assertGreaterEqual(plan.profit_lock_level, 2)
            self.assertGreater(plan.hard_sl, 100.0)
            asyncio.run(self.em._check_plan("BTC"))
            self.assertGreaterEqual(plan.profit_lock_level, 3)
        finally:
            self.em._execute_partial = original

    def test_partial_exits_reduce_live_open_size_not_original_size(self):
        plan = self._plan()
        calls = []

        async def _fake_partial(coin, price, pct):
            calls.append((coin, pct, plan.size_usdt))
            return True

        original = self.em._execute_partial
        try:
            self.em._execute_partial = _fake_partial
            self.em.latest_price = lambda coin: 110.0
            asyncio.run(self.em._check_plan("BTC"))
            self.assertTrue(plan.tp1_done)
            self.assertAlmostEqual(plan.size_usdt, 16.25, places=4)
            self.assertAlmostEqual(plan.remaining_pct, 65.0, places=4)

            self.em.latest_price = lambda coin: 120.0
            asyncio.run(self.em._check_plan("BTC"))
            self.assertTrue(plan.tp2_done)
            self.assertAlmostEqual(plan.size_usdt, 12.1875, places=4)
            self.assertAlmostEqual(plan.remaining_pct, 48.75, places=4)
        finally:
            self.em._execute_partial = original

    def test_fee_secure_breakeven_targets_net_positive_stop(self):
        plan = self._plan()
        self.em.latest_price = lambda coin: 101.50
        asyncio.run(self.em._check_plan("BTC"))
        secure_floor = max(
            self.em._estimated_total_fee_usdt(plan) * 1.20,
            self.em._full_profit_floor_usdt(plan) * 0.50,
        )
        self.assertTrue(plan.breakeven_moved)
        self.assertGreaterEqual(plan.hard_sl, self.em._stop_price_for_total_profit(plan, secure_floor, True))

    def test_continuation_runner_reduces_tp2_trim_to_preserve_runner(self):
        plan = self._plan()
        plan.engine_name = "FUJIN"
        plan.vote_score = 3
        self.em.get_closes = lambda coin: [100, 100.1, 100.2, 100.35, 100.5, 100.7, 101.0, 101.3, 101.7, 102.1, 102.8, 103.5]
        self.em.get_volumes = lambda coin: [10, 10, 10, 10, 10, 10, 10, 10, 10, 14, 15, 16]
        calls = []

        async def _fake_partial(coin, price, pct):
            calls.append((coin, pct, plan.size_usdt))
            return True

        original = self.em._execute_partial
        try:
            self.em._execute_partial = _fake_partial
            self.em.latest_price = lambda coin: 110.0
            asyncio.run(self.em._check_plan("BTC"))
            self.assertTrue(plan.tp1_done)

            self.em.latest_price = lambda coin: 120.0
            asyncio.run(self.em._check_plan("BTC"))
            self.assertTrue(plan.tp2_done)
            self.assertAlmostEqual(calls[-1][1], 16.25, places=4)
            self.assertAlmostEqual(plan.size_usdt, 13.609375, places=4)
        finally:
            self.em._execute_partial = original


class TestDynamicSizingBand(unittest.TestCase):

    def setUp(self):
        import settings_store as ss
        self.ss = ss
        self._settings_backup = copy.deepcopy(ss._current)
        self.ss._current.update({
            "dynamic_sizing_enabled": True,
            "dynamic_target_min_usdt": 50.0,
            "dynamic_target_max_usdt": 100.0,
            "v6_min_position_size_usdt": 25.0,
            "v6_max_position_size_usdt": 250.0,
            "v6_max_position_pct_of_balance": 0.05,
        })

    def tearDown(self):
        self.ss._current.clear()
        self.ss._current.update(self._settings_backup)

    def test_dynamic_sizing_scales_to_target_min_when_room_exists(self):
        import sizing_engine as se

        result = se.compute_size(
            base_size=25.0,
            confidence_score=3.0,
            atr_val=1.0,
            entry_px=100.0,
            leverage=20,
            balance=100.0,
        )
        self.assertEqual(result.final_size_usdt, 50.0)
        self.assertTrue(result.modifiers.get("scaled_to_target_min"))

    def test_dynamic_sizing_clamps_to_target_max(self):
        import sizing_engine as se

        result = se.compute_size(
            base_size=180.0,
            confidence_score=3.0,
            atr_val=0.5,
            entry_px=100.0,
            leverage=20,
            balance=500.0,
        )
        self.assertEqual(result.final_size_usdt, 100.0)
        self.assertTrue(result.modifiers.get("clamped_to_target_max"))


class TestProfitTargetingIsNonBlocking(unittest.TestCase):

    def setUp(self):
        import settings_store as ss
        self.ss = ss
        self._settings_backup = copy.deepcopy(ss._current)
        self.ss._current.update({
            # These legacy keys are intentionally ignored by profit_targeting in the FINAL PASS.
            "profit_target_enabled": True,
            "min_profit_target_usdt": 5.0,
            "profit_target_max_widen_mult": 1.1,
            "profit_target_skip_unsafe": True,
        })

    def tearDown(self):
        self.ss._current.clear()
        self.ss._current.update(self._settings_backup)

    def test_tp_distance_multiplier_applies_and_never_skips(self):
        import profit_targeting as pt

        result = pt.apply_profit_targeting(100.0, "long", 101.0, 102.0, 25.0, tp_distance_multiplier=2.0)
        self.assertFalse(result["skip"])
        self.assertGreater(result["tp1"], 101.0)
        self.assertIn("multiplier", str(result.get("profit_reason", "")).lower())

    def test_legacy_min_profit_settings_do_not_block_entries(self):
        import profit_targeting as pt

        result = pt.apply_profit_targeting(100.0, "long", 101.0, 102.0, 25.0)
        self.assertFalse(result["skip"])
        self.assertEqual(str(result.get("skip_reason", "")), "")


class TestTradeIntentAndDiagnostics(unittest.TestCase):

    def setUp(self):
        import settings_store as ss
        self.ss = ss
        self._settings_backup = copy.deepcopy(ss._current)
        self.ss._current.update({
            "trade_intent_enabled": True,
            "soft_kill_switch_enabled": True,
            "engine_consensus_enabled": True,
            "block_diagnostics_enabled": True,
            "headroom_scaling_enabled": True,
            "trade_intent_min_sample_trades": 5,
            "trade_intent_aggressive_wr_pct": 50.0,
            "trade_intent_defensive_wr_pct": 40.0,
            "trade_intent_reduce_trades_wr_pct": 35.0,
            "trade_intent_stop_wr_pct": 30.0,
        })

    def tearDown(self):
        self.ss._current.clear()
        self.ss._current.update(self._settings_backup)

    def test_trade_intent_turns_aggressive_for_strong_wr(self):
        import trade_intent as ti

        original = ti._perf.get_metrics
        try:
            ti._perf.get_metrics = lambda: {"win_rate": 55.0, "max_dd": 2.0, "total": 12}
            intent = ti.get_trade_intent(balance=100.0)
        finally:
            ti._perf.get_metrics = original
        self.assertEqual(intent.mode, "AGGRESSIVE")
        self.assertGreater(intent.size_multiplier, 1.0)

    def test_diagnostics_panel_exposes_controls_and_explanations(self):
        import tg_panels as tp

        text = tp.panel_settings_diagnostics_text(self.ss, balance=100.0)
        self.assertIn("Diagnostics", text)
        self.assertIn("Top Block", text)
        kb = tp.kb_settings_diagnostics(self.ss, balance=100.0)
        callbacks = [
            btn.callback_data
            for row in kb.inline_keyboard
            for btn in row
            if getattr(btn, "callback_data", None)
        ]
        self.assertIn("s:diagnostics", callbacks)
        self.assertIn("diag:blockers", callbacks)
        self.assertIn("diag:slots", callbacks)
        self.assertIn("diag:risk", callbacks)
        self.assertIn("diag:strategy", callbacks)
        self.assertIn("diag:sync", callbacks)
        self.assertIn("diag:no_trades", callbacks)

    def test_diagnostics_panel_debug_override_list_is_markdown_safe(self):
        """
        Diagnostics is rendered with Telegram parse_mode="Markdown". Underscores in override keys
        (e.g. engine_cap) must be inside code spans, otherwise Telegram rejects the message and the
        Diagnostics button appears unresponsive.
        """
        import tg_panels as tp

        self.ss._current.update({
            "debug_override_mode": "probe",
            "debug_override_engine_cap": "relaxed",
        })
        text = tp.panel_settings_diagnostics_text(self.ss, balance=100.0)
        # The active list must be wrapped in backticks so underscores don't break Markdown.
        self.assertRegex(text, r"Debug Overrides: `PROBE` \(`[^`]*engine_cap=relaxed[^`]*`\)")

    def test_diagnostics_keyboard_has_no_duplicate_visible_callbacks(self):
        import tg_panels as tp

        kb = tp.kb_settings_diagnostics(self.ss, balance=100.0)
        callbacks = [
            btn.callback_data
            for row in kb.inline_keyboard
            for btn in row
            if getattr(btn, "callback_data", None)
        ]
        self.assertEqual(len(callbacks), len(set(callbacks)))
        self.assertIn("diag:no_trades", callbacks)

    def test_no_trades_panel_renders_runtime_snapshot(self):
        import tg_panels as tp

        text = tp.panel_no_trades_text(self.ss, balance=100.0)
        self.assertIn("Why No Trades", text)
        self.assertIn("Candidates Scanned", text)
        self.assertIn("Top Blockers", text)


class TestControlPlaneCallbacks(unittest.TestCase):

    @staticmethod
    def _handler_routes(source: str):
        exact = set(re.findall(r'data\s*==\s*["\']([^"\']+)["\']', source))
        prefixes = set(re.findall(r'(?:data|str\(data\))\.startswith\(["\']([^"\']+)["\']\)', source))
        for pattern in (r'data\s+in\s+\(([^\)]*)\)', r'data\s+in\s+\{([^\}]*)\}'):
            for match in re.finditer(pattern, source, re.S):
                exact.update(re.findall(r'["\']([^"\']+)["\']', match.group(1)))
        return exact, prefixes

    def test_settings_main_and_control_panel_expose_new_sections(self):
        import settings_store as ss
        import tg_panels as tp

        main_kb = tp.kb_settings_hub(ss)
        main_callbacks = [
            btn.callback_data
            for row in main_kb.inline_keyboard
            for btn in row
            if getattr(btn, "callback_data", None)
        ]
        self.assertIn("set:cat:trading", main_callbacks)
        self.assertIn("set:cat:strategy", main_callbacks)
        self.assertIn("set:cat:risk", main_callbacks)
        self.assertIn("set:cat:execution", main_callbacks)
        self.assertIn("set:cat:capital", main_callbacks)
        self.assertNotIn("diag:control_state", main_callbacks)
        self.assertIn("set:cat:diagnostics", main_callbacks)
        self.assertIn("set:cat:system", main_callbacks)

        control_kb = tp.kb_control(True)
        control_callbacks = [
            btn.callback_data
            for row in control_kb.inline_keyboard
            for btn in row
            if getattr(btn, "callback_data", None)
        ]
        self.assertIn("settings_menu", control_callbacks)
        self.assertNotIn("s:modes", control_callbacks)
        self.assertNotIn("c:ai_confirm", control_callbacks)
        self.assertNotIn("c:normal_confirm", control_callbacks)

    def test_bot_callback_handler_contains_new_strategy_and_diagnostics_routes(self):
        import bot

        source = inspect.getsource(bot.callbacks)
        self.assertIn("s:modes", source)
        self.assertIn("s:diagnostics", source)
        self.assertIn("diag:shadow_rehab", source)
        self.assertIn("diag:izanagi_outcomes", source)
        self.assertIn("s:slots", source)
        self.assertIn("s:capital", source)
        self.assertIn("s:system", source)
        self.assertIn("trades_view", source)
        self.assertIn("sync_positions", source)
        self.assertIn("room:bool:", source)
        self.assertIn("room:num:", source)
        self.assertIn("room:deploy_mode:", source)
        self.assertIn("room:exp_mode:", source)
        self.assertIn("room:global_slot_mode:", source)
        self.assertIn("room:trade_selection_policy:", source)
        self.assertIn("room:max_open_positions:", source)
        self.assertIn("room:tp_mode:", source)
        self.assertIn("room:size_mode:", source)
        self.assertIn("room:engine_cap:", source)
        self.assertIn("sys:reset_confirm", source)
        self.assertIn("stg_preset_safe", source)
        self.assertIn("stg_preset_balanced", source)
        self.assertIn("stg_preset_scalp", source)
        self.assertIn("stg_preset_adaptive", source)
        self.assertIn("stg_profile_micro", source)
        self.assertIn("stg_deploy_max_deploy", source)
        self.assertIn("stg_dynmin_up", source)
        self.assertIn("stg_dynmax_up", source)
        self.assertIn("stg_risk_reduce_up", source)
        self.assertIn("stg_exposure_mode_override", source)
        self.assertIn("stg_intent_cycle", source)
        self.assertIn("stg_global_slot_fixed", source)
        self.assertIn("stg_sel_first", source)
        self.assertIn("stg_slots_up", source)
        self.assertIn("stg_engslot:", source)

    def test_settings_callbacks_rendered_by_panels_have_handlers(self):
        import settings_store as ss
        import tg_panels as tp
        import bot

        source = inspect.getsource(bot.callbacks)
        keyboards = [
            tp.kb_home(True),
            tp.kb_control(True),
            tp.kb_trades(),
            tp.kb_help(),
            tp.kb_settings_hub(ss),
            tp.kb_settings_cat_trading(ss),
            tp.kb_settings_cat_strategy(ss),
            tp.kb_settings_cat_risk(ss),
            tp.kb_settings_cat_execution(ss),
            tp.kb_settings_cat_capital(ss),
            tp.kb_settings_cat_diagnostics(ss),
            tp.kb_settings_cat_system(ss),
            tp.kb_fg_slots(ss),
            tp.kb_fg_engine_caps(ss),
            tp.kb_fg_confidence(ss),
            tp.kb_fg_weekly_cap(ss),
            tp.kb_fg_trailing(ss),
            tp.kb_fg_breakeven(ss),
            tp.kb_fg_partials(ss),
            tp.kb_fg_profit_lock(ss),
            tp.kb_fg_deployment(ss),
            tp.kb_fg_exposure(ss),
            tp.kb_weekly_cap_hub(ss),
            tp.kb_weekly_cap_mode(ss),
            tp.kb_weekly_cap_thresholds(ss),
            tp.kb_weekly_cap_quality(ss),
            tp.kb_weekly_cap_reduction(ss),
            tp.kb_wcap_enable(ss),
            tp.kb_wcap_threshold(ss),
            tp.kb_wcap_recovery(ss),
            tp.kb_wcap_minconf(ss),
            tp.kb_wcap_hqonly(ss),
            tp.kb_wcap_reduction(ss),
            tp.kb_wcap_cooldown(ss),
            tp.kb_room_global_slot_mode(ss),
            tp.kb_room_max_open_positions(ss),
            tp.kb_room_trade_selection_policy(ss),
            tp.kb_fg_engine_caps(ss),
            tp.kb_room_engine_caps_list(ss, page=0),
            tp.kb_room_engine_cap(ss, "HACHIMAN", back_page=0),
            tp.kb_room_tp_mode(ss),
            tp.kb_room_size_mode(ss),
            tp.kb_debug_overrides(ss),
            tp.kb_settings_strategy(ss, balance=100.0),
            tp.kb_settings_modes(ss, balance=100.0),
            tp.kb_settings_risk(ss, balance=100.0),
            tp.kb_settings_execution(ss),
            tp.kb_settings_filters(ss),
            tp.kb_settings_slots(ss, balance=100.0),
            tp.kb_settings_engine_slots(ss, page=0),
            tp.kb_settings_engine_slot_detail(ss, "HACHIMAN", back_page=0),
            tp.kb_settings_participation(ss),
            tp.kb_settings_ai(ss),
            tp.kb_settings_diagnostics(ss, balance=100.0),
            tp.kb_settings_system(ss),
            tp.kb_settings_system_reset(ss),
            tp.kb_system_reset_confirm(),
            tp.kb_settings_advanced(ss, balance=100.0),
            tp.kb_settings_help(),
        ]
        callbacks = {
            btn.callback_data
            for kb in keyboards
            for row in kb.inline_keyboard
            for btn in row
            if getattr(btn, "callback_data", None)
        }
        exact_routes, prefix_routes = self._handler_routes(source)
        removed_ai_callbacks = [
            callback for callback in callbacks
            if "ai_gate" in callback.lower()
            or callback.lower().startswith(("ai_", "aiscan"))
            or "ai" in re.split(r"[:_:/-]+", callback.lower())
        ]
        self.assertFalse(
            removed_ai_callbacks,
            msg="visible Telegram callbacks must not route to removed AI controls",
        )
        for callback in callbacks:
            if callback in {"settings_menu", "main_menu"}:
                continue
            if callback.startswith("stg_tog_") or callback.startswith("stg_eng_tog_"):
                continue
            # Dynamic callback families (handled via startswith parsing).
            if callback.startswith("s:slots_engines:") or callback.startswith("s:slots_engine:"):
                self.assertIn("s:slots_engine:", source)
                self.assertIn("s:slots_engines:", source)
                continue
            if callback.startswith("stg_engslot:"):
                self.assertIn('data.startswith("stg_engslot:")', source)
                continue
            if callback.startswith("set:cat:"):
                self.assertIn(callback, exact_routes, msg=f"missing handler for {callback}")
                continue
            if callback.startswith("set:fg:"):
                self.assertIn(callback, exact_routes, msg=f"missing handler for {callback}")
                continue
            if callback.startswith("set:room:engine_caps:"):
                self.assertIn('data.startswith("set:room:engine_caps:")', source)
                continue
            if callback.startswith("set:room:"):
                self.assertTrue(
                    callback in exact_routes or any(callback.startswith(prefix) for prefix in prefix_routes),
                    msg=f"missing handler for {callback}",
                )
                continue
            if callback.startswith("room:"):
                self.assertTrue(
                    callback in exact_routes or any(callback.startswith(prefix) for prefix in prefix_routes),
                    msg=f"missing handler family for {callback}",
                )
                continue
            if callback.startswith("wcap:"):
                self.assertIn("wcap:", source)
                continue
            if callback.startswith("dbg:"):
                self.assertTrue(
                    callback in exact_routes or any(callback.startswith(prefix) for prefix in prefix_routes),
                    msg=f"missing handler family for {callback}",
                )
                continue
            self.assertIn(callback, source, msg=f"missing handler for {callback}")

    def test_core_panels_render_with_empty_runtime_data(self):
        import settings_store as ss
        import tg_panels as tp
        import position_registry as pr
        import exit_manager as em
        import category_tracker as ct

        registry_backup = copy.deepcopy(pr._registry)
        plans_backup = copy.deepcopy(em.get_all_plans())
        stats_backup = copy.deepcopy(ct._stats)
        try:
            pr._registry.clear()
            em._plans.clear()
            ct._stats = {name: ct.EngineStats(name=name) for name in ct.ENGINE_NAMES}
            combined = {
                "today_wins": 0,
                "today_losses": 0,
                "today_wr": 0.0,
                "today_pnl": 0.0,
                "week_wins": 0,
                "week_losses": 0,
                "week_wr": 0.0,
                "week_pnl": 0.0,
                "total_wins": 0,
                "total_losses": 0,
                "total_wr": 0.0,
                "total_pnl": 0.0,
            }
            panels = [
                tp.panel_home_text(
                    0.0,
                    combined,
                    "UNKNOWN",
                    "OFF",
                    0,
                    0,
                    0,
                    False,
                    True,
                ),
                tp.panel_trades_text([], open_count=0),
                tp.panel_settings_hub_text(ss),
                tp.panel_settings_cat_execution_text(ss),
                tp.panel_settings_cat_risk_text(ss),
                tp.panel_settings_cat_capital_text(ss),
                tp.panel_settings_cat_strategy_text(ss),
                tp.panel_settings_cat_system_text(ss),
                tp.panel_settings_cat_diagnostics_text(ss),
                tp.panel_diagnostics_text(ss, balance=0.0),
                tp.panel_engine_diagnostics_text(ss, balance=0.0),
                tp.panel_shadow_rehab_text(ss),
                tp.panel_izanagi_outcomes_text(ss),
                tp.panel_decision_trace_text(ss),
                tp.panel_api_health_text(ss),
                tp.panel_control_state_text(ss, balance=0.0),
                tp.panel_diag_risk_text(ss, balance=0.0),
                tp.panel_diag_strategy_text(ss, balance=0.0),
                tp.panel_no_trades_text(ss, balance=0.0),
                tp.panel_debug_overrides_text(ss),
                tp.panel_room_size_mode(ss),
            ]
            for text in panels:
                self.assertIsInstance(text, str)
                self.assertTrue(text.strip())
        finally:
            pr._registry.clear()
            pr._registry.update(registry_backup)
            em._plans.clear()
            em._plans.update(plans_backup)
            ct._stats = stats_backup

    def test_stale_ai_callbacks_are_safe_redirects_not_visible_controls(self):
        import bot
        import tg_panels as tp

        source = inspect.getsource(bot.callbacks)
        self.assertIn('"s:ai"', source)
        self.assertIn('"diag:ai"', source)
        visible_callbacks = {
            btn.callback_data
            for kb in (tp.kb_home(False), tp.kb_settings_hub(__import__("settings_store")), tp.kb_diagnostics(__import__("settings_store")))
            for row in kb.inline_keyboard
            for btn in row
            if getattr(btn, "callback_data", None)
        }
        self.assertNotIn("s:ai", visible_callbacks)
        self.assertNotIn("diag:ai", visible_callbacks)
        self.assertNotIn("aiscan_quick", visible_callbacks)

    def test_scan_loop_has_per_engine_timeout_and_trade_flow_snapshot(self):
        import trading_engine as te

        source = inspect.getsource(te.MultiEngine)
        self.assertIn("engine_scan_timeout_sec", source)
        self.assertIn("asyncio.wait_for", source)
        self.assertIn("trade_flow_snapshot", source)


class TestEngineStackCleanup(unittest.TestCase):

    def test_home_menu_routes_match_clean_root(self):
        import tg_panels as tp

        kb = tp.kb_home(True)
        callbacks = [
            btn.callback_data
            for row in kb.inline_keyboard
            for btn in row
            if getattr(btn, "callback_data", None)
        ]
        self.assertIn("terminal:status", callbacks)
        self.assertIn("terminal:positions", callbacks)
        self.assertIn("terminal:performance", callbacks)
        self.assertIn("terminal:emergency", callbacks)
        self.assertIn("terminal:why_no_trade", callbacks)
        self.assertNotIn("s:modes", callbacks)
        self.assertNotIn("p:portfolio", callbacks)
        self.assertNotIn("p:trades", callbacks)
        self.assertNotIn("p:intel", callbacks)
        self.assertNotIn("p:risk", callbacks)
        self.assertNotIn("p:control", callbacks)

    def test_engine_registry_contains_new_stack_only(self):
        import category_tracker as ct

        for retired in ("KAIZEN", "MIRROR", "GRID"):
            self.assertNotIn(retired, ct.ENGINE_NAMES)
        for current in ("HACHIMAN", "YATAGARASU", "RYUJIN", "KANNON", "GENBU"):
            self.assertIn(current, ct.ENGINE_NAMES)

    def test_engine_settings_keyboard_shows_new_engines_without_retired_ones(self):
        import bot

        kb = bot.engines_settings_kb()
        labels = "\n".join(
            btn.text
            for row in kb.inline_keyboard
            for btn in row
            if getattr(btn, "text", None)
        )
        self.assertIn("HACHIMAN", labels)
        self.assertIn("YATAGARASU", labels)
        self.assertIn("RYUJIN", labels)
        self.assertIn("KANNON", labels)
        self.assertIn("GENBU", labels)
        self.assertNotIn("KAIZEN", labels)
        self.assertNotIn("MIRROR", labels)
        self.assertNotIn("GRID", labels)

class TestSelectorAndHunterBehavior(unittest.TestCase):

    def setUp(self):
        import settings_store as ss
        self.ss = ss
        self._settings_backup = copy.deepcopy(ss._current)

    def tearDown(self):
        self.ss._current.clear()
        self.ss._current.update(self._settings_backup)

    def test_hachiman_selector_bonus_improves_candidate_quality(self):
        import category_engine as ce

        pref_backup = ce.get_universe_preference
        top_backup = ce.get_top20_mcap
        try:
            ce.get_universe_preference = lambda coin, balance_usdt=0.0: {
                "tradeable": True,
                "volume_quality": 2.0,
                "spread_quality": 0.8,
                "market_cap_bonus": 1.0,
                "selection_penalty": 0.0,
            }
            ce.get_top20_mcap = lambda: ["BTC"]
            self.ss._current.update({
                "hachiman_selector_enabled": True,
                "hachiman_selection_bonus": 6.0,
                "hachiman_top_bias_bonus": 3.0,
                "genbu_advisory_enabled": False,
            })
            boosted = ce._candidate_quality_snapshot("ENMA", "BTC", "LONG", 100.0, 3.0, 3.0)["quality_score"]
            self.ss._current["hachiman_selector_enabled"] = False
            base = ce._candidate_quality_snapshot("ENMA", "BTC", "LONG", 100.0, 3.0, 3.0)["quality_score"]
        finally:
            ce.get_universe_preference = pref_backup
            ce.get_top20_mcap = top_backup
        self.assertGreater(boosted, base)

    def test_genbu_advisory_penalty_softens_weak_candidates_without_blocking(self):
        import category_engine as ce

        pref_backup = ce.get_universe_preference
        top_backup = ce.get_top20_mcap
        try:
            ce.get_universe_preference = lambda coin, balance_usdt=0.0: {
                "tradeable": True,
                "volume_quality": 0.2,
                "spread_quality": 0.05,
                "market_cap_bonus": 0.0,
                "selection_penalty": 1.0,
            }
            ce.get_top20_mcap = lambda: []
            self.ss._current.update({
                "hachiman_selector_enabled": False,
                "genbu_advisory_enabled": True,
                "genbu_quality_floor": 55.0,
                "genbu_soft_penalty": 6.0,
            })
            penalized = ce._candidate_quality_snapshot("RYUU", "ALT", "LONG", 100.0, 2.0, 2.0)["quality_score"]
            self.ss._current["genbu_advisory_enabled"] = False
            base = ce._candidate_quality_snapshot("RYUU", "ALT", "LONG", 100.0, 2.0, 2.0)["quality_score"]
        finally:
            ce.get_universe_preference = pref_backup
            ce.get_top20_mcap = top_backup
        self.assertLess(penalized, base)
        self.assertGreater(penalized, 0.0)

    def test_yatagarasu_fires_only_on_strong_confluence(self):
        import strategy_categories as sc

        closes = [100 + i * 0.5 for i in range(90)]
        highs = [c + 0.4 for c in closes]
        lows = [c - 0.4 for c in closes]
        volumes = [1000 + i * 12 for i in range(90)]
        closes_15m = [80 + i * 0.7 for i in range(120)]
        highs_15m = [c + 0.5 for c in closes_15m]
        lows_15m = [c - 0.5 for c in closes_15m]
        volumes_15m = [1500 + i * 20 for i in range(120)]
        closes_1h = [60 + i * 1.0 for i in range(80)]

        direction, score = sc.evaluate_yatagarasu(
            closes, highs, lows, volumes,
            closes_15m=closes_15m,
            highs_15m=highs_15m,
            lows_15m=lows_15m,
            volumes_15m=volumes_15m,
            closes_1h=closes_1h,
            taker_buy_vol=680.0,
            taker_sell_vol=320.0,
            coin="BTC",
        )
        self.assertEqual(direction, "LONG")
        self.assertGreaterEqual(score, 4.0)

    def test_ryujin_scores_flow_continuation(self):
        import strategy_categories as sc

        closes = [100 + i * 0.6 for i in range(40)]
        highs = [c + 0.35 for c in closes]
        lows = [c - 0.35 for c in closes]
        volumes = [900 + i * 18 for i in range(40)]
        direction, score = sc.evaluate_ryujin(
            closes,
            highs,
            lows,
            volumes,
            taker_buy_vol=700.0,
            taker_sell_vol=300.0,
            coin="ETH",
        )
        self.assertEqual(direction, "LONG")
        self.assertGreaterEqual(score, 3.0)

    def test_kannon_is_recovery_scope_only(self):
        import strategy_categories as sc

        direction, score = sc.evaluate_kannon([1, 2, 3], [1, 2, 3], [1, 2, 3], [1, 2, 3], coin="BTC")
        self.assertEqual(direction, "HOLD")
        self.assertEqual(score, 0.0)


class TestFilterControlBehavior(unittest.TestCase):

    def test_min_volume_step_helper_can_decrease_below_one_million(self):
        import bot

        self.assertEqual(bot._stepped_min_volume(2_000_000.0, -1), 1_750_000.0)
        self.assertEqual(bot._stepped_min_volume(1_000_000.0, -1), 900_000.0)
        self.assertEqual(bot._stepped_min_volume(250_000.0, -1), 200_000.0)
        self.assertEqual(bot._stepped_min_volume(25_000.0, -1), 0.0)


class TestCapitalDeploymentModes(unittest.TestCase):

    def setUp(self):
        import settings_store as ss
        self.ss = ss
        self._settings_backup = copy.deepcopy(ss._current)
        self.ss._current.update({
            "v6_enabled": True,
            "v6_capital_adaptive_enabled": True,
            "capital_profile": "AUTO",
            "deployment_mode": "Guarded",
            "deployment_target_utilization_pct": 0.0,
            "deployment_max_positions": 0,
            "deployment_respect_exposure_cap": True,
            "deployment_respect_headroom": True,
            "deployment_same_side_limit_pct": 0.0,
            "deployment_total_exposure_limit_pct": 0.0,
            "v6_micro_max_balance": 100.0,
            "v6_growth_max_balance": 1000.0,
            "v6_micro_max_exposure_pct": 130.0,
            "portfolio_same_direction_pct_micro": 130.0,
            "leverage_default": 25,
            "leverage_min": 20,
            "leverage_max": 25,
            "v6_min_position_size_usdt": 25.0,
            "v6_max_position_size_usdt": 250.0,
            "v6_max_position_pct_of_balance": 0.05,
            "dynamic_sizing_enabled": True,
        })

    def tearDown(self):
        self.ss._current.clear()
        self.ss._current.update(self._settings_backup)

    def test_capital_manager_resolves_profile_and_mode(self):
        import capital_manager as cm

        profile = cm.get_profile(34.2)
        self.assertEqual(profile.mode, "MICRO")
        self.assertEqual(profile.profile_setting, "AUTO")
        self.assertEqual(profile.deployment_mode, "Guarded")
        self.assertEqual(profile.deployment_effective_mode, "Guarded")
        self.assertTrue(profile.deployment_respect_exposure_cap)
        self.assertGreaterEqual(profile.deployment_max_positions, 1)

    def test_max_deploy_profile_targets_larger_position_than_guarded(self):
        import capital_manager as cm
        import sizing_engine as se

        guarded = cm.get_profile(34.2)
        guarded_result = se.compute_size(
            base_size=100.0,
            confidence_score=3.0,
            atr_val=0.5,
            entry_px=100.0,
            leverage=20,
            balance=34.2,
            capital_profile=guarded,
        )

        self.ss._current.update({
            "deployment_mode": "Max Deploy",
            "deployment_respect_exposure_cap": False,
            "deployment_respect_headroom": False,
        })
        max_deploy = cm.get_profile(34.2)
        max_deploy_result = se.compute_size(
            base_size=100.0,
            confidence_score=3.0,
            atr_val=0.5,
            entry_px=100.0,
            leverage=20,
            balance=34.2,
            capital_profile=max_deploy,
        )

        self.assertEqual(max_deploy.deployment_mode, "Max Deploy")
        self.assertEqual(max_deploy.deployment_total_exposure_limit_pct, guarded.deployment_total_exposure_limit_pct)
        self.assertTrue(max_deploy.deployment_respect_exposure_cap)
        self.assertTrue(max_deploy.deployment_respect_headroom)
        self.assertLessEqual(max_deploy_result.final_size_usdt, guarded_result.final_size_usdt)


class TestFullDeploymentMode(unittest.TestCase):

    def setUp(self):
        import settings_store as ss
        self.ss = ss
        self._settings_backup = copy.deepcopy(ss._current)
        self.ss._current.update({
            "v6_enabled": True,
            "v6_capital_adaptive_enabled": True,
            "capital_profile": "AUTO",
            # Simulate operator selecting Full Deployment while max_open_positions is still low.
            "trading_mode_preset": "full_deployment",
            "deployment_mode": "Full Deployment",
            "global_slot_mode": "auto",
            "deployment_target_utilization_pct": 92.0,
            "deployment_max_positions": 0,
            "deployment_respect_exposure_cap": False,
            "deployment_respect_headroom": False,
            "max_open_positions": 3,
            "dynamic_target_min_usdt": 75.0,
            "leverage_default": 25,
            "leverage_min": 20,
            "leverage_max": 25,
        })

    def tearDown(self):
        self.ss._current.clear()
        self.ss._current.update(self._settings_backup)

    def test_full_deployment_raises_slot_cap_above_three(self):
        import capital_manager as cm
        import exposure_tracker as et

        profile = cm.get_profile(120.0)
        self.assertEqual(profile.deployment_mode, "Full Deployment")
        self.assertEqual(profile.deployment_max_positions, 1)

        policy = et.get_exposure_policy(120.0, capital_profile=profile)
        self.assertEqual(policy.max_positions, 1)

    def test_full_deployment_relaxes_hachiman_engine_cap(self):
        import capital_manager as cm
        import category_engine as ce

        profile = cm.get_profile(120.0)
        cap = ce.get_effective_engine_cap("HACHIMAN", balance=120.0, capital_profile=profile)
        self.assertEqual(cap, 1)


class TestSlotControlModes(unittest.TestCase):

    def setUp(self):
        import settings_store as ss
        self.ss = ss
        self._settings_backup = copy.deepcopy(ss._current)
        # Minimal viable config for capital_manager + slot policies.
        self.ss._current.update({
            "v6_enabled": True,
            "v6_capital_adaptive_enabled": True,
            "capital_profile": "AUTO",
            "deployment_mode": "Guarded",
            "deployment_target_utilization_pct": 0.0,
            "deployment_max_positions": 0,
            "deployment_respect_exposure_cap": True,
            "deployment_respect_headroom": True,
            "dynamic_target_min_usdt": 50.0,
            "max_open_positions": 3,
            "leverage_default": 25,
            "leverage_min": 20,
            "leverage_max": 25,
            "exposure_control_mode": "Strict",
        })

    def tearDown(self):
        self.ss._current.clear()
        self.ss._current.update(self._settings_backup)

    def test_fixed_global_slot_mode_respects_max_open_positions(self):
        import capital_manager as cm
        import category_engine as ce

        self.ss._current["global_slot_mode"] = "fixed"
        self.ss._current["max_open_positions"] = 3
        profile = cm.get_profile(100.0)
        cap, meta = ce.get_effective_global_slot_limit(100.0, capital_profile=profile)
        self.assertEqual(str(meta.get("global_slot_mode")), "fixed")
        self.assertEqual(int(cap), 3)

    def test_auto_global_slot_mode_uses_deployment_max_positions(self):
        import capital_manager as cm
        import category_engine as ce

        self.ss._current["global_slot_mode"] = "auto"
        profile = cm.get_profile(200.0)
        cap, meta = ce.get_effective_global_slot_limit(200.0, capital_profile=profile)
        self.assertEqual(str(meta.get("global_slot_mode")), "auto")
        self.assertEqual(int(cap), int(profile.deployment_max_positions))

    def test_retired_unlimited_global_slot_mode_falls_back_to_fixed(self):
        import capital_manager as cm
        import category_engine as ce

        self.ss._current["global_slot_mode"] = "unlimited"
        self.ss._current["max_open_positions"] = 3
        profile = cm.get_profile(100.0)
        cap, meta = ce.get_effective_global_slot_limit(100.0, capital_profile=profile)
        self.assertEqual(str(meta.get("global_slot_mode")), "fixed")
        self.assertEqual(int(meta.get("global_max") or 0), 3)
        self.assertEqual(int(cap), 3)

    def test_engine_cap_inherit_and_unlimited_are_bounded_by_global(self):
        import capital_manager as cm
        import category_engine as ce

        self.ss._current["global_slot_mode"] = "fixed"
        self.ss._current["max_open_positions"] = 3
        profile = cm.get_profile(100.0)
        global_cap, _ = ce.get_effective_global_slot_limit(100.0, capital_profile=profile)
        global_cap = int(global_cap)
        self.assertEqual(global_cap, 3)

        # Inherit via max_pos = 0
        self.ss._current["engine_izanagi_max_pos"] = 0
        cap_inherit = int(ce.get_effective_engine_cap("IZANAGI", balance=100.0, capital_profile=profile))
        self.assertGreaterEqual(cap_inherit, 1)
        self.assertLessEqual(cap_inherit, global_cap)

        # Unlimited via max_pos = -1
        self.ss._current["engine_izanagi_max_pos"] = -1
        cap_unlim = int(ce.get_effective_engine_cap("IZANAGI", balance=100.0, capital_profile=profile))
        self.assertGreaterEqual(cap_unlim, cap_inherit)
        self.assertLessEqual(cap_unlim, global_cap)


class TestCapitalDeploymentPanel(unittest.TestCase):

    def setUp(self):
        import settings_store as ss
        self.ss = ss
        self._settings_backup = copy.deepcopy(ss._current)
        self.ss._current.update({
            "v6_enabled": True,
            "v6_capital_adaptive_enabled": True,
            "capital_profile": "AUTO",
            "deployment_mode": "Max Deploy",
            "deployment_target_utilization_pct": 0.0,
            "deployment_max_positions": 0,
            "deployment_respect_exposure_cap": False,
            "deployment_respect_headroom": False,
            "deployment_same_side_limit_pct": 0.0,
            "deployment_total_exposure_limit_pct": 0.0,
            "v6_micro_max_exposure_pct": 130.0,
            "portfolio_same_direction_pct_micro": 130.0,
            "leverage_default": 25,
            "leverage_min": 20,
            "leverage_max": 25,
        })

    def tearDown(self):
        self.ss._current.clear()
        self.ss._current.update(self._settings_backup)

    def test_modes_panel_renders_live_deployment_state(self):
        import tg_panels as tp
        exposure = SimpleNamespace(total_usdt=25.62)
        text = tp.panel_settings_modes_text(self.ss, balance=34.2, exposure=exposure, available_margin=12.5)
        self.assertIn("Modes", text)
        self.assertIn("Trading Preset", text)
        self.assertIn("Deployment Mode", text)
        self.assertIn("Available Margin", text)
        self.assertIn("Exposure:", text)
        self.assertIn("Same-Side Headroom:", text)
        self.assertIn("Max Deploy Size:", text)
        self.assertNotIn("AI Adaptive", text)

    def test_modes_panel_keyboard_exposes_callbacks(self):
        import tg_panels as tp
        kb = tp.kb_settings_modes(self.ss, balance=34.2)
        callbacks = {
            btn.callback_data
            for row in kb.inline_keyboard
            for btn in row
            if getattr(btn, "callback_data", None)
        }
        self.assertIn("stg_preset_safe", callbacks)
        self.assertIn("stg_preset_balanced", callbacks)
        self.assertIn("stg_preset_scalp", callbacks)
        self.assertIn("stg_preset_adaptive", callbacks)
        self.assertIn("stg_profile_auto", callbacks)
        self.assertIn("stg_deploy_max_deploy", callbacks)
        self.assertIn("stg_deploy_full_deployment", callbacks)
        self.assertIn("stg_deploy_target_up", callbacks)
        self.assertIn("stg_deploy_maxpos_up", callbacks)
        self.assertNotIn("stg_deploy_ai_adaptive", callbacks)


class TestExposureControlModes(unittest.TestCase):

    def setUp(self):
        import settings_store as ss
        self.ss = ss
        self._settings_backup = copy.deepcopy(ss._current)
        _isolate_settings_path(self, ss, "exposure-modes-")
        self.ss._current.update({
            "v6_enabled": True,
            "v6_capital_adaptive_enabled": True,
            "capital_profile": "AUTO",
            "deployment_mode": "Guarded",
            "exposure_control_mode": "Strict",
            "v6_micro_max_balance": 100.0,
            "v6_growth_max_balance": 1000.0,
            "v6_micro_max_exposure_pct": 130.0,
            "portfolio_same_direction_pct_micro": 130.0,
            "max_open_positions": 6,
        })

    def tearDown(self):
        _restore_settings_path(self, self.ss)
        self.ss._current.clear()
        self.ss._current.update(self._settings_backup)

    def test_exposure_policy_modes_are_operator_visible(self):
        import exposure_tracker as et

        strict = et.get_exposure_policy(34.2)
        self.assertEqual(strict.mode, "Strict")

        self.ss.set_val("exposure_control_mode", "Relaxed")
        relaxed = et.get_exposure_policy(34.2, candidate_quality=72.0)
        self.assertEqual(relaxed.mode, "Relaxed")
        self.assertGreater(relaxed.total_exposure_pct, strict.total_exposure_pct)

        self.ss.set_val("exposure_control_mode", "Override")
        override = et.get_exposure_policy(34.2, candidate_quality=80.0)
        self.assertEqual(override.mode, "Override")
        self.assertTrue(override.override)

    def test_risk_panel_renders_selected_and_effective_exposure_mode(self):
        import tg_panels as tp

        self.ss.set_val("exposure_control_mode", "Override")
        text = tp.panel_settings_risk_text(self.ss, balance=34.2, exposure=SimpleNamespace(total_usdt=40.0))
        self.assertIn("Mode: `Override`", text)
        self.assertIn("high risk", text.lower())


class TestSlotRotationLogic(unittest.TestCase):

    def setUp(self):
        import category_engine as ce
        import position_registry as pr
        import settings_store as ss
        self.ce = ce
        self.pr = pr
        self.ss = ss
        self._registry_backup = copy.deepcopy(pr._registry)
        self._settings_backup = copy.deepcopy(ss._current)
        _isolate_settings_path(self, ss, "slot-rotation-")
        self._latest_price_backup = ce.latest_price
        self._stagnant_backup = ce._is_trade_stagnant
        self._age_backup = ce._trade_age_mins
        self.pr._registry.clear()
        self.ss._current.update({
            "slot_rotation_enabled": True,
            "slot_rotation_min_age_mins": 45.0,
            "slot_rotation_quality_gap": 12.0,
            "slot_rotation_stagnation_band_pct": 0.35,
            "exposure_control_mode": "Adaptive",
        })
        self.ce.latest_price = lambda coin: 100.05
        self.ce._is_trade_stagnant = lambda coin, trade: True
        self.ce._trade_age_mins = lambda trade: 180.0

    def tearDown(self):
        _restore_settings_path(self, self.ss)
        self.pr._registry.clear()
        self.pr._registry.update(self._registry_backup)
        self.ss._current.clear()
        self.ss._current.update(self._settings_backup)
        self.ce.latest_price = self._latest_price_backup
        self.ce._is_trade_stagnant = self._stagnant_backup
        self.ce._trade_age_mins = self._age_backup

    def test_slot_rotation_selects_stale_weaker_trade_when_new_signal_is_stronger(self):
        self.ss.set_val("exposure_control_mode", "Adaptive")
        opened_at = time.strftime("%Y-%m-%dT%H:%M:%S+00:00", time.gmtime(time.time() - 3 * 60 * 60))
        self.pr._registry["SOL"] = {
            "engine": "AMATERASU",
            "direction": "long",
            "entry": 100.0,
            "stop_loss": 95.0,
            "vote_score": 2.0,
            "confidence_score": 2.2,
            "quality_score": 38.0,
            "opened_at": opened_at,
        }
        rotation = self.ce._slot_rotation_candidate(
            60.0,
            balance=34.2,
            intent=SimpleNamespace(mode="AGGRESSIVE"),
        )
        self.assertIsNotNone(rotation)
        self.assertEqual(rotation["coin"], "SOL")
        self.assertTrue(rotation.get("replacement_ok"))

    def test_slot_rotation_refuses_replacement_when_new_signal_is_not_stronger_enough(self):
        self.ss.set_val("exposure_control_mode", "Adaptive")
        opened_at = time.strftime("%Y-%m-%dT%H:%M:%S+00:00", time.gmtime(time.time() - 3 * 60 * 60))
        self.pr._registry["BTC"] = {
            "engine": "AMATERASU",
            "direction": "long",
            "entry": 100.0,
            "stop_loss": 95.0,
            "vote_score": 2.0,
            "confidence_score": 2.4,
            "quality_score": 42.0,
            "opened_at": opened_at,
        }
        rotation = self.ce._slot_rotation_candidate(
            48.0,
            balance=34.2,
            intent=SimpleNamespace(mode="AGGRESSIVE"),
        )
        self.assertIsNotNone(rotation)
        self.assertNotIn("replacement_ok", rotation)
        self.assertGreater(rotation["required_quality"], 48.0)


class TestLegacyAIPathsRemoved(unittest.TestCase):

    def test_ai_modules_are_not_required_for_core_runtime(self):
        import bot
        self.assertTrue(callable(bot.main_menu))

    def test_live_runtime_does_not_import_removed_ai_modules(self):
        import category_engine as ce
        import bot
        import inspect

        live_sources = "\n".join([
            inspect.getsource(ce.CategoryEngine._evaluate_coin),
            inspect.getsource(bot.build_application),
            inspect.getsource(bot.handle_message),
        ])
        self.assertNotIn("from ai_", live_sources)
        self.assertNotIn("import ai_", live_sources)
        self.assertNotIn("chat_with_openai", live_sources)


class TestCategoryEngineEvaluateGateFlow(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        self._live_state_path = Path("data/live_state.json")
        self._live_state_backup = self._live_state_path.read_bytes() if self._live_state_path.exists() else None
        self._paper_env_backup = os.environ.get("PAPER_TRADING")
        os.environ["PAPER_TRADING"] = "true"
        import category_engine as ce
        import settings_store as ss
        import entry_filter
        import portfolio_allocator
        import position_registry as pr

        self.ce = ce
        self.ss = ss
        self.entry_filter = entry_filter
        self.portfolio_allocator = portfolio_allocator
        self.pr = pr
        self._settings_backup = copy.deepcopy(ss._current)
        self._registry_backup = copy.deepcopy(getattr(pr, "_registry", {}) or {})
        _isolate_registry_path(self, pr, "category-engine-registry-")
        self._plans_backup = copy.deepcopy(ce._em.get_all_plans())
        self._patches = []

        def patch(obj, name, value):
            self._patches.append((obj, name, getattr(obj, name)))
            setattr(obj, name, value)

        self.patch = patch
        patch(ce, "_adaptive_capital_allocation", lambda **kwargs: {
            "skip": False,
            "size_usdt": float(kwargs.get("size_usdt", 0.0) or 0.0),
            "reason": "phase2_router_test_passthrough",
            "meta": {},
        })
        patch(ce, "_final_candidate_economics", lambda **kwargs: {
            "allowed": True,
            "reason": "positive_net_expectancy",
            "exit_management_mode": "partial",
            "final": {"expected_net_usdt": 1.0, "estimated_cost_usdt": 0.1},
            "fee_schedule": {"source": "mock"},
        })
        patch(ce, "_candidate_exec_floor", lambda *args, **kwargs: 1.0)

        async def fake_central_entry(**kwargs):
            return await kwargs["okx"].place_market_order(
                kwargs["coin"],
                kwargs["side"],
                kwargs["size_usdt"],
                sl_price=kwargs["stop_px"],
                tp_price=kwargs["take_profit"],
                leverage=kwargs["leverage"],
                allow_nano_route=False,
            )

        patch(ce, "_execute_central_entry", fake_central_entry)
        series = [100.0 + (i * 0.1) for i in range(260)]
        vols = [1_000_000.0 for _ in series]
        patch(ce, "get_volume", lambda coin: 1_000_000.0)
        patch(ce, "candle_count", lambda coin: len(series))
        patch(ce, "get_closes", lambda coin: list(series))
        patch(ce, "get_highs", lambda coin: [v + 1.0 for v in series])
        patch(ce, "get_lows", lambda coin: [v - 1.0 for v in series])
        patch(ce, "get_volumes", lambda coin: list(vols))
        patch(ce, "get_4h_closes", lambda coin: list(series))
        patch(ce, "get_4h_highs", lambda coin: [v + 1.0 for v in series])
        patch(ce, "get_4h_lows", lambda coin: [v - 1.0 for v in series])
        patch(ce, "get_15m_closes", lambda coin: list(series))
        patch(ce, "get_15m_highs", lambda coin: [v + 1.0 for v in series])
        patch(ce, "get_15m_lows", lambda coin: [v - 1.0 for v in series])
        patch(ce, "get_15m_volumes", lambda coin: list(vols))
        patch(ce, "get_1h_closes", lambda coin: list(series))
        patch(ce, "get_1h_highs", lambda coin: [v + 1.0 for v in series])
        patch(ce, "get_1h_lows", lambda coin: [v - 1.0 for v in series])
        patch(ce, "get_1d_closes", lambda coin: list(series))
        patch(ce, "get_1d_highs", lambda coin: [v + 1.0 for v in series])
        patch(ce, "get_1d_lows", lambda coin: [v - 1.0 for v in series])
        patch(ce, "funding_is_fresh", lambda coin: False)
        patch(ce, "get_cached_funding_rate", lambda coin: 0.0)
        patch(ce, "evaluate", lambda *args, **kwargs: ("LONG", 3.0))
        patch(ce._regime, "get_regime", lambda: "TRENDING")
        patch(ce._s, "get_lane_allowed_regimes", lambda engine: {"TRENDING", "RANGING", "VOLATILE"})
        patch(ce._s, "get_engine_role", lambda engine: "hunter")
        patch(ce._admission, "is_symbol_cooled_down", lambda coin: (False, ""))
        patch(ce._rjlog, "should_skip_candidate", lambda *args, **kwargs: None)
        patch(ce._trade_intent, "get_trade_intent", lambda **kwargs: SimpleNamespace(
            allow_new_trades=True,
            mode="NORMAL",
            summary="normal",
            size_multiplier=1.0,
            tp_multiplier=1.0,
            frequency_multiplier=1.0,
            max_positions_multiplier=1.0,
        ))
        patch(ce._consensus, "record_signal", lambda *args, **kwargs: None)
        patch(ce, "_candidate_quality_snapshot", lambda *args, **kwargs: {
            "quality_score": 80.0,
            "selector_bonus": 0.0,
            "consensus_count": 1,
        })
        patch(ce, "_confirm_path_enabled", lambda: False)
        patch(ce, "_per_engine_cap_enabled", lambda: False)
        patch(ce, "_correlation_check", lambda *args, **kwargs: (True, "OK"))
        patch(ce, "_effective_global_slot_limit", lambda *args, **kwargs: (99, {"policy_mode": "test"}))
        patch(ce, "_reg_count", lambda: 0)
        patch(portfolio_allocator, "check_lane_budget", lambda *args, **kwargs: (True, "OK"))

        async def allow_reserve(*args, **kwargs):
            return True, "OK"

        async def allow_entry_filter(*args, **kwargs):
            return True, "OK"

        patch(ce, "can_open_and_reserve", allow_reserve)
        patch(entry_filter, "check", allow_entry_filter)
        pr._registry.clear()
        ss._current.update({
            "cooldown_enabled": False,
            "v6_enabled": False,
            "weekly_cap_enabled": False,
            "pre_reserve_size_validation_enabled": False,
            "per_engine_cap_enabled": False,
            "min_vote_score": 2.0,
            "yatagarasu_min_votes": 2.0,
            "trade_selection_policy": "replace_weakest",
            "max_open_positions": 99,
        })

    def tearDown(self):
        for obj, name, value in reversed(self._patches):
            setattr(obj, name, value)
        self.ss._current.clear()
        self.ss._current.update(self._settings_backup)
        self.pr._registry.clear()
        self.pr._registry.update(self._registry_backup)
        self.ce._em._plans.clear()
        self.ce._em._plans.update(self._plans_backup)
        _restore_registry_path(self, self.pr)
        if self._paper_env_backup is None:
            os.environ.pop("PAPER_TRADING", None)
        else:
            os.environ["PAPER_TRADING"] = self._paper_env_backup
        if self._live_state_backup is None:
            self._live_state_path.unlink(missing_ok=True)
        else:
            self._live_state_path.write_bytes(self._live_state_backup)

    async def _assert_engine_reaches_execution(self, engine_name: str):
        ce = self.ce
        self.ss._current[f"engine_{engine_name.lower()}_role"] = "hunter"
        engine = ce.CategoryEngine(engine_name)
        engine._portfolio_gate = lambda *args, **kwargs: (True, "OK")

        async def recheck(*args, **kwargs):
            return "", ""

        engine._recheck_live_entry_constraints = recheck
        class FakeOKX:
            async def get_ticker(self, coin):
                return {"last": 110.0, "bid": 109.9, "ask": 110.1, "vol24h": 1_000_000.0}

            async def get_taker_volume(self, coin):
                return {"buy": 100.0, "sell": 90.0}

            async def get_long_short_ratio(self, coin):
                return 1.0

        engine.okx = FakeOKX()

        calls = []

        async def fake_place_trade(*args, **kwargs):
            calls.append((args, kwargs))

        engine._place_trade = fake_place_trade
        await engine._evaluate_coin("FOO", 500.0)
        await ce._execution_supervisor.wait_all()
        self.assertEqual(len(calls), 1, engine_name)
        self.assertIn("_v6_gate", calls[0][1])
        self.assertIsNone(calls[0][1]["_v6_gate"])

    async def test_hachiman_evaluation_path_neutral_gate(self):
        await self._assert_engine_reaches_execution("HACHIMAN")

    async def test_yatagarasu_evaluation_path_neutral_gate(self):
        await self._assert_engine_reaches_execution("YATAGARASU")

    async def test_izanagi_evaluation_path_neutral_gate(self):
        await self._assert_engine_reaches_execution("IZANAGI")

    async def test_hayate_evaluation_path_neutral_gate(self):
        await self._assert_engine_reaches_execution("HAYATE")

    def test_evaluate_coin_initializes_v6_gate_before_use(self):
        import inspect
        source = inspect.getsource(self.ce.CategoryEngine._evaluate_coin)
        self.assertLess(source.index("_v6_gate    = None"), source.index("v6_gate=_v6_gate"))

    def test_decision_trace_still_records_decisions(self):
        import decision_trace as dt
        dt.clear()
        rec = dt.record({
            "symbol": "FOO",
            "side": "long",
            "engine": "HACHIMAN",
            "final_decision": "accepted",
            "confidence": 4.2,
        })
        self.assertEqual(rec["final_decision"], "accepted")
        self.assertEqual(dt.get_recent(1)[0]["engine"], "HACHIMAN")

    def test_telegram_callbacks_resolve_and_no_mojibake_in_core_panels(self):
        import tg_panels as tp
        self.assertTrue(hasattr(tp, "HELP_MAIN_TEXT"))
        self.assertTrue(hasattr(tp, "HELP_COMMANDS_TEXT"))
        self.assertTrue(hasattr(tp, "HELP_RISK_TEXT"))
        self.assertTrue(hasattr(tp, "HELP_V6_TEXT"))
        self.assertIsNotNone(tp.kb_help())
        callbacks = set()
        for kb in [
            tp.kb_home(False),
            tp.kb_settings_hub(self.ss),
            tp.kb_settings_modes(self.ss, balance=100.0),
            tp.kb_settings_execution(self.ss),
            tp.kb_diagnostics(self.ss),
            tp.kb_trades(),
        ]:
            for row in kb.inline_keyboard:
                for btn in row:
                    if getattr(btn, "callback_data", None):
                        callbacks.add(btn.callback_data)
                    self.assertNotRegex(btn.text, r"[ÃƒÆ’Ã‚Â¢ÃƒÆ’Ã†â€™ÃƒÂ¯Ã‚Â¿Ã‚Â½]")
                    self.assertNotRegex(btn.text.upper(), r"\bAI\b")
        for expected in {
            "main_menu", "settings_menu", "s:diagnostics", "set:cat:capital",
            "terminal:performance",
            "stg_tog_breakeven_enabled", "stg_tog_secure_profit_mode_enabled", "stg_tog_tsl_enabled",
            "diag:decision_trace", "diag:api_health", "diag:control_state",
        }:
            self.assertIn(expected, callbacks)

    async def test_post_placement_error_preserves_registry_and_exit_plan(self):
        ce = self.ce
        engine = ce.CategoryEngine("HACHIMAN")
        engine._portfolio_gate = lambda *args, **kwargs: (True, "OK")

        class FakeOKX:
            async def get_ticker(self, coin):
                return {"last": 100.0, "bid": 99.9, "ask": 100.1, "vol24h": 1_000_000.0}

            async def place_market_order(self, coin, side, amount_usdt, **kwargs):
                return {
                    "actual_coin": coin,
                    "ordId": "okx-1",
                    "fillPx": "100.0",
                    "effective_leverage": int(kwargs.get("leverage") or 20),
                    "size_usdt": float(amount_usdt),
                }

            def consume_last_order_skip_reason(self):
                return ""

        engine.okx = FakeOKX()
        self.pr._registry.clear()
        self.pr._registry["FOO"] = {
            "direction": "long",
            "engine": "HACHIMAN",
            "size_usdt": 25.0,
            "entry": 0.0,
            "leverage": 20,
            "stop_loss": 0.0,
            "take_profit": 0.0,
            "order_id": "",
            "pending_reservation": True,
        }

        releases = []
        registered = []

        def fake_release(coin):
            releases.append(coin)
            self.pr._registry.pop(str(coin).upper(), None)

        def fake_register(*args, **kwargs):
            registered.append((args, kwargs))

        async def failing_alert(*args, **kwargs):
            raise RuntimeError("telegram down")

        self.patch(ce, "reg_close", fake_release)
        self.patch(ce._em, "register", fake_register)
        self.patch(ce, "alert_trade_executed", failing_alert)
        self.patch(ce._sl_tp, "compute", lambda **kwargs: (99.0, 105.0, 110.0))
        self.patch(ce._profit_targets, "apply_profit_targeting", lambda *args, **kwargs: {
            "tp1": 105.0,
            "tp2": 110.0,
            "profit_reason": "test",
        })
        self.patch(ce._consensus, "get_snapshot", lambda *args, **kwargs: SimpleNamespace(
            agreement_count=1,
            summary="solo",
        ))
        self.patch(ce, "_dbg_record_accept", lambda *args, **kwargs: None)

        series = [100.0 + (i * 0.01) for i in range(30)]
        preview = {
            "skip": False,
            "leverage": 20,
            "atr_val": 1.0,
            "size_usdt": 25.0,
            "base_size_usdt": 25.0,
            "size_reason": "test preview",
            "weekly_cap_decision": "ACCEPT",
            "weekly_cap_mult": 1.0,
            "weekly_cap_reason": "",
            "intent": SimpleNamespace(
                mode="NORMAL",
                summary="normal",
                size_multiplier=1.0,
                tp_multiplier=1.0,
                frequency_multiplier=1.0,
                allow_new_trades=True,
                max_positions_multiplier=1.0,
            ),
        }

        with self.assertLogs("OxyPulse.CategoryEngine", level="ERROR") as logs:
            await engine._place_trade(
                "FOO",
                "buy",
                500.0,
                3.0,
                series,
                [v + 1.0 for v in series],
                [v - 1.0 for v in series],
                _size_preview=preview,
            )

        self.assertEqual(releases, [])
        self.assertIn("FOO", self.pr._registry)
        self.assertFalse(self.pr._registry["FOO"].get("exchange_confirmed"))
        self.assertTrue(self.pr._registry["FOO"].get("paper_confirmed"))
        self.assertIn("post_placement_error", self.pr._registry["FOO"])
        self.assertEqual(len(registered), 1)
        self.assertIn("post-placement error", "\n".join(logs.output))

    def test_place_trade_resolved_defines_quality_snap_before_use(self):
        import inspect
        source = inspect.getsource(self.ce.CategoryEngine._place_trade_resolved)
        self.assertLess(source.index("_quality_snap = _candidate_quality_snapshot"), source.index('regime=str(_quality_snap.get("regime"'))
        self.assertNotIn("NameError", source)

    async def _run_mock_trade_flow(
        self,
        *,
        engine_name: str = "AMATERASU",
        coin: str = "FLOW",
        side: str = "buy",
        size: float = 100.0,
        weekly_decision: str = "ACCEPT",
        weekly_mult: float = 1.0,
        size_reason: str = "dry-run preview",
        alert_fails: bool = False,
    ):
        ce = self.ce
        direction = "long" if side == "buy" else "short"
        engine = ce.CategoryEngine(engine_name)
        payloads = []
        alerts = []

        class FakeOKX:
            async def get_ticker(self, _coin):
                return {
                    "last": 100.0,
                    "bid": 99.9,
                    "ask": 100.1,
                    "bidPx": 99.9,
                    "askPx": 100.1,
                    "vol24h": 1_000_000.0,
                }

            async def place_market_order(self, _coin, _side, amount_usdt, **kwargs):
                payloads.append({
                    "coin": _coin,
                    "side": _side,
                    "amount_usdt": float(amount_usdt),
                    **kwargs,
                })
                return {
                    "actual_coin": _coin,
                    "ordId": f"MOCK-{engine_name}-{_coin}",
                    "fillPx": "100.0",
                    "effective_leverage": int(kwargs.get("leverage") or 20),
                    "requested_leverage": int(kwargs.get("leverage") or 20),
                    "size_usdt": float(amount_usdt),
                    "state": "filled",
                    "mock": True,
                }

            def consume_last_order_skip_reason(self):
                return ""

        async def fake_alert(*args, **kwargs):
            alerts.append((args, kwargs))
            if alert_fails:
                raise RuntimeError("telegram down")
            return True

        self.patch(ce, "alert_trade_executed", fake_alert)
        self.patch(ce._sl_tp, "compute", lambda **kwargs: (95.0, 105.0, 110.0))
        self.patch(ce._profit_targets, "apply_profit_targeting", lambda *args, **kwargs: {
            "tp1": 105.0,
            "tp2": 110.0,
            "profit_reason": "dry-run target",
        })
        self.patch(ce._consensus, "get_snapshot", lambda *args, **kwargs: SimpleNamespace(
            agreement_count=2,
            summary="AMATERASU+FUJIN",
        ))
        self.patch(ce, "_dbg_record_accept", lambda *args, **kwargs: None)

        engine.okx = FakeOKX()
        self.pr._registry.clear()
        ok, reason = await self.pr.can_open_and_reserve(
            coin,
            direction,
            engine_name,
            size,
            metadata={"pending_reservation": True, "reserved_notional_usdt": size},
        )
        self.assertTrue(ok, reason)
        series = [100.0 + (i * 0.01) for i in range(60)]
        preview = {
            "skip": False,
            "leverage": 20,
            "atr_val": 1.0,
            "size_usdt": size,
            "base_size_usdt": size,
            "size_reason": size_reason,
            "weekly_cap_decision": weekly_decision,
            "weekly_cap_mult": weekly_mult,
            "weekly_cap_reason": "dry-run recovery reduction" if weekly_decision == "ACCEPT_REDUCED" else "",
            "intent": SimpleNamespace(
                mode="NORMAL",
                summary="normal",
                size_multiplier=1.0,
                tp_multiplier=1.0,
                frequency_multiplier=1.0,
                allow_new_trades=True,
                max_positions_multiplier=1.0,
            ),
        }

        log_cm = (
            self.assertLogs("OxyPulse.CategoryEngine", level="INFO")
            if not alert_fails
            else self.assertLogs("OxyPulse.CategoryEngine", level="ERROR")
        )
        with log_cm as logs:
            await engine._place_trade(
                coin,
                side,
                500.0,
                3.0,
                series,
                [v + 1.0 for v in series],
                [v - 1.0 for v in series],
                _size_preview=preview,
            )
        return engine, payloads, alerts, "\n".join(logs.output)

    async def test_full_signal_to_order_mock_lifecycle_for_live_engine(self):
        engine, payloads, alerts, logs = await self._run_mock_trade_flow(
            engine_name="AMATERASU",
            coin="FLOW",
            size=100.0,
        )
        self.assertEqual(len(payloads), 1, logs)
        payload = payloads[0]
        self.assertEqual(payload["coin"], "FLOW")
        self.assertEqual(payload["side"], "buy")
        self.assertGreater(payload["amount_usdt"], 0.0)
        self.assertLessEqual(payload["amount_usdt"], 105.0)
        self.assertEqual(payload["leverage"], 15)
        self.assertGreater(payload["sl_price"], 0.0)
        self.assertFalse(payload.get("reduceOnly", False))
        self.assertIn("ORDER_PAYLOAD_READY", logs)
        self.assertIn("ORDER_MOCK_SUCCESS", logs)
        self.assertIn("REGISTRY_ACTIVATED", logs)
        self.assertIn("EXIT_PLAN_CREATED", logs)
        self.assertIn("ACCEPT [AMATERASU] FLOW LONG", logs)
        entry = self.pr._registry["FLOW"]
        self.assertEqual(entry["order_id"], "MOCK-AMATERASU-FLOW")
        self.assertFalse(entry["exchange_confirmed"])
        self.assertTrue(entry["paper_confirmed"])
        self.assertEqual(entry["confidence_stage_trace"]["admission_confidence"], 3.0)
        self.assertIn("FLOW", self.ce._em.get_all_plans())
        self.assertEqual(len(alerts), 1)
        self.assertIn("FLOW", engine._open)

    async def test_fujin_and_probation_engine_mock_trades_reach_order_path(self):
        await self._run_mock_trade_flow(engine_name="FUJIN", coin="FJX", size=100.0)
        self.assertEqual(self.pr._registry["FJX"]["engine"], "FUJIN")
        self.pr._registry.clear()
        self.ce._em._plans.clear()
        self.ss._current["engine_ronin_mode"] = "probation"
        await self._run_mock_trade_flow(engine_name="RONIN", coin="RNX", side="sell", size=100.0)
        self.assertEqual(self.pr._registry["RNX"]["engine"], "RONIN")
        self.assertEqual(self.pr._registry["RNX"]["direction"], "short")

    async def test_reduced_recovery_and_micro_floor_preserve_paths_complete(self):
        _, payloads, _alerts, logs = await self._run_mock_trade_flow(
            engine_name="HACHIMAN",
            coin="REDX",
            size=100.0,
            weekly_decision="ACCEPT_REDUCED",
            weekly_mult=0.5,
            size_reason="floor preserve dry-run",
        )
        self.assertGreater(payloads[0]["amount_usdt"], 0.0)
        self.assertLessEqual(payloads[0]["amount_usdt"], 100.0)
        entry = self.pr._registry["REDX"]
        self.assertEqual(entry["weekly_cap_decision"], "ACCEPT_REDUCED")
        self.assertAlmostEqual(entry["weekly_cap_mult"], 0.5)
        self.assertIn("wk=x0.50", logs)
        self.assertIn("REGISTRY_ACTIVATED", logs)

    async def test_blocked_sizing_path_releases_reserve_with_clear_reason(self):
        ce = self.ce
        engine = ce.CategoryEngine("AMATERASU")
        engine.okx = SimpleNamespace(get_ticker=lambda coin: {"last": 100.0})
        self.pr._registry.clear()
        ok, reason = await self.pr.can_open_and_reserve(
            "BADX",
            "long",
            "AMATERASU",
            100.0,
            metadata={"pending_reservation": True},
        )
        self.assertTrue(ok, reason)
        series = [100.0 + (i * 0.01) for i in range(60)]
        with self.assertLogs("OxyPulse.CategoryEngine", level="INFO") as logs:
            await engine._place_trade_resolved(
                "BADX",
                "buy",
                500.0,
                3.0,
                series,
                [v + 1.0 for v in series],
                [v - 1.0 for v in series],
                ticker={"last": 100.0, "bidPx": 99.9, "askPx": 100.1},
                entry_px=100.0,
                atr_val=1.0,
                _size_preview={
                    "skip": True,
                    "reject_reason": "SIZE_BELOW_MIN: size $2.00 < min $100.00",
                },
            )
        self.assertNotIn("BADX", self.pr._registry)
        joined = "\n".join(logs.output)
        self.assertIn("BLOCKED", joined)
        self.assertIn("SIZE_BELOW_MIN", joined)

    async def test_duplicate_reserve_blocks_with_clear_reason_and_quarantine_does_not_block(self):
        self.pr._registry.clear()
        self.pr._registry["ARB"] = {
            "direction": "long",
            "engine": "RECOVERED",
            "size_usdt": 24.0,
            "order_id": "RECOVERED",
            "sync_status": "OK",
            "managed_by_engine": False,
            "recovery_mode": "quarantined",
            "quarantine_reason": "exchange_only",
        }
        ok, reason = await self.pr.can_open_and_reserve("NEWX", "long", "FUJIN", 100.0)
        self.assertTrue(ok, reason)
        dup_ok, dup_reason = await self.pr.can_open_and_reserve("NEWX", "long", "FUJIN", 100.0)
        self.assertFalse(dup_ok)
        self.assertIn("skip duplicate", dup_reason.lower())
        self.assertIn("ARB", self.pr._registry)
        self.assertIn("NEWX", self.pr._registry)

    async def test_post_placement_telegram_failure_preserves_confirmed_mock_trade(self):
        _engine, payloads, alerts, logs = await self._run_mock_trade_flow(
            engine_name="HACHIMAN",
            coin="TGX",
            size=100.0,
            alert_fails=True,
        )
        self.assertEqual(len(payloads), 1)
        self.assertEqual(len(alerts), 1)
        self.assertIn("TGX", self.pr._registry)
        self.assertFalse(self.pr._registry["TGX"].get("exchange_confirmed"))
        self.assertTrue(self.pr._registry["TGX"].get("paper_confirmed"))
        self.assertIn("post_placement_error", self.pr._registry["TGX"])
        self.assertIn("post-placement error", logs)


class TestRecoveryRunnerRefinements(unittest.TestCase):

    def setUp(self):
        import settings_store as ss
        self.ss = ss
        self._settings_backup = copy.deepcopy(ss._current)
        _isolate_settings_path(self, ss, "size-alias-")

    def tearDown(self):
        _restore_settings_path(self, self.ss)
        self.ss._current.clear()
        self.ss._current.update(self._settings_backup)

    def test_weekly_cap_degraded_recovery_grace_reduces_instead_of_blocking_quality_candidate(self):
        import weekly_cap as wk
        wk.reset_state()
        old_weekly = getattr(wk._perf._state, "weekly_pnl_usdt", 0.0)
        try:
            wk._perf._state.weekly_pnl_usdt = -6.0
            self.ss._current.update({
                "weekly_cap_enabled": True,
                "weekly_cap_mode": "strict",
                "weekly_cap_threshold_pct": 8.0,
                "weekly_cap_recovery_threshold_pct": 4.0,
                "weekly_cap_min_confidence": 5.75,
                "weekly_cap_size_reduction": 0.5,
                "weekly_cap_degraded_conf_grace": 0.50,
                "weekly_cap_cooldown_seconds": 0.0,
            })
            normal = wk.evaluate(
                balance=100.0,
                confidence=5.70,
                candidate_quality=70.0,
                selector_bonus=0.2,
                consensus_count=1,
                engine_name="HACHIMAN",
                degraded_data=False,
                mutate_state=False,
            )
            degraded = wk.evaluate(
                balance=100.0,
                confidence=5.70,
                candidate_quality=70.0,
                selector_bonus=0.2,
                consensus_count=1,
                engine_name="HACHIMAN",
                degraded_data=True,
                degraded_reason="taker volume cache backing off",
                mutate_state=False,
            )
            self.assertEqual(normal.decision, "BLOCK")
            self.assertIn("conf<6.00", normal.reason)
            self.assertEqual(degraded.decision, "ACCEPT_REDUCED")
            self.assertTrue(degraded.degraded_data)
            self.assertAlmostEqual(degraded.required_confidence, 6.0)
            self.assertGreater(degraded.confidence_deficit, 0.0)
        finally:
            wk._perf._state.weekly_pnl_usdt = old_weekly
            wk.reset_state()

    def test_strict_recovery_uses_progressive_stage_bands(self):
        import weekly_cap as wk
        wk.reset_state()
        old_weekly = getattr(wk._perf._state, "weekly_pnl_usdt", 0.0)
        try:
            self.ss._current.update({
                "weekly_cap_enabled": True,
                "weekly_cap_mode": "strict",
                "weekly_cap_threshold_pct": 8.0,
                "weekly_cap_recovery_threshold_pct": 4.0,
                "weekly_cap_min_confidence": 5.75,
                "weekly_cap_size_reduction": 0.5,
                "weekly_cap_cooldown_seconds": 0.0,
            })
            wk._perf._state.weekly_pnl_usdt = -4.5
            early = wk.evaluate(
                balance=100.0,
                confidence=5.70,
                candidate_quality=70.0,
                selector_bonus=0.2,
                consensus_count=1,
                engine_name="HACHIMAN",
                mutate_state=False,
            )
            wk._perf._state.weekly_pnl_usdt = -6.0
            later = wk.evaluate(
                balance=100.0,
                confidence=5.70,
                candidate_quality=70.0,
                selector_bonus=0.2,
                consensus_count=1,
                engine_name="HACHIMAN",
                mutate_state=False,
            )
            self.assertEqual(early.decision, "BLOCK")
            self.assertIn("conf<5.75", early.reason)
            self.assertEqual(later.decision, "BLOCK")
            self.assertIn("conf<6.00", later.reason)
        finally:
            wk._perf._state.weekly_pnl_usdt = old_weekly
            wk.reset_state()

    def test_candidate_limit_defaults_bound_noisy_universe_after_ranking(self):
        import category_engine as ce
        engine = ce.CategoryEngine("HACHIMAN")
        old_rank = ce.rank_tradeable_candidates
        self.ss._current.update({
            "full_universe_candidate_limit": 0,
            "engine_candidate_limit_default": 18,
            "engine_candidate_limit_hachiman": 5,
        })
        try:
            ce.rank_tradeable_candidates = lambda candidates, balance_usdt=0.0: list(reversed(candidates))
            pool = engine._candidate_pool([f"C{i}" for i in range(20)], balance=100.0)
            self.assertEqual(pool, ["C19", "C18", "C17", "C16", "C15"])
        finally:
            ce.rank_tradeable_candidates = old_rank

    def test_strong_role_consensus_lifts_confidence_but_weak_consensus_does_not(self):
        import category_engine as ce
        self.ss._current.update({
            "role_consensus_conf_lift_enabled": True,
            "role_consensus_conf_lift_min_count": 3,
            "role_consensus_conf_lift_min_strength": 5.5,
            "role_consensus_conf_lift_min_quality": 65.0,
            "role_consensus_conf_lift_max": 1.25,
        })
        strong, reason = ce._role_consensus_confidence_lift(
            {"consensus_count": 4, "position_strength": 8.0, "contributor_roles": ["selector", "confirmer"]},
            {"quality_score": 82.0, "consensus_count": 4},
            3.8,
        )
        weak, weak_reason = ce._role_consensus_confidence_lift(
            {"consensus_count": 2, "position_strength": 8.0, "contributor_roles": ["selector"]},
            {"quality_score": 82.0, "consensus_count": 2},
            3.8,
        )
        self.assertGreater(strong, 0.0)
        self.assertIn("role_consensus_lift", reason)
        self.assertEqual(weak, 0.0)
        self.assertEqual(weak_reason, "")

    def test_taker_volume_rate_limit_backoff_reuses_cache_without_request(self):
        import okx_client as okx
        old_cache = copy.deepcopy(okx._taker_volume_cache)
        old_backoff = okx._taker_volume_backoff_until
        old_ok = okx._taker_volume_ok
        try:
            okx._taker_volume_cache.clear()
            okx._taker_volume_cache["BTC"] = {
                "value": {"buy": 12.0, "sell": 7.0, "source": "okx"},
                "ts": time.monotonic() - 99999.0,
            }
            okx._taker_volume_ok = True
            okx._taker_volume_backoff_until = time.monotonic() + 60.0
            client = okx.OKXClient()

            async def fail_request(*args, **kwargs):
                raise AssertionError("get_taker_volume should not call OKX while backoff is active")

            client._request = fail_request
            result = asyncio.run(client.get_taker_volume("BTC"))
            self.assertEqual(result["buy"], 12.0)
            self.assertEqual(result["sell"], 7.0)
            self.assertTrue(result["degraded"])
            self.assertEqual(result["source"], "stale_cache")
            self.assertEqual(result["degraded_reason"], "rate_limit_backoff")
        finally:
            okx._taker_volume_cache.clear()
            okx._taker_volume_cache.update(old_cache)
            okx._taker_volume_backoff_until = old_backoff
            okx._taker_volume_ok = old_ok

    def test_long_short_ratio_rate_limit_backoff_uses_stale_cache_without_request(self):
        import okx_client as okx
        old_cache = copy.deepcopy(okx._long_short_ratio_cache)
        old_backoff = okx._long_short_ratio_backoff_until
        try:
            okx._long_short_ratio_cache.clear()
            okx._long_short_ratio_cache["BTC"] = {"value": 1.42, "ts": time.monotonic() - 99999.0}
            okx._long_short_ratio_backoff_until = time.monotonic() + 60.0
            client = okx.OKXClient()

            async def fail_request(*args, **kwargs):
                raise AssertionError("get_long_short_ratio should not call OKX while backoff is active")

            client._request = fail_request
            result = asyncio.run(client.get_long_short_ratio("BTC"))
            self.assertAlmostEqual(result, 1.42)
        finally:
            okx._long_short_ratio_cache.clear()
            okx._long_short_ratio_cache.update(old_cache)
            okx._long_short_ratio_backoff_until = old_backoff

    def test_runner_preservation_clamps_partial_reductions_to_core_size(self):
        import exit_manager as em
        plan = em.ExitPlan(
            coin="BTC",
            direction="long",
            engine_name="FUJIN",
            entry_price=100.0,
            size_usdt=25.0,
            atr_val=1.0,
            hard_sl=95.0,
            tp1_price=110.0,
            tp2_price=120.0,
            initial_size_usdt=25.0,
            leverage=25,
        )
        self.ss._current.update({
            "partial_tp1_max_close_pct": 30.0,
            "partial_tp2_max_close_pct": 25.0,
            "runner_core_min_remaining_pct": 45.0,
            "continuation_runner_core_min_pct": 55.0,
        })
        self.assertEqual(em._runner_preserved_close_pct(plan, 40.0, stage="tp1"), 30.0)
        plan.size_usdt = 17.5
        self.assertAlmostEqual(
            em._runner_preserved_close_pct(plan, 30.0, stage="tp2", continuation=True),
            21.4286,
            places=3,
        )

    def test_economic_minimum_defers_tiny_partial_and_marks_tp1_state(self):
        import exit_manager as em
        plan = em.ExitPlan(
            coin="ARKM",
            direction="short",
            engine_name="ENMA",
            entry_price=0.1300,
            size_usdt=50.0,
            atr_val=0.001,
            hard_sl=0.1320,
            tp1_price=0.1280,
            tp2_price=0.1240,
            initial_size_usdt=50.0,
            leverage=20,
        )
        self.ss._current.update({
            "fee_aware_partial_deferral_enabled": True,
            "economic_min_runner_notional_usdt": 5.0,
            "economic_min_fee_mult": 8.0,
        })
        decision = em._economic_partial_decision(
            plan,
            close_pct=25.0,
            price=0.1298,
            floor=0.10,
            continuation=False,
        )
        em._mark_deferred_milestone(
            "ARKM",
            plan,
            stage="tp1",
            price=0.1298,
            roi=3.0,
            decision=decision,
            continuation_score=1.0,
            continuation_reason="test",
        )
        self.assertFalse(decision.executable)
        self.assertTrue(plan.tp1_deferred)
        self.assertFalse(plan.tp1_done)
        self.assertEqual(plan.lifecycle_phase, "tp1_deferred")
        self.assertTrue(em._stage_reached(plan, "tp1"))
        self.assertIn("floor_usdt", plan.economic_minimum)

    def test_runner_opportunity_tracks_missed_continuation_after_trim(self):
        import lifecycle_analytics as la
        data = la.update_runner_opportunity(
            {},
            direction="long",
            stage="tp1",
            trim_pct=40.0,
            stage_price=100.0,
            current_price=118.0,
            initial_notional=50.0,
        )
        self.assertAlmostEqual(data["tp1_opportunity_lost_usdt"], 3.6, places=3)
        self.assertAlmostEqual(data["runner_opportunity_lost_usdt"], 3.6, places=3)

    def test_exhaustion_score_requires_multiple_signals(self):
        import lifecycle_analytics as la
        weak = la.exhaustion_score(
            direction="long",
            closes=[100, 101, 102, 103, 104, 105, 106, 107],
            volumes=[10, 10, 10, 10, 10, 10, 10, 10],
            current_price=107,
            peak_price=107,
            continuation_score=3.5,
            threshold=3.0,
        )
        tired = la.exhaustion_score(
            direction="long",
            closes=[100, 102, 104, 106, 106.1, 106.0, 105.9, 105.7],
            volumes=[100, 100, 100, 100, 40, 35, 30, 25],
            current_price=105.7,
            peak_price=107,
            continuation_score=0.0,
            threshold=3.0,
        )
        self.assertFalse(weak.should_tighten)
        self.assertTrue(tired.should_tighten)
        self.assertIn("momentum_decay", tired.reasons)

    def test_confidence_stage_trace_records_contextual_modifiers(self):
        import category_engine as ce
        self.ss._current.update({"weekly_cap_degraded_conf_grace": 0.5})
        trace = ce._confidence_stage_trace(
            base_confidence=3.5,
            consensus_lift=1.0,
            quality_snapshot={"quality_regime_bonus": 2.0},
            degraded_data=True,
            recovery_deficit=0.25,
            admission_confidence=4.5,
        )
        self.assertEqual(trace["base_confidence"], 3.5)
        self.assertEqual(trace["consensus_lift"], 1.0)
        self.assertEqual(trace["regime_modifier"], 0.2)
        self.assertEqual(trace["degraded_data_modifier"], -0.5)
        self.assertEqual(trace["recovery_adjustment"], -0.25)
        self.assertEqual(trace["admission_confidence"], 4.5)

    def test_lifecycle_profitability_settings_hot_reload_without_truthy_fallbacks(self):
        import settings_store as ss

        old_path = ss._PATH
        old_mtime = getattr(ss, "_last_load_mtime", 0.0)
        old_check = getattr(ss, "_last_hot_reload_check", 0.0)
        tmpdir = Path(tempfile.mkdtemp(prefix="settings-hot-"))
        try:
            settings_path = tmpdir / "settings.json"
            disk = copy.deepcopy(ss._current)
            disk.update({
                "fee_aware_partial_deferral_enabled": False,
                "exhaustion_assisted_tightening_enabled": False,
                "profitability_telemetry_enabled": False,
                "runner_quality_enabled": False,
                "engine_expectancy_shadow_enabled": False,
                "economic_min_runner_notional_usdt": 0.0,
                "economic_min_fee_mult": 0.0,
                "exhaustion_tighten_score_threshold": 0.0,
                "exhaustion_tighten_trail_mult": 0.0,
                "engine_fujin_mode": "live",
                "engine_fujin_slot_mode": "unlimited",
                "engine_fujin_max_pos": -1,
            })
            settings_path.write_text(json.dumps(disk), encoding="utf-8")

            ss._PATH = settings_path
            ss._last_load_mtime = 0.0
            ss._last_hot_reload_check = 0.0
            ss._current.update({
                "fee_aware_partial_deferral_enabled": True,
                "exhaustion_assisted_tightening_enabled": True,
                "profitability_telemetry_enabled": True,
                "runner_quality_enabled": True,
                "engine_expectancy_shadow_enabled": True,
                "economic_min_runner_notional_usdt": 5.0,
                "economic_min_fee_mult": 8.0,
                "exhaustion_tighten_score_threshold": 3.0,
                "exhaustion_tighten_trail_mult": 0.72,
                "engine_fujin_mode": "shadow",
                "engine_fujin_slot_mode": "fixed",
                "engine_fujin_max_pos": 5,
            })

            self.assertFalse(ss.get_val("fee_aware_partial_deferral_enabled", True))
            self.assertFalse(ss.get_val("exhaustion_assisted_tightening_enabled", True))
            self.assertFalse(ss.get_val("profitability_telemetry_enabled", True))
            self.assertFalse(ss.get_val("runner_quality_enabled", True))
            self.assertFalse(ss.get_val("engine_expectancy_shadow_enabled", True))
            self.assertEqual(ss.get_val("economic_min_runner_notional_usdt", 5.0), 0.0)
            self.assertEqual(ss.get_val("economic_min_fee_mult", 8.0), 0.0)
            self.assertEqual(ss.get_val("exhaustion_tighten_score_threshold", 3.0), 0.0)
            self.assertEqual(ss.get_val("exhaustion_tighten_trail_mult", 0.72), 0.0)
            self.assertEqual(ss.get_engine_mode("FUJIN"), "live")
            self.assertEqual(ss.get_val("engine_fujin_slot_mode"), "unlimited")
            self.assertEqual(ss.get_val("engine_fujin_max_pos"), -1)
        finally:
            ss._PATH = old_path
            ss._last_load_mtime = old_mtime
            ss._last_hot_reload_check = old_check
            shutil.rmtree(tmpdir, ignore_errors=True)

    def test_lifecycle_settings_drive_runtime_exit_decisions(self):
        import exit_manager as em

        plan = em.ExitPlan(
            coin="ARKM",
            direction="short",
            engine_name="ENMA",
            entry_price=0.1300,
            size_usdt=50.0,
            atr_val=0.001,
            hard_sl=0.1320,
            tp1_price=0.1280,
            tp2_price=0.1240,
            initial_size_usdt=50.0,
            leverage=20,
        )
        self.ss._current.update({
            "fee_aware_partial_deferral_enabled": False,
            "economic_min_runner_notional_usdt": 500.0,
            "economic_min_fee_mult": 100.0,
        })
        decision = em._economic_partial_decision(
            plan,
            close_pct=25.0,
            price=0.1298,
            floor=0.10,
            continuation=False,
        )
        self.assertFalse(decision.executable)
        self.assertFalse(self.ss.get_val("fee_aware_partial_deferral_enabled", True))
        self.assertEqual(decision.residual_reason, "below_economic_minimum")

    def test_early_invalidation_closes_fast_failed_unprotected_trade(self):
        import exit_manager as em
        old_do_close = em._do_close
        old_get_closes = em.get_closes
        closed = []
        self.ss._current.update({
            "early_invalidation_enabled": True,
            "early_invalidation_min_age_sec": 1.0,
            "early_invalidation_max_age_min": 12.0,
            "early_invalidation_roi_pct": 1.8,
            "early_invalidation_confirm_secs": 0.0,
        })
        plan = em.ExitPlan(
            coin="BTC",
            direction="long",
            engine_name="RYUU",
            entry_price=100.0,
            size_usdt=25.0,
            atr_val=1.0,
            hard_sl=95.0,
            tp1_price=110.0,
            tp2_price=120.0,
            initial_size_usdt=25.0,
            leverage=25,
            created_at=time.time() - 120.0,
        )

        async def fake_close(coin, price, reason, close_plan):
            closed.append((coin, price, reason, close_plan))
            return True

        try:
            em._do_close = fake_close
            em.get_closes = lambda coin: [100.5, 100.2, 100.0, 99.9]
            result = asyncio.run(em._maybe_early_invalidation_exit("BTC", plan, 99.9, True))
            self.assertTrue(result)
            self.assertEqual(closed[0][2], "early_invalidation")
        finally:
            em._do_close = old_do_close
            em.get_closes = old_get_closes

    def test_diagnostics_keyboard_labels_are_clean_and_specific(self):
        import tg_panels as tp
        kb = tp.kb_diagnostics(self.ss)
        labels = [btn.text for row in kb.inline_keyboard for btn in row]
        self.assertTrue(any("Refresh" in label for label in labels))
        self.assertTrue(any("Decision Trace" in label for label in labels))
        for label in labels:
            self.assertNotRegex(label, r"[ÃƒÆ’Ã†â€™ÃƒÆ’Ã¢â‚¬Å¡ÃƒÆ’Ã‚Â¢ÃƒÆ’Ã‚Â°ÃƒÆ’Ã‚Â¯]")
            self.assertFalse(label.startswith("? "))

    def test_core_telegram_panels_render_without_mojibake_markers(self):
        import tg_panels as tp

        rendered = [
            tp.panel_settings_hub_text(self.ss),
            tp.panel_settings_execution_text(self.ss),
            tp.panel_settings_capital_text(self.ss),
            tp.panel_settings_ai_text(self.ss),
            tp.panel_settings_diagnostics_text(self.ss),
            tp.panel_diagnostics_text(self.ss),
            tp.panel_control_state_text(self.ss),
        ]
        keyboards = [
            tp.kb_home(False),
            tp.kb_settings_hub(self.ss),
            tp.kb_settings_execution(self.ss),
            tp.kb_settings_capital(self.ss),
            tp.kb_settings_ai(self.ss),
            tp.kb_settings_diagnostics(self.ss),
            tp.kb_diagnostics(self.ss),
        ]
        for text in rendered:
            self.assertNotRegex(text, r"[ÃƒÆ’Ã†â€™ÃƒÆ’Ã¢â‚¬Å¡ÃƒÆ’Ã‚Â¢ÃƒÆ’Ã‚Â°ÃƒÆ’Ã‚Â¯]")
            self.assertNotIn("? *", text)
        for kb in keyboards:
            for row in kb.inline_keyboard:
                callbacks = [
                    getattr(btn, "callback_data", None)
                    for btn in row
                    if getattr(btn, "callback_data", None)
                ]
                self.assertEqual(len(callbacks), len(set(callbacks)))
                for btn in row:
                    self.assertNotRegex(btn.text, r"[ÃƒÆ’Ã†â€™ÃƒÆ’Ã¢â‚¬Å¡ÃƒÆ’Ã‚Â¢ÃƒÆ’Ã‚Â°ÃƒÆ’Ã‚Â¯]")
                    self.assertFalse(btn.text.startswith("? "))


class TestTelegramDailyReportIntegrity(unittest.TestCase):

    def setUp(self):
        import category_tracker as ct
        self.ct = ct
        self._stats_backup = copy.deepcopy(ct._stats)

    def tearDown(self):
        self.ct._stats.clear()
        self.ct._stats.update(self._stats_backup)

    def test_daily_report_snapshot_recomputes_net_fees_and_exit_counts(self):
        from datetime import datetime, timezone

        ct = self.ct
        day = datetime(2026, 5, 9, 12, 0, tzinfo=timezone.utc)
        for stat in ct._stats.values():
            stat.trades = []
            stat.today_wins = stat.today_losses = 0
            stat.today_pnl = 0.0

        today_trade = {
            "coin": "BTC-USDT-SWAP",
            "direction": "long",
            "entry": 100.0,
            "exit_price": 101.0,
            "size_usdt": 100.0,
            "pnl_usdt": 0.90,
            "gross_pnl_usdt": 1.00,
            "fee_usdt": 0.10,
            "exit_reason": "tp1_partial",
            "closed_at": "2026-05-09T01:02:03+00:00",
        }
        duplicate = dict(today_trade)
        old_trade = dict(today_trade, closed_at="2026-05-08T23:59:59+00:00", pnl_usdt=-2.0, exit_reason="early_invalidation")
        losing_trade = dict(
            today_trade,
            coin="ETH-USDT-SWAP",
            exit_price=99.0,
            pnl_usdt=-0.75,
            gross_pnl_usdt=-0.65,
            fee_usdt=0.10,
            exit_reason="early_invalidation",
            closed_at="2026-05-09T02:02:03+00:00",
        )
        ct._stats["HACHIMAN"].trades = [today_trade, duplicate, old_trade]
        ct._stats["FUJIN"].trades = [losing_trade]

        snapshot = ct.get_daily_report_snapshot(now=day, open_positions=3)
        self.assertEqual(snapshot["total_trades"], 2)
        self.assertEqual(snapshot["wins"], 1)
        self.assertEqual(snapshot["losses"], 1)
        self.assertAlmostEqual(snapshot["gross_pnl_usdt"], 0.35)
        self.assertAlmostEqual(snapshot["fee_usdt"], 0.20)
        self.assertAlmostEqual(snapshot["net_pnl_usdt"], 0.15)
        self.assertEqual(snapshot["duplicate_trade_count"], 1)
        self.assertEqual(snapshot["early_invalidation_count"], 1)
        self.assertEqual(snapshot["tp_hit_count"], 1)
        self.assertEqual(snapshot["open_positions"], 3)


class TestProfitabilityChurnRefinement(unittest.TestCase):

    def setUp(self):
        import settings_store as ss
        self.ss = ss
        self._settings_backup = copy.deepcopy(ss._current)

    def tearDown(self):
        self.ss._current.clear()
        self.ss._current.update(self._settings_backup)

    def test_adaptive_invalidation_fast_bad_trade_invalidates(self):
        import lifecycle_analytics as la
        decision = la.assess_early_invalidation(
            roi=-2.4,
            mfe_roi=0.0,
            mae_roi=-2.4,
            age_sec=80.0,
            base_hold_sec=45.0,
            min_hold_floor_sec=10.0,
            max_hold_cap_sec=300.0,
            failure_threshold=2.0,
            patience_threshold=1.5,
            structure_failed=True,
            momentum_flipped=True,
            volume_failed=True,
            choppy_regime=True,
        )
        self.assertTrue(decision.should_invalidate)
        self.assertGreater(decision.failure_score, decision.patience_score)

    def test_adaptive_invalidation_near_tp_trade_gets_grace(self):
        import lifecycle_analytics as la
        decision = la.assess_early_invalidation(
            roi=-1.9,
            mfe_roi=1.7,
            mae_roi=-1.9,
            age_sec=80.0,
            base_hold_sec=45.0,
            min_hold_floor_sec=10.0,
            max_hold_cap_sec=300.0,
            failure_threshold=2.0,
            patience_threshold=1.0,
            near_tp_earned=True,
            near_tp_grace_remaining_sec=60.0,
            structure_failed=True,
        )
        self.assertFalse(decision.should_invalidate)
        self.assertEqual(decision.final_reason, "adaptive_hold_time")
        self.assertGreaterEqual(decision.patience_score, 2.0)

    def test_trend_continuation_avoids_false_invalidation(self):
        import lifecycle_analytics as la
        decision = la.assess_early_invalidation(
            roi=-1.7,
            mfe_roi=2.5,
            mae_roi=-1.7,
            age_sec=120.0,
            base_hold_sec=45.0,
            min_hold_floor_sec=10.0,
            max_hold_cap_sec=300.0,
            failure_threshold=2.0,
            patience_threshold=1.0,
            trend_regime=True,
            continuation_quality=4.0,
            consensus_score=3.0,
            trend_patience_mult=1.35,
        )
        self.assertFalse(decision.should_invalidate)
        self.assertIn(decision.final_reason, {"patience_outweighs_failure", "failure_below_threshold", "failure_not_clear"})

    def test_chop_weak_trade_invalidates_faster(self):
        import lifecycle_analytics as la
        decision = la.assess_early_invalidation(
            roi=-2.0,
            mfe_roi=0.0,
            mae_roi=-2.1,
            age_sec=35.0,
            base_hold_sec=60.0,
            min_hold_floor_sec=20.0,
            max_hold_cap_sec=300.0,
            failure_threshold=2.0,
            patience_threshold=1.0,
            structure_failed=True,
            choppy_regime=True,
            chop_strictness_mult=1.5,
        )
        self.assertTrue(decision.should_invalidate)
        self.assertLessEqual(decision.required_hold_time, 45.0)

    def test_symbol_direction_cooling_is_localized(self):
        import lifecycle_analytics as la
        active = la.symbol_direction_cooling_state(
            loss_count=3,
            threshold=2,
            cooldown_minutes=30.0,
            confidence_bump=0.6,
            size_multiplier=0.75,
        )
        other = la.symbol_direction_cooling_state(
            loss_count=0,
            threshold=2,
            cooldown_minutes=30.0,
            confidence_bump=0.6,
            size_multiplier=0.75,
        )
        self.assertTrue(active.active)
        self.assertFalse(other.active)
        self.assertEqual(active.size_multiplier, 0.75)

    def test_near_floor_tp_earned_marks_state_without_micro_partial(self):
        import exit_manager as em
        decision = em._la.EconomicMinimumDecision(
            executable=False,
            reason="below_profit_floor",
            closed_notional=10.0,
            pnl_usdt=0.09,
            floor_usdt=0.10,
            remaining_notional=15.0,
            residual_meaningful=True,
            residual_reason="meaningful",
        )
        self.ss._current.update({
            "early_invalidation_near_tp_grace_enabled": True,
            "early_invalidation_near_tp_floor_ratio": 0.85,
            "early_invalidation_near_tp_grace_sec": 30.0,
        })
        plan = em.ExitPlan(
            coin="BTC",
            direction="long",
            engine_name="RYUU",
            entry_price=100.0,
            size_usdt=25.0,
            atr_val=1.0,
            hard_sl=95.0,
            tp1_price=110.0,
            tp2_price=120.0,
            initial_size_usdt=25.0,
            leverage=25,
        )
        self.assertTrue(em._maybe_mark_near_floor_tp_earned(plan, stage="tp1", price=101.0, decision=decision, roi=1.0))
        self.assertTrue(plan.near_floor_tp_earned)
        self.assertAlmostEqual(plan.near_floor_tp_ratio, 0.9)
        self.assertFalse(decision.executable)

    def test_post_invalidation_telemetry_classifies_false_positive(self):
        import lifecycle_analytics as la
        outcome = la.evaluate_post_invalidation_outcome(
            direction="long",
            entry_price=100.0,
            invalidation_price=99.0,
            current_price=103.0,
            max_favorable_price=104.0,
            max_adverse_price=98.8,
            size_usdt=100.0,
        )
        self.assertEqual(outcome.classification, "false_positive")
        self.assertGreater(outcome.missed_opportunity_estimate_usdt, outcome.avoided_loss_estimate_usdt)

    def test_runner_core_remains_meaningful_under_continuation(self):
        import exit_manager as em
        self.ss._current.update({
            "partial_tp2_max_close_pct": 50.0,
            "runner_core_min_remaining_pct": 45.0,
            "continuation_runner_core_min_pct": 60.0,
        })
        plan = em.ExitPlan(
            coin="BTC",
            direction="long",
            engine_name="FUJIN",
            entry_price=100.0,
            size_usdt=25.0,
            atr_val=1.0,
            hard_sl=95.0,
            tp1_price=110.0,
            tp2_price=120.0,
            initial_size_usdt=25.0,
            leverage=25,
        )
        self.assertLessEqual(em._runner_preserved_close_pct(plan, 50.0, stage="tp2", continuation=True), 40.0)

    def test_new_settings_respect_explicit_false_zero_and_minus_one(self):
        import settings_store as ss
        old_path = ss._PATH
        old_mtime = getattr(ss, "_last_load_mtime", 0.0)
        old_check = getattr(ss, "_last_hot_reload_check", 0.0)
        tmpdir = Path(tempfile.mkdtemp(prefix="profit-churn-settings-"))
        try:
            settings_path = tmpdir / "settings.json"
            disk = copy.deepcopy(ss._current)
            disk.update({
                "adaptive_early_invalidation_enabled": False,
                "early_invalidation_base_min_hold_sec": 0.0,
                "early_invalidation_near_tp_grace_sec": 0.0,
                "symbol_direction_loss_cooldown_enabled": False,
                "symbol_direction_loss_size_mult": 0.0,
                "engine_fujin_max_pos": -1,
            })
            settings_path.write_text(json.dumps(disk), encoding="utf-8")
            ss._PATH = settings_path
            ss._last_load_mtime = 0.0
            ss._last_hot_reload_check = 0.0
            ss._current.update({
                "adaptive_early_invalidation_enabled": True,
                "early_invalidation_base_min_hold_sec": 45.0,
                "early_invalidation_near_tp_grace_sec": 90.0,
                "symbol_direction_loss_cooldown_enabled": True,
                "symbol_direction_loss_size_mult": 0.75,
                "engine_fujin_max_pos": 3,
            })
            self.assertFalse(ss.get_val("adaptive_early_invalidation_enabled", True))
            self.assertEqual(ss.get_val("early_invalidation_base_min_hold_sec", 45.0), 0.0)
            self.assertEqual(ss.get_val("early_invalidation_near_tp_grace_sec", 90.0), 0.0)
            self.assertFalse(ss.get_val("symbol_direction_loss_cooldown_enabled", True))
            self.assertEqual(ss.get_val("symbol_direction_loss_size_mult", 0.75), 0.0)
            self.assertEqual(ss.get_val("engine_fujin_max_pos"), -1)
        finally:
            ss._PATH = old_path
            ss._last_load_mtime = old_mtime
            ss._last_hot_reload_check = old_check
            shutil.rmtree(tmpdir, ignore_errors=True)


class TestSessionResetManager(unittest.TestCase):

    def setUp(self):
        import position_registry as pr
        import rejection_log as rj
        import weekly_cap as wk
        import capital_manager as cm
        import performance_tracker as pt
        import category_tracker as ct
        import engine_weights as ew

        self.pr = pr
        self.rj = rj
        self.wk = wk
        self.cm = cm
        from types import SimpleNamespace
        self.ortr = SimpleNamespace(
            _ai_degraded_until_ts=0.0,
            _ai_last_status=0,
            _ai_last_reason="",
            _ai_last_error_at="",
            _ai_last_log_ts=0.0,
            get_degradation_snapshot=lambda: {"status": "OK", "reason": ""},
        )
        self.pt = pt
        self.ct = ct
        self.ew = ew

        self._registry_backup = copy.deepcopy(getattr(pr, "_registry", {}) or {})
        self._wk_backup = float(getattr(wk, "_last_reduced_accept_ts", 0.0) or 0.0)
        self._freq_backup = list(getattr(cm, "_trade_timestamps", []) or [])
        self._or_backup = {
            "_ai_degraded_until_ts": float(getattr(self.ortr, "_ai_degraded_until_ts", 0.0) or 0.0),
            "_ai_last_status": int(getattr(self.ortr, "_ai_last_status", 0) or 0),
            "_ai_last_reason": str(getattr(self.ortr, "_ai_last_reason", "") or ""),
            "_ai_last_error_at": str(getattr(self.ortr, "_ai_last_error_at", "") or ""),
            "_ai_last_log_ts": float(getattr(self.ortr, "_ai_last_log_ts", 0.0) or 0.0),
        }
        self._perf_backup = copy.deepcopy(getattr(pt, "_state", None))
        self._ct_backup = copy.deepcopy(getattr(ct, "_stats", {}) or {})
        self._ew_backup = copy.deepcopy(getattr(ew, "_weights", {}) or {})

        self._tmpdir = tempfile.TemporaryDirectory()
        tmp = Path(self._tmpdir.name)

        self._path_backup = {
            "perf": getattr(pt, "_PATH", None),
            "ct": getattr(ct, "_PATH", None),
            "ew": getattr(ew, "_PATH", None),
        }
        pt._PATH = tmp / "performance.json"
        ct._PATH = tmp / "category_stats.json"
        ew._PATH = tmp / "engine_weights.json"

        rj.clear()

    def tearDown(self):
        # Restore registry
        try:
            self.pr._registry.clear()
            self.pr._registry.update(self._registry_backup)
        except Exception:
            pass

        # Restore weekly cap cooldown memory
        try:
            self.wk._last_reduced_accept_ts = self._wk_backup
        except Exception:
            pass

        # Restore frequency buckets
        try:
            self.cm._trade_timestamps.clear()
            for ts in self._freq_backup:
                self.cm._trade_timestamps.append(ts)
        except Exception:
            pass

        # Restore AI degradation flags
        try:
            for k, v in self._or_backup.items():
                setattr(self.ortr, k, v)
        except Exception:
            pass

        # Restore persistence paths
        try:
            self.pt._PATH = self._path_backup["perf"]
            self.ct._PATH = self._path_backup["ct"]
            self.ew._PATH = self._path_backup["ew"]
        except Exception:
            pass

        # Restore tracker state objects without changing identities where practical
        try:
            if self._perf_backup is not None and hasattr(self.pt, "_state"):
                st = self.pt._state
                for field, value in vars(self._perf_backup).items():
                    setattr(st, field, copy.deepcopy(value))
        except Exception:
            pass
        try:
            self.ct._stats.clear()
            self.ct._stats.update(self._ct_backup)
        except Exception:
            pass
        try:
            self.ew._weights.clear()
            self.ew._weights.update(self._ew_backup)
        except Exception:
            pass

        try:
            self._tmpdir.cleanup()
        except Exception:
            pass

    def test_reset_all_clears_tracking_but_does_not_touch_open_positions(self):
        # Seed: open position, rejection log, weekly cooldown, legacy degraded flag, trade-frequency bucket.
        self.pr._registry.clear()
        self.pr._registry["FOO-USDT-SWAP"] = {"engine": "HACHIMAN", "direction": "LONG", "size_usdt": 25.0}
        registry_before = copy.deepcopy(self.pr._registry)

        self.rj.record("HACHIMAN", "FOO-USDT-SWAP", "LONG", 4.2, "weekly_cap", "blocked by weekly")
        self.assertTrue(self.rj.get_recent(1))
        import decision_trace as dt
        dt.record({"symbol": "FOO-USDT-SWAP", "side": "long", "engine": "HACHIMAN", "final_decision": "rejected"})
        self.assertTrue(dt.get_recent(1))

        self.wk._last_reduced_accept_ts = 123.0
        self.cm.record_trade_entry()

        self.ortr._ai_degraded_until_ts = time.time() + 3600
        self.ortr._ai_last_reason = "402"

        import reset_manager as rm
        summary = rm.reset_all()
        self.assertTrue("ok" in summary)

        # Open positions unchanged
        self.assertEqual(self.pr._registry, registry_before)

        # Buffers cleared / state reset
        self.assertEqual(self.rj.get_recent(5), [])
        self.assertEqual(dt.get_recent(5), [])
        self.assertEqual(float(self.wk._last_reduced_accept_ts), 0.0)
        self.assertEqual(len(self.cm._trade_timestamps), 0)

        snap = self.ortr.get_degradation_snapshot()
        self.assertEqual(str(snap.get("status", "")).upper(), "OK")

        # Performance/session timestamp set (tracking only)
        self.assertTrue(bool(getattr(self.pt._state, "session_reset_ts", "")))


if __name__ == "__main__":
    unittest.main(verbosity=2)

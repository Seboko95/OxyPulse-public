from __future__ import annotations

import copy
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def _micro_settings():
    import settings_store as ss

    ss._current.update(
        {
            "micro_sizing_enabled": True,
            "adaptive_min_order_floor_enabled": True,
            "min_order_usdt": 100.0,
            "min_order_usdt_micro": 25.0,
            "min_order_usdt_growth": 50.0,
            "min_order_usdt_pro": 100.0,
            "micro_min_order_usdt_floor": 5.0,
            "micro_margin_buffer_min_usdt": 0.5,
            "micro_margin_buffer_pct_balance": 12.0,
            "micro_margin_buffer_max_usdt": 5.0,
            "micro_target_min_pct_balance": 300.0,
            "micro_target_max_pct_balance": 900.0,
            "v6_micro_max_balance": 100.0,
            "v6_growth_max_balance": 1000.0,
            "v6_min_position_size_usdt": 25.0,
            "v6_max_position_size_usdt": 250.0,
            "v6_max_position_pct_of_balance": 0.35,
            "dynamic_target_min_usdt": 75.0,
            "dynamic_target_max_usdt": 150.0,
            "leverage_default": 25,
            "leverage_min": 25,
            "leverage_max": 25,
            "risk_floor_preserve_enabled": True,
            "risk_floor_preserve_modes": "full_deployment",
            "deployment_mode": "Full Deployment",
            "deployment_target_utilization_pct": 75.0,
            "deployment_max_positions": 11,
            "deployment_respect_exposure_cap": False,
            "deployment_respect_headroom": False,
            "deployment_total_exposure_limit_pct": 1800.0,
            "position_size_usdt": 1.0,
            "position_size_pct": 0.7,
            "risk_pct": 1.25,
            "dynamic_sizing_enabled": True,
            "v6_enabled": True,
            "v6_capital_adaptive_enabled": True,
            "capital_profile": "AUTO",
        }
    )
    try:
        import capital_manager
        import sizing_engine

        capital_manager._ss = ss
        sizing_engine._ss = ss
    except Exception:
        pass
    return ss


def test_micro_account_floor_and_margin_buffer_adapt_by_balance():
    import settings_store as ss
    import micro_sizing as ms

    backup = copy.deepcopy(ss._current)
    try:
        _micro_settings()
        assert ss.get_effective_min_order_usdt(6.0) == 5.0
        assert ss.get_effective_min_order_usdt(25.0) == 25.0
        assert ss.get_effective_min_order_usdt(50.0) == 25.0
        assert ss.get_effective_min_order_usdt(100.0) == 50.0
        assert ms.margin_buffer_usdt(6.0, ss) == 0.72
        assert ms.margin_buffer_usdt(25.0, ss) == 3.0
    finally:
        ss._current.clear()
        ss._current.update(backup)


def test_deployment_positions_are_micro_adaptive_not_forced_to_large_slots():
    import settings_store as ss
    from capital_manager import get_profile

    backup = copy.deepcopy(ss._current)
    try:
        _micro_settings()
        assert get_profile(6.0).deployment_max_positions == 1
        assert get_profile(20.0).deployment_max_positions <= 2
        assert get_profile(35.0).deployment_max_positions <= 3
        assert get_profile(50.0).deployment_max_positions == 1
        assert get_profile(6.0).deployment_per_position_target_usdt > 0.0
    finally:
        ss._current.clear()
        ss._current.update(backup)


def test_micro_blended_sizing_produces_nonzero_executable_size():
    import settings_store as ss
    from capital_manager import get_profile
    from sizing_engine import compute_size

    backup = copy.deepcopy(ss._current)
    try:
        _micro_settings()
        profile = get_profile(6.0)
        result = compute_size(
            base_size=4.2,
            confidence_score=3.5,
            atr_val=0.01,
            entry_px=1.0,
            leverage=25,
            balance=6.0,
            capital_profile=profile,
        )
        assert not result.skipped
        assert result.final_size_usdt >= 5.0
        assert result.final_size_usdt <= 54.0
        assert result.modifiers["micro_sizing_active"] is True
    finally:
        ss._current.clear()
        ss._current.update(backup)


def test_floor_aware_reductions_do_not_silently_collapse_to_zero():
    import settings_store as ss
    import micro_sizing as ms

    backup = copy.deepcopy(ss._current)
    try:
        _micro_settings()
        preserved = ms.floor_aware_reduce(
            18.0,
            0.2,
            balance_usdt=6.0,
            leverage=25,
            settings=ss,
            reason="net_expectancy",
        )
        assert not preserved.skipped
        assert preserved.final_size_usdt == 5.0
        assert preserved.floor_preserved is True

        blocked = ms.floor_aware_reduce(
            4.0,
            0.2,
            balance_usdt=1.0,
            leverage=25,
            settings=ss,
            reason="net_expectancy",
        )
        assert blocked.skipped
        assert "below_floor" in blocked.reason
    finally:
        ss._current.clear()
        ss._current.update(backup)


def test_net_expectancy_thresholds_are_account_aware():
    import settings_store as ss
    import micro_sizing as ms

    backup = copy.deepcopy(ss._current)
    try:
        _micro_settings()
        ss._current["net_expectancy_min_cost_mult"] = 2.0
        ss._current["net_expectancy_weak_cost_mult"] = 3.0
        tiny = ms.net_expectancy_cost_multipliers(6.0, ss)
        growth = ms.net_expectancy_cost_multipliers(100.0, ss)
        assert tiny[0] < growth[0]
        assert tiny[1] < growth[1]
        assert tiny[0] > 0.0
    finally:
        ss._current.clear()
        ss._current.update(backup)


def test_telegram_micro_sizing_panel_renders_runtime_fields():
    import settings_store as ss
    import tg_panels

    backup = copy.deepcopy(ss._current)
    try:
        _micro_settings()
        text = tg_panels.panel_micro_sizing_text(ss, balance=6.0)
        assert "Effective Account Floor" in text
        assert "Exchange/Symbol Floor" in text
        assert "Final Executable Size" in text
        assert "Micro sizing mode" in text
    finally:
        ss._current.clear()
        ss._current.update(backup)

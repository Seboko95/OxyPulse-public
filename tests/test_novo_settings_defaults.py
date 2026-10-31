from __future__ import annotations

import ast
from pathlib import Path

import settings_store as ss


NOVO_DEFAULTS = {
    "novo_intelligence_enabled": False,
    "novo_market_dna_enabled": True,
    "novo_market_structure_enabled": True,
    "novo_4h_bias_enabled": True,
    "novo_crt_enabled": True,
    "novo_liquidity_sweep_enabled": True,
    "novo_session_model_enabled": True,
    "novo_trade_quality_enabled": True,
    "novo_ai_trade_memory_enabled": False,
    "novo_gem_hunter_enabled": False,
    "novo_soft_confidence_enabled": False,
    "novo_exit_enrichment_enabled": False,
    "novo_profit_lock_enrichment_enabled": False,
    "novo_fee_bleed_guard_enabled": False,
    "novo_elite_size_boost_enabled": False,
    "novo_enrichment_outcome_telemetry_enabled": False,
    "novo_trade_quality_advisory_only": True,
    "novo_no_hard_blocks": True,
    "gem_hunter_alert_only": True,
    "positive_mfe_profit_guard_enabled": False,
}

POSITIVE_MFE_GUARD_DEFAULTS = {
    "positive_mfe_profit_guard_enabled": False,
    "positive_mfe_guard_mfe_roi_trigger": 3.0,
    "positive_mfe_guard_min_profit_usdt": 0.10,
    "positive_mfe_guard_fee_mult": 2.0,
    "positive_mfe_guard_lock_roi_pct": 0.35,
    "positive_mfe_guard_min_age_sec": 30.0,
    "positive_mfe_guard_max_lock_levels": 1,
    "positive_profit_lock_enrichment_enabled": False,
    "profit_lock_enrichment_mfe_trigger_roi": 25.0,
    "profit_lock_enrichment_lock_pct_1": 0.20,
    "profit_lock_enrichment_lock_pct_2": 0.35,
    "profit_lock_enrichment_lock_pct_3": 0.50,
    "profit_lock_enrichment_max_level": 3,
}

FEE_BLEED_GUARD_DEFAULTS = {
    "fee_bleed_guard_min_net_profit_usdt": 0.10,
    "fee_bleed_guard_max_fee_share": 0.35,
    "fee_bleed_guard_min_partial_notional_usdt": 10.0,
    "fee_bleed_guard_trace_all_small_exits": True,
}

VOTE_RESCUE_DEFAULTS = {
    "vote_rescue_enabled": False,
    "vote_rescue_max_vote_gap": 1.0,
    "vote_rescue_min_quality": 62.0,
    "vote_rescue_min_admission_confidence": 2.0,
    "vote_rescue_require_support": True,
    "vote_rescue_novo_min_tier": "GOOD",
    "vote_rescue_live_opener_roles": "hunter,specialist,confirmer,booster",
}


def test_novo_defaults_exist():
    for key, expected in NOVO_DEFAULTS.items():
        assert key in ss.DEFAULTS
        assert ss.DEFAULTS[key] is expected


def test_master_novo_intelligence_is_false_by_default():
    assert ss.DEFAULTS["novo_intelligence_enabled"] is False


def test_advisory_only_flags_are_true():
    assert ss.DEFAULTS["novo_trade_quality_advisory_only"] is True
    assert ss.DEFAULTS["novo_no_hard_blocks"] is True


def test_gem_hunter_disabled_by_default_and_alert_only():
    assert ss.DEFAULTS["novo_gem_hunter_enabled"] is False
    assert ss.DEFAULTS["gem_hunter_alert_only"] is True


def test_memory_disabled_by_default():
    assert ss.DEFAULTS["novo_ai_trade_memory_enabled"] is False
    assert ss.DEFAULTS["novo_enrichment_outcome_telemetry_enabled"] is False


def test_positive_mfe_guard_defaults_exist_and_are_safe():
    for key, expected in POSITIVE_MFE_GUARD_DEFAULTS.items():
        assert key in ss.DEFAULTS
        assert ss.DEFAULTS[key] == expected


def test_fee_bleed_guard_coverage_defaults_exist_and_are_safe():
    for key, expected in FEE_BLEED_GUARD_DEFAULTS.items():
        assert key in ss.DEFAULTS
        assert ss.DEFAULTS[key] == expected


def test_vote_rescue_defaults_exist_and_are_default_off():
    for key, expected in VOTE_RESCUE_DEFAULTS.items():
        assert key in ss.DEFAULTS
        assert ss.DEFAULTS[key] == expected


def test_existing_settings_defaults_still_load():
    assert isinstance(ss.DEFAULTS, dict)
    assert ss.DEFAULTS["risk_pct"] == 1.0
    assert "v6_enabled" in ss.DEFAULTS
    assert ss.DEFAULTS["novo_intelligence_enabled"] is False


def test_no_live_settings_json_mutation_required():
    settings_path = Path("data/settings.json")
    before = settings_path.read_bytes() if settings_path.exists() else None

    for key in NOVO_DEFAULTS:
        assert key in ss.DEFAULTS

    after = settings_path.read_bytes() if settings_path.exists() else None
    assert after == before


def test_forbidden_execution_imports_not_introduced_into_settings_store():
    source_path = Path(__file__).resolve().parents[1] / "settings_store.py"
    tree = ast.parse(source_path.read_text(encoding="utf-8-sig"))
    imported = set()

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            if node.module:
                imported.add(node.module)

    forbidden = {
        "execution_router",
        "execution_admission",
        "okx_client",
        "risk_manager",
        "sizing_engine",
        "position_registry",
        "exit_manager",
    }
    assert imported.isdisjoint(forbidden)

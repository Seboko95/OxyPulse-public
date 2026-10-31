from __future__ import annotations

import ast
from pathlib import Path

import decision_trace as dt
import tg_panels as tp


class SettingsStub:
    def __init__(self, values=None):
        self.values = dict(values or {})

    def get_val(self, key, default=None):
        return self.values.get(key, default)


def test_each_novo_render_helper_returns_safe_text():
    settings = SettingsStub()
    renderers = [
        tp.render_novo_quality_panel,
        tp.render_novo_liquidity_panel,
        tp.render_novo_crt_panel,
        tp.render_novo_bias_panel,
        tp.render_novo_learning_panel,
        tp.render_novo_gems_panel,
        tp.render_silent_signal_diagnostics_panel,
    ]

    for renderer in renderers:
        text = renderer(settings)
        assert isinstance(text, str)
        assert text
        assert len(text) < 4096


def test_disabled_master_state_is_shown():
    text = tp.render_novo_quality_panel(SettingsStub({"novo_intelligence_enabled": False}))

    assert "Novo Intelligence: `DISABLED`" in text
    assert "Status: `DISABLED`" in text


def test_collecting_state_is_shown_when_enabled_but_no_runtime_data():
    dt.clear()
    settings = SettingsStub({"novo_intelligence_enabled": True})

    assert "COLLECTING / NO NOVO TRACE YET" in tp.render_novo_quality_panel(settings)
    assert "COLLECTING / NO NOVO TRACE YET" in tp.render_novo_liquidity_panel(settings)
    assert "COLLECTING / NO NOVO TRACE YET" in tp.render_novo_crt_panel(settings)
    assert "COLLECTING / NO NOVO TRACE YET" in tp.render_novo_bias_panel(settings)


def _enrichment():
    return {
        "status": "OK",
        "band": "ELITE",
        "advisory_only": True,
        "no_hard_blocks": True,
        "enabled_flags": {
            "exit_enrichment": True,
            "profit_lock_enrichment": True,
            "fee_bleed_guard": True,
            "elite_size_boost": True,
        },
        "suggestions": {
            "tp2_multiplier": 1.12,
            "trailing_activation_multiplier": 1.10,
            "break_even_delay_multiplier": 1.10,
            "profit_lock_multiplier": 1.08,
            "partial_min_notional_multiplier": 1.20,
            "execution_split_preference": "LIMIT_PREFERRED",
            "size_multiplier": 1.12,
        },
    }


def _record_novo_trace(*, fallback=False, enrichment=False, corrupt_enrichment=False):
    dt.clear()
    novo = {
        "enabled": True,
        "advisory_only": True,
        "quality_score": 84,
        "quality_tier": "STRONG",
        "modules": {
            "market_dna": "OK",
            "market_structure": "OK",
            "bias_4h": "OK",
            "crt": "INSUFFICIENT_DATA",
            "liquidity_sweep": "DISABLED",
            "session_model": "ERROR",
        },
    }
    enrichment_value = "bad" if corrupt_enrichment else _enrichment()
    if fallback:
        trace_payload = {"novo": novo}
        if enrichment:
            trace_payload["novo_enrichment"] = enrichment_value
        extra = {"confidence_stage_trace": trace_payload}
    else:
        extra = {"novo": novo}
        if enrichment:
            extra["novo_enrichment"] = enrichment_value
    dt.record({
        "symbol": "SUI",
        "side": "LONG",
        "engine": "ENMA",
        "final_decision": "novo_annotation",
        "extra": extra,
    })


def test_trace_with_extra_novo_renders_quality_score_and_tier():
    _record_novo_trace()
    text = tp.render_novo_quality_panel(SettingsStub({"novo_intelligence_enabled": True}))

    assert "Latest: `SUI LONG [ENMA]`" in text
    assert "Quality Score: `84`" in text
    assert "Quality Tier: `STRONG`" in text


def test_quality_shows_no_enrichment_trace_fallback():
    _record_novo_trace()
    text = tp.render_novo_quality_panel(SettingsStub({"novo_intelligence_enabled": True}))

    assert "Dry-Run Enrichment: `COLLECTING / NO ENRICHMENT TRACE YET`" in text


def test_quality_renders_enrichment_status_band_and_suggestions():
    _record_novo_trace(enrichment=True)
    text = tp.render_novo_quality_panel(SettingsStub({"novo_intelligence_enabled": True}))

    assert "Dry-Run Enrichment: `OK`" in text
    assert "Band: `ELITE`" in text
    assert "TP2 Multiplier: `x1.12`" in text
    assert "Trailing Activation: `x1.10`" in text
    assert "BE Delay: `x1.10`" in text
    assert "Profit Lock: `x1.08`" in text
    assert "Partial Min Notional: `x1.20`" in text
    assert "Execution Split: `LIMIT_PREFERRED`" in text
    assert "Size Multiplier: `x1.12`" in text


def test_quality_renders_nested_enrichment_fallback_path():
    _record_novo_trace(fallback=True, enrichment=True)
    text = tp.render_novo_quality_panel(SettingsStub({"novo_intelligence_enabled": True}))

    assert "Dry-Run Enrichment: `OK`" in text
    assert "Band: `ELITE`" in text
    assert "Execution Split: `LIMIT_PREFERRED`" in text


def test_bad_or_corrupt_enrichment_trace_does_not_crash():
    _record_novo_trace(enrichment=True, corrupt_enrichment=True)
    text = tp.render_novo_quality_panel(SettingsStub({"novo_intelligence_enabled": True}))

    assert "Quality Score: `84`" in text
    assert "Dry-Run Enrichment: `COLLECTING / NO ENRICHMENT TRACE YET`" in text


def test_liquidity_panel_renders_latest_liquidity_status():
    _record_novo_trace()
    text = tp.render_novo_liquidity_panel(SettingsStub({"novo_intelligence_enabled": True}))

    assert "Latest Liquidity Sweep: `DISABLED`" in text


def test_crt_panel_renders_latest_crt_status():
    _record_novo_trace()
    text = tp.render_novo_crt_panel(SettingsStub({"novo_intelligence_enabled": True}))

    assert "Latest CRT: `INSUFFICIENT_DATA`" in text


def test_bias_panel_renders_latest_bias_status():
    _record_novo_trace()
    text = tp.render_novo_bias_panel(SettingsStub({"novo_intelligence_enabled": True}))

    assert "Latest 4H Bias: `OK`" in text


def test_other_novo_panels_do_not_render_enrichment_lines():
    _record_novo_trace(enrichment=True)
    settings = SettingsStub({"novo_intelligence_enabled": True})

    for renderer in (tp.render_novo_liquidity_panel, tp.render_novo_crt_panel, tp.render_novo_bias_panel):
        text = renderer(settings)
        assert "Dry-Run Enrichment" not in text
        assert "TP2 Multiplier" not in text


def test_fallback_confidence_stage_trace_novo_works():
    _record_novo_trace(fallback=True)
    text = tp.render_novo_quality_panel(SettingsStub({"novo_intelligence_enabled": True}))

    assert "Quality Score: `84`" in text
    assert "Quality Tier: `STRONG`" in text


def test_bad_or_corrupt_trace_does_not_crash():
    dt.clear()
    dt.record({
        "symbol": "BAD",
        "side": "LONG",
        "engine": "ENMA",
        "final_decision": "novo_annotation",
        "extra": "not-a-dict",
    })

    assert "COLLECTING / NO NOVO TRACE YET" in tp.render_novo_quality_panel(SettingsStub({"novo_intelligence_enabled": True}))


def test_learning_handles_missing_memory_file_without_creating_it(monkeypatch, tmp_path):
    import novo_trade_memory as memory

    missing = tmp_path / "missing" / "novo_trade_memory.jsonl"
    monkeypatch.setattr(memory, "DEFAULT_PATH", missing)
    settings = SettingsStub({
        "novo_intelligence_enabled": True,
        "novo_ai_trade_memory_enabled": True,
    })

    text = tp.render_novo_learning_panel(settings)

    assert "COLLECTING / NO MEMORY YET" in text
    assert "Records: `0`" in text
    assert "Enrichment Outcome Telemetry: `DISABLED`" in text
    assert "Enrichment Outcomes: `DISABLED`" in text
    assert not missing.exists()


def test_learning_enrichment_telemetry_disabled_line_appears_by_default(monkeypatch, tmp_path):
    import novo_trade_memory as memory

    missing = tmp_path / "missing" / "novo_trade_memory.jsonl"
    monkeypatch.setattr(memory, "DEFAULT_PATH", missing)

    text = tp.render_novo_learning_panel(SettingsStub())

    assert "Enrichment Outcome Telemetry: `DISABLED`" in text
    assert "Enrichment Outcomes: `DISABLED`" in text
    assert not missing.exists()


def test_learning_enrichment_enabled_missing_memory_collects_safely(monkeypatch, tmp_path):
    import novo_trade_memory as memory

    missing = tmp_path / "missing" / "novo_trade_memory.jsonl"
    monkeypatch.setattr(memory, "DEFAULT_PATH", missing)
    settings = SettingsStub({"novo_enrichment_outcome_telemetry_enabled": True})

    text = tp.render_novo_learning_panel(settings)

    assert "Enrichment Outcome Telemetry: `ON`" in text
    assert "Enrichment Outcomes: `COLLECTING / NO OUTCOME DATA YET`" in text
    assert not missing.exists()


def test_learning_enrichment_summary_with_records(monkeypatch, tmp_path):
    import novo_trade_memory as memory

    path = tmp_path / "novo_trade_memory.jsonl"
    monkeypatch.setattr(memory, "DEFAULT_PATH", path)
    memory.append_memory({
        "record_type": "novo_enrichment_outcome",
        "symbol": "SUI",
        "engine": "ENMA",
        "quality_score": 91,
        "quality_tier": "ELITE",
        "outcome": "WIN",
        "pnl": 0.50,
    })
    memory.append_memory({
        "record_type": "novo_enrichment_outcome",
        "symbol": "APE",
        "engine": "ARASHI",
        "quality_score": 62,
        "quality_tier": "WEAK",
        "outcome": "LOSS",
        "pnl": -0.20,
    })
    settings = SettingsStub({
        "novo_ai_trade_memory_enabled": True,
        "novo_enrichment_outcome_telemetry_enabled": True,
    })

    text = tp.render_novo_learning_panel(settings)

    assert "Enrichment Outcomes: `OK`" in text
    assert "Outcome Records: `2`" in text
    assert "Outcomes: `2`" in text
    assert "Wins/Losses: `1 / 1`" in text
    assert "Total PnL: `+0.3000`" in text
    assert "Avg Quality: `76.5`" in text
    assert "Tier Counts: `ELITE:1, WEAK:1`" in text
    assert "Best Symbols: `SUI:+0.5000" in text
    assert "Worst Symbols: `APE:-0.2000" in text
    assert "Best Engines: `ENMA:+0.5000" in text
    assert "Worst Engines: `ARASHI:-0.2000" in text


def test_learning_corrupt_memory_does_not_crash(monkeypatch, tmp_path):
    import novo_trade_memory as memory

    path = tmp_path / "novo_trade_memory.jsonl"
    path.write_text("{bad json\n", encoding="utf-8")
    monkeypatch.setattr(memory, "DEFAULT_PATH", path)
    settings = SettingsStub({"novo_enrichment_outcome_telemetry_enabled": True})

    text = tp.render_novo_learning_panel(settings)

    assert "Enrichment Outcomes: `COLLECTING / NO OUTCOME DATA YET`" in text
    assert len(text) < 4096


def test_gems_panel_renders_safely():
    text = tp.render_novo_gems_panel(SettingsStub())

    assert isinstance(text, str)
    assert "*Novo Gem Hunter*" in text
    assert len(text) < 4096


def test_gems_disabled_state_is_shown():
    text = tp.render_novo_gems_panel(SettingsStub({"novo_gem_hunter_enabled": False}))

    assert "Gem Hunter: `DISABLED`" in text


def test_gems_alert_only_state_and_collecting_are_shown():
    text = tp.render_novo_gems_panel(SettingsStub({
        "novo_gem_hunter_enabled": True,
        "gem_hunter_alert_only": True,
    }))

    assert "Gem Hunter: `ENABLED`" in text
    assert "Alert Only: `ON`" in text
    assert "COLLECTING / NO GEM SCAN DATA YET" in text


def test_gems_alert_only_off_warns_but_remains_display_only():
    text = tp.render_novo_gems_panel(SettingsStub({
        "novo_gem_hunter_enabled": True,
        "gem_hunter_alert_only": False,
    }))

    assert "Alert Only: `OFF`" in text
    assert "Warning: alert-only setting is OFF" in text
    assert "display-only" in text


def test_all_panel_outputs_are_telegram_safe_length(monkeypatch, tmp_path):
    _record_novo_trace()
    import novo_trade_memory as memory

    missing = tmp_path / "missing" / "novo_trade_memory.jsonl"
    monkeypatch.setattr(memory, "DEFAULT_PATH", missing)
    settings = SettingsStub({
        "novo_intelligence_enabled": True,
        "novo_ai_trade_memory_enabled": True,
    })
    for renderer in (
        tp.render_novo_quality_panel,
        tp.render_novo_liquidity_panel,
        tp.render_novo_crt_panel,
        tp.render_novo_bias_panel,
        tp.render_novo_learning_panel,
        tp.render_novo_gems_panel,
        tp.render_silent_signal_diagnostics_panel,
    ):
        assert len(renderer(settings)) < 4096


def test_signaldiag_panel_renders_with_runtime_counters(monkeypatch):
    import category_engine as ce

    def fake_diag():
        return {
            "started_at": 1780000000.0,
            "total": 11,
            "reasons": {
                "raw_hold_return": 6,
                "near_vote_floor": 2,
                "selector_contribution_only": 1,
                "shadow_contribution_only": 1,
                "unknown_exception": 1,
            },
            "by_engine": {
                "RAIJIN": {
                    "engine": "RAIJIN",
                    "role": "hunter",
                    "total": 6,
                    "reasons": {"raw_hold_return": 6},
                },
                "YUREI": {
                    "engine": "YUREI",
                    "role": "selector",
                    "total": 1,
                    "reasons": {"selector_contribution_only": 1},
                },
            },
            "recent": [],
        }

    monkeypatch.setattr(ce, "get_silent_signal_diagnostics", fake_diag)
    text = tp.render_silent_signal_diagnostics_panel(SettingsStub())

    assert "*Signal Diagnostics*" in text
    assert "Total Silent Events: `11`" in text
    assert "Near-Floor Count: `2`" in text
    assert "Vote Rescue Eligible: `2`" in text
    assert "Selector-Only Count: `1`" in text
    assert "Shadow-Only Count: `1`" in text
    assert "Unknown Exceptions: `1`" in text
    assert "`raw_hold_return`: `6`" in text
    assert "`RAIJIN` `hunter` total `6`" in text
    assert len(text) < 4096


def test_signaldiag_panel_renders_when_counters_unavailable(monkeypatch):
    import category_engine as ce

    def broken_diag():
        raise RuntimeError("not loaded")

    monkeypatch.setattr(ce, "get_silent_signal_diagnostics", broken_diag)
    text = tp.render_silent_signal_diagnostics_panel(SettingsStub())

    assert "Status: `UNAVAILABLE`" in text
    assert "No counters are reset" in text
    assert len(text) < 4096


def test_signaldiag_panel_renders_spread_liquidity_details(monkeypatch):
    import category_engine as ce

    def fake_diag():
        return {
            "started_at": 1780000000.0,
            "total": 4,
            "reasons": {"spread_liquidity_block": 4},
            "by_engine": {},
            "recent": [],
        }

    def fake_spread_diag(limit=50):
        return {
            "status": "OK",
            "total_recent": 2,
            "records": [
                {
                    "symbol": "APT",
                    "engine": "RYUJIN",
                    "side": "SHORT",
                    "sub_reason": "spread_too_wide",
                    "spread_pct": 0.41,
                    "spread_threshold": 0.3,
                    "volume_24h": 5000000,
                    "volume_threshold": 2000000,
                },
                {
                    "symbol": "SUI",
                    "engine": "RAIJIN",
                    "side": "LONG",
                    "sub_reason": "volume_too_low",
                    "spread_pct": 0.12,
                    "spread_threshold": 0.3,
                    "volume_24h": 100000,
                    "volume_threshold": 2000000,
                },
            ],
        }

    monkeypatch.setattr(ce, "get_silent_signal_diagnostics", fake_diag)
    monkeypatch.setattr(ce, "get_spread_liquidity_block_diagnostics", fake_spread_diag)
    text = tp.render_silent_signal_diagnostics_panel(SettingsStub())

    assert "*Spread/Liquidity Blocks*" in text
    assert "Recent Captured: `2`" in text
    assert "`APT`" in text
    assert "`spread_too_wide`" in text
    assert "`volume_too_low`" in text
    assert len(text) < 4096


def test_signaldiag_panel_does_not_reset_counters(monkeypatch):
    import category_engine as ce

    def fake_diag():
        return {
            "started_at": 1780000000.0,
            "total": 1,
            "reasons": {"raw_hold_return": 1},
            "by_engine": {},
            "recent": [],
        }

    def fail_reset():
        raise AssertionError("signal diagnostics panel must not reset counters")

    monkeypatch.setattr(ce, "get_silent_signal_diagnostics", fake_diag)
    monkeypatch.setattr(ce, "reset_silent_signal_diagnostics", fail_reset)

    text = tp.render_silent_signal_diagnostics_panel(SettingsStub())

    assert "Total Silent Events: `1`" in text
    assert len(text) < 4096


def test_signaldiag_command_and_diagnostics_button_exist():
    bot_source = Path("bot.py").read_text(encoding="utf-8")
    panel_source = Path("tg_panels.py").read_text(encoding="utf-8-sig")

    assert '("signaldiag",cmd_signaldiag)' in bot_source
    assert "async def cmd_signaldiag" in bot_source
    assert "diag:signaldiag" in bot_source
    assert "📊 Signal Diagnostics" in panel_source
    assert 'callback_data="diag:signaldiag"' in panel_source


def test_signaldiag_panel_helper_has_no_forbidden_behavior():
    source = Path("tg_panels.py").read_text(encoding="utf-8-sig")
    start = source.index("def render_silent_signal_diagnostics_panel")
    next_start = source.find("\ndef ", start + 1)
    block = source[start: next_start if next_start != -1 else len(source)]
    forbidden = {
        "execution_router",
        "execution_admission",
        "okx_client",
        "risk_manager",
        "sizing_engine",
        "position_registry",
        "exit_manager",
        "place_market_order",
        "close_coin",
        "close_all_positions",
        "set_val",
        "append_memory",
        "reset_silent_signal_diagnostics",
    }

    assert all(token not in block for token in forbidden)


def test_novo_commands_registered_including_gems():
    source = Path("bot.py").read_text(encoding="utf-8")

    for name in ("quality", "liquidity", "crt", "bias", "learning", "gems"):
        assert f'("{name}",cmd_{name})' in source
    assert "async def cmd_gems" in source


def test_gems_panel_does_not_call_analyzer():
    source = Path("tg_panels.py").read_text(encoding="utf-8-sig")
    start = source.index("def render_novo_gems_panel")
    next_start = source.find("\ndef ", start + 1)
    block = source[start: next_start if next_start != -1 else len(source)]

    assert "analyze_gem_candidate" not in block
    assert "import gem_hunter" not in block


def test_new_command_functions_do_not_reference_forbidden_modules():
    source = Path("bot.py").read_text(encoding="utf-8")
    forbidden = {
        "execution_router",
        "execution_admission",
        "okx_client",
        "risk_manager",
        "sizing_engine",
        "position_registry",
        "exit_manager",
        "place_market_order",
        "close_coin",
        "close_all_positions",
    }
    for fn_name in ("cmd_quality", "cmd_liquidity", "cmd_crt", "cmd_bias", "cmd_learning", "cmd_gems", "cmd_signaldiag"):
        start = source.index(f"async def {fn_name}")
        next_start = source.find("\nasync def ", start + 1)
        block = source[start: next_start if next_start != -1 else len(source)]
        assert all(token not in block for token in forbidden)


def test_existing_bot_and_panel_sources_parse():
    for filename in ("bot.py", "tg_panels.py"):
        ast.parse(Path(filename).read_text(encoding="utf-8-sig"))


def test_novo_panel_helpers_do_not_reference_forbidden_execution_paths():
    source = Path("tg_panels.py").read_text(encoding="utf-8-sig")
    forbidden = {
        "execution_router",
        "execution_admission",
        "okx_client",
        "risk_manager",
        "sizing_engine",
        "position_registry",
        "exit_manager",
        "place_market_order",
        "close_coin",
        "close_all_positions",
        "novo_intelligence.analyze_candidate",
        "novo_soft_enrichment.build_novo_enrichment",
        "append_memory",
    }
    function_names = (
        "_latest_novo_trace",
        "_latest_novo_enrichment_trace",
        "_novo_trace_header",
        "_novo_module_status",
        "_novo_module_summary",
        "_novo_enrichment_lines",
        "render_novo_quality_panel",
        "_render_novo_module_panel",
        "render_novo_liquidity_panel",
        "render_novo_crt_panel",
        "render_novo_bias_panel",
        "render_novo_gems_panel",
    )
    for fn_name in function_names:
        start = source.index(f"def {fn_name}")
        next_start = source.find("\ndef ", start + 1)
        block = source[start: next_start if next_start != -1 else len(source)]
        assert all(token not in block for token in forbidden)

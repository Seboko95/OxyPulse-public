from __future__ import annotations

from pathlib import Path

import category_engine as ce


def _source() -> str:
    return Path("category_engine.py").read_text(encoding="utf-8")


def _novo_hook_block() -> str:
    source = _source()
    start = source.index('if bool(_s.get_val("novo_intelligence_enabled", False)):')
    end = source.index("if _consensus_conf_lift > 0:", start)
    return source[start:end]


def test_disabled_by_default_guard_prevents_novo_call():
    block = _novo_hook_block()

    assert 'if bool(_s.get_val("novo_intelligence_enabled", False)):' in block
    assert "_novo_intelligence.analyze_candidate" in block


def test_enabled_metadata_is_stored_in_quality_snapshot_and_trace_extra():
    block = _novo_hook_block()

    assert '_quality_snapshot["novo"] = _novo_result' in block
    assert '_conf_trace["novo"] = dict(_novo_compact)' in block
    assert '"novo": dict(_novo_compact)' in block
    assert 'decision="novo_annotation"' in block


def test_enrichment_metadata_is_dry_run_only_after_novo_metadata_exists():
    block = _novo_hook_block()

    novo_store = block.index('_quality_snapshot["novo"] = _novo_result')
    enrich_call = block.index("_novo_soft_enrichment.build_novo_enrichment")
    assert novo_store < enrich_call
    assert '_quality_snapshot["novo_enrichment"] = _novo_enrichment' in block
    assert '_conf_trace["novo_enrichment"] = dict(_novo_enrichment_compact)' in block
    assert '"novo_enrichment": dict(_novo_enrichment_compact)' in block


def test_enrichment_failure_isolated_from_candidate_flow():
    block = _novo_hook_block()

    assert "except Exception as _novo_enrich_exc:" in block
    assert "Novo enrichment skipped" in block
    enrich_except_start = block.index("except Exception as _novo_enrich_exc:")
    enrich_except_end = block.index("_record_decision_trace", enrich_except_start)
    enrich_except_block = block[enrich_except_start:enrich_except_end]
    assert "return" not in enrich_except_block
    assert "raise" not in enrich_except_block


def test_failure_isolation_hook_does_not_return_or_raise():
    block = _novo_hook_block()

    assert "except Exception as _novo_exc:" in block
    assert "logger.debug" in block
    assert "return" not in block
    assert "raise" not in block


def test_no_confidence_admission_or_quality_mutation_in_novo_hook():
    block = _novo_hook_block()

    forbidden_assignments = (
        "_signal_conf =",
        "_admission_conf =",
        "_candidate_quality =",
        "votes =",
        "_v6_conf =",
    )
    for assignment in forbidden_assignments:
        assert assignment not in block


def test_no_admission_execution_rejection_or_sizing_change_in_novo_hook():
    block = _novo_hook_block()

    forbidden_tokens = (
        "_rej(",
        "_place_trade",
        "can_open_and_reserve",
        "_preview_trade_execution",
        "_reserved_size_usdt",
        "size_usdt =",
        "risk_ceiling",
        "send_alert",
        "place_market_order",
        "_sl_tp.compute",
        "apply_profit_targeting",
    )
    for token in forbidden_tokens:
        assert token not in block


def test_weak_novo_score_cannot_block():
    compact = ce._novo_compact_snapshot({
        "enabled": True,
        "advisory_only": True,
        "quality_score": 1,
        "quality_tier": "WEAK",
        "modules": {
            "market_dna": {"status": "OK"},
            "market_structure": {"status": "OK"},
            "bias_4h": {"status": "OK"},
            "crt": {"status": "OK"},
            "liquidity_sweep": {"status": "OK"},
            "session_model": {"status": "OK"},
        },
    })

    assert compact["quality_score"] == 1
    assert compact["quality_tier"] == "WEAK"
    assert compact["advisory_only"] is True


def test_candle_builder_does_not_mutate_inputs_and_uses_previous_close_as_open():
    closes = [1.0, 2.0, 3.0]
    highs = [1.2, 2.2, 3.2]
    lows = [0.8, 1.8, 2.8]
    volumes = [10.0, 20.0, 30.0]
    before = (list(closes), list(highs), list(lows), list(volumes))

    candles = ce._novo_candles_from_series(closes, highs, lows, volumes)

    assert (closes, highs, lows, volumes) == before
    assert candles[0]["open"] == 1.0
    assert candles[1]["open"] == 1.0
    assert candles[2]["open"] == 2.0
    assert candles[2]["volume"] == 30.0


def test_full_novo_metadata_is_stored_under_quality_snapshot_and_memory_is_compact():
    block = _novo_hook_block()

    assert '_quality_snapshot["novo"] = _novo_result' in block
    assert "_maybe_append_novo_candidate_observation" in block
    assert "_novo_trade_memory.append_memory(record)" in _source()


def test_candidate_observation_record_is_advisory_dry_run_and_compact():
    record = ce._novo_candidate_observation_record(
        coin="sui-usdt-swap",
        direction="short",
        engine="ENMA",
        signal_confidence=0.61,
        admission_confidence=0.67,
        candidate_quality=82,
        novo_result={
            "quality_score": 91,
            "quality_tier": "ELITE",
            "modules": {
                "market_dna": {"status": "OK", "score": 88, "bias": "SHORT"},
                "crt": {"status": "INSUFFICIENT_DATA"},
            },
        },
        novo_compact={"modules": {"market_dna": "OK", "crt": "INSUFFICIENT_DATA"}},
        novo_enrichment_compact={"status": "DISABLED", "band": "ELITE"},
    )

    assert record["record_type"] == "novo_candidate_observation"
    assert record["dry_run"] is True
    assert record["advisory_only"] is True
    assert record["no_hard_blocks"] is True
    assert record["symbol"] == "SUI-USDT-SWAP"
    assert record["side"] == "SHORT"
    assert record["quality_score"] == 91
    assert record["quality_tier"] == "ELITE"
    assert record["admission_result"] == "PENDING"
    assert record["rejection_reason"] is None
    assert record["novo_modules"]["market_dna"] == {"status": "OK", "score": 88, "bias": "SHORT"}
    assert record["novo_enrichment"] == {"status": "DISABLED", "band": "ELITE"}


def test_candidate_memory_not_appended_when_memory_flag_disabled(monkeypatch):
    monkeypatch.setattr(ce._s, "get_val", lambda key, default=None: False if key == "novo_ai_trade_memory_enabled" else default)
    calls = []
    monkeypatch.setattr(ce._novo_trade_memory, "append_memory", lambda record: calls.append(record))

    result = ce._maybe_append_novo_candidate_observation(
        coin="SUI-USDT-SWAP",
        direction="SHORT",
        engine="ENMA",
        signal_confidence=0.5,
        admission_confidence=0.6,
        candidate_quality=70,
        novo_result={"quality_score": 80, "quality_tier": "STRONG"},
        novo_compact={},
        novo_enrichment_compact={},
    )

    assert result is False
    assert calls == []


def test_candidate_memory_appends_when_enabled(monkeypatch):
    monkeypatch.setattr(ce._s, "get_val", lambda key, default=None: True if key == "novo_ai_trade_memory_enabled" else default)
    calls = []
    monkeypatch.setattr(ce._novo_trade_memory, "append_memory", lambda record: calls.append(record) or {"status": "OK"})

    result = ce._maybe_append_novo_candidate_observation(
        coin="SUI-USDT-SWAP",
        direction="SHORT",
        engine="ENMA",
        signal_confidence=0.5,
        admission_confidence=0.6,
        candidate_quality=70,
        novo_result={"quality_score": 80, "quality_tier": "STRONG"},
        novo_compact={},
        novo_enrichment_compact={"status": "DISABLED"},
    )

    assert result is True
    assert len(calls) == 1
    assert calls[0]["record_type"] == "novo_candidate_observation"
    assert calls[0]["dry_run"] is True
    assert calls[0]["advisory_only"] is True
    assert calls[0]["quality_score"] == 80


def test_candidate_memory_append_failure_does_not_raise(monkeypatch):
    monkeypatch.setattr(ce._s, "get_val", lambda key, default=None: True if key == "novo_ai_trade_memory_enabled" else default)

    def boom(record):
        raise OSError("disk unavailable")

    monkeypatch.setattr(ce._novo_trade_memory, "append_memory", boom)

    result = ce._maybe_append_novo_candidate_observation(
        coin="SUI-USDT-SWAP",
        direction="SHORT",
        engine="ENMA",
        signal_confidence=0.5,
        admission_confidence=0.6,
        candidate_quality=70,
        novo_result={"quality_score": 80, "quality_tier": "STRONG"},
        novo_compact={},
        novo_enrichment_compact={},
    )

    assert result is False


def test_candidate_memory_feature_does_not_reference_protected_runtime_files():
    source = _source()
    start = source.index("def _novo_candidate_observation_record(")
    end = source.index("def _apply_soft_accept_size", start)
    feature_source = source[start:end]

    for token in (
        "data/performance.json",
        "data/category_stats.json",
        "data/position_registry.json",
        "data/settings.json",
        "record_trade(",
        "can_open_and_reserve",
        "place_market_order",
        "send_alert",
    ):
        assert token not in feature_source


def test_novo_enrichment_compact_summary_shape_is_safe_and_neutral_capable():
    compact = ce._novo_enrichment_compact_snapshot({
        "status": "OK",
        "band": "WEAK",
        "advisory_only": True,
        "no_hard_blocks": True,
        "enabled_flags": {"fee_bleed_guard": True},
        "suggestions": {
            "tp2_multiplier": 1.0,
            "trailing_activation_multiplier": 1.0,
            "break_even_delay_multiplier": 1.0,
            "profit_lock_multiplier": 1.0,
            "partial_min_notional_multiplier": 1.05,
            "execution_split_preference": "UNCHANGED",
            "size_multiplier": 1.0,
        },
    })

    assert compact == {
        "status": "OK",
        "band": "WEAK",
        "advisory_only": True,
        "no_hard_blocks": True,
        "enabled_flags": {"fee_bleed_guard": True},
        "suggestions": {
            "tp2_multiplier": 1.0,
            "trailing_activation_multiplier": 1.0,
            "break_even_delay_multiplier": 1.0,
            "profit_lock_multiplier": 1.0,
            "partial_min_notional_multiplier": 1.05,
            "execution_split_preference": "UNCHANGED",
            "size_multiplier": 1.0,
        },
    }


def test_no_forbidden_execution_risk_sizing_exit_import_added_for_enrichment():
    source = _source()
    assert "import novo_soft_enrichment as _novo_soft_enrichment" in source
    forbidden_new_imports = (
        "import execution_router as _novo",
        "import execution_admission as _novo",
        "import risk_manager as _novo",
        "import sizing_engine as _novo",
        "import exit_manager as _novo",
        "import position_registry as _novo",
        "import okx_client as _novo",
    )
    for token in forbidden_new_imports:
        assert token not in source

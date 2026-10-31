from __future__ import annotations

import ast
import copy
from pathlib import Path

import novo_soft_enrichment as nse


class SettingsStub:
    def __init__(self, values=None):
        self.values = dict(values or {})

    def get_val(self, key, default=None):
        return self.values.get(key, default)


def _settings(**values):
    base = {
        "novo_no_hard_blocks": True,
        "novo_trade_quality_advisory_only": True,
    }
    base.update(values)
    return SettingsStub(base)


def _novo(score, tier=None):
    return {
        "enabled": True,
        "advisory_only": True,
        "quality_score": score,
        "quality_tier": tier or (
            "ELITE" if score >= 90 else "STRONG" if score >= 80 else "GOOD" if score >= 70 else "WEAK"
        ),
        "modules": {},
    }


def test_all_flags_default_false_returns_disabled():
    result = nse.build_novo_enrichment(novo=_novo(95), settings=_settings())

    assert result["status"] == "DISABLED"
    assert result["advisory_only"] is True
    assert result["no_hard_blocks"] is True
    assert result["suggestions"]["size_multiplier"] == 1.0


def test_missing_novo_metadata_returns_collecting_when_any_flag_enabled():
    result = nse.build_novo_enrichment(
        novo=None,
        settings=_settings(novo_exit_enrichment_enabled=True),
    )

    assert result["status"] == "COLLECTING"
    assert result["band"] == "COLLECTING"


def test_elite_band_detected_correctly():
    result = nse.build_novo_enrichment(_novo(90), settings=_settings(novo_exit_enrichment_enabled=True))

    assert result["status"] == "OK"
    assert result["band"] == "ELITE"
    assert result["quality_tier"] == "ELITE"


def test_strong_band_detected_correctly():
    result = nse.build_novo_enrichment(_novo(89), settings=_settings(novo_exit_enrichment_enabled=True))

    assert result["band"] == "STRONG"
    assert result["quality_tier"] == "STRONG"


def test_good_band_detected_correctly():
    result = nse.build_novo_enrichment(_novo(79), settings=_settings(novo_exit_enrichment_enabled=True))

    assert result["band"] == "GOOD"
    assert result["quality_tier"] == "GOOD"


def test_weak_band_detected_correctly():
    result = nse.build_novo_enrichment(_novo(42), settings=_settings(novo_exit_enrichment_enabled=True))

    assert result["band"] == "WEAK"
    assert result["quality_tier"] == "WEAK"


def test_exit_enrichment_changes_only_elite_and_strong_patience_and_tp():
    settings = _settings(novo_exit_enrichment_enabled=True)
    elite = nse.build_novo_enrichment(_novo(94), settings=settings)
    strong = nse.build_novo_enrichment(_novo(84), settings=settings)
    good = nse.build_novo_enrichment(_novo(74), settings=settings)
    weak = nse.build_novo_enrichment(_novo(50), settings=settings)

    assert elite["suggestions"]["tp2_multiplier"] > 1.0
    assert elite["suggestions"]["break_even_delay_multiplier"] > 1.0
    assert strong["suggestions"]["tp2_multiplier"] > 1.0
    assert strong["suggestions"]["trailing_activation_multiplier"] > 1.0
    assert good["suggestions"]["tp2_multiplier"] == 1.0
    assert weak["suggestions"]["break_even_delay_multiplier"] == 1.0


def test_fee_bleed_guard_changes_partial_minimum_without_close_or_block_output():
    result = nse.build_novo_enrichment(
        _novo(72),
        settings=_settings(novo_fee_bleed_guard_enabled=True),
    )

    assert result["suggestions"]["partial_min_notional_multiplier"] > 1.0
    forbidden = {"close", "block", "reject", "execute", "order", "trade_intent"}
    assert forbidden.isdisjoint(result)
    assert forbidden.isdisjoint(result["suggestions"])


def test_elite_size_boost_applies_only_to_elite_and_is_capped():
    settings = _settings(novo_elite_size_boost_enabled=True, novo_elite_size_boost_max=2.0)
    elite = nse.build_novo_enrichment(_novo(96), settings=settings)
    strong = nse.build_novo_enrichment(_novo(88), settings=settings)

    assert 1.05 <= elite["suggestions"]["size_multiplier"] <= 1.12
    assert strong["suggestions"]["size_multiplier"] == 1.0


def test_soft_confidence_event_without_approval_or_refusal():
    result = nse.build_novo_enrichment(
        _novo(82),
        settings=_settings(novo_soft_confidence_enabled=True),
    )

    assert "SOFT_CONFIDENCE_OBSERVED" in result["events"]
    forbidden = {"approve", "reject", "admission_override"}
    assert forbidden.isdisjoint(result)


def test_inputs_are_not_mutated():
    novo = _novo(93)
    settings = {"novo_exit_enrichment_enabled": True, "novo_no_hard_blocks": True}
    trade_context = {"symbol": "SUI", "nested": {"mfe": 1.2}}
    before_novo = copy.deepcopy(novo)
    before_settings = copy.deepcopy(settings)
    before_context = copy.deepcopy(trade_context)

    nse.build_novo_enrichment(novo=novo, settings=settings, trade_context=trade_context)

    assert novo == before_novo
    assert settings == before_settings
    assert trade_context == before_context


def test_bad_or_corrupt_input_does_not_raise():
    result = nse.build_novo_enrichment(
        novo={"quality_score": object()},
        settings=_settings(novo_exit_enrichment_enabled=True),
        trade_context=object(),
    )

    assert result["status"] == "COLLECTING"
    assert result["advisory_only"] is True


def test_forbidden_imports_are_absent():
    source_path = Path(__file__).resolve().parents[1] / "novo_soft_enrichment.py"
    tree = ast.parse(source_path.read_text(encoding="utf-8"))
    imported = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported.add(node.module)

    forbidden = {
        "execution_router",
        "execution_admission",
        "okx_client",
        "risk_manager",
        "sizing_engine",
        "position_registry",
        "exit_manager",
        "bot",
        "tg_panels",
        "trading_engine",
        "category_engine",
        "scheduler",
    }
    assert imported.isdisjoint(forbidden)


def test_output_contains_no_forbidden_control_fields():
    result = nse.build_novo_enrichment(
        _novo(95),
        settings=_settings(
            novo_soft_confidence_enabled=True,
            novo_exit_enrichment_enabled=True,
            novo_profit_lock_enrichment_enabled=True,
            novo_fee_bleed_guard_enabled=True,
            novo_elite_size_boost_enabled=True,
        ),
    )
    forbidden = {
        "order",
        "execute",
        "block",
        "reject",
        "approve",
        "admission_override",
        "trade_intent",
    }

    assert forbidden.isdisjoint(result)
    assert forbidden.isdisjoint(result["suggestions"])

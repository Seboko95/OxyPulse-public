from __future__ import annotations

import ast
import copy
from pathlib import Path
from types import SimpleNamespace

import novo_trade_quality as tq


def _mod(score, bias="LONG", status="OK", events=None):
    return {
        "status": status,
        "score": score,
        "bias": bias,
        "events": events or [],
        "explanation": "test module",
    }


def test_no_components_returns_collecting():
    result = tq.calculate_trade_quality(symbol="BTC", side="LONG")

    assert result["status"] == "INSUFFICIENT_DATA"
    assert result["quality_score"] == 0
    assert result["quality_tier"] == "COLLECTING"
    assert result["advisory_only"] is True


def test_all_aligned_strong_modules_returns_elite_or_strong():
    modules = {
        "market_dna": _mod(88),
        "market_structure": _mod(90),
        "bias_4h": _mod(87),
        "crt": _mod(84),
        "liquidity_sweep": _mod(86),
        "session_model": _mod(82),
    }

    result = tq.calculate_trade_quality(
        symbol="ETH",
        side="LONG",
        engine="ENMA",
        existing_confidence=9.0,
        consensus={"agreement_count": 4},
        modules=modules,
    )

    assert result["status"] == "OK"
    assert result["quality_tier"] in {"ELITE", "STRONG"}
    assert result["quality_score"] >= 80
    assert result["alignment"]["aligned"] == 6
    assert "ALIGNMENT_BOOST" in result["events"]


def test_conflicting_modules_reduce_score_but_do_not_error():
    modules = {
        "market_dna": _mod(85, "SHORT"),
        "market_structure": _mod(82, "SHORT"),
        "bias_4h": _mod(75, "LONG"),
    }

    result = tq.calculate_trade_quality(side="LONG", existing_confidence=8.0, modules=modules)

    assert result["status"] == "OK"
    assert result["quality_score"] < 85
    assert result["alignment"]["conflicted"] == 2
    assert "CONFLICT_PENALTY" in result["events"]
    assert result["advisory_only"] is True


def test_missing_modules_are_normalized_and_do_not_crush_score():
    result = tq.calculate_trade_quality(
        side="LONG",
        modules={"market_dna": _mod(90, "LONG")},
    )

    assert result["status"] == "OK"
    assert result["quality_score"] >= 90
    assert result["components"]["market_dna"] == 90
    assert result["components"]["bias_4h"] is None


def test_malformed_module_outputs_do_not_raise():
    modules = {
        "market_dna": {"status": "OK", "score": "bad", "bias": "LONG"},
        "market_structure": object(),
        "bias_4h": _mod(72, "NEUTRAL"),
    }

    result = tq.calculate_trade_quality(side="LONG", modules=modules)

    assert result["status"] == "OK"
    assert result["components"]["bias_4h"] == 72
    assert result["warnings"]
    assert result["advisory_only"] is True


def test_existing_confidence_and_consensus_are_included_when_valid():
    result = tq.calculate_trade_quality(
        existing_confidence=3.5,
        consensus=SimpleNamespace(agreement_count=3),
    )

    assert result["status"] == "OK"
    assert result["components"]["existing_confidence"] == 35
    assert result["components"]["consensus"] == 60
    assert "EXISTING_CONFIDENCE_INCLUDED" in result["events"]
    assert "CONSENSUS_INCLUDED" in result["events"]


def test_quality_tier_boundaries_work():
    assert tq.calculate_trade_quality(modules={"market_dna": _mod(95)})["quality_tier"] == "ELITE"
    assert tq.calculate_trade_quality(modules={"market_dna": _mod(85)})["quality_tier"] == "STRONG"
    assert tq.calculate_trade_quality(modules={"market_dna": _mod(75)})["quality_tier"] == "GOOD"
    assert tq.calculate_trade_quality(modules={"market_dna": _mod(50)})["quality_tier"] == "WEAK"


def test_side_alignment_counts_are_correct():
    modules = {
        "market_dna": _mod(70, "LONG"),
        "market_structure": _mod(70, "SHORT"),
        "bias_4h": _mod(70, "NEUTRAL"),
        "crt": {"status": "INSUFFICIENT_DATA"},
    }

    result = tq.calculate_trade_quality(side="LONG", modules=modules)

    assert result["alignment"] == {
        "aligned": 1,
        "conflicted": 1,
        "neutral": 1,
        "unknown": 3,
    }


def test_advisory_only_is_always_true():
    assert tq.calculate_trade_quality()["advisory_only"] is True
    assert tq.calculate_trade_quality(modules={"market_dna": _mod(80)})["advisory_only"] is True
    assert tq.calculate_trade_quality(modules=object())["advisory_only"] is True


def test_input_objects_are_not_mutated():
    consensus = {"agreement_count": 3}
    modules = {
        "market_dna": _mod(80, "LONG", events=["A"]),
        "market_structure": {"status": "OK", "score": 75, "bias": "NEUTRAL", "events": ["B"]},
    }
    before_consensus = copy.deepcopy(consensus)
    before_modules = copy.deepcopy(modules)

    tq.calculate_trade_quality(side="LONG", consensus=consensus, modules=modules)

    assert consensus == before_consensus
    assert modules == before_modules


def test_forbidden_imports_are_absent():
    source_path = Path(__file__).resolve().parents[1] / "novo_trade_quality.py"
    tree = ast.parse(source_path.read_text(encoding="utf-8"))
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
        "bot",
        "tg_panels",
        "trading_engine",
    }
    assert imported.isdisjoint(forbidden)


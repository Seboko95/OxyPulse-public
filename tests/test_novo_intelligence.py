from __future__ import annotations

import ast
import copy
from pathlib import Path
from types import SimpleNamespace

import novo_intelligence as novo


def _c(open_, high, low, close, volume=100.0):
    return {"open": open_, "high": high, "low": low, "close": close, "volume": volume}


def _candles():
    return [
        _c(10, 10.8, 9.8, 10.5, 100),
        _c(10.5, 11.2, 10.3, 10.9, 100),
        _c(10.9, 11.5, 10.7, 11.2, 100),
        _c(11.2, 11.9, 11.0, 11.6, 100),
        _c(11.6, 12.8, 11.4, 12.4, 150),
        _c(12.4, 14.0, 12.2, 13.7, 170),
        _c(13.7, 15.8, 13.5, 15.4, 190),
        _c(15.4, 19.0, 15.2, 18.7, 230),
    ]


def _market_data():
    candles = _candles()
    return {
        "candles": copy.deepcopy(candles),
        "candles_15m": copy.deepcopy(candles),
        "candles_1h": copy.deepcopy(candles),
        "candles_4h": copy.deepcopy(candles),
        "levels": {"previous_high": 18.0, "previous_low": 9.0},
        "session_levels": {"asia_high": 18.0, "asia_low": 9.0},
    }


def test_disabled_by_default_remains_unchanged():
    snap = novo.analyze_candidate("BTC-USDT-SWAP", "long")

    assert snap["enabled"] is False
    assert snap["advisory_only"] is True
    assert snap["quality_score"] is None
    assert snap["quality_tier"] == "DISABLED"
    assert snap["modules"]["market_dna"] == {"status": "PENDING"}


def test_enabled_with_valid_candles_populates_modules():
    snap = novo.analyze_candidate(
        "ETH-USDT-SWAP",
        "long",
        engine="ENMA",
        market_data=_market_data(),
        confidence=8.5,
        consensus=SimpleNamespace(agreement_count=4),
        settings={"novo_intelligence_enabled": True},
    )

    assert snap["enabled"] is True
    assert snap["advisory_only"] is True
    assert set(snap["modules"]) == {
        "market_dna",
        "market_structure",
        "bias_4h",
        "crt",
        "liquidity_sweep",
        "session_model",
    }
    assert all(result["status"] in {"OK", "INSUFFICIENT_DATA"} for result in snap["modules"].values())
    assert snap["modules"]["market_dna"]["status"] == "OK"
    assert snap["modules"]["bias_4h"]["status"] == "OK"
    assert snap["explanations"]


def test_missing_market_data_does_not_crash():
    settings = SimpleNamespace(get_val=lambda key, default=None: True if key == "novo_intelligence_enabled" else default)

    snap = novo.analyze_candidate("SUI-USDT-SWAP", "LONG", settings=settings)

    assert snap["enabled"] is True
    assert snap["advisory_only"] is True
    assert snap["warnings"]
    assert "Market data unavailable" in snap["warnings"][0]
    assert all(result["status"] in {"INSUFFICIENT_DATA", "ERROR"} for result in snap["modules"].values())


def test_individual_module_failure_does_not_crash_full_analysis(monkeypatch):
    def boom(*_args, **_kwargs):
        raise RuntimeError("module boom")

    monkeypatch.setattr(novo.novo_market_dna, "analyze_market_dna", boom)

    snap = novo.analyze_candidate(
        "ETH-USDT-SWAP",
        "LONG",
        market_data=_market_data(),
        settings={"novo_intelligence_enabled": True},
    )

    assert snap["enabled"] is True
    assert snap["modules"]["market_dna"]["status"] == "ERROR"
    assert "module boom" in snap["modules"]["market_dna"]["errors"][0]
    assert snap["modules"]["market_structure"]["status"] == "OK"
    assert snap["advisory_only"] is True


def test_disabled_module_flag_returns_disabled():
    snap = novo.analyze_candidate(
        "ETH-USDT-SWAP",
        "LONG",
        market_data=_market_data(),
        settings={"novo_intelligence_enabled": True, "novo_crt_enabled": False},
    )

    assert snap["modules"]["crt"] == {"status": "DISABLED"}
    assert snap["modules"]["market_dna"]["status"] == "OK"


def test_trade_quality_populates_score_and_tier_when_enabled():
    snap = novo.analyze_candidate(
        "ETH-USDT-SWAP",
        "LONG",
        engine="ENMA",
        market_data=_market_data(),
        confidence=9.0,
        consensus={"agreement_count": 4},
        settings={"novo_intelligence_enabled": True},
    )

    assert isinstance(snap["quality_score"], int)
    assert snap["quality_score"] > 0
    assert snap["quality_tier"] in {"ELITE", "STRONG", "GOOD", "WEAK"}
    assert snap["trade_quality"]["advisory_only"] is True


def test_inputs_are_not_mutated():
    market_data = _market_data()
    consensus = {"agreement_count": 3}
    before_market = copy.deepcopy(market_data)
    before_consensus = copy.deepcopy(consensus)

    novo.analyze_candidate(
        "ETH-USDT-SWAP",
        "LONG",
        market_data=market_data,
        consensus=consensus,
        settings={"novo_intelligence_enabled": True},
    )

    assert market_data == before_market
    assert consensus == before_consensus


class BrokenSettings:
    def get_val(self, *_args, **_kwargs):
        raise RuntimeError("broken settings")


def test_advisory_only_is_always_true():
    assert novo.analyze_candidate("BTC", "LONG")["advisory_only"] is True
    assert novo.analyze_candidate("BTC", "LONG", settings={"novo_intelligence_enabled": True})["advisory_only"] is True
    assert novo.analyze_candidate(None, None, settings=BrokenSettings())["advisory_only"] is True


def test_forbidden_imports_are_absent_from_novo_intelligence():
    source_path = Path(__file__).resolve().parents[1] / "novo_intelligence.py"
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


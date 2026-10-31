from __future__ import annotations

import ast
import copy
from pathlib import Path

import gem_hunter as gh


class SettingsStub:
    def __init__(self, values=None):
        self.values = dict(values or {})

    def get_val(self, key, default=None):
        return self.values.get(key, default)


def _settings(**values):
    base = {"novo_gem_hunter_enabled": True, "gem_hunter_alert_only": True}
    base.update(values)
    return SettingsStub(base)


def _c(open_, high, low, close, volume=100.0):
    return {"open": open_, "high": high, "low": low, "close": close, "volume": volume}


def _base_candles():
    return [
        _c(10.0, 10.4, 9.9, 10.2, 100),
        _c(10.2, 10.5, 10.0, 10.3, 100),
        _c(10.3, 10.6, 10.1, 10.4, 100),
        _c(10.4, 10.7, 10.2, 10.5, 100),
        _c(10.5, 10.8, 10.3, 10.6, 100),
        _c(10.6, 10.9, 10.4, 10.7, 100),
        _c(10.7, 11.0, 10.5, 10.8, 100),
        _c(10.8, 11.1, 10.6, 10.9, 100),
        _c(10.9, 11.2, 10.7, 11.0, 100),
        _c(11.0, 11.3, 10.8, 11.1, 100),
    ]


def test_disabled_by_default_returns_disabled():
    result = gh.analyze_gem_candidate("SUI", candles=_base_candles())

    assert result["status"] == "DISABLED"
    assert result["alert_only"] is True
    assert result["gem_tier"] == "COLLECTING"


def test_enabled_but_insufficient_data_returns_collecting():
    result = gh.analyze_gem_candidate("SUI", candles=[_c(1, 2, 0.9, 1.5)], settings=_settings())

    assert result["status"] == "INSUFFICIENT_DATA"
    assert result["gem_tier"] == "COLLECTING"
    assert result["gem_score"] == 0


def test_volume_expansion_increases_score():
    candles = _base_candles()
    candles[-3:] = [
        _c(11.1, 11.6, 11.0, 11.5, 260),
        _c(11.5, 12.0, 11.4, 11.9, 280),
        _c(11.9, 12.6, 11.8, 12.5, 310),
    ]

    result = gh.analyze_gem_candidate("SUI", candles=candles, settings=_settings())

    assert result["status"] == "OK"
    assert result["signals"]["volume_expansion"] > 0
    assert "VOLUME_EXPANSION" in result["events"]
    assert result["gem_score"] > 0


def test_volatility_expansion_increases_score():
    candles = _base_candles()
    candles[-3:] = [
        _c(11.1, 12.8, 10.9, 12.4, 120),
        _c(12.4, 14.2, 12.1, 13.8, 130),
        _c(13.8, 16.0, 13.5, 15.7, 140),
    ]

    result = gh.analyze_gem_candidate("SUI", candles=candles, settings=_settings())

    assert result["signals"]["volatility_expansion"] > 0
    assert "VOLATILITY_EXPANSION" in result["events"]


def test_trend_acceleration_detects_long():
    candles = _base_candles()
    candles[-4:] = [
        _c(11.1, 11.8, 11.0, 11.7, 120),
        _c(11.7, 12.5, 11.6, 12.3, 140),
        _c(12.3, 13.2, 12.2, 13.0, 160),
        _c(13.0, 14.4, 12.9, 14.2, 200),
    ]

    result = gh.analyze_gem_candidate("SUI", candles=candles, settings=_settings())

    assert result["direction"] == "LONG"
    assert result["signals"]["trend_acceleration"] > 0
    assert "TREND_ACCELERATION" in result["events"]


def test_breakdown_acceleration_detects_short():
    candles = _base_candles()
    candles[-4:] = [
        _c(11.1, 11.2, 10.4, 10.5, 120),
        _c(10.5, 10.6, 9.8, 9.9, 140),
        _c(9.9, 10.0, 9.0, 9.1, 160),
        _c(9.1, 9.2, 7.8, 8.0, 220),
    ]

    result = gh.analyze_gem_candidate("SUI", candles=candles, settings=_settings())

    assert result["direction"] == "SHORT"
    assert result["signals"]["trend_acceleration"] > 0


def test_compression_breakout_detects_event():
    candles = [
        _c(10, 13, 9, 12, 100),
        _c(12, 14, 11, 13, 100),
        _c(13, 15, 12, 14, 100),
        _c(14, 16, 13, 15, 100),
        _c(15, 15.4, 14.8, 15.1, 100),
        _c(15.1, 15.5, 14.9, 15.2, 100),
        _c(15.2, 15.6, 15.0, 15.3, 100),
        _c(15.3, 15.7, 15.1, 15.4, 100),
        _c(15.4, 15.8, 15.2, 15.5, 100),
        _c(15.5, 17.4, 15.4, 17.2, 180),
    ]

    result = gh.analyze_gem_candidate("SUI", candles=candles, settings=_settings())

    assert result["signals"]["compression_breakout"] > 0
    assert "COMPRESSION_BREAKOUT" in result["events"]


def test_liquidity_reclaim_detects_event():
    candles = _base_candles()
    candles[-1] = _c(10.8, 11.0, 9.7, 10.2, 180)

    result = gh.analyze_gem_candidate("SUI", candles=candles, settings=_settings())

    assert result["signals"]["liquidity_reclaim"] > 0
    assert "LIQUIDITY_RECLAIM" in result["events"]
    assert result["direction"] == "LONG"


def test_social_narrative_context_gives_small_bonus_only():
    candles = _base_candles()
    plain = gh.analyze_gem_candidate("SUI", candles=candles, settings=_settings())
    contextual = gh.analyze_gem_candidate(
        "SUI",
        candles=candles,
        market_context={"social_score": 95, "narrative": "AI infra", "listing_age_days": 12},
        settings=_settings(),
    )

    assert contextual["gem_score"] >= plain["gem_score"]
    assert contextual["gem_score"] - plain["gem_score"] <= 7
    assert "SOCIAL_CONTEXT_BONUS" in contextual["events"]


def test_alert_only_is_always_true():
    result = gh.analyze_gem_candidate("SUI", candles=_base_candles(), settings=_settings())

    assert result["alert_only"] is True


def test_alert_only_false_produces_warning_and_no_execution_output():
    result = gh.analyze_gem_candidate(
        "SUI",
        candles=_base_candles(),
        settings=_settings(gem_hunter_alert_only=False),
    )

    assert result["alert_only"] is True
    assert result["warnings"]
    forbidden_output_keys = {"order", "trade_intent", "client_order_id", "size", "entry", "stop_loss", "take_profit"}
    assert forbidden_output_keys.isdisjoint(result)


def test_inputs_are_not_mutated():
    candles = [{"o": 10, "h": 11, "l": 9.8, "c": 10.5, "v": 100} for _ in range(10)]
    market_context = {"social_score": 80, "narrative": "L2"}
    before_candles = copy.deepcopy(candles)
    before_context = copy.deepcopy(market_context)

    gh.analyze_gem_candidate("SUI", candles=candles, market_context=market_context, settings=_settings())

    assert candles == before_candles
    assert market_context == before_context


def test_bad_or_corrupt_candles_do_not_raise():
    result = gh.analyze_gem_candidate("SUI", candles=[None, "bad", {"o": "x"}], settings=_settings())

    assert result["status"] == "INSUFFICIENT_DATA"
    assert result["alert_only"] is True


def test_forbidden_imports_are_absent():
    source_path = Path(__file__).resolve().parents[1] / "gem_hunter.py"
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

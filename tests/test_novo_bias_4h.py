from __future__ import annotations

import ast
import copy
from pathlib import Path

import novo_bias_4h as bias4h


def _c(open_, high, low, close, volume=100.0):
    return {"open": open_, "high": high, "low": low, "close": close, "volume": volume}


def test_insufficient_data_returns_safe_result():
    result = bias4h.analyze_4h_bias([_c(1, 2, 0.5, 1.5)], side="LONG")

    assert result["status"] == "INSUFFICIENT_DATA"
    assert result["timeframe"] == "4H"
    assert result["bias"] == "NEUTRAL"
    assert result["bias_type"] == "UNKNOWN"
    assert result["score"] == 0


def test_bullish_expansion_detection():
    candles = [
        _c(10, 10.8, 9.8, 10.5, 100),
        _c(10.5, 11.2, 10.3, 10.9, 100),
        _c(10.9, 11.5, 10.7, 11.2, 100),
        _c(11.2, 11.9, 11.0, 11.6, 100),
        _c(11.6, 12.8, 11.4, 12.4, 150),
        _c(12.4, 14.0, 12.2, 13.7, 170),
        _c(13.7, 15.8, 13.5, 15.4, 190),
        _c(15.4, 19.0, 15.2, 18.7, 230),
    ]

    result = bias4h.analyze_4h_bias(candles)

    assert result["status"] == "OK"
    assert result["bias"] == "LONG"
    assert result["bias_type"] == "BULLISH_EXPANSION"
    assert "BULLISH_EXPANSION" in result["events"]
    assert result["score"] >= 80


def test_bearish_expansion_detection():
    candles = [
        _c(20, 20.2, 19.2, 19.5, 100),
        _c(19.5, 19.7, 18.8, 19.0, 100),
        _c(19.0, 19.2, 18.4, 18.7, 100),
        _c(18.7, 18.9, 18.0, 18.2, 100),
        _c(18.2, 18.4, 16.8, 17.0, 150),
        _c(17.0, 17.2, 15.2, 15.6, 170),
        _c(15.6, 15.8, 13.4, 13.8, 190),
        _c(13.8, 14.0, 10.0, 10.3, 230),
    ]

    result = bias4h.analyze_4h_bias(candles)

    assert result["bias"] == "SHORT"
    assert result["bias_type"] == "BEARISH_EXPANSION"
    assert "BEARISH_EXPANSION" in result["events"]
    assert result["score"] >= 80


def test_sweep_low_reclaim_manipulation_gives_long_bias():
    candles = [
        _c(10, 11, 9, 10.5, 100),
        _c(10.5, 11.2, 9.5, 10.8, 100),
        _c(10.8, 11.4, 9.8, 10.9, 100),
        _c(10.9, 11.5, 10.0, 11.1, 100),
        _c(11.1, 11.7, 10.2, 11.3, 100),
        _c(11.3, 11.8, 10.4, 11.5, 100),
        _c(11.5, 12.0, 10.0, 10.8, 100),
        _c(10.7, 11.7, 9.4, 11.2, 120),
    ]

    result = bias4h.analyze_4h_bias(candles)

    assert result["bias"] == "LONG"
    assert result["bias_type"] == "MANIPULATION"
    assert "SWEEP_LOW_RECLAIM" in result["events"]


def test_sweep_high_rejection_manipulation_gives_short_bias():
    candles = [
        _c(10, 11, 9, 10.5, 100),
        _c(10.5, 11.2, 9.5, 10.8, 100),
        _c(10.8, 11.4, 9.8, 10.9, 100),
        _c(10.9, 11.5, 10.0, 11.1, 100),
        _c(11.1, 11.7, 10.2, 11.3, 100),
        _c(11.3, 11.8, 10.4, 11.5, 100),
        _c(11.5, 12.0, 10.5, 11.8, 100),
        _c(11.8, 12.6, 10.7, 11.4, 120),
    ]

    result = bias4h.analyze_4h_bias(candles)

    assert result["bias"] == "SHORT"
    assert result["bias_type"] == "MANIPULATION"
    assert "SWEEP_HIGH_REJECT" in result["events"]


def test_indecision_detection():
    candles = [
        _c(10, 11, 9, 10.5, 100),
        _c(10.5, 11.2, 9.5, 10.8, 100),
        _c(10.8, 11.4, 9.8, 10.9, 100),
        _c(10.9, 11.5, 10.0, 11.1, 100),
        _c(11.1, 11.7, 10.2, 11.3, 100),
        _c(11.3, 11.8, 10.4, 11.5, 100),
        _c(11.5, 12.0, 10.5, 11.7, 100),
        _c(11.6, 12.8, 10.4, 11.7, 120),
    ]

    result = bias4h.analyze_4h_bias(candles)

    assert result["bias"] == "NEUTRAL"
    assert result["bias_type"] == "INDECISION"
    assert "INDECISION" in result["events"]
    assert result["score"] <= 55


def test_range_detection():
    candles = [
        _c(10.0, 11.0, 9.0, 10.4, 100),
        _c(10.4, 11.1, 9.2, 10.1, 100),
        _c(10.1, 11.0, 9.1, 10.5, 100),
        _c(10.5, 11.2, 9.3, 10.2, 100),
        _c(10.2, 11.0, 9.1, 10.6, 100),
        _c(10.6, 11.3, 9.4, 10.3, 100),
        _c(10.3, 11.1, 9.2, 10.4, 100),
        _c(10.4, 11.0, 10.0, 10.7, 100),
    ]

    result = bias4h.analyze_4h_bias(candles)

    assert result["bias"] == "NEUTRAL"
    assert result["bias_type"] == "RANGE"
    assert "RANGE" in result["events"]


def test_side_alignment_event_and_explanation():
    candles = [
        _c(10, 10.8, 9.8, 10.5, 100),
        _c(10.5, 11.2, 10.3, 10.9, 100),
        _c(10.9, 11.5, 10.7, 11.2, 100),
        _c(11.2, 11.9, 11.0, 11.6, 100),
        _c(11.6, 12.8, 11.4, 12.4, 150),
        _c(12.4, 14.0, 12.2, 13.7, 170),
        _c(13.7, 15.8, 13.5, 15.4, 190),
        _c(15.4, 19.0, 15.2, 18.7, 230),
    ]

    aligned = bias4h.analyze_4h_bias(candles, side="LONG")
    conflict = bias4h.analyze_4h_bias(candles, side="SHORT")

    assert "SIDE_ALIGNED" in aligned["events"]
    assert "supports LONG" in aligned["explanation"]
    assert "SIDE_CONFLICT" in conflict["events"]
    assert "conflicts with SHORT" in conflict["explanation"]


def test_bad_or_corrupt_candles_do_not_raise():
    result = bias4h.analyze_4h_bias([None, "bad", {"o": "x", "h": 1, "l": 1, "c": 1}], side=object())

    assert result["status"] == "INSUFFICIENT_DATA"
    assert "errors" in result


def test_input_candles_are_not_mutated():
    candles = [
        {"o": 10, "h": 10.8, "l": 9.8, "c": 10.5, "v": 100, "ts": 1},
        {"o": 10.5, "h": 11.2, "l": 10.3, "c": 10.9, "v": 100, "ts": 2},
        {"o": 10.9, "h": 11.5, "l": 10.7, "c": 11.2, "v": 100, "ts": 3},
        {"o": 11.2, "h": 11.9, "l": 11.0, "c": 11.6, "v": 100, "ts": 4},
        {"o": 11.6, "h": 12.8, "l": 11.4, "c": 12.4, "v": 150, "ts": 5},
        {"o": 12.4, "h": 14.0, "l": 12.2, "c": 13.7, "v": 170, "ts": 6},
        {"o": 13.7, "h": 15.8, "l": 13.5, "c": 15.4, "v": 190, "ts": 7},
        {"o": 15.4, "h": 19.0, "l": 15.2, "c": 18.7, "v": 230, "ts": 8},
    ]
    before = copy.deepcopy(candles)

    bias4h.analyze_4h_bias(candles)

    assert candles == before


def test_forbidden_imports_are_absent():
    source_path = Path(__file__).resolve().parents[1] / "novo_bias_4h.py"
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

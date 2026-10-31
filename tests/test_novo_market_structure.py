from __future__ import annotations

import ast
import copy
from pathlib import Path

import novo_market_structure as ms


def _c(high, low, close):
    return {"open": close, "high": high, "low": low, "close": close, "volume": 1.0}


def test_insufficient_data_returns_safe_result():
    result = ms.analyze_market_structure([_c(2, 1, 1.5)], side="LONG", timeframe="15m")

    assert result["status"] == "INSUFFICIENT_DATA"
    assert result["structure"] == "INSUFFICIENT_DATA"
    assert result["bias"] == "NEUTRAL"
    assert result["score"] == 0
    assert result["advisory_only"] if "advisory_only" in result else True


def test_bullish_hh_hl_detection():
    candles = [
        _c(10, 5, 7),
        _c(12, 6, 8),
        _c(11, 7, 8),
        _c(15, 8, 10),
        _c(14, 9, 10),
        _c(16, 10, 12),
    ]

    result = ms.analyze_market_structure(candles)

    assert result["status"] == "OK"
    assert result["bias"] == "LONG"
    assert "HH" in result["events"]
    assert "HL" in result["events"]
    assert result["score"] >= 70


def test_bearish_lh_ll_detection():
    candles = [
        _c(16, 10, 14),
        _c(15, 9, 13),
        _c(14, 8, 12),
        _c(13, 7, 11),
        _c(12, 6, 10),
        _c(11, 5, 9),
    ]

    result = ms.analyze_market_structure(candles)

    assert result["status"] == "OK"
    assert result["bias"] == "SHORT"
    assert "LH" in result["events"]
    assert "LL" in result["events"]
    assert result["score"] >= 70


def test_bos_bullish_detection():
    candles = [
        _c(10, 5, 7),
        _c(12, 6, 8),
        _c(11, 7, 8),
        _c(13, 7.5, 10),
        _c(12.5, 8, 10),
        _c(16, 9, 15),
    ]

    result = ms.analyze_market_structure(candles)

    assert result["structure"] == "BOS_BULLISH"
    assert result["bias"] == "LONG"
    assert "BOS_BULLISH" in result["events"]
    assert result["score"] >= 85


def test_bos_bearish_detection():
    candles = [
        _c(16, 9, 13),
        _c(15, 8, 12),
        _c(14, 7, 11),
        _c(13, 6, 10),
        _c(12, 5, 9),
        _c(11, 3, 4),
    ]

    result = ms.analyze_market_structure(candles)

    assert result["structure"] == "BOS_BEARISH"
    assert result["bias"] == "SHORT"
    assert "BOS_BEARISH" in result["events"]
    assert result["score"] >= 85


def test_range_mixed_detection():
    candles = [
        _c(10, 5, 7),
        _c(12, 6, 8),
        _c(11, 4, 7),
        _c(13, 5, 8),
        _c(12, 3.8, 7),
        _c(12.5, 5.2, 8),
    ]

    result = ms.analyze_market_structure(candles)

    assert result["status"] == "OK"
    assert result["bias"] == "NEUTRAL"
    assert result["structure"] == "RANGE_BOUND"
    assert 40 <= result["score"] <= 60


def test_side_alignment_explanation():
    candles = [
        _c(10, 5, 7),
        _c(12, 6, 8),
        _c(11, 7, 8),
        _c(15, 8, 10),
        _c(14, 9, 10),
        _c(16, 10, 12),
    ]

    aligned = ms.analyze_market_structure(candles, side="LONG")
    conflict = ms.analyze_market_structure(candles, side="SHORT")

    assert "SIDE_ALIGNED" in aligned["events"]
    assert "supports LONG" in aligned["explanation"]
    assert "SIDE_CONFLICT" in conflict["events"]
    assert "conflicts with SHORT" in conflict["explanation"]


def test_bad_or_corrupt_candles_do_not_raise():
    result = ms.analyze_market_structure([None, "bad", {"h": "x", "l": 1, "c": 1}], side=object())

    assert result["status"] == "INSUFFICIENT_DATA"
    assert "errors" in result


def test_input_candles_are_not_mutated():
    candles = [
        {"o": 7, "h": 10, "l": 5, "c": 7, "ts": 1},
        {"o": 8, "h": 12, "l": 6, "c": 8, "ts": 2},
        {"o": 8, "h": 11, "l": 7, "c": 8, "ts": 3},
        {"o": 10, "h": 15, "l": 8, "c": 10, "ts": 4},
        {"o": 10, "h": 14, "l": 9, "c": 10, "ts": 5},
        {"o": 12, "h": 16, "l": 10, "c": 12, "ts": 6},
    ]
    before = copy.deepcopy(candles)

    ms.analyze_market_structure(candles)

    assert candles == before


def test_forbidden_imports_are_absent():
    source_path = Path(__file__).resolve().parents[1] / "novo_market_structure.py"
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

from __future__ import annotations

import ast
import copy
from pathlib import Path

import novo_liquidity_sweep as ls


def _c(open_, high, low, close, volume=100.0):
    return {"open": open_, "high": high, "low": low, "close": close, "volume": volume}


def _base():
    return [
        _c(10, 11, 9, 10.5),
        _c(10.5, 12, 9.5, 11),
        _c(11, 11.8, 9.8, 10.8),
        _c(10.8, 11.7, 10.0, 11.1),
        _c(11.1, 11.6, 10.2, 11.3),
    ]


def test_insufficient_data_returns_safe_result():
    result = ls.analyze_liquidity_sweep([_c(1, 2, 0.5, 1.5)], side="LONG")

    assert result["status"] == "INSUFFICIENT_DATA"
    assert result["bias"] == "NEUTRAL"
    assert result["sweep_state"] == "UNKNOWN"
    assert result["score"] == 0


def test_sweep_low_reclaim_gives_long_bias():
    candles = _base() + [_c(10.4, 11.4, 8.8, 9.6)]

    result = ls.analyze_liquidity_sweep(candles)

    assert result["bias"] == "LONG"
    assert result["sweep_state"] == "SWEEP_LOW_RECLAIM"
    assert "SWEEP_LOW_RECLAIM" in result["events"]


def test_sweep_high_reject_gives_short_bias():
    candles = _base() + [_c(11.5, 12.3, 10.8, 11.7)]

    result = ls.analyze_liquidity_sweep(candles)

    assert result["bias"] == "SHORT"
    assert result["sweep_state"] == "SWEEP_HIGH_REJECT"
    assert "SWEEP_HIGH_REJECT" in result["events"]


def test_breakout_hold_gives_long_bias():
    candles = _base() + [_c(11.8, 13.0, 11.5, 12.6)]

    result = ls.analyze_liquidity_sweep(candles)

    assert result["bias"] == "LONG"
    assert result["sweep_state"] == "BREAKOUT_HOLD"
    assert "BREAKOUT_HOLD" in result["events"]


def test_breakdown_hold_gives_short_bias():
    candles = _base() + [_c(9.4, 9.8, 8.5, 8.7)]

    result = ls.analyze_liquidity_sweep(candles)

    assert result["bias"] == "SHORT"
    assert result["sweep_state"] == "BREAKDOWN_HOLD"
    assert "BREAKDOWN_HOLD" in result["events"]


def test_equal_highs_detection():
    candles = [
        _c(10, 12.0, 9, 10.5),
        _c(10.5, 12.01, 9.5, 11),
        _c(11, 11.99, 9.8, 10.8),
        _c(10.8, 11.7, 10.0, 11.1),
        _c(11.1, 11.6, 10.2, 11.3),
        _c(11.2, 11.5, 10.7, 11.1),
    ]

    result = ls.analyze_liquidity_sweep(candles)

    assert result["levels"]["equal_high"] is not None
    assert "EQUAL_HIGHS" in result["events"]


def test_equal_lows_detection():
    candles = [
        _c(10, 11, 9.0, 10.5),
        _c(10.5, 12, 9.01, 11),
        _c(11, 11.8, 8.99, 10.8),
        _c(10.8, 11.7, 10.0, 11.1),
        _c(11.1, 11.6, 10.2, 11.3),
        _c(11.2, 11.5, 10.7, 11.1),
    ]

    result = ls.analyze_liquidity_sweep(candles)

    assert result["levels"]["equal_low"] is not None
    assert "EQUAL_LOWS" in result["events"]


def test_optional_provided_levels_are_used():
    candles = _base() + [_c(10.4, 11.4, 9.2, 10.2)]
    levels = {"session_low": 9.5, "previous_high": 20.0}

    result = ls.analyze_liquidity_sweep(candles, levels=levels)

    assert result["sweep_state"] == "SWEEP_LOW_RECLAIM"
    assert result["swept_level"]["type"] == "session_low"
    assert result["levels"]["session_low"] == 9.5


def test_volume_confirmation_event_appears_when_appropriate():
    candles = [
        _c(10, 11, 9, 10.5, 100),
        _c(10.5, 12, 9.5, 11, 100),
        _c(11, 11.8, 9.8, 10.8, 100),
        _c(10.8, 11.7, 10.0, 11.1, 100),
        _c(11.1, 11.6, 10.2, 11.3, 100),
        _c(10.4, 11.4, 8.8, 9.6, 180),
    ]

    result = ls.analyze_liquidity_sweep(candles)

    assert "VOLUME_CONFIRMATION" in result["events"]


def test_side_alignment_event_and_explanation():
    candles = _base() + [_c(10.4, 11.4, 8.8, 9.6)]

    aligned = ls.analyze_liquidity_sweep(candles, side="LONG")
    conflict = ls.analyze_liquidity_sweep(candles, side="SHORT")

    assert "SIDE_ALIGNED" in aligned["events"]
    assert "supports LONG" in aligned["explanation"]
    assert "SIDE_CONFLICT" in conflict["events"]
    assert "conflicts with SHORT" in conflict["explanation"]


def test_bad_or_corrupt_candles_do_not_raise():
    result = ls.analyze_liquidity_sweep([None, "bad", {"o": "x", "h": 1, "l": 1, "c": 1}], side=object())

    assert result["status"] == "INSUFFICIENT_DATA"
    assert "errors" in result


def test_input_candles_and_levels_are_not_mutated():
    candles = [
        {"o": 10, "h": 11, "l": 9, "c": 10.5, "v": 100, "ts": 1},
        {"o": 10.5, "h": 12, "l": 9.5, "c": 11, "v": 100, "ts": 2},
        {"o": 11, "h": 11.8, "l": 9.8, "c": 10.8, "v": 100, "ts": 3},
        {"o": 10.8, "h": 11.7, "l": 10.0, "c": 11.1, "v": 100, "ts": 4},
        {"o": 11.1, "h": 11.6, "l": 10.2, "c": 11.3, "v": 100, "ts": 5},
        {"o": 10.4, "h": 11.4, "l": 8.8, "c": 9.6, "v": 180, "ts": 6},
    ]
    levels = {"session_low": 9.5, "previous_high": 20.0}
    before_candles = copy.deepcopy(candles)
    before_levels = copy.deepcopy(levels)

    ls.analyze_liquidity_sweep(candles, levels=levels)

    assert candles == before_candles
    assert levels == before_levels


def test_forbidden_imports_are_absent():
    source_path = Path(__file__).resolve().parents[1] / "novo_liquidity_sweep.py"
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
        "liq_heatmap",
    }
    assert imported.isdisjoint(forbidden)


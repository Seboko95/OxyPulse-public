from __future__ import annotations

import ast
import copy
from pathlib import Path

import novo_crt_engine as crt


def _c(open_, high, low, close, volume=100.0):
    return {"open": open_, "high": high, "low": low, "close": close, "volume": volume}


def _base_with_ref(ref=None):
    base = [
        _c(10, 11, 9.8, 10.5),
        _c(10.5, 11.2, 10.2, 10.9),
        _c(10.9, 11.5, 10.6, 11.1),
        ref or _c(10, 20, 0, 18),
        _c(18, 19, 17, 18.2),
    ]
    return base


def test_insufficient_data_returns_safe_result():
    result = crt.analyze_crt([_c(1, 2, 0.5, 1.5)], side="LONG", timeframe="15m")

    assert result["status"] == "INSUFFICIENT_DATA"
    assert result["bias"] == "NEUTRAL"
    assert result["crt_state"] == "UNKNOWN"
    assert result["score"] == 0


def test_reference_candle_zones_are_calculated_correctly():
    candles = _base_with_ref()
    result = crt.analyze_crt(candles, settings={"novo_crt_reference_lookback": 3})

    ref = result["reference_candle"]
    assert ref["high"] == 20.0
    assert ref["low"] == 0.0
    assert ref["range"] == 20.0
    assert ref["mid"] == 10.0
    assert ref["top_25"] == 15.0
    assert ref["bottom_25"] == 5.0


def test_bullish_reclaim_detection():
    candles = _base_with_ref() + [_c(4.0, 12.0, -1.0, 8.0)]

    result = crt.analyze_crt(candles)

    assert result["bias"] == "LONG"
    assert result["crt_state"] == "BULLISH_RECLAIM"
    assert "BULLISH_RECLAIM" in result["events"]


def test_bearish_reject_detection():
    candles = _base_with_ref() + [_c(16.0, 21.0, 8.0, 12.0)]

    result = crt.analyze_crt(candles)

    assert result["bias"] == "SHORT"
    assert result["crt_state"] == "BEARISH_REJECT"
    assert "BEARISH_REJECT" in result["events"]


def test_bullish_retrace_hold_detection():
    candles = _base_with_ref(ref=_c(10, 20, 0, 18)) + [_c(7.0, 14.0, 4.0, 12.0)]

    result = crt.analyze_crt(candles)

    assert result["bias"] == "LONG"
    assert result["crt_state"] == "BULLISH_RETRACE_HOLD"
    assert "BULLISH_RETRACE_HOLD" in result["events"]


def test_bearish_retrace_hold_detection():
    candles = _base_with_ref(ref=_c(18, 20, 0, 2)) + [_c(13.0, 16.0, 5.0, 8.0)]

    result = crt.analyze_crt(candles)

    assert result["bias"] == "SHORT"
    assert result["crt_state"] == "BEARISH_RETRACE_HOLD"
    assert "BEARISH_RETRACE_HOLD" in result["events"]


def test_range_expansion_above_gives_long_bias():
    candles = _base_with_ref() + [_c(18.0, 27.0, 17.0, 24.0)]

    result = crt.analyze_crt(candles)

    assert result["bias"] == "LONG"
    assert result["crt_state"] == "RANGE_EXPANSION"
    assert "RANGE_EXPANSION_ABOVE" in result["events"]


def test_range_expansion_below_gives_short_bias():
    candles = _base_with_ref() + [_c(2.0, 3.0, -8.0, -4.0)]

    result = crt.analyze_crt(candles)

    assert result["bias"] == "SHORT"
    assert result["crt_state"] == "RANGE_EXPANSION"
    assert "RANGE_EXPANSION_BELOW" in result["events"]


def test_no_setup_returns_neutral():
    candles = _base_with_ref() + [_c(11.0, 13.0, 8.0, 9.0)]

    result = crt.analyze_crt(candles)

    assert result["bias"] == "NEUTRAL"
    assert result["crt_state"] == "NO_SETUP"
    assert "NO_SETUP" in result["events"]


def test_side_alignment_event_and_explanation():
    candles = _base_with_ref() + [_c(4.0, 12.0, -1.0, 8.0)]

    aligned = crt.analyze_crt(candles, side="LONG")
    conflict = crt.analyze_crt(candles, side="SHORT")

    assert "SIDE_ALIGNED" in aligned["events"]
    assert "supports LONG" in aligned["explanation"]
    assert "SIDE_CONFLICT" in conflict["events"]
    assert "conflicts with SHORT" in conflict["explanation"]


def test_bad_or_corrupt_candles_do_not_raise():
    result = crt.analyze_crt([None, "bad", {"o": "x", "h": 1, "l": 1, "c": 1}], side=object())

    assert result["status"] == "INSUFFICIENT_DATA"
    assert "errors" in result


def test_input_candles_are_not_mutated():
    candles = [
        {"o": 10, "h": 11, "l": 9.8, "c": 10.5, "v": 100, "ts": 1},
        {"o": 10.5, "h": 11.2, "l": 10.2, "c": 10.9, "v": 100, "ts": 2},
        {"o": 10.9, "h": 11.5, "l": 10.6, "c": 11.1, "v": 100, "ts": 3},
        {"o": 10, "h": 20, "l": 0, "c": 18, "v": 100, "ts": 4},
        {"o": 18, "h": 19, "l": 17, "c": 18.2, "v": 100, "ts": 5},
        {"o": 4.0, "h": 12.0, "l": -1.0, "c": 8.0, "v": 100, "ts": 6},
    ]
    before = copy.deepcopy(candles)

    crt.analyze_crt(candles)

    assert candles == before


def test_forbidden_imports_are_absent():
    source_path = Path(__file__).resolve().parents[1] / "novo_crt_engine.py"
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

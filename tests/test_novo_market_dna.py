from __future__ import annotations

import ast
import copy
from pathlib import Path

import novo_market_dna as dna


def _c(open_, high, low, close, volume=100.0):
    return {"open": open_, "high": high, "low": low, "close": close, "volume": volume}


def test_insufficient_data_returns_safe_result():
    result = dna.analyze_market_dna([_c(1, 2, 0.5, 1.5)], side="LONG", timeframe="15m")

    assert result["status"] == "INSUFFICIENT_DATA"
    assert result["bias"] == "NEUTRAL"
    assert result["score"] == 0
    assert result["dna"]["volatility_state"] == "UNKNOWN"


def test_volatility_expansion_detection():
    candles = [
        _c(10, 10.5, 9.8, 10.2, 100),
        _c(10.2, 10.7, 10.0, 10.5, 100),
        _c(10.5, 11.0, 10.3, 10.8, 100),
        _c(10.8, 11.3, 10.6, 11.0, 100),
        _c(11.0, 13.2, 10.8, 12.8, 120),
        _c(12.8, 15.4, 12.6, 15.1, 130),
        _c(15.1, 18.0, 14.9, 17.7, 140),
        _c(17.7, 21.0, 17.5, 20.6, 150),
    ]

    result = dna.analyze_market_dna(candles)

    assert result["status"] == "OK"
    assert result["dna"]["volatility_state"] == "EXPANDING"
    assert "VOLATILITY_EXPANDING" in result["events"]


def test_compression_detection():
    candles = [
        _c(10, 13, 9, 12, 100),
        _c(12, 15, 11, 14, 100),
        _c(14, 17, 13, 16, 100),
        _c(16, 19, 15, 18, 100),
        _c(18, 18.6, 17.8, 18.2, 100),
        _c(18.2, 18.7, 18.0, 18.4, 100),
        _c(18.4, 18.9, 18.2, 18.5, 100),
        _c(18.5, 19.0, 18.4, 18.7, 100),
    ]

    result = dna.analyze_market_dna(candles)

    assert result["dna"]["volatility_state"] == "COMPRESSING"
    assert "VOLATILITY_COMPRESSING" in result["events"]
    assert result["score"] <= 55


def test_volume_expansion_detection():
    candles = [
        _c(10, 11, 9.8, 10.6, 100),
        _c(10.6, 11.2, 10.4, 10.9, 100),
        _c(10.9, 11.5, 10.7, 11.3, 100),
        _c(11.3, 11.8, 11.1, 11.6, 100),
        _c(11.6, 12.2, 11.4, 12.0, 180),
        _c(12.0, 12.8, 11.8, 12.5, 200),
        _c(12.5, 13.4, 12.3, 13.1, 220),
        _c(13.1, 14.0, 12.9, 13.8, 240),
    ]

    result = dna.analyze_market_dna(candles)

    assert result["dna"]["volume_state"] == "EXPANDING"
    assert "VOLUME_EXPANDING" in result["events"]


def test_volume_drying_detection():
    candles = [
        _c(10, 11, 9.8, 10.6, 300),
        _c(10.6, 11.2, 10.4, 10.9, 300),
        _c(10.9, 11.5, 10.7, 11.3, 300),
        _c(11.3, 11.8, 11.1, 11.6, 300),
        _c(11.6, 12.0, 11.4, 11.8, 90),
        _c(11.8, 12.1, 11.6, 11.9, 80),
        _c(11.9, 12.2, 11.7, 12.0, 70),
        _c(12.0, 12.3, 11.8, 12.1, 60),
    ]

    result = dna.analyze_market_dna(candles)

    assert result["dna"]["volume_state"] == "DRYING"
    assert result["dna"]["risk_texture"] == "THIN"
    assert "VOLUME_DRYING" in result["events"]


def test_impulse_candle_detection():
    candles = [
        _c(10, 10.6, 9.9, 10.4, 100),
        _c(10.4, 11.0, 10.2, 10.8, 100),
        _c(10.8, 11.4, 10.6, 11.2, 100),
        _c(11.2, 11.8, 11.0, 11.6, 100),
        _c(11.6, 12.5, 11.4, 12.2, 130),
        _c(12.2, 13.4, 12.0, 13.1, 140),
        _c(13.1, 14.7, 12.9, 14.4, 150),
        _c(14.4, 18.0, 14.2, 17.8, 180),
    ]

    result = dna.analyze_market_dna(candles, side="LONG")

    assert result["dna"]["candle_state"] == "IMPULSE"
    assert "IMPULSE" in result["events"]
    assert result["bias"] == "LONG"


def test_wick_reversal_or_absorption_detection():
    candles = [
        _c(10, 11, 9.8, 10.7, 100),
        _c(10.7, 11.4, 10.5, 11.2, 100),
        _c(11.2, 11.8, 11.0, 11.5, 100),
        _c(11.5, 12.0, 11.3, 11.7, 100),
        _c(11.7, 12.3, 11.4, 12.0, 100),
        _c(12.0, 12.5, 11.7, 12.2, 100),
        _c(12.2, 12.8, 12.0, 12.5, 100),
        _c(12.0, 13.5, 11.9, 12.1, 120),
    ]

    result = dna.analyze_market_dna(candles)

    assert result["dna"]["candle_state"] in {"REVERSAL", "ABSORPTION"}
    assert result["dna"]["risk_texture"] in {"WICKY", "NOISY", "CLEAN"}


def test_choppy_noisy_classification():
    candles = [
        _c(10.0, 11.0, 9.0, 10.8, 100),
        _c(10.8, 11.1, 9.1, 9.4, 100),
        _c(9.4, 11.2, 9.2, 10.7, 100),
        _c(10.7, 11.0, 9.0, 9.3, 100),
        _c(9.3, 11.1, 9.1, 10.6, 100),
        _c(10.6, 11.2, 9.2, 9.5, 100),
        _c(9.5, 11.0, 9.0, 10.5, 100),
        _c(10.5, 11.1, 9.1, 9.6, 100),
    ]

    result = dna.analyze_market_dna(candles)

    assert result["dna"]["trend_state"] == "CHOPPY"
    assert result["dna"]["risk_texture"] == "NOISY"
    assert result["score"] <= 60


def test_side_alignment_explanation_and_event():
    candles = [
        _c(10, 10.6, 9.9, 10.4, 100),
        _c(10.4, 11.0, 10.2, 10.8, 100),
        _c(10.8, 11.4, 10.6, 11.2, 100),
        _c(11.2, 11.8, 11.0, 11.6, 100),
        _c(11.6, 12.5, 11.4, 12.2, 130),
        _c(12.2, 13.4, 12.0, 13.1, 140),
        _c(13.1, 14.7, 12.9, 14.4, 150),
        _c(14.4, 18.0, 14.2, 17.8, 180),
    ]

    aligned = dna.analyze_market_dna(candles, side="LONG")
    conflict = dna.analyze_market_dna(candles, side="SHORT")

    assert "SIDE_ALIGNED" in aligned["events"]
    assert "supports LONG" in aligned["explanation"]
    assert "SIDE_CONFLICT" in conflict["events"]
    assert "conflicts with SHORT" in conflict["explanation"]


def test_bad_or_corrupt_candles_do_not_raise():
    result = dna.analyze_market_dna([None, "bad", {"o": "x", "h": 1, "l": 1, "c": 1}], side=object())

    assert result["status"] == "INSUFFICIENT_DATA"
    assert "errors" in result


def test_input_candles_are_not_mutated():
    candles = [
        {"o": 10, "h": 10.6, "l": 9.9, "c": 10.4, "v": 100, "ts": 1},
        {"o": 10.4, "h": 11.0, "l": 10.2, "c": 10.8, "v": 100, "ts": 2},
        {"o": 10.8, "h": 11.4, "l": 10.6, "c": 11.2, "v": 100, "ts": 3},
        {"o": 11.2, "h": 11.8, "l": 11.0, "c": 11.6, "v": 100, "ts": 4},
        {"o": 11.6, "h": 12.5, "l": 11.4, "c": 12.2, "v": 130, "ts": 5},
        {"o": 12.2, "h": 13.4, "l": 12.0, "c": 13.1, "v": 140, "ts": 6},
        {"o": 13.1, "h": 14.7, "l": 12.9, "c": 14.4, "v": 150, "ts": 7},
        {"o": 14.4, "h": 18.0, "l": 14.2, "c": 17.8, "v": 180, "ts": 8},
    ]
    before = copy.deepcopy(candles)

    dna.analyze_market_dna(candles)

    assert candles == before


def test_forbidden_imports_are_absent():
    source_path = Path(__file__).resolve().parents[1] / "novo_market_dna.py"
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


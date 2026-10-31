from __future__ import annotations

import ast
import copy
from datetime import datetime, timezone
from pathlib import Path

import novo_session_model as sm


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
    result = sm.analyze_session_model([_c(1, 2, 0.5, 1.5)], side="LONG", now="2026-06-01T08:00:00Z")

    assert result["status"] == "INSUFFICIENT_DATA"
    assert result["active_session"] == "LONDON"
    assert result["bias"] == "NEUTRAL"
    assert result["score"] == 0


def test_active_session_classification():
    candles = _base()

    assert sm.analyze_session_model(candles, now="2026-06-01T01:00:00Z")["active_session"] == "ASIA"
    assert sm.analyze_session_model(candles, now="2026-06-01T08:00:00Z")["active_session"] == "LONDON"
    assert sm.analyze_session_model(candles, now=datetime(2026, 6, 1, 14, tzinfo=timezone.utc))["active_session"] == "NEW_YORK"
    assert sm.analyze_session_model(candles, now="2026-06-01T22:00:00+00:00")["active_session"] == "OFF_HOURS"


def test_asia_range_detection():
    result = sm.analyze_session_model(_base(), now="2026-06-01T02:00:00Z", session_levels={"asia_high": 12.0, "asia_low": 9.0})

    assert result["session_state"] == "ASIA_RANGE"
    assert result["bias"] == "NEUTRAL"
    assert "ASIA_RANGE" in result["events"]


def test_london_sweep_low_gives_long_bias():
    candles = _base() + [_c(10.4, 11.4, 8.7, 9.7)]

    result = sm.analyze_session_model(candles, now="2026-06-01T08:00:00Z", session_levels={"asia_high": 12.0, "asia_low": 9.0})

    assert result["session_state"] == "LONDON_SWEEP_LOW"
    assert result["bias"] == "LONG"
    assert "LONDON_SWEEP_LOW" in result["events"]


def test_london_sweep_high_gives_short_bias():
    candles = _base() + [_c(11.4, 12.4, 10.8, 11.7)]

    result = sm.analyze_session_model(candles, now="2026-06-01T08:00:00Z", session_levels={"asia_high": 12.0, "asia_low": 9.0})

    assert result["session_state"] == "LONDON_SWEEP_HIGH"
    assert result["bias"] == "SHORT"
    assert "LONDON_SWEEP_HIGH" in result["events"]


def test_new_york_sweep_low_gives_long_bias():
    candles = _base() + [_c(10.0, 11.0, 8.2, 9.4)]

    result = sm.analyze_session_model(
        candles,
        now="2026-06-01T15:00:00Z",
        session_levels={"asia_high": 12.0, "asia_low": 9.0, "london_high": 12.5, "london_low": 8.8},
    )

    assert result["session_state"] == "NY_SWEEP_LOW"
    assert result["bias"] == "LONG"
    assert "NY_SWEEP_LOW" in result["events"]


def test_new_york_sweep_high_gives_short_bias():
    candles = _base() + [_c(11.4, 12.8, 10.8, 12.1)]

    result = sm.analyze_session_model(
        candles,
        now="2026-06-01T15:00:00Z",
        session_levels={"asia_high": 12.0, "asia_low": 9.0, "london_high": 12.5, "london_low": 8.8},
    )

    assert result["session_state"] == "NY_SWEEP_HIGH"
    assert result["bias"] == "SHORT"
    assert "NY_SWEEP_HIGH" in result["events"]


def test_session_expansion_up_gives_long_bias():
    candles = _base() + [_c(12.0, 13.0, 11.8, 12.6)]

    result = sm.analyze_session_model(candles, now="2026-06-01T08:00:00Z", session_levels={"asia_high": 12.0, "asia_low": 9.0})

    assert result["session_state"] == "SESSION_EXPANSION_UP"
    assert result["bias"] == "LONG"
    assert "SESSION_EXPANSION_UP" in result["events"]


def test_session_expansion_down_gives_short_bias():
    candles = _base() + [_c(9.0, 9.4, 8.0, 8.5)]

    result = sm.analyze_session_model(candles, now="2026-06-01T08:00:00Z", session_levels={"asia_high": 12.0, "asia_low": 9.0})

    assert result["session_state"] == "SESSION_EXPANSION_DOWN"
    assert result["bias"] == "SHORT"
    assert "SESSION_EXPANSION_DOWN" in result["events"]


def test_provided_session_levels_are_respected():
    candles = _base() + [_c(10.4, 11.4, 8.7, 9.7)]
    levels = {"asia_high": 50.0, "asia_low": 9.0}

    result = sm.analyze_session_model(candles, now="2026-06-01T08:00:00Z", session_levels=levels)

    assert result["levels"]["asia_high"] == 50.0
    assert result["levels"]["asia_low"] == 9.0
    assert result["session_state"] == "LONDON_SWEEP_LOW"


def test_side_alignment_event_and_explanation():
    candles = _base() + [_c(10.4, 11.4, 8.7, 9.7)]

    aligned = sm.analyze_session_model(candles, side="LONG", now="2026-06-01T08:00:00Z", session_levels={"asia_high": 12.0, "asia_low": 9.0})
    conflict = sm.analyze_session_model(candles, side="SHORT", now="2026-06-01T08:00:00Z", session_levels={"asia_high": 12.0, "asia_low": 9.0})

    assert "SIDE_ALIGNED" in aligned["events"]
    assert "supports LONG" in aligned["explanation"]
    assert "SIDE_CONFLICT" in conflict["events"]
    assert "conflicts with SHORT" in conflict["explanation"]


def test_bad_or_corrupt_candles_do_not_raise():
    result = sm.analyze_session_model([None, "bad", {"o": "x", "h": 1, "l": 1, "c": 1}], side=object(), now="bad-date")

    assert result["status"] in {"INSUFFICIENT_DATA", "ERROR"}
    assert "errors" in result


def test_input_candles_and_session_levels_are_not_mutated():
    candles = [
        {"o": 10, "h": 11, "l": 9, "c": 10.5, "v": 100, "ts": 1},
        {"o": 10.5, "h": 12, "l": 9.5, "c": 11, "v": 100, "ts": 2},
        {"o": 11, "h": 11.8, "l": 9.8, "c": 10.8, "v": 100, "ts": 3},
        {"o": 10.8, "h": 11.7, "l": 10.0, "c": 11.1, "v": 100, "ts": 4},
        {"o": 11.1, "h": 11.6, "l": 10.2, "c": 11.3, "v": 100, "ts": 5},
        {"o": 10.4, "h": 11.4, "l": 8.7, "c": 9.7, "v": 100, "ts": 6},
    ]
    levels = {"asia_high": 12.0, "asia_low": 9.0}
    before_candles = copy.deepcopy(candles)
    before_levels = copy.deepcopy(levels)

    sm.analyze_session_model(candles, now="2026-06-01T08:00:00Z", session_levels=levels)

    assert candles == before_candles
    assert levels == before_levels


def test_forbidden_imports_are_absent():
    source_path = Path(__file__).resolve().parents[1] / "novo_session_model.py"
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
        "session_context",
    }
    assert imported.isdisjoint(forbidden)


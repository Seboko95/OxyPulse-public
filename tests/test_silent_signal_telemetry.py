from __future__ import annotations

import ast
from pathlib import Path

import pytest

import category_engine as ce
telemetry = pytest.importorskip("silent_signal_telemetry_report")


class SettingsStub:
    roles = {
        "RAIJIN": "hunter",
        "HACHIMAN": "selector",
        "IZANAGI": "shadow_advisor",
        "KAGUTSUCHI": "confirmer",
    }
    modes = {
        "RAIJIN": "live",
        "HACHIMAN": "live",
        "IZANAGI": "live",
        "KAGUTSUCHI": "shadow",
    }
    values = {
        "min_vote_score": 3.0,
        "raijin_min_votes": 3.0,
        "hachiman_min_votes": 2.0,
        "izanagi_min_votes": 2.0,
        "vote_rescue_max_vote_gap": 1.0,
        "vote_rescue_live_opener_roles": "hunter,specialist,confirmer,booster",
    }

    @classmethod
    def get_engine_role(cls, engine):
        return cls.roles.get(str(engine).upper(), "hunter")

    @classmethod
    def get_engine_mode(cls, engine):
        return cls.modes.get(str(engine).upper(), "live")

    @classmethod
    def get_val(cls, key, default=None):
        return cls.values.get(key, default)


def _sample_log() -> str:
    return """
2026-06-09 13:29:10,906 | INFO | OxyPulse.WSStream | WebSocket stream active for 257 coins.
2026-06-09 13:30:01,000 | INFO | OxyPulse.CategoryEngine | [RAIJIN] Signal LONG DYDX votes=2.0
2026-06-09 13:30:02,000 | INFO | OxyPulse.CategoryEngine | [HACHIMAN] Signal SHORT HBAR votes=3.0
2026-06-09 13:30:02,010 | INFO | OxyPulse.CategoryEngine | ROLE CONTRIBUTION [HACHIMAN] HBAR SHORT role=selector q=185.57 strength=9.93 | selector contribution only
2026-06-09 13:30:03,000 | INFO | OxyPulse.CategoryEngine | ENGINE SCAN SUMMARY [RAIJIN] evaluated=18 lane_override=0 errors=0 elapsed=0.99s
2026-06-09 13:30:03,001 | INFO | OxyPulse.CategoryEngine | ENGINE SCAN SUMMARY [HACHIMAN] evaluated=10 lane_override=0 errors=0 elapsed=0.58s
2026-06-09 13:30:03,002 | INFO | OxyPulse.CategoryEngine | ENGINE SCAN SUMMARY [IZANAGI] evaluated=18 lane_override=0 errors=0 elapsed=0.88s
2026-06-09 13:30:03,003 | INFO | OxyPulse.Engine | Scan cycle complete: universe=257 engines=28 duration=8.67s timeouts=0 errors=0
"""


def test_report_handles_missing_logs_safely(tmp_path):
    summary = telemetry.parse_silent_signal_telemetry("", settings_module=SettingsStub)
    out = telemetry.write_report(summary, tmp_path / "silent.md")

    assert summary["total_evaluations"] == 0
    assert summary["total_signals"] == 0
    assert out.exists()


def test_per_engine_counters_calculate_correctly():
    summary = telemetry.parse_silent_signal_telemetry(_sample_log(), settings_module=SettingsStub)

    assert summary["total_evaluations"] == 46
    assert summary["total_signals"] == 2
    assert summary["per_engine"]["RAIJIN"]["evaluations"] == 18
    assert summary["per_engine"]["RAIJIN"]["signal_count"] == 1
    assert summary["per_engine"]["RAIJIN"]["long_count"] == 1
    assert summary["per_engine"]["RAIJIN"]["near_vote_floor"] == 1
    assert summary["per_engine"]["RAIJIN"]["vote_rescue_eligible"] == 1
    assert summary["per_engine"]["HACHIMAN"]["selector_only_contribution"] == 1


def test_unknown_silent_returns_are_counted():
    summary = telemetry.parse_silent_signal_telemetry(_sample_log(), settings_module=SettingsStub)

    assert summary["drop_reasons"]["unknown_silent_return"] > 0
    assert summary["per_engine"]["IZANAGI"]["hold_returns"] == 18


def test_runtime_counters_get_and_reset_work():
    ce.reset_silent_signal_diagnostics()

    ce._bump_silent_signal_counter("raw_hold_return", engine="RAIJIN", symbol="DYDX", role="hunter")
    ce._bump_silent_signal_counter("votes_below_floor", engine="RAIJIN", symbol="DYDX", role="hunter")
    ce._bump_silent_signal_counter("selector_contribution_only", engine="HACHIMAN", symbol="HBAR", role="selector")
    diag = ce.get_silent_signal_diagnostics()

    assert diag["total"] == 3
    assert diag["reasons"]["raw_hold_return"] == 1
    assert diag["reasons"]["votes_below_floor"] == 1
    assert diag["reasons"]["selector_contribution_only"] == 1
    assert diag["by_engine"]["RAIJIN"]["reasons"]["raw_hold_return"] == 1

    ce.reset_silent_signal_diagnostics()
    assert ce.get_silent_signal_diagnostics()["total"] == 0


def test_signal_funnel_counters_survive_reload_and_rescue_is_monotonic(monkeypatch, tmp_path):
    path = tmp_path / "signal_funnel.json"
    monkeypatch.setattr(ce, "_SIGNAL_FUNNEL_COUNTER_PATH", path)
    ce.reset_silent_signal_diagnostics()
    ce._bump_silent_signal_counter("vote_rescue_eligible", engine="RYUU")
    ce._bump_silent_signal_counter("vote_rescue_success", engine="RYUU")
    before = ce.get_silent_signal_diagnostics()
    ce._silent_signal_diagnostics["total"] = 0
    ce._silent_signal_diagnostics["reasons"] = {reason: 0 for reason in ce._SILENT_SIGNAL_REASONS}
    ce._load_signal_funnel_counters()
    after = ce.get_silent_signal_diagnostics()
    assert after["reasons"]["vote_rescue_eligible"] == before["reasons"]["vote_rescue_eligible"]
    assert after["reasons"]["vote_rescue_success"] <= after["reasons"]["vote_rescue_eligible"]


def test_spread_liquidity_block_diagnostics_record_and_bound():
    ce._spread_liquidity_block_diagnostics.clear()

    for idx in range(305):
        ce._record_spread_liquidity_block(
            symbol=f"SYM{idx}",
            engine="RAIJIN",
            side="LONG",
            sub_reason="spread_too_wide",
            ticker={"bid": "1.0", "ask": "1.01", "last": "1.0", "volCcy24h": "12345"},
            spread_threshold=0.3,
            volume_threshold=100000,
            market_regime="TRENDING",
            role="hunter",
        )

    diag = ce.get_spread_liquidity_block_diagnostics(limit=500)

    assert diag["status"] == "OK"
    assert diag["total_recent"] == 300
    assert diag["limit"] == 300
    assert len(diag["records"]) == 300
    assert diag["records"][-1]["symbol"] == "SYM304"
    assert diag["records"][-1]["sub_reason"] == "spread_too_wide"
    assert diag["records"][-1]["spread_pct"] is not None


def test_spread_liquidity_accessor_is_read_only():
    ce._spread_liquidity_block_diagnostics.clear()
    ce._record_spread_liquidity_block(
        symbol="APT",
        engine="RYUJIN",
        side="SHORT",
        sub_reason="volume_too_low",
        volume_24h=50,
        volume_threshold=100,
        role="hunter",
    )

    snap = ce.get_spread_liquidity_block_diagnostics(limit=1)
    snap["records"][0]["symbol"] = "MUTATED"
    fresh = ce.get_spread_liquidity_block_diagnostics(limit=1)

    assert fresh["records"][0]["symbol"] == "APT"


def test_report_uses_runtime_counters_when_available():
    runtime = {
        "total": 3,
        "reasons": {
            "raw_hold_return": 2,
            "votes_below_floor": 1,
            "unknown_exception": 0,
        },
        "by_engine": {},
        "recent": [],
    }

    summary = telemetry.parse_silent_signal_telemetry(
        _sample_log(),
        settings_module=SettingsStub,
        runtime_diagnostics=runtime,
    )

    assert summary["runtime_counter_total"] == 3
    assert summary["runtime_reason_counts"]["raw_hold_return"] == 2
    assert summary["drop_reasons"]["raw_hold_return"] == 2
    assert summary["drop_reasons"]["unknown_silent_return"] < 44


def test_report_includes_spread_liquidity_details(tmp_path):
    summary = telemetry.parse_silent_signal_telemetry(
        _sample_log(),
        settings_module=SettingsStub,
        runtime_diagnostics={},
        spread_liquidity_diagnostics={
            "status": "OK",
            "total_recent": 1,
            "records": [
                {
                    "symbol": "APT",
                    "engine": "RYUJIN",
                    "side": "SHORT",
                    "sub_reason": "spread_too_wide",
                    "spread_pct": 0.41,
                    "spread_threshold": 0.3,
                    "volume_24h": 5000000,
                    "volume_threshold": 2000000,
                    "market_regime": "TRENDING",
                    "role": "hunter",
                }
            ],
        },
    )
    path = telemetry.write_report(summary, tmp_path / "spread.md")
    text = path.read_text(encoding="utf-8")

    assert "Spread/Liquidity Block Details" in text
    assert "APT" in text
    assert "spread_too_wide" in text


def test_report_falls_back_when_runtime_counters_unavailable():
    summary = telemetry.parse_silent_signal_telemetry(
        _sample_log(),
        settings_module=SettingsStub,
        runtime_diagnostics={},
    )

    assert summary["runtime_counter_total"] == 0
    assert summary["drop_reasons"]["unknown_silent_return"] > 0


def test_report_output_contains_required_sections(tmp_path):
    summary = telemetry.parse_silent_signal_telemetry(_sample_log(), settings_module=SettingsStub)
    path = telemetry.write_report(summary, tmp_path / "report.md")
    text = path.read_text(encoding="utf-8")

    assert "Total evaluations" in text
    assert "Per-Engine Summary" in text
    assert "Top Silent-Drop Reasons" in text
    assert "Vote Rescue eligible" in text


def test_no_execution_risk_sizing_exit_imports():
    source_path = Path(__file__).resolve().parents[1] / "silent_signal_telemetry_report.py"
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
        "risk_manager",
        "sizing_engine",
        "exit_manager",
        "position_registry",
        "okx_client",
        "trading_engine",
    }
    assert imported.isdisjoint(forbidden)


def test_report_script_has_no_trading_behavior_tokens():
    source = Path("silent_signal_telemetry_report.py").read_text(encoding="utf-8")
    forbidden = (
        "place_market_order",
        "start_all(",
        "stop_all(",
        "set_val(",
        "set_engine_mode",
        "register(",
        "release(",
        "record_trade(",
    )
    for token in forbidden:
        assert token not in source

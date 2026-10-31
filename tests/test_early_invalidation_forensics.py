from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest

eif = pytest.importorskip("early_invalidation_forensics")


def _trade(
    *,
    engine="ENMA",
    coin="SUI",
    direction="short",
    pnl=-0.2,
    fee=0.04,
    reason="early_invalidation",
    entry=1.0,
    exit_price=1.001,
    opened="2026-06-08T00:00:00+00:00",
    closed="2026-06-08T00:05:00+00:00",
) -> dict:
    return {
        "coin": coin,
        "direction": direction,
        "engine": engine,
        "entry": entry,
        "exit_price": exit_price,
        "pnl_usdt": pnl,
        "fee_usdt": fee,
        "exit_reason": reason,
        "opened_at": opened,
        "closed_at": closed,
        "holding_minutes": 5,
    }


def _outcome(
    *,
    engine="ENMA",
    symbol="SUI",
    side="SHORT",
    opened="2026-06-08T00:00:00+00:00",
    closed="2026-06-08T00:05:00+00:00",
    mfe=3.0,
    mae=-1.0,
    tier="STRONG",
    pnl=-0.2,
) -> dict:
    return {
        "record_type": "novo_enrichment_outcome",
        "engine": engine,
        "symbol": symbol,
        "side": side,
        "opened_at": opened,
        "closed_at": closed,
        "mfe": mfe,
        "mae": mae,
        "quality_tier": tier,
        "net_pnl": pnl,
    }


def test_analyze_engine_rankings_and_exit_breakdown(tmp_path):
    category = {
        "ENMA": {"trades": [_trade(engine="ENMA", pnl=1.0, reason="tp_exchange")]},
        "RYUJIN": {"trades": [_trade(engine="RYUJIN", pnl=-0.5), _trade(engine="RYUJIN", pnl=-0.25)]},
    }

    result = eif.analyze({}, category, [], log_path=tmp_path / "missing.log")

    assert result["closed_trade_count"] == 3
    assert result["engine_pnl"][0][0] == "RYUJIN"
    assert dict(result["exit_reason_breakdown"])["early_invalidation"] == 2
    assert result["early_invalidation_frequency"][0][0] == "RYUJIN"


def test_mfe_positive_loser_and_quality_tier_are_attributed(tmp_path):
    category = {"ENMA": {"trades": [_trade()]}}
    novo = [_outcome()]

    result = eif.analyze({}, category, novo, log_path=tmp_path / "missing.log")
    row = result["rows"][0]

    assert row["quality_tier"] == "STRONG"
    assert row["mfe"] == 3.0
    assert row["mae"] == -1.0
    assert row["ever_positive"] is True
    assert result["positive_but_lost_count"] == 1
    assert result["mfe_realized"]["mfe_positive_lost_count"] == 1


def test_tp1_distance_and_nearly_reached_uses_log_exit_plan(tmp_path):
    log = tmp_path / "oxypulse.log"
    log.write_text(
        "2026-06-08 00:00:01 | INFO | OxyPulse.CategoryEngine | "
        "EXIT_PLAN_CREATED [ENMA] SUI SHORT order=abc SL=1.10 TP1=0.90000000 TP2=0.80000000\n",
        encoding="utf-8",
    )
    category = {
        "ENMA": {
            "trades": [
                _trade(direction="short", entry=1.0, exit_price=0.91, pnl=-0.01),
            ]
        }
    }

    result = eif.analyze({}, category, [], log_path=log)
    row = result["rows"][0]

    assert row["tp1_nearly_reached"] is True
    assert row["distance_from_tp1"] == 1.0
    assert result["almost_tp1_count"] == 1


def test_entry_quality_verdict_for_fast_losers_without_mfe(tmp_path):
    category = {
        "HAYATE": {
            "trades": [
                _trade(engine="HAYATE", coin=f"SYM{i}", pnl=-0.1, reason="early_invalidation")
                for i in range(6)
            ]
        }
    }

    result = eif.analyze({}, category, [], log_path=tmp_path / "missing.log")

    assert result["verdict"] in {"ENTRY_QUALITY_PROBLEM", "MIXED"}
    assert result["died_before_meaningful_profit_count"] == 6


def test_build_report_contains_required_sections(tmp_path):
    result = eif.analyze(
        {},
        {"ENMA": {"trades": [_trade()]}},
        [_outcome()],
        log_path=tmp_path / "missing.log",
    )

    report = eif.build_report(result, ["sample warning"])

    assert "# Early Invalidation Forensics" in report
    assert "Engine Ranking By Net PnL" in report
    assert "Engine Ranking By Early Invalidation Frequency" in report
    assert "MFE Vs Realized PnL" in report
    assert "Top Recurring Loss Patterns" in report
    assert "sample warning" in report


def test_write_reports_creates_latest_and_timestamped(tmp_path):
    latest, timestamped = eif.write_reports(
        "report\n",
        report_dir=tmp_path,
        now=datetime(2026, 6, 8, 0, 0, tzinfo=timezone.utc),
    )

    assert latest.name == "early_invalidation_forensics_latest.md"
    assert timestamped.name == "early_invalidation_forensics_20260608_000000.md"
    assert latest.read_text(encoding="utf-8") == "report\n"


def test_load_inputs_handles_missing_and_malformed_jsonl(tmp_path):
    perf = tmp_path / "performance.json"
    cat = tmp_path / "category_stats.json"
    mem = tmp_path / "memory.jsonl"
    perf.write_text(json.dumps({"trades": []}), encoding="utf-8")
    cat.write_text(json.dumps({}), encoding="utf-8")
    mem.write_text("not-json\n" + json.dumps(_outcome()) + "\n", encoding="utf-8")

    performance, category, records, errors = eif.load_inputs(perf, cat, mem)

    assert performance == {"trades": []}
    assert category == {}
    assert len(records) == 1
    assert errors


def test_verdict_is_known_value(tmp_path):
    result = eif.analyze(
        {},
        {"ENMA": {"trades": [_trade(pnl=-0.1), _trade(coin="APE", pnl=0.2, reason="tp_exchange")]}},
        [_outcome()],
        log_path=tmp_path / "missing.log",
    )

    assert result["verdict"] in eif.VERDICTS

from __future__ import annotations

import json
from pathlib import Path

import pytest

report = pytest.importorskip("report_novo_evidence")


def test_missing_memory_file_is_safe(tmp_path):
    text, metrics = report.build_report(
        memory_path=tmp_path / "missing.jsonl",
        performance_path=tmp_path / "missing_performance.json",
    )

    assert metrics["readiness"] == "NOT_READY"
    assert metrics["candidate_threshold"] == "NOT_READY"
    assert metrics["outcome_threshold"] == "OUTCOME_NOT_READY"
    assert metrics["ready_for_review"] is False
    assert metrics["ready_for_small_live_test"] is False
    assert metrics["candidate_count"] == 0
    assert metrics["outcome_count"] == 0
    assert "memory file missing" in text
    assert "Novo remains advisory/dry-run" in text
    assert "Telegram Summary" in text


def test_candidate_and_outcome_summary(tmp_path):
    memory = tmp_path / "novo_trade_memory.jsonl"
    records = [
        {
            "record_type": "novo_candidate_observation",
            "timestamp": "2026-06-03T00:00:00+00:00",
            "symbol": "TON",
            "side": "LONG",
            "engine": "IZANAGI",
            "quality_score": 91,
            "quality_tier": "ELITE",
            "advisory_only": True,
            "dry_run": True,
            "no_hard_blocks": True,
        },
        {
            "record_type": "novo_candidate_observation",
            "timestamp": "2026-06-03T00:01:00+00:00",
            "symbol": "TRX",
            "side": "SHORT",
            "engine": "RONIN",
            "quality_score": 55,
            "quality_tier": "WEAK",
            "advisory_only": True,
            "dry_run": True,
            "no_hard_blocks": True,
        },
        {
            "record_type": "novo_enrichment_outcome",
            "timestamp": "2026-06-03T00:02:00+00:00",
            "symbol": "TON",
            "engine": "IZANAGI",
            "quality_tier": "ELITE",
            "net_pnl": 1.25,
        },
    ]
    memory.write_text("\n".join(json.dumps(r) for r in records) + "\n", encoding="utf-8")
    performance = tmp_path / "performance.json"
    performance.write_text(json.dumps({"closed_trades": []}), encoding="utf-8")

    text, metrics = report.build_report(memory_path=memory, performance_path=performance)

    assert metrics["candidate_count"] == 2
    assert metrics["outcome_count"] == 1
    assert metrics["wins"] == 1
    assert metrics["readiness"] == "NOT_READY"
    assert metrics["candidate_threshold"] == "NOT_READY"
    assert metrics["outcome_threshold"] == "OUTCOME_NOT_READY"
    assert "TON" in text
    assert "IZANAGI" in text


def test_threshold_logic_candidate_and_outcome_states():
    assert report._candidate_threshold(0) == "NOT_READY"
    assert report._candidate_threshold(99) == "NOT_READY"
    assert report._candidate_threshold(100) == "EARLY"
    assert report._candidate_threshold(499) == "EARLY"
    assert report._candidate_threshold(500) == "CANDIDATE_READY"
    assert report._outcome_threshold(19) == "OUTCOME_NOT_READY"
    assert report._outcome_threshold(20) == "OUTCOME_READY"


def test_ready_for_review_requires_candidates_and_outcomes():
    state = report._readiness_state(500, 20, [])

    assert state["readiness"] == "READY_FOR_REVIEW"
    assert state["candidate_threshold"] == "CANDIDATE_READY"
    assert state["outcome_threshold"] == "OUTCOME_READY"
    assert state["ready_for_review"] is True
    assert state["ready_for_small_live_test"] is False


def test_small_live_test_requires_material_outperformance_and_sample_size():
    outcomes = []
    for idx in range(10):
        outcomes.append({"quality_tier": "STRONG", "_computed_pnl": 0.25 + idx * 0.01})
        outcomes.append({"quality_tier": "WEAK", "_computed_pnl": -0.05})

    state = report._readiness_state(500, 20, outcomes)

    assert state["readiness"] == "READY_FOR_SMALL_LIVE_TEST"
    assert state["ready_for_review"] is True
    assert state["ready_for_small_live_test"] is True
    assert state["outperformance"]["sample_size_enough"] is True
    assert state["outperformance"]["clearly_better"] is True


def test_telegram_summary_is_short():
    summary = report._telegram_summary(
        readiness="NOT_READY",
        candidate_threshold="NOT_READY",
        outcome_threshold="OUTCOME_NOT_READY",
        candidate_count=10,
        outcome_count=1,
        wins=0,
        losses=1,
        total_pnl=-0.5,
        avg_quality=64.3,
    )

    assert len(summary) <= 1200
    assert "Novo Evidence Update" in summary
    assert "Do not enable live influence" in summary


def test_write_reports_creates_latest_and_timestamped(tmp_path):
    latest, timestamped = report.write_reports("hello\n", report_dir=tmp_path)

    assert latest.name == "novo_evidence_latest.md"
    assert timestamped.name.startswith("novo_evidence_")
    assert latest.read_text(encoding="utf-8") == "hello\n"
    assert timestamped.read_text(encoding="utf-8") == "hello\n"

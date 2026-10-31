from __future__ import annotations

import json

import pytest

nta = pytest.importorskip("novo_tier_analysis")


def _candidate(tier: str, score: float, engine: str = "ENMA", symbol: str = "SUI") -> dict:
    return {
        "record_type": "novo_candidate_observation",
        "quality_tier": tier,
        "quality_score": score,
        "engine": engine,
        "symbol": symbol,
        "side": "SHORT",
        "dry_run": True,
        "advisory_only": True,
    }


def _outcome(tier: str, score: float, pnl: float, fee: float = 0.01) -> dict:
    return {
        "record_type": "novo_enrichment_outcome",
        "quality_tier": tier,
        "quality_score": score,
        "net_pnl": pnl,
        "actual_fee": fee,
        "hold_time_seconds": 60,
        "mfe": 2.0,
        "mae": -1.0,
    }


def test_missing_memory_is_safe(tmp_path):
    records, errors = nta._read_jsonl(tmp_path / "missing.jsonl")
    report, result = nta.build_report(records, errors)

    assert records == []
    assert errors
    assert result["recommendation"] == "NOT_READY"
    assert result["correlation"]["result"] == "INSUFFICIENT_DATA"
    assert "Novo Tier Analysis Summary" in report


def test_tier_stats_include_observations_and_outcomes():
    records = [
        _candidate("STRONG", 85),
        _candidate("WEAK", 45),
        _outcome("STRONG", 85, 1.0),
        _outcome("STRONG", 82, -0.25),
        _outcome("WEAK", 45, -0.5),
    ]

    result = nta.analyze(records)

    strong = result["tier_stats"]["STRONG"]
    weak = result["tier_stats"]["WEAK"]
    assert strong["observations"] == 1
    assert strong["closed_outcomes"] == 2
    assert strong["win_count"] == 1
    assert strong["loss_count"] == 1
    assert strong["total_net_pnl"] == 0.75
    assert strong["profit_factor"] == 4.0
    assert weak["closed_outcomes"] == 1
    assert result["tier_ranking"][0] == "STRONG"


def test_no_classified_outcomes_is_unranked():
    records = [
        _candidate("STRONG", 85),
        {
            "record_type": "novo_enrichment_outcome",
            "quality_tier": "COLLECTING",
            "quality_score": None,
            "net_pnl": -0.5,
        },
    ]

    result = nta.analyze(records)
    text, _ = nta.build_report(records)

    assert result["tier_ranking"] == []
    assert "No tier has classified closed outcomes yet." in text


def test_correlation_labels_positive_negative_and_insufficient():
    insufficient = [_outcome("STRONG", 90, 1.0)]
    assert nta._correlation_label(insufficient)[0] == "INSUFFICIENT_DATA"

    positive = [_outcome("STRONG", 50 + i, -1.0 + i * 0.1) for i in range(25)]
    assert nta._correlation_label(positive)[0] == "STRONG_POSITIVE"

    negative = [_outcome("STRONG", 50 + i, 1.0 - i * 0.1) for i in range(25)]
    assert nta._correlation_label(negative)[0] == "NEGATIVE_CORRELATION"


def test_fee_bleed_guard_recommendation_when_review_ready_and_fee_drag_negative():
    records = [_candidate("WEAK", 40) for _ in range(500)]
    records.extend(_outcome("WEAK", 40, -0.2, fee=0.05) for _ in range(20))

    result = nta.analyze(records)

    assert result["fee_bleed_guard_justified"] is True
    assert result["recommendation"] == "READY_FOR_FEE_BLEED_GUARD_ONLY"


def test_small_live_test_recommendation_requires_good_sample_and_positive_correlation():
    records = [_candidate("STRONG", 85) for _ in range(500)]
    for i in range(20):
        records.append(_outcome("STRONG", 80 + i, 0.1 + i * 0.03))
    for i in range(20):
        records.append(_outcome("WEAK", 30 + i, -0.4 + i * 0.005))

    result = nta.analyze(records)

    assert result["correlation"]["result"] in {"WEAK_POSITIVE", "STRONG_POSITIVE"}
    assert result["recommendation"] == "READY_FOR_SMALL_LIVE_TEST"


def test_write_reports_creates_latest_and_timestamped(tmp_path):
    latest, timestamped = nta.write_reports("report\n", report_dir=tmp_path)

    assert latest.name == "novo_tier_analysis_latest.md"
    assert timestamped.name.startswith("novo_tier_analysis_")
    assert latest.read_text(encoding="utf-8") == "report\n"
    assert timestamped.read_text(encoding="utf-8") == "report\n"


def test_script_reads_jsonl_records(tmp_path):
    path = tmp_path / "memory.jsonl"
    path.write_text(json.dumps(_candidate("GOOD", 75)) + "\nnot-json\n", encoding="utf-8")

    records, errors = nta._read_jsonl(path)

    assert len(records) == 1
    assert errors

from __future__ import annotations

import json
from pathlib import Path

import pytest

rpt = pytest.importorskip("oxypulse_loss_fee_bleed_report")


def test_loss_fee_bleed_report_generates_from_temp_data(monkeypatch, tmp_path):
    perf = tmp_path / "performance.json"
    cat = tmp_path / "category_stats.json"
    pos = tmp_path / "positions.json"
    reg = tmp_path / "position_registry.json"
    mem = tmp_path / "novo_trade_memory.jsonl"
    out = tmp_path / "reports"
    perf.write_text(json.dumps({
        "trades": [
            {
                "coin": "SUI",
                "direction": "long",
                "entry": 1.0,
                "exit": 1.001,
                "size_usdt": 5.0,
                "pnl_usdt": 0.005,
                "fee_usdt": 0.01,
                "net_pnl_usdt": -0.005,
                "closed_at": "2026-06-08T00:00:00+00:00",
                "exit_reason": "milestone_tp1",
            },
            {
                "coin": "TON",
                "direction": "short",
                "entry": 1.0,
                "exit": 0.9,
                "size_usdt": 10.0,
                "pnl_usdt": 1.0,
                "fee_usdt": 0.02,
                "net_pnl_usdt": 0.98,
                "closed_at": "2026-06-08T00:10:00+00:00",
                "exit_reason": "tp_exchange",
            },
        ],
        "total_fees_usdt": 0.03,
    }), encoding="utf-8")
    cat.write_text(json.dumps({"ENMA": {"trades": []}}), encoding="utf-8")
    pos.write_text("{}", encoding="utf-8")
    reg.write_text("{}", encoding="utf-8")
    mem.write_text(json.dumps({"record_type": "novo_enrichment_outcome"}) + "\n", encoding="utf-8")
    monkeypatch.setattr(rpt, "PERFORMANCE_PATH", perf)
    monkeypatch.setattr(rpt, "CATEGORY_PATH", cat)
    monkeypatch.setattr(rpt, "POSITIONS_PATH", pos)
    monkeypatch.setattr(rpt, "REGISTRY_PATH", reg)
    monkeypatch.setattr(rpt, "NOVO_MEMORY_PATH", mem)

    text, metrics = rpt.build_report()
    latest, timestamped = rpt.write_report(text, report_dir=out)

    assert metrics["wins"] == 1
    assert metrics["losses"] == 1
    assert metrics["tiny_partials"] == 1
    assert "OxyPulse Loss And Fee-Bleed Report" in text
    assert latest.exists()
    assert timestamped.exists()

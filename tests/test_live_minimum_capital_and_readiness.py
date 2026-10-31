from pathlib import Path

import live_minimum_capital as capital
import readiness_analysis


def test_minimum_capital_uses_contract_metadata_and_risk_budget():
    row = capital.analyze_instrument(
        {"inst_id": "X-USDT-SWAP", "ct_val": 1, "min_sz": 1, "max_leverage": 20},
        price=1, leverage=10, taker_fee_rate=0.0005, stop_pct=1, risk_pct=1,
    )
    assert row["minimum_notional_usdt"] == 1
    assert row["isolated_margin_required_usdt"] == 0.1
    assert row["economic_minimum_equity_usdt"] >= 3.0


def test_software_floor_controls_economic_minimum():
    row = capital.analyze_instrument(
        {"inst_id": "X", "ct_val": .01, "min_sz": 1, "max_leverage": 20},
        price=.01, leverage=10, taker_fee_rate=.0005, stop_pct=1, risk_pct=1,
        software_min_notional_usdt=5,
    )
    assert row["economic_minimum_equity_usdt"] >= 5.0


def test_rank_candidates_orders_by_economic_floor():
    metas = [
        {"inst_id": "A", "ct_val": 1, "min_sz": 1, "max_leverage": 10, "state": "live"},
        {"inst_id": "B", "ct_val": 0.1, "min_sz": 1, "max_leverage": 10, "state": "live"},
    ]
    rows = capital.rank_candidates(metas, {"A": 10, "B": 1}, leverage=10, taker_fee_rate=.0005, stop_pct=1, risk_pct=1)
    assert [row["instrument"] for row in rows] == ["B", "A"]


def test_engine_audit_has_exactly_28_rows_and_no_contributor_pnl_double_count(tmp_path: Path):
    rows = readiness_analysis.engine_rows({})
    assert len(rows) == 28
    assert len({row["engine"] for row in rows}) == 28
    report = tmp_path / "engines.md"
    readiness_analysis.write_engine_report(report)
    assert "Historical paper evidence only" in report.read_text(encoding="utf-8")


def test_engine_log_parser_finds_disappearance_stage(tmp_path: Path):
    log = tmp_path / "oxypulse.log"
    log.write_text(
        "ENGINE SCAN SUMMARY [RYUU] evaluated=18 lane_override=0 errors=0 elapsed=1s\n"
        "[RYUU] Signal LONG BTC votes=2\n"
        "ACCEPT [RYUU] BTC LONG\n",
        encoding="utf-8",
    )
    rows = readiness_analysis.parse_engine_logs(tmp_path)
    assert rows["RYUU"] == {"evaluations": 18, "raw_signals": 1, "accepted": 1, "errors": 0}

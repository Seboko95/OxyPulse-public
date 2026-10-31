import json

import generate_live_operations_reports as reports


def test_live_reports_exclude_nonfinal_and_nonproduction_pnl(tmp_path, monkeypatch):
    data = tmp_path / "data"
    output = tmp_path / "reports"
    data.mkdir()
    (data / "order_intents.json").write_text(json.dumps({"intents": {}}))
    (data / "pnl_reconciliation.json").write_text(json.dumps({"records": {
        "paper": {"execution_environment": "paper", "exchange_final": True, "net_realized_pnl": 99},
        "pending": {"execution_environment": "production", "exchange_final": False, "net_realized_pnl": 5},
        "live": {"execution_environment": "production", "exchange_final": True, "net_realized_pnl": 1.25, "reconciliation_difference": 0},
    }}))
    monkeypatch.setattr(reports, "DATA", data)
    monkeypatch.setattr(reports, "REPORTS", output)

    result = reports.generate()

    assert result["exchange_final"] == 1
    text = (output / "live_trade_performance_latest.md").read_text()
    assert "`1`" in text
    assert "$+1.25000000" in text
    assert "historical paper results are intentionally excluded" in text

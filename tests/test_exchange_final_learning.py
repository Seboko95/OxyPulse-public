from pathlib import Path

import exchange_final_learning as learning
from pnl_reconciler import PnLReconciler


def test_learning_rejects_provisional_and_paper(tmp_path: Path):
    assert not learning.ingest({"pnl_status": "PROVISIONAL"}, model_path=tmp_path / "m.json", audit_path=tmp_path / "a.jsonl")["accepted"]
    row = {"exchange_final": True, "pnl_status": "RECONCILED", "exchange_realized_pnl": 1, "execution_environment": "paper"}
    assert not learning.ingest(row, model_path=tmp_path / "m.json", audit_path=tmp_path / "a.jsonl")["accepted"]


def test_reconciled_exchange_record_is_final_and_learnable(tmp_path: Path, monkeypatch):
    model = tmp_path / "model.json"
    audit = tmp_path / "audit.jsonl"
    monkeypatch.setattr(learning, "MODEL_PATH", model)
    monkeypatch.setattr(learning, "AUDIT_PATH", audit)
    rec = PnLReconciler(tmp_path / "pnl.json")
    rec.record_provisional("T1", engine="RYUU", execution_environment="production", local_estimated_net_pnl=1.0)
    row = rec.reconcile_payloads(
        "T1",
        entry_fills=[{"fillId": "E", "fillSz": "1", "fillPx": "100", "fee": "-0.1"}],
        exit_fills=[{"fillId": "X", "fillSz": "1", "fillPx": "101", "fee": "-0.1"}],
        bills=[],
        exchange_realized_pnl=1.0,
        evidence={
            "fills": {"status": "SUCCESS_DATA", "data": [{}]},
            "funding": {"status": "SUCCESS_EMPTY", "data": []},
            "position_history": {"status": "SUCCESS_DATA", "data": [{}], "realized_pnl_present": True},
            "fees": {"status": "SUCCESS_DATA", "data": [{}]},
        },
    )
    assert row["exchange_final"] is True
    assert row["learning_event"]["accepted"] is True


def test_mismatch_is_not_exchange_final(tmp_path: Path):
    rec = PnLReconciler(tmp_path / "pnl.json")
    rec.record_provisional("T2", engine="RYUU", local_estimated_net_pnl=1.0)
    row = rec.reconcile_payloads(
        "T2",
        entry_fills=[{"fillId": "E", "fillSz": "1", "fillPx": "100", "fee": "-0.1"}],
        exit_fills=[{"fillId": "X", "fillSz": "1", "fillPx": "101", "fee": "-0.1"}],
        bills=[],
        exchange_realized_pnl=9.0,
        evidence={
            "fills": {"status": "SUCCESS_DATA", "data": [{}]},
            "funding": {"status": "SUCCESS_EMPTY", "data": []},
            "position_history": {"status": "SUCCESS_DATA", "data": [{}], "realized_pnl_present": True},
            "fees": {"status": "SUCCESS_DATA", "data": [{}]},
        },
    )
    assert row["pnl_status"] == "MISMATCH"
    assert row["exchange_final"] is False

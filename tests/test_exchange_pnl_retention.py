import asyncio

import pytest

from exit_manager import ExitPlan, _update_lifecycle_telemetry
from pnl_reconciler import PnLReconciler, canonical_exit_reason, profit_retention_metrics


def entry_fill(*, fill_id="E1", qty="10", price="100", fee="-0.10"):
    return {"fillId": fill_id, "ordId": "ENTRY", "fillSz": qty, "fillPx": price, "fee": fee}


def exit_fill(*, fill_id="X1", order_id="EXIT", qty="10", price="101", fee="-0.20"):
    return {"fillId": fill_id, "ordId": order_id, "fillSz": qty, "fillPx": price, "fee": fee}


def seed(reconciler, *, trade_id="T1", **fields):
    payload = {
        "direction": "long",
        "entry_order_id": "ENTRY",
        "exit_order_id": "EXIT",
        "entry_notional_usdt": 100.0,
        "local_estimated_net_pnl": 0.70,
        "oxypulse_reported_final_pnl": 0.70,
    }
    payload.update(fields)
    reconciler.record_provisional(
        trade_id,
        **payload,
    )


def verified_evidence(*, funding_status="SUCCESS_EMPTY", funding_data=None, realized=True, fees=True):
    return {
        "fills": {"status": "SUCCESS_DATA", "data": [{}], "source": "test.fills"},
        "funding": {
            "status": funding_status,
            "data": list(funding_data or []),
            "source": "test.bills",
        },
        "position_history": {
            "status": "SUCCESS_DATA",
            "data": [{}],
            "realized_pnl_present": realized,
            "source": "test.position_history",
        },
        "fees": {
            "status": "SUCCESS_DATA" if fees else "QUERY_FAILED",
            "data": [{}] if fees else [],
            "source": "test.fill_fees",
        },
    }


def test_single_entry_exit_reconciles_exchange_net_and_fill_cross_check(tmp_path):
    rec = PnLReconciler(tmp_path / "pnl.json")
    seed(rec)

    row = rec.reconcile_payloads(
        "T1",
        entry_fills=[entry_fill()],
        exit_fills=[exit_fill()],
        bills=[],
        exchange_realized_pnl=0.70,
        evidence=verified_evidence(),
    )

    assert row["pnl_status"] == "RECONCILED"
    assert row["exchange_realized_pnl"] == pytest.approx(0.70)
    assert row["net_exchange_pnl"] == pytest.approx(0.70)
    assert row["gross_price_pnl"] == pytest.approx(1.0)
    assert row["entry_fee"] == pytest.approx(-0.10)
    assert row["exit_fee"] == pytest.approx(-0.20)
    assert row["total_fees_usdt"] == pytest.approx(0.30)
    assert row["computed_net_pnl"] == pytest.approx(0.70)
    assert row["computed_vs_exchange_difference"] == pytest.approx(0.0)
    assert len(row["entry_fills"]) == 1 and len(row["exit_fills"]) == 1


def test_loss_and_fee_components_remain_exchange_authoritative(tmp_path):
    rec = PnLReconciler(tmp_path / "pnl.json")
    seed(rec, trade_id="LOSS", local_estimated_net_pnl=-2.2, oxypulse_reported_final_pnl=-2.2)

    row = rec.reconcile_payloads(
        "LOSS",
        entry_fills=[entry_fill()],
        exit_fills=[exit_fill(price="98", fee="-0.20")],
        bills=[],
        exchange_realized_pnl=-2.2,
        evidence=verified_evidence(),
    )

    assert row["gross_price_pnl"] == pytest.approx(-2.0)
    assert row["net_exchange_pnl"] == pytest.approx(-2.2)
    assert row["pnl_status"] == "RECONCILED"


def test_multiple_exit_fills_and_partial_tp_are_aggregated(tmp_path):
    rec = PnLReconciler(tmp_path / "pnl.json")
    seed(rec, trade_id="PARTIAL", local_estimated_net_pnl=0.0, oxypulse_reported_final_pnl=0.0)

    row = rec.reconcile_payloads(
        "PARTIAL",
        entry_fills=[entry_fill()],
        exit_fills=[
            exit_fill(fill_id="X1", qty="4", price="102"),
            exit_fill(fill_id="X2", qty="6", price="99", order_id="EXIT2"),
        ],
        bills=[],
        exchange_realized_pnl=-0.10,
        evidence=verified_evidence(),
    )

    assert row["exit_contracts"] == pytest.approx(10.0)
    assert row["average_exit_price"] == pytest.approx(100.2)
    assert row["gross_price_pnl"] == pytest.approx(0.2)
    assert row["net_exchange_pnl"] == pytest.approx(-0.10)
    assert len(row["exit_fills"]) == 2


@pytest.mark.parametrize(
    ("funding_value", "expected"),
    [("-0.05", -0.05), ("0.07", 0.07)],
)
def test_funding_paid_or_received_is_attributed(tmp_path, funding_value, expected):
    rec = PnLReconciler(tmp_path / "pnl.json")
    seed(rec, trade_id=f"FUND-{funding_value}")

    row = rec.reconcile_payloads(
        f"FUND-{funding_value}",
        entry_fills=[entry_fill()],
        exit_fills=[exit_fill()],
        bills=[{"billId": "B1", "type": "funding", "balChg": funding_value}],
        exchange_realized_pnl=0.70 + expected,
        evidence=verified_evidence(
            funding_status="SUCCESS_DATA",
            funding_data=[{"billId": "B1", "type": "funding", "balChg": funding_value}],
        ),
    )

    assert row["funding"] == pytest.approx(expected)
    assert row["funding_bills"]


@pytest.mark.parametrize(
    ("exchange_pnl", "expected_status"),
    [(0.70, "RECONCILED"), (0.715, "ROUNDING_DIFFERENCE"), (0.90, "MISMATCH")],
)
def test_exact_tolerance_and_mismatch_statuses(tmp_path, exchange_pnl, expected_status):
    rec = PnLReconciler(tmp_path / "pnl.json")
    seed(rec)
    row = rec.reconcile_payloads(
        "T1",
        entry_fills=[entry_fill()],
        exit_fills=[exit_fill()],
        bills=[],
        exchange_realized_pnl=exchange_pnl,
        evidence=verified_evidence(),
        tolerance_usdt=0.01,
    )
    assert row["pnl_status"] == expected_status
    assert row["net_exchange_pnl"] == pytest.approx(exchange_pnl)


def test_open_trade_and_missing_exit_remain_provisional(tmp_path):
    rec = PnLReconciler(tmp_path / "pnl.json")
    row = rec.record_provisional(
        "OPEN-LINK",
        coin="LINK",
        direction="long",
        entry_contracts=0.4,
        average_entry_price=8.831,
        entry_fee=-0.0017662,
        local_estimated_net_pnl=0.0,
        execution_environment="production",
    )
    assert row["pnl_status"] == "PROVISIONAL"
    assert row["exchange_final"] is False
    assert rec.production_performance_snapshot()["closed_final"] == 0

    closed = rec.reconcile_payloads(
        "OPEN-LINK",
        entry_fills=[entry_fill(qty="0.4", price="8.831", fee="-0.0017662")],
        exit_fills=[],
        bills=[],
        exchange_realized_pnl=None,
    )
    assert closed["pnl_status"] == "PROVISIONAL"
    assert closed["net_exchange_pnl"] is None


def test_missing_fee_data_is_explicit_not_fabricated(tmp_path):
    rec = PnLReconciler(tmp_path / "pnl.json")
    seed(rec)
    row = rec.reconcile_payloads(
        "T1",
        entry_fills=[entry_fill(fee="")],
        exit_fills=[exit_fill(fee="")],
        bills=[],
        exchange_realized_pnl=0.70,
        evidence=verified_evidence(fees=False),
    )
    assert row["fee_data_complete"] is False
    assert row["total_fees_usdt"] == pytest.approx(0.0)
    assert row["net_exchange_pnl"] == pytest.approx(0.70)
    assert row["pnl_status"] == "AWAITING_EXCHANGE_DATA"
    assert row["exchange_final"] is False


def test_success_empty_funding_is_confirmed_zero(tmp_path):
    rec = PnLReconciler(tmp_path / "pnl.json")
    seed(rec)
    row = rec.reconcile_payloads(
        "T1",
        entry_fills=[entry_fill()],
        exit_fills=[exit_fill()],
        bills=[],
        exchange_realized_pnl=0.70,
        evidence=verified_evidence(funding_status="SUCCESS_EMPTY"),
    )
    assert row["pnl_status"] == "RECONCILED"
    assert row["funding"] == pytest.approx(0.0)


def test_missing_provenance_cannot_finalize(tmp_path):
    rec = PnLReconciler(tmp_path / "pnl.json")
    seed(rec)
    row = rec.reconcile_payloads(
        "T1",
        entry_fills=[entry_fill()],
        exit_fills=[exit_fill()],
        bills=[],
        exchange_realized_pnl=0.70,
    )
    assert row["pnl_status"] == "AWAITING_EXCHANGE_DATA"
    assert row["exchange_final"] is False
    assert "fills_unknown" in row["finalization_blockers"]


def test_funding_query_failed_cannot_finalize(tmp_path):
    rec = PnLReconciler(tmp_path / "pnl.json")
    seed(rec)
    row = rec.reconcile_payloads(
        "T1",
        entry_fills=[entry_fill()],
        exit_fills=[exit_fill()],
        bills=[],
        exchange_realized_pnl=0.70,
        evidence=verified_evidence(funding_status="QUERY_FAILED"),
    )
    assert row["pnl_status"] == "AWAITING_EXCHANGE_DATA"
    assert row["funding"] == pytest.approx(0.0)
    assert "funding_query_failed" in row["finalization_blockers"]


def test_fill_query_failed_cannot_finalize(tmp_path):
    rec = PnLReconciler(tmp_path / "pnl.json")
    seed(rec)
    row = rec.reconcile_payloads(
        "T1",
        entry_fills=[entry_fill()],
        exit_fills=[exit_fill()],
        bills=[],
        exchange_realized_pnl=0.70,
        evidence={
            **verified_evidence(),
            "fills": {"status": "QUERY_FAILED", "data": [], "source": "test.fills"},
        },
    )
    assert row["pnl_status"] == "AWAITING_EXCHANGE_DATA"
    assert "fills_query_failed" in row["finalization_blockers"]


def test_position_history_query_failed_cannot_finalize(tmp_path):
    rec = PnLReconciler(tmp_path / "pnl.json")
    seed(rec)
    row = rec.reconcile_payloads(
        "T1",
        entry_fills=[entry_fill()],
        exit_fills=[exit_fill()],
        bills=[],
        exchange_realized_pnl=0.70,
        evidence={
            **verified_evidence(),
            "position_history": {"status": "QUERY_FAILED", "data": [], "source": "test.history"},
        },
    )
    assert row["pnl_status"] == "AWAITING_EXCHANGE_DATA"
    assert "position_history_query_failed" in row["finalization_blockers"]


def test_position_history_success_zero_is_confirmed_zero(tmp_path):
    rec = PnLReconciler(tmp_path / "pnl.json")
    seed(rec, local_estimated_net_pnl=0.0, oxypulse_reported_final_pnl=0.0)
    row = rec.reconcile_payloads(
        "T1",
        entry_fills=[entry_fill()],
        exit_fills=[exit_fill(price="100")],
        bills=[],
        exchange_realized_pnl=0.0,
        evidence=verified_evidence(),
    )
    assert row["pnl_status"] == "RECONCILED"
    assert row["exchange_realized_pnl"] == pytest.approx(0.0)


def test_duplicate_reconciliation_updates_risk_once(tmp_path, monkeypatch):
    applied = []
    learned = []
    monkeypatch.setattr("risk_manager.risk.record_realized_pnl", lambda pnl, **kwargs: applied.append(pnl))
    monkeypatch.setattr("exchange_final_learning.ingest", lambda row: learned.append(row["trade_id"]) or {"accepted": True})
    rec = PnLReconciler(tmp_path / "pnl.json")
    seed(rec)
    payload = dict(
        trade_id="T1",
        entry_fills=[entry_fill()],
        exit_fills=[exit_fill()],
        bills=[],
        exchange_realized_pnl=0.70,
        evidence=verified_evidence(),
    )
    first = rec.reconcile_payloads(**payload)
    second = rec.reconcile_payloads(**payload)
    assert first["pnl_status"] == second["pnl_status"] == "RECONCILED"
    assert len(second["entry_fills"]) == len(second["exit_fills"]) == 1
    assert applied == [pytest.approx(0.70)]
    assert learned == ["T1"]


def test_provisional_local_pnl_never_updates_risk(tmp_path, monkeypatch):
    applied = []
    monkeypatch.setattr("risk_manager.risk.record_realized_pnl", lambda pnl, **kwargs: applied.append(pnl))
    rec = PnLReconciler(tmp_path / "pnl.json")
    loss = rec.record_provisional("LOSS", local_estimated_net_pnl=-1.0)
    gain = rec.record_provisional("GAIN", local_estimated_net_pnl=1.0)
    assert loss["risk_applied_amount"] == pytest.approx(0.0)
    assert gain["risk_applied_amount"] == pytest.approx(0.0)
    assert applied == []


def test_restart_then_reconcile_keeps_one_risk_update(tmp_path, monkeypatch):
    applied = []
    monkeypatch.setattr("risk_manager.risk.record_realized_pnl", lambda pnl, **kwargs: applied.append(pnl))
    monkeypatch.setattr("exchange_final_learning.ingest", lambda row: {"accepted": True})
    path = tmp_path / "pnl.json"
    rec = PnLReconciler(path)
    seed(rec)
    kwargs = dict(
        trade_id="T1",
        entry_fills=[entry_fill()],
        exit_fills=[exit_fill()],
        bills=[],
        exchange_realized_pnl=0.70,
        evidence=verified_evidence(),
    )
    rec.reconcile_payloads(**kwargs)
    restarted = PnLReconciler(path)
    row = restarted.reconcile_payloads(**kwargs)
    assert row["pnl_status"] == "RECONCILED"
    assert applied == [pytest.approx(0.70)]


def test_retention_and_giveback_never_label_loss_as_positive_retention():
    result = profit_retention_metrics(
        mfe_gross_pnl_usdt=0.40,
        realized_net_pnl_usdt=0.05,
        total_fees_usdt=0.0,
    )
    assert result["profit_retention_pct"] == pytest.approx(12.5)
    assert result["profit_giveback_usdt"] == pytest.approx(0.35)

    loss = profit_retention_metrics(
        mfe_gross_pnl_usdt=0.40,
        realized_net_pnl_usdt=-0.10,
        total_fees_usdt=0.0,
    )
    assert loss["profit_retention_pct"] is None
    assert loss["profit_giveback_usdt"] == pytest.approx(0.5)


def test_exit_manager_persists_mfe_mae_prices_gross_values_and_meaningful_timeline():
    plan = ExitPlan(
        coin="LINK",
        direction="long",
        engine_name="RYUU",
        entry_price=100.0,
        size_usdt=100.0,
        initial_size_usdt=100.0,
        atr_val=1.0,
        hard_sl=98.0,
        tp1_price=102.0,
        tp2_price=104.0,
        leverage=1,
    )

    _update_lifecycle_telemetry(plan, 102.0, True)
    _update_lifecycle_telemetry(plan, 98.0, True)

    assert plan.mfe_price == pytest.approx(102.0)
    assert plan.mfe_gross_pnl_usdt == pytest.approx(2.0)
    assert plan.mae_price == pytest.approx(98.0)
    assert plan.mae_gross_pnl_usdt == pytest.approx(-2.0)
    assert {row["event"] for row in plan.exit_timeline} >= {"MFE_UPDATE", "MAE_UPDATE"}


def test_fee_bleed_and_exit_reason_attribution_are_diagnostic(tmp_path):
    rec = PnLReconciler(tmp_path / "pnl.json")
    rec.record_provisional(
        "FEE",
        direction="long",
        entry_notional_usdt=10.0,
        local_estimated_net_pnl=0.02,
        oxypulse_reported_final_pnl=0.02,
        exit_reason="profit_lock_l1",
    )
    row = rec.reconcile_payloads(
        "FEE",
        entry_fills=[entry_fill(qty="1", price="100", fee="-0.03")],
        exit_fills=[exit_fill(qty="1", price="100.2", fee="-0.03")],
        bills=[],
        exchange_realized_pnl=-0.04,
        evidence=verified_evidence(),
    )
    assert row["fee_bleed_classification"] == "MICRO_PROFIT_FEE_BLEED"
    assert row["primary_exit_reason"] == "PROFIT_LOCK"


@pytest.mark.parametrize(
        ("raw", "canonical"),
    [
        ("sl", "HARD_SL"),
        ("tp1", "TP1"),
        ("tp2_hit", "TP2"),
        ("trailing_stop", "TRAILING"),
        ("breakeven_stop", "BREAK_EVEN"),
        ("emergency_close", "EMERGENCY"),
        ("manual", "MANUAL"),
        ("exchange_algo", "EXCHANGE_PROTECTION"),
        ("sl_exchange", "EXCHANGE_PROTECTION"),
    ],
)
def test_exit_reason_attribution(raw, canonical):
    assert canonical_exit_reason(raw) == canonical


def test_reconcile_from_okx_consumes_all_exit_fills_without_mutations(tmp_path):
    rec = PnLReconciler(tmp_path / "pnl.json")
    seed(rec, trade_id="OKX", local_estimated_net_pnl=0.70, oxypulse_reported_final_pnl=0.70)

    class Client:
        async def get_fills(self, **kwargs):
            return [entry_fill(), exit_fill(fill_id="X1"), exit_fill(fill_id="X2", qty="0", price="101")]

        async def get_fills_status(self, **kwargs):
            return {"ok": True, "status": "SUCCESS_DATA", "fills": await self.get_fills(**kwargs), "source": "test.fills"}

        async def get_bills(self, **kwargs):
            return []

        async def get_bills_status(self, **kwargs):
            return {"ok": True, "status": "SUCCESS_EMPTY", "bills": [], "source": "test.bills"}

        async def get_position_history(self, **kwargs):
            return [{"posId": "P1", "realizedPnl": "0.70"}]

        async def get_position_history_status(self, **kwargs):
            return {
                "ok": True,
                "status": "SUCCESS_DATA",
                "history": await self.get_position_history(**kwargs),
                "source": "test.position_history",
            }

    row = asyncio.run(rec.reconcile_from_okx(Client(), "OKX", "LINK-USDT-SWAP", ["ENTRY", "EXIT"]))
    assert row["pnl_status"] == "RECONCILED"
    assert row["exchange_realized_pnl"] == pytest.approx(0.70)
    assert row["exit_contracts"] == pytest.approx(10.0)


def test_production_snapshot_uses_exchange_net_and_positive_fee_cost(tmp_path):
    rec = PnLReconciler(tmp_path / "pnl.json")
    rec._records = {
        "LIVE": {
            "execution_environment": "production",
            "pnl_status": "RECONCILED",
            "exchange_realized_pnl": 0.70,
            "gross_price_pnl": 1.0,
            "entry_fee": -0.1,
            "exit_fee": -0.2,
            "total_fees_usdt": 0.3,
            "funding": 0.0,
        }
    }
    snapshot = rec.production_performance_snapshot()
    assert snapshot["net_pnl_usdt"] == pytest.approx(0.70)
    assert snapshot["fees_usdt"] == pytest.approx(0.30)
    assert snapshot["exchange_authority"] == "NET_EXCHANGE_PNL"


def test_telegram_production_snapshot_reads_reconciler_exchange_value(tmp_path, monkeypatch):
    import pnl_reconciler
    import telegram_backend

    rec = PnLReconciler(tmp_path / "pnl.json")
    rec._records = {
        "LIVE": {
            "execution_environment": "production",
            "pnl_status": "RECONCILED",
            "exchange_realized_pnl": 1.25,
            "gross_price_pnl": 1.50,
            "total_fees_usdt": 0.25,
        },
        "PAPER-LIVE": {
            "execution_environment": "paper",
            "pnl_status": "RECONCILED",
            "exchange_realized_pnl": 99.0,
        },
    }
    monkeypatch.setattr(pnl_reconciler, "reconciler", rec)
    snapshot = telegram_backend.production_pnl_snapshot()
    assert snapshot["net_pnl_usdt"] == pytest.approx(1.25)
    assert snapshot["exchange_authority"] == "NET_EXCHANGE_PNL"


def test_mismatch_remains_exchange_authoritative_but_reconciling(tmp_path):
    rec = PnLReconciler(tmp_path / "pnl.json")
    seed(
        rec,
        trade_id="MISMATCH",
        execution_environment="production",
        local_estimated_net_pnl=0.10,
        oxypulse_reported_final_pnl=0.10,
    )
    row = rec.reconcile_payloads(
        "MISMATCH",
        entry_fills=[entry_fill()],
        exit_fills=[exit_fill()],
        bills=[],
        exchange_realized_pnl=0.90,
        evidence=verified_evidence(),
    )
    assert row["pnl_status"] == "MISMATCH"
    snapshot = rec.production_performance_snapshot()
    assert snapshot["net_pnl_usdt"] == pytest.approx(0.0)
    assert snapshot["reconciling"] == 1
    assert snapshot["mismatch_rows"][0]["exchange_realized_pnl"] == pytest.approx(0.90)
    assert snapshot["mismatch_exchange_pnl_usdt"] == pytest.approx(0.90)


def test_later_exit_observation_updates_record_without_changing_exchange_pnl(tmp_path):
    rec = PnLReconciler(tmp_path / "pnl.json")
    rec.record_provisional("T1", execution_environment="production")
    rec.reconcile_payloads(
        "T1",
        entry_fills=[entry_fill()],
        exit_fills=[exit_fill()],
        bills=[],
        exchange_realized_pnl=0.25,
        evidence=verified_evidence(),
    )
    updated = rec.update_record(
        "T1",
        post_exit_continuation={"classification": "false_positive", "samples": 4},
        premature_exit_observed=True,
    )
    assert updated["exchange_realized_pnl"] == pytest.approx(0.25)
    assert updated["premature_exit_observed"] is True

import asyncio
import json

import pytest

from liquidity_model import quote_volume_24h, snapshot
from pnl_reconciler import PnLReconciler
from profitability_control_plane import calibrate_probabilities, diversity_adjustment, evaluate_candidate


def test_linear_swap_quote_volume_converts_base_units_to_usdt():
    ticker = {"last": "2.5", "volCcy24h": "1000", "vol24h": "999999"}
    assert quote_volume_24h(ticker) == pytest.approx(2500.0)
    snap = snapshot({**ticker, "bidPx": "2.49", "askPx": "2.51"})
    assert snap.quote_volume_24h_usdt == pytest.approx(2500.0)
    assert snap.spread_available


def test_probability_calibration_shrinks_sparse_engine_results():
    history = [{"engine": "RYUU", "direction": "long", "regime": "TRENDING", "closed_at": "x", "exit_reason": "tp2_hit"}]
    estimate = calibrate_probabilities(engine="RYUU", direction="long", regime="TRENDING", history=history)
    assert estimate.sample_size == 1
    assert estimate.probabilities["tp2"] < 0.20
    assert sum(estimate.probabilities.values()) == pytest.approx(1.0)
    assert estimate.uncertainty > 0.0


def test_consensus_diversity_penalizes_duplicate_feature_families():
    families = {"A": "trend", "B": "trend", "C": "order_flow"}
    assert diversity_adjustment(["A", "C"], families) > diversity_adjustment(["A", "B"], families)


def test_profitability_decision_exposes_full_cost_decomposition():
    decision = evaluate_candidate(
        size_usdt=200, target_move_pct=4.0, stop_move_pct=1.0,
        fee_rate=0.0005, spread_pct=0.05, slippage_pct=0.03,
        funding_pct=0.01, impact_pct=0.02, engine="RYUU", direction="short",
    )
    assert set(decision.costs) == {
        "entry_fee", "exit_fee", "spread", "entry_slippage", "exit_slippage",
        "funding", "market_impact", "total",
    }
    assert decision.costs["total"] == pytest.approx(sum(v for k, v in decision.costs.items() if k != "total"))
    assert decision.action in {"accept", "reduce", "queue", "shadow"}


def test_reconciler_requires_explicit_entry_and_exit_order_ids(tmp_path):
    reconciler = PnLReconciler(tmp_path / "pnl.json")
    reconciler.record_provisional("T", entry_order_id="E", local_estimated_net_pnl=0)

    class Client:
        async def get_fills(self, **kwargs):
            raise AssertionError("must not query until both sides are attributable")

    row = asyncio.run(reconciler.reconcile_from_okx(Client(), "T", "TEST-USDT-SWAP", ["E"]))
    assert row["pnl_status"] == "AWAITING_EXCHANGE_DATA"
    assert row["reconciliation_reason"] == "explicit_entry_and_exit_order_ids_required"


def test_reconciler_attributes_fills_by_order_id_not_midpoint(tmp_path):
    reconciler = PnLReconciler(tmp_path / "pnl.json")
    reconciler.record_provisional("T", entry_order_id="ENTRY", exit_order_id="EXIT", position_id="P1", local_estimated_net_pnl=1.0)

    class Client:
        async def get_fills(self, **kwargs):
            return [
                {"fillId": "x1", "ordId": "EXIT", "fee": "-0.2"},
                {"fillId": "e1", "ordId": "ENTRY", "fee": "-0.1"},
                {"fillId": "other", "ordId": "OTHER", "fee": "-9"},
            ]

        async def get_fills_status(self, **kwargs):
            return {"ok": True, "status": "SUCCESS_DATA", "fills": await self.get_fills(**kwargs), "source": "test.fills"}

        async def get_bills(self, **kwargs):
            return []

        async def get_bills_status(self, **kwargs):
            return {"ok": True, "status": "SUCCESS_EMPTY", "bills": [], "source": "test.bills"}

        async def get_position_history(self, **kwargs):
            return [{"posId": "P1", "realizedPnl": "0.7"}]

        async def get_position_history_status(self, **kwargs):
            return {
                "ok": True,
                "status": "SUCCESS_DATA",
                "history": await self.get_position_history(**kwargs),
                "source": "test.position_history",
            }

    row = asyncio.run(reconciler.reconcile_from_okx(Client(), "T", "TEST-USDT-SWAP", ["ENTRY", "EXIT"]))
    assert row["entry_fee"] == pytest.approx(-0.1)
    assert row["exit_fee"] == pytest.approx(-0.2)
    assert row["exchange_realized_pnl"] == pytest.approx(0.7)


def test_shadow_probation_close_is_idempotent(tmp_path, monkeypatch):
    import shadow_rehab_tracker as tracker

    monkeypatch.setattr(tracker, "PATH", tmp_path / "rehab.json")
    monkeypatch.setattr(tracker, "_LEDGER", None)
    monkeypatch.setattr(tracker, "_LEDGER_PATH", None)
    fields = dict(engine="RYUU", symbol="BTC", side="long", entry_price=100, exit_price=101, size_usdt=10, pnl_usdt=0.1, opened_at="2026-01-01T00:00:00+00:00")
    tracker.record_probation_close(**fields)
    tracker.record_probation_close(**fields)
    payload = json.loads((tmp_path / "rehab.json").read_text())
    assert len(payload["rehab_engines"]["RYUU"]["real_probation"]) == 1

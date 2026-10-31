import asyncio
import json

import exit_manager
import okx_client
import paper_validation


def test_paper_validation_writes_entry_skip_and_close(tmp_path, monkeypatch):
    path = tmp_path / "paper_validation_trades.jsonl"
    monkeypatch.setattr(paper_validation, "PATH", path)

    paper_validation.record_entry(
        order_id="PAPER-BUY-BTC",
        pair="BTC",
        entry_price=100.0,
        stop_price=98.0,
        sl_dist_pct=2.0,
        risk_amount_usdt=5.0,
        raw_size_usdt=250.0,
        min_exec_usdt=50.0,
        cap_applied=False,
        cap_source="",
        final_size_usdt=250.0,
    )
    paper_validation.record_skip(
        pair="ETH",
        entry_price=100.0,
        stop_price=99.0,
        sl_dist_pct=1.0,
        risk_amount_usdt=5.0,
        raw_size_usdt=500.0,
        min_exec_usdt=50.0,
        cap_applied=True,
        cap_source="position_size_cap_usdt",
        final_size_usdt=25.0,
        skip_reason="risk_sizing_below_min",
    )
    paper_validation.record_close(
        context={
            "order_id": "PAPER-BUY-BTC",
            "pair": "BTC",
            "entry_price": 100.0,
            "stop_price": 98.0,
            "sl_dist_pct": 2.0,
            "risk_amount_usdt": 5.0,
            "raw_size_usdt": 250.0,
            "min_exec_usdt": 50.0,
            "cap_applied": False,
            "cap_source": "",
            "final_size_usdt": 250.0,
        },
        exit_price=101.0,
        pnl_usdt=2.5,
        exit_reason="tp",
    )

    rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
    assert [row["event"] for row in rows] == ["entry", "skip", "close"]
    assert rows[0]["outcome"] == "pending"
    assert rows[1]["outcome"] == "skipped"
    assert rows[2]["outcome"] == "win"
    assert rows[2]["risk_amount_usdt"] == 5.0


def test_okx_paper_snapshot_uses_500_usdt(monkeypatch):
    monkeypatch.setattr(okx_client, "PAPER_TRADING", True)
    snapshot = asyncio.run(okx_client.OKXClient().get_account_snapshot())

    assert snapshot["total_eq_usdt"] == 500.0
    assert snapshot["avail_eq_usdt"] == 500.0
    assert snapshot["details"]["USDT"]["availBal"] == 500.0


def test_paper_validation_close_allows_field_overrides(tmp_path, monkeypatch):
    path = tmp_path / "paper_validation_trades.jsonl"
    monkeypatch.setattr(paper_validation, "PATH", path)

    paper_validation.record_close(
        context={"order_id": "original", "pair": "OP"},
        order_id="override",
        pair="OP",
        exit_price=0.09,
        pnl_usdt=-0.5,
        exit_reason="sl",
    )

    row = json.loads(path.read_text(encoding="utf-8"))
    assert row["event"] == "close"
    assert row["order_id"] == "override"
    assert row["outcome"] == "loss"


def test_paper_validation_zero_gross_slot_rotation_is_neutral(tmp_path, monkeypatch):
    path = tmp_path / "paper_validation_trades.jsonl"
    monkeypatch.setattr(paper_validation, "PATH", path)

    paper_validation.record_close(
        context={"order_id": "paper-slot", "pair": "TRX"},
        exit_price=0.33,
        pnl_usdt=0.0,
        fee_usdt=0.2,
        exit_reason="slot_rotation",
    )

    row = json.loads(path.read_text(encoding="utf-8"))
    assert row["event"] == "close"
    assert row["outcome"] == "neutral"
    assert row["net_pnl_usdt"] == -0.2


def _install_paper_exit_test_harness(monkeypatch):
    monkeypatch.setenv("PAPER_TRADING", "true")
    monkeypatch.setattr(okx_client, "PAPER_TRADING", True)
    exit_manager._plans.clear()
    closes = []

    async def _noop_alert(*args, **kwargs):
        return None

    def _capture_notify(coin, exit_price, pnl_usdt, reason, plan, via_exchange=False):
        closes.append(
            {
                "coin": coin,
                "exit_price": exit_price,
                "pnl_usdt": pnl_usdt,
                "reason": reason,
                "via_exchange": via_exchange,
            }
        )

    async def _exchange_closed_should_not_run(*args, **kwargs):
        raise AssertionError("paper exit fell through to exchange close handling")

    monkeypatch.setattr(exit_manager, "alert_trade_closed", _noop_alert)
    monkeypatch.setattr(exit_manager, "_notify_engine", _capture_notify)
    monkeypatch.setattr(exit_manager, "_handle_exchange_closed", _exchange_closed_should_not_run)
    yield closes
    exit_manager._plans.clear()


def test_paper_mode_exchange_sync_does_not_close_absent_okx_position(monkeypatch):
    harness = _install_paper_exit_test_harness(monkeypatch)
    closes = next(harness)

    class FakeClient:
        async def get_live_open_coins_status(self):
            return {"ok": True, "coins": set(), "reason": ""}

    try:
        exit_manager.register(
            "BTC", "long", 100.0, 200.0, 1.0,
            95.0, 105.0, 110.0,
            order_id="PAPER-BUY-BTC", engine_name="TEST", leverage=10,
        )
        monkeypatch.setattr(exit_manager, "_okx_client", FakeClient())

        asyncio.run(exit_manager._do_exchange_sync())

        assert "BTC" in exit_manager.get_all_plans()
        assert closes == []
    finally:
        try:
            next(harness)
        except StopIteration:
            pass


def test_paper_plan_closes_on_local_sl_hit_without_exchange_sync(monkeypatch):
    harness = _install_paper_exit_test_harness(monkeypatch)
    closes = next(harness)
    try:
        exit_manager.register(
            "BTC", "long", 100.0, 200.0, 1.0,
            95.0, 105.0, 110.0,
            order_id="PAPER-BUY-BTC", engine_name="TEST", leverage=10,
        )
        monkeypatch.setattr(exit_manager, "latest_price", lambda coin: 94.9)

        asyncio.run(exit_manager._check_plan("BTC"))

        assert "BTC" not in exit_manager.get_all_plans()
        assert closes[0]["reason"] == "sl_hit"
        assert closes[0]["via_exchange"] is False
    finally:
        try:
            next(harness)
        except StopIteration:
            pass


def test_paper_plan_closes_on_local_tp2_hit_without_exchange_sync(monkeypatch):
    harness = _install_paper_exit_test_harness(monkeypatch)
    closes = next(harness)
    try:
        exit_manager.register(
            "BTC", "long", 100.0, 200.0, 1.0,
            95.0, 105.0, 110.0,
            order_id="PAPER-BUY-BTC", engine_name="TEST", leverage=10,
        )
        monkeypatch.setattr(exit_manager, "latest_price", lambda coin: 110.1)

        asyncio.run(exit_manager._check_plan("BTC"))

        assert "BTC" not in exit_manager.get_all_plans()
        assert closes[0]["reason"] == "tp2_hit"
        assert closes[0]["via_exchange"] is False
    finally:
        try:
            next(harness)
        except StopIteration:
            pass


def test_paper_plan_closes_on_local_trailing_stop_without_exchange_sync(monkeypatch):
    harness = _install_paper_exit_test_harness(monkeypatch)
    closes = next(harness)
    settings_backup = dict(exit_manager._s._current)
    try:
        exit_manager._s._current.update(
            {
                "tsl_enabled": True,
                "secure_profit_mode_enabled": False,
                "tsl_activation_pct": 1.0,
                "tsl_trail_pct": 1.0,
                "trail_distance_mode": "percent",
                "trail_distance_atr_mult": 0.0,
                "stale_trade_exit_enabled": False,
            }
        )
        plan = exit_manager.register(
            "BTC", "long", 100.0, 200.0, 1.0,
            95.0, 105.0, 110.0,
            order_id="PAPER-BUY-BTC", engine_name="TEST", leverage=10,
        )
        monkeypatch.setattr(exit_manager, "latest_price", lambda coin: 102.0)
        asyncio.run(exit_manager._check_plan("BTC"))
        assert "BTC" in exit_manager.get_all_plans()
        assert plan.tsl_armed is True

        monkeypatch.setattr(exit_manager, "latest_price", lambda coin: 100.9)
        asyncio.run(exit_manager._check_plan("BTC"))

        assert "BTC" not in exit_manager.get_all_plans()
        assert closes[0]["reason"] == "trailing_stop_hit"
        assert closes[0]["via_exchange"] is False
    finally:
        exit_manager._s._current.clear()
        exit_manager._s._current.update(settings_backup)
        try:
            next(harness)
        except StopIteration:
            pass

from __future__ import annotations

import asyncio
import json
import os
import time
from pathlib import Path
from types import SimpleNamespace


def test_latency_telemetry_records_all_required_stages(tmp_path):
    from telegram_control_plane import TelegramControlPlane

    tracker = TelegramControlPlane(tmp_path / "telegram.json")
    token = tracker.begin("command", "status", update_id=42)
    tracker.dispatch(token)
    tracker.backend_complete(token)
    tracker.sent(token)
    snapshot = tracker.snapshot()

    assert snapshot["counters"]["telegram_commands_total"] == 1
    row = snapshot["recent"][-1]
    for field in (
        "received_at", "dispatch_at", "backend_complete_at", "send_at",
        "total_latency_ms", "backend_latency_ms", "telegram_send_latency_ms",
    ):
        assert row[field] not in (None, "")
    assert snapshot["latency_by_action"]["status"]["count"] == 1
    assert snapshot["handler_backlog"] == 0


def test_latency_telemetry_reports_backlog_and_error(tmp_path):
    from telegram_control_plane import TelegramControlPlane

    tracker = TelegramControlPlane(tmp_path / "telegram.json")
    pending = tracker.begin("button", "live:halt")
    assert tracker.snapshot()["handler_backlog"] == 1
    tracker.error(pending, RuntimeError("network"), api_failure=True)
    snapshot = tracker.snapshot()
    assert snapshot["handler_backlog"] == 0
    assert snapshot["counters"]["telegram_errors_total"] == 1
    assert snapshot["counters"]["telegram_api_failures_total"] == 1


def test_control_plane_flush_is_restart_safe(tmp_path):
    from telegram_control_plane import TelegramControlPlane

    path = tmp_path / "telegram.json"
    tracker = TelegramControlPlane(path)
    token = tracker.begin("command", "positions")
    tracker.dispatch(token)
    tracker.backend_complete(token)
    tracker.sent(token)
    async def lifecycle():
        await tracker.start()
        await tracker.stop()
    asyncio.run(lifecycle())
    restored = TelegramControlPlane(path).snapshot()
    assert restored["counters"]["telegram_commands_total"] == 1
    assert restored["recent"][-1]["action"] == "positions"


def test_control_plane_start_rewrites_process_local_health(tmp_path):
    from telegram_control_plane import TelegramControlPlane

    path = tmp_path / "telegram.json"
    path.write_text(json.dumps({
        "handler_backlog": 7,
        "oldest_pending_command_age_sec": 999.0,
        "recent": [],
        "counters": {},
    }), encoding="utf-8")
    tracker = TelegramControlPlane(path)

    async def lifecycle():
        await tracker.start()
        await asyncio.sleep(2.2)
        await tracker.stop()

    asyncio.run(lifecycle())
    persisted = json.loads(path.read_text(encoding="utf-8"))
    assert persisted["handler_backlog"] == 0
    assert persisted["oldest_pending_command_age_sec"] == 0.0
    assert persisted["process_started_at"]
    assert persisted["event_loop_lag_ms"]["samples"] > 0


def test_production_pnl_excludes_paper_and_counterfactual(tmp_path):
    from pnl_reconciler import PnLReconciler

    reconciler = PnLReconciler(tmp_path / "pnl.json")
    reconciler._records = {
        "PAPER-OP": {
            "execution_environment": "paper",
            "pnl_status": "RECONCILED",
            "exchange_realized_pnl": 99.0,
        },
        "CF-BTC": {
            "execution_environment": "counterfactual",
            "pnl_status": "RECONCILED",
            "exchange_realized_pnl": 88.0,
        },
        "LIVE-1": {
            "execution_environment": "production",
            "pnl_status": "RECONCILED",
            "exchange_realized_pnl": 1.25,
            "gross_price_pnl": 1.5,
            "entry_fee": -0.1,
            "exit_fee": -0.15,
        },
        "LIVE-2": {
            "execution_environment": "production",
            "pnl_status": "AWAITING_EXCHANGE_DATA",
            "local_estimated_net_pnl": -0.4,
        },
    }
    snapshot = reconciler.production_performance_snapshot()
    assert snapshot["closed_final"] == 1
    assert snapshot["reconciling"] == 1
    assert snapshot["net_pnl_usdt"] == 1.25
    assert snapshot["wins"] == 1
    assert {row["trade_id"] for row in snapshot["final_rows"]} == {"LIVE-1"}


def test_disarm_and_halt_preserve_runtime_and_position_management(tmp_path, monkeypatch):
    import live_state
    import telegram_operator_controls as controls

    monkeypatch.setenv("PAPER_TRADING", "false")
    state = live_state.LiveStateMachine(tmp_path / "live_state.json")
    state.transition("LIVE_ARMED", "test")
    monkeypatch.setattr(live_state, "state", state)
    engine = SimpleNamespace(running=True)

    disarmed = asyncio.run(controls.disarm(engine))
    assert disarmed["ok"] is True
    assert disarmed["engine_running"] is True
    assert disarmed["position_management_preserved"] is True
    assert state.snapshot()["state"] == "LIVE_READY_UNARMED"

    state.transition("LIVE_ARMED", "test")
    halted = asyncio.run(controls.halt(engine))
    assert halted["ok"] is True
    assert halted["engine_running"] is True
    assert halted["position_management_preserved"] is True
    assert state.snapshot()["state"] == "LIVE_HALTED"


def test_arm_requires_preflight_and_verifies_effective_state(tmp_path, monkeypatch):
    import live_preflight
    import live_state
    import telegram_operator_controls as controls

    monkeypatch.setenv("PAPER_TRADING", "false")
    state = live_state.LiveStateMachine(tmp_path / "live_state.json")
    monkeypatch.setattr(live_state, "state", state)

    required = (
        "account_equity", "available_balance", "instrument_metadata", "metadata_freshness",
        "quantity_conversion", "capital_allocator", "execution_router",
        "protection_manager", "pnl_reconciler", "leverage_set_and_readback",
        "no_unresolved_orders", "no_unknown_exchange_positions", "no_unknown_local_exposure",
        "no_orphan_algo_orders",
    )

    async def clean_preflight(*args, **kwargs):
        assert kwargs["persist_state"] is False
        return {"ok": True, "checks": {key: {"ok": True} for key in required}}

    monkeypatch.setattr(live_preflight, "run_preflight", clean_preflight)
    engine = SimpleNamespace(research_only=True)
    monkeypatch.setenv("OXYPULSE_PRODUCTION_RESEARCH_ONLY", "true")
    result = asyncio.run(controls.arm(object(), engine))
    assert result["ok"] is True
    assert result["state"]["state"] == "LIVE_ARMED"
    assert result["state"]["armed"] is True
    assert result["runtime_execution_eligible"] is True
    assert engine.research_only is False
    assert "OXYPULSE_PRODUCTION_RESEARCH_ONLY" not in os.environ


def test_account_snapshot_uses_authenticated_backend_values(monkeypatch):
    import capital_reservations
    import telegram_backend

    class OKX:
        async def get_account_snapshot(self):
            return {
                "total_eq_usdt": 11.01,
                "avail_eq_usdt": 10.5,
                "usdt_cash_bal": 11.0,
                "usdt_frozen_bal": 0.5,
            }

    monkeypatch.setattr(capital_reservations.book, "snapshot", lambda: [
        {"margin_usdt": 0.25}, {"margin_usdt": 0.10},
    ])
    positions = {"positions": {"BTC": {
        "unrealised_pnl": 0.3,
        "isolated_margin": 0.8,
        "risk_at_stop_usdt": 0.2,
    }}}
    snapshot = asyncio.run(telegram_backend.account_snapshot(OKX(), position_snapshot=positions))
    assert snapshot["equity_usdt"] == 11.01
    assert snapshot["available_usdt"] == 10.5
    assert snapshot["unrealized_pnl_usdt"] == 0.3
    assert snapshot["reserved_margin_usdt"] == 0.35
    assert snapshot["source"] == "okx_authenticated_account"


def test_fast_operational_snapshot_times_out_without_fabricating_values(monkeypatch):
    import telegram_backend

    telegram_backend._operational_exchange_cache.clear()

    class SlowOKX:
        async def get_account_snapshot(self):
            await asyncio.sleep(1)

    async def slow_positions():
        await asyncio.sleep(1)

    account, positions = asyncio.run(
        telegram_backend.fast_operational_exchange_snapshot(
            SlowOKX(), slow_positions, timeout_sec=0.01,
        )
    )
    assert account["equity_usdt"] is None
    assert account["source"] == "UNAVAILABLE"
    assert positions["source"] == "UNAVAILABLE"
    assert positions["age_sec"] is None


def test_fast_operational_snapshot_uses_timestamped_last_good(monkeypatch):
    import telegram_backend

    telegram_backend._operational_exchange_cache.clear()

    class OKX:
        async def get_account_snapshot(self):
            return {"total_eq_usdt": 11.0, "avail_eq_usdt": 10.0}

    async def positions():
        return {"positions": {}, "open_pnl_usdt": 0.0, "source": "EXCHANGE_TRUTH"}

    first_account, _ = asyncio.run(
        telegram_backend.fast_operational_exchange_snapshot(OKX(), positions, timeout_sec=1)
    )
    assert first_account["equity_usdt"] == 11.0

    class SlowOKX:
        async def get_account_snapshot(self):
            await asyncio.sleep(1)

    async def slow_positions():
        await asyncio.sleep(1)

    cached_account, cached_positions = asyncio.run(
        telegram_backend.fast_operational_exchange_snapshot(
            SlowOKX(), slow_positions, timeout_sec=0.01,
        )
    )
    assert cached_account["equity_usdt"] == 11.0
    assert cached_account["stale"] is True
    assert cached_account["last_good_at"]
    assert cached_positions["stale"] is True


def test_application_allows_concurrent_telegram_updates():
    import bot

    app = bot.build_application(eng=bot.engine)
    assert app.update_processor.max_concurrent_updates == 8


def test_startup_makes_telegram_available_before_runtime_warmup():
    source = Path("bot.py").read_text(encoding="utf-8")
    post_init = source[source.index("async def _post_init"):source.index("async def _post_shutdown")]
    assert "_tg_control_plane.start()" in post_init
    assert "asyncio.create_task(_runtime_startup" in post_init
    assert "await engine.start_all()" not in post_init


def test_visible_operator_rooms_have_backend_callbacks():
    import bot
    import tg_panels

    callback_source = Path("bot.py").read_text(encoding="utf-8")
    expected = {
        "main_menu", "terminal:status", "terminal:account", "terminal:positions",
        "terminal:opportunities", "terminal:engines", "terminal:execution",
        "terminal:risk", "terminal:exits", "terminal:liquidity",
        "terminal:performance", "terminal:research", "terminal:system",
        "terminal:why_no_trade", "terminal:recent_events", "terminal:emergency",
        "live:disarm", "live:halt_confirm", "live:emergency_confirm",
    }
    home_callbacks = {
        button.callback_data
        for row in tg_panels.kb_home(True).inline_keyboard
        for button in row if button.callback_data
    }
    assert {"terminal:status", "terminal:account", "terminal:positions", "terminal:emergency"} <= home_callbacks
    assert not any("paper" in value.lower() for value in home_callbacks)
    for callback in expected:
        assert callback in callback_source


def test_cooperative_scan_batch_is_config_driven_and_bounded():
    settings = json.loads(Path("data/settings.json").read_text(encoding="utf-8"))
    source = Path("category_engine.py").read_text(encoding="utf-8")
    assert settings["engine_eval_cooperative_batch_size"] == 4
    assert 'get_val("engine_eval_cooperative_batch_size", 4)' in source
    assert source.count("await asyncio.sleep(0)") >= 4


def test_safety_action_remains_schedulable_during_candidate_burst(tmp_path):
    from telegram_control_plane import TelegramControlPlane

    async def run():
        tracker = TelegramControlPlane(tmp_path / "latency.json")

        async def candidate():
            for _ in range(5):
                sum(i * i for i in range(100))
                await asyncio.sleep(0)

        tasks = [asyncio.create_task(candidate()) for _ in range(280)]
        started = time.perf_counter()
        token = tracker.begin("button", "live:disarm")
        tracker.dispatch(token)
        await asyncio.sleep(0)
        tracker.backend_complete(token)
        tracker.sent(token)
        elapsed = time.perf_counter() - started
        await asyncio.gather(*tasks)
        return elapsed, tracker.snapshot()

    elapsed, snapshot = asyncio.run(run())
    assert elapsed < 0.5
    assert snapshot["latency_by_action"]["live:disarm"]["p95_ms"] < 500.0


def test_mode_banner_uses_authoritative_live_state(monkeypatch):
    import bot
    import live_state

    monkeypatch.setattr(bot, "PAPER_TRADING", False)
    monkeypatch.setattr(live_state, "state", SimpleNamespace(snapshot=lambda: {"state": "LIVE_ARMED", "armed": True}))
    rendered = bot._with_mode_banner("Settings")
    assert "LIVE ARMED" in rendered.splitlines()[0]


def test_runtime_snapshot_does_not_report_paper_mode(monkeypatch):
    import telegram_backend

    monkeypatch.setenv("PAPER_TRADING", "false")
    assert os.getenv("PAPER_TRADING") == "false"
    # The production backend is deliberately separate from paper-validation stores.
    assert "paper_validation" not in telegram_backend.runtime_snapshot.__code__.co_names


def test_live_state_read_only_import_does_not_rewrite_shared_file(tmp_path, monkeypatch):
    import live_state

    monkeypatch.setenv("PAPER_TRADING", "false")
    path = tmp_path / "live_state.json"
    original = {
        "state": "LIVE_ARMED",
        "armed": True,
        "updated_at": 123.0,
        "reason": "operator",
    }
    path.write_text(json.dumps(original), encoding="utf-8")
    loaded = live_state.LiveStateMachine(path)
    assert loaded.snapshot()["state"] == "LIVE_READY_UNARMED"
    assert json.loads(path.read_text(encoding="utf-8")) == original

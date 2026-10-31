import asyncio

import category_engine
import entry_filter
import position_registry
import risk_manager


def _open_trade(direction="long", size=100.0, entry=100.0, stop=97.5):
    return {
        "direction": direction,
        "engine": "TEST",
        "size_usdt": size,
        "entry": entry,
        "stop_loss": stop,
        "leverage": 10,
    }


def _isolate_registry(monkeypatch, tmp_path):
    position_registry._registry.clear()
    monkeypatch.setattr(position_registry, "_PATH", tmp_path / "position_registry.json")


def test_global_slot_limit_allows_fifteen_and_blocks_sixteenth(monkeypatch):
    position_registry._registry.clear()
    category_engine._registry.clear()
    monkeypatch.setattr(category_engine._s, "get_val", lambda key, default=None: {
        "max_open_positions": 15,
        "global_slot_mode": "fixed",
        "v6_enabled": False,
    }.get(key, default))
    monkeypatch.setattr(category_engine, "_per_engine_cap_enabled", lambda: False)
    monkeypatch.setattr(
        category_engine.CategoryEngine,
        "_portfolio_gate",
        lambda self, *args, **kwargs: (True, "OK"),
    )

    engine = category_engine.CategoryEngine("AMATERASU")
    for idx in range(14):
        position_registry._registry[f"C{idx}"] = _open_trade()

    tag, reason = asyncio.run(engine._recheck_live_entry_constraints(
        "NEW15", "long", 500.0, 25.0, 3.0, lane_override=True
    ))
    assert tag == ""
    assert reason == "ok"

    position_registry._registry["C14"] = _open_trade()
    tag, reason = asyncio.run(engine._recheck_live_entry_constraints(
        "NEW16", "long", 500.0, 25.0, 3.0, lane_override=True
    ))
    assert tag == "global_cap"
    assert "15/15" in reason


def test_aggregate_open_risk_sums_multiple_positions(monkeypatch):
    open_trades = {
        "A": _open_trade(size=200.0, entry=100.0, stop=95.0),
        "B": _open_trade(size=200.0, entry=100.0, stop=95.0),
        "C": _open_trade(size=100.0, entry=50.0, stop=47.5),
    }
    assert risk_manager.total_open_risk_pct(open_trades, 500.0) == 5.0

    monkeypatch.setattr(entry_filter._s, "get_val", lambda key, default=None: {
        "rule_357_max_open_risk_pct": 5.0,
    }.get(key, default))
    allowed, reason = entry_filter._check_357(500.0, open_trades)
    assert allowed is False
    assert "open risk 5.0%" in reason


def test_daily_loss_halt_includes_aggregate_open_unrealized_pnl(monkeypatch):
    test_risk = risk_manager.RiskManager()
    monkeypatch.setattr(risk_manager._perf, "get_today_realized_net_pnl", lambda: 0.0)
    monkeypatch.setattr(risk_manager._s, "is_daily_cap_enabled", lambda: True)
    monkeypatch.setattr(risk_manager._s, "get_val", lambda key, default=None: {
        "daily_loss_pct": 8.0,
        "rule_357_enabled": False,
    }.get(key, default))

    open_trades = {
        "AAA": _open_trade(direction="long", size=200.0, entry=100.0),
        "BBB": _open_trade(direction="long", size=200.0, entry=100.0),
    }
    monkeypatch.setattr(risk_manager, "aggregate_unrealized_pnl_usdt", lambda trades: -45.0)

    result = test_risk.validate_trade("CCC", "buy", 500.0, open_trades=open_trades)
    assert result["valid"] is False
    assert "Daily loss cap" in result["reason"]

    status = test_risk.daily_cap_status(500.0, open_trades=open_trades)
    assert status["halted"] is True
    assert status["open_unrealized_pnl_usdt"] == -45.0
    assert status["total_pnl_usdt"] == -45.0
    assert status["loss_pct"] == 9.0


def test_registry_candidate_projected_risk_allows_exact_cap(monkeypatch, tmp_path):
    _isolate_registry(monkeypatch, tmp_path)
    monkeypatch.setattr(position_registry._s, "get_val", lambda key, default=None: {
        "rule_357_enabled": True,
        "rule_357_max_open_risk_pct": 12.0,
        "min_sl_dist_pct": 1.0,
    }.get(key, default))
    position_registry._registry["OPEN"] = _open_trade(size=110.0, entry=100.0, stop=50.0)

    ok, reason = asyncio.run(position_registry.can_open_and_reserve(
        "EXACT",
        "long",
        "TEST",
        10.0,
        entry=100.0,
        stop_loss=50.0,
        balance_usdt=500.0,
    ))

    assert ok is True, reason
    assert position_registry._registry["EXACT"]["rule_357_projected_risk_pct"] == 12.0


def test_registry_candidate_projected_risk_blocks_over_cap(monkeypatch, tmp_path):
    _isolate_registry(monkeypatch, tmp_path)
    monkeypatch.setattr(position_registry._s, "get_val", lambda key, default=None: {
        "rule_357_enabled": True,
        "rule_357_max_open_risk_pct": 12.0,
        "min_sl_dist_pct": 1.0,
    }.get(key, default))
    position_registry._registry["OPEN"] = _open_trade(size=110.0, entry=100.0, stop=50.0)

    ok, reason = asyncio.run(position_registry.can_open_and_reserve(
        "OVER",
        "long",
        "TEST",
        20.0,
        entry=100.0,
        stop_loss=50.0,
        balance_usdt=500.0,
    ))

    assert ok is False
    assert "projected open risk 13.00%" in reason
    assert "OVER" not in position_registry._registry


def test_registry_candidate_projected_risk_is_atomic_for_concurrent_reserves(monkeypatch, tmp_path):
    _isolate_registry(monkeypatch, tmp_path)
    monkeypatch.setattr(position_registry._s, "get_val", lambda key, default=None: {
        "rule_357_enabled": True,
        "rule_357_max_open_risk_pct": 12.0,
        "min_sl_dist_pct": 1.0,
    }.get(key, default))
    position_registry._registry["OPEN"] = _open_trade(size=110.0, entry=100.0, stop=50.0)

    async def reserve(symbol):
        return await position_registry.can_open_and_reserve(
            symbol,
            "long",
            "TEST",
            10.0,
            entry=100.0,
            stop_loss=50.0,
            balance_usdt=500.0,
        )

    async def run_both():
        return await asyncio.gather(reserve("RACE1"), reserve("RACE2"))

    results = asyncio.run(run_both())

    admitted = [r for r in results if r[0]]
    blocked = [r for r in results if not r[0]]
    assert len(admitted) == 1
    assert len(blocked) == 1
    assert len([s for s in ("RACE1", "RACE2") if s in position_registry._registry]) == 1
    assert "projected open risk 13.00%" in blocked[0][1]


def test_timeout_cleanup_releases_only_uncommitted_pending_registry_rows(monkeypatch, tmp_path):
    _isolate_registry(monkeypatch, tmp_path)
    position_registry._registry.update({
        "ORPHAN": {
            **_open_trade(), "engine": "AMATERASU", "pending_reservation": True,
            "entry_lifecycle": "PRE_SUBMIT_RESERVED", "mutation_attempted": False,
            "entry": {"symbol": "ORPHAN"}, "order_id": "",
        },
        "PROTECTED": {
            **_open_trade(), "engine": "AMATERASU", "pending_reservation": True,
            "entry_lifecycle": "PRE_SUBMIT_RESERVED", "mutation_attempted": False,
            "entry": {"symbol": "PROTECTED"}, "order_id": "",
        },
        "CONFIRMED": {
            **_open_trade(), "engine": "AMATERASU", "pending_reservation": False, "order_id": "123",
        },
        "OTHER": {
            **_open_trade(), "engine": "HAYATE", "pending_reservation": True,
            "entry_lifecycle": "PRE_SUBMIT_RESERVED", "mutation_attempted": False,
            "entry": {"symbol": "OTHER"}, "order_id": "",
        },
    })

    removed = asyncio.run(position_registry.release_uncommitted_pending_for_engine(
        "AMATERASU", protected_coins={"PROTECTED"}
    ))

    assert removed == ["ORPHAN"]
    assert set(position_registry._registry) == {"PROTECTED", "CONFIRMED", "OTHER"}

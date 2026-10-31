import asyncio
import time
from types import SimpleNamespace

import pytest

import category_engine
import execution_router
import exit_manager
import trading_engine
from leverage_truth import (
    LeverageTruthCache,
    configured_from_rows,
    position_effective_from_rows,
    select_configured_row,
)


def _rows():
    return [
        {"instId": "XRP-USDT-SWAP", "posSide": "long", "mgnMode": "isolated", "lever": "15"},
        {"instId": "XRP-USDT-SWAP", "posSide": "short", "mgnMode": "isolated", "lever": "12"},
        {"instId": "BTC-USDT-SWAP", "posSide": "short", "mgnMode": "isolated", "lever": "8"},
    ]


def test_configured_leverage_is_per_instrument_side_and_mode():
    rows = _rows()
    assert configured_from_rows(rows, "XRP-USDT-SWAP", "short") == 12
    assert configured_from_rows(rows, "XRP-USDT-SWAP", "long") == 15
    assert configured_from_rows(rows, "BTC-USDT-SWAP", "short") == 8
    assert select_configured_row(rows, "XRP-USDT-SWAP", "short")["mgnMode"] == "isolated"


def test_position_effective_leverage_uses_filled_position_truth():
    rows = [{"instId": "XRP-USDT-SWAP", "posSide": "short", "pos": "0.03", "lever": "12"}]
    assert position_effective_from_rows(rows, "XRP-USDT-SWAP", "short") == 12
    assert position_effective_from_rows(rows, "XRP-USDT-SWAP", "long") is None


class _LeverageReader:
    def __init__(self):
        self.calls = 0

    async def get_leverage_info(self, instrument, margin_mode="isolated"):
        self.calls += 1
        await asyncio.sleep(0.01)
        return [{"instId": instrument, "posSide": "short", "mgnMode": margin_mode, "lever": "12"}]


def test_cache_reuses_fresh_value_and_coalesces_same_key():
    async def run():
        client = _LeverageReader()
        cache = LeverageTruthCache(max_age_sec=1.0)
        values = await asyncio.gather(*[
            cache.resolve(client, "XRP-USDT-SWAP", "short", "isolated", 15)
            for _ in range(10)
        ])
        assert client.calls == 1
        assert cache.stats()["underlying_reads"] == 1
        assert all(value.exchange_configured_leverage == 12 for value in values)
        assert all(value.requested_leverage == 15 for value in values)
        cached = await cache.resolve(client, "XRP-USDT-SWAP", "short", "isolated", 15)
        assert cached.source == "okx.account.leverage-info"
        assert client.calls == 1

    asyncio.run(run())


def test_cached_exchange_fact_recomputes_mismatch_for_each_requester():
    async def run():
        client = _LeverageReader()
        cache = LeverageTruthCache(max_age_sec=1.0)
        first = await cache.resolve(client, "XRP-USDT-SWAP", "short", "isolated", 15)
        second = await cache.resolve(client, "XRP-USDT-SWAP", "short", "isolated", 12)
        third = await cache.resolve(client, "XRP-USDT-SWAP", "short", "isolated", 10)
        assert first.exchange_configured_leverage == 12
        assert first.mismatch is True
        assert second.mismatch is False
        assert third.mismatch is True
        assert client.calls == 1
        cached_fact = next(iter(cache._values.values()))
        assert not hasattr(cached_fact, "requested_leverage")

    asyncio.run(run())


def test_cache_isolates_side_and_instrument_keys():
    class Reader:
        def __init__(self):
            self.calls = 0
        async def get_leverage_info(self, instrument, margin_mode="isolated"):
            self.calls += 1
            lever = "9" if margin_mode == "cross" else "12"
            return [
                {"instId": instrument, "posSide": "long", "mgnMode": margin_mode, "lever": "15"},
                {"instId": instrument, "posSide": "short", "mgnMode": margin_mode, "lever": lever},
            ]

    async def run():
        reader = Reader()
        cache = LeverageTruthCache(max_age_sec=1.0)
        short = await cache.resolve(reader, "XRP-USDT-SWAP", "short", "isolated", 15)
        long = await cache.resolve(reader, "XRP-USDT-SWAP", "long", "isolated", 15)
        other = await cache.resolve(reader, "BTC-USDT-SWAP", "short", "isolated", 15)
        assert short.exchange_configured_leverage == 12
        assert long.exchange_configured_leverage == 15
        assert other.exchange_configured_leverage == 12
        cross = await cache.resolve(reader, "XRP-USDT-SWAP", "short", "cross", 9)
        assert cross.exchange_configured_leverage == 9
        assert reader.calls == 4

    asyncio.run(run())


def test_failed_leverage_read_does_not_poison_cache():
    class Reader:
        def __init__(self):
            self.calls = 0
        async def get_leverage_info(self, instrument, margin_mode="isolated"):
            self.calls += 1
            if self.calls == 1:
                raise TimeoutError("temporary")
            return [{"instId": instrument, "posSide": "short", "mgnMode": margin_mode, "lever": "12"}]

    async def run():
        reader = Reader()
        cache = LeverageTruthCache(max_age_sec=30.0)
        with pytest.raises(Exception):
            await cache.resolve(reader, "XRP-USDT-SWAP", "short")
        value = await cache.resolve(reader, "XRP-USDT-SWAP", "short")
        assert value.exchange_configured_leverage == 12
        assert reader.calls == 2

    asyncio.run(run())


def test_cache_rejects_stale_value_at_boundary():
    async def run():
        client = _LeverageReader()
        cache = LeverageTruthCache(max_age_sec=0.01)
        await cache.resolve(client, "XRP-USDT-SWAP", "short", "isolated", 15)
        await asyncio.sleep(0.12)
        await cache.resolve(client, "XRP-USDT-SWAP", "short", "isolated", 15)
        assert client.calls == 2

    asyncio.run(run())


def test_xrp_roi_uses_effective_12x_not_requested_15x():
    plan = exit_manager.ExitPlan(
        coin="XRP", direction="short", engine_name="AMATERASU",
        entry_price=0.9995, size_usdt=3.0, atr_val=0.0,
        hard_sl=1.0091, tp1_price=0.0, tp2_price=0.0,
        leverage=15, requested_leverage=15,
        exchange_configured_leverage=12, position_effective_leverage=12,
        leverage_truth_status="POSITION_EFFECTIVE",
    )
    roi = exit_manager._roi_pct(plan, 0.9988)
    assert roi == pytest.approx(0.8404, rel=1e-3)
    assert roi < 1.1


def test_legacy_profit_lock_does_not_use_requested_leverage_when_effective_is_unknown():
    plan = exit_manager.ExitPlan(
        coin="XRP", direction="long", engine_name="AMATERASU",
        entry_price=1.0, size_usdt=3.0, atr_val=0.0,
        hard_sl=0.99, tp1_price=0.0, tp2_price=0.0,
        leverage=15, requested_leverage=15,
        exchange_configured_leverage=12,
        position_effective_leverage=None,
        leverage_truth_status="LEVERAGE_UNKNOWN",
    )
    exit_manager._apply_legacy_profit_lock("XRP", plan, 1.02, True)
    assert plan.hard_sl == pytest.approx(0.99)


def test_raw_mfe_mae_are_price_based_and_record_observation_leverage():
    plan = exit_manager.ExitPlan(
        coin="XRP", direction="short", engine_name="AMATERASU",
        entry_price=0.9995, size_usdt=3.0, atr_val=0.0,
        hard_sl=1.0091, tp1_price=0.0, tp2_price=0.0,
        leverage=15, requested_leverage=15,
        position_effective_leverage=12, leverage_truth_status="POSITION_EFFECTIVE",
        initial_size_usdt=3.0,
    )
    exit_manager._update_lifecycle_telemetry(plan, 0.9988, False)
    exit_manager._update_lifecycle_telemetry(plan, 1.0005, False)
    assert plan.mfe_raw_move_pct == pytest.approx((0.9995 - 0.9988) / 0.9995 * 100, rel=1e-6)
    assert plan.mae_raw_move_pct == pytest.approx((0.9995 - 1.0005) / 0.9995 * 100, rel=1e-6)
    assert plan.mfe_observed_leverage == 12
    assert plan.mae_observed_leverage == 12


def test_xrp_mfe_recomputes_when_effective_leverage_changes():
    plan = exit_manager.ExitPlan(
        coin="XRP", direction="short", engine_name="AMATERASU",
        entry_price=0.9995, size_usdt=3.0, atr_val=0.0,
        hard_sl=1.0091, tp1_price=0.0, tp2_price=0.0,
        leverage=15, requested_leverage=15,
        exchange_configured_leverage=15, position_effective_leverage=15,
        leverage_truth_status="POSITION_EFFECTIVE", initial_size_usdt=3.0,
    )
    exit_manager._update_lifecycle_telemetry(plan, 0.9988, False)
    assert exit_manager._current_mfe_roi(plan) == pytest.approx(1.0505, rel=1e-3)
    plan.position_effective_leverage = 12
    plan.exchange_configured_leverage = 12
    plan.mfe_current_roi = exit_manager._current_mfe_roi(plan)
    assert plan.mfe_raw_move_pct == pytest.approx((0.9995 - 0.9988) / 0.9995 * 100, rel=1e-6)
    assert plan.mfe_current_roi == pytest.approx(0.8404, rel=1e-3)
    assert plan.mfe_current_roi < 1.1
    plan.position_effective_leverage = 15
    assert exit_manager._current_mfe_roi(plan) == pytest.approx(1.0505, rel=1e-3)


def test_restore_with_missing_effective_leverage_fails_roi_closed(monkeypatch):
    class Engine:
        def is_enabled(self):
            return True

    engine = object.__new__(trading_engine.MultiEngine)
    engine.engines = {"AMATERASU": Engine()}
    coin = "XRP-RESTORE-UNKNOWN"
    trading_engine._em.remove(coin)
    details = {
        "exposure_truth_status": "ACTIVE_POSITION",
        "recovery_mode": "managed",
        "engine": "AMATERASU",
        "direction": "short",
        "entry": 0.9995,
        "size_usdt": 3.0,
        "leverage": 15,
        "requested_leverage": 15,
        "exchange_configured_leverage": 15,
        "stop_loss": 1.01,
    }
    asyncio.run(engine._register_exit_plan(coin, "AMATERASU", details, live={"contracts": 1.0}))
    plan = trading_engine._em.get_plan(coin)
    assert plan is not None
    assert plan.position_effective_leverage is None
    assert plan.leverage_truth_status == "LEVERAGE_UNKNOWN"
    assert exit_manager._position_leverage(plan) == 0
    assert exit_manager._current_mfe_roi(plan) == 0.0
    trading_engine._em.remove(coin)


def test_effective_value_with_unknown_status_still_fails_closed():
    plan = exit_manager.ExitPlan(
        coin="XRP-STATUS-MISMATCH",
        engine_name="AMATERASU",
        direction="short",
        entry_price=1.0,
        size_usdt=3.0,
        atr_val=0.0,
        hard_sl=1.01,
        tp1_price=0.0,
        tp2_price=0.0,
        leverage=15,
        requested_leverage=15,
        exchange_configured_leverage=15,
        position_effective_leverage=12,
        leverage_truth_status="UNKNOWN",
    )
    assert exit_manager._position_leverage(plan) == 0
    plan.leverage_truth_status = "POSITION_EFFECTIVE"
    assert exit_manager._position_leverage(plan) == 12


def test_category_restore_live_missing_effective_leverage_is_not_legacy_fallback(monkeypatch):
    monkeypatch.setenv("PAPER_TRADING", "false")
    exit_manager._plans.clear()
    engine = category_engine.CategoryEngine.__new__(category_engine.CategoryEngine)
    engine.name = "AMATERASU"
    engine._open = {}
    details = {
        "exposure_truth_status": "ACTIVE_POSITION",
        "direction": "short", "entry": 0.9995, "size_usdt": 3.0,
        "leverage": 15, "requested_leverage": 15,
        "exchange_configured_leverage": 15,
        "stop_loss": 1.01, "mfe_price": 0.9988, "mae_price": 1.0005,
        "mfe_raw_move_pct": 0.070035, "mae_raw_move_pct": -0.10005,
    }
    try:
        assert engine.restore_position("XRP", details) is True
        plan = exit_manager.get_plan("XRP")
        assert plan is not None
        assert plan.leverage_truth_status == "LEVERAGE_UNKNOWN"
        assert plan.position_effective_leverage is None
        assert exit_manager._position_leverage(plan) == 0
        assert exit_manager._roi_pct(plan, 0.9988) == 0.0
        assert exit_manager._current_mfe_roi(plan) == 0.0
        assert plan.mfe_raw_move_pct == pytest.approx(0.070035)
        assert plan.mae_raw_move_pct == pytest.approx(-0.10005)
        assert plan.hard_sl == pytest.approx(1.01)
    finally:
        exit_manager._plans.clear()


def test_fresh_exchange_resolution_updates_unknown_registry_without_nameerror(monkeypatch, tmp_path):
    import position_registry

    monkeypatch.setenv("PAPER_TRADING", "false")
    monkeypatch.setattr(position_registry, "_PATH", tmp_path / "registry.json")
    monkeypatch.setattr(position_registry, "_registry", {
        "XRP": {
            "direction": "short", "engine": "AMATERASU", "size_usdt": 3.0,
            "entry": 0.9995, "leverage": 15, "requested_leverage": 15,
            "exchange_configured_leverage": 15,
            "exposure_truth_status": "EXPOSURE_UNKNOWN",
            "recovery_mode": "managed", "stop_loss": 1.01,
        }
    })

    class Client:
        async def get_positions_status(self):
            return {"ok": True, "positions": [{
                "instId": "XRP-USDT-SWAP", "posSide": "short", "pos": "0.4",
                "avgPx": "0.9995", "lever": "12",
            }]}

    class Engine:
        def __init__(self):
            self.positions = {}
        def is_enabled(self):
            return True
        def open_positions(self):
            return self.positions
        def restore_position(self, coin, details):
            self.positions[coin] = details
            return True
        async def close_position(self, coin, reason):
            return {"ok": True, "pnl": 0.0}

    engine = object.__new__(trading_engine.MultiEngine)
    engine.engines = {"AMATERASU": Engine()}
    monkeypatch.setattr(trading_engine, "OKXClient", Client)
    async def estimate(*args, **kwargs):
        return 3.0
    monkeypatch.setattr(engine, "_estimate_exchange_notional", estimate)
    trading_engine._em.remove("XRP")
    result = asyncio.run(engine.close_coin("XRP"))
    assert "Closed XRP" in result
    row = position_registry._registry["XRP"]
    assert row["exposure_truth_status"] == "ACTIVE_POSITION"
    assert row["position_effective_leverage"] == 12
    assert row["leverage_truth_timestamp"]
    assert row["leverage_truth_source"] == "okx.position"
    trading_engine._em.remove("XRP")


def test_fresh_zero_exchange_resolution_hands_off_without_active_leverage(monkeypatch, tmp_path):
    import position_registry

    monkeypatch.setenv("PAPER_TRADING", "false")
    monkeypatch.setattr(position_registry, "_PATH", tmp_path / "registry.json")
    monkeypatch.setattr(position_registry, "_registry", {
        "XRP": {
            "direction": "short", "engine": "AMATERASU", "size_usdt": 3.0,
            "entry": 0.9995, "leverage": 15,
            "exposure_truth_status": "EXPOSURE_UNKNOWN",
        }
    })

    class Client:
        async def get_positions_status(self):
            return {"ok": True, "positions": []}

    engine = object.__new__(trading_engine.MultiEngine)
    engine.engines = {}
    monkeypatch.setattr(trading_engine, "OKXClient", Client)
    result = asyncio.run(engine.close_coin("XRP"))
    assert "No close submitted" in result
    row = position_registry._registry["XRP"]
    assert row["exposure_truth_status"] == "EXCHANGE_CLOSED_AWAITING_RECONCILIATION"
    assert row.get("position_effective_leverage") in {None, 0}


def test_authoritative_configured_leverage_precedes_allocator(monkeypatch):
    import capital_allocator
    import instrument_registry

    monkeypatch.setenv("PAPER_TRADING", "false")
    events = []

    class Instrument:
        def eligible(self): return True, ""
        def stale(self): return False

    class Client:
        async def get_authoritative_leverage(self, *args, **kwargs):
            events.append("leverage")
            return {"exchange_configured_leverage": 12, "status": "EXCHANGE_CONFIGURED"}

    class Decision:
        allowed = True
        final_size_usdt = 2.9985
        reason = "ok"
        def metadata(self): return {"exchange_configured_leverage": 12}

    monkeypatch.setattr(instrument_registry.registry, "get", lambda *args, **kwargs: Instrument())
    monkeypatch.setattr(capital_allocator, "allocate_entry", lambda **kwargs: (events.append(f"allocate:{kwargs['leverage']}") or Decision()))
    result = asyncio.run(category_engine._adaptive_capital_allocation(
        coin="XRP", balance=10.0, size_usdt=2.9985, entry_px=1.0, stop_px=0.99,
        leverage=15, risk_meta={}, okx=Client(), side="short",
    ))
    assert result["skip"] is False
    assert events == ["leverage", "allocate:12"]


def test_exact_xrp_margin_uses_configured_12x():
    from instrument_registry import InstrumentMetadata

    instrument = InstrumentMetadata(
        inst_id="XRP-USDT-SWAP", inst_type="SWAP", settle_ccy="USDT",
        ct_val=1.0, ct_val_ccy="USDT", ct_type="linear", lot_sz=0.0001,
        min_sz=0.0001, tick_sz=0.0001, state="live",
    )
    contracts = 2.9985
    assert instrument.required_margin(contracts, 1.0, 12) == pytest.approx(0.249875)
    assert instrument.required_margin(contracts, 1.0, 15) == pytest.approx(0.1999)


def test_reservation_waits_for_leverage_resolution(monkeypatch):
    import capital_reservations
    import instrument_registry
    import opportunity_queue

    monkeypatch.setenv("PAPER_TRADING", "false")
    events = []

    class Instrument:
        min_sz = 0.0
        def notional_to_contracts(self, notional, price): return 2.9985
        def contracts_to_notional(self, contracts, price): return float(contracts)
        def required_margin(self, contracts, price, leverage):
            events.append(f"margin:{leverage}")
            return float(contracts) / leverage
        def round_price(self, price, **kwargs): return price
        def contract_notional(self, price): return 1.0

    class Client:
        async def get_authoritative_leverage(self, *args, **kwargs):
            events.append("leverage")
            return {"exchange_configured_leverage": 12, "status": "EXCHANGE_CONFIGURED"}
        def _set_last_order_skip_reason(self, reason): pass

    async def reserve(**kwargs):
        events.append("reserve")
        return False, "capacity", "rid"

    monkeypatch.setattr(instrument_registry.registry, "require", lambda *args, **kwargs: Instrument())
    monkeypatch.setattr(capital_reservations.book, "reserve", reserve)
    monkeypatch.setattr(opportunity_queue.queue, "enqueue", lambda *args, **kwargs: None)
    result = asyncio.run(category_engine._execute_central_entry(
        okx=Client(), engine_name="AMATERASU", coin="XRP", side="sell",
        size_usdt=2.9985, entry_px=1.0, stop_px=1.01, take_profit=0.98,
        leverage=15, allocator_meta={"allocator_approved_contracts": 2.9985,
        "allocator_approved_risk": 0.02, "allocator_deployable_margin": 10.0,
        "allocator_currently_deployed_margin": 0.0},
        execution_plan=SimpleNamespace(mode="market"),
    ))
    assert result is None
    assert events[:2] == ["leverage", "margin:12"]
    assert events[-1] == "reserve"


class _RouterTruthClient:
    async def get_authoritative_leverage(self, coin, side, **kwargs):
        return {
            "instrument": "XRP-USDT-SWAP",
            "pos_side": side,
            "exchange_configured_leverage": 12,
            "status": "EXCHANGE_CONFIGURED",
            "timestamp": "2026-08-14T00:00:00+00:00",
            "source": "test",
        }

    async def set_leverage(self, *args, **kwargs):
        raise AssertionError("entry path must not mutate leverage")


def test_router_enriches_requested_and_configured_leverage_without_mutation(monkeypatch):
    monkeypatch.setenv("PAPER_TRADING", "false")
    request = execution_router.ExecutionRequest(
        intent_id="i", candidate_id="c", reservation_id="r", mode="Guarded", engine="AMATERASU",
        coin="XRP", direction="short", requested_contracts=1, approved_contracts=1,
        approved_notional_usdt=3.0, order_type="market", leverage=15,
    )
    router = object.__new__(execution_router.ExecutionRouter)
    router.client = _RouterTruthClient()
    router._locks = {}
    router.reservations = None
    router.orders = None
    router.instruments = None
    router.live_state = None
    enriched = asyncio.run(router._enrich_leverage_truth(request))
    assert enriched.requested_leverage == 15
    assert enriched.exchange_configured_leverage == 12
    assert enriched.leverage == 12
    assert enriched.leverage_truth_source == "test"
    assert enriched.metadata["leverage_truth_mismatch"] is True
    assert enriched.requested_vs_configured_mismatch is True
    assert enriched.configured_vs_effective_mismatch is False
    assert enriched.metadata["requested_vs_configured_mismatch"] is True
    assert enriched.metadata["configured_vs_effective_mismatch"] is False


def test_execution_result_exposes_three_leverage_values():
    result = execution_router.ExecutionResult(
        ok=True, reason="filled", intent_id="i", client_order_id="c",
        requested_leverage=15, exchange_configured_leverage=12,
        position_effective_leverage=12, leverage_truth_status="POSITION_EFFECTIVE",
        leverage_truth_source="okx.position", leverage_truth_mismatch=True,
        requested_vs_configured_mismatch=True,
        configured_vs_effective_mismatch=False,
    )
    row = result.legacy_response()
    assert row["requested_leverage"] == 15
    assert row["exchange_configured_leverage"] == 12
    assert row["position_effective_leverage"] == 12
    assert row["effective_leverage"] == 12
    assert row["leverage_truth_source"] == "okx.position"
    assert row["leverage_truth_mismatch"] is True
    assert row["requested_vs_configured_mismatch"] is True
    assert row["configured_vs_effective_mismatch"] is False

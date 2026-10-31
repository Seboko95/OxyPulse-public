import asyncio

import okx_client
import trading_engine


class _FakeOKX:
    def __init__(self, result):
        self._result = result

    async def auth_preflight(self):
        return self._result


class _DummyEngine:
    def __init__(self):
        self.running = False
        self.started = False

    async def start(self):
        self.started = True
        self.running = True


class _FakeTask:
    def done(self):
        return False


def _make_engine(monkeypatch, preflight_result, paper_mode):
    eng = trading_engine.MultiEngine.__new__(trading_engine.MultiEngine)
    eng.running = False
    eng.research_only = False
    eng.engines = {"TEST": _DummyEngine()}
    eng._scan_task = None

    async def fake_scan_loop():
        return None

    async def fake_sync_positions(*args, **kwargs):
        return "sync ok"

    async def fake_start_monitor(*args, **kwargs):
        return None

    async def fake_universe():
        return []

    async def fake_ws_start(*args, **kwargs):
        return None

    def fake_create_task(coro, **_kwargs):
        if hasattr(coro, "close"):
            coro.close()
        return _FakeTask()

    monkeypatch.setattr(trading_engine, "OKXClient", lambda: _FakeOKX(preflight_result))
    monkeypatch.setattr(trading_engine._okx_mod, "PAPER_TRADING", paper_mode)
    monkeypatch.setattr(trading_engine._em, "start_monitor", fake_start_monitor)
    monkeypatch.setattr(trading_engine, "get_futures_universe", fake_universe)
    monkeypatch.setattr(trading_engine._ws_stream, "start", fake_ws_start)
    monkeypatch.setattr(trading_engine.asyncio, "create_task", fake_create_task)
    monkeypatch.setattr(eng, "_scan_loop", fake_scan_loop)
    monkeypatch.setattr(eng, "sync_positions", fake_sync_positions)
    return eng


def _bootstrap_engine(monkeypatch, unresolved, events, *, recovery_rows=None, readiness=(True, "")):
    """Build an unarmed live start whose recovery/preflight order is observable."""
    import controlled_lifecycle
    import execution_router
    import live_preflight
    import live_state

    class Client(_FakeOKX):
        async def mutation_transport_ready(self):
            events.append("readiness")
            return readiness

    class Store:
        def __init__(self, rows):
            self.rows = list(rows)

        def unresolved(self):
            return list(self.rows)

    class RecoveryRouter:
        async def recover_unresolved(self):
            events.append("recovery")
            if recovery_rows is not None:
                store.rows = list(recovery_rows)
            return []

    class State:
        def __init__(self):
            self.recorded_preflights = []

        def record_preflight(self, _ok, _reason):
            self.recorded_preflights.append((bool(_ok), str(_reason)))

        def can_submit_live(self):
            return False, "operator_unarmed"

        def snapshot(self):
            return {"state": "LIVE_READY_UNARMED", "armed": False}

    store = Store(unresolved)
    state = State()
    eng = _make_engine(monkeypatch, {"ok": True, "reason": ""}, paper_mode=False)
    client = Client({"ok": True, "reason": ""})
    monkeypatch.setattr(trading_engine, "OKXClient", lambda: client)
    monkeypatch.setattr(execution_router, "router_for", lambda _client: RecoveryRouter())
    monkeypatch.setattr(live_preflight, "order_store", store)
    monkeypatch.setattr(live_state, "state", state)
    eng._test_live_state = state
    monkeypatch.setattr(controlled_lifecycle.gate, "can_auto_arm", lambda: False)
    monkeypatch.setattr(trading_engine._regime, "update_regime", lambda *_args: asyncio.sleep(0))

    async def full_preflight(*_args, **_kwargs):
        events.append("preflight")
        return {"ok": not store.unresolved(), "checks": {"no_unresolved_orders": {"ok": not store.unresolved()}}}

    monkeypatch.setattr(live_preflight, "run_preflight", full_preflight)
    return eng, store


def test_start_all_refuses_live_when_auth_preflight_fails(monkeypatch):
    eng = _make_engine(
        monkeypatch,
        {"ok": False, "reason": "balance code=401 reason=Invalid Sign"},
        paper_mode=False,
    )

    result = asyncio.run(eng.start_all())

    assert "Refusing to start live trading" in result
    assert eng.running is False
    assert eng.engines["TEST"].started is False


def test_bootstrap_recovery_runs_before_no_unresolved_preflight(monkeypatch):
    events = []
    eng, store = _bootstrap_engine(
        monkeypatch,
        [{"client_order_id": "ETH"}, {"client_order_id": "XRP"}],
        events,
        recovery_rows=[],
    )

    result = asyncio.run(eng.start_all())

    assert events == ["recovery", "preflight", "readiness"]
    assert store.unresolved() == []
    assert eng.running is True
    assert eng.research_only is True
    assert "PRODUCTION_RESEARCH_ONLY" in result


def test_bootstrap_recovery_leaves_remaining_uncertainty_fail_closed(monkeypatch):
    events = []
    eng, store = _bootstrap_engine(
        monkeypatch,
        [{"client_order_id": "ETH"}, {"client_order_id": "XRP"}],
        events,
        recovery_rows=[{"client_order_id": "XRP"}],
    )

    result = asyncio.run(eng.start_all())

    assert events == ["recovery", "preflight"]
    assert len(store.unresolved()) == 1
    assert "full production preflight failed" in result
    assert eng.running is False


def test_bootstrap_readiness_failure_keeps_live_start_unarmed(monkeypatch):
    events = []
    eng, store = _bootstrap_engine(
        monkeypatch,
        [],
        events,
        recovery_rows=[],
        readiness=(False, "ConnectTimeout"),
    )

    result = asyncio.run(eng.start_all())

    assert events == ["recovery", "preflight", "readiness"]
    assert store.unresolved() == []
    assert eng.running is True
    assert eng.research_only is True
    assert "PRODUCTION_RESEARCH_ONLY" in result
    trading_engine.os.environ.pop("OXYPULSE_PRODUCTION_RESEARCH_ONLY", None)


def test_successful_unarmed_start_persists_ready_after_readiness(monkeypatch):
    events = []
    eng, _store = _bootstrap_engine(monkeypatch, [], events, recovery_rows=[])

    result = asyncio.run(eng.start_all())

    assert eng.running is True
    assert eng.research_only is True
    assert eng._test_live_state.recorded_preflights == [(True, "startup_preflight_passed_unarmed")]
    assert "PRODUCTION_RESEARCH_ONLY" in result
    trading_engine.os.environ.pop("OXYPULSE_PRODUCTION_RESEARCH_ONLY", None)


def test_unarmed_start_refuses_when_ready_state_cannot_be_persisted(monkeypatch):
    events = []
    eng, _store = _bootstrap_engine(monkeypatch, [], events, recovery_rows=[])

    def fail_persist(_ok, _reason):
        raise OSError("state disk unavailable")

    eng._test_live_state.record_preflight = fail_persist
    result = asyncio.run(eng.start_all())

    assert eng.running is False
    assert "successful readiness state could not be persisted" in result


def test_start_all_allows_paper_when_auth_preflight_fails(monkeypatch):
    eng = _make_engine(
        monkeypatch,
        {"ok": False, "reason": "balance code=401 reason=Invalid Sign"},
        paper_mode=True,
    )

    result = asyncio.run(eng.start_all())

    assert "engines started" in result
    assert eng.running is True
    assert eng.engines["TEST"].started is True


def test_start_all_refuses_live_when_margin_mode_is_not_isolated(monkeypatch):
    eng = _make_engine(
        monkeypatch,
        {"ok": True, "reason": ""},
        paper_mode=False,
    )
    monkeypatch.setattr(trading_engine.mm, "td_mode", lambda: "cross")

    result = asyncio.run(eng.start_all())

    assert "margin mode must be isolated" in result
    assert eng.running is False
    assert eng.engines["TEST"].started is False


def test_start_all_allows_latched_unarmed_production_research(monkeypatch):
    import controlled_lifecycle
    import live_preflight
    import live_state

    eng = _make_engine(monkeypatch, {"ok": True, "reason": ""}, paper_mode=False)
    monkeypatch.setattr(trading_engine.mm, "td_mode", lambda: "isolated")

    async def full_preflight(*args, **kwargs):
        return {"ok": True}

    class State:
        def record_preflight(self, _ok, _reason):
            return {"state": "LIVE_READY_UNARMED", "armed": False}

        def can_submit_live(self):
            return False, "operator_unarmed"

        def snapshot(self):
            return {"state": "LIVE_READY_UNARMED", "armed": False}

    monkeypatch.setattr(live_preflight, "run_preflight", full_preflight)
    monkeypatch.setattr(live_state, "state", State())
    monkeypatch.setattr(controlled_lifecycle.gate, "can_auto_arm", lambda: False)

    result = asyncio.run(eng.start_all())

    assert eng.running is True
    assert eng.research_only is True
    assert eng.engines["TEST"].started is True
    assert "PRODUCTION_RESEARCH_ONLY" in result
    assert trading_engine.os.environ["OXYPULSE_PRODUCTION_RESEARCH_ONLY"] == "true"
    trading_engine.os.environ.pop("OXYPULSE_PRODUCTION_RESEARCH_ONLY", None)


def test_explicit_operator_startup_arm_is_consumed_and_execution_eligible(monkeypatch):
    import controlled_lifecycle
    import live_preflight
    import live_state

    eng = _make_engine(monkeypatch, {"ok": True, "reason": ""}, paper_mode=False)
    monkeypatch.setattr(trading_engine.mm, "td_mode", lambda: "isolated")

    async def full_preflight(*args, **kwargs):
        return {
            "ok": True,
            "checks": {
                key: {"ok": True} for key in (
                    "instrument_metadata", "capital_allocator", "execution_router",
                    "protection_manager", "pnl_reconciler", "no_unresolved_orders",
                    "no_unknown_positions",
                )
            },
        }

    class State:
        armed = False

        def arm(self, _preflight):
            self.armed = True
            return True, "armed"

        def can_submit_live(self):
            return (True, "OK") if self.armed else (False, "operator_unarmed")

        def snapshot(self):
            return {"state": "LIVE_ARMED" if self.armed else "LIVE_READY_UNARMED", "armed": self.armed}

    state = State()
    monkeypatch.setattr(live_preflight, "run_preflight", full_preflight)
    monkeypatch.setattr(live_state, "state", state)
    monkeypatch.setattr(controlled_lifecycle.gate, "can_auto_arm", lambda: False)
    monkeypatch.setenv("OXYPULSE_OPERATOR_LIVE_ARM_START", "true")

    result = asyncio.run(eng.start_all())

    assert eng.running is True
    assert eng.research_only is False
    assert state.armed is True
    assert "EXECUTION_ELIGIBLE" in result
    assert "OXYPULSE_OPERATOR_LIVE_ARM_START" not in trading_engine.os.environ
    assert "OXYPULSE_PRODUCTION_RESEARCH_ONLY" not in trading_engine.os.environ


def test_okx_live_market_order_refuses_cross_margin(monkeypatch):
    monkeypatch.setattr(okx_client, "PAPER_TRADING", False)
    monkeypatch.setattr(okx_client, "OKX_API_KEY", "present")
    monkeypatch.setattr(okx_client.mm, "td_mode", lambda: "cross")

    client = okx_client.OKXClient()

    async def fake_snapshot():
        return {"avail_eq_usdt": 500.0, "details": {}}

    async def fake_ticker(_coin):
        return {"last": 100.0}

    async def fake_set_leverage(*args, **kwargs):
        return {"effective_leverage": 10}

    monkeypatch.setattr(client, "get_account_snapshot", fake_snapshot)
    monkeypatch.setattr(client, "get_ticker", fake_ticker)
    monkeypatch.setattr(client, "set_leverage", fake_set_leverage)

    result = asyncio.run(client.place_market_order("BTC", "buy", amount_usdt=100.0, leverage=10))

    assert result is None
    assert any(
        "isolated_margin_required" in str(reason)
        for reason in getattr(client, "_order_skip_reasons", {}).values()
    )

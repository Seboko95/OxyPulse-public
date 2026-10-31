import asyncio
import json

import pytest


def _persisted(path):
    return json.loads(path.read_text(encoding="utf-8"))


def test_successful_unarmed_preflight_replaces_stale_failure_on_disk(tmp_path, monkeypatch):
    import live_state

    monkeypatch.setenv("PAPER_TRADING", "false")
    path = tmp_path / "live_state.json"
    path.write_text(
        json.dumps({"state": "LIVE_PREFLIGHT_FAILED", "armed": False, "reason": "old"}),
        encoding="utf-8",
    )

    state = live_state.LiveStateMachine(path)
    assert state.snapshot()["state"] == "LIVE_READY_UNARMED"

    result = state.record_preflight(True, "startup_preflight_passed_unarmed")

    assert result["state"] == "LIVE_READY_UNARMED"
    assert result["armed"] is False
    assert _persisted(path)["state"] == "LIVE_READY_UNARMED"
    assert _persisted(path)["armed"] is False


def test_restart_from_armed_state_persists_unarmed_only_after_success(tmp_path, monkeypatch):
    import live_state

    monkeypatch.setenv("PAPER_TRADING", "false")
    path = tmp_path / "live_state.json"
    path.write_text(
        json.dumps({"state": "LIVE_ARMED", "armed": True, "reason": "operator"}),
        encoding="utf-8",
    )

    state = live_state.LiveStateMachine(path)
    assert state.snapshot()["state"] == "LIVE_READY_UNARMED"
    assert state.snapshot()["armed"] is False

    state.record_preflight(True, "startup_preflight_passed_unarmed")
    persisted = _persisted(path)

    assert persisted["state"] == "LIVE_READY_UNARMED"
    assert persisted["armed"] is False
    assert persisted["state"] != "LIVE_ARMED"


def test_failed_preflight_remains_persisted(tmp_path, monkeypatch):
    import live_state

    monkeypatch.setenv("PAPER_TRADING", "false")
    path = tmp_path / "live_state.json"
    state = live_state.LiveStateMachine(path)

    state.record_preflight(False, "auth_failed")

    assert state.snapshot()["state"] == "LIVE_PREFLIGHT_FAILED"
    assert _persisted(path)["state"] == "LIVE_PREFLIGHT_FAILED"
    assert _persisted(path)["armed"] is False


def test_successful_readiness_persistence_failure_does_not_update_memory_or_arm(tmp_path, monkeypatch):
    import live_state

    monkeypatch.setenv("PAPER_TRADING", "false")
    path = tmp_path / "live_state.json"
    path.write_text(
        json.dumps({"state": "LIVE_PREFLIGHT_FAILED", "armed": False, "reason": "old"}),
        encoding="utf-8",
    )
    state = live_state.LiveStateMachine(path)
    before = state.snapshot()

    def fail_save(_data=None):
        raise OSError("disk unavailable")

    monkeypatch.setattr(state, "_save", fail_save)
    with pytest.raises(OSError):
        state.record_preflight(True, "startup_preflight_passed_unarmed")

    assert state.snapshot() == before
    assert _persisted(path)["state"] == "LIVE_PREFLIGHT_FAILED"
    assert state.can_submit_live() == (False, "live_not_armed:LIVE_READY_UNARMED")


def test_repeated_successful_unarmed_persistence_is_idempotent(tmp_path, monkeypatch):
    import live_state

    monkeypatch.setenv("PAPER_TRADING", "false")
    path = tmp_path / "live_state.json"
    state = live_state.LiveStateMachine(path)

    first = state.record_preflight(True, "startup_preflight_passed_unarmed")
    second = state.record_preflight(True, "startup_preflight_passed_unarmed")

    assert first["state"] == second["state"] == "LIVE_READY_UNARMED"
    assert first["armed"] is second["armed"] is False
    assert _persisted(path)["state"] == "LIVE_READY_UNARMED"


def test_terminal_legacy_rows_are_normalized_and_persisted(tmp_path, monkeypatch):
    import position_registry as registry

    old_path, old_registry = registry._PATH, registry._registry
    path = tmp_path / "position_registry.json"
    path.write_text(
        json.dumps(
            {
                "LINK": {"status": "closed", "direction": "long", "entry": 8.831},
                "ADA": {"status": "inactive", "direction": "short", "entry": 0.4},
                "SUI": {"status": "open", "direction": "long", "entry": 1.0, "size_usdt": 5.0},
                "AMBIG": {"status": "corrupt", "direction": "short"},
                "HANDOFF": {
                    "exposure_truth_status": registry.EXCHANGE_CLOSED_AWAITING_RECONCILIATION,
                    "direction": "long",
                },
            }
        ),
        encoding="utf-8",
    )
    registry._PATH = path
    registry._registry = {}
    try:
        registry._load()
        rows = registry.get_all_details()

        assert rows["LINK"]["exposure_truth_status"] == registry.EXPOSURE_TERMINAL
        assert rows["ADA"]["exposure_truth_status"] == registry.EXPOSURE_TERMINAL
        assert rows["SUI"]["exposure_truth_status"] == registry.EXPOSURE_ACTIVE
        assert rows["AMBIG"]["exposure_truth_status"] == registry.EXPOSURE_UNKNOWN
        assert registry.is_actionable_position(rows["LINK"]) is False
        assert registry.is_actionable_position(rows["HANDOFF"]) is False
        assert registry.count() == 2
        assert set(registry.get_active_details()) == {"SUI", "AMBIG"}
        assert set(registry.get_restorable_details()) == {"SUI"}
        assert "LINK" not in registry.get_nonterminal_details()
        assert "HANDOFF" in registry.get_nonterminal_details()
        assert _persisted(path)["LINK"]["exposure_truth_status"] == registry.EXPOSURE_TERMINAL

        # A terminal historical row cannot block a new admission for its symbol.
        allowed, _ = asyncio.run(
            registry.can_open_and_reserve(
                "LINK",
                "long",
                "TEST",
                size_usdt=1.0,
                entry=8.9,
                stop_loss=8.8,
                balance_usdt=500.0,
            )
        )
        assert allowed is True
    finally:
        registry._PATH, registry._registry = old_path, old_registry


def test_legacy_open_and_ambiguous_rows_remain_conservative(tmp_path, monkeypatch):
    import position_registry as registry

    old_path, old_registry = registry._PATH, registry._registry
    path = tmp_path / "position_registry.json"
    path.write_text(
        json.dumps(
            {
                "OPEN": {"status": "open", "direction": "long", "entry": 100.0},
                "UNKNOWN": {"status": "unknown", "direction": "short"},
            }
        ),
        encoding="utf-8",
    )
    registry._PATH = path
    registry._registry = {}
    try:
        registry._load()
        rows = registry.get_all_details()
        assert rows["OPEN"]["exposure_truth_status"] == registry.EXPOSURE_ACTIVE
        assert rows["UNKNOWN"]["exposure_truth_status"] == registry.EXPOSURE_UNKNOWN
        assert registry.is_open("OPEN") is True
        assert registry.is_open("UNKNOWN") is True
        assert registry.can_open("OPEN", "long")[0] is False
        assert registry.can_open("UNKNOWN", "short")[0] is False
    finally:
        registry._PATH, registry._registry = old_path, old_registry


def test_terminal_legacy_rows_are_not_restored_or_reported_as_open(monkeypatch):
    import os
    import position_registry as registry
    import trading_engine

    old_registry = registry._registry
    registry._registry = {
        "LINK": {
            "status": "closed",
            "direction": "long",
            "entry": 8.831,
            "size_usdt": 3.532,
        }
    }
    monkeypatch.setenv("PAPER_TRADING", "true")
    engine = trading_engine.MultiEngine.__new__(trading_engine.MultiEngine)
    engine.engines = {}
    engine.running = False
    engine._sync_failure_count = 0
    engine._last_sync_error = ""
    try:
        assert engine.all_open_positions() == []
        result = asyncio.run(engine.sync_positions(startup=True))
        assert "Registry: `0`" in result
        assert registry.get_all_details()["LINK"]["status"] == "closed"
    finally:
        registry._registry = old_registry


def test_local_unknown_exposure_fails_preflight_with_diagnostics(tmp_path, monkeypatch):
    import live_preflight
    import position_registry as registry

    old_path, old_registry = registry._PATH, registry._registry
    path = tmp_path / "position_registry.json"
    path.write_text(
        json.dumps({"XRP": {"registry_id": "legacy-xrp-1", "status": "corrupt"}}),
        encoding="utf-8",
    )
    registry._PATH = path
    registry._registry = {}
    try:
        registry._load()
        checks = live_preflight.local_exposure_preflight_checks()
        assert checks["no_unknown_local_exposure"]["ok"] is False
        detail = checks["no_unknown_local_exposure"]["detail"]
        assert "XRP" in detail
        assert "legacy-xrp-1" in detail
        assert "LOCAL_EXPOSURE_UNKNOWN" in detail
    finally:
        registry._PATH, registry._registry = old_path, old_registry


def test_exposure_semantics_separate_admission_from_mutation(tmp_path):
    import position_registry as registry

    old_path, old_registry = registry._PATH, registry._registry
    registry._PATH = tmp_path / "position_registry.json"
    registry._registry = {
        "ACTIVE": {"exposure_truth_status": registry.EXPOSURE_ACTIVE, "status": "open"},
        "UNKNOWN": {"exposure_truth_status": registry.EXPOSURE_UNKNOWN, "status": "corrupt"},
        "TERMINAL": {"exposure_truth_status": registry.EXPOSURE_TERMINAL, "status": "closed"},
    }
    try:
        assert registry.is_actionable_position(registry._registry["ACTIVE"]) is True
        assert registry.is_mutation_actionable_position(registry._registry["ACTIVE"]) is True
        assert registry.is_actionable_position(registry._registry["UNKNOWN"]) is True
        assert registry.is_unresolved_exposure(registry._registry["UNKNOWN"]) is True
        assert registry.is_mutation_actionable_position(registry._registry["UNKNOWN"]) is False
        assert registry.is_actionable_position(registry._registry["TERMINAL"]) is False
        assert registry.is_mutation_actionable_position(registry._registry["TERMINAL"]) is False
        assert registry.exposure_status({"exposure_truth_status": "CORRUPT"}) == registry.EXPOSURE_UNKNOWN
    finally:
        registry._PATH, registry._registry = old_path, old_registry


def test_unknown_manual_close_resolves_zero_exchange_position_without_post(tmp_path, monkeypatch):
    import os
    import position_registry as registry
    import trading_engine

    class FakeClient:
        posts = 0

        async def get_positions_status(self):
            return {"ok": True, "positions": []}

    old_path, old_registry = registry._PATH, registry._registry
    registry._PATH = tmp_path / "position_registry.json"
    registry._registry = {
        "XRP": {
            "registry_id": "xrp-unknown",
            "exposure_truth_status": registry.EXPOSURE_UNKNOWN,
            "status": "corrupt",
            "direction": "long",
            "size_usdt": 5.0,
            "entry": 1.0,
        }
    }
    monkeypatch.setenv("PAPER_TRADING", "false")
    monkeypatch.setattr(trading_engine, "OKXClient", FakeClient)
    engine = trading_engine.MultiEngine.__new__(trading_engine.MultiEngine)
    engine.engines = {}
    try:
        result = asyncio.run(engine.close_coin("XRP"))
        assert "No close submitted" in result
        assert registry.exposure_status(registry.get_all_details()["XRP"]) == registry.EXCHANGE_CLOSED_AWAITING_RECONCILIATION
        assert FakeClient.posts == 0
    finally:
        registry._PATH, registry._registry = old_path, old_registry


def test_unknown_protection_and_partial_are_mutation_inert(tmp_path, monkeypatch):
    import position_registry as registry
    import protection_manager
    import exit_manager

    class FakeClient:
        posts = 0

        async def place_protective_order(self, *args, **kwargs):
            self.posts += 1
            return {"code": "0", "data": [{"algoId": "bad"}]}

    old_path, old_registry = registry._PATH, registry._registry
    registry._PATH = tmp_path / "position_registry.json"
    registry._registry = {
        "XRP": {
            "exposure_truth_status": registry.EXPOSURE_UNKNOWN,
            "status": "corrupt",
            "direction": "long",
            "size_usdt": 5.0,
            "entry": 1.0,
        }
    }
    fake = FakeClient()
    try:
        manager = protection_manager.ProtectionManager(fake, strict_intent_persistence=True)
        result = asyncio.run(
            manager.ensure(
                coin="XRP",
                direction="long",
                contracts=1.0,
                stop_price=0.9,
                paper=False,
            )
        )
        assert result.reason == "local_exposure_not_mutation_actionable"
        assert fake.posts == 0

        monkeypatch.setattr(exit_manager, "_registry_row_for_plan", lambda _coin: dict(registry._registry["XRP"]))
        assert exit_manager._local_mutation_authorized("XRP") == (False, "LOCAL_EXPOSURE_UNKNOWN")
    finally:
        registry._PATH, registry._registry = old_path, old_registry


def test_truth_snapshot_exposes_active_and_unknown_counts(monkeypatch):
    import live_position_truth
    import position_registry as registry

    monkeypatch.setattr(live_position_truth, "_latest_price", lambda _symbol: 1.0)
    registry_rows = {
        "ACTIVE": {"exposure_truth_status": registry.EXPOSURE_ACTIVE, "direction": "long", "entry": 1.0, "size_usdt": 5.0, "leverage": 10},
        "UNKNOWN": {"exposure_truth_status": registry.EXPOSURE_UNKNOWN, "direction": "short", "entry": 1.0, "size_usdt": 5.0, "leverage": 10},
    }
    snapshot = asyncio.run(live_position_truth._build_paper_snapshot(None, registry_rows, None))
    assert snapshot["local_active_count"] == 1
    assert snapshot["local_unknown_count"] == 1
    assert snapshot["local_unknown_symbols"] == ["UNKNOWN"]

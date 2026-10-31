from __future__ import annotations

import asyncio
import json

import pytest

from execution_router import ExecutionRouter
from order_state_store import OrderStateStore


@pytest.fixture(autouse=True)
def isolated_position_registry(tmp_path, monkeypatch):
    import position_registry

    monkeypatch.setattr(position_registry, "_registry", {})
    monkeypatch.setattr(position_registry, "_PATH", tmp_path / "position_registry.json")


class Instrument:
    inst_id = "XRP-USDT-SWAP"
    raw = {"instId": inst_id, "ctVal": "1", "lotSz": "1", "minSz": "1", "tickSz": "0.0001"}


class Instruments:
    def __init__(self):
        self.item = Instrument()
        self.refreshes = 0

    def get(self, coin, *, require_fresh=True):
        return self.item

    async def refresh(self, client, *, force=False, priority="background"):
        self.refreshes += 1
        return 1

    def require(self, coin):
        return self.item


class Client:
    def __init__(self, *, positions=None, response=None, close_clears=True, position_mode="long_short_mode"):
        self.positions = list(positions or [])
        self.response = response or {"code": "0", "data": [{"ordId": "ORD-1"}]}
        self.close_clears = close_clears
        self.position_mode = position_mode
        self.post_count = 0
        self.close_kwargs = []
        self.position_reads = 0
        self.events = []

    async def get_positions_status(self, *, force_refresh=False, priority="P0"):
        self.position_reads += 1
        self.events.append(f"POSITION_READ_{self.position_reads}")
        return {"ok": True, "positions": list(self.positions), "reason": ""}

    async def get_account_config(self, *, priority="P0"):
        return {"posMode": self.position_mode}

    async def close_futures_position(self, coin, pos_side, size_usdt, **kwargs):
        await self.get_positions_status(force_refresh=True, priority="P0")
        callback = kwargs.get("before_send")
        if callback is not None:
            callback()
            self.events.append("SUBMISSION_ATTEMPTED_PERSISTED")
        self.events.append("HTTP_POST")
        self.post_count += 1
        self.close_kwargs.append(dict(kwargs))
        if self.close_clears:
            self.positions = []
        return dict(self.response)


def position(qty=3.0, side="short", open_time="100"):
    return {
        "instId": "XRP-USDT-SWAP", "pos": str(qty), "posSide": side,
        "mgnMode": "isolated", "cTime": str(open_time),
    }


def run(coro):
    return asyncio.run(coro)


def make_router(tmp_path, client, instruments=None):
    return ExecutionRouter(
        client,
        instruments=instruments or Instruments(),
        orders=OrderStateStore(tmp_path / "order_intents.json"),
    )


def test_operator_close_uses_fresh_exchange_qty_and_persists_before_post(tmp_path, monkeypatch):
    monkeypatch.setenv("PAPER_TRADING", "false")
    client = Client(positions=[position(0.4)])
    router = make_router(tmp_path, client)

    result = run(router.operator_close("XRP", requested_pos_side="long"))

    assert result["status"] == "CLOSE_FILLED"
    assert client.post_count == 1
    assert client.close_kwargs[0]["contracts_override"] == pytest.approx(0.4)
    assert client.close_kwargs[0]["client_order_id"]
    row = router.orders.get(result["intent_id"])
    assert row["mutation_attempted"] is True
    assert row["reduce_only"] is True
    assert [item["state"] for item in row["history"]][-5:] == [
        "ROUTER_VALIDATING", "PAYLOAD_READY", "SUBMISSION_ATTEMPTED", "SUBMITTED", "FILLED",
    ]


def test_operator_close_uses_canonical_terminal_telemetry_once(tmp_path, monkeypatch):
    monkeypatch.setenv("PAPER_TRADING", "false")
    terminal_events = []
    monkeypatch.setattr(
        "execution_router._flow.record_payload_terminal",
        lambda **fields: terminal_events.append(fields),
    )
    router = make_router(tmp_path, Client(positions=[position(0.4)]))

    result = run(router.operator_close("XRP", requested_pos_side="long"))
    row = router.orders.get(result["intent_id"])

    assert result["status"] == "CLOSE_FILLED"
    assert row["terminal_class"] == "filled"
    assert row["terminal_reason"] == "manual_close_authoritatively_confirmed"
    assert len(terminal_events) == 1


def test_manual_close_definitive_rejection_uses_canonical_terminal_outbox(tmp_path, monkeypatch):
    monkeypatch.setenv("PAPER_TRADING", "false")
    terminal_events = []
    monkeypatch.setattr(
        "execution_router._flow.record_payload_terminal",
        lambda **fields: terminal_events.append(fields),
    )
    client = Client(
        positions=[position(0.4)], close_clears=False,
        response={"code": "0", "data": [{"sCode": "51000", "sMsg": "bad close"}]},
    )
    router = make_router(tmp_path, client)

    result = run(router.operator_close("XRP", requested_pos_side="short"))
    row = router.orders.get(result["intent_id"])
    assert result["status"] == "CLOSE_REJECTED"
    assert row["terminal_class"] == "exchange_rejection"
    assert row["terminal_reason"] == "manual_close_exchange_rejected"
    assert row["terminal_telemetry_status"] == "complete"
    assert len(terminal_events) == 1


def test_exchange_zero_is_truthful_and_never_posts(tmp_path, monkeypatch):
    monkeypatch.setenv("PAPER_TRADING", "false")
    client = Client(positions=[])
    router = make_router(tmp_path, client)

    result = run(router.operator_close("XRP", requested_pos_side="short"))

    assert result["status"] == "CLOSE_ALREADY_ZERO"
    assert client.post_count == 0
    assert router.orders.snapshot()["total"] == 0


def test_exchange_truth_failure_is_fail_closed(tmp_path, monkeypatch):
    monkeypatch.setenv("PAPER_TRADING", "false")

    class Broken(Client):
        async def get_positions_status(self, **kwargs):
            return {"ok": False, "positions": None, "reason": "timeout"}

    client = Broken(positions=[position()])
    result = run(make_router(tmp_path, client).operator_close("XRP"))

    assert result["status"] == "CLOSE_FAILED_BEFORE_SUBMISSION"
    assert client.post_count == 0


def test_stale_shared_metadata_is_refreshed_before_close(tmp_path, monkeypatch):
    monkeypatch.setenv("PAPER_TRADING", "false")
    instruments = Instruments()
    instruments.item = None

    class Refreshing(Instruments):
        def __init__(self):
            super().__init__()
            self.item = None

        async def refresh(self, client, *, force=False, priority="background"):
            self.refreshes += 1
            self.item = Instrument()

    instruments = Refreshing()
    client = Client(positions=[position()])
    result = run(make_router(tmp_path, client, instruments).operator_close("XRP"))

    assert result["status"] == "CLOSE_FILLED"
    assert instruments.refreshes == 1
    assert client.post_count == 1


def test_duplicate_callback_deduplicates_active_intent(tmp_path, monkeypatch):
    monkeypatch.setenv("PAPER_TRADING", "false")
    client = Client(positions=[position()], close_clears=False)
    router = make_router(tmp_path, client)

    first = run(router.operator_close("XRP"))
    second = run(router.operator_close("XRP"))

    assert first["status"] == "CLOSE_SUBMITTED"
    assert second["status"] == "CLOSE_IN_PROGRESS"
    assert second["intent_id"] == first["intent_id"]
    assert client.post_count == 1


def test_item_level_rejection_is_final_and_not_replayed(tmp_path, monkeypatch):
    monkeypatch.setenv("PAPER_TRADING", "false")
    client = Client(
        positions=[position()],
        response={"code": "0", "data": [{"sCode": "51000", "sMsg": "Parameter clOrdId error"}]},
        close_clears=False,
    )
    router = make_router(tmp_path, client)

    first = run(router.operator_close("XRP"))
    second = run(router.operator_close("XRP"))

    assert first["status"] == "CLOSE_REJECTED"
    assert second["status"] == "CLOSE_REJECTED"
    assert client.post_count == 2
    assert router.orders.get(first["intent_id"])["state"] == "FAILED_FINAL"


def test_ambiguous_response_enters_readback_uncertain_without_replay(tmp_path, monkeypatch):
    monkeypatch.setenv("PAPER_TRADING", "false")
    client = Client(positions=[position()], response={"code": "0", "data": []}, close_clears=False)
    router = make_router(tmp_path, client)

    first = run(router.operator_close("XRP"))
    second = run(router.operator_close("XRP"))

    assert first["status"] == "CLOSE_UNCERTAIN"
    assert second["status"] == "CLOSE_IN_PROGRESS"
    assert client.post_count == 1
    assert router.orders.get(first["intent_id"])["state"] == "SUBMIT_UNCERTAIN"

    restarted = make_router(tmp_path, client)
    recovered = run(restarted.recover_unresolved())
    assert recovered[0].reason == "manual_close_recovery_pending"
    assert client.post_count == 1


def test_old_lifecycle_does_not_deduplicate_new_same_symbol_position(tmp_path, monkeypatch):
    monkeypatch.setenv("PAPER_TRADING", "false")
    client = Client(positions=[position(open_time="A")], response={"code": "0", "data": []}, close_clears=False)
    router = make_router(tmp_path, client)

    first = run(router.operator_close("XRP"))
    old_row = router.orders.get(first["intent_id"])

    client.positions = [position(open_time="B")]
    second = run(router.operator_close("XRP"))
    new_row = router.orders.get(second["intent_id"])

    assert first["status"] == "CLOSE_UNCERTAIN"
    assert second["status"] == "CLOSE_UNCERTAIN"
    assert old_row["position_lifecycle_id"] != new_row["position_lifecycle_id"]
    assert first["intent_id"] != second["intent_id"]
    assert client.post_count == 2


def test_stale_registry_lifecycle_is_replaced_by_fresh_exchange_identity(tmp_path, monkeypatch):
    monkeypatch.setenv("PAPER_TRADING", "false")
    import position_registry
    anchor_a = position_registry.derive_exchange_lifecycle_anchor(position(open_time="A"))

    monkeypatch.setattr(
        position_registry,
        "_registry",
        {"XRP": {
            "position_lifecycle_id": "L1",
            "exchange_lifecycle_anchor": anchor_a,
            "status": "open",
            "exposure_truth_status": "ACTIVE_POSITION",
            "direction": "short",
        }},
    )
    monkeypatch.setattr(position_registry, "_save", lambda **kwargs: None)
    client = Client(
        positions=[position(open_time="B")],
        response={"code": "0", "data": []},
        close_clears=False,
    )
    router = make_router(tmp_path, client)
    old_intent = router.orders.create({
        "intent_id": "MANUAL-CLOSE-OLD-A",
        "kind": "manual_close",
        "coin": "XRP",
        "pos_side": "short",
        "position_lifecycle_id": "L1",
        "state": "ACKNOWLEDGED",
    })

    result = run(router.operator_close("XRP"))

    assert result["status"] == "CLOSE_UNCERTAIN"
    assert client.post_count == 1
    assert result["intent_id"] != old_intent["intent_id"]
    assert router.orders.get(old_intent["intent_id"])["position_lifecycle_id"] == "L1"
    new_row = router.orders.get(result["intent_id"])
    assert new_row["position_lifecycle_id"] != old_intent["position_lifecycle_id"]
    assert new_row["exchange_lifecycle_anchor"] == position_registry.derive_exchange_lifecycle_anchor(client.positions[0])
    assert position_registry.get_all_details()["XRP"]["position_lifecycle_id"] == new_row["position_lifecycle_id"]


def test_real_fill_registry_lifecycle_binds_exchange_anchor_and_converges_consumers(tmp_path, monkeypatch):
    monkeypatch.setenv("PAPER_TRADING", "false")
    import position_registry
    from exit_manager import _resolve_position_lifecycle_id
    from protection_manager import ProtectionManager

    monkeypatch.setattr(position_registry, "_registry", {})
    position_registry.register(
        "XRP", "short", "ENGINE", size_usdt=3.5, entry=1.0,
        order_id="ENTRY-ORD-X",
    )
    position_registry.update(
        "XRP",
        order_id="ENTRY-ORD-X",
        metadata={
            "origin_entry_order_id": "ENTRY-ORD-X",
            "origin_entry_client_id": "ENTRY-CL-X",
            "entry_lifecycle": "FILLED",
            "mutation_attempted": True,
            "exchange_confirmed": True,
            "exchange_position_evidence": position(open_time="E1"),
            "exposure_truth_status": "ACTIVE_POSITION",
        },
        require_persist=True,
    )
    row_before = position_registry.get_all_details()["XRP"]
    lifecycle_l1 = row_before["position_lifecycle_id"]
    assert lifecycle_l1 == "entry:ENTRY-ORD-X"

    client = Client(
        positions=[position(open_time="E1")],
        response={"code": "0", "data": []},
        close_clears=False,
    )
    router = make_router(tmp_path, client)
    context = router._operator_lifecycle_context("XRP", client.positions[0])
    assert context["status"] == "BOUND"
    assert context["position_lifecycle_id"] == lifecycle_l1

    result = run(router.operator_close("XRP"))
    assert result["status"] == "CLOSE_UNCERTAIN"
    row_after = position_registry.get_all_details()["XRP"]
    assert row_after["position_lifecycle_id"] == lifecycle_l1
    assert row_after["exchange_lifecycle_anchor"]
    assert router._operator_lifecycle_context("XRP", client.positions[0])["status"] == "MATCHED"

    assert _resolve_position_lifecycle_id("XRP", order_id="ENTRY-ORD-X") == lifecycle_l1
    protection = ProtectionManager(client)
    assert protection._position_lifecycle_id(
        "XRP", row=row_after, prior_intent={}, client_order_id="OXYC-1",
    ) == lifecycle_l1

    position_registry._registry = {}
    position_registry._load()
    restored = position_registry.get_all_details()["XRP"]
    assert restored["position_lifecycle_id"] == lifecycle_l1
    assert restored["exchange_lifecycle_anchor"] == row_after["exchange_lifecycle_anchor"]


def test_legacy_lifecycle_without_exchange_association_fails_closed(tmp_path, monkeypatch):
    monkeypatch.setenv("PAPER_TRADING", "false")
    import position_registry

    position_registry.register("XRP", "short", "ENGINE", 25.0, order_id="ENTRY-ORD-X")
    position_registry.update(
        "XRP",
        metadata={
            "origin_entry_order_id": "ENTRY-ORD-X",
            "entry_lifecycle": "FILLED",
            "mutation_attempted": True,
            "exchange_confirmed": True,
            "exposure_truth_status": "ACTIVE_POSITION",
        },
        require_persist=True,
    )
    client = Client(positions=[position(open_time="NEW-EVIDENCE")])
    result = run(make_router(tmp_path, client).operator_close("XRP"))
    assert result["status"] == "CLOSE_FAILED_BEFORE_SUBMISSION"
    assert result["reason"] == "registry_lifecycle_association_unproven"
    assert client.post_count == 0


def test_unbound_local_lifecycle_without_proven_association_fails_closed(tmp_path, monkeypatch):
    monkeypatch.setenv("PAPER_TRADING", "false")
    import position_registry

    monkeypatch.setattr(
        position_registry,
        "_registry",
        {"XRP": {
            "position_lifecycle_id": "L1",
            "status": "open",
            "exposure_truth_status": "ACTIVE_POSITION",
        }},
    )
    monkeypatch.setattr(position_registry, "_save", lambda **kwargs: None)
    client = Client(positions=[position(open_time="E1")])
    router = make_router(tmp_path, client)

    result = run(router.operator_close("XRP"))

    assert result["status"] == "CLOSE_FAILED_BEFORE_SUBMISSION"
    assert result["reason"] == "registry_lifecycle_association_unproven"
    assert client.post_count == 0


def test_stale_lifecycle_restart_recovery_is_read_only_against_new_position(tmp_path, monkeypatch):
    monkeypatch.setenv("PAPER_TRADING", "false")
    import position_registry
    anchor_a = position_registry.derive_exchange_lifecycle_anchor(position(open_time="A"))

    monkeypatch.setattr(
        position_registry,
        "_registry",
        {"XRP": {
            "position_lifecycle_id": "L1",
            "exchange_lifecycle_anchor": anchor_a,
            "status": "open",
            "exposure_truth_status": "ACTIVE_POSITION",
        }},
    )
    monkeypatch.setattr(position_registry, "_save", lambda **kwargs: None)
    client = Client(
        positions=[position(open_time="A")],
        response={"code": "0", "data": []},
        close_clears=False,
    )
    router = make_router(tmp_path, client)
    first = run(router.operator_close("XRP"))
    assert first["status"] == "CLOSE_UNCERTAIN"

    client.positions = [position(open_time="B")]
    restarted = make_router(tmp_path, client)
    recovered = run(restarted.recover_unresolved())

    assert recovered[0].reason == "manual_close_lifecycle_mismatch"
    assert restarted.orders.get(first["intent_id"])["state"] == "FAILED_FINAL"
    assert client.post_count == 1


class ZeroBeforePostClient(Client):
    async def close_futures_position(self, coin, pos_side, size_usdt, **kwargs):
        self.positions = []
        return {
            "code": "0",
            "data": [],
            "mutation_outcome": "SUCCESS",
            "post_attempted": False,
            "exchange_closed": True,
        }


def test_position_zero_before_post_is_already_zero_and_not_filled(tmp_path, monkeypatch):
    monkeypatch.setenv("PAPER_TRADING", "false")
    client = ZeroBeforePostClient(positions=[position(0.03)])
    router = make_router(tmp_path, client)

    result = run(router.operator_close("XRP"))

    assert result["status"] == "CLOSE_ALREADY_ZERO"
    assert result["post_count"] == 0
    assert client.post_count == 0
    row = router.orders.get(result["intent_id"])
    assert row["state"] == "FAILED_FINAL"
    assert row["mutation_attempted"] is False
    assert row["physical_post_count"] == 0
    assert row["mutation_outcome"] == "ALREADY_ZERO"
    assert row["close_outcome"] == "CLOSE_ALREADY_ZERO"
    history = [item["state"] for item in row["history"]]
    assert "SUBMISSION_ATTEMPTED" not in history
    assert "SUBMITTED" not in history
    assert "SUBMIT_UNCERTAIN" not in history

    repeated = run(router.operator_close("XRP"))
    assert repeated["status"] == "CLOSE_ALREADY_ZERO"
    assert repeated["post_count"] == 0
    assert router.orders.snapshot()["total"] == 1


def test_position_zero_before_post_reconciles_original_risk_slot_before_terminal(tmp_path, monkeypatch):
    monkeypatch.setenv("PAPER_TRADING", "false")
    client = ZeroBeforePostClient(positions=[position(0.03)])
    router = make_router(tmp_path, client)
    reconciled = []
    monkeypatch.setattr(router, "_reconcile_closed_position_risk_slot", lambda coin: reconciled.append(coin) or True)

    result = run(router.operator_close("XRP"))
    assert result["status"] == "CLOSE_ALREADY_ZERO"
    assert reconciled == ["XRP"]
    assert router.orders.get(result["intent_id"])["terminal_finalized_at"]


def test_position_zero_before_post_cleanup_failure_stays_recovery_owned(tmp_path, monkeypatch):
    monkeypatch.setenv("PAPER_TRADING", "false")
    router = make_router(tmp_path, ZeroBeforePostClient(positions=[position(0.03)]))
    monkeypatch.setattr(router, "_reconcile_closed_position_risk_slot", lambda _coin: False)

    result = run(router.operator_close("XRP"))
    row = router.orders.get(result["intent_id"])
    assert result["status"] == "CLOSE_UNCERTAIN"
    assert result["post_count"] == 0
    assert row["state"] == "SUBMIT_UNCERTAIN"
    assert row["recovery_reason"] == "manual_close_position_risk_slot_close_pending"
    assert not row.get("terminal_finalized_at")


def test_submission_boundary_order_is_final_reread_then_persist_then_post(tmp_path, monkeypatch):
    monkeypatch.setenv("PAPER_TRADING", "false")
    client = Client(positions=[position(0.03)], close_clears=True)
    router = make_router(tmp_path, client)
    result = run(router.operator_close("XRP"))

    assert result["status"] == "CLOSE_FILLED"
    assert client.post_count == 1
    assert client.events.index("POSITION_READ_2") < client.events.index("SUBMISSION_ATTEMPTED_PERSISTED")
    assert client.events.index("SUBMISSION_ATTEMPTED_PERSISTED") < client.events.index("HTTP_POST")


def test_submission_boundary_persistence_failure_blocks_post(tmp_path, monkeypatch):
    monkeypatch.setenv("PAPER_TRADING", "false")
    client = Client(positions=[position(0.03)], close_clears=True)
    router = make_router(tmp_path, client)
    original_transition = router.orders.transition

    def fail_boundary(intent_id, state, **fields):
        if state == "SUBMISSION_ATTEMPTED":
            raise OSError("simulated boundary persistence failure")
        return original_transition(intent_id, state, **fields)

    monkeypatch.setattr(router.orders, "transition", fail_boundary)
    result = run(router.operator_close("XRP"))

    assert result["status"] == "CLOSE_FAILED_BEFORE_SUBMISSION"
    assert client.post_count == 0
    row = router.orders.get(result["intent_id"])
    history = [item["state"] for item in row["history"]]
    assert "SUBMISSION_ATTEMPTED" not in history
    assert row["mutation_attempted"] is False
    assert row["state"] == "FAILED_FINAL"
    assert row["terminal_reason"] == "manual_close_pre_submission_persistence_failure"
    assert history.count("FAILED_FINAL") == 1
    assert row["terminal_telemetry_event"]["terminal_event_id"] == f"terminal:{result['intent_id']}"


def test_post_boundary_value_error_stays_uncertain_and_restart_never_reposts(tmp_path, monkeypatch):
    monkeypatch.setenv("PAPER_TRADING", "false")

    class MalformedPostClient(Client):
        async def close_futures_position(self, coin, pos_side, size_usdt, **kwargs):
            callback = kwargs["before_send"]
            callback()
            self.post_count += 1
            # Models response parsing after the HTTP mutation boundary.
            raise ValueError("malformed fillPx")

    client = MalformedPostClient(positions=[position(0.03)], close_clears=False)
    router = make_router(tmp_path, client)
    first = run(router.operator_close("XRP"))
    row = router.orders.get(first["intent_id"])

    assert first["status"] == "CLOSE_UNCERTAIN"
    assert client.post_count == 1
    assert row["state"] == "SUBMIT_UNCERTAIN"
    assert row["mutation_attempted"] is True
    assert row["physical_post_count"] == 1
    assert not row.get("terminal_finalized_at")

    restarted = make_router(tmp_path, client)
    recovered = run(restarted.recover_unresolved())
    assert recovered[0].reason == "manual_close_recovery_pending"
    assert client.post_count == 1


def test_submission_boundary_finalizer_failure_stays_nonterminal_then_retries_locally(tmp_path, monkeypatch):
    monkeypatch.setenv("PAPER_TRADING", "false")
    terminal_events = []
    monkeypatch.setattr(
        "execution_router._flow.record_payload_terminal",
        lambda **event: terminal_events.append(event),
    )
    client = Client(positions=[position(0.03)], close_clears=True)
    router = make_router(tmp_path, client)
    original_transition = router.orders.transition
    original_finalizer = router.orders.finalize_terminal

    def fail_boundary(intent_id, state, **fields):
        if state == "SUBMISSION_ATTEMPTED":
            raise OSError("boundary write unavailable")
        return original_transition(intent_id, state, **fields)

    def fail_finalizer(*_args, **_kwargs):
        raise OSError("terminal write unavailable")

    monkeypatch.setattr(router.orders, "transition", fail_boundary)
    monkeypatch.setattr(router.orders, "finalize_terminal", fail_finalizer)
    pending = run(router.operator_close("XRP"))
    assert pending["status"] == "CLOSE_PERSISTENCE_PENDING"
    assert pending["post_count"] == 0
    row = router.orders.get(pending["intent_id"])
    assert row["state"] == "PAYLOAD_READY"
    assert not row.get("terminal_finalized_at")
    assert client.post_count == 0

    monkeypatch.setattr(router.orders, "transition", original_transition)
    monkeypatch.setattr(router.orders, "finalize_terminal", original_finalizer)
    retried = run(router.operator_close("XRP"))
    final = router.orders.get(retried["intent_id"])
    assert retried["status"] == "CLOSE_FAILED_BEFORE_SUBMISSION"
    assert final["state"] == "FAILED_FINAL"
    assert final["terminal_reason"] == "manual_close_pre_submission_persistence_failure"
    assert [item["state"] for item in final["history"]].count("FAILED_FINAL") == 1
    assert len(terminal_events) == 1
    assert client.post_count == 0


def test_restart_recovery_does_not_reconcile_old_intent_against_new_lifecycle(tmp_path, monkeypatch):
    monkeypatch.setenv("PAPER_TRADING", "false")
    client = Client(positions=[position(open_time="A")], response={"code": "0", "data": []}, close_clears=False)
    router = make_router(tmp_path, client)
    first = run(router.operator_close("XRP"))

    client.positions = [position(open_time="B")]
    restarted = make_router(tmp_path, client)
    recovered = run(restarted.recover_unresolved())

    assert recovered[0].reason == "manual_close_lifecycle_mismatch"
    assert restarted.orders.get(first["intent_id"])["state"] == "FAILED_FINAL"
    assert client.post_count == 1


def test_acknowledged_pending_close_is_read_back_after_restart_without_replay(tmp_path, monkeypatch):
    monkeypatch.setenv("PAPER_TRADING", "false")
    client = Client(positions=[position(open_time="A")], close_clears=False)
    router = make_router(tmp_path, client)
    first = run(router.operator_close("XRP"))
    assert first["status"] == "CLOSE_SUBMITTED"
    assert router.orders.get(first["intent_id"])["state"] == "ACKNOWLEDGED"

    restarted = make_router(tmp_path, client)
    recovered = run(restarted.recover_unresolved())

    assert recovered[0].reason == "manual_close_recovery_pending"
    assert client.post_count == 1


def test_unknown_position_mode_fails_before_post(tmp_path, monkeypatch):
    monkeypatch.setenv("PAPER_TRADING", "false")
    client = Client(positions=[position()], position_mode="")
    result = run(make_router(tmp_path, client).operator_close("XRP"))
    assert result["status"] == "CLOSE_FAILED_BEFORE_SUBMISSION"
    assert "position_mode_unknown" in result["reason"]
    assert client.post_count == 0


def test_close_all_includes_exchange_only_positions_without_local_quantity(tmp_path, monkeypatch):
    import trading_engine

    class ExchangeTruth:
        async def get_positions_status(self, **kwargs):
            return {"ok": True, "positions": [position(0.4, side="short", open_time="A")]}

    monkeypatch.setattr(trading_engine, "OKXClient", ExchangeTruth)
    monkeypatch.setattr(trading_engine, "registry_all_details", lambda: {})
    monkeypatch.setattr(trading_engine, "registry_unknown_details", lambda: {})
    engine = object.__new__(trading_engine.MultiEngine)
    engine.engines = {}
    seen = []

    async def fake_close(coin):
        seen.append(coin)
        return f"CLOSE_FILLED {coin}"

    engine.close_coin = fake_close
    result = run(engine.close_all_positions())
    assert "XRP" in seen
    assert "CLOSE_FILLED XRP" in result


def test_persistence_failure_blocks_post(tmp_path, monkeypatch):
    monkeypatch.setenv("PAPER_TRADING", "false")

    class FailingStore(OrderStateStore):
        def create(self, intent):
            raise OSError("disk full")

    client = Client(positions=[position()])
    router = ExecutionRouter(client, instruments=Instruments(), orders=FailingStore(tmp_path / "orders.json"))
    result = run(router.operator_close("XRP"))

    assert result["status"] == "CLOSE_FAILED_BEFORE_SUBMISSION"
    assert client.post_count == 0


def test_atomic_store_reload_and_no_abandoned_temp(tmp_path):
    path = tmp_path / "orders.json"
    store = OrderStateStore(path)
    store.create({"intent_id": "I-1", "kind": "manual_close"})
    assert json.loads(path.read_text(encoding="utf-8"))["intents"]["I-1"]["state"] == "INTENT_CREATED"
    assert not list(tmp_path.glob(f".{path.name}.*.tmp"))
    assert OrderStateStore(path).get("I-1")["intent_id"] == "I-1"


def test_atomic_replace_failure_preserves_previous_state_and_blocks_post(tmp_path, monkeypatch):
    import order_state_store

    path = tmp_path / "orders.json"
    store = OrderStateStore(path)
    store.create({"intent_id": "I-1", "kind": "manual_close"})
    before = path.read_text(encoding="utf-8")

    def fail_replace(*args, **kwargs):
        raise OSError("replace failed")

    monkeypatch.setattr(order_state_store.os, "replace", fail_replace)
    with pytest.raises(OSError):
        store.transition("I-1", "PAYLOAD_READY")
    assert path.read_text(encoding="utf-8") == before
    assert OrderStateStore(path).get("I-1")["state"] == "INTENT_CREATED"

    client = Client(positions=[position()])
    result = run(ExecutionRouter(client, instruments=Instruments(), orders=store).operator_close("XRP"))
    assert result["status"] == "CLOSE_FAILED_BEFORE_SUBMISSION"
    assert client.post_count == 0


def test_okx_close_helper_uses_item_level_mutation_classifier(monkeypatch):
    import okx_client

    monkeypatch.setattr(okx_client, "PAPER_TRADING", False)
    client = okx_client.OKXClient()
    calls = []

    async def positions(**kwargs):
        return {"ok": True, "positions": [position(1.0)]}

    async def ticker(coin):
        return {"last": "0.5"}

    async def request(method, path, *, data=None, **kwargs):
        calls.append((method, path, data))
        return {"code": "0", "data": [{"sCode": "51000", "sMsg": "Parameter clOrdId error"}]}

    client.get_positions_status = positions
    client.get_execution_ticker = ticker
    client._request = request

    with pytest.raises(okx_client.OKXDefinitiveRejection) as exc:
        run(client.close_futures_position(
            "XRP", "short", 0.0, contracts_override=1.0,
            client_order_id="OXYC123", instrument_metadata=Instrument(),
            position_mode="long_short_mode", verify_authoritative_position=True,
        ))
    assert exc.value.s_code == "51000"
    assert len(calls) == 1


def test_okx_close_success_keeps_exchange_order_id_separate(monkeypatch):
    import okx_client

    monkeypatch.setattr(okx_client, "PAPER_TRADING", False)
    client = okx_client.OKXClient()
    seen = []
    reads = iter([
        {"ok": True, "positions": [position(1.0)]},
    ])

    async def positions(**kwargs):
        try:
            return next(reads)
        except StopIteration:
            return {"ok": True, "positions": []}

    async def ticker(coin):
        return {"last": "0.5"}

    async def request(method, path, *, data=None, **kwargs):
        seen.append(data)
        return {"code": "0", "data": [{"ordId": "EXCHANGE-42", "state": "live"}]}

    client.get_positions_status = positions
    client.get_execution_ticker = ticker
    client._request = request
    result = run(client.close_futures_position(
        "XRP", "short", 0.0, contracts_override=1.0,
        client_order_id="OXYC123", instrument_metadata=Instrument(),
        position_mode="long_short_mode", verify_authoritative_position=True,
    ))

    assert result["mutation_outcome"] == "SUCCESS"
    assert result["exchange_order_id"] == "EXCHANGE-42"
    assert result["client_order_id"] == "OXYC123"
    assert seen[0]["reduceOnly"] == "true"
    assert seen[0]["clOrdId"] == "OXYC123"


@pytest.mark.parametrize(
    ("mode", "side", "expected_side", "expected_pos_side"),
    [
        ("net_mode", "long", "sell", "net"),
        ("net_mode", "short", "buy", "net"),
        ("long_short_mode", "long", "sell", "long"),
        ("long_short_mode", "short", "buy", "short"),
    ],
)
def test_close_payload_matches_authoritative_position_mode(
    monkeypatch, mode, side, expected_side, expected_pos_side,
):
    import okx_client

    monkeypatch.setattr(okx_client, "PAPER_TRADING", False)
    client = okx_client.OKXClient()
    seen = []

    async def positions(**kwargs):
        signed = "1" if side == "long" else "-1"
        return {"ok": True, "positions": [position(1.0, side if mode == "long_short_mode" else "net") | {"pos": signed}]}

    async def ticker(coin):
        return {"last": "0.5"}

    async def request(method, path, *, data=None, **kwargs):
        seen.append(data)
        return {"code": "0", "data": [{"ordId": "EXCHANGE-42"}]}

    client.get_positions_status = positions
    client.get_execution_ticker = ticker
    client._request = request
    result = run(client.close_futures_position(
        "XRP", side, 0.0, contracts_override=1.0,
        client_order_id="OXYC123", instrument_metadata=Instrument(),
        position_mode=mode, verify_authoritative_position=True,
    ))

    assert result["mutation_outcome"] == "SUCCESS"
    assert seen[0]["side"] == expected_side
    assert seen[0]["posSide"] == expected_pos_side
    assert seen[0]["reduceOnly"] == "true"

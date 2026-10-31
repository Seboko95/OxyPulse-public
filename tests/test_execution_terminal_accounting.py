from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sys
import threading
import time
from dataclasses import replace
from types import SimpleNamespace

import pytest

import capital_scheduler
from execution_router import ExecutionRequest, ExecutionRouter
import execution_flow_telemetry as flow
from okx_client import _require_place_order_acknowledgement
from okx_mutation_results import MUTATION_DEFINITIVE_REJECTION, MUTATION_READBACK_REQUIRED, classify_mutation_response
from order_state_store import OrderStateStore
from execution_router import (
    FILL_KNOWN_POSITIVE, FILL_KNOWN_ZERO, FILL_UNKNOWN,
    _authoritative_cumulative_fill,
)
from risk_manager import RiskManager
from capital_reservations import CapitalReservationBook
from order_state_store import OrderStateStoreUnavailable


@pytest.fixture(autouse=True)
def isolate_durable_execution_state(tmp_path, monkeypatch):
    """Keep terminal lifecycle proofs out of shared runtime state."""
    import position_registry
    import risk_manager

    monkeypatch.setattr(position_registry, "_PATH", tmp_path / "position_registry.json")
    monkeypatch.setattr(position_registry, "_registry", {})
    monkeypatch.setattr(position_registry, "_healthy", True)
    monkeypatch.setattr(position_registry, "_load_error", "")
    monkeypatch.setattr(risk_manager, "risk", RiskManager(tmp_path / "risk_slots.sqlite3"))
    yield


def _request(intent_id: str = "terminal-intent") -> ExecutionRequest:
    return ExecutionRequest(
        intent_id=intent_id, candidate_id="candidate", reservation_id="reservation",
        mode="Guarded", engine="TEST", coin="TEST", direction="long",
        requested_contracts=1.0, approved_contracts=1.0,
        approved_notional_usdt=10.0, order_type="market", price=100.0,
    )


def test_malformed_reservation_shape_is_unhealthy_and_cannot_reserve(tmp_path):
    path = tmp_path / "reservations.json"
    path.write_text(json.dumps({"reservations": []}), encoding="utf-8")
    book = CapitalReservationBook(path)

    assert book.health()["healthy"] is False
    assert book.total_margin() == float("inf")
    allowed, reason, reservation_id = asyncio.run(book.reserve(
        intent_id="malformed-reservation", candidate_id="candidate",
        margin_usdt=1.0, risk_usdt=1.0, max_total_margin_usdt=10.0,
    ))
    assert allowed is False
    assert reason == "capital_reservation_store_unavailable"
    assert reservation_id == ""


def test_positive_intended_entry_is_releasable_pre_submit_reservation(monkeypatch):
    import position_registry

    position_registry._registry["TEST"] = {
        "direction": "long", "engine": "TEST", "size_usdt": 10.0,
        "entry": 100.0, "leverage": 3, "stop_loss": 90.0,
        "take_profit": 120.0, "order_id": "", "pending_reservation": True,
        "entry_lifecycle": "PRE_SUBMIT_RESERVED", "mutation_attempted": False,
    }
    monkeypatch.setattr(position_registry, "_save", lambda **_kwargs: True)

    assert position_registry.is_pre_submit_reservation(position_registry._registry["TEST"])
    assert position_registry.release_pre_submit_reservation("TEST") is True
    assert "TEST" not in position_registry._registry


def test_non_strict_registry_update_reports_persistence_failure_and_restores_row(monkeypatch):
    import position_registry

    original = {
        "direction": "long", "engine": "TEST", "size_usdt": 10.0,
        "entry": 100.0, "leverage": 3, "order_id": "order-1",
    }
    position_registry._registry["TEST"] = dict(original)
    monkeypatch.setattr(position_registry, "_save", lambda **_kwargs: False)

    assert position_registry.update("TEST", entry=101.0) is False
    assert position_registry._registry["TEST"] == original


def test_corrupt_position_registry_preserves_prior_truth_and_blocks_admission(tmp_path, monkeypatch):
    import position_registry

    path = tmp_path / "position_registry.json"
    path.write_text(json.dumps({"BROKEN": []}), encoding="utf-8")
    prior = {"KNOWN": {"direction": "long", "engine": "TEST"}}
    monkeypatch.setattr(position_registry, "_PATH", path)
    monkeypatch.setattr(position_registry, "_registry", prior.copy())
    monkeypatch.setattr(position_registry, "_healthy", True)
    monkeypatch.setattr(position_registry, "_load_error", "")

    position_registry._load()

    assert position_registry.health()["healthy"] is False
    assert position_registry._registry == prior
    allowed, reason = position_registry.can_open("NEW", "long")
    assert allowed is False
    assert reason.startswith("position_registry_unavailable:")
    with pytest.raises(position_registry.PositionRegistryUnavailable):
        position_registry.get_all_details()


def test_terminal_finalization_is_idempotent_and_keeps_one_history_row(tmp_path):
    store = OrderStateStore(tmp_path / "orders.json")
    store.create({"intent_id": "i", "state": "PAYLOAD_READY"})

    first, first_new = store.finalize_terminal(
        "i", "FAILED_FINAL", terminal_class="local_rejection", terminal_reason="never_submitted",
    )
    second, second_new = store.finalize_terminal(
        "i", "REJECTED", terminal_class="exchange_rejection", terminal_reason="must_not_replace",
    )

    assert first_new is True
    assert second_new is False
    assert second["terminal_class"] == "local_rejection"
    assert second["terminal_reason"] == "never_submitted"
    assert [row["state"] for row in second["history"]].count("FAILED_FINAL") == 1
    assert first["terminal_finalized_at"] == second["terminal_finalized_at"]


def test_ordinary_transition_cannot_overwrite_finalized_terminal_truth(tmp_path):
    store = OrderStateStore(tmp_path / "orders.json")
    store.create({"intent_id": "i", "state": "ACKNOWLEDGED"})
    finalized, _ = store.finalize_terminal(
        "i", "FILLED", terminal_class="filled", terminal_reason="authoritative_fill",
        evidence={"ordId": "one"},
    )
    returned = store.transition(
        "i", "CANCEL_PENDING", terminal_class="cancelled",
        terminal_reason="must_not_replace", evidence={"ordId": "two"},
    )
    assert returned == finalized
    assert returned["state"] == "FILLED"
    assert returned["terminal_class"] == "filled"
    assert returned["terminal_reason"] == "authoritative_fill"
    assert returned["evidence"] == {"ordId": "one"}
    assert [entry["state"] for entry in returned["history"]].count("FILLED") == 1


def test_legacy_terminal_row_is_backfilled_and_immutable_after_reload(tmp_path):
    path = tmp_path / "orders.json"
    path.write_text(json.dumps({"intents": {
        "legacy": {
            "intent_id": "legacy", "state": "CANCELLED", "created_at": 10.0,
            "updated_at": 12.0, "history": [{"state": "CANCELLED", "at": 11.0}],
            "exchange_order_id": "order-1",
        },
    }}), encoding="utf-8")

    store = OrderStateStore(path)
    row = store.get("legacy")
    assert row["terminal_finalized_at"] == 11.0
    assert row["terminal_class"] == "legacy_terminal"
    assert store.transition("legacy", "ACKNOWLEDGED", exchange_order_id="must-not-replace") == row
    assert OrderStateStore(path).get("legacy")["exchange_order_id"] == "order-1"


@pytest.mark.parametrize(
    "response, reason",
    [
        ({"code": "0", "data": [{"sCode": "51008", "ordId": "1"}]}, "item_sCode_not_zero"),
        ({"code": "0", "data": [{"sCode": "0"}]}, "ordId_missing"),
        ({"code": "0", "data": [{}]}, "item_sCode_not_zero"),
        ({"code": "0", "data": [{"sCode": "0", "ordId": "1", "clOrdId": "other"}]}, "client_id_mismatch"),
        ({"code": "0", "data": [{"sCode": "0", "ordId": "1"}]}, "client_id_missing"),
    ],
)
def test_place_order_acknowledgement_contract_fails_closed(response, reason):
    with pytest.raises(RuntimeError, match=reason):
        _require_place_order_acknowledgement(response, "expected")


def test_place_order_acknowledgement_contract_accepts_only_usable_item():
    result = _require_place_order_acknowledgement(
        {"code": "0", "data": [{"sCode": "0", "ordId": "123", "clOrdId": "expected"}]},
        "expected",
    )
    assert result["ordId"] == "123"


def test_business_rejection_is_not_transport_uncertainty():
    rejected = classify_mutation_response(
        {"code": "0", "data": [{"sCode": "51008", "sMsg": "position missing"}]},
        expected_items=1, submitted_client_id="expected",
    )
    ambiguous = classify_mutation_response(
        {"code": "0", "data": [{"sCode": "0"}]},
        expected_items=1, submitted_client_id="expected",
    )
    assert rejected.outcome == MUTATION_DEFINITIVE_REJECTION
    assert ambiguous.outcome == MUTATION_READBACK_REQUIRED


def test_cancellation_before_post_callback_gets_terminal_accounting(tmp_path, monkeypatch):
    class Reservations:
        async def release(self, _reservation_id):
            return True

    class Client:
        async def place_market_order(self, *args, **kwargs):
            raise asyncio.CancelledError()

    request = _request("cancel-before-post")
    request = ExecutionRequest(**{**request.__dict__, "approved_notional_usdt": 100.0})
    orders = OrderStateStore(tmp_path / "orders.json")
    router = ExecutionRouter(Client(), reservations=Reservations(), orders=orders)
    monkeypatch.setenv("PAPER_TRADING", "true")
    monkeypatch.setattr(router, "_release_pre_submit_registry", lambda _coin: None)

    with pytest.raises(asyncio.CancelledError):
        asyncio.run(router.submit(request))
    row = orders.get(request.intent_id)
    assert row["state"] == "REJECTED"
    assert row["terminal_class"] == "local_rejection"
    assert row["terminal_reason"] == "submission_task_cancelled_before_exchange_request"


def test_payload_ready_recovery_never_posts_and_terminalizes(tmp_path, monkeypatch):
    class Reservations:
        async def release(self, _reservation_id):
            return True

    class Client:
        async def place_market_order(self, *args, **kwargs):
            raise AssertionError("recovery must never POST a payload-ready intent")

    orders = OrderStateStore(tmp_path / "orders.json")
    request = _request()
    orders.create({**request.__dict__, "client_order_id": "expected", "state": "PAYLOAD_READY"})
    router = ExecutionRouter(Client(), reservations=Reservations(), orders=orders)
    monkeypatch.setattr(router, "_release_pre_submit_registry", lambda _coin: None)

    recovered = asyncio.run(router.recover_unresolved())
    row = orders.get(request.intent_id)

    assert len(recovered) == 1
    assert row["state"] == "FAILED_FINAL"
    assert row["terminal_class"] == "local_rejection"
    assert row["terminal_reason"] == "recovery_payload_ready_never_submitted"


def test_post_boundary_empty_recovery_remains_exchange_owned_and_keeps_reservation(tmp_path):
    class Reservations:
        def __init__(self):
            self.releases = []

        async def release(self, reservation_id):
            self.releases.append(reservation_id)
            return True

    class Client:
        async def get_order(self, *_args, **_kwargs):
            return {}

        async def get_order_history(self, *_args, **_kwargs):
            return []

        async def get_fills(self, *_args, **_kwargs):
            return []

        async def get_positions(self, *_args, **_kwargs):
            return []

    request = _request("empty-post-boundary")
    orders = OrderStateStore(tmp_path / "orders.json")
    orders.create({**request.__dict__, "client_order_id": "expected", "state": "SUBMISSION_ATTEMPTED"})
    reservations = Reservations()
    result = asyncio.run(ExecutionRouter(Client(), reservations=reservations, orders=orders)._recover_uncertain(
        orders.get(request.intent_id), request,
    ))
    row = orders.get(request.intent_id)
    assert result.state == "SUBMIT_UNCERTAIN"
    assert row["recovery_reason"] == "uncertain_submission_no_authoritative_evidence"
    assert not row.get("terminal_finalized_at")
    assert reservations.releases == []


@pytest.mark.parametrize("correlated_fill", [False, True])
def test_recovery_never_uses_position_or_event_fill_without_order_cumulative_fill(tmp_path, correlated_fill):
    class Client:
        async def get_order(self, *_args, **_kwargs):
            return {"state": "live", "ordId": "order-1", "clOrdId": "expected"}

        async def get_order_history(self, *_args, **_kwargs):
            return []

        async def get_fills(self, *_args, **_kwargs):
            return ([{"ordId": "order-1", "clOrdId": "expected", "fillSz": "1", "fillPx": "100"}]
                    if correlated_fill else [])

        async def get_positions(self, *_args, **_kwargs):
            return [{"instId": "TEST-USDT-SWAP", "posSide": "long", "pos": "1", "avgPx": "100"}]

    request = _request(f"unknown-cumulative-{correlated_fill}")
    orders = OrderStateStore(tmp_path / "orders.json")
    orders.create({**request.__dict__, "client_order_id": "expected", "state": "SUBMISSION_ATTEMPTED"})
    router = ExecutionRouter(Client(), orders=orders)
    result = asyncio.run(router._recover_uncertain(orders.get(request.intent_id), request))
    row = orders.get(request.intent_id)
    assert result.state == "SUBMIT_UNCERTAIN"
    assert not row.get("terminal_finalized_at")
    assert row["filled_contracts"] == 0.0
    assert "authoritative_order_cumulative_fill" in row["recovery_reason"]


def test_post_boundary_reservation_pin_survives_ttl_but_proven_presubmit_expires(tmp_path):
    book = CapitalReservationBook(tmp_path / "reservations.json")

    async def reserve():
        return await book.reserve(
            intent_id="pinned", candidate_id="c", margin_usdt=3, risk_usdt=1,
            max_total_margin_usdt=10, ttl_secs=1,
        )

    ok, _, pinned_id = asyncio.run(reserve())
    assert ok
    router = ExecutionRouter(object(), reservations=book, orders=OrderStateStore(tmp_path / "orders.json"))
    assert router._pin_reservation_at_boundary(
        pinned_id, state="SUBMISSION_ATTEMPTED", reason="test_boundary",
    )
    row = book.get(pinned_id)
    assert book.expire_sync(float(row["expires_at"]) + 10) == 0
    assert book.get(pinned_id)["ownership_state"] == "SUBMISSION_ATTEMPTED"
    ok, _, fresh_id = asyncio.run(book.reserve(
        intent_id="pre", candidate_id="c", margin_usdt=1, risk_usdt=1,
        max_total_margin_usdt=10, ttl_secs=1,
    ))
    assert ok
    assert book.expire_sync(float(book.get(fresh_id)["expires_at"]) + 10) == 1
    assert book.get(fresh_id) is None


def test_cancel_boundary_serializes_concurrent_same_intent_posts(tmp_path):
    class Client:
        def __init__(self):
            self.posts = 0

        async def cancel_order(self, *_args, **_kwargs):
            self.posts += 1
            await asyncio.sleep(0)
            return {"sCode": "0", "ordId": "order-1"}

        async def get_order(self, *_args, **_kwargs):
            return {}

    request = _request("cancel-one-post")
    orders = OrderStateStore(tmp_path / "orders.json")
    orders.create({**request.__dict__, "client_order_id": "expected", "exchange_order_id": "order-1", "state": "ACKNOWLEDGED"})
    client = Client()
    router = ExecutionRouter(client, orders=orders)

    async def cancel_twice():
        await asyncio.gather(
            router.cancel("TEST", order_id="order-1", client_order_id="expected", intent_id=request.intent_id),
            router.cancel("TEST", order_id="order-1", client_order_id="expected", intent_id=request.intent_id),
        )

    asyncio.run(cancel_twice())
    assert client.posts == 1
    assert orders.get(request.intent_id)["cancel_boundary"]["state"] == "POST_ATTEMPTED"


def test_close_adapter_routes_to_durable_operator_lifecycle(tmp_path):
    class Client:
        async def close_futures_position(self, *_args, **_kwargs):
            raise AssertionError("legacy direct close bypass")

    router = ExecutionRouter(Client(), orders=OrderStateStore(tmp_path / "orders.json"))
    calls = []

    async def durable_close(*args, **kwargs):
        calls.append((args, kwargs))
        return {"status": "CLOSE_UNCERTAIN", "intent_id": kwargs["intent_id"], "post_count": 0}

    router.operator_close = durable_close
    result = asyncio.run(router.close("TEST", "long", 0.0, emergency=True, intent_id="close-adapter"))
    assert result["status"] == "CLOSE_UNCERTAIN"
    assert calls and calls[0][1]["source"] == "emergency_strategy_close"
    assert calls[0][1]["intent_id"] == "close-adapter"


def test_corrupt_order_state_store_blocks_reads_and_writes(tmp_path):
    path = tmp_path / "orders.json"
    path.write_text("{not-json", encoding="utf-8")
    store = OrderStateStore(path)
    assert store.health()["healthy"] is False
    with pytest.raises(OrderStateStoreUnavailable):
        store.get("unknown")
    with pytest.raises(OrderStateStoreUnavailable):
        store.create({"intent_id": "must-not-overwrite"})

    class Client:
        def __init__(self):
            self.posts = 0

        async def place_market_order(self, *_args, **_kwargs):
            self.posts += 1

    client = Client()
    with pytest.raises(OrderStateStoreUnavailable):
        asyncio.run(ExecutionRouter(client, orders=store).submit(_request("corrupt-store-submit")))
    assert client.posts == 0
    assert path.read_text(encoding="utf-8") == "{not-json"


@pytest.mark.parametrize("confirmed_state, terminal", [("live", False), ("partially_filled", False), ("", False), ("cancelled", True)])
def test_cancel_acknowledgement_requires_authoritative_terminal_readback(tmp_path, confirmed_state, terminal):
    class Reservations:
        async def release(self, _reservation_id):
            return True

    class Client:
        async def cancel_order(self, *_args, **_kwargs):
            return {"sCode": "0", "ordId": "order-1"}

        async def get_order(self, *_args, **_kwargs):
            if not confirmed_state:
                return {}
            snapshot = {"state": confirmed_state, "ordId": "order-1"}
            if confirmed_state == "cancelled":
                snapshot["accFillSz"] = "0"
            return snapshot

    request = _request("cancel-confirmation")
    orders = OrderStateStore(tmp_path / "orders.json")
    orders.create({**request.__dict__, "client_order_id": "expected", "exchange_order_id": "order-1", "state": "ACKNOWLEDGED"})
    router = ExecutionRouter(Client(), reservations=Reservations(), orders=orders)
    asyncio.run(router.cancel("TEST", order_id="order-1", client_order_id="expected", intent_id=request.intent_id))
    row = orders.get(request.intent_id)
    assert bool(row.get("terminal_finalized_at")) is terminal
    assert row["state"] == ("CANCELLED" if terminal else "CANCEL_PENDING")


@pytest.mark.parametrize("restart_state", ["SUBMISSION_ATTEMPTED", "SUBMITTED", "ACKNOWLEDGED"])
def test_post_boundary_restart_reconciles_without_reposting(tmp_path, monkeypatch, restart_state):
    class Reservations:
        async def release(self, _reservation_id):
            return True

    class Client:
        async def place_market_order(self, *args, **kwargs):
            raise AssertionError("restart reconciliation must never POST")

        async def get_order(self, *args, **kwargs):
            return {"state": "cancelled", "ordId": "order-1", "clOrdId": "expected", "accFillSz": "0"}

        async def get_order_history(self, *args, **kwargs):
            return []

        async def get_fills(self, *args, **kwargs):
            return []

        async def get_positions(self, *args, **kwargs):
            return []

    orders = OrderStateStore(tmp_path / "orders.json")
    request = _request(f"restart-{restart_state}")
    orders.create({
        **request.__dict__, "client_order_id": "expected", "exchange_order_id": "order-1",
        "state": restart_state, "mutation_attempted": True,
    })
    router = ExecutionRouter(Client(), reservations=Reservations(), orders=orders)
    monkeypatch.setattr(router, "_release_pre_submit_registry", lambda _coin: None)

    recovered = asyncio.run(router.recover_unresolved())
    row = orders.get(request.intent_id)

    assert len(recovered) == 1
    assert row["state"] == "CANCELLED"
    assert row["terminal_class"] == "cancelled"
    assert row["recovery_evidence"]["order"]["ordId"] == "order-1"


def test_same_intent_concurrency_has_one_physical_post_but_distinct_intents_progress(tmp_path, monkeypatch):
    """The intent lock owns admission without serializing unrelated intents."""
    monkeypatch.setenv("PAPER_TRADING", "true")

    class Reservations:
        async def release(self, _reservation_id):
            return True

    class Client:
        def __init__(self):
            self.posts = []
            self.two_distinct_posts = asyncio.Event()
            self.release_posts = asyncio.Event()

        async def place_market_order(self, _coin, _side, _notional, **kwargs):
            kwargs["on_exchange_request"]()
            self.posts.append(kwargs["client_order_id"])
            if len(set(self.posts)) >= 2:
                self.two_distinct_posts.set()
            await self.release_posts.wait()
            return None

    async def scenario():
        client = Client()
        router = ExecutionRouter(client, reservations=Reservations(), orders=OrderStateStore(tmp_path / "orders.json"))
        monkeypatch.setattr(router, "_local_executable_check", lambda *_args, **_kwargs: (True, "OK", {}))
        monkeypatch.setattr(router, "_release_pre_submit_registry", lambda _coin: None)

        same = _request("same")
        first = asyncio.create_task(router.submit(same))
        await asyncio.sleep(0)
        second = asyncio.create_task(router.submit(same))
        await asyncio.sleep(0.02)
        assert len(client.posts) == 1

        other = asyncio.create_task(router.submit(_request("other")))
        await asyncio.wait_for(client.two_distinct_posts.wait(), timeout=1)
        client.release_posts.set()
        await asyncio.gather(first, second, other, return_exceptions=True)
        return client.posts

    posts = asyncio.run(scenario())
    assert len(set(posts)) == 2
    assert len(posts) == 2


def test_terminal_reservation_cleanup_retries_without_second_terminal_event(tmp_path, monkeypatch):
    calls = {"release": 0, "telemetry": 0}

    class Reservations:
        async def release(self, _reservation_id):
            calls["release"] += 1
            if calls["release"] == 1:
                raise RuntimeError("temporary release failure")
            return True

    request = _request("cleanup-retry")
    orders = OrderStateStore(tmp_path / "orders.json")
    orders.create({**request.__dict__, "state": "ACKNOWLEDGED"})
    router = ExecutionRouter(object(), reservations=Reservations(), orders=orders)
    monkeypatch.setattr("execution_router._flow.record_payload_terminal", lambda **_kwargs: calls.__setitem__("telemetry", calls["telemetry"] + 1))

    first = asyncio.run(router._finalize_terminal(
        request, state="REJECTED", terminal_class="exchange_rejection",
        reason="business_rejection", exchange_contacted=True,
    ))
    second = asyncio.run(router._finalize_terminal(
        request, state="REJECTED", terminal_class="exchange_rejection",
        reason="ignored_retry_reason", exchange_contacted=True,
    ))

    assert first["terminal_cleanup_status"] == "pending"
    assert second["terminal_cleanup_status"] == "complete"
    assert calls == {"release": 2, "telemetry": 1}
    assert [item["state"] for item in second["history"]].count("REJECTED") == 1


def test_terminal_cleanup_pending_recovers_after_reload_without_post(tmp_path, monkeypatch):
    calls = {"release": 0}

    class Reservations:
        async def release(self, _reservation_id):
            calls["release"] += 1
            if calls["release"] == 1:
                raise OSError("interrupted")
            return True

    class Client:
        async def place_market_order(self, *_args, **_kwargs):
            raise AssertionError("cleanup recovery must not POST")

    request = _request("cleanup-reload")
    path = tmp_path / "orders.json"
    first = OrderStateStore(path)
    first.create({**request.__dict__, "state": "ACKNOWLEDGED"})
    router = ExecutionRouter(Client(), reservations=Reservations(), orders=first)
    asyncio.run(router._finalize_terminal(request, state="REJECTED", terminal_class="exchange_rejection", reason="x", exchange_contacted=True))
    assert first.get(request.intent_id)["terminal_cleanup_status"] == "pending"

    reloaded = OrderStateStore(path)
    asyncio.run(ExecutionRouter(Client(), reservations=Reservations(), orders=reloaded).recover_unresolved())
    row = reloaded.get(request.intent_id)
    assert calls["release"] == 2
    assert row["terminal_cleanup_status"] == "complete"
    assert row["terminal_class"] == "exchange_rejection"
    assert [item["state"] for item in row["history"]].count("REJECTED") == 1


def test_terminal_cleanup_false_release_stays_pending(tmp_path):
    class Reservations:
        async def release(self, _reservation_id):
            return False

    request = _request("cleanup-unconfirmed")
    orders = OrderStateStore(tmp_path / "orders.json")
    orders.create({**request.__dict__, "state": "ACKNOWLEDGED"})
    router = ExecutionRouter(object(), reservations=Reservations(), orders=orders)

    row = asyncio.run(router._finalize_terminal(
        request, state="REJECTED", terminal_class="exchange_rejection",
        reason="business_rejection", exchange_contacted=True,
    ))

    assert row["terminal_cleanup_status"] == "pending"
    assert row["terminal_cleanup_error"] == "reservation_release_not_confirmed"
    assert row["terminal_class"] == "exchange_rejection"


def test_terminal_telemetry_outbox_retries_and_sink_dedupes(tmp_path, monkeypatch):
    committed = []
    request = _request("telemetry-outbox")
    orders = OrderStateStore(tmp_path / "orders.json")
    orders.create({**request.__dict__, "state": "ACKNOWLEDGED"})
    router = ExecutionRouter(object(), orders=orders)

    def failing_sink(**_kwargs):
        raise OSError("audit unavailable")

    monkeypatch.setattr("execution_router._flow.record_payload_terminal", failing_sink)
    asyncio.run(router._finalize_terminal(request, state="REJECTED", terminal_class="exchange_rejection", reason="x", exchange_contacted=True))
    assert orders.get(request.intent_id)["terminal_telemetry_status"] == "pending"

    seen = set()
    def deduping_sink(**event):
        if event["terminal_event_id"] not in seen:
            seen.add(event["terminal_event_id"])
            committed.append(event)

    monkeypatch.setattr("execution_router._flow.record_payload_terminal", deduping_sink)
    asyncio.run(router.recover_unresolved())
    assert len(committed) == 1
    assert orders.get(request.intent_id)["terminal_telemetry_status"] == "complete"

    # Simulate crash after durable sink commit but before acknowledgement.
    orders.mark_terminal_telemetry(request.intent_id, status="pending")
    asyncio.run(router.recover_unresolved())
    assert len(committed) == 1


def test_partial_cancel_ack_stays_nonterminal_until_authoritative_cancel(tmp_path, monkeypatch):
    class Reservations:
        async def release(self, _reservation_id):
            return True

    class Client:
        cancel_requested = False

        async def cancel_order(self, *_args, **_kwargs):
            self.cancel_requested = True
            return {"sCode": "0", "ordId": "order-1"}

        async def get_order(self, *_args, **_kwargs):
            if self.cancel_requested:
                return {"state": "canceled", "ordId": "order-1", "accFillSz": "0.4", "avgPx": "100"}
            return {"state": "partially_filled", "ordId": "order-1", "accFillSz": "0.4", "avgPx": "100"}

        async def get_positions_status(self, **_kwargs):
            return {"positions": [{
                "instId": "TEST-USDT-SWAP", "posSide": "long", "pos": "0.4", "lever": "1",
            }]}

    class Instruments:
        def require(self, _coin):
            return SimpleNamespace(contracts_to_notional=lambda contracts, price: contracts * price)

    request = ExecutionRequest(**{**_request("partial-cancel").__dict__, "timeout_secs": 0.001})
    orders = OrderStateStore(tmp_path / "orders.json")
    orders.create({
        **request.__dict__, "client_order_id": "expected", "exchange_order_id": "order-1",
        "state": "ACKNOWLEDGED",
    })
    router = ExecutionRouter(Client(), reservations=Reservations(), orders=orders, instruments=Instruments())
    monkeypatch.setattr(router, "paper_mode", lambda: False)
    import position_registry
    monkeypatch.setattr(position_registry, "_PATH", tmp_path / "position_registry.json")
    monkeypatch.setattr(position_registry, "_registry", {
        "TEST": {"direction": "long", "engine": "TEST", "size_usdt": 10.0,
                 "entry": 100.0, "leverage": 1, "stop_loss": 0.0,
                 "take_profit": 0.0, "pending_reservation": True},
    })
    monkeypatch.setattr(
        router.protection, "ensure",
        lambda **_kwargs: asyncio.sleep(0, result=SimpleNamespace(
            protected=True, reason="", protection_state="CONFIRMED",
            confirmed_stop_price=0.0, sl_algo_id="", tp_algo_ids=[],
        )),
    )
    asyncio.run(router._confirm_live_state(request, "order-1", "expected"))
    pending = orders.get(request.intent_id)
    assert pending["state"] == "CANCEL_PENDING"
    assert not pending.get("terminal_finalized_at")
    assert pending["filled_contracts"] == pytest.approx(0.4)

    asyncio.run(router.cancel("TEST", order_id="order-1", client_order_id="expected", intent_id=request.intent_id))
    final = orders.get(request.intent_id)
    assert final["state"] == "CANCELLED"
    assert final["filled_contracts"] == pytest.approx(0.4)
    assert final["terminal_class"] == "cancelled"
    assert final["partial_execution"] is True
    assert final["unfilled_remainder_cancelled"] is True


@pytest.mark.parametrize("protected, terminal_class", [(True, "filled"), (False, "protection_failure")])
def test_confirmed_fill_finalizes_only_after_protection_result(tmp_path, monkeypatch, protected, terminal_class):
    monkeypatch.setenv("PAPER_TRADING", "true")

    class Reservations:
        async def release(self, _reservation_id):
            return True

    class Client:
        async def place_market_order(self, *_args, **kwargs):
            kwargs["on_exchange_request"]()
            return {"ordId": "paper-1", "contracts": 1.0, "fillPx": "100"}

    request = _request(f"protection-{protected}")
    router = ExecutionRouter(Client(), reservations=Reservations(), orders=OrderStateStore(tmp_path / f"{protected}.json"))
    monkeypatch.setattr(router, "_local_executable_check", lambda *_args, **_kwargs: (True, "OK", {}))
    monkeypatch.setattr(
        router,
        "_position_effective_leverage",
        lambda req: asyncio.sleep(0, result={
            "position_effective_leverage": int(req.exchange_configured_leverage or req.leverage or 1),
            "leverage_truth_status": req.leverage_truth_status or "PAPER_LOCAL",
            "leverage_truth_source": "paper",
            "leverage_truth_mismatch": False,
            "requested_vs_configured_mismatch": False,
            "configured_vs_effective_mismatch": False,
        }),
    )
    monkeypatch.setattr("position_registry.update", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(
        router.protection, "ensure",
        lambda **_kwargs: asyncio.sleep(0, result=SimpleNamespace(
            protected=protected, reason="readback_failed" if not protected else "",
            protection_state="CONFIRMED" if protected else "READBACK_FAILED",
            confirmed_stop_price=0.0, sl_algo_id="", tp_algo_ids=[],
        )),
    )

    result = asyncio.run(router.submit(request))
    row = router.orders.get(request.intent_id)
    assert row["state"] == "FILLED"
    assert row["terminal_class"] == terminal_class
    assert result.ok is protected


def test_confirmed_fill_protection_exception_is_durable_protection_failure(tmp_path, monkeypatch):
    monkeypatch.setenv("PAPER_TRADING", "true")

    class Reservations:
        async def release(self, _reservation_id):
            return True

    class Client:
        async def place_market_order(self, *_args, **kwargs):
            kwargs["on_exchange_request"]()
            return {"ordId": "paper-1", "contracts": 1.0, "fillPx": "100"}

    async def protection_error(**_kwargs):
        raise RuntimeError("protection unavailable")

    request = _request("protection-exception")
    router = ExecutionRouter(Client(), reservations=Reservations(), orders=OrderStateStore(tmp_path / "orders.json"))
    monkeypatch.setattr(router, "_local_executable_check", lambda *_args, **_kwargs: (True, "OK", {}))
    monkeypatch.setattr(
        router,
        "_position_effective_leverage",
        lambda req: asyncio.sleep(0, result={
            "position_effective_leverage": int(req.exchange_configured_leverage or req.leverage or 1),
            "leverage_truth_status": req.leverage_truth_status or "PAPER_LOCAL",
            "leverage_truth_source": "paper",
            "leverage_truth_mismatch": False,
            "requested_vs_configured_mismatch": False,
            "configured_vs_effective_mismatch": False,
        }),
    )
    monkeypatch.setattr("position_registry.update", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(router.protection, "ensure", protection_error)

    result = asyncio.run(router.submit(request))
    row = router.orders.get(request.intent_id)
    assert not result.ok
    assert row["state"] == "FILLED"
    assert row["terminal_class"] == "protection_failure"
    assert "protection_exception_after_fill" in row["terminal_reason"]


def test_staged_children_do_not_release_parent_reservation_between_posts(tmp_path):
    events = []

    class Reservations:
        async def release(self, _reservation_id):
            events.append("release-parent")

    orders = OrderStateStore(tmp_path / "orders.json")
    router = ExecutionRouter(object(), reservations=Reservations(), orders=orders)
    parent = _request("parent")
    child_one = ExecutionRequest(**{**_request("parent-S1").__dict__, "metadata": {"reservation_owner_intent_id": "parent"}})
    child_two = ExecutionRequest(**{**_request("parent-S2").__dict__, "metadata": {"reservation_owner_intent_id": "parent"}})
    for request in (parent, child_one, child_two):
        orders.create({**request.__dict__, "state": "ACKNOWLEDGED"})

    events.append("post-child-1")
    asyncio.run(router._finalize_terminal(child_one, state="FILLED", terminal_class="filled", reason="child", exchange_contacted=True))
    events.append("post-child-2")
    asyncio.run(router._finalize_terminal(child_two, state="FILLED", terminal_class="filled", reason="child", exchange_contacted=True))
    asyncio.run(router._finalize_terminal(parent, state="FILLED", terminal_class="filled", reason="parent", exchange_contacted=True))

    assert events == ["post-child-1", "post-child-2", "release-parent"]


def test_staged_parent_propagates_terminal_child_failure_with_aggregate_fill(tmp_path):
    class Reservations:
        async def release(self, _reservation_id):
            return True

    class Instruments:
        def require(self, _coin):
            return SimpleNamespace(lot_sz=0.1)

    parent = ExecutionRequest(**{**_request("staged-child-failure").__dict__, "order_type": "staged", "approved_contracts": 2.0})
    manifest = {"version": 1, "staged_parent_intent_id": parent.intent_id,
                "reservation_owner_intent_id": parent.intent_id, "reservation_id": "reservation",
                "stage_count": 2, "children": [
                    {"intent_id": f"{parent.intent_id}-S1", "stage_index": 1, "requested_contracts": 1.0, "approved_contracts": 1.0, "approved_notional_usdt": 10.0, "price": 100.0},
                    {"intent_id": f"{parent.intent_id}-S2", "stage_index": 2, "requested_contracts": 1.0, "approved_contracts": 1.0, "approved_notional_usdt": 10.0, "price": 100.0},
                ]}
    orders = OrderStateStore(tmp_path / "orders.json")
    orders.create({**parent.__dict__, "state": "INTENT_CREATED", "staged_manifest": manifest})
    for child_id, terminal_class in ((f"{parent.intent_id}-S1", "filled"), (f"{parent.intent_id}-S2", "protection_failure")):
        child = ExecutionRequest(**{**_request(child_id).__dict__, "order_type": "limit"})
        orders.create({**child.__dict__, "state": "ACKNOWLEDGED"})
        orders.finalize_terminal(child_id, "FILLED", terminal_class=terminal_class,
                                terminal_reason="child", filled_contracts=1.0, average_fill_price=100.0)
    router = ExecutionRouter(object(), reservations=Reservations(), orders=orders, instruments=Instruments())

    result = asyncio.run(router._resume_staged_parent(parent, orders.get(parent.intent_id)))
    row = orders.get(parent.intent_id)
    assert not result.ok
    assert row["terminal_class"] == "protection_failure"
    assert row["filled_contracts"] == pytest.approx(2.0)
    assert any(item["terminal_class"] == "protection_failure" for item in row["child_results"])


def test_live_shared_reservation_requires_durable_staged_lineage(tmp_path, monkeypatch):
    class Reservations:
        def get(self, _reservation_id):
            return {"intent_id": "parent", "candidate_id": "candidate"}

    class Client:
        async def get_execution_ticker(self, _coin):
            return {"last": "100"}

        async def get_account_snapshot(self, **_kwargs):
            return {"avail_eq_usdt": 100, "total_eq_usdt": 100}

        async def get_account_config(self, **_kwargs):
            return {"posMode": "net_mode"}

    class Live:
        def can_submit_live(self):
            return True, "OK"

    class Instruments:
        def require(self, _coin):
            return SimpleNamespace(inst_id="TEST-USDT-SWAP")

    orders = OrderStateStore(tmp_path / "orders.json")
    parent = ExecutionRequest(**{**_request("parent").__dict__, "leverage": 5})
    child = ExecutionRequest(**{**_request("parent-S1").__dict__, "leverage": 5, "metadata": {
        "reservation_owner_intent_id": "parent", "staged_parent_intent_id": "parent",
        "staged_stage_index": 1, "staged_stage_count": 1,
    }})
    orders.create({**parent.__dict__, "state": "ACKNOWLEDGED", "staged_manifest": {
        "version": 1, "staged_parent_intent_id": "parent",
        "reservation_owner_intent_id": "parent", "reservation_id": "reservation",
        "stage_count": 1, "children": [{
            "intent_id": "parent-S1", "stage_index": 1,
            "requested_contracts": 1.0, "approved_contracts": 1.0,
            "approved_notional_usdt": 10.0, "price": 100.0,
        }],
    }})
    orders.create({**child.__dict__, "state": "INTENT_CREATED"})
    router = ExecutionRouter(Client(), reservations=Reservations(), orders=orders, instruments=Instruments(), live_state=Live())
    monkeypatch.setattr(router, "paper_mode", lambda: False)
    monkeypatch.setattr(router, "_fresh_position_truth", lambda: asyncio.sleep(0, result=(True, "OK", {})))
    valid, reason, _ = asyncio.run(router._validate_live_submission(child))
    assert valid, reason

    unrelated = ExecutionRequest(**{**_request("other").__dict__, "leverage": 5, "metadata": {
        "reservation_owner_intent_id": "parent", "staged_parent_intent_id": "parent",
    }})
    orders.create({**unrelated.__dict__, "state": "INTENT_CREATED"})
    valid, reason, _ = asyncio.run(router._validate_live_submission(unrelated))
    assert not valid
    assert reason == "capital_reservation_intent_mismatch"

    forged = ExecutionRequest(**{**_request("parent-S999").__dict__, "leverage": 5, "metadata": {
        "reservation_owner_intent_id": "parent", "staged_parent_intent_id": "parent",
        "staged_stage_index": 999, "staged_stage_count": 1,
    }})
    orders.create({**forged.__dict__, "state": "INTENT_CREATED"})
    valid, reason, _ = asyncio.run(router._validate_live_submission(forged))
    assert not valid
    assert reason == "capital_reservation_intent_mismatch"

    orders.transition("parent-S1", "INTENT_CREATED", metadata={
        **child.metadata, "staged_stage_index": 2,
    })
    valid, reason, _ = asyncio.run(router._validate_live_submission(child))
    assert not valid
    assert reason == "capital_reservation_intent_mismatch"


@pytest.mark.parametrize("protected, expected_class", [(True, "cancelled"), (False, "protection_failure")])
def test_direct_authoritative_partial_cancel_preserves_truth_and_protection(tmp_path, monkeypatch, protected, expected_class):
    class Reservations:
        async def release(self, _reservation_id):
            return True

    class Client:
        async def cancel_order(self, *_args, **_kwargs):
            return {"sCode": "0", "ordId": "order-1"}

        async def get_order(self, *_args, **_kwargs):
            return {"state": "canceled", "ordId": "order-1", "accFillSz": "0.4", "avgPx": "100", "fee": "-0.1"}

        async def get_positions_status(self, **_kwargs):
            return {"positions": [{
                "instId": "TEST-USDT-SWAP", "posSide": "long", "pos": "0.4", "lever": "7",
            }]}

    request = ExecutionRequest(**{
        **_request(f"direct-partial-{protected}").__dict__,
        "stop_price": 90.0, "take_profit": 120.0, "leverage": 7,
        "metadata": {"tp_owner": "router", "tp_mode_at_entry": "attached"},
    })
    orders = OrderStateStore(tmp_path / "orders.json")
    orders.create({**request.__dict__, "state": "ACKNOWLEDGED", "client_order_id": "expected", "exchange_order_id": "order-1"})
    class Instruments:
        def require(self, _coin):
            return SimpleNamespace(contracts_to_notional=lambda contracts, price: contracts * price)

    router = ExecutionRouter(Client(), reservations=Reservations(), orders=orders, instruments=Instruments())
    monkeypatch.setattr(router, "paper_mode", lambda: False)
    import position_registry
    monkeypatch.setattr(position_registry, "_PATH", tmp_path / "position_registry.json")
    monkeypatch.setattr(position_registry, "_registry", {
        "TEST": {"direction": "long", "engine": "TEST", "size_usdt": 10.0,
                 "entry": 100.0, "leverage": 7, "stop_loss": 90.0,
                 "take_profit": 120.0, "pending_reservation": True},
    })
    seen = {}
    async def ensure(**kwargs):
        seen.update(kwargs)
        return SimpleNamespace(
        protected=protected, reason="protection failed" if not protected else "", protection_state="CONFIRMED",
        confirmed_stop_price=0.0, sl_algo_id="", tp_algo_ids=[],
        )
    monkeypatch.setattr(router.protection, "ensure", ensure)

    asyncio.run(router.cancel("TEST", order_id="order-1", client_order_id="expected", intent_id=request.intent_id))
    row = orders.get(request.intent_id)
    assert row["requested_contracts"] == pytest.approx(1.0)
    assert row["filled_contracts"] == pytest.approx(0.4)
    assert row["partial_execution"] is True
    assert row["unfilled_remainder_cancelled"] is True
    assert row["cancel_confirmation"]["state"] == "canceled"
    assert row["terminal_class"] == expected_class
    assert seen["stop_price"] == pytest.approx(90.0)
    assert seen["take_profit"] == pytest.approx(120.0)
    assert seen["tp_owner"] == "router"
    assert position_registry._registry["TEST"]["actual_filled_contracts"] == pytest.approx(0.4)
    assert position_registry._registry["TEST"]["pos_side"] == "long"

    # Repeated direct convergence keeps the one exact surviving position and
    # does not run another protection attachment.
    asyncio.run(router.cancel("TEST", order_id="order-1", client_order_id="expected", intent_id=request.intent_id))
    assert position_registry._registry["TEST"]["actual_filled_contracts"] == pytest.approx(0.4)


def test_recovered_partial_cancel_protection_failure_keeps_cancel_truth(tmp_path, monkeypatch):
    class Reservations:
        async def release(self, _reservation_id):
            return True

    class Client:
        async def get_order(self, *_args, **_kwargs):
            return {"state": "canceled", "ordId": "order-1", "clOrdId": "expected", "accFillSz": "0.4", "avgPx": "100"}

        async def get_order_history(self, *_args, **_kwargs):
            return []

        async def get_fills(self, *_args, **_kwargs):
            return [{"ordId": "order-1", "fillId": "fill-1", "fillSz": "0.4", "fillPx": "100"}]

        async def get_positions(self, *_args, **_kwargs):
            return [{
                "instId": "TEST-USDT-SWAP", "posSide": "long", "pos": "0.4", "lever": "1",
            }]

    class Instruments:
        def require(self, _coin):
            return SimpleNamespace(contracts_to_notional=lambda contracts, price: contracts * price)

    request = ExecutionRequest(**{
        **_request("recovered-partial-protection-failure").__dict__,
        "stop_price": 90.0, "take_profit": 120.0, "metadata": {"tp_owner": "router"},
    })
    orders = OrderStateStore(tmp_path / "orders.json")
    orders.create({**request.__dict__, "state": "CANCEL_PENDING", "client_order_id": "expected", "exchange_order_id": "order-1"})
    router = ExecutionRouter(Client(), reservations=Reservations(), orders=orders, instruments=Instruments())
    monkeypatch.setattr("position_registry.update", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(router.protection, "ensure", lambda **_kwargs: asyncio.sleep(0, result=SimpleNamespace(
        protected=False, reason="stop failed", protection_state="FAILED", confirmed_stop_price=0.0,
        sl_algo_id="", tp_algo_ids=[],
    )))

    result = asyncio.run(router._recover_uncertain(orders.get(request.intent_id), request))
    row = orders.get(request.intent_id)
    assert not result.ok
    assert row["state"] == "CANCELLED"
    assert row["terminal_class"] == "protection_failure"
    assert row["terminal_reason"] == "partial_cancel_protection_failure"
    assert row["partial_execution"] is True
    assert row["unfilled_remainder_cancelled"] is True
    assert row["cancel_confirmation"]["state"] == "canceled"


def test_same_intent_rereads_post_boundary_state_after_lock(tmp_path, monkeypatch):
    class Reservations:
        async def release(self, _reservation_id):
            return True

    class Client:
        async def get_order(self, *_args, **_kwargs):
            return {"state": "live", "ordId": "order-1", "clOrdId": "expected"}

        async def get_order_history(self, *_args, **_kwargs):
            return []

        async def get_fills(self, *_args, **_kwargs):
            return []

        async def get_positions(self, *_args, **_kwargs):
            if self.state == "filled":
                return [{"instId": "TEST-USDT-SWAP", "posSide": "long", "pos": "1", "lever": "10", "avgPx": "100"}]
            return []

    request = _request("stale-payload-ready")
    orders = OrderStateStore(tmp_path / "orders.json")
    orders.create({**request.__dict__, "state": "PAYLOAD_READY", "client_order_id": "expected"})
    router = ExecutionRouter(Client(), reservations=Reservations(), orders=orders)
    monkeypatch.setattr(router, "_release_pre_submit_registry", lambda _coin: None)

    async def scenario():
        async with router._lock(request.intent_id):
            duplicate = asyncio.create_task(router.submit(request))
            await asyncio.sleep(0)
            orders.transition(request.intent_id, "SUBMIT_UNCERTAIN", exchange_request_at=1.0, mutation_attempted=True)
        return await duplicate

    result = asyncio.run(scenario())
    row = orders.get(request.intent_id)
    assert not result.ok
    assert row["state"] != "FAILED_FINAL"
    assert row.get("exchange_request_at") == 1.0


def test_authoritative_zero_fill_cancel_releases_risk_slot_once(tmp_path, monkeypatch):
    class Reservations:
        async def release(self, _reservation_id):
            return True

    class Client:
        async def cancel_order(self, *_args, **_kwargs):
            return {"sCode": "0", "ordId": "order-1"}

        async def get_order(self, *_args, **_kwargs):
            return {"state": "canceled", "ordId": "order-1", "accFillSz": "0"}

    calls = []
    import risk_manager
    monkeypatch.setattr(
        risk_manager, "risk",
        SimpleNamespace(apply_slot_operation=lambda operation_id, action: calls.append((operation_id, action))),
    )
    request = _request("zero-fill-risk-slot")
    orders = OrderStateStore(tmp_path / "orders.json")
    orders.create({**request.__dict__, "state": "ACKNOWLEDGED", "client_order_id": "expected", "exchange_order_id": "order-1", "risk_slot_acquired": True})
    router = ExecutionRouter(Client(), reservations=Reservations(), orders=orders)

    asyncio.run(router.cancel("TEST", order_id="order-1", client_order_id="expected", intent_id=request.intent_id))
    asyncio.run(router.cancel("TEST", order_id="order-1", client_order_id="expected", intent_id=request.intent_id))
    assert calls == [(f"risk-close:{request.intent_id}", "close")]
    assert orders.get(request.intent_id)["risk_slot_status"] == "complete"


def test_confirm_live_state_authoritative_zero_fill_releases_risk_slot_once(tmp_path, monkeypatch):
    class Client:
        async def get_order(self, *_args, **_kwargs):
            return {"state": "canceled", "ordId": "order-1", "accFillSz": "0"}

    calls = []
    import risk_manager
    monkeypatch.setattr(
        risk_manager, "risk",
        SimpleNamespace(apply_slot_operation=lambda operation_id, action: calls.append((operation_id, action))),
    )
    request = _request("confirm-zero-fill-risk-slot")
    orders = OrderStateStore(tmp_path / "orders.json")
    orders.create({
        **request.__dict__, "state": "ACKNOWLEDGED", "client_order_id": "expected",
        "exchange_order_id": "order-1", "risk_slot_acquired": True,
    })
    router = ExecutionRouter(Client(), orders=orders)
    asyncio.run(router._confirm_live_state(request, "order-1", "expected"))
    asyncio.run(router._confirm_live_state(request, "order-1", "expected"))
    assert calls == [(f"risk-close:{request.intent_id}", "close")]
    assert orders.get(request.intent_id)["state"] == "CANCELLED"


def test_risk_open_operation_survives_completion_write_failure_without_double_increment(tmp_path, monkeypatch):
    class Sink:
        def __init__(self):
            self.applied = set()
            self.opens = 0

        def apply_slot_operation(self, operation_id, action):
            if operation_id in self.applied:
                return False
            self.applied.add(operation_id)
            self.opens += action == "open"
            return True

    import risk_manager
    sink = Sink()
    monkeypatch.setattr(risk_manager, "risk", sink)
    request = _request("risk-open-crash")
    path = tmp_path / "orders.json"
    orders = OrderStateStore(path)
    orders.create({
        **request.__dict__, "state": "SUBMITTED", "risk_slot_acquired": True,
        "risk_slot_operations": {"open": {"operation_id": f"risk-open:{request.intent_id}", "action": "open", "status": "pending"}},
    })
    original_mark = orders.mark_risk_slot_operation
    failed = {"once": True}

    def fail_completion(*args, **kwargs):
        if kwargs.get("action") == "open" and kwargs.get("status") == "complete" and failed["once"]:
            failed["once"] = False
            raise OSError("completion write failed")
        return original_mark(*args, **kwargs)

    monkeypatch.setattr(orders, "mark_risk_slot_operation", fail_completion)
    router = ExecutionRouter(object(), orders=orders)
    assert router._apply_risk_slot_operation(request.intent_id, "open") is False
    assert sink.opens == 1
    monkeypatch.setattr(orders, "mark_risk_slot_operation", original_mark)
    restarted = ExecutionRouter(object(), orders=OrderStateStore(path))
    assert restarted._apply_risk_slot_operation(request.intent_id, "open") is True
    assert sink.opens == 1


def test_risk_close_operation_retries_after_sink_failure_and_is_idempotent(tmp_path, monkeypatch):
    class Sink:
        def __init__(self):
            self.calls = 0
            self.applied = set()
            self.fail_once = True

        def apply_slot_operation(self, operation_id, action):
            self.calls += 1
            if self.fail_once:
                self.fail_once = False
                raise OSError("sink unavailable")
            if operation_id in self.applied:
                return False
            self.applied.add(operation_id)
            return True

    import risk_manager
    sink = Sink()
    monkeypatch.setattr(risk_manager, "risk", sink)
    request = _request("risk-close-retry")
    orders = OrderStateStore(tmp_path / "orders.json")
    orders.create({
        **request.__dict__, "state": "CANCELLED", "filled_contracts": 0.0,
        "risk_slot_acquired": True, "cumulative_fill_evidence": FILL_KNOWN_ZERO,
    })
    router = ExecutionRouter(object(), orders=orders)
    assert router._reconcile_authoritative_zero_fill_risk_slot(request.intent_id) is False
    assert orders.get(request.intent_id)["risk_slot_operations"]["close"]["status"] == "pending"
    assert router._reconcile_authoritative_zero_fill_risk_slot(request.intent_id) is True
    assert router._reconcile_authoritative_zero_fill_risk_slot(request.intent_id) is True
    assert sink.calls == 2
    assert len(sink.applied) == 1


def test_risk_open_operation_retries_after_sink_failure_once(tmp_path, monkeypatch):
    class Sink:
        attempts = 0
        opens = 0

        def apply_slot_operation(self, _operation_id, action):
            self.attempts += 1
            if self.attempts == 1:
                raise OSError("open sink unavailable")
            self.opens += action == "open"
            return True

    import risk_manager
    sink = Sink()
    monkeypatch.setattr(risk_manager, "risk", sink)
    request = _request("risk-open-retry")
    orders = OrderStateStore(tmp_path / "orders.json")
    orders.create({
        **request.__dict__, "state": "SUBMITTED", "risk_slot_acquired": True,
        "risk_slot_operations": {"open": {"operation_id": f"risk-open:{request.intent_id}", "action": "open", "status": "pending"}},
    })
    router = ExecutionRouter(object(), orders=orders)
    assert router._apply_risk_slot_operation(request.intent_id, "open") is False
    assert router._apply_risk_slot_operation(request.intent_id, "open") is True
    assert sink.opens == 1


def test_risk_close_completion_write_failure_retries_without_second_decrement(tmp_path, monkeypatch):
    class Sink:
        applied = set()
        closes = 0

        def apply_slot_operation(self, operation_id, action):
            if operation_id in self.applied:
                return False
            self.applied.add(operation_id)
            self.closes += action == "close"
            return True

    import risk_manager
    sink = Sink()
    monkeypatch.setattr(risk_manager, "risk", sink)
    request = _request("risk-close-crash")
    orders = OrderStateStore(tmp_path / "orders.json")
    orders.create({
        **request.__dict__, "state": "CANCELLED", "filled_contracts": 0.0,
        "risk_slot_acquired": True, "cumulative_fill_evidence": FILL_KNOWN_ZERO,
    })
    router = ExecutionRouter(object(), orders=orders)
    original_mark = orders.mark_risk_slot_operation
    failed = {"once": True}

    def fail_completion(*args, **kwargs):
        if kwargs.get("action") == "close" and kwargs.get("status") == "complete" and failed["once"]:
            failed["once"] = False
            raise OSError("close completion write failed")
        return original_mark(*args, **kwargs)

    monkeypatch.setattr(orders, "mark_risk_slot_operation", fail_completion)
    assert router._reconcile_authoritative_zero_fill_risk_slot(request.intent_id) is False
    monkeypatch.setattr(orders, "mark_risk_slot_operation", original_mark)
    assert router._reconcile_authoritative_zero_fill_risk_slot(request.intent_id) is True
    assert sink.closes == 1


def test_zero_fill_fallback_closes_original_slot_before_child_open(tmp_path, monkeypatch):
    class Sink:
        def __init__(self):
            self.events = []

        def apply_slot_operation(self, operation_id, action):
            self.events.append((operation_id, action))
            return True

    class Client:
        async def get_order(self, *_args, **_kwargs):
            return {"state": "canceled", "ordId": "original", "accFillSz": "0"}

    import risk_manager
    sink = Sink()
    monkeypatch.setattr(risk_manager, "risk", sink)
    request = _request("fallback-risk-order")
    orders = OrderStateStore(tmp_path / "orders.json")
    orders.create({
        **request.__dict__, "state": "ACKNOWLEDGED", "client_order_id": "expected",
        "risk_slot_acquired": True,
    })
    router = ExecutionRouter(Client(), orders=orders)

    async def submit_child(child):
        orders.transition(
            child.intent_id, "SUBMITTED", risk_slot_acquired=True,
            risk_slot_operations={"open": {"operation_id": f"risk-open:{child.intent_id}", "action": "open", "status": "pending"}},
        )
        assert sink.events == [(f"risk-close:{request.intent_id}", "close")]
        router._apply_risk_slot_operation(child.intent_id, "open")
        orders.finalize_terminal(child.intent_id, "FILLED", terminal_class="filled", terminal_reason="child")
        return SimpleNamespace(ok=True)

    monkeypatch.setattr(router, "submit", submit_child)
    asyncio.run(router._limit_timeout_fallback(request, "original", "expected"))
    assert sink.events == [
        (f"risk-close:{request.intent_id}", "close"),
        (f"risk-open:{request.intent_id}-F", "open"),
    ]


def test_fallback_plan_is_durable_before_original_risk_close(tmp_path, monkeypatch):
    class Client:
        async def get_order(self, *_args, **_kwargs):
            return {"state": "canceled", "ordId": "original", "accFillSz": "0"}

    request = _request("fallback-plan-before-close")
    orders = OrderStateStore(tmp_path / "orders.json")
    orders.create({
        **request.__dict__, "state": "ACKNOWLEDGED", "client_order_id": "expected",
        "risk_slot_acquired": True,
    })
    router = ExecutionRouter(Client(), orders=orders)
    monkeypatch.setattr(router, "_reconcile_authoritative_zero_fill_risk_slot", lambda *_a, **_k: False)

    asyncio.run(router._limit_timeout_fallback(request, "original", "expected"))
    parent = orders.get(request.intent_id)
    assert parent["fallback_plan"]["child_intent_id"] == f"{request.intent_id}-F"
    assert parent["fallback_establishment_status"] == "WAITING_FOR_ORIGINAL_RISK_RELEASE"
    assert parent["recovery_required"] is True


def test_risk_manager_slot_operations_are_identity_idempotent(tmp_path):
    from risk_manager import RiskManager

    manager = RiskManager(tmp_path / "risk_slots.sqlite3")
    assert manager.apply_slot_operation("risk-open:i", "open") is True
    assert manager.apply_slot_operation("risk-open:i", "open") is False
    assert manager._open_positions == 1
    assert manager.apply_slot_operation("risk-close:i", "close") is True
    assert manager.apply_slot_operation("risk-close:i", "close") is False
    assert manager._open_positions == 0


def test_risk_slot_ledger_survives_fresh_manager_open_and_close_retries(tmp_path):
    from risk_manager import RiskManager

    ledger = tmp_path / "risk_slots.sqlite3"
    first = RiskManager(ledger)
    assert first.apply_slot_operation("risk-open:fresh", "open") is True
    assert first._open_positions == 1

    # This models a crash after the ledger transaction but before the router's
    # lifecycle completion marker.  No manager memory is reused.
    second = RiskManager(ledger)
    assert second.apply_slot_operation("risk-open:fresh", "open") is False
    assert second._open_positions == 1
    assert second.apply_slot_operation("risk-close:fresh", "close") is True
    assert second._open_positions == 0

    third = RiskManager(ledger)
    assert third.apply_slot_operation("risk-close:fresh", "close") is False
    assert third._open_positions == 0


def test_risk_slot_close_tombstone_cannot_be_resurrected(tmp_path):
    from risk_manager import RiskManager

    ledger = tmp_path / "risk_slots.sqlite3"
    first = RiskManager(ledger)
    assert first.apply_slot_operation("risk-close:tombstone", "close") is True
    assert first.apply_slot_operation("risk-open:tombstone", "open") is False
    assert first._refresh_open_slots() == 0

    fresh = RiskManager(ledger)
    assert fresh.apply_slot_operation("risk-open:tombstone", "open") is False
    assert fresh._refresh_open_slots() == 0


def test_router_marks_stale_open_lifecycle_operation_complete_after_tombstone(tmp_path, monkeypatch):
    import risk_manager
    from risk_manager import RiskManager

    ledger = tmp_path / "risk_slots.sqlite3"
    monkeypatch.setattr(risk_manager, "risk", RiskManager(ledger))
    request = _request("router-tombstone")
    store = OrderStateStore(tmp_path / "orders.json")
    store.create({
        **request.__dict__, "state": "ACKNOWLEDGED", "risk_slot_acquired": True,
        "risk_slot_operations": {
            "open": {"operation_id": f"risk-open:{request.intent_id}", "action": "open", "status": "pending"},
            "close": {"operation_id": f"risk-close:{request.intent_id}", "action": "close", "status": "pending"},
        },
    })
    router = ExecutionRouter(object(), orders=store)
    assert router._apply_risk_slot_operation(request.intent_id, "close") is True
    assert router._apply_risk_slot_operation(request.intent_id, "open") is True
    assert store.get(request.intent_id)["risk_slot_operations"]["open"]["status"] == "complete"
    assert RiskManager(ledger)._refresh_open_slots() == 0


def test_risk_slot_stale_open_process_replay_remains_closed(tmp_path):
    from risk_manager import RiskManager

    ledger = tmp_path / "risk_slots.sqlite3"
    manager = RiskManager(ledger)
    manager.apply_slot_operation("risk-open:process", "open")
    manager.apply_slot_operation("risk-close:process", "close")
    code = (
        "from risk_manager import RiskManager; import sys; "
        "RiskManager(sys.argv[1]).apply_slot_operation('risk-open:process', 'open')"
    )
    env = {**os.environ, "PYTHONDONTWRITEBYTECODE": "1", "PYTHONPATH": os.getcwd()}
    completed = subprocess.run(
        [sys.executable, "-c", code, str(ledger)], env=env,
        capture_output=True, text=True, check=False,
    )
    assert completed.returncode == 0, completed.stderr
    assert RiskManager(ledger)._refresh_open_slots() == 0


def test_two_sqlite_connections_open_close_converge_to_closed(tmp_path):
    from risk_manager import RiskManager

    ledger = tmp_path / "risk_slots.sqlite3"
    first, second = RiskManager(ledger), RiskManager(ledger)
    gate = threading.Barrier(2)

    def apply(manager, operation, action):
        gate.wait()
        manager.apply_slot_operation(operation, action)

    open_thread = threading.Thread(target=apply, args=(first, "risk-open:concurrent", "open"))
    close_thread = threading.Thread(target=apply, args=(second, "risk-close:concurrent", "close"))
    open_thread.start(); close_thread.start()
    open_thread.join(); close_thread.join()
    # Whichever writer acquired SQLite first, a later/first CLOSE tombstone
    # has terminal authority over the immutable intent.
    assert RiskManager(ledger)._refresh_open_slots() == 0


def test_position_release_closes_only_its_original_entry_slot(monkeypatch):
    import position_registry

    calls = []
    monkeypatch.setattr(position_registry, "_save", lambda **_kwargs: None)
    monkeypatch.setattr(
        __import__("risk_manager"), "risk",
        SimpleNamespace(apply_slot_operation=lambda operation_id, action: calls.append((operation_id, action))),
    )
    monkeypatch.setattr(position_registry, "_registry", {
        "A": {"execution_intent_id": "entry-a", "direction": "long", "engine": "TEST"},
        "B": {"execution_intent_id": "entry-b", "direction": "long", "engine": "TEST"},
    })
    assert position_registry.release("A") is False
    assert "A" in position_registry._registry
    assert position_registry.release("A", authoritative_position_zero=True) is True
    assert calls == [("risk-close:entry-a", "close")]
    assert "A" not in position_registry._registry
    assert "B" in position_registry._registry


@pytest.mark.parametrize(
    "snapshot, status, quantity",
    [
        ({"accFillSz": "0"}, FILL_KNOWN_ZERO, 0.0),
        ({"accFillSz": "0.0"}, FILL_KNOWN_ZERO, 0.0),
        ({"accFillSz": "0.4"}, FILL_KNOWN_POSITIVE, 0.4),
        ({}, FILL_UNKNOWN, None),
        ({"accFillSz": ""}, FILL_UNKNOWN, None),
        ({"accFillSz": "NaN"}, FILL_UNKNOWN, None),
        ({"accFillSz": "Infinity"}, FILL_UNKNOWN, None),
        ({"fillSz": "0"}, FILL_UNKNOWN, None),
    ],
)
def test_cumulative_fill_parser_never_treats_missing_event_fill_as_zero(snapshot, status, quantity):
    actual_status, actual_quantity = _authoritative_cumulative_fill(snapshot)
    assert actual_status == status
    assert actual_quantity == quantity


def test_live_partial_protection_failure_remains_nonterminal(tmp_path, monkeypatch):
    class Client:
        async def get_order(self, *_args, **_kwargs):
            return {"state": "partially_filled", "ordId": "order-1", "accFillSz": "0.4", "avgPx": "100"}

    class Protection:
        async def ensure(self, **_kwargs):
            return SimpleNamespace(protected=False, reason="stop_failed")

    request = _request("live-partial-protection")
    store = OrderStateStore(tmp_path / "orders.json")
    store.create({**request.__dict__, "state": "ACKNOWLEDGED", "client_order_id": "expected", "risk_slot_acquired": True})
    router = ExecutionRouter(Client(), orders=store)
    router.protection = Protection()
    result = asyncio.run(router._confirm_live_state(request, "order-1", "expected"))
    row = store.get(request.intent_id)
    assert row["state"] == "PARTIALLY_FILLED"
    assert not row.get("terminal_finalized_at")
    assert row["recovery_reason"] == "live_partial_protection_failure"


def test_submit_live_partial_protection_failure_stays_recovery_owned_without_second_attempt(tmp_path, monkeypatch):
    """The complete submit path must not re-enter generic fill protection."""
    import position_registry

    posts = []
    protection_attempts = []
    releases = []

    class Reservations:
        async def release(self, reservation_id):
            releases.append(reservation_id)
            return True

    class Client:
        async def get_order(self, *_args, **_kwargs):
            return {"state": "partially_filled", "ordId": "order-1", "accFillSz": "0.4", "avgPx": "100"}

        async def get_order_history(self, *_args, **_kwargs):
            return []

        async def get_fills(self, *_args, **_kwargs):
            return []

        async def get_positions(self, *_args, **_kwargs):
            return []

    class Protection:
        async def ensure(self, **_kwargs):
            protection_attempts.append("ensure")
            return SimpleNamespace(protected=False, reason="stop_failed", protection_state="FAILED")

    request = _request("complete-live-partial-protection")
    orders = OrderStateStore(tmp_path / "orders.json")
    router = ExecutionRouter(Client(), reservations=Reservations(), orders=orders)
    router.protection = Protection()
    monkeypatch.setattr(router, "paper_mode", lambda: False)
    monkeypatch.setattr(router.live_state, "can_submit_live", lambda: (True, "OK"))
    monkeypatch.setattr(router, "_enrich_leverage_truth", lambda req: asyncio.sleep(0, result=req))
    monkeypatch.setattr(router, "_local_executable_check", lambda *_args, **_kwargs: (True, "OK", {}))
    monkeypatch.setattr(router, "_position_effective_leverage", lambda *_args, **_kwargs: asyncio.sleep(0, result={}))
    monkeypatch.setattr(router.instruments, "require", lambda _coin: SimpleNamespace(lot_sz=0.1))
    position_registry._registry["TEST"] = {
        "direction": "long", "engine": "TEST", "pending_reservation": True,
        "execution_intent_id": request.intent_id,
    }

    async def submit_once(_request, _client_order_id, mark_exchange_request):
        posts.append("entry")
        mark_exchange_request()
        return {"ordId": "order-1"}

    monkeypatch.setattr(router, "_submit_once", submit_once)
    result = asyncio.run(router.submit(request))
    row = orders.get(request.intent_id)

    assert result.ok is False
    assert row["state"] == "PARTIALLY_FILLED"
    assert row["recovery_required"] is True
    assert row["recovery_reason"] == "live_partial_protection_failure"
    assert not row.get("terminal_finalized_at")
    assert protection_attempts == ["ensure"]
    assert posts == ["entry"]
    assert releases == []

    # A fresh router reads the durable intent instead of reposting it.
    restarted = ExecutionRouter(Client(), reservations=Reservations(), orders=OrderStateStore(tmp_path / "orders.json"))
    restarted.protection = Protection()
    asyncio.run(restarted.recover_unresolved())
    assert posts == ["entry"]
    assert not OrderStateStore(tmp_path / "orders.json").get(request.intent_id).get("terminal_finalized_at")
    assert releases == []


def test_restart_keeps_durable_live_partial_protection_failure_recovery_owned(tmp_path):
    """A live partial remainder cannot be terminalized by a later local success."""
    protection_calls = []
    releases = []

    class Reservations:
        async def release(self, reservation_id):
            releases.append(reservation_id)

    class Client:
        async def get_order(self, *_args, **_kwargs):
            return {
                "state": "partially_filled", "ordId": "order-1",
                "accFillSz": "0.4", "avgPx": "100",
            }

        async def get_order_history(self, *_args, **_kwargs):
            return []

        async def get_fills(self, *_args, **_kwargs):
            return []

        async def get_positions(self, *_args, **_kwargs):
            return []

    class Protection:
        async def ensure(self, **_kwargs):
            protection_calls.append("must_not_retry")
            return SimpleNamespace(protected=True, protection_state="CONFIRMED")

    request = _request("restart-live-partial-protection-failure")
    orders_path = tmp_path / "orders.json"
    orders = OrderStateStore(orders_path)
    # This durable seed represents the first, pre-restart protection attempt
    # having failed.  Recovery must only retain/reconcile its live remainder.
    orders.create({
        **request.__dict__, "state": "PARTIALLY_FILLED",
        "client_order_id": "expected", "exchange_order_id": "order-1",
        "filled_contracts": 0.4, "average_fill_price": 100.0,
        "recovery_required": True,
        "recovery_reason": "live_partial_protection_failure",
        "protection_failure": True,
    })
    router = ExecutionRouter(Client(), reservations=Reservations(), orders=OrderStateStore(orders_path))
    router.protection = Protection()

    results = asyncio.run(router.recover_unresolved())
    row = OrderStateStore(orders_path).get(request.intent_id)

    assert len(results) == 1
    assert results[0].ok is False
    assert results[0].state == "PARTIALLY_FILLED"
    assert protection_calls == []
    assert row["state"] == "PARTIALLY_FILLED"
    assert row["recovery_required"] is True
    assert row["recovery_reason"] == "live_partial_protection_failure"
    assert not row.get("terminal_finalized_at")
    assert releases == []


def test_restart_live_partial_successful_protection_remains_nonterminal_until_exchange_terminal(tmp_path, monkeypatch):
    """Protection of live 0.4/1.0 exposure cannot prove entry completion."""
    protection_calls = []
    releases = []

    class Reservations:
        async def release(self, reservation_id):
            releases.append(reservation_id)

    class Client:
        def __init__(self):
            self.state = "partially_filled"
            self.fill = "0.4"

        async def get_order(self, *_args, **_kwargs):
            return {
                "state": self.state, "ordId": "order-1", "accFillSz": self.fill,
                "avgPx": "100",
            }

        async def get_order_history(self, *_args, **_kwargs):
            return []

        async def get_fills(self, *_args, **_kwargs):
            return []

        async def get_positions(self, *_args, **_kwargs):
            if self.state == "filled":
                return [{"instId": "TEST-USDT-SWAP", "posSide": "long", "pos": "1", "lever": "10", "avgPx": "100"}]
            return []

        async def place_market_order(self, *_args, **_kwargs):
            raise AssertionError("restart recovery must never repost a live partial")

    class Protection:
        async def ensure(self, **kwargs):
            protection_calls.append(kwargs)
            return SimpleNamespace(
                protected=True, protection_state="CONFIRMED", reason="",
                confirmed_stop_price=90.0, sl_algo_id="sl", tp_algo_ids=[],
            )

    request = _request("restart-live-partial-success")
    orders_path = tmp_path / "orders.json"
    OrderStateStore(orders_path).create({
        **request.__dict__, "state": "PARTIALLY_FILLED",
        "client_order_id": "expected", "exchange_order_id": "order-1",
        "filled_contracts": 0.4, "average_fill_price": 100.0,
    })
    client = Client()
    router = ExecutionRouter(client, reservations=Reservations(), orders=OrderStateStore(orders_path))
    router.protection = Protection()
    monkeypatch.setattr("position_registry.update", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(
        router.instruments,
        "require",
        lambda _coin: SimpleNamespace(contracts_to_notional=lambda contracts, price: contracts * price),
    )

    partial = asyncio.run(router.recover_unresolved())
    row = OrderStateStore(orders_path).get(request.intent_id)

    assert len(partial) == 1
    assert partial[0].ok is False
    assert partial[0].state == "PARTIALLY_FILLED"
    assert row["state"] == "PARTIALLY_FILLED"
    assert row["recovery_required"] is True
    assert row["recovery_reason"] == "recovered_live_partial_protection_confirmed"
    assert not row.get("terminal_finalized_at")
    assert row["filled_contracts"] == pytest.approx(0.4)
    assert protection_calls and protection_calls[0]["contracts"] == pytest.approx(0.4)
    assert releases == []
    assert not row.get("terminal_telemetry_event")
    assert not dict(row.get("risk_slot_operations", {}) or {}).get("close")
    assert not any(
        entry["state"] in {"FILLED", "CANCELLED", "REJECTED", "FAILED_FINAL"}
        for entry in row["history"]
    )

    # Repeating the still-live snapshot keeps its durable protection outcome;
    # recovery does not mutate the exchange or repeat protection work.
    repeated = asyncio.run(router.recover_unresolved())
    repeated_row = OrderStateStore(orders_path).get(request.intent_id)
    assert len(repeated) == 1
    assert repeated[0].state == "PARTIALLY_FILLED"
    assert repeated_row["recovery_reason"] == "recovered_live_partial_protection_confirmed"
    assert len(protection_calls) == 1
    assert releases == []

    # A later authoritative terminal fact follows the existing canonical
    # terminalization path exactly once.
    client.state = "filled"
    client.fill = "1"
    completed = asyncio.run(router.recover_unresolved())
    final = OrderStateStore(orders_path).get(request.intent_id)

    assert len(completed) == 1
    assert completed[0].state == "FILLED"
    assert final["state"] == "FILLED"
    assert final.get("terminal_finalized_at")
    assert releases == [request.reservation_id]


def test_staged_children_share_parent_slot_and_authoritative_zero_closes_it_after_restart(tmp_path, monkeypatch):
    """Two staged children represent one resulting position lifecycle slot."""
    import position_registry
    import risk_manager

    parent = ExecutionRequest(**{**_request("staged-slot-parent").__dict__, "order_type": "staged"})
    child_ids = ["staged-slot-parent-S1", "staged-slot-parent-S2"]
    manifest = {
        "version": 1, "staged_parent_intent_id": parent.intent_id,
        "reservation_owner_intent_id": parent.intent_id, "reservation_id": parent.reservation_id,
        "stage_count": 2,
        "children": [
            {"intent_id": child_ids[0], "stage_index": 1, "requested_contracts": 0.5,
             "approved_contracts": 0.5, "approved_notional_usdt": 5.0, "price": 100.0},
            {"intent_id": child_ids[1], "stage_index": 2, "requested_contracts": 0.5,
             "approved_contracts": 0.5, "approved_notional_usdt": 5.0, "price": 99.0},
        ],
    }
    orders_path = tmp_path / "orders.json"
    orders = OrderStateStore(orders_path)
    orders.create({**parent.__dict__, "state": "INTENT_CREATED", "staged_manifest": manifest})
    for index, child_id in enumerate(child_ids, start=1):
        child = ExecutionRequest(**{
            **_request(child_id).__dict__, "requested_contracts": 0.5, "approved_contracts": 0.5,
            "approved_notional_usdt": 5.0, "order_type": "limit", "metadata": {
                "staged_parent_intent_id": parent.intent_id,
                "reservation_owner_intent_id": parent.intent_id,
                "staged_stage_index": index, "staged_stage_count": 2,
            },
        })
        orders.create({**child.__dict__, "state": "ACKNOWLEDGED"})

    ledger = tmp_path / "risk_slots.sqlite3"
    monkeypatch.setattr(risk_manager, "risk", RiskManager(ledger))
    router = ExecutionRouter(object(), orders=orders)
    for child_id in child_ids:
        assert router._ensure_recovered_open_risk_slot(child_id) is True
        child = orders.get(child_id)
        assert child["risk_slot_owner_intent_id"] == parent.intent_id
        assert child["risk_slot_operations"]["open"]["operation_id"] == f"risk-open:{parent.intent_id}"
    assert RiskManager(ledger)._refresh_open_slots() == 1

    # Fresh router/manager retain the single durable parent identity.  The
    # registry carries that immutable lineage, not a symbol-derived slot key.
    restarted = ExecutionRouter(object(), orders=OrderStateStore(orders_path))
    position_registry._registry["TEST"] = {
        "direction": "long", "engine": "TEST", "execution_intent_id": child_ids[-1],
        "risk_slot_intent_id": parent.intent_id,
    }
    assert position_registry.risk_slot_intent(position_registry._registry["TEST"]) == parent.intent_id
    assert restarted._reconcile_closed_position_risk_slot("TEST") is True
    assert RiskManager(ledger)._refresh_open_slots() == 0
    assert RiskManager(ledger).apply_slot_operation(f"risk-open:{parent.intent_id}", "open") is False


def test_fallback_canceled_order_without_accumulated_fill_stays_recovery_owned(tmp_path):
    class Client:
        async def get_order(self, *_args, **_kwargs):
            return {"state": "canceled", "ordId": "order-1", "fillSz": "0"}

    request = _request("unknown-cumulative-fallback")
    store = OrderStateStore(tmp_path / "orders.json")
    store.create({
        **request.__dict__, "state": "ACKNOWLEDGED", "client_order_id": "expected",
        "exchange_order_id": "order-1", "risk_slot_acquired": True,
    })
    router = ExecutionRouter(Client(), orders=store)
    row = asyncio.run(router._limit_timeout_fallback(request, "order-1", "expected"))
    assert row["state"] == "CANCEL_PENDING"
    assert row["recovery_reason"] == "fallback_cumulative_fill_evidence_unknown"
    assert not store.get(f"{request.intent_id}-F")


def test_recovered_acknowledged_order_rebuilds_missing_open_slot_without_post(tmp_path, monkeypatch):
    from risk_manager import RiskManager
    import risk_manager

    request = _request("recovered-live-slot")
    orders = OrderStateStore(tmp_path / "orders.json")
    orders.create({
        **request.__dict__, "state": "ACKNOWLEDGED", "exchange_order_id": "live-order",
        "client_order_id": "expected", "risk_slot_acquired": False,
    })
    ledger = tmp_path / "risk_slots.sqlite3"
    monkeypatch.setattr(risk_manager, "risk", RiskManager(ledger))
    router = ExecutionRouter(object(), orders=orders)

    assert router._ensure_recovered_open_risk_slot(request.intent_id) is True
    restarted_sink = RiskManager(ledger)
    assert restarted_sink._refresh_open_slots() == 1
    row = orders.get(request.intent_id)
    assert row["risk_slot_operations"]["open"]["status"] == "complete"


def test_recovered_filled_exposure_waits_for_durable_open_risk_slot(tmp_path, monkeypatch):
    class Client:
        async def get_order(self, *_args, **_kwargs):
            return {"state": "filled", "ordId": "order-1", "accFillSz": "1", "avgPx": "100"}

        async def get_order_history(self, *_args, **_kwargs):
            return []

        async def get_fills(self, *_args, **_kwargs):
            return []

        async def get_positions(self, *_args, **_kwargs):
            return [{"instId": "TEST-USDT-SWAP", "posSide": "long", "pos": "1", "lever": "10", "avgPx": "100"}]

        async def get_positions_status(self, **_kwargs):
            return {"ok": True, "positions": await self.get_positions(), "reason": ""}

    request = _request("recovered-fill-open-pending")
    orders = OrderStateStore(tmp_path / "orders.json")
    orders.create({**request.__dict__, "state": "SUBMISSION_ATTEMPTED", "client_order_id": "expected",
                   "exchange_order_id": "order-1", "risk_slot_acquired": True,
                   "risk_slot_operations": {
                       "open": {"operation_id": f"risk-open:{request.intent_id}", "action": "open", "status": "pending"},
                   }})
    router = ExecutionRouter(Client(), orders=orders)
    monkeypatch.setattr(router, "_apply_risk_slot_operation", lambda _intent, _action: False)

    result = asyncio.run(router.recover_unresolved())
    row = orders.get(request.intent_id)
    assert result[0].reason == "recovered_filled_risk_slot_open_pending"
    assert row["state"] == "PARTIALLY_FILLED"
    assert row["recovery_required"] is True
    assert not row.get("terminal_finalized_at")


def test_recovered_live_fill_registry_activation_false_stays_recovery_owned(tmp_path, monkeypatch):
    import position_registry

    class Client:
        async def get_order(self, *_args, **_kwargs):
            return {"state": "filled", "ordId": "order-1", "accFillSz": "1", "avgPx": "100"}

        async def get_order_history(self, *_args, **_kwargs):
            return []

        async def get_fills(self, *_args, **_kwargs):
            return []

        async def get_positions(self, *_args, **_kwargs):
            return [{"instId": "TEST-USDT-SWAP", "posSide": "long", "pos": "1", "lever": "10", "avgPx": "100"}]

    class Protection:
        async def ensure(self, **_kwargs):
            raise AssertionError("protection must wait for durable registry activation")

    request = _request("recovered-fill-registry-pending")
    orders = OrderStateStore(tmp_path / "orders.json")
    orders.create({**request.__dict__, "state": "SUBMISSION_ATTEMPTED", "client_order_id": "expected",
                   "exchange_order_id": "order-1", "risk_slot_acquired": True,
                   "risk_slot_operations": {
                       "open": {"operation_id": f"risk-open:{request.intent_id}", "action": "open", "status": "complete"},
                   }})
    router = ExecutionRouter(Client(), orders=orders)
    router.protection = Protection()
    monkeypatch.setattr(router.instruments, "require", lambda _coin: SimpleNamespace(
        contracts_to_notional=lambda contracts, price: contracts * price,
    ))
    monkeypatch.setattr(position_registry, "update", lambda *_args, **_kwargs: False)

    result = asyncio.run(router._confirm_live_state(request, "order-1", "expected"))
    row = orders.get(request.intent_id)

    # An authoritative FILLED readback is retained as a durable partial
    # lifecycle state until the remaining protection/slot convergence paths
    # complete.  This path does not invoke registry activation or invent a
    # recovery reason merely because the test hook declines an unused update.
    assert result["state"] == "PARTIALLY_FILLED"
    assert row["state"] == "PARTIALLY_FILLED"
    assert row["filled_contracts"] == pytest.approx(1.0)
    assert row["average_fill_price"] == pytest.approx(100.0)
    assert row["exchange_fill_confirmed"] is True
    assert not row.get("terminal_finalized_at")


def test_submit_acknowledgement_waits_for_durable_open_risk_slot_before_fill_processing(tmp_path, monkeypatch):
    """The real submit caller cannot continue after the post-ack OPEN gate fails."""
    monkeypatch.setenv("PAPER_TRADING", "true")
    protection_calls = []

    class Client:
        async def place_market_order(self, *_args, **kwargs):
            kwargs["on_exchange_request"]()
            return {"ordId": "ack-order", "contracts": 1.0, "fillPx": "100"}

    request = replace(_request("ack-open-gate"), reservation_id="reservation")
    reservations = CapitalReservationBook(tmp_path / "reservations.json")
    reservations._items["reservation"] = {
        "reservation_id": "reservation", "intent_id": request.intent_id,
        "candidate_id": request.candidate_id, "margin_usdt": 1.0,
        "risk_usdt": 0.1, "created_at": time.time(),
        "expires_at": time.time() + 120.0,
        "ownership_state": "PRE_SUBMIT_RESERVED",
        "ownership_reason": "test_fixture",
        "ownership_updated_at": time.time(), "metadata": {},
    }
    reservations._save()
    router = ExecutionRouter(
        Client(), reservations=reservations,
        orders=OrderStateStore(tmp_path / "orders.json"),
    )
    monkeypatch.setattr(router, "_local_executable_check", lambda *_args, **_kwargs: (True, "OK", {}))
    monkeypatch.setattr(router, "_pin_reservation_for_router", lambda *_args, **_kwargs: asyncio.sleep(0, result=True))
    monkeypatch.setattr(router, "_ensure_recovered_open_risk_slot", lambda _intent: False)
    monkeypatch.setattr(
        router.protection,
        "ensure",
        lambda **_kwargs: protection_calls.append("ensure"),
    )

    result = asyncio.run(router.submit(request))
    row = router.orders.get(request.intent_id)

    assert result.ok is False
    assert result.reason == "submitted_risk_slot_open_pending"
    assert row["state"] == "SUBMITTED"
    assert row["recovery_required"] is True
    assert row["recovery_reason"] == "submitted_risk_slot_open_pending"
    assert not row.get("terminal_finalized_at")
    assert protection_calls == []


def test_recovered_filled_without_positive_cumulative_fill_stays_recovery_owned(tmp_path):
    class Client:
        async def get_order(self, *_args, **_kwargs):
            return {"state": "filled", "ordId": "order-1", "fillPx": "100"}

        async def get_order_history(self, *_args, **_kwargs):
            return []

        async def get_fills(self, *_args, **_kwargs):
            # Even a correlated event fill cannot replace accFillSz for a
            # terminal FILLED classification.
            return [{"ordId": "order-1", "fillId": "fill-1", "fillSz": "1", "fillPx": "100"}]

        async def get_positions(self, *_args, **_kwargs):
            return []

    request = _request("filled-missing-acc-fill")
    orders = OrderStateStore(tmp_path / "orders.json")
    orders.create({
        **request.__dict__, "state": "SUBMISSION_ATTEMPTED", "client_order_id": "expected",
        "exchange_order_id": "order-1", "risk_slot_acquired": True,
    })
    router = ExecutionRouter(Client(), orders=orders)

    result = asyncio.run(router._recover_uncertain(orders.get(request.intent_id), request))
    row = orders.get(request.intent_id)

    assert result.ok is False
    assert result.reason == "recovered_filled_cumulative_evidence_unknown"
    assert row["state"] == "SUBMIT_UNCERTAIN"
    assert row["recovery_required"] is True
    assert row["cumulative_fill_evidence"] == FILL_UNKNOWN
    assert not row.get("terminal_finalized_at")


def test_cancel_full_fill_race_stays_recovery_owned_until_filled_convergence(tmp_path):
    releases = []

    class Reservations:
        async def release(self, reservation_id):
            releases.append(reservation_id)

    class Client:
        async def cancel_order(self, *_args, **_kwargs):
            return {"sCode": "0", "ordId": "order-1"}

        async def get_order(self, *_args, **_kwargs):
            return {"state": "canceled", "ordId": "order-1", "accFillSz": "1", "avgPx": "100"}

    request = _request("cancel-full-fill-race")
    orders = OrderStateStore(tmp_path / "orders.json")
    orders.create({**request.__dict__, "state": "ACKNOWLEDGED", "client_order_id": "expected", "exchange_order_id": "order-1"})
    router = ExecutionRouter(Client(), reservations=Reservations(), orders=orders)

    asyncio.run(router.cancel("TEST", order_id="order-1", client_order_id="expected", intent_id=request.intent_id))
    row = orders.get(request.intent_id)

    assert row["state"] == "PARTIALLY_FILLED"
    assert row["filled_contracts"] == pytest.approx(1.0)
    assert row["recovery_required"] is True
    assert row["recovery_reason"] == "authoritative_cancel_full_fill_requires_filled_convergence"
    assert not row.get("terminal_finalized_at")
    assert releases == []


def test_staged_underfill_retains_parent_child_and_reservation_ownership(tmp_path):
    releases = []

    class Reservations:
        async def release(self, reservation_id):
            releases.append(reservation_id)

    class Instruments:
        def require(self, _coin):
            return SimpleNamespace(lot_sz=0.1)

    parent = ExecutionRequest(**{**_request("staged-underfill").__dict__, "order_type": "staged"})
    child_ids = [f"{parent.intent_id}-S1", f"{parent.intent_id}-S2"]
    manifest = {
        "version": 1, "staged_parent_intent_id": parent.intent_id,
        "reservation_owner_intent_id": parent.intent_id, "reservation_id": parent.reservation_id,
        "stage_count": 2, "children": [
            {"intent_id": child_ids[0], "stage_index": 1, "requested_contracts": 0.5,
             "approved_contracts": 0.5, "approved_notional_usdt": 5.0, "price": 100.0},
            {"intent_id": child_ids[1], "stage_index": 2, "requested_contracts": 0.5,
             "approved_contracts": 0.5, "approved_notional_usdt": 5.0, "price": 100.0},
        ],
    }
    orders = OrderStateStore(tmp_path / "orders.json")
    orders.create({**parent.__dict__, "state": "INTENT_CREATED", "staged_manifest": manifest})
    for child_id in child_ids:
        child = ExecutionRequest(**{
            **_request(child_id).__dict__, "requested_contracts": 0.5, "approved_contracts": 0.5,
            "approved_notional_usdt": 5.0, "order_type": "limit",
            "metadata": {"reservation_owner_intent_id": parent.intent_id,
                         "staged_parent_intent_id": parent.intent_id},
        })
        orders.create({**child.__dict__, "state": "ACKNOWLEDGED"})
        orders.finalize_terminal(
            child_id, "FILLED", terminal_class="filled", terminal_reason="child_underfill",
            filled_contracts=0.4, average_fill_price=100.0,
        )
    router = ExecutionRouter(object(), reservations=Reservations(), orders=orders, instruments=Instruments())

    result = asyncio.run(router._resume_staged_parent(parent, orders.get(parent.intent_id)))
    row = orders.get(parent.intent_id)

    assert result.ok is False
    assert result.reason == "staged_underfill_requires_authoritative_full_fill"
    assert row["state"] == "PARTIALLY_FILLED"
    assert row["filled_contracts"] == pytest.approx(0.8)
    assert row["recovery_required"] is True
    assert row["staged_children_terminal"] is True
    assert row["staged_underfill"] is True
    assert row["staged_recovery_action"] == "retain_parent_reservation_no_repost"
    assert not row.get("terminal_finalized_at")
    assert releases == []


def test_scheduler_real_failure_caller_retains_registry_for_recovery_owned_result(monkeypatch):
    """A scheduler result.ok false is not authority to close a risk slot."""
    release_calls = []

    class Instrument:
        def contracts_to_notional(self, contracts, price):
            return float(contracts) * float(price)

        def required_margin(self, contracts, price, leverage):
            return float(contracts) * float(price) / float(leverage)

        def round_price(self, price, **_kwargs):
            return float(price)

    class Client:
        async def get_ticker(self, _coin):
            return {"last": "100"}

        async def get_account_snapshot(self):
            return {"equity_usdt": 100.0}

    class Router:
        async def submit(self, _request):
            return SimpleNamespace(
                ok=False, state="PARTIALLY_FILLED", reason="live_partial_protection_failure",
                average_fill_price=0.0, order_id="order-1",
            )

    async def reserve_registry(*_args, **_kwargs):
        return True, "OK"

    async def reserve_capital(**_kwargs):
        return True, "OK", "reservation"

    monkeypatch.setattr(capital_scheduler.instrument_registry, "require", lambda _coin: Instrument())
    monkeypatch.setattr(capital_scheduler, "can_open_and_reserve", reserve_registry)
    monkeypatch.setattr(capital_scheduler.reservation_book, "reserve", reserve_capital)
    monkeypatch.setattr(capital_scheduler, "router_for", lambda _client: Router())
    monkeypatch.setattr(capital_scheduler, "registry_release_pre_submit", lambda coin: release_calls.append(coin))
    monkeypatch.setattr(capital_scheduler.micro_sizing, "usable_margin_usdt", lambda balance, _settings: balance)
    monkeypatch.setattr(capital_scheduler._s, "get_val", lambda _key, default=None: default)
    opportunity = capital_scheduler.Opportunity(
        candidate_id="queued-recovery", pair="TEST", direction="long", engine="TEST",
        signal_timestamp=time.time(), expiry_timestamp=time.time() + 60.0,
        original_score=1.0, current_score=1.0, confidence=1.0,
        intended_entry=100.0, stop=99.0, tp_structure={}, risk_requirement=1.0,
        estimated_margin_requirement=1.0, instrument_eligibility="eligible",
        deployment_mode="Guarded", correlation_group="test", queued_reason="test",
        market_geometry_timestamp=time.time(), payload={"approved_contracts": 1.0, "leverage": 1},
    )
    scheduler = capital_scheduler.CapitalScheduler()
    scheduler.configure(Client())

    assert asyncio.run(scheduler._attempt(opportunity)) is False
    assert release_calls == []


def test_registry_close_persistence_failure_keeps_retryable_row_and_closed_tombstone(tmp_path, monkeypatch):
    import position_registry
    import risk_manager

    intent_id = "registry-close-pending"
    ledger = tmp_path / "risk_slots.sqlite3"
    monkeypatch.setattr(risk_manager, "risk", RiskManager(ledger))
    assert risk_manager.risk.apply_slot_operation(f"risk-open:{intent_id}", "open") is True
    monkeypatch.setattr(position_registry, "_registry", {
        "TEST": {"execution_intent_id": intent_id, "direction": "long", "engine": "TEST"},
    })
    saves = []

    def save(*, strict=False):
        saves.append(strict)
        if len(saves) == 1:
            raise OSError("disk unavailable")

    monkeypatch.setattr(position_registry, "_save", save)
    assert position_registry.release("TEST", authoritative_position_zero=True) is False
    assert "TEST" in position_registry._registry
    assert RiskManager(ledger)._refresh_open_slots() == 0
    # A stale registry OPEN reconciliation cannot override the durable close.
    assert position_registry.ensure_open_risk_slot("TEST") is True
    assert RiskManager(ledger)._refresh_open_slots() == 0
    assert position_registry.release("TEST", authoritative_position_zero=True) is True
    assert "TEST" not in position_registry._registry


def test_partial_cancel_registry_initial_write_failure_retains_restart_repair(tmp_path, monkeypatch):
    import position_registry

    class Instruments:
        def require(self, _coin):
            return SimpleNamespace(contracts_to_notional=lambda contracts, price: contracts * price)

    request = ExecutionRequest(**{
        **_request("partial-registry-repair").__dict__, "stop_price": 90.0, "take_profit": 120.0,
    })
    path = tmp_path / "orders.json"
    orders = OrderStateStore(path)
    orders.create({**request.__dict__, "state": "PARTIALLY_FILLED", "client_order_id": "expected"})
    monkeypatch.setattr(position_registry, "_PATH", tmp_path / "positions.json")
    monkeypatch.setattr(position_registry, "_registry", {})
    original = position_registry.upsert_authoritative_position
    monkeypatch.setattr(position_registry, "upsert_authoritative_position", lambda *_a, **_k: (_ for _ in ()).throw(OSError("disk")))
    router = ExecutionRouter(object(), orders=orders)
    monkeypatch.setattr(router, "paper_mode", lambda: False)
    monkeypatch.setattr(
        router, "_position_effective_leverage",
        lambda _request: asyncio.sleep(0, result={
            "position_effective_leverage": 3,
            "leverage_truth_status": "POSITION_EFFECTIVE",
            "leverage_truth_source": "test.position",
        }),
    )
    first = asyncio.run(router._converge_authoritative_partial_cancel(
        request, {"state": "canceled", "ordId": "o", "accFillSz": "0.4", "avgPx": "100"}, {}, "o",
    ))
    assert first["state"] == "PARTIALLY_FILLED"
    assert first["recovery_reason"] == "partial_cancel_registry_initial_write_pending"
    assert not first.get("terminal_finalized_at")

    monkeypatch.setattr(position_registry, "upsert_authoritative_position", original)
    restarted = ExecutionRouter(object(), orders=OrderStateStore(path), instruments=Instruments())
    monkeypatch.setattr(restarted, "paper_mode", lambda: False)
    monkeypatch.setattr(
        restarted, "_position_effective_leverage",
        lambda _request: asyncio.sleep(0, result={
            "position_effective_leverage": 3,
            "leverage_truth_status": "POSITION_EFFECTIVE",
            "leverage_truth_source": "test.position",
        }),
    )
    monkeypatch.setattr(restarted.protection, "ensure", lambda **_k: asyncio.sleep(0, result=SimpleNamespace(
        protected=True, reason="", protection_state="CONFIRMED", confirmed_stop_price=90.0,
        sl_algo_id="sl", tp_algo_ids=[],
    )))
    final = asyncio.run(restarted._converge_authoritative_partial_cancel(
        request, {"state": "canceled", "ordId": "o", "accFillSz": "0.4", "avgPx": "100"}, {}, "o",
    ))
    assert final["state"] == "CANCELLED"
    assert position_registry._registry["TEST"]["actual_filled_contracts"] == pytest.approx(0.4)


def test_staged_parent_without_manifest_converges_and_releases(tmp_path):
    calls = []

    class Reservations:
        async def release(self, reservation_id):
            calls.append(reservation_id)

    request = ExecutionRequest(**{**_request("staged-missing-manifest").__dict__, "order_type": "staged"})
    orders = OrderStateStore(tmp_path / "orders.json")
    orders.create({**request.__dict__, "state": "INTENT_CREATED"})
    router = ExecutionRouter(object(), reservations=Reservations(), orders=orders)
    asyncio.run(router.recover_unresolved())
    row = orders.get(request.intent_id)
    assert row["state"] == "FAILED_FINAL"
    assert row["terminal_reason"] == "staged_manifest_missing_before_child_boundary"
    assert calls == ["reservation"]


def test_confirm_live_state_does_not_add_authoritative_cumulative_fill_twice(tmp_path):
    class Client:
        def __init__(self):
            self.reads = 0

        async def get_order(self, *_args, **_kwargs):
            self.reads += 1
            return {
                "state": "partially_filled", "ordId": "order-1",
                "accFillSz": "0.4", "avgPx": "100",
            }

        async def cancel_order(self, *_args, **_kwargs):
            return {"sCode": "0", "ordId": "order-1"}

    request = ExecutionRequest(**{
        **_request("cumulative-poll-dedup").__dict__, "timeout_secs": 0.55,
    })
    orders = OrderStateStore(tmp_path / "orders.json")
    orders.create({**request.__dict__, "state": "ACKNOWLEDGED", "client_order_id": "expected"})
    router = ExecutionRouter(Client(), orders=orders)
    router.protection.ensure = lambda **_kwargs: asyncio.sleep(0, result=SimpleNamespace(
        protected=True, confirmed_stop_price=0.0, reason="", protection_state="CONFIRMED",
    ))

    result = asyncio.run(router._confirm_live_state(request, "order-1", "expected"))
    row = orders.get(request.intent_id)

    assert result["state"] == "CANCEL_PENDING"
    assert sum(float(fill["fillSz"]) for fill in row["fills"]) == pytest.approx(0.4)
    assert row["filled_contracts"] == pytest.approx(0.4)
    assert row["authoritative_cumulative_fill"] == pytest.approx(0.4)


def test_confirm_live_state_missing_cumulative_fill_cannot_accept_filled(tmp_path):
    class Client:
        async def get_order(self, *_args, **_kwargs):
            return {"state": "filled", "ordId": "order-1", "avgPx": "100"}

    request = _request("filled-no-cumulative")
    orders = OrderStateStore(tmp_path / "orders.json")
    orders.create({**request.__dict__, "state": "ACKNOWLEDGED", "client_order_id": "expected"})
    result = asyncio.run(ExecutionRouter(Client(), orders=orders)._confirm_live_state(
        request, "order-1", "expected",
    ))
    row = orders.get(request.intent_id)

    assert result["state"] == "SUBMIT_UNCERTAIN"
    assert result["recovery_reason"] == "filled_cumulative_evidence_unknown"
    assert row["filled_contracts"] == pytest.approx(0.0)
    assert not row.get("terminal_finalized_at")


def test_live_position_effective_leverage_accepts_signed_net_mode(monkeypatch):
    class Client:
        async def get_positions_status(self, **_kwargs):
            return {"positions": [{
                "instId": "TEST-USDT-SWAP", "posSide": "net", "pos": "-1",
                "lever": "7", "uTime": "123",
            }]}

    router = ExecutionRouter(Client())
    monkeypatch.setattr(router, "paper_mode", lambda: False)
    request = ExecutionRequest(**{
        **_request("net-leverage").__dict__, "direction": "short",
        "requested_leverage": 3, "exchange_configured_leverage": 3,
    })

    truth = asyncio.run(router._position_effective_leverage(request))

    assert truth["position_effective_leverage"] == 7
    assert truth["leverage_truth_status"] == "POSITION_EFFECTIVE"


def test_partial_cancel_missing_effective_leverage_stays_recovery_owned(tmp_path, monkeypatch):
    import position_registry

    class Client:
        async def get_positions_status(self, **_kwargs):
            return {"positions": [{
                "instId": "TEST-USDT-SWAP", "posSide": "net", "pos": "0.4", "lever": "",
            }]}

    class Instruments:
        def require(self, _coin):
            return SimpleNamespace(contracts_to_notional=lambda contracts, price: contracts * price)

    request = _request("partial-missing-effective-leverage")
    orders = OrderStateStore(tmp_path / "orders.json")
    orders.create({**request.__dict__, "state": "CANCEL_PENDING", "client_order_id": "expected"})
    router = ExecutionRouter(Client(), orders=orders, instruments=Instruments())
    monkeypatch.setattr(router, "paper_mode", lambda: False)
    upserts = []
    monkeypatch.setattr(
        position_registry, "upsert_authoritative_position",
        lambda **_kwargs: upserts.append(True),
    )

    result = asyncio.run(router._converge_authoritative_partial_cancel(
        request,
        {"state": "canceled", "ordId": "order-1", "accFillSz": "0.4", "avgPx": "100"},
        {},
        "order-1",
    ))
    row = orders.get(request.intent_id)

    assert result["state"] == "PARTIALLY_FILLED"
    assert result["recovery_reason"] == "partial_cancel_effective_leverage_unknown"
    assert result["recovery_required"] is True
    assert result["filled_contracts"] == pytest.approx(0.4)
    assert upserts == []
    assert position_registry._registry == {}
    assert not result.get("terminal_finalized_at")


def test_partial_cancel_upsert_requires_authoritative_instrument_metadata(tmp_path, monkeypatch):
    import position_registry

    request = _request("partial-no-instrument")
    orders = OrderStateStore(tmp_path / "orders.json")
    orders.create({**request.__dict__, "state": "PARTIALLY_FILLED"})
    router = ExecutionRouter(object(), orders=orders)
    called = []
    monkeypatch.setattr(position_registry, "upsert_authoritative_position", lambda **_kwargs: called.append(True))

    with pytest.raises(Exception, match="instrument|metadata"):
        router._upsert_surviving_partial_cancel_position(
            request, filled_contracts=0.4, average_fill_price=100.0, order_id="order-1", leverage=3,
        )
    assert called == []


def test_fallback_child_persistence_failure_is_locally_recoverable(tmp_path):
    posts = []
    releases = []

    class Reservations:
        async def release(self, reservation_id):
            releases.append(reservation_id)

    class Client:
        async def get_order(self, *_args, **_kwargs):
            return {"state": "canceled", "ordId": "order-1", "accFillSz": "0"}

        async def place_market_order(self, *_args, **_kwargs):
            posts.append("post")
            raise AssertionError("test does not authorize fallback POST")

    request = _request("fallback-persist-failure")
    orders = OrderStateStore(tmp_path / "orders.json")
    orders.create({**request.__dict__, "state": "ACKNOWLEDGED", "client_order_id": "expected"})
    router = ExecutionRouter(Client(), reservations=Reservations(), orders=orders)
    original_create = orders.create

    def fail_child_create(row):
        if row["intent_id"] == "fallback-persist-failure-F":
            raise OSError("disk interrupted")
        return original_create(row)

    orders.create = fail_child_create
    asyncio.run(router._limit_timeout_fallback(request, "order-1", "expected"))
    parent = orders.get(request.intent_id)
    assert parent["state"] == "CANCEL_PENDING"
    assert parent["fallback_establishment_status"] == "pending"
    assert not orders.get("fallback-persist-failure-F")
    assert posts == []

    orders.create = original_create

    async def finish_recovered_child(fallback):
        child = orders.get(fallback.intent_id)
        assert child and child["state"] == "INTENT_CREATED"
        orders.finalize_terminal(fallback.intent_id, "FILLED", terminal_class="filled", terminal_reason="test_child")
        return SimpleNamespace(ok=True, state="FILLED", filled_contracts=1.0, average_fill_price=100.0, order_id="")

    router.submit = finish_recovered_child
    asyncio.run(router.recover_unresolved())
    asyncio.run(router.recover_unresolved())
    parent = orders.get(request.intent_id)
    assert parent["state"] == "CANCELLED"
    assert releases == ["reservation"]
    assert posts == []


def test_staged_restart_reconciles_post_boundary_child_without_repost(tmp_path):
    posts = []
    releases = []

    class Reservations:
        async def release(self, reservation_id):
            releases.append(reservation_id)

    class Client:
        async def place_limit_order(self, *_args, **_kwargs):
            posts.append("post")
            raise AssertionError("post-boundary staged child must reconcile, never repost")

        async def get_order(self, *_args, **_kwargs):
            return {"state": "canceled", "ordId": "order-1", "clOrdId": "stage-client", "accFillSz": "0"}

        async def get_order_history(self, *_args, **_kwargs):
            return []

        async def get_fills(self, *_args, **_kwargs):
            return []

        async def get_positions(self, *_args, **_kwargs):
            return []

    class Instruments:
        def require(self, _coin):
            return SimpleNamespace(lot_sz=0.1)

    parent_request = ExecutionRequest(**{**_request("staged-restart").__dict__, "order_type": "staged"})
    child_request = ExecutionRequest(**{**_request("staged-restart-S1").__dict__, "order_type": "limit", "metadata": {
        "reservation_owner_intent_id": "staged-restart", "staged_parent_intent_id": "staged-restart",
        "staged_stage_index": 1, "staged_stage_count": 1,
    }})
    manifest = {"version": 1, "staged_parent_intent_id": "staged-restart",
                "reservation_owner_intent_id": "staged-restart", "reservation_id": "reservation",
                "stage_count": 1, "children": [{"intent_id": "staged-restart-S1", "stage_index": 1,
                "requested_contracts": 1.0, "approved_contracts": 1.0, "approved_notional_usdt": 10.0, "price": 100.0}]}
    orders = OrderStateStore(tmp_path / "orders.json")
    orders.create({**parent_request.__dict__, "state": "INTENT_CREATED", "staged_manifest": manifest})
    orders.create({**child_request.__dict__, "state": "SUBMISSION_ATTEMPTED", "client_order_id": "stage-client", "exchange_order_id": "order-1"})
    router = ExecutionRouter(Client(), reservations=Reservations(), orders=orders, instruments=Instruments())

    asyncio.run(router.recover_unresolved())
    assert posts == []
    assert orders.get("staged-restart-S1")["state"] == "CANCELLED"
    assert orders.get("staged-restart")["terminal_finalized_at"]
    assert releases == ["reservation"]


@pytest.mark.parametrize("precreate_child", [False, True])
def test_staged_restart_resumes_manifest_child_only_from_preboundary_truth(tmp_path, precreate_child):
    calls = []
    releases = []

    class Reservations:
        async def release(self, reservation_id):
            releases.append(reservation_id)

    class Instruments:
        def require(self, _coin):
            return SimpleNamespace(lot_sz=0.1)

    parent_request = ExecutionRequest(**{**_request("staged-preboundary").__dict__, "order_type": "staged"})
    child_id = "staged-preboundary-S1"
    manifest = {"version": 1, "staged_parent_intent_id": parent_request.intent_id,
                "reservation_owner_intent_id": parent_request.intent_id, "reservation_id": "reservation",
                "stage_count": 1, "children": [{"intent_id": child_id, "stage_index": 1,
                "requested_contracts": 1.0, "approved_contracts": 1.0, "approved_notional_usdt": 10.0, "price": 100.0}]}
    path = tmp_path / "orders.json"
    first = OrderStateStore(path)
    first.create({**parent_request.__dict__, "state": "INTENT_CREATED", "staged_manifest": manifest})
    if precreate_child:
        first.create({**_request(child_id).__dict__, "order_type": "limit", "state": "INTENT_CREATED", "metadata": {
            "reservation_owner_intent_id": parent_request.intent_id, "staged_parent_intent_id": parent_request.intent_id,
            "staged_stage_index": 1, "staged_stage_count": 1,
        }})
    orders = OrderStateStore(path)  # process-reload durable truth
    router = ExecutionRouter(object(), reservations=Reservations(), orders=orders, instruments=Instruments())

    async def complete_child(child):
        calls.append(child.intent_id)
        assert orders.get(child.intent_id)["state"] == "INTENT_CREATED"
        orders.finalize_terminal(child.intent_id, "FILLED", terminal_class="filled", terminal_reason="test_child",
                                filled_contracts=1.0, average_fill_price=100.0)
        return SimpleNamespace(ok=True, state="FILLED", filled_contracts=1.0, average_fill_price=100.0, order_id="")

    router.submit = complete_child
    asyncio.run(router.recover_unresolved())
    assert calls == [child_id]
    assert orders.get(parent_request.intent_id)["state"] == "FILLED"
    assert releases == ["reservation"]


def test_fallback_post_boundary_restart_reconciles_without_repost(tmp_path):
    posts = []
    releases = []

    class Reservations:
        async def release(self, reservation_id):
            releases.append(reservation_id)
            return True

    class Client:
        async def place_market_order(self, *_args, **_kwargs):
            posts.append("post")
            raise AssertionError("post-boundary fallback must not repost")

        async def get_order(self, *_args, **_kwargs):
            return {"state": "canceled", "ordId": "fallback-order", "clOrdId": "fallback-client", "accFillSz": "0"}

        async def get_order_history(self, *_args, **_kwargs):
            return []

        async def get_fills(self, *_args, **_kwargs):
            return []

        async def get_positions(self, *_args, **_kwargs):
            return []

    parent_request = _request("fallback-restart")
    child_request = ExecutionRequest(**{**_request("fallback-restart-F").__dict__, "metadata": {
        "reservation_owner_intent_id": "fallback-restart", "fallback_parent_intent_id": "fallback-restart",
    }})
    orders = OrderStateStore(tmp_path / "orders.json")
    orders.create({**parent_request.__dict__, "state": "CANCEL_PENDING", "fallback_child_intent_id": "fallback-restart-F",
                   "fallback_plan": {"version": 1, "parent_intent_id": "fallback-restart", "child_intent_id": "fallback-restart-F",
                                     "reservation_owner_intent_id": "fallback-restart", "reservation_id": "reservation"}})
    orders.create({**child_request.__dict__, "state": "SUBMISSION_ATTEMPTED", "client_order_id": "fallback-client",
                   "exchange_order_id": "fallback-order"})
    router = ExecutionRouter(Client(), reservations=Reservations(), orders=orders)
    asyncio.run(router.recover_unresolved())
    asyncio.run(router.recover_unresolved())
    assert posts == []
    assert orders.get("fallback-restart-F")["state"] == "CANCELLED"
    assert orders.get("fallback-restart")["state"] == "CANCELLED"
    assert releases == ["reservation"]


def test_limit_fallback_canceled_first_uses_initial_confirmation(tmp_path):
    class Client:
        async def get_order(self, *_args, **_kwargs):
            return {"state": "canceled", "ordId": "order-1", "accFillSz": "0"}

    request = _request("fallback")
    orders = OrderStateStore(tmp_path / "orders.json")
    orders.create({**request.__dict__, "state": "ACKNOWLEDGED", "client_order_id": "expected"})
    router = ExecutionRouter(Client(), orders=orders)

    async def fake_submit(fallback):
        assert fallback.intent_id == "fallback-F"
        assert fallback.metadata["fallback_parent_intent_id"] == "fallback"
        assert fallback.metadata["reservation_owner_intent_id"] == "fallback"
        orders.create({**fallback.__dict__, "state": "ACKNOWLEDGED"})
        orders.finalize_terminal(
            fallback.intent_id, "FILLED", terminal_class="filled", terminal_reason="fallback_filled",
        )
        return SimpleNamespace(ok=True)

    router.submit = fake_submit
    result = asyncio.run(router._limit_timeout_fallback(request, "order-1", "expected"))
    assert result["state"] == "CANCELLED"
    assert result["cancel_confirmation"]["state"] == "canceled"
    assert result["fallback_child_intent_id"] == "fallback-F"


def test_fallback_cancel_boundary_persists_before_first_cancel_and_restart_does_not_repeat(tmp_path):
    cancel_observations = []
    request = _request("fallback-cancel-boundary")
    orders_path = tmp_path / "orders.json"
    orders = OrderStateStore(orders_path)
    orders.create({**request.__dict__, "state": "ACKNOWLEDGED", "client_order_id": "expected",
                   "exchange_order_id": "order-1"})

    class Client:
        async def get_order(self, *_args, **_kwargs):
            return {"state": "live", "ordId": "order-1", "accFillSz": "0"}

        async def cancel_order(self, *_args, **_kwargs):
            row = OrderStateStore(orders_path).get(request.intent_id)
            cancel_observations.append(dict(row))
            raise TimeoutError("cancel response lost")

        async def get_order_history(self, *_args, **_kwargs):
            return []

        async def get_fills(self, *_args, **_kwargs):
            return []

        async def get_positions(self, *_args, **_kwargs):
            return []

    router = ExecutionRouter(Client(), orders=orders)
    pending = asyncio.run(router._limit_timeout_fallback(request, "order-1", "expected"))
    assert pending["state"] == "CANCEL_PENDING"
    assert pending["recovery_reason"] == "fallback_cancel_readback_required"
    assert len(cancel_observations) == 1
    assert cancel_observations[0]["cancel_attempted"] is True
    assert cancel_observations[0]["cancel_physical_attempt_count"] == 1

    # Recovery has only durable cancel-boundary truth, so it reads the order
    # and never sends a second cancel or fallback entry POST.
    restarted = ExecutionRouter(Client(), orders=OrderStateStore(orders_path))
    asyncio.run(restarted.recover_unresolved())
    assert len(cancel_observations) == 1


def test_partial_timeout_cancel_boundary_persists_before_first_cancel(tmp_path, monkeypatch):
    observations = []
    request = ExecutionRequest(**{**_request("partial-timeout-cancel-boundary").__dict__, "timeout_secs": 0.1})
    orders_path = tmp_path / "orders.json"
    orders = OrderStateStore(orders_path)
    orders.create({**request.__dict__, "state": "ACKNOWLEDGED", "client_order_id": "expected"})

    class Client:
        async def get_order(self, *_args, **_kwargs):
            return {"state": "partially_filled", "ordId": "order-1", "accFillSz": "0.4", "avgPx": "100"}

        async def cancel_order(self, *_args, **_kwargs):
            observations.append(OrderStateStore(orders_path).get(request.intent_id))
            raise TimeoutError("cancel response lost")

    router = ExecutionRouter(Client(), orders=orders)
    router.protection = SimpleNamespace(ensure=lambda **_kwargs: asyncio.sleep(0, result=SimpleNamespace(
        protected=True, reason="", protection_state="CONFIRMED",
    )))
    result = asyncio.run(router._confirm_live_state(request, "order-1", "expected"))
    assert result["state"] == "CANCEL_PENDING"
    assert result["recovery_reason"] == "partial_remainder_cancel_readback_required"
    assert len(observations) == 1
    assert observations[0]["cancel_attempted"] is True
    assert observations[0]["remainder_cancel_requested"] is True


def test_definitive_exchange_rejection_increments_funnel_once(tmp_path, monkeypatch):
    monkeypatch.setenv("PAPER_TRADING", "true")
    flow.reset_counters_for_tests()

    class OKXDefinitiveRejection(Exception):
        evidence = {"sCode": "51008"}

    class Reservations:
        async def release(self, _reservation_id):
            return True

    class Client:
        async def place_market_order(self, *_args, **kwargs):
            kwargs["on_exchange_request"]()
            raise OKXDefinitiveRejection("business rejection")

    request = _request("definitive-rejection")
    router = ExecutionRouter(Client(), reservations=Reservations(), orders=OrderStateStore(tmp_path / "orders.json"))
    monkeypatch.setattr(router, "_local_executable_check", lambda *_args, **_kwargs: (True, "OK", {}))
    monkeypatch.setattr(router, "_release_pre_submit_registry", lambda _coin: None)

    result = asyncio.run(router.submit(request))
    row = router.orders.get(request.intent_id)
    assert not result.ok
    assert row["terminal_class"] == "exchange_rejection"
    assert flow.counters_snapshot()["submission_attempted"] == 1
    assert flow.counters_snapshot()["exchange_rejected"] == 1

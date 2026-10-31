import asyncio

import httpx
import pytest

import okx_client
import exit_manager
import position_registry
from instrument_registry import InstrumentMetadata
from okx_mutation_results import (
    MUTATION_DEFINITIVE_REJECTION,
    MUTATION_READBACK_REQUIRED,
    MUTATION_SUCCESS,
    classify_mutation_response,
)
from okx_client_ids import (
    InvalidOKXClientID,
    canonical_client_id,
    canonical_protection_algo_client_id,
    validate_client_id,
)
from protection_manager import (
    ProtectionManager,
    PROTECTION_SUBMISSION_ATTEMPTED,
    REPROTECTION_CONFIRMED,
    REPROTECTION_FAILED,
)


def _old_protection():
    return {
        "algoId": "OLD",
        "instId": "XRP-USDT-SWAP",
        "ordType": "conditional",
        "posSide": "short",
        "side": "buy",
        "sz": "0.03",
        "reduceOnly": "true",
        "state": "live",
        "slTriggerPx": "1.0091",
        "tpTriggerPx": "0",
    }


class RejectingProtectionClient:
    def __init__(self, *, cancel_response=None, cancel_removes=True):
        self.orders = [_old_protection()]
        self.place_calls = []
        self.cancel_calls = []
        self.read_calls = 0
        self.instrument_metadata = InstrumentMetadata(
            inst_id="XRP-USDT-SWAP", inst_type="SWAP", settle_ccy="USDT",
            ct_val=1.0, ct_val_ccy="USDT", ct_type="linear", lot_sz=0.01,
            min_sz=0.01, tick_sz=0.0001, state="live",
        )
        self.reject = True
        self.item_reject = False
        self.cancel_response = cancel_response
        self.cancel_removes = cancel_removes

    async def list_live_protection_orders(self, coin, *, pos_side=None, **kwargs):
        self.read_calls += 1
        return {"ok": True, "orders": list(self.orders), "errors": [], "queried_types": ["oco", "conditional"]}

    async def get_positions(self):
        return [{"instId": "XRP-USDT-SWAP", "posSide": "short", "pos": "0.03"}]

    async def place_protective_order(self, **kwargs):
        self.place_calls.append(kwargs)
        if self.item_reject:
            return {
                "code": "0",
                "msg": "",
                "data": [{
                    "sCode": "51000",
                    "sMsg": "Parameter algoClOrdId error",
                    "algoClOrdId": kwargs.get("client_order_id", ""),
                }],
            }
        if self.reject:
            raise okx_client.OKXDefinitiveRejection(
                "Parameter algoClOrdId error", code="51000"
            )
        self.orders.append({**_old_protection(), "algoId": "NEW", "slTriggerPx": str(kwargs["stop_price"])})
        return {"code": "0", "data": [{"algoId": "NEW"}]}

    async def cancel_protective_orders(self, orders):
        self.cancel_calls.append(list(orders))
        if self.cancel_removes:
            ids = {str(row.get("algoId", "")) for row in orders}
            self.orders[:] = [row for row in self.orders if str(row.get("algoId", "")) not in ids]
        if self.cancel_response is not None:
            return self.cancel_response
        return {
            "code": "0",
            "data": [{"sCode": "0", "algoId": str(row.get("algoId", ""))} for row in orders],
        }


def _reprotect(client, *, stop=0.9952):
    return asyncio.run(ProtectionManager(client).reprotect(
        coin="XRP", direction="short", current_contracts=0.03,
        desired_stop_price=stop, take_profit=0.0,
        client_order_id="OXYEentry", paper=False,
    ))


def test_xrp_incident_id_is_valid_deterministic_and_distinct():
    old_identity = "f78ffd805d17a03133bb5eb5"
    first = canonical_protection_algo_client_id(old_identity)
    second = canonical_protection_algo_client_id(old_identity)
    other = canonical_protection_algo_client_id("different-logical-intent")

    assert first == second
    assert first != other
    assert len(first) <= 32
    assert first.isalnum()
    assert "-" not in first


def test_long_source_identity_is_encoded_without_losing_determinism():
    value = canonical_client_id("OXYA", "x" * 1000)
    assert validate_client_id(value) == value
    assert len(value) == 32


def test_invalid_client_id_is_rejected_before_protection_attempt(monkeypatch):
    client = RejectingProtectionClient()
    manager = ProtectionManager(client)
    monkeypatch.setattr(manager, "_client_id", lambda identity: "OXYR-invalid-id")
    lifecycles = []
    async def persist(*args, **kwargs):
        lifecycles.append((args[2], args[3]))
        return True

    monkeypatch.setattr(manager, "_persist_lifecycle", persist)

    result = asyncio.run(manager.reprotect(
        coin="XRP", direction="short", current_contracts=0.03,
        desired_stop_price=0.9952, take_profit=0.0,
        client_order_id="OXYEentry", paper=False,
    ))

    assert result.reason == REPROTECTION_FAILED
    assert result.error.startswith("invalid_local_client_id:")
    assert client.place_calls == []
    assert (PROTECTION_SUBMISSION_ATTEMPTED, True) not in lifecycles


def test_definitive_rejection_keeps_old_exchange_stop_and_is_terminal():
    client = RejectingProtectionClient()

    result = _reprotect(client)

    assert result.reason == REPROTECTION_FAILED
    assert result.protected
    assert result.protection_state == "PROTECTED"
    assert result.confirmed_stop_price == pytest.approx(1.0091)
    assert client.cancel_calls == []
    assert len(client.place_calls) == 1


def test_item_level_51000_is_terminal_and_persists_rejection_evidence(monkeypatch):
    client = RejectingProtectionClient()
    client.reject = False
    client.item_reject = True
    manager = ProtectionManager(client)
    row = {}

    def persist(*, coin, intent, lifecycle, mutation_attempted):
        row.update({
            "protection_intent": dict(intent),
            "position_lifecycle_id": intent.get("position_lifecycle_id", ""),
            "protection_lifecycle": lifecycle,
            "exchange_protection_status": "STOP_CONFIRMED",
            "exchange_confirmed_stop_price": 1.0091,
        })
        return True

    monkeypatch.setattr(manager, "_registry_row", lambda coin: dict(row))
    monkeypatch.setattr(manager, "_persist_intent", persist)

    result = asyncio.run(manager.reprotect(
        coin="XRP", direction="short", current_contracts=0.03,
        desired_stop_price=0.9952, take_profit=0.0,
        client_order_id="OXYEentry", paper=False,
    ))

    assert result.reason == REPROTECTION_FAILED
    assert result.protected
    assert result.mutation_attempted
    assert len(client.place_calls) == 1
    evidence = row["protection_intent"]["rejection_evidence"]
    assert evidence["item_s_code"] == "51000"
    assert evidence["item_s_msg"] == "Parameter algoClOrdId error"
    assert evidence["timestamp"]


def test_mutation_classifier_requires_item_evidence_and_checks_every_item():
    success = classify_mutation_response(
        {"code": "0", "data": [{"sCode": "0", "algoId": "A", "algoClOrdId": "OXYAclient"}]},
        expected_items=1,
        submitted_client_id="OXYAclient",
    )
    assert success.outcome == MUTATION_SUCCESS
    assert success.client_id == "OXYAclient"
    assert success.exchange_id == "A"
    assert success.exchange_algo_id == "A"
    assert success.exchange_order_id == ""

    order_success = classify_mutation_response(
        {"code": "0", "data": [{"sCode": "0", "ordId": "ORDER-1", "clOrdId": "OXYEclient"}]},
        expected_items=1,
        submitted_client_id="OXYEclient",
    )
    assert order_success.exchange_id == "ORDER-1"
    assert order_success.exchange_order_id == "ORDER-1"
    assert order_success.exchange_algo_id == ""

    missing_exchange_id = classify_mutation_response(
        {"code": "0", "data": [{"sCode": "0", "algoClOrdId": "OXYAclient"}]},
        expected_items=1,
        submitted_client_id="OXYAclient",
    )
    assert missing_exchange_id.client_id == "OXYAclient"
    assert missing_exchange_id.exchange_id == ""
    assert missing_exchange_id.exchange_algo_id == ""

    rejection = classify_mutation_response(
        {"code": "0", "data": [
            {"sCode": "0", "algoId": "A"},
            {"sCode": "51000", "sMsg": "bad", "algoId": "B"},
        ]},
        expected_items=2,
    )
    assert rejection.outcome == MUTATION_DEFINITIVE_REJECTION
    assert rejection.item_s_code == "51000"
    assert rejection.timestamp

    assert classify_mutation_response({"code": "0", "data": []}).outcome == MUTATION_READBACK_REQUIRED
    assert classify_mutation_response({"code": "0", "data": [{}]}).outcome == MUTATION_READBACK_REQUIRED
    assert classify_mutation_response({"code": "0", "data": ["bad"]}).outcome == MUTATION_READBACK_REQUIRED
    mismatch = classify_mutation_response(
        {"code": "0", "data": [{"sCode": "0", "algoClOrdId": "OTHER", "algoId": "EXCHANGE"}]},
        expected_items=1,
        submitted_client_id="SUBMITTED",
    )
    assert mismatch.outcome == MUTATION_READBACK_REQUIRED
    assert mismatch.evidence["reason"] == "client_id_mismatch"


def test_protection_transaction_identity_is_lifecycle_and_geometry_aware():
    kwargs = {
        "inst_id": "XRP-USDT-SWAP",
        "pos_side": "short",
        "contracts": 0.03,
        "stop_price": 0.9952,
        "take_profit": 0.0,
        "reason": "dynamic_reprotection",
    }
    first = ProtectionManager._intent_identity(position_lifecycle_id="entry:ONE", **kwargs)
    restarted = ProtectionManager._intent_identity(position_lifecycle_id="entry:ONE", **kwargs)
    different_position = ProtectionManager._intent_identity(position_lifecycle_id="entry:TWO", **kwargs)
    different_stop = ProtectionManager._intent_identity(
        position_lifecycle_id="entry:ONE", **{**kwargs, "stop_price": 0.9940}
    )
    assert first == restarted
    assert first != different_position
    assert first != different_stop


def test_real_position_lifecycles_converge_through_registry_exitplan_and_algo_id(monkeypatch):
    monkeypatch.setattr(position_registry, "_registry", {})
    monkeypatch.setattr(position_registry, "_save", lambda *args, **kwargs: None)
    kwargs = {
        "entry_price": 1.0,
        "size_usdt": 3.0,
        "atr_val": 0.01,
        "hard_sl": 1.0091,
        "tp1_price": 0.0,
        "tp2_price": 0.0,
        "engine_name": "TEST",
        "leverage": 10,
    }

    registry_kwargs = {
        "size_usdt": kwargs["size_usdt"], "entry": kwargs["entry_price"],
        "leverage": kwargs["leverage"], "stop_loss": kwargs["hard_sl"],
        "take_profit": kwargs["tp1_price"], "engine": kwargs["engine_name"],
    }
    position_registry.register(
        "XRP", "short", order_id="ENTRY-A",
        metadata={"entry_client_order_id": "OXYEENTRYA"}, **registry_kwargs,
    )
    plan_a = exit_manager.register("XRP", "short", order_id="ENTRY-A", client_order_id="OXYEENTRYA", **kwargs)
    lifecycle_a = plan_a.position_lifecycle_id
    identity_a = ProtectionManager._intent_identity(
        position_lifecycle_id=lifecycle_a,
        inst_id="XRP-USDT-SWAP", pos_side="short", contracts=0.03,
        stop_price=0.9952, take_profit=0.0, reason="dynamic_reprotection",
    )
    algo_a = ProtectionManager._client_id(identity_a)

    position_registry.register(
        "XRP", "short", order_id="ENTRY-B",
        metadata={"entry_client_order_id": "OXYEENTRYB"}, **registry_kwargs,
    )
    plan_b = exit_manager.register("XRP", "short", order_id="ENTRY-B", client_order_id="OXYEENTRYB", **kwargs)
    lifecycle_b = plan_b.position_lifecycle_id
    identity_b = ProtectionManager._intent_identity(
        position_lifecycle_id=lifecycle_b,
        inst_id="XRP-USDT-SWAP", pos_side="short", contracts=0.03,
        stop_price=0.9952, take_profit=0.0, reason="dynamic_reprotection",
    )
    algo_b = ProtectionManager._client_id(identity_b)

    assert lifecycle_a != lifecycle_b
    assert identity_a != identity_b
    assert algo_a != algo_b
    assert algo_a.startswith("OXYA") and algo_b.startswith("OXYA")
    assert plan_b.position_lifecycle_id == position_registry.get_all_details()["XRP"]["position_lifecycle_id"]

    restored = exit_manager.register(
        "XRP", "short", order_id="ENTRY-B", client_order_id="OXYEENTRYB",
        position_lifecycle_id=lifecycle_b, **kwargs,
    )
    assert restored.position_lifecycle_id == lifecycle_b


def test_legacy_live_row_without_durable_identity_fails_closed_without_post(monkeypatch):
    client = RejectingProtectionClient()
    manager = ProtectionManager(client)
    monkeypatch.setattr(manager, "_registry_row", lambda coin: {
        "exchange_protection_status": "STOP_CONFIRMED",
        "exchange_confirmed_stop_price": 1.0091,
    })

    result = asyncio.run(manager.reprotect(
        coin="XRP", direction="short", current_contracts=0.03,
        desired_stop_price=0.9952, take_profit=0.0,
        client_order_id="", paper=False,
    ))

    assert result.reason == REPROTECTION_FAILED
    assert result.protected
    assert result.error == "position_lifecycle_identity_unavailable"
    assert client.place_calls == []


def test_same_definitive_rejection_is_not_reposted_but_new_stop_is_allowed(monkeypatch):
    client = RejectingProtectionClient()
    manager = ProtectionManager(client)
    row = {}

    def persist(*, coin, intent, lifecycle, mutation_attempted):
        row.update({
            "protection_intent": dict(intent),
            "position_lifecycle_id": intent.get("position_lifecycle_id", ""),
            "protection_lifecycle": lifecycle,
            "exchange_protection_status": "STOP_CONFIRMED" if lifecycle == REPROTECTION_FAILED else "",
            "exchange_confirmed_stop_price": 1.0091,
        })
        return True

    monkeypatch.setattr(manager, "_registry_row", lambda coin: dict(row))
    monkeypatch.setattr(manager, "_persist_intent", persist)

    first = asyncio.run(manager.reprotect(
        coin="XRP", direction="short", current_contracts=0.03,
        desired_stop_price=0.9952, take_profit=0.0,
        client_order_id="OXYEentry", paper=False,
    ))
    reads_after_first = client.read_calls
    second = asyncio.run(manager.reprotect(
        coin="XRP", direction="short", current_contracts=0.03,
        desired_stop_price=0.9952, take_profit=0.0,
        client_order_id="OXYEentry", paper=False,
    ))

    assert first.reason == REPROTECTION_FAILED
    assert second.reason == REPROTECTION_FAILED
    assert second.protected
    assert len(client.place_calls) == 1
    assert client.read_calls == reads_after_first

    client.reject = False
    third = asyncio.run(manager.reprotect(
        coin="XRP", direction="short", current_contracts=0.03,
        desired_stop_price=0.9940, take_profit=0.0,
        client_order_id="OXYEentry", paper=False,
    ))
    assert third.reason == REPROTECTION_CONFIRMED
    assert len(client.place_calls) == 2
    assert client.place_calls[0]["client_order_id"] != client.place_calls[1]["client_order_id"]


def test_okx_protection_boundary_rejects_invalid_algo_id_without_request(monkeypatch):
    monkeypatch.setattr(okx_client, "PAPER_TRADING", False)
    client = okx_client.OKXClient()
    calls = []
    monkeypatch.setattr(client, "_request", lambda *args, **kwargs: calls.append(args) or {"code": "0"})

    with pytest.raises(InvalidOKXClientID):
        asyncio.run(client.place_protective_order(
            coin="XRP", direction="short", contracts=0.03,
            stop_price=1.0091, client_order_id="OXYR-invalid-id",
        ))

    assert calls == []


def test_http_51000_response_is_structured_definitive_rejection(monkeypatch):
    class MutationHTTP:
        async def request(self, *args, **kwargs):
            return httpx.Response(
                400,
                request=httpx.Request("POST", "https://example.test"),
                json={"code": "51000", "msg": "Parameter algoClOrdId error", "data": []},
            )

    monkeypatch.setattr(okx_client, "_mutation_http_client", MutationHTTP())
    monkeypatch.setattr(okx_client, "_mutation_request_gate", None)
    client = okx_client.OKXClient()
    monkeypatch.setattr(client, "_headers", lambda *_args: {})

    with pytest.raises(okx_client.OKXDefinitiveRejection) as exc_info:
        asyncio.run(client._request("POST", "/api/v5/trade/order-algo", data={"algoClOrdId": "OXYAvalid"}))

    assert exc_info.value.category == "definitive_rejection"
    assert exc_info.value.code == "51000"
    assert exc_info.value.evidence["top_level_code"] == "51000"
    assert exc_info.value.evidence["client_id"] == "OXYAvalid"


def test_successful_http_response_with_item_51000_is_definitive(monkeypatch):
    monkeypatch.setattr(okx_client, "PAPER_TRADING", False)
    client = okx_client.OKXClient()

    async def request(*args, **kwargs):
        return {
            "code": "0",
            "msg": "",
            "data": [{
                "sCode": "51000",
                "sMsg": "Parameter algoClOrdId error",
                "algoClOrdId": "OXYA1234567890123456789012345",
            }],
        }

    monkeypatch.setattr(client, "_request", request)

    with pytest.raises(okx_client.OKXDefinitiveRejection) as exc_info:
        asyncio.run(client.place_protective_order(
            coin="XRP", direction="short", contracts=0.03,
            stop_price=1.0091, client_order_id="OXYA1234567890123456789012345",
        ))

    assert exc_info.value.s_code == "51000"
    assert exc_info.value.evidence["item_s_code"] == "51000"


@pytest.mark.parametrize(
    "cancel_response",
    [
        {"code": "0", "data": [{"sCode": "51000", "sMsg": "cancel rejected", "algoId": "OLD"}]},
        {"code": "51000", "msg": "cancel rejected", "data": []},
        {"code": "0", "data": []},
        {"code": "0", "data": ["malformed"]},
        {"code": "0", "data": [{"sCode": "0"}]},
    ],
)
def test_cleanup_response_requires_classifier_and_readback(cancel_response, monkeypatch):
    client = RejectingProtectionClient(cancel_response=cancel_response, cancel_removes=False)
    client.reject = False
    manager = ProtectionManager(client)
    row = {}

    def persist(*, coin, intent, lifecycle, mutation_attempted):
        row.update({
            "protection_intent": dict(intent),
            "position_lifecycle_id": intent.get("position_lifecycle_id", ""),
            "protection_lifecycle": lifecycle,
            "exchange_protection_status": "STOP_CONFIRMED",
            "exchange_confirmed_stop_price": 1.0091,
        })
        return True

    monkeypatch.setattr(manager, "_registry_row", lambda coin: dict(row))
    monkeypatch.setattr(manager, "_persist_intent", persist)
    kwargs = {
        "coin": "XRP", "direction": "short", "current_contracts": 0.03,
        "desired_stop_price": 0.9952, "take_profit": 0.0,
        "client_order_id": "OXYEentry", "paper": False,
    }
    result = asyncio.run(manager.reprotect(**kwargs))

    assert result.protected
    assert result.reason == "PROTECTION_CONFIRMED_CLEANUP_PENDING"
    assert result.cleanup_pending
    assert len(client.cancel_calls) == 1
    evidence = row["protection_intent"]["cleanup_response_evidence"]
    assert evidence
    if cancel_response.get("data") and isinstance(cancel_response["data"][0], dict):
        assert evidence.get("item_s_code") == cancel_response["data"][0].get("sCode", "") or evidence.get("outcome") == "READBACK_REQUIRED"

    repeated = asyncio.run(manager.reprotect(**kwargs))
    assert repeated.reason == "PROTECTION_CONFIRMED_CLEANUP_PENDING"
    assert len(client.cancel_calls) == 1


def test_cleanup_success_requires_item_evidence_and_readback(monkeypatch):
    client = RejectingProtectionClient(
        cancel_response={"code": "0", "data": [{"sCode": "0", "algoId": "OLD"}]},
    )
    client.reject = False
    manager = ProtectionManager(client)
    row = {}

    def persist(*, coin, intent, lifecycle, mutation_attempted):
        row.update({
            "protection_intent": dict(intent),
            "position_lifecycle_id": intent.get("position_lifecycle_id", ""),
            "protection_lifecycle": lifecycle,
            "exchange_protection_status": "STOP_CONFIRMED",
            "exchange_confirmed_stop_price": 1.0091,
        })
        return True

    monkeypatch.setattr(manager, "_registry_row", lambda coin: dict(row))
    monkeypatch.setattr(manager, "_persist_intent", persist)
    result = asyncio.run(manager.reprotect(
        coin="XRP", direction="short", current_contracts=0.03,
        desired_stop_price=0.9952, take_profit=0.0,
        client_order_id="OXYEentry", paper=False,
    ))

    assert result.reason == REPROTECTION_CONFIRMED
    assert result.protected
    assert len(client.cancel_calls) == 1

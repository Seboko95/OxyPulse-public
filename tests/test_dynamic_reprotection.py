import asyncio
import time

import pytest

from tp_ownership import EXCHANGE_FULL

from instrument_registry import InstrumentMetadata
from protection_manager import (
    ProtectionManager,
    PROTECTION_CONFIRMED_CLEANUP_PENDING,
    PROTECTION_CONFIRMED_NOT_FOUND,
    PROTECTION_SUBMIT_UNCERTAIN,
    REPROTECTION_CONFIRMED,
    REPROTECTION_FAILED,
    REPROTECTION_PENDING,
    REPROTECTION_UNKNOWN,
    REPROTECTION_UNCHANGED,
)


def protection(*, algo_id, stop, take="9.462", size="0.4", pos_side="long", ord_type="oco"):
    return {
        "algoId": algo_id,
        "instId": "LINK-USDT-SWAP",
        "ordType": ord_type,
        "posSide": pos_side,
        "side": "sell" if pos_side == "long" else "buy",
        "sz": size,
        "reduceOnly": "true",
        "state": "live",
        "slTriggerPx": str(stop),
        "tpTriggerPx": str(take),
    }


class ReprotectClient:
    def __init__(self, orders=None, *, visible_after_place=True, fail_place=False, read_fail=False, fail_cancel=False):
        self.orders = list(orders or [])
        self.visible_after_place = visible_after_place
        self.fail_place = fail_place
        self.read_fail = read_fail
        self.fail_cancel = fail_cancel
        self.place_calls = []
        self.cancel_calls = []
        self.call_order = []
        self.instrument_metadata = InstrumentMetadata(
            inst_id="LINK-USDT-SWAP", inst_type="SWAP", settle_ccy="USDT",
            ct_val=1.0, ct_val_ccy="USDT", ct_type="linear", lot_sz=0.1,
            min_sz=0.1, tick_sz=0.001, state="live", refreshed_at=time.time(),
        )

    async def list_live_protection_orders(self, coin, *, pos_side=None):
        self.call_order.append("read")
        if self.read_fail:
            return {"ok": False, "orders": [], "errors": ["timeout"]}
        return {"ok": True, "orders": list(self.orders), "errors": [], "queried_types": ["oco", "conditional"]}

    async def place_protective_order(self, **kwargs):
        self.call_order.append("place")
        self.place_calls.append(kwargs)
        if self.fail_place:
            raise TimeoutError("mutation timeout")
        if self.visible_after_place:
            self.orders.append(
                protection(
                    algo_id="NEW",
                    stop=kwargs["stop_price"],
                    take=kwargs.get("take_profit", 0.0) or "0",
                    size=kwargs["contracts"],
                    pos_side="long" if kwargs["direction"] == "long" else "short",
                    ord_type="oco" if kwargs.get("take_profit", 0.0) else "conditional",
                )
            )
        return {"code": "0", "data": [{"algoId": "NEW"}]}

    async def get_positions(self):
        row = self.orders[0] if self.orders else {}
        return [{
            "instId": "LINK-USDT-SWAP",
            "posSide": row.get("posSide", "long"),
            "pos": row.get("sz", "0.4") or "0.4",
        }]

    async def cancel_protective_orders(self, orders):
        self.call_order.append("cancel")
        self.cancel_calls.append(list(orders))
        if self.fail_cancel:
            raise TimeoutError("cleanup timeout")
        ids = {str(row.get("algoId", "")) for row in orders}
        self.orders[:] = [row for row in self.orders if str(row.get("algoId", "")) not in ids]
        return {
            "code": "0",
            "data": [{"sCode": "0", "algoId": str(row.get("algoId", ""))} for row in orders],
        }


def reprotect(client, **kwargs):
    defaults = {
        "coin": "LINK",
        "direction": "long",
        "current_contracts": 0.4,
        "desired_stop_price": 8.800,
        "take_profit": 9.462,
        "tp_owner": EXCHANGE_FULL,
        "client_order_id": "ENTRY-1",
        "paper": False,
    }
    defaults.update(kwargs)
    return asyncio.run(ProtectionManager(client).reprotect(**defaults))


def test_improvement_places_current_quantity_then_readback_then_cleans_old():
    client = ReprotectClient([protection(algo_id="OLD", stop="8.742")])
    result = reprotect(client)

    assert result.reason == REPROTECTION_CONFIRMED
    assert result.confirmed_stop_price == pytest.approx(8.8)
    assert client.place_calls[0]["contracts"] == pytest.approx(0.4)
    assert client.call_order.index("place") < client.call_order.index("cancel")
    assert [row["algoId"] for row in client.cancel_calls[0]] == ["OLD"]


def test_short_improvement_is_monotonic_and_tick_normalized():
    client = ReprotectClient([protection(algo_id="OLD", stop="8.800", pos_side="short")])
    result = reprotect(client, direction="short", desired_stop_price=8.7417, take_profit=8.2)

    assert result.reason == REPROTECTION_CONFIRMED
    assert result.confirmed_stop_price == pytest.approx(8.741)
    assert client.place_calls[0]["contracts"] == pytest.approx(0.4)


@pytest.mark.parametrize("desired", [8.742, 8.7417, 8.700])
def test_equal_or_weaker_stop_is_noop(desired):
    client = ReprotectClient([protection(algo_id="OLD", stop="8.742")])
    result = reprotect(client, desired_stop_price=desired)

    assert result.reason == REPROTECTION_UNCHANGED
    assert client.place_calls == []
    assert client.cancel_calls == []


def test_material_tick_difference_is_an_improvement():
    client = ReprotectClient([protection(algo_id="OLD", stop="8.742")])
    result = reprotect(client, desired_stop_price=8.743)

    assert result.reason == REPROTECTION_CONFIRMED
    assert client.place_calls[0]["stop_price"] == pytest.approx(8.743)


def test_reprotection_uses_remaining_exchange_quantity_not_original_size():
    client = ReprotectClient([protection(algo_id="OLD", stop="8.742", size="0.2")])
    result = reprotect(client, current_contracts=0.2)

    assert result.reason == REPROTECTION_CONFIRMED
    assert client.place_calls[0]["contracts"] == pytest.approx(0.2)


def test_tp_is_preserved_when_caller_does_not_supply_one():
    client = ReprotectClient([protection(algo_id="OLD", stop="8.742", take="9.462")])
    result = reprotect(client, take_profit=0.0)

    assert result.reason == REPROTECTION_CONFIRMED
    assert client.place_calls[0]["take_profit"] == pytest.approx(9.462)


def test_exchange_tp_wins_over_caller_fallback_during_stop_only_change():
    client = ReprotectClient([protection(algo_id="OLD", stop="8.742", take="9.300")])
    result = reprotect(client, take_profit=9.900)

    assert result.reason == REPROTECTION_CONFIRMED
    assert client.place_calls[0]["take_profit"] == pytest.approx(9.300)


def test_missing_existing_protection_is_unknown_and_never_creates():
    client = ReprotectClient()
    result = reprotect(client)

    assert result.reason == REPROTECTION_UNKNOWN
    assert client.place_calls == []
    assert client.cancel_calls == []


def test_read_failure_is_unknown_and_old_protection_is_untouched():
    client = ReprotectClient([protection(algo_id="OLD", stop="8.742")], read_fail=True)
    result = reprotect(client)

    assert result.reason == REPROTECTION_UNKNOWN
    assert client.place_calls == []
    assert client.cancel_calls == []


def test_mutation_failure_is_pending_and_does_not_cancel_old_protection():
    client = ReprotectClient([protection(algo_id="OLD", stop="8.742")], fail_place=True)
    result = reprotect(client)

    assert result.reason == REPROTECTION_PENDING
    assert client.cancel_calls == []
    assert [row["algoId"] for row in client.orders] == ["OLD"]


def test_readback_absence_is_pending_and_does_not_cancel_old_protection():
    client = ReprotectClient([protection(algo_id="OLD", stop="8.742")], visible_after_place=False)
    result = reprotect(client)

    assert result.reason == REPROTECTION_PENDING
    assert client.cancel_calls == []
    assert [row["algoId"] for row in client.orders] == ["OLD"]


def test_conditional_protection_is_discovered_and_replaced():
    client = ReprotectClient([protection(algo_id="OLD", stop="8.742", take="0", ord_type="conditional")])
    result = reprotect(client, take_profit=0.0)

    assert result.reason == REPROTECTION_CONFIRMED
    assert client.place_calls[0]["take_profit"] == pytest.approx(0.0)


def test_duplicate_old_protection_is_cleaned_only_after_new_is_visible():
    client = ReprotectClient([
        protection(algo_id="OLD-1", stop="8.742"),
        protection(algo_id="OLD-2", stop="8.742"),
    ])
    result = reprotect(client)

    assert result.reason == REPROTECTION_CONFIRMED
    assert {row["algoId"] for row in client.cancel_calls[0]} == {"OLD-1", "OLD-2"}
    assert [row["algoId"] for row in client.orders] == ["NEW"]


def test_repeated_ensure_is_idempotent_after_first_reprotection():
    client = ReprotectClient([protection(algo_id="OLD", stop="8.742")])
    manager = ProtectionManager(client)
    kwargs = {
        "coin": "LINK", "direction": "long", "current_contracts": 0.4,
        "desired_stop_price": 8.8, "take_profit": 9.462, "client_order_id": "ENTRY-1",
        "paper": False, "tp_owner": EXCHANGE_FULL,
    }
    first = asyncio.run(manager.reprotect(**kwargs))
    second = asyncio.run(manager.reprotect(**kwargs))

    assert first.reason == REPROTECTION_CONFIRMED
    assert second.reason == REPROTECTION_UNCHANGED
    assert len(client.place_calls) == 1


def test_concurrent_reprotection_requests_do_not_duplicate_post():
    client = ReprotectClient([protection(algo_id="OLD", stop="8.742")])
    manager = ProtectionManager(client)
    kwargs = {
        "coin": "LINK", "direction": "long", "current_contracts": 0.4,
        "desired_stop_price": 8.8, "take_profit": 9.462, "client_order_id": "ENTRY-1",
        "paper": False, "tp_owner": EXCHANGE_FULL,
    }

    async def run():
        return await asyncio.gather(manager.reprotect(**kwargs), manager.reprotect(**kwargs))

    results = asyncio.run(run())
    assert {result.reason for result in results} == {REPROTECTION_CONFIRMED, REPROTECTION_UNCHANGED}
    assert len(client.place_calls) == 1


def test_paper_reprotection_is_local_and_never_contacts_exchange():
    client = ReprotectClient([protection(algo_id="OLD", stop="8.742")])
    result = reprotect(client, paper=True)

    assert result.reason == REPROTECTION_CONFIRMED
    assert client.place_calls == []
    assert client.cancel_calls == []


def test_exit_manager_records_local_desired_stop_until_exchange_confirmation(monkeypatch):
    import exit_manager

    monkeypatch.setattr(exit_manager, "_paper_trading_enabled", lambda: False)
    plan = exit_manager.ExitPlan(
        coin="LINK", direction="long", engine_name="TEST", entry_price=8.7,
        size_usdt=100.0, atr_val=0.1, hard_sl=8.742, tp1_price=9.2,
        tp2_price=9.462,
    )
    exit_manager._apply_stop_improvement("LINK", plan, 8.8, True, "trail_s3")

    assert plan.desired_stop_price == pytest.approx(8.8)
    assert plan.exchange_confirmed_stop_price == pytest.approx(0.0)
    assert plan.exchange_protection_status == "STOP_PROPOSED"
    assert any(row["event"] == "STOP_IMPROVEMENT_PROPOSED" for row in plan.exit_timeline)


def test_telegram_positions_show_desired_and_exchange_confirmed_stops():
    from tg_panels import panel_positions_truth_text

    text = panel_positions_truth_text({
        "positions": {
            "LINK": {
                "side": "long", "source_label": "EXCHANGE_TRUTH", "unrealised_pnl": 0.2,
                "roi_pct": 1.0, "registry": {
                    "stop_loss": 8.8,
                    "desired_stop_price": 8.8,
                    "exchange_confirmed_stop_price": 8.742,
                    "exchange_protection_status": "STOP_PROPOSED",
                    "take_profit": 9.462,
                },
            }
        },
        "open_pnl_usdt": 0.2, "source_label": "EXCHANGE_TRUTH", "age_sec": 1,
    })

    assert "SL desired" in text
    assert "exchange" in text
    assert "STOP_PROPOSED" in text


def test_intent_is_persisted_reserved_then_attempted_before_post(monkeypatch):
    client = ReprotectClient([protection(algo_id="OLD", stop="8.742")])
    manager = ProtectionManager(client)
    events = []

    def persist(**kwargs):
        events.append((kwargs["lifecycle"], kwargs["mutation_attempted"]))
        return True

    monkeypatch.setattr(manager, "_persist_intent", persist)
    result = asyncio.run(manager.reprotect(**{
        "coin": "LINK", "direction": "long", "current_contracts": 0.4,
        "desired_stop_price": 8.8, "take_profit": 9.462,
            "client_order_id": "ENTRY-1", "paper": False, "tp_owner": EXCHANGE_FULL,
    }))

    assert result.reason == REPROTECTION_CONFIRMED
    assert events[:2] == [
        ("PROTECTION_INTENT_RESERVED", False),
        ("PROTECTION_SUBMISSION_ATTEMPTED", True),
    ]
    assert client.call_order.index("place") > 0


def test_intent_persistence_failure_blocks_post(monkeypatch):
    client = ReprotectClient([protection(algo_id="OLD", stop="8.742")])
    manager = ProtectionManager(client)
    monkeypatch.setattr(manager, "_persist_intent", lambda **kwargs: False)

    result = asyncio.run(manager.reprotect(
        coin="LINK", direction="long", current_contracts=0.4,
        desired_stop_price=8.8, take_profit=9.462, paper=False, tp_owner=EXCHANGE_FULL,
    ))

    assert result.reason == REPROTECTION_FAILED
    assert client.place_calls == []
    assert client.cancel_calls == []


def test_ambiguous_submission_is_not_replayed(monkeypatch):
    client = ReprotectClient([protection(algo_id="OLD", stop="8.742")], fail_place=True)
    manager = ProtectionManager(client)
    stored = {}

    def persist(**kwargs):
        stored.update(kwargs["intent"])
        stored["lifecycle"] = kwargs["lifecycle"]
        stored["protection_lifecycle"] = kwargs["lifecycle"]
        stored["mutation_attempted"] = kwargs["mutation_attempted"]
        stored["protection_intent"] = dict(kwargs["intent"])
        return True

    monkeypatch.setattr(manager, "_persist_intent", persist)
    monkeypatch.setattr(manager, "_registry_row", lambda coin: stored)
    first = asyncio.run(manager.reprotect(
        coin="LINK", direction="long", current_contracts=0.4,
        desired_stop_price=8.8, take_profit=9.462, paper=False, tp_owner=EXCHANGE_FULL,
        position_lifecycle_id="legacy-test-lifecycle",
    ))
    client.fail_place = False
    second = asyncio.run(manager.reprotect(
        coin="LINK", direction="long", current_contracts=0.4,
        desired_stop_price=8.8, take_profit=9.462, paper=False, tp_owner=EXCHANGE_FULL,
        position_lifecycle_id="legacy-test-lifecycle",
    ))

    assert first.intent_lifecycle == PROTECTION_SUBMIT_UNCERTAIN
    assert second.intent_lifecycle == PROTECTION_CONFIRMED_NOT_FOUND
    assert len(client.place_calls) == 1


def test_cleanup_failure_is_durable_and_retry_does_not_post_again(monkeypatch):
    client = ReprotectClient([protection(algo_id="OLD", stop="8.742")], fail_cancel=True)
    manager = ProtectionManager(client)
    stored = {}

    def persist(**kwargs):
        stored.update(kwargs["intent"])
        stored["lifecycle"] = kwargs["lifecycle"]
        stored["protection_lifecycle"] = kwargs["lifecycle"]
        stored["mutation_attempted"] = kwargs["mutation_attempted"]
        stored["prior_algo_ids"] = kwargs["intent"].get("prior_algo_ids", [])
        stored["protection_intent"] = dict(kwargs["intent"])
        return True

    monkeypatch.setattr(manager, "_persist_intent", persist)
    monkeypatch.setattr(manager, "_registry_row", lambda coin: stored)
    first = asyncio.run(manager.reprotect(
        coin="LINK", direction="long", current_contracts=0.4,
        desired_stop_price=8.8, take_profit=9.462, paper=False, tp_owner=EXCHANGE_FULL,
        position_lifecycle_id="legacy-test-lifecycle",
    ))
    assert first.reason == PROTECTION_CONFIRMED_CLEANUP_PENDING
    assert len(client.place_calls) == 1

    client.fail_cancel = False
    second = asyncio.run(manager.reprotect(
        coin="LINK", direction="long", current_contracts=0.4,
        desired_stop_price=8.8, take_profit=9.462, paper=False, tp_owner=EXCHANGE_FULL,
        position_lifecycle_id="legacy-test-lifecycle",
    ))
    assert second.intent_lifecycle == PROTECTION_CONFIRMED_CLEANUP_PENDING
    assert len(client.place_calls) == 1
    assert len(client.cancel_calls) == 1


def test_zero_or_unknown_position_blocks_protection_post():
    class ClosedClient(ReprotectClient):
        async def get_positions(self):
            return []

    client = ClosedClient([protection(algo_id="OLD", stop="8.742")])
    result = reprotect(client)
    assert result.reason == REPROTECTION_UNKNOWN
    assert client.place_calls == []

    class UnknownClient(ReprotectClient):
        async def get_positions(self):
            raise TimeoutError("position timeout")

    client = UnknownClient([protection(algo_id="OLD", stop="8.742")])
    result = reprotect(client)
    assert result.reason == REPROTECTION_UNKNOWN
    assert client.place_calls == []

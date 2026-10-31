import asyncio
import time

import okx_client
import pytest

from instrument_registry import InstrumentMetadata
from okx_client_ids import validate_client_id
from protection_manager import ProtectionManager
from tp_ownership import EXCHANGE_FULL


def oco(
    *,
    inst_id="LINK-USDT-SWAP",
    pos_side="long",
    side="sell",
    size="0.4",
    stop="8.742",
    take="9.462",
    state="live",
    reduce_only="true",
    algo_id="A1",
    algo_client_id="",
    close_fraction="",
    ord_type="oco",
):
    return {
        "algoId": algo_id,
        "algoClOrdId": algo_client_id,
        "instId": inst_id,
        "ordType": ord_type,
        "posSide": pos_side,
        "side": side,
        "sz": size,
        "closeFraction": close_fraction,
        "state": state,
        "reduceOnly": reduce_only,
        "slTriggerPx": stop,
        "tpTriggerPx": take,
    }


class ProtectionClient:
    def __init__(self, orders=None, *, responses=None):
        self.orders = list(orders or [])
        self.responses = list(responses) if responses is not None else None
        self.place_calls = []
        self.cancel_calls = []
        self.close_calls = []
        self.lookup_calls = 0
        self.instrument_metadata = InstrumentMetadata(
            inst_id="LINK-USDT-SWAP",
            inst_type="SWAP",
            settle_ccy="USDT",
            ct_val=1.0,
            ct_val_ccy="USDT",
            ct_type="linear",
            lot_sz=0.1,
            min_sz=0.1,
            tick_sz=0.001,
            state="live",
            refreshed_at=time.time(),
        )

    async def list_live_protection_orders(self, coin, *, pos_side=None):
        self.lookup_calls += 1
        if self.responses:
            return self.responses.pop(0)
        return {"ok": True, "orders": list(self.orders), "errors": [], "queried_types": ["oco", "conditional"]}

    async def place_protective_order(self, **kwargs):
        self.place_calls.append(kwargs)
        self.orders.append(oco(
            algo_id="NEW",
            size=str(kwargs["contracts"]),
            stop=str(kwargs["stop_price"]),
            take=str(kwargs.get("take_profit", 0.0) or 0.0) if kwargs.get("take_profit", 0.0) else "0",
            ord_type="oco" if kwargs.get("take_profit", 0.0) else "conditional",
        ))
        return {"code": "0", "data": [{"algoId": "NEW"}]}

    async def cancel_protective_orders(self, orders):
        self.cancel_calls.append(list(orders))
        ids = {str(row.get("algoId", "")) for row in orders}
        self.orders[:] = [row for row in self.orders if str(row.get("algoId", "")) not in ids]
        return {
            "code": "0",
            "data": [{"sCode": "0", "algoId": str(row.get("algoId", ""))} for row in orders],
        }

    async def close_futures_position(self, *args, **kwargs):
        self.close_calls.append((args, kwargs))
        return {"code": "0"}


def ensure(client, *, contracts=0.4, stop=8.742, take=9.462, tp_owner=EXCHANGE_FULL):
    return asyncio.run(
        ProtectionManager(client).ensure(
            coin="LINK",
            direction="long",
            contracts=contracts,
            stop_price=stop,
            take_profit=take,
            client_order_id="OXYE-entry",
            paper=False,
            tp_owner=tp_owner,
        )
    )


def test_empty_owner_strips_exchange_tp_but_preserves_stop_protection():
    client = ProtectionClient()

    result = ensure(client, tp_owner="")

    assert result.protected
    assert len(client.place_calls) == 1
    assert client.place_calls[0]["take_profit"] == 0.0
    assert client.place_calls[0]["stop_price"] == pytest.approx(8.742)


def test_invalid_owner_strips_exchange_tp_but_preserves_stop_protection():
    client = ProtectionClient()

    result = ensure(client, tp_owner="UNKNOWN_OWNER")

    assert result.protected
    assert len(client.place_calls) == 1
    assert client.place_calls[0]["take_profit"] == 0.0
    assert client.place_calls[0]["stop_price"] == pytest.approx(8.742)


def test_unresolved_owner_preserves_existing_profit_protection_without_mutation():
    client = ProtectionClient([oco(algo_id="EXISTING-TP")])

    result = ensure(client, tp_owner="UNKNOWN_OWNER")

    assert result.protected
    assert result.reason == "tp_owner_unresolved_preserved"
    assert client.place_calls == []
    assert client.cancel_calls == []


def test_entry_attached_oco_without_client_id_is_adopted():
    client = ProtectionClient([oco(algo_id="ENTRY-OCO")])

    result = ensure(client)

    assert result.protected
    assert result.reason == "existing_exchange_protection"
    assert result.sl_algo_id == "ENTRY-OCO"
    assert client.place_calls == []
    assert client.cancel_calls == []


def test_matching_conditional_protection_is_adopted():
    client = ProtectionClient([oco(ord_type="conditional", algo_id="COND-1")])

    result = ensure(client)

    assert result.protected
    assert client.place_calls == []


def test_no_protection_submits_exactly_one_order():
    client = ProtectionClient()

    result = ensure(client)

    assert result.protected
    assert result.reason == "exchange_protection_created"
    assert len(client.place_calls) == 1
    assert client.place_calls[0]["contracts"] == pytest.approx(0.4)
    assert client.lookup_calls == 4


def test_tick_equivalent_link_prices_are_adopted_without_replacement():
    client = ProtectionClient([oco(stop="8.742", take="9.462")])

    result = ensure(client, stop=8.7417, take=9.461345)

    assert result.protected
    assert result.reason == "existing_exchange_protection"
    assert client.place_calls == []


def test_one_material_tick_difference_is_not_equivalent():
    client = ProtectionClient([oco(stop="8.742", take="9.462")])

    result = ensure(client, stop=8.743, take=9.462)

    assert result.protected
    assert result.reason == "exchange_protection_created"
    assert len(client.place_calls) == 1
    assert len(client.cancel_calls) == 1
    assert validate_client_id(client.place_calls[0]["client_order_id"], field="algoClOrdId")


def test_lot_normalized_quantity_is_adopted():
    client = ProtectionClient([oco(size="0.4000")])

    result = ensure(client, contracts=0.4)

    assert result.protected
    assert client.place_calls == []


def test_partial_lot_quantity_is_not_adopted_as_full_position():
    client = ProtectionClient([oco(size="0.3")])

    result = ensure(client, contracts=0.4)

    assert result.protected
    assert len(client.place_calls) == 1


def test_two_equivalent_ocos_are_diagnosed_without_third_submission():
    client = ProtectionClient([
        oco(algo_id="ENTRY-OCO"),
        oco(algo_id="MANAGER-OCO", algo_client_id="OXYE-entryP"),
    ])

    result = ensure(client)

    assert result.protected
    assert result.reason == "existing_exchange_protection_duplicate"
    assert result.tp_algo_ids == ["ENTRY-OCO", "MANAGER-OCO"]
    assert client.place_calls == []
    assert client.cancel_calls == []


def test_repeated_ensure_calls_are_idempotent():
    client = ProtectionClient([oco(algo_id="ENTRY-OCO")])
    manager = ProtectionManager(client)

    first = asyncio.run(manager.ensure(coin="LINK", direction="long", contracts=0.4, stop_price=8.742, take_profit=9.462, paper=False))
    second = asyncio.run(manager.ensure(coin="LINK", direction="long", contracts=0.4, stop_price=8.742, take_profit=9.462, paper=False))

    assert first.protected and second.protected
    assert len(client.place_calls) == 0


def test_existing_oco_is_not_recreated_after_manager_restart():
    client = ProtectionClient([oco(algo_id="ENTRY-OCO")])

    assert ensure(client).protected
    assert ensure(client).protected
    assert client.place_calls == []


@pytest.mark.parametrize(
    "row",
    [
        oco(state="canceled"),
        oco(inst_id="OTHER-USDT-SWAP"),
        oco(pos_side="short", side="buy"),
        oco(reduce_only="false"),
        oco(stop="8.700"),
        oco(take="9.500"),
    ],
)
def test_non_equivalent_or_inactive_protection_is_not_accepted(row):
    client = ProtectionClient([row])

    result = ensure(client)

    assert result.protected
    assert len(client.place_calls) == 1


def test_full_close_fraction_representation_matches_remaining_quantity():
    client = ProtectionClient([oco(size="0", close_fraction="100", algo_id="CLOSE-ALL")])

    result = ensure(client, contracts=0.3)

    assert result.protected
    assert client.place_calls == []


def test_non_full_close_fraction_is_not_accepted():
    client = ProtectionClient([oco(size="0", close_fraction="50", algo_id="PARTIAL-CLOSE")])

    result = ensure(client, contracts=0.4)

    assert result.protected
    assert len(client.place_calls) == 1


def test_partial_position_does_not_create_full_size_duplicate():
    client = ProtectionClient([oco(size="0.3", algo_id="PARTIAL")])

    result = ensure(client, contracts=0.3)

    assert result.protected
    assert client.place_calls == []


@pytest.mark.parametrize(
    "responses",
    [
        [{"ok": False, "orders": [], "errors": ["oco:Timeout"], "queried_types": ["oco", "conditional"]}],
        [{"ok": False, "orders": [], "errors": ["conditional:Timeout"], "queried_types": ["oco", "conditional"]}],
        [{"ok": False, "orders": [], "errors": ["oco:Timeout", "conditional:Timeout"], "queried_types": ["oco", "conditional"]}],
    ],
)
def test_any_protection_query_failure_is_unknown_and_never_closes_or_creates(responses):
    client = ProtectionClient(responses=responses)

    result = ensure(client)

    assert not result.protected
    assert result.reason == "protection_state_unknown"
    assert result.protection_state == "PROTECTION_STATE_UNKNOWN"
    assert client.place_calls == []
    assert client.close_calls == []


def test_post_fill_visibility_race_adopts_protection_on_confirmation_read():
    client = ProtectionClient(responses=[
        {"ok": True, "orders": [], "errors": [], "queried_types": ["oco", "conditional"]},
        {"ok": True, "orders": [oco(algo_id="ENTRY-OCO")], "errors": [], "queried_types": ["oco", "conditional"]},
    ])

    result = ensure(client)

    assert result.protected
    assert result.reason == "existing_exchange_protection"
    assert client.lookup_calls == 2
    assert client.place_calls == []


def test_visibility_confirmation_failure_is_unknown_and_does_not_create():
    client = ProtectionClient(responses=[
        {"ok": True, "orders": [], "errors": [], "queried_types": ["oco", "conditional"]},
        {"ok": False, "orders": [], "errors": ["conditional:Timeout"], "queried_types": ["oco", "conditional"]},
    ])

    result = ensure(client)

    assert not result.protected
    assert result.reason == "protection_state_unknown"
    assert client.place_calls == []
    assert client.close_calls == []


def test_discovery_exception_is_unknown_and_does_not_emergency_close():
    class BrokenProtectionClient(ProtectionClient):
        async def list_live_protection_orders(self, coin, *, pos_side=None):
            raise TimeoutError("protection read timed out")

    client = BrokenProtectionClient()

    result = ensure(client)

    assert not result.protected
    assert result.reason == "protection_state_unknown"
    assert result.protection_state == "PROTECTION_STATE_UNKNOWN"
    assert client.place_calls == []
    assert client.close_calls == []


def test_canonical_missing_protection_is_created_only_after_two_empty_reads():
    client = ProtectionClient(responses=[
        {"ok": True, "orders": [], "errors": [], "queried_types": ["oco", "conditional"]},
        {"ok": True, "orders": [], "errors": [], "queried_types": ["oco", "conditional"]},
    ])

    result = ensure(client)

    assert result.protected
    assert result.reason == "exchange_protection_created"
    assert client.lookup_calls == 4
    assert len(client.place_calls) == 1


@pytest.mark.parametrize(
    "failed_type",
    ["oco", "conditional", "both"],
)
def test_okx_canonical_helper_keeps_partial_or_total_feed_failure_unknown(monkeypatch, failed_type):
    monkeypatch.setattr(okx_client, "PAPER_TRADING", False)
    monkeypatch.setattr(okx_client, "OKX_API_KEY", "fixture-key")
    client = okx_client.OKXClient()

    async def request(method, path, *, params=None, data=None, priority=None):
        if failed_type == "both" or params["ordType"] == failed_type:
            return {"code": "50001", "msg": "fixture failure", "data": []}
        return {"code": "0", "data": [oco()]}

    monkeypatch.setattr(client, "_request", request)
    result = asyncio.run(client.list_live_protection_orders("LINK", pos_side="long"))

    assert result["ok"] is False
    assert result["errors"]


def test_canonical_okx_view_queries_oco_and_conditional_without_entry_orders(monkeypatch):
    monkeypatch.setattr(okx_client, "PAPER_TRADING", False)
    monkeypatch.setattr(okx_client, "OKX_API_KEY", "fixture-key")
    client = okx_client.OKXClient()
    calls = []

    async def request(method, path, *, params=None, data=None, priority=None):
        calls.append(dict(params or {}))
        if params["ordType"] == "oco":
            return {"code": "0", "data": [oco(algo_id="OCO-1")]}
        return {"code": "0", "data": [oco(ord_type="conditional", algo_id="COND-1")]}

    monkeypatch.setattr(client, "_request", request)
    result = asyncio.run(client.list_live_protection_orders("LINK", pos_side="long"))

    assert result["ok"]
    assert [params["ordType"] for params in calls] == ["oco", "conditional"]
    assert {row["algoId"] for row in result["orders"]} == {"OCO-1", "COND-1"}
    assert all(row["instId"] == "LINK-USDT-SWAP" for row in result["orders"])


def test_protection_path_never_replays_entry_order():
    client = ProtectionClient([oco(algo_id="ENTRY-OCO")])

    ensure(client)

    assert client.place_calls == []
    assert client.cancel_calls == []
    assert client.close_calls == []

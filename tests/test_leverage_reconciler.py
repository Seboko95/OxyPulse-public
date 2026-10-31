import asyncio

from leverage_reconciler import apply, inspect


class Client:
    def __init__(self, leverage=50):
        self.leverage = leverage
        self.set_calls = []

    async def get_leverage_info(self, coin, margin_mode="isolated"):
        return [
            {"instId": f"{coin}-USDT-SWAP", "posSide": "long", "lever": str(self.leverage)},
            {"instId": f"{coin}-USDT-SWAP", "posSide": "short", "lever": str(self.leverage)},
        ]

    async def set_leverage(self, coin, leverage, pos_side):
        self.set_calls.append((coin, leverage, pos_side))
        self.leverage = leverage
        return {"code": "0"}

    async def get_positions(self):
        return []


def test_inspection_is_read_only_and_detects_mismatch(monkeypatch):
    client = Client(50)
    plan = asyncio.run(inspect(client, ["BTC"]))
    assert not plan["ok"]
    assert not plan["mutation_performed"]
    assert client.set_calls == []


def test_apply_requires_explicit_authorization():
    client = Client(50)
    plan = asyncio.run(inspect(client, ["BTC"]))
    result = asyncio.run(apply(client, plan, authorized=False))
    assert not result["ok"]
    assert client.set_calls == []


def test_authorized_apply_reconciles_both_position_sides(tmp_path):
    client = Client(50)
    plan = asyncio.run(inspect(client, ["BTC"]))
    result = asyncio.run(apply(client, plan, authorized=True, audit_path=tmp_path / "audit.jsonl"))
    assert result["ok"]
    assert len(client.set_calls) == 2
    assert (tmp_path / "audit.jsonl").exists()


def test_okx_rejection_is_reported(tmp_path):
    class Rejected(Client):
        async def set_leverage(self, *args, **kwargs):
            return {"code": "51000"}
    client = Rejected(50)
    result = asyncio.run(apply(client, asyncio.run(inspect(client, ["BTC"])), authorized=True, audit_path=tmp_path / "a.jsonl"))
    assert not result["ok"]
    assert all("okx_rejected" in row["reason"] for row in result["results"])


def test_readback_mismatch_is_reported(tmp_path):
    class NoChange(Client):
        async def set_leverage(self, coin, leverage, pos_side):
            self.set_calls.append((coin, leverage, pos_side))
            return {"code": "0"}
    client = NoChange(50)
    result = asyncio.run(apply(client, asyncio.run(inspect(client, ["BTC"])), authorized=True, audit_path=tmp_path / "a.jsonl"))
    assert not result["ok"]
    assert all(row["reason"] == "readback_mismatch" for row in result["results"])

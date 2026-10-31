import time

from controlled_lifecycle import ControlledLifecycleGate


def test_one_shot_gate_claims_exactly_one_intent(tmp_path):
    gate = ControlledLifecycleGate(tmp_path / "gate.json")
    gate.authorize(
        authorization_id="AUTH-1",
        expires_in_seconds=60,
        account_snapshot={"equity": 11.0},
    )
    assert gate.can_auto_arm() is True
    assert gate.claim("INTENT-1", {"coin": "BTC"}) == (True, "controlled_lifecycle_claimed")
    assert gate.claim("INTENT-1", {"coin": "BTC"}) == (True, "existing_controlled_intent")
    accepted, reason = gate.claim("INTENT-2", {"coin": "ETH"})
    assert accepted is False
    assert reason == "controlled_lifecycle_claimed"
    assert gate.snapshot()["submission_attempts"] == 1


def test_expired_gate_cannot_claim(tmp_path):
    gate = ControlledLifecycleGate(tmp_path / "gate.json")
    gate.authorize(
        authorization_id="AUTH-2",
        expires_in_seconds=60,
        account_snapshot={},
    )
    state = gate._load()
    state["expires_at"] = time.time() - 1
    gate._save(state)
    assert gate.can_auto_arm() is False
    accepted, reason = gate.claim("INTENT-1", {})
    assert accepted is False
    assert reason == "controlled_lifecycle_expired"


def test_completed_gate_remains_closed_to_second_trade(tmp_path):
    gate = ControlledLifecycleGate(tmp_path / "gate.json")
    gate.authorize(authorization_id="AUTH-3", expires_in_seconds=60, account_snapshot={})
    assert gate.claim("INTENT-1", {})[0] is True
    gate.mark_submission(intent_id="INTENT-1", order_id="123", state="SUBMITTED")
    gate.complete(reason="exchange_flat", evidence={"positions": 0})
    assert gate.enforced() is True
    assert gate.claim("INTENT-2", {}) == (False, "controlled_lifecycle_completed")

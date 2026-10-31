"""Floor-preserve trade-flow tests for capital_allocator + placement minimum."""
from __future__ import annotations

import pytest

import capital_allocator
from instrument_registry import InstrumentMetadata
import time


def instrument(**overrides):
    values = {
        "inst_id": "TEST-USDT-SWAP",
        "inst_type": "SWAP",
        "settle_ccy": "USDT",
        "ct_val": 0.01,
        "ct_val_ccy": "TEST",
        "ct_type": "linear",
        "lot_sz": 1.0,
        "min_sz": 1.0,
        "tick_sz": 0.1,
        "state": "live",
        "max_mkt_sz": 1000.0,
        "max_lmt_sz": 1000.0,
        "max_leverage": 20.0,
        "refreshed_at": time.time(),
        "source_hash": "abc",
        "raw": {},
    }
    values.update(overrides)
    return InstrumentMetadata(**values)


def _settings(monkeypatch, floor=6.35, cap_pct=100.0, risk_cap=12.0):
    values = {
        "adaptive_capital_allocator_enabled": True,
        "adaptive_min_economic_notional_usdt": 1.0,
        "position_size_cap_usdt": 10_000.0,
        "position_size_cap_pct_balance": cap_pct,
        "leverage_default": 10,
        "rule_357_enabled": True,
        "rule_357_max_open_risk_pct": risk_cap,
        "min_sl_dist_pct": 1.0,
        "live_taker_fee_rate": 0.0005,
        "deployment_mode": "Guarded",
    }
    monkeypatch.setattr(capital_allocator._s, "get_val", lambda key, default=None: values.get(key, default))
    monkeypatch.setattr(capital_allocator._s, "get_effective_min_order_usdt", lambda balance: floor)
    monkeypatch.setattr(capital_allocator.micro_sizing, "usable_margin_usdt", lambda balance, settings: balance * 0.9)


def test_floor_preserve_when_safe(monkeypatch):
    """Request below floor; floor fits cap/margin/risk → approve at floor."""
    # Floor $10 with $1 contract notional → exact 10 contracts after lot conversion.
    _settings(monkeypatch, floor=10.0, cap_pct=100.0, risk_cap=12.0)
    decision = capital_allocator.allocate_entry(
        balance_usdt=50.0,
        requested_size_usdt=5.0,
        entry_price=100.0,
        stop_price=99.0,
        leverage=10,
        instrument_metadata=instrument(ct_val=0.01, min_sz=1.0, lot_sz=1.0),
        available_margin_usdt=45.0,
    )
    assert decision.approved
    assert decision.final_size_usdt == pytest.approx(10.0, abs=0.02)
    assert any("floor_preserve" in n for n in decision.notes)
    assert decision.min_exec_usdt == pytest.approx(10.0)


def test_floor_exceeds_position_cap_rejected(monkeypatch):
    """Floor above 40% notional cap → rejected_min_notional, no preserve."""
    _settings(monkeypatch, floor=6.35, cap_pct=40.0, risk_cap=12.0)
    decision = capital_allocator.allocate_entry(
        balance_usdt=11.0,
        requested_size_usdt=4.0,
        entry_price=100.0,
        stop_price=99.0,
        leverage=10,
        instrument_metadata=instrument(ct_val=0.01, min_sz=1.0, lot_sz=1.0),
        available_margin_usdt=9.69,
    )
    assert not decision.approved
    assert "below_min_exec" in decision.reason
    assert "position_or_instrument_cap" in decision.reason or decision.final_size_usdt < 6.35


def test_floor_exceeds_risk_headroom_rejected(monkeypatch):
    """Floor would breach rule_357 → reject, never upsize through risk."""
    _settings(monkeypatch, floor=50.0, cap_pct=100.0, risk_cap=1.0)
    # 1% open risk budget at $100 equity with 1% SL ⇒ max size ≈ $100
    # Wait: trade_risk = size * sl_dist / balance; sl=1%, risk_cap=1%
    # size_max = 1 * 100 / 1 = 100. Floor 50 fits...
    # Use tiny remaining risk via open positions.
    open_pos = {
        "OTHER": {
            "size_usdt": 90.0,
            "entry": 100.0,
            "stop_loss": 99.0,
            "leverage": 10,
        }
    }
    # open risk ≈ 90 * 1 / 100 = 0.9%; remaining 0.1%; size_max = 0.1 * 100 / 1 = 10
    decision = capital_allocator.allocate_entry(
        balance_usdt=100.0,
        requested_size_usdt=5.0,
        entry_price=100.0,
        stop_price=99.0,
        leverage=10,
        open_positions=open_pos,
        instrument_metadata=instrument(ct_val=0.01, min_sz=1.0, lot_sz=1.0),
        available_margin_usdt=90.0,
    )
    assert not decision.approved
    assert "below_min_exec" in decision.reason or "open_risk" in decision.reason
    assert decision.final_size_usdt < 50.0 or not decision.approved


def test_floor_preserve_survives_lot_conversion(monkeypatch):
    """Preserved floor remains executable after lot round-down."""
    _settings(monkeypatch, floor=10.0, cap_pct=100.0, risk_cap=12.0)
    # ct_val=0.01, price=100 → contract notional=1.0; min_sz=1 → min 1 contract = $1
    # floor $10 → 10 contracts exactly
    decision = capital_allocator.allocate_entry(
        balance_usdt=100.0,
        requested_size_usdt=7.0,
        entry_price=100.0,
        stop_price=99.0,
        leverage=10,
        instrument_metadata=instrument(ct_val=0.01, min_sz=1.0, lot_sz=1.0),
        available_margin_usdt=90.0,
    )
    assert decision.approved
    assert decision.final_size_usdt + 0.01 >= 10.0
    assert decision.approved_contracts >= 10.0


def test_lot_geometry_blocks_unsafe_round_up(monkeypatch):
    """Lot geometry cannot reach floor without round-up → reject."""
    _settings(monkeypatch, floor=25.0, cap_pct=100.0, risk_cap=50.0)
    # Base-ccy contract: notional/contract = ct_val * price = 1 * 20 = $20
    # Preserve to $25 → 1.25 contracts → round down 1 → $20 < $25 → reject (no round-up)
    decision = capital_allocator.allocate_entry(
        balance_usdt=200.0,
        requested_size_usdt=15.0,
        entry_price=20.0,
        stop_price=19.8,
        leverage=10,
        instrument_metadata=instrument(ct_val=1.0, min_sz=1.0, lot_sz=1.0, ct_val_ccy="COIN"),
        available_margin_usdt=180.0,
    )
    assert not decision.approved
    assert "below_min_exec" in decision.reason


def test_above_floor_request_unchanged(monkeypatch):
    """Already executable requested size is not altered by preserve logic."""
    _settings(monkeypatch, floor=6.35, cap_pct=100.0, risk_cap=12.0)
    decision = capital_allocator.allocate_entry(
        balance_usdt=100.0,
        requested_size_usdt=20.0,
        entry_price=100.0,
        stop_price=99.0,
        leverage=10,
        instrument_metadata=instrument(ct_val=0.01, min_sz=1.0, lot_sz=1.0),
        available_margin_usdt=90.0,
    )
    assert decision.approved
    assert decision.final_size_usdt == pytest.approx(20.0)
    assert not any("floor_preserve" in n for n in decision.notes)

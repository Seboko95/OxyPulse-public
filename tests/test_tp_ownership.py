import asyncio
import time

import pytest

import exit_manager as em
from tg_panels import panel_positions_truth_text
from tp_ownership import (
    EXCHANGE_FULL,
    LOCAL_STAGED,
    NO_PROFIT_TP,
    UNRESOLVED,
    derive_tp_owner,
    effective_exchange_take_profit,
    normalize_persisted_owner,
    owner_flags,
    resolve_legacy_owner,
)


@pytest.mark.parametrize(
    ("mode", "partials", "expected"),
    [
        ("off", True, NO_PROFIT_TP),
        ("off", False, NO_PROFIT_TP),
        ("reference_only", True, LOCAL_STAGED),
        ("reference_only", False, NO_PROFIT_TP),
        ("exchange_tp", True, EXCHANGE_FULL),
        ("exchange_tp", False, EXCHANGE_FULL),
        ("hybrid", True, LOCAL_STAGED),
        ("hybrid", False, EXCHANGE_FULL),
    ],
)
def test_new_position_owner_matrix(mode, partials, expected):
    assert derive_tp_owner(mode, partial_enabled=partials) == expected


def test_hybrid_without_staged_management_uses_exchange_full_owner():
    assert derive_tp_owner("hybrid", partial_enabled=False) == EXCHANGE_FULL


@pytest.mark.parametrize(
    ("details", "expected"),
    [
        ({"exchange_tp_algo_id": "TP-1"}, EXCHANGE_FULL),
        ({"local_tp1_enabled": True, "exchange_protection_status": "STOP_CONFIRMED"}, LOCAL_STAGED),
        ({"tp1_done": True, "partial_exit_state": {"tp1": {"lifecycle": "PARTIAL_FILLED"}}}, LOCAL_STAGED),
        ({"exchange_protection_status": "STOP_CONFIRMED", "local_tp2_enabled": True}, LOCAL_STAGED),
        ({"tp_mode_at_entry": "off"}, NO_PROFIT_TP),
        ({"exchange_protection_status": "PROTECTION_STATE_UNKNOWN"}, UNRESOLVED),
        ({"tp_mode_at_entry": "hybrid"}, UNRESOLVED),
    ],
)
def test_legacy_owner_resolution_uses_evidence(details, expected):
    assert resolve_legacy_owner(details, mode_at_entry=details.get("tp_mode_at_entry", "hybrid")) == expected


def test_nonempty_corrupt_owner_is_not_repaired():
    assert normalize_persisted_owner("UNKNOWN_OWNER") == UNRESOLVED
    plan = em.register(
        "CORRUPT",
        "long",
        100.0,
        100.0,
        1.0,
        98.0,
        102.0,
        105.0,
        tp_owner="UNKNOWN_OWNER",
        tp_mode_at_entry="hybrid",
    )
    try:
        assert plan.tp_owner == UNRESOLVED
        assert plan.local_tp1_enabled is False
        assert plan.local_tp2_enabled is False
    finally:
        em.remove("CORRUPT")


def test_legacy_hybrid_position_adopts_existing_exchange_owner():
    assert derive_tp_owner("hybrid", partial_enabled=True, legacy_position=True) == EXCHANGE_FULL


def test_owner_flags_disable_local_profit_closes_for_non_local_owners():
    assert owner_flags(LOCAL_STAGED, partial_enabled=True) == (True, True)
    assert owner_flags(EXCHANGE_FULL, partial_enabled=True) == (False, False)
    assert owner_flags(NO_PROFIT_TP, partial_enabled=True) == (False, False)


def test_only_exchange_full_can_attach_exchange_tp():
    assert effective_exchange_take_profit(LOCAL_STAGED, 105.0) == 0.0
    assert effective_exchange_take_profit(NO_PROFIT_TP, 105.0) == 0.0
    assert effective_exchange_take_profit(EXCHANGE_FULL, 105.0) == 105.0


def test_register_persists_owner_and_mode_fields():
    plan = em.register(
        "TPTEST",
        "long",
        100.0,
        100.0,
        1.0,
        98.0,
        102.0,
        105.0,
        tp_owner=LOCAL_STAGED,
        tp_mode_at_entry="hybrid",
        local_tp1_enabled=True,
        local_tp2_enabled=True,
    )
    try:
        assert plan.tp_owner == LOCAL_STAGED
        assert plan.tp_mode_at_entry == "hybrid"
        assert plan.local_tp1_enabled is True
        assert plan.local_tp2_enabled is True
        assert plan.ownership_timestamp
        assert plan.tp_owner_timestamp == plan.ownership_timestamp
        assert plan.partials_enabled_at_entry is True
    finally:
        em.remove("TPTEST")


def test_telegram_positions_show_current_mode_and_persisted_owner():
    text = panel_positions_truth_text({
        "configured_tp_mode": "off",
        "open_pnl_usdt": 0.0,
        "source_label": "PAPER",
        "age_sec": 0.0,
        "positions": {
            "BTC": {
                "side": "long",
                "age_sec": 0.0,
                "confidence": "HIGH",
                "requested_leverage": 15,
                "exchange_configured_leverage": 12,
                "position_effective_leverage": 12,
                "leverage_truth_status": "POSITION_EFFECTIVE",
                "registry": {
                    "tp_owner": EXCHANGE_FULL,
                    "tp_mode_at_entry": "exchange_tp",
                    "take_profit": 105.0,
                    "stop_loss": 98.0,
                },
            },
        },
    })
    assert "TP mode current: `OFF`" in text
    assert "TP mode-at-entry `EXCHANGE_TP` owner `EXCHANGE_FULL`" in text
    assert "Leverage requested `15x` configured `12x` exchange/effective `12x`" in text


def test_telegram_positions_show_unknown_effective_leverage_without_fallback():
    text = panel_positions_truth_text({
        "positions": {
            "XRP": {
                "side": "short",
                "requested_leverage": 15,
                "exchange_configured_leverage": 12,
                "position_effective_leverage": None,
                "leverage": 15,
                "leverage_truth_status": "LEVERAGE_UNKNOWN",
                "registry": {},
            },
        },
    })
    assert "requested `15x` configured `12x` exchange/effective `UNKNOWN`" in text


@pytest.mark.parametrize("owner", [EXCHANGE_FULL, NO_PROFIT_TP, UNRESOLVED])
def test_non_local_owner_never_calls_partial_profit_close(monkeypatch, owner):
    plan = em.ExitPlan(
        coin="TPTEST",
        direction="long",
        engine_name="TEST",
        entry_price=100.0,
        size_usdt=100.0,
        atr_val=1.0,
        hard_sl=98.0,
        tp1_price=102.0,
        tp2_price=105.0,
        initial_size_usdt=100.0,
        leverage=10,
        tp_owner=owner,
        local_tp1_enabled=False,
        local_tp2_enabled=False,
    )
    calls = []

    async def fail_partial(*args, **kwargs):
        calls.append((args, kwargs))
        raise AssertionError("non-local TP owner submitted a local partial")

    monkeypatch.setattr(em, "_execute_partial", fail_partial)
    monkeypatch.setattr(em, "_maybe_positive_mfe_profit_guard", lambda *a, **k: None)
    monkeypatch.setattr(em, "_maybe_positive_profit_lock_enrichment", lambda *a, **k: None)
    monkeypatch.setattr(em, "_apply_stop_improvement", lambda *a, **k: None)
    monkeypatch.setattr(em, "_maybe_stale_exit", _false_async)
    monkeypatch.setattr(em, "_maybe_early_invalidation_exit", _false_async)
    em._s._current.update({
        "partial_exit_enabled": True,
        "secure_profit_mode_enabled": True,
        "breakeven_enabled": False,
        "profit_lock_enabled": False,
        "tsl_enabled": False,
        "tp1_trigger_pct": 1.0,
        "tp2_trigger_pct": 2.0,
    })
    try:
        assert asyncio.run(em._apply_milestone_profit_system("TPTEST", plan, 130.0, True)) is False
        assert calls == []
    finally:
        for key in ("partial_exit_enabled", "secure_profit_mode_enabled", "breakeven_enabled", "profit_lock_enabled", "tsl_enabled", "tp1_trigger_pct", "tp2_trigger_pct"):
            em._s._current.pop(key, None)


def test_non_local_owner_preserves_legacy_non_tp_safety_checks(monkeypatch):
    plan = em.ExitPlan(
        coin="TPTEST",
        direction="long",
        engine_name="TEST",
        entry_price=100.0,
        size_usdt=100.0,
        atr_val=1.0,
        hard_sl=98.0,
        tp1_price=102.0,
        tp2_price=105.0,
        initial_size_usdt=100.0,
        leverage=10,
        tp_owner=EXCHANGE_FULL,
        local_tp1_enabled=False,
        local_tp2_enabled=False,
        created_at=time.time() - 120.0,
    )
    calls = []

    async def stale(*args, **kwargs):
        calls.append("stale")
        return True

    async def early(*args, **kwargs):
        calls.append("early")
        return False

    monkeypatch.setattr(em, "latest_price", lambda _coin: 100.0)
    monkeypatch.setattr(em, "_paper_trading_enabled", lambda: False)
    monkeypatch.setattr(em, "_reprotect_if_needed", _false_async)
    monkeypatch.setattr(em, "_maybe_stale_exit", stale)
    monkeypatch.setattr(em, "_maybe_early_invalidation_exit", early)
    em._s._current.update({
        "secure_profit_mode_enabled": False,
        "profit_lock_enabled": False,
        "tsl_enabled": False,
    })
    em._plans["TPTEST"] = plan
    try:
        asyncio.run(em._check_plan("TPTEST"))
        assert calls == ["stale"]
    finally:
        em._plans.pop("TPTEST", None)
        for key in ("secure_profit_mode_enabled", "profit_lock_enabled", "tsl_enabled"):
            em._s._current.pop(key, None)


async def _false_async(*args, **kwargs):
    return False


def test_paper_tp1_only_belongs_to_local_staged(monkeypatch):
    plan = em.ExitPlan(
        coin="TPTEST",
        direction="long",
        engine_name="TEST",
        entry_price=100.0,
        size_usdt=100.0,
        atr_val=1.0,
        hard_sl=98.0,
        tp1_price=102.0,
        tp2_price=105.0,
        tp_owner=EXCHANGE_FULL,
        local_tp1_enabled=False,
        local_tp2_enabled=False,
    )
    paper_closes = []

    async def close(*args, **kwargs):
        paper_closes.append((args, kwargs))
        return True

    monkeypatch.setattr(em, "_do_paper_close", close)
    monkeypatch.setattr(em, "_update_tsl", _false_async)
    assert asyncio.run(em._check_paper_plan("TPTEST", plan, 102.0, True)) is False
    assert paper_closes == []


def test_paper_tp2_exchange_full_is_terminal_simulation_not_partial(monkeypatch):
    plan = em.ExitPlan(
        coin="TPTEST",
        direction="long",
        engine_name="TEST",
        entry_price=100.0,
        size_usdt=100.0,
        atr_val=1.0,
        hard_sl=98.0,
        tp1_price=102.0,
        tp2_price=105.0,
        tp_owner=EXCHANGE_FULL,
        local_tp1_enabled=False,
        local_tp2_enabled=False,
    )
    paper_closes = []

    async def close(*args, **kwargs):
        paper_closes.append((args, kwargs))
        return True

    monkeypatch.setattr(em, "_do_paper_close", close)
    monkeypatch.setattr(em, "_update_tsl", _false_async)
    assert asyncio.run(em._check_paper_plan("TPTEST", plan, 105.0, True)) is True
    assert paper_closes[0][0][2] == "tp2_hit"


@pytest.mark.parametrize(
    ("owner", "expected_tp"),
    [(LOCAL_STAGED, 0.0), (EXCHANGE_FULL, 105.0)],
)
def test_dynamic_reprotection_preserves_only_authorized_tp_owner(monkeypatch, owner, expected_tp):
    plan = em.ExitPlan(
        coin="TPTEST",
        direction="long",
        engine_name="TEST",
        entry_price=100.0,
        size_usdt=100.0,
        atr_val=1.0,
        hard_sl=98.0,
        tp1_price=102.0,
        tp2_price=105.0,
        desired_stop_price=99.0,
        exchange_confirmed_stop_price=98.0,
        tp_owner=owner,
    )
    seen = {}

    class Manager:
        async def reprotect(self, **kwargs):
            seen.update(kwargs)
            return type("Result", (), {
                "reason": "REPROTECTION_CONFIRMED",
                "error": "",
                "confirmed_stop_price": 99.0,
                "sl_algo_id": "SL",
                "tp_algo_ids": ["TP"] if expected_tp else [],
                "intent_lifecycle": "PROTECTION_CONFIRMED",
                "cleanup_pending": False,
            })()

    monkeypatch.setattr(em, "_paper_trading_enabled", lambda: False)
    monkeypatch.setattr(em, "_current_exchange_contracts", _one_contract_async)
    monkeypatch.setattr(em, "_persist_exit_state", lambda *args, **kwargs: None)
    old = em._protection_manager
    em._protection_manager = Manager()
    try:
        asyncio.run(em._reprotect_if_needed("TPTEST", plan))
    finally:
        em._protection_manager = old
    assert seen["take_profit"] == expected_tp
    assert seen["tp_owner"] == owner


async def _one_contract_async(*args, **kwargs):
    return 1.0

from __future__ import annotations

from pathlib import Path


def test_account_balance_truth_distinguishes_unfunded_and_unknown():
    import live_preflight

    zero = {
        "code": "0",
        "data": [{"totalEq": "0", "details": [{"ccy": "USDT", "availEq": "0"}]}],
    }
    equity, available, parsed_equity, parsed_available = live_preflight._account_balance_checks(zero)
    assert equity["ok"] is False and "UNFUNDED_ACCOUNT" in equity["detail"]
    assert available["ok"] is False and "UNFUNDED_ACCOUNT" in available["detail"]
    assert (parsed_equity, parsed_available) == (0.0, 0.0)

    for malformed in (
        {"code": "50000", "data": []},
        {"code": "0", "data": []},
        {"code": "0", "data": [{"totalEq": "not-a-number", "details": []}]},
        {"code": "0", "data": [{"totalEq": "-1", "details": [{"ccy": "USDT", "availEq": "-2"}]}]},
    ):
        equity, available, *_ = live_preflight._account_balance_checks(malformed)
        assert equity["ok"] is False
        assert available["ok"] is False

    invalid, invalid_available, *_ = live_preflight._account_balance_checks(
        {"code": "0", "data": [{"totalEq": "-1", "details": [{"ccy": "USDT", "availEq": "0"}]}]},
    )
    assert "ACCOUNT_BALANCE_INVALID" in invalid["detail"]
    assert "UNFUNDED_ACCOUNT" not in invalid_available["detail"]

    explicit_empty, explicit_empty_available, *_ = live_preflight._account_balance_checks(
        {"code": "0", "data": [{"totalEq": "0", "details": []}]},
    )
    assert "UNFUNDED_ACCOUNT" in explicit_empty["detail"]
    assert "UNFUNDED_ACCOUNT" in explicit_empty_available["detail"]
    ambiguous, ambiguous_available, *_ = live_preflight._account_balance_checks(
        {"code": "0", "data": [{"totalEq": "0"}]},
    )
    assert ambiguous["ok"] is False and "UNFUNDED_ACCOUNT" not in ambiguous["detail"]
    assert ambiguous_available["ok"] is False


def test_structural_readiness_can_be_ready_for_funding_without_live_capital(tmp_path, monkeypatch):
    import live_preflight

    monkeypatch.chdir(tmp_path)
    checks = {name: {"ok": True, "detail": "fixture"} for name in live_preflight.REQUIRED_CHECKS}
    checks["account_equity"] = {"ok": False, "detail": "UNFUNDED_ACCOUNT:equity_usdt=0"}
    checks["available_balance"] = {"ok": False, "detail": "UNFUNDED_ACCOUNT:available_usdt=0"}
    checks["capital_reservation"] = {"ok": False, "detail": "DEFERRED_STAGE2:positive funded reservation probe not run"}
    checks["reservation_release"] = {"ok": False, "detail": "DEFERRED_STAGE2:reservation release proof not run"}
    result = live_preflight._finalize_preflight_result(
        checks,
        paper=False,
        persist_state=False,
        invocation_source="fixture_structural_readiness",
        exchange_results={},
    )
    assert result["ok"] is False
    assert result["readiness"] == {
        "ready_for_funding": True,
        "structural_ready": True,
        "funding_status": "UNFUNDED_ACCOUNT",
        "live_arm_ready": False,
    }

    checks["account_equity"] = {"ok": False, "detail": "ACCOUNT_BALANCE_INVALID:negative"}
    checks["available_balance"] = {"ok": False, "detail": "ACCOUNT_BALANCE_INVALID:negative"}
    invalid_result = live_preflight._finalize_preflight_result(
        checks,
        paper=False,
        persist_state=False,
        invocation_source="fixture_invalid_funding",
        exchange_results={},
    )
    assert invalid_result["readiness"]["ready_for_funding"] is False
    assert invalid_result["readiness"]["funding_status"] == "ACCOUNT_BALANCE_INVALID"


def test_entry_payloads_carry_authoritative_position_mode():
    source = Path("okx_client.py").read_text(encoding="utf-8")
    router = Path("execution_router.py").read_text(encoding="utf-8")
    assert 'body["posSide"] = "net" if resolved_position_mode == "net_mode"' in source
    assert 'else ("long" if side == "buy" else "short")' in source
    assert 'position_mode=str(request.metadata.get("position_mode", "") or "")' in router
    assert "except ValueError as exc:" in source
    assert "quantity_conversion_failed:{exc}" in source
    limit_source = source[source.index("async def place_limit_order"):source.index("async def cancel_order")]
    assert "sz = 1" not in limit_source
    close_source = source[source.index("async def close_futures_position"):]
    assert "close_price_unavailable" in close_source
    assert "close_quantity_conversion_failed" in close_source
    assert "price = 1.0" not in close_source[:close_source.index("async def get_algo_orders")]
    assert "sz = 1" not in close_source[:close_source.index("async def get_algo_orders")]


def test_startup_keeps_structural_ready_for_funding_disarmed():
    source = Path("trading_engine.py").read_text(encoding="utf-8")
    assert '"ready_for_funding_unarmed"' in source
    assert "stage1_ready" in source
    assert "mutation_ready = not stage1_ready" in source


def test_positions_endpoint_failure_is_not_empty_exposure():
    source = Path("live_preflight.py").read_text(encoding="utf-8")
    assert "positions_truth_ok" in source
    assert '"positions_truth_unavailable"' in source


def test_protection_health_proves_router_emergency_callback():
    source = Path("live_preflight.py").read_text(encoding="utf-8")
    assert "router_for" in source
    assert 'getattr(router, "protection", None)' in source
    assert 'getattr(protection, "_emergency_close", None)' in source
    assert "telegram_operator_controls" in source
    assert 'getattr(telegram_controls, "emergency_close", None)' in source


def test_live_metadata_and_notional_paths_fail_closed_without_synthetic_values():
    client = Path("okx_client.py").read_text(encoding="utf-8")
    engine = Path("trading_engine.py").read_text(encoding="utf-8")
    assert "authoritative_contract_value_unavailable" in client
    assert "invalid_contract_cost" in client
    assert "execution_price_unavailable" in client
    fetch_source = client[client.index("async def fetch_contract_value"):client.index("async def get_contract_cost")]
    paper_block = fetch_source[fetch_source.index("if PAPER_TRADING:"):]
    assert "_ct_val_cache[coin] = fallback" not in paper_block
    assert "if not math.isfinite(float(ct_val)) or float(ct_val) <= 0" in engine
    estimate = engine[engine.index("async def _estimate_exchange_notional"):engine.index("async def sync_positions")]
    assert "ct_val = 1.0" not in estimate
    assert "return 0.0" in estimate


def test_pnl_and_telegram_preflight_are_not_tautological():
    source = Path("live_preflight.py").read_text(encoding="utf-8")
    assert "unresolved_statuses" in source
    assert "pnl_unresolved=" in source
    assert "emergency_ok" in source


def test_allocator_and_router_preflight_prove_non_mutating_boundaries():
    source = Path("live_preflight.py").read_text(encoding="utf-8")
    assert "allocator_health = reservation_book.health()" in source
    assert "reservation_book.snapshot()" not in source
    assert "DEFERRED_STAGE2:positive funded reservation probe not run" in source
    assert 'router = router_for(client)' in source
    assert 'getattr(router, "submit", None)' in source
    assert 'getattr(router, "_local_executable_check", None)' in source

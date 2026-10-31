from __future__ import annotations

import asyncio
import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path


def _checks(live_preflight, *, failed: str | None = None, detail: str = "safe detail") -> dict:
    return {
        name: {"ok": name != failed, "detail": detail if name == failed else "OK"}
        for name in live_preflight.REQUIRED_CHECKS
    }


def _live_preflight(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    import live_preflight

    monkeypatch.setattr(live_preflight, "REPORTS_DIR", tmp_path / "reports")
    return live_preflight


def test_diagnostic_artifacts_have_required_schema_unique_ids_and_atomic_latest(tmp_path, monkeypatch):
    live_preflight = _live_preflight(tmp_path, monkeypatch)

    first = live_preflight._finalize_preflight_result(
        _checks(live_preflight),
        paper=False,
        persist_state=False,
        invocation_source="test_first",
        exchange_results={"authentication": {"ok": True, "reason": "OK"}},
    )
    second = live_preflight._finalize_preflight_result(
        _checks(live_preflight, failed="authentication", detail="code=50113 token=do-not-persist"),
        paper=False,
        persist_state=False,
        invocation_source="test_second",
        exchange_results={"authentication": {"code": "50113", "msg": "token=do-not-persist", "data": []}},
    )

    assert first["attempt_id"] != second["attempt_id"]
    assert datetime.fromisoformat(first["generated_at"]).tzinfo == timezone.utc
    latest_path = tmp_path / "reports" / "live_preflight_latest.json"
    latest = json.loads(latest_path.read_text(encoding="utf-8"))
    assert latest["attempt_id"] == second["attempt_id"]
    assert latest["aggregate"] == {"passed": False, "reason": "one_or_more_required_checks_failed"}
    assert latest["invocation_source"] == "test_second"
    by_name = {row["name"]: row for row in latest["checks"]}
    assert by_name["authentication"] == {
        "name": "authentication",
        "required": True,
        "ok": False,
        "detail": "code=50113 token=<redacted>",
        "exchange": {
            "classification": "exchange_error",
            "code": "50113",
            "message": "token=<redacted>",
        },
    }
    assert (tmp_path / "reports" / "live_preflight_attempts" / f"{first['attempt_id']}.json").exists()
    assert (tmp_path / "reports" / "live_preflight_attempts" / f"{second['attempt_id']}.json").exists()
    assert not list((tmp_path / "reports").rglob("*.tmp"))


def test_diagnostics_persist_when_state_persistence_is_disabled(tmp_path, monkeypatch):
    live_preflight = _live_preflight(tmp_path, monkeypatch)

    class State:
        def record_preflight(self, *_args):
            raise AssertionError("persist_state=False must not write live state")

    monkeypatch.setattr(live_preflight, "live_state", State())
    result = live_preflight._finalize_preflight_result(
        _checks(live_preflight, failed="authentication"),
        paper=False,
        persist_state=False,
        invocation_source="telegram_live_preflight",
        exchange_results={},
    )

    assert result["ok"] is False
    assert result["diagnostics"]["attempt_id"] == result["attempt_id"]
    assert Path(result["diagnostics"]["artifacts"]["latest"]).exists()


def test_sanitization_excludes_credentials_private_identifiers_and_payloads(tmp_path, monkeypatch):
    live_preflight = _live_preflight(tmp_path, monkeypatch)
    variants = {
        "api_key=key-value": "key-value",
        "apiKey: camel-api-value": "camel-api-value",
        '{"apiKey":"json-api-value"}': "json-api-value",
        "secret secret-value": "secret-value",
        "passphrase=pass-value": "pass-value",
        "signature=sig-value": "sig-value",
        "Authorization: Bearer auth-value": "auth-value",
        "authHeader=header-value": "header-value",
        "telegram_token=telegram-value": "telegram-value",
        "telegramToken: camel-telegram-value": "camel-telegram-value",
        "account_id=account-value": "account-value",
        "accountId=camel-account-value": "camel-account-value",
        "acctId=acct-value": "acct-value",
        "order_id=order-value": "order-value",
        "orderId=camel-order-value": "camel-order-value",
        "ordId=ord-value": "ord-value",
        "clOrdId=client-order-value": "client-order-value",
        "clientOrderId=client-order-camel-value": "client-order-camel-value",
        "algo_id=algo-value": "algo-value",
        "algoId=camel-algo-value": "camel-algo-value",
        "fill_id=fill-value": "fill-value",
        "fillId=camel-fill-value": "camel-fill-value",
        "OK-ACCESS-KEY=access-key-value": "access-key-value",
        "OK-ACCESS-SIGN=access-sign-value": "access-sign-value",
        "OK-ACCESS-PASSPHRASE=access-pass-value": "access-pass-value",
        "registryIds=[registry-one,registry-two]": "registry-one",
        "positionIds=[position-one,position-two]": "position-one",
        "reservationIds=[reservation-one,reservation-two]": "reservation-one",
        "intentIds=[intent-one,intent-two]": "intent-one",
    }
    for raw, forbidden in variants.items():
        sanitized = live_preflight.sanitize_diagnostic_detail(raw)
        assert forbidden not in sanitized
        assert "<redacted>" in sanitized
    assert live_preflight.sanitize_diagnostic_detail({"ordId": "private-order"}) == "structured detail omitted"


def test_sanitization_redacts_real_local_exposure_ids_field(tmp_path, monkeypatch):
    live_preflight = _live_preflight(tmp_path, monkeypatch)
    sanitized = live_preflight.sanitize_diagnostic_detail(
        "count=2 symbols=BTC-USDT-SWAP,ETH-USDT-SWAP ids=registry-alpha,position-beta reason=LOCAL_EXPOSURE_UNKNOWN"
    )

    assert "registry-alpha" not in sanitized
    assert "position-beta" not in sanitized
    assert "ids=<redacted>" in sanitized
    assert "count=2" in sanitized
    assert "symbols=BTC-USDT-SWAP,ETH-USDT-SWAP" in sanitized
    assert "reason=LOCAL_EXPOSURE_UNKNOWN" in sanitized


def test_exchange_classification_distinguishes_empty_success_and_error(tmp_path, monkeypatch):
    live_preflight = _live_preflight(tmp_path, monkeypatch)

    assert live_preflight._safe_exchange_result({"code": "0", "data": []}) == {
        "classification": "empty_response", "code": "0",
    }
    assert live_preflight._safe_exchange_result({"code": "0", "data": [{"private": "ignored"}]}) == {
        "classification": "success", "code": "0",
    }
    assert live_preflight._safe_exchange_result({"code": "50011", "msg": "account_id=private"}) == {
        "classification": "exchange_error", "code": "50011", "message": "account_id=<redacted>",
    }


def test_diagnostic_write_failure_forces_fail_closed_result(tmp_path, monkeypatch):
    live_preflight = _live_preflight(tmp_path, monkeypatch)
    monkeypatch.setattr(live_preflight, "_persist_diagnostic_record", lambda _record: (_ for _ in ()).throw(OSError("token=private")))

    result = live_preflight._finalize_preflight_result(
        _checks(live_preflight),
        paper=False,
        persist_state=False,
        invocation_source="test_failure",
        exchange_results={},
    )

    assert result["ok"] is False
    assert result["reason"] == "diagnostics_persistence_failed"
    assert result["diagnostics"]["aggregate"] == {"passed": False, "reason": "diagnostics_persistence_failed"}
    assert "private" not in result["diagnostics_persistence_error"]


def test_partial_persistence_overwrites_history_with_fail_closed_aggregate(tmp_path, monkeypatch):
    live_preflight = _live_preflight(tmp_path, monkeypatch)
    original_write = live_preflight._atomic_write_json
    failed_latest = False

    def fail_only_first_latest(path, payload):
        nonlocal failed_latest
        if path.name == "live_preflight_latest.json" and not failed_latest:
            failed_latest = True
            raise OSError("latest write unavailable")
        return original_write(path, payload)

    monkeypatch.setattr(live_preflight, "_atomic_write_json", fail_only_first_latest)
    result = live_preflight._finalize_preflight_result(
        _checks(live_preflight),
        paper=False,
        persist_state=False,
        invocation_source="test_partial_persistence",
        exchange_results={},
    )

    assert result["ok"] is False
    assert result["reason"] == "diagnostics_persistence_failed"
    attempt_path = tmp_path / "reports" / "live_preflight_attempts" / f"{result['attempt_id']}.json"
    latest_path = tmp_path / "reports" / "live_preflight_latest.json"
    assert json.loads(attempt_path.read_text(encoding="utf-8"))["aggregate"] == {
        "passed": False, "reason": "diagnostics_persistence_failed",
    }
    assert json.loads(latest_path.read_text(encoding="utf-8"))["aggregate"] == {
        "passed": False, "reason": "diagnostics_persistence_failed",
    }


def test_partial_persistence_never_leaves_successful_history_when_repair_fails(tmp_path, monkeypatch):
    live_preflight = _live_preflight(tmp_path, monkeypatch)
    original_write = live_preflight._atomic_write_json
    history_writes = 0

    def fail_latest_and_history_repair(path, payload):
        nonlocal history_writes
        if path.name == "live_preflight_latest.json":
            raise OSError("latest write unavailable")
        history_writes += 1
        if history_writes > 1:
            raise OSError("history repair unavailable")
        return original_write(path, payload)

    monkeypatch.setattr(live_preflight, "_atomic_write_json", fail_latest_and_history_repair)
    result = live_preflight._finalize_preflight_result(
        _checks(live_preflight),
        paper=False,
        persist_state=False,
        invocation_source="test_partial_persistence_unrepaired",
        exchange_results={},
    )

    assert result["ok"] is False
    attempt_path = tmp_path / "reports" / "live_preflight_attempts" / f"{result['attempt_id']}.json"
    assert json.loads(attempt_path.read_text(encoding="utf-8"))["aggregate"] == {
        "passed": False, "reason": "diagnostics_persistence_pending",
    }


def test_current_latest_pass_is_removed_when_final_history_and_repair_fail(tmp_path, monkeypatch):
    live_preflight = _live_preflight(tmp_path, monkeypatch)
    original_write = live_preflight._atomic_write_json
    history_writes = 0
    latest_writes = 0

    def fail_final_history_and_repair_latest(path, payload):
        nonlocal history_writes, latest_writes
        if path.name == "live_preflight_latest.json":
            latest_writes += 1
            if latest_writes > 1:
                raise OSError("latest repair unavailable")
        else:
            history_writes += 1
            if history_writes > 1:
                raise OSError("final history unavailable")
        return original_write(path, payload)

    monkeypatch.setattr(live_preflight, "_atomic_write_json", fail_final_history_and_repair_latest)
    result = live_preflight._finalize_preflight_result(
        _checks(live_preflight),
        paper=False,
        persist_state=False,
        invocation_source="test_current_latest_pass_partial_failure",
        exchange_results={},
    )

    assert result["ok"] is False
    assert result["reason"] == "diagnostics_persistence_failed"
    attempt_path = tmp_path / "reports" / "live_preflight_attempts" / f"{result['attempt_id']}.json"
    latest_path = tmp_path / "reports" / "live_preflight_latest.json"
    assert not latest_path.exists()
    assert json.loads(attempt_path.read_text(encoding="utf-8"))["aggregate"]["passed"] is False


def test_stale_latest_pass_is_removed_when_current_latest_and_repair_fail(tmp_path, monkeypatch):
    live_preflight = _live_preflight(tmp_path, monkeypatch)
    latest_path = tmp_path / "reports" / "live_preflight_latest.json"
    live_preflight._atomic_write_json(latest_path, {
        "attempt_id": "previous-passing-attempt",
        "aggregate": {"passed": True, "reason": "OK"},
    })
    original_write = live_preflight._atomic_write_json

    def fail_all_current_latest_writes(path, payload):
        if path.name == "live_preflight_latest.json":
            raise OSError("current latest unavailable")
        return original_write(path, payload)

    monkeypatch.setattr(live_preflight, "_atomic_write_json", fail_all_current_latest_writes)
    result = live_preflight._finalize_preflight_result(
        _checks(live_preflight),
        paper=False,
        persist_state=False,
        invocation_source="test_stale_latest_partial_failure",
        exchange_results={},
    )

    assert result["ok"] is False
    assert result["reason"] == "diagnostics_persistence_failed"
    attempt_path = tmp_path / "reports" / "live_preflight_attempts" / f"{result['attempt_id']}.json"
    assert not latest_path.exists()
    assert json.loads(attempt_path.read_text(encoding="utf-8"))["aggregate"]["passed"] is False


def test_unhandled_check_failure_still_emits_complete_fail_closed_diagnostics(tmp_path, monkeypatch):
    live_preflight = _live_preflight(tmp_path, monkeypatch)

    async def boom(*_args, **_kwargs):
        raise RuntimeError("authorization=private-value")

    monkeypatch.setattr(live_preflight, "_run_preflight_checks", boom)
    result = asyncio.run(
        live_preflight.run_preflight(
            object(), persist_state=False, invocation_source="test_unhandled_failure",
        )
    )

    assert result["ok"] is False
    rows = {row["name"]: row for row in result["diagnostics"]["checks"]}
    assert all(rows[name]["required"] and rows[name]["ok"] is False for name in live_preflight.REQUIRED_CHECKS)
    assert rows["preflight_execution"]["detail"] == "authorization=<redacted>"
    assert (tmp_path / "reports" / "live_preflight_latest.json").exists()


def test_operator_formats_only_sanitized_failed_required_diagnostics(tmp_path, monkeypatch):
    _live_preflight(tmp_path, monkeypatch)
    from telegram_operator_controls import format_preflight_failure

    preflight = {
        "ok": False,
        "checks": {"authentication": {"ok": False, "detail": "raw-token=must-not-render"}},
        "diagnostics": {
            "attempt_id": "attempt-123",
            "failed_required_checks": [
                {"name": "authentication", "required": True, "ok": False, "detail": "code=50113 token=<redacted>"},
            ],
        },
    }
    rendered = format_preflight_failure(preflight)
    assert "Attempt ID: `attempt-123`" in rendered
    assert "`authentication`" in rendered
    assert "token=<redacted>" in rendered
    assert "must-not-render" not in rendered


def test_operator_escapes_sanitized_detail_for_telegram_markdown(tmp_path, monkeypatch):
    _live_preflight(tmp_path, monkeypatch)
    from telegram_operator_controls import format_preflight_failure

    rendered = format_preflight_failure({
        "diagnostics": {
            "attempt_id": "attempt-markdown",
            "failed_required_checks": [{
                "name": "authentication",
                "required": True,
                "ok": False,
                "detail": r"`code` *bold* _italic_ [link] \\ path",
            }],
        },
    })
    assert r"\`code\` \*bold\* \_italic\_ \[link\] \\\\ path" in rendered


def test_failed_arm_is_refused_and_successful_arm_remains_compatible(tmp_path, monkeypatch):
    _live_preflight(tmp_path, monkeypatch)
    import live_preflight
    import live_state
    import telegram_operator_controls as controls

    monkeypatch.setenv("PAPER_TRADING", "false")
    state = live_state.LiveStateMachine(tmp_path / "live_state.json")
    monkeypatch.setattr(live_state, "state", state)

    async def failed_preflight(*_args, **kwargs):
        assert kwargs["persist_state"] is False
        assert kwargs["invocation_source"] == "telegram_startbot"
        return {
            "ok": False,
            "reason": "one_or_more_required_checks_failed",
            "checks": {},
            "diagnostics": {"attempt_id": "attempt-blocked", "failed_required_checks": []},
        }

    monkeypatch.setattr(live_preflight, "run_preflight", failed_preflight)
    blocked = asyncio.run(controls.arm(object(), invocation_source="telegram_startbot"))
    assert blocked["ok"] is False
    assert blocked["state"]["state"] == "LIVE_PREFLIGHT_FAILED"
    assert blocked["state"]["armed"] is False

    required = (
        "account_equity", "available_balance", "instrument_metadata", "metadata_freshness",
        "quantity_conversion", "capital_allocator", "execution_router", "protection_manager",
        "pnl_reconciler", "leverage_set_and_readback", "no_unresolved_orders",
        "no_unknown_exchange_positions", "no_unknown_local_exposure", "no_orphan_algo_orders",
    )

    async def passed_preflight(*_args, **_kwargs):
        return {"ok": True, "reason": "OK", "checks": {name: {"ok": True} for name in required}}

    monkeypatch.setattr(live_preflight, "run_preflight", passed_preflight)
    engine = type("Engine", (), {"research_only": True})()
    result = asyncio.run(controls.arm(object(), engine, invocation_source="telegram_startbot"))
    assert result["ok"] is True
    assert result["state"]["state"] == "LIVE_ARMED"
    assert engine.research_only is False


def test_startbot_and_read_only_live_preflight_present_sanitized_details_without_arming():
    source = Path("bot.py").read_text(encoding="utf-8")
    preflight = {
        "ok": False,
        "diagnostics": {
            "attempt_id": "attempt-read-only",
            "failed_required_checks": [
                {"name": "authentication", "required": True, "ok": False, "detail": "code=50113"},
            ],
        },
    }
    snapshot = {"state": "LIVE_READY_UNARMED", "armed": False}
    assert snapshot == {"state": "LIVE_READY_UNARMED", "armed": False}
    assert "def _preflight_operator_presentation" in source
    assert "format_preflight_failure(preflight)" in source
    assert "Live Preflight (read-only)" in source
    assert "Orders submitted: `0`" in source
    startbot_source = source[source.index("async def cmd_startbot"):source.index("async def cmd_stopbot")]
    assert 'invocation_source="telegram_startbot"' in startbot_source
    assert "_preflight_operator_presentation" in startbot_source
    live_callback = source[source.index('elif data == "live:preflight":'):source.index('elif data == "queue:view":')]
    assert 'invocation_source="telegram_live_preflight"' in live_callback
    assert "persist_state=False" in live_callback
    assert "_preflight_operator_presentation(result, snap, read_only=True)" in live_callback
    assert ".arm(" not in live_callback


def test_resume_callback_matches_head_exactly():
    current = Path("bot.py").read_text(encoding="utf-8")
    baseline = subprocess.check_output(["git", "show", "HEAD:bot.py"]).decode("utf-8-sig")
    start = '    elif data == "c:resume":'
    end = '    elif data == "c:safe_confirm":'
    assert current[current.index(start):current.index(end, current.index(start))] == baseline[
        baseline.index(start):baseline.index(end, baseline.index(start))
    ]

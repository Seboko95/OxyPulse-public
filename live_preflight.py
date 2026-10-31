"""Read-only production live-readiness preflight."""
from __future__ import annotations

import os
import json
import asyncio
import time
import uuid
import re
import tempfile
import math
from collections.abc import Mapping
from datetime import datetime, timezone
from pathlib import Path

import market_mode
import settings_store
from capital_reservations import book as reservation_book
from instrument_registry import registry as instrument_registry
from live_state import state as live_state
from order_state_store import store as order_store
from opportunity_queue import queue as opportunity_queue
from pnl_reconciler import reconciler


REPORTS_DIR = Path("reports")
DIAGNOSTIC_SCHEMA_VERSION = 1
REQUIRED_CHECKS = (
    "production_endpoint", "no_demo_header", "server_clock_drift", "authentication", "account_mode", "position_mode",
    "account_equity", "available_balance",
    "isolated_margin", "instrument_metadata", "metadata_freshness", "quantity_conversion",
    "capital_allocator", "capital_reservation", "reservation_release", "opportunity_queue",
    "execution_router", "order_query_capability", "algo_order_capability", "fill_retrieval",
    "fee_retrieval", "actual_fee_schedule", "funding_retrieval", "position_reconciliation", "protection_manager",
    "pnl_reconciler", "pnl_storage", "audit_storage", "telegram_emergency_control",
    "live_state_persistence", "leverage_set_and_readback", "no_unresolved_orders",
    "no_unknown_exchange_positions", "no_unknown_local_exposure",
    "no_local_only_positions", "no_orphan_algo_orders",
)
FUNDING_CHECKS = ("account_equity", "available_balance")
DEFERRED_STAGE2_CHECKS = ("capital_reservation", "reservation_release")
_SENSITIVE_FIELD = (
    r"api[_ -]?key|apikey|secret|passphrase|signature|authorization|"
    r"auth(?:entication)?[_ -]?header|authheader|telegram[_ -]?token|telegramtoken|token|"
    r"account[_ -]?ids?|accountids?|acct[_ -]?ids?|acctids?|"
    r"order[_ -]?ids?|orderids?|ordids?|clordids?|client[_ -]?order[_ -]?ids?|clientorderids?|"
    r"algo[_ -]?ids?|algoids?|fill[_ -]?ids?|fillids?|"
    r"ok[-_ ]access[-_ ](?:key|sign|passphrase)|"
    r"(?:position|registry|reservation|intent)[_ -]?ids?|(?:position|registry|reservation|intent)ids?"
)
_SENSITIVE_VALUE = re.compile(
    rf"(?ix)(?P<key>[\"']?(?:{_SENSITIVE_FIELD})[\"']?)\s*(?P<sep>[:=])\s*"
    r"(?P<value>\[[^\]]*\]|\{[^\}]*\}|\"(?:\\.|[^\"])*\"|'(?:\\.|[^'])*'|[^,;\s\}\]]+)"
)
_SENSITIVE_BARE_VALUE = re.compile(
    rf"(?ix)\b(?P<key>{_SENSITIVE_FIELD})\b\s+(?P<value>[^,;\s\}}\]]+)"
)
_GENERIC_IDS_LIST = re.compile(
    r"(?ix)(?P<key>(?<![\w-])[\"']?ids?[\"']?)\s*(?P<sep>[:=])\s*"
    r"(?P<value>[^,;\s\}\]]+(?:,[^,;\s\}\]]+)*)"
)
_BEARER_VALUE = re.compile(r"(?i)\bbearer\s+[^,;\s\}\]]+")
_LONG_PRIVATE_VALUE = re.compile(r"\b(?:[0-9a-fA-F]{16,}|[0-9]{10,})\b")
_UUID_VALUE = re.compile(r"\b[0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}\b")


def _check(ok: bool, detail: str = "") -> dict:
    return {"ok": bool(ok), "detail": str(detail)}


def _account_balance_checks(raw_balance: Mapping[str, object]) -> tuple[dict, dict, float, float]:
    """Parse authenticated USDT account truth without coercing unknown to zero."""
    balance_rows = raw_balance.get("data", []) if str(raw_balance.get("code", "")) == "0" else []
    balance_row = dict(balance_rows[0]) if balance_rows and isinstance(balance_rows[0], Mapping) else {}
    details = balance_row.get("details")
    detail_rows = details if isinstance(details, list) else []
    usdt_detail = next(
        (row for row in detail_rows if isinstance(row, Mapping) and str(row.get("ccy", "")).upper() == "USDT"),
        {},
    )
    try:
        equity = float(balance_row.get("totalEq", usdt_detail.get("eq")))
        raw_available = usdt_detail.get("availEq", usdt_detail.get("availBal"))
        if equity == 0.0 and not usdt_detail and isinstance(details, list) and not details:
            raw_available = 0.0
        available = float(raw_available)
        if not math.isfinite(equity) or not math.isfinite(available):
            raise ValueError("non_finite_balance")
        detail = f"equity_usdt={equity:.8f} available_usdt={available:.8f}"
        if equity < 0.0 or available < 0.0:
            invalid = "ACCOUNT_BALANCE_INVALID:" + detail
            return _check(False, invalid), _check(False, invalid), equity, available
        if equity == 0.0 and available == 0.0:
            unfunded = "UNFUNDED_ACCOUNT:" + detail
            return _check(False, unfunded), _check(False, unfunded), equity, available
        if equity == 0.0 or available == 0.0:
            invalid = "ACCOUNT_BALANCE_INVALID:inconsistent_zero_balance:" + detail
            return _check(False, invalid), _check(False, invalid), equity, available
        return _check(True, detail), _check(True, detail), equity, available
    except (TypeError, ValueError) as exc:
        detail = f"account_balance_unavailable:{type(exc).__name__}:{exc}"
        return _check(False, detail), _check(False, detail), 0.0, 0.0


def sanitize_diagnostic_detail(detail: object) -> str:
    """Return an allowlisted, bounded diagnostic string suitable for operator artifacts."""
    if isinstance(detail, (Mapping, list, tuple, set)):
        return "structured detail omitted"
    text = str(detail or "").replace("\r", " ").replace("\n", " ").replace("`", "'")
    text = _BEARER_VALUE.sub("Bearer <redacted>", text)
    text = _GENERIC_IDS_LIST.sub(lambda match: f"{match.group('key')}{match.group('sep')}<redacted>", text)
    text = _SENSITIVE_VALUE.sub(lambda match: f"{match.group('key')}{match.group('sep')}<redacted>", text)
    text = _SENSITIVE_BARE_VALUE.sub(lambda match: f"{match.group('key')} <redacted>", text)
    text = _UUID_VALUE.sub("<redacted>", text)
    text = _LONG_PRIVATE_VALUE.sub("<redacted>", text)
    return text[:240] or "no detail"


def _safe_exchange_result(payload: object) -> dict | None:
    """Project exchange results to safe, stable observability fields only."""
    if not isinstance(payload, Mapping):
        return None
    has_code = "code" in payload
    has_ok = "ok" in payload
    if not has_code and not has_ok:
        return None
    code = str(payload.get("code", "")) if has_code else ""
    if has_code:
        classification = "success" if code == "0" else "exchange_error"
        if code == "0" and "data" in payload and not list(payload.get("data") or []):
            classification = "empty_response"
    else:
        classification = "success" if bool(payload.get("ok")) else "failed"
    message = payload.get("msg", payload.get("message", payload.get("reason", "")))
    result = {"classification": classification}
    if code:
        result["code"] = sanitize_diagnostic_detail(code)
    if message not in (None, ""):
        result["message"] = sanitize_diagnostic_detail(message)
    return result


def _diagnostic_record(
    result: Mapping[str, object],
    *,
    required: tuple[str, ...],
    exchange_results: Mapping[str, object],
) -> dict:
    rows = []
    for name, row in dict(result.get("checks", {}) or {}).items():
        check = dict(row or {})
        diagnostic = {
            "name": str(name),
            "required": name in required,
            "ok": bool(check.get("ok")),
            "detail": sanitize_diagnostic_detail(check.get("detail", "")),
        }
        exchange = _safe_exchange_result(exchange_results.get(name))
        if exchange:
            diagnostic["exchange"] = exchange
        rows.append(diagnostic)
    failed_required = [row for row in rows if row["required"] and not row["ok"]]
    return {
        "schema_version": DIAGNOSTIC_SCHEMA_VERSION,
        "attempt_id": str(result["attempt_id"]),
        "generated_at": str(result["generated_at"]),
        "invocation_source": sanitize_diagnostic_detail(result.get("invocation_source", "unknown")),
        "aggregate": {
            "passed": bool(result.get("ok")),
            "reason": sanitize_diagnostic_detail(result.get("reason", "preflight_failed")),
        },
        "checks": rows,
        "failed_required_checks": failed_required,
    }


def _atomic_write_json(path: Path, payload: Mapping[str, object]) -> None:
    """Atomically replace a diagnostic artifact without exposing a partial JSON file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True, ensure_ascii=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_name, path)
    finally:
        try:
            os.unlink(temp_name)
        except OSError:
            pass


def _diagnostic_paths(record: Mapping[str, object]) -> tuple[Path, Path]:
    attempt_id = str(record["attempt_id"])
    attempts_dir = REPORTS_DIR / "live_preflight_attempts"
    attempt_path = attempts_dir / f"{attempt_id}.json"
    latest_path = REPORTS_DIR / "live_preflight_latest.json"
    return attempt_path, latest_path


def _invalidate_latest_diagnostic(record: Mapping[str, object]) -> None:
    """Remove a stale or incomplete authoritative latest diagnostic artifact."""
    _, latest_path = _diagnostic_paths(record)
    try:
        latest_path.unlink()
    except FileNotFoundError:
        pass


def _persist_diagnostic_record(record: Mapping[str, object]) -> dict[str, str]:
    attempt_path, latest_path = _diagnostic_paths(record)
    # History must never claim success until the authoritative latest artifact
    # has been published. This protects the history/latest partial-write case.
    pending_record = dict(record)
    pending_record["aggregate"] = {
        "passed": False,
        "reason": "diagnostics_persistence_pending",
    }
    try:
        _atomic_write_json(attempt_path, pending_record)
        # A prior result cannot remain authoritative while this attempt publishes.
        _invalidate_latest_diagnostic(record)
        _atomic_write_json(latest_path, record)
        _atomic_write_json(attempt_path, record)
    except Exception:
        # Never leave a published PASS authoritative after an incomplete attempt.
        _invalidate_latest_diagnostic(record)
        raise
    return {"attempt": str(attempt_path), "latest": str(latest_path)}


def _repair_partial_diagnostic_record(record: Mapping[str, object]) -> dict[str, str]:
    """Best-effort overwrite of surviving artifacts with the fail-closed aggregate."""
    attempt_path, latest_path = _diagnostic_paths(record)
    written: dict[str, str] = {}
    for label, path in (("attempt", attempt_path), ("latest", latest_path)):
        try:
            _atomic_write_json(path, record)
        except Exception:
            continue
        written[label] = str(path)
    return written


def _finalize_preflight_result(
    checks: Mapping[str, object],
    *,
    paper: bool,
    persist_state: bool,
    invocation_source: str,
    exchange_results: Mapping[str, object],
) -> dict:
    """Attach safe diagnostics and preserve the existing fail-closed state transition."""
    check_map = dict(checks)
    ok = all(check_map.get(key, {}).get("ok", False) for key in REQUIRED_CHECKS)
    structural_ready = all(
        check_map.get(key, {}).get("ok", False)
        for key in REQUIRED_CHECKS
        if key not in FUNDING_CHECKS and key not in DEFERRED_STAGE2_CHECKS
    )
    funding_observed = all(check_map.get(key, {}).get("ok", False) for key in FUNDING_CHECKS)
    result = {
        "ok": ok,
        "paper_mode": paper,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "attempt_id": uuid.uuid4().hex,
        "invocation_source": str(invocation_source or "unknown"),
        "checks": dict(checks),
        "reason": "OK" if ok else "one_or_more_required_checks_failed",
        "readiness": {
            "ready_for_funding": bool(
                structural_ready
                and not funding_observed
                and all("UNFUNDED_ACCOUNT" in str(check_map.get(key, {}).get("detail", "")) for key in FUNDING_CHECKS)
            ),
            "structural_ready": bool(structural_ready),
            "funding_status": "FUNDED" if funding_observed else (
                "UNFUNDED_ACCOUNT"
                if all("UNFUNDED_ACCOUNT" in str(check_map.get(key, {}).get("detail", "")) for key in FUNDING_CHECKS)
                else "ACCOUNT_BALANCE_INVALID"
                if any("ACCOUNT_BALANCE_INVALID" in str(check_map.get(key, {}).get("detail", "")) for key in FUNDING_CHECKS)
                else "FUNDING_UNKNOWN"
            ),
            "live_arm_ready": bool(ok),
        },
        "live_orders_submitted": False,
        "armed": False,
    }
    record = _diagnostic_record(result, required=REQUIRED_CHECKS, exchange_results=exchange_results)
    try:
        artifacts = _persist_diagnostic_record(record)
    except Exception as exc:
        # A missing diagnostic record must never allow an otherwise passing live arm.
        result["ok"] = False
        result["reason"] = "diagnostics_persistence_failed"
        record["aggregate"] = {"passed": False, "reason": result["reason"]}
        try:
            _invalidate_latest_diagnostic(record)
        except Exception:
            pass
        repaired = _repair_partial_diagnostic_record(record)
        result["diagnostics"] = {**record, "artifacts": repaired} if repaired else record
        result["diagnostics_persistence_error"] = sanitize_diagnostic_detail(exc)
    else:
        result["diagnostics"] = {**record, "artifacts": artifacts}
    if persist_state:
        # An intentionally empty account is a funding boundary, not a software
        # defect. Keep the safe unarmed state when structural checks pass; the
        # arm path still requires the full aggregate, including funding.
        state_ready = bool(result["ok"] or result["readiness"]["ready_for_funding"])
        live_state.record_preflight(state_ready, str(result["reason"]))
    return result


def local_exposure_preflight_checks() -> dict[str, dict]:
    """Return local exposure readiness checks without contacting the exchange."""
    from position_registry import get_unknown_details

    unknown = get_unknown_details()
    symbols = sorted(unknown)
    ids = sorted(
        str(row.get("registry_id") or row.get("position_id") or coin)
        for coin, row in unknown.items()
    )
    return {
        "no_unknown_local_exposure": _check(
            not unknown,
            "count=0 symbols=none" if not unknown else (
                f"count={len(unknown)} symbols={','.join(symbols)} "
                f"ids={','.join(ids)} reason=LOCAL_EXPOSURE_UNKNOWN"
            ),
        ),
    }


async def _run_preflight_checks(
    client,
    *,
    refresh_metadata: bool = True,
    persist_state: bool = True,
    invocation_source: str = "production_preflight",
    checks: dict[str, dict] | None = None,
    exchange_results: dict[str, object] | None = None,
) -> dict:
    """Run read-only checks. This function never submits an order."""
    checks = checks if checks is not None else {}
    exchange_results = exchange_results if exchange_results is not None else {}
    paper = os.getenv("PAPER_TRADING", "true").strip().lower() == "true"
    demo = os.getenv("OKX_DEMO_TRADING", "false").strip().lower() in {"1", "true", "yes", "on"}
    checks["production_endpoint"] = _check(not demo, "demo header disabled" if not demo else "demo mode configured")
    checks["no_demo_header"] = _check(not demo, "x-simulated-trading not enabled")
    try:
        server_time = await client._request("GET", "/api/v5/public/time")
        exchange_results["server_clock_drift"] = server_time
        server_rows = list(server_time.get("data", []) or [])
        server_ms = float(server_rows[0].get("ts", 0.0) or 0.0) if server_rows else 0.0
        drift_ms = abs(server_ms - time.time() * 1000.0) if server_ms > 0 else float("inf")
        checks["server_clock_drift"] = _check(drift_ms <= 5000.0, f"drift_ms={drift_ms:.0f}")
    except Exception as exc:
        checks["server_clock_drift"] = _check(False, str(exc))
    auth = await client.auth_preflight()
    exchange_results["authentication"] = auth
    checks["authentication"] = _check(bool(auth.get("ok")), str(auth.get("reason", "OK")))
    raw_balance = await client._request("GET", "/api/v5/account/balance")
    exchange_results["account_equity"] = raw_balance
    exchange_results["available_balance"] = raw_balance
    checks["account_equity"], checks["available_balance"], equity, available = _account_balance_checks(raw_balance)
    raw_config = await client._request("GET", "/api/v5/account/config")
    exchange_results["account_mode"] = raw_config
    exchange_results["position_mode"] = raw_config
    config_rows = raw_config.get("data", []) if str(raw_config.get("code", "0")) == "0" else []
    config = dict(config_rows[0]) if config_rows else {}
    checks["account_mode"] = _check(bool(config), str(config.get("acctLv", "unknown")) if config else "missing")
    checks["position_mode"] = _check(str(config.get("posMode", "")).lower() in {"long_short_mode", "net_mode"}, str(config.get("posMode", "unknown")))
    checks["isolated_margin"] = _check(str(market_mode.td_mode()).lower() == "isolated", str(market_mode.td_mode()))
    try:
        if refresh_metadata:
            await instrument_registry.refresh(client, force=True)
        meta_snap = instrument_registry.snapshot()
        checks["instrument_metadata"] = _check(meta_snap["count"] > 0, str(meta_snap))
        checks["metadata_freshness"] = _check(meta_snap["healthy"], str(meta_snap.get("last_refresh")))
    except Exception as exc:
        checks["instrument_metadata"] = _check(False, str(exc))
        checks["metadata_freshness"] = _check(False, str(exc))
    try:
        sample = instrument_registry.get("BTC", require_fresh=True)
        eligible, eligibility_reason = sample.eligible() if sample is not None else (False, "BTC metadata missing")
        ticker_getter = getattr(client, "get_execution_ticker", None)
        ticker = await ticker_getter("BTC") if callable(ticker_getter) else {}
        price = float((ticker or {}).get("last", 0.0) or 0.0)
        contracts = sample.notional_to_contracts(100.0, price) if eligible and price > 0.0 else 0.0
        quantity_ok, quantity_reason = sample.quantity_feasible(contracts, "market") if eligible else (False, eligibility_reason)
        checks["quantity_conversion"] = _check(
            eligible and price > 0.0 and contracts > 0.0 and quantity_ok,
            f"inst=BTC-USDT-SWAP eligible={eligible} price={price:.8f} contracts={contracts} reason={quantity_reason}",
        )
        checks["tick_rounding"] = _check(sample.round_price(price, side="long", purpose="stop") > 0, f"tick={sample.tick_sz}") if eligible and price > 0.0 else _check(False, eligibility_reason)
    except Exception as exc:
        checks["quantity_conversion"] = _check(False, f"BTC_quantity_unavailable:{type(exc).__name__}:{exc}")
        checks["tick_rounding"] = _check(False, f"BTC_quantity_unavailable:{type(exc).__name__}:{exc}")
    try:
        allocator_health = reservation_book.health()
        allocator_ok = (
            isinstance(allocator_health, Mapping)
            and bool(allocator_health.get("healthy"))
            and not str(allocator_health.get("error", "") or "")
            and all(callable(getattr(reservation_book, name, None)) for name in ("reserve", "release", "pin", "pin_sync"))
        )
        allocator_detail = "healthy reservation book and durable reserve/release/pin interfaces" if allocator_ok else "UNAVAILABLE:reservation allocator health/interfaces"
    except Exception as exc:
        allocator_ok = False
        allocator_detail = f"UNAVAILABLE:{type(exc).__name__}:{exc}"
    checks["capital_allocator"] = _check(allocator_ok, allocator_detail)
    # A positive live-capital reservation probe would mutate protected local
    # reservation state. Defer that funded Stage-2 proof and fail live arm
    # closed without contaminating Stage-1 structural readiness.
    checks["capital_reservation"] = _check(False, "DEFERRED_STAGE2:positive funded reservation probe not run")
    checks["reservation_release"] = _check(False, "DEFERRED_STAGE2:reservation release proof not run")
    checks["opportunity_queue"] = _check(True, str(opportunity_queue.snapshot(limit=0)))
    try:
        from execution_router import router_for
        router = router_for(client)
        router_ok = callable(getattr(router, "submit", None)) and callable(getattr(router, "_local_executable_check", None))
        router_reason = "constructed router submit/local executable interfaces available"
    except Exception as exc:
        router_ok = False
        router_reason = f"router_import_failed:{type(exc).__name__}:{exc}"
    checks["execution_router"] = _check(router_ok, router_reason)
    pending_orders = await client._request("GET", "/api/v5/trade/orders-pending", params={"instType": "SWAP"})
    exchange_results["order_query_capability"] = pending_orders
    checks["order_query_capability"] = _check(str(pending_orders.get("code", "")) == "0", f"pending={len(pending_orders.get('data', []) or [])}")
    pending_algos = await client._request("GET", "/api/v5/trade/orders-algo-pending", params={"ordType": "conditional"})
    exchange_results["algo_order_capability"] = pending_algos
    checks["algo_order_capability"] = _check(str(pending_algos.get("code", "")) == "0", f"pending={len(pending_algos.get('data', []) or [])}")
    fills_result = await client._request("GET", "/api/v5/trade/fills", params={"instType": "SWAP", "limit": "1"})
    exchange_results["fill_retrieval"] = fills_result
    checks["fill_retrieval"] = _check(str(fills_result.get("code", "")) == "0", f"rows={len(fills_result.get('data', []) or [])}")
    bills_result = await client._request("GET", "/api/v5/account/bills", params={"instType": "SWAP", "limit": "1"})
    exchange_results["fee_retrieval"] = bills_result
    checks["fee_retrieval"] = _check(str(bills_result.get("code", "")) == "0", f"rows={len(bills_result.get('data', []) or [])}")
    funding_result = await client._request("GET", "/api/v5/public/funding-rate", params={"instId": "BTC-USDT-SWAP"})
    exchange_results["funding_retrieval"] = funding_result
    funding_rows = list(funding_result.get("data", []) or []) if str(funding_result.get("code", "")) == "0" else []
    funding_ok = bool(funding_rows) and isinstance(funding_rows[0], Mapping) and "fundingRate" in funding_rows[0]
    checks["funding_retrieval"] = _check(funding_ok, f"rows={len(funding_rows)} source=public_funding_rate")
    try:
        fee_schedule = await client.get_trade_fee(inst_type="SWAP")
        exchange_results["actual_fee_schedule"] = fee_schedule
        maker = float(fee_schedule.get("maker", fee_schedule.get("makerU", 0.0)) or 0.0)
        taker = float(fee_schedule.get("taker", fee_schedule.get("takerU", 0.0)) or 0.0)
        from live_fee_schedule import record_okx

        recorded_fees = record_okx(fee_schedule)
        checks["actual_fee_schedule"] = _check(
            maker != 0.0 or taker != 0.0,
            f"maker={maker:.8f} taker={taker:.8f} source={recorded_fees.source}",
        )
    except Exception as exc:
        checks["actual_fee_schedule"] = _check(False, str(exc))
    history_result = await client._request("GET", "/api/v5/account/positions-history", params={"instType": "SWAP", "limit": "1"})
    exchange_results["position_reconciliation"] = history_result
    checks["position_reconciliation"] = _check(str(history_result.get("code", "")) == "0", f"rows={len(history_result.get('data', []) or [])}")
    try:
        pnl_snapshot = reconciler.snapshot()
        statuses = dict(pnl_snapshot.get("statuses", {})) if isinstance(pnl_snapshot, Mapping) else {}
        unresolved_statuses = {
            key: int(statuses.get(key, 0) or 0)
            for key in ("UNRESOLVED", "MISMATCH", "AWAITING_EXCHANGE_DATA", "PROVISIONAL")
        }
        pnl_ok = isinstance(pnl_snapshot, Mapping) and all(value == 0 for value in unresolved_statuses.values())
        checks["pnl_reconciler"] = _check(
            pnl_ok,
            "snapshot_available unresolved=0" if pnl_ok else f"pnl_unresolved={unresolved_statuses}",
        )
    except Exception as exc:
        checks["pnl_reconciler"] = _check(False, f"UNAVAILABLE:{type(exc).__name__}:{exc}")
    pnl_path = Path(reconciler.path)
    checks["pnl_storage"] = _check(
        pnl_path.is_file() and os.access(pnl_path, os.R_OK | os.W_OK),
        f"path={pnl_path} readable_writable={pnl_path.is_file() and os.access(pnl_path, os.R_OK | os.W_OK)}",
    )
    reports_path = Path("reports")
    checks["audit_storage"] = _check(
        reports_path.is_dir() and os.access(reports_path, os.R_OK | os.W_OK),
        f"path={reports_path} readable_writable={reports_path.is_dir() and os.access(reports_path, os.R_OK | os.W_OK)}",
    )
    try:
        import telegram_operator_controls as telegram_controls

        emergency_methods = (
            getattr(live_state, "halt", None),
            getattr(live_state, "can_submit_live", None),
            getattr(telegram_controls, "halt", None),
            getattr(telegram_controls, "emergency_close", None),
        )
        emergency_ok = all(callable(method) for method in emergency_methods)
        emergency_detail = "live state and Telegram halt/emergency bindings available" if emergency_ok else "UNAVAILABLE:Telegram emergency bindings"
    except Exception as exc:
        emergency_ok = False
        emergency_detail = f"UNAVAILABLE:{type(exc).__name__}:{exc}"
    checks["telegram_emergency_control"] = _check(
        emergency_ok,
        emergency_detail,
    )
    checks["live_state_persistence"] = _check(bool(live_state.snapshot().get("state")), str(live_state.snapshot()))
    unresolved = order_store.unresolved()
    orders_truth_ok = str(pending_orders.get("code", "")) == "0" and isinstance(pending_orders.get("data"), list)
    algo_truth_ok = str(pending_algos.get("code", "")) == "0" and isinstance(pending_algos.get("data"), list)
    pending_order_rows = list(pending_orders.get("data", []) or []) if orders_truth_ok else []
    pending_algo_rows = list(pending_algos.get("data", []) or []) if algo_truth_ok else []
    checks["no_unresolved_orders"] = _check(
        orders_truth_ok and algo_truth_ok and not unresolved and not pending_order_rows and not pending_algo_rows,
        (
            f"local={len(unresolved)} exchange_pending={len(pending_order_rows)} exchange_algo_pending={len(pending_algo_rows)}"
            if orders_truth_ok and algo_truth_ok
            else "orders_truth_unavailable"
        ),
    )
    raw_positions = await client._request("GET", "/api/v5/account/positions", params={"instType": "SWAP"})
    exchange_results["no_unknown_exchange_positions"] = raw_positions
    exchange_results["no_unknown_positions"] = raw_positions
    exchange_results["orphaned_position_check"] = raw_positions
    positions_truth_ok = str(raw_positions.get("code", "")) == "0" and isinstance(raw_positions.get("data"), list)
    positions = list(raw_positions.get("data", []) or []) if positions_truth_ok else []
    from position_registry import (
        get_active_details,
        get_unknown_details,
        is_unresolved_exposure,
    )
    local = get_active_details()
    checks.update(local_exposure_preflight_checks())
    exchange_coins = {
        str(row.get("instId", "")).split("-")[0].upper()
        for row in positions if abs(float(row.get("pos", 0.0) or 0.0)) > 0
    }
    unknown = [
        row for row in positions
        if abs(float(row.get("pos", 0.0) or 0.0)) > 0
        and str(row.get("instId", "")).split("-")[0].upper() not in local
    ]
    checks["no_unknown_exchange_positions"] = _check(
        positions_truth_ok and not unknown,
        f"exchange_positions={len(positions)} unknown={len(unknown)}" if positions_truth_ok else "positions_truth_unavailable",
    )
    # Compatibility alias for existing readers; this remains exchange-side
    # only and is deliberately separate from local exposure uncertainty.
    checks["no_unknown_positions"] = checks["no_unknown_exchange_positions"]
    checks["orphaned_position_check"] = checks["no_unknown_positions"]
    local_only = [
        coin for coin, row in local.items()
        if coin not in exchange_coins and not is_unresolved_exposure(row) and (
            bool(row.get("exchange_confirmed", False))
            or bool(row.get("paper_confirmed", False))
            or float(row.get("entry", 0.0) or 0.0) > 0.0
        )
    ]
    checks["no_local_only_positions"] = _check(
        not local_only,
        f"local_positions={len(local)} local_only={','.join(sorted(local_only)) or 'none'}",
    )
    algo_rows = list(pending_algos.get("data", []) or []) if algo_truth_ok else []
    orphan_algos = [
        row for row in algo_rows
        if str(row.get("instId", "")).split("-")[0].upper() not in exchange_coins
    ]
    checks["no_orphan_algo_orders"] = _check(
        algo_truth_ok and positions_truth_ok and not orphan_algos,
        (
            f"pending_algos={len(algo_rows)} orphan_algos={len(orphan_algos)}"
            if algo_truth_ok and positions_truth_ok
            else "algo_or_position_truth_unavailable"
        ),
    )
    leverage_result = await client._request(
        "GET", "/api/v5/account/leverage-info",
        params={"instId": "BTC-USDT-SWAP", "mgnMode": "isolated"},
    )
    exchange_results["leverage_set_and_readback"] = leverage_result
    exchange_results["leverage_readback"] = leverage_result
    leverage_rows = list(leverage_result.get("data", []) or []) if str(leverage_result.get("code", "")) == "0" else []
    configured_values = []
    for row in leverage_rows:
        try:
            if str(row.get("instId", "")).upper() != "BTC-USDT-SWAP" or str(row.get("mgnMode", "")).lower() != "isolated":
                continue
            value = int(float(row.get("lever", 0) or 0))
            if value > 0:
                configured_values.append(value)
        except (TypeError, ValueError):
            continue
    checks["leverage_set_and_readback"] = _check(
        bool(configured_values),
        f"BTC isolated leverage readback rows={len(configured_values)} values={configured_values or 'none'}; GET-only",
    )
    checks["leverage_readback"] = checks["leverage_set_and_readback"]
    try:
        from execution_router import router_for

        router = router_for(client)
        protection = getattr(router, "protection", None)
        emergency_close = getattr(protection, "_emergency_close", None)
        protection_ok = (
            protection is not None
            and callable(getattr(protection, "ensure", None))
            and callable(emergency_close)
            and callable(getattr(client, "place_protective_order", None))
        )
        protection_detail = (
            "router protection ensure/emergency-close and client protective-order interface available"
            if protection_ok
            else "UNAVAILABLE:router protection emergency-close callback"
        )
    except Exception as exc:
        protection_ok = False
        protection_detail = f"UNAVAILABLE:{type(exc).__name__}:{exc}"
    checks["protection_manager"] = _check(protection_ok, protection_detail)
    return _finalize_preflight_result(
        checks,
        paper=paper,
        persist_state=persist_state,
        invocation_source=invocation_source,
        exchange_results=exchange_results,
    )


async def run_preflight(
    client,
    *,
    refresh_metadata: bool = True,
    persist_state: bool = True,
    invocation_source: str = "production_preflight",
) -> dict:
    """Run the read-only preflight and always emit a fail-closed diagnostic record."""
    checks: dict[str, dict] = {}
    exchange_results: dict[str, object] = {}
    paper = os.getenv("PAPER_TRADING", "true").strip().lower() == "true"
    try:
        return await _run_preflight_checks(
            client,
            refresh_metadata=refresh_metadata,
            persist_state=persist_state,
            invocation_source=invocation_source,
            checks=checks,
            exchange_results=exchange_results,
        )
    except Exception as exc:
        # Preserve the failing decision while making incomplete checks explicit
        # rather than silently dropping the attempt from operator diagnostics.
        for name in REQUIRED_CHECKS:
            checks.setdefault(name, _check(False, "not completed due to preflight execution failure"))
        checks["preflight_execution"] = _check(False, str(exc))
        return _finalize_preflight_result(
            checks,
            paper=paper,
            persist_state=persist_state,
            invocation_source=invocation_source,
            exchange_results=exchange_results,
        )


async def _main() -> None:
    from okx_client import OKXClient
    result = await run_preflight(OKXClient(), refresh_metadata=True, invocation_source="standalone")
    result["live_orders_submitted"] = False
    result["live_arming_state"] = live_state.snapshot()
    print(json.dumps({
        "ok": result["ok"],
        "reason": result["reason"],
        "attempt_id": result.get("attempt_id"),
        "path": result.get("diagnostics", {}).get("artifacts", {}).get("latest"),
    }))


if __name__ == "__main__":
    asyncio.run(_main())

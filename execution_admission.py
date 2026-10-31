from __future__ import annotations

import logging
import time

import settings_store as _s

logger = logging.getLogger("OxyPulse.ExecutionAdmission")

_failure_history: dict[str, list[dict[str, object]]] = {}
_cooldown_until: dict[str, float] = {}


def clear() -> None:
    _failure_history.clear()
    _cooldown_until.clear()


def _now() -> float:
    return time.monotonic()


def _coin_key(coin: str) -> str:
    return str(coin or "").upper()


def _prune(coin: str, *, now: float | None = None) -> None:
    ts_now = float(now if now is not None else _now())
    key = _coin_key(coin)
    lookback = max(
        float(_s.get_val("execution_failure_penalty_window_secs", 3600.0) or 3600.0),
        float(_s.get_val("preflight_zero_contract_window_secs", 1800.0) or 1800.0),
        1.0,
    )
    rows = [
        row for row in _failure_history.get(key, [])
        if ts_now - float(row.get("ts", 0.0) or 0.0) <= lookback
    ]
    if rows:
        _failure_history[key] = rows
    else:
        _failure_history.pop(key, None)

    until = float(_cooldown_until.get(key, 0.0) or 0.0)
    if until and until <= ts_now:
        _cooldown_until.pop(key, None)
        logger.info("SYMBOL COOLDOWN EXPIRED %s | reason=preflight_zero_contracts", key)


def _record_failure(coin: str, tag: str, detail: str = "") -> int:
    key = _coin_key(coin)
    ts_now = _now()
    _prune(key, now=ts_now)
    rows = _failure_history.setdefault(key, [])
    rows.append({"ts": ts_now, "tag": str(tag or "").lower(), "detail": str(detail or "")})
    return len(rows)


def record_preflight_zero_contract(coin: str, *, detail: str = "") -> bool:
    key = _coin_key(coin)
    _record_failure(key, "preflight_zero_contracts", detail=detail)
    if not bool(_s.get_val("preflight_cooldown_enabled", True)):
        return False
    ts_now = _now()
    window = float(_s.get_val("preflight_zero_contract_window_secs", 1800.0) or 1800.0)
    trigger_count = max(int(_s.get_val("preflight_zero_contract_trigger_count", 2) or 2), 1)
    cooldown_secs = float(_s.get_val("preflight_zero_contract_cooldown_secs", 1800.0) or 1800.0)
    rows = [
        row for row in _failure_history.get(key, [])
        if str(row.get("tag", "")) == "preflight_zero_contracts"
        and ts_now - float(row.get("ts", 0.0) or 0.0) <= window
    ]
    if len(rows) < trigger_count:
        return False
    prev_until = float(_cooldown_until.get(key, 0.0) or 0.0)
    until = max(prev_until, ts_now + cooldown_secs)
    _cooldown_until[key] = until
    logger.warning(
        "SYMBOL COOLDOWN APPLIED %s | reason=preflight_zero_contracts count=%d window=%.0fs cooldown=%.0fs detail=%s",
        key,
        len(rows),
        window,
        cooldown_secs,
        detail or "n/a",
    )
    return True


def record_exchange_admission_failure(coin: str, tag: str, detail: str = "") -> None:
    _record_failure(coin, tag, detail=detail)


def record_success(coin: str) -> None:
    key = _coin_key(coin)
    if key in _failure_history or key in _cooldown_until:
        logger.info("SYMBOL EXECUTION RESET %s | reason=successful_order", key)
    _failure_history.pop(key, None)
    _cooldown_until.pop(key, None)


def is_symbol_cooled_down(coin: str) -> tuple[bool, str]:
    key = _coin_key(coin)
    ts_now = _now()
    _prune(key, now=ts_now)
    if not bool(_s.get_val("preflight_cooldown_enabled", True)):
        return False, ""
    until = float(_cooldown_until.get(key, 0.0) or 0.0)
    if until <= ts_now:
        return False, ""
    remaining = max(until - ts_now, 0.0)
    return True, f"preflight_zero_contract_cooldown:{remaining:.0f}s"


def get_selection_penalty(coin: str) -> float:
    key = _coin_key(coin)
    ts_now = _now()
    _prune(key, now=ts_now)
    if not bool(_s.get_val("executability_penalty_enabled", True)):
        return 0.0
    penalty_window = float(_s.get_val("execution_failure_penalty_window_secs", 3600.0) or 3600.0)
    preflight_penalty = float(_s.get_val("preflight_zero_contract_selection_penalty", 40.0) or 40.0)
    generic_penalty = float(_s.get_val("exchange_admission_selection_penalty", 12.0) or 12.0)
    cooldown_penalty = float(_s.get_val("execution_cooldown_selection_penalty", 1000.0) or 1000.0)
    penalty = 0.0
    for row in _failure_history.get(key, []):
        if ts_now - float(row.get("ts", 0.0) or 0.0) > penalty_window:
            continue
        tag = str(row.get("tag", "") or "")
        if tag == "preflight_zero_contracts":
            penalty += preflight_penalty
        else:
            penalty += generic_penalty
    if key in _cooldown_until:
        penalty += cooldown_penalty
    return penalty


def get_recent_failure_count(coin: str, tag: str | None = None) -> int:
    key = _coin_key(coin)
    ts_now = _now()
    _prune(key, now=ts_now)
    penalty_window = float(_s.get_val("execution_failure_penalty_window_secs", 3600.0) or 3600.0)
    count = 0
    for row in _failure_history.get(key, []):
        if ts_now - float(row.get("ts", 0.0) or 0.0) > penalty_window:
            continue
        if tag and str(row.get("tag", "") or "") != str(tag or "").lower():
            continue
        count += 1
    return count

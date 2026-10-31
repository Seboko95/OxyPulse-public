"""Immutable production-candidate evidence and forward counterfactual research.

This module is deliberately analytics-only. It has no exchange client, order,
reservation, registry, or arming dependency.
"""
from __future__ import annotations

import hashlib
import json
import math
import os
import threading
import time
from dataclasses import asdict, is_dataclass
from datetime import datetime, timezone
from pathlib import Path
from statistics import median
from typing import Any


DEFAULT_EVENT_PATH = Path("data/production_candidate_expectancy.jsonl")
DEFAULT_REJECTION_PATH = Path("data/rejected_candidate_calibration.jsonl")
DEFAULT_STATE_PATH = Path("data/rejected_candidate_counterfactual_state.json")
POSITIVE_CASE_PATH = Path("data/positive_production_candidate_cases.jsonl")
EVENT_PATH = DEFAULT_EVENT_PATH
REJECTION_PATH = DEFAULT_REJECTION_PATH
STATE_PATH = DEFAULT_STATE_PATH
MODEL_VERSION = "profitability_control_plane_v2"
CALIBRATION_VERSION = "production_provenance_v1"
SNAPSHOT_VERSION = "candidate_snapshot_v2"
HORIZONS_SECS = (15, 30, 60, 180, 300, 900, 1800, 3600)
_LOCK = threading.RLock()
_STATE_CACHE: dict[str, Any] | None = None
_STATE_CACHE_PATH = ""


def _pytest_default_path(path: Path) -> bool:
    if "PYTEST_CURRENT_TEST" not in os.environ:
        return False
    defaults = {DEFAULT_EVENT_PATH, DEFAULT_REJECTION_PATH, DEFAULT_STATE_PATH}
    try:
        return path.resolve() in {item.resolve() for item in defaults}
    except OSError:
        return False


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _f(value: Any, default: float = 0.0) -> float:
    try:
        result = float(value)
        return result if math.isfinite(result) else default
    except (TypeError, ValueError):
        return default


def _freeze(value: Any) -> Any:
    """Convert runtime objects into immutable JSON-safe evidence."""
    if value is None or isinstance(value, (str, int, bool)):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if is_dataclass(value):
        return _freeze(asdict(value))
    if isinstance(value, dict):
        return {str(key): _freeze(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_freeze(item) for item in value]
    return str(value)


def _append(path: Path, row: dict[str, Any]) -> None:
    if _pytest_default_path(path):
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    frozen = _freeze(row)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(frozen, sort_keys=True, separators=(",", ":")) + "\n")


def _load_state() -> dict[str, Any]:
    global _STATE_CACHE, _STATE_CACHE_PATH
    resolved = str(STATE_PATH.resolve())
    if _pytest_default_path(STATE_PATH):
        if _STATE_CACHE_PATH != f"pytest:{resolved}":
            _STATE_CACHE = {"active": {}, "opportunities": {}}
            _STATE_CACHE_PATH = f"pytest:{resolved}"
        return _STATE_CACHE
    if _STATE_CACHE is not None and _STATE_CACHE_PATH == resolved:
        return _STATE_CACHE
    try:
        payload = json.loads(STATE_PATH.read_text(encoding="utf-8"))
        _STATE_CACHE = payload if isinstance(payload, dict) else {}
    except Exception:
        _STATE_CACHE = {}
    _STATE_CACHE.setdefault("active", {})
    _STATE_CACHE.setdefault("opportunities", {})
    _STATE_CACHE_PATH = resolved
    return _STATE_CACHE


def _save_state(payload: dict[str, Any]) -> None:
    global _STATE_CACHE, _STATE_CACHE_PATH
    if _pytest_default_path(STATE_PATH):
        _STATE_CACHE = payload
        _STATE_CACHE_PATH = f"pytest:{STATE_PATH.resolve()}"
        return
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    temporary = STATE_PATH.with_suffix(".tmp")
    temporary.write_text(json.dumps(_freeze(payload), indent=2, sort_keys=True), encoding="utf-8")
    temporary.replace(STATE_PATH)
    _STATE_CACHE = payload
    _STATE_CACHE_PATH = str(STATE_PATH.resolve())


def _opportunity_key(engine: str, symbol: str, direction: str) -> str:
    return f"{str(engine).upper()}:{str(symbol).upper()}:{str(direction).lower()}"


def _new_id(key: str, timestamp: str) -> str:
    return hashlib.sha256(f"{key}|{timestamp}".encode("utf-8")).hexdigest()[:20]


def _route_costs(economics: dict[str, Any]) -> float:
    return _f(economics.get("estimated_cost_usdt"))


def _family_horizon(engine_family: str) -> int:
    family = str(engine_family or "").lower()
    if family in {"order_flow", "mean_reversion", "selector", "protector"}:
        return 3600
    if family in {"structure", "breakout"}:
        return 7200
    return 14400


def record_evaluation(
    *,
    symbol: str,
    direction: str,
    engine: str,
    confidence: float,
    role: str,
    contributors: list[str],
    quality_snapshot: dict[str, Any],
    economics: dict[str, Any],
    disposition: str,
    feature_snapshot: dict[str, Any] | None = None,
    scan_id: str = "",
    raw_score: float | None = None,
    engine_roles: dict[str, str] | None = None,
    engine_family: str = "",
    exit_profile: str = "",
    model_version: str = MODEL_VERSION,
    calibration_version: str = CALIBRATION_VERSION,
    exit_profile_version: str = "exit_profile_v1",
    timestamp: str | None = None,
    timestamp_epoch: float | None = None,
    opportunity_ttl_secs: float = 300.0,
) -> str:
    """Persist one immutable revision and return its stable candidate ID."""
    timestamp = str(timestamp or _now())
    now_epoch = float(timestamp_epoch if timestamp_epoch is not None else time.time())
    key = _opportunity_key(engine, symbol, direction)
    frozen_quality = _freeze(dict(quality_snapshot or {}))
    frozen_economics = _freeze(dict(economics or {}))
    frozen_features = _freeze(dict(feature_snapshot or {}))
    with _LOCK:
        state = _load_state()
        opportunities = state.setdefault("opportunities", {})
        prior = dict(opportunities.get(key, {}) or {})
        alive = prior and now_epoch - _f(prior.get("last_seen_epoch"), 0.0) <= max(float(opportunity_ttl_secs), 1.0)
        opportunity_id = str(prior.get("opportunity_id")) if alive else _new_id(key, timestamp)
        candidate_id = opportunity_id
        revision = int(prior.get("revision", 0) or 0) + 1 if alive else 1
        row = {
            "event": "candidate_snapshot",
            "snapshot_version": SNAPSHOT_VERSION,
            "candidate_id": candidate_id,
            "opportunity_id": opportunity_id,
            "scan_id": str(scan_id or f"scan-{int(now_epoch)}"),
            "candidate_revision": revision,
            "timestamp": timestamp,
            "timestamp_epoch": now_epoch,
            "symbol": str(symbol or "").upper(),
            "direction": str(direction or "").lower(),
            "primary_engine": str(engine or "").upper(),
            "contributing_engines": sorted({str(item).upper() for item in contributors or [] if item}),
            "engine_roles": dict(engine_roles or {}),
            "role": str(role or ""),
            "engine_family": str(engine_family or frozen_quality.get("lane", "") or ""),
            "raw_score": _f(raw_score, _f(confidence)),
            "raw_confidence": _f(confidence),
            "calibrated_probability": frozen_economics.get("probability", {}).get("probabilities", {}),
            "uncertainty": frozen_economics.get("probability", {}).get("uncertainty"),
            "market_regime": str(frozen_quality.get("regime", "") or ""),
            "quality_snapshot": frozen_quality,
            "features": frozen_features,
            "economics": frozen_economics,
            "exit_profile": str(exit_profile or frozen_features.get("exit_profile", "") or ""),
            "model_version": str(model_version),
            "calibration_version": str(calibration_version),
            "exit_profile_version": str(exit_profile_version),
            "disposition": str(disposition or ""),
            "environment": "production_counterfactual",
            "eligible_for_production_learning": False,
            "order_submitted": False,
        }
        _append(EVENT_PATH, row)
        predicted_net = _f(frozen_economics.get("expected_net_usdt"))
        predicted_gross = _f(frozen_economics.get("expected_gross_usdt"))
        if predicted_net > 0.0:
            _append(POSITIVE_CASE_PATH, {
                "event": "positive_candidate_case", "timestamp": timestamp,
                "candidate_id": candidate_id, "opportunity_id": opportunity_id,
                "candidate_revision": revision, "symbol": row["symbol"],
                "direction": row["direction"], "engine": row["primary_engine"],
                "contributors": row["contributing_engines"], "regime": row["market_regime"],
                "entry_price": frozen_features.get("planned_entry"),
                "spread_bps": frozen_features.get("spread_bps"),
                "stop_price": frozen_features.get("planned_stop"),
                "tp1_price": frozen_features.get("tp1"), "tp2_price": frozen_features.get("tp2"),
                "size_usdt": frozen_economics.get("size_usdt"),
                "gross_edge_usdt": predicted_gross, "fees_slippage_usdt": _route_costs(frozen_economics),
                "uncertainty": frozen_economics.get("probability", {}).get("uncertainty"),
                "net_edge_usdt": predicted_net,
                "expected_return_on_risk": frozen_economics.get("expected_return_on_risk"),
                "mfe_pct": None, "mae_pct": None, "forward_result": "TRACKING",
                "research_only": True, "order_submitted": False,
            })
        opportunities[key] = {
            "opportunity_id": opportunity_id,
            "revision": revision,
            "last_seen_epoch": now_epoch,
            "last_disposition": row["disposition"],
        }
        classification = str(frozen_economics.get("classification", "") or "")
        active = state.setdefault("active", {})
        geometry = dict(frozen_economics.get("geometry_preview", {}) or {})
        entry = _f(frozen_features.get("planned_entry"), _f(geometry.get("entry_price")))
        if candidate_id not in active:
            tracking = {
                **row,
                "event": "counterfactual_open",
                "entry_price": entry,
                "entry_executable_price": _f(frozen_features.get("entry_executable_price"), entry),
                "stop_price": _f(frozen_features.get("planned_stop"), _f(geometry.get("stop_price"))),
                "tp1_price": _f(frozen_features.get("tp1"), _f(geometry.get("tp1_price"))),
                "tp2_price": _f(frozen_features.get("tp2"), _f(geometry.get("tp2_price"))),
                "spread_bps": _f(frozen_features.get("spread_bps")),
                "opened_epoch": now_epoch,
                "last_seen_epoch": now_epoch,
                "max_price": entry,
                "min_price": entry,
                "mfe_pct": 0.0,
                "mae_pct": 0.0,
                "time_to_mfe_secs": 0.0,
                "time_to_mae_secs": 0.0,
                "tp1_seen": False,
                "tp2_seen": False,
                "sl_seen": False,
                "horizons_seen": [],
                "engine_horizon_secs": _family_horizon(row["engine_family"]),
                "status": "tracking",
            }
            active[candidate_id] = tracking
            _append(REJECTION_PATH, tracking)
        else:
            active[candidate_id]["latest_revision"] = revision
            active[candidate_id]["latest_economics"] = frozen_economics
            active[candidate_id]["last_seen_epoch"] = now_epoch
            if classification == "POSITIVE_EXPECTANCY":
                active[candidate_id]["became_positive_revision"] = revision
        _save_state(state)
        return candidate_id


def record_execution(
    *,
    candidate_id: str,
    opportunity_id: str,
    symbol: str,
    direction: str,
    engine: str,
    intent_id: str,
    reservation_id: str,
    order_id: str,
    filled_contracts: float,
    average_fill_price: float,
    protection: dict[str, Any] | None = None,
    timestamp_epoch: float | None = None,
) -> dict[str, Any]:
    """Persist the immutable transition from research candidate to live execution."""
    now_epoch = float(timestamp_epoch if timestamp_epoch is not None else time.time())
    row = {
        "event": "production_execution",
        "timestamp": datetime.fromtimestamp(now_epoch, timezone.utc).isoformat(),
        "timestamp_epoch": now_epoch,
        "candidate_id": str(candidate_id),
        "opportunity_id": str(opportunity_id),
        "symbol": str(symbol or "").upper(),
        "direction": str(direction or "").lower(),
        "primary_engine": str(engine or "").upper(),
        "disposition": "EXECUTED_PRODUCTION",
        "environment": "live",
        "eligible_for_production_learning": False,
        "exchange_final": False,
        "order_submitted": True,
        "intent_id": str(intent_id),
        "reservation_id": str(reservation_id),
        "order_id": str(order_id),
        "filled_contracts": _f(filled_contracts),
        "average_fill_price": _f(average_fill_price),
        "protection": _freeze(dict(protection or {})),
    }
    with _LOCK:
        _append(EVENT_PATH, row)
    return row


def _executable_price(row: dict[str, Any], observed: float, bid: float, ask: float) -> float:
    direction = str(row.get("direction", "")).lower()
    half_spread = _f(row.get("spread_bps")) / 20_000.0
    if direction in {"long", "buy"}:
        return bid if bid > 0.0 else observed * (1.0 - half_spread)
    return ask if ask > 0.0 else observed * (1.0 + half_spread)


def _move_pct(direction: str, entry: float, price: float) -> float:
    if entry <= 0.0:
        return 0.0
    move = (price - entry) / entry * 100.0
    return -move if direction in {"short", "sell"} else move


def _result_classification(row: dict[str, Any], result: str) -> str:
    max_net = _f(row.get("max_favorable_net_pnl_usdt"))
    if max_net > 0.0 and (row.get("tp1_seen") or row.get("tp2_seen")):
        return "FALSE_NEGATIVE"
    if result == "sl_before_tp" and max_net <= 0.0:
        return "CORRECT_REJECTION"
    if result == "expired" and max_net <= 0.0:
        return "EXPIRED_WITHOUT_EDGE"
    return "AMBIGUOUS"


def _timing_classification(row: dict[str, Any], result: str) -> str:
    if result == "sl_before_tp" and not row.get("tp1_seen"):
        return "WRONG_DIRECTION"
    if row.get("tp1_seen") and result == "sl_before_tp":
        return "LATE_ENTRY"
    if result == "tp2_before_sl" and _f(row.get("mae_pct")) < -0.5 * abs(_f(row.get("mfe_pct"))):
        return "EARLY_ENTRY"
    if result == "expired" and _f(row.get("mfe_pct")) > 0.0:
        return "WEAK_MOVE"
    return "NO_REAL_EDGE" if result == "expired" else "DIRECTION_CONFIRMED"


def observe_price(
    symbol: str,
    price: float,
    *,
    bid: float = 0.0,
    ask: float = 0.0,
    high: float | None = None,
    low: float | None = None,
    timestamp_epoch: float | None = None,
) -> int:
    """Advance all active opportunities for a symbol without execution effects."""
    observed = _f(price)
    if observed <= 0.0:
        return 0
    now_epoch = float(timestamp_epoch if timestamp_epoch is not None else time.time())
    completed = 0
    with _LOCK:
        state = _load_state()
        active = state.setdefault("active", {})
        if not any(str(row.get("symbol", "")).upper() == str(symbol).upper() for row in active.values()):
            return 0
        for candidate_id, row in list(active.items()):
            if str(row.get("symbol", "")).upper() != str(symbol or "").upper():
                continue
            entry = _f(row.get("entry_executable_price"), _f(row.get("entry_price")))
            direction = str(row.get("direction", "")).lower()
            elapsed = max(now_epoch - _f(row.get("opened_epoch"), now_epoch), 0.0)
            favorable_price = _f(high, observed) if direction in {"long", "buy"} else _f(low, observed)
            adverse_price = _f(low, observed) if direction in {"long", "buy"} else _f(high, observed)
            favorable_move = _move_pct(direction, entry, favorable_price)
            adverse_move = _move_pct(direction, entry, adverse_price)
            if favorable_move > _f(row.get("mfe_pct")):
                row["mfe_pct"] = favorable_move
                row["time_to_mfe_secs"] = elapsed
            if adverse_move < _f(row.get("mae_pct")):
                row["mae_pct"] = adverse_move
                row["time_to_mae_secs"] = elapsed
            row["max_price"] = max(_f(row.get("max_price"), observed), _f(high, observed))
            row["min_price"] = min(_f(row.get("min_price"), observed), _f(low, observed))
            executable = _executable_price(row, observed, _f(bid), _f(ask))
            economics = dict(row.get("latest_economics", row.get("economics", {})) or {})
            size = _f(economics.get("size_usdt"))
            costs = _route_costs(economics)
            row["max_favorable_net_pnl_usdt"] = size * _f(row.get("mfe_pct")) / 100.0 - costs
            row["max_adverse_net_pnl_usdt"] = size * _f(row.get("mae_pct")) / 100.0 - costs
            for horizon in HORIZONS_SECS:
                if elapsed >= horizon and horizon not in row.get("horizons_seen", []):
                    row.setdefault("horizons_seen", []).append(horizon)
                    _append(REJECTION_PATH, {
                        "event": "counterfactual_horizon",
                        "candidate_id": candidate_id,
                        "opportunity_id": row.get("opportunity_id"),
                        "candidate_revision": row.get("latest_revision", row.get("candidate_revision", 1)),
                        "timestamp": _now(),
                        "horizon_secs": horizon,
                        "symbol": row.get("symbol"),
                        "mfe_pct": row.get("mfe_pct"),
                        "mae_pct": row.get("mae_pct"),
                        "hypothetical_gross_pnl_usdt": round(size * _move_pct(direction, entry, executable) / 100.0, 8),
                        "hypothetical_net_pnl_usdt": round(size * _move_pct(direction, entry, executable) / 100.0 - costs, 8),
                    })
            stop = _f(row.get("stop_price"))
            tp1 = _f(row.get("tp1_price"))
            tp2 = _f(row.get("tp2_price"))
            result = ""
            if direction in {"long", "buy"}:
                if stop and _f(low, observed) <= stop:
                    row["sl_seen"] = True
                    result = "sl_before_tp"
                elif tp2 and _f(high, observed) >= tp2:
                    row["tp2_seen"] = row["tp1_seen"] = True
                    result = "tp2_before_sl"
                elif tp1 and _f(high, observed) >= tp1:
                    row["tp1_seen"] = True
            else:
                if stop and _f(high, observed) >= stop:
                    row["sl_seen"] = True
                    result = "sl_before_tp"
                elif tp2 and _f(low, observed) <= tp2:
                    row["tp2_seen"] = row["tp1_seen"] = True
                    result = "tp2_before_sl"
                elif tp1 and _f(low, observed) <= tp1:
                    row["tp1_seen"] = True
            if not result and elapsed >= _f(row.get("engine_horizon_secs"), 3600.0):
                result = "expired"
            if result:
                move_pct = _move_pct(direction, entry, executable)
                hypothetical_net = size * move_pct / 100.0 - costs
                hypothetical_gross = size * move_pct / 100.0
                row["hypothetical_net_pnl_usdt"] = hypothetical_net
                outcome = {
                    "event": "counterfactual_close",
                    "candidate_id": candidate_id,
                    "opportunity_id": row.get("opportunity_id"),
                    "candidate_revision": row.get("latest_revision", row.get("candidate_revision", 1)),
                    "timestamp": _now(),
                    "symbol": row.get("symbol"),
                    "direction": direction,
                    "primary_engine": row.get("primary_engine"),
                    "engine_family": row.get("engine_family"),
                    "market_regime": row.get("market_regime"),
                    "role": row.get("role"),
                    "contributing_engines": row.get("contributing_engines", []),
                    "engine_roles": row.get("engine_roles", {}),
                    "vote_rescue": bool((row.get("quality_snapshot", {}) or {}).get("vote_rescue", {}).get("allowed")),
                    "result": result,
                    "classification": _result_classification(row, result),
                    "timing_classification": _timing_classification(row, result),
                    "exit_price": executable,
                    "mfe_pct": round(_f(row.get("mfe_pct")), 8),
                    "mae_pct": round(_f(row.get("mae_pct")), 8),
                    "time_to_mfe_secs": round(_f(row.get("time_to_mfe_secs")), 3),
                    "time_to_mae_secs": round(_f(row.get("time_to_mae_secs")), 3),
                    "tp1_before_sl": bool(row.get("tp1_seen")),
                    "tp2_before_sl": bool(row.get("tp2_seen")),
                    "sl_before_tp": result == "sl_before_tp" and not bool(row.get("tp1_seen")),
                    "max_favorable_net_pnl_usdt": round(_f(row.get("max_favorable_net_pnl_usdt")), 8),
                    "max_adverse_net_pnl_usdt": round(_f(row.get("max_adverse_net_pnl_usdt")), 8),
                    "hypothetical_exit_result": result,
                    "hypothetical_gross_pnl_usdt": round(hypothetical_gross, 8),
                    "hypothetical_net_pnl_usdt": round(hypothetical_net, 8),
                    "environment": "production_counterfactual",
                    "eligible_for_production_learning": False,
                }
                _append(REJECTION_PATH, outcome)
                del active[candidate_id]
                completed += 1
            else:
                row["last_seen_epoch"] = now_epoch
        _save_state(state)
    return completed


def _rows(path: Path | str) -> list[dict[str, Any]]:
    try:
        return [json.loads(line) for line in Path(path).read_text(encoding="utf-8").splitlines() if line.strip()]
    except Exception:
        return []


def summarize(path: Path | str | None = None, event_path: Path | str | None = None) -> dict[str, Any]:
    rows = _rows(path or REJECTION_PATH)
    snapshots = [row for row in _rows(event_path or EVENT_PATH) if row.get("event") in {"candidate_snapshot", "candidate_expectancy"}]
    latest: dict[str, dict[str, Any]] = {}
    for row in snapshots:
        key = str(row.get("candidate_id", ""))
        if int(row.get("candidate_revision", 1) or 1) >= int(latest.get(key, {}).get("candidate_revision", 0) or 0):
            latest[key] = row
    opens = {str(row.get("candidate_id")) for row in rows if row.get("event") in {"counterfactual_open", "expectancy_rejection_open"}}
    closes = [row for row in rows if row.get("event") in {"counterfactual_close", "expectancy_rejection_close"}]
    pnl = [_f(row.get("hypothetical_net_pnl_usdt")) for row in closes]
    wins = [value for value in pnl if value > 0.0]
    losses = [value for value in pnl if value < 0.0]
    false_negatives = [row for row in closes if row.get("classification") == "FALSE_NEGATIVE" or _f(row.get("hypothetical_net_pnl_usdt")) > 0.0]
    correct = [row for row in closes if row.get("classification") == "CORRECT_REJECTION"]
    gross = [_f((row.get("economics", {}) or {}).get("expected_gross_usdt")) for row in latest.values()]
    net = [_f((row.get("economics", {}) or {}).get("expected_net_usdt")) for row in latest.values()]
    maturity = "INSUFFICIENT" if len(closes) < 10 else "PRELIMINARY" if len(closes) < 30 else "MODERATE" if len(closes) < 100 else "STRONG"
    return {
        "unique_opportunities": len(latest),
        "candidate_revisions": len(snapshots),
        "rejected_count": len(opens),
        "resolved_count": len(closes),
        "positive_gross_edge_count": sum(value > 0.0 for value in gross),
        "negative_gross_edge_count": sum(value < 0.0 for value in gross),
        "positive_net_edge_count": sum(value > 0.0 for value in net),
        "negative_net_edge_count": sum(value < 0.0 for value in net),
        "rejected_counterfactual_net_pnl": round(sum(pnl), 8),
        "rejected_profit_factor": round(sum(wins) / abs(sum(losses)), 8) if losses else None,
        "false_negative_count": len(false_negatives),
        "false_negative_rate": round(len(false_negatives) / len(closes), 8) if closes else None,
        "correct_rejection_count": len(correct),
        "correct_rejection_rate": round(len(correct) / len(closes), 8) if closes else None,
        "evidence_maturity": maturity,
    }


def aggregate_engine_alpha(
    event_path: Path | str | None = None,
    outcome_path: Path | str | None = None,
    engine_names: list[str] | tuple[str, ...] = (),
) -> dict[str, dict[str, Any]]:
    snapshots = [row for row in _rows(event_path or EVENT_PATH) if row.get("event") in {"candidate_snapshot", "candidate_expectancy"}]
    outcomes = [row for row in _rows(outcome_path or REJECTION_PATH) if row.get("event") in {"counterfactual_close", "expectancy_rejection_close"}]
    names = {str(name).upper() for name in engine_names}
    names.update(str(row.get("primary_engine", "")).upper() for row in snapshots if row.get("primary_engine"))
    names.update(str(row.get("primary_engine", "")).upper() for row in outcomes if row.get("primary_engine"))
    result: dict[str, dict[str, Any]] = {}
    for name in sorted(names):
        revisions = [row for row in snapshots if str(row.get("primary_engine", "")).upper() == name]
        unique = {}
        for row in revisions:
            unique[str(row.get("candidate_id"))] = row
        candidate_rows = list(unique.values())
        engine_outcomes = [row for row in outcomes if str(row.get("primary_engine", "")).upper() == name]
        gross = [_f((row.get("economics", {}) or {}).get("expected_gross_usdt")) for row in candidate_rows]
        cf_net = [_f(row.get("hypothetical_net_pnl_usdt")) for row in engine_outcomes]
        cf_gross = [_f(row.get("hypothetical_gross_pnl_usdt"), _f(row.get("hypothetical_net_pnl_usdt"))) for row in engine_outcomes]
        false_negative = [row for row in engine_outcomes if row.get("classification") == "FALSE_NEGATIVE"]
        correct = [row for row in engine_outcomes if row.get("classification") == "CORRECT_REJECTION"]
        regimes = Counter(str(row.get("market_regime", "UNKNOWN") or "UNKNOWN") for row in candidate_rows)
        directions = Counter(str(row.get("direction", "unknown") or "unknown") for row in candidate_rows)
        def _best_outcome_dimension(field: str) -> str | None:
            grouped: dict[str, list[float]] = {}
            for outcome in engine_outcomes:
                key = str(outcome.get(field, "unknown") or "unknown")
                grouped.setdefault(key, []).append(_f(outcome.get("hypothetical_net_pnl_usdt")))
            return max(grouped, key=lambda key: sum(grouped[key]) / len(grouped[key])) if grouped else None
        result[name] = {
            "contexts_seen": len({(row.get("market_regime"), row.get("direction"), (row.get("features", {}) or {}).get("liquidity_bucket"), (row.get("features", {}) or {}).get("volatility_bucket")) for row in candidate_rows}),
            "evaluations": len(revisions),
            "candidates": len(candidate_rows),
            "average_predicted_gross_edge": round(sum(gross) / len(gross), 8) if gross else None,
            "median_predicted_gross_edge": round(median(gross), 8) if gross else None,
            "positive_gross_pct": round(sum(1 for value in gross if value > 0) / len(gross) * 100.0, 4) if gross else None,
            "negative_gross_pct": round(sum(1 for value in gross if value < 0) / len(gross) * 100.0, 4) if gross else None,
            "counterfactual_net_edge": round(sum(cf_net), 8) if cf_net else None,
            "counterfactual_gross_edge": round(sum(cf_gross), 8) if cf_gross else None,
            "resolved_opportunities": len(engine_outcomes),
            "counterfactual_winners": sum(1 for value in cf_net if value > 0.0),
            "false_negative_rate": round(len(false_negative) / len(engine_outcomes), 8) if engine_outcomes else None,
            "correct_rejection_rate": round(len(correct) / len(engine_outcomes), 8) if engine_outcomes else None,
            "mfe_pct": round(sum(_f(row.get("mfe_pct")) for row in engine_outcomes) / len(engine_outcomes), 8) if engine_outcomes else None,
            "mae_pct": round(sum(_f(row.get("mae_pct")) for row in engine_outcomes) / len(engine_outcomes), 8) if engine_outcomes else None,
            "tp1_hit_rate": round(sum(bool(row.get("tp1_before_sl")) for row in engine_outcomes) / len(engine_outcomes), 8) if engine_outcomes else None,
            "tp2_hit_rate": round(sum(bool(row.get("tp2_before_sl")) for row in engine_outcomes) / len(engine_outcomes), 8) if engine_outcomes else None,
            "sl_rate": round(sum(bool(row.get("sl_before_tp")) for row in engine_outcomes) / len(engine_outcomes), 8) if engine_outcomes else None,
            "signal_overlap": sum(1 for row in candidate_rows if len(row.get("contributing_engines", [])) > 1),
            "unique_signal_count": sum(1 for row in candidate_rows if len(row.get("contributing_engines", [])) <= 1),
            "regimes": dict(regimes),
            "directions": dict(directions),
            "best_regime": _best_outcome_dimension("market_regime"),
            "best_direction": _best_outcome_dimension("direction"),
            "timing_diagnosis": Counter(str(row.get("timing_classification", "unknown") or "unknown") for row in engine_outcomes).most_common(1)[0][0] if engine_outcomes else None,
            "sample_confidence": "none" if not engine_outcomes else "thin" if len(engine_outcomes) < 30 else "developing" if len(engine_outcomes) < 100 else "moderate",
        }
    return result


def aggregate_legacy_cap_shadow(event_path: Path | str | None = None) -> dict[str, Any]:
    snapshots = [row for row in _rows(event_path or EVENT_PATH) if row.get("event") in {"candidate_snapshot", "candidate_expectancy"}]
    latest: dict[str, dict[str, Any]] = {}
    for row in snapshots:
        key = str(row.get("candidate_id", ""))
        if int(row.get("candidate_revision", 1) or 1) >= int(latest.get(key, {}).get("candidate_revision", 0) or 0):
            latest[key] = row
    comparisons = []
    for row in latest.values():
        shadow = dict((row.get("features", {}) or {}).get("legacy_40pct_cap_shadow", {}) or {})
        if shadow.get("available") and shadow.get("research_only") and not shadow.get("applied_to_admission"):
            comparisons.append(shadow)
    def avg(key: str) -> float | None:
        return round(sum(_f(row.get(key)) for row in comparisons) / len(comparisons), 8) if comparisons else None
    return {
        "candidate_count": len(comparisons),
        "average_notional_delta_usdt": avg("notional_delta_usdt"),
        "average_margin_delta_usdt": avg("margin_delta_usdt"),
        "average_risk_delta_usdt": avg("risk_delta_usdt"),
        "larger_without_pct_cap_count": sum(_f(row.get("notional_delta_usdt")) > 0.0 for row in comparisons),
        "applied_to_admission_count": sum(bool(row.get("applied_to_admission")) for row in comparisons),
    }


def aggregate_research_dimensions(
    event_path: Path | str | None = None,
    outcome_path: Path | str | None = None,
) -> dict[str, dict[str, dict[str, Any]]]:
    """Aggregate prediction and forward evidence without promoting it to calibration."""
    snapshots = [row for row in _rows(event_path or EVENT_PATH) if row.get("event") in {"candidate_snapshot", "candidate_expectancy"}]
    latest: dict[str, dict[str, Any]] = {}
    for row in snapshots:
        key = str(row.get("candidate_id"))
        if int(row.get("candidate_revision", 1) or 1) >= int(latest.get(key, {}).get("candidate_revision", 0) or 0):
            latest[key] = row
    outcomes = {str(row.get("candidate_id")): row for row in _rows(outcome_path or REJECTION_PATH) if row.get("event") in {"counterfactual_close", "expectancy_rejection_close"}}
    buckets: dict[str, dict[str, list[tuple[dict[str, Any], dict[str, Any] | None]]]] = {
        name: {} for name in ("regime", "direction", "liquidity", "volatility", "role", "independent_families", "vote_rescue", "timing")
    }
    for candidate_id, row in latest.items():
        features = dict(row.get("features", {}) or {})
        contributors = list(row.get("contributing_engines", []) or [])
        roles = dict(row.get("engine_roles", {}) or {})
        contributor_families = dict(features.get("contributor_families", {}) or {})
        families = {str(value) for value in contributor_families.values() if value}
        if not families and contributors:
            families = {str(item).upper() for item in contributors}
        outcome = outcomes.get(candidate_id)
        dimensions = {
            "regime": str(row.get("market_regime", "UNKNOWN") or "UNKNOWN"),
            "direction": str(row.get("direction", "unknown") or "unknown"),
            "liquidity": str(features.get("liquidity_bucket", "unknown") or "unknown"),
            "volatility": str(features.get("volatility_bucket", "unknown") or "unknown"),
            "role": str(row.get("role", "unknown") or "unknown"),
            "independent_families": str(max(len(families), 1)),
            "vote_rescue": "rescued" if bool((features.get("vote_rescue", {}) or {}).get("allowed")) else "not_rescued",
            "timing": str((outcome or {}).get("timing_classification", "unresolved") or "unresolved"),
        }
        if roles:
            dimensions["role"] = "+".join(sorted(set(str(value) for value in roles.values())))
        for dimension, value in dimensions.items():
            buckets[dimension].setdefault(value, []).append((row, outcome))
    result: dict[str, dict[str, dict[str, Any]]] = {name: {} for name in buckets}
    for dimension, groups in buckets.items():
        for value, records in groups.items():
            gross = [_f((row.get("economics", {}) or {}).get("expected_gross_usdt")) for row, _ in records]
            resolved = [outcome for _, outcome in records if outcome is not None]
            net = [_f(outcome.get("hypothetical_net_pnl_usdt")) for outcome in resolved]
            result[dimension][value] = {
                "candidate_count": len(records),
                "resolved_count": len(resolved),
                "average_predicted_gross_edge": round(sum(gross) / len(gross), 8) if gross else None,
                "counterfactual_net_edge": round(sum(net), 8) if net else None,
                "mfe_pct": round(sum(_f(item.get("mfe_pct")) for item in resolved) / len(resolved), 8) if resolved else None,
                "mae_pct": round(sum(_f(item.get("mae_pct")) for item in resolved) / len(resolved), 8) if resolved else None,
                "false_negative_rate": round(sum(item.get("classification") == "FALSE_NEGATIVE" for item in resolved) / len(resolved), 8) if resolved else None,
                "sample_confidence": "none" if not resolved else "thin" if len(resolved) < 30 else "developing" if len(resolved) < 100 else "moderate",
            }
    return result


# Counter is imported late in source order to keep the top-level dependency set minimal.
from collections import Counter

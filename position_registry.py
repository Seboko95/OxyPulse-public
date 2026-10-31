"""
OxyPulse — position_registry.py  v3
Shared position registry across all 6 engines.

v3 fixes:
  • Full trade details stored: entry, size, leverage, stop, take, engine, direction.
    /positions and PnL display pull from this rather than stale _open dicts.
  • can_open_and_reserve stays atomic (Lock).
  • update() method — updates size/price after order fill without re-acquiring.
  • get_all_details() — returns full trade details for position reporting.

Rules:
  • Before ANY open  → can_open_and_reserve(coin, direction, engine, size)
  • On ANY close     → release(coin)
  • Persists to data/position_registry.json
"""
from __future__ import annotations
import asyncio
import hashlib
import json
import logging
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import settings_store as _s

logger = logging.getLogger("OxyPulse.Registry")

def _default_registry_path() -> Path:
    paper = os.getenv("PAPER_TRADING", "true").strip().lower() == "true"
    return Path("data/position_registry.paper.json" if paper else "data/position_registry.json")


_PATH = _default_registry_path()

# {
#   COIN: {
#     "direction": "long" | "short",
#     "engine":    "RYUU",
#     "size_usdt": float,
#     "entry":     float,
#     "leverage":  int,
#     "stop_loss": float,
#     "take_profit": float,
#     "order_id":  str,
#     "opened_at": ISO timestamp,
#   }
# }
_registry: dict[str, dict] = {}
_healthy = True
_load_error = ""

_lock = asyncio.Lock()


class PositionRegistryUnavailable(RuntimeError):
    """Raised when persisted position truth cannot be read safely."""

PRE_SUBMIT_ENTRY_STATE = "PRE_SUBMIT_RESERVED"
EXPOSURE_ACTIVE = "ACTIVE_POSITION"
EXPOSURE_UNKNOWN = "EXPOSURE_UNKNOWN"
EXPOSURE_TERMINAL = "NON_ACTIONABLE_TERMINAL"
EXCHANGE_CLOSED_AWAITING_RECONCILIATION = "EXCHANGE_CLOSED_AWAITING_RECONCILIATION"

_LEGACY_TERMINAL_STATUSES = {
    "CLOSED",
    "INACTIVE",
    "FULLY_CLOSED",
    "RECONCILED",
    "EXCHANGE_CLOSED",
    "CANCELLED",
    "CANCELED",
    "EXPIRED",
    "REJECTED",
}
_LEGACY_ACTIVE_STATUSES = {
    "OPEN",
    "ACTIVE",
    "FILLED",
    "PARTIALLY_FILLED",
    "POSITION_OPEN",
}
_LEGACY_STATUS_KEYS = (
    "status",
    "position_status",
    "position_state",
    "exposure_status",
    "lifecycle_status",
)

_LIFECYCLE_ID_KEYS = (
    "position_lifecycle_id",
    "position_id",
    "pos_id",
    "order_id",
    "entry_order_id",
    "entry_client_order_id",
    "entry_clOrdId",
    "exchange_position_open_time",
    "position_open_time",
    "execution_intent_id",
    "intent_id",
)


def derive_exchange_lifecycle_anchor(position: dict | None = None) -> str:
    """Return stable exchange recognition evidence, separate from lifecycle ID."""
    position = dict(position or {})
    inst_id = _usable_identity(position.get("instId", position.get("inst_id", ""))).upper()
    opened_at = _usable_identity(position.get("cTime", position.get("openTime", "")))
    pos_side = _usable_identity(position.get("posSide", "")).lower()
    if pos_side not in {"long", "short"}:
        try:
            signed = float(position.get("pos", 0.0) or 0.0)
        except (TypeError, ValueError):
            signed = 0.0
        pos_side = "long" if signed > 0 else "short" if signed < 0 else ""
    if not inst_id or not opened_at or not pos_side:
        return ""
    seed = f"{inst_id}|{pos_side}|{opened_at}"
    return f"EXA-{hashlib.sha256(seed.encode('utf-8')).hexdigest()[:32]}"


def derive_exchange_adopted_lifecycle_id(anchor: str) -> str:
    """Derive a durable lifecycle ID for an exchange-only adoption."""
    anchor = _usable_identity(anchor)
    if not anchor:
        return ""
    return f"exchange-adopted:{hashlib.sha256(anchor.encode('utf-8')).hexdigest()[:32]}"


def _exchange_evidence_matches(row: dict, position: dict, anchor: str) -> bool:
    """Prove a first bind from persisted or explicit immutable evidence."""
    persisted = row.get("exchange_position_evidence")
    if isinstance(persisted, dict) and derive_exchange_lifecycle_anchor(persisted) == anchor:
        return True

    origin_order = _usable_identity(
        row.get("origin_entry_order_id", row.get("order_id", ""))
    )
    origin_client = _usable_identity(row.get("origin_entry_client_id", ""))
    for key in ("originEntryOrdId", "entryOrdId", "entryOrderId"):
        if origin_order and _usable_identity(position.get(key)) == origin_order:
            return True
    for key in ("originEntryClOrdId", "entryClOrdId", "entryClientOrderId"):
        if origin_client and _usable_identity(position.get(key)) == origin_client:
            return True
    return False


def resolve_position_lifecycle(row: dict | None, position: dict | None) -> dict:
    """Resolve one canonical lifecycle and its exchange recognition anchor.

    A filled managed row may bind its existing immutable lifecycle to the first
    authoritative exchange anchor. Once bound, a changed anchor proves that a
    later position replaced the old lifecycle. Geometry is never used.
    """
    row = dict(row or {})
    anchor = derive_exchange_lifecycle_anchor(position)
    lifecycle_id = str(derive_position_lifecycle_id(row) or "")
    stored_anchor = _usable_identity(row.get("exchange_lifecycle_anchor"))
    if not anchor:
        return {
            "status": "UNRESOLVED",
            "position_lifecycle_id": lifecycle_id,
            "exchange_lifecycle_anchor": "",
            "stored_exchange_lifecycle_anchor": stored_anchor,
            "reason": "exchange_lifecycle_anchor_unavailable",
        }
    if stored_anchor:
        if stored_anchor == anchor and lifecycle_id:
            return {
                "status": "MATCHED",
                "position_lifecycle_id": lifecycle_id,
                "exchange_lifecycle_anchor": anchor,
                "stored_exchange_lifecycle_anchor": stored_anchor,
            }
        if stored_anchor != anchor:
            return {
                "status": "STALE_LOCAL",
                "position_lifecycle_id": derive_exchange_adopted_lifecycle_id(anchor),
                "exchange_lifecycle_anchor": anchor,
                "stored_exchange_lifecycle_anchor": stored_anchor,
                "previous_position_lifecycle_id": lifecycle_id,
            }
    safe_first_bind = bool(
        lifecycle_id
        and str(row.get("entry_lifecycle", "") or "").upper() == "FILLED"
        and bool(row.get("mutation_attempted", False))
        and bool(row.get("exchange_confirmed", False))
        and _usable_identity(
            row.get("origin_entry_order_id", row.get("order_id", ""))
        )
        and _exchange_evidence_matches(row, position, anchor)
    )
    if safe_first_bind:
        return {
            "status": "BOUND",
            "position_lifecycle_id": lifecycle_id,
            "exchange_lifecycle_anchor": anchor,
            "stored_exchange_lifecycle_anchor": "",
        }
    if not lifecycle_id:
        return {
            "status": "NEW_LIFECYCLE",
            "position_lifecycle_id": derive_exchange_adopted_lifecycle_id(anchor),
            "exchange_lifecycle_anchor": anchor,
            "stored_exchange_lifecycle_anchor": "",
        }
    return {
        "status": "UNRESOLVED",
        "position_lifecycle_id": lifecycle_id,
        "exchange_lifecycle_anchor": anchor,
        "stored_exchange_lifecycle_anchor": "",
        "reason": "registry_lifecycle_association_unproven",
    }


def _usable_identity(value: object) -> str:
    text = str(value or "").strip()
    if not text or text in {"--", "?", "UNKNOWN", "NONE", "NULL"}:
        return ""
    return text


def derive_position_lifecycle_id(
    row: dict | None = None,
    *,
    order_id: object = "",
    client_order_id: object = "",
    metadata: dict | None = None,
) -> str:
    """Return a durable lifecycle key from immutable entry evidence.

    Geometry is deliberately excluded. A new position with the same symbol,
    side, quantity, and stop must still receive a different lifecycle key.
    """
    sources = []
    if isinstance(metadata, dict):
        sources.append(metadata)
    if isinstance(row, dict):
        sources.append(row)
    for source in sources:
        for key in _LIFECYCLE_ID_KEYS:
            value = _usable_identity(source.get(key))
            if value:
                if key == "position_lifecycle_id":
                    return value
                return f"{key}:{value}"
    value = _usable_identity(order_id) or _usable_identity(client_order_id)
    return f"entry:{value}" if value else ""


def _float(value, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _normalize_sync_status(status: str | None) -> str:
    norm = str(status or "UNCERTAIN").upper()
    if norm == "LOST":
        return "MISSING"
    if norm not in {"OK", "UNCERTAIN", "MISSING"}:
        return "UNCERTAIN"
    return norm


def _sync_defaults() -> dict:
    return {
        "last_seen_on_exchange": "",
        "last_successful_sync": "",
        "sync_status": "UNCERTAIN",
        "exchange_absence_confirmations": 0,
        "last_sync_error": "",
    }


def _status_token(value) -> str:
    return str(value or "").strip().upper().replace("-", "_").replace(" ", "_")


def is_terminal_historical_position(row: dict | None) -> bool:
    """Return true only for an explicitly terminal legacy position row."""
    if not isinstance(row, dict):
        return False
    if _status_token(row.get("exposure_truth_status")) == EXPOSURE_TERMINAL:
        return True
    return any(
        _status_token(row.get(key)) in _LEGACY_TERMINAL_STATUSES
        for key in _LEGACY_STATUS_KEYS
    )


def _exposure_default(row: dict) -> str:
    if bool(row.get("pending_reservation", False)):
        return "PENDING_RESERVATION"
    if is_terminal_historical_position(row):
        return EXPOSURE_TERMINAL
    status_tokens = {_status_token(row.get(key)) for key in _LEGACY_STATUS_KEYS}
    if status_tokens & _LEGACY_ACTIVE_STATUSES:
        return EXPOSURE_ACTIVE
    # Older active rows generally have at least one concrete position field.
    # A row with neither a terminal marker nor position evidence remains
    # unresolved so admission stays conservative instead of inventing truth.
    if any(row.get(key) not in (None, "", 0, 0.0) for key in ("entry", "size_usdt", "order_id")):
        return EXPOSURE_ACTIVE
    return EXPOSURE_UNKNOWN


def is_actionable_position(row: dict | None) -> bool:
    """Return whether a registry row may participate in active trading gates."""
    if not isinstance(row, dict):
        return False
    exposure = (
        _exposure_default(row)
        if "exposure_truth_status" not in row
        else str(row.get("exposure_truth_status", EXPOSURE_ACTIVE) or EXPOSURE_ACTIVE).upper()
    )
    return exposure not in {
        EXPOSURE_TERMINAL,
        EXCHANGE_CLOSED_AWAITING_RECONCILIATION,
    }


def exposure_status(row: dict | None) -> str:
    """Return the normalized local exposure classification for a row."""
    if not isinstance(row, dict):
        return EXPOSURE_UNKNOWN
    if "exposure_truth_status" not in row:
        return _exposure_default(row)
    value = _status_token(row.get("exposure_truth_status"))
    if value in {
        EXPOSURE_ACTIVE,
        EXPOSURE_UNKNOWN,
        EXPOSURE_TERMINAL,
        EXCHANGE_CLOSED_AWAITING_RECONCILIATION,
        "PENDING_RESERVATION",
    }:
        return value
    return EXPOSURE_UNKNOWN


def is_unresolved_exposure(row: dict | None) -> bool:
    """Return true when local exposure cannot safely be classified."""
    return exposure_status(row) == EXPOSURE_UNKNOWN


def is_mutation_actionable_position(row: dict | None) -> bool:
    """Return true only for explicitly active local exposure.

    Admission remains more conservative: ``is_actionable_position`` includes
    unresolved rows so they continue to block duplicate/capacity admission.
    An unresolved row must never, by itself, authorize an order.
    """
    return exposure_status(row) == EXPOSURE_ACTIVE


def _execution_environment() -> str:
    return "paper" if os.getenv("PAPER_TRADING", "true").strip().lower() == "true" else "production"


def is_pre_submit_reservation(row: dict | None) -> bool:
    """Return true only for an explicitly unsubmitted entry reservation.

    These rows still participate in duplicate, capacity, and risk accounting,
    but they are not exchange positions until the mutation boundary is crossed.
    Ambiguous or legacy pending rows remain visible to reconciliation so startup
    recovery cannot silently bypass fail-closed truth checks.
    """
    if not isinstance(row, dict):
        return False
    return (
        row.get("pending_reservation") is True
        and "entry_lifecycle" in row
        and str(row.get("entry_lifecycle", "") or "").upper() == PRE_SUBMIT_ENTRY_STATE
        and row.get("mutation_attempted") is False
        and "order_id" in row
        and not str(row.get("order_id", "") or "").strip()
        and "entry" in row
    )


def _position_risk_pct(trade: dict, balance_usdt: float) -> float:
    if balance_usdt <= 0:
        return 0.0
    size = _float(trade.get("size_usdt", trade.get("size", 0.0)))
    if size <= 0:
        return 0.0
    sl_dist_pct = _float(trade.get("sl_dist_pct", 0.0))
    if sl_dist_pct <= 0:
        entry = _float(trade.get("entry", 0.0))
        stop = _float(trade.get("stop_loss", 0.0))
        if entry > 0 and stop > 0:
            sl_dist_pct = abs(entry - stop) / entry * 100.0
    if sl_dist_pct <= 0:
        sl_dist_pct = _float(_s.get_val("min_sl_dist_pct", 1.0), 1.0)
    return (size * sl_dist_pct / 100.0) / balance_usdt * 100.0


def _candidate_risk_pct(
    *,
    balance_usdt: float,
    size_usdt: float,
    entry: float,
    stop_loss: float,
    metadata: dict | None,
) -> float:
    meta = metadata or {}
    risk_meta = meta.get("risk_sizing") if isinstance(meta.get("risk_sizing"), dict) else {}
    sl_dist_pct = _float(meta.get("sl_dist_pct", 0.0))
    if sl_dist_pct <= 0:
        sl_dist_pct = _float(risk_meta.get("sl_dist_pct", 0.0))
    return _position_risk_pct(
        {
            "size_usdt": size_usdt,
            "entry": entry,
            "stop_loss": stop_loss,
            "sl_dist_pct": sl_dist_pct,
        },
        balance_usdt,
    )


def _projected_357_check(
    *,
    balance_usdt: float,
    candidate_size_usdt: float,
    entry: float,
    stop_loss: float,
    metadata: dict | None,
) -> tuple[bool, str, dict]:
    if not _s.get_val("rule_357_enabled", True):
        return True, "OK", {}
    if balance_usdt <= 0:
        return True, "OK", {}
    max_open = _float(_s.get_val("rule_357_max_open_risk_pct", 5.0), 5.0)
    if max_open <= 0:
        return True, "OK", {}
    open_risk = sum(
        _position_risk_pct(t, balance_usdt)
        for t in _registry.values()
        if is_actionable_position(t)
    )
    candidate_risk = _candidate_risk_pct(
        balance_usdt=balance_usdt,
        size_usdt=candidate_size_usdt,
        entry=entry,
        stop_loss=stop_loss,
        metadata=metadata,
    )
    projected = open_risk + candidate_risk
    risk_meta = {
        "rule_357_open_risk_pct": round(open_risk, 6),
        "rule_357_candidate_risk_pct": round(candidate_risk, 6),
        "rule_357_projected_risk_pct": round(projected, 6),
        "rule_357_max_open_risk_pct": round(max_open, 6),
    }
    if projected > max_open + 1e-9:
        return (
            False,
            f"3-5-7: projected open risk {projected:.2f}% > {max_open:.2f}% cap "
            f"(open {open_risk:.2f}% + candidate {candidate_risk:.2f}%).",
            risk_meta,
        )
    return True, "OK", risk_meta


# ── Persistence ───────────────────────────────────────────────────────────────

def _save(*, strict: bool = False) -> bool:
    payload = json.dumps(_registry, indent=2, ensure_ascii=False)
    if strict:
        _PATH.parent.mkdir(parents=True, exist_ok=True)
        fd, temp_name = tempfile.mkstemp(
            prefix=f".{_PATH.name}.", suffix=".tmp", dir=str(_PATH.parent)
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8", newline="") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temp_name, _PATH)
        except Exception:
            try:
                os.unlink(temp_name)
            except OSError:
                pass
            raise
        return True
    try:
        _PATH.parent.mkdir(parents=True, exist_ok=True)
        _PATH.write_text(payload, encoding="utf-8")
    except Exception as e:
        logger.warning("Registry save failed: %s", e)
        return False
    return True


def _load() -> None:
    global _registry, _healthy, _load_error
    try:
        if _PATH.exists():
            raw = json.loads(_PATH.read_text(encoding="utf-8"))
            if not isinstance(raw, dict):
                raise ValueError("position_registry_top_level_must_be_object")
            if any(not isinstance(value, dict) for value in raw.values()):
                raise ValueError("position_registry_row_must_be_object")
            # Only an explicit, never-submitted reservation is safe to prune.
            # Legacy, malformed, and post-mutation rows must survive restart so
            # reconciliation can keep them fail-closed until resolved.
            prunable = [c for c, v in raw.items() if is_pre_submit_reservation(v)]
            for c in prunable:
                logger.warning("Registry pruned safe pre-submit reservation: %s", c)
                del raw[c]
            normalized = False
            for c, v in raw.items():
                if not isinstance(v, dict):
                    logger.warning("Registry preserved malformed row: %s", c)
                    continue
                v["sync_status"] = _normalize_sync_status(v.get("sync_status"))
                for sk, sv in _sync_defaults().items():
                    v.setdefault(sk, sv)
                if "exposure_truth_status" not in v:
                    v["exposure_truth_status"] = _exposure_default(v)
                    normalized = True
            _registry = raw
            if prunable or normalized:
                try:
                    _save()
                except Exception as save_exc:
                    # Keep the normalized rows in memory and retry on the next
                    # startup; a persistence hiccup must not erase exposure
                    # truth or make admission fail open.
                    logger.warning("Registry legacy-state normalization was not persisted: %s", save_exc)
            logger.info(
                "Registry loaded: %d positions (%d safe pre-submit reservations pruned).",
                len(_registry), len(prunable),
            )
        _healthy = True
        _load_error = ""
    except Exception as e:
        # Never turn unreadable persisted exposure into an invented empty
        # registry.  Preserve the last known in-memory truth and block new
        # exposure/mutations until the durable source is repaired.
        _healthy = False
        _load_error = f"{type(e).__name__}:{e}"
        logger.error("Registry load failed; exposure admission is blocked: %s", _load_error)


def health() -> dict:
    return {"healthy": bool(_healthy), "error": str(_load_error or ""), "path": str(_PATH)}


def _require_healthy() -> None:
    if not _healthy:
        raise PositionRegistryUnavailable(_load_error or "position_registry_unavailable")


# ── Core API ──────────────────────────────────────────────────────────────────

async def can_open_and_reserve(
    coin: str, direction: str, engine: str,
    size_usdt: float = 0.0,
    entry: float = 0.0,
    leverage: int = 5,
    stop_loss: float = 0.0,
    take_profit: float = 0.0,
    order_id: str = "",
    metadata: dict | None = None,
    balance_usdt: float = 0.0,
) -> tuple[bool, str]:
    """
    ATOMIC check-and-reserve.

    Checks for conflicts (duplicate / opposing direction) and if clear,
    registers the position in one uninterruptible lock-protected operation.

    Returns (allowed, reason).
    """
    coin = coin.upper()
    async with _lock:
        if not _healthy:
            return False, f"position_registry_unavailable:{_load_error or 'unreadable_persisted_state'}"
        if coin in _registry and not is_terminal_historical_position(_registry[coin]):
            existing = _registry[coin]
            if not is_actionable_position(existing):
                return False, f"{coin} closure reconciliation pending."
            if existing["direction"] == direction:
                return False, (
                    f"{coin} already {direction.upper()} via {existing['engine']} — skip duplicate."
                )
            return False, (
                f"{coin} has OPPOSING {existing['direction'].upper()} via {existing['engine']} "
                f"— hard block."
            )

        allowed_risk, risk_reason, risk_meta = _projected_357_check(
            balance_usdt=_float(balance_usdt, 0.0),
            candidate_size_usdt=_float(size_usdt, 0.0),
            entry=_float(entry, 0.0),
            stop_loss=_float(stop_loss, 0.0),
            metadata=metadata,
        )
        if not allowed_risk:
            logger.info(
                "Registry reserve blocked +%s %s [%s] | %s",
                coin, direction.upper(), engine, risk_reason,
            )
            return False, risk_reason

        previous = dict(_registry.get(coin, {}) or {})
        opened_at = datetime.now(timezone.utc).isoformat()
        lifecycle_id = derive_position_lifecycle_id(
            order_id=order_id,
            metadata=metadata,
        )
        _registry[coin] = {
            "direction":   direction,
            "engine":      engine,
            "size_usdt":   size_usdt,
            "entry":       entry,
            "leverage":    leverage,
            "requested_leverage": leverage,
            "exchange_configured_leverage": None,
            "position_effective_leverage": None,
            "leverage_truth_status": "PENDING",
            "leverage_truth_timestamp": "",
            "stop_loss":   stop_loss,
            "take_profit": take_profit,
            "order_id":    order_id,
            "opened_at":   opened_at,
            "execution_environment": _execution_environment(),
            "exposure_truth_status": EXPOSURE_ACTIVE,
            **_sync_defaults(),
        }
        if lifecycle_id:
            _registry[coin]["position_lifecycle_id"] = lifecycle_id
        merged_metadata = dict(metadata or {})
        if risk_meta:
            merged_metadata.update(risk_meta)
        if bool(merged_metadata.get("pending_reservation", False)):
            merged_metadata.setdefault("entry_lifecycle", PRE_SUBMIT_ENTRY_STATE)
            merged_metadata.setdefault("mutation_attempted", False)
        if merged_metadata:
            _registry[coin].update(merged_metadata)
        if not _save():
            if previous:
                _registry[coin] = previous
            else:
                _registry.pop(coin, None)
            return False, "position_registry_persistence_failed"
        is_pending = bool(_registry[coin].get("pending_reservation", False))
        if is_pending:
            logger.info(
                "Registry reserve +%s %s [%s] projected=$%.2f",
                coin, direction.upper(), engine, float(size_usdt or 0.0),
            )
        elif size_usdt <= 0 and entry <= 0 and stop_loss <= 0 and take_profit <= 0 and not order_id:
            logger.info("Registry reserve +%s %s [%s]", coin, direction.upper(), engine)
        else:
            logger.info(
                "Registry +%s %s [%s] $%.2f lev=%dx SL=%.6f TP=%.6f",
                coin, direction.upper(), engine, size_usdt, leverage, stop_loss, take_profit,
            )
        return True, "OK"


async def confirm(coin: str, engine: str, metadata: dict | None = None) -> dict | None:
    """Record same-coin/same-direction confirmation without opening a new position."""
    coin = coin.upper()
    engine = engine.upper()
    async with _lock:
        current = _registry.get(coin)
        if not current:
            return None
        existing_engine = str(current.get("engine", "") or "").upper()
        confirmed_by = list(current.get("confirmed_by", []) or [])
        confirmation_added = False
        if engine and engine != existing_engine and engine not in confirmed_by:
            confirmed_by.append(engine)
            confirmation_added = True
        current["confirmed_by"] = confirmed_by
        current["confirming_engines"] = confirmed_by
        current["confirmation_count"] = len(confirmed_by)
        current["consensus_count"] = max(
            int(current.get("consensus_count", 1) or 1),
            len({existing_engine, *confirmed_by}) if existing_engine else len(confirmed_by),
        )
        current["last_confirmed_at"] = datetime.now(timezone.utc).isoformat()
        if metadata:
            role_contrib = dict(current.get("role_contributions", {}) or {})
            for key, value in dict(metadata).items():
                if key == "role_contributions" and isinstance(value, dict):
                    role_contrib.update(value)
                elif key in {"confirmed_by", "confirming_engines"}:
                    merged = list(current.get("confirming_engines", []) or [])
                    for item in list(value or []):
                        item_u = str(item or "").upper()
                        if item_u and item_u not in merged and item_u != existing_engine:
                            merged.append(item_u)
                    current["confirmed_by"] = merged
                    current["confirming_engines"] = merged
                    current["confirmation_count"] = len(merged)
                    current["consensus_count"] = max(
                        int(current.get("consensus_count", 1) or 1),
                        len({existing_engine, *merged}) if existing_engine else len(merged),
                    )
                elif key == "selector_score":
                    current[key] = max(float(current.get(key, 0.0) or 0.0), float(value or 0.0))
                elif key == "booster_score":
                    current[key] = max(float(current.get(key, 0.0) or 0.0), float(value or 0.0))
                elif key == "position_strength":
                    current[key] = max(float(current.get(key, 0.0) or 0.0), float(value or 0.0))
                else:
                    current[key] = value
            if role_contrib:
                current["role_contributions"] = role_contrib
        _save()
        return {
            "coin": coin,
            "engine": existing_engine,
            "confirmation_added": confirmation_added,
            "confirmation_count": int(current.get("confirmation_count", 0) or 0),
            "confirmed_by": list(current.get("confirmed_by", []) or []),
        }


def update(
    coin: str,
    size_usdt: float | None = None,
    entry: float | None = None,
    leverage: int | None = None,
    stop_loss: float | None = None,
    take_profit: float | None = None,
    order_id: str | None = None,
    metadata: dict | None = None,
    require_persist: bool = False,
) -> bool:
    """
    Update fields in an existing registry entry after order fill.
    Called by category_engine once the actual fill price and contract count
    are known (especially after NANO routing may have changed the coin).
    """
    if not _healthy:
        logger.error("Registry update blocked while persisted state is unhealthy: %s", _load_error)
        return False
    coin = coin.upper()
    if coin not in _registry:
        logger.debug("Registry update: %s not found, skipping.", coin)
        return False
    before = dict(_registry[coin])
    before_size = float(_registry[coin].get("size_usdt", 0) or 0)
    before_entry = float(_registry[coin].get("entry", 0) or 0)
    if size_usdt    is not None: _registry[coin]["size_usdt"]   = size_usdt
    if entry        is not None: _registry[coin]["entry"]        = entry
    if leverage     is not None: _registry[coin]["leverage"]     = leverage
    if stop_loss    is not None: _registry[coin]["stop_loss"]    = stop_loss
    if take_profit  is not None: _registry[coin]["take_profit"]  = take_profit
    if order_id     is not None: _registry[coin]["order_id"]     = order_id
    lifecycle_id = derive_position_lifecycle_id(
        _registry[coin], order_id=order_id, metadata=metadata,
    )
    if metadata:
        _registry[coin].update(metadata)
    if lifecycle_id and not _usable_identity(_registry[coin].get("position_lifecycle_id")):
        _registry[coin]["position_lifecycle_id"] = lifecycle_id
    authoritative_leverage = _registry[coin].get("position_effective_leverage") or _registry[coin].get("exchange_configured_leverage")
    if authoritative_leverage:
        _registry[coin]["leverage"] = int(authoritative_leverage)
    if float(_registry[coin].get("entry", 0.0) or 0.0) > 0:
        _registry[coin].pop("pending_reservation", None)
        _registry[coin].pop("reserved_notional_usdt", None)
        _registry[coin].pop("reserved_at", None)
    try:
        persisted = _save(strict=require_persist)
        if persisted is False:
            _registry[coin] = before
            return False
    except Exception:
        _registry[coin] = before
        raise
    after_size = float(_registry[coin].get("size_usdt", 0) or 0)
    after_entry = float(_registry[coin].get("entry", 0) or 0)
    if (before_size <= 0 and after_size > 0) or (before_entry <= 0 and after_entry > 0):
        logger.info(
            "Registry activate %s %s [%s] $%.2f lev=%dx SL=%.6f TP=%.6f",
            coin,
            str(_registry[coin].get("direction", "")).upper(),
            _registry[coin].get("engine", ""),
            after_size,
            int(_registry[coin].get("leverage", 0) or 0),
            float(_registry[coin].get("stop_loss", 0) or 0),
            float(_registry[coin].get("take_profit", 0) or 0),
        )
    return True


def upsert_authoritative_position(
    coin: str,
    *,
    direction: str,
    engine: str,
    size_usdt: float,
    entry: float,
    leverage: int,
    stop_loss: float,
    take_profit: float,
    order_id: str,
    metadata: dict | None = None,
) -> bool:
    """Persist one exact exchange-confirmed position, creating it if absent.

    Recovery cannot safely treat a missing local registry row as proof that a
    surviving exchange position is gone.  This is deliberately replacement
    semantics, never quantity aggregation, and the strict save rolls the
    in-memory change back if persistence fails.
    """
    if not _healthy:
        logger.error("Authoritative registry upsert blocked while persisted state is unhealthy: %s", _load_error)
        return False
    coin = str(coin or "").upper()
    if not coin:
        raise ValueError("position coin is required")
    if coin in _registry:
        return update(
            coin, size_usdt=size_usdt, entry=entry, leverage=leverage,
            stop_loss=stop_loss, take_profit=take_profit, order_id=order_id,
            metadata=metadata, require_persist=True,
        )
    before = dict(_registry.get(coin, {}) or {})
    lifecycle_id = derive_position_lifecycle_id(order_id=order_id, metadata=metadata)
    row = {
        "direction": str(direction or "").lower(),
        "engine": str(engine or "RECOVERED"),
        "size_usdt": float(size_usdt or 0.0),
        "entry": float(entry or 0.0),
        "leverage": int(leverage or 1),
        "requested_leverage": int(leverage or 1),
        "exchange_configured_leverage": None,
        "position_effective_leverage": None,
        "leverage_truth_status": "PENDING",
        "leverage_truth_timestamp": "",
        "stop_loss": float(stop_loss or 0.0),
        "take_profit": float(take_profit or 0.0),
        "order_id": str(order_id or ""),
        "opened_at": datetime.now(timezone.utc).isoformat(),
        "execution_environment": _execution_environment(),
        "exposure_truth_status": EXPOSURE_ACTIVE,
        **_sync_defaults(),
        **dict(metadata or {}),
    }
    if lifecycle_id:
        row["position_lifecycle_id"] = lifecycle_id
    _registry[coin] = row
    try:
        _save(strict=True)
    except Exception:
        if before:
            _registry[coin] = before
        else:
            _registry.pop(coin, None)
        raise
    return True


def can_open(coin: str, direction: str) -> tuple[bool, str]:
    """Sync read-only check (no lock). Use only for pre-filtering."""
    if not _healthy:
        return False, f"position_registry_unavailable:{_load_error or 'unreadable_persisted_state'}"
    coin = coin.upper()
    if coin not in _registry or is_terminal_historical_position(_registry.get(coin)):
        return True, "OK"
    existing = _registry[coin]
    if not is_actionable_position(existing):
        return False, f"{coin} closure reconciliation pending."
    existing_direction = str(existing.get("direction", "unknown") or "unknown")
    existing_engine = str(existing.get("engine", "UNKNOWN") or "UNKNOWN")
    if existing_direction == direction:
        return False, f"{coin} already {direction.upper()} via {existing_engine}."
    return False, f"{coin} has OPPOSING {existing_direction.upper()} via {existing_engine}."


def register(
    coin: str, direction: str, engine: str,
    size_usdt: float = 0.0,
    entry: float = 0.0,
    leverage: int = 5,
    stop_loss: float = 0.0,
    take_profit: float = 0.0,
    order_id: str = "",
    metadata: dict | None = None,
) -> None:
    """Sync register — kept for compatibility. Prefer can_open_and_reserve()."""
    if not _healthy:
        logger.error("Registry register blocked while persisted state is unhealthy: %s", _load_error)
        return
    coin = coin.upper()
    opened_at = datetime.now(timezone.utc).isoformat()
    lifecycle_id = derive_position_lifecycle_id(order_id=order_id, metadata=metadata)
    _registry[coin] = {
        "direction":   direction,
        "engine":      engine,
        "size_usdt":   size_usdt,
        "entry":       entry,
        "leverage":    leverage,
        "requested_leverage": leverage,
        "exchange_configured_leverage": None,
        "position_effective_leverage": None,
        "leverage_truth_status": "PENDING",
        "leverage_truth_timestamp": "",
        "stop_loss":   stop_loss,
        "take_profit": take_profit,
        "order_id":    order_id,
        "opened_at":   opened_at,
        "execution_environment": _execution_environment(),
        "exposure_truth_status": EXPOSURE_ACTIVE,
        **_sync_defaults(),
    }
    if lifecycle_id:
        _registry[coin]["position_lifecycle_id"] = lifecycle_id
    if metadata:
        _registry[coin].update(metadata)
    _save()
    logger.info(
        "Registry +%s %s [%s] $%.2f",
        coin, direction.upper(), engine, size_usdt,
    )


def adopt_exchange_position(
    coin: str,
    position: dict,
    *,
    position_lifecycle_id: str,
    exchange_lifecycle_anchor: str,
    size_usdt: float = 0.0,
    metadata: dict | None = None,
) -> bool:
    """Persist a lifecycle for a legitimate exchange-only position."""
    if not _healthy:
        logger.error("Exchange-position adoption blocked while persisted state is unhealthy: %s", _load_error)
        return False
    coin = str(coin or "").upper()
    position = dict(position or {})
    try:
        quantity = abs(float(position.get("pos", position.get("availPos", 0.0)) or 0.0))
    except (TypeError, ValueError):
        quantity = 0.0
    side = str(position.get("posSide", "") or "").lower()
    if side not in {"long", "short"}:
        try:
            signed = float(position.get("pos", 0.0) or 0.0)
        except (TypeError, ValueError):
            signed = 0.0
        side = "long" if signed > 0 else "short"
    entry = float(position.get("avgPx", position.get("markPx", 0.0)) or 0.0)
    before = dict(_registry.get(coin, {}) or {})
    if coin in _registry:
        try:
            update(
                coin,
                metadata={
                    "position_lifecycle_id": str(position_lifecycle_id or ""),
                    "exchange_lifecycle_anchor": str(exchange_lifecycle_anchor or ""),
                    "exchange_adopted": True,
                    **dict(metadata or {}),
                },
                require_persist=True,
            )
            return True
        except Exception:
            return False
    _registry[coin] = {
        "direction": side,
        "engine": "EXCHANGE_ADOPTED",
        "size_usdt": float(size_usdt or 0.0),
        "entry": entry,
        "leverage": int(float(position.get("lever", 0) or 0)),
        "order_id": "",
        "opened_at": str(position.get("cTime", position.get("openTime", "")) or ""),
        "position_lifecycle_id": str(position_lifecycle_id or ""),
        "exchange_lifecycle_anchor": str(exchange_lifecycle_anchor or ""),
        "exchange_adopted": True,
        "entry_lifecycle": "EXCHANGE_ADOPTED",
        "mutation_attempted": False,
        "exchange_confirmed": True,
        "exposure_truth_status": EXPOSURE_ACTIVE,
        "confirmed_remaining_contracts": quantity,
        **_sync_defaults(),
        **dict(metadata or {}),
    }
    try:
        _save(strict=True)
    except Exception:
        if before:
            _registry[coin] = before
        else:
            _registry.pop(coin, None)
        return False
    return True


def risk_slot_intent(row: dict | None) -> str:
    """Return the immutable entry lineage used by the risk-slot ledger.

    Position accounting must never infer ownership from a symbol.  Legacy rows
    can participate only when they retain immutable order/lifecycle evidence.
    """
    row = dict(row or {})
    resulting_position = _usable_identity(row.get("risk_slot_intent_id"))
    if resulting_position:
        return resulting_position
    intent = _usable_identity(row.get("execution_intent_id"))
    if intent:
        return intent
    lifecycle = _usable_identity(row.get("position_lifecycle_id"))
    if not lifecycle:
        lifecycle = _usable_identity(row.get("origin_entry_order_id", row.get("order_id", "")))
    return f"legacy-position:{lifecycle}" if lifecycle else ""


def ensure_open_risk_slot(coin: str) -> bool:
    """Durably restore capacity for one known surviving registry position."""
    if not _healthy:
        logger.error("Registry risk-slot open blocked while persisted state is unhealthy: %s", _load_error)
        return False
    row = _registry.get(str(coin or "").upper())
    intent = risk_slot_intent(row)
    if not row or not intent:
        return False
    try:
        from risk_manager import risk
        risk.apply_slot_operation(f"risk-open:{intent}", "open")
    except Exception as exc:
        logger.error("Registry risk-slot open pending for %s: %s", coin, exc)
        return False
    return True


def close_authoritative_risk_slot(coin: str) -> bool:
    """Close the original entry slot after authoritative position-zero truth."""
    if not _healthy:
        logger.error("Registry risk-slot close blocked while persisted state is unhealthy: %s", _load_error)
        return False
    row = _registry.get(str(coin or "").upper())
    intent = risk_slot_intent(row)
    if not row or not intent:
        # A pre-ledger row without immutable lineage has no identifiable slot;
        # do not manufacture a symbol-derived close.
        return True
    try:
        from risk_manager import risk
        risk.apply_slot_operation(f"risk-close:{intent}", "close")
    except Exception as exc:
        logger.error("Registry risk-slot close pending for %s: %s", coin, exc)
        return False
    return True


def release(coin: str, *, authoritative_position_zero: bool = False) -> bool:
    """Remove a position only when the caller proved authoritative zero exposure.

    A registry row carries the original risk-slot lineage. Deleting it without
    exchange-zero proof would manufacture a close tombstone and could free a
    recovery-owned position. Pre-submit cleanup is the separate
    ``release_pre_submit_reservation`` operation.
    """
    if not _healthy:
        logger.error("Registry release blocked while persisted state is unhealthy: %s", _load_error)
        return False
    coin = coin.upper()
    if coin in _registry:
        if not authoritative_position_zero:
            logger.warning("Registry release denied for %s: authoritative position-zero proof required", coin)
            return False
        if not close_authoritative_risk_slot(coin):
            return False
        # The risk ledger has now durably installed a CLOSED tombstone.  Keep
        # the registry row in memory if its removal cannot be persisted, so a
        # later local reconciliation can retry the exact removal.  A startup
        # OPEN replay is harmless (the immutable ledger tombstone wins), but
        # losing the row here would hide the pending registry reconciliation.
        before = dict(_registry[coin])
        del _registry[coin]
        try:
            _save(strict=True)
        except Exception:
            _registry[coin] = before
            logger.exception("Registry close reconciliation pending for %s", coin)
            return False
        logger.info("Registry -%s released.", coin)
    return True


def release_pre_submit_reservation(coin: str) -> bool:
    """Idempotently release only an explicit never-submitted reservation."""
    if not _healthy:
        logger.error("Pre-submit registry release blocked while persisted state is unhealthy: %s", _load_error)
        return False
    coin = coin.upper()
    row = _registry.get(coin)
    if not is_pre_submit_reservation(row):
        return False
    del _registry[coin]
    if not _save():
        _registry[coin] = dict(row)
        return False
    logger.info("Registry pre-submit reservation -%s released.", coin)
    return True


async def release_uncommitted_pending_for_engine(
    engine: str,
    *,
    protected_coins: set[str] | None = None,
) -> list[str]:
    """Release reservations cancelled before an order intent became durable."""
    if not _healthy:
        logger.error("Pending-reservation cleanup blocked while persisted state is unhealthy: %s", _load_error)
        return []
    engine_name = str(engine or "").upper()
    protected = {str(coin or "").upper() for coin in (protected_coins or set())}
    async with _lock:
        removed = []
        for coin, row in list(_registry.items()):
            if not is_pre_submit_reservation(row):
                continue
            if str(row.get("engine", "") or "").upper() != engine_name:
                continue
            if coin.upper() in protected:
                continue
            removed.append(coin)
            del _registry[coin]
        if removed:
            _save()
            logger.warning(
                "Registry released %d uncommitted pending reservation(s) after %s cancellation: %s",
                len(removed), engine_name, ", ".join(sorted(removed)),
            )
        return sorted(removed)


def mark_sync_state(
    coin: str,
    sync_status: str,
    *,
    success_ts: str | None = None,
    seen_ts: str | None = None,
    absence_confirmations: int | None = None,
    error_reason: str | None = None,
) -> None:
    if not _healthy:
        logger.error("Registry sync-state update blocked while persisted state is unhealthy: %s", _load_error)
        return
    coin = coin.upper()
    if coin not in _registry:
        return
    payload = {"sync_status": _normalize_sync_status(sync_status)}
    if success_ts is not None:
        payload["last_successful_sync"] = str(success_ts or "")
    if seen_ts is not None:
        payload["last_seen_on_exchange"] = str(seen_ts or "")
    if absence_confirmations is not None:
        payload["exchange_absence_confirmations"] = int(absence_confirmations)
    if error_reason is not None:
        payload["last_sync_error"] = str(error_reason or "")
    _registry[coin].update(payload)
    _save()


def get_all() -> dict:
    """Return a copy of the registry (direction+engine only, for /scan command)."""
    _require_healthy()
    return {
        k: {"direction": v["direction"], "engine": v["engine"]}
        for k, v in _registry.items()
        if is_actionable_position(v)
    }


def get_all_details() -> dict:
    """Return full trade details for every open position — used by /positions."""
    _require_healthy()
    return dict(_registry)


def get_active_details() -> dict:
    """Return rows that may participate in active trading and lifecycle work."""
    _require_healthy()
    return {
        coin: dict(row)
        for coin, row in _registry.items()
        if is_actionable_position(row)
    }


def get_unknown_details() -> dict:
    """Return rows whose local exposure truth is unresolved."""
    _require_healthy()
    return {
        coin: dict(row)
        for coin, row in _registry.items()
        if is_unresolved_exposure(row)
    }


def get_restorable_details() -> dict:
    """Return only rows with explicit active exposure suitable for rehydration."""
    _require_healthy()
    return {
        coin: dict(row)
        for coin, row in _registry.items()
        if is_mutation_actionable_position(row)
    }


def get_nonterminal_details() -> dict:
    """Return rows retained for active work or exchange reconciliation."""
    _require_healthy()
    return {
        coin: dict(row)
        for coin, row in _registry.items()
        if not is_terminal_historical_position(row)
    }


def get_engine_coins(engine: str) -> list[str]:
    _require_healthy()
    return [c for c, v in _registry.items() if v.get("engine") == engine and is_actionable_position(v)]


def is_open(coin: str) -> bool:
    _require_healthy()
    return coin.upper() in _registry and is_actionable_position(_registry.get(coin.upper()))


def get_direction(coin: str) -> str | None:
    _require_healthy()
    entry = _registry.get(coin.upper())
    return entry["direction"] if entry else None


def get_engine(coin: str) -> str | None:
    _require_healthy()
    entry = _registry.get(coin.upper())
    return entry["engine"] if entry else None


def count() -> int:
    _require_healthy()
    return sum(1 for row in _registry.values() if is_actionable_position(row))


def engine_count(engine: str) -> int:
    _require_healthy()
    return sum(1 for v in _registry.values() if v.get("engine") == engine and is_actionable_position(v))


def direction_count(direction: str) -> int:
    _require_healthy()
    direction = direction.lower()
    return sum(
        1 for v in _registry.values()
        if str(v.get("direction", "")).lower() == direction and is_actionable_position(v)
    )


def direction_notional(direction: str) -> float:
    direction = direction.lower()
    total = 0.0
    for trade in _registry.values():
        if str(trade.get("direction", "")).lower() != direction:
            continue
        if not is_actionable_position(trade):
            continue
        total += float(trade.get("size_usdt", 0.0) or 0.0)
    return round(total, 4)


def clear_engine(engine: str) -> list[str]:
    removed = [
        c for c, v in list(_registry.items())
        if v.get("engine") == engine and is_actionable_position(v)
    ]
    for c in removed:
        del _registry[c]
    if removed:
        _save()
    return removed


def emergency_clear() -> None:
    global _registry
    _registry = {}
    _save()
    logger.warning("Registry emergency cleared.")


_load()

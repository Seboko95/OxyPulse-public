"""Canonical profit-taking ownership for a live position.

The exchange stop is independent of this choice.  These values describe only
which component is allowed to submit profit-taking closes.
"""

from __future__ import annotations

from datetime import datetime, timezone


LOCAL_STAGED = "LOCAL_STAGED"
EXCHANGE_FULL = "EXCHANGE_FULL"
NO_PROFIT_TP = "NO_PROFIT_TP"
UNRESOLVED = "UNRESOLVED"

VALID_TP_OWNERS = {LOCAL_STAGED, EXCHANGE_FULL, NO_PROFIT_TP}
VALID_TP_MODES = {"off", "reference_only", "exchange_tp", "hybrid"}


def normalize_tp_mode(value: str | None, default: str = "reference_only") -> str:
    mode = str(value or default).strip().lower()
    return mode if mode in VALID_TP_MODES else default


def normalize_persisted_owner(value: str | None) -> str:
    """Normalize persisted ownership without repairing corrupt state."""
    owner = str(value or "").strip().upper()
    if not owner:
        return ""
    return owner if owner in VALID_TP_OWNERS else UNRESOLVED


def _has_local_staged_evidence(details: dict) -> bool:
    if bool(details.get("local_tp1_enabled", False)) or bool(details.get("local_tp2_enabled", False)):
        return True
    if any(bool(details.get(key, False)) for key in ("tp1_done", "tp2_done", "tp1_deferred", "tp2_deferred")):
        return True
    if float(details.get("runner_core_pct", 0.0) or 0.0) > 0.0:
        return True
    if str(details.get("lifecycle_phase", "") or "") in {"runner_core", "meaningful_profit_secured"}:
        return True
    if dict(details.get("partial_exit_state", {}) or {}) or list(details.get("partial_exit_events", []) or []):
        return True
    return False


def _has_exchange_full_tp_evidence(details: dict) -> bool:
    if str(details.get("exchange_tp_algo_id", "") or "").strip():
        return True
    if str(details.get("tp_algo_id", "") or "").strip():
        return True
    if list(details.get("exchange_tp_algo_ids", []) or []):
        return True
    intent = dict(details.get("protection_intent", {}) or {})
    return float(intent.get("take_profit", intent.get("tp_price", 0.0)) or 0.0) > 0.0


def _protection_discovery_unknown(details: dict) -> bool:
    values = [
        str(details.get(key, "") or "").upper()
        for key in (
            "exchange_protection_status",
            "protection_lifecycle",
            "protection_resolution",
            "protection_reason",
        )
    ]
    return bool(details.get("protection_discovery_unknown", False)) or any(
        any(token in value for token in ("UNKNOWN", "READ_FAILED", "PENDING"))
        for value in values
    )


def resolve_legacy_owner(
    details: dict | None,
    *,
    mode_at_entry: str | None = None,
    partial_enabled: bool = True,
) -> str:
    """Resolve owner from durable evidence; never guess HYBRID as exchange TP."""
    details = dict(details or {})
    mode = normalize_tp_mode(mode_at_entry or details.get("tp_mode_at_entry", details.get("tp_mode", "")), default="")

    if _has_exchange_full_tp_evidence(details):
        return EXCHANGE_FULL
    if _protection_discovery_unknown(details):
        return UNRESOLVED
    if _has_local_staged_evidence(details):
        return LOCAL_STAGED

    # A historically persisted mode is stronger than the current global mode.
    if mode == "exchange_tp":
        return EXCHANGE_FULL
    if mode == "off":
        return NO_PROFIT_TP
    if mode == "reference_only":
        return LOCAL_STAGED if partial_enabled else NO_PROFIT_TP
    return UNRESOLVED


def derive_tp_owner(
    mode: str | None,
    *,
    partial_enabled: bool = True,
    legacy_position: bool = False,
) -> str:
    """Resolve one owner from the configured mode.

    New HYBRID positions use local staged exits whenever staged management is
    available.  A legacy position without a persisted owner is conservatively
    adopted as EXCHANGE_FULL in HYBRID mode so restart does not silently remove
    an already-attached exchange TP or start competing local closes.
    """
    normalized = normalize_tp_mode(mode)
    if normalized == "exchange_tp":
        return EXCHANGE_FULL
    if normalized == "hybrid":
        if legacy_position:
            return EXCHANGE_FULL
        return LOCAL_STAGED if partial_enabled else EXCHANGE_FULL
    if normalized == "reference_only":
        return LOCAL_STAGED if partial_enabled else NO_PROFIT_TP
    return NO_PROFIT_TP


def owner_flags(owner: str, *, partial_enabled: bool = True) -> tuple[bool, bool]:
    """Return local TP1/TP2 execution permissions for a position."""
    return (
        owner == LOCAL_STAGED and bool(partial_enabled),
        owner == LOCAL_STAGED and bool(partial_enabled),
    )


def effective_exchange_take_profit(owner: str, take_profit: float) -> float:
    """Return the only exchange TP allowed for this ownership state."""
    return float(take_profit or 0.0) if owner == EXCHANGE_FULL else 0.0


def ownership_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat()

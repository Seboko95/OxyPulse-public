"""Scale-aware lifecycle economics for OxyPulse exits.

This module is deliberately side-effect free.  It is the single place where
the exit manager turns current exposure, known costs, funding evidence and a
candidate protective price into an auditable economic decision.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any


MANDATORY_OR_PROTECTIVE = "MANDATORY_OR_PROTECTIVE"
OPTIONAL_PROFIT_CAPTURE = "OPTIONAL_PROFIT_CAPTURE"
CAPITAL_MANAGEMENT = "CAPITAL_MANAGEMENT"
ALREADY_EXCHANGE_CLOSED = "ALREADY_EXCHANGE_CLOSED"

NET_CAPTURE_HEALTHY = "NET_CAPTURE_HEALTHY"
MICRO_PROFIT_FEE_BLEED = "MICRO_PROFIT_FEE_BLEED"
NET_CAPTURE_UNKNOWN = "NET_CAPTURE_UNKNOWN"
MANDATORY_EXIT_NO_FEE_GATE = "MANDATORY_EXIT_NO_FEE_GATE"

ECONOMIC_EVIDENCE_AUTHORITATIVE = "AUTHORITATIVE"
ECONOMIC_EVIDENCE_ESTIMATED_CONSERVATIVE = "ESTIMATED_CONSERVATIVE"
ECONOMIC_EVIDENCE_INSUFFICIENT = "INSUFFICIENT"
# Compatibility aliases for callers/tests from the first scale-aware patch.
ECONOMIC_EVIDENCE_COMPLETE = ECONOMIC_EVIDENCE_AUTHORITATIVE
ECONOMIC_EVIDENCE_UNKNOWN = ECONOMIC_EVIDENCE_INSUFFICIENT
CRITICAL_EXPOSURE_UNKNOWN = "CRITICAL_EXPOSURE_UNKNOWN"
COST_COMPONENT_UNKNOWN = "COST_COMPONENT_UNKNOWN"

_KNOWN_FUNDING_STATUSES = {"SUCCESS_DATA", "SUCCESS_EMPTY", "KNOWN", "CONFIRMED", "PAID", "RECEIVED"}
_ESTIMATED_FUNDING_STATUS = "ESTIMATED_CONSERVATIVE"
_OPTIONAL_REASONS = {
    "tp", "tp_full", "tp1_hit", "tp2_hit", "target", "profit_target",
    "optional_profit_capture",
}
_CAPITAL_REASONS = {
    "stale", "stale_exit", "early_invalidation", "adaptive_early_invalidation",
    "slot_rotation",
}
_EXCHANGE_REASONS = {"tp_exchange", "sl_exchange", "exchange_closed", "sync_confirmed_close"}


def classify_full_close_reason(reason: str) -> str:
    """Return the safety class for a terminal full-close reason."""
    normalized = str(reason or "").strip().lower()
    if normalized in _EXCHANGE_REASONS or normalized.endswith("_exchange"):
        return ALREADY_EXCHANGE_CLOSED
    if normalized in _CAPITAL_REASONS:
        return CAPITAL_MANAGEMENT
    if normalized in _OPTIONAL_REASONS:
        return OPTIONAL_PROFIT_CAPTURE
    if any(token in normalized for token in (
        "hard_sl", "stop", "trailing", "emergency", "manual", "orphan",
        "liquidation", "unsafe", "risk", "protect",
    )):
        return MANDATORY_OR_PROTECTIVE
    return MANDATORY_OR_PROTECTIVE


def _abs_cost(value: float | None) -> float | None:
    return None if value is None else abs(float(value))


def _price_move_pnl(entry_price: float, price: float | None, notional: float, side: str) -> float | None:
    if price is None or entry_price <= 0.0 or notional < 0.0:
        return None
    move = ((float(price) - entry_price) / entry_price
            if str(side or "long").lower() in {"long", "buy"}
            else (entry_price - float(price)) / entry_price)
    return round(notional * move, 8)


@dataclass(frozen=True)
class PositionLifecycleEconomics:
    """Current and candidate-stop economics for the current position remainder."""

    original_entry_contracts: float | None
    current_remaining_contracts: float | None
    current_remaining_notional: float | None
    already_realized_partial_gross: float
    known_entry_fee: float | None
    known_partial_exit_fees: float | None
    estimated_remaining_exit_fee: float | None
    known_funding: float | None
    funding_status: str
    slippage_allowance: float | None
    side: str
    entry_price: float
    current_price: float | None
    candidate_stop_price: float | None
    current_gross_pnl: float | None
    current_estimated_net_pnl: float | None
    candidate_stop_gross_pnl: float | None
    candidate_stop_estimated_net_pnl: float | None
    total_known_and_estimated_cost: float | None
    economic_evidence_status: str
    evidence_class: str
    unknown_fields: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["unknown_fields"] = list(self.unknown_fields)
        return data


def build_position_lifecycle_economics(
    *,
    original_entry_contracts: float | None,
    current_remaining_contracts: float | None,
    current_remaining_notional: float | None,
    entry_price: float,
    side: str,
    current_price: float | None,
    candidate_stop_price: float | None,
    already_realized_partial_gross: float = 0.0,
    known_entry_fee: float | None,
    known_partial_exit_fees: float | None,
    estimated_remaining_exit_fee: float | None,
    known_funding: float | None,
    funding_status: str,
    slippage_allowance: float | None,
    entry_fee_evidence: str = "AUTHORITATIVE",
    funding_evidence: str = "AUTHORITATIVE",
) -> PositionLifecycleEconomics:
    """Build a complete/unknown lifecycle economics snapshot.

    ``None`` is intentionally different from zero.  In particular, a local
    notional cannot make an unknown current exchange quantity authoritative.
    """
    unknown: list[str] = []
    critical_unknown: list[str] = []
    estimated_cost = False
    contracts = None if current_remaining_contracts is None else float(current_remaining_contracts)
    notional = None if current_remaining_notional is None else float(current_remaining_notional)
    if contracts is None:
        unknown.append("remaining_contracts")
        critical_unknown.append("remaining_contracts")
    if notional is None or notional <= 0.0:
        unknown.append("remaining_notional")
        critical_unknown.append("remaining_notional")
    if float(entry_price or 0.0) <= 0.0:
        unknown.append("entry_price")
        critical_unknown.append("entry_price")
    if str(side or "").lower() not in {"long", "buy", "short", "sell"}:
        unknown.append("side")
        critical_unknown.append("side")
    if known_entry_fee is None:
        unknown.append("entry_fee")
    elif str(entry_fee_evidence or "").upper() == ECONOMIC_EVIDENCE_ESTIMATED_CONSERVATIVE:
        estimated_cost = True
    if known_partial_exit_fees is None:
        unknown.append("partial_exit_fees")
    if estimated_remaining_exit_fee is None:
        unknown.append("remaining_exit_fee")
    if slippage_allowance is None:
        unknown.append("slippage_allowance")

    funding_state = str(funding_status or "UNKNOWN").strip().upper()
    if funding_state == _ESTIMATED_FUNDING_STATUS:
        if known_funding is None:
            unknown.append("funding")
        else:
            estimated_cost = True
    elif funding_state not in _KNOWN_FUNDING_STATUSES:
        unknown.append("funding")
    elif funding_state != "SUCCESS_EMPTY" and known_funding is None:
        unknown.append("funding_amount")
    elif str(funding_evidence or "").upper() == ECONOMIC_EVIDENCE_ESTIMATED_CONSERVATIVE:
        estimated_cost = True

    current_gross_remaining = _price_move_pnl(float(entry_price or 0.0), current_price, float(notional or 0.0), side)
    candidate_gross_remaining = _price_move_pnl(float(entry_price or 0.0), candidate_stop_price, float(notional or 0.0), side)
    current_gross = None if current_gross_remaining is None else round(current_gross_remaining + float(already_realized_partial_gross or 0.0), 8)
    candidate_gross = None if candidate_gross_remaining is None else round(candidate_gross_remaining + float(already_realized_partial_gross or 0.0), 8)

    entry_fee = _abs_cost(known_entry_fee)
    partial_fees = _abs_cost(known_partial_exit_fees)
    exit_fee = _abs_cost(estimated_remaining_exit_fee)
    slippage = _abs_cost(slippage_allowance)
    funding = 0.0 if funding_state == "SUCCESS_EMPTY" else (None if known_funding is None else float(known_funding))
    total_cost = None
    current_net = None
    candidate_net = None
    if not unknown:
        total_cost = round(entry_fee + partial_fees + exit_fee + slippage + max(-funding, 0.0), 8)
        current_net = round(current_gross - entry_fee - partial_fees - exit_fee - slippage + funding, 8)
        if candidate_gross is not None:
            candidate_net = round(candidate_gross - entry_fee - partial_fees - exit_fee - slippage + funding, 8)

    return PositionLifecycleEconomics(
        original_entry_contracts=(None if original_entry_contracts is None else float(original_entry_contracts)),
        current_remaining_contracts=contracts,
        current_remaining_notional=notional,
        already_realized_partial_gross=float(already_realized_partial_gross or 0.0),
        known_entry_fee=known_entry_fee,
        known_partial_exit_fees=known_partial_exit_fees,
        estimated_remaining_exit_fee=estimated_remaining_exit_fee,
        known_funding=known_funding,
        funding_status=funding_state,
        slippage_allowance=slippage_allowance,
        side=str(side or "long"),
        entry_price=float(entry_price or 0.0),
        current_price=(None if current_price is None else float(current_price)),
        candidate_stop_price=(None if candidate_stop_price is None else float(candidate_stop_price)),
        current_gross_pnl=current_gross,
        current_estimated_net_pnl=current_net,
        candidate_stop_gross_pnl=candidate_gross,
        candidate_stop_estimated_net_pnl=candidate_net,
        total_known_and_estimated_cost=total_cost,
        economic_evidence_status=(
            ECONOMIC_EVIDENCE_INSUFFICIENT if unknown
            else (ECONOMIC_EVIDENCE_ESTIMATED_CONSERVATIVE if estimated_cost else ECONOMIC_EVIDENCE_AUTHORITATIVE)
        ),
        evidence_class=(
            CRITICAL_EXPOSURE_UNKNOWN if critical_unknown
            else (COST_COMPONENT_UNKNOWN if unknown or estimated_cost else "COMPLETE")
        ),
        unknown_fields=tuple(sorted(set(unknown))),
    )


def required_cost_relative_net(economics: PositionLifecycleEconomics, fee_multiple: float = 1.0) -> float | None:
    """Return the dynamic net floor; it scales with current costs, not size alone."""
    if economics.economic_evidence_status not in {
        ECONOMIC_EVIDENCE_AUTHORITATIVE,
        ECONOMIC_EVIDENCE_ESTIMATED_CONSERVATIVE,
    }:
        return None
    return round(max(float(economics.total_known_and_estimated_cost or 0.0), 0.0) * max(float(fee_multiple or 0.0), 0.0), 8)


def candidate_stop_is_economically_eligible(economics: PositionLifecycleEconomics, fee_multiple: float = 1.0) -> bool:
    floor = required_cost_relative_net(economics, fee_multiple)
    candidate = economics.candidate_stop_estimated_net_pnl
    if floor is None or candidate is None:
        return False
    if economics.current_price is not None and economics.candidate_stop_price is not None:
        if str(economics.side or "long").lower() in {"long", "buy"} and economics.candidate_stop_price >= economics.current_price:
            return False
        if str(economics.side or "long").lower() not in {"long", "buy"} and economics.candidate_stop_price <= economics.current_price:
            return False
    return candidate > 0.0 and candidate + 1e-12 >= floor


@dataclass(frozen=True)
class FullCloseDecision:
    status: str
    reason_class: str
    should_defer: bool
    estimated_net_capture: float | None
    gross_capture: float | None
    required_min_net_capture: float
    unknown_fields: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["unknown_fields"] = list(self.unknown_fields)
        return data


def _decision_from_values(
    *,
    reason: str,
    gross_capture: float | None,
    estimated_net_capture: float | None,
    required_min_net_capture: float,
    unknown_fields: tuple[str, ...] = (),
) -> FullCloseDecision:
    reason_class = classify_full_close_reason(reason)
    floor = max(float(required_min_net_capture or 0.0), 0.0)
    if reason_class != OPTIONAL_PROFIT_CAPTURE:
        return FullCloseDecision(MANDATORY_EXIT_NO_FEE_GATE, reason_class, False, None, None, floor)
    if unknown_fields:
        return FullCloseDecision(NET_CAPTURE_UNKNOWN, reason_class, False, None, gross_capture, floor, unknown_fields)
    if estimated_net_capture is not None and estimated_net_capture > 0.0 and estimated_net_capture + 1e-12 >= floor:
        return FullCloseDecision(NET_CAPTURE_HEALTHY, reason_class, False, estimated_net_capture, gross_capture, floor)
    if float(gross_capture or 0.0) > 0.0:
        return FullCloseDecision(MICRO_PROFIT_FEE_BLEED, reason_class, True, estimated_net_capture, gross_capture, floor)
    return FullCloseDecision(NET_CAPTURE_UNKNOWN, reason_class, False, estimated_net_capture, gross_capture, floor)


def evaluate_optional_full_close(
    *, reason: str, economics: PositionLifecycleEconomics, fee_multiple: float = 1.0,
) -> FullCloseDecision:
    """Evaluate optional profit capture using current remainder economics."""
    floor = required_cost_relative_net(economics, fee_multiple)
    if floor is None:
        floor = 0.0
    return _decision_from_values(
        reason=reason,
        gross_capture=economics.current_gross_pnl,
        estimated_net_capture=economics.current_estimated_net_pnl,
        required_min_net_capture=floor,
        unknown_fields=economics.unknown_fields if economics.economic_evidence_status == ECONOMIC_EVIDENCE_INSUFFICIENT else (),
    )


def evaluate_full_close_net_capture(
    *,
    reason: str,
    current_remaining_contracts: float | None,
    current_remaining_notional: float | None,
    gross_remaining_profit: float,
    already_realized_partial_gross: float = 0.0,
    known_entry_fee: float | None,
    known_partial_exit_fees: float | None,
    estimated_final_exit_fee: float | None,
    known_funding: float | None,
    funding_status: str,
    slippage_allowance: float | None,
    required_min_net_capture: float,
) -> FullCloseDecision:
    """Backward-compatible value-based wrapper used by older callers/tests.

    New runtime paths use :func:`build_position_lifecycle_economics` and
    :func:`evaluate_optional_full_close`; this wrapper retains the old pure API
    without coupling the runtime to an absolute setting.
    """
    reason_class = classify_full_close_reason(reason)
    unknown: list[str] = []
    if current_remaining_contracts is None:
        unknown.append("remaining_contracts")
    if current_remaining_notional is None or float(current_remaining_notional or 0.0) <= 0.0:
        unknown.append("remaining_notional")
    if known_entry_fee is None:
        unknown.append("entry_fee")
    if known_partial_exit_fees is None:
        unknown.append("partial_exit_fees")
    if estimated_final_exit_fee is None:
        unknown.append("final_exit_fee")
    funding_state = str(funding_status or "UNKNOWN").strip().upper()
    if funding_state == _ESTIMATED_FUNDING_STATUS:
        if known_funding is None:
            unknown.append("funding")
    elif funding_state not in _KNOWN_FUNDING_STATUSES:
        unknown.append("funding")
    elif funding_state != "SUCCESS_EMPTY" and known_funding is None:
        unknown.append("funding_amount")
    if slippage_allowance is None:
        unknown.append("slippage_allowance")

    gross = round(float(gross_remaining_profit or 0.0) + float(already_realized_partial_gross or 0.0), 8)
    if unknown:
        if "remaining_contracts" in unknown or "remaining_notional" in unknown:
            unknown.append("remaining_quantity")
        return _decision_from_values(
            reason=reason, gross_capture=gross, estimated_net_capture=None,
            required_min_net_capture=float(required_min_net_capture or 0.0),
            unknown_fields=tuple(sorted(set(unknown))),
        )
    funding = 0.0 if funding_state == "SUCCESS_EMPTY" else float(known_funding or 0.0)
    net = round(
        gross - abs(float(known_entry_fee or 0.0)) - abs(float(known_partial_exit_fees or 0.0))
        - abs(float(estimated_final_exit_fee or 0.0)) - abs(float(slippage_allowance or 0.0)) + funding,
        8,
    )
    return _decision_from_values(
        reason=reason, gross_capture=gross, estimated_net_capture=net,
        required_min_net_capture=float(required_min_net_capture or 0.0),
    )

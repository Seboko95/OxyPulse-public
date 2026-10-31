"""Authoritative final-placement executable minimum for OxyPulse.

This is the single source of truth for the notional floor used by:
  - capital_allocator (admission)
  - execution_router (pre-PAYLOAD_READY local validation)
  - okx_client place_market_order (final local guard)

It intentionally mirrors the historical place_market_order floor:
  max(get_effective_min_order_usdt(balance), configured_tier_min, instrument_min, 1.0)

When fresh contract metadata is available, callers may explicitly select the
contract-aware policy. That policy uses the true instrument minimum plus the
configured economic floor instead of an account-size target.

Do not invent a second formula elsewhere.
"""
from __future__ import annotations

from typing import Any


# Normalized funnel counter keys (final pre-submit taxonomy).
FUNNEL_COUNTER_KEYS: tuple[str, ...] = (
    "router_requested",
    "router_validated",
    "payload_ready",
    "rejected_zero_contracts",
    "rejected_min_notional",
    "rejected_candidate_stale",
    "rejected_market_stale",
    "rejected_account_state",
    "rejected_participation_cap",
    "rejected_reservation",
    "rejected_leverage",
    "rejected_position_mode",
    "rejected_economics_after_reprice",
    "rejected_other",
    "submission_attempted",
    "exchange_acknowledged",
    "exchange_rejected",
    "exchange_uncertain",
    "filled",
)

# Durable execution terminal/recovery taxonomy.  These labels intentionally do
# not encode a strategy decision; they make a completed (or explicitly pending)
# mutation attributable across restarts.
EXECUTION_TERMINAL_CLASSES: frozenset[str] = frozenset({
    "local_rejection",
    "exchange_rejection",
    "exchange_uncertain",
    "acknowledged_unfilled",
    "cancelled",
    "filled",
    "protection_failure",
    "reconciliation_terminal",
})


def _float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _configured_tier_min_trade_usdt(balance_usdt: float | None, settings: Any) -> float:
    """Resolve the account tier floor centrally so callers cannot omit it."""
    balance = _float(balance_usdt, 0.0)
    if balance <= 0.0 or not hasattr(settings, "get_tier"):
        return 0.0

    # Keep these bands aligned with risk_manager without importing it here;
    # risk_manager itself imports settings_store and is part of the placement path.
    tier_name = "NANO"
    if balance >= 1000.0:
        tier_name = "STANDARD"
    elif balance >= 200.0:
        tier_name = "SMALL"
    elif balance >= 50.0:
        tier_name = "MICRO"
    try:
        tier = settings.get_tier(tier_name) or {}
        return max(_float(tier.get("min_trade_usdt", 0.0), 0.0), 0.0)
    except Exception:
        return 0.0


def placement_min_notional_usdt(
    balance_usdt: float | None,
    *,
    tier_min_trade_usdt: float | None = None,
    instrument_min_notional_usdt: float = 0.0,
    settings: Any = None,
    absolute_floor_usdt: float = 1.0,
    contract_aware: bool = False,
) -> float:
    """Return the authoritative minimum notional for placement / admission.

    ``contract_aware`` is explicit so a caller cannot accidentally discard the
    account/tier policy. It is used only after instrument metadata is present;
    all capital, risk, margin, and final execution checks still apply.
    """
    if settings is None:
        import settings_store as settings  # type: ignore

    try:
        effective = float(settings.get_effective_min_order_usdt(balance_usdt))  # type: ignore[attr-defined]
    except Exception:
        try:
            effective = float(settings.get_val("v6_min_position_size_usdt", 25.0) or 25.0)
        except Exception:
            effective = 25.0

    supplied_tier_min = _float(tier_min_trade_usdt, 0.0)
    if contract_aware:
        return max(
            supplied_tier_min if supplied_tier_min > 0.0 else 0.0,
            _float(instrument_min_notional_usdt, 0.0),
            max(_float(absolute_floor_usdt, 1.0), 1.0),
        )

    configured_tier_min = _configured_tier_min_trade_usdt(balance_usdt, settings)
    return max(
        _float(effective, 1.0),
        configured_tier_min,
        supplied_tier_min,
        _float(instrument_min_notional_usdt, 0.0),
        max(_float(absolute_floor_usdt, 1.0), 1.0),
    )


def notional_meets_placement_floor(
    notional_usdt: float,
    min_notional_usdt: float,
    *,
    epsilon: float = 0.01,
) -> bool:
    return _float(notional_usdt) + max(_float(epsilon), 0.0) >= _float(min_notional_usdt)


def classify_pre_okx_reason(reason: str | None) -> str:
    """Map free-text skip / rejection reasons to normalized funnel counters."""
    text = str(reason or "").strip().lower()
    if not text:
        return "rejected_other"

    if any(token in text for token in (
        "zero_contract", "preflight_zero_contracts", "contracts=0", "returned 0 contracts",
        "quantity_below_min", "min_sz", "quantity_infeasible", "zero contracts",
    )):
        return "rejected_zero_contracts"

    if any(token in text for token in (
        "min_notional", "below_min_exec", "downsized_below_floor", "below floor",
        "below_floor", "allocator_below_min_exec", "min_order", "notional_below",
        "placement_floor", "executable_floor",
    )):
        return "rejected_min_notional"

    if any(token in text for token in (
        "candidate_expired", "candidate_stale", "signal_expired", "opportunity_expired",
    )):
        return "rejected_candidate_stale"

    if any(token in text for token in (
        "stale_execution_price", "stale_1m", "market_stale", "market_price_unavailable",
        "market_freshness", "stream_stale", "price_unavailable",
    )):
        return "rejected_market_stale"

    if any(token in text for token in (
        "participation_cap", "participation notional cap",
    )):
        return "rejected_participation_cap"

    if any(token in text for token in (
        "account_", "available_balance", "free_balance", "balance_non_positive",
        "account_revalidation", "no_available_margin", "position_truth",
        "account_truth_stale",
    )):
        return "rejected_account_state"

    if any(token in text for token in (
        "reservation", "capital_reservation",
    )):
        return "rejected_reservation"

    if any(token in text for token in (
        "leverage_out_of_bounds", "invalid_leverage", "leverage_",
    )):
        return "rejected_leverage"

    if any(token in text for token in (
        "position_mode", "invalid_position_mode",
    )):
        return "rejected_position_mode"

    if any(token in text for token in (
        "economics", "expectancy", "uneconomic", "spread", "slippage", "reprice",
        "execution economics",
    )):
        return "rejected_economics_after_reprice"

    return "rejected_other"


def classify_validation_reason(reason: str | None) -> str:
    """Alias used by the router validation path."""
    return classify_pre_okx_reason(reason)

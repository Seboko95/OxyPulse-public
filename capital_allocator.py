"""Central funded-capacity allocator for OxyPulse entries.

This module owns balance-adaptive checks that are not strategy signals:
minimum executable notional, per-position caps, usable margin, and aggregate
open stop-risk. It does not create signals and it never rounds size upward.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import executable_minimum
import micro_sizing
import settings_store as _s


@dataclass(frozen=True)
class InstrumentLimits:
    min_notional_usdt: float = 0.0
    max_notional_usdt: float = 0.0
    max_leverage: int = 0
    tick_size: float = 0.0
    lot_size: float = 0.0
    contract_value: float = 0.0
    contract_value_ccy: str = ""
    min_contracts: float = 0.0
    max_market_contracts: float = 0.0
    metadata_timestamp: float = 0.0
    state: str = "live"


@dataclass(frozen=True)
class AllocationDecision:
    allowed: bool
    final_size_usdt: float
    reason: str
    cap_applied: bool = False
    cap_source: str = ""
    participation_cap_usdt: float | None = None
    raw_size_usdt: float = 0.0
    min_exec_usdt: float = 0.0
    usable_margin_usdt: float = 0.0
    required_margin_usdt: float = 0.0
    projected_open_risk_pct: float = 0.0
    max_open_risk_pct: float = 0.0
    projected_margin_usdt: float = 0.0
    max_funded_positions: int = 0
    notes: list[str] = field(default_factory=list)
    mode: str = ""
    equity: float = 0.0
    available_margin: float = 0.0
    operational_reserve: float = 0.0
    deployable_margin: float = 0.0
    currently_reserved_margin: float = 0.0
    currently_deployed_margin: float = 0.0
    remaining_deployable_margin: float = 0.0
    requested_risk: float = 0.0
    approved_risk: float = 0.0
    approved_notional: float = 0.0
    raw_contracts: float = 0.0
    approved_contracts: float = 0.0
    minimum_executable_contracts: float = 0.0
    contract_floor_notional_usdt: float = 0.0
    contract_floor_uplift_applied: bool = False
    contract_floor_uplift_from_usdt: float = 0.0
    estimated_margin: float = 0.0
    estimated_entry_fee: float = 0.0
    estimated_exit_fee: float = 0.0
    aggregate_risk_before: float = 0.0
    aggregate_risk_after: float = 0.0
    binding_cap: str = ""
    reservation_id: str = ""
    metadata_timestamp: float = 0.0
    rejection_class: str = ""

    @property
    def approved(self) -> bool:
        return self.allowed

    def metadata(self) -> dict[str, Any]:
        equity = max(float(self.equity or 0.0), 0.0)
        approved_notional = float(self.approved_notional or self.final_size_usdt or 0.0)
        estimated_margin = float(self.estimated_margin or self.required_margin_usdt or 0.0)
        approved_risk = float(self.approved_risk or 0.0)
        return {
            "capital_allocator_enabled": True,
            "capital_allocator_allowed": bool(self.allowed),
            "capital_allocator_reason": self.reason,
            "allocator_raw_size_usdt": round(float(self.raw_size_usdt or 0.0), 6),
            "allocator_final_size_usdt": round(float(self.final_size_usdt or 0.0), 6),
            "allocator_min_exec_usdt": round(float(self.min_exec_usdt or 0.0), 6),
            "allocator_cap_applied": bool(self.cap_applied),
            "allocator_cap_source": str(self.cap_source or ""),
            "participation_cap_usdt": (
                round(float(self.participation_cap_usdt), 6)
                if self.participation_cap_usdt is not None else None
            ),
            "allocator_usable_margin_usdt": round(float(self.usable_margin_usdt or 0.0), 6),
            "allocator_required_margin_usdt": round(float(self.required_margin_usdt or 0.0), 6),
            "allocator_projected_margin_usdt": round(float(self.projected_margin_usdt or 0.0), 6),
            "allocator_projected_open_risk_pct": round(float(self.projected_open_risk_pct or 0.0), 6),
            "allocator_max_open_risk_pct": round(float(self.max_open_risk_pct or 0.0), 6),
            "allocator_max_funded_positions": int(self.max_funded_positions or 0),
            "allocator_notes": list(self.notes),
            "allocator_mode": self.mode,
            "allocator_equity": round(self.equity, 6),
            "allocator_available_margin": round(self.available_margin, 6),
            "allocator_operational_reserve": round(self.operational_reserve, 6),
            "allocator_deployable_margin": round(self.deployable_margin, 6),
            "allocator_currently_reserved_margin": round(self.currently_reserved_margin, 6),
            "allocator_currently_deployed_margin": round(self.currently_deployed_margin, 6),
            "allocator_remaining_deployable_margin": round(self.remaining_deployable_margin, 6),
            "allocator_requested_risk": round(self.requested_risk, 6),
            "allocator_approved_risk": round(self.approved_risk, 6),
            "allocator_position_notional_usdt": round(approved_notional, 6),
            "allocator_isolated_margin_required_usdt": round(estimated_margin, 6),
            "allocator_risk_at_stop_usdt": round(approved_risk, 6),
            "allocator_risk_pct_equity": round(approved_risk / equity * 100.0, 6) if equity > 0 else None,
            "allocator_margin_pct_equity": round(estimated_margin / equity * 100.0, 6) if equity > 0 else None,
            "allocator_notional_pct_equity": round(approved_notional / equity * 100.0, 6) if equity > 0 else None,
            "allocator_position_cap_basis": "notional_pct_equity",
            "allocator_raw_contracts": round(self.raw_contracts, 8),
            "allocator_approved_contracts": round(self.approved_contracts, 8),
            "allocator_minimum_executable_contracts": round(self.minimum_executable_contracts, 8),
            "allocator_contract_floor_notional_usdt": round(self.contract_floor_notional_usdt, 8),
            "allocator_contract_floor_uplift_applied": bool(self.contract_floor_uplift_applied),
            "allocator_contract_floor_uplift_from_usdt": round(self.contract_floor_uplift_from_usdt, 8),
            "allocator_estimated_entry_fee": round(self.estimated_entry_fee, 8),
            "allocator_estimated_exit_fee": round(self.estimated_exit_fee, 8),
            "allocator_binding_cap": self.binding_cap,
            "allocator_reservation_id": self.reservation_id,
            "allocator_metadata_timestamp": self.metadata_timestamp,
            "allocator_rejection_class": self.rejection_class,
        }


def _float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _stop_dist_pct(entry: float, stop: float, metadata: dict | None) -> float:
    meta = metadata or {}
    risk_meta = meta.get("risk_sizing") if isinstance(meta.get("risk_sizing"), dict) else {}
    sl_dist_pct = _float(meta.get("sl_dist_pct", 0.0))
    if sl_dist_pct <= 0:
        sl_dist_pct = _float(risk_meta.get("sl_dist_pct", 0.0))
    if sl_dist_pct <= 0 and entry > 0 and stop > 0:
        sl_dist_pct = abs(entry - stop) / entry * 100.0
    if sl_dist_pct <= 0:
        sl_dist_pct = _float(_s.get_val("min_sl_dist_pct", 1.0), 1.0)
    return max(sl_dist_pct, 0.0)


def _trade_risk_pct(size_usdt: float, entry: float, stop: float, balance: float, metadata: dict | None) -> float:
    if balance <= 0 or size_usdt <= 0:
        return 0.0
    sl_dist_pct = _stop_dist_pct(entry, stop, metadata)
    return (size_usdt * sl_dist_pct / 100.0) / balance * 100.0


def _open_risk_pct(open_positions: dict[str, dict], balance: float) -> float:
    return sum(
        _trade_risk_pct(
            _float(pos.get("size_usdt", pos.get("size", 0.0))),
            _float(pos.get("entry", pos.get("entry_price", 0.0))),
            _float(pos.get("stop_loss", pos.get("stop_price", 0.0))),
            balance,
            pos,
        )
        for pos in (open_positions or {}).values()
    )


def _open_margin_usdt(open_positions: dict[str, dict]) -> float:
    total = 0.0
    for pos in (open_positions or {}).values():
        size = _float(pos.get("size_usdt", pos.get("size", 0.0)))
        lev = max(int(_float(pos.get("leverage", _s.get_val("leverage_default", 10)), 10)), 1)
        total += size / lev
    return total


def _floor_down_usdt(size_usdt: float) -> float:
    return max(round(float(size_usdt or 0.0), 2), 0.0)


def allocate_entry(
    *,
    balance_usdt: float,
    requested_size_usdt: float,
    entry_price: float,
    stop_price: float,
    leverage: int,
    open_positions: dict[str, dict] | None = None,
    metadata: dict | None = None,
    instrument: InstrumentLimits | None = None,
    instrument_metadata: Any | None = None,
    currently_reserved_margin_usdt: float = 0.0,
    available_margin_usdt: float | None = None,
    deployment_mode: str = "",
    taker_fee_rate: float | None = None,
    ignore_position_size_pct_cap: bool = False,
    contract_aware_minimum: bool | None = None,
    participation_cap_usdt: float | None = None,
) -> AllocationDecision:
    """Return a funded-capacity decision for one candidate entry."""
    participation_cap_present = participation_cap_usdt is not None
    participation_cap = max(_float(participation_cap_usdt), 0.0)
    if participation_cap_present and participation_cap <= 0.0:
        return AllocationDecision(
            False,
            0.0,
            "participation_cap_exceeded cap=$0.0000",
            raw_size_usdt=max(_float(requested_size_usdt), 0.0),
            participation_cap_usdt=participation_cap,
            rejection_class="temporary_lack_of_capital",
        )
    if not bool(_s.get_val("adaptive_capital_allocator_enabled", True)):
        size = _floor_down_usdt(requested_size_usdt)
        if participation_cap_present:
            size = min(size, participation_cap)
        return AllocationDecision(
            True,
            size,
            "allocator_disabled",
            raw_size_usdt=size,
            participation_cap_usdt=participation_cap if participation_cap_present else None,
        )

    balance = max(_float(balance_usdt), 0.0)
    requested = max(_float(requested_size_usdt), 0.0)
    entry = _float(entry_price)
    stop = _float(stop_price)
    lev = max(int(_float(leverage, _s.get_val("leverage_default", 10))), 1)
    open_map = dict(open_positions or {})
    limits = instrument or InstrumentLimits()
    exchange_meta = instrument_metadata
    notes: list[str] = []
    contract_aware = (
        bool(_s.get_val("contract_aware_execution_floor_enabled", False))
        if contract_aware_minimum is None
        else bool(contract_aware_minimum)
    )

    if balance <= 0 or requested <= 0 or entry <= 0 or stop <= 0:
        return AllocationDecision(False, 0.0, "allocator_invalid_inputs", raw_size_usdt=requested, rejection_class="execution_incompatibility")

    if exchange_meta is not None:
        ok, eligibility_reason = exchange_meta.eligible()
        if not ok:
            return AllocationDecision(False, 0.0, eligibility_reason, raw_size_usdt=requested, rejection_class="execution_incompatibility")
        if exchange_meta.stale():
            return AllocationDecision(False, 0.0, "stale_instrument_metadata", raw_size_usdt=requested, rejection_class="stale_metadata")
        contract_notional = exchange_meta.contract_notional(entry)
        limits = InstrumentLimits(
            min_notional_usdt=float(exchange_meta.min_sz) * contract_notional,
            max_notional_usdt=float(exchange_meta.max_mkt_sz) * contract_notional if exchange_meta.max_mkt_sz else 0.0,
            max_leverage=int(exchange_meta.max_leverage or 0),
            tick_size=float(exchange_meta.tick_sz),
            lot_size=float(exchange_meta.lot_sz),
            contract_value=float(exchange_meta.ct_val),
            contract_value_ccy=str(exchange_meta.ct_val_ccy),
            min_contracts=float(exchange_meta.min_sz),
            max_market_contracts=float(exchange_meta.max_mkt_sz),
            metadata_timestamp=float(exchange_meta.refreshed_at),
            state=str(exchange_meta.state),
        )

    # Authoritative placement floor (must match okx_client final local guard).
    # Instrument min + economic floor remain components of the same helper.
    min_exec = executable_minimum.placement_min_notional_usdt(
        balance,
        instrument_min_notional_usdt=_float(limits.min_notional_usdt),
        settings=_s,
        absolute_floor_usdt=_float(_s.get_val("adaptive_min_economic_notional_usdt", 1.0), 1.0),
        contract_aware=bool(exchange_meta is not None and contract_aware),
    )
    notes.append(
        f"placement_min_exec ${min_exec:.4f}"
        + (" contract_aware" if exchange_meta is not None and contract_aware else " account_floor")
    )
    cfg_cap = _float(_s.get_val("position_size_cap_usdt", 250.0), 250.0)
    pct_cap = balance * _float(_s.get_val("position_size_cap_pct_balance", 100.0), 100.0) / 100.0
    if ignore_position_size_pct_cap:
        max_size = cfg_cap
        cap_source = "position_size_cap_usdt"
        notes.append("research_shadow_ignored_position_size_cap_pct_balance")
    else:
        max_size = min(cfg_cap, pct_cap) if cfg_cap > 0 and pct_cap > 0 else max(cfg_cap, pct_cap)
        cap_source = "position_size_cap_pct_balance" if pct_cap <= cfg_cap else "position_size_cap_usdt"
    if limits.max_notional_usdt > 0 and (max_size <= 0 or limits.max_notional_usdt < max_size):
        max_size = limits.max_notional_usdt
        cap_source = "instrument_max_notional_usdt"
    if participation_cap_present and (max_size <= 0 or participation_cap < max_size):
        max_size = participation_cap
        cap_source = "participation_cap_usdt"

    size = requested
    cap_applied = False
    if max_size > 0 and size > max_size:
        notes.append(f"per_position_cap {size:.2f}->{max_size:.2f}")
        size = max_size
        cap_applied = True

    usable_margin = _float(micro_sizing.usable_margin_usdt(balance, _s))
    operational_reserve = max(balance - usable_margin, 0.0)
    open_margin = _open_margin_usdt(open_map)
    reserved_margin = max(_float(currently_reserved_margin_usdt), 0.0)
    exchange_available = usable_margin if available_margin_usdt is None else max(_float(available_margin_usdt), 0.0)
    deployable_margin = min(usable_margin, exchange_available)
    available_margin = max(deployable_margin - open_margin - reserved_margin, 0.0)
    margin_cap_size = available_margin * lev / 1.05
    if margin_cap_size > 0 and size > margin_cap_size:
        reduced = _floor_down_usdt(margin_cap_size)
        notes.append(f"margin_cap {size:.2f}->{reduced:.2f}")
        size = reduced
        cap_applied = True
        cap_source = "available_margin_usdt"
    elif margin_cap_size <= 0:
        return AllocationDecision(
            False,
            0.0,
            "allocator_no_available_margin",
            raw_size_usdt=requested,
            min_exec_usdt=min_exec,
            usable_margin_usdt=usable_margin,
            projected_margin_usdt=open_margin,
            notes=notes,
            rejection_class="temporary_lack_of_capital",
        )

    size = _floor_down_usdt(size)

    # Hard upper bound: floor-preserve may only raise size up to these limits.
    hard_upper = float("inf")
    if max_size > 0:
        hard_upper = min(hard_upper, max_size)
    if participation_cap_present:
        hard_upper = min(hard_upper, participation_cap)
    if margin_cap_size > 0:
        hard_upper = min(hard_upper, margin_cap_size)

    max_risk = _float(_s.get_val("rule_357_max_open_risk_pct", 12.0), 12.0)
    open_risk = _open_risk_pct(open_map, balance)
    risk_enabled = bool(_s.get_val("rule_357_enabled", True)) and max_risk > 0
    remaining_risk_pct = max(max_risk - open_risk, 0.0) if risk_enabled else float("inf")
    sl_dist = _stop_dist_pct(entry, stop, metadata)
    risk_cap_size = float("inf")
    if risk_enabled and remaining_risk_pct > 0 and sl_dist > 0 and balance > 0:
        # trade_risk_pct = size * sl_dist / balance  (see _trade_risk_pct)
        risk_cap_size = remaining_risk_pct * balance / sl_dist
        hard_upper = min(hard_upper, risk_cap_size)
    elif risk_enabled and remaining_risk_pct <= 0:
        risk_cap_size = 0.0
        hard_upper = 0.0
    if hard_upper == float("inf"):
        hard_upper = max(size, min_exec, 0.0)

    if size + 0.01 < min_exec:
        # Trade-flow preserve: raise only to the shared placement floor when every
        # existing hard limit still allows that minimum executable notional.
        floor_fits = (
            min_exec > 0
            and hard_upper + 1e-9 >= min_exec
            and margin_cap_size + 1e-9 >= min_exec
            and (max_size <= 0 or max_size + 1e-9 >= min_exec)
        )
        if floor_fits and risk_enabled:
            floor_risk = _trade_risk_pct(min_exec, entry, stop, balance, metadata)
            if open_risk + floor_risk > max_risk + 1e-9:
                floor_fits = False
                notes.append(
                    f"floor_preserve_blocked_by_risk floor_risk={floor_risk:.4f}% "
                    f"open={open_risk:.4f}% max={max_risk:.4f}%"
                )
        if floor_fits:
            notes.append(f"floor_preserve {size:.4f}->{min_exec:.4f}")
            size = _floor_down_usdt(min_exec)
            cap_applied = True
            cap_source = "placement_floor_preserve"
        else:
            binding = "min_exec"
            if participation_cap_present and participation_cap + 1e-9 < min_exec:
                binding = "participation_cap_usdt"
            elif max_size > 0 and max_size + 1e-9 < min_exec:
                binding = "position_or_instrument_cap"
            elif margin_cap_size + 1e-9 < min_exec:
                binding = "available_margin"
            elif risk_enabled and risk_cap_size + 1e-9 < min_exec:
                binding = "open_risk_cap"
            return AllocationDecision(
                False,
                size,
                f"allocator_below_min_exec size=${size:.2f} < min=${min_exec:.2f} binding={binding}",
                cap_applied=cap_applied,
                cap_source=cap_source,
                raw_size_usdt=requested,
                min_exec_usdt=min_exec,
                usable_margin_usdt=usable_margin,
                required_margin_usdt=size / lev,
                projected_margin_usdt=open_margin + size / lev,
                projected_open_risk_pct=open_risk + _trade_risk_pct(size, entry, stop, balance, metadata),
                max_open_risk_pct=max_risk,
                participation_cap_usdt=participation_cap if participation_cap_present else None,
                notes=notes,
                rejection_class="execution_incompatibility",
            )

    raw_contracts = 0.0
    approved_contracts = 0.0
    minimum_executable_contracts = 0.0
    contract_floor_notional = 0.0
    contract_floor_uplift_applied = False
    contract_floor_uplift_from = 0.0
    if exchange_meta is not None:
        raw_contracts = exchange_meta.notional_to_raw_contracts(size, entry)
        approved_contracts = exchange_meta.round_contracts_down(raw_contracts)
        minimum_executable_contracts = exchange_meta.minimum_executable_contracts(min_exec, entry)
        contract_floor_notional = exchange_meta.contracts_to_notional(minimum_executable_contracts, entry)
        feasible, feasibility_reason = exchange_meta.quantity_feasible(approved_contracts, "market")
        if contract_aware and (
            size + 0.01 < min_exec
            or not feasible
            or not executable_minimum.notional_meets_placement_floor(
                exchange_meta.contracts_to_notional(approved_contracts, entry), min_exec
            )
        ):
            # Uplift only to the smallest exchange-valid quantity. It is still
            # rejected if any configured cap, margin, or risk ceiling disallows it.
            uplift_notional = contract_floor_notional
            if (
                minimum_executable_contracts <= 0
                or (participation_cap_present and uplift_notional > participation_cap + 1e-9)
                or uplift_notional > hard_upper + 1e-9
                or not exchange_meta.quantity_feasible(minimum_executable_contracts, "market")[0]
            ):
                if participation_cap_present and uplift_notional > participation_cap + 1e-9:
                    return AllocationDecision(
                        False,
                        size,
                        f"participation_cap_exceeded minimum_notional=${uplift_notional:.4f} cap=${participation_cap:.4f}",
                        cap_applied=cap_applied,
                        cap_source="participation_cap_usdt",
                        raw_size_usdt=requested,
                        raw_contracts=raw_contracts,
                        approved_contracts=approved_contracts,
                        min_exec_usdt=min_exec,
                        participation_cap_usdt=participation_cap,
                        minimum_executable_contracts=minimum_executable_contracts,
                        contract_floor_notional_usdt=contract_floor_notional,
                        metadata_timestamp=float(exchange_meta.refreshed_at),
                        rejection_class="temporary_lack_of_capital",
                    )
                return AllocationDecision(
                    False, size, feasibility_reason or "allocator_contract_floor_unavailable",
                    raw_size_usdt=requested, raw_contracts=raw_contracts,
                    approved_contracts=approved_contracts, min_exec_usdt=min_exec,
                    participation_cap_usdt=participation_cap if participation_cap_present else None,
                    minimum_executable_contracts=minimum_executable_contracts,
                    contract_floor_notional_usdt=contract_floor_notional,
                    metadata_timestamp=float(exchange_meta.refreshed_at),
                    rejection_class="execution_incompatibility",
                )
            contract_floor_uplift_from = size
            size = uplift_notional
            approved_contracts = minimum_executable_contracts
            contract_floor_uplift_applied = True
            cap_applied = True
            cap_source = "contract_floor_uplift"
            notes.append(
                f"contract_floor_uplift ${contract_floor_uplift_from:.6f}->${size:.6f} "
                f"contracts={approved_contracts:.8f}"
            )
            candidate_uplift_risk = _trade_risk_pct(size, entry, stop, balance, metadata)
            if risk_enabled and open_risk + candidate_uplift_risk > max_risk + 1e-9:
                return AllocationDecision(
                    False, size, "allocator_contract_floor_uplift_breaches_open_risk",
                    cap_applied=cap_applied, cap_source=cap_source, raw_size_usdt=requested,
                    min_exec_usdt=min_exec, projected_open_risk_pct=open_risk + candidate_uplift_risk,
                    max_open_risk_pct=max_risk, notes=notes, raw_contracts=raw_contracts,
                    participation_cap_usdt=participation_cap if participation_cap_present else None,
                    approved_contracts=approved_contracts,
                    minimum_executable_contracts=minimum_executable_contracts,
                    contract_floor_notional_usdt=contract_floor_notional,
                    contract_floor_uplift_applied=True,
                    contract_floor_uplift_from_usdt=contract_floor_uplift_from,
                    rejection_class="strategy_risk_cap",
                )
            # The allocator normally receives no strategy expectancy model. For
            # an uplift, however, require a supplied target so the additional
            # contracts cannot be approved on fees alone. The full final
            # economics gate remains authoritative after allocation.
            target = _float((metadata or {}).get("tp2_price", (metadata or {}).get("take_profit", 0.0)))
            fee_rate = max(
                _float(taker_fee_rate, _s.get_val("live_taker_fee_rate", 0.0005))
                if taker_fee_rate is not None
                else _float(_s.get_val("live_taker_fee_rate", 0.0005), 0.0005),
                0.0,
            )
            expected_slippage_pct = max(
                _float((metadata or {}).get("expected_slippage_pct", _s.get_val("net_expectancy_default_slippage_pct", 0.03)), 0.03),
                0.0,
            )
            if target <= 0.0:
                return AllocationDecision(
                    False, size, "allocator_contract_floor_economics_unknown",
                    cap_applied=cap_applied, cap_source=cap_source, raw_size_usdt=requested,
                    min_exec_usdt=min_exec, raw_contracts=raw_contracts,
                    participation_cap_usdt=participation_cap if participation_cap_present else None,
                    approved_contracts=approved_contracts,
                    minimum_executable_contracts=minimum_executable_contracts,
                    contract_floor_notional_usdt=contract_floor_notional,
                    contract_floor_uplift_applied=True,
                    contract_floor_uplift_from_usdt=contract_floor_uplift_from,
                    notes=notes, rejection_class="execution_incompatibility",
                )
            gross_capture = size * abs(target - entry) / entry
            estimated_cost = size * (2.0 * fee_rate + 2.0 * expected_slippage_pct / 100.0)
            if gross_capture <= estimated_cost + 1e-9:
                return AllocationDecision(
                    False, size, "allocator_contract_floor_fee_negative",
                    cap_applied=cap_applied, cap_source=cap_source, raw_size_usdt=requested,
                    min_exec_usdt=min_exec, raw_contracts=raw_contracts,
                    participation_cap_usdt=participation_cap if participation_cap_present else None,
                    approved_contracts=approved_contracts,
                    minimum_executable_contracts=minimum_executable_contracts,
                    contract_floor_notional_usdt=contract_floor_notional,
                    contract_floor_uplift_applied=True,
                    contract_floor_uplift_from_usdt=contract_floor_uplift_from,
                    notes=notes + [
                        f"uplift_economics gross=${gross_capture:.6f} cost=${estimated_cost:.6f}"
                    ], rejection_class="execution_incompatibility",
                )
            notes.append(f"uplift_economics net=${gross_capture - estimated_cost:.6f}")
            feasible, feasibility_reason = exchange_meta.quantity_feasible(approved_contracts, "market")
        if not feasible:
            return AllocationDecision(
                False, 0.0, feasibility_reason, raw_size_usdt=requested,
                raw_contracts=raw_contracts, approved_contracts=approved_contracts,
                min_exec_usdt=min_exec, metadata_timestamp=float(exchange_meta.refreshed_at),
                participation_cap_usdt=participation_cap if participation_cap_present else None,
                minimum_executable_contracts=minimum_executable_contracts,
                contract_floor_notional_usdt=contract_floor_notional,
                contract_floor_uplift_applied=contract_floor_uplift_applied,
                contract_floor_uplift_from_usdt=contract_floor_uplift_from,
                rejection_class="execution_incompatibility",
            )
        rounded_notional = exchange_meta.contracts_to_notional(approved_contracts, entry)
        if rounded_notional + 1e-9 < size:
            notes.append(f"lot_round_down {size:.6f}->{rounded_notional:.6f}")
            size = rounded_notional
            cap_applied = True
            if cap_source != "placement_floor_preserve":
                cap_source = "instrument_lot_size"
        if approved_contracts <= 0:
            return AllocationDecision(
                False, size,
                f"allocator_zero_contracts after_lot_round raw={raw_contracts:.8f}",
                raw_size_usdt=requested,
                raw_contracts=raw_contracts,
                approved_contracts=approved_contracts,
                min_exec_usdt=min_exec,
                participation_cap_usdt=participation_cap if participation_cap_present else None,
                metadata_timestamp=float(exchange_meta.refreshed_at),
                minimum_executable_contracts=minimum_executable_contracts,
                contract_floor_notional_usdt=contract_floor_notional,
                contract_floor_uplift_applied=contract_floor_uplift_applied,
                contract_floor_uplift_from_usdt=contract_floor_uplift_from,
                rejection_class="execution_incompatibility",
            )
        # Never round contracts upward to force the floor; reject if still short.
        if not executable_minimum.notional_meets_placement_floor(size, min_exec):
            return AllocationDecision(
                False, size,
                f"allocator_below_min_exec_after_lot_round size=${size:.4f} < min=${min_exec:.4f}",
                raw_size_usdt=requested,
                raw_contracts=raw_contracts,
                approved_contracts=approved_contracts,
                min_exec_usdt=min_exec,
                metadata_timestamp=float(exchange_meta.refreshed_at),
                minimum_executable_contracts=minimum_executable_contracts,
                contract_floor_notional_usdt=contract_floor_notional,
                contract_floor_uplift_applied=contract_floor_uplift_applied,
                contract_floor_uplift_from_usdt=contract_floor_uplift_from,
                rejection_class="execution_incompatibility",
            )
        if participation_cap_present and size > participation_cap + 1e-9:
            return AllocationDecision(
                False,
                size,
                f"participation_cap_exceeded final_notional=${size:.4f} cap=${participation_cap:.4f}",
                cap_applied=cap_applied,
                cap_source="participation_cap_usdt",
                raw_size_usdt=requested,
                raw_contracts=raw_contracts,
                approved_contracts=approved_contracts,
                min_exec_usdt=min_exec,
                participation_cap_usdt=participation_cap,
                minimum_executable_contracts=minimum_executable_contracts,
                contract_floor_notional_usdt=contract_floor_notional,
                metadata_timestamp=float(exchange_meta.refreshed_at),
                rejection_class="temporary_lack_of_capital",
            )

    candidate_risk = _trade_risk_pct(size, entry, stop, balance, metadata)
    projected_risk = open_risk + candidate_risk
    if risk_enabled and projected_risk > max_risk + 1e-9:
        return AllocationDecision(
            False,
            size,
            f"allocator_open_risk_cap projected={projected_risk:.2f}% > {max_risk:.2f}%",
            cap_applied=cap_applied,
            cap_source=cap_source,
            raw_size_usdt=requested,
            min_exec_usdt=min_exec,
            usable_margin_usdt=usable_margin,
            required_margin_usdt=size / lev,
            projected_margin_usdt=open_margin + size / lev,
            projected_open_risk_pct=projected_risk,
            max_open_risk_pct=max_risk,
            participation_cap_usdt=participation_cap if participation_cap_present else None,
            notes=notes,
            rejection_class="strategy_risk_cap",
        )

    max_funded_positions = 0
    if min_exec > 0 and lev > 0:
        max_funded_positions = int(max((usable_margin * lev / 1.05) // min_exec, 0))

    if taker_fee_rate is None:
        try:
            from live_fee_schedule import current as _current_fee

            taker_fee_rate = _current_fee(execution_type="market")[0]
        except Exception:
            taker_fee_rate = _s.get_val("live_taker_fee_rate", 0.0005)
    fee_rate = max(_float(taker_fee_rate, _s.get_val("live_taker_fee_rate", 0.0005)), 0.0)
    entry_fee = size * fee_rate
    exit_fee = size * fee_rate
    requested_risk = requested * _stop_dist_pct(entry, stop, metadata) / 100.0
    approved_risk = size * _stop_dist_pct(entry, stop, metadata) / 100.0
    reason = "allocator_ok"
    if cap_applied:
        reason = f"allocator_scaled_by_{cap_source}"
    return AllocationDecision(
        True,
        size,
        reason,
        cap_applied=cap_applied,
        cap_source=cap_source if cap_applied else "",
        raw_size_usdt=requested,
        min_exec_usdt=min_exec,
        usable_margin_usdt=usable_margin,
        required_margin_usdt=size / lev,
        projected_margin_usdt=open_margin + size / lev,
        projected_open_risk_pct=projected_risk,
        max_open_risk_pct=max_risk,
        max_funded_positions=max_funded_positions,
        notes=notes,
        mode=str(deployment_mode or _s.get_val("deployment_mode", "Guarded")),
        equity=balance,
        available_margin=exchange_available,
        operational_reserve=operational_reserve,
        deployable_margin=deployable_margin,
        currently_reserved_margin=reserved_margin,
        currently_deployed_margin=open_margin,
        remaining_deployable_margin=max(available_margin - size / lev - entry_fee - exit_fee, 0.0),
        requested_risk=requested_risk,
        approved_risk=approved_risk,
        approved_notional=size,
        raw_contracts=raw_contracts,
        approved_contracts=approved_contracts,
        minimum_executable_contracts=minimum_executable_contracts,
        contract_floor_notional_usdt=contract_floor_notional,
        contract_floor_uplift_applied=contract_floor_uplift_applied,
        contract_floor_uplift_from_usdt=contract_floor_uplift_from,
        estimated_margin=size / lev,
        estimated_entry_fee=entry_fee,
        estimated_exit_fee=exit_fee,
        aggregate_risk_before=open_risk,
        aggregate_risk_after=projected_risk,
        binding_cap=cap_source if cap_applied else "",
        participation_cap_usdt=participation_cap if participation_cap_present else None,
        metadata_timestamp=float(limits.metadata_timestamp or 0.0),
    )

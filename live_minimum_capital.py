"""Calculate technical, safety, and economic capital floors from OKX metadata."""
from __future__ import annotations

from typing import Any, Iterable


def analyze_instrument(
    meta: dict[str, Any],
    *,
    price: float,
    leverage: int,
    taker_fee_rate: float,
    stop_pct: float,
    risk_pct: float,
    operational_reserve_pct: float = 20.0,
    exit_legs: int = 3,
    software_min_notional_usdt: float = 0.0,
) -> dict[str, Any]:
    ct_val = float(meta.get("ct_val", meta.get("ctVal", 0.0)) or 0.0)
    min_qty = float(meta.get("min_sz", meta.get("minSz", 0.0)) or 0.0)
    max_leverage = float(meta.get("max_leverage", meta.get("lever", leverage)) or leverage)
    configured = max(1, min(int(leverage), int(max_leverage or leverage)))
    minimum_notional = min_qty * ct_val * float(price)
    minimum_margin = minimum_notional / configured
    roundtrip_fee = minimum_notional * abs(float(taker_fee_rate)) * 2.0
    stop_risk = minimum_notional * float(stop_pct) / 100.0
    reserve = max(minimum_margin * operational_reserve_pct / 100.0, roundtrip_fee * 3.0)
    technical_equity = minimum_margin + roundtrip_fee
    software_notional = max(float(software_min_notional_usdt or 0.0), minimum_notional)
    software_margin = software_notional / configured
    software_stop_risk = software_notional * float(stop_pct) / 100.0
    software_fees = software_notional * abs(float(taker_fee_rate)) * 2.0
    software_reserve = max(software_margin * operational_reserve_pct / 100.0, software_fees * 3.0)
    safe_equity = software_margin + software_fees + software_stop_risk + software_reserve
    viable_notional = max(minimum_notional * max(int(exit_legs), 1), software_notional)
    viable_margin = viable_notional / configured
    viable_fees = viable_notional * abs(float(taker_fee_rate)) * 2.0
    viable_risk = viable_notional * float(stop_pct) / 100.0
    equity_for_risk_budget = viable_risk / max(float(risk_pct) / 100.0, 1e-9)
    economic_equity = max(equity_for_risk_budget, viable_margin + viable_fees + reserve)
    return {
        "instrument": str(meta.get("inst_id", meta.get("instId", ""))),
        "price": float(price),
        "minimum_contract_quantity": min_qty,
        "minimum_notional_usdt": minimum_notional,
        "allowed_leverage": max_leverage,
        "configured_leverage": configured,
        "isolated_margin_required_usdt": minimum_margin,
        "estimated_entry_fee_usdt": minimum_notional * abs(float(taker_fee_rate)),
        "estimated_close_fee_usdt": minimum_notional * abs(float(taker_fee_rate)),
        "operational_reserve_usdt": reserve,
        "risk_at_stop_usdt": stop_risk,
        "minimum_viable_sl_pct": float(stop_pct),
        "minimum_viable_exit_contracts": min_qty * max(int(exit_legs), 1),
        "software_min_notional_usdt": software_notional,
        "technical_minimum_equity_usdt": technical_equity,
        "safe_minimum_equity_usdt": safe_equity,
        "economic_minimum_equity_usdt": economic_equity,
        "economic_minimum_available_usdt": viable_margin + viable_fees + max(reserve, software_reserve),
    }


def rank_candidates(metadata: Iterable[dict[str, Any]], prices: dict[str, float], **kwargs: Any) -> list[dict[str, Any]]:
    rows = []
    for meta in metadata:
        inst = str(meta.get("inst_id", meta.get("instId", "")))
        price = float(prices.get(inst, 0.0) or 0.0)
        if price <= 0 or str(meta.get("state", "live")) != "live":
            continue
        row = analyze_instrument(meta, price=price, **kwargs)
        if row["minimum_notional_usdt"] > 0:
            rows.append(row)
    return sorted(rows, key=lambda row: (row["economic_minimum_equity_usdt"], row["technical_minimum_equity_usdt"]))
